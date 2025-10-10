
#define _POSIX_C_SOURCE 199309L  /* Ensure CLOCK_REALTIME is defined */

#include "ctrl.h"
#include "context.h"
#include "log.h"

#include <pthread.h>
#include <stdatomic.h>
#include <string.h>
#include <inttypes.h>  /* For PRIu64 */
#include <errno.h>
#include <unistd.h>
#include <fcntl.h>
#include <arpa/inet.h>
#include <netinet/tcp.h>
#include <sys/socket.h>
#include <sys/select.h>
#include <time.h>
#include <stdlib.h>
#include <stdio.h>

#ifndef CTRL_PORT_DELTA
#define CTRL_PORT_DELTA 190   /* base_port + 190 => matches your logs 23456->23646 */
#endif

#ifndef MSG_NOSIGNAL
#define MSG_NOSIGNAL 0
#endif

/* ---- small protocol ------------------------------------------------------ */
enum {
    MSG_HELLO   = 1,  /* client->coord: {hdr}                        */
    MSG_HELLO_ACK=2,  /* coord->client: {hdr}                        */
    MSG_ARRIVE  = 3,  /* client->coord: {hdr + name[name_len]}       */
    MSG_RELEASE = 4,  /* coord->client: {hdr + name[name_len]}       */
    MSG_GOODBYE = 5   /* client->coord: {hdr}                         */
};

#define CTRL_NAME_MAX 63

#pragma pack(push,1)
typedef struct {
    uint8_t  type;        /* MSG_* */
    uint8_t  rsv;         /* reserved=0 */
    uint16_t name_len;    /* payload bytes of name (0..63), network order */
    uint32_t rank;        /* sender rank, network order */
    uint64_t gen;         /* barrier generation, network order */
} ctrl_hdr_t;
#pragma pack(pop)

/* ---- helpers ------------------------------------------------------------- */

static inline const char* ip_of(int r) {
    return g_ctx.tcp_cfg.ip_of_rank[r];
}
static inline int world_size(void) { return g_ctx.world_size; }
static inline int ctrl_port(void)  { return g_ctx.tcp_cfg.base_port + CTRL_PORT_DELTA; }

static int set_reuseaddr(int fd) {
    int yes = 1;
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(yes)) != 0) {
        log_warn("CTRL: setsockopt(SO_REUSEADDR) failed: %s", strerror(errno));
        return -1;
    }
    return 0;
}
static void set_tcp_opts(int fd) {
    int on = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &on, sizeof(on));
    /* optional: TCP keepalive */
    setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &on, sizeof(on));
}

static void set_rcv_timeout(int fd, uint32_t ms) {
    struct timeval tv; tv.tv_sec = ms/1000; tv.tv_usec = (ms%1000)*1000;
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
}
static void set_snd_timeout(int fd, uint32_t ms) {
    struct timeval tv; tv.tv_sec = ms/1000; tv.tv_usec = (ms%1000)*1000;
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
}

static int send_all(int fd, const void* buf, size_t len) {
    const char* p = (const char*)buf;
    while (len) {
        ssize_t n = send(fd, p, len, MSG_NOSIGNAL);
        if (n < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (n == 0) return -1;
        p += n; len -= (size_t)n;
    }
    return 0;
}
static int recv_all(int fd, void* buf, size_t len) {
    char* p = (char*)buf;
    while (len) {
        ssize_t n = recv(fd, p, len, 0);
        if (n < 0) {
            if (errno == EINTR) continue;
            return -1;
        }
        if (n == 0) return -1; /* peer closed */
        p += n; len -= (size_t)n;
    }
    return 0;
}

static int send_msg(int fd, uint8_t type, uint32_t rank, uint64_t gen,
                    const char* name) {
    uint16_t name_len = (uint16_t)(name ? strnlen(name, CTRL_NAME_MAX) : 0);

    ctrl_hdr_t h;
    h.type = type; h.rsv = 0;
    h.name_len = htons(name_len);
    h.rank     = htonl(rank);
    h.gen      = htobe64(gen);

    if (send_all(fd, &h, sizeof(h)) != 0) return -1;
    if (name_len) {
        if (send_all(fd, name, name_len) != 0) return -1;
    }
    return 0;
}

/* ---- sessions & barrier states ------------------------------------------ */

typedef struct {
    int fd;                 /* -1 if not connected */
    int rank;               /* peer rank (0..world-1) */
    int ready;              /* handshake completed */
    pthread_mutex_t send_mu;
} session_t;

typedef struct bstate_s {
    char name[CTRL_NAME_MAX+1];
    uint64_t gen;
    int expected;           /* world_size */
    int arrived;            /* count */
    uint8_t* bitmap;        /* arrived bitmap, bytes=(expected+7)/8 */
    int released;           /* 1 after release broadcast */
    pthread_mutex_t mu;
    pthread_cond_t  cv;
    struct bstate_s* next;
} bstate_t;

typedef struct {
    /* config snapshot */
    int rank;
    int world;
    int coord;

    /* role flags */
    int is_coord;

    /* networking */
    int listen_fd;          /* only for coordinator, -1 otherwise */
    session_t* sess;        /* array[world], used on both roles */
    pthread_t io_th;
    _Atomic int running;

    /* barrier state list (both roles, semantics differ) */
    pthread_mutex_t bst_mu;
    bstate_t* bst_head;

    /* coordinator: HELLO readiness */
    pthread_mutex_t hello_mu;
    pthread_cond_t  hello_cv;
    int hello_ready;        /* number of ready client sessions */
} ctrl_state_t;

static ctrl_state_t C;

/* ---- bitmap helpers ---- */
static inline int bm_test_set(uint8_t* bm, int idx) {
    int byte = idx >> 3, bit = idx & 7;
    uint8_t m = (uint8_t)(1u << bit);
    int old = !!(bm[byte] & m);
    bm[byte] |= m;
    return old;
}

/* ---- barrier state table ---- */
static bstate_t* bst_get_or_create(const char* name, uint64_t gen, int expected) {
    pthread_mutex_lock(&C.bst_mu);
    bstate_t* s = C.bst_head;
    while (s) {
        if (s->gen == gen && strncmp(s->name, name, CTRL_NAME_MAX) == 0) {
            pthread_mutex_unlock(&C.bst_mu);
            return s;
        }
        s = s->next;
    }
    /* create */
    s = (bstate_t*)calloc(1, sizeof(*s));
    if (!s) { pthread_mutex_unlock(&C.bst_mu); return NULL; }
    strncpy(s->name, name, CTRL_NAME_MAX);
    s->name[CTRL_NAME_MAX] = '\0';
    s->gen = gen;
    s->expected = expected;
    s->arrived = 0;
    s->released = 0;
    pthread_mutex_init(&s->mu, NULL);
    pthread_cond_init(&s->cv, NULL);
    int bytes = (expected + 7) / 8;
    s->bitmap = (uint8_t*)calloc((size_t)bytes, 1);
    s->next = C.bst_head;
    C.bst_head = s;
    pthread_mutex_unlock(&C.bst_mu);
    return s;
}

/* ---- coordinator: release barrier --------------------------------------- */
static void coord_release_barrier(bstate_t* s) {
    if (s->released) return;
    /* send RELEASE to all arrived remote sessions (exclude self rank=0) */
    for (int r = 0; r < C.world; ++r) {
        if (r == C.rank) continue;
        int byte = r >> 3, bit = r & 7;
        if (s->bitmap[byte] & (uint8_t)(1u<<bit)) {
            session_t* se = &C.sess[r];
            if (se->fd >= 0 && se->ready) {
                /* only to those who arrived */
                pthread_mutex_lock(&se->send_mu);
                int rc = send_msg(se->fd, MSG_RELEASE, (uint32_t)C.rank, s->gen, s->name);
                pthread_mutex_unlock(&se->send_mu);
                if (rc != 0) {
                    log_warn("CTRL: send RELEASE to rank=%d failed: %s", r, strerror(errno));
                }
            }
        }
    }
    s->released = 1;
    pthread_cond_broadcast(&s->cv);  /* wake local coordinator waiter */
    log_info("CTRL: RELEASE name=%s gen=%" PRIu64 " broadcasted", s->name, s->gen);
}

/* ---- I/O threads --------------------------------------------------------- */

/* coordinator I/O loop: accept + read ARRIVE/GOODBYE + send HELLO_ACK/RELEASE */
static void* coord_io_main(void* arg) {
    (void)arg;
    log_info("CTRL: server I/O thread started (rank=%d)", C.rank);

    while (atomic_load(&C.running)) {
        fd_set rfds; FD_ZERO(&rfds);
        int maxfd = -1;

        if (C.listen_fd >= 0) { FD_SET(C.listen_fd, &rfds); if (C.listen_fd > maxfd) maxfd = C.listen_fd; }
        for (int r = 0; r < C.world; ++r) {
            if (r == C.rank) continue;
            if (C.sess[r].fd >= 0) { FD_SET(C.sess[r].fd, &rfds); if (C.sess[r].fd > maxfd) maxfd = C.sess[r].fd; }
        }

        struct timeval tv = { .tv_sec = 0, .tv_usec = 200*1000 }; /* 200ms */
        int rv = select(maxfd+1, &rfds, NULL, NULL, &tv);
        if (rv < 0) {
            if (errno == EINTR) continue;
            log_warn("CTRL: select error: %s", strerror(errno));
            continue;
        }
        /* accept new */
        if (C.listen_fd >= 0 && FD_ISSET(C.listen_fd, &rfds)) {
            struct sockaddr_in cli; socklen_t sl = sizeof(cli);
            int cfd = accept(C.listen_fd, (struct sockaddr*)&cli, &sl);
            if (cfd < 0) {
                log_warn("CTRL: accept failed: %s", strerror(errno));
            } else {
                set_tcp_opts(cfd);
                char rip[INET_ADDRSTRLEN]; inet_ntop(AF_INET, &cli.sin_addr, rip, sizeof(rip));
                log_debug("CTRL: accepted cfd=%d from %s:%u", cfd, rip, (unsigned)ntohs(cli.sin_port));

                /* Expect HELLO immediately */
                ctrl_hdr_t h;
                if (recv_all(cfd, &h, sizeof(h)) != 0) {
                    log_warn("CTRL: recv HELLO header failed (closing cfd=%d)", cfd);
                    close(cfd); continue;
                }
                uint8_t type = h.type;
                uint32_t src = ntohl(h.rank);
                if (type != MSG_HELLO || src <= 0 || src >= (uint32_t)C.world) {
                    log_warn("CTRL: bad HELLO (type=%u rank=%u), closing cfd=%d", type, src, cfd);
                    close(cfd); continue;
                }
                /* register session */
                if (C.sess[src].fd >= 0) {
                    log_warn("CTRL: duplicate HELLO from rank=%u, closing new cfd=%d", src, cfd);
                    close(cfd); continue;
                }
                C.sess[src].fd = cfd;
                C.sess[src].rank = (int)src;
                C.sess[src].ready = 1;
                pthread_mutex_init(&C.sess[src].send_mu, NULL);

                /* ACK */
                if (send_msg(cfd, MSG_HELLO_ACK, (uint32_t)C.rank, 0, NULL) != 0) {
                    log_warn("CTRL: send HELLO_ACK to rank=%u failed", src);
                } else {
                    log_info("CTRL: HELLO from rank=%u accepted", src);
                }

                /* mark readiness */
                pthread_mutex_lock(&C.hello_mu);
                C.hello_ready++;
                pthread_cond_broadcast(&C.hello_cv);
                pthread_mutex_unlock(&C.hello_mu);
            }
        }

        /* read messages from clients */
        for (int r = 0; r < C.world; ++r) {
            if (r == C.rank) continue;
            int fd = C.sess[r].fd;
            if (fd < 0) continue;
            if (!FD_ISSET(fd, &rfds)) continue;

            ctrl_hdr_t h;
            if (recv_all(fd, &h, sizeof(h)) != 0) {
                log_warn("CTRL: peer rank=%d closed or header read failed; closing session", r);
                close(fd); C.sess[r].fd = -1; C.sess[r].ready = 0;
                continue;
            }
            uint8_t type = h.type;
            uint16_t nlen = ntohs(h.name_len);
            uint32_t sr   = ntohl(h.rank);
            uint64_t gen  = be64toh(h.gen);

            if (type == MSG_ARRIVE) {
                if (nlen > CTRL_NAME_MAX) { log_warn("CTRL: ARRIVE name too long"); /* drain */ char tmp[256]; size_t drain = nlen; while (drain) { size_t k = drain>sizeof(tmp)?sizeof(tmp):drain; if (recv_all(fd,tmp,k)!=0) break; drain-=k; } continue; }
                char name[CTRL_NAME_MAX+1]; memset(name,0,sizeof(name));
                if (nlen && recv_all(fd, name, nlen) != 0) {
                    log_warn("CTRL: ARRIVE name read failed from rank=%u", sr);
                    continue;
                }

                bstate_t* s = bst_get_or_create(name, gen, C.world);
                if (!s) { log_error("CTRL: OOM for bstate name=%s", name); continue; }

                pthread_mutex_lock(&s->mu);
                if (!bm_test_set(s->bitmap, (int)sr)) {
                    s->arrived++;
                    log_info("CTRL: ARRIVE name=%s gen=%" PRIu64 " from rank=%u (%d/%d)",
                             s->name, s->gen, sr, s->arrived, s->expected);
                } else {
                    log_warn("CTRL: duplicate ARRIVE ignored name=%s gen=%" PRIu64 " from rank=%u",
                             s->name, s->gen, sr);
                }
                if (s->arrived >= s->expected) {
                    log_info("CTRL: threshold reached for name=%s gen=%" PRIu64 ", releasing",
                             s->name, s->gen);
                    coord_release_barrier(s);
                }
                pthread_mutex_unlock(&s->mu);
            } else if (type == MSG_GOODBYE) {
                log_info("CTRL: GOODBYE from rank=%u", sr);
                close(fd); C.sess[r].fd = -1; C.sess[r].ready = 0;
            } else {
                /* unexpected or HELLO/ACK on data path */
                if (nlen) {
                    char sink[256]; size_t drain=nlen; while (drain){ size_t k=drain>sizeof(sink)?sizeof(sink):drain; if (recv_all(fd,sink,k)!=0) break; drain-=k; }
                }
                log_warn("CTRL: unexpected msg type=%u from rank=%u", type, sr);
            }
        }
    }

    log_info("CTRL: server I/O thread exiting");
    return NULL;
}

/* client I/O loop: only recv RELEASE and wake local waiters */
static void* client_io_main(void* arg) {
    (void)arg;
    log_info("CTRL: client I/O thread started (rank=%d)", C.rank);

    int fd = C.sess[C.coord].fd;
    while (atomic_load(&C.running)) {
        fd_set rfds; FD_ZERO(&rfds);
        FD_SET(fd, &rfds);
        struct timeval tv = { .tv_sec = 0, .tv_usec = 200*1000 };
        int rv = select(fd+1, &rfds, NULL, NULL, &tv);
        if (rv < 0) {
            if (errno == EINTR) continue;
            log_warn("CTRL: select error: %s", strerror(errno));
            continue;
        }
        if (rv == 0) continue;

        ctrl_hdr_t h;
        if (recv_all(fd, &h, sizeof(h)) != 0) {
            log_warn("CTRL: coordinator closed; client I/O exiting");
            break;
        }
        uint8_t  type = h.type;
        uint16_t nlen = ntohs(h.name_len);
        uint64_t gen  = be64toh(h.gen);

        if (type != MSG_RELEASE) {
            /* drain payload if any */
            if (nlen) {
                char sink[256]; size_t d=nlen; while (d){ size_t k=d>sizeof(sink)?sizeof(sink):d; if (recv_all(fd,sink,k)!=0) break; d-=k; }
            }
            log_warn("CTRL: unexpected msg type=%u from coordinator", type);
            continue;
        }

        if (nlen > CTRL_NAME_MAX) { /* drain and ignore */
            char sink[256]; size_t d=nlen; while (d){ size_t k=d>sizeof(sink)?sizeof(sink):d; if (recv_all(fd,sink,k)!=0) break; d-=k; }
            log_warn("CTRL: RELEASE name too long");
            continue;
        }
        char name[CTRL_NAME_MAX+1]; memset(name,0,sizeof(name));
        if (nlen && recv_all(fd, name, nlen) != 0) {
            log_warn("CTRL: RELEASE name read failed");
            continue;
        }

        bstate_t* s = bst_get_or_create(name, gen, C.world);
        if (!s) { log_error("CTRL: OOM on client bstate"); continue; }

        pthread_mutex_lock(&s->mu);
        s->released = 1;
        pthread_cond_broadcast(&s->cv);
        pthread_mutex_unlock(&s->mu);

        log_info("CTRL: RELEASE received name=%s gen=%" PRIu64, name, gen);
    }

    log_info("CTRL: client I/O thread exiting");
    return NULL;
}

/* ---- public API ---------------------------------------------------------- */

int ctrl_start(void) {
    memset(&C, 0, sizeof(C));
    C.rank  = g_ctx.rank;
    C.world = g_ctx.world_size;
    C.coord = 0;
    C.is_coord = (C.rank == C.coord);
    C.listen_fd = -1;
    pthread_mutex_init(&C.bst_mu, NULL);
    pthread_mutex_init(&C.hello_mu, NULL);
    pthread_cond_init(&C.hello_cv, NULL);
    atomic_store(&C.running, 1);

    /* alloc sessions */
    C.sess = (session_t*)calloc((size_t)C.world, sizeof(session_t));
    if (!C.sess) { log_error("CTRL: OOM sessions"); return -1; }
    for (int i=0;i<C.world;++i) { C.sess[i].fd = -1; C.sess[i].rank = i; C.sess[i].ready = 0; }

    if (C.is_coord) {
        /* rank0: listen */
        int fd = socket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0) { log_error("CTRL: socket() failed: %s", strerror(errno)); return -1; }
        set_reuseaddr(fd);
        set_tcp_opts(fd);

        struct sockaddr_in addr; memset(&addr, 0, sizeof(addr));
        addr.sin_family = AF_INET;
        addr.sin_port   = htons((uint16_t)ctrl_port());
        /* bind to INADDR_ANY for robustness */
        addr.sin_addr.s_addr = htonl(INADDR_ANY);

        if (bind(fd, (struct sockaddr*)&addr, sizeof(addr)) != 0) {
            log_error("CTRL: bind(0.0.0.0:%d) failed: %s", ctrl_port(), strerror(errno));
            close(fd); return -1;
        }
        if (listen(fd, 128) != 0) {
            log_error("CTRL: listen failed: %s", strerror(errno));
            close(fd); return -1;
        }
        C.listen_fd = fd;

        int rc = pthread_create(&C.io_th, NULL, coord_io_main, NULL);
        if (rc != 0) {
            log_error("CTRL: failed to start coord I/O thread: rc=%d", rc);
            close(fd); C.listen_fd=-1; return -1;
        }

        /* Option: wait until all HELLOs arrived (best practice) */
        uint32_t boot_wait_ms = 60000;
        struct timespec ts; clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_sec += boot_wait_ms/1000; ts.tv_nsec += (long)(boot_wait_ms%1000)*1000000L;
        if (ts.tv_nsec >= 1000000000L) { ts.tv_sec++; ts.tv_nsec -= 1000000000L; }

        pthread_mutex_lock(&C.hello_mu);
        while (C.hello_ready < (C.world - 1)) {
            int e = pthread_cond_timedwait(&C.hello_cv, &C.hello_mu, &ts);
            if (e == ETIMEDOUT) break;
        }
        int ready = C.hello_ready;
        pthread_mutex_unlock(&C.hello_mu);

        log_info("CTRL: started (rank=%d, world=%d, coord=%d, port=%d) hello_ready=%d/%d",
                 C.rank, C.world, C.coord, ctrl_port(), ready, C.world-1);
        return 0;
    } else {
        /* client: connect to coordinator and HELLO/ACK */
        const char* host = ip_of(C.coord);
        if (!host || !*host) { log_error("CTRL: coordinator IP not found"); return -1; }

        int retry_max = g_ctx.tcp_cfg.retry_max > 0 ? g_ctx.tcp_cfg.retry_max : 10;
        uint32_t pertry = g_ctx.tcp_cfg.connect_timeout_ms > 0 ? (uint32_t)g_ctx.tcp_cfg.connect_timeout_ms : 3000;

        int fd = -1;
        for (int a=1;a<=retry_max;++a) {
            fd = socket(AF_INET, SOCK_STREAM, 0);
            if (fd < 0) { usleep(1000*50); continue; }
            set_tcp_opts(fd);
            set_rcv_timeout(fd, pertry);
            set_snd_timeout(fd, pertry);

            struct sockaddr_in a4; memset(&a4, 0, sizeof(a4));
            a4.sin_family = AF_INET; a4.sin_port = htons((uint16_t)ctrl_port());
            if (inet_pton(AF_INET, host, &a4.sin_addr) != 1) {
                log_error("CTRL: inet_pton failed for %s", host);
                close(fd); fd=-1; break;
            }
            log_info("CTRL: dialing attempt %d/%d -> %s:%d", a, retry_max, host, ctrl_port());
            if (connect(fd, (struct sockaddr*)&a4, sizeof(a4)) != 0) {
                log_warn("CTRL: connect failed: %s", strerror(errno));
                close(fd); fd = -1;
                if (a < retry_max) usleep(1000*100);
                continue;
            }
            /* HELLO */
            ctrl_hdr_t h; memset(&h,0,sizeof(h));
            h.type = MSG_HELLO; h.rank = htonl((uint32_t)C.rank);
            if (send_all(fd, &h, sizeof(h)) != 0) {
                log_warn("CTRL: send HELLO failed: %s", strerror(errno));
                close(fd); fd=-1; if (a<retry_max) usleep(1000*100); continue;
            }
            /* ACK */
            ctrl_hdr_t ack;
            if (recv_all(fd, &ack, sizeof(ack)) != 0 || ack.type != MSG_HELLO_ACK) {
                log_warn("CTRL: HELLO_ACK failed/invalid");
                close(fd); fd=-1; if (a<retry_max) usleep(1000*100); continue;
            }
            break; /* success */
        }
        if (fd < 0) {
            log_error("CTRL: failed to connect coordinator");
            return -1;
        }
        C.sess[C.coord].fd = fd;
        C.sess[C.coord].ready = 1;
        pthread_mutex_init(&C.sess[C.coord].send_mu, NULL);

        int rc = pthread_create(&C.io_th, NULL, client_io_main, NULL);
        if (rc != 0) {
            log_error("CTRL: failed to start client I/O thread rc=%d", rc);
            close(fd); C.sess[C.coord].fd=-1; return -1;
        }

        log_info("CTRL: started (rank=%d, world=%d) connected to %s:%d",
                 C.rank, C.world, host, ctrl_port());
        return 0;
    }
}

/* Return 0 on success; ETIMEDOUT on timeout; -1 on error */
int ctrl_barrier(const char* name, uint64_t gen, uint32_t timeout_ms) {
    if (!name || !*name) return -1;
    if (timeout_ms == 0) timeout_ms = 60000;

    log_info("CTRL: barrier enter name=%s gen=%" PRIu64 " role=%s timeout=%ums",
             name, gen, (C.is_coord ? "coordinator" : "client"), (unsigned)timeout_ms);

    bstate_t* s = bst_get_or_create(name, gen, C.world);
    if (!s) { log_error("CTRL: OOM barrier state"); return -1; }

    if (!C.is_coord) {
        /* client: send ARRIVE then wait RELEASE */
        if (C.sess[C.coord].fd < 0 || !C.sess[C.coord].ready) {
            log_error("CTRL: no control session to coordinator");
            return -1;
        }
        /* send ARRIVE */
        pthread_mutex_lock(&C.sess[C.coord].send_mu);
        int rc = send_msg(C.sess[C.coord].fd, MSG_ARRIVE, (uint32_t)C.rank, gen, name);
        pthread_mutex_unlock(&C.sess[C.coord].send_mu);
        if (rc != 0) {
            log_warn("CTRL: send ARRIVE failed: %s", strerror(errno));
            return -1;
        }

        /* wait for RELEASE */
        struct timespec ts; clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_sec += timeout_ms/1000; ts.tv_nsec += (long)(timeout_ms%1000)*1000000L;
        if (ts.tv_nsec >= 1000000000L) { ts.tv_sec++; ts.tv_nsec -= 1000000000L; }

        int e = 0;
        pthread_mutex_lock(&s->mu);
        while (!s->released && e == 0) {
            e = pthread_cond_timedwait(&s->cv, &s->mu, &ts);
        }
        int ok = s->released;
        pthread_mutex_unlock(&s->mu);

        if (!ok) {
            log_warn("CTRL: barrier(name=%s, gen=%" PRIu64 ") timeout (client)", name, gen);
            return ETIMEDOUT;
        }
        log_info("CTRL: barrier(name=%s, gen=%" PRIu64 ") passed (client)", name, gen);
        return 0;
    } else {
        /* coordinator: mark self arrived then wait release (I/O thread releases) */
        pthread_mutex_lock(&s->mu);
        if (!bm_test_set(s->bitmap, C.rank)) {
            s->arrived++;
            log_info("CTRL: ARRIVE (self) name=%s gen=%" PRIu64 " (%d/%d)",
                     s->name, s->gen, s->arrived, s->expected);
        }
        if (s->arrived >= s->expected) {
            coord_release_barrier(s);
        }
        if (!s->released) {
            struct timespec ts; clock_gettime(CLOCK_REALTIME, &ts);
            ts.tv_sec += timeout_ms/1000; ts.tv_nsec += (long)(timeout_ms%1000)*1000000L;
            if (ts.tv_nsec >= 1000000000L) { ts.tv_sec++; ts.tv_nsec -= 1000000000L; }

            int e = 0;
            while (!s->released && e == 0) {
                e = pthread_cond_timedwait(&s->cv, &s->mu, &ts);
            }
            if (!s->released) {
                pthread_mutex_unlock(&s->mu);
                log_warn("CTRL: barrier(name=%s, gen=%" PRIu64 ") timeout (coord)", name, gen);
                return ETIMEDOUT;
            }
        }
        pthread_mutex_unlock(&s->mu);
        log_info("CTRL: barrier(name=%s, gen=%" PRIu64 ") passed (coordinator)", name, gen);
        return 0;
    }
}

void ctrl_stop(void) {
    if (!atomic_load(&C.running)) return;
    atomic_store(&C.running, 0);

    if (!C.is_coord) {
        /* send GOODBYE (best effort) */
        if (C.sess[C.coord].fd >= 0) {
            pthread_mutex_lock(&C.sess[C.coord].send_mu);
            (void)send_msg(C.sess[C.coord].fd, MSG_GOODBYE, (uint32_t)C.rank, 0, NULL);
            pthread_mutex_unlock(&C.sess[C.coord].send_mu);
        }
    }

    if (C.io_th) pthread_join(C.io_th, NULL);

    /* close sockets */
    if (C.listen_fd >= 0) { close(C.listen_fd); C.listen_fd = -1; }
    for (int r=0;r<C.world;++r) {
        if (C.sess && C.sess[r].fd >= 0) {
            close(C.sess[r].fd);
            C.sess[r].fd = -1;
        }
    }

    /* free barrier states */
    pthread_mutex_lock(&C.bst_mu);
    bstate_t* s = C.bst_head;
    while (s) {
        bstate_t* nx = s->next;
        free(s->bitmap);
        pthread_mutex_destroy(&s->mu);
        pthread_cond_destroy(&s->cv);
        free(s);
        s = nx;
    }
    C.bst_head = NULL;
    pthread_mutex_unlock(&C.bst_mu);
    pthread_mutex_destroy(&C.bst_mu);

    if (C.sess) {
        for (int r=0;r<C.world;++r) pthread_mutex_destroy(&C.sess[r].send_mu);
        free(C.sess); C.sess = NULL;
    }

    pthread_mutex_destroy(&C.hello_mu);
    pthread_cond_destroy(&C.hello_cv);

    log_info("CTRL: stopped");
}
