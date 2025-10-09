/**
 * @file ctrl_min.c
 * @brief Minimal blocking TCP barrier implementation (no epoll, no token ring).
 *        Coordinator keeps sockets of ARRIVE participants and sends RELEASE to all
 *        once the expected count is reached; non-coordinators just connect and wait.
 */

#define _GNU_SOURCE 1
#include "ctrl_min.h"
#include "log.h"
#include "context.h"   /* for tcp_config_t definition and ip_of_rank[] fields */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <inttypes.h>
#include <stdatomic.h>
#include <unistd.h>
#include <pthread.h>
#include <fcntl.h>
#include <netdb.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <sys/time.h>

#ifndef MSG_NOSIGNAL
#define MSG_NOSIGNAL 0
#endif

/* ---- On-wire message (fixed little header + name bytes) ---- */
#pragma pack(push, 1)
typedef struct {
    uint32_t magic;     /* 'L','E','O','B' => 0x424F454C */
    uint32_t op;        /* 1=ARRIVE */
    uint32_t name_len;  /* bytes, not including NUL */
    uint32_t src_rank;  /* who arrived */
    uint64_t gen;       /* barrier round */
} arrive_hdr_t;
#pragma pack(pop)

enum { MAGIC_LEOB = 0x424F454Cu, OP_ARRIVE = 1 };

/* ---- Coordinator-side barrier state ---- */
typedef struct wait_fd_s {
    int fd;
    struct wait_fd_s* next;
} wait_fd_t;

typedef struct bstate_s {
    char     name[64];
    uint64_t gen;
    int      expected;        /* world_size */
    int      arrived_cnt;
    unsigned char* bitmap;    /* world_size bits */
    wait_fd_t* fds;           /* list of participant sockets */
    int      released;        /* 1 after release */
    pthread_cond_t  cv;       /* for coordinator's own barrier wait */
    pthread_mutex_t mu;
    struct bstate_s* next;
} bstate_t;

/* ---- Global ---- */
static struct {
    ctrlm_cfg_t cfg;
    _Atomic int running;
    int listen_fd;
    pthread_t srv_th;

    /* coordinator state list */
    bstate_t* head;
    pthread_mutex_t list_mu;
} G;

 /* ---- Helpers ---- */
static inline int world_size(void) { return G.cfg.tcp ? G.cfg.tcp->world_size : 0; }
static inline const char* ip_of(int rank) { return G.cfg.tcp ? G.cfg.tcp->ip_of_rank[rank] : NULL; }
static inline int ctrl_port(void) { return G.cfg.tcp ? (G.cfg.tcp->base_port + CTRL_PORT_DELTA) : 0; }
 
/* ---- Utilities ---- */
static int set_timeouts(int fd, uint32_t timeout_ms) {
    struct timeval tv;
    tv.tv_sec  = timeout_ms / 1000;
    tv.tv_usec = (timeout_ms % 1000) * 1000;
    if (setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv)) != 0) return -1;
    if (setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO,  &tv, sizeof(tv)) != 0) return -1;
    int one = 1;
    (void)setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &one, sizeof(one));
    return 0;
}
 
static int set_reuseaddr(int fd) {
    int one = 1;
    if (setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one)) != 0) return -1;
#ifdef SO_REUSEPORT
    (void)setsockopt(fd, SOL_SOCKET, SO_REUSEPORT, &one, sizeof(one));
#endif
    return 0;
}
 
static int bitmap_test_set(unsigned char* bm, int idx) {
    unsigned u = (unsigned)idx;
    unsigned pos = u >> 3;
    unsigned char mask = (unsigned char)(1u << (u & 7u));
    int was = (bm[pos] & mask) != 0;
    bm[pos] |= mask;
    return was;
}
 
/* Find or create barrier state by (name, gen). Coordinator only. */
static bstate_t* bstate_get_or_create(const char* name, uint64_t gen) {
    pthread_mutex_lock(&G.list_mu);
    bstate_t* s = G.head;
    while (s) {
        if (s->gen == gen && strncmp(s->name, name, sizeof(s->name)) == 0) break;
        s = s->next;
    }
    if (!s) {
        s = (bstate_t*)calloc(1, sizeof(*s));
        if (!s) { pthread_mutex_unlock(&G.list_mu); return NULL; }
        strncpy(s->name, name, sizeof(s->name)-1);
        s->gen = gen;
        s->expected = world_size();
        size_t sz = (size_t)((world_size() + 7) / 8);
        s->bitmap = (unsigned char*)calloc(1, sz);
        pthread_mutex_init(&s->mu, NULL);
        pthread_cond_init(&s->cv, NULL);
        s->next = G.head; G.head = s;
    }
    pthread_mutex_unlock(&G.list_mu);
    return s;
}

/* Release all participants: send one byte, close sockets, signal local waiters. */
static void bstate_release(bstate_t* s) {
    if (!s || s->released) return;
    s->released = 1;
    /* send 'R' to each participant and close */
    wait_fd_t* w = s->fds;
    while (w) {
        char R = 'R';
        (void)send(w->fd, &R, 1, MSG_NOSIGNAL);
        close(w->fd);
        wait_fd_t* nx = w->next; free(w); w = nx;
    }
    s->fds = NULL;
    /* wake coordinator local waiter if present */
    pthread_cond_broadcast(&s->cv);
    log_info("CTRLm: RELEASE name=%s gen=%" PRIu64 " broadcasted", s->name, s->gen);
}

/* ---- Coordinator server thread ---- */
static void* server_main(void* arg) {
    (void)arg;
    log_info("CTRLm: server started on %s:%d (coord rank=%d)",
        G.cfg.bind_ip ? G.cfg.bind_ip : "0.0.0.0", ctrl_port(), G.cfg.coordinator_rank);

    while (atomic_load(&G.running)) {
        struct sockaddr_in cli; socklen_t sl = sizeof(cli);
        int cfd = accept(G.listen_fd, (struct sockaddr*)&cli, &sl);
        if (cfd < 0) {
            if (errno == EINTR) continue;
            if (errno == EAGAIN || errno == EWOULDBLOCK) { usleep(1000); continue; }
            log_warn("CTRLm: accept error: %s", strerror(errno));
            usleep(1000);
            continue;
        }

        arrive_hdr_t h;
        ssize_t n = recv(cfd, &h, sizeof(h), MSG_WAITALL);
        if (n != (ssize_t)sizeof(h)) {
            log_warn("CTRLm: short header (%zd), closing", n);
            close(cfd);
            continue;
        }
        if (h.magic != MAGIC_LEOB || h.op != OP_ARRIVE || h.name_len > 63u) {
            log_warn("CTRLm: bad header (magic/op/name_len), closing");
            close(cfd);
            continue;
        }
        char name[64] = {0};
        if (h.name_len) {
            n = recv(cfd, name, h.name_len, MSG_WAITALL);
            if (n != (ssize_t)h.name_len) {
                log_warn("CTRLm: short name (%zd), closing", n);
                close(cfd);
                continue;
            }
            name[h.name_len] = '\0';
        }

        bstate_t* s = bstate_get_or_create(name, h.gen);
        if (!s) { log_error("CTRLm: OOM state"); close(cfd); continue; }

        pthread_mutex_lock(&s->mu);
        if (!bitmap_test_set(s->bitmap, (int)h.src_rank)) {
            s->arrived_cnt++;
            /* track this socket for later RELEASE */
            wait_fd_t* w = (wait_fd_t*)malloc(sizeof(*w));
            if (w) { w->fd = cfd; w->next = s->fds; s->fds = w; }
            log_debug("CTRLm: ARRIVE name=%s gen=%" PRIu64 " from rank=%u (%d/%d)",
                    s->name, s->gen, h.src_rank, s->arrived_cnt, s->expected);
        } else {
            /* duplicate arrive: close socket immediately */
            close(cfd);
        }

        if (s->arrived_cnt >= s->expected) {
            bstate_release(s);
        }
        pthread_mutex_unlock(&s->mu);
    }

    log_info("CTRLm: server exiting");
    return NULL;
}

/* ---- Public API ---- */
int ctrlm_start() {
    memset(&G, 0, sizeof(G));
    G.cfg.rank = g_ctx.rank;
    G.cfg.coordinator_rank = 0;                 /* change if you want a different coordinator */
    G.cfg.tcp  = &g_ctx.tcp_cfg;                /* reuse your already-loaded tcp_config_t */
    G.cfg.bind_ip = ip_of(G.cfg.rank);                       /* NULL => bind to INADDR_ANY ("0.0.0.0") */


    if (!G.cfg.tcp) {
        log_error("CTRLm: tcp_config_t pointer is NULL");
        return -1;
    }

    pthread_mutex_init(&G.list_mu, NULL);
    G.listen_fd = -1;
    atomic_store(&G.running, 1);

    if (G.cfg.rank == G.cfg.coordinator_rank) {
        /* create listen socket */
        int fd = socket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0) { log_error("CTRLm: socket() failed"); return -1; }
        if (set_reuseaddr(fd) != 0) { close(fd); return -1; }

        struct sockaddr_in addr; memset(&addr, 0, sizeof(addr));
        addr.sin_family = AF_INET;
        addr.sin_port   = htons((uint16_t)ctrl_port());
        if (G.cfg.bind_ip && strcmp(G.cfg.bind_ip, "0.0.0.0") != 0) {
            if (inet_pton(AF_INET, G.cfg.bind_ip, &addr.sin_addr) != 1) {
                log_error("CTRLm: invalid bind_ip");
                close(fd); return -1;
            }
        } else {
            addr.sin_addr.s_addr = htonl(INADDR_ANY);
        }
        if (bind(fd, (struct sockaddr*)&addr, sizeof(addr)) != 0) {
            log_error("CTRLm: bind failed: %s", strerror(errno));
            close(fd); return -1;
        }
        if (listen(fd, 128) != 0) {
            log_error("CTRLm: listen failed: %s", strerror(errno));
            close(fd); return -1;
        }
        /* non-blocking accept helps shutdown responsiveness */
        int flags = fcntl(fd, F_GETFL, 0); fcntl(fd, F_SETFL, flags | O_NONBLOCK);

        G.listen_fd = fd;
        int rc = pthread_create(&G.srv_th, NULL, server_main, NULL);
        if (rc != 0) {
            log_error("CTRLm: failed to start server thread (rc=%d)", rc);
            close(fd); G.listen_fd = -1; return -1;
        }
    }
    log_info("CTRLm: started (rank=%d, world=%d, coord=%d, port=%d)",
             G.cfg.rank, world_size(), G.cfg.coordinator_rank, ctrl_port());
    return 0;
}

void ctrlm_stop(void) {
    if (!atomic_load(&G.running)) return;
    atomic_store(&G.running, 0);

    if (G.listen_fd >= 0) {
        /* close listen to break accept loop */
        close(G.listen_fd);
        G.listen_fd = -1;
        pthread_join(G.srv_th, NULL);
    }

    /* free barrier states */
    pthread_mutex_lock(&G.list_mu);
    bstate_t* s = G.head;
    while (s) {
        bstate_t* nx = s->next;
        /* close any pending fds just in case */
        wait_fd_t* w = s->fds;
        while (w) { close(w->fd); wait_fd_t* wn = w->next; free(w); w = wn; }
        free(s->bitmap);
        pthread_mutex_destroy(&s->mu);
        pthread_cond_destroy(&s->cv);
        free(s);
        s = nx;
    }
    G.head = NULL;
    pthread_mutex_unlock(&G.list_mu);
    pthread_mutex_destroy(&G.list_mu);

    log_info("CTRLm: stopped");
}

/* Non-coordinator connects, sends ARRIVE, waits for single-byte RELEASE. */
static int barrier_client_wait(const char* host, int port,
                            const char* name, uint64_t gen, uint32_t timeout_ms,
                            int src_rank)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) { log_error("CTRLm: socket() fail"); return -1; }
    set_timeouts(fd, timeout_ms);

    struct sockaddr_in a; memset(&a, 0, sizeof(a));
    a.sin_family = AF_INET; a.sin_port = htons((uint16_t)port);
    if (inet_pton(AF_INET, host, &a.sin_addr) != 1) {
        struct hostent* he = gethostbyname(host);
        if (!he || !he->h_addr_list || !he->h_addr_list[0]) { close(fd); return -1; }
        memcpy(&a.sin_addr, he->h_addr_list[0], (size_t)he->h_length);
    }
    if (connect(fd, (struct sockaddr*)&a, sizeof(a)) != 0) {
        log_warn("CTRLm: connect(%s:%d) fail: %s", host, port, strerror(errno));
        close(fd); return -1;
    }

    arrive_hdr_t h;
    h.magic = MAGIC_LEOB; h.op = OP_ARRIVE;
    h.name_len = (uint32_t)strnlen(name, 63);
    h.src_rank = (uint32_t)src_rank;
    h.gen = gen;

    if (send(fd, &h, sizeof(h), MSG_NOSIGNAL) != (ssize_t)sizeof(h)) { close(fd); return -1; }
    if (h.name_len) {
        if (send(fd, name, h.name_len, MSG_NOSIGNAL) != (ssize_t)h.name_len) { close(fd); return -1; }
    }

    /* wait for single-byte RELEASE */
    char R;
    ssize_t n = recv(fd, &R, 1, 0);
    int ok = (n == 1 && R == 'R') ? 0 : -1;
    close(fd);
    return ok;
}

int ctrlm_barrier(const char* name, uint64_t gen, uint32_t timeout_ms) {
    if (!name || !*name) return -1;
    if (timeout_ms == 0) timeout_ms = 60000;

    if (G.cfg.rank != G.cfg.coordinator_rank) {
        const char* host = ip_of(G.cfg.coordinator_rank);
        if (!host || !*host) { log_error("CTRLm: coordinator IP not found"); return -1; }
        int rc = barrier_client_wait(host, ctrl_port(), name, gen, timeout_ms,
                                     G.cfg.rank);
        if (rc != 0) {
            log_warn("CTRLm: barrier(name=%s, gen=%" PRIu64 ") client timeout/fail", name, gen);
            return rc;
        }
        log_info("CTRLm: barrier(name=%s, gen=%" PRIu64 ") passed (client)", name, gen);
        return 0;
    }

    /* coordinator counts itself and waits on state cv until release() */
    bstate_t* s = bstate_get_or_create(name, gen);
    if (!s) return -1;

    pthread_mutex_lock(&s->mu);
    if (!s->released) {
        /* mark self arrived if not already */
        if (!bitmap_test_set(s->bitmap, G.cfg.rank)) {
            s->arrived_cnt++;
            log_debug("CTRLm: ARRIVE (self) name=%s gen=%" PRIu64 " (%d/%d)",
                    s->name, s->gen, s->arrived_cnt, s->expected);
        }
        if (s->arrived_cnt >= s->expected) {
            bstate_release(s);
        }
        if (!s->released) {
            /* wait with timeout */
            struct timespec ts;
            clock_gettime(CLOCK_REALTIME, &ts);
            ts.tv_sec  += timeout_ms / 1000;
            ts.tv_nsec += (long)(timeout_ms % 1000) * 1000000L;
            if (ts.tv_nsec >= 1000000000L) { ts.tv_sec++; ts.tv_nsec -= 1000000000L; }

            int rc = 0;
            while (!s->released && rc == 0) {
                rc = pthread_cond_timedwait(&s->cv, &s->mu, &ts);
            }
            if (!s->released) {
                pthread_mutex_unlock(&s->mu);
                log_warn("CTRLm: barrier(name=%s, gen=%" PRIu64 ") coordinator timeout", name, gen);
                return ETIMEDOUT;
            }
        }
    }
    pthread_mutex_unlock(&s->mu);

    log_info("CTRLm: barrier(name=%s, gen=%" PRIu64 ") passed (coordinator)", name, gen);
    return 0;
}
