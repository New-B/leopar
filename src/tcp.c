/**
 * @file tcp.c
 * @author Wang Bo
 * @date 2025-09-16
 * @brief TCP helpers for LeoPar runtime bootstrap and control plane.
 */

#include "tcp.h"
#include "context.h"   /* for g_ctx and tcp_config_t */
#include "log.h"

#include <sys/socket.h>
#include <sys/types.h>
#include <sys/select.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <pthread.h>
#include <fcntl.h>
#include <errno.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>

/* ---- Internal helpers ---- */

/* Set or clear non-blocking mode */ 
static int set_nonblock(int fd, int nb)
{
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0) return -1;
    if (nb) flags |= O_NONBLOCK; else flags &= ~O_NONBLOCK;
    return fcntl(fd, F_SETFL, flags);
}

/* Create a listening socket on given port */
static int tcp_listen_on(int port, int *out_fd)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;

    int on = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family      = AF_INET;
    addr.sin_port        = htons((uint16_t)port);
    addr.sin_addr.s_addr = htonl(INADDR_ANY);

    if (bind(fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        close(fd);
        return -1;
    }
    if (listen(fd, 128) < 0) {
        close(fd);
        return -1;
    }
    *out_fd = fd;
    return 0;
}

/* Connect to ip:port with a timeout */
static int tcp_connect_timeout(const char *ip, int port, int timeout_ms)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        log_error("socket() failed: %s", strerror(errno));
        return -1;
    }

    set_nonblock(fd, 1);

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family      = AF_INET;
    addr.sin_port        = htons((uint16_t)port);
    if (inet_pton(AF_INET, ip, &addr.sin_addr) != 1) {
        close(fd);
        return -1;
    }

    int rc = connect(fd, (struct sockaddr*)&addr, sizeof(addr));
    if (rc == 0) {
        set_nonblock(fd, 0);
        return fd;
    }
    if (errno != EINPROGRESS) {
        log_error("connect() to %s:%d failed immediately: %s", ip, port, strerror(errno));
        close(fd);
        return -1;
    }

    fd_set wfds;
    FD_ZERO(&wfds);
    FD_SET(fd, &wfds);
    struct timeval tv;
    tv.tv_sec  = timeout_ms / 1000;
    tv.tv_usec = (timeout_ms % 1000) * 1000;

    rc = select(fd + 1, NULL, &wfds, NULL, &tv);
    if (rc == 1 && FD_ISSET(fd, &wfds)) {
        int err = 0; socklen_t len = sizeof(err);
        getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &len);
        if (err == 0) { set_nonblock(fd, 0); return fd; }
        log_error("connect() to %s:%d completed with error: %s", ip, port, strerror(err));
    } else if (rc == 0) {
        log_error("connect() to %s:%d timed out after %d ms", ip, port, timeout_ms);
    } else {
        log_error("select() for connect to %s:%d failed: %s", ip, port, strerror(errno));
    }
    close(fd);
    return -1;
}

/* Blocking send n bytes with a per-iteration poll timeout */
static int tcp_sendn(int fd, const void *buf, size_t n, int timeout_ms)
{
    const char *p = (const char*)buf;
    size_t left = n;
    while (left > 0) {
        fd_set wfds; FD_ZERO(&wfds); FD_SET(fd, &wfds);
        struct timeval tv;
        tv.tv_sec  = timeout_ms / 1000;
        tv.tv_usec = (timeout_ms % 1000) * 1000;
        int rc = select(fd + 1, NULL, &wfds, NULL, &tv);
        if (rc <= 0) return -1;

        ssize_t w = send(fd, p, left, 0);
        if (w <= 0) return -1;
        p += w; left -= (size_t)w;
    }
    return 0;
}

/* Blocking recv n bytes with a per-iteration poll timeout */
static int tcp_recvn(int fd, void *buf, size_t n, int timeout_ms)
{
    char *p = (char*)buf;
    size_t left = n;
    while (left > 0) {
        fd_set rfds; FD_ZERO(&rfds); FD_SET(fd, &rfds);
        struct timeval tv;
        tv.tv_sec  = timeout_ms / 1000;
        tv.tv_usec = (timeout_ms % 1000) * 1000;
        int rc = select(fd + 1, &rfds, NULL, NULL, &tv);
        if (rc <= 0) return -1;

        ssize_t r = recv(fd, p, left, 0);
        if (r <= 0) return -1;
        p += r; left -= (size_t)r;
    }
    return 0;
}

static int tcp_close_fd(int fd)
{
    if (fd >= 0) close(fd);
    return 0;
}

/* Send length-prefixed blob: [u32 len][len bytes] */
static int send_blob(int fd, const void *buf, uint32_t len, int io_to_ms)
{
    if (tcp_sendn(fd, &len, sizeof(len), io_to_ms) != 0) return -1;
    if (len == 0) return 0;
    return tcp_sendn(fd, buf, len, io_to_ms);
}

/* Receive length-prefixed blob into newly malloc'd buffer */
static int recv_blob(int fd, void **out_buf, uint32_t *out_len, int io_to_ms)
{
    uint32_t len = 0;
    if (tcp_recvn(fd, &len, sizeof(len), io_to_ms) != 0) return -1;
    void *b = NULL;
    if (len > 0) {
        b = malloc(len);
        if (!b) return -1;
        if (tcp_recvn(fd, b, len, io_to_ms) != 0) {
            free(b); return -1;
        }
    }
    *out_buf = b;
    *out_len = len;
    return 0;
}

/* Simple authentication: send/recv secret */
static int send_secret(int fd, const char *secret, int io_ms)
{
    uint32_t n = (uint32_t)strlen(secret);
    return send_blob(fd, secret, n, io_ms);
}
static int recv_secret_and_check(int fd, const char *expected, int io_ms)
{
    void *buf = NULL; uint32_t n = 0;
    if (recv_blob(fd, &buf, &n, io_ms) != 0) return -1;
    int ok = 0;
    if (n == strlen(expected) && memcmp(buf, expected, n) == 0) ok = 1;
    free(buf);
    return ok ? 0 : -1;
}



 
//  /* ---- Small helper for concurrent send on rank0 ---- */
//  typedef struct send_task_s {
//     int                 fd;
//     const uint8_t      *buf;
//     size_t              len;
//     int                 timeout_ms;
//     int                 rank;       /* dst rank (for logging) */
//     int                 rc;         /* result per-thread */
// } send_task_t;
 
//  static void *send_table_worker(void *arg)
//  {
//      send_task_t *t = (send_task_t*)arg;
//      /* send whole buffer once */
//      int rc = tcp_sendn(t->fd, t->buf, t->len, t->timeout_ms);
//      if (rc != 0) {
//          log_error("Rank0: send table to rank=%d failed (rc=%d)", t->rank, rc);
//          t->rc = -1;
//      } else {
//          log_info("Rank0: served table to rank=%d (bytes=%zu)", t->rank, t->len);
//          t->rc = 0;
//      }
//      /* close per-peer socket after sending */
//      tcp_close_fd(t->fd);
//      return NULL;
//  }
 
//  /* Drop-in replacement for tcp_allgather_ucx_addrs (Plan A)
//  *
//  * Design:
//  *   - Rank0 listens on base_port, accepts W-1 peers.
//  *   - Each non-zero rank connects once, sends {secret, rank, UCX address}, and
//  *     keeps the same TCP connection open.
//  *   - After rank0 gathers all addresses, it serializes the full table into a
//  *     contiguous buffer: [uint32_t world_size][uint32_t len[W]][blob0..blobW-1]
//  *   - Rank0 concurrently sends this single buffer to every peer via the SAME
//  *     TCP connection, then closes the sockets.
//  *   - Non-zero ranks receive [ws][lens[]][payload] on the same socket, parse
//  *     into out_tbl_{addrs,lens}, then close the socket.
//  *
//  * Pros:
//  *   - No extra ports (firewall-friendly).
//  *   - No listen/accept time coupling across ranks.
//  *   - Single large send per peer -> fewer syscalls, better throughput.
//  */
// int tcp_allgather_ucx_addrs(const struct tcp_config_t *cfg,
//                             int my_rank, int world_size,
//                             const ucp_address_t *my_addr, size_t my_len,
//                             ucp_address_t ***out_tbl_addrs,
//                             size_t **out_tbl_lens)
// {
//     if (!cfg || !out_tbl_addrs || !out_tbl_lens) return -1;

//     /* allocate output arrays (caller frees) */
//     ucp_address_t **tbl_addrs = (ucp_address_t**)calloc((size_t)world_size, sizeof(*tbl_addrs));
//     size_t *tbl_lens          = (size_t*)calloc((size_t)world_size, sizeof(*tbl_lens));
//     if (!tbl_addrs || !tbl_lens) {
//         free(tbl_addrs); free(tbl_lens);
//         log_error("allgather: OOM for output tables");
//         return -1;
//     }

//     int rc = 0;

//     if (my_rank == 0) {
//         /* ----------------- Rank 0: accept W-1 peers on base_port ----------------- */
//         int listen_fd = -1;
//         if (tcp_listen_on(cfg->base_port, &listen_fd) != 0) {
//             log_error("Rank0 listen failed on port=%d", cfg->base_port);
//             free(tbl_addrs); free(tbl_lens);
//             return -1;
//         }
//         log_info("Rank0 listening on port=%d", cfg->base_port);

//         /* save own address */
//         tbl_lens[0]  = my_len;
//         tbl_addrs[0] = (ucp_address_t*)malloc(my_len);
//         if (!tbl_addrs[0]) {
//             log_error("Rank0: OOM for self address");
//             tcp_close_fd(listen_fd);
//             free(tbl_addrs); free(tbl_lens);
//             return -1;
//         }
//         memcpy(tbl_addrs[0], my_addr, my_len);

//         /* per-peer sockets to reuse for broadcasting */
//         int *peer_cfd = (int*)malloc(sizeof(int) * (size_t)world_size);
//         int  *got     = (int*)calloc((size_t)world_size, sizeof(int));
//         if (!peer_cfd || !got) {
//             log_error("Rank0: OOM for peer socket bookkeeping");
//             tcp_close_fd(listen_fd);
//             free(tbl_addrs[0]);
//             free(tbl_addrs); free(tbl_lens);
//             free(peer_cfd); free(got);
//             return -1;
//         }
//         for (int i = 0; i < world_size; ++i) peer_cfd[i] = -1;
//         got[0] = 1;

//         int gathered = 1;
//         while (gathered < world_size) {
//             struct sockaddr_in cli; socklen_t clilen = sizeof(cli);
//             int cfd = accept(listen_fd, (struct sockaddr*)&cli, &clilen);
//             if (cfd < 0) {
//                 log_error("Rank0: accept() failed: errno=%d", errno);
//                 rc = -1; break;
//             }

//             /* auth */
//             if (recv_secret_and_check(cfd, cfg->secret, cfg->io_timeout_ms) != 0) {
//                 log_error("Rank0: auth failed");
//                 tcp_close_fd(cfd);
//                 rc = -1; break;
//             }

//             /* recv peer rank */
//             int peer_rank = -1;
//             if (tcp_recvn(cfd, &peer_rank, sizeof(peer_rank), cfg->io_timeout_ms) != 0 ||
//                 peer_rank <= 0 || peer_rank >= world_size) {
//                 log_error("Rank0: invalid peer rank (got=%d)", peer_rank);
//                 tcp_close_fd(cfd);
//                 rc = -1; break;
//             }

//             /* recv peer UCX address blob */
//             void *b = NULL; uint32_t blen = 0;
//             if (recv_blob(cfd, &b, &blen, cfg->io_timeout_ms) != 0) {
//                 log_error("Rank0: recv_blob failed for rank=%d", peer_rank);
//                 tcp_close_fd(cfd);
//                 rc = -1; break;
//             }

//             if (got[peer_rank]) {
//                 log_warn("Rank0: duplicate UCX addr from rank=%d, overwrite", peer_rank);
//                 free(tbl_addrs[peer_rank]);
//                 if (peer_cfd[peer_rank] >= 0) tcp_close_fd(peer_cfd[peer_rank]);
//             } else {
//                 gathered++;
//             }
//             tbl_lens[peer_rank]  = (size_t)blen;
//             tbl_addrs[peer_rank] = (ucp_address_t*)b;
//             peer_cfd[peer_rank]  = cfd;
//             got[peer_rank]       = 1;

//             log_info("Rank0: gathered UCX addr from rank=%d (len=%u)", peer_rank, blen);
//         }

//         /* no longer need the listening socket */
//         tcp_close_fd(listen_fd);

//         /* If gathering failed, close any accepted CFDS and clean up */
//         if (rc != 0) {
//             for (int r = 1; r < world_size; ++r)
//                 if (peer_cfd[r] >= 0) tcp_close_fd(peer_cfd[r]);
//             free(peer_cfd); free(got);
//             for (int i = 0; i < world_size; ++i) free(tbl_addrs[i]);
//             free(tbl_addrs); free(tbl_lens);
//             return -1;
//         }

//         /* ----------------- Serialize the full table once ----------------- */
//         size_t sum_bytes = 0;
//         for (int i = 0; i < world_size; ++i) sum_bytes += tbl_lens[i];

//         size_t ser_len = sizeof(uint32_t)                          /* ws */
//                     + (size_t)world_size * sizeof(uint32_t)     /* lens[] */
//                     + sum_bytes;                                /* blobs   */

//         uint8_t *ser = (uint8_t*)malloc(ser_len);
//         if (!ser) {
//             log_error("Rank0: OOM for serialization buffer (%zu bytes)", ser_len);
//             for (int r = 1; r < world_size; ++r)
//                 if (peer_cfd[r] >= 0) tcp_close_fd(peer_cfd[r]);
//             free(peer_cfd); free(got);
//             for (int i = 0; i < world_size; ++i) free(tbl_addrs[i]);
//             free(tbl_addrs); free(tbl_lens);
//             return -1;
//         }

//         uint8_t *p = ser;
//         uint32_t ws_u32 = (uint32_t)world_size;
//         memcpy(p, &ws_u32, sizeof(ws_u32)); p += sizeof(ws_u32);

//         for (int i = 0; i < world_size; ++i) {
//             uint32_t L = (uint32_t)tbl_lens[i];
//             memcpy(p, &L, sizeof(L)); p += sizeof(L);
//         }
//         for (int i = 0; i < world_size; ++i) {
//             if (tbl_lens[i] > 0) {
//                 memcpy(p, tbl_addrs[i], tbl_lens[i]);
//                 p += tbl_lens[i];
//             }
//         }

//         /* ----------------- Concurrently send to all peers (r=1..W-1) ----------------- */
//         int peer_cnt = world_size - 1;
//         if (peer_cnt > 0) {
//             /* create up to K worker threads; simple 1:1 mapping is fine for W<=64 */
//             pthread_t *ths = (pthread_t*)malloc(sizeof(pthread_t) * (size_t)peer_cnt);
//             send_task_t *tasks = (send_task_t*)malloc(sizeof(send_task_t) * (size_t)peer_cnt);
//             if (!ths || !tasks) {
//                 log_error("Rank0: OOM for send workers");
//                 rc = -1;
//                 /* fallthrough to cleanup below */
//             } else {
//                 int idx = 0;
//                 for (int r = 1; r < world_size; ++r) {
//                     tasks[idx].fd         = peer_cfd[r];
//                     tasks[idx].buf        = ser;
//                     tasks[idx].len        = ser_len;
//                     tasks[idx].timeout_ms = cfg->io_timeout_ms;
//                     tasks[idx].rank       = r;
//                     tasks[idx].rc         = -1;

//                     int prc = pthread_create(&ths[idx], NULL, send_table_worker, &tasks[idx]);
//                     if (prc != 0) {
//                         log_error("Rank0: pthread_create failed for rank=%d (rc=%d)", r, prc);
//                         /* fallback: synchronous send here */
//                         int sync_rc = tcp_sendn(peer_cfd[r], ser, ser_len, cfg->io_timeout_ms);
//                         if (sync_rc != 0) {
//                             log_error("Rank0: sync send failed to rank=%d (rc=%d)", r, sync_rc);
//                             rc = -1;
//                         } else {
//                             log_info("Rank0: sync served table to rank=%d", r);
//                         }
//                         tcp_close_fd(peer_cfd[r]);
//                     }
//                     idx++;
//                 }

//                 /* join threads and collect their rc */
//                 for (int i = 0; i < peer_cnt; ++i) {
//                     if (ths[i]) pthread_join(ths[i], NULL);
//                     if (tasks[i].rc != 0) rc = -1;
//                 }
//             }

//             free(ths);
//             free(tasks);
//         }

//         /* rank0 cleanup: own serialization buffer is local */
//         free(ser);
//         for (int r = 1; r < world_size; ++r) peer_cfd[r] = -1;
//         free(peer_cfd); free(got);

//         if (rc != 0) {
//             for (int i = 0; i < world_size; ++i) free(tbl_addrs[i]);
//             free(tbl_addrs); free(tbl_lens);
//             return -1;
//         }

//     } else {
//         /* ----------------- Non-zero ranks: single connection to rank0 ----------------- */
//         const char *ip0 = cfg->ip_of_rank[0];
//         int cfd = -1;

//         for (int attempt = 0; attempt < cfg->retry_max && cfd < 0; ++attempt) {
//             cfd = tcp_connect_timeout(ip0, cfg->base_port, cfg->connect_timeout_ms);
//         }
//         if (cfd < 0) {
//             log_error("Rank%d: connect to rank0 %s:%d failed after %d attempts",
//                     my_rank, ip0, cfg->base_port, cfg->retry_max);
//             free(tbl_addrs); free(tbl_lens);
//             return -1;
//         }

//         /* auth + send my rank + send my UCX address (keep connection open) */
//         if (send_secret(cfd, cfg->secret, cfg->io_timeout_ms) != 0) {
//             log_error("Rank%d: send_secret failed", my_rank);
//             tcp_close_fd(cfd); free(tbl_addrs); free(tbl_lens); return -1;
//         }
//         if (tcp_sendn(cfd, &my_rank, sizeof(my_rank), cfg->io_timeout_ms) != 0) {
//             log_error("Rank%d: send my_rank failed", my_rank);
//             tcp_close_fd(cfd); free(tbl_addrs); free(tbl_lens); return -1;
//         }
//         if (send_blob(cfd, my_addr, (uint32_t)my_len, cfg->io_timeout_ms) != 0) {
//             log_error("Rank%d: send my UCX addr failed", my_rank);
//             tcp_close_fd(cfd); free(tbl_addrs); free(tbl_lens); return -1;
//         }

//         /* receive serialized table on the SAME connection */
//         uint32_t ws = 0;
//         if (tcp_recvn(cfd, &ws, sizeof(ws), cfg->io_timeout_ms) != 0 || ws != (uint32_t)world_size) {
//             log_error("Rank%d: recv world_size failed or mismatch (got=%u, expect=%d)",
//                     my_rank, ws, world_size);
//             tcp_close_fd(cfd); free(tbl_addrs); free(tbl_lens); return -1;
//         }

//         /* receive lengths array */
//         uint32_t *lens32 = (uint32_t*)malloc(sizeof(uint32_t) * (size_t)world_size);
//         if (!lens32) {
//             log_error("Rank%d: OOM for lens32", my_rank);
//             tcp_close_fd(cfd); free(tbl_addrs); free(tbl_lens); return -1;
//         }
//         if (tcp_recvn(cfd, lens32, sizeof(uint32_t) * (size_t)world_size, cfg->io_timeout_ms) != 0) {
//             log_error("Rank%d: recv lens32 failed", my_rank);
//             free(lens32); tcp_close_fd(cfd); free(tbl_addrs); free(tbl_lens); return -1;
//         }

//         size_t sum_bytes = 0;
//         for (int i = 0; i < world_size; ++i) { tbl_lens[i] = (size_t)lens32[i]; sum_bytes += tbl_lens[i]; }
//         free(lens32);

//         /* receive all payload blobs into a temporary buffer, then split to per-rank malloc */
//         uint8_t *payload = NULL;
//         if (sum_bytes > 0) {
//             payload = (uint8_t*)malloc(sum_bytes);
//             if (!payload) {
//                 log_error("Rank%d: OOM for payload buffer (%zu bytes)", my_rank, sum_bytes);
//                 tcp_close_fd(cfd); free(tbl_addrs); free(tbl_lens); return -1;
//             }
//             if (tcp_recvn(cfd, payload, sum_bytes, cfg->io_timeout_ms) != 0) {
//                 log_error("Rank%d: recv payload bytes failed (sum=%zu)", my_rank, sum_bytes);
//                 free(payload); tcp_close_fd(cfd); free(tbl_addrs); free(tbl_lens); return -1;
//             }
//         }

//         /* split payload to per-rank malloc buffers (caller will free per entry) */
//         uint8_t *q = payload;
//         for (int i = 0; i < world_size; ++i) {
//             if (tbl_lens[i] > 0) {
//                 tbl_addrs[i] = (ucp_address_t*)malloc(tbl_lens[i]);
//                 if (!tbl_addrs[i]) {
//                     log_error("Rank%d: OOM for tbl_addrs[%d] (%zu bytes)", my_rank, i, tbl_lens[i]);
//                     /* cleanup partial allocations */
//                     for (int k = 0; k < i; ++k) free(tbl_addrs[k]);
//                     free(tbl_addrs); free(tbl_lens); free(payload); tcp_close_fd(cfd);
//                     return -1;
//                 }
//                 memcpy(tbl_addrs[i], q, tbl_lens[i]);
//                 q += tbl_lens[i];
//             } else {
//                 tbl_addrs[i] = NULL;
//             }
//         }
//         free(payload);
//         tcp_close_fd(cfd);
//     }

//     if (rc != 0) {
//         for (int i = 0; i < world_size; ++i) free(tbl_addrs[i]);
//         free(tbl_addrs); free(tbl_lens);
//         return -1;
//     }

//     *out_tbl_addrs = tbl_addrs;
//     *out_tbl_lens  = tbl_lens;
//     return 0;
// }


int tcp_allgather_ucx_addrs(const struct tcp_config_t *cfg,
                            int my_rank, int world_size,
                            const ucp_address_t *my_addr, size_t my_len,
                            ucp_address_t ***out_tbl_addrs,
                            size_t **out_tbl_lens)
{
    if (!cfg || !out_tbl_addrs || !out_tbl_lens) return -1;

    ucp_address_t **tbl_addrs = (ucp_address_t**)calloc(world_size, sizeof(*tbl_addrs));
    size_t *tbl_lens          = (size_t*)calloc(world_size, sizeof(*tbl_lens));
    if (!tbl_addrs || !tbl_lens) { free(tbl_addrs); free(tbl_lens); return -1; }

    int rc = 0;

    if (my_rank == 0) {
        /* rank0: listen on base_port, accept world_size-1 peers, keep their cfds */
        int listen_fd = -1;
        if (tcp_listen_on(cfg->base_port, &listen_fd) != 0) {
            log_error("Rank0 listen failed on port=%d", cfg->base_port);
            free(tbl_addrs); free(tbl_lens);
            return -1;
        }
        log_info("Rank0 listening on port=%d", cfg->base_port);


        /* save self address */
        tbl_lens[0]  = my_len;
        tbl_addrs[0] = (ucp_address_t*)malloc(my_len);
        if (!tbl_addrs[0]) { tcp_close_fd(listen_fd); free(tbl_addrs); free(tbl_lens); return -1; }
        memcpy(tbl_addrs[0], my_addr, my_len);

        /* track accepted peers */
        int *peer_cfd = (int*)malloc(sizeof(int) * world_size);
        int  *got     = (int*)calloc(world_size, sizeof(int));
        if (!peer_cfd || !got) { tcp_close_fd(listen_fd); free(tbl_addrs[0]); free(tbl_addrs); free(tbl_lens); free(peer_cfd); free(got); return -1; }
        for (int i = 0; i < world_size; ++i) peer_cfd[i] = -1;
        got[0] = 1;

        // global barrier to avoid connection race on rank0 listen port
        if (ctrlm_barrier("ucx_tcp_listen_ready", /*gen*/1, /*timeout_ms*/0) != 0) {
            log_error("Barrier 'ucx_tcp_listen_ready' failed");
            return -1;
        }


        int gathered = 1;
        while (gathered < world_size) {
            struct sockaddr_in cli; socklen_t clilen = sizeof(cli);
            int cfd = accept(listen_fd, (struct sockaddr*)&cli, &clilen);
            if (cfd < 0) { rc = -1; log_error("Rank0 accept failed"); break; }

            /* auth */
            if (recv_secret_and_check(cfd, cfg->secret, cfg->io_timeout_ms) != 0) {
                log_error("Rank0 auth failed"); tcp_close_fd(cfd); rc = -1; break;
            }

            /* recv rank */
            int peer_rank = -1;
            if (tcp_recvn(cfd, &peer_rank, sizeof(peer_rank), cfg->io_timeout_ms) != 0 ||
                peer_rank <= 0 || peer_rank >= world_size) {
                log_error("Rank0 invalid peer rank");
                tcp_close_fd(cfd); rc = -1; break;
            }

            /* recv UCX addr blob */
            void *b = NULL; uint32_t blen = 0;
            if (recv_blob(cfd, &b, &blen, cfg->io_timeout_ms) != 0) {
                log_error("Rank0 recv_blob failed for rank=%d", peer_rank);
                tcp_close_fd(cfd); rc = -1; break;
            }

            if (got[peer_rank]) {
                log_warn("Rank0 duplicate addr from rank=%d, overwrite", peer_rank);
                free(tbl_addrs[peer_rank]);
                if (peer_cfd[peer_rank] >= 0) tcp_close_fd(peer_cfd[peer_rank]);
            } else {
                gathered++;
            }
            tbl_lens[peer_rank]  = (size_t)blen;
            tbl_addrs[peer_rank] = (ucp_address_t*)b;
            peer_cfd[peer_rank]  = cfd;
            got[peer_rank]       = 1;

            log_info("Rank0 gathered UCX addr from rank=%d (len=%u)", peer_rank, blen);
        }

        /* 所有地址收齐后：在“同一 TCP 连接”上直接下发表 */
        if (rc == 0) {
            for (int r = 1; r < world_size; ++r) {
                int cfd = peer_cfd[r];
                if (cfd < 0) { log_error("Rank0 missing cfd for rank=%d", r); rc = -1; continue; }

                uint32_t ws = (uint32_t)world_size;
                if (tcp_sendn(cfd, &ws, sizeof(ws), cfg->io_timeout_ms) != 0) {
                    log_error("Rank0 send ws to rank=%d failed", r); tcp_close_fd(cfd); peer_cfd[r] = -1; rc = -1; continue;
                }
                log_debug("Rank0 sending world_size(%u) to rank=%d", ws, r);
                for (int i = 0; i < world_size; ++i) {
                    if (send_blob(cfd, tbl_addrs[i], (uint32_t)tbl_lens[i], cfg->io_timeout_ms) != 0) {
                        log_error("Rank0 send blob[%d] to rank=%d failed", i, r);
                        tcp_close_fd(cfd); peer_cfd[r] = -1; rc = -1; break;
                    }
                }
                if (peer_cfd[r] >= 0) {
                    tcp_close_fd(peer_cfd[r]); peer_cfd[r] = -1;
                }
                if (rc == 0) log_info("Rank0 served table to rank=%d (reuse connection)", r);
            }
        }

        for (int r = 1; r < world_size; ++r)
            if (peer_cfd[r] >= 0) tcp_close_fd(peer_cfd[r]);

        tcp_close_fd(listen_fd);
        free(peer_cfd); free(got);
    } else {
        // global barrier to avoid connection race in non-zero ranks
        if (ctrlm_barrier("ucx_tcp_listen_ready", /*gen*/1, /*timeout_ms*/0) != 0) {
            log_error("Barrier 'ucx_tcp_listen_ready' failed");
            return -1;
        }

        /* 非0 rank：连 base_port，提交地址后在同一连接上等待表 */
        const char *ip0 = cfg->ip_of_rank[0];
        int cfd = -1;
        for (int attempt = 0; attempt < cfg->retry_max && cfd < 0; ++attempt)
            cfd = tcp_connect_timeout(ip0, cfg->base_port, cfg->connect_timeout_ms);
        if (cfd < 0) { log_error("Rank%d connect rank0 %s:%d failed", my_rank, ip0, cfg->base_port); free(tbl_addrs); free(tbl_lens); return -1; }

        if (send_secret(cfd, cfg->secret, cfg->io_timeout_ms) != 0) { tcp_close_fd(cfd); free(tbl_addrs); free(tbl_lens); return -1; }
        if (tcp_sendn(cfd, &my_rank, sizeof(my_rank), cfg->io_timeout_ms) != 0) { tcp_close_fd(cfd); free(tbl_addrs); free(tbl_lens); return -1; }
        if (send_blob(cfd, my_addr, (uint32_t)my_len, cfg->io_timeout_ms) != 0) { tcp_close_fd(cfd); free(tbl_addrs); free(tbl_lens); return -1; }
        log_debug("Rank%d sent UCX addr to rank0", my_rank);

        /* 在同一连接上等待 rank0 下发表 */
        uint32_t ws = 0;
        if (tcp_recvn(cfd, &ws, sizeof(ws), cfg->io_timeout_ms) != 0 || ws != (uint32_t)world_size) {
            log_error("Rank%d recv ws failed or mismatch (got=%u expect=%d)", my_rank, ws, world_size);
            tcp_close_fd(cfd); free(tbl_addrs); free(tbl_lens); return -1;
        }
        log_debug("Rank%d recv world_size=%u", my_rank, ws);
        for (int i = 0; i < world_size; ++i) {
            void *b = NULL; uint32_t blen = 0;
            if (recv_blob(cfd, &b, &blen, cfg->io_timeout_ms) != 0) {
                log_error("Rank%d recv blob[%d] failed", my_rank, i);
                tcp_close_fd(cfd); free(tbl_addrs); free(tbl_lens); return -1;
            }
            tbl_lens[i]  = (size_t)blen;
            tbl_addrs[i] = (ucp_address_t*)b;
        }
        tcp_close_fd(cfd);
    }

    if (rc != 0) {
        for (int i = 0; i < world_size; ++i) free(tbl_addrs[i]);
        free(tbl_addrs); free(tbl_lens);
        return -1;
    }

    *out_tbl_addrs = tbl_addrs;
    *out_tbl_lens  = tbl_lens;
    return 0;
}






// /* Coordinator-based allgather (rank 0 server) */
// int tcp_allgather_ucx_addrs(const struct tcp_config_t *cfg,
//                             int my_rank, int world_size,
//                             const ucp_address_t *my_addr, size_t my_len,
//                             ucp_address_t ***out_tbl_addrs,
//                             size_t **out_tbl_lens)
// {
//     if (!cfg || !out_tbl_addrs || !out_tbl_lens) return -1;

//     /* Allocate outputs */
//     ucp_address_t **tbl_addrs = (ucp_address_t**)calloc(world_size, sizeof(*tbl_addrs));
//     size_t *tbl_lens          = (size_t*)calloc(world_size, sizeof(*tbl_lens));
//     if (!tbl_addrs || !tbl_lens) {
//         free(tbl_addrs); free(tbl_lens);
//         return -1;
//     }

//     int rc = 0;

//     if (my_rank == 0) {
//         /* Rank 0 listens and accepts world_size-1 connections */
//         int listen_fd = -1;
//         int listen_port = cfg->base_port;
//         if (tcp_listen_on(listen_port, &listen_fd) != 0) {
//             log_error("Rank0 listen failed on port=%d", listen_port);
//             free(tbl_addrs); free(tbl_lens);
//             return -1;
//         }
//         log_info("Rank0 listening on port=%d", listen_port);

//         /* Save own address */
//         tbl_lens[0]  = my_len;
//         tbl_addrs[0] = (ucp_address_t*)malloc(my_len);
//         if (!tbl_addrs[0]) { tcp_close_fd(listen_fd); free(tbl_addrs); free(tbl_lens); return -1; }
//         memcpy(tbl_addrs[0], my_addr, my_len);

//         /* Accept peers and gather their {rank, secret, addr} */
//         for (int count = 1; count < world_size; ++count) {
//             struct sockaddr_in cli; socklen_t clilen = sizeof(cli);
//             int cfd = accept(listen_fd, (struct sockaddr*)&cli, &clilen);
//             if (cfd < 0) { rc = -1; break; }

//             /* auth */
//             if (recv_secret_and_check(cfd, cfg->secret, cfg->io_timeout_ms) != 0) { tcp_close_fd(cfd); rc = -1; break; }

//             /* recv their rank */
//             int peer_rank = -1;
//             if (tcp_recvn(cfd, &peer_rank, sizeof(peer_rank), cfg->io_timeout_ms) != 0) { tcp_close_fd(cfd); rc = -1; break; }
//             if (peer_rank <= 0 || peer_rank >= world_size) { tcp_close_fd(cfd); rc = -1; break; }

//             /* recv their addr blob */
//             void *b = NULL; uint32_t blen = 0;
//             if (recv_blob(cfd, &b, &blen, cfg->io_timeout_ms) != 0) { tcp_close_fd(cfd); rc = -1; break; }

//             tbl_lens[peer_rank]  = (size_t)blen;
//             tbl_addrs[peer_rank] = (ucp_address_t*)b;

//             tcp_close_fd(cfd);
//             log_info("Rank0 gathered UCX addr from rank=%d (len=%u)", peer_rank, blen);
//         }

//         /* Now broadcast the full table to all ranks: each rank connects to rank0 and pulls */
//         for (int r = 1; rc == 0 && r < world_size; ++r) {
//             int port = cfg->base_port + 1 + r; /* use a per-rank pull port to avoid accept loop complexities */
//             int pfd  = -1;
//             if (tcp_listen_on(port, &pfd) != 0) { rc = -1; break; }
//             log_info("Rank0 ready for table pull on port=%d (rank=%d)", port, r);

//             struct sockaddr_in cli; socklen_t clilen = sizeof(cli);
//             int cfd = accept(pfd, (struct sockaddr*)&cli, &clilen);
//             if (cfd < 0) { tcp_close_fd(pfd); rc = -1; break; }
//             log_debug("Rank0 accepted table pull connection from rank=%d", r);

//             /* auth */
//             if (recv_secret_and_check(cfd, cfg->secret, cfg->io_timeout_ms) != 0) { tcp_close_fd(cfd); tcp_close_fd(pfd); rc = -1; break; }

//             /* send world_size, then each blob */
//             uint32_t ws = (uint32_t)world_size;
//             if (tcp_sendn(cfd, &ws, sizeof(ws), cfg->io_timeout_ms) != 0) { tcp_close_fd(cfd); tcp_close_fd(pfd); rc = -1; break; }
//             for (int i = 0; i < world_size; ++i) {
//                 if (send_blob(cfd, tbl_addrs[i], (uint32_t)tbl_lens[i], cfg->io_timeout_ms) != 0) {
//                     tcp_close_fd(cfd); tcp_close_fd(pfd); rc = -1; break;
//                 }
//             }
//             tcp_close_fd(cfd);
//             tcp_close_fd(pfd);
//             log_info("Rank0 served table to rank=%d", r);
//         }

//         tcp_close_fd(listen_fd);
//     } else {
//         /* Non-zero ranks: connect to rank0 to send our addr */
//         int cfd = -1;
//         const char *ip0 = g_ctx.tcp_cfg.ip_of_rank[0];
//         int port0 = cfg->base_port;

//         for (int attempt = 0; attempt < cfg->retry_max && cfd < 0; ++attempt) {
//             cfd = tcp_connect_timeout(ip0, port0, cfg->connect_timeout_ms);
//         }
//         if (cfd < 0) {
//             log_error("Rank%d failed to connect to rank0:%s:%d", my_rank, ip0, port0);
//             free(tbl_addrs); free(tbl_lens);
//             return -1;
//         }

//         /* auth */
//         if (send_secret(cfd, cfg->secret, cfg->io_timeout_ms) != 0) { tcp_close_fd(cfd); free(tbl_addrs); free(tbl_lens); return -1; }

//         /* send my rank */
//         if (tcp_sendn(cfd, &my_rank, sizeof(my_rank), cfg->io_timeout_ms) != 0) { tcp_close_fd(cfd); free(tbl_addrs); free(tbl_lens); return -1; }

//         /* send my UCX address blob */
//         if (send_blob(cfd, my_addr, (uint32_t)my_len, cfg->io_timeout_ms) != 0) { tcp_close_fd(cfd); free(tbl_addrs); free(tbl_lens); return -1; }
//         tcp_close_fd(cfd);
//         log_debug("Rank%d sent UCX addr to rank0", my_rank);

//         /* pull full table from rank0 */
//         int pull_fd = -1;
//         int pull_port = cfg->base_port + 1 + my_rank;

//         for (int attempt = 0; attempt < cfg->retry_max && pull_fd < 0; ++attempt) {
//             pull_fd = tcp_connect_timeout(ip0, pull_port, cfg->connect_timeout_ms);
//         }
//         if (pull_fd < 0) {
//             log_error("Rank%d failed to pull table from rank0:%s:%d", my_rank, ip0, pull_port);
//             free(tbl_addrs); free(tbl_lens);
//             return -1;
//         }

//         /* auth */
//         if (send_secret(pull_fd, cfg->secret, cfg->io_timeout_ms) != 0) { tcp_close_fd(pull_fd); free(tbl_addrs); free(tbl_lens); return -1; }

//         uint32_t ws = 0;
//         if (tcp_recvn(pull_fd, &ws, sizeof(ws), cfg->io_timeout_ms) != 0 || ws != (uint32_t)world_size) {
//             tcp_close_fd(pull_fd); free(tbl_addrs); free(tbl_lens); return -1;
//         }

//         for (int i = 0; i < world_size; ++i) {
//             void *b = NULL; uint32_t blen = 0;
//             if (recv_blob(pull_fd, &b, &blen, cfg->io_timeout_ms) != 0) {
//                 tcp_close_fd(pull_fd); free(tbl_addrs); free(tbl_lens); return -1;
//             }
//             tbl_lens[i]  = (size_t)blen;
//             tbl_addrs[i] = (ucp_address_t*)b;
//         }
//         tcp_close_fd(pull_fd);
//     }

//     if (rc != 0) {
//         for (int i = 0; i < world_size; ++i) free(tbl_addrs[i]);
//         free(tbl_addrs); free(tbl_lens);
//         return -1;
//     }

//     *out_tbl_addrs = tbl_addrs;
//     *out_tbl_lens  = tbl_lens;
//     return 0;
// }
