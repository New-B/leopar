/**
 * @file barrier.c
 * @author Wang Bo
 * @date 2025-09-20
 * @brief Centralized barriers (TCP pre-UCX, UCX post-EP) to synchronize phases.
 */

#define _POSIX_C_SOURCE 200809L
#include "barrier.h"
#include "context.h"
#include "ucx.h"
#include "proto.h"
#include "dispatcher.h"
#include "log.h"

#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <netinet/in.h>
#include <time.h>
#include <unistd.h>

/* ---------- proto additions ----------
* Add to proto.h once globally:
*   OP_BARRIER = 40
* And define its message layout here.
*/
#ifndef OP_BARRIER
#define OP_BARRIER 40
#endif

enum { BARR_ARRIVE = 1, BARR_RELEASE = 2 };

#pragma pack(push, 1)
typedef struct {
    uint32_t opcode;   /* OP_BARRIER */
    uint32_t stage;    /* user stage id */
    uint32_t type;     /* BARR_ARRIVE or BARR_RELEASE */
} msg_barrier_t;
#pragma pack(pop)

/* ---- helpers ---- */
static void add_ms(struct timespec *ts, int ms) {
    clock_gettime(CLOCK_REALTIME, ts);
    if (ms <= 0) return;
    ts->tv_sec  += ms / 1000;
    ts->tv_nsec += (ms % 1000) * 1000000L;
    if (ts->tv_nsec >= 1000000000L) { ts->tv_sec++; ts->tv_nsec -= 1000000000L; }
}

// /* ===========================================
// * TCP barrier (rank0 coordinator)
// * =========================================== */
// int tcp_barrier(const tcp_config_t *cfg, int my_rank, int world_size,
//                 int stage_id, int timeout_ms)
// {
//     /* Use two well-known ports derived from base_port for this stage. */
//     int port_arrive  = cfg->base_port + 100 + stage_id*2;
//     int port_release = cfg->base_port + 101 + stage_id*2;

//     if (my_rank == 0) {
//         /* rank0: accept ARRIVE from all others */
//         int lfd = -1;
//         if (tcp_listen_on(port_arrive, &lfd) != 0) {
//             log_error("TCP barrier stage %d: listen arrive failed on %d", stage_id, port_arrive);
//             return -1;
//         }
//         log_debug("TCP barrier stage %d: listening ARRIVE on %d", stage_id, port_arrive);

//         int arrived = 0;
//         while (arrived < world_size - 1) {
//             struct sockaddr_in cli; socklen_t cl = sizeof(cli);
//             int cfd = accept(lfd, (struct sockaddr*)&cli, &cl);
//             if (cfd < 0) { tcp_close_fd(lfd); return -1; }
//             /* Optionally: receive small token (not necessary) */
//             tcp_close_fd(cfd);
//             arrived++;
//         }
//         tcp_close_fd(lfd);

//         /* Now serve RELEASE on a second port; each node connects to pull release. */
//         int rlfd = -1;
//         if (tcp_listen_on(port_release, &rlfd) != 0) {
//             log_error("TCP barrier stage %d: listen release failed on %d", stage_id, port_release);
//             return -1;
//         }
//         int served = 0;
//         while (served < world_size - 1) {
//             struct sockaddr_in cli; socklen_t cl = sizeof(cli);
//             int cfd = accept(rlfd, (struct sockaddr*)&cli, &cl);
//             if (cfd < 0) { tcp_close_fd(rlfd); return -1; }
//             /* Send a 4-byte stage id as release token. */
//             int32_t token = stage_id;
//             if (tcp_sendn(cfd, &token, sizeof(token), cfg->io_timeout_ms) != 0) {
//                 tcp_close_fd(cfd); tcp_close_fd(rlfd); return -1;
//             }
//             tcp_close_fd(cfd);
//             served++;
//         }
//         tcp_close_fd(rlfd);
//         log_info("TCP barrier stage %d: released all", stage_id);
//         return 0;

//     } else {
//         /* non-zero rank: connect for ARRIVE */
//         int cfd = -1;
//         const char *ip0 = g_ctx.tcp_cfg.ip_of_rank[0];
//         int attempts = cfg->retry_max > 0 ? cfg->retry_max : 20;
//         for (int a=0; a<attempts && cfd<0; ++a) {
//             cfd = tcp_connect_timeout(ip0, port_arrive, cfg->connect_timeout_ms);
//             if (cfd < 0) usleep(100*1000);
//         }
//         if (cfd < 0) {
//             log_error("TCP barrier stage %d: connect ARRIVE to %s:%d failed",
//                     stage_id, ip0, port_arrive);
//             return -1;
//         }
//         /* Optionally send small token; here we just close immediately to signal ARRIVE */
//         tcp_close_fd(cfd);

//         /* Then wait RELEASE on the second port */
//         int rfd = -1;
//         for (int a=0; a<attempts && rfd<0; ++a) {
//             rfd = tcp_connect_timeout(ip0, port_release, cfg->connect_timeout_ms);
//             if (rfd < 0) usleep(100*1000);
//         }
//         if (rfd < 0) {
//             log_error("TCP barrier stage %d: connect RELEASE to %s:%d failed",
//                     stage_id, ip0, port_release);
//             return -1;
//         }
//         int32_t token = -1;
//         if (tcp_recvn(rfd, &token, sizeof(token), cfg->io_timeout_ms) != 0) {
//             tcp_close_fd(rfd);
//             log_error("TCP barrier stage %d: recv release token failed", stage_id);
//             return -1;
//         }
//         tcp_close_fd(rfd);
//         if (token != stage_id) {
//             log_warn("TCP barrier stage %d: release token mismatch %d", stage_id, token);
//         }
//         return 0;
//     }
// }

/* ===========================================
* UCX barrier (rank0 coordinator)
* =========================================== */
int ucx_barrier(int stage_id, int timeout_ms)
{
    msg_barrier_t msg;
    msg.opcode = OP_BARRIER;
    msg.stage  = (uint32_t)stage_id;

    if (g_ctx.rank != 0) {
        /* Send ARRIVE to rank0 */
        msg.type = BARR_ARRIVE;
        if (ucx_send_bytes(0, &msg, sizeof(msg), OP_BARRIER) != 0) {
            log_error("UCX barrier stage %d: send ARRIVE failed", stage_id);
            return -1;
        }

        /* Wait for RELEASE from rank0 (with timeout) */
        struct timespec dl; add_ms(&dl, timeout_ms>0?timeout_ms:10000);
        while (1) {
            size_t len=0; ucp_tag_t tag=0; ucp_tag_recv_info_t info;
            void *buf = ucx_recv_any_alloc(&len, &tag, &info);
            if (!buf) { ucp_worker_progress(g_ctx.ucx_ctx.ucp_worker); usleep(1000); continue; }

            uint32_t opcode = (uint32_t)(tag >> 32);
            uint32_t src    = (uint32_t)(tag & 0xffffffffu);

            if (opcode == OP_BARRIER && src == 0 && len >= sizeof(msg_barrier_t)) {
                msg_barrier_t *m = (msg_barrier_t*)buf;
                int ok = (m->opcode==OP_BARRIER && m->type==BARR_RELEASE && m->stage==(uint32_t)stage_id);
                free(buf);
                if (ok) return 0; /* released */
            } else {
                /* Dispatch other control messages so runtime keeps making progress */
                dispatch_msg(buf, len, tag);
                free(buf);
            }

            struct timespec now; clock_gettime(CLOCK_REALTIME, &now);
            if (timeout_ms > 0 &&
                (now.tv_sec > dl.tv_sec || (now.tv_sec==dl.tv_sec && now.tv_nsec >= dl.tv_nsec))) {
                log_error("UCX barrier stage %d: timeout waiting release", stage_id);
                return -1;
            }
        }

    } else {
        /* rank0: collect ARRIVE from all others */
        int arrived = 0;
        struct timespec dl; add_ms(&dl, timeout_ms>0?timeout_ms:10000);

        while (arrived < g_ctx.world_size - 1) {
            size_t len=0; ucp_tag_t tag=0; ucp_tag_recv_info_t info;
            void *buf = ucx_recv_any_alloc(&len, &tag, &info);
            if (!buf) { ucp_worker_progress(g_ctx.ucx_ctx.ucp_worker); usleep(1000); continue; }

            uint32_t opcode = (uint32_t)(tag >> 32);
            uint32_t src    = (uint32_t)(tag & 0xffffffffu);

            if (opcode == OP_BARRIER && len >= sizeof(msg_barrier_t)) {
                msg_barrier_t *m = (msg_barrier_t*)buf;
                if (m->type == BARR_ARRIVE && m->stage == (uint32_t)stage_id) {
                    arrived++;
                } else {
                    log_warn("UCX barrier: unexpected type=%u stage=%u from %u",
                            m->type, m->stage, src);
                }
                free(buf);
            } else {
                dispatch_msg(buf, len, tag);
                free(buf);
            }

            struct timespec now; clock_gettime(CLOCK_REALTIME, &now);
            if (timeout_ms > 0 &&
                (now.tv_sec > dl.tv_sec || (now.tv_sec==dl.tv_sec && now.tv_nsec >= dl.tv_nsec))) {
                log_error("UCX barrier stage %d: timeout collecting arrives (%d/%d)",
                        stage_id, arrived, g_ctx.world_size-1);
                return -1;
            }
        }

        /* Broadcast RELEASE to all others */
        msg.type = BARR_RELEASE;
        for (int r=1; r<g_ctx.world_size; ++r) {
            (void)ucx_send_bytes(r, &msg, sizeof(msg), OP_BARRIER);
        }
        log_info("UCX barrier stage %d: released all", stage_id);
        return 0;
    }
}
