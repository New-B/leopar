/**
 * @file sync.c
 * @author Wang Bo
 * @date 2025-09-19
 * @brief Global barrier for LeoPar.
 */
#define _POSIX_C_SOURCE 200112L
#include "leopar.h"
#include "context.h"
#include "proto.h"
#include "ucx.h"
#include "dispatcher.h"
#include "threadtable.h"
#include "tid.h"
#include "log.h"

#include <time.h>
#include <errno.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#ifdef __linux__
#include <pthread.h>
#endif

 /* ---- small time helper ---- */
 static inline unsigned long long now_ms(void) {
    struct timespec ts; clock_gettime(CLOCK_REALTIME, &ts);
    return (unsigned long long)ts.tv_sec * 1000ull + (unsigned long long)ts.tv_nsec / 1000000ull;
}

/* Barrier epoch to distinguish rounds */
static volatile uint32_t g_barrier_epoch = 0;

/* ===== barrier implementation ===== */
int leo_barrier(void)
{
    const int me = g_ctx.rank;
    const int n  = g_ctx.world_size;
    if (n <= 1) return 0;

    const uint32_t epoch = __atomic_add_fetch(&g_barrier_epoch, 1, __ATOMIC_SEQ_CST);

    if (me != 0) {
        /* send ARRIVE to rank0 */
        msg_barrier_arrive_t a = { .opcode = OP_BARRIER_ARRIVE, .epoch = epoch };
        if (ucx_send_bytes(0, &a, sizeof(a), OP_BARRIER_ARRIVE) != 0) {
            log_error("barrier: ARRIVE send failed (epoch=%u)", epoch);
            return -1;
        }

        /* wait for RELEASE from rank0 */
        while (1) {
            size_t len=0; ucp_tag_t tag=0; ucp_tag_recv_info_t info;
            void *buf = ucx_recv_any_alloc(&len, &tag, &info);
            if (!buf) { dispatcher_progress_once(); continue; }

            uint32_t opcode = (uint32_t)(tag >> 32);
            uint32_t src    = (uint32_t)(tag & 0xffffffffu);

            if (opcode == OP_BARRIER_RELEASE && src == 0 && len >= sizeof(msg_barrier_release_t)) {
                msg_barrier_release_t *rel = (msg_barrier_release_t*)buf;
                if (rel->epoch == epoch) { free(buf); break; }
            }

            /* not our RELEASE → dispatch or drop as appropriate */
            dispatch_msg(buf, len, tag);
            free(buf);
        }
        return 0;

    } else {
        /* rank0: gather ARRIVE from all others using tag-probe to avoid stealing responses */
        int arrived = 0;

        while (arrived < n - 1) {
            ucp_worker_h w = g_ctx.ucx_ctx.ucp_worker;

            ucp_tag_message_h msg = ucp_tag_probe_nb(
                w,
                ((uint64_t)OP_BARRIER_ARRIVE) << 32,  /* tag value with opcode in hi32 */
                0xffffffff00000000ull,                /* mask: match only opcode field */
                0, NULL);

            if (msg == NULL) {
                dispatcher_progress_once();
                usleep(1000);
                continue;
            }

            msg_barrier_arrive_t a;
            ucp_request_param_t prm; memset(&prm, 0, sizeof(prm));
            prm.op_attr_mask = UCP_OP_ATTR_FIELD_DATATYPE;
            prm.datatype     = ucp_dt_make_contig(1);

            ucs_status_ptr_t r = ucp_tag_msg_recv_nbx(w, &a, sizeof(a), msg, &prm);
            if (UCS_PTR_IS_PTR(r)) {
                ucs_status_t st;
                do { ucp_worker_progress(w); st = ucp_request_check_status(r); }
                while (st == UCS_INPROGRESS);
                ucp_request_free(r);
            }

            if (a.epoch == epoch) {
                arrived++;
                log_debug("barrier: ARRIVE %d/%d (epoch=%u)", arrived, n-1, epoch);
            }
        }

        /* release everyone */
        msg_barrier_release_t rel = { .opcode = OP_BARRIER_RELEASE, .epoch = epoch };
        for (int r = 1; r < n; ++r) {
            (void)ucx_send_bytes(r, &rel, sizeof(rel), OP_BARRIER_RELEASE);
        }
        return 0;
    }
}

/* ===== join with timeout ===== */
int leo_thread_join_timeout(leo_thread_t t, void **retval, int64_t timeout_ms)
{
    (void)retval; /* return value propagation is not implemented in this prototype */

    const int owner = LEO_TID_RANK(t);
    const int ltid  = LEO_TID_LOCAL(t);

    unsigned long long t0 = now_ms();

    if (owner == g_ctx.rank) {
        /* local join with bounded wait */
        while (1) {
#ifdef __linux__
            void *res = NULL;
            int prc = pthread_tryjoin_np(g_local_threads[ltid].thread, &res);
            if (prc == 0) {
                g_local_threads[ltid].in_use = 0;
                g_local_threads[ltid].finished = 1;
                return 0;
            }
#endif
            if (g_local_threads[ltid].finished) {
                return 0; /* already reaped elsewhere */
            }
            if ((int64_t)(now_ms() - t0) > timeout_ms) return -ETIMEDOUT;
            dispatcher_progress_once();
            usleep(1000);
        }
    } else {
        /* remote join protocol with timeout */
        msg_join_req_t req = { .opcode = OP_JOIN_REQ, .gtid = t };
        if (ucx_send_bytes(owner, &req, sizeof(req), OP_JOIN_REQ) != 0) {
            return -1;
        }

        while ((int64_t)(now_ms() - t0) <= timeout_ms) {
            size_t len=0; ucp_tag_t tag=0; ucp_tag_recv_info_t info;
            void *buf = ucx_recv_any_alloc(&len, &tag, &info);
            if (!buf) { dispatcher_progress_once(); continue; }

            uint32_t opcode = (uint32_t)(tag >> 32);
            uint32_t src    = (uint32_t)(tag & 0xffffffffu);

            if (opcode == OP_JOIN_RESP && src == (uint32_t)owner && len >= sizeof(msg_join_resp_t)) {
                msg_join_resp_t *resp = (msg_join_resp_t*)buf;
                int ok = (resp->gtid == t && resp->done);
                free(buf);
                return ok ? 0 : -1;
            }
            dispatch_msg(buf, len, tag);
            free(buf);
        }
        return LEO_ETIMEDOUT;
    }
}