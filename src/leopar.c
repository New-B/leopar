/**
 * @file leopar.c
 * @author Wang Bo
 * @date 2025-09-09
 * @brief LeoPar runtime implementation: integrates UCX TCP bootstrap + log system.
 */


#include "leopar.h"
#include "context.h"
#include "functable.h"
#include "scheduler.h"
#include "threadtable.h"
#include "dispatcher.h"
#include "log.h"
#include "ucx.h"
#include "proto.h"
#include "tid.h"
#include "ctrl.h"

#include <ucp/api/ucp.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <inttypes.h>
#include <time.h>
#include <stdatomic.h>
#include <sys/stat.h>
#ifdef __linux__
#include <pthread.h>
#endif
 
/* Define TAG_MASK_OPCODE for tag matching */
/* ---- Tag helpers: hi32=opcode, lo32=rank ---- */
#define TAG_MAKE(op, rank)     ((((uint64_t)(op)) << 32) | (uint32_t)(rank))
#define TAG_MASK_OPCODE 0xFFFFFFFF00000000ULL

static int leo_thread_create_impl(leo_thread_t *thread,
                                  const pthread_attr_t *attr,
                                  void *(*start_routine)(void*),
                                  const char *func_name,
                                  const void *arg,
                                  size_t arg_len,
                                  int target_rank,
                                  int copy_arg)
{
    (void)attr; /* not used in this prototype */

    int func_id = functable_get_id_by_ptr(start_routine);
    if (func_id < 0) {
        func_id = functable_register(func_name, start_routine);
        if (func_id < 0) {
            log_error("functable_register failed for %s", func_name);
            return -1;
        }
    }
    log_debug("leo_thread_create_impl: func_name=%s func_id=%d", func_name, func_id);

    int dest_rank = (target_rank >= 0) ? target_rank : scheduler_choose_rank(g_ctx.world_size);
    if (dest_rank >= g_ctx.world_size) dest_rank %= g_ctx.world_size;
    log_debug("leo_thread_create_impl: chosen target rank=%d copy_arg=%d arg_len=%zu",
              dest_rank, copy_arg, arg_len);

    if (dest_rank == g_ctx.rank) {
        int tid = threadtable_alloc();
        if (tid < 0) {
            log_error("Thread table full at rank=%d", g_ctx.rank);
            return -1;
        }

        void *spawn_arg = (void*)arg;
        if (copy_arg && arg_len > 0) {
            spawn_arg = malloc(arg_len);
            if (!spawn_arg) {
                (void)threadtable_reclaim(tid);
                return -1;
            }
            memcpy(spawn_arg, arg, arg_len);
        }

        uint64_t gtid = LEO_TID_MAKE(g_ctx.rank, tid);
        if (threadtable_spawn(tid, start_routine, spawn_arg, arg_len, gtid, g_ctx.rank) != 0) {
            log_error("Failed to spawn local LeoPar thread tid=%d", tid);
            if (copy_arg && arg_len > 0) free(spawn_arg);
            (void)threadtable_reclaim(tid);
            return -1;
        }

        scheduler_note_placement(dest_rank);
        if (thread) *thread = gtid;
        return 0;
    }

    const size_t name_len = strlen(func_name);
    const size_t msg_len = sizeof(msg_create_req_t) + name_len + arg_len;
    char *msg_buf = (char*)malloc(msg_len);
    if (!msg_buf) return -1;

    msg_create_req_t *req = (msg_create_req_t*)msg_buf;
    req->opcode       = OP_CREATE_REQ;
    req->creator_rank = (uint32_t)g_ctx.rank;
    req->func_id      = (uint32_t)func_id;
    req->arg_len      = (uint32_t)arg_len;
    req->name_len     = (uint32_t)name_len;
    req->gtid         = 0;

    size_t off = sizeof(*req);
    memcpy(msg_buf + off, func_name, name_len);
    off += name_len;
    if (arg_len > 0 && arg) {
        memcpy(msg_buf + off, arg, arg_len);
    }

    if (ucx_send_bytes(dest_rank, msg_buf, msg_len, OP_CREATE_REQ) != 0) {
        log_error("Failed to send create request to rank=%d", dest_rank);
        free(msg_buf);
        return -1;
    }
    free(msg_buf);

    while (1) {
        size_t len=0; ucp_tag_t tag=0; ucp_tag_recv_info_t info;
        void *ack_buf = ucx_recv_any_alloc(&len, &tag, &info);
        if (!ack_buf) continue;

        uint32_t opcode = (uint32_t)(tag >> 32);
        uint32_t src    = (uint32_t)(tag & 0xffffffffu);

        if (opcode == OP_CREATE_ACK &&
            src == (uint32_t)dest_rank &&
            len >= sizeof(msg_create_ack_t)) {

            msg_create_ack_t *ack = (msg_create_ack_t*)ack_buf;
            if (ack->status == 0) {
                scheduler_note_placement(dest_rank);
                if (thread) *thread = (leo_thread_t)ack->gtid;
                free(ack_buf);
                break;
            }

            log_error("CREATE_ACK failed status=%d from rank=%d", (int)ack->status, dest_rank);
            free(ack_buf);
            return -1;
        }

        free(ack_buf);
    }

    log_info("Sent thread create request func_id=%d to rank=%d", func_id, dest_rank);
    return 0;
}
 
/* -------------------- Public API -------------------- */
/* 
* Initialize the LeoPar runtime
* config_path: path to cluster.ini
* rank:       my rank (provided by launcher)
* return 0 on success, <0 on error
*/
int leopar_init(const char *config_path, int rank, const char *log_path)
{
    /* 1. Setup logging: log file name = leopar_rank<rank>-<ip>-<timestamp>.log */
    if (log_init(log_path, LEOPAR_DEFAULT_LOG_LEVEL) != 0) {
        fprintf(stderr, "Failed to open log file: %s\n", log_path);
        return -1;
    }
    log_set_rank(rank);

    /* 2. Parse cluster config (IPs, ports, timeouts, etc.) */
    if (load_config(config_path, rank) != 0) {
        log_error("Failed to load cluster config: %s", config_path);
        return -1;
    }

    log_info("LeoPar runtime starting: rank=%d size=%d ip=%s", g_ctx.rank, g_ctx.world_size, g_ctx.tcp_cfg.ip_of_rank[rank]);

    /* 3. Start control-plane (blocking TCP barrier, coordinator thread if rank==0) */
    if (ctrl_start() != 0) {
        log_error("CTRLm start failed");
        return -1;
    }

    /* 4. Synchronize all agents at early boot (before UCX)
    This ensures every node has loaded config and started the control-plane. */
    if (ctrl_barrier("boot", /*gen*/0, /*timeout_ms*/0) != 0) {
        log_warn("CTRLm barrier 'boot' timed out or failed; continuing may be unsafe");
    /* You may choose to bail out here instead of continuing. */
    }
    
    /* 5. Initialize UCX runtime (worker + endpoints) */
    if (ucx_init(&g_ctx.ucx_ctx, &g_ctx.tcp_cfg, g_ctx.rank) != 0) {
        log_error("UCX init failed at rank=%d", g_ctx.rank);
        return -1;
    }
    log_info("UCX context+worker initialized");

    /* 6. Initialize local dispatcher for remote requests */
    if (dispatcher_start() != 0) {
        log_error("dispatcher_start failed");
        return -1;
    }

    if (functable_init() != 0) {
        log_error("Function table init failed");
        return -1;
    }

    /* 7. Initialize local thread table */
    if (threadtable_init() != 0) {
        log_error("Thread table init failed for rank=%d", g_ctx.rank);
        return -1;
    }
    log_info("Thread table initialized (capacity=%d)", MAX_LOCAL_THREADS);

    /* 8. init dsm */
    if (dsm_init_c(&g_ctx.dsm_cfg) != 0) {
        log_warn("DSM init failed; continuing may be unsafe");
    }
    log_info("DSM initialized (rank=%d)", g_ctx.rank);
    ctrl_barrier("dsm_ready", /*gen*/0, 60000);

    /* 9. Optional: synchronize after runtime is fully online
       Only proceed when every node's UCX and dispatcher are ready. */
    if (ctrl_barrier("runtime_ready", /*gen*/1, /*timeout_ms*/0) != 0) {
        log_warn("CTRLm barrier 'runtime_ready' failed; cluster readiness may be inconsistent");
        /* Consider returning error here if you want strict semantics. */
    }

    log_info("LeoPar runtime initialized successfully at rank %d", g_ctx.rank);
    return 0;
}

void leopar_finalize(void)
{
    log_info("Finalizing LeoPar runtime (rank=%d)", g_ctx.rank);

    /* A. Synchronize shutdown across agents.
    The idea: you call finalize only after your application logic is done.
    This barrier ensures all nodes reach this point before tearing down transports. */
    if (ctrl_barrier("shutdown", /*gen*/2, /*timeout_ms*/0) != 0) {
        log_warn("CTRLm barrier 'shutdown' failed or timed out");
        /* You can still proceed locally to avoid hanging forever. */
    }

    /* 1. Destroy thread table. Ensure local threads have exited */
    threadtable_finalize();
    (void)functable_finalize();

    /* 2. stop dispatcher */
    dispatcher_stop();

    /* 3. Finalize UCX layer */
    if (ucx_finalize(&g_ctx.ucx_ctx) != 0) {
        log_error("UCX finalize failed at rank=%d", g_ctx.rank);
    }

    ctrl_stop();
    dsm_finalize_c();

    /* 4. Close log system */
    log_info("LeoPar runtime finalized at rank %d", g_ctx.rank);
    log_finalize();
}

int leo_thread_create_named(leo_thread_t *thread,
                            const pthread_attr_t *attr,
                            void *(*start_routine)(void*),
                            const char *func_name,
                            void *arg,
                            int target_rank)
{
    return leo_thread_create_impl(thread, attr, start_routine, func_name,
                                  arg, (arg ? sizeof(void*) : 0),
                                  target_rank, /*copy_arg=*/0);
}

int leo_thread_create_copy_named(leo_thread_t *thread,
                                 const pthread_attr_t *attr,
                                 void *(*start_routine)(void*),
                                 const char *func_name,
                                 const void *arg,
                                 size_t arg_len,
                                 int target_rank)
{
    if ((arg_len > 0 && !arg) || arg_len > UINT32_MAX) {
        return -1;
    }
    return leo_thread_create_impl(thread, attr, start_routine, func_name,
                                  arg, arg_len, target_rank, /*copy_arg=*/1);
}

/* ===== leo_thread_join (pthread-style) ===== */
int leo_thread_join(leo_thread_t thread, void **retval)
{
    int owner_rank = LEO_TID_RANK(thread);
    int local_tid  = LEO_TID_LOCAL(thread);

    /* 1. Local thread join */
    if (owner_rank == g_ctx.rank) {
        int rc = threadtable_wait_local(local_tid, retval, -1);
        if (rc == 0) {
            log_info("Joined local thread tid=%d", local_tid);
            return 0;
        }
        log_error("Invalid local tid=%d for join", local_tid);
        return -1;
    }

    /* 2. Remote thread join: send JOIN_REQ */
    msg_join_req_t req;
    req.opcode = OP_JOIN_REQ;
    req.gtid   = thread;

    if (ucx_send_bytes(owner_rank, &req, sizeof(req), OP_JOIN_REQ) != 0) {
        log_error("Failed to send JOIN_REQ for gtid=%" PRIu64, req.gtid);
        return -1;
    }
    log_debug("Sent JOIN_REQ for gtid=%" PRIu64 " to rank=%d", req.gtid, owner_rank);

    /* 3. Wait for JOIN_RESP */
    // while (1) {
    //     size_t len = 0;
    //     ucp_tag_t tag = 0;
    //     ucp_tag_recv_info_t info;
    //     void *resp_buf = ucx_recv_any_alloc(&len, &tag, &info);

    //     if (!resp_buf) continue;

    //     msg_join_resp_t *resp = (msg_join_resp_t*)resp_buf;
    //     if (resp->opcode == OP_JOIN_RESP && resp->gtid == thread) {
    //         if (resp->done) {
    //             log_info("JOIN_RESP: gtid=%" PRIu64 " completed", thread);
    //             free(resp_buf);
    //             return 0;
    //         } else {
    //             log_warn("JOIN_RESP: gtid=%" PRIu64 " not finished yet", thread);
    //             /* Blocking design: keep waiting until done==1 */
    //         }
    //     }
    //     free(resp_buf);
    // }

    /* Replace your any-recv loop with this: */
    /* owner_rank = LEO_TID_RANK(thread) already known */

    ucp_worker_h worker = g_ctx.ucx_ctx.ucp_worker;

    /* buffer for response */
    msg_join_resp_t resp;
    memset(&resp, 0, sizeof(resp));

    /* build expected tag: hi32 = OP_JOIN_RESP, lo32 = owner_rank */
    ucp_tag_t expect_tag = TAG_MAKE(OP_JOIN_RESP, owner_rank);

    /* recv params */
    ucp_request_param_t prm;
    memset(&prm, 0, sizeof(prm));
    prm.op_attr_mask = UCP_OP_ATTR_FIELD_DATATYPE;
    prm.datatype     = ucp_dt_make_contig(1);

    /* post one tagged receive for this join */
    ucs_status_ptr_t rreq = ucp_tag_recv_nbx(worker, &resp, sizeof(resp),
                                            expect_tag, TAG_MASK_OPCODE, &prm);

    if (UCS_PTR_IS_PTR(rreq)) {
        ucs_status_t st;
        do {
            ucp_worker_progress(worker);
            st = ucp_request_check_status(rreq);
        } while (st == UCS_INPROGRESS);
        ucp_request_free(rreq);
        if (st != UCS_OK) {
            log_warn("JOIN: recv completion status=%d for gtid=%" PRIu64, st, thread);
            return -1;
        }
    } else {
        ucs_status_t st = UCS_PTR_STATUS(rreq);
        if (st != UCS_OK) {
            log_warn("JOIN: recv immediate status=%d for gtid=%" PRIu64, (int)st, thread);
            return -1;
        }
    }

    /* verify payload */
    if (resp.opcode != OP_JOIN_RESP || resp.gtid != thread) {
        log_warn("JOIN: unexpected resp opcode=%d gtid=%" PRIu64 " (expected=%" PRIu64 ")",
                resp.opcode, resp.gtid, thread);
        return -1;
    }
    if (resp.done) {
        log_info("JOIN_RESP: gtid=%" PRIu64 " completed", thread);
        return 0;
    } else {
        log_warn("JOIN_RESP: gtid=%" PRIu64 " not finished yet", thread);
        /* protocol dependent: either retry or return waiting status */
        return -1;
    }

}

