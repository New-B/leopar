/**
 * @file leopar.c
 * @author Wang Bo
 * @date 2025-09-09
 * @brief LeoPar runtime implementation: integrates UCX TCP bootstrap + log system.
 */


#include "leopar.h"
#include "dsm.h"
#include "context.h"
#include "functable.h"
#include "scheduler.h"
#include "threadtable.h"
#include "dispatcher.h"
#include "log.h"
#include "ucx.h"
#include "proto.h"
#include "tid.h"

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
    
    /* 3. Initialize UCX runtime (worker + endpoints) */
    if (ucx_init(&g_ctx.ucx_ctx, &g_ctx.tcp_cfg, g_ctx.rank) != 0) {
        log_error("UCX init failed at rank=%d", g_ctx.rank);
        return -1;
    }
    log_info("UCX context+worker initialized");

    /* 4. Initialize local dispatcher for remote requests */
    if (dispatcher_start() != 0) {
        log_error("dispatcher_start failed");
        return -1;
    }

    /* 5. Initialize local thread table */
    if (threadtable_init() != 0) {
        log_error("Thread table init failed for rank=%d", g_ctx.rank);
        return -1;
    }
    log_info("Thread table initialized (capacity=%d)", MAX_LOCAL_THREADS);

    /* 6. Initialize DSM (local arena + rkey exchange) */
    size_t pool_bytes = (g_ctx.dsm_pool_mb > 0 ? (size_t)g_ctx.dsm_pool_mb << 20 : (64ull<<20));
    if (dsm_init(pool_bytes) != 0) { 
        log_error("DSM init failed"); 
        return -1; 
    }   

    log_info("LeoPar runtime initialized successfully at rank %d", g_ctx.rank);
    return 0;
}

void leopar_finalize(void)
{
    log_info("Finalizing LeoPar runtime (rank=%d)", g_ctx.rank);

    /* 1. Destroy thread table. Ensure local threads have exited */
    threadtable_finalize();

    /* 2. Finalize UCX layer */
    if (ucx_finalize(&g_ctx.ucx_ctx) != 0) {
        log_error("UCX finalize failed at rank=%d", g_ctx.rank);
    }

    dispatcher_stop();

    dsm_finalize();

    /* 3. Close log system */
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
    (void)attr; /* not used in this prototype */

    /* 1.Ensure a global func_id locally (dynamic registration) */
    int func_id = functable_get_id_by_ptr(start_routine);
    if (func_id < 0) {
        func_id = functable_register(func_name, start_routine);
        if (func_id < 0) {
            log_error("functable_register failed for %s", func_name);
            return -1;
        }
    }
    log_debug("leo_thread_create_named: func_name=%s func_id=%d", func_name, func_id);

    /* 2.Choose target rank */
    int dest_rank = (target_rank >= 0) ? target_rank : scheduler_choose_rank(g_ctx.world_size);
    if (dest_rank >= g_ctx.world_size) dest_rank %= g_ctx.world_size;
    log_debug("leo_thread_create_named: chosen target rank=%d", dest_rank);

    /* 3.Build CREATE_REQ with name + arg */
    size_t name_len = strlen(func_name);
    size_t arg_len  = sizeof(void*); /* TODO: real serialization */
    size_t msg_len  = sizeof(msg_create_req_t) + name_len + arg_len;

    /* 4. Local execution if target_rank == my_rank */
    if (dest_rank == g_ctx.rank){
        int tid = threadtable_alloc();
        if (tid < 0) {
            log_error("Thread table full at rank=%d", g_ctx.rank);
            return -1;
        }

        g_local_threads[tid].in_use = 1;
        g_local_threads[tid].finished = 0;

        pthread_create(&g_local_threads[tid].thread, NULL, start_routine, arg);

        if (thread) *thread = LEO_TID_MAKE(g_ctx.rank, tid); /*tid*/;
        log_debug("leo_thread_create_named: Created local thread tid=%d for func_id=%d", tid, func_id);
        return 0;
    } else {
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
        memcpy(msg_buf + off, func_name, name_len); off += name_len;
        memcpy(msg_buf + off, &arg, arg_len);       off += arg_len;

        /* 5. Remote execution via UCX */
        if (ucx_send_bytes(dest_rank, msg_buf, msg_len, OP_CREATE_REQ) != 0) {
            log_error("Failed to send create request to rank=%d", dest_rank);
            free(msg_buf);
            return -1;
        }
        free(msg_buf);

        /* 6) Wait for CREATE_ACK from target */
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
                    if (thread) *thread = (leo_thread_t)ack->gtid;
                    log_info("CREATE_ACK ok from rank=%d: gtid=%" PRIu64, dest_rank, ack->gtid);
                    free(ack_buf);
                    break;
                } else {
                    log_error("CREATE_ACK failed status=%d from rank=%d", (int)ack->status, dest_rank);
                    free(ack_buf);
                    return -1;
                }
            }

            /* Non-matching message: let dispatcher handle or drop; we free here to keep simple */
            free(ack_buf);
        }
    }
    log_info("Sent thread create request func_id=%d to rank=%d", func_id, target_rank);

    return 0;
}

/* ===== leo_thread_join (pthread-style) ===== */
int leo_thread_join(leo_thread_t thread, void **retval)
{
    int owner_rank = LEO_TID_RANK(thread);
    int local_tid  = LEO_TID_LOCAL(thread);

    /* 1. Local thread join */
    if (owner_rank == g_ctx.rank) {
        if (local_tid < MAX_LOCAL_THREADS &&
            g_local_threads[local_tid].in_use) {

            pthread_join(g_local_threads[local_tid].thread, retval);
            g_local_threads[local_tid].in_use = 0;
            g_local_threads[local_tid].finished = 1;

            log_info("Joined local thread tid=%d", local_tid);
            return 0;
        } else {
            log_error("Invalid local tid=%d for join", local_tid);
            return -1;
        }
    }

    /* 2. Remote thread join: send JOIN_REQ */
    msg_join_req_t req;
    req.opcode = OP_JOIN_REQ;
    req.gtid   = thread;

    if (ucx_send_bytes(owner_rank, &req, sizeof(req), OP_JOIN_REQ) != 0) {
        log_error("Failed to send JOIN_REQ for gtid=%" PRIu64, req.gtid);
        return -1;
    }

    /* 3. Wait for JOIN_RESP */
    while (1) {
        size_t len = 0;
        ucp_tag_t tag = 0;
        ucp_tag_recv_info_t info;
        void *resp_buf = ucx_recv_any_alloc(&len, &tag, &info);

        if (!resp_buf) continue;

        msg_join_resp_t *resp = (msg_join_resp_t*)resp_buf;
        if (resp->opcode == OP_JOIN_RESP && resp->gtid == thread) {
            if (resp->done) {
                log_info("JOIN_RESP: gtid=%" PRIu64 " completed", thread);
                free(resp_buf);
                return 0;
            } else {
                log_warn("JOIN_RESP: gtid=%" PRIu64 " not finished yet", thread);
                /* Blocking design: keep waiting until done==1 */
            }
        }
        free(resp_buf);
    }
}





