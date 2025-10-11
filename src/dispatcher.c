/**
 * @file dispatcher.c
 * @author Wang Bo
 * @date 2025-09-16
 * @brief Implementation of message dispatcher & progress loop.
 *
 * The progress thread polls UCX worker for request opcodes (CREATE_REQ,
 * JOIN_REQ, EXIT_NOTIFY) and handles them. Response messages (CREATE_ACK,
 * JOIN_RESP) are not consumed here (they are awaited by API callers).
 */

#include "dispatcher.h"
#include "proto.h"
#include "context.h"
#include "ucx.h"
#include "functable.h"
#include "threadtable.h"
#include "log.h"
#include "tid.h"

#include <pthread.h>
#include <stdlib.h>
#include <string.h>
#include <inttypes.h>
#include <stdatomic.h>
#include <unistd.h>
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif
#include <dlfcn.h>   /* for dlsym, requires linking with -ldl */

/* ---- Tag helpers: hi32=opcode, lo32=rank ---- */
#define TAG_MAKE(op, rank)     ((((uint64_t)(op)) << 32) | (uint32_t)(rank))
#define TAG_MASK_OPCODE        (0xffffffff00000000ull)

#define TAG_OPCODE(tag)     ((int)((tag) >> 32))
#define TAG_SRC(tag)        ((int)((tag) & 0xffffffffu))

/* 后台线程控制 */
static pthread_t       g_disp_thr;
static atomic_int      g_disp_running = 0;

/* 为了简单演示，这里设置单条消息最大 64KB。
* 若需要可更大，但请注意堆分配与协议中的最大负载约束。 */
#ifndef DISPATCH_MAX_MSG
#define DISPATCH_MAX_MSG (64*1024)
#endif

/* Handle function announce broadcast */
/* ---- handle_func_announce: bind mapping & try cold-start resolve ---- */
static void handle_func_announce(const void *buf, size_t len, uint32_t src_rank)
{
    if (len < sizeof(msg_func_announce_t)) {
        log_error("FUNC_ANNOUNCE too short (len=%zu)", len);
        return;
    }
    const msg_func_announce_t *hdr = (const msg_func_announce_t*)buf;
    if (sizeof(*hdr) + hdr->name_len > len) {
        log_error("FUNC_ANNOUNCE malformed name_len=%u", hdr->name_len);
        return;
    }
    const char *name = (const char*)buf + sizeof(*hdr);
    /* Ensure a NUL-terminated local copy */
    char cname[MAX_FUNC_NAME];
    size_t n = hdr->name_len;
    if (n >= MAX_FUNC_NAME) n = MAX_FUNC_NAME - 1;
    memcpy(cname, name, n); cname[n] = '\0';
    log_debug("Received FUNC_ANNOUNCE of %s from rank=%u (len=%zu)", cname, src_rank, len);

    /* Try resolve pointer (optional). dlsym needs -rdynamic at link. */
    void *sym = dlsym(RTLD_DEFAULT, cname);
    void *(*fn)(void*) = (void*(*)(void*))sym;

    /* Bind mapping locally; pointer may be NULL if not resolvable now. */
    if (functable_bind_local(cname, (int)hdr->func_id, fn) != 0) {
        log_warn("FUNC_ANNOUNCE bind failed: name=%s id=%u", cname, hdr->func_id);
        return;
    }

    log_info("FUNC_ANNOUNCE: name=%s id=%u from rank=%u (fn=%p)",
            cname, hdr->func_id, src_rank, (void*)fn);
}


/*  ---- handle_create_req: no cold-start here, rely on announce ---- */
static void handle_create_req(const void *buf, size_t len, uint32_t src_rank)
{
    if (len < sizeof(msg_create_req_t)) {
        log_error("CREATE_REQ too short (len=%zu)", len);
        return;
    }
    const msg_create_req_t *req = (const msg_create_req_t*)buf;
    size_t off = sizeof(*req);

    /* Skip optional name payload if present (but we don't use it here) */
    if (off + req->name_len > len) {
        log_error("CREATE_REQ malformed name_len");
        return;
    }
    off += req->name_len;

    /* Extract arg payload */
    void *arg_copy = NULL;
    if (req->arg_len > 0) {
        if (off + req->arg_len > len) {
            log_error("CREATE_REQ malformed arg_len");
            return;
        }
        arg_copy = malloc(req->arg_len);
        if (!arg_copy) {
            log_error("malloc failed for arg_copy");
            return;
        }
        memcpy(arg_copy, ((const char*)buf)+off, req->arg_len);
        off += req->arg_len;
    }

    /* Lookup function by id (must have been announced already) */
    void *(*fn)(void*) = functable_get_by_id((int)req->func_id);

    msg_create_ack_t ack = { .opcode = OP_CREATE_ACK, .gtid = 0, .status = 0 };

    if (!fn) {
        log_warn("CREATE_REQ: func_id=%u not known yet (announce race?)", req->func_id);
        ack.status = -2;
        ucx_send_bytes(src_rank, &ack, sizeof(ack), OP_CREATE_ACK);
        free(arg_copy);
        return;
    }

    int tid = threadtable_alloc();
    if (tid < 0) {
        log_error("No local thread slot available");
        ack.status = -1;
        ucx_send_bytes(src_rank, &ack, sizeof(ack), OP_CREATE_ACK);
        free(arg_copy);
        return;
    }
    g_local_threads[tid].in_use = 1;
    g_local_threads[tid].finished = 0;

    pthread_create(&g_local_threads[tid].thread, NULL, fn, arg_copy);

    ack.gtid   = LEO_TID_MAKE(g_ctx.rank, tid);
    ack.status = 0;
    ucx_send_bytes(src_rank, &ack, sizeof(ack), OP_CREATE_ACK);

    log_info("Handled CREATE_REQ func_id=%u -> gtid=%" PRIu64, req->func_id, ack.gtid);
}

/* ----------- internal：handle JOIN_REQ ----------- */
static void handle_join_req(const void *buf, size_t len, uint32_t src_rank)
{
    if (len < sizeof(msg_join_req_t)) {
        log_error("JOIN_REQ too short (len=%zu)", len);
        return;
    }
    const msg_join_req_t *req = (const msg_join_req_t*)buf;
    uint64_t gtid = req->gtid;

    int owner_rank = LEO_TID_RANK(gtid);
    int local_tid  = LEO_TID_LOCAL(gtid);

    msg_join_resp_t resp;
    resp.opcode = OP_JOIN_RESP;
    resp.gtid   = gtid;
    resp.done   = 0;

    // if (owner_rank != g_ctx.rank || local_tid < 0 || local_tid >= MAX_LOCAL_THREADS) {
    //     log_warn("JOIN_REQ invalid owner"); 
    //     ucx_send_bytes(src_rank, &resp, sizeof(resp), OP_JOIN_RESP); 
    //     return;
    // }

    // local_thread_t* local_thread_entry = &g_local_threads[local_tid];
    // if (!atomic_load(&local_thread_entry->in_use)) {
    //     resp.done = atomic_load(&local_thread_entry->finished) ? 1 : 1; // 已经没人占用，视为done
    //     ucx_send_bytes(src_rank, &resp, sizeof(resp), OP_JOIN_RESP);
    //     return;
    // }

    // if (atomic_load(&local_thread_entry->finished)) {
    //     resp.done = 1;
    //     ucx_send_bytes(src_rank, &resp, sizeof(resp), OP_JOIN_RESP);
    //     return;
    // }

    // // 还未完成：登记等待者，**不阻塞 dispatcher**
    // join_waiter_t* w = malloc(sizeof(*w));
    // if (!w) { ucx_send_bytes(src_rank, &resp, sizeof(resp), OP_JOIN_RESP); return; }
    // w->src_rank = src_rank; w->next = NULL;

    // pthread_mutex_lock(&local_thread_entry->mu);
    // w->next = local_thread_entry->waiters; local_thread_entry->waiters = w;
    // pthread_mutex_unlock(&local_thread_entry->mu);

    if (owner_rank == g_ctx.rank &&
        local_tid >= 0 && local_tid < MAX_LOCAL_THREADS &&
        g_local_threads[local_tid].in_use) {

        /* 阻塞等待该本地线程结束（与 pthread_join 语义一致） */
        pthread_join(g_local_threads[local_tid].thread, NULL);
        g_local_threads[local_tid].in_use = 0;
        g_local_threads[local_tid].finished = 1;
        resp.done = 1;

        log_info("JOIN_REQ handled: gtid=%" PRIu64 " joined", gtid);
    } else {
        log_warn("JOIN_REQ invalid or not owner: gtid=%" PRIu64, gtid);
    }

    ucx_send_bytes(src_rank, &resp, sizeof(resp), OP_JOIN_RESP);
}

/* ----------- 分发入口：根据 opcode 调用处理 ----------- */
void dispatch_msg(void *buf, size_t len, ucp_tag_t tag)
{
    uint32_t opcode   = (uint32_t)(tag >> 32);
    uint32_t src_rank = (uint32_t)(tag & 0xffffffffu);

    switch (opcode) {
        case OP_FUNC_ANNOUNCE: handle_func_announce(buf, len, src_rank); break;
        case OP_CREATE_REQ: handle_create_req(buf, len, src_rank); break;
        case OP_JOIN_REQ:   handle_join_req(buf, len, src_rank);   break;
        case OP_EXIT_NOTIFY:
            /* 这里可补：更新元数据/回收资源/转发通知等 */
            log_info("EXIT_NOTIFY received (len=%zu) from rank=%u", len, src_rank);
            break;
        case OP_CREATE_ACK:
        case OP_JOIN_RESP:
            /* 响应类消息由 API 的等待逻辑处理，这里不消费 */
            log_debug("Response opcode=%u from rank=%u ignored by dispatcher", opcode, src_rank);
            break;
        default:
            log_warn("Unknown opcode=%u from rank=%u", opcode, src_rank);
            break;
    }
}

/* Try to receive one message of a given request opcode.
* Returns 1 if a message was received & dispatched, else 0.
*/
static int try_recv_request_opcode(int opcode)
{
    ucp_worker_h worker = g_ctx.ucx_ctx.ucp_worker;

    ucp_tag_recv_info_t info;
    ucp_tag_message_h msg =
        ucp_tag_probe_nb(worker, TAG_MAKE(opcode, 0), TAG_MASK_OPCODE, /*remove=*/1, &info);
    if (msg == NULL) return 0; /* no message */

    size_t real_len = info.length;
    if (real_len == 0) real_len = 1; /* UCX 不保证非零，至少给1字节 */

    /* 收取该消息（用固定缓冲区大小；如需更大消息，请扩展协议/分段） */
    char *rbuf = (char*)malloc(real_len);
    if (!rbuf) {
        log_error("dispatcher: malloc %zu failed", real_len);
        return 0;
    }

    ucp_request_param_t prm;
    memset(&prm, 0, sizeof(prm));
    prm.op_attr_mask = UCP_OP_ATTR_FIELD_DATATYPE;
    prm.datatype     = ucp_dt_make_contig(1);

    ucs_status_ptr_t rreq = ucp_tag_msg_recv_nbx(worker, rbuf, real_len, msg, &prm);
    if (UCS_PTR_IS_PTR(rreq)) {
        /* 轮询直到完成 */
        ucs_status_t st;
        do {
            ucp_worker_progress(worker);
            st = ucp_request_check_status(rreq);
        } while (st == UCS_INPROGRESS);
        ucp_request_free(rreq);
        if (st != UCS_OK) {
            log_warn("dispatcher: recv completion status=%d", (int)st);
            free(rbuf);
            return 1; /* 已经拿走了消息，但失败，返回1避免死循环 */
        }
    } else {
        ucs_status_t st = UCS_PTR_STATUS(rreq);
        if (st != UCS_OK) {
            log_warn("dispatcher: recv immediate status=%d", (int)st);
            free(rbuf);
            return 1;
        }
    }

    dispatch_msg(rbuf, real_len, info.sender_tag);

    free(rbuf);
    return 1;
}

/* ----------- 对外：手动推进一次 ----------- */
void dispatcher_progress_once(void)
{
    ucp_worker_h worker = g_ctx.ucx_ctx.ucp_worker;

    /* 无条件先推进一次，驱动 wireup/收发完成 */
    ucp_worker_progress(worker);

    /* 只探测请求类 opcode，避免与 API 对响应竞争 */
    static const int req_ops[] = { OP_FUNC_ANNOUNCE, OP_CREATE_REQ, OP_JOIN_REQ, OP_EXIT_NOTIFY, OP_JOIN_REQ, OP_JOIN_RESP };
    int progressed = 0;

    for (size_t i = 0; i < sizeof(req_ops)/sizeof(req_ops[0]); i++) {
        progressed |= try_recv_request_opcode(req_ops[i]);
        /* 每次处理后再推进一次，减少延迟 */
        ucp_worker_progress(worker);
    }

    /* 无事可做时小睡片刻，避免空转；有 UCX 事件fd时可改为 poll/epoll */
    if (!progressed) {
        usleep(2000); /* 2ms */
        ucp_worker_progress(worker);
    }
}

/* ----------- 后台线程主体 ----------- */
static void* disp_thread_main(void *arg)
{
    (void)arg;
    log_info("Dispatcher thread started");
    while (atomic_load(&g_disp_running)) {
        dispatcher_progress_once();
        /* 也可调用 ucp_worker_progress(g_ctx.ucx_ctx.ucp_worker); */
    }
    log_info("Dispatcher thread exiting");
    return NULL;
}

/* ----------- public API：start/stop ----------- */
int dispatcher_start(void)
{
    if (atomic_exchange(&g_disp_running, 1) == 1) {
        return 0; /* already running */
    }
    int rc = pthread_create(&g_disp_thr, NULL, disp_thread_main, NULL);
    if (rc != 0) {
        atomic_store(&g_disp_running, 0);
        log_error("Failed to start dispatcher thread (rc=%d)", rc);
        return -1;
    }
    return 0;
}

int dispatcher_stop(void)
{
    if (atomic_exchange(&g_disp_running, 0) == 0) {
        return 0; /* not running */
    }
    pthread_join(g_disp_thr, NULL);
    return 0;
}