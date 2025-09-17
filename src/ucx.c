/**
 * ucx_tcp.c
 * Author: Your Name
 * Date: 2025-09-11
 *
 * TCP bootstrap implementation for UCX address exchange.
 * Implements:
 *  - config parsing
 *  - ring-based allgather over TCP with timeouts/retries and shared-secret auth
 *  - helper to create UCX endpoints from gathered address table
 *
 * Logging: log_info/log_warn/log_error/log_debug from your log module.
 */

#include "ucx.h"
#include "tcp.h"
#include "log.h"
#include "proto.h"
#include "context.h"

#include <ucp/api/ucp.h>
#include <ucs/type/status.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <poll.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <fcntl.h>
#include <unistd.h>
#include <errno.h>

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <inttypes.h>


/* Compose UCX tag: high 32 bits = opcode, low 32 bits = src_rank */
static inline ucp_tag_t make_tag(int opcode, int src_rank)
{
    return (((uint64_t)(uint32_t)opcode) << 32) | (uint32_t)src_rank;
}

/**
 * Initialize UCX context, worker, address exchange, and endpoints.
 * This replaces the old scattered UCX initialization logic.
 *
 * @param ucx   pointer to ucx_context_t (inside g_ctx)
 * @param cfg   pointer to cluster config (g_ctx.tcp_cfg)
 * @param rank  this process's rank
 * @return 0 on success, <0 on error
 */
int ucx_init(ucx_context_t *ucx, tcp_config_t *cfg, int rank)
{
    if (!ucx || !cfg) {
        log_error("ucx_init: Invalid input parameters (ucx=%p, cfg=%p)", ucx, cfg);
        return -1;
    }
    log_debug("ucx_init: Starting UCX initialization for rank=%d", rank);

    /* 1. Read UCX config and init context */
    ucp_params_t ucp_params;
    memset(&ucp_params, 0, sizeof(ucp_params));
    ucp_params.field_mask = UCP_PARAM_FIELD_FEATURES;
    ucp_params.features   = UCP_FEATURE_TAG; /* tag API */

    ucp_config_t *ucp_cfg = NULL;
    if (ucp_config_read(NULL, NULL, &ucp_cfg) != UCS_OK) {
        log_error("ucx_init: ucp_config_read failed");
        return -1;
    }
    log_debug("ucx_init: UCX config read successfully");
    ucp_config_print(ucp_cfg, stdout, NULL, UCS_CONFIG_PRINT_CONFIG);

    if (ucp_init(&ucp_params, ucp_cfg, &ucx->ucp_context) != UCS_OK) {
        log_error("ucx_init: ucp_init failed");
        ucp_config_release(ucp_cfg);
        return -1;
    }
    ucp_config_release(ucp_cfg);

    /* 2. Create worker */
    ucp_worker_params_t worker_params;
    memset(&worker_params, 0, sizeof(worker_params));
    worker_params.field_mask = UCP_WORKER_PARAM_FIELD_THREAD_MODE;
    worker_params.thread_mode = UCS_THREAD_MODE_MULTI;  /* multithreaded runtime */

    if (ucp_worker_create(ucx->ucp_context, &worker_params, &ucx->ucp_worker) != UCS_OK) {
        log_error("ucx_init: ucp_worker_create failed");
        ucp_cleanup(ucx->ucp_context); ucx->ucp_context = NULL;
        return -1;
    }

    /* 3. Get my worker address. */
    ucp_address_t *my_addr = NULL;
    size_t my_len = 0;
    if (ucp_worker_get_address(ucx->ucp_worker, &my_addr, &my_len) != UCS_OK) {
        log_error("ucx_init: ucp_worker_get_address failed");
        ucp_worker_destroy(ucx->ucp_worker); ucx->ucp_worker = NULL;
        ucp_cleanup(ucx->ucp_context); ucx->ucp_context = NULL;
        return -1;
    }

    /* 4. Exchange UCX addresses via TCP ring */
    ucp_address_t **tbl_addrs = NULL;
    size_t *tbl_lens = NULL;
    if (tcp_allgather_ucx_addrs(&g_ctx.tcp_cfg, g_ctx.rank, g_ctx.world_size,
                                my_addr, my_len, &tbl_addrs, &tbl_lens) != 0) {
        log_error("ucx_init: TCP allgather UCX addresses failed");
        ucp_worker_release_address(ucx->ucp_worker, my_addr);
        ucp_worker_destroy(ucx->ucp_worker); ucx->ucp_worker = NULL;
        ucp_cleanup(ucx->ucp_context); ucx->ucp_context = NULL;
        return -1;
    }
    ucp_worker_release_address(ucx->ucp_worker, my_addr);

    /* 5. Create endpoints */
    ucx->eps = (ucp_ep_h*)calloc(cfg->world_size, sizeof(ucp_ep_h));
    if (!ucx->eps) {
        log_error("ucx_init: Failed to allocate endpoint array");
        for (int i = 0; i < cfg->world_size; ++i) free(tbl_addrs[i]);
        free(tbl_addrs); free(tbl_lens);
        ucp_worker_destroy(ucx->ucp_worker); ucx->ucp_worker = NULL;
        ucp_cleanup(ucx->ucp_context); ucx->ucp_context = NULL;
        return -1;
    }

    if (ucx_tcp_create_all_eps(ucx->ucp_context, ucx->ucp_worker,
                               rank, cfg->world_size,
                               tbl_addrs, tbl_lens, ucx->eps) != 0) {
        log_warn("ucx_init: Some UCX endpoints may have failed");
    }

    for (int i = 0; i < cfg->world_size; ++i) {
        free(tbl_addrs[i]); free(tbl_addrs); free(tbl_lens);
    }

    /* 6. Save rank/world_size */
    ucx->rank = rank;
    ucx->world_size = cfg->world_size;

    log_info("ucx_init: UCX init complete: rank=%d, world_size=%d", rank, ucx->world_size);
    return 0;
}

int ucx_finalize(ucx_context_t *ucx)
{
    if (!ucx) return 0;

    /* 1. Destroy endpoints */
    if (ucx->eps) {
        for (int i = 0; i < ucx->world_size; i++) {
            if (ucx->eps[i]) {
                ucp_ep_destroy(ucx->eps[i]);
                ucx->eps[i] = NULL;
            }
        }
        free(ucx->eps);
        ucx->eps = NULL;
    }

    /* 2. Destroy worker */
    if (ucx->ucp_worker) {
        ucp_worker_destroy(ucx->ucp_worker);
        ucx->ucp_worker = NULL;
    }

    /* 3. Cleanup UCX context */
    if (ucx->ucp_context) {
        ucp_cleanup(ucx->ucp_context);
        ucx->ucp_context = NULL;
    }

    log_info("UCX finalized for rank=%d", ucx->rank);
    return 0;
}

/* ---- Convenience messaging APIs (use global g_ctx) ---- */

static inline ucp_worker_h W(void) { return g_ctx.ucx_ctx.ucp_worker; }
static inline ucp_ep_h     EP(int r){ return g_ctx.ucx_ctx.eps ? g_ctx.ucx_ctx.eps[r] : NULL; }

/* Send a contiguous byte buffer to dest_rank with (hi32=opcode, lo32=src).
 * Returns 0 on success, nonzero on error. */
int ucx_send_bytes(int dest_rank, const void *buf, size_t len, int opcode)
{
    if (dest_rank < 0 || dest_rank >= g_ctx.world_size) return -1;
    if (!g_ctx.ucx_ctx.eps || !g_ctx.ucx_ctx.eps[dest_rank]) return -1;

    ucp_request_param_t prm;
    memset(&prm, 0, sizeof(prm));
    prm.op_attr_mask = UCP_OP_ATTR_FIELD_DATATYPE;
    prm.datatype     = ucp_dt_make_contig(1);

    ucp_tag_t tag = make_tag(opcode, g_ctx.rank);

    ucs_status_ptr_t req = ucp_tag_send_nbx(g_ctx.ucx_ctx.eps[dest_rank], buf, len, tag, &prm);
    if (UCS_PTR_IS_PTR(req)) {
        /* wait for completion */
        ucs_status_t st;
        do {
            ucp_worker_progress(g_ctx.ucx_ctx.ucp_worker);
            st = ucp_request_check_status(req);
        } while (st == UCS_INPROGRESS);
        ucp_request_free(req);
        if (st != UCS_OK) {
            log_warn("ucx_send_bytes completion status=%d", (int)st);
            return -1;
        }
    } else {
        ucs_status_t st = UCS_PTR_STATUS(req);
        if (st != UCS_OK) {
            log_warn("ucx_send_bytes immediate status=%d", (int)st);
            return -1;
        }
    }
    return 0;
}

void *ucx_recv_any_alloc(size_t *out_len, ucp_tag_t *out_tag, ucp_tag_recv_info_t *out_info)
{
    /* Probe any message: tag=0, mask=0 matches everything */
    ucp_tag_recv_info_t info;
    ucp_tag_message_h msg;

    for (;;) {
        msg = ucp_tag_probe_nb(g_ctx.ucx_ctx.ucp_worker, 0, 0 /*mask*/, 1 /*remove*/, &info);
        if (msg != NULL) break;
        ucp_worker_progress(g_ctx.ucx_ctx.ucp_worker);
    }

    size_t len = info.length;

    /* allocate exact-size buffer */
    void *buf = malloc(len > 0 ? len : 1);
    if (!buf) return NULL;

    ucp_request_param_t prm;
    memset(&prm, 0, sizeof(prm));
    prm.op_attr_mask = UCP_OP_ATTR_FIELD_DATATYPE;
    prm.datatype     = ucp_dt_make_contig(1);

    ucs_status_ptr_t rreq = ucp_tag_msg_recv_nbx(g_ctx.ucx_ctx.ucp_worker, buf, len, msg, &prm);
    if (UCS_PTR_IS_PTR(rreq)) {
        ucs_status_t st;
        do {
            ucp_worker_progress(g_ctx.ucx_ctx.ucp_worker);
            st = ucp_request_check_status(rreq);
        } while (st == UCS_INPROGRESS);
        ucp_request_free(rreq);
        if (st != UCS_OK) {
            log_warn("ucx_recv_any_alloc completion status=%d", (int)st);
            free(buf);
            return NULL;
        }
    } else {
        ucs_status_t st = UCS_PTR_STATUS(rreq);
        if (st != UCS_OK) {
            log_warn("ucx_recv_any_alloc immediate status=%d", (int)st);
            free(buf);
            return NULL;
        }
    }

    if (out_len) *out_len = len;
    if (out_tag) *out_tag = info.sender_tag;
    if (out_info) *out_info = info;
    return buf;
}

/* Broadcast a small message to all ranks (except self).*/
int ucx_broadcast_bytes(const void *buf, size_t len, int opcode)
{
    int rc = 0;
    for (int r = 0; r < g_ctx.world_size; r++) {
        if (r == g_ctx.rank) continue;
        if (ucx_send_bytes(r, buf, len, opcode) != 0 && rc == 0){
            log_warn("ucx_broadcast_bytes: send failed to rank=%d", r);
            rc = -1; break;
        }
    }
    return rc;
}

/* ------------------------- create UCX endpoints ------------------------- */

int ucx_tcp_create_all_eps(ucp_context_h context,
                            ucp_worker_h worker,
                            int my_rank, int world_size,
                            ucp_address_t **tbl_addrs,
                            size_t *tbl_lens,
                            ucp_ep_h *eps_out)
{
    int rc_all = 0;
    for (int i=0;i<world_size;i++) {
        eps_out[i] = NULL;
        if (i == my_rank) continue;
        if (tbl_lens[i]==0 || tbl_addrs[i]==NULL) {
            log_warn("[ucx_tcp] no address for rank %d, skip ep_create", i);
            rc_all = -1; continue;
        }
        ucp_ep_params_t ep_params;
        memset(&ep_params, 0, sizeof(ep_params));
        ep_params.field_mask = UCP_EP_PARAM_FIELD_REMOTE_ADDRESS;
        ep_params.address    = tbl_addrs[i];

        ucs_status_t st = ucp_ep_create(worker, &ep_params, &eps_out[i]);
        if (st != UCS_OK) {
            log_warn("[ucx_tcp] ucp_ep_create to rank %d failed: %d", i, st);
            rc_all = -1; /* continue trying others */
        }else {
            log_info("UCX EP created to rank=%d", i);
        }
        
    }
    return rc_all;
}

