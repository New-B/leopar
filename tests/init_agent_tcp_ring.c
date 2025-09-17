/**
 * init_agent_tcp_ring.c
 * Author: Your Name
 * Date: 2025-09-11
 *
 * Demo program for UCX TCP bootstrap (ring allgather).
 * Usage:
 *   ./init_agent_tcp_ring <config_path> <my_rank>
 */

#include "ucx_tcp.h"
#include "log.h"

#include <ucp/api/ucp.h>
#include <ucs/type/status.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static ucp_context_h g_ctx = NULL;
static ucp_worker_h  g_worker = NULL;

static int ucx_minimal_init()
{
    ucp_config_t *config=NULL;
    if (ucp_config_read(NULL, NULL, &config) != UCS_OK) return -1;

    ucp_params_t params;
    memset(&params, 0, sizeof(params));
    params.field_mask = UCP_PARAM_FIELD_FEATURES;
    params.features   = UCP_FEATURE_TAG | UCP_FEATURE_RMA | UCP_FEATURE_AM;

    if (ucp_init(&params, config, &g_ctx) != UCS_OK) {
        ucp_config_release(config);
        return -1;
    }
    ucp_config_release(config);

    ucp_worker_params_t wp;
    memset(&wp, 0, sizeof(wp));
    wp.field_mask  = UCP_WORKER_PARAM_FIELD_THREAD_MODE;
    wp.thread_mode = UCS_THREAD_MODE_MULTI;

    if (ucp_worker_create(g_ctx, &wp, &g_worker) != UCS_OK) {
        ucp_cleanup(g_ctx);
        g_ctx=NULL;
        return -1;
    }
    return 0;
}

static void ucx_minimal_finalize()
{
    if (g_worker) { ucp_worker_destroy(g_worker); g_worker=NULL; }
    if (g_ctx)    { ucp_cleanup(g_ctx); g_ctx=NULL; }
}

int main(int argc, char **argv)
{
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <config_path> <my_rank>\n", argv[0]);
        return 1;
    }
    const char *cfg_path = argv[1];
    int my_rank = atoi(argv[2]);

    if (log_init("leopar_ucx_ring.log", LOG_INFO) == 0) {
        log_info("Runtime initialized.");
    } else {
        log_error("Failed to initialize log system.");
    }
    log_set_rank(my_rank);

    ucx_tcp_config_t cfg;
    if (ucx_tcp_load_config(cfg_path, &cfg) != 0) {
        log_error("load config failed");
        return 2;
    }
    if (my_rank < 0 || my_rank >= cfg.world_size) {
        log_error("invalid my_rank=%d (world_size=%d)", my_rank, cfg.world_size);
        return 3;
    }

    if (ucx_minimal_init() != 0) {
        log_error("UCX init failed");
        return 4;
    }
    log_info("UCX initialized at rank %d", my_rank);

    ucp_address_t **tbl_addrs = NULL;
    size_t *tbl_lens          = NULL;
    if (ucx_tcp_ring_allgather_ucx_addrs(&cfg, my_rank, g_worker, &tbl_addrs, &tbl_lens) != 0) {
        log_error("ring allgather failed");
        ucx_minimal_finalize();
        return 5;
    }

    ucp_ep_h *eps = (ucp_ep_h*)calloc(cfg.world_size, sizeof(ucp_ep_h));
    if (!eps) {
        log_error("allocate eps array failed");
        for (int i=0;i<cfg.world_size;i++) if (tbl_addrs[i]) free(tbl_addrs[i]);
        free(tbl_addrs); free(tbl_lens);
        ucx_minimal_finalize();
        return 6;
    }
    if (ucx_tcp_create_all_eps(g_ctx, g_worker, my_rank, cfg.world_size, tbl_addrs, tbl_lens, eps) != 0) {
        log_warn("some endpoints may have failed; continuing");
    }

    for (int i=0;i<cfg.world_size;i++) if (tbl_addrs[i]) free(tbl_addrs[i]);
    free(tbl_addrs); free(tbl_lens);

    log_info("endpoints created at rank %d", my_rank);

    for (int i=0;i<50;i++) {
        ucp_worker_progress(g_worker);
        usleep(20000);
    }

    for (int i=0;i<cfg.world_size;i++) {
        if (eps[i]) ucp_ep_destroy(eps[i]);
    }
    free(eps);

    ucx_minimal_finalize();
    log_info("done at rank %d", my_rank);
    log_finalize();
    return 0;
}
