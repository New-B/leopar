/**
 * @file context.c
 * @author Wang Bo
 * @date 2025-09-13
 * @brief Global runtime context definition for LeoPar.
 */

#include "context.h"
#include "log.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h> 

/* Definition of the global context */
leopar_context_t g_ctx;


int load_config(const char *filename, int my_rank)
{
    /* ---- Fill DSM defaults ---- */
    dsm_conf_c* dsmConf = &g_ctx.dsm_cfg;
    dsmConf->is_master          = (my_rank == 0) ? 1 : 0;
    dsmConf->master_port        = 12345;
    dsmConf->master_ip          = ip_of(0);
    dsmConf->master_bindaddr    = NULL;

    dsmConf->worker_port        = 12346;
    dsmConf->worker_ip          = ip_of(my_rank);
    dsmConf->worker_bindaddr    = NULL;

    dsmConf->size               = 1024ull * 1024 * 512;  /* 512 MiB */
    dsmConf->ghost_th           = 1024ull * 1024;
    dsmConf->cache_th           = 0.15;
    dsmConf->unsynced_th        = 1;
    dsmConf->factor             = 1.25;
    dsmConf->maxclients         = 1024;
    dsmConf->maxthreads         = 10;
    dsmConf->backlog            = 128;
    dsmConf->loglevel           = 3;     /* assume LOG_DEBUG ~= 2; 可被 ini 覆盖 */
    dsmConf->logfile            = NULL;
    dsmConf->timeout_ms         = 10;
    dsmConf->eviction_period_ms = 100;

    FILE *fp = fopen(filename, "r");
    if (!fp) {
        log_error("Cannot open config file %s", filename);
        return -1;
    }

    char line[256];
    int ip_count = 0;

    /* init defaults */
    memset(&g_ctx, 0, sizeof(g_ctx));
    g_ctx.rank = my_rank;
    g_ctx.default_threads = 1;  /* fallback */

    while (fgets(line, sizeof(line), fp)) {
        /* remove trailing \n or \r\n */
        line[strcspn(line, "\r\n")] = '\0';

        if (strncmp(line, "world_size=", 11) == 0) {
            g_ctx.world_size = atoi(line + 11);
            g_ctx.tcp_cfg.world_size = g_ctx.world_size;

            /* allocate arrays */
            if (g_ctx.world_size > 0) {
                g_ctx.rank_ips = (char**)calloc((size_t)g_ctx.world_size, sizeof(char*));
                g_ctx.rank_threads = (int*)calloc((size_t)g_ctx.world_size, sizeof(int));
                if (!g_ctx.rank_ips || !g_ctx.rank_threads) {
                    log_error("Out of memory for rank arrays");
                    fclose(fp);
                    return -1;
                }
            }
        } else if (strncmp(line, "base_port=", 10) == 0) {
            g_ctx.tcp_cfg.base_port = atoi(line + 10);
        } else if (strncmp(line, "default_threads=", 16) == 0) {
            g_ctx.default_threads = atoi(line + 16);
        } else if (strncmp(line, "connect_timeout_ms=", 19) == 0) {
            g_ctx.tcp_cfg.connect_timeout_ms = atoi(line + 19);
        } else if (strncmp(line, "io_timeout_ms=", 14) == 0) {
            g_ctx.tcp_cfg.io_timeout_ms = atoi(line + 14);
        } else if (strncmp(line, "retry_max=", 10) == 0) {
            g_ctx.tcp_cfg.retry_max = atoi(line + 10);
        } else if (strncmp(line, "secret=", 7) == 0) {
            strncpy(g_ctx.tcp_cfg.secret, line + 7, TCP_SECRET_MAX - 1);
            g_ctx.tcp_cfg.secret[TCP_SECRET_MAX - 1] = '\0';  /* ensure NUL */
        } else if (strncmp(line, "rank", 4) == 0) {
            /* format: rankN=IP */
            char *eq = strchr(line, '=');
            if (eq) {
                int rank = atoi(line + 4);
                char *ip = eq + 1;

                if (rank < g_ctx.world_size && rank >= 0) {
                    if (g_ctx.rank_ips) {
                        g_ctx.rank_ips[rank] = strdup(ip);
                        if (!g_ctx.rank_ips[rank]) {
                            log_error("strdup failed for ip %s\n", ip);
                            fclose(fp);
                            return -1;
                        }
                    }
                    strncpy(g_ctx.tcp_cfg.ip_of_rank[rank], ip, 63);
                    g_ctx.tcp_cfg.ip_of_rank[rank][63] = '\0';
                    if (g_ctx.rank_threads) {
                        g_ctx.rank_threads[rank] = g_ctx.default_threads;
                    }
                    ip_count++;
                }
            }
        }
    }
    fclose(fp);

    /* ---- Optional: second pass for DSM keys ---- */
    FILE *fp2 = fopen(filename, "r");
    if (fp2) {
        char line[256];
        while (fgets(line, sizeof(line), fp2)) {
            line[strcspn(line, "\r\n")] = '\0';

            if      (strncmp(line, "dsm_master_port=", 16) == 0) dsmConf->master_port = atoi(line+16);
            else if (strncmp(line, "dsm_master_ip=",   14) == 0) dsmConf->master_ip   = strdup(line+14);
            else if (strncmp(line, "dsm_master_bindaddr=", 20) == 0) dsmConf->master_bindaddr = strdup(line+20);

            else if (strncmp(line, "dsm_worker_port=", 16) == 0) dsmConf->worker_port = atoi(line+16);
            else if (strncmp(line, "dsm_worker_ip=",   14) == 0) dsmConf->worker_ip   = strdup(line+14);
            else if (strncmp(line, "dsm_worker_bindaddr=", 20) == 0) dsmConf->worker_bindaddr = strdup(line+20);

            else if (strncmp(line, "dsm_pool_mb=",     12) == 0) {
                long long mb = atoll(line+12);
                if (mb > 0) dsmConf->size = (uint64_t)mb * 1024ull * 1024ull;
            }
            else if (strncmp(line, "dsm_ghost_th=",    13) == 0) dsmConf->ghost_th           = (uint64_t)atoll(line+13);
            else if (strncmp(line, "dsm_cache_th=",    13) == 0) dsmConf->cache_th           = atof(line+13);
            else if (strncmp(line, "dsm_unsynced_th=", 16) == 0) dsmConf->unsynced_th        = atoi(line+16);
            else if (strncmp(line, "dsm_factor=",      11) == 0) dsmConf->factor             = atof(line+11);
            else if (strncmp(line, "dsm_maxclients=",  15) == 0) dsmConf->maxclients         = atoi(line+15);
            else if (strncmp(line, "dsm_maxthreads=",  15) == 0) dsmConf->maxthreads         = atoi(line+15);
            else if (strncmp(line, "dsm_backlog=",     12) == 0) dsmConf->backlog            = atoi(line+12);
            else if (strncmp(line, "dsm_loglevel=",    13) == 0) dsmConf->loglevel           = atoi(line+13);
            else if (strncmp(line, "dsm_logfile=",     12) == 0) dsmConf->logfile            = strdup(line+12);
            else if (strncmp(line, "dsm_timeout_ms=",  15) == 0) dsmConf->timeout_ms         = atoi(line+15);
            else if (strncmp(line, "dsm_eviction_period_ms=", 23) == 0) dsmConf->eviction_period_ms = atoi(line+23);
        }
        fclose(fp2);
    }


    if (ip_count != g_ctx.world_size) {
        log_warn("Parsed %d IPs but world_size=%d\n", ip_count, g_ctx.world_size);
    }
    
    log_info("Config loaded: rank=%d world_size=%d base_port=%d default_threads=%d",
            g_ctx.rank, g_ctx.world_size, g_ctx.tcp_cfg.base_port, g_ctx.default_threads);
    log_info("DSM cfg: is_master=%d master=%s:%d worker=%s:%d size=%llu cache_th=%.2f",
        dsmConf->is_master, dsmConf->master_ip, dsmConf->master_port, dsmConf->worker_ip, dsmConf->worker_port,
        (unsigned long long)dsmConf->size, dsmConf->cache_th);

    return 0;
}