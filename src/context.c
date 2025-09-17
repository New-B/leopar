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

    if (ip_count != g_ctx.world_size) {
        log_warn("Parsed %d IPs but world_size=%d\n", ip_count, g_ctx.world_size);
    }
    
    log_info("Config loaded: rank=%d world_size=%d base_port=%d default_threads=%d",
            g_ctx.rank, g_ctx.world_size, g_ctx.tcp_cfg.base_port, g_ctx.default_threads);

    return 0;
}