/**
 * @file context.h
 * @author Wang Bo
 * @date 2025-09-13
 * @brief Global runtime context for LeoPar system.
 *
 * This header centralizes cluster and runtime configuration so that
 * all runtime components (leopar.c, ucx_tcp.c, scheduler, sync, etc.)
 * can access global state via g_ctx.
 */

#ifndef CONTEXT_H
#define CONTEXT_H

#include "tcp.h"
#include "ucx.h"

/* Global runtime context */
typedef struct {
    int world_size;       /* number of ranks */
    int rank;             /* my rank */
    char **rank_ips;      /* array of IP addresses, indexed by rank */
    int *rank_threads;    /* number of threads per rank */
    int default_threads;  /* default threads per process */

    tcp_config_t tcp_cfg; /* UCX TCP config */
    ucx_context_t ucx_ctx;    /* embedded UCX context */
} leopar_context_t;


/* The global context (defined in context.c) */
extern leopar_context_t g_ctx;

/* Load cluster.ini into g_ctx */
int load_config(const char *filename, int my_rank);

#endif /* CONTEXT_H */
 