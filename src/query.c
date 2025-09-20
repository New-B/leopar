/**
 * @file runtime_query.c
 * @author …
 * @date 2025-09-20
 * @brief Runtime query helpers and lightweight stats.
 */

#include "leopar.h"
#include "context.h"
#include "log.h"

#include <string.h>

static leo_stats_t g_stats;

int leo_world_size(void) { return g_ctx.world_size; }
int leo_rank(void)       { return g_ctx.rank; }

int leo_stats_get(leo_stats_t *s)
{
    if (!s) return -1;
    *s = g_stats;
    return 0;
}

int leo_set_log_level(int lvl)
{
    /* Wire to your existing logging system if it exposes setter; else return 0. */
    (void)lvl;
    return 0;
}

/* Optionally, you can add small inline hooks in your create/join paths:
    g_stats.creates_sent++;
    g_stats.joins_sent++;
    etc.
    Or expose functions here to be called from those sites. */
