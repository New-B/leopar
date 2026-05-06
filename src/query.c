/**
 * @file runtime_query.c
 * @author …
 * @date 2025-09-20
 * @brief Runtime query helpers and lightweight stats.
 */

#include "leopar.h"
#include "context.h"
#include "log.h"

#include <stdatomic.h>
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

int leo_stats_reset(void)
{
    memset(&g_stats, 0, sizeof(g_stats));
    return 0;
}

int leo_set_log_level(int lvl)
{
    /* Wire to your existing logging system if it exposes setter; else return 0. */
    (void)lvl;
    return 0;
}

void leo_stats_note_create_sent(void)           { g_stats.creates_sent++; }
void leo_stats_note_create_recv(void)           { g_stats.creates_recv++; }
void leo_stats_note_join_sent(void)             { g_stats.joins_sent++; }
void leo_stats_note_join_recv(void)             { g_stats.joins_recv++; }
void leo_stats_note_ctrl_tx(size_t bytes)       { g_stats.bytes_ctrl_tx += bytes; }
void leo_stats_note_ctrl_rx(size_t bytes)       { g_stats.bytes_ctrl_rx += bytes; }
void leo_stats_note_scheduler_rr(void)          { g_stats.scheduler_rr++; }
void leo_stats_note_scheduler_hint(void)        { g_stats.scheduler_hint++; }
void leo_stats_note_scheduler_locality(void)    { g_stats.scheduler_locality++; }
void leo_stats_note_scheduler_locality_hit(void){ g_stats.scheduler_locality_hit++; }
void leo_stats_note_scheduler_locality_miss(void){ g_stats.scheduler_locality_miss++; }
void leo_stats_note_scheduler_load_escape(void) { g_stats.scheduler_load_escape++; }
