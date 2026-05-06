/**
 * @file scheduler.c
 * @author Wang Bo
 * @date 2025-09-13
 * @brief Placement policies for pinned execution in LeoPar.
 */

#include "scheduler.h"
#include "query.h"

#include <stdatomic.h>
#include <stdint.h>

#ifndef SCHED_MAX_RANKS
#define SCHED_MAX_RANKS 8192
#endif

/* Per-process cached load view.
 * The creator increments on successful placement and decrements when it
 * observes task completion (locally or via EXIT_NOTIFY). */
static atomic_int g_est_load[SCHED_MAX_RANKS];
static atomic_int g_next_rr = 0;

static int clamp_rank(int rank, int world_size)
{
    if (world_size <= 0) return 0;
    if (rank < 0) return 0;
    if (rank >= world_size) return rank % world_size;
    return rank;
}

int scheduler_locality_home_rank(uint64_t locality_key, int world_size)
{
    if (world_size <= 0 || locality_key == UINT64_MAX) return -1;
    return (int)(locality_key % (uint64_t)world_size);
}

static int pick_least_loaded(int world_size, int rr_seed)
{
    int best_rank = 0;
    int best_load = atomic_load(&g_est_load[0]);

    for (int step = 0; step < world_size; ++step) {
        const int r = (rr_seed + step) % world_size;
        const int load = atomic_load(&g_est_load[r]);
        if (step == 0 || load < best_load) {
            best_load = load;
            best_rank = r;
        }
    }
    return best_rank;
}

int scheduler_choose_rank(int world_size)
{
    leo_stats_note_scheduler_rr();
    return scheduler_choose_rank_hint(world_size, UINT64_MAX, 0);
}

int scheduler_choose_rank_hint(int world_size, uint64_t locality_key, int priority)
{
    if (world_size <= 0) return 0;
    if (world_size > SCHED_MAX_RANKS) world_size = SCHED_MAX_RANKS;

    const int rr_seed = atomic_fetch_add(&g_next_rr, 1);
    const int least_loaded = pick_least_loaded(world_size, rr_seed);
    const int preferred = scheduler_locality_home_rank(locality_key, world_size);

    if (preferred < 0) {
        leo_stats_note_scheduler_hint();
        return least_loaded;
    }

    leo_stats_note_scheduler_locality();
    const int pref_load = atomic_load(&g_est_load[preferred]);
    const int min_load = atomic_load(&g_est_load[least_loaded]);

    /* Allow a small imbalance to preserve locality.
     * Higher priority tolerates slightly more imbalance before escaping. */
    int escape_margin = 1;
    if (priority > 0) {
        escape_margin += (priority > 3) ? 3 : priority;
    }

    if (pref_load <= min_load + escape_margin) {
        leo_stats_note_scheduler_locality_hit();
        return preferred;
    }
    leo_stats_note_scheduler_locality_miss();
    leo_stats_note_scheduler_load_escape();
    return least_loaded;
}

void scheduler_note_placement(int rank)
{
    if (rank < 0 || rank >= SCHED_MAX_RANKS) return;
    atomic_fetch_add(&g_est_load[rank], 1);
}

void scheduler_note_completion(int rank)
{
    if (rank < 0 || rank >= SCHED_MAX_RANKS) return;

    int cur = atomic_load(&g_est_load[rank]);
    while (cur > 0 &&
           !atomic_compare_exchange_weak(&g_est_load[rank], &cur, cur - 1)) {
        /* retry until success or observed zero */
    }
}

int scheduler_estimated_load(int rank)
{
    if (rank < 0 || rank >= SCHED_MAX_RANKS) return -1;
    return atomic_load(&g_est_load[rank]);
}
