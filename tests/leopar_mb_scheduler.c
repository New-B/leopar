#define _POSIX_C_SOURCE 200809L
#include "leopar.h"
#include "tid.h"

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

typedef struct {
    int id;
    int sleep_ms;
    int preferred_rank;
} sched_arg_t;

static void* sleeper_worker(void *vp)
{
    sched_arg_t a = *(sched_arg_t*)vp;
    free(vp);
    usleep((useconds_t)a.sleep_ms * 1000u);
    return NULL;
}

static double wall_us(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec * 1e6 + (double)ts.tv_nsec / 1e3;
}

static void print_stats_snapshot(const char *label)
{
    leo_stats_t s;
    if (leo_stats_get(&s) != 0) return;

    printf("[%s] creates_sent=%" PRIu64 " creates_recv=%" PRIu64
           " joins_sent=%" PRIu64 " joins_recv=%" PRIu64
           " ctrl_tx=%" PRIu64 " ctrl_rx=%" PRIu64 "\n",
           label, s.creates_sent, s.creates_recv, s.joins_sent, s.joins_recv,
           s.bytes_ctrl_tx, s.bytes_ctrl_rx);

    printf("[%s] sched_rr=%" PRIu64 " sched_hint=%" PRIu64
           " locality_used=%" PRIu64 " hit=%" PRIu64
           " miss=%" PRIu64 " load_escape=%" PRIu64 "\n",
           label, s.scheduler_rr, s.scheduler_hint, s.scheduler_locality,
           s.scheduler_locality_hit, s.scheduler_locality_miss,
           s.scheduler_load_escape);
}

static void print_csv_phase(const char *label,
                            int tasks,
                            int sleep_ms,
                            int hits,
                            int total_pref,
                            double phase_us,
                            const int *placed,
                            int world)
{
    leo_stats_t s;
    if (leo_stats_get(&s) != 0) return;

    const double hit_rate = (total_pref > 0)
        ? (100.0 * (double)hits / (double)total_pref)
        : 0.0;

    printf("CSV_PHASE,%s,%d,%d,%.2f,%d,%d,%" PRIu64 ",%" PRIu64 ",%" PRIu64 ",%" PRIu64
           ",%" PRIu64 ",%" PRIu64 ",%" PRIu64 ",%" PRIu64 ",%" PRIu64
           ",%" PRIu64 ",%" PRIu64 ",%" PRIu64 "\n",
           label, tasks, sleep_ms, phase_us, hits, total_pref,
           s.creates_sent, s.creates_recv, s.joins_sent, s.joins_recv,
           s.bytes_ctrl_tx, s.bytes_ctrl_rx, s.scheduler_rr, s.scheduler_hint,
           s.scheduler_locality, s.scheduler_locality_hit,
           s.scheduler_locality_miss, s.scheduler_load_escape);

    for (int r = 0; r < world; ++r) {
        printf("CSV_PLACE,%s,%d,%d,%.2f\n", label, r, placed[r], hit_rate);
    }
}

static void run_phase(const char *label, int world, int tasks, int sleep_ms, int mode)
{
    leo_thread_t *tids = calloc((size_t)tasks, sizeof(*tids));
    int *placed = calloc((size_t)world, sizeof(*placed));
    int hits = 0;
    int pref_total = 0;
    const double t0 = wall_us();

    if (!tids || !placed) {
        fprintf(stderr, "alloc failed in phase %s\n", label);
        free(tids);
        free(placed);
        return;
    }

    leo_stats_reset();

    for (int i = 0; i < tasks; ++i) {
        sched_arg_t *a = malloc(sizeof(*a));
        if (!a) break;
        a->id = i;
        a->sleep_ms = sleep_ms;

        leo_attr_t attr;
        leo_attr_init(&attr);
        attr.target_rank = -1;

        if (mode == 0) {
            a->preferred_rank = -1;
            attr.locality_key = UINT64_MAX;
        } else if (mode == 1) {
            a->preferred_rank = (world > 1) ? 1 : 0;
            attr.locality_key = (uint64_t)a->preferred_rank;
        } else {
            a->preferred_rank = i % world;
            attr.locality_key = (uint64_t)a->preferred_rank;
        }

        if (leo_thread_create_copy_attr(&tids[i], &attr, sleeper_worker, a) != 0) {
            fprintf(stderr, "create failed in phase %s at task %d\n", label, i);
            free(a);
            tasks = i;
            break;
        }

        int chosen = LEO_TID_RANK(tids[i]);
        if (chosen >= 0 && chosen < world) placed[chosen]++;
        if (a->preferred_rank >= 0) {
            pref_total++;
            if (chosen == a->preferred_rank) hits++;
        }
        free(a);
    }

    for (int i = 0; i < tasks; ++i) {
        (void)leo_thread_join(tids[i], NULL);
    }
    const double phase_us = wall_us() - t0;

    printf("\n=== %s ===\n", label);
    printf("phase_us=%.2f\n", phase_us);
    printf("placements:");
    for (int r = 0; r < world; ++r) {
        printf(" r%d=%d", r, placed[r]);
    }
    printf("\n");

    if (mode != 0) {
        printf("locality_hit_rate=%.2f%% (%d/%d)\n",
               pref_total > 0 ? (100.0 * (double)hits / (double)pref_total) : 0.0,
               hits, pref_total);
    }

    print_stats_snapshot(label);
    print_csv_phase(label, tasks, sleep_ms, hits, pref_total, phase_us, placed, world);

    free(tids);
    free(placed);
}

int main(int argc, char **argv)
{
    if (argc < 3 || argc > 5) {
        fprintf(stderr, "Usage: %s <config_path> <rank> [tasks=32] [sleep_ms=200]\n", argv[0]);
        return 1;
    }

    const char *cfg = argv[1];
    int rank = atoi(argv[2]);
    int tasks = (argc >= 4) ? atoi(argv[3]) : 32;
    int sleep_ms = (argc >= 5) ? atoi(argv[4]) : 200;

    if (leopar_init(cfg, rank, NULL) != 0) {
        fprintf(stderr, "leopar_init failed\n");
        return 2;
    }

    int world = leo_world_size();
    if (rank == 0) {
        printf("scheduler microbenchmark: world=%d tasks=%d sleep_ms=%d\n",
               world, tasks, sleep_ms);
        printf("CSV_HEADER,phase,tasks,sleep_ms,phase_us,locality_hits,locality_total,"
               "creates_sent,creates_recv,joins_sent,joins_recv,ctrl_tx,ctrl_rx,"
               "sched_rr,sched_hint,locality_used,locality_hit,locality_miss,load_escape\n");
        printf("CSV_PLACE_HEADER,phase,rank,placements,hit_rate\n");
        run_phase("auto_rr_baseline", world, tasks, sleep_ms, 0);
        run_phase("hotspot_locality", world, tasks, sleep_ms, 1);
        run_phase("striped_locality", world, tasks, sleep_ms, 2);
    }

    leopar_finalize();
    return 0;
}
