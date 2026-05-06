#define _POSIX_C_SOURCE 200809L
#include "leopar.h"
#include "dsm_c_api.h"
#include "tid.h"

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#ifndef IRREG_MAX_RANKS
#define IRREG_MAX_RANKS 32
#endif

typedef struct {
    int world;
    int region_len;
    int iters;
    int remote_period;
    int dominant_rank;
    int slot;
    GAddr in_shard[IRREG_MAX_RANKS];
    GAddr out_shard[IRREG_MAX_RANKS];
} irregular_arg_t;

static inline double wall_us(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec * 1e6 + (double)ts.tv_nsec / 1e3;
}

static inline int gam_node_id_from_rank(int r) { return r + 1; }

static inline GAddr addr_elem(GAddr base, int idx)
{
    return base + (GAddr)((size_t)idx * sizeof(double));
}

static void fill_input(double *buf, int n, int rank_seed)
{
    for (int i = 0; i < n; ++i) {
        buf[i] = (double)(rank_seed * 100000 + i);
    }
}

static int choose_remote_region(int dominant, int world, int wave)
{
    if (world <= 1) return dominant;
    return (dominant + 1 + (wave % (world - 1))) % world;
}

static void* irregular_worker(void *vp)
{
    irregular_arg_t a = *(irregular_arg_t*)vp;
    free(vp);

    double sum = 0.0;
    for (int it = 0; it < a.iters; ++it) {
        int region = a.dominant_rank;
        if (a.remote_period > 0 && (it % a.remote_period) == 0) {
            region = choose_remote_region(a.dominant_rank, a.world, (it / a.remote_period) + a.slot);
        }

        const int idx = (a.slot * 131 + it * 17) % a.region_len;
        double v = 0.0;
        (void)dsm_read_c(addr_elem(a.in_shard[region], idx), &v, sizeof(v));
        sum += v * 1.0000001;
    }

    (void)dsm_write_c(addr_elem(a.out_shard[a.dominant_rank], a.slot), &sum, sizeof(sum));
    return NULL;
}

static void print_scheduler_summary(const char *label,
                                    int world,
                                    int tasks,
                                    int hits,
                                    int total_pref,
                                    const int *placed,
                                    double phase_us)
{
    leo_stats_t s;
    if (leo_stats_get(&s) != 0) return;

    const double hit_rate = (total_pref > 0)
        ? (100.0 * (double)hits / (double)total_pref)
        : 0.0;

    printf("\n=== %s ===\n", label);
    printf("phase_us=%.2f locality_hit_rate=%.2f%% (%d/%d)\n",
           phase_us, hit_rate, hits, total_pref);
    printf("placements:");
    for (int r = 0; r < world; ++r) {
        printf(" r%d=%d", r, placed[r]);
    }
    printf("\n");

    printf("[scheduler] creates_sent=%" PRIu64 " creates_recv=%" PRIu64
           " joins_sent=%" PRIu64 " joins_recv=%" PRIu64
           " ctrl_tx=%" PRIu64 " ctrl_rx=%" PRIu64 "\n",
           s.creates_sent, s.creates_recv, s.joins_sent, s.joins_recv,
           s.bytes_ctrl_tx, s.bytes_ctrl_rx);
    printf("[scheduler] hint=%" PRIu64 " locality=%" PRIu64
           " hit=%" PRIu64 " miss=%" PRIu64 " load_escape=%" PRIu64 "\n",
           s.scheduler_hint, s.scheduler_locality, s.scheduler_locality_hit,
           s.scheduler_locality_miss, s.scheduler_load_escape);

    printf("CSV_IRREG_SUMMARY,%s,%d,%.2f,%d,%d,%" PRIu64 ",%" PRIu64
           ",%" PRIu64 ",%" PRIu64 ",%" PRIu64 ",%" PRIu64
           ",%" PRIu64 ",%" PRIu64 ",%" PRIu64 ",%" PRIu64 ",%" PRIu64 "\n",
           label, tasks, phase_us, hits, total_pref,
           s.creates_sent, s.creates_recv, s.joins_sent, s.joins_recv,
           s.bytes_ctrl_tx, s.bytes_ctrl_rx, s.scheduler_hint,
           s.scheduler_locality, s.scheduler_locality_hit,
           s.scheduler_locality_miss, s.scheduler_load_escape);
    for (int r = 0; r < world; ++r) {
        printf("CSV_IRREG_PLACE,%s,%d,%d,%.2f\n", label, r, placed[r], hit_rate);
    }
}

static int build_regions(int world, int region_len, int tasks_per_region,
                         GAddr *in_shard, GAddr *out_shard)
{
    double *tmp_in = malloc((size_t)region_len * sizeof(double));
    double *tmp_out = calloc((size_t)tasks_per_region, sizeof(double));
    if (!tmp_in || !tmp_out) {
        free(tmp_in);
        free(tmp_out);
        return -1;
    }

    for (int r = 0; r < world; ++r) {
        const int owner = gam_node_id_from_rank(r);
        in_shard[r] = dsm_malloc_c((Size)((size_t)region_len * sizeof(double)), owner);
        out_shard[r] = dsm_malloc_c((Size)((size_t)tasks_per_region * sizeof(double)), owner);
        if (!in_shard[r] || !out_shard[r]) {
            free(tmp_in);
            free(tmp_out);
            return -1;
        }

        fill_input(tmp_in, region_len, r + 1);
        if (dsm_write_c(in_shard[r], tmp_in, (Size)((size_t)region_len * sizeof(double))) != 0) {
            free(tmp_in);
            free(tmp_out);
            return -1;
        }
        if (dsm_write_c(out_shard[r], tmp_out, (Size)((size_t)tasks_per_region * sizeof(double))) != 0) {
            free(tmp_in);
            free(tmp_out);
            return -1;
        }
    }

    free(tmp_in);
    free(tmp_out);
    return 0;
}

int main(int argc, char **argv)
{
    if (argc < 3 || argc > 7) {
        fprintf(stderr,
                "Usage: %s <config_path> <rank> [tasks_per_region=4] [iters=256] [remote_period=4] [region_len=4096]\n",
                argv[0]);
        return 1;
    }

    const char *cfg = argv[1];
    const int rank = atoi(argv[2]);
    const int tasks_per_region = (argc >= 4) ? atoi(argv[3]) : 4;
    const int iters = (argc >= 5) ? atoi(argv[4]) : 256;
    const int remote_period = (argc >= 6) ? atoi(argv[5]) : 4;
    const int region_len = (argc >= 7) ? atoi(argv[6]) : 4096;

    if (leopar_init(cfg, rank, NULL) != 0) {
        fprintf(stderr, "leopar_init failed\n");
        return 2;
    }

    const int world = leo_world_size();
    if (world <= 0 || world > IRREG_MAX_RANKS) {
        fprintf(stderr, "unsupported world=%d (max=%d)\n", world, IRREG_MAX_RANKS);
        leopar_finalize();
        return 3;
    }

    GAddr in_shard[IRREG_MAX_RANKS] = {0};
    GAddr out_shard[IRREG_MAX_RANKS] = {0};

    if (rank == 0) {
        if (build_regions(world, region_len, tasks_per_region, in_shard, out_shard) != 0) {
            fprintf(stderr, "build_regions failed\n");
            leopar_finalize();
            return 4;
        }

        const int tasks = world * tasks_per_region;
        leo_thread_t *tids = calloc((size_t)tasks, sizeof(*tids));
        int *placed = calloc((size_t)world, sizeof(*placed));
        int hits = 0;
        int launched = 0;

        if (!tids || !placed) {
            fprintf(stderr, "alloc failed for launch state\n");
            free(tids);
            free(placed);
            leopar_finalize();
            return 5;
        }

        (void)leo_stats_reset();
        const double t0 = wall_us();

        for (int dominant = 0; dominant < world; ++dominant) {
            for (int slot = 0; slot < tasks_per_region; ++slot) {
                irregular_arg_t *a = malloc(sizeof(*a));
                if (!a) continue;
                memset(a, 0, sizeof(*a));
                a->world = world;
                a->region_len = region_len;
                a->iters = iters;
                a->remote_period = remote_period;
                a->dominant_rank = dominant;
                a->slot = slot;
                memcpy(a->in_shard, in_shard, (size_t)world * sizeof(GAddr));
                memcpy(a->out_shard, out_shard, (size_t)world * sizeof(GAddr));

                leo_attr_t attr;
                leo_attr_init(&attr);
                attr.target_rank = -1;
                attr.locality_key = (uint64_t)dominant;

                int rc = leo_thread_create_copy_attr(&tids[launched], &attr, irregular_worker, a);
                if (rc != 0) {
                    fprintf(stderr, "create failed dominant=%d slot=%d rc=%d\n", dominant, slot, rc);
                    free(a);
                    continue;
                }

                const int chosen = LEO_TID_RANK(tids[launched]);
                if (chosen >= 0 && chosen < world) placed[chosen]++;
                if (chosen == dominant) hits++;
                launched++;
                free(a);
            }
        }

        for (int i = 0; i < launched; ++i) {
            (void)leo_thread_join(tids[i], NULL);
        }

        const double phase_us = wall_us() - t0;
        print_scheduler_summary("irregular_kernel", world, launched, hits, launched, placed, phase_us);

        free(tids);
        free(placed);
    }

    (void)leo_barrier();
    leopar_finalize();
    return 0;
}
