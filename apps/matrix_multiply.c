// matrix_multiply.c
// Build example:
//   gcc -O2 -Wall -Wextra -rdynamic matrix_multiply.c -o mm \
//       -lpthread   /* plus your leopar / dsm / ucx libs as needed */
//
// Run example (per rank):
//   ./mm <config_path> <rank> <M> <threads_per_node> [log_path]
//   e.g. ./mm cluster.json 0 2048 8 rank0.log
//
// Matrix: C[MxN] = A[MxK] * B[KxN]
// We set N=K=M for square cases (1024/2048/4096), easy to extend.

#define _POSIX_C_SOURCE 200809L
#include "leopar.h"
#include "dsm_c_api.h"
#include "ctrl.h"     // ctrl_barrier(name, gen, timeout_ms) if available here
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <inttypes.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <errno.h>

#ifndef MAX_RANKS
#define MAX_RANKS 128
#endif

#ifndef BTILE
#define BTILE 256      /* column tile of B (and C output tile) */
#endif

/* -------- small helpers -------- */
static inline double wall_us(void) {
    struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec*1e6 + (double)ts.tv_nsec/1e3;
}
static inline int min_int(int a, int b) { return a < b ? a : b; }

/* -------- Global registry (published in DSM by rank0) -------- */
typedef struct Registry_ {
    int M, N, K;
    int world_size;
    int threads_per_node;

    /* row partition per rank: [row0[r], row0[r]+rows[r]) */
    int row0_of_rank[MAX_RANKS];
    int rows_of_rank[MAX_RANKS];

    /* DSM shards */
    GAddr A_shard[MAX_RANKS];   /* A rows owned by rank r (rows[r] x K) */
    GAddr C_shard[MAX_RANKS];   /* C rows owned by rank r (rows[r] x N) */
    GAddr B_full;               /* B is stored as one global block (K x N) */

    volatile int ready_flag;    /* 1 when registry filled */
} Registry;

static GAddr g_registry_gaddr = 0;
static Registry g_reg;          /* local cached copy */

/* -------- DSM helpers for row-striped shards -------- */

/* Returns start row and rows count for a rank r */
static void rows_of_rank(int M, int world, int r, int* row0, int* rows) {
    int base = M / world;
    int rem  = M % world;
    int first = r * base + (r < rem ? r : rem);
    int count = base + (r < rem ? 1 : 0);
    *row0 = first;
    *rows = count;
}

/* DSM address helpers: compute address within a row-striped shard (rank r) */
static inline GAddr addr_A_row(const Registry* R, int r, int local_i, int k, size_t elem_sz) {
    /* A_shard[r] is laid out row-major: rows_of_rank[r] x K */
    return R->A_shard[r] + (GAddr)((size_t)local_i * (size_t)R->K + (size_t)k) * elem_sz;
}
static inline GAddr addr_C_row(const Registry* R, int r, int local_i, int j, size_t elem_sz) {
    /* C_shard[r] is laid out row-major: rows_of_rank[r] x N */
    return R->C_shard[r] + (GAddr)((size_t)local_i * (size_t)R->N + (size_t)j) * elem_sz;
}
static inline GAddr addr_B(const Registry* R, int k, int j, size_t elem_sz) {
    /* B_full is K x N row-major */
    return R->B_full + (GAddr)((size_t)k * (size_t)R->N + (size_t)j) * elem_sz;
}

/* -------- Worker argument passed via LeoPar -------- */
typedef struct {
    int my_rank;          /* target rank that runs this worker */
    int world_size;
    int threads_per_node;

    int M, N, K;          /* matrix dims (square if you pass same M) */

    int owner_row0;       /* first global row owned by my_rank */
    int owner_rows;       /* rows owned by my_rank */
    int my_sub_row0;      /* my sub-range within owner's rows (global index) */
    int my_sub_rows;      /* size of my sub-range */

    GAddr A_shard_r;      /* local owner's A shard */
    GAddr C_shard_r;      /* local owner's C shard */
    GAddr B_full;         /* global B block */

    int btile;            /* B-Column tile size */
} MatmulArg;

/* -------- Worker function running on remote ranks --------
 * Each worker computes rows [my_sub_row0, my_sub_row0+my_sub_rows)
 * for output C, using DSM reads for A (from A_shard_r) and B (B_full),
 * then DSM writes into C_shard_r. We use column tiles of B (size = btile).
 */
void* worker_matmul(void* arg_) {
    MatmulArg a = *(MatmulArg*)arg_;
    free(arg_);

    const size_t SZ = sizeof(double);
    const int M = a.M, N=a.N, K=a.K;
    const int row0 = a.owner_row0;                 /* global first row of owner */
    const int sub0 = a.my_sub_row0;                /* global row start of this worker */
    const int subN = a.my_sub_rows;                /* rows to compute */
    const int local_base = sub0 - row0;            /* local index base within the shard */

    /* Buffers: a single A row, B tile, and C tile row */
    double* Arow = (double*)malloc((size_t)K * SZ);
    double* Btile = (double*)malloc((size_t)K * (size_t)a.btile * SZ);
    double* Crow  = (double*)malloc((size_t)a.btile * SZ);
    if (!Arow || !Btile || !Crow) {
        // If OOM, we abort this worker
        fprintf(stderr, "[rank %d worker] OOM in buffers\n", a.my_rank);
        return NULL;
    }

    /* For each output row i in my sub-range */
    for (int gi = sub0; gi < sub0 + subN; ++gi) {
        const int li = gi - row0; /* local row index in owner's shard */

        /* Read A row into Arow[] */
        GAddr baseA = a.A_shard_r + (GAddr)((size_t)li * (size_t)K) * SZ;
        dsm_read_c(baseA, Arow, (Size)((size_t)K * SZ));

        /* Process C(i, :) in column tiles of B */
        for (int j0 = 0; j0 < N; j0 += a.btile) {
            const int jw = min_int(a.btile, N - j0);

            /* Read B tile: rows 0..K-1, cols j0..j0+jw-1 into Btile (row-major by K) */
            for (int kk = 0; kk < K; ++kk) {
                GAddr g = addr_B((Registry*)&a, kk, j0, SZ); /* reuse helper shape */
                dsm_read_c(g, &Btile[(size_t)kk * (size_t)a.btile], (Size)((size_t)jw * SZ));
            }

            /* Compute Crow[j] = sum_k Arow[k] * B[k, j] over this tile */
            memset(Crow, 0, (size_t)jw * SZ);
            for (int kk = 0; kk < K; ++kk) {
                const double aik = Arow[kk];
                const double* Brow = &Btile[(size_t)kk * (size_t)a.btile];
                for (int jj = 0; jj < jw; ++jj) {
                    Crow[jj] += aik * Brow[jj];
                }
            }

            /* Write back C row-tile */
            GAddr baseC = a.C_shard_r + (GAddr)((size_t)li * (size_t)N + (size_t)j0) * SZ;
            dsm_write_c(baseC, Crow, (Size)((size_t)jw * SZ));
        }
    }

    free(Arow); free(Btile); free(Crow);
    return NULL;
}

/* -------- Rank0: publish registry to DSM -------- */
static int publish_registry(int M, int N, int K, int world, int threads_per_node,
                            GAddr* out_reg_gaddr) {
    Registry R; memset(&R, 0, sizeof(R));
    R.M = M; R.N = N; R.K = K;
    R.world_size = world;
    R.threads_per_node = threads_per_node;

    /* Row partition for A/C owners */
    for (int r=0; r<world; ++r) {
        rows_of_rank(M, world, r, &R.row0_of_rank[r], &R.rows_of_rank[r]);

        /* Allocate A shard (rows[r] x K) placed on owner rank r */
        Size sa = (Size)((size_t)R.rows_of_rank[r] * (size_t)K * sizeof(double));
        R.A_shard[r] = dsm_malloc_c(sa, (Node)r);

        /* Allocate C shard (rows[r] x N) placed on owner rank r */
        Size sc = (Size)((size_t)R.rows_of_rank[r] * (size_t)N * sizeof(double));
        R.C_shard[r] = dsm_malloc_c(sc, (Node)r);
    }

    /* Allocate B as one global block (K x N). For better balance, you can:
       - replicate B per rank, or
       - split B by column blocks across ranks.
       Here we keep it simple: one contiguous block. */
    Size sb = (Size)((size_t)K * (size_t)N * sizeof(double));
    R.B_full = dsm_malloc_c(sb, (Node)0);

    R.ready_flag = 1;

    /* Allocate registry object in DSM and write it */
    GAddr reg = dsm_malloc_c((Size)sizeof(Registry), (Node)0);
    if (!reg) return -1;
    dsm_write_c(reg, &R, (Size)sizeof(R));

    *out_reg_gaddr = reg;
    return 0;
}

/* -------- Rank0: fill A, B with random doubles and write to DSM -------- */
static void fill_random_matrixes_rank0(int M, int N, int K, const Registry* R) {
    /* Simple deterministic RNG for reproducibility */
    srand(12345);

    /* Fill A shards row-striped */
    for (int r=0; r<R->world_size; ++r) {
        const int row0 = R->row0_of_rank[r];
        const int rows = R->rows_of_rank[r];
        /* stream rows */
        double* rowbuf = (double*)malloc((size_t)K * sizeof(double));
        for (int i=0; i<rows; ++i) {
            for (int k=0; k<K; ++k) {
                rowbuf[k] = (double)rand() / (double)RAND_MAX - 0.5; /* [-0.5, 0.5] */
            }
            GAddr g = R->A_shard[r] + (GAddr)((size_t)i * (size_t)K) * sizeof(double);
            dsm_write_c(g, rowbuf, (Size)((size_t)K * sizeof(double)));
        }
        free(rowbuf);
    }

    /* Fill B global block (K x N). We write row by row for locality */
    double* brow = (double*)malloc((size_t)N * sizeof(double));
    for (int k=0; k<K; ++k) {
        for (int j=0; j<N; ++j) {
            brow[j] = (double)rand() / (double)RAND_MAX - 0.5;
        }
        GAddr g = R->B_full + (GAddr)((size_t)k * (size_t)N) * sizeof(double);
        dsm_write_c(g, brow, (Size)((size_t)N * sizeof(double)));
    }
    free(brow);

    /* Clear C shards (optional, workers will overwrite) */
    for (int r=0; r<R->world_size; ++r) {
        const int rows = R->rows_of_rank[r];
        Size sc = (Size)((size_t)rows * (size_t)N * sizeof(double));
        // You can skip zeroing if worker overwrites all cells.
        // Here we skip to save time.
        (void)sc;
    }
}

/* -------- Launch LeoPar workers: threads_per_node per rank -------- */
static void launch_workers_and_wait(int world, int threads_per_node, const Registry* R) {
    const int total_workers = world * threads_per_node;
    leo_thread_t *ths = (leo_thread_t*)malloc(sizeof(leo_thread_t) * (size_t)total_workers);

    /* Partition each owner's row range into threads_per_node chunks */
    int idx = 0;
    for (int r=0; r<world; ++r) {
        const int row0 = R->row0_of_rank[r];
        const int rows = R->rows_of_rank[r];
        int base = rows / threads_per_node;
        int rem  = rows % threads_per_node;

        int cursor = row0;
        for (int t=0; t<threads_per_node; ++t) {
            int take = base + (t < rem ? 1 : 0);
            if (take == 0) { /* more threads than rows: skip empty tasks */
                // Create a dummy task with 0 rows? Just skip creation.
                continue;
            }

            MatmulArg* a = (MatmulArg*)malloc(sizeof(*a));
            a->my_rank = r;
            a->world_size = world;
            a->threads_per_node = threads_per_node;
            a->M = R->M; a->N=R->N; a->K=R->K;

            a->owner_row0 = row0;
            a->owner_rows = rows;
            a->my_sub_row0 = cursor;
            a->my_sub_rows = take;

            a->A_shard_r = R->A_shard[r];
            a->C_shard_r = R->C_shard[r];
            a->B_full    = R->B_full;

            a->btile = BTILE;

            int rc = leo_thread_create(&ths[idx], NULL, worker_matmul, a, r);
            if (rc != 0) {
                fprintf(stderr, "create worker failed for rank=%d (rc=%d)\n", r, rc);
                free(a);
            } else {
                idx++;
            }
            cursor += take;
        }
    }
    const int launched = idx;

    /* Join all launched workers */
    for (int i=0; i<launched; ++i) {
        leo_thread_join(ths[i], NULL);
    }
    free(ths);
}

/* -------- main -------- */
int main(int argc, char** argv) {
    if (argc < 5 || argc > 6) {
        fprintf(stderr, "Usage: %s <config_path> <rank> <M:1024|2048|4096> <threads_per_node:2|4|8|16|28> [log_path]\n", argv[0]);
        return 1;
    }
    const char* cfg = argv[1];
    int rank = atoi(argv[2]);
    int M = atoi(argv[3]);
    int threads_per_node = atoi(argv[4]);
    const char* log_path = (argc == 6) ? argv[5] : NULL;

    /* We use square matrices: MxM */
    int N = M, K = M;

    if (leopar_init(cfg, rank, log_path) != 0) {
        fprintf(stderr, "[rank %d] leopar_init failed\n", rank);
        return 1;
    }
    dsm_init_c(NULL);

    int world = leo_world_size();
    if (world <= 0 || world > MAX_RANKS) {
        fprintf(stderr, "[rank %d] invalid world_size=%d\n", rank, world);
        goto OUT;
    }

    /* Rank0 publishes registry, data; others wait for ready */
    if (rank == 0) {
        if (publish_registry(M, N, K, world, threads_per_node, &g_registry_gaddr) != 0) {
            fprintf(stderr, "[rank 0] publish_registry failed\n");
            goto OUT;
        }
        /* read back registry into local cache */
        dsm_read_c(g_registry_gaddr, &g_reg, (Size)sizeof(g_reg));

        /* Fill A,B into DSM */
        fill_random_matrixes_rank0(M, N, K, &g_reg);

        /* Barrier: data ready */
        ctrl_barrier("dataset_ready", 0, 60000);
    } else {
        /* Discover registry address: rank0 must communicate it.
           For simplicity here we assume registry GAddr is at same known place.
           If not, you can pass it via a small control message or config.
           In this example we assume rank0 wrote g_registry_gaddr into a known slot,
           or compiled scenario sets it (adjust as per your system).
         */
        // In a real system, you'd have: g_registry_gaddr = discover_from_ctrl();
        // For demo, we busy-wait until rank0 writes ready registry.
        // If you have a way to obtain g_registry_gaddr, assign it here.
        // ---- BEGIN: simple discovery (optional path) ----
        // If your DSM offers a "well-known symbol" or "bootstrap slot", use it.
        // Else, you may hard-fail here and wire it in from your control plane.
        fprintf(stderr, "[rank %d] ERROR: please wire g_registry_gaddr discovery from ctrl plane.\n", rank);
        // Exit to avoid hanging; replace with real discovery in your system.
        goto OUT;
        // ---- END ----

        // while (1) {
        //     dsm_read(g_registry_gaddr, &g_reg, (Size)sizeof(g_reg));
        //     if (g_reg.ready_flag) break;
        //     usleep(1000);
        // }
        // ctrl_barrier("dataset_ready", 0, 60000);
    }

    /* Everyone should have g_reg cached here (rank0 already did). */
    if (rank == 0) {
        /* Start timing */
        double t0 = wall_us();

        /* Launch workers and join them */
        launch_workers_and_wait(world, threads_per_node, &g_reg);

        double t1 = wall_us();
        printf("[rank 0] M=%d N=%d K=%d world=%d thr/node=%d  total_time=%.3f ms\n",
               M, N, K, world, threads_per_node, (t1 - t0)/1000.0);

        /* Final barrier so others can safely stop */
        ctrl_barrier("matmul_done", 1, 60000);

        /* Optional: free DSM data */
        for (int r=0; r<world; ++r) {
            dsm_free_c(g_reg.A_shard[r]);
            dsm_free_c(g_reg.C_shard[r]);
        }
        dsm_free_c(g_reg.B_full);
        dsm_free_c(g_registry_gaddr);
    } else {
        /* Non-zero ranks:
         * Your LeoPar dispatcher thread is already running because leopar_init()
         * started it (per your runtime); workers will be created remotely by rank0.
         * We only need to wait for final "matmul_done" barrier to exit.
         */
        ctrl_barrier("matmul_done", 1, 60000);
    }

OUT:
    dsm_finalize_c();
    leopar_finalize();
    return 0;
}
