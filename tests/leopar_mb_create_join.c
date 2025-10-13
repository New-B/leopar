#define _POSIX_C_SOURCE 200809L
#include "leopar.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <errno.h>
#include <unistd.h>
#include <stdarg.h>
#include <math.h>
#include <stdatomic.h>

/* -------- timing helpers -------- */
static inline double now_us(void) {
    struct timespec ts; clock_gettime(CLOCK_MONOTONIC, &ts);
    return (double)ts.tv_sec * 1e6 + (double)ts.tv_nsec / 1e3;
}
static int cmp_dbl(const void *a, const void *b) {
    double da = *(const double*)a, db = *(const double*)b;
    return (da < db) ? -1 : (da > db);
}
static void print_stats(const char *name, double *x, int n) {
    qsort(x, n, sizeof(double), cmp_dbl);
    int idx50 = (int)ceil(0.50 * n) - 1;
    int idx95 = (int)ceil(0.95 * n) - 1;
    int idx99 = (int)ceil(0.99 * n) - 1;
    if (idx50 < 0) idx50 = 0;
    if (idx95 < 0) idx95 = 0;
    if (idx99 < 0) idx99 = 0;

    double sum = 0.0;
    for (int i = 0; i < n; i++)
        sum += x[i];

    printf("%s: avg=%.2f us  p50=%.2f  p95=%.2f  p99=%.2f  max=%.2f (n=%d)\n",
           name, sum / n, x[idx50], x[idx95], x[idx99], x[n - 1], n);
}

/* -------- worker functions to be executed remotely -------- */
/* simple noop (fast) */
void* noop(void *arg) { (void)arg; return NULL; }

/* a long-running worker to stress join: sleeps for given seconds */
void* sleeper(void *arg) {
    int secs = arg ? *(int*)arg : 1;
    sleep(secs);
    return NULL;
}

/* -------- utilities: thread-safe printing -------- */
static void safe_pr(const char *fmt, ...) {
    va_list ap;
    va_start(ap, fmt);
    vprintf(fmt, ap);
    va_end(ap);
    fflush(stdout);
}

/* concurrent create worker arg */
typedef struct {
    int idx;
    int target;
    double *lat_out;
    leo_thread_t *out_handle;
    int *rc_out;
} cworker_arg_t;

/* concurrent join worker arg */
typedef struct {
    int idx;
    double *lat_out;
    leo_thread_t handle;
    int *rc_out;
} jworker_arg_t;

static double *g_create_lat = NULL;    /* per-create latency (us) */
static int    *g_create_rc  = NULL;    /* per-create return code */
static leo_thread_t *g_handles = NULL; /* created handles */
static pthread_t *g_cths = NULL;       /* thread ids for create workers */
static cworker_arg_t *g_cargs = NULL;  /* create worker args */

static double *g_join_lat = NULL;
static int    *g_join_rc  = NULL;
static pthread_t *g_jths  = NULL;
static jworker_arg_t *g_jargs = NULL;

/* allocate globals before launching threads */
static int alloc_globals(int n) {
    g_create_lat = calloc((size_t)n, sizeof(double));
    g_create_rc  = calloc((size_t)n, sizeof(int));
    g_handles    = calloc((size_t)n, sizeof(leo_thread_t));
    g_cths       = calloc((size_t)n, sizeof(pthread_t));
    g_cargs      = calloc((size_t)n, sizeof(cworker_arg_t));
    g_join_lat   = calloc((size_t)n, sizeof(double));
    g_join_rc    = calloc((size_t)n, sizeof(int));
    g_jths       = calloc((size_t)n, sizeof(pthread_t));
    g_jargs      = calloc((size_t)n, sizeof(jworker_arg_t));
    if (!g_create_lat || !g_create_rc || !g_handles || !g_cths || 
        !g_cargs || !g_join_lat || !g_join_rc || !g_jths || !g_jargs) {
        return -1;
    }
    return 0;
}

/* create worker: uses global arrays and its index passed via arg */
static void* cworker_main_idx(void *v) {
    int idx = (int)(intptr_t)v;
    double t0 = now_us();
    leo_thread_t t;
    int rc = leo_thread_create(&t, NULL, noop, NULL, /*target*/ 1);
    double t1 = now_us();

    /* write to global arrays at slot idx -- unique to this thread */
    g_create_lat[idx] = t1 - t0;
    g_create_rc[idx] = rc;
    if (rc == 0) g_handles[idx] = t;
    else g_handles[idx] = (leo_thread_t)0;
    return NULL;
}

static void* jworker_main_idx(void *v) {
    int idx = (int)(intptr_t)v;
    // safe_pr("concurrent join request start for in %d\n", idx);
    double t0 = now_us();
    int rc = leo_thread_join(g_handles[idx], NULL);
    double t1 = now_us();
    // safe_pr("join request finished for thread %d\n", idx);
    g_join_lat[idx] = t1 - t0;
    g_join_rc[idx] = rc;
    return NULL;
}



/* -------- tests implemented on initiator (rank 0) -------- */

/* Baseline single create/join test (keeps your original behavior) */
static void test_baseline_single(int iters, int target) {
    safe_pr("=== Baseline single create/join: iters=%d target=%d ===\n", iters, target);

    double *tc = (double*)malloc(sizeof(double)*iters);
    double *tj = (double*)malloc(sizeof(double)*iters);
    double *tt = (double*)malloc(sizeof(double)*iters);

    /* warmup */
    for (int i=0;i<100;i++) {
        leo_thread_t t; leo_thread_create(&t, NULL, noop, NULL, target);
        leo_thread_join(t, NULL);
    }

    for (int p = 0; p < 6; p++){
        for (int i=0;i<iters;i++) {
            if(i >= p*0.2*iters){
                target = 0;
            } else {
                target = 1;
            }
            double t0 = now_us();
            leo_thread_t t;
            leo_thread_create(&t, NULL, noop, NULL, target);
            double t1 = now_us();
            leo_thread_join(t, NULL);
            double t2 = now_us();
            tc[i] = t1 - t0;
            tj[i] = t2 - t1;
            tt[i] = t2 - t0;
        }
        safe_pr("=== create/join latency (remote percentage: %.2f) ===\n", p*0.2);
        print_stats("create", tc, iters);
        print_stats("join",   tj, iters);
        print_stats("total",  tt, iters);
    }

    free(tc); free(tj); free(tt);
}


/* Helper: compute stddev (population) */
static double compute_stddev(double *x, int n) {
    if (n <= 1) return 0.0;
    double sum = 0;
    for (int i=0;i<n;i++) sum += x[i];
    double mean = sum / n;
    double s = 0;
    for (int i=0;i<n;i++) {
        double d = x[i] - mean;
        s += d*d;
    }
    return sqrt(s / (n-1));
}


/* The test: concurrent creates then concurrent join on created gtids.
 * - creates: number of concurrent create requests (all launched simultaneously by local helper threads)
 * - target: rank to create tasks on
 */
static void test_concurrent_create_then_join(int concurrency, int target) {
    safe_pr("=== concurrent create: concurrency=%d target=%d ===\n", concurrency, target);

    if (alloc_globals(concurrency) != 0) { /* handle OOM */ }

    /* warmup small set */
    for (int w=0; w<10; ++w) {
        leo_thread_t t; leo_thread_create(&t, NULL, noop, NULL, target);
        leo_thread_join(t, NULL);
    }

    /* Launch concurrent create workers */
    double t_start_creates = now_us();
    for (int i = 0; i < concurrency; ++i) {
        pthread_create(&g_cths[i], NULL, cworker_main_idx, (void*)(intptr_t)i);
    }
    for (int i=0;i<concurrency;i++) pthread_join(g_cths[i], NULL);
    double t_end_creates = now_us();


    /* Summarize create results */
    int succ_create = 0;
    for (int i=0;i<concurrency;i++) if (g_create_rc[i] == 0) succ_create++;
    safe_pr("Creates done: total=%d success=%d fail=%d elapsed_ms=%.2f\n",
        concurrency, succ_create, concurrency - succ_create, (t_end_creates - t_start_creates)/1e3);

    /* print create latency stats for successful ones */
    double *create_lat_succ = malloc(sizeof(double)*succ_create);
    int idx=0;
    for (int i=0;i<concurrency;i++) if (g_create_rc[i]==0) create_lat_succ[idx++] = g_create_lat[i];
    print_stats("create_latency_us", create_lat_succ, succ_create);
    safe_pr("create stddev=%.2f us\n", compute_stddev(create_lat_succ, succ_create));
    free(create_lat_succ);

    /* Now concurrently join all successful handles (one joiner per created handle) */
    int joins = succ_create;
    if (joins == 0) {
        safe_pr("No successful creates, skipping join stage\n");
        return;
    }

    safe_pr("=== concurrent then join: concurrency=%d target=%d ===\n", concurrency, target);

    double t_start_joins = now_us();
    /* spawn joiners for all successful creates */
    for (int i=0, j=0; i<concurrency; ++i) {
        if (g_create_rc[i] != 0) continue; /* skip failed create */
        pthread_create(&g_jths[j], NULL, jworker_main_idx, (void*)(intptr_t)i);
        ++j;
    }
    safe_pr("=== concurrent join end ===\n");
    for (int k=0;k<succ_create;k++) pthread_join(g_jths[k], NULL);
    double t_end_joins = now_us();
    safe_pr("=== concurrent join end ===\n");


    /* Summarize join results */
    int succ_join = 0;
    for (int i=0;i<joins;i++) if (g_join_rc[i] == 0) succ_join++;
    safe_pr("Joins done: total=%d success=%d fail=%d elapsed_ms=%.2f\n",
            joins, succ_join, joins - succ_join, (t_end_joins - t_start_joins)/1e3);

    /* stats for successful joins */
    double *join_lat_succ = malloc(sizeof(double)*succ_join);
    idx = 0;
    for (int i=0;i<joins;i++) if (g_join_rc[i]==0) join_lat_succ[idx++] = g_join_lat[i];
    print_stats("join_latency_us", join_lat_succ, succ_join);
    safe_pr("join stddev=%.2f us\n", compute_stddev(join_lat_succ, succ_join));
    free(join_lat_succ);

}


int main(int argc, char **argv)
{
    if (argc < 3 || argc > 4) {
        fprintf(stderr, "Usage: %s <config_path> <rank> [log_path]\n", argv[0]);
        return 1;
    }
    const char *cfg = argv[1];
    int my_rank     = atoi(argv[2]);
    const char *log_path = argv[3];
    int iters       = 10000;

    if (leopar_init(cfg, my_rank, log_path) != 0) {
        fprintf(stderr, "leopar_init failed\n");
        return 1;
    }
    if(my_rank == 0){
        safe_pr("[rank %d] initiator: running microbench suite\n", my_rank);

        /* Concurrent create/join on single target: vary concurrent levels */
        int concurrency_levels[] = {16, 32, 48, 64, 80, 96, 112, 128};

        /* Baseline single create/join */
        for(int i=0;i<2;i++) {
            //test_baseline_single( (iters>0?iters:10000), 1 );

            for (size_t i=0;i<sizeof(concurrency_levels)/sizeof(concurrency_levels[0]); ++i) {
                
                test_concurrent_create_then_join(concurrency_levels[i], 1);
            }
        }


        safe_pr("[rank %d] done\n", my_rank);

        // int target = 0;

        // double *tc = (double*)malloc(sizeof(double)*iters);
        // double *tj = (double*)malloc(sizeof(double)*iters);
        // double *tt = (double*)malloc(sizeof(double)*iters);

        // /* warmup */
        // for (int i=0;i<100;i++) {
        //     leo_thread_t t; leo_thread_create(&t, NULL, noop, NULL, target);
        //     leo_thread_join(t, NULL);
        // }

        // for (int p = 0; p < 6; p++){
        //     for (int i=0;i<iters;i++) {
        //         if(i >= p*0.2*iters){
        //             target = 0;
        //         } else {
        //             target = 1;
        //         }
        //         double t0 = now_us();
        //         leo_thread_t t; 
        //         leo_thread_create(&t, NULL, noop, NULL, target);
        //         double t1 = now_us();
        //         leo_thread_join(t, NULL);
        //         double t2 = now_us();
        //         tc[i] = t1 - t0;
        //         tj[i] = t2 - t1;
        //         tt[i] = t2 - t0;
        //     }
        //     printf("=== create/join latency (remote percentage: %f) ===\n", p*0.2);
        //     print_stats("create", tc, iters);
        //     print_stats("join",   tj, iters);
        //     print_stats("total",  tt, iters);
        // }

        // free(tc); free(tj); free(tt);

        // printf("[rank %d] done\n", my_rank);
        leopar_finalize();

        return 0;
    } else {
        /* responder: keep process alive to run incoming remote threads */
        safe_pr("[rank %d] responder: waiting for initiator, no local creates\n", my_rank);
        /* Keep alive longer than tests */
        sleep(250);
        safe_pr("[rank %d] done\n", my_rank);
        leopar_finalize();

        return 0;
    }
    return 0;
}
