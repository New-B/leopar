#define _POSIX_C_SOURCE 200809L
#include "leopar.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <errno.h>

static inline double now_us(void){ struct timespec ts; clock_gettime(CLOCK_MONOTONIC,&ts);
    return (double)ts.tv_sec*1e6 + (double)ts.tv_nsec/1e3; }

static int cmp_dbl(const void *a, const void *b) {
    double da = *(const double*)a, db = *(const double*)b;
    return (da<db)?-1:(da>db);
}
static void stats(const char *name, double *x, int n) {
    qsort(x, n, sizeof(double), cmp_dbl);
    int p50=(int)(0.5*n), p95=(int)(0.95*n), p99=(int)(0.99*n);
    double s=0; for(int i=0;i<n;i++) s+=x[i];
    printf("%s: avg=%.2f p50=%.2f p95=%.2f p99=%.2f (us)\n", name, s/n, x[p50], x[p95], x[p99]);
}

static void* tiny(void* a){ (void)a; return NULL; }

static void run_mode(const char *label, int target, int iters, int rank_print)
{
    /* warmup */
    for(int i=0;i<200;i++){ leo_thread_t t; leo_thread_create(&t,NULL,tiny,NULL,target); leo_thread_join(t,NULL); }

    double *tc = (double*)malloc(sizeof(double)*iters);
    for (int i=0;i<iters;i++) {
        double t0 = now_us();
        leo_thread_t t; leo_thread_create(&t,NULL,tiny,NULL,target);
        double t1 = now_us();
        leo_thread_join(t,NULL);
        tc[i] = t1 - t0; /* focus on create cost */
    }
    if (rank_print==0) {
        printf("=== scheduler mode: %s ===\n", label);
        stats("create", tc, iters);
    }
    free(tc);
}

int main(int argc, char **argv)
{
    if (argc < 6) {
        fprintf(stderr, "usage: %s <cluster.ini> <rank> <peer_rank> <iters> <repeat>\n", argv[0]);
        return 1;
    }
    const char *cfg = argv[1];
    int my_rank     = atoi(argv[2]);
    int peer_rank   = atoi(argv[3]);
    int iters       = atoi(argv[4]);
    int repeat      = atoi(argv[5]);

    if (leopar_init(cfg, my_rank, NULL) != 0) {
        fprintf(stderr, "leopar_init failed\n"); return 1;
    }

    for (int r=0;r<repeat;r++) {
        run_mode("explicit-local",  my_rank, iters, my_rank);
        run_mode("explicit-remote", peer_rank, iters, my_rank);
        run_mode("auto(-1)",       -1,       iters, my_rank);
    }

    leopar_finalize();
    return 0;
}
