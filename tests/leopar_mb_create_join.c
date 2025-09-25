#define _POSIX_C_SOURCE 200809L
#include "leopar.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <errno.h>

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
    int p50 = (int)(0.50 * n), p95 = (int)(0.95 * n), p99 = (int)(0.99 * n);
    double sum = 0; for (int i=0;i<n;i++) sum += x[i];
    printf("%s: avg=%.2f us  p50=%.2f  p95=%.2f  p99=%.2f (n=%d)\n",
           name, sum/n, x[p50], x[p95], x[p99], n);
}

/* -------- empty worker -------- */
static void* noop(void *arg) { (void)arg; return NULL; }

int main(int argc, char **argv)
{
    if (argc < 6) {
        fprintf(stderr, "usage: %s <cluster.ini> <rank> <peer_rank> <iters> <mode>\n", argv[0]);
        fprintf(stderr, "  mode: remote|local|auto\n");
        return 1;
    }
    const char *cfg = argv[1];
    int my_rank     = atoi(argv[2]);
    int peer_rank   = atoi(argv[3]);
    int iters       = atoi(argv[4]);
    const char *mode= argv[5];

    if (leopar_init(cfg, my_rank, NULL) != 0) {
        fprintf(stderr, "leopar_init failed\n");
        return 1;
    }

    int target = 0;
    if (!strcmp(mode,"remote")) target = peer_rank;
    else if (!strcmp(mode,"local")) target = my_rank;
    else if (!strcmp(mode,"auto")) target = -1;
    else { fprintf(stderr, "unknown mode\n"); return 1; }

    /* warmup */
    for (int i=0;i<100;i++) {
        leo_thread_t t; leo_thread_create(&t, NULL, noop, NULL, target);
        leo_thread_join(t, NULL);
    }

    double *tc = (double*)malloc(sizeof(double)*iters);
    double *tj = (double*)malloc(sizeof(double)*iters);
    double *tt = (double*)malloc(sizeof(double)*iters);

    for (int i=0;i<iters;i++) {
        double t0 = now_us();
        leo_thread_t t; leo_thread_create(&t, NULL, noop, NULL, target);
        double t1 = now_us();
        leo_thread_join(t, NULL);
        double t2 = now_us();
        tc[i] = t1 - t0;
        tj[i] = t2 - t1;
        tt[i] = t2 - t0;
    }

    if (my_rank == 0) {
        printf("=== create/join latency (%s) ===\n", mode);
        print_stats("create", tc, iters);
        print_stats("join",   tj, iters);
        print_stats("total",  tt, iters);
    }

    free(tc); free(tj); free(tt);
    leopar_finalize();
    return 0;
}
