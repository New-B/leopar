#define _POSIX_C_SOURCE 200809L
#include "leopar.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <errno.h>

/* timing */
static inline double now_us(void){ struct timespec ts; clock_gettime(CLOCK_MONOTONIC,&ts);
    return (double)ts.tv_sec*1e6 + (double)ts.tv_nsec/1e3; }

/* Generate many distinct functions to trigger FUNC_ANNOUNCE */
#define GEN_FUNC(ID) \
    static void* fn_##ID(void *arg){ (void)arg; return NULL; }

GEN_FUNC(00) GEN_FUNC(01) GEN_FUNC(02) GEN_FUNC(03) GEN_FUNC(04) GEN_FUNC(05) GEN_FUNC(06) GEN_FUNC(07)
GEN_FUNC(08) GEN_FUNC(09) GEN_FUNC(10) GEN_FUNC(11) GEN_FUNC(12) GEN_FUNC(13) GEN_FUNC(14) GEN_FUNC(15)
GEN_FUNC(16) GEN_FUNC(17) GEN_FUNC(18) GEN_FUNC(19) GEN_FUNC(20) GEN_FUNC(21) GEN_FUNC(22) GEN_FUNC(23)
GEN_FUNC(24) GEN_FUNC(25) GEN_FUNC(26) GEN_FUNC(27) GEN_FUNC(28) GEN_FUNC(29) GEN_FUNC(30) GEN_FUNC(31)

typedef void* (*fn_t)(void*);
static fn_t table[] = {
    fn_00,fn_01,fn_02,fn_03,fn_04,fn_05,fn_06,fn_07,
    fn_08,fn_09,fn_10,fn_11,fn_12,fn_13,fn_14,fn_15,
    fn_16,fn_17,fn_18,fn_19,fn_20,fn_21,fn_22,fn_23,
    fn_24,fn_25,fn_26,fn_27,fn_28,fn_29,fn_30,fn_31
};

int main(int argc, char **argv)
{
    if (argc < 5) {
        fprintf(stderr, "usage: %s <cluster.ini> <rank> <peer_rank> <count>\n", argv[0]);
        return 1;
    }
    const char *cfg = argv[1];
    int my_rank     = atoi(argv[2]);
    int peer_rank   = atoi(argv[3]);
    int count       = atoi(argv[4]); /* number of distinct functions to test (<=32) */
    if (count > 32) count = 32;

    if (leopar_init(cfg, my_rank, NULL) != 0) {
        fprintf(stderr, "leopar_init failed\n"); return 1;
    }

    /* warmup */
    for (int i=0;i<100;i++){ leo_thread_t t; leo_thread_create(&t,NULL,table[0],NULL,peer_rank); leo_thread_join(t,NULL); }

    double *first = (double*)malloc(sizeof(double)*count);
    double *again = (double*)malloc(sizeof(double)*count);

    for (int i=0;i<count;i++) {
        /* first call (should include FUNC_ANNOUNCE) */
        double t0 = now_us();
        leo_thread_t t1; leo_thread_create(&t1,NULL,table[i],NULL,peer_rank);
        double t1e = now_us();
        leo_thread_join(t1,NULL);
        /* measure only create latency to isolate control plane */
        first[i] = t1e - t0;

        /* second call (announce already done) */
        t0 = now_us();
        leo_thread_t t2; leo_thread_create(&t2,NULL,table[i],NULL,peer_rank);
        t1e = now_us();
        leo_thread_join(t2,NULL);
        again[i] = t1e - t0;
    }

    if (my_rank == 0) {
        double sum1=0,sum2=0;
        for (int i=0;i<count;i++){ sum1+=first[i]; sum2+=again[i]; }
        printf("FUNC_ANNOUNCE cost over %d symbols: first=%.2f us, next=%.2f us (averages)\n",
               count, sum1/count, sum2/count);
        printf("Per-symbol detail (us):\n");
        for (int i=0;i<count;i++) {
            printf("  f[%02d]: first=%.2f  next=%.2f  delta=%.2f\n",
                   i, first[i], again[i], first[i]-again[i]);
        }
    }

    free(first); free(again);
    leopar_finalize();
    return 0;
}
