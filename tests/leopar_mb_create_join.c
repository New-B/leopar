#define _POSIX_C_SOURCE 200809L
#include "leopar.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <errno.h>
#include <unistd.h>
#include <stdarg.h>

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
    printf("%s: avg=%.2f us  p50=%.2f  p95=%.2f  p99=%.2f p100=%.2f (n=%d)\n",
           name, sum/n, x[p50], x[p95], x[p99], sum, n);
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

// /* -------- concurrent join helpers -------- */

// /* joiner for a single gtid: measures join latency and stores into out[idx] */
// typedef struct {
//     leo_thread_t t;
//     double *out;
//     int idx;
// } joiner_arg_t;

// static void* joiner_thread_main(void *v) {
//     joiner_arg_t *a = (joiner_arg_t*)v;
//     double t0 = now_us();
//     /* perform join; assuming leo_thread_join blocks until thread finishes on owner */
//     int rc = leo_thread_join(a->t, NULL);
//     double t1 = now_us();
//     a->out[a->idx] = t1 - t0;
//     if (rc != 0) {
//         safe_pr("JOINER: leo_thread_join rc=%d idx=%d\n", rc, a->idx);
//     }
//     return NULL;
// }

// /* helper: create remote thread and return leo_thread_t in *out_t */
// static int create_remote_thread(int target_rank, leo_thread_t *out_t, void *(*fn)(void*), void *arg) {
//     leo_thread_t t;
//     int rc = leo_thread_create(&t, NULL, fn, arg, target_rank);
//     if (rc != 0) {
//         safe_pr("create_remote_thread: create failed rc=%d\n", rc);
//         return rc;
//     }
//     *out_t = t;
//     return 0;
// }

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

// /* Long task join: create a long-running remote task (sleep secs), then spawn many joiners
//    while it is running to stress dispatcher & join-path. joiners will block until sleeper exits. */
// static void test_long_task_join(int secs, int joiners, int target) {
//     safe_pr("=== Long-task join: sleeper=%ds joiners=%d target=%d ===\n", secs, joiners, target);

//     int arg = secs;
//     leo_thread_t sleeper_t;
//     if (create_remote_thread(target, &sleeper_t, sleeper, &arg) != 0) {
//         safe_pr("Failed to create sleeper thread\n");
//         return;
//     }

//     /* wait a brief moment to ensure remote task started */
//     usleep(100*1000);

//     double *lat = (double*)malloc(sizeof(double)*joiners);
//     pthread_t *ths = (pthread_t*)malloc(sizeof(pthread_t)*joiners);
//     joiner_arg_t *args = (joiner_arg_t*)malloc(sizeof(joiner_arg_t)*joiners);

//     for (int i=0;i<joiners;i++) {
//         args[i].t = sleeper_t;
//         args[i].out = lat;
//         args[i].idx = i;
//         pthread_create(&ths[i], NULL, joiner_thread_main, &args[i]);
//         usleep(1000);
//     }

//     for (int i=0;i<joiners;i++) pthread_join(ths[i], NULL);

//     print_stats("longtask-join-latency-us", lat, joiners);

//     free(lat); free(ths); free(args);
// }

// /* concurrent create worker arg */
// typedef struct {
//     int idx;
//     int target;
//     double *lat_out;
//     leo_thread_t *out_handle;
//     int *rc_out;
// } cworker_arg_t;

// static void* cworker_main(void* v) {
//     cworker_arg_t *a = (cworker_arg_t*)v;
//     double t0 = now_us();
//     leo_thread_t t;
//     int rc = leo_thread_create(&t, NULL, noop, NULL, a->target);
//     double t1 = now_us();
//     a->lat_out[a->idx] = t1 - t0;
//     a->rc_out[a->idx] = rc;
//     if (rc == 0) a->out_handle[a->idx] = t;
//     else a->out_handle[a->idx] = (leo_thread_t)0;
//     return NULL;
// }

// /* concurrent join worker arg */
// typedef struct {
//     int idx;
//     double *lat_out;
//     leo_thread_t handle;
//     int *rc_out;
// } jworker_arg_t;

// static void* jworker_main(void* v) {
//     jworker_arg_t *a = (jworker_arg_t*)v;
//     double t0 = now_us();
//     int rc = leo_thread_join(a->handle, NULL);
//     double t1 = now_us();
//     a->lat_out[a->idx] = t1 - t0;
//     a->rc_out[a->idx] = rc;
//     return NULL;
// }


/* The test: concurrent creates then concurrent join on created gtids.
 * - creates: number of concurrent create requests (all launched simultaneously by local helper threads)
 * - target: rank to create tasks on
 */
static void test_concurrent_create_then_join(int concurrency, int target) {
    safe_pr("=== concurrent create then join: concurrency=%d target=%d ===\n", concurrency, target);

    // /* allocate arrays */
    // double *create_lat = calloc((size_t)creates, sizeof(double));
    // int *create_rc = calloc((size_t)creates, sizeof(int));

    // pthread_t *cths = calloc((size_t)creates, sizeof(pthread_t));
    // cworker_arg_t *cargs = calloc((size_t)creates, sizeof(cworker_arg_t));

    double *tc = (double*)malloc(sizeof(double)*concurrency);
    double *tj = (double*)malloc(sizeof(double)*concurrency);
    leo_thread_t *handles = calloc((size_t)concurrency, sizeof(leo_thread_t));

    /* warmup small set */
    for (int w=0; w<10; ++w) {
        leo_thread_t t; leo_thread_create(&t, NULL, noop, NULL, target);
        leo_thread_join(t, NULL);
    }

    /* Launch concurrent create workers */
    double t_start_creates = now_us();
    for(int i = 0; i < concurrency; i++) {
        double t0 = now_us();
        leo_thread_create(&handles[i], NULL, noop, NULL, target);
        double t1 = now_us();
        tc[i] = t1 - t0;
    }
    double t_end_creates = now_us();

    /* Launch joiners concurrently */
    double t_start_joins = now_us();
    for (int i=0;i<concurrency;i++) {
        double t0 = now_us();
        leo_thread_join(handles[i], NULL);
        double t1 = now_us();
        tj[i] = t1 - t0;
    }
    double t_end_joins = now_us();

    print_stats("create", tc, concurrency);
    print_stats("join",   tj, concurrency);
    safe_pr("total concurrent create latency = %.2f \n", (t_end_creates - t_start_creates));
    safe_pr("total concurrent join latency = %.2f \n", (t_end_joins - t_start_joins));



    // /* Summarize create results */
    // int succ_create = 0;
    // for (int i=0;i<creates;i++) if (create_rc[i] == 0) succ_create++;
    // safe_pr("Creates done: total=%d success=%d fail=%d elapsed_ms=%.2f\n",
    //         creates, succ_create, creates - succ_create, (t_end_creates - t_start_creates)/1e3);

    // /* print create latency stats for successful ones */
    // double *create_lat_succ = malloc(sizeof(double)*succ_create);
    // int idx=0;
    // for (int i=0;i<creates;i++) if (create_rc[i]==0) create_lat_succ[idx++] = create_lat[i];
    // print_stats("create_latency_us", create_lat_succ, succ_create);
    // safe_pr("create stddev=%.2f us\n", compute_stddev(create_lat_succ, succ_create));
    // free(create_lat_succ);

    // /* Now concurrently join all successful handles (one joiner per created handle) */
    // int joins = succ_create;
    // if (joins == 0) {
    //     safe_pr("No successful creates, skipping join stage\n");
    //     free(create_lat); free(create_rc); free(handles);
    //     free(cths); free(cargs);
    //     return;
    // }

    // double *join_lat = calloc((size_t)joins, sizeof(double));
    // int *join_rc = calloc((size_t)joins, sizeof(int));
    // pthread_t *jths = calloc((size_t)joins, sizeof(pthread_t));
    // jworker_arg_t *jargs = calloc((size_t)joins, sizeof(jworker_arg_t));

    // /* map successful handles into join arrays */
    // idx = 0;
    // for (int i=0;i<creates;i++) {
    //     if (create_rc[i] == 0) {
    //         jargs[idx].idx = idx;
    //         jargs[idx].lat_out = join_lat;
    //         jargs[idx].handle = handles[i];
    //         jargs[idx].rc_out = join_rc;
    //         idx++;
    //     }
    // }

    // /* Launch joiners concurrently */
    // double t_start_joins = now_us();
    // for (int i=0;i<joins;i++) {
    //     pthread_create(&jths[i], NULL, jworker_main, &jargs[i]);
    // }
    // double t_end_joins = now_us();
    // for (int i=0;i<joins;i++) pthread_join(jths[i], NULL);


    // /* Summarize join results */
    // int succ_join = 0;
    // for (int i=0;i<joins;i++) if (join_rc[i] == 0) succ_join++;
    // safe_pr("Joins done: total=%d success=%d fail=%d elapsed_ms=%.2f\n",
    //         joins, succ_join, joins - succ_join, (t_end_joins - t_start_joins)/1e3);

    // /* stats for successful joins */
    // double *join_lat_succ = malloc(sizeof(double)*succ_join);
    // idx = 0;
    // for (int i=0;i<joins;i++) if (join_rc[i]==0) join_lat_succ[idx++] = join_lat[i];
    // print_stats("join_latency_us", join_lat_succ, succ_join);
    // safe_pr("join stddev=%.2f us\n", compute_stddev(join_lat_succ, succ_join));
    // free(join_lat_succ);

    /* cleanup arrays */
    free(tc); free(tj); free(handles);
    // free(cths); free(cargs);
    // free(join_lat); free(join_rc); free(jths); free(jargs);
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
        for(int i=0;i<5;i++) {
            test_baseline_single( (iters>0?iters:10000), 1 );

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
        sleep(500);
        safe_pr("[rank %d] done\n", my_rank);
        leopar_finalize();

        return 0;
    }
    return 0;
}
