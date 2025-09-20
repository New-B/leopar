/**
 * @file leopar_byval_demo.c
 * @author …
 * @date 2025-09-20
 * @brief By-value argument passing demo using LeoPar (create/join across nodes).
 *
 * Usage:
 *   bin/leopar_byval_demo <rank> <num_tasks> <span>
 *
 * Example (3 nodes, ranks 0/1/2):
 *   node0$ bin/leopar_byval_demo 0 6 1000
 *   node1$ bin/leopar_byval_demo 1 6 1000
 *   node2$ bin/leopar_byval_demo 2 6 1000
 *
 * Notes:
 * - User code only includes "leopar.h". The runtime serializes the small arg
 *   struct by value and delivers a malloc'ed copy to the remote thread.
 * - The worker frees its received arg (copy); the caller frees its local arg
 *   right after leo_thread_create() returns (since the runtime already copied).
 */

#include "leopar.h"
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>

/* Small by-value argument payload. Keep it POD and small (e.g., <= few KB). */
struct Args {
    int id;      /* task id */
    int start;   /* inclusive */
    int end;     /* exclusive */
};

/* Worker executed on the destination rank. It must free(arg) after use. */
static void* worker_sum(void *p)
{
    struct Args *a = (struct Args*)p;
    int me = leo_rank();  /* LeoPar runtime query: my rank */

    long long sum = 0;
    for (int i = a->start; i < a->end; ++i) sum += i;

    /* Print to stdout just for demo observability. */
    printf("[rank %d] worker id=%d args=[%d,%d) sum=%lld\n",
        me, a->id, a->start, a->end, sum);

    /* IMPORTANT: free the by-value copy delivered by the runtime. */
    free(a);
    return NULL;
}

int main(int argc, char **argv)
{
    if (argc < 3 || argc > 4) {
        fprintf(stderr, "Usage: %s <config_path> <rank> [log_path]\n", argv[0]);
        return 1;
    }

    const char *config = argv[1];
    int my_rank = atoi(argv[2]);
    const char *log_path = (argc >= 4) ? argv[3] : NULL;

    /* 1) Initialize LeoPar runtime (handles config, UCX, dispatcher, logging) */
    if (leopar_init(config, my_rank, log_path) != 0) {
        fprintf(stderr, "leopar_init failed\n");
        return 1;
    }

    my_rank   = leo_rank(); /* this process rank */
    int num_tasks = 4;       /* how many threads to create */
    int span      = 10;      /* range length per task */



    int world = leo_world_size();

    /* Optional: sync all ranks before starting the demo. */
    (void)leo_barrier();

    /* Create tasks with round-robin placement across ranks. */
    leo_thread_t *tids = (leo_thread_t*)malloc(sizeof(leo_thread_t) * num_tasks);
    if (!tids) {
        fprintf(stderr, "alloc tids failed\n");
        leopar_finalize();
        return 1;
    }

    for (int t = 0; t < num_tasks; ++t) {
        /* Build a small POD arg and heap-allocate it.
        The runtime will copy this buffer into the message,
        and the *remote* thread will receive its own malloc'ed copy. */
        struct Args *a = (struct Args*)malloc(sizeof(*a));
        if (!a) { fprintf(stderr, "alloc arg failed at t=%d\n", t); break; }

        a->id    = t;
        a->start = t * span;
        a->end   = a->start + span;

        int target_rank = (world > 0) ? (t % world) : 0;  /* round-robin */

        int rc = leo_thread_create(&tids[t],
                                /*attr=*/NULL,
                                worker_sum,
                                /*arg(by value)*/ a,
                                /*target*/ target_rank);
        if (rc != 0) {
            fprintf(stderr, "leo_thread_create failed at t=%d (rc=%d)\n", t, rc);
            free(a);
            /* continue or abort as you wish; here we abort the loop */
            num_tasks = t;
            break;
        }

        /* IMPORTANT: free our local copy immediately — runtime has already serialized it. */
        free(a);
    }

    /* Join all tasks (across nodes). */
    for (int t = 0; t < num_tasks; ++t) {
        int rc = leo_thread_join(tids[t], /*retval*/NULL);
        if (rc != 0) {
            fprintf(stderr, "leo_thread_join failed at t=%d (rc=%d)\n", t, rc);
        }
    }

    free(tids);

    /* Optional: sync before teardown. */
    (void)leo_barrier();

    leopar_finalize();
    return 0;
}
 