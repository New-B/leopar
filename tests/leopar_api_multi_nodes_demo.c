/**
 * @file leopar_api_three_node_demo.c
 * @author
 * @date 2025-09-16
 * @brief Pure-API demo: run on 3 nodes (ranks 0,1,2). Rank0 spawns two
 *        remote threads (target_rank = -1 -> runtime auto-selection).
 *
 * Usage (on three machines/terminals):
 *   ./bin/leopar_api_three_node_demo cluster.ini 0
 *   ./bin/leopar_api_three_node_demo cluster.ini 1
 *   ./bin/leopar_api_three_node_demo cluster.ini 2
 *
 * NOTE:
 *  - This program only includes "leopar.h" and standard headers.
 *  - All runtime setup (config, UCX, dispatcher, logging) is handled
 *    inside leopar_init()/leopar_finalize().
 *  - Printing to stdout is allowed for demo programs; the runtime logs
 *    still go to log files.
 */

#include <stdio.h>
#include <stdlib.h>
#include <inttypes.h>
#include <unistd.h>     /* sleep */
#include "leopar.h"     /* public API only */

/* A simple worker executed remotely.
* This must be globally visible for dlsym() on remote ranks.
*/
void* demo_worker(void *arg)
{
    (void)arg;
    /* Demo prints allowed on stdout */
    printf("[demo_worker] Hello from a remote LeoPar thread!\n");
    /* simulate some work */
    usleep(200 * 1000); /* 200 ms */
    printf("[demo_worker] Work done, exiting.\n");
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

    /* 2) Rank0 creates two remote threads; other ranks just stay alive.
    *    We purposely use target_rank = -1 to let the runtime pick nodes.
    */
    if (my_rank == 0) {
        leo_thread_t t1 = 0, t2 = 0;

        printf("[rank0] Creating remote thread #1 (auto rank)\n");
        if (leo_thread_create(&t1, NULL, demo_worker, NULL, -1) != 0) {
            printf("[rank0] create #1 failed\n");
        } else {
            printf("[rank0] created thread #1 gtid=%" PRIu64 "\n", (uint64_t)t1);
        }

        printf("[rank0] Creating remote thread #2 (auto rank)\n");
        if (leo_thread_create(&t2, NULL, demo_worker, NULL, -1) != 0) {
            printf("[rank0] create #2 failed\n");
        } else {
            printf("[rank0] created thread #2 gtid=%" PRIu64 "\n", (uint64_t)t2);
        }

        /* Join both threads (wait until they finish) */
        if (t1) {
            if (leo_thread_join(t1, NULL) == 0)
                printf("[rank0] join OK for gtid=%" PRIu64 "\n", (uint64_t)t1);
            else
                printf("[rank0] join FAILED for gtid=%" PRIu64 "\n", (uint64_t)t1);
        }
        if (t2) {
            if (leo_thread_join(t2, NULL) == 0)
                printf("[rank0] join OK for gtid=%" PRIu64 "\n", (uint64_t)t2);
            else
                printf("[rank0] join FAILED for gtid=%" PRIu64 "\n", (uint64_t)t2);
        }

        printf("[rank0] All remote threads joined. Exiting.\n");
    } else {
        /* Keep process alive long enough to service incoming creates/joins.
        * In a real app, you'd do useful work here; for the demo, sleep a bit.
        */
        printf("[rank%d] Ready to execute remote threads. Waiting...\n", my_rank);
        sleep(3);
        printf("[rank%d] Done waiting.\n", my_rank);
    }

    /* 3) Finalize runtime (tears down dispatcher/UCX/logs internally) */
    leopar_finalize();
    return 0;
}
