/**
 * @file leopar_pthread_style_demo.c
 * @brief Demo: use LeoPar like pthreads, but threads may run on remote nodes.
 */

#include "leopar.h"
#include "log.h"
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

/* user function */
void* worker(void *arg) {
    int v = *(int*)arg;
    log_info("Hello from worker, arg=%d", v);
    sleep(1);
    return NULL;
}

int main(int argc, char **argv)
{
    if (argc < 3 || argc > 4) {
        fprintf(stderr, "Usage: %s <config_path> <rank> [log_path]\n", argv[0]);
        return 1;
    }

    const char *config_path = argv[1];
    int rank = atoi(argv[2]);
    const char *log_path = (argc >= 4) ? argv[3] : NULL;

    if (leopar_init(config_path, rank, log_path) != 0) {
        fprintf(stderr, "runtime init failed\n");
        return 2;
    }

    leo_thread_t tid;
    int arg = 42;

    if (rank == 0) {
        /* like pthread_create */
        leo_thread_create(&tid, NULL, worker, &arg, 1);  // force on rank 1
        leo_thread_join(tid, NULL);
        log_info("joined remote worker");
    }

    sleep(2);
    leopar_finalize();
    return 0;
}
