/* tests/dsm_smoke.c */
#include "leopar.h"   /* for init/finalize */
#include "dsm.h"
#include <stdio.h>
#include <string.h>
#include <stdlib.h>

int main(int argc, char **argv) {
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


    /* Each rank allocates locally, writes its rank id, others read it. */
    leo_gaddr_t g = leo_malloc(64);
    char msg[64]; snprintf(msg, sizeof(msg), "hello-from-rank-%d", rank);
    leo_write(g, msg, strlen(msg)+1);

    /* Let rank 0 read from everybody */
    if (rank == 0) {
        for (int r = 0; r < leo_world_size(); ++r) {
            leo_gaddr_t gr = LEO_GPTR_MAKE(r, LEO_GPTR_OFFSET(g)); /* same offset on each rank's arena */
            char buf[64] = {0};
            leo_read(buf, gr, sizeof(buf));
            printf("read[%d]: %s\n", r, buf);
        }
    }

    leopar_finalize();
    return 0;
}
