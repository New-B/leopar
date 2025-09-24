/* tests/dsm_smoke.c */
#include "leopar.h"   /* for init/finalize */
#include "dsm.h"
#include "log.h"
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
    log_info("leopar_init succeeded at rank %d", my_rank);


    /* Each rank allocates locally, writes its rank id, others read it. */
    leo_gaddr_t g = leo_malloc(64,1);
    char msg[64]; 
    log_debug("leo_malloc returned gaddr=0x%llx", (unsigned long long)g);
    snprintf(msg, sizeof(msg), "hello-from-rank-%d", my_rank);
    leo_write(g, msg, strlen(msg)+1);
    log_debug("msg:%s", msg); 


    //leo_gaddr_t gr = LEO_GADDR_MAKE(r, LEO_GADDR_OFFSET(g)); /* same offset on each rank's arena */
    char buf[64] = {0};
    leo_read(buf, g, sizeof(buf));
    log_debug("read from rank %d: %s", my_rank, buf);

    leopar_finalize();
    return 0;
}
