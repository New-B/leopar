#define _POSIX_C_SOURCE 200809L
#include "leopar.h"
#include "dsm_c_api.h"
#include "ctrl.h"     // ctrl_barrier(name, gen, timeout_ms) if available here
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <inttypes.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <errno.h>

int main(int argc, char** argv) {
    if (argc < 3 || argc > 4) {
        fprintf(stderr, "Usage: %s <config_path> <rank> [log_path]\n", argv[0]);
        return 1;
    }
    const char *cfg = argv[1];
    int my_rank     = atoi(argv[2]);
    const char *log_path = argv[3];

    if (leopar_init(cfg, my_rank, log_path) != 0) {
        fprintf(stderr, "leopar_init failed\n");
        return 1;
    }

    int world_size = leo_world_size();

    leopar_finalize();
    return 0;


}