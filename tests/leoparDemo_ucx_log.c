/**
 * @file leoparDemo_ucx_log.c
 * @author Wang Bo
 * @date 2025-09-12
 * @brief Example application using LeoPar runtime (with ucx and log on multi-nodes).
 */

 #include "leopar.h"
 #include "log.h"
 #include <stdio.h>
 #include <stdlib.h>
 
 int main(int argc, char **argv)
 {
     if (argc < 3) {
         fprintf(stderr, "Usage: %s <config_path> <rank>\n", argv[0]);
         return 1;
     }
     const char *config_path = argv[1];
     int rank = atoi(argv[2]);
 
     if (leopar_init(config_path, rank) != 0) {
         fprintf(stderr, "LeoPar runtime init failed\n");
         return 2;
     }
 
     log_info("Hello from LeoPar rank %d!", rank);
 
     leopar_finalize();
     return 0;
 }
 