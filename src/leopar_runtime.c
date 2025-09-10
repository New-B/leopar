/**
 * @file leopar_runtime.c
 * @author Wang Bo
 * @date 2025-09-09
 * @brief Runtime implementation for LeoPar programming model.
 */

 #include "leopar.h"
 #include "log.h"
 #include <stdlib.h>
 
 int leoinit(int *argc, char ***argv) {
     if (log_init("logs/leopar.log") == 0) {
         log_info("Runtime initialized.");
     } else {
         log_error("Failed to initialize log system.");
     }
     return 0;
 }
 
 int leofinalize(void) {
     log_info("Runtime finalized.");
     log_finalize();
     return 0;
 }
 
 int leothread_create(leothread_t *thread, void* (*func)(void*), void *arg) {
     pthread_t *pt = malloc(sizeof(pthread_t));
     int ret = pthread_create(pt, NULL, func, arg);
     *thread = (leothread_t)pt;
     log_debug("Thread created.");
     return ret;
 }
 
 int leothread_join(leothread_t thread, void **retval) {
     pthread_t *pt = (pthread_t*)thread;
     int ret = pthread_join(*pt, retval);
     free(pt);
     log_debug("Thread joined.");
     return ret;
 }
 
 int leotask_spawn(void (*func)(void*), void *arg) {
     log_info("Task spawned.");
     func(arg);
     return 0;
 }
 
 int leofor(long start, long end, long chunk,
            void (*func)(long, void*), void *arg) {
     log_info("Parallel for start: [%ld, %ld), chunk=%ld", start, end, chunk);
     for (long i = start; i < end; i += chunk) {
         func(i, arg);
     }
     log_info("Parallel for finished.");
     return 0;
 }
 