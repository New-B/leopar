/**
 * @file leopar.h
 * @author Wang Bo
 * @date 2025-09-09
 * @brief Public API for LeoPar: a parallel programming model runtime prototype.
 */

 #ifndef LEOPAR_H
 #define LEOPAR_H
 
 #include <pthread.h>
 
 /* Thread handle */
 typedef void* leothread_t;
 
 /* Runtime system initialization and finalization */
 int leoinit(int *argc, char ***argv);
 int leofinalize(void);
 
 /* Thread management */
 int leothread_create(leothread_t *thread, void* (*func)(void*), void *arg);
 int leothread_join(leothread_t thread, void **retval);
 
 /* Task management */
 int leotask_spawn(void (*func)(void*), void *arg);
 
 /* Parallel for loop */
 int leofor(long start, long end, long chunk,
            void (*func)(long, void*), void *arg);
 
 #endif /* LEOPAR_H */
 
