/**
 * @file test_threads.c
 * @author Wang Bo
 * @date 2025-09-09
 * @brief Test basic thread creation and joining in LeoPar.
 */

 #include "leopar.h"
 #include <stdio.h>
 
 void* thread_func(void *arg) {
     int id = *(int*)arg;
     printf("Hello from thread %d\n", id);
     return NULL;
 }
 
 int main(int argc, char **argv) {
     leoinit(&argc, &argv);
 
     leothread_t t1, t2;
     int id1 = 1, id2 = 2;
 
     leothread_create(&t1, thread_func, &id1);
     leothread_create(&t2, thread_func, &id2);
 
     leothread_join(t1, NULL);
     leothread_join(t2, NULL);
 
     leofinalize();
     return 0;
 }
 