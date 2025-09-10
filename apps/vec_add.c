/**
 * @file vec_add.c
 * @author Wang Bo
 * @date 2025-09-09
 * @brief Vector addition example using LeoPar parallel for.
 */

 #include "leopar.h"
 #include <stdio.h>
 #include <stdlib.h>
 
 #define N 100
 
 void vec_add_body(long i, void *arg) {
     int *arrays = (int*)arg;
     int *A = arrays;
     int *B = arrays + N;
     int *C = arrays + 2*N;
     C[i] = A[i] + B[i];
 }
 
 int main(int argc, char **argv) {
     leoinit(&argc, &argv);
 
     int *arrays = malloc(3 * N * sizeof(int));
     int *A = arrays;
     int *B = arrays + N;
     int *C = arrays + 2*N;
 
     for (int i = 0; i < N; i++) {
         A[i] = i;
         B[i] = 2*i;
     }
 
     leofor(0, N, 1, vec_add_body, arrays);
 
     for (int i = 0; i < 10; i++) {
         printf("C[%d] = %d\n", i, C[i]);
     }
 
     free(arrays);
     leofinalize();
     return 0;
 }
 