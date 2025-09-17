/**
 * @file scheduler.c
 * @author Wang Bo
 * @date 2025-09-13
 * @brief Implementation of scheduling policies for LeoPar runtime.
 */

#include "scheduler.h"
#include <stdatomic.h>

/* global atomic counter for round-robin */
static atomic_int g_next_rank = 0;

/* Round-robin rank selection */
int scheduler_choose_rank(int world_size)
{
    int r = atomic_fetch_add(&g_next_rank, 1);
    if (world_size <= 0) {
        return 0; /* fallback */
    }
    return (r % world_size);
}
