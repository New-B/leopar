/**
 * @file threadtable.c
 * @author Wang Bo
 * @date 2025-09-13
 * @brief Implementation of local thread table management for LeoPar runtime.
 */

#include "threadtable.h"
#include "log.h"

#include <string.h>

/* Global local thread table */
local_thread_t g_local_threads[MAX_LOCAL_THREADS];

/* Initialize all thread slots */
int threadtable_init(void)
{
    for (int i = 0; i < MAX_LOCAL_THREADS; i++) {
        g_local_threads[i].in_use = 0;
        g_local_threads[i].finished = 0;
        g_local_threads[i].thread = 0;
    }
    log_info("Thread table initialized with %d slots", MAX_LOCAL_THREADS);
    return 0; /* always success for now */
}

/* Ensure all threads are joined before shutdown */
void threadtable_finalize(void)
{
    for (int i = 0; i < MAX_LOCAL_THREADS; i++) {
        if (g_local_threads[i].in_use && !g_local_threads[i].finished) {
            log_warn("Thread %d still active, joining...", i);
            pthread_join(g_local_threads[i].thread, NULL);
            g_local_threads[i].in_use = 0;
            g_local_threads[i].finished = 1;
        }
    }
    log_info("Thread table finalized");
}

/* Get pointer to local thread entry */
local_thread_t* threadtable_get(int local_tid)
{
    if (local_tid < 0 || local_tid >= MAX_LOCAL_THREADS) return NULL;
    return &g_local_threads[local_tid];
}

/* Allocate a free thread slot */
int threadtable_alloc(void)
{
    for (int i = 0; i < MAX_LOCAL_THREADS; i++) {
        if (!g_local_threads[i].in_use) {
            g_local_threads[i].in_use = 1;
            g_local_threads[i].finished = 0;
            g_local_threads[i].task_id = -1;
            g_local_threads[i].arg_buf = NULL;
            g_local_threads[i].arg_len = 0;
            g_local_threads[i].local_tid = i;
            g_local_threads[i].gtid = 0;
            g_local_threads[i].creator_rank = -1;
            pthread_mutex_init(&g_local_threads[i].mu, NULL);
            pthread_cond_init(&g_local_threads[i].cv, NULL);
            return i;
        }
    }
    return -1;
}
