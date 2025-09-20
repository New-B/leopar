/**
 * @file async.c
 * @author Wang Bo
 * @date 2025-09-20
 * @brief Futures built on top of LeoPar threads (await == join semantics).
 */

#include "leopar.h"
#include "tid.h"
#include "log.h"
#include "context.h"

#include <stdlib.h>
#include <string.h>
#include <errno.h>

typedef struct {
    leo_thread_t gtid;
    int          ready;   /* cached readiness (best-effort) */
} future_rec_t;

/* Very small fixed table for prototype; can be replaced by a hash map later. */
#ifndef FUTURE_MAX
#define FUTURE_MAX 4096
#endif

static future_rec_t g_futures[FUTURE_MAX];

static int future_alloc_slot(void) {
    for (int i = 0; i < FUTURE_MAX; ++i) {
        if (g_futures[i].gtid == 0 && g_futures[i].ready == 0) return i;
    }
    return -1;
}

int leo_async(leo_future_t *f, void *(*fn)(void*), void *arg, int target_rank)
{
    if (!f) return -1;
    int slot = future_alloc_slot();
    if (slot < 0) return -1;

    leo_thread_t tid = 0;
    int rc = leo_thread_create((leothread_t*)&tid, NULL, fn, arg, target_rank);
    if (rc) return rc;

    g_futures[slot].gtid  = tid;
    g_futures[slot].ready = 0;

    f->h = (uint64_t)slot;
    return 0;
}

int leo_await(leo_future_t f, void *result_buf, size_t result_len)
{
    (void)result_buf; (void)result_len; /* not used in this prototype */
    int slot = (int)f.h;
    if (slot < 0 || slot >= FUTURE_MAX) return -1;
    leo_thread_t tid = g_futures[slot].gtid;
    if (!tid) return -1;

    int rc = leo_thread_join((leothread_t)tid, NULL);
    if (rc == 0) {
        g_futures[slot].ready = 1;
        g_futures[slot].gtid  = 0;
    }
    return rc;
}

int leo_future_ready(leo_future_t f)
{
    int slot = (int)f.h;
    if (slot < 0 || slot >= FUTURE_MAX) return -1;
    return g_futures[slot].ready ? 1 : 0;
}
