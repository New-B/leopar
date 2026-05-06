/**
 * @file threadtable.c
 * @author Wang Bo
 * @date 2025-09-13
 * @brief Implementation of local thread table management for LeoPar runtime.
 */

#include "threadtable.h"
#include "log.h"
#include "proto.h"
#include "ucx.h"
#include "context.h"

#include <errno.h>
#include <inttypes.h>
#include <string.h>
#include <stdlib.h>
#include <time.h>

typedef struct {
    int local_tid;
} thread_start_arg_t;

static void threadtable_reset_slot(local_thread_t *t)
{
    t->thread = 0;
    t->in_use = 0;
    t->finished = 0;
    t->state = LEO_THREAD_SLOT_FREE;
    t->waiters = NULL;
    t->task_id = -1;
    t->arg_buf = NULL;
    t->arg_len = 0;
    t->local_tid = -1;
    t->gtid = 0;
    t->creator_rank = -1;
    t->start_routine = NULL;
    t->retval = NULL;
}

static void notify_remote_waiters(int local_tid, uint64_t gtid, join_waiter_t *waiters)
{
    msg_join_resp_t resp;
    resp.opcode = OP_JOIN_RESP;
    resp.gtid   = gtid;
    resp.done   = 1;

    while (waiters) {
        join_waiter_t *next = waiters->next;
        if (ucx_send_bytes((int)waiters->src_rank, &resp, sizeof(resp), OP_JOIN_RESP) != 0) {
            log_warn("Failed to send JOIN_RESP for gtid=%" PRIu64 " to rank=%u",
                     gtid, waiters->src_rank);
        }
        free(waiters);
        waiters = next;
    }

    log_debug("Thread slot %d notified remote waiters for gtid=%" PRIu64, local_tid, gtid);
}

static void* thread_start_main(void *vp)
{
    thread_start_arg_t *tsa = (thread_start_arg_t*)vp;
    const int local_tid = tsa->local_tid;
    free(tsa);

    local_thread_t *slot = threadtable_get(local_tid);
    if (!slot) return NULL;

    pthread_mutex_lock(&slot->mu);
    slot->state = LEO_THREAD_RUNNING;
    void *(*fn)(void*) = slot->start_routine;
    void *arg = slot->arg_buf;
    pthread_mutex_unlock(&slot->mu);

    void *retval = NULL;
    if (fn) retval = fn(arg);

    pthread_mutex_lock(&slot->mu);
    slot->retval = retval;
    slot->finished = 1;
    slot->state = LEO_THREAD_FINISHED;
    join_waiter_t *waiters = slot->waiters;
    slot->waiters = NULL;
    const int creator_rank = slot->creator_rank;
    const uint64_t gtid = slot->gtid;
    pthread_cond_broadcast(&slot->cv);
    pthread_mutex_unlock(&slot->mu);

    const int had_waiters = (waiters != NULL);
    notify_remote_waiters(local_tid, gtid, waiters);

    if (creator_rank >= 0 && creator_rank != g_ctx.rank && had_waiters) {
        threadtable_reclaim(local_tid);
    }
    return NULL;
}

/* Global local thread table */
local_thread_t g_local_threads[MAX_LOCAL_THREADS];

/* Initialize all thread slots */
int threadtable_init(void)
{
    for (int i = 0; i < MAX_LOCAL_THREADS; i++) {
        threadtable_reset_slot(&g_local_threads[i]);
        g_local_threads[i].local_tid = i;
        pthread_mutex_init(&g_local_threads[i].mu, NULL);
        pthread_cond_init(&g_local_threads[i].cv, NULL);
    }
    log_info("Thread table initialized with %d slots", MAX_LOCAL_THREADS);
    return 0; /* always success for now */
}

/* Ensure all threads are joined before shutdown */
void threadtable_finalize(void)
{
    for (int i = 0; i < MAX_LOCAL_THREADS; i++) {
        if (g_local_threads[i].in_use) {
            (void)threadtable_wait_local(i, NULL, -1);
        }
        pthread_mutex_destroy(&g_local_threads[i].mu);
        pthread_cond_destroy(&g_local_threads[i].cv);
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
            threadtable_reset_slot(&g_local_threads[i]);
            g_local_threads[i].in_use = 1;
            g_local_threads[i].finished = 0;
            g_local_threads[i].local_tid = i;
            g_local_threads[i].state = LEO_THREAD_CREATED;
            return i;
        }
    }
    return -1;
}

int threadtable_spawn(int local_tid,
                      void *(*start_routine)(void*),
                      void *arg,
                      size_t arg_len,
                      uint64_t gtid,
                      int creator_rank)
{
    local_thread_t *slot = threadtable_get(local_tid);
    if (!slot || !start_routine) return -1;

    pthread_mutex_lock(&slot->mu);
    slot->start_routine = start_routine;
    slot->arg_buf = arg;
    slot->arg_len = arg_len;
    slot->gtid = gtid;
    slot->creator_rank = creator_rank;
    slot->state = LEO_THREAD_CREATED;
    pthread_mutex_unlock(&slot->mu);

    thread_start_arg_t *tsa = (thread_start_arg_t*)malloc(sizeof(*tsa));
    if (!tsa) return -1;
    tsa->local_tid = local_tid;

    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);

    int rc = pthread_create(&slot->thread, &attr, thread_start_main, tsa);
    pthread_attr_destroy(&attr);
    if (rc != 0) {
        free(tsa);
        threadtable_reclaim(local_tid);
        errno = rc;
        return -1;
    }
    return 0;
}

int threadtable_wait_local(int local_tid, void **retval, int64_t timeout_ms)
{
    local_thread_t *slot = threadtable_get(local_tid);
    if (!slot || !slot->in_use) return -1;

    pthread_mutex_lock(&slot->mu);
    if (timeout_ms < 0) {
        while (!slot->finished) {
            pthread_cond_wait(&slot->cv, &slot->mu);
        }
    } else {
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_sec += timeout_ms / 1000;
        ts.tv_nsec += (long)(timeout_ms % 1000) * 1000000L;
        if (ts.tv_nsec >= 1000000000L) {
            ts.tv_sec++;
            ts.tv_nsec -= 1000000000L;
        }

        int rc = 0;
        while (!slot->finished && rc == 0) {
            rc = pthread_cond_timedwait(&slot->cv, &slot->mu, &ts);
        }
        if (!slot->finished) {
            pthread_mutex_unlock(&slot->mu);
            return -ETIMEDOUT;
        }
    }

    if (retval) *retval = slot->retval;
    pthread_mutex_unlock(&slot->mu);
    return threadtable_reclaim(local_tid);
}

int threadtable_reclaim(int local_tid)
{
    local_thread_t *slot = threadtable_get(local_tid);
    if (!slot) return -1;

    pthread_mutex_lock(&slot->mu);
    join_waiter_t *w = slot->waiters;
    slot->waiters = NULL;
    threadtable_reset_slot(slot);
    slot->local_tid = local_tid;
    pthread_mutex_unlock(&slot->mu);

    while (w) {
        join_waiter_t *next = w->next;
        free(w);
        w = next;
    }
    return 0;
}
