/**
 * @file batch.c
 * @author Wang Bo
 * @date 2025-09-20
 * @brief parallel_for，Batch spawn and wait helpers on top of LeoPar threads.
 */

#include "leopar.h"
#include "log.h"

#include <stdlib.h>
#include <string.h>

/* ---- helper parallel_for sugar ---- */
typedef struct {
    size_t begin, end, grain;
    void (*body)(size_t, void*);
    void *ctx;
} pf_ctx_t;

static void* pf_worker(void *vp) {
    pf_ctx_t p = *(pf_ctx_t*)vp;
    for (size_t i = p.begin; i < p.end; ++i) {
        p.body(i, p.ctx);
    }
    return NULL;
}


/* ===== tiny parallel_for sugar ===== */
int leo_parallel_for(size_t begin, size_t end, size_t grain,
                     void (*body)(size_t i, void *ctx), void *ctx,
                     int target_rank_hint)
{
    if (end <= begin) return 0;
    if (grain == 0) grain = 1;

    const size_t total   = end - begin;
    const size_t nchunks = (total + grain - 1) / grain;

    leo_thread_t *tids = (leo_thread_t*)malloc(sizeof(leo_thread_t) * nchunks);
    if (!tids) return -1;

    size_t b = begin;
    for (size_t c = 0; c < nchunks; ++c) {
        size_t e = b + grain; if (e > end) e = end;
        pf_ctx_t arg = { .begin=b, .end=e, .grain=grain, .body=body, .ctx=ctx };
        int rc = leo_thread_create(&tids[c], NULL, pf_worker, &arg, target_rank_hint);
        if (rc) { free(tids); return rc; }
        b = e;
    }

    int rc_all = 0;
    for (size_t c = 0; c < nchunks; ++c) {
        int rc = leo_thread_join(tids[c], NULL);
        if (rc && rc_all == 0) rc_all = rc;
    }
    free(tids);
    return rc_all;
}

int leo_spawn_many(size_t n,
                leo_thread_t *tids,
                void *(*fns[])(void*),
                void *args[],
                const int target_ranks[])
{
    if (!tids || !fns || n == 0) return -1;
    for (size_t i = 0; i < n; ++i) {
        int tr = (target_ranks ? target_ranks[i] : -1);
        int rc = leo_thread_create((leothread_t*)&tids[i], NULL, fns[i], (args ? args[i] : NULL), tr);
        if (rc) return rc;
    }
    return 0;
}

int leo_wait_all(size_t n, const leo_thread_t *tids)
{
    if (!tids) return -1;
    int rc_all = 0;
    for (size_t i = 0; i < n; ++i) {
        int rc = leo_thread_join((leothread_t)tids[i], NULL);
        if (rc && rc_all == 0) rc_all = rc;
    }
    return rc_all;
}

int leo_wait_any(size_t n, const leo_thread_t *tids, size_t *index_ready)
{
    if (!tids || n == 0 || !index_ready) return -1;

    /* Minimal prototype: linearly try join with short timeouts. */
    for (;;) {
        for (size_t i = 0; i < n; ++i) {
            int rc = leo_thread_join_timeout((leothread_t)tids[i], NULL, /*ms*/1);
            if (rc == 0) { *index_ready = i; return 0; }
        }
        /* Backoff – in a real impl use a completion queue to avoid spinning. */
    }
}
