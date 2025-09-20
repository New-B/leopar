/**
 * @file leopar.h
 * @author Wang Bo
 * @date 2025-09-09
 * @brief Public API for LeoPar: a parallel programming model runtime prototype.
 *  
 * Additions in this revision:
 *  - Join with timeout
 *  - Global barrier
 *  - Futures (async/await) minimal variant (await == join semantics)
 *  - Batch spawn/wait (many)
 *  - Teams (subset barrier with deterministic team id)
 *  - Scheduler hints (attributes)
 *  - Runtime query/stats helpers
 */

#ifndef LEOPAR_H
#define LEOPAR_H

#include <pthread.h>
#include <stdint.h>
#include <errno.h>

#ifndef LEO_ETIMEDOUT
#define LEO_ETIMEDOUT (-ETIMEDOUT)
#endif

#ifdef __cplusplus
extern "C" {
#endif
/* Thread handle */
typedef void* leothread_t;
/* 
 * Global thread id encodes owner rank and local id: (owner_rank << 32) | local_id
 */
typedef unsigned long long leo_thread_t;

/* ---------- Runtime lifecycle ---------- */
/* Initialize LeoPar runtime.
 * @param config_path Path to cluster config file (INI)
 * @param rank        This process rank
 * @param log_path    Path to log file
 * @return 0 on success, -1 on error
 */
int leopar_init(const char *config_path, int rank, const char *log_path);

/* Finalize LeoPar runtime (destroy UCX endpoints, worker, context). */
void leopar_finalize(void);

/* ---- public pthread-like API (name-capturing macro) ----
 * Keep the user-facing API name `leo_thread_create`, but make it a macro
 * that captures the function name via `#fn` and forwards to the named
 * implementation. This is crucial for dynamic registration & remote lookup.
 */

/* Named variant implementation (real function in leopar.c) */
int leo_thread_create_named(leo_thread_t *thread,
        const pthread_attr_t *attr,
        void *(*start_routine)(void*),
        const char *func_name,
        void *arg,
        int target_rank);

/* User-facing macro: preserves pthread-style callsite, captures name string */
#define leo_thread_create(thread, attr, fn, arg, rank) \
        leo_thread_create_named(thread, attr, (void*(*)(void*))(fn), #fn, arg, rank)

/* Join a (possibly remote) thread; returns 0 when the remote execution has completed. */
int leo_thread_join(leo_thread_t thread, void **retval);

/* join with timeout (ms). Returns 0 on success, -ETIMEDOUT on timeout. */
int leo_thread_join_timeout(leo_thread_t thread, void **retval, int64_t timeout_ms);

/* global barrier across all ranks  */
int leo_barrier(void);

/* Optional timeout form (returns 0 or -ETIMEDOUT); may be implemented later. */
int leo_barrier_timeout(int64_t timeout_ms);

/* tiny parallel_for sugar built atop create/join */
int leo_parallel_for(size_t begin, size_t end, size_t grain,
                     void (*body)(size_t i, void *ctx), void *ctx,
                     int target_rank_hint /* -1=auto */);

/* ---------- NEW: Futures/Promises (minimal) ---------- */
/* Opaque future handle for a single remote thread result (join semantics). */
typedef struct { uint64_t h; } leo_future_t;

/* Spawn as a future (equivalent to leo_thread_create, but returns a future). */
int leo_async(leo_future_t *f,
              void *(*fn)(void*),
              void *arg,
              int target_rank /* -1=auto */);

/* Await completion (semantics == join). result_buf/result_len are reserved for future data-plane integration. */
int leo_await(leo_future_t f, void *result_buf, size_t result_len);

/* Non-blocking readiness probe: 1=ready, 0=not yet, <0=error. */
int leo_future_ready(leo_future_t f);

/* ---------- NEW: Batch spawn/wait (control-plane batching) ---------- */
int leo_spawn_many(size_t n,
                   leo_thread_t *tids,
                   void *(*fns[])(void*),
                   void *args[],
                   const int target_ranks[] /* NULL=>auto */);

int leo_wait_all(size_t n, const leo_thread_t *tids);
int leo_wait_any(size_t n, const leo_thread_t *tids, size_t *index_ready);

/* ---------- NEW: Teams (subset barrier) ---------- */
typedef struct { uint64_t id; } leo_team_t;

int leo_team_create(leo_team_t *team, const int *ranks, int nranks);
int leo_team_barrier(leo_team_t team);
int leo_team_barrier_timeout(leo_team_t team, int64_t timeout_ms);
int leo_team_destroy(leo_team_t team);

/* ---------- NEW: Scheduler hints (attributes) ---------- */
typedef struct {
    int      target_rank;   /* -1=auto placement */
    int      priority;      /* higher => earlier under pressure */
    uint64_t locality_key;  /* app-defined key for co-location */
} leo_attr_t;
    
int leo_attr_init(leo_attr_t *a);

/* Named variant to capture function name; user macro mirrors leo_thread_create style if needed. */
int leo_thread_create_attr_named(leo_thread_t *thread,
                                const leo_attr_t *attr,
                                void *(*start_routine)(void*),
                                const char *func_name,
                                void *arg);

#define leo_thread_create_attr(thread, attr, fn, arg) \
leo_thread_create_attr_named(thread, (attr), (void*(*)(void*))(fn), #fn, (arg))

/* ---------- NEW: Runtime query & stats ---------- */
int leo_world_size(void);  /* equals g_ctx.world_size */
int leo_rank(void);        /* equals g_ctx.rank */

typedef struct {
    uint64_t creates_sent, creates_recv;
    uint64_t joins_sent, joins_recv;
    uint64_t bytes_ctrl_tx, bytes_ctrl_rx; /* best-effort */
    uint64_t scheduler_rr, scheduler_hint, scheduler_locality;
} leo_stats_t;
    
    int leo_stats_get(leo_stats_t *s);
    
    /* Adjust logging level (implementation may forward to your log system). */
    int leo_set_log_level(int lvl);

#ifdef __cplusplus
}
#endif
        
#endif /* LEOPAR_H */

