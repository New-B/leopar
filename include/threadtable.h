/**
 * @file threadtable.h
 * @author Wang Bo
 * @date 2025-09-13
 * @brief Internal local thread table management for LeoPar runtime.
 *
 * NOTE: This header is only for runtime-internal use.
 *       User applications should not include this file directly.
 */

 #ifndef THREADTABLE_H
 #define THREADTABLE_H

#include <pthread.h>
#include <stdint.h>
#include <stddef.h>
#include <sys/types.h>

/* Maximum number of local threads per rank */
#define MAX_LOCAL_THREADS 128

typedef enum {
    LEO_THREAD_SLOT_FREE = 0,
    LEO_THREAD_CREATED,
    LEO_THREAD_RUNNING,
    LEO_THREAD_FINISHED
} leo_thread_state_t;

typedef struct join_waiter {
    uint32_t src_rank;
    struct join_waiter* next;
} join_waiter_t;

/* Local thread table entry */
typedef struct {
    pthread_t thread;           /* POSIX thread handle */
    int       in_use;        /* whether this slot is occupied */
    int       finished;      /* whether the thread finished */
    leo_thread_state_t state; /* lifecycle state managed by LeoPar */
    join_waiter_t *waiters;   /* ranks waiting for remote completion */
    pthread_mutex_t mu;       /* mutex for finished state */
    pthread_cond_t  cv;      /* condvar for finished state */
    int       task_id;       /* registered function id */
    void     *arg_buf;       /* serialized arguments buffer */
    size_t    arg_len;       /* argument size */
    int       local_tid;     /* index in local thread table */
    uint64_t  gtid;          /* global thread id (owner<<32 | local) */
    int       creator_rank;  /* creator rank (for exit notify) */
    void *(*start_routine)(void*);
    void     *retval;        /* cached thread result */
} local_thread_t;



/* Global thread table */
extern local_thread_t g_local_threads[MAX_LOCAL_THREADS];

/* Initialize all thread slots. Return 0 on success, -1 on error */
int threadtable_init(void);

/* Finalize thread table: join all active threads */
void threadtable_finalize(void);

/* Internal API */
local_thread_t* threadtable_get(int local_tid);
int threadtable_alloc(void);
int threadtable_spawn(int local_tid,
                      void *(*start_routine)(void*),
                      void *arg,
                      size_t arg_len,
                      uint64_t gtid,
                      int creator_rank);
int threadtable_wait_local(int local_tid, void **retval, int64_t timeout_ms);
int threadtable_reclaim(int local_tid);

#endif /* THREADTABLE_H */
