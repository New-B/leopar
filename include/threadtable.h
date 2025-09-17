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

/* Maximum number of local threads per rank */
#define MAX_LOCAL_THREADS 128

/* Local thread table entry */
typedef struct {
    pthread_t thread;           /* POSIX thread handle */
    int       in_use;        /* whether this slot is occupied */
    int       finished;      /* whether the thread finished */
    pthread_mutex_t m;       /* mutex for finished state */
    pthread_cond_t  cv;      /* condvar for finished state */
    int       task_id;       /* registered function id */
    void     *arg_buf;       /* serialized arguments buffer */
    size_t    arg_len;       /* argument size */
    int       local_tid;     /* index in local thread table */
    uint64_t  gtid;          /* global thread id (owner<<32 | local) */
    int       creator_rank;  /* creator rank (for exit notify) */
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

#endif /* THREADTABLE_H */