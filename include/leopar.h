/**
 * @file leopar.h
 * @author Wang Bo
 * @date 2025-09-09
 * @brief Public API for LeoPar: a parallel programming model runtime prototype.
 */

#ifndef LEOPAR_H
#define LEOPAR_H

#include <pthread.h>

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




// /* ---------- Task registry (function table) ----------
//  * Users must register functions they want to call remotely.
//  * The function signature is: void (*fn)(const void *buf, size_t len)
//  * - Arguments should be serialized by the caller into a byte buffer.
//  * - Return values are not supported in this minimal prototype.
//  */
// typedef void (*leo_task_fn)(const void *arg_buf, size_t arg_len);

// /* Register a task function and get a positive task_id, or -1 on error. */
// int  leo_register_task(const char *name, leo_task_fn fn);


// /* Create a "thread" on target_rank executing the registered task (task_id) with arg buffer. */
// int leo_thread_create(leo_thread_t *thread,
//         const pthread_attr_t *attr,
//         void *(*start_routine)(void*),
//         void *arg,
//         int target_rank /* -1 = auto */);


// /* Task management */
// int leotask_spawn(void (*func)(void*), void *arg);

// /* Parallel for loop */
// int leofor(long start, long end, long chunk,
//         void (*func)(long, void*), void *arg);

#ifdef __cplusplus
}
#endif
        
#endif /* LEOPAR_H */

