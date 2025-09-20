/**
 * @file attr.c
 * @author Wnag Bo
 * @date 2025-09-20
 * @brief Scheduler hints (attributes) and named create wrapper.
 */

#include "leopar.h"

int leo_attr_init(leo_attr_t *a)
{
    if (!a) return -1;
    a->target_rank = -1;
    a->priority    = 0;
    a->locality_key= 0;
    return 0;
}

int leo_thread_create_attr_named(leo_thread_t *thread,
                                const leo_attr_t *attr,
                                void *(*start_routine)(void*),
                                const char *func_name,
                                void *arg)
{
    int tr = (attr ? attr->target_rank : -1);
    /* In a later revision, priority/locality_key can be passed to scheduler */
    return leo_thread_create_named((leothread_t*)thread, NULL, start_routine, func_name, arg, tr);
}
