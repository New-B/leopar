/**
 * @file attr.c
 * @author Wnag Bo
 * @date 2025-09-20
 * @brief Scheduler hints (attributes) and named create wrapper.
 */

#include "leopar.h"
#include "scheduler.h"
#include <stdint.h>

int leo_attr_init(leo_attr_t *a)
{
    if (!a) return -1;
    a->target_rank = -1;
    a->priority    = 0;
    a->locality_key= UINT64_MAX;
    return 0;
}

int leo_thread_create_attr_named(leo_thread_t *thread,
                                const leo_attr_t *attr,
                                void *(*start_routine)(void*),
                                const char *func_name,
                                void *arg)
{
    int tr = -1;
    if (attr && attr->target_rank >= 0) {
        tr = attr->target_rank;
    } else if (attr) {
        tr = scheduler_choose_rank_hint(leo_world_size(), attr->locality_key, attr->priority);
    }
    return leo_thread_create_named(thread, NULL, start_routine, func_name, arg, tr);
}

int leo_thread_create_copy_attr_named(leo_thread_t *thread,
                                      const leo_attr_t *attr,
                                      void *(*start_routine)(void*),
                                      const char *func_name,
                                      const void *arg,
                                      size_t arg_len)
{
    int tr = -1;
    if (attr && attr->target_rank >= 0) {
        tr = attr->target_rank;
    } else if (attr) {
        tr = scheduler_choose_rank_hint(leo_world_size(), attr->locality_key, attr->priority);
    }
    return leo_thread_create_copy_named(thread, NULL, start_routine, func_name, arg, arg_len, tr);
}
