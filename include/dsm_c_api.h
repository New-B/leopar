#ifndef DSM_C_API_H
#define DSM_C_API_H

#include <stdint.h>
#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef uint64_t GAddr;
typedef uint64_t Size;
typedef int32_t  Node;

/* Pure-C mirror of GAM::Conf */
typedef struct dsm_conf_c {
    int         is_master;           /* non-zero => master */
    int         master_port;
    const char* master_ip;           /* nullable -> default */
    const char* master_bindaddr;     /* nullable -> default */

    int         worker_port;
    const char* worker_ip;           /* nullable -> default */
    const char* worker_bindaddr;     /* nullable -> default */

    uint64_t    size;                /* bytes per-server pre-alloc */
    uint64_t    ghost_th;
    double      cache_th;
    int         unsynced_th;
    double      factor;
    int         maxclients;
    int         maxthreads;
    int         backlog;
    int         loglevel;            /* follows GAM’s LOG_* ints */
    const char* logfile;             /* nullable */
    int         timeout_ms;          /* ms */
    int         eviction_period_ms;  /* ms */
} dsm_conf_c;

/* 返回 0 成功，<0 失败 */
int   dsm_init_c(const dsm_conf_c* c);
GAddr dsm_malloc_c(Size size, Node nid);
int   dsm_read_c(GAddr addr, void* buf, Size count);
int   dsm_write_c(GAddr addr, const void* buf, Size count);
int   dsm_free_c(GAddr addr);
void  dsm_finalize_c(void);

#ifdef __cplusplus
} /* extern "C" */
#endif

#endif /* DSM_C_API_H */
