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

/* 前向声明，保持不透明即可 */
typedef struct Conf Conf;

/* 返回 0 成功，<0 失败 */
int   dsm_init_c(const Conf* c);
GAddr dsm_malloc_c(Size size, Node nid);
int   dsm_read_c(GAddr addr, void* buf, Size count);
int   dsm_write_c(GAddr addr, const void* buf, Size count);
int   dsm_free_c(GAddr addr);
void  dsm_finalize_c(void);

#ifdef __cplusplus
} /* extern "C" */
#endif

#endif /* DSM_C_API_H */
