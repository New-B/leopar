/**
 * @file dsm.h
 * @brief Minimal RDMA-backed DSM for LeoPar (MVP): leo_malloc/read/write/free.
 *
 * Design:
 *  - Each rank maps a local arena (pool) with UCX mem_map, packs rkey.
 *  - Broadcast {base_vaddr, rkey_len, rkey_bytes} to all peers over UCX control plane.
 *  - leo_malloc() allocates from *local* arena (bump-pointer).
 *  - Global pointer encodes (owner_rank, offset).
 *  - leo_read/leo_write do UCX GET/PUT to owner_rank:(base+offset).
 *  - leo_free() is a no-op in MVP.
 */

#ifndef LEO_DSM_H
#define LEO_DSM_H

#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Global pointer: 16-bit owner, 48-bit offset (up to 256 TB pool space) */
typedef uint64_t leo_gaddr_t;

#define LEO_GADDR_MAKE(owner, off)  ((((uint64_t)(owner) & 0xffffull) << 48) | ((uint64_t)(off) & 0x0000FFFFFFFFFFFFull))
#define LEO_GADDR_OWNER(g)          ( (int)(((g) >> 48) & 0xffffull) )
#define LEO_GADDR_OFFSET(g)         ( (uint64_t)((g) & 0x0000FFFFFFFFFFFFull) )

/* Initialize DSM with a local arena size in bytes (e.g., from config).
* Returns 0 on success.
*/
int dsm_init(size_t local_pool_bytes);

/* Tear down DSM: destroy rkeys, unmap memory, free caches. */
void dsm_finalize(void);

/* Allocate n bytes from *local* arena and return global pointer. */
leo_gaddr_t leo_malloc(size_t n);

/* Free a global pointer (MVP: no-op). */
int leo_free(leo_gaddr_t g);

/* Read n bytes from global pointer g into local buffer dst. */
int leo_read(void *dst, leo_gaddr_t g, size_t n);

/* Write n bytes from local buffer src to global pointer g. */
int leo_write(leo_gaddr_t g, const void *src, size_t n);

/* NEW: dispatcher will call this when OP_DSM_ANN arrives */
void dsm_on_announce(const void *buf, size_t len, uint32_t src_rank);

/* NEW: dsm_init 在广播自身通告后调用它等待所有同伴通告就绪（超时 ms；<=0 表示无限等） */
int  dsm_wait_announces(int timeout_ms);

#ifdef __cplusplus
}
#endif
#endif /* LEO_DSM_H */
 