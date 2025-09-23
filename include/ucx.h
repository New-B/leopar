/**
 * ucx_tcp.h
 * Author: Your Name
 * Date: 2025-09-11
 *
 * UCX TCP bootstrap module for LeoPar runtime.
 * Features:
 *   - Load configuration from INI-style file
 *   - Ring allgather UCX worker addresses over TCP
 *   - Authentication with shared secret
 *   - Timeout and retry support
 *   - Create UCX endpoints for all peers
 */

#ifndef UCX_TCP_H
#define UCX_TCP_H

#include <stddef.h>
#include <ucp/api/ucp.h>

#ifdef __cplusplus
extern "C" {
#endif

/* UCX TCP context */
typedef struct {
    ucp_context_h ucp_context;
    ucp_worker_h  ucp_worker;
    ucp_ep_h     *eps;         /* array of endpoints, one per rank */
    int           world_size;
    int           rank;
} ucx_context_t;

typedef struct tcp_config_t tcp_config_t;   // forward declaration
/**
 * Initialize UCX context, worker, address exchange, and endpoints.
 * This replaces the old scattered UCX initialization logic.
 *
 * @param ucx   pointer to ucx_context_t (inside g_ctx)
 * @param cfg   pointer to cluster config (g_ctx.tcp_cfg)
 * @param rank  this process's rank
 * @return 0 on success, <0 on error
 */
int ucx_init(ucx_context_t *ucx, tcp_config_t *cfg, int rank);

/* Finalize UCX (EPs, worker, context). Safe to call multiple times. */
int ucx_finalize(ucx_context_t *ucx);

/* Send a contiguous byte buffer to dest_rank with (hi32=opcode, lo32=src).
 * Returns 0 on success, nonzero on error. */
int ucx_send_bytes(int dest_rank, const void *buf, size_t len, int opcode);

/* Receive ANY incoming tag message (blocking). Allocates a buffer and
 * returns it; caller must free() it. Fills out_len, out_tag, out_info if
 * not NULL. */
void *ucx_recv_any_alloc(size_t *out_len, ucp_tag_t *out_tag, ucp_tag_recv_info_t *out_info);

/* Best-effort broadcast to all ranks except self; returns 0 if all sends
 * succeeded, nonzero if any send failed. */
int ucx_broadcast_bytes(const void *buf, size_t len, int opcode);


/* === NEW: simple contiguous RDMA helpers ===
 * remote_addr must be the peer's base + offset (UCS_PTR_BYTE_OFFSET style).
 * rkey is the peer's unpacked rkey for its arena.
 */
 int ucx_put_block(const void *src, size_t len, int dst_rank,
    uint64_t remote_addr, ucp_rkey_h rkey);

int ucx_get_block(void *dst, size_t len, int src_rank,
    uint64_t remote_addr, ucp_rkey_h rkey);



/* Create UCX endpoints for all peers */
int ucx_tcp_create_all_eps(ucp_context_h context,
                        ucp_worker_h worker,
                        int my_rank, int world_size,
                        ucp_address_t **tbl_addrs,
                        size_t *tbl_lens,
                        ucp_ep_h *eps_out);

#ifdef __cplusplus
}
#endif

#endif /* UCX_TCP_H */
 