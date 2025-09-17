/**
 * @file tcp.h
 * @author Wang Bo
 * @date 2025-09-13
 * @brief tcp settings for LeoPar distributed runtime.
 *
 */
#ifndef TCP_H
#define TCP_H

#include <stddef.h>
#include <stdint.h>
#include <ucp/api/ucp.h>

// #include "context.h"

/* Limits for config (keep aligned with context.h) */
#ifndef TCP_MAX_RANKS
#define TCP_MAX_RANKS    8192
#endif
#ifndef TCP_SECRET_MAX
#define TCP_SECRET_MAX   256
#endif
#define TCP_MAX_LINE     1024   /* 可选，配置解析时用 */

typedef struct tcp_config_t{
    int   world_size;                 /* total number of processes */
    int   base_port;                  /* base TCP port */
    int   connect_timeout_ms;         /* connect timeout in ms */
    int   io_timeout_ms;              /* I/O timeout in ms */
    int   retry_max;                  /* maximum retries */
    char  secret[TCP_SECRET_MAX];     /* shared secret token */
    char  ip_of_rank[TCP_MAX_RANKS][64]; /* IPs for each rank */
} tcp_config_t;


/* Coordinator-based allgather of UCX worker addresses via TCP.
 * Rank 0 acts as a server: gathers all UCX worker addresses and then
 * broadcasts the full table back to every rank.
 *
 * On success:
 *   - *out_tbl_addrs points to an array (size = world_size) of ucp_address_t*
 *   - *out_tbl_lens  points to an array (size = world_size) of size_t lengths
 * Caller is responsible to free each out_tbl_addrs[i] and free the arrays.
 *
 * Returns 0 on success, nonzero otherwise.
 */
int tcp_allgather_ucx_addrs(const struct tcp_config_t *cfg,
    int my_rank, int world_size,
    const ucp_address_t *my_addr, size_t my_len,
    ucp_address_t ***out_tbl_addrs,
    size_t **out_tbl_lens);


/* TCP primitives */
int tcp_listen(int port);
int tcp_connect(const char *ip, int port);
int tcp_accept(int listen_fd);

#endif /* TCP_H */
