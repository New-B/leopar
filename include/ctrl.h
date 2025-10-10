#ifndef CTRL_H
#define CTRL_H

/*
 * Lightweight control-plane with long-lived TCP session to rank0 (coordinator).
 * Provides global barriers across all ranks:
 *   - ctrl_start(): bring up server (rank0) or connect+handshake (others)
 *   - ctrl_barrier(name, gen, timeout_ms): cluster-wide barrier
 *   - ctrl_stop(): graceful teardown
 *
 * Dependencies:
 *   - context.h: g_ctx { rank, world_size, tcp_cfg{ base_port, retry_max, connect_timeout_ms, ip_of_rank[][] } }
 *   - log.h     : log_info/log_warn/log_error/log_debug
 */

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

int ctrl_start(void);
/* Return 0 on success; ETIMEDOUT on timeout; -1 on error */
int ctrl_barrier(const char* name, uint64_t gen, uint32_t timeout_ms);
void ctrl_stop(void);

#ifdef __cplusplus
}
#endif

#endif /* CTRL_H */
