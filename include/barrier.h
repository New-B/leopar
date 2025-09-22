/**
 * @file barrier.h
 * @author Wang Bo
 * @date 2025-09-20
 * @brief Phased barriers for LeoPar init & runtime (TCP pre-UCX, UCX post-EP).
 */
#ifndef LEO_BARRIER_H
#define LEO_BARRIER_H

#include "tcp.h"    /* tcp_config_t */
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* One shot TCP barrier coordinated by rank0 before UCX endpoints are ready.
* Returns 0 on success, <0 on error/timeout.
*/
int tcp_barrier(const tcp_config_t *cfg, int my_rank, int world_size,
            int stage_id, int timeout_ms);

/* UCX-based centralized barrier coordinated by rank0 after endpoints exist.
* Returns 0 on success, <0 on error/timeout.
*/
int ucx_barrier(int stage_id, int timeout_ms);

#ifdef __cplusplus
}
#endif
#endif /* LEO_BARRIER_H */
