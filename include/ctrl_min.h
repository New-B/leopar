/**
 * @file ctrl_min.h
 * @brief Minimal control-plane for LeoPar agents: blocking TCP barrier.
 *        - Coordinator (usually rank 0) runs a tiny blocking server.
 *        - Non-coordinators connect/send ARRIVE and block until RELEASE.
 *        - "Orderly exit" is just a final barrier named "shutdown".
 *
 * Usage:
 *   1) Call ctrlm_start(&cfg) early (coordinator will start a thread).
 *   2) At any sync point: ctrlm_barrier("stageX", gen, timeout_ms).
 *   3) Before quitting:   ctrlm_barrier("shutdown", gen, timeout_ms).
 *   4) Call ctrlm_stop() on exit (coordinator only; others are no-ops).
 */

#ifndef LEO_CTRLM_MIN_H
#define LEO_CTRLM_MIN_H

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/* Forward declare tcp_config_t so this header does not depend on context.h */
typedef struct tcp_config_t tcp_config_t;

typedef struct ctrlm_cfg_s {
    int   rank;                 /* this agent's rank */
    int   coordinator_rank;     /* usually 0; can be changed if needed */
    const tcp_config_t* tcp;    /* pointer to already-loaded cluster tcp config */

    /* Coordinator bind IP for the tiny server (NULL -> "0.0.0.0"). */
    const char* bind_ip;

    /* Optional hook: called after the final barrier and right before exiting. */
    void (*prepare_shutdown_cb)(void* user);
    void* prepare_shutdown_user;
} ctrlm_cfg_t;


/* Compile-time control-plane port selection: base_port + CTRL_PORT_DELTA. */
#ifndef CTRL_PORT_DELTA
#define CTRL_PORT_DELTA  190   /* keep it away from your data-plane */
#endif

/* Start/stop control plane (only coordinator actually starts a server thread). */
int  ctrlm_start();
void ctrlm_stop(void);

/* Named barrier across agents. gen is the round number (0,1,2...), for clarity. */
int  ctrlm_barrier(const char* name, uint64_t gen, uint32_t timeout_ms);

#ifdef __cplusplus
}
#endif

#endif /* LEO_CTRLM_MIN_H */
 