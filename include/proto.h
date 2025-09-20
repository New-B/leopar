/**
 * @file proto.h
 * @author Wang Bo
 * @date 2025-09-13
 * @brief Protocol definitions for LeoPar distributed runtime.
 *
 * This header centralizes:
 *   - Operation codes (opcodes) for UCX message passing
 *   - Message structures used for create/join/exit communication
 * 
 * Notes:
 *   - Both UCX tag (hi32=opcode, lo32=rank) and struct field `opcode`
 *     carry the operation type, for robustness.
 */

#ifndef PROTO_H
#define PROTO_H

#include <stdint.h>

/* ---------- Operation Codes (UCX tag opcodes) ----------
* These values occupy the high 32 bits of a UCX tag.
* The low 32 bits are typically used for the sender rank.
*/
enum {
    OP_CREATE_REQ  = 1,   /* thread create request */
    OP_CREATE_ACK  = 2,   /* thread create acknowledgement */
    OP_EXIT_NOTIFY = 3,   /* notify creator that thread has exited */
    OP_JOIN_REQ    = 4,   /* join request */
    OP_JOIN_RESP   = 5,    /* join response */

    /* barrier control */
    OP_BARRIER_ARRIVE  = 6,   /* ranks -> rank0 */
    OP_BARRIER_RELEASE = 7,   /* rank0 -> ranks */

    /* function table sync */
    OP_FUNC_ANNOUNCE = 12,  /* broadcast {name_len, name, func_id} */

    /* ---- NEW: team (subset) barrier ---- */
    OP_TB_ARRIVE      = 20,  /* team members -> leader */
    OP_TB_RELEASE     = 21   /* leader -> team members */
};

/* ---------- Message Structures ----------
* All messages are defined as packed structs for network transport.
* NOTE: Arguments to tasks are serialized separately and appended after headers.
*/

#pragma pack(push, 1)

/* function announce broadcast
 * Payload: [header][name[name_len]]
 */
typedef struct {
    uint32_t opcode;     /* OP_FUNC_ANNOUNCE */
    uint32_t name_len;   /* bytes of function name (no trailing NUL required) */
    uint32_t func_id;    /* globally agreed function id */
} msg_func_announce_t;

/*  Thread creation request
 * Payload: [header][name[name_len]][arg[arg_len]]
 * Carry name so the remote can cold-start resolve & bind if needed.
 */
typedef struct {
    uint32_t opcode;        /* OP_CREATE_REQ */
    uint32_t creator_rank;  /* rank that requested creation */
    uint32_t func_id;       /* ID of the registered function */
    uint32_t arg_len;       /* size of argument buffer */
    uint32_t name_len;      /* NEW: size of function name (bytes) */
    uint64_t gtid;          /* global thread id (assigned by owner) */
    /* followed by arg_buf[arg_len] */
} msg_create_req_t;

/* Thread creation acknowledgement */
typedef struct {
    uint32_t opcode;        /* OP_CREATE_ACK */
    uint64_t gtid;          /* global thread id assigned by owner */
    int32_t  status;        /* 0 = ok, <0 = error */
} msg_create_ack_t;

/* Exit notification (optional) */
typedef struct {
    uint32_t opcode;        /* OP_EXIT_NOTIFY */
    uint64_t gtid;          /* global thread id that exited */
} msg_exit_notify_t;

/* Join request */
typedef struct {
    uint32_t opcode;        /* OP_JOIN_REQ */
    uint64_t gtid;          /* target global thread id */
} msg_join_req_t;

/* Join response */
typedef struct {
    uint32_t opcode;        /* OP_JOIN_RESP */
    uint64_t gtid;          /* same as request */
    int32_t  done;          /* 1 = completed */
} msg_join_resp_t;

/*  barrier arrive/release */
typedef struct {
    uint32_t opcode;        /* OP_BARRIER_ARRIVE */
    uint32_t epoch;         /* barrier generation id */
} msg_barrier_arrive_t;

typedef struct {
    uint32_t opcode;        /* OP_BARRIER_RELEASE */
    uint32_t epoch;         /* barrier generation id */
} msg_barrier_release_t;

typedef struct {
    uint32_t opcode;        /* OP_TB_ARRIVE */
    uint32_t epoch;         /* barrier generation id within the team */
    uint64_t team_id;       /* deterministic team id */
    uint32_t sender_rank;   /* NEW: rank of the arriving member */
} msg_tb_arrive_t;

typedef struct {
    uint32_t opcode;        /* OP_TB_RELEASE */
    uint32_t epoch;         /* barrier generation id within the team */
    uint64_t team_id;       /* deterministic team id */
} msg_tb_release_t;

#pragma pack(pop)

#endif /* PROTO_H */
