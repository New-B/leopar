/**
 * @file team.c
 * @author Wang Bo
 * @date 2025-09-20
 * @brief Team (subset) creation and barrier over UCX control plane.
 */
#define _POSIX_C_SOURCE 200809L
#include "leopar.h"
#include "context.h"
#include "proto.h"
#include "ucx.h"
#include "dispatcher.h"
#include "log.h"

#include <time.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <errno.h>
#include <unistd.h>


/* Deterministic 64-bit FNV-1a hash for team ranks */
static uint64_t fnv1a64(const void *data, size_t len) {
    const uint8_t *p = (const uint8_t*)data;
    uint64_t h = 1469598103934665603ull;
    for (size_t i = 0; i < len; ++i) {
        h ^= p[i];
        h *= 1099511628211ull;
    }
    return h;
}

typedef struct {
    uint64_t id;
    int     *ranks;
    int      nranks;
    int      leader;    /* min rank in team */
    uint32_t epoch;
} team_rec_t;

#ifndef TEAM_MAX
#define TEAM_MAX 256
#endif
static team_rec_t g_teams[TEAM_MAX];

static int team_slot_alloc(void) {
    for (int i = 0; i < TEAM_MAX; ++i) if (g_teams[i].id == 0) return i;
    return -1;
}

static int team_slot_find(uint64_t id) {
    for (int i = 0; i < TEAM_MAX; ++i) if (g_teams[i].id == id) return i;
    return -1;
}

int leo_team_create(leo_team_t *team, const int *ranks, int nranks)
{
    if (!team || !ranks || nranks <= 0) return -1;

    /* Copy and sort ranks to ensure deterministic id. */
    int *rs = (int*)malloc(sizeof(int)*nranks);
    if (!rs) return -1;
    memcpy(rs, ranks, sizeof(int)*nranks);
    for (int i = 0; i+1 < nranks; ++i) {
        for (int j = i+1; j < nranks; ++j) if (rs[j] < rs[i]) { int t=rs[i]; rs[i]=rs[j]; rs[j]=t; }
    }
    uint64_t id = fnv1a64(rs, sizeof(int)*nranks);

    int slot = team_slot_alloc();
    if (slot < 0) { free(rs); return -1; }

    g_teams[slot].id     = id;
    g_teams[slot].ranks  = rs;
    g_teams[slot].nranks = nranks;
    g_teams[slot].leader = rs[0];
    g_teams[slot].epoch  = 0;

    team->id = id;
    return 0;
}

int leo_team_destroy(leo_team_t team)
{
    int slot = team_slot_find(team.id);
    if (slot < 0) return -1;
    free(g_teams[slot].ranks);
    memset(&g_teams[slot], 0, sizeof(g_teams[slot]));
    return 0;
}

static int in_team(const team_rec_t *t, int rank)
{
    for (int i = 0; i < t->nranks; ++i) if (t->ranks[i] == rank) return 1;
    return 0;
}

int leo_team_barrier(leo_team_t team) { return leo_team_barrier_timeout(team, /*ms*/-1); }

int leo_team_barrier_timeout(leo_team_t team, int64_t timeout_ms)
{
    int slot = team_slot_find(team.id);
    if (slot < 0) return -1;

    team_rec_t *T = &g_teams[slot];
    if (!in_team(T, g_ctx.rank)) return -1; /* not a member */

    const uint32_t epoch = ++T->epoch;
    const int leader = T->leader;

    if (g_ctx.world_size <= 1 || T->nranks == 1) return 0;

    unsigned long long t0 = 0;
    if (timeout_ms >= 0) {
        struct timespec ts; clock_gettime(CLOCK_REALTIME, &ts);
        t0 = (unsigned long long)ts.tv_sec * 1000ull + (unsigned long long)ts.tv_nsec / 1000000ull;
    }

    if (g_ctx.rank != leader) {
        /* Non-leader: ARRIVE to leader, wait RELEASE */
        msg_tb_arrive_t a = { .opcode = OP_TB_ARRIVE, .epoch = epoch, .team_id = team.id, .sender_rank = g_ctx.rank };
        if (ucx_send_bytes(leader, &a, sizeof(a), OP_TB_ARRIVE) != 0) return -1;

        for (;;) {
            size_t len=0; ucp_tag_t tag=0; ucp_tag_recv_info_t info;
            void *buf = ucx_recv_any_alloc(&len, &tag, &info);
            if (!buf) { dispatcher_progress_once(); usleep(1000); goto check_to; }

            uint32_t opcode = (uint32_t)(tag >> 32);
            uint32_t src    = (uint32_t)(tag & 0xffffffffu);
            if (opcode == OP_TB_RELEASE && src == (uint32_t)leader && len >= sizeof(msg_tb_release_t)) {
                msg_tb_release_t *rel = (msg_tb_release_t*)buf;
                if (rel->team_id == team.id && rel->epoch == epoch) { free(buf); return 0; }
            }
            dispatch_msg(buf, len, tag);
            free(buf);

        check_to:
            if (timeout_ms >= 0) {
                struct timespec ts; clock_gettime(CLOCK_REALTIME, &ts);
                unsigned long long now = (unsigned long long)ts.tv_sec * 1000ull + (unsigned long long)ts.tv_nsec / 1000000ull;
                if ((int64_t)(now - t0) > timeout_ms) return -ETIMEDOUT;
            }
        }

    } else {
        /* Allocate a small seen[] bitmap for team members (excluding leader). */
        int *seen = (int*)calloc(T->nranks, sizeof(int));
        if (!seen) return -1;

        /* Leader: gather ARRIVE from team-1 members, then RELEASE */
        int arrived = 0;
        while (arrived < T->nranks - 1) {
            ucp_worker_h w = g_ctx.ucx_ctx.ucp_worker;
            ucp_tag_message_h msg = ucp_tag_probe_nb(
                w,
                ((uint64_t)OP_TB_ARRIVE) << 32,
                0xffffffff00000000ull,
                0, NULL);
            if (!msg) { dispatcher_progress_once(); usleep(1000); goto check_to; }

            msg_tb_arrive_t a;
            ucp_request_param_t prm; memset(&prm, 0, sizeof(prm));
            prm.op_attr_mask = UCP_OP_ATTR_FIELD_DATATYPE;
            prm.datatype     = ucp_dt_make_contig(1);

            ucs_status_ptr_t r = ucp_tag_msg_recv_nbx(w, &a, sizeof(a), msg, &prm);
            if (UCS_PTR_IS_PTR(r)) {
                ucs_status_t st;
                do { ucp_worker_progress(w); st = ucp_request_check_status(r); } while (st == UCS_INPROGRESS);
                ucp_request_free(r);
            }
            /* Validate and de-duplicate by sender_rank */
            if (a.epoch == epoch && a.team_id == team.id) {
                /* find sender in team ranks */
                int idx = -1;
                for (int i = 0; i < T->nranks; ++i) {
                    if (T->ranks[i] == (int)a.sender_rank) { idx = i; break; }
                }
                if (idx > 0 && !seen[idx]) { /* exclude leader (idx==0) */
                    seen[idx] = 1;
                    ++arrived;
                }
            }
        check_to:
            if (timeout_ms >= 0) {
                struct timespec ts; clock_gettime(CLOCK_REALTIME, &ts);
                unsigned long long now = (unsigned long long)ts.tv_sec * 1000ull + (unsigned long long)ts.tv_nsec / 1000000ull;
                if ((int64_t)(now - t0) > timeout_ms) return -ETIMEDOUT;
            }
        }
        free(seen);

        /* send RELEASE to all team members */
        msg_tb_release_t rel = { .opcode = OP_TB_RELEASE, .epoch = epoch, .team_id = team.id };
        for (int i = 1; i < T->nranks; ++i) {
            (void)ucx_send_bytes(T->ranks[i], &rel, sizeof(rel), OP_TB_RELEASE);
        }
        return 0;
    }
}
