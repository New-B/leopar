/**
 * @file dsm.c
 * @brief Minimal RDMA-backed DSM implementation over UCX RMA (GET/PUT).
 *
 * Notes:
 *  - This MVP assumes homogeneous arch and virtual addressing (x86_64).
 *  - Coherence is not implemented; operations are explicit and blocking.
 *  - Writes use ucp_put_nbx then flush to ensure remote visibility on return.
 *  - We exchange a single arena per rank; all allocations are offsets in it.
 */

#define _POSIX_C_SOURCE 200809L
#include "dsm.h"
#include "context.h"
#include "ucx.h"
#include "proto.h"
#include "dispatcher.h"
#include "log.h"

#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <pthread.h>
#include <inttypes.h>
#include <unistd.h>

#ifndef CLOCK_REALTIME
#define CLOCK_REALTIME 0
#endif

/* ---- Control-plane message for DSM rkey announce ---- */
typedef struct {
    uint32_t opcode;     /* custom: OP_DSM_ANN = 30 (pick a free code) */
    uint32_t rkey_len;   /* bytes following */
    uint64_t base_addr;  /* remote virtual address of arena base */
} msg_dsm_ann_t;


/* ---------- Ready-count and CV for announce collection ---------- */
static pthread_mutex_t g_ann_mtx = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t  g_ann_cv  = PTHREAD_COND_INITIALIZER;
/* number of peers (excluding self) whose rkey/base have been received */
static int g_peers_ready = 0;

/* ---- DSM global state ---- */
typedef struct {
    /* Local arena */
    void        *arena_base;     /* local mapped base */
    size_t       arena_bytes;    /* size of local arena */
    size_t       bump;           /* bump-pointer offset */
    pthread_mutex_t alloc_mtx;   /* protects bump-pointer */

    /* UCX memory handle and rkey */
    ucp_mem_h    memh;           /* UCX mem handle for local arena */
    void        *packed_rkey;    /* bytes to send to peers */
    size_t       packed_len;     /* length of packed_rkey */

    /* Per-peer remote base and rkey handle */
    uint64_t    *peer_base;      /* remote virtual base address per rank */
    ucp_rkey_h  *peer_rkey;      /* unpacked rkey handle per rank */
    unsigned char *have_peer;    /* bitmap: received rkey from that peer */
} dsm_state_t;

static dsm_state_t g_dsm;

/* Utility: timespec add milliseconds */
static void _deadline_from_now(struct timespec *ts, int timeout_ms) {
    clock_gettime(CLOCK_REALTIME, ts);
    if (timeout_ms <= 0) return;
    ts->tv_sec  += timeout_ms / 1000;
    ts->tv_nsec += (timeout_ms % 1000) * 1000000L;
    if (ts->tv_nsec >= 1000000000L) { ts->tv_sec += 1; ts->tv_nsec -= 1000000000L; }
}


/* ---------- Called by dispatcher upon receiving OP_DSM_ANN ---------- */
void dsm_on_announce(const void *buf, size_t len, uint32_t src_rank)
{
    if (len < sizeof(msg_dsm_ann_t)) {
        log_error("DSM: ANN too short from %u", src_rank);
        return;
    }
    const msg_dsm_ann_t *mh = (const msg_dsm_ann_t*)buf;
    if (mh->opcode != OP_DSM_ANN) {
        log_warn("DSM: ANN wrong opcode=%u from %u", mh->opcode, src_rank);
        return;
    }
    if (len < sizeof(*mh) + mh->rkey_len) {
        log_error("DSM: ANN malformed rkey_len from %u", src_rank);
        return;
    }
    if (src_rank >= (uint32_t)g_ctx.world_size) {
        log_error("DSM: ANN invalid src=%u", src_rank);
        return;
    }

    /* If already have this peer, ignore duplicates (idempotent) */
    if (g_dsm.have_peer[src_rank]) {
        log_debug("DSM: duplicate ANN from %u ignored", src_rank);
        return;
    }

    /* Unpack rkey for this endpoint */
    ucp_rkey_h rkey = NULL;
    const void *packed = (const void*)(mh + 1);
    ucs_status_t st = ucp_ep_rkey_unpack(g_ctx.ucx_ctx.eps[src_rank], (void*)packed, &rkey);
    if (st != UCS_OK) {
        log_error("DSM: rkey_unpack failed from %u", src_rank);
        return;
    }

    /* Publish into caches */
    g_dsm.peer_base[src_rank] = mh->base_addr;
    g_dsm.peer_rkey[src_rank] = rkey;
    g_dsm.have_peer[src_rank] = 1;

    /* Accounting */
    pthread_mutex_lock(&g_ann_mtx);
    g_peers_ready++;
    pthread_cond_broadcast(&g_ann_cv);
    pthread_mutex_unlock(&g_ann_mtx);

    log_info("DSM: got rkey from rank %u base=0x%llx len=%u\n",
             src_rank, (unsigned long long)mh->base_addr, mh->rkey_len);
}

/* ---------- Wait until all (world_size-1) peers announced or timeout ---------- */
int dsm_wait_announces(int timeout_ms)
{
    int target = g_ctx.world_size - 1; /* excluding self */
    pthread_mutex_lock(&g_ann_mtx);
    if (timeout_ms <= 0) {
        while (g_peers_ready < target) {
            pthread_cond_wait(&g_ann_cv, &g_ann_mtx);
        }
        pthread_mutex_unlock(&g_ann_mtx);
        return 0;
    } else {
        struct timespec dl; _deadline_from_now(&dl, timeout_ms);
        while (g_peers_ready < target) {
            int rc = pthread_cond_timedwait(&g_ann_cv, &g_ann_mtx, &dl);
            if (rc == ETIMEDOUT) { pthread_mutex_unlock(&g_ann_mtx); return -ETIMEDOUT; }
        }
        pthread_mutex_unlock(&g_ann_mtx);
        return 0;
    }
}

// /* ---- Helpers: UCX request wait ---- */
// static int ucx_wait(ucs_status_ptr_t req)
// {
//     if (req == NULL) return 0;
//     if (UCS_PTR_IS_ERR(req)) return UCS_PTR_STATUS(req) == UCS_OK ? 0 : -1;
//     while (1) {
//         ucs_status_t st = ucp_request_check_status(req);
//         if (st == UCS_OK) break;
//         if (st != UCS_INPROGRESS) { ucp_request_free(req); return -1; }
//         ucp_worker_progress(g_ctx.ucx_ctx.ucp_worker);
//     }
//     ucp_request_free(req);
//     return 0;
// }

/* dsm.c: a very small directory */
typedef struct {
    leo_gaddr_t gaddr;
    size_t      size;
    int         in_use;
    /* owner-side lock bookkeeping */
    pthread_rwlock_t rwlock;  /* for local owner fast-path */
    /* optional: wait queues / counters, simplified here */
} dsm_dir_entry_t;

#define DSM_DIR_MAX 65536
static dsm_dir_entry_t g_dir[DSM_DIR_MAX];
static pthread_mutex_t g_dir_mtx = PTHREAD_MUTEX_INITIALIZER;

static int dsm_dir_find_idx(leo_gaddr_t g)
{
    /* trivial O(N) for MVP; later use hashmap */
    for (int i=0;i<DSM_DIR_MAX;i++) if (g_dir[i].in_use && g_dir[i].gaddr==g) return i;
    return -1;
}
static int dsm_dir_insert(leo_gaddr_t g, size_t sz)
{
    for (int i=0;i<DSM_DIR_MAX;i++) if (!g_dir[i].in_use) {
        g_dir[i].gaddr=g; g_dir[i].size=sz; g_dir[i].in_use=1;
        pthread_rwlock_init(&g_dir[i].rwlock,NULL);
        return 0;
    }
    return -1;
}

/* Internal: perform remote allocation by RPC */
static int dsm_remote_alloc(int owner, size_t n, leo_gaddr_t *out)
{
    msg_dsm_alloc_req_t req = {
        .opcode     = OP_DSM_ALLOC_REQ,
        .size_bytes = (uint64_t)n,   // ← 用 size_bytes
        .align      = 64,
        .reserved   = 0
    };
    int rc = ucx_send_bytes(owner, &req, sizeof(req), OP_DSM_ALLOC_REQ);
    if (rc != 0) {
        log_error("ALLOC_REQ send failed to %d", owner);
        return rc;
    }
    log_debug("DSM: sent alloc request of %zu bytes to owner %d", n, owner);

    // 2) blocking recv RESP：精准匹配 opcode + from_rank
    msg_dsm_alloc_resp_t resp;
    rc = ucx_recv_blocking(OP_DSM_ALLOC_RESP, owner, &resp, sizeof(resp), /*timeout_ms*/10000);
    if (rc != 0) {
        log_error("ALLOC_RESP recv failed from %d, rc=%d", owner, rc);
        return rc;
    }
    if (resp.status != 0) {
        log_error("ALLOC_RESP status=%d from %d", resp.status, owner);
        return -1;
    }

    // 3) 返回全局地址（按你的结构体字段命名）
    *out = (leo_gaddr_t)resp.gaddr;  // 或根据 offset 拼 gaddr
    log_debug("ALLOC ok owner=%d gaddr=0x%llx", owner, (unsigned long long)*out);
    return 0;

}

/* Dispatcher handler: owner side alloc */
void dsm_on_alloc_req(const void *buf, size_t len, uint32_t src_rank)
{
    if (len < sizeof(msg_dsm_alloc_req_t)) {
        log_error("ALLOC_REQ too short len=%zu", len);
        return;
    }
    const msg_dsm_alloc_req_t *rq = (const msg_dsm_alloc_req_t*)buf;

    size_t need = (size_t)rq->size_bytes;
    size_t align= rq->align ? rq->align : 64;
    /* local bump allocator on owner */
    size_t off = 0;  /* obtain offset from your allocator */
    int    ok  = 0;
    pthread_mutex_lock(&g_dsm.alloc_mtx);
    size_t aligned = (g_dsm.bump + (align-1)) & ~(align-1);
    if (aligned + need <= g_dsm.arena_bytes) {
        off = aligned;
        g_dsm.bump = aligned + need;
        ok = 1;
        /* record metadata for lock mgmt: see 5.3 */
        dsm_dir_insert(LEO_GADDR_MAKE(g_ctx.rank, off), rq->size_bytes);
    }
    pthread_mutex_unlock(&g_dsm.alloc_mtx);

    msg_dsm_alloc_resp_t resp = {
        .opcode = OP_DSM_ALLOC_RESP,
        .status = ok ? 0 : -1,
        .gaddr  = ok ? (uint64_t)LEO_GADDR_MAKE(g_ctx.rank, off) : 0
    };
    int rc = ucx_send_bytes(src_rank, &resp, sizeof(resp), OP_DSM_ALLOC_RESP);
    if (rc != 0) {
        log_error("ALLOC_RESP send to %u failed rc=%d", src_rank, rc);
    } else {
        log_info("ALLOC_REQ from %u: size=%zu off=%zu status=%d",
                 src_rank, need, off, resp.status);
    }
}

void dsm_on_lock_req(const void *buf, size_t len, uint32_t src_rank)
{
    if (len < sizeof(msg_dsm_lock_req_t)) return;
    const msg_dsm_lock_req_t *rq = (const msg_dsm_lock_req_t*)buf;
    msg_dsm_lock_resp_t resp = { .opcode=OP_DSM_LOCK_RESP, .status = 0 };

    int idx = dsm_dir_find_idx(rq->gaddr);
    if (idx < 0) { resp.status = -2; goto send; }

    /* Owner local lock: use pthread rwlock to serialize */
    if (rq->mode == DSM_LOCK_SHARED) {
        if (pthread_rwlock_rdlock(&g_dir[idx].rwlock) != 0) resp.status = -1;
    } else {
        if (pthread_rwlock_wrlock(&g_dir[idx].rwlock) != 0) resp.status = -1;
    }

send:
    ucx_send_bytes(src_rank, &resp, sizeof(resp), OP_DSM_LOCK_RESP);
}

void dsm_on_unlock(const void *buf, size_t len, uint32_t src_rank)
{
    if (len < sizeof(msg_dsm_unlock_t)) return;
    const msg_dsm_unlock_t *rq = (const msg_dsm_unlock_t*)buf;
    int idx = dsm_dir_find_idx(rq->gaddr);
    if (idx < 0) return;
    (void)src_rank; /* no-op */

    pthread_rwlock_unlock(&g_dir[idx].rwlock);
}

/* Assumed existing globals:
 *  - g_dsm.memh (ucp_mem_h), g_dsm.peer_rkey[], g_dsm.peer_base[]
 *  - directory g_dir[], guarded by g_dir_mtx, with fields: in_use, gaddr, size, rwlock
 *  - helpers: dsm_dir_find_idx(leo_gaddr_t), etc.
 */
 void dsm_on_free_req(const void *buf, size_t len, uint32_t src_rank)
 {
     if (len < sizeof(msg_dsm_free_req_t)) {
         log_warn("DSM FREE_REQ too short from rank=%u", src_rank);
         return;
     }
     const msg_dsm_free_req_t *rq = (const msg_dsm_free_req_t*)buf;
 
     msg_dsm_free_resp_t resp;
     resp.opcode = OP_DSM_FREE_RESP;
     resp.status = 0;
 
     /* Only the owner rank is allowed to free the chunk */
     int owner = LEO_GADDR_OWNER(rq->gaddr);
     if (owner != g_ctx.rank) {
         log_warn("DSM FREE_REQ for non-owner: g=0x%llx owner=%d me=%d",
                  (unsigned long long)rq->gaddr, owner, g_ctx.rank);
         resp.status = -2;
         ucx_send_bytes(src_rank, &resp, sizeof(resp), OP_DSM_FREE_RESP);
         return;
     }
 
     pthread_mutex_lock(&g_dir_mtx);
     int idx = dsm_dir_find_idx(rq->gaddr);
     if (idx < 0 || !g_dir[idx].in_use) {
         pthread_mutex_unlock(&g_dir_mtx);
         log_warn("DSM FREE_REQ unknown gaddr=0x%llx", (unsigned long long)rq->gaddr);
         resp.status = -3;
         ucx_send_bytes(src_rank, &resp, sizeof(resp), OP_DSM_FREE_RESP);
         return;
     }
 
     /* Optionally validate no one is holding the lock; for MVP, we destroy directly. */
     pthread_rwlock_destroy(&g_dir[idx].rwlock);
     g_dir[idx].in_use = 0; /* mark as free; simple bump-allocator does not reuse */
     pthread_mutex_unlock(&g_dir_mtx);
 
     log_info("DSM: freed g=0x%llx by request from rank=%u",
              (unsigned long long)rq->gaddr, src_rank);
 
     ucx_send_bytes(src_rank, &resp, sizeof(resp), OP_DSM_FREE_RESP);
 }

/* Internal helpers: acquire/release remote lock */
static int dsm_lock_remote(leo_gaddr_t g, int mode, int owner)
{
    msg_dsm_lock_req_t rq = { .opcode=OP_DSM_LOCK_REQ, .gaddr=g, .mode=mode };
    if (ucx_send_bytes(owner, &rq, sizeof(rq), OP_DSM_LOCK_REQ) != 0) return -1;

    while (1) {
        size_t len=0; ucp_tag_t tag=0; ucp_tag_recv_info_t info;
        void *buf = ucx_recv_any_alloc(&len, &tag, &info);
        if (!buf) { ucp_worker_progress(g_ctx.ucx_ctx.ucp_worker); usleep(1000); continue; }
        uint32_t op=(uint32_t)(tag>>32), src=(uint32_t)(tag&0xffffffffu);

        if (op==OP_DSM_LOCK_RESP && src==(uint32_t)owner && len>=sizeof(msg_dsm_lock_resp_t)) {
            int rc = ((msg_dsm_lock_resp_t*)buf)->status;
            free(buf);
            return rc;
        }
        dispatch_msg(buf, len, tag); free(buf);
    }
}
static void dsm_unlock_remote(leo_gaddr_t g, int mode, int owner)
{
    msg_dsm_unlock_t un = { .opcode=OP_DSM_UNLOCK, .gaddr=g, .mode=mode };
    (void)ucx_send_bytes(owner, &un, sizeof(un), OP_DSM_UNLOCK);
}


/* Helper: allocate/zero arrays if not yet */
static int dsm_alloc_peer_arrays_if_needed(void)
{
    int n = g_ctx.world_size;
    if (!g_dsm.peer_base) {
        g_dsm.peer_base = (uint64_t*)calloc(n, sizeof(uint64_t));
        if (!g_dsm.peer_base) return -1;
    }
    if (!g_dsm.peer_rkey) {
        g_dsm.peer_rkey = (ucp_rkey_h*)calloc(n, sizeof(ucp_rkey_h));
        if (!g_dsm.peer_rkey) return -1;
    }
    if (!g_dsm.have_peer) {
        g_dsm.have_peer = (unsigned char*)calloc(n, sizeof(unsigned char)); 
        if (!g_dsm.have_peer) return -1;
    }
    return 0;
}

/* ---- Init / finalize ---- */
int dsm_init(size_t arena_bytes)
{
    const int my_rank   = g_ctx.rank;
    const int world_size = g_ctx.world_size;

    /* 0) Sanity */
    if (world_size <= 0 || my_rank < 0 || my_rank >= world_size) {
        log_error("DSM: invalid world rank/size (rank=%d size=%d)", my_rank, world_size);
        return -1;
    }
    log_debug("DSM: initializing with local pool size %zu bytes", arena_bytes);

    /* 1) Allocate per-peer arrays if needed */
    if (dsm_alloc_peer_arrays_if_needed() != 0) {
        log_error("DSM: alloc peer arrays failed");
        return -1;
    }

    /* 2) Create local arena (page aligned), zero it for determinism (optional) */
    void *base = NULL;
    const size_t align = 4096;
    int rc = posix_memalign(&base, align, arena_bytes);
    if (rc != 0 || !base) {
        log_error("DSM: posix_memalign failed: rc=%d (%s)", rc, strerror(rc));
        return -1;
    }
    memset(base, 0, arena_bytes); /* optional but useful */

    g_dsm.arena_base  = base;
    g_dsm.arena_bytes = arena_bytes;
    g_dsm.bump        = 0;

    /* 3) Register arena with UCX (mem map) */
    ucp_mem_map_params_t mpar;
    memset(&mpar, 0, sizeof(mpar));
    mpar.field_mask = UCP_MEM_MAP_PARAM_FIELD_ADDRESS |
                      UCP_MEM_MAP_PARAM_FIELD_LENGTH;
    mpar.address    = base;
    mpar.length     = arena_bytes;

    ucs_status_t st = ucp_mem_map(g_ctx.ucx_ctx.ucp_context, &mpar, &g_dsm.memh);
    if (st != UCS_OK) {
        log_error("DSM: ucp_mem_map failed: %s", ucs_status_string(st));
        free(g_dsm.arena_base); g_dsm.arena_base = NULL;
        return -1;
    }

     /* 4) Pack local rkey */
     void   *packed = NULL;
     size_t  packed_len = 0;
     st = ucp_rkey_pack(g_ctx.ucx_ctx.ucp_context, g_dsm.memh, &packed, &packed_len);
     if (st != UCS_OK) {
         log_error("DSM: ucp_rkey_pack failed: %s", ucs_status_string(st));
         ucp_mem_unmap(g_ctx.ucx_ctx.ucp_context, g_dsm.memh); g_dsm.memh = NULL;
         free(g_dsm.arena_base); g_dsm.arena_base = NULL;
         return -1;
     }
     g_dsm.packed_rkey = packed;
     g_dsm.packed_len  = packed_len;
 
     log_info("DSM: local arena base=%p bytes=%zu rkey_len=%zu",
              g_dsm.arena_base, g_dsm.arena_bytes, g_dsm.packed_len);

    /* 5) Send my {base_addr, rkey_len, arena_bytes} header + rkey bytes to all peers */
    msg_dsm_announce_t hdr;
    hdr.opcode      = OP_DSM_ANN;
    hdr.rkey_len    = (uint32_t)g_dsm.packed_len;
    hdr.base_addr   = (uint64_t)(uintptr_t)g_dsm.arena_base;
    hdr.arena_bytes = (uint64_t)g_dsm.arena_bytes;

    for (int r = 0; r < world_size; ++r) {
        if (r == my_rank) continue;
        /* header */
        if (ucx_send_bytes(r, &hdr, sizeof(hdr), OP_DSM_ANN) != 0) {
            log_error("DSM: send header to rank %d failed", r);
            goto fail;
        }
        /* rkey blob */
        if (ucx_send_bytes(r, g_dsm.packed_rkey, g_dsm.packed_len, OP_DSM_ANN) != 0) {
            log_error("DSM: send rkey to rank %d failed", r);
            goto fail;
        }
    }

    /* 6) Receive peers' {base_addr, rkey} and unpack */
    for (int r = 0; r < world_size; ++r) {
        if (r == my_rank) {
            /* self */
            g_dsm.peer_base[r] = (uint64_t)(uintptr_t)g_dsm.arena_base;
            g_dsm.peer_rkey[r] = NULL; /* self RMA无需 rkey */
            g_dsm.have_peer[r] = 1;
            continue;
        }

        msg_dsm_announce_t rh;
        if (ucx_recv_blocking(OP_DSM_ANN, r, &rh, sizeof(rh), /*timeout_ms*/10000) != 0) {
            log_error("DSM: recv header from rank %d failed", r);
            goto fail;
        }
        if (rh.opcode != OP_DSM_ANN) {
            log_error("DSM: invalid opcode from rank %d: %u", r, rh.opcode);
            goto fail;
        }
        if (rh.rkey_len == 0 || rh.rkey_len > (16*1024)) { /* sanity limit */
            log_error("DSM: suspicious rkey_len=%u from rank %d", rh.rkey_len, r);
            goto fail;
        }

        void *peer_buf = malloc(rh.rkey_len);
        if (!peer_buf) {
            log_error("DSM: malloc peer rkey buf failed (rank=%d, len=%u)", r, rh.rkey_len);
            goto fail;
        }
        if (ucx_recv_blocking(OP_DSM_ANN, r, peer_buf, rh.rkey_len, 10000) != 0) {
            log_error("DSM: recv rkey bytes from rank %d failed", r);
            free(peer_buf);
            goto fail;
        }

        /* Unpack rkey against EP[r] */
        ucp_ep_h ep = g_ctx.ucx_ctx.eps[r];
        if (!ep) {
            log_error("DSM: EP to rank %d is NULL", r);
            free(peer_buf);
            goto fail;
        }
        ucp_rkey_h rkey_h = NULL;
        ucs_status_t ust = ucp_ep_rkey_unpack(ep, peer_buf, &rkey_h);
        free(peer_buf);
        if (ust != UCS_OK) {
            log_error("DSM: ucp_ep_rkey_unpack from rank %d failed: %s",
                      r, ucs_status_string(ust));
            goto fail;
        }

        g_dsm.peer_base[r] = rh.base_addr;
        g_dsm.peer_rkey[r] = rkey_h;
        g_dsm.have_peer[r] = 1;

        log_info("DSM: learned peer %d base=0x%" PRIx64 " rkey_len=%u arena=%" PRIu64,
                 r, rh.base_addr, rh.rkey_len, rh.arena_bytes);
    }

    log_info("DSM: init done on rank %d (world=%d)", my_rank, world_size);
    return 0;

fail:
    /* Best-effort cleanup (leave g_dsm valid for finalize to re-run) */
    for (int r = 0; r < world_size; ++r) {
        if (g_dsm.peer_rkey && g_dsm.peer_rkey[r]) {
            ucp_rkey_destroy(g_dsm.peer_rkey[r]);
            g_dsm.peer_rkey[r] = NULL;
        }
    }
    if (g_dsm.packed_rkey) {
        ucp_rkey_buffer_release(g_dsm.packed_rkey);
        g_dsm.packed_rkey = NULL;
        g_dsm.packed_len  = 0;
    }
    if (g_dsm.memh) {
        ucp_mem_unmap(g_ctx.ucx_ctx.ucp_context, g_dsm.memh);
        g_dsm.memh = NULL;
    }
    if (g_dsm.arena_base) {
        free(g_dsm.arena_base);
        g_dsm.arena_base = NULL;
        g_dsm.arena_bytes = 0;
    }
    return -1;
}


    // memset(&g_dsm, 0, sizeof(g_dsm));
    // if (local_pool_bytes == 0) {
    //     /* fall back to config or a small default, e.g., 64MB */
    //     local_pool_bytes = 64ull << 20;
    // }

    // log_debug("DSM: initializing with local pool size %zu bytes", local_pool_bytes);
    // /* Allocate and register local arena */
    // const size_t align = 4096;
    // void *base = NULL;
    // if (posix_memalign(&base, align, local_pool_bytes) != 0 || !base) {
    //     log_error("DSM: posix_memalign failed");
    //     return -1;
    // }
    // log_debug("DSM: local arena %p..%p (%zu bytes) allocated by posix_memalign",
    //           base, (void*)((uintptr_t)base + local_pool_bytes - 1), local_pool_bytes);
    // //memset(base, 0, local_pool_bytes);
    // log_debug("DSM: local arena %p..%p (%zu bytes) allocated",
    //           base, (void*)((uintptr_t)base + local_pool_bytes - 1), local_pool_bytes);

    // ucp_mem_map_params_t mp; memset(&mp, 0, sizeof(mp));
    // mp.field_mask = UCP_MEM_MAP_PARAM_FIELD_ADDRESS |
    //                 UCP_MEM_MAP_PARAM_FIELD_LENGTH;
    // mp.address    = base;
    // mp.length     = local_pool_bytes;
    // sleep(2);
    // log_debug("pinning memory to ucx!");
    // ucs_status_t st = ucp_mem_map(g_ctx.ucx_ctx.ucp_context, &mp, &g_dsm.memh); //pin the memory to ucx, generate mem handle(memh)
    // if (st != UCS_OK) {
    //     log_error("DSM: ucp_mem_map failed");
    //     free(base);
    //     return -1;
    // }
    // log_debug("pinned memory to ucx");

    // void *rkey_buf = NULL; size_t rkey_len = 0;
    // st = ucp_rkey_pack(g_ctx.ucx_ctx.ucp_context, g_dsm.memh, &rkey_buf, &rkey_len); /* 把授权打包成字节串*/
    // if (st != UCS_OK) {
    //     log_error("DSM: ucp_rkey_pack failed");
    //     ucp_mem_unmap(g_ctx.ucx_ctx.ucp_context, g_dsm.memh);
    //     free(base);
    //     return -1;
    // }

    // g_dsm.arena_base  = base;
    // g_dsm.arena_bytes = local_pool_bytes;
    // g_dsm.bump        = 0;
    // pthread_mutex_init(&g_dsm.alloc_mtx, NULL);
    // g_dsm.packed_rkey = rkey_buf;
    // g_dsm.packed_len  = rkey_len;

    // /* Allocate per-peer caches */
    // int W = g_ctx.world_size;
    // g_dsm.peer_base  = (uint64_t*)calloc(W, sizeof(uint64_t));
    // g_dsm.peer_rkey  = (ucp_rkey_h*)calloc(W, sizeof(ucp_rkey_h));
    // g_dsm.have_peer  = (unsigned char*)calloc(W, 1);
    // if (!g_dsm.peer_base || !g_dsm.peer_rkey || !g_dsm.have_peer) {
    //     log_error("DSM: allocate peer caches failed");
    //     dsm_finalize();
    //     return -1;
    // }
    // log_debug("DSM: local arena %p..%p (%zu bytes) registered",
    //           g_dsm.arena_base,
    //           (void*)((uintptr_t)g_dsm.arena_base + g_dsm.arena_bytes - 1),
    //           g_dsm.arena_bytes);


    // /* Fill self entry, mark self as available */
    // g_dsm.peer_base[g_ctx.rank] = (uint64_t)(uintptr_t)g_dsm.arena_base;
    // g_dsm.have_peer[g_ctx.rank] = 1;
    // g_peers_ready = 0; /* reset counter for peers (excluding self) */
    // /* Self rkey: can unpack to self ep if you want, but local path will memcpy. */

    // /* build my ANN packet */
    // msg_dsm_ann_t hdr; memset(&hdr, 0, sizeof(hdr));
    // hdr.opcode    = OP_DSM_ANN;
    // hdr.rkey_len  = (uint32_t)g_dsm.packed_len;
    // hdr.base_addr = (uint64_t)(uintptr_t)g_dsm.arena_base;

    // const size_t pkt_len = sizeof(hdr) + g_dsm.packed_len;
    // char *pkt = (char*)malloc(pkt_len);
    // memcpy(pkt, &hdr, sizeof(hdr));
    // memcpy(pkt + sizeof(hdr), g_dsm.packed_rkey, g_dsm.packed_len);

    // for (int r = 0; r < W; ++r) if (r != g_ctx.rank) {
    //     int rc = ucx_send_bytes(r, pkt, pkt_len, OP_DSM_ANN);
    //     if (rc != 0) {
    //         log_error("DSM: send ANN to rank %d failed rc=%d", r, rc);
    //     }
    // }
    // free(pkt);
    // log_debug("DSM: sent ANN to %d peers", W-1);

    // /* wait others’ ANN handled by dispatcher */
    // int tmo = g_ctx.tcp_cfg.connect_timeout_ms > 0 ? g_ctx.tcp_cfg.connect_timeout_ms*2 : 10000;
    // int wrc = dsm_wait_announces(tmo);
    // if (wrc != 0) {
    //     log_error("DSM: wait announces timeout/failed (ready=%d of %d)",
    //               g_peers_ready, g_ctx.world_size-1);
    //     return -1;
    // }

    // log_info("DSM init done: arena=%p size=%zu", g_dsm.arena_base, g_dsm.arena_bytes);
    // return 0;

    // /* Receive from W-1 peers */
    // int need = W - 1;
    // while (need > 0) {
    //     size_t len=0; ucp_tag_t tag=0; ucp_tag_recv_info_t info;
    //     void *buf = ucx_recv_any_alloc(&len, &tag, &info);
    //     if (!buf) { dispatcher_progress_once(); usleep(1000); continue; }

    //     uint32_t opcode = (uint32_t)(tag >> 32);
    //     uint32_t src    = (uint32_t)(tag & 0xffffffffu);

    //     if (opcode == OP_DSM_ANN && len >= sizeof(msg_dsm_ann_t)) {
    //         msg_dsm_ann_t *mh = (msg_dsm_ann_t*)buf;
    //         if (len < sizeof(*mh) + mh->rkey_len) {
    //             log_error("DSM: malformed ANN from %u", src);
    //             free(buf);
    //             continue;
    //         }
    //         if (!g_dsm.have_peer[src]) {
    //             g_dsm.peer_base[src] = mh->base_addr;

    //             ucp_rkey_h rkey = NULL;
    //             ucs_status_t st2 = ucp_ep_rkey_unpack(g_ctx.ucx_ctx.eps[src],
    //                                                 (void*)(mh+1), &rkey);
    //             if (st2 != UCS_OK) {
    //                 log_error("DSM: rkey_unpack failed from %u", src);
    //                 free(buf);
    //                 continue;
    //             }
    //             g_dsm.peer_rkey[src] = rkey;
    //             g_dsm.have_peer[src] = 1;
    //             --need;
    //             log_info("DSM: got rkey from rank %u base=0x%llx len=%u",
    //                     src, (unsigned long long)mh->base_addr, mh->rkey_len);
    //         }
    //         free(buf);
    //     } else {
    //         /* Not a DSM msg: hand over to dispatcher (create/join/announce etc.) */
    //         dispatch_msg(buf, len, tag);
    //         free(buf);
    //     }
    // }

    // log_info("DSM init done: arena=%p size=%zu", g_dsm.arena_base, g_dsm.arena_bytes);
    // return 0;
// }

void dsm_finalize(void)
{
    /* Destroy peer rkeys */
    if (g_dsm.peer_rkey) {
        for (int r = 0; r < g_ctx.world_size; ++r) {
            if (g_dsm.peer_rkey[r]) {
                ucp_rkey_destroy(g_dsm.peer_rkey[r]);
                g_dsm.peer_rkey[r] = NULL;
            }
        }
    }

    /* 2) Release my packed rkey buffer (pack side), if you kept it */
    if (g_dsm.packed_rkey) {
        ucp_rkey_buffer_release(g_dsm.packed_rkey);
        g_dsm.packed_rkey = NULL;
    }

    /* 3) Unmap my registered arena before freeing the raw memory */
    if (g_dsm.memh) {
        ucs_status_t st = ucp_mem_unmap(g_ctx.ucx_ctx.ucp_context, g_dsm.memh);
        if (st != UCS_OK) {
            log_warn("DSM: ucp_mem_unmap failed: %s", ucs_status_string(st));
        }
        g_dsm.memh = NULL;
    }

    /* 4) Free the arena host memory */
    if (g_dsm.arena_base) {
        free(g_dsm.arena_base);
        g_dsm.arena_base = NULL;
        g_dsm.arena_bytes = 0;
        g_dsm.bump = 0;
    }

    /* 5) Optional: clear directory and destroy per-entry locks */
    pthread_mutex_lock(&g_dir_mtx);
    for (int i = 0; i < DSM_DIR_MAX; ++i) {
        if (g_dir[i].in_use) {
            pthread_rwlock_destroy(&g_dir[i].rwlock);
            g_dir[i].in_use = 0;
        }
    }
    pthread_mutex_unlock(&g_dir_mtx);

    /* 6) 释放跨节点元数据数组（仅当是动态分配时） */
    if (g_dsm.peer_base)  { free(g_dsm.peer_base);  g_dsm.peer_base  = NULL; }
    if (g_dsm.peer_rkey)  { free(g_dsm.peer_rkey);  g_dsm.peer_rkey  = NULL; }
    if (g_dsm.have_peer)  { free(g_dsm.have_peer);  g_dsm.have_peer  = NULL; }

    memset(&g_dsm, 0, sizeof(g_dsm));
    log_info("DSM finalized for rank=%d", g_ctx.rank);
}

/* ---- Alloc/free ---- */
/* Thread-safe bump allocator guarded by g_dsm.alloc_mtx:
   - g_dsm.arena_base, g_dsm.arena_bytes, g_dsm.bump */
/* ... existing globals ... */

leo_gaddr_t leo_malloc(size_t n, int owner_rank)
{
    if (n == 0) {
        log_warn("DSM: malloc size==0");
        return 0;
    }

    /* Default placement: local rank */
    if (owner_rank < 0) owner_rank = g_ctx.rank;

    if (owner_rank >= g_ctx.world_size) {
        log_error("DSM: owner_rank=%d out of range [0,%d)", owner_rank, g_ctx.world_size);
        return 0;
    }

    if (owner_rank == g_ctx.rank) {
        /* Local fast-path allocation */
        size_t off = 0;

        pthread_mutex_lock(&g_dsm.alloc_mtx);
        if (g_dsm.bump + n <= g_dsm.arena_bytes) {
            off = g_dsm.bump;
            g_dsm.bump += n;
        } else {
            pthread_mutex_unlock(&g_dsm.alloc_mtx);
            log_error("DSM: local arena OOM (req=%zu avail=%zu)",
                      n, (g_dsm.arena_bytes - g_dsm.bump));
            return 0;
        }
        pthread_mutex_unlock(&g_dsm.alloc_mtx);

        leo_gaddr_t g = LEO_GADDR_MAKE(g_ctx.rank, off);
        if (dsm_dir_insert(g, n) != 0) {
            log_error("DSM: dir_insert failed for g=0x%llx", (unsigned long long)g);
            return 0;
        }
        log_debug("DSM: local alloc rank=%d off=%zu size=%zu -> g=0x%llx",
                  g_ctx.rank, off, n, (unsigned long long)g);
        return g;
    } else {
        /* Remote allocation via control-plane RPC */
        leo_gaddr_t g = 0;
        int rc = dsm_remote_alloc(owner_rank, n, &g);
        if (rc != 0 || g == 0) {
            log_error("DSM: remote alloc failed owner=%d size=%zu rc=%d", owner_rank, n, rc);
            return 0;
        }
        log_debug("DSM: remote alloc owner=%d size=%zu -> g=0x%llx",
                  owner_rank, n, (unsigned long long)g);
        return g;
    }
}

int leo_free(leo_gaddr_t g)
{
    (void)g;
    /* MVP: no-op; could push to a free-list here. */
    return 0;
}

/* leo_read: SHARED lock for remote */
int leo_read(void *dst, leo_gaddr_t src, size_t n)
{
    int owner = LEO_GADDR_OWNER(src);
    uint64_t off = LEO_GADDR_OFFSET(src);
    if (owner == g_ctx.rank) {
        memcpy(dst, (uint8_t*)g_dsm.arena_base + off, n);
        return 0;
    } else {
        if (dsm_lock_remote(src, DSM_LOCK_SHARED, owner) != 0) return -1;

        /* RDMA GET ... (omitted: your existing ucx_get_nbx + flush) */
        int rc = ucx_get_block(dst, n, owner, g_dsm.peer_base[owner] + off, g_dsm.peer_rkey[owner]);

        dsm_unlock_remote(src, DSM_LOCK_SHARED, owner);
        return rc;
    }
}

/* leo_write: EXCLUSIVE lock for remote */
int leo_write(leo_gaddr_t dst, const void *src, size_t n)
{
    int owner = LEO_GADDR_OWNER(dst);
    uint64_t off = LEO_GADDR_OFFSET(dst);
    if (owner == g_ctx.rank) {
        memcpy((uint8_t*)g_dsm.arena_base + off, src, n);
        return 0;
    } else {
        if (dsm_lock_remote(dst, DSM_LOCK_EXCL, owner) != 0) return -1;

        /* RDMA PUT ... (omitted: your existing ucx_put_nbx + flush) */
        int rc = ucx_put_block(src, n, owner, g_dsm.peer_base[owner] + off, g_dsm.peer_rkey[owner]);

        dsm_unlock_remote(dst, DSM_LOCK_EXCL, owner);
        return rc;
    }
}
 