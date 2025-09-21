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

/* ---- Control-plane message for DSM rkey announce ---- */
typedef struct {
    uint32_t opcode;     /* custom: OP_DSM_ANN = 30 (pick a free code) */
    uint32_t rkey_len;   /* bytes following */
    uint64_t base_addr;  /* remote virtual address of arena base */
} msg_dsm_ann_t;

/* Choose an unused opcode in your proto.h; keep consistent everywhere */
#ifndef OP_DSM_ANN
#define OP_DSM_ANN 30
#endif

/* ---- DSM global state ---- */
typedef struct {
    /* Local arena */
    void        *arena_base;
    size_t       arena_bytes;
    size_t       bump;           /* bump-pointer offset */
    pthread_mutex_t alloc_mtx;

    /* UCX memory handle and rkey */
    ucp_mem_h    memh;
    void        *packed_rkey;    /* bytes to send to peers */
    size_t       packed_len;

    /* Per-peer remote base and rkey handle */
    uint64_t    *peer_base;      /* remote virtual base address per rank */
    ucp_rkey_h  *peer_rkey;      /* unpacked rkey handle per rank */
    unsigned char *have_peer;    /* bitmap: received rkey from that peer */
} dsm_state_t;

static dsm_state_t g_dsm;

/* ---- Helpers: UCX request wait ---- */
static int ucx_wait(ucs_status_ptr_t req)
{
    if (req == NULL) return 0;
    if (UCS_PTR_IS_ERR(req)) return UCS_PTR_STATUS(req) == UCS_OK ? 0 : -1;
    while (1) {
        ucs_status_t st = ucp_request_check_status(req);
        if (st == UCS_OK) break;
        if (st != UCS_INPROGRESS) { ucp_request_free(req); return -1; }
        ucp_worker_progress(g_ctx.ucx_ctx.ucp_worker);
    }
    ucp_request_free(req);
    return 0;
}

/* ---- Init / finalize ---- */
int dsm_init(size_t local_pool_bytes)
{
    memset(&g_dsm, 0, sizeof(g_dsm));
    if (local_pool_bytes == 0) {
        /* fall back to config or a small default, e.g., 64MB */
        local_pool_bytes = 64ull << 20;
    }

    /* Allocate and register local arena */
    const size_t align = 4096;
    void *base = NULL;
    if (posix_memalign(&base, align, local_pool_bytes) != 0 || !base) {
        log_error("DSM: posix_memalign failed");
        return -1;
    }
    memset(base, 0, local_pool_bytes);

    ucp_mem_map_params_t mp; memset(&mp, 0, sizeof(mp));
    mp.field_mask = UCP_MEM_MAP_PARAM_FIELD_ADDRESS |
                    UCP_MEM_MAP_PARAM_FIELD_LENGTH;
    mp.address    = base;
    mp.length     = local_pool_bytes;

    ucs_status_t st = ucp_mem_map(g_ctx.ucx_ctx.ucp_context, &mp, &g_dsm.memh);
    if (st != UCS_OK) {
        log_error("DSM: ucp_mem_map failed");
        free(base);
        return -1;
    }

    void *rkey_buf = NULL; size_t rkey_len = 0;
    st = ucp_rkey_pack(g_ctx.ucx_ctx.ucp_context, g_dsm.memh, &rkey_buf, &rkey_len);
    if (st != UCS_OK) {
        log_error("DSM: ucp_rkey_pack failed");
        ucp_mem_unmap(g_ctx.ucx_ctx.ucp_context, g_dsm.memh);
        free(base);
        return -1;
    }

    g_dsm.arena_base  = base;
    g_dsm.arena_bytes = local_pool_bytes;
    g_dsm.bump        = 0;
    pthread_mutex_init(&g_dsm.alloc_mtx, NULL);
    g_dsm.packed_rkey = rkey_buf;
    g_dsm.packed_len  = rkey_len;

    /* Allocate per-peer caches */
    int W = g_ctx.world_size;
    g_dsm.peer_base  = (uint64_t*)calloc(W, sizeof(uint64_t));
    g_dsm.peer_rkey  = (ucp_rkey_h*)calloc(W, sizeof(ucp_rkey_h));
    g_dsm.have_peer  = (unsigned char*)calloc(W, 1);
    if (!g_dsm.peer_base || !g_dsm.peer_rkey || !g_dsm.have_peer) {
        log_error("DSM: allocate peer caches failed");
        dsm_finalize();
        return -1;
    }

    /* Fill self entry */
    g_dsm.peer_base[g_ctx.rank] = (uint64_t)(uintptr_t)g_dsm.arena_base;
    g_dsm.have_peer[g_ctx.rank] = 1;
    /* Self rkey: can unpack to self ep if you want, but local path will memcpy. */

    /* Announce to all peers and collect theirs */
    msg_dsm_ann_t hdr; memset(&hdr, 0, sizeof(hdr));
    hdr.opcode    = OP_DSM_ANN;
    hdr.rkey_len  = (uint32_t)g_dsm.packed_len;
    hdr.base_addr = (uint64_t)(uintptr_t)g_dsm.arena_base;

    const size_t pkt_len = sizeof(hdr) + g_dsm.packed_len;
    char *pkt = (char*)malloc(pkt_len);
    memcpy(pkt, &hdr, sizeof(hdr));
    memcpy(pkt + sizeof(hdr), g_dsm.packed_rkey, g_dsm.packed_len);

    for (int r = 0; r < W; ++r) if (r != g_ctx.rank) {
        (void)ucx_send_bytes(r, pkt, pkt_len, OP_DSM_ANN);
    }
    free(pkt);

    /* Receive from W-1 peers */
    int need = W - 1;
    while (need > 0) {
        size_t len=0; ucp_tag_t tag=0; ucp_tag_recv_info_t info;
        void *buf = ucx_recv_any_alloc(&len, &tag, &info);
        if (!buf) { dispatcher_progress_once(); usleep(1000); continue; }

        uint32_t opcode = (uint32_t)(tag >> 32);
        uint32_t src    = (uint32_t)(tag & 0xffffffffu);

        if (opcode == OP_DSM_ANN && len >= sizeof(msg_dsm_ann_t)) {
            msg_dsm_ann_t *mh = (msg_dsm_ann_t*)buf;
            if (len < sizeof(*mh) + mh->rkey_len) {
                log_error("DSM: malformed ANN from %u", src);
                free(buf);
                continue;
            }
            if (!g_dsm.have_peer[src]) {
                g_dsm.peer_base[src] = mh->base_addr;

                ucp_rkey_h rkey = NULL;
                ucs_status_t st2 = ucp_ep_rkey_unpack(g_ctx.ucx_ctx.eps[src],
                                                    (void*)(mh+1), &rkey);
                if (st2 != UCS_OK) {
                    log_error("DSM: rkey_unpack failed from %u", src);
                    free(buf);
                    continue;
                }
                g_dsm.peer_rkey[src] = rkey;
                g_dsm.have_peer[src] = 1;
                --need;
                log_info("DSM: got rkey from rank %u base=0x%llx len=%u",
                        src, (unsigned long long)mh->base_addr, mh->rkey_len);
            }
            free(buf);
        } else {
            /* Not a DSM msg: hand over to dispatcher (create/join/announce etc.) */
            dispatch_msg(buf, len, tag);
            free(buf);
        }
    }

    log_info("DSM init done: arena=%p size=%zu", g_dsm.arena_base, g_dsm.arena_bytes);
    return 0;
}

void dsm_finalize(void)
{
    /* Destroy peer rkeys */
    if (g_dsm.peer_rkey) {
        for (int r = 0; r < g_ctx.world_size; ++r) {
            if (g_dsm.peer_rkey[r]) ucp_rkey_destroy(g_dsm.peer_rkey[r]);
        }
    }
    /* Release packed rkey buffer */
    if (g_dsm.packed_rkey) ucp_rkey_buffer_release(g_dsm.packed_rkey);

    /* Unmap local mem */
    if (g_dsm.memh) ucp_mem_unmap(g_ctx.ucx_ctx.ucp_context, g_dsm.memh);

    /* Free arena */
    if (g_dsm.arena_base) free(g_dsm.arena_base);

    /* Free arrays */
    free(g_dsm.peer_base);
    free(g_dsm.peer_rkey);
    free(g_dsm.have_peer);

    memset(&g_dsm, 0, sizeof(g_dsm));
}

/* ---- Alloc/free ---- */
leo_gaddr_t leo_malloc(size_t n)
{
    /* 8-byte align */
    const size_t a = 8;
    n = (n + (a-1)) & ~(a-1);

    pthread_mutex_lock(&g_dsm.alloc_mtx);
    size_t off = g_dsm.bump;
    if (off + n > g_dsm.arena_bytes) {
        pthread_mutex_unlock(&g_dsm.alloc_mtx);
        log_error("DSM: OOM in local arena (req=%zu avail=%zu)", n, g_dsm.arena_bytes - off);
        return 0;
    }
    g_dsm.bump += n;
    pthread_mutex_unlock(&g_dsm.alloc_mtx);

    return LEO_GPTR_MAKE(g_ctx.rank, off);
}

int leo_free(leo_gaddr_t g)
{
    (void)g;
    /* MVP: no-op; could push to a free-list here. */
    return 0;
}

/* ---- Read/Write ---- */
int leo_read(void *dst, leo_gaddr_t g, size_t n)
{
    if (g == 0 || n == 0) return 0;
    int owner = LEO_GPTR_OWNER(g);
    uint64_t off = LEO_GPTR_OFFSET(g);

    if ((off + n) > g_dsm.arena_bytes) {
        if (owner == g_ctx.rank) {
            /* local bound check against our arena */
        } else {
            /* remote bound check is unknown; we trust caller */
        }
    }

    if (owner == g_ctx.rank) {
        memcpy(dst, (char*)g_dsm.arena_base + off, n);
        return 0;
    } else {
        if (!g_dsm.have_peer[owner] || !g_dsm.peer_rkey[owner]) return -ENOENT;

        uint64_t remote_addr = g_dsm.peer_base[owner] + off;

        ucp_request_param_t prm; memset(&prm, 0, sizeof(prm));
        prm.op_attr_mask = UCP_OP_ATTR_FIELD_DATATYPE;
        prm.datatype     = ucp_dt_make_contig(1);
        ucs_status_ptr_t req = ucp_get_nbx(g_ctx.ucx_ctx.eps[owner],
                                        dst, n, remote_addr, g_dsm.peer_rkey[owner], &prm);
        if (ucx_wait(req) != 0) return -1;
        return 0;
    }
}

int leo_write(leo_gaddr_t g, const void *src, size_t n)
{
    if (g == 0 || n == 0) return 0;
    int owner = LEO_GPTR_OWNER(g);
    uint64_t off = LEO_GPTR_OFFSET(g);

    if (owner == g_ctx.rank) {
        memcpy((char*)g_dsm.arena_base + off, src, n);
        return 0;
    } else {
        if (!g_dsm.have_peer[owner] || !g_dsm.peer_rkey[owner]) return -ENOENT;

        uint64_t remote_addr = g_dsm.peer_base[owner] + off;

        ucp_request_param_t prm; memset(&prm, 0, sizeof(prm));
        prm.op_attr_mask = UCP_OP_ATTR_FIELD_DATATYPE;
        prm.datatype     = ucp_dt_make_contig(1);
        ucs_status_ptr_t req = ucp_put_nbx(g_ctx.ucx_ctx.eps[owner],
                                        src, n, remote_addr, g_dsm.peer_rkey[owner], &prm);
        if (ucx_wait(req) != 0) return -1;

        /* Ensure remote visibility before return: flush ep */
        ucs_status_ptr_t f = ucp_ep_flush_nbx(g_ctx.ucx_ctx.eps[owner], &prm);
        if (ucx_wait(f) != 0) return -1;
        return 0;
    }
}
 