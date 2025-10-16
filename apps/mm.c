#define _POSIX_C_SOURCE 200809L
#include "leopar.h"
#include "dsm_c_api.h"
#include "ctrl.h"     // ctrl_barrier(name, gen, timeout_ms) if available here
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <inttypes.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <errno.h>
#include "log.h"


/* Element type: double */
typedef double mm_elem_t;

/* Directory object stored in DSM as a single blob:
 * Header + a packed payload of arrays with world_size elements each.
 *
 * Layout (in order):
 *   - GAddr A_addr[world_size]   // A shards per rank
 *   - GAddr B_addr[world_size]   // B full replicas per rank
 *   - GAddr C_addr[world_size]   // C shards per rank
 *   - Size  A_bytes[world_size]  // byte size of each A shard
 *   - Size  B_bytes[world_size]  // byte size of each B replica
 *   - Size  C_bytes[world_size]  // byte size of each C shard
 *   - int   row0[world_size]     // owned rows for A/C: [row0, row1)
 *   - int   row1[world_size]
 */
typedef struct {
    uint64_t magic;        /* e.g., 0x4D4D4433'00000001ULL ("MMD3\1") */
    uint32_t version;      /* start with 1 */
    uint32_t world_size;   /* number of ranks */
    int32_t  dimension;      /* A: MxK, B: KxN, C: MxN */
    Size     elem_bytes;   /* sizeof(element), e.g., sizeof(double) */
    unsigned char data[];  /* flexible array payload with packed vectors */
} mm_dir3_t;

/* ---- Size helpers (host-side) ---- */
static inline size_t mm_dir3_payload_bytes(uint32_t W) {
    return (size_t)W * ( 3*sizeof(GAddr)         /* A/B/C addresses */
                       + 3*sizeof(Size)          /* A/B/C sizes     */
                       + 2*sizeof(int) );        /* row0/row1       */
}

static inline size_t mm_dir3_total_bytes(uint32_t W) {
    return sizeof(mm_dir3_t) + mm_dir3_payload_bytes(W);
}

/* ---- Accessors (mutable) ---- */
static inline GAddr* mm_dir3_A_addrs(mm_dir3_t* d) {
    return (GAddr*)d->data;
}
static inline GAddr* mm_dir3_B_addrs(mm_dir3_t* d) {
    return mm_dir3_A_addrs(d) + d->world_size;
}
static inline GAddr* mm_dir3_C_addrs(mm_dir3_t* d) {
    return mm_dir3_B_addrs(d) + d->world_size;
}
static inline Size* mm_dir3_A_bytes(mm_dir3_t* d) {
    return (Size*)(mm_dir3_C_addrs(d) + d->world_size);
}
static inline Size* mm_dir3_B_bytes(mm_dir3_t* d) {
    return mm_dir3_A_bytes(d) + d->world_size;
}
static inline Size* mm_dir3_C_bytes(mm_dir3_t* d) {
    return mm_dir3_B_bytes(d) + d->world_size;
}
static inline int* mm_dir3_row0(mm_dir3_t* d) {
    return (int*)(mm_dir3_C_bytes(d) + d->world_size);
}
static inline int* mm_dir3_row1(mm_dir3_t* d) {
    return mm_dir3_row0(d) + d->world_size;
}

/* ---- Accessors (const) ---- */
static inline const GAddr* mm_dir3_A_addrs_c(const mm_dir3_t* d) {
    return (const GAddr*)d->data;
}
static inline const GAddr* mm_dir3_B_addrs_c(const mm_dir3_t* d) {
    return mm_dir3_A_addrs_c(d) + d->world_size;
}
static inline const GAddr* mm_dir3_C_addrs_c(const mm_dir3_t* d) {
    return mm_dir3_B_addrs_c(d) + d->world_size;
}
static inline const Size* mm_dir3_A_bytes_c(const mm_dir3_t* d) {
    return (const Size*)(mm_dir3_C_addrs_c(d) + d->world_size);
}
static inline const Size* mm_dir3_B_bytes_c(const mm_dir3_t* d) {
    return mm_dir3_A_bytes_c(d) + d->world_size;
}
static inline const Size* mm_dir3_C_bytes_c(const mm_dir3_t* d) {
    return mm_dir3_B_bytes_c(d) + d->world_size;
}
static inline const int* mm_dir3_row0_c(const mm_dir3_t* d) {
    return (const int*)(mm_dir3_C_bytes_c(d) + d->world_size);
}
static inline const int* mm_dir3_row1_c(const mm_dir3_t* d) {
    return mm_dir3_row0_c(d) + d->world_size;
}

/* simple block-row partition */
static void compute_rows(int M, int world_size, int rank, int* row0, int* row1) {
    int base = M / world_size;
    int rem  = M % world_size;
    int s = rank * base + (rank < rem ? rank : rem);
    int len = base + (rank < rem ? 1 : 0);
    *row0 = s; *row1 = s + len;
}

/* tiny RNG for reproducible fill on rank0 */
static void fill_random_double(double* p, size_t n, unsigned* seed) {
    unsigned x = (seed && *seed) ? *seed : 12345u;
    for (size_t i = 0; i < n; i++) {
        x = 1664525u * x + 1013904223u;
        p[i] = (double)(x & 0x00ffffffu) / (double)0x01000000u;
    }
    if (seed) *seed = x;
}

/* Build A/B/C allocations via DSM for all ranks, write A/B data */
int mm_dir3_build_on_rank0(const int dims,
                            unsigned seed,
                            /* out */ GAddr* out_dir_gaddr)
{
    if (!dims || !out_dir_gaddr) return -1;
    const int W = leo_world_size();
    int my_rank = leo_rank();
    const size_t elem = (size_t)sizeof(mm_elem_t);

    if (my_rank != 0) {
        log_error("mm_dir3_build_and_publish_on_rank0 must be called on rank0");
        return -1;
    }

    /* 1) prepare directory blob in host memory (header + packed arrays) */
    const size_t dir_bytes = mm_dir3_total_bytes((uint32_t)W);
    mm_dir3_t* dir = (mm_dir3_t*)malloc(dir_bytes);
    if (!dir) { log_error("OOM dir host blob"); return -1; }
    memset(dir, 0, dir_bytes);

    dir->magic      = 0x4D4D443300000001ULL; /* "MMD3\1" */
    dir->version    = 1;
    dir->world_size = (uint32_t)W;
    dir->dimension = dims;
    dir->elem_bytes = (Size)elem;

    GAddr* A_addr = mm_dir3_A_addrs(dir);
    GAddr* B_addr = mm_dir3_B_addrs(dir);
    GAddr* C_addr = mm_dir3_C_addrs(dir);
    Size*  A_bytes= mm_dir3_A_bytes(dir);
    Size*  B_bytes= mm_dir3_B_bytes(dir);
    Size*  C_bytes= mm_dir3_C_bytes(dir);
    int*   row0v  = mm_dir3_row0(dir);
    int*   row1v  = mm_dir3_row1(dir);

    /* 2) per-rank DSM allocations (A shard, B full replica, C shard) */
    for (int r = 0; r < W; r++) {
        int row0, row1; 
        compute_rows(dims, W, r, &row0, &row1);
        const size_t rows = (size_t)(row1 - row0);

        const size_t Asz = rows * dims * elem;
        const size_t Bsz = dims * dims * elem;
        const size_t Csz = rows * dims * elem;

        GAddr A = dsm_malloc_c((Size)Asz, /*owner=*/r + 1);
        GAddr B = dsm_malloc_c((Size)Bsz, /*owner=*/r + 1);
        GAddr C = dsm_malloc_c((Size)Csz, /*owner=*/r + 1);
        if (!A || !B || !C) {
            log_error("DSM alloc failed on r=%d (A=%#lx B=%#lx C=%#lx)", r,
                      (unsigned long)A,(unsigned long)B,(unsigned long)C);
            /* TODO: free previous allocations if you want to be strict */
            free(dir);
            return -1;
        }

        row0v[r] = row0; row1v[r] = row1;
        A_addr[r]= A;    A_bytes[r]=(Size)Asz;
        B_addr[r]= B;    B_bytes[r]=(Size)Bsz;
        C_addr[r]= C;    C_bytes[r]=(Size)Csz;

        log_info("mm_dir3: r=%d rows=[%d,%d) A=%#lx(%lu) B=%#lx(%lu) C=%#lx(%lu)",
                 r, row0, row1,
                 (unsigned long)A,(unsigned long)Asz,
                 (unsigned long)B,(unsigned long)Bsz,
                 (unsigned long)C,(unsigned long)Csz);
    }

    /* 3) write A shards and B replicas from rank0 host */
    {
        /* allocate temp host buffers just once */
        double* Aall = (double*)malloc((size_t)dims * (size_t)dims * sizeof(double));
        double* Ball = (double*)malloc((size_t)dims * (size_t)dims * sizeof(double));
        if (!Aall || !Ball) {
            free(Aall); free(Ball); free(dir);
            log_error("OOM Aall/Ball");
            return -1;
        }
        fill_random_double(Aall, (size_t)dims * (size_t)dims, &seed);
        fill_random_double(Ball, (size_t)dims * (size_t)dims, &seed);

        for (int r = 0; r < W; r++) {
            const int r0 = row0v[r], r1 = row1v[r];
            const size_t rows = (size_t)(r1 - r0);

            /* A shard: contiguous block in row-major Aall starting at r0*K */
            const size_t Aelems = rows * (size_t)dims;
            const size_t Abytes = Aelems * sizeof(double);
            const size_t Aoff   = (size_t)r0 * (size_t)dims;

            if (dsm_write_c(A_addr[r], (const void*)(Aall + Aoff), (Size)Abytes) != 0) {
                log_error("write A shard to r=%d failed", r);
                free(Aall); free(Ball); free(dir);
                return -1;
            }
            /* B replica: full Ball to each rank */
            const size_t Bbytes = (size_t)dims * (size_t)dims * sizeof(double);
            if (dsm_write_c(B_addr[r], (const void*)Ball, (Size)Bbytes) != 0) {
                log_error("write B replica to r=%d failed", r);
                free(Aall); free(Ball); free(dir);
                return -1;
            }
        }
        free(Aall); free(Ball);
    }

    /* 4) create directory object in DSM and write once */
    GAddr dir_gaddr = dsm_malloc_c((Size)dir_bytes, /*owner=*/0);
    if (!dir_gaddr) {
        free(dir);
        log_error("DSM alloc dir blob failed");
        return -1;
    }
    if (dsm_write_c(dir_gaddr, (const void*)dir, (Size)dir_bytes) != 0) {
        dsm_free_c(dir_gaddr);
        free(dir);
        log_error("DSM write dir blob failed");
        return -1;
    }
    free(dir);

    log_info("mm_dir3: directory created at GAddr=%#lx (bytes=%zu)",
             (unsigned long)dir_gaddr, dir_bytes);

    // /* 5) publish GAddr via control-plane (KV) and barrier */
    // if (ctrl_put_u64("mm_dir3", (uint64_t)dir_gaddr, /*to=*/10000) != 0) {
    //     log_warn("ctrl_put_u64(mm_dir3) failed (others may not find dir)");
    //     /* still return success, caller may handle */
    // }
    // if (ctrl_barrier("mm_dir3_ready", /*gen=*/0, /*to=*/60000) != 0) {
    //     log_warn("barrier mm_dir3_ready failed");
    // }

    *out_dir_gaddr = dir_gaddr;
    return 0;
}

int main(int argc, char** argv) {
    if (argc < 3 || argc > 4) {
        fprintf(stderr, "Usage: %s <config_path> <rank> [log_path]\n", argv[0]);
        return 1;
    }
    const char *cfg = argv[1];
    int my_rank     = atoi(argv[2]);
    const char *log_path = argv[3];

    if (leopar_init(cfg, my_rank, log_path) != 0) {
        fprintf(stderr, "leopar_init failed\n");
        return 1;
    }
    int world_size = leo_world_size();
    int matrix_size[] = {1024, 2048, 4096};
    if(my_rank == 0) {
        log_info("mm.c: leopar_init ok: rank %d", my_rank);
        GAddr dir_gaddr = 0;
        for(int i = 0; i < sizeof(matrix_size)/sizeof(matrix_size[0]); i++) {
            log_info("mm.c: Building matrices of size %dx%d", matrix_size[i], matrix_size[i]);
            mm_dir3_build_on_rank0(matrix_size[i], /*seed=*/12345u, /*out*/&dir_gaddr);
        }
        
        leopar_finalize();
    } else {
        log_info("mm.c: leopar_init ok: rank %d", my_rank);
        sleep(80); // ensure rank 0 prints first
        leopar_finalize();
    }
    return 0;
}