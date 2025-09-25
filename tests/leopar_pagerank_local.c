/**
 * @file leopar_pagerank_web.c
 * @author You
 * @date 2025-09-25
 * @brief PageRank on SNAP Web-Google graph using LeoPar threads (single machine, no DSM).
 *
 * Usage:
 *   ./bin/leopar_pagerank_web <cluster.ini> <rank> <graph.txt> <threads> <iters> <damp> [topk]
 *   Example:
 *     ./bin/leopar_pagerank_web cluster.ini 0 web-Google.txt 8 20 0.85 10
 *
 * Notes:
 *   - This program assumes world_size=1 (single process) in cluster.ini.
 *   - Graph format: SNAP Web-Google (lines "src dst", '#' comments ignored, 0-based IDs).
 *   - Two-pass loader builds in-edge CSR and out-degree.
 *   - PageRank uses pull model with explicit dangling mass redistribution.
 */

#include "leopar.h"     /* leo_thread_create / leo_thread_join / leopar_init / finalize */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <math.h>

/* ---------------- Graph in CSR (in-edges) ---------------- */
typedef struct {
    int     n;           /* number of vertices (assumed 0..n-1) */
    long long m;         /* number of directed edges */
    int    *in_rowptr;   /* length n+1 */
    int    *in_colidx;   /* length m  */
    int    *outdeg;      /* length n  */
} Graph;

/* Safe fgets line reader */
static int read_line(FILE *fp, char *buf, size_t cap) {
    return fgets(buf, cap, fp) != NULL;
}

/* Parse a line "u v" ignoring comments and blank lines; returns 1 on success */
static int parse_edge_line(const char *s, int *u, int *v) {
    /* skip leading spaces */
    while (*s==' '||*s=='\t'||*s=='\r') ++s;
    if (*s=='#' || *s=='\n' || *s=='\0') return 0;
    /* parse two integers */
    char *endp = NULL;
    long su = strtol(s, &endp, 10);
    if (s==endp) return 0;
    s = endp;
    long sv = strtol(s, &endp, 10);
    if (s==endp) return 0;
    if (su < 0 || sv < 0) return 0;
    *u = (int)su; *v = (int)sv;
    return 1;
}

/* Two-pass loader for SNAP Web-Google format */
static Graph* load_snap_web_google(const char *path) {
    FILE *fp = fopen(path, "r");
    if (!fp) {
        fprintf(stderr, "ERROR: cannot open %s\n", path);
        return NULL;
    }

    const size_t L = 1<<20; /* 1MB line buffer */
    char *line = (char*)malloc(L);
    if (!line) { fclose(fp); fprintf(stderr, "OOM line buffer\n"); return NULL; }

    /* ---- Pass 1: count N, M; compute indeg/outdeg ---- */
    long long M = 0;
    int max_id = -1;

    /* For pass1, we don't know N yet; we accumulate counts in a dynamic map by resizing arrays.
    Strategy: first find max_id by single scan, then allocate arrays and rewind to recount. */

    /* First scan to get max_id and M */
    while (read_line(fp, line, L)) {
        int u, v;
        if (!parse_edge_line(line, &u, &v)) continue;
        if (u > max_id) max_id = u;
        if (v > max_id) max_id = v;
        M++;
    }
    if (max_id < 0) {
        free(line); fclose(fp);
        fprintf(stderr, "ERROR: empty or invalid graph file\n");
        return NULL;
    }
    int N = max_id + 1;

    int *indeg  = (int*)calloc(N, sizeof(int));
    int *outdeg = (int*)calloc(N, sizeof(int));
    if (!indeg || !outdeg) {
        free(line); fclose(fp);
        free(indeg); free(outdeg);
        fprintf(stderr, "OOM indeg/outdeg arrays (N=%d)\n", N);
        return NULL;
    }

    /* Rewind and count indegree/outdegree */
    rewind(fp);
    long long M2 = 0;
    while (read_line(fp, line, L)) {
        int u, v;
        if (!parse_edge_line(line, &u, &v)) continue;
        outdeg[u]++;
        indeg[v]++;
        M2++;
    }
    if (M2 != M) {
        fprintf(stderr, "WARN: edge count changed between passes: %lld vs %lld\n", M, M2);
        M = M2;
    }

    /* Build in_rowptr by prefix sum of indeg */
    int *in_rowptr = (int*)malloc(sizeof(int)*(N+1));
    if (!in_rowptr) {
        free(line); fclose(fp);
        free(indeg); free(outdeg);
        fprintf(stderr, "OOM in_rowptr\n");
        return NULL;
    }
    in_rowptr[0] = 0;
    for (int v = 0; v < N; ++v) in_rowptr[v+1] = in_rowptr[v] + indeg[v];

    /* Allocate in_colidx and a cursor */
    int *in_colidx = (int*)malloc(sizeof(int)*M);
    int *cursor    = (int*)malloc(sizeof(int)*N);
    if (!in_colidx || !cursor) {
        free(line); fclose(fp);
        free(indeg); free(outdeg); free(in_rowptr); free(in_colidx); free(cursor);
        fprintf(stderr, "OOM in_colidx/cursor\n");
        return NULL;
    }
    memcpy(cursor, in_rowptr, sizeof(int)*N);

    /* ---- Pass 2: fill in_colidx with in-neighbors ---- */
    rewind(fp);
    while (read_line(fp, line, L)) {
        int u, v;
        if (!parse_edge_line(line, &u, &v)) continue;
        int pos = cursor[v]++;
        in_colidx[pos] = u;
    }

    free(cursor);
    free(indeg);
    free(line);
    fclose(fp);

    Graph *G = (Graph*)malloc(sizeof(Graph));
    if (!G) {
        free(in_rowptr); free(in_colidx); free(outdeg);
        fprintf(stderr, "OOM Graph struct\n");
        return NULL;
    }
    G->n = N;
    G->m = M;
    G->in_rowptr = in_rowptr;
    G->in_colidx = in_colidx;
    G->outdeg    = outdeg;
    return G;
}

static void free_graph(Graph *G) {
    if (!G) return;
    free(G->in_rowptr);
    free(G->in_colidx);
    free(G->outdeg);
    free(G);
}

/* --------------- PageRank worker --------------- */
typedef struct {
    const Graph *G;
    const double *pr_old;
    double *pr_new;
    int v_start;          /* inclusive */
    int v_end;            /* exclusive */
    double base;          /* (1-d)/N + d*dangling/N */
    double d;             /* damping factor */
} PRTask;

/* Worker computes pr_new[v] for v in [v_start, v_end) using pull model */
static void* pr_worker(void *arg)
{
    PRTask *t = (PRTask*)arg;
    const Graph *G = t->G;
    const double *pr_old = t->pr_old;
    double *pr_new = t->pr_new;
    const int *in_rowptr = G->in_rowptr;
    const int *in_colidx = G->in_colidx;
    const int *outdeg    = G->outdeg;

    for (int v = t->v_start; v < t->v_end; ++v) {
        double sum = 0.0;
        int beg = in_rowptr[v], end = in_rowptr[v+1];
        for (int p = beg; p < end; ++p) {
            int u = in_colidx[p];
            int od = outdeg[u];
            if (od > 0) sum += pr_old[u] / (double)od;
            /* Dangling nodes handled globally via "base" */
        }
        pr_new[v] = t->base + t->d * sum;
    }
    return NULL;
}

/* Compute L1 diff between two PR vectors */
static double l1_diff(const double *a, const double *b, int n)
{
    double s = 0.0;
    for (int i = 0; i < n; ++i) s += fabs(a[i] - b[i]);
    return s;
}

/* Top-k printer */
static int cmp_desc_indexed(const void *pa, const void *pb, void *ctx)
{
    const double *pr = (const double*)ctx;
    int ia = *(const int*)pa;
    int ib = *(const int*)pb;
    if (pr[ia] > pr[ib]) return -1;
    if (pr[ia] < pr[ib]) return 1;
    return 0;
}

static void print_topk(const double *pr, int n, int k)
{
    if (k > n) k = n;
    int *idx = (int*)malloc(sizeof(int)*n);
    for (int i = 0; i < n; ++i) idx[i] = i;
#if defined(__GLIBC__)
    qsort_r(idx, n, sizeof(int), cmp_desc_indexed, (void*)pr);
#else
    /* Portable fallback: use global context if qsort_r unavailable */
    static const double *gctx;
    gctx = pr;
    int cmp(const void *pa, const void *pb) {
        int ia = *(const int*)pa, ib = *(const int*)pb;
        if (gctx[ia] > gctx[ib]) return -1;
        if (gctx[ia] < gctx[ib]) return 1;
        return 0;
    }
    qsort(idx, n, sizeof(int), cmp);
#endif
    printf("Top-%d nodes by PageRank:\n", k);
    for (int i = 0; i < k; ++i) {
        printf("  v=%d  pr=%.8f\n", idx[i], pr[idx[i]]);
    }
    free(idx);
}

/* --------------- Main --------------- */
int main(int argc, char **argv)
{
    if (argc < 7) {
        fprintf(stderr, "usage: %s <cluster.ini> <rank> <log_file> <graph.txt> <threads> <iters> <damp> [topk]\n", argv[0]);
        return 1;
    }

    const char *cfg    = argv[1];
    int         rank   = atoi(argv[2]);          /* use 0 for single-machine demo */
    const char *log_path = argv[3];
    const char *gfile  = argv[4];
    int         T      = atoi(argv[5]);
    int         iters  = atoi(argv[6]);
    double      d      = atof(argv[7]);
    int         topk   = (argc >= 9) ? atoi(argv[8]) : 9;

    /* Initialize LeoPar runtime (world_size must be 1) */
    if (leopar_init(cfg, rank, NULL) != 0) {
        fprintf(stderr, "leopar_init failed\n");
        return 1;
    }



    if (argc < 7) {
        fprintf(stderr, "usage: %s <cluster.ini> <rank> <N> <threads> <iters> <d>\n", argv[0]);
        fprintf(stderr, "example: %s cluster.ini 0 10000 8 10 0.85\n", argv[0]);
        return 1;
    }

    const char *cfg   = argv[1];
    int rank          = atoi(argv[2]);          /* use 0 for single-machine demo */
    int N             = atoi(argv[3]);
    int T             = atoi(argv[4]);
    int iters         = atoi(argv[5]);
    double d          = atof(argv[6]);

    /* Initialize LeoPar runtime (world_size must be 1 in cluster.ini) */
    const char *config = argv[1];
    int my_rank = atoi(argv[2]);
    const char *log_path = (argc >= 4) ? argv[3] : NULL;

    if (leopar_init(config, my_rank, log_path) != 0) {
        fprintf(stderr, "leopar_init failed\n");
        return 1;
    }






    /* Load graph */
    Graph *G = load_snap_web_google(gfile);
    if (!G) { leopar_finalize(); return 1; }
    const int N = G->n;

    /* Allocate PR vectors and init to 1/N */
    double *pr_old = (double*)malloc(sizeof(double)*N);
    double *pr_new = (double*)malloc(sizeof(double)*N);
    if (!pr_old || !pr_new) {
        fprintf(stderr, "OOM PR vectors (N=%d)\n", N);
        free_graph(G); leopar_finalize(); return 1;
    }
    for (int i = 0; i < N; ++i) pr_old[i] = 1.0 / (double)N;

    const double eps  = 1e-8;  /* convergence threshold */
    double base_term  = 0.0;   /* per-iteration base including dangling mass */

    for (int it = 0; it < iters; ++it) {
        /* Compute dangling mass: sum PR of nodes with outdeg=0 */
        double dangling = 0.0;
        for (int u = 0; u < N; ++u) {
            if (G->outdeg[u] == 0) dangling += pr_old[u];
        }
        base_term = (1.0 - d) / (double)N + d * (dangling / (double)N);

        /* Zero pr_new (each thread only writes its slice) */
        memset(pr_new, 0, sizeof(double)*N);

        /* Spawn T workers covering disjoint ranges of vertices */
        leo_thread_t *tids  = (leo_thread_t*)malloc(sizeof(leo_thread_t) * T);
        PRTask *tasks       = (PRTask*)malloc(sizeof(PRTask) * T);
        if (!tids || !tasks) { fprintf(stderr, "OOM thread arrays\n"); return 1; }

        for (int t = 0; t < T; ++t) {
            long long s = (long long)t     * N / T;
            long long e = (long long)(t+1) * N / T;
            tasks[t].G       = G;
            tasks[t].pr_old  = pr_old;
            tasks[t].pr_new  = pr_new;
            tasks[t].v_start = (int)s;
            tasks[t].v_end   = (int)e;
            tasks[t].base    = base_term;
            tasks[t].d       = d;

            /* target_rank = 0 (local) */
            if (leo_thread_create(&tids[t], NULL, pr_worker, &tasks[t], 0) != 0) {
                fprintf(stderr, "leo_thread_create failed at t=%d\n", t);
                return 1;
            }
        }
        for (int t = 0; t < T; ++t) {
            leo_thread_join(tids[t], NULL);
        }
        free(tids);
        free(tasks);

        /* Check convergence (L1 norm) */
        double diff = l1_diff(pr_new, pr_old, N);
        printf("iter %d: L1 diff=%.10f\n", it, diff);

        /* Swap */
        double *tmp = pr_old; pr_old = pr_new; pr_new = tmp;

        if (diff < eps) {
            printf("converged at iter %d\n", it);
            break;
        }
    }

    print_topk(pr_old, N, topk);

    free(pr_old);
    free(pr_new);
    free_graph(G);
    leopar_finalize();
    return 0;
}




























/**
 * @file leopar_pagerank_local.c
 * @author You
 * @date 2025-09-25
 * @brief Single-machine PageRank using LeoPar threads (no DSM).
 *
 * Build: add to your tests targets.
 * Run example:
 *   ./bin/leopar_pagerank_local cluster.ini 0 10000 8 10 0.85
 *     arg1: cluster.ini  (must set world_size=1)
 *     arg2: rank         (use 0)
 *     arg3: N            (number of vertices)
 *     arg4: T            (threads per iteration)
 *     arg5: iters        (max iterations)
 *     arg6: d            (damping factor, e.g., 0.85)
 *
 * Notes:
 * - No DSM calls. All data are in process-local memory.
 * - Parallelization pattern: per-iteration spawn T threads for disjoint vertex ranges,
 *   each thread computes pr_new[v] by pulling from in-edges (read-only pr_old/outdeg).
 * - No locking/barriers needed because each thread writes to a disjoint range.
 */

#include "leopar.h"     /* leo_thread_create / leo_thread_join / leopar_init / finalize */
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <math.h>
#include <time.h>

/* ---------------- Graph in CSR (in-edges) ----------------
* We keep:
*  - in_rowptr[v] : index into in_colidx where in-neighbors of v begin
*  - in_colidx[k] : in-neighbor vertex id (u -> v)
*  - outdeg[u]    : out-degree for each vertex (needed for PR normalization)
*/
typedef struct {
    int     n;           /* number of vertices */
    int     m;           /* number of directed edges */
    int    *in_rowptr;   /* length n+1 */
    int    *in_colidx;   /* length m */
    int    *outdeg;      /* length n */
} Graph;

/* Simple random directed graph generator (uniform targets, no self-loop check) */
static Graph* gen_random_graph(int n, int avg_out)
{
    int m = n * avg_out;
    /* Temporary edge list (u -> v) */
    int *src = (int*)malloc(sizeof(int) * m);
    int *dst = (int*)malloc(sizeof(int) * m);
    if (!src || !dst) { fprintf(stderr, "OOM edge list\n"); exit(1); }

    /* Seed RNG */
    srand(12345);
    /* Build avg_out out-edges for each vertex u */
    int idx = 0;
    for (int u = 0; u < n; ++u) {
        for (int k = 0; k < avg_out; ++k) {
            int v = rand() % n;
            src[idx] = u;
            dst[idx] = v;
            idx++;
        }
    }

    /* Compute out-degree */
    int *outdeg = (int*)calloc(n, sizeof(int));
    for (int e = 0; e < m; ++e) outdeg[src[e]]++;

    /* Build in-degree counts */
    int *indeg = (int*)calloc(n, sizeof(int));
    for (int e = 0; e < m; ++e) indeg[dst[e]]++;

    /* Prefix sum to get in_rowptr */
    int *in_rowptr = (int*)malloc(sizeof(int) * (n + 1));
    in_rowptr[0] = 0;
    for (int v = 0; v < n; ++v) in_rowptr[v+1] = in_rowptr[v] + indeg[v];

    /* Fill in_colidx with a cursor per vertex */
    int *cursor = (int*)malloc(sizeof(int) * n);
    memcpy(cursor, in_rowptr, sizeof(int) * n);

    int *in_colidx = (int*)malloc(sizeof(int) * m);
    for (int e = 0; e < m; ++e) {
        int u = src[e], v = dst[e];
        int pos = cursor[v]++;
        in_colidx[pos] = u;
    }

    free(src); free(dst); free(indeg); free(cursor);

    Graph *G = (Graph*)malloc(sizeof(Graph));
    G->n = n; G->m = m;
    G->in_rowptr = in_rowptr;
    G->in_colidx = in_colidx;
    G->outdeg    = outdeg;
    return G;
}

static void free_graph(Graph *G)
{
    if (!G) return;
    free(G->in_rowptr);
    free(G->in_colidx);
    free(G->outdeg);
    free(G);
}

/* --------------- PageRank worker --------------- */

typedef struct {
    const Graph *G;
    const double *pr_old;
    double *pr_new;
    int v_start;          /* inclusive */
    int v_end;            /* exclusive */
    double base;          /* (1-d)/N */
    double d;             /* damping factor */
} PRTask;

/* Worker computes pr_new[v] for v in [v_start, v_end) using pull model */
static void* pr_worker(void *arg)
{
    PRTask *t = (PRTask*)arg;
    const Graph *G = t->G;
    const double *pr_old = t->pr_old;
    double *pr_new = t->pr_new;
    const int *in_rowptr = G->in_rowptr;
    const int *in_colidx = G->in_colidx;
    const int *outdeg = G->outdeg;

    for (int v = t->v_start; v < t->v_end; ++v) {
        double sum = 0.0;
        int beg = in_rowptr[v], end = in_rowptr[v+1];
        for (int p = beg; p < end; ++p) {
            int u = in_colidx[p];
            int od = outdeg[u] > 0 ? outdeg[u] : 1;  /* guard divide-by-zero */
            sum += pr_old[u] / (double)od;
        }
        pr_new[v] = t->base + t->d * sum;
    }
    return NULL;
}

/* Compute L1 diff between two PR vectors */
static double l1_diff(const double *a, const double *b, int n)
{
    double s = 0.0;
    for (int i = 0; i < n; ++i) s += fabs(a[i] - b[i]);
    return s;
}

/* Top-k printer */
static int cmp_desc_indexed(const void *pa, const void *pb, void *ctx)
{
    const double *pr = (const double*)ctx;
    int ia = *(const int*)pa;
    int ib = *(const int*)pb;
    if (pr[ia] > pr[ib]) return -1;
    if (pr[ia] < pr[ib]) return 1;
    return 0;
}

static void print_topk(const double *pr, int n, int k)
{
    if (k > n) k = n;
    int *idx = (int*)malloc(sizeof(int)*n);
    for (int i = 0; i < n; ++i) idx[i] = i;
#if defined(__GLIBC__)
    qsort_r(idx, n, sizeof(int), cmp_desc_indexed, (void*)pr);
#else
    /* portable fallback: wrap global */
    /* Simple bubble for small k (avoid complicating), or implement a small partial sort.
    For brevity, use full qsort with static context for non-glibc compilers. */
    static const double *gctx;
    gctx = pr;
    int cmp(const void *pa, const void *pb) {
        int ia = *(const int*)pa, ib = *(const int*)pb;
        if (gctx[ia] > gctx[ib]) return -1;
        if (gctx[ia] < gctx[ib]) return 1;
        return 0;
    }
    qsort(idx, n, sizeof(int), cmp);
#endif
    printf("Top-%d nodes by PageRank:\n", k);
    for (int i = 0; i < k; ++i) {
        printf("  v=%d  pr=%.6f\n", idx[i], pr[idx[i]]);
    }
    free(idx);
}

/* --------------- Main --------------- */

int main(int argc, char **argv)
{
    if (argc < 7) {
        fprintf(stderr, "usage: %s <cluster.ini> <rank> <N> <threads> <iters> <d>\n", argv[0]);
        fprintf(stderr, "example: %s cluster.ini 0 10000 8 10 0.85\n", argv[0]);
        return 1;
    }

    const char *cfg   = argv[1];
    int rank          = atoi(argv[2]);          /* use 0 for single-machine demo */
    int N             = atoi(argv[3]);
    int T             = atoi(argv[4]);
    int iters         = atoi(argv[5]);
    double d          = atof(argv[6]);

    /* Initialize LeoPar runtime (world_size must be 1 in cluster.ini) */
    const char *config = argv[1];
    int my_rank = atoi(argv[2]);
    const char *log_path = (argc >= 4) ? argv[3] : NULL;

    if (leopar_init(config, my_rank, log_path) != 0) {
        fprintf(stderr, "leopar_init failed\n");
        return 1;
    }

    /* Build a random graph with avg_out ~ 8 (you can change) */
    int avg_out = 8;
    Graph *G = gen_random_graph(N, avg_out);

    /* Allocate PR vectors */
    double *pr_old = (double*)malloc(sizeof(double)*N);
    double *pr_new = (double*)malloc(sizeof(double)*N);
    if (!pr_old || !pr_new) { fprintf(stderr, "OOM pr\n"); return 1; }

    /* Init PR to 1/N */
    for (int i = 0; i < N; ++i) pr_old[i] = 1.0 / (double)N;

    double base = (1.0 - d) / (double)N;
    double eps  = 1e-6;   /* convergence threshold */

    /* Iterations */
    for (int it = 0; it < iters; ++it) {
        /* zero pr_new */
        memset(pr_new, 0, sizeof(double)*N);

        /* spawn T workers: disjoint vertex ranges */
        leo_thread_t *tids = (leo_thread_t*)malloc(sizeof(leo_thread_t) * T);
        PRTask *tasks = (PRTask*)malloc(sizeof(PRTask) * T);

        for (int t = 0; t < T; ++t) {
            int v_start = (long long)t     * N / T;
            int v_end   = (long long)(t+1) * N / T;
            tasks[t].G = G;
            tasks[t].pr_old = pr_old;
            tasks[t].pr_new = pr_new;
            tasks[t].v_start = v_start;
            tasks[t].v_end   = v_end;
            tasks[t].base    = base;
            tasks[t].d       = d;

            /* target_rank=0 (local), attr=NULL, arg=&tasks[t] */
            int rc = leo_thread_create(&tids[t], NULL, pr_worker, &tasks[t], /*target_rank=*/0);
            if (rc != 0) {
                fprintf(stderr, "leo_thread_create failed at t=%d\n", t);
                return 1;
            }
        }
        for (int t = 0; t < T; ++t) {
            leo_thread_join(tids[t], NULL);
        }
        free(tids);
        free(tasks);

        /* check convergence */
        double diff = l1_diff(pr_new, pr_old, N);
        printf("iter %d: L1 diff=%.8f\n", it, diff);
        /* swap */
        double *tmp = pr_old; pr_old = pr_new; pr_new = tmp;

        if (diff < eps) { printf("converged at iter %d\n", it); break; }
    }

    print_topk(pr_old, N, 10);

    free(pr_old);
    free(pr_new);
    free_graph(G);

    leopar_finalize();
    return 0;
}
 