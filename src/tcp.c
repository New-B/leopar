/**
 * @file tcp.c
 * @author Wang Bo
 * @date 2025-09-16
 * @brief TCP helpers for LeoPar runtime bootstrap and control plane.
 */

#include "tcp.h"
#include "context.h"   /* for g_ctx and tcp_config_t */
#include "log.h"

#include <sys/socket.h>
#include <sys/types.h>
#include <sys/select.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <errno.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>

/* ---- Internal helpers ---- */

/* Set or clear non-blocking mode */ 
static int set_nonblock(int fd, int nb)
{
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0) return -1;
    if (nb) flags |= O_NONBLOCK; else flags &= ~O_NONBLOCK;
    return fcntl(fd, F_SETFL, flags);
}

/* Create a listening socket on given port */
static int tcp_listen_on(int port, int *out_fd)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) return -1;

    int on = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &on, sizeof(on));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family      = AF_INET;
    addr.sin_port        = htons((uint16_t)port);
    addr.sin_addr.s_addr = htonl(INADDR_ANY);

    if (bind(fd, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        close(fd);
        return -1;
    }
    if (listen(fd, 128) < 0) {
        close(fd);
        return -1;
    }
    *out_fd = fd;
    return 0;
}

/* Connect to ip:port with a timeout */
static int tcp_connect_timeout(const char *ip, int port, int timeout_ms)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        log_error("socket() failed: %s", strerror(errno));
        return -1;
    }

    set_nonblock(fd, 1);

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family      = AF_INET;
    addr.sin_port        = htons((uint16_t)port);
    if (inet_pton(AF_INET, ip, &addr.sin_addr) != 1) {
        close(fd);
        return -1;
    }

    int rc = connect(fd, (struct sockaddr*)&addr, sizeof(addr));
    if (rc == 0) {
        set_nonblock(fd, 0);
        return fd;
    }
    if (errno != EINPROGRESS) {
        log_error("connect() to %s:%d failed immediately: %s", ip, port, strerror(errno));
        close(fd);
        return -1;
    }

    fd_set wfds;
    FD_ZERO(&wfds);
    FD_SET(fd, &wfds);
    struct timeval tv;
    tv.tv_sec  = timeout_ms / 1000;
    tv.tv_usec = (timeout_ms % 1000) * 1000;

    rc = select(fd + 1, NULL, &wfds, NULL, &tv);
    if (rc == 1 && FD_ISSET(fd, &wfds)) {
        int err = 0; socklen_t len = sizeof(err);
        getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &len);
        if (err == 0) { set_nonblock(fd, 0); return fd; }
        log_error("connect() to %s:%d completed with error: %s", ip, port, strerror(err));
    } else if (rc == 0) {
        log_error("connect() to %s:%d timed out after %d ms", ip, port, timeout_ms);
    } else {
        log_error("select() for connect to %s:%d failed: %s", ip, port, strerror(errno));
    }
    close(fd);
    return -1;
}

/* Blocking send n bytes with a per-iteration poll timeout */
static int tcp_sendn(int fd, const void *buf, size_t n, int timeout_ms)
{
    const char *p = (const char*)buf;
    size_t left = n;
    while (left > 0) {
        fd_set wfds; FD_ZERO(&wfds); FD_SET(fd, &wfds);
        struct timeval tv;
        tv.tv_sec  = timeout_ms / 1000;
        tv.tv_usec = (timeout_ms % 1000) * 1000;
        int rc = select(fd + 1, NULL, &wfds, NULL, &tv);
        if (rc <= 0) return -1;

        ssize_t w = send(fd, p, left, 0);
        if (w <= 0) return -1;
        p += w; left -= (size_t)w;
    }
    return 0;
}

/* Blocking recv n bytes with a per-iteration poll timeout */
static int tcp_recvn(int fd, void *buf, size_t n, int timeout_ms)
{
    char *p = (char*)buf;
    size_t left = n;
    while (left > 0) {
        fd_set rfds; FD_ZERO(&rfds); FD_SET(fd, &rfds);
        struct timeval tv;
        tv.tv_sec  = timeout_ms / 1000;
        tv.tv_usec = (timeout_ms % 1000) * 1000;
        int rc = select(fd + 1, &rfds, NULL, NULL, &tv);
        if (rc <= 0) return -1;

        ssize_t r = recv(fd, p, left, 0);
        if (r <= 0) return -1;
        p += r; left -= (size_t)r;
    }
    return 0;
}

static int tcp_close_fd(int fd)
{
    if (fd >= 0) close(fd);
    return 0;
}

/* Send length-prefixed blob: [u32 len][len bytes] */
static int send_blob(int fd, const void *buf, uint32_t len, int io_to_ms)
{
    if (tcp_sendn(fd, &len, sizeof(len), io_to_ms) != 0) return -1;
    if (len == 0) return 0;
    return tcp_sendn(fd, buf, len, io_to_ms);
}

/* Receive length-prefixed blob into newly malloc'd buffer */
static int recv_blob(int fd, void **out_buf, uint32_t *out_len, int io_to_ms)
{
    uint32_t len = 0;
    if (tcp_recvn(fd, &len, sizeof(len), io_to_ms) != 0) return -1;
    void *b = NULL;
    if (len > 0) {
        b = malloc(len);
        if (!b) return -1;
        if (tcp_recvn(fd, b, len, io_to_ms) != 0) {
            free(b); return -1;
        }
    }
    *out_buf = b;
    *out_len = len;
    return 0;
}

/* Simple authentication: send/recv secret */
static int send_secret(int fd, const char *secret, int io_ms)
{
    uint32_t n = (uint32_t)strlen(secret);
    return send_blob(fd, secret, n, io_ms);
}
static int recv_secret_and_check(int fd, const char *expected, int io_ms)
{
    void *buf = NULL; uint32_t n = 0;
    if (recv_blob(fd, &buf, &n, io_ms) != 0) return -1;
    int ok = 0;
    if (n == strlen(expected) && memcmp(buf, expected, n) == 0) ok = 1;
    free(buf);
    return ok ? 0 : -1;
}

/* Coordinator-based allgather (rank 0 server) */
int tcp_allgather_ucx_addrs(const struct tcp_config_t *cfg,
                            int my_rank, int world_size,
                            const ucp_address_t *my_addr, size_t my_len,
                            ucp_address_t ***out_tbl_addrs,
                            size_t **out_tbl_lens)
{
    if (!cfg || !out_tbl_addrs || !out_tbl_lens) return -1;

    /* Allocate outputs */
    ucp_address_t **tbl_addrs = (ucp_address_t**)calloc(world_size, sizeof(*tbl_addrs));
    size_t *tbl_lens          = (size_t*)calloc(world_size, sizeof(*tbl_lens));
    if (!tbl_addrs || !tbl_lens) {
        free(tbl_addrs); free(tbl_lens);
        return -1;
    }

    int rc = 0;

    if (my_rank == 0) {
        /* Rank 0 listens and accepts world_size-1 connections */
        int listen_fd = -1;
        int listen_port = cfg->base_port;
        if (tcp_listen_on(listen_port, &listen_fd) != 0) {
            log_error("Rank0 listen failed on port=%d", listen_port);
            free(tbl_addrs); free(tbl_lens);
            return -1;
        }
        log_info("Rank0 listening on port=%d", listen_port);

        /* Save own address */
        tbl_lens[0]  = my_len;
        tbl_addrs[0] = (ucp_address_t*)malloc(my_len);
        if (!tbl_addrs[0]) { tcp_close_fd(listen_fd); free(tbl_addrs); free(tbl_lens); return -1; }
        memcpy(tbl_addrs[0], my_addr, my_len);

        /* Accept peers and gather their {rank, secret, addr} */
        for (int count = 1; count < world_size; ++count) {
            struct sockaddr_in cli; socklen_t clilen = sizeof(cli);
            int cfd = accept(listen_fd, (struct sockaddr*)&cli, &clilen);
            if (cfd < 0) { rc = -1; break; }

            /* auth */
            if (recv_secret_and_check(cfd, cfg->secret, cfg->io_timeout_ms) != 0) { tcp_close_fd(cfd); rc = -1; break; }

            /* recv their rank */
            int peer_rank = -1;
            if (tcp_recvn(cfd, &peer_rank, sizeof(peer_rank), cfg->io_timeout_ms) != 0) { tcp_close_fd(cfd); rc = -1; break; }
            if (peer_rank <= 0 || peer_rank >= world_size) { tcp_close_fd(cfd); rc = -1; break; }

            /* recv their addr blob */
            void *b = NULL; uint32_t blen = 0;
            if (recv_blob(cfd, &b, &blen, cfg->io_timeout_ms) != 0) { tcp_close_fd(cfd); rc = -1; break; }

            tbl_lens[peer_rank]  = (size_t)blen;
            tbl_addrs[peer_rank] = (ucp_address_t*)b;

            tcp_close_fd(cfd);
            log_info("Rank0 gathered UCX addr from rank=%d (len=%u)", peer_rank, blen);
        }

        /* Now broadcast the full table to all ranks: each rank connects to rank0 and pulls */
        for (int r = 1; rc == 0 && r < world_size; ++r) {
            int port = cfg->base_port + 1 + r; /* use a per-rank pull port to avoid accept loop complexities */
            int pfd  = -1;
            if (tcp_listen_on(port, &pfd) != 0) { rc = -1; break; }
            log_info("Rank0 ready for table pull on port=%d (rank=%d)", port, r);

            struct sockaddr_in cli; socklen_t clilen = sizeof(cli);
            int cfd = accept(pfd, (struct sockaddr*)&cli, &clilen);
            if (cfd < 0) { tcp_close_fd(pfd); rc = -1; break; }
            log_debug("Rank0 accepted table pull connection from rank=%d", r);

            /* auth */
            if (recv_secret_and_check(cfd, cfg->secret, cfg->io_timeout_ms) != 0) { tcp_close_fd(cfd); tcp_close_fd(pfd); rc = -1; break; }

            /* send world_size, then each blob */
            uint32_t ws = (uint32_t)world_size;
            if (tcp_sendn(cfd, &ws, sizeof(ws), cfg->io_timeout_ms) != 0) { tcp_close_fd(cfd); tcp_close_fd(pfd); rc = -1; break; }
            for (int i = 0; i < world_size; ++i) {
                if (send_blob(cfd, tbl_addrs[i], (uint32_t)tbl_lens[i], cfg->io_timeout_ms) != 0) {
                    tcp_close_fd(cfd); tcp_close_fd(pfd); rc = -1; break;
                }
            }
            tcp_close_fd(cfd);
            tcp_close_fd(pfd);
            log_info("Rank0 served table to rank=%d", r);
        }

        tcp_close_fd(listen_fd);
    } else {
        /* Non-zero ranks: connect to rank0 to send our addr */
        int cfd = -1;
        const char *ip0 = g_ctx.tcp_cfg.ip_of_rank[0];
        int port0 = cfg->base_port;

        for (int attempt = 0; attempt < cfg->retry_max && cfd < 0; ++attempt) {
            cfd = tcp_connect_timeout(ip0, port0, cfg->connect_timeout_ms);
        }
        if (cfd < 0) {
            log_error("Rank%d failed to connect to rank0:%s:%d", my_rank, ip0, port0);
            free(tbl_addrs); free(tbl_lens);
            return -1;
        }

        /* auth */
        if (send_secret(cfd, cfg->secret, cfg->io_timeout_ms) != 0) { tcp_close_fd(cfd); free(tbl_addrs); free(tbl_lens); return -1; }

        /* send my rank */
        if (tcp_sendn(cfd, &my_rank, sizeof(my_rank), cfg->io_timeout_ms) != 0) { tcp_close_fd(cfd); free(tbl_addrs); free(tbl_lens); return -1; }

        /* send my UCX address blob */
        if (send_blob(cfd, my_addr, (uint32_t)my_len, cfg->io_timeout_ms) != 0) { tcp_close_fd(cfd); free(tbl_addrs); free(tbl_lens); return -1; }
        tcp_close_fd(cfd);
        log_debug("Rank%d sent UCX addr to rank0", my_rank);

        /* pull full table from rank0 */
        int pull_fd = -1;
        int pull_port = cfg->base_port + 1 + my_rank;

        for (int attempt = 0; attempt < cfg->retry_max && pull_fd < 0; ++attempt) {
            pull_fd = tcp_connect_timeout(ip0, pull_port, cfg->connect_timeout_ms);
        }
        if (pull_fd < 0) {
            log_error("Rank%d failed to pull table from rank0:%s:%d", my_rank, ip0, pull_port);
            free(tbl_addrs); free(tbl_lens);
            return -1;
        }

        /* auth */
        if (send_secret(pull_fd, cfg->secret, cfg->io_timeout_ms) != 0) { tcp_close_fd(pull_fd); free(tbl_addrs); free(tbl_lens); return -1; }

        uint32_t ws = 0;
        if (tcp_recvn(pull_fd, &ws, sizeof(ws), cfg->io_timeout_ms) != 0 || ws != (uint32_t)world_size) {
            tcp_close_fd(pull_fd); free(tbl_addrs); free(tbl_lens); return -1;
        }

        for (int i = 0; i < world_size; ++i) {
            void *b = NULL; uint32_t blen = 0;
            if (recv_blob(pull_fd, &b, &blen, cfg->io_timeout_ms) != 0) {
                tcp_close_fd(pull_fd); free(tbl_addrs); free(tbl_lens); return -1;
            }
            tbl_lens[i]  = (size_t)blen;
            tbl_addrs[i] = (ucp_address_t*)b;
        }
        tcp_close_fd(pull_fd);
    }

    if (rc != 0) {
        for (int i = 0; i < world_size; ++i) free(tbl_addrs[i]);
        free(tbl_addrs); free(tbl_lens);
        return -1;
    }

    *out_tbl_addrs = tbl_addrs;
    *out_tbl_lens  = tbl_lens;
    return 0;
}
