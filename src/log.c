/**
 * @file log.c
 * @author Wang Bo
 * @date 2025-09-09
 * @brief Implementation of logging system with levels for LeoPar runtime.
 */
#define _GNU_SOURCE 1

#include "log.h"
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <stdarg.h>
#include <inttypes.h>

#if defined(__linux__)
  #include <sys/syscall.h>
  #include <unistd.h>
#elif defined(__APPLE__)
  #include <pthread.h>
#endif

static FILE *log_file = NULL;
static LogLevel log_level = LOG_INFO;
static int g_rank_context = -1;

static const char *level_str[] = {
    "ERROR", "WARN", "INFO", "DEBUG"
};

int log_init(const char *filename, LogLevel level)
{
    log_file = fopen(filename, "a");
    if (!log_file) {
        log_file = stderr;
        return -1;
    }
    log_level = level;
    return 0;
}


void log_finalize(void)
{
    if (log_file && log_file != stderr) {
        fclose(log_file);
    }
    log_file = NULL;
}

void log_set_rank(int rank)
{
    g_rank_context = rank;
}


/* --- 获取系统线程ID（TID） --- */
static inline uint64_t get_tid_u64(void) {
    #if defined(__linux__)
        return (uint64_t)syscall(SYS_gettid);               // Linux kernel TID
    #elif defined(__APPLE__)
        uint64_t tid = 0; (void)pthread_threadid_np(NULL, &tid);
        return tid ? tid : (uint64_t)(uintptr_t)pthread_self();
    #else
        return (uint64_t)(uintptr_t)pthread_self();         // print pthread_self ptr value
    #endif
}

void log_msg_internal(LogLevel level, const char *file, int line, const char *fmt, ...) {
    if (!log_file) return;
    if (level > log_level) return;

    /* Timestamp */
    time_t now = time(NULL);
    struct tm *t = localtime(&now);
    char buf[64];
    strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", t);

    uint64_t tid = get_tid_u64();

    /* Print prefix */
    if (g_rank_context >= 0) {
        fprintf(log_file, "[%s] %-5s [rank %d] [tid=%" PRIu64 "] (%s:%d) ",
                buf, level_str[level], g_rank_context, tid, file, line);
    } else {
        fprintf(log_file, "[%s] %-5s  [tid=%" PRIu64 "] (%s:%d) ",
                buf, level_str[level], tid, file, line);
    }

    /* Print log message */
    va_list args;
    va_start(args, fmt);
    vfprintf(log_file, fmt, args);
    va_end(args);

    fprintf(log_file, "\n");
    fflush(log_file);
}
