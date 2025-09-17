/**
 * @file log.c
 * @author Wang Bo
 * @date 2025-09-09
 * @brief Implementation of logging system with levels for LeoPar runtime.
 */

#include "log.h"
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <stdarg.h>

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

void log_msg_internal(LogLevel level, const char *file, int line, const char *fmt, ...) {
    if (!log_file) return;
    if (level > log_level) return;

    /* Timestamp */
    time_t now = time(NULL);
    struct tm *t = localtime(&now);
    char buf[64];
    strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", t);

    /* Print prefix */
    if (g_rank_context >= 0) {
        fprintf(log_file, "[%s] %-5s [rank %d] (%s:%d) ",
                buf, level_str[level], g_rank_context, file, line);
    } else {
        fprintf(log_file, "[%s] %-5s (%s:%d) ",
                buf, level_str[level], file, line);
    }

    /* Print log message */
    va_list args;
    va_start(args, fmt);
    vfprintf(log_file, fmt, args);
    va_end(args);

    fprintf(log_file, "\n");
    fflush(log_file);
}
