/**
 * @file log.h
 * @author Wang Bo
 * @date 2025-09-09
 * @brief Logging system with log levels for LeoPar runtime.
 */

#ifndef LOG_H
#define LOG_H

#include <stdio.h>
#include <stdarg.h>

 /* Log levels */
typedef enum {
    LOG_ERROR = 0,
    LOG_WARN,
    LOG_INFO,
    LOG_DEBUG
} LogLevel;

#ifndef LEOPAR_DEFAULT_LOG_LEVEL
#define LEOPAR_DEFAULT_LOG_LEVEL LOG_ERROR
#endif

 /* Logging interface */
int log_init(const char *filename, LogLevel level);
void log_finalize(void);
void log_set_rank(int rank);

/* Core logging function (internal use) */
void log_msg_internal(LogLevel level, const char *file, int line, const char *fmt, ...);

 /* Shortcut macros: automatically add __FIFE__ and __LINE__ */
#define log_debug(fmt, ...) log_msg_internal(LOG_DEBUG, __FILE__, __LINE__, fmt, ##__VA_ARGS__)
#define log_info(fmt, ...)  log_msg_internal(LOG_INFO,  __FILE__, __LINE__, fmt, ##__VA_ARGS__)
#define log_warn(fmt, ...)  log_msg_internal(LOG_WARN,  __FILE__, __LINE__, fmt, ##__VA_ARGS__)
#define log_error(fmt, ...) log_msg_internal(LOG_ERROR, __FILE__, __LINE__, fmt, ##__VA_ARGS__)

#endif /* LOG_H */
