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
     LOG_DEBUG,
     LOG_INFO,
     LOG_WARN,
     LOG_ERROR
 } log_level_t;
 
 /* Logging interface */
 int log_init(const char *filename);
 void log_finalize(void);
 void log_msg(log_level_t level, const char *fmt, ...);
 
 /* Shortcut macros */
 #define log_debug(fmt, ...) log_msg(LOG_DEBUG, fmt, ##__VA_ARGS__)
 #define log_info(fmt, ...)  log_msg(LOG_INFO,  fmt, ##__VA_ARGS__)
 #define log_warn(fmt, ...)  log_msg(LOG_WARN,  fmt, ##__VA_ARGS__)
 #define log_error(fmt, ...) log_msg(LOG_ERROR, fmt, ##__VA_ARGS__)
 
 #endif /* LOG_H */
 