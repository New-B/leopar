/**
 * @file log.c
 * @author Wang Bo
 * @date 2025-09-09
 * @brief Implementation of logging system with levels for LeoPar runtime.
 */

 #include "log.h"
 #include <stdlib.h>
 #include <time.h>
 
 static FILE *log_file = NULL;
 
 static const char *level_str[] = {
     "DEBUG", "INFO", "WARN", "ERROR"
 };
 
 int log_init(const char *filename) {
     log_file = fopen(filename, "w");
     if (!log_file) return -1;
     return 0;
 }
 
 void log_finalize(void) {
     if (log_file) {
         fclose(log_file);
         log_file = NULL;
     }
 }
 
 void log_msg(log_level_t level, const char *fmt, ...) {
     if (!log_file) return;
 
     /* Timestamp */
     time_t now = time(NULL);
     struct tm *t = localtime(&now);
     char buf[64];
     strftime(buf, sizeof(buf), "%Y-%m-%d %H:%M:%S", t);
 
     /* Print log */
     fprintf(log_file, "[%s] %-5s ", buf, level_str[level]);
 
     va_list args;
     va_start(args, fmt);
     vfprintf(log_file, fmt, args);
     va_end(args);
 
     fprintf(log_file, "\n");
     fflush(log_file);
 }
 