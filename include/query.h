/**
 * @file query.h
 * @brief Internal stats hooks for LeoPar runtime.
 */

#ifndef QUERY_H
#define QUERY_H

#include <stddef.h>

void leo_stats_note_create_sent(void);
void leo_stats_note_create_recv(void);
void leo_stats_note_join_sent(void);
void leo_stats_note_join_recv(void);
void leo_stats_note_ctrl_tx(size_t bytes);
void leo_stats_note_ctrl_rx(size_t bytes);
void leo_stats_note_scheduler_rr(void);
void leo_stats_note_scheduler_hint(void);
void leo_stats_note_scheduler_locality(void);
void leo_stats_note_scheduler_locality_hit(void);
void leo_stats_note_scheduler_locality_miss(void);
void leo_stats_note_scheduler_load_escape(void);

#endif /* QUERY_H */
