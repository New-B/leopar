/**
 * @file scheduler.h
 * @author Wang Bo
 * @date 2025-09-13
 * @brief Scheduling policies for thread placement in LeoPar runtime.
 *
 * This module provides rank selection strategies for distributed
 * thread creation. Currently, it implements a round-robin scheduler.
 */

#ifndef SCHEDULER_H
#define SCHEDULER_H

#include <stdint.h>

/* Choose a rank for thread execution automatically.
* Returns rank in range [0, world_size-1].
*/
int scheduler_choose_rank(int world_size);
int scheduler_choose_rank_hint(int world_size, uint64_t locality_key, int priority);
void scheduler_note_placement(int rank);
void scheduler_note_completion(int rank);
int scheduler_locality_home_rank(uint64_t locality_key, int world_size);
int scheduler_estimated_load(int rank);

#endif /* SCHEDULER_H */
