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

/* Choose a rank for thread execution automatically.
* Returns rank in range [0, world_size-1].
*/
int scheduler_choose_rank(int world_size);

#endif /* SCHEDULER_H */
