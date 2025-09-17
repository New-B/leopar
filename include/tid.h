/**
 * @file tid.h
 * @author Wang Bo
 * @date 2025-09-16
 * @brief Utilities for LeoPar global thread IDs (gtid).
 *
 * gtid layout: high 32 bits = rank, low 32 bits = local thread id.
 * This header centralizes packing/unpacking helpers so all modules
 * (leopar.c, dispatcher.c, etc.) share the same definition.
 */

#ifndef TID_H
#define TID_H

#include <stdint.h>

/* ---- Layout constants ---- */
#define LEO_TID_SHIFT    32
#define LEO_TID_MASK_LO  0xffffffffu

/* ---- Static asserts for sanity (C11) ---- */
#if defined(__STDC_VERSION__) && __STDC_VERSION__ >= 201112L
_Static_assert(sizeof(uint64_t) == 8, "uint64_t must be 64-bit");
_Static_assert(sizeof(unsigned int) >= 4, "unsigned int must be at least 32-bit");
#endif

/* ---- Type-safe inline helpers (preferred) ---- */
static inline uint64_t leo_tid_make(int rank, int local_tid)
{
    return (((uint64_t)(uint32_t)rank) << LEO_TID_SHIFT) | (uint32_t)local_tid;
}
static inline int leo_tid_rank(uint64_t gtid)
{
    return (int)((gtid >> LEO_TID_SHIFT) & 0xffffffffu);
}
static inline int leo_tid_local(uint64_t gtid)
{
    return (int)((uint32_t)(gtid & LEO_TID_MASK_LO));
}

/* ---- Macro aliases for backward-compatibility ----
* Use these if you already refer to the macro names in code.
* New code can prefer the inline helpers above.
*/
#define LEO_TID_MAKE(rank, tid)  ( leo_tid_make((rank),(tid)) )
#define LEO_TID_RANK(gtid)       ( leo_tid_rank((uint64_t)(gtid)) )
#define LEO_TID_LOCAL(gtid)      ( leo_tid_local((uint64_t)(gtid)) )

#endif /* TID_H */
