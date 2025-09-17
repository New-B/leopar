/**
 * @file functable.h
 * @author Wang Bo
 * @date 2025-09-13
 * @brief Function table for LeoPar runtime.
 *
 * This module manages the function table for pthread-style thread creation.
 * Provides dynamic registration of functions with unique IDs.
 * Ensures mapping between function name, function ID, and function pointer.
 * It allows registering user functions and retrieving them by ID, so that
 * remote nodes can execute the same function identified by an integer.
 */

#ifndef FUNCTABLE_H
#define FUNCTABLE_H

#include <stddef.h>

/* Maximum number of functions to register */
#define MAX_FUNCS 2048
/* Maximum length of a function name */
#define MAX_FUNC_NAME 128

/* Function table entry */
typedef struct {
    int   in_use;
    int   func_id;
    char  name[MAX_FUNC_NAME];
    void *(*fn)(void*);
} func_entry_t;

/* Initialize the function table */
int functable_init(void);

/* Finalize the function table (cleanup) */
int functable_finalize(void);

/* Register a function by name and pointer.
 * If the function is already registered, returns its existing ID.
 * Otherwise assigns a new ID and returns it.
 * Returns -1 on error (table full or name too long). */
int functable_register(const char *name, void *(*fn)(void*));

/* Bind at a specific func_id. If the slot is empty, this will fill it.
 * If already in use and name matches, it will update the pointer if NULL.
 * Returns 0 on success, -1 on error (invalid id/range or name mismatch).
 */
int functable_bind_local(const char *name, int func_id, void *(*fn)(void*));

/* Get function pointer by ID.
 * Returns NULL if ID is invalid or not registered. */
void *(*functable_get_by_id(int id))(void*);

/* Get ID by function name.
 * Returns -1 if not found. */
int functable_get_id_by_name(const char *name);

/* Get ID by function pointer.
 * Returns -1 if not found. */
int functable_get_id_by_ptr(void *(*fn)(void*));


#endif /* FUNCTABLE_H */
