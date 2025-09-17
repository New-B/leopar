/**
 * @file functable.c
 * @author Wang Bo
 * @date 2025-09-13
 * @brief Implementation of function table for LeoPar runtime.
 */

#include "functable.h"
#include "context.h"
#include "proto.h"
#include "ucx.h"   
#include "log.h"

#include <pthread.h>
#include <string.h>
#include <stdlib.h>
#include <assert.h>

static func_entry_t g_func_table[MAX_FUNCS];
static pthread_mutex_t g_func_mtx = PTHREAD_MUTEX_INITIALIZER;
static int g_next_id = 0;

/* Initialize function table */
int functable_init(void)
{
    pthread_mutex_lock(&g_func_mtx);
    for (int i = 0; i < MAX_FUNCS; i++) {
        g_func_table[i].in_use = 0;
        g_func_table[i].func_id = -1;
        g_func_table[i].name[0] = '\0';
        g_func_table[i].fn = NULL;
    }
    g_next_id = 0;
    pthread_mutex_unlock(&g_func_mtx);

    log_info("Function table initialized with %d slots", MAX_FUNCS);
    return 0;
}

/* Finalize function table */
int functable_finalize(void)
{
    pthread_mutex_lock(&g_func_mtx);
    for (int i = 0; i < MAX_FUNCS; i++) {
        g_func_table[i].in_use = 0;
        g_func_table[i].func_id = -1;
        g_func_table[i].name[0] = '\0';
        g_func_table[i].fn = NULL;
    }
    g_next_id = 0;
    pthread_mutex_unlock(&g_func_mtx);

    log_info("Function table finalized");
    return 0;
}

/* Register a function pointer, return func_id */
int functable_register(const char *name, void *(*fn)(void*))
{
    if (!name || !fn) return -1;
    size_t name_len = strlen(name);
    if (name_len == 0 || name_len >= MAX_FUNC_NAME) return -1;

    /* 1) fast path: already registered */
    int existing = functable_get_id_by_ptr(fn);
    if (existing >= 0) return existing;

    existing = functable_get_id_by_name(name);
    if (existing >= 0) {
        /* Fill pointer if missing */
        functable_bind_local(name, existing, fn);
        return existing;
    }

    /* 2) allocate new id locally */
    int new_id = -1;

    pthread_mutex_lock(&g_func_mtx);
    if (g_next_id < MAX_FUNCS) {
        new_id = g_next_id++;
        g_func_table[new_id].in_use  = 1;
        g_func_table[new_id].func_id = new_id;
        strncpy(g_func_table[new_id].name, name, MAX_FUNC_NAME - 1);
        g_func_table[new_id].name[MAX_FUNC_NAME - 1] = '\0';
        g_func_table[new_id].fn = fn;
    }
    pthread_mutex_unlock(&g_func_mtx);

    if (new_id < 0) {
        log_error("Function table full, cannot register %s", name);
        return -1;
    }

    log_info("Registered function name=%s id=%d (local)", name, new_id);

    /* 3) broadcast announce (best-effort, out of lock) */
    {
        size_t blen = sizeof(msg_func_announce_t) + name_len;
        char *bbuf = (char*)malloc(blen);
        if (!bbuf) {
            log_warn("FUNC_ANNOUNCE alloc failed (name=%s id=%d)", name, new_id);
            return new_id; /* still usable locally */
        }

        msg_func_announce_t *ann = (msg_func_announce_t*)bbuf;
        ann->opcode   = OP_FUNC_ANNOUNCE;
        ann->name_len = (uint32_t)name_len;
        ann->func_id  = (uint32_t)new_id;
        memcpy(bbuf + sizeof(*ann), name, name_len);

        int rc = ucx_broadcast_bytes(bbuf, blen, OP_FUNC_ANNOUNCE);
        if (rc != 0) {
            log_warn("FUNC_ANNOUNCE broadcast had failures (name=%s id=%d)", name, new_id);
        } else {
            log_debug("Broadcasted FUNC_ANNOUNCE name=%s id=%d", name, new_id);
        }
        free(bbuf);
    }

    return new_id;

    // /* First check if already registered */
    // for (int i = 0; i < MAX_FUNCS; i++) {
    //     if (g_func_table[i].in_use) {
    //         if (strcmp(g_func_table[i].name, name) == 0) {
    //             int id = g_func_table[i].func_id;
    //             pthread_mutex_unlock(&g_func_mtx);
    //             return id;
    //         }
    //         if (g_func_table[i].fn == fn) {
    //             int id = g_func_table[i].func_id;
    //             pthread_mutex_unlock(&g_func_mtx);
    //             return id;
    //         }
    //     }
    // }

    // /* Register new function */
    // if (g_next_id >= MAX_FUNCS) {
    //     pthread_mutex_unlock(&g_func_mtx);
    //     log_error("Function table full, cannot register %s", name);
    //     return -1;
    // }

    // int id = g_next_id++;
    // g_func_table[id].in_use = 1;
    // g_func_table[id].func_id = id;
    // strncpy(g_func_table[id].name, name, MAX_FUNC_NAME - 1);
    // g_func_table[id].name[MAX_FUNC_NAME - 1] = '\0';
    // g_func_table[id].fn = fn;

    // pthread_mutex_unlock(&g_func_mtx);

    // log_info("Registered function name=%s id=%d", name, id);
    // return id;
}

int functable_bind_local(const char *name, int func_id, void *(*fn)(void*))
{
    if (!name || func_id < 0 || func_id >= MAX_FUNCS) return -1;
    if (strlen(name) >= MAX_FUNC_NAME) return -1;

    pthread_mutex_lock(&g_func_mtx);

    /* If slot empty, fill entirely. */
    if (!g_func_table[func_id].in_use) {
        g_func_table[func_id].in_use   = 1;
        g_func_table[func_id].func_id  = func_id;
        strncpy(g_func_table[func_id].name, name, MAX_FUNC_NAME - 1);
        g_func_table[func_id].name[MAX_FUNC_NAME - 1] = '\0';
        g_func_table[func_id].fn = fn;
        pthread_mutex_unlock(&g_func_mtx);
        log_info("functable_bind_local: name=%s id=%d (new)", name, func_id);
        return 0;
    }

    /* If slot used, name must match; then update pointer if provided. */
    if (strcmp(g_func_table[func_id].name, name) != 0) {
        pthread_mutex_unlock(&g_func_mtx);
        log_error("functable_bind_local: name mismatch at id=%d: %s != %s",
                  func_id, g_func_table[func_id].name, name);
        return -1;
    }

    if (fn && g_func_table[func_id].fn == NULL) {
        g_func_table[func_id].fn = fn;
        log_info("functable_bind_local: name=%s id=%d pointer updated", name, func_id);
    }

    pthread_mutex_unlock(&g_func_mtx);
    return 0;
}

/* Get function pointer by ID.
 * Returns NULL if ID is invalid or not registered. */
void *(*functable_get_by_id(int id))(void*)
{
    if (id < 0 || id >= MAX_FUNCS) return NULL;
    if (!g_func_table[id].in_use) return NULL;
    return g_func_table[id].fn;
}

/* Get ID by function name.
 * Returns -1 if not found. */
int functable_get_id_by_name(const char *name)
{
    if (!name) return -1;

    pthread_mutex_lock(&g_func_mtx);
    for (int i = 0; i < MAX_FUNCS; i++) {
        if (g_func_table[i].in_use && strcmp(g_func_table[i].name, name) == 0) {
            int id = g_func_table[i].func_id;
            pthread_mutex_unlock(&g_func_mtx);
            return id;
        }
    }
    pthread_mutex_unlock(&g_func_mtx);
    return -1;
}

/* Get ID by function pointer.
 * Returns -1 if not found. */
int functable_get_id_by_ptr(void *(*fn)(void*))
{
    if (!fn) return -1;

    pthread_mutex_lock(&g_func_mtx);
    for (int i = 0; i < MAX_FUNCS; i++) {
        if (g_func_table[i].in_use && g_func_table[i].fn == fn) {
            int id = g_func_table[i].func_id;
            pthread_mutex_unlock(&g_func_mtx);
            return id;
        }
    }
    pthread_mutex_unlock(&g_func_mtx);
    return -1;
}