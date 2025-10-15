// src/dsm_c_api.cpp
#include "dsm_c_api.h"

// 这里包含 GAM 的 C++ 头，而不是在 C 源里包含
#include <dsm.h>              // 按你真实路径调整：/users/NewB/gam/include/dsm.h
#include <exception>

/* ---- 链接到 GAM 导出符号：混合链接约定 ---- */
/* 这两个是 C 符号（unmangled） */
extern "C" void dsm_init(const Conf* c);
extern "C" void dsm_finalize(void);

/* 这四个是 C++ 符号（mangled），不要 extern "C" */
unsigned long dsmMalloc(unsigned long size, int node);
int           dsmRead(unsigned long addr, void* buf, unsigned long count);
int           dsmWrite(unsigned long addr, void* buf, unsigned long count);
void          dsmFree(unsigned long addr);

extern "C" {

int dsm_init_c(const Conf* c) {
    try { dsm_init(c); return 0; } catch (...) { return -1; }
}

GAddr dsm_malloc_c(Size size, Node nid) {
    try { return (GAddr)dsmMalloc((unsigned long)size, (int)nid); }
    catch (...) { return (GAddr)0; }
}

int dsm_read_c(GAddr addr, void* buf, Size count) {
    try { return dsmRead((unsigned long)addr, buf, (unsigned long)count); }
    catch (...) { return -1; }
}

int dsm_write_c(GAddr addr, const void* buf, Size count) {
    try { return dsmWrite((unsigned long)addr, const_cast<void*>(buf), (unsigned long)count); }
    catch (...) { return -1; }
}

int dsm_free_c(GAddr addr) {
    try { dsmFree((unsigned long)addr); return 0; }
    catch (...) { return -1; }
}

void dsm_finalize_c(void) {
    try { dsm_finalize(); } catch (...) { /* swallow */ }
}

} // extern "C"
