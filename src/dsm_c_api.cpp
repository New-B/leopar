// src/dsm_c_api.cpp
#include "dsm_c_api.h"
#include <string>       // for std::string
extern "C" {
#include "log.h"
}

// 这里包含 GAM 的 C++ 头，而不是在 C 源里包含
// #include <dsm.h>              // 按你真实路径调整：/users/NewB/gam/include/dsm.h
#include "pgasapi.h"
#include <exception>

// ---- Keep this global so its lifetime covers the whole process ----
static std::string g_gam_logfile = "/sharenvme/usershome/wangbo/projects/gam/log/dsm.log";

/* C linkage (unmangled) provided by libdsm.a */
//extern "C" void dsm_init(const Conf* c);
// extern "C" void dsm_finalize(void);

// /* C++ linkage (mangled) provided by libdsm.a */
unsigned long dsmMalloc(unsigned long size, int node);
int           dsmRead(unsigned long addr, void* buf, unsigned long count);
int           dsmWrite(unsigned long addr, void* buf, unsigned long count);
void          dsmFree(unsigned long addr);

// /* Layout-compatible Conf (fields & order must match) */
// struct Conf {
//     bool           is_master;
//     int            master_port;
//     std::string    master_ip;
//     std::string    master_bindaddr;
//     int            worker_port;
//     std::string    worker_bindaddr;
//     std::string    worker_ip;
//     unsigned long  size;
//     unsigned long  ghost_th;
//     double         cache_th;
//     int            unsynced_th;
//     double         factor;
//     int            maxclients;
//     int            maxthreads;
//     int            backlog;
//     int            loglevel;
//     std::string*   logfile;          // same as GAM Conf
//     int            timeout;          // ms
//     int            eviction_period;  // ms
// };

extern "C" {

int dsm_init_c(const dsm_conf_c* c) {
    try {
        Conf conf{};

        if (c) {
            conf.is_master       = (c->is_master != 0);
            conf.master_port     = c->master_port;
            if (c->master_ip)       conf.master_ip       = c->master_ip;
            // if (c->master_bindaddr) conf.master_bindaddr = c->master_bindaddr;

            conf.worker_port     = c->worker_port;
            if (c->worker_ip)       conf.worker_ip       = c->worker_ip;
            // if (c->worker_bindaddr) conf.worker_bindaddr = c->worker_bindaddr;

            conf.size            = (unsigned long)c->size;
            conf.ghost_th        = (unsigned long)c->ghost_th;
            conf.cache_th        = c->cache_th;
            conf.unsynced_th     = c->unsynced_th;
            conf.factor          = c->factor;
            conf.maxclients      = c->maxclients;
            conf.maxthreads      = c->maxthreads;
            conf.backlog         = c->backlog;
            conf.loglevel        = c->loglevel;          // 直接用整型值即可
            conf.timeout         = c->timeout_ms;
            // conf.eviction_period = c->eviction_period_ms;

        }
        // >>> Hardcode GAM log file to an absolute path
        conf.logfile = &g_gam_logfile;
        // epicLog(LOG_INFO, "DSM init: is_master=%d master=%s:%d worker_ip=%s worker_port=%d ",
        //     (int)conf.is_master, conf.master_ip.c_str(), conf.master_port,
        //     conf.worker_ip.c_str(), conf.worker_port);

        // dsm_init(&conf);
        InitSystem(&conf); // redundant but safe
        return 0;
    } catch (...) {
        return -1;
    }
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
