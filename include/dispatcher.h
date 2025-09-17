/**
 * @file dispatcher.h
 * @author Wang Bo
 * @date 2025-09-16
 * @brief Message dispatcher & progress loop for LeoPar runtime.
 */

#ifndef DISPATCHER_H
#define DISPATCHER_H

#include <stddef.h>
#include <ucp/api/ucp.h>

/* 启动后台进度线程（循环轮询并分发请求类消息） */
int dispatcher_start(void);

/* 停止后台进度线程（优雅退出） */
int dispatcher_stop(void);

/* 手动推进一次（无后台线程时可用） */
void dispatcher_progress_once(void);

/* 分发一条消息（若你已有缓冲区与tag，可直接调用） */
void dispatch_msg(void *buf, size_t len, ucp_tag_t tag);

#endif /* DISPATCHER_H */
