#define _XOPEN_SOURCE 600
#include <stdint.h>
#include <fcntl.h>
#include <stdlib.h>
#include <errno.h>
#include <sys/epoll.h>
#include "dat.h"

#ifndef EPOLLRDHUP
#define EPOLLRDHUP 0x2000
#endif

static int epfd;//epool全局fd


/* Allocate disk space.
 * Expects fd's offset to be 0; may also reset fd's offset to 0.
 * Returns 0 on success, and a positive errno otherwise. */
int
rawfalloc(int fd, int len)
{
    return posix_fallocate(fd, 0, len);
}


int
sockinit(void)
{
    epfd = epoll_create(1);//创建epoll  epfd是全局变量
    if (epfd == -1) {
        twarn("epoll_create");
        return -1;
    }
    return 0;
}


int
sockwant(Socket *s, int rw)//epool监听的时间
{
    int op;
    struct epoll_event ev = {};

    if (!s->added && !rw) {//没有被添加过  然后rw=0 其实是非法的 返回0
        return 0;
    } else if (!s->added && rw) {//如果没有被添加过 那么标记为已经添加 将socket添加到epool队列
        s->added = 1;
        op = EPOLL_CTL_ADD;
    } else if (!rw) {
        op = EPOLL_CTL_DEL;//如果已经添加了  现在rw=0 那么从epoll里面删除
    } else {
        op = EPOLL_CTL_MOD;//如果已经添加了 现在rw!=0 那么说明是修改
    }

    switch (rw) {
    case 'r':
        ev.events = EPOLLIN;
        break;
    case 'w':
        ev.events = EPOLLOUT;
        break;
    }
    ev.events |= EPOLLRDHUP | EPOLLPRI;
    ev.data.ptr = s;//注意：传入的是sokcet指针；（socket的x字段会指向server或者conn结构体，当socket对应的fd发生事件时，可以得到server或conn对象）

    return epoll_ctl(epfd, op, s->fd, &ev);
}


int
socknext(Socket **s, int64 timeout)
{
    int r;
    struct epoll_event ev;

    r = epoll_wait(epfd, &ev, 1, (int)(timeout/1000000));//从epoll就绪队列里面取出一个进行处理
    if (r == -1 && errno != EINTR) {
        twarn("epoll_wait");
        exit(1);
    }

    if (r > 0) {
        *s = ev.data.ptr;
        if (ev.events & (EPOLLHUP|EPOLLRDHUP)) {
            return 'h';
        } else if (ev.events & EPOLLIN) {
            return 'r';
        } else if (ev.events & EPOLLOUT) {
            return 'w';
        }
    }
    return 0;
}
