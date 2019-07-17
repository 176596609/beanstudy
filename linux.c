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

static int epfd;//epoolȫ��fd


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
    epfd = epoll_create(1);//����epoll  epfd��ȫ�ֱ���
    if (epfd == -1) {
        twarn("epoll_create");
        return -1;
    }
    return 0;
}


int
sockwant(Socket *s, int rw)//epool������ʱ��
{
    int op;
    struct epoll_event ev = {};

    if (!s->added && !rw) {//û�б���ӹ�  Ȼ��rw=0 ��ʵ�ǷǷ��� ����0
        return 0;
    } else if (!s->added && rw) {//���û�б���ӹ� ��ô���Ϊ�Ѿ���� ��socket��ӵ�epool����
        s->added = 1;
        op = EPOLL_CTL_ADD;
    } else if (!rw) {
        op = EPOLL_CTL_DEL;//����Ѿ������  ����rw=0 ��ô��epoll����ɾ��
    } else {
        op = EPOLL_CTL_MOD;//����Ѿ������ ����rw!=0 ��ô˵�����޸�
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
    ev.data.ptr = s;//ע�⣺�������sokcetָ�룻��socket��x�ֶλ�ָ��server����conn�ṹ�壬��socket��Ӧ��fd�����¼�ʱ�����Եõ�server��conn����

    return epoll_ctl(epfd, op, s->fd, &ev);
}


int
socknext(Socket **s, int64 timeout)
{
    int r;
    struct epoll_event ev;

    r = epoll_wait(epfd, &ev, 1, (int)(timeout/1000000));//��epoll������������ȡ��һ�����д���
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
