#include <stdint.h>
#include <stdlib.h>
#include <sys/socket.h>
#include "dat.h"

struct Server srv = {
    Portdef,
    NULL,
    NULL,
    {
        Filesizedef,
    },
};


void
srvserve(Server *s)
{
    int r;
    Socket *sock;
    int64 period;

    if (sockinit() == -1) {//socket初始化，其实就是调用epoll_create得到一个epfd。
        twarnx("sockinit");
        exit(1);
    }

    s->sock.x = s;//s->sock为server监听的socket；设置其处理函数为srvaccept；
    s->sock.f = (Handle)srvaccept;// 当有连接请求到来时，调用Socket结构中的f回调函数
    s->conns.less = (Less)connless;//设置s->conns堆的函数指针
    s->conns.rec = (Record)connrec;

    r = listen(s->sock.fd, 1024);//监听
    if (r == -1) {
        twarn("listen");
        return;
    }

    r = sockwant(&s->sock, 'r');//注册到epoll  为s->sock.fd添加读事件到epoll_ctl中，用来相应客户端的连接请求
    if (r == -1) {
        twarn("sockwant");
        exit(2);
    }


    for (;;) {//开启循环
        period = prottick(s);//服务器有一些事件需要在特定时间执行，获得最早待执行事件的时间间隔，作为epoll_wait的等待时间；

        int rw = socknext(&sock, period);//epoll wait
        if (rw == -1) {
            twarnx("socknext");
            exit(1);
        }

        if (rw) {
            sock->f(sock->x, rw);//调用socket的处理函数
        }
    }
}


void
srvaccept(Server *s, int ev)
{
    h_accept(s->sock.fd, ev, s);
}
