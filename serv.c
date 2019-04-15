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

    if (sockinit() == -1) {//socket��ʼ������ʵ���ǵ���epoll_create�õ�һ��epfd��
        twarnx("sockinit");
        exit(1);
    }

    s->sock.x = s;//s->sockΪserver������socket�������䴦����Ϊsrvaccept��
    s->sock.f = (Handle)srvaccept;// ��������������ʱ������Socket�ṹ�е�f�ص�����
    s->conns.less = (Less)connless;//����s->conns�ѵĺ���ָ��
    s->conns.rec = (Record)connrec;

    r = listen(s->sock.fd, 1024);//����
    if (r == -1) {
        twarn("listen");
        return;
    }

    r = sockwant(&s->sock, 'r');//ע�ᵽepoll  Ϊs->sock.fd��Ӷ��¼���epoll_ctl�У�������Ӧ�ͻ��˵���������
    if (r == -1) {
        twarn("sockwant");
        exit(2);
    }


    for (;;) {//����ѭ��
        period = prottick(s);//��������һЩ�¼���Ҫ���ض�ʱ��ִ�У���������ִ���¼���ʱ��������Ϊepoll_wait�ĵȴ�ʱ�䣻

        int rw = socknext(&sock, period);//epoll wait
        if (rw == -1) {
            twarnx("socknext");
            exit(1);
        }

        if (rw) {
            sock->f(sock->x, rw);//����socket�Ĵ�����
        }
    }
}


void
srvaccept(Server *s, int ev)
{
    h_accept(s->sock.fd, ev, s);
}
