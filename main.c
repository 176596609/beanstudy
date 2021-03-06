#include <stdint.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>
#include <sys/stat.h>
#include <unistd.h>
#include <pwd.h>
#include <fcntl.h>
#include "dat.h"

static void
su(const char *user) {
    int r;
    struct passwd *pwent;

    errno = 0;
    pwent = getpwnam(user);
    if (errno) twarn("getpwnam(\"%s\")", user), exit(32);
    if (!pwent) twarnx("getpwnam(\"%s\"): no such user", user), exit(33);

    r = setgid(pwent->pw_gid);
    if (r == -1) twarn("setgid(%d \"%s\")", pwent->pw_gid, user), exit(34);

    r = setuid(pwent->pw_uid);
    if (r == -1) twarn("setuid(%d \"%s\")", pwent->pw_uid, user), exit(34);
}


static void
set_sig_handlers()
{
    int r;
    struct sigaction sa;

    sa.sa_handler = SIG_IGN;
    sa.sa_flags = 0;
    r = sigemptyset(&sa.sa_mask);
    if (r == -1) twarn("sigemptyset()"), exit(111);

    r = sigaction(SIGPIPE, &sa, 0);
    if (r == -1) twarn("sigaction(SIGPIPE)"), exit(111);

    sa.sa_handler = enter_drain_mode;
    r = sigaction(SIGUSR1, &sa, 0);
    if (r == -1) twarn("sigaction(SIGUSR1)"), exit(111);
}

int
main(int argc, char **argv)
{
    int r;
    struct job list = {};

    progname = argv[0];
    setlinebuf(stdout);
    optparse(&srv, argv+1);//解析输入参数 gittest  gittest2

    if (verbose) {
        printf("pid %d\n", getpid());
    }

    r = make_server_socket(srv.addr, srv.port);//创建socket  fd
    if (r == -1) twarnx("make_server_socket()"), exit(111);
    srv.sock.fd = r;

    prot_init();//初始化全局tubes集合，创建名称为default的默认tube

    if (srv.user) su(srv.user); //转换成srv.user用户来运行
    set_sig_handlers();// 安装信号处理函数

    if (srv.wal.use) {
        // We want to make sure that only one beanstalkd tries
        // to use the wal directory at a time. So acquire a lock
        // now and never release it.
        if (!waldirlock(&srv.wal)) {//抢占目录锁 抢占不到 进程退出
            twarnx("failed to lock wal dir %s", srv.wal.dir);
            exit(10);
        }

        list.prev = list.next = &list;
        walinit(&srv.wal, &list);//反初始化 序列化的job 恢复机制
        r = prot_replay(&srv, &list);
        if (!r) {
            twarnx("failed to replay log");
            return 1;
        }
    }

    srvserve(&srv);//启动服务器事件监听
    return 0;
}
