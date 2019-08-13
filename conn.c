#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <limits.h>
#include <unistd.h>
#include "dat.h"

#define SAFETY_MARGIN (1000000000) /* 1 second */

static int cur_conn_ct = 0, cur_worker_ct = 0, cur_producer_ct = 0;
static uint tot_conn_ct = 0;
int verbose = 0;

static void
on_watch(ms a, tube t, size_t i)
{
    tube_iref(t);
    t->watching_ct++;
}

static void
on_ignore(ms a, tube t, size_t i)
{
    t->watching_ct--;
    tube_dref(t);
}

Conn *
make_conn(int fd, char start_state, tube use, tube watch)//start_state 初始状态 状态机    use 给生产者用的 往哪个tube里面生产  watch给消费者用的 消费者监控哪个tube 注意一个消费者可以监控多个tube
{
    job j;
    Conn *c;

    c = new(Conn);
    if (!c) return twarn("OOM"), NULL;

    ms_init(&c->watch, (ms_event_fn) on_watch, (ms_event_fn) on_ignore);
    if (!ms_append(&c->watch, watch)) {
        free(c);
        return twarn("OOM"), NULL;
    }

    TUBE_ASSIGN(c->use, use);
    use->using_ct++;

    c->sock.fd = fd;
    c->state = start_state;
    c->pending_timeout = -1;
    c->tickpos = -1;
    j = &c->reserved_jobs;
    j->prev = j->next = j;

    /* stats */
    cur_conn_ct++;
    tot_conn_ct++;

    return c;
}

void
connsetproducer(Conn *c)//put，说明是生产者，设置conn类型为生产者
{
    if (c->type & CONN_TYPE_PRODUCER) return;
    c->type |= CONN_TYPE_PRODUCER;
    cur_producer_ct++; /* stats */
}

void
connsetworker(Conn *c)
{
    if (c->type & CONN_TYPE_WORKER) return;
    c->type |= CONN_TYPE_WORKER;
    cur_worker_ct++; /* stats */
}

int
count_cur_conns()
{
    return cur_conn_ct;
}

uint
count_tot_conns()
{
    return tot_conn_ct;
}

int
count_cur_producers()
{
    return cur_producer_ct;
}

int
count_cur_workers()
{
    return cur_worker_ct;
}

static int
has_reserved_job(Conn *c)
{
    return job_list_any_p(&c->reserved_jobs);
}


static int64
conntickat(Conn *c)//计算当前客户端待发生的某个事件的时间 事件1 正在reserved的job ttr要过期的时间  事件2 客户端wait时间超时    看哪个事件优先发生
{
    int margin = 0, should_timeout = 0;
    int64 t = INT64_MAX;

    if (conn_waiting(c)) {
        margin = SAFETY_MARGIN;
    }

	if (has_reserved_job(c)) {//如果客户端有reserved状态的任务，则获取到期时间最近的；（当客户端处于阻塞状态时，应该提前SAFETY_MARGIN时间处理此事件）    connsoonestjob：获取到期时间最近的reserved job
        t = connsoonestjob(c)->r.deadline_at - nanoseconds() - margin;
        should_timeout = 1;
    }
    if (c->pending_timeout >= 0) {//客户端阻塞超时时间
        t = min(t, ((int64)c->pending_timeout) * 1000000000);
        should_timeout = 1;
    }

    if (should_timeout) { //返回时间发生的时间；后续会将此客户端插入srv->conns堆，且是按照此时间排序的；
        return nanoseconds() + t;
    }
    return 0;
}


void
connwant(Conn *c, int rw)//pending_timeout 以及保留的job过期时间会被参考，决定在conn堆里面的的排序
{
    c->rw = rw;//c->rw记录当前客户端关心的socket事件
    connsched(c);
}


void
connsched(Conn *c)//根据tickat的优先级来决定client 在conns里面的位置
{
    if (c->tickpos > -1) {
        heapremove(&c->srv->conns, c->tickpos);
    }
    c->tickat = conntickat(c);
    if (c->tickat) {
        heapinsert(&c->srv->conns, c);
    }
}


/* return the reserved job with the earliest deadline,
 * or NULL if there's no reserved job */
job
connsoonestjob(Conn *c)//从reserved_jobs链表中找到一个ttr最小的job
{
    job j = NULL;
    job soonest = c->soonest_job;

    if (soonest == NULL) {
        for (j = c->reserved_jobs.next; j != &c->reserved_jobs; j = j->next) {
            if (j->r.deadline_at <= (soonest ? : j)->r.deadline_at) soonest = j;
        }
    }
    c->soonest_job = soonest;
    return soonest;
}


/* return true if c has a reserved job with less than one second until its
 * deadline */
int
conndeadlinesoon(Conn *c)//从参数c的reserved_jobs链表中找到一个ttr最小的job，并判断该Job是否至少还有1秒过期，若是返回true。否则，返回false。
{
    int64 t = nanoseconds();
    job j = connsoonestjob(c);

    return j && t >= j->r.deadline_at - SAFETY_MARGIN;
}

int
conn_ready(Conn *c)//该函数的功能是：检查当前连接c的watch数组(tube数组)的ready job堆是否有准备好的job。
{
    size_t i;

    for (i = 0; i < c->watch.used; i++) {
        if (((tube) c->watch.items[i])->ready.len) return 1;
    }
    return 0;
}


int
connless(Conn *a, Conn *b)
{
    return a->tickat < b->tickat;
}


void
connrec(Conn *c, int i)
{
    c->tickpos = i;
}


void
connclose(Conn *c)
{
    sockwant(&c->sock, 0);//删除epool监听事件 
    close(c->sock.fd);//关闭socket
    if (verbose) {
        printf("close %d\n", c->sock.fd);
    }

    job_free(c->in_job);//释放injob缓存

    /* was this a peek or stats command? */
    if (c->out_job && !c->out_job->r.id) job_free(c->out_job);//释放输出job

    c->in_job = c->out_job = NULL;
    c->in_job_read = 0;

    if (c->type & CONN_TYPE_PRODUCER) cur_producer_ct--; /* stats */
    if (c->type & CONN_TYPE_WORKER) cur_worker_ct--; /* stats */

    cur_conn_ct--; /* stats */

	remove_waiting_conn(c);//从当前客户端conn监听的所有tube的waiting队列中移除自己
    if (has_reserved_job(c)) enqueue_reserved_jobs(c);//当前自己没处理完reserved 的job放回ready 堆  以便别的消费者可以消费 以免丢失

    ms_clear(&c->watch);
    c->use->using_ct--;
    TUBE_ASSIGN(c->use, NULL);

    if (c->tickpos > -1) {
        heapremove(&c->srv->conns, c->tickpos);
    }

    free(c);
}
