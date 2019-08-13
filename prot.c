#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>
#include <fcntl.h>
#include <string.h>
#include <errno.h>
#include <sys/resource.h>
#include <sys/uio.h>
#include <sys/types.h>
#include <sys/utsname.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <inttypes.h>
#include <stdarg.h>
#include "dat.h"

/* job body cannot be greater than this many bytes long */
size_t job_data_size_limit = JOB_DATA_SIZE_LIMIT_DEFAULT;

#define NAME_CHARS \
    "ABCDEFGHIJKLMNOPQRSTUVWXYZ" \
    "abcdefghijklmnopqrstuvwxyz" \
    "0123456789-+/;.$_()"

#define CMD_PUT "put "
#define CMD_PEEKJOB "peek "
#define CMD_PEEK_READY "peek-ready"
#define CMD_PEEK_DELAYED "peek-delayed"
#define CMD_PEEK_BURIED "peek-buried"
#define CMD_RESERVE "reserve"
#define CMD_RESERVE_TIMEOUT "reserve-with-timeout "
#define CMD_DELETE "delete "
#define CMD_RELEASE "release "
#define CMD_BURY "bury "
#define CMD_KICK "kick "
#define CMD_JOBKICK "kick-job "
#define CMD_TOUCH "touch "
#define CMD_STATS "stats"
#define CMD_JOBSTATS "stats-job "
#define CMD_USE "use "
#define CMD_WATCH "watch "
#define CMD_IGNORE "ignore "
#define CMD_LIST_TUBES "list-tubes"
#define CMD_LIST_TUBE_USED "list-tube-used"
#define CMD_LIST_TUBES_WATCHED "list-tubes-watched"
#define CMD_STATS_TUBE "stats-tube "
#define CMD_QUIT "quit"
#define CMD_PAUSE_TUBE "pause-tube"

#define CONSTSTRLEN(m) (sizeof(m) - 1)

#define CMD_PEEK_READY_LEN CONSTSTRLEN(CMD_PEEK_READY)
#define CMD_PEEK_DELAYED_LEN CONSTSTRLEN(CMD_PEEK_DELAYED)
#define CMD_PEEK_BURIED_LEN CONSTSTRLEN(CMD_PEEK_BURIED)
#define CMD_PEEKJOB_LEN CONSTSTRLEN(CMD_PEEKJOB)
#define CMD_RESERVE_LEN CONSTSTRLEN(CMD_RESERVE)
#define CMD_RESERVE_TIMEOUT_LEN CONSTSTRLEN(CMD_RESERVE_TIMEOUT)
#define CMD_DELETE_LEN CONSTSTRLEN(CMD_DELETE)
#define CMD_RELEASE_LEN CONSTSTRLEN(CMD_RELEASE)
#define CMD_BURY_LEN CONSTSTRLEN(CMD_BURY)
#define CMD_KICK_LEN CONSTSTRLEN(CMD_KICK)
#define CMD_JOBKICK_LEN CONSTSTRLEN(CMD_JOBKICK)
#define CMD_TOUCH_LEN CONSTSTRLEN(CMD_TOUCH)
#define CMD_STATS_LEN CONSTSTRLEN(CMD_STATS)
#define CMD_JOBSTATS_LEN CONSTSTRLEN(CMD_JOBSTATS)
#define CMD_USE_LEN CONSTSTRLEN(CMD_USE)
#define CMD_WATCH_LEN CONSTSTRLEN(CMD_WATCH)
#define CMD_IGNORE_LEN CONSTSTRLEN(CMD_IGNORE)
#define CMD_LIST_TUBES_LEN CONSTSTRLEN(CMD_LIST_TUBES)
#define CMD_LIST_TUBE_USED_LEN CONSTSTRLEN(CMD_LIST_TUBE_USED)
#define CMD_LIST_TUBES_WATCHED_LEN CONSTSTRLEN(CMD_LIST_TUBES_WATCHED)
#define CMD_STATS_TUBE_LEN CONSTSTRLEN(CMD_STATS_TUBE)
#define CMD_PAUSE_TUBE_LEN CONSTSTRLEN(CMD_PAUSE_TUBE)

#define MSG_FOUND "FOUND"
#define MSG_NOTFOUND "NOT_FOUND\r\n"
#define MSG_RESERVED "RESERVED"
#define MSG_DEADLINE_SOON "DEADLINE_SOON\r\n"
#define MSG_TIMED_OUT "TIMED_OUT\r\n"
#define MSG_DELETED "DELETED\r\n"
#define MSG_RELEASED "RELEASED\r\n"
#define MSG_BURIED "BURIED\r\n"
#define MSG_KICKED "KICKED\r\n"
#define MSG_TOUCHED "TOUCHED\r\n"
#define MSG_BURIED_FMT "BURIED %"PRIu64"\r\n"
#define MSG_INSERTED_FMT "INSERTED %"PRIu64"\r\n"
#define MSG_NOT_IGNORED "NOT_IGNORED\r\n"

#define MSG_NOTFOUND_LEN CONSTSTRLEN(MSG_NOTFOUND)
#define MSG_DELETED_LEN CONSTSTRLEN(MSG_DELETED)
#define MSG_TOUCHED_LEN CONSTSTRLEN(MSG_TOUCHED)
#define MSG_RELEASED_LEN CONSTSTRLEN(MSG_RELEASED)
#define MSG_BURIED_LEN CONSTSTRLEN(MSG_BURIED)
#define MSG_KICKED_LEN CONSTSTRLEN(MSG_KICKED)
#define MSG_NOT_IGNORED_LEN CONSTSTRLEN(MSG_NOT_IGNORED)

#define MSG_OUT_OF_MEMORY "OUT_OF_MEMORY\r\n"
#define MSG_INTERNAL_ERROR "INTERNAL_ERROR\r\n"
#define MSG_DRAINING "DRAINING\r\n"
#define MSG_BAD_FORMAT "BAD_FORMAT\r\n"
#define MSG_UNKNOWN_COMMAND "UNKNOWN_COMMAND\r\n"
#define MSG_EXPECTED_CRLF "EXPECTED_CRLF\r\n"
#define MSG_JOB_TOO_BIG "JOB_TOO_BIG\r\n"

#define STATE_WANTCOMMAND 0
#define STATE_WANTDATA 1
#define STATE_SENDJOB 2
#define STATE_SENDWORD 3
#define STATE_WAIT 4
#define STATE_BITBUCKET 5
#define STATE_CLOSE 6

#define OP_UNKNOWN 0
#define OP_PUT 1
#define OP_PEEKJOB 2
#define OP_RESERVE 3
#define OP_DELETE 4
#define OP_RELEASE 5
#define OP_BURY 6
#define OP_KICK 7
#define OP_STATS 8
#define OP_JOBSTATS 9
#define OP_PEEK_BURIED 10
#define OP_USE 11
#define OP_WATCH 12
#define OP_IGNORE 13
#define OP_LIST_TUBES 14
#define OP_LIST_TUBE_USED 15
#define OP_LIST_TUBES_WATCHED 16
#define OP_STATS_TUBE 17
#define OP_PEEK_READY 18
#define OP_PEEK_DELAYED 19
#define OP_RESERVE_TIMEOUT 20
#define OP_TOUCH 21
#define OP_QUIT 22
#define OP_PAUSE_TUBE 23
#define OP_JOBKICK 24
#define TOTAL_OPS 25

#define STATS_FMT "---\n" \
    "current-jobs-urgent: %u\n" \
    "current-jobs-ready: %u\n" \
    "current-jobs-reserved: %u\n" \
    "current-jobs-delayed: %u\n" \
    "current-jobs-buried: %u\n" \
    "cmd-put: %" PRIu64 "\n" \
    "cmd-peek: %" PRIu64 "\n" \
    "cmd-peek-ready: %" PRIu64 "\n" \
    "cmd-peek-delayed: %" PRIu64 "\n" \
    "cmd-peek-buried: %" PRIu64 "\n" \
    "cmd-reserve: %" PRIu64 "\n" \
    "cmd-reserve-with-timeout: %" PRIu64 "\n" \
    "cmd-delete: %" PRIu64 "\n" \
    "cmd-release: %" PRIu64 "\n" \
    "cmd-use: %" PRIu64 "\n" \
    "cmd-watch: %" PRIu64 "\n" \
    "cmd-ignore: %" PRIu64 "\n" \
    "cmd-bury: %" PRIu64 "\n" \
    "cmd-kick: %" PRIu64 "\n" \
    "cmd-touch: %" PRIu64 "\n" \
    "cmd-stats: %" PRIu64 "\n" \
    "cmd-stats-job: %" PRIu64 "\n" \
    "cmd-stats-tube: %" PRIu64 "\n" \
    "cmd-list-tubes: %" PRIu64 "\n" \
    "cmd-list-tube-used: %" PRIu64 "\n" \
    "cmd-list-tubes-watched: %" PRIu64 "\n" \
    "cmd-pause-tube: %" PRIu64 "\n" \
    "job-timeouts: %" PRIu64 "\n" \
    "total-jobs: %" PRIu64 "\n" \
    "max-job-size: %zu\n" \
    "current-tubes: %zu\n" \
    "current-connections: %u\n" \
    "current-producers: %u\n" \
    "current-workers: %u\n" \
    "current-waiting: %u\n" \
    "total-connections: %u\n" \
    "pid: %ld\n" \
    "version: %s\n" \
    "rusage-utime: %d.%06d\n" \
    "rusage-stime: %d.%06d\n" \
    "uptime: %u\n" \
    "binlog-oldest-index: %d\n" \
    "binlog-current-index: %d\n" \
    "binlog-records-migrated: %" PRId64 "\n" \
    "binlog-records-written: %" PRId64 "\n" \
    "binlog-max-size: %d\n" \
    "id: %s\n" \
    "hostname: %s\n" \
    "\r\n"

#define STATS_TUBE_FMT "---\n" \
    "name: %s\n" \
    "current-jobs-urgent: %u\n" \
    "current-jobs-ready: %u\n" \
    "current-jobs-reserved: %u\n" \
    "current-jobs-delayed: %u\n" \
    "current-jobs-buried: %u\n" \
    "total-jobs: %" PRIu64 "\n" \
    "current-using: %u\n" \
    "current-watching: %u\n" \
    "current-waiting: %u\n" \
    "cmd-delete: %" PRIu64 "\n" \
    "cmd-pause-tube: %u\n" \
    "pause: %" PRIu64 "\n" \
    "pause-time-left: %" PRId64 "\n" \
    "\r\n"

#define STATS_JOB_FMT "---\n" \
    "id: %" PRIu64 "\n" \
    "tube: %s\n" \
    "state: %s\n" \
    "pri: %u\n" \
    "age: %" PRId64 "\n" \
    "delay: %" PRId64 "\n" \
    "ttr: %" PRId64 "\n" \
    "time-left: %" PRId64 "\n" \
    "file: %d\n" \
    "reserves: %u\n" \
    "timeouts: %u\n" \
    "releases: %u\n" \
    "buries: %u\n" \
    "kicks: %u\n" \
    "\r\n"

/* this number is pretty arbitrary */
#define BUCKET_BUF_SIZE 1024

static char bucket[BUCKET_BUF_SIZE];

static uint ready_ct = 0;
static struct stats global_stat = {0, 0, 0, 0, 0};

static tube default_tube;

static int drain_mode = 0;
static int64 started_at;

enum {
  NumIdBytes = 8
};

static char id[NumIdBytes * 2 + 1]; // hex-encoded len of NumIdBytes

static struct utsname node_info;
static uint64 op_ct[TOTAL_OPS], timeout_ct = 0;//记录某条命令执行了多少次

static Conn *dirty;//看起来是一个全局的链表

static const char * op_names[] = {
    "<unknown>",
    CMD_PUT,
    CMD_PEEKJOB,
    CMD_RESERVE,
    CMD_DELETE,
    CMD_RELEASE,
    CMD_BURY,
    CMD_KICK,
    CMD_STATS,
    CMD_JOBSTATS,
    CMD_PEEK_BURIED,
    CMD_USE,
    CMD_WATCH,
    CMD_IGNORE,
    CMD_LIST_TUBES,
    CMD_LIST_TUBE_USED,
    CMD_LIST_TUBES_WATCHED,
    CMD_STATS_TUBE,
    CMD_PEEK_READY,
    CMD_PEEK_DELAYED,
    CMD_RESERVE_TIMEOUT,
    CMD_TOUCH,
    CMD_QUIT,
    CMD_PAUSE_TUBE,
    CMD_JOBKICK,
};

static job remove_buried_job(job j);

static int
buried_job_p(tube t)//返回当前tube buried的任务
{
    return job_list_any_p(&t->buried);
}

static void
reply(Conn *c, char *line, int len, int state)//调用reply函数；只是将数据写入到输出缓冲区，并修改了客户端状态为     注册了写事件
{
    if (!c) return;

    connwant(c, 'w');//修改关心的事件为可写事件,表明，我要向客户端写数据
    c->next = dirty;//放入dirty链表
    dirty = c;
    c->reply = line;//输出数据缓冲区
    c->reply_len = len;
    c->reply_sent = 0;
    c->state = state;//设置conn状态机的状态为state
    if (verbose >= 2) {
        printf(">%d reply %.*s\n", c->sock.fd, len-2, line);
    }
}


static void
protrmdirty(Conn *c)//客户端关闭 摘除当前节点
{
    Conn *x, *newdirty = NULL;

    while (dirty) {
        x = dirty;
        dirty = dirty->next;
        x->next = NULL;

        if (x != c) {
            x->next = newdirty;
            newdirty = x;
        }
    }
    dirty = newdirty;
}


#define reply_msg(c,m) reply((c),(m),CONSTSTRLEN(m),STATE_SENDWORD)

#define reply_serr(c,e) (twarnx("server error: %s",(e)),\
                         reply_msg((c),(e)))

static void
reply_line(Conn*, int, const char*, ...)
__attribute__((format(printf, 3, 4)));

static void
reply_line(Conn *c, int state, const char *fmt, ...)
{
    int r;
    va_list ap;

    va_start(ap, fmt);
    r = vsnprintf(c->reply_buf, LINE_BUF_SIZE, fmt, ap);
    va_end(ap);

    /* Make sure the buffer was big enough. If not, we have a bug. */
    if (r >= LINE_BUF_SIZE) return reply_serr(c, MSG_INTERNAL_ERROR);

    return reply(c, c->reply_buf, r, state);//调用reply函数；只是将数据写入到输出缓冲区
}

static void
reply_job(Conn *c, job j, const char *word)//发送job（带着job前面的一行协议）
{
    /* tell this connection which job to send */
    c->out_job = j;
    c->out_job_sent = 0;

    return reply_line(c, STATE_SENDJOB, "%s %"PRIu64" %u\r\n",
                      word, j->r.id, j->r.body_size - 2);//返回数据；并设置conn状态为STATE_SENDJOB
}

Conn *
remove_waiting_conn(Conn *c)//remove_waiting_conn：从当前客户端conn监听的所有tube的waiting队列中移除自己
{
    tube t;
    size_t i;

    if (!conn_waiting(c)) return NULL;

    c->type &= ~CONN_TYPE_WAITING;//去除CONN_TYPE_WAITING标志  去除正在等待job的状态属性
    global_stat.waiting_ct--;
    for (i = 0; i < c->watch.used; i++) { //遍历客户端监听的所有tube，挨个从tube的waiting队列中删除自己
        t = c->watch.items[i];
        t->stat.waiting_ct--;
        ms_remove(&t->waiting, c);
    }
    return c;
}

static void
reserve_job(Conn *c, job j) //reserve_job：返回此job给客户端
{
    j->r.deadline_at = nanoseconds() + j->r.ttr;//job的失效时间
    global_stat.reserved_ct++; /* stats */
    j->tube->stat.reserved_ct++;
    j->r.reserve_ct++;
    j->r.state = Reserved;//状态改为Reserved
    job_insert(&c->reserved_jobs, j);
    j->reserver = c;//记录job当前消费者是谁
    c->pending_timeout = -1;
    if (c->soonest_job && j->r.deadline_at < c->soonest_job->r.deadline_at) {//soonest_job记录最近要到期的Reserved状态的job，更新；
        c->soonest_job = j;
    }
    return reply_job(c, j, MSG_RESERVED);//返回job
}

static job
next_eligible_job(int64 now)//遍历所有tube，当tube有客户端等待，且有ready状态的job时，返回job 返回的是当前优先级最高的job
{
    tube t;
    size_t i;
    job j = NULL, candidate;

    for (i = 0; i < tubes.used; i++) {//循环所有tube
        t = tubes.items[i];
        if (t->pause) {//假如tube正在暂停，且超时时间未到，则跳过
            if (t->deadline_at > now) continue;
            t->pause = 0;
        }
        if (t->waiting.used && t->ready.len) {//tube的waiting集合有元素说明有客户端正在阻塞等待此tube产生任务；有ready状态的任务
            candidate = t->ready.data[0];//堆里面的第一个必然是优先级最高的
            if (!j || job_pri_less(candidate, j)) {
                j = candidate;
            }
        }
    }

    return j;
}

static void
process_queue()  //检查有没有消费者正在阻塞等待此tube产生job，若有需要返回job；  该函数遍历所有tube，找出tube中已经过期且pri最小的job，把该job从对应的tube的ready对中删除，然后把该job添加到当前连接的reserved_jobs链表中，并把找到的job返回给连接的客户端。
{
    job j;
    int64 now = nanoseconds();

    while ((j = next_eligible_job(now))) {//遍历所有tube，当tube有客户端等待，且有ready状态的job时，返回job
        heapremove(&j->tube->ready, j->heap_index);//在tube堆中删除这个job
        ready_ct--;
        if (j->r.pri < URGENT_THRESHOLD) {
            global_stat.urgent_ct--;
            j->tube->stat.urgent_ct--;
        }
        reserve_job(remove_waiting_conn(ms_take(&j->tube->waiting)), j);  //ms_take：将客户端从此job所属tube的waiting集合中删除；并返回客户端conn  //remove_waiting_conn：从当前客户端conn监听的所有tube的waiting队列中移除自己
    }
}

static job
delay_q_peek()
{
    int i;
    tube t;
    job j = NULL, nj;

    for (i = 0; i < tubes.used; i++) {
        t = tubes.items[i];
        if (t->delay.len == 0) {
            continue;
        }
        nj = t->delay.data[0];
        if (!j || nj->r.deadline_at < j->r.deadline_at) j = nj;
    }

    return j;
}

static int
enqueue_job(Server *s, job j, int64 delay, char update_store)
{
    int r;

    j->reserver = NULL;
    if (delay) { //入delay队列（堆），设置任务的deadline_at
        j->r.deadline_at = nanoseconds() + delay;
        r = heapinsert(&j->tube->delay, j);
        if (!r) return 0;
        j->r.state = Delayed;//job状态改为Delayed
    } else {
        r = heapinsert(&j->tube->ready, j); //入ready队列（堆）
        if (!r) return 0;
        j->r.state = Ready;
        ready_ct++;
        if (j->r.pri < URGENT_THRESHOLD) {
            global_stat.urgent_ct++;
            j->tube->stat.urgent_ct++;
        }
    }

    if (update_store) {
        if (!walwrite(&s->wal, j)) {
            return 0;
        }
        walmaint(&s->wal);
    }

    process_queue();//检查有没有消费者正在阻塞等待此tube产生job，若有需要返回job；
    return 1;
}

static int
bury_job(Server *s, job j, char update_store)
{
    int z;

    if (update_store) {
        z = walresvupdate(&s->wal, j);
        if (!z) return 0;
        j->walresv += z;
    }

    job_insert(&j->tube->buried, j);
    global_stat.buried_ct++;
    j->tube->stat.buried_ct++;
    j->r.state = Buried;
    j->reserver = NULL;
    j->r.bury_ct++;

    if (update_store) {
        if (!walwrite(&s->wal, j)) {
            return 0;
        }
        walmaint(&s->wal);
    }

    return 1;
}

void
enqueue_reserved_jobs(Conn *c)
{
    int r;
    job j;

    while (job_list_any_p(&c->reserved_jobs)) {
        j = job_remove(c->reserved_jobs.next);
        r = enqueue_job(c->srv, j, 0, 0);
        if (r < 1) bury_job(c->srv, j, 0);
        global_stat.reserved_ct--;
        j->tube->stat.reserved_ct--;
        c->soonest_job = NULL;
    }
}

static job
delay_q_take()
{
    job j = delay_q_peek();
    if (!j) {
        return 0;
    }
    heapremove(&j->tube->delay, j->heap_index);
    return j;
}

static int
kick_buried_job(Server *s, job j)
{
    int r;
    int z;

    z = walresvupdate(&s->wal, j);
    if (!z) return 0;
    j->walresv += z;

    remove_buried_job(j);

    j->r.kick_ct++;
    r = enqueue_job(s, j, 0, 1);
    if (r == 1) return 1;

    /* ready queue is full, so bury it */
    bury_job(s, j, 0);
    return 0;
}

static uint
get_delayed_job_ct()
{
    tube t;
    size_t i;
    uint count = 0;

    for (i = 0; i < tubes.used; i++) {
        t = tubes.items[i];
        count += t->delay.len;
    }
    return count;
}

static int
kick_delayed_job(Server *s, job j)
{
    int r;
    int z;

    z = walresvupdate(&s->wal, j);
    if (!z) return 0;
    j->walresv += z;

    heapremove(&j->tube->delay, j->heap_index);

    j->r.kick_ct++;
    r = enqueue_job(s, j, 0, 1);
    if (r == 1) return 1;

    /* ready queue is full, so delay it again */
    r = enqueue_job(s, j, j->r.delay, 0);
    if (r == 1) return 0;

    /* last resort */
    bury_job(s, j, 0);
    return 0;
}

/* return the number of jobs successfully kicked */
static uint
kick_buried_jobs(Server *s, tube t, uint n)
{
    uint i;
    for (i = 0; (i < n) && buried_job_p(t); ++i) {
        kick_buried_job(s, t->buried.next);
    }
    return i;
}

/* return the number of jobs successfully kicked */
static uint
kick_delayed_jobs(Server *s, tube t, uint n)
{
    uint i;
    for (i = 0; (i < n) && (t->delay.len > 0); ++i) {
        kick_delayed_job(s, (job)t->delay.data[0]);
    }
    return i;
}

static uint
kick_jobs(Server *s, tube t, uint n)
{
    if (buried_job_p(t)) return kick_buried_jobs(s, t, n);
    return kick_delayed_jobs(s, t, n);
}

static job
remove_buried_job(job j)
{
    if (!j || j->r.state != Buried) return NULL;
    j = job_remove(j);
    if (j) {
        global_stat.buried_ct--;
        j->tube->stat.buried_ct--;
    }
    return j;
}

static job
remove_delayed_job(job j)
{
    if (!j || j->r.state != Delayed) return NULL;
    heapremove(&j->tube->delay, j->heap_index);

    return j;
}

static job
remove_ready_job(job j)
{
    if (!j || j->r.state != Ready) return NULL;
    heapremove(&j->tube->ready, j->heap_index);
    ready_ct--;
    if (j->r.pri < URGENT_THRESHOLD) {
        global_stat.urgent_ct--;
        j->tube->stat.urgent_ct--;
    }
    return j;
}

static void
enqueue_waiting_conn(Conn *c)//将客户端放入各个tube的waiting列表中
{
    tube t;
    size_t i;

    global_stat.waiting_ct++;
    c->type |= CONN_TYPE_WAITING;
    for (i = 0; i < c->watch.used; i++) {
        t = c->watch.items[i];
        t->stat.waiting_ct++;
        ms_append(&t->waiting, c);
    }
}

static job
find_reserved_job_in_conn(Conn *c, job j)//检查job是否被c  Reserved
{
    return (j && j->reserver == c && j->r.state == Reserved) ? j : NULL;
}

static job
touch_job(Conn *c, job j)
{
    j = find_reserved_job_in_conn(c, j);//若job的订阅者(reserver)是当前连接的终端，且job的状态是Reserved，返回该Job
    if (j) {
        j->r.deadline_at = nanoseconds() + j->r.ttr;// 重新设置该job的过期时间(j->r.deadline_at字段)值为：j->ttr+now()
        c->soonest_job = NULL;
    }
    return j;
}

static job
peek_job(uint64 id)
{
    return job_find(id);
}

static void
check_err(Conn *c, const char *s)
{
    if (errno == EAGAIN) return;//这个返回需要重新读取
    if (errno == EINTR) return;//这个是说在读取的中间被信号中断了 需要重新读取
    if (errno == EWOULDBLOCK) return;//同EAGAIN  windows上返回EWOULDBLOCK

    twarn("%s", s);
    c->state = STATE_CLOSE;//说明出现错误了 需要关闭套接字  修改状态机标记
    return;
}

/* Scan the given string for the sequence "\r\n" and return the line length.
 * Always returns at least 2 if a match is found. Returns 0 if no match. */
static int
scan_line_end(const char *s, int size)
{
    char *match;

    match = memchr(s, '\r', size - 1);//检查是否读取到了\r
    if (!match) return 0;

    /* this is safe because we only scan size - 1 chars above */
    if (match[1] == '\n') return match - s + 2;//检查\r后面是否跟着\n  是的话返回当前命令行的长度 包括\r\n

    return 0;
}

static int
cmd_len(Conn *c)
{
    return scan_line_end(c->cmd, c->cmd_read);
}

/* parse the command line */
static int
which_cmd(Conn *c)
{
#define TEST_CMD(s,c,o) if (strncmp((s), (c), CONSTSTRLEN(c)) == 0) return (o);
    TEST_CMD(c->cmd, CMD_PUT, OP_PUT);
    TEST_CMD(c->cmd, CMD_PEEKJOB, OP_PEEKJOB);
    TEST_CMD(c->cmd, CMD_PEEK_READY, OP_PEEK_READY);
    TEST_CMD(c->cmd, CMD_PEEK_DELAYED, OP_PEEK_DELAYED);
    TEST_CMD(c->cmd, CMD_PEEK_BURIED, OP_PEEK_BURIED);
    TEST_CMD(c->cmd, CMD_RESERVE_TIMEOUT, OP_RESERVE_TIMEOUT);
    TEST_CMD(c->cmd, CMD_RESERVE, OP_RESERVE);
    TEST_CMD(c->cmd, CMD_DELETE, OP_DELETE);
    TEST_CMD(c->cmd, CMD_RELEASE, OP_RELEASE);
    TEST_CMD(c->cmd, CMD_BURY, OP_BURY);
    TEST_CMD(c->cmd, CMD_KICK, OP_KICK);
    TEST_CMD(c->cmd, CMD_JOBKICK, OP_JOBKICK);
    TEST_CMD(c->cmd, CMD_TOUCH, OP_TOUCH);
    TEST_CMD(c->cmd, CMD_JOBSTATS, OP_JOBSTATS);
    TEST_CMD(c->cmd, CMD_STATS_TUBE, OP_STATS_TUBE);
    TEST_CMD(c->cmd, CMD_STATS, OP_STATS);
    TEST_CMD(c->cmd, CMD_USE, OP_USE);
    TEST_CMD(c->cmd, CMD_WATCH, OP_WATCH);
    TEST_CMD(c->cmd, CMD_IGNORE, OP_IGNORE);
    TEST_CMD(c->cmd, CMD_LIST_TUBES_WATCHED, OP_LIST_TUBES_WATCHED);
    TEST_CMD(c->cmd, CMD_LIST_TUBE_USED, OP_LIST_TUBE_USED);
    TEST_CMD(c->cmd, CMD_LIST_TUBES, OP_LIST_TUBES);
    TEST_CMD(c->cmd, CMD_QUIT, OP_QUIT);
    TEST_CMD(c->cmd, CMD_PAUSE_TUBE, OP_PAUSE_TUBE);
    return OP_UNKNOWN;
}

/* Copy up to body_size trailing bytes into the job, then the rest into the cmd
 * buffer. If c->in_job exists, this assumes that c->in_job->body is empty.
 * This function is idempotent(). */
static void
fill_extra_data(Conn *c)//解析客户端发来的任务数据，存储在c->in_job的body数据字段
{
    int extra_bytes, job_data_bytes = 0, cmd_bytes;

    if (!c->sock.fd) return; /* the connection was closed */
    if (!c->cmd_len) return; /* we don't have a complete command */

    /* how many extra bytes did we read? */
    extra_bytes = c->cmd_read - c->cmd_len;//除了命令段，额外读取的字节 其实就是body

    /* how many bytes should we put into the job body? */
    if (c->in_job) {//如果in_job不为空，说明是在读取job  否则要丢弃数据（数据量超过了配置文件允许的范围）
        job_data_bytes = min(extra_bytes, c->in_job->r.body_size);//job_data_bytes如果等于body_size  那么说明这个body也已经读取结束了
        memcpy(c->in_job->body, c->cmd + c->cmd_len, job_data_bytes);//拷贝正确的body数据到正确的位置
        c->in_job_read = job_data_bytes;
    } else if (c->in_job_read) {//走到这里是要扔掉的数据
        /* we are in bit-bucket mode, throwing away data */
        job_data_bytes = min(extra_bytes, c->in_job_read);
        c->in_job_read -= job_data_bytes;
    }

    /* how many bytes are left to go into the future cmd? */
    cmd_bytes = extra_bytes - job_data_bytes;//如果extra_bytes>job_data_bytes  cmd_bytes就是下个命令的输入了
    memmove(c->cmd, c->cmd + c->cmd_len + job_data_bytes, cmd_bytes);
    c->cmd_read = cmd_bytes;
    c->cmd_len = 0; /* we no longer know the length of the new command */
}

static void
_skip(Conn *c, int n, char *line, int len)
{
    /* Invert the meaning of in_job_read while throwing away data -- it
     * counts the bytes that remain to be thrown away. */
    c->in_job = 0;
    c->in_job_read = n;
    fill_extra_data(c);

    if (c->in_job_read == 0) return reply(c, line, len, STATE_SENDWORD);

    c->reply = line;
    c->reply_len = len;
    c->reply_sent = 0;
    c->state = STATE_BITBUCKET;
    return;
}

#define skip(c,n,m) (_skip(c,n,m,CONSTSTRLEN(m)))

static void
enqueue_incoming_job(Conn *c)
{
    int r;
    job j = c->in_job;

    c->in_job = NULL; /* the connection no longer owns this job */  //重置
    c->in_job_read = 0;

    /* check if the trailer is present and correct */
    if (memcmp(j->body + j->r.body_size - 2, "\r\n", 2)) {
        job_free(j);
        return reply_msg(c, MSG_EXPECTED_CRLF);
    }

    if (verbose >= 2) {
        printf("<%d job %"PRIu64"\n", c->sock.fd, j->r.id);
    }

    if (drain_mode) {
        job_free(j);
        return reply_serr(c, MSG_DRAINING);
    }

    if (j->walresv) return reply_serr(c, MSG_INTERNAL_ERROR);
    j->walresv = walresvput(&c->srv->wal, j);
    if (!j->walresv) return reply_serr(c, MSG_OUT_OF_MEMORY);

    /* we have a complete job, so let's stick it in the pqueue */
	r = enqueue_job(c->srv, j, j->r.delay, 1);//入队列
    if (r < 0) return reply_serr(c, MSG_INTERNAL_ERROR);

    global_stat.total_jobs_ct++;//更新统计数据
    j->tube->stat.total_jobs_ct++;

    if (r == 1) return reply_line(c, STATE_SENDWORD, MSG_INSERTED_FMT, j->r.id);//返回数据；并设置conn状态为STATE_SENDWORD

    /* out of memory trying to grow the queue, so it gets buried */
    bury_job(c->srv, j, 0);
    reply_line(c, STATE_SENDWORD, MSG_BURIED_FMT, j->r.id); 
}

static uint
uptime()
{
    return (nanoseconds() - started_at) / 1000000000;
}

static int
fmt_stats(char *buf, size_t size, void *x)
{
    int whead = 0, wcur = 0;
    Server *srv;
    struct rusage ru = {{0, 0}, {0, 0}};

    srv = x;

    if (srv->wal.head) {
        whead = srv->wal.head->seq;
    }

    if (srv->wal.cur) {
        wcur = srv->wal.cur->seq;
    }

    getrusage(RUSAGE_SELF, &ru); /* don't care if it fails */
    return snprintf(buf, size, STATS_FMT,
            global_stat.urgent_ct,
            ready_ct,
            global_stat.reserved_ct,
            get_delayed_job_ct(),
            global_stat.buried_ct,
            op_ct[OP_PUT],
            op_ct[OP_PEEKJOB],
            op_ct[OP_PEEK_READY],
            op_ct[OP_PEEK_DELAYED],
            op_ct[OP_PEEK_BURIED],
            op_ct[OP_RESERVE],
            op_ct[OP_RESERVE_TIMEOUT],
            op_ct[OP_DELETE],
            op_ct[OP_RELEASE],
            op_ct[OP_USE],
            op_ct[OP_WATCH],
            op_ct[OP_IGNORE],
            op_ct[OP_BURY],
            op_ct[OP_KICK],
            op_ct[OP_TOUCH],
            op_ct[OP_STATS],
            op_ct[OP_JOBSTATS],
            op_ct[OP_STATS_TUBE],
            op_ct[OP_LIST_TUBES],
            op_ct[OP_LIST_TUBE_USED],
            op_ct[OP_LIST_TUBES_WATCHED],
            op_ct[OP_PAUSE_TUBE],
            timeout_ct,
            global_stat.total_jobs_ct,
            job_data_size_limit,
            tubes.used,
            count_cur_conns(),
            count_cur_producers(),
            count_cur_workers(),
            global_stat.waiting_ct,
            count_tot_conns(),
            (long) getpid(),
            version,
            (int) ru.ru_utime.tv_sec, (int) ru.ru_utime.tv_usec,
            (int) ru.ru_stime.tv_sec, (int) ru.ru_stime.tv_usec,
            uptime(),
            whead,
            wcur,
            srv->wal.nmig,
            srv->wal.nrec,
            srv->wal.filesize,
            id,
            node_info.nodename);

}

/* Read a priority value from the given buffer and place it in pri.
 * Update end to point to the address after the last character consumed.
 * Pri and end can be NULL. If they are both NULL, read_pri() will do the
 * conversion and return the status code but not update any values. This is an
 * easy way to check for errors.
 * If end is NULL, read_pri will also check that the entire input string was
 * consumed and return an error code otherwise.
 * Return 0 on success, or nonzero on failure.
 * If a failure occurs, pri and end are not modified. */
static int
read_pri(uint *pri, const char *buf, char **end)//读取数字
{
    char *tend;
    uint tpri;

    errno = 0;
    while (buf[0] == ' ') buf++;
    if (buf[0] < '0' || '9' < buf[0]) return -1;
    tpri = strtoul(buf, &tend, 10);
    if (tend == buf) return -1;
    if (errno && errno != ERANGE) return -1;
    if (!end && tend[0] != '\0') return -1;

    if (pri) *pri = tpri;
    if (end) *end = tend;
    return 0;
}

/* Read a delay value from the given buffer and place it in delay.
 * The interface and behavior are analogous to read_pri(). */
static int
read_delay(int64 *delay, const char *buf, char **end)
{
    int r;
    uint delay_sec;

    r = read_pri(&delay_sec, buf, end);
    if (r) return r;
    *delay = ((int64) delay_sec) * 1000000000;
    return 0;
}

/* Read a timeout value from the given buffer and place it in ttr.
 * The interface and behavior are the same as in read_delay(). */
static int
read_ttr(int64 *ttr, const char *buf, char **end)
{
    return read_delay(ttr, buf, end);
}

/* Read a tube name from the given buffer moving the buffer to the name start */
static int
read_tube_name(char **tubename, char *buf, char **end)//读取tube name
{
    size_t len;

    while (buf[0] == ' ') buf++;
    len = strspn(buf, NAME_CHARS);
    if (len == 0) return -1;
    if (tubename) *tubename = buf;
    if (end) *end = buf + len;
    return 0;
}

static void
wait_for_job(Conn *c, int timeout)//该函数的功能是：修改当前连接的状态为STATE_WAIT，并把该连接添加到，watch的tube的waiting数组中。然后把当前连接添加到dirty链表中。
{
    c->state = STATE_WAIT;//设置客户端状态为STATE_WAIT
    enqueue_waiting_conn(c);//将客户端添加到其监听的所有tube的waiting队列中

    /* Set the pending timeout to the requested timeout amount */
    c->pending_timeout = timeout;//设置客户端的超时时间 默认为-1   connwant(Conn *c, int rw)//pending_timeout 会被参考，决定在conn堆里面的的排序

    connwant(c, 'h'); // only care if they hang up  当一个终端只是等待有JOB  reday时,其实只要关注终端网络是否断开就好
    c->next = dirty;
    dirty = c;
}

typedef int(*fmt_fn)(char *, size_t, void *);

static void
do_stats(Conn *c, fmt_fn fmt, void *data)
{
    int r, stats_len;

    /* first, measure how big a buffer we will need */
    stats_len = fmt(NULL, 0, data) + 16;

    c->out_job = allocate_job(stats_len); /* fake job to hold stats data */
    if (!c->out_job) return reply_serr(c, MSG_OUT_OF_MEMORY);

    /* Mark this job as a copy so it can be appropriately freed later on */
    c->out_job->r.state = Copy;

    /* now actually format the stats data */
    r = fmt(c->out_job->body, stats_len, data);
    /* and set the actual body size */
    c->out_job->r.body_size = r;
    if (r > stats_len) return reply_serr(c, MSG_INTERNAL_ERROR);

    c->out_job_sent = 0;
    return reply_line(c, STATE_SENDJOB, "OK %d\r\n", r - 2);
}

static void
do_list_tubes(Conn *c, ms l)//发送所有l 里面 tube名字给客户端   返回的形式包装在一个job里面
{
    char *buf;
    tube t;
    size_t i, resp_z;

    /* first, measure how big a buffer we will need */
    resp_z = 6; /* initial "---\n" and final "\r\n" */
    for (i = 0; i < l->used; i++) {
        t = l->items[i];
        resp_z += 3 + strlen(t->name); /* including "- " and "\n" */
    }

    c->out_job = allocate_job(resp_z); /* fake job to hold response data */
    if (!c->out_job) return reply_serr(c, MSG_OUT_OF_MEMORY);

    /* Mark this job as a copy so it can be appropriately freed later on */
    c->out_job->r.state = Copy;

    /* now actually format the response */
    buf = c->out_job->body;
    buf += snprintf(buf, 5, "---\n");
    for (i = 0; i < l->used; i++) {
        t = l->items[i];
        buf += snprintf(buf, 4 + strlen(t->name), "- %s\n", t->name);
    }
    buf[0] = '\r';
    buf[1] = '\n';

    c->out_job_sent = 0;
    return reply_line(c, STATE_SENDJOB, "OK %zu\r\n", resp_z - 2);
}

static int
fmt_job_stats(char *buf, size_t size, job j)
{
    int64 t;
    int64 time_left;
    int file = 0;

    t = nanoseconds();
    if (j->r.state == Reserved || j->r.state == Delayed) {
        time_left = (j->r.deadline_at - t) / 1000000000;
    } else {
        time_left = 0;
    }
    if (j->file) {
        file = j->file->seq;
    }
    return snprintf(buf, size, STATS_JOB_FMT,
            j->r.id,
            j->tube->name,
            job_state(j),
            j->r.pri,
            (t - j->r.created_at) / 1000000000,
            j->r.delay / 1000000000,
            j->r.ttr / 1000000000,
            time_left,
            file,
            j->r.reserve_ct,
            j->r.timeout_ct,
            j->r.release_ct,
            j->r.bury_ct,
            j->r.kick_ct);
}

static int
fmt_stats_tube(char *buf, size_t size, tube t)
{
    uint64 time_left;

    if (t->pause > 0) {
        time_left = (t->deadline_at - nanoseconds()) / 1000000000;
    } else {
        time_left = 0;
    }
    return snprintf(buf, size, STATS_TUBE_FMT,
            t->name,
            t->stat.urgent_ct,
            t->ready.len,
            t->stat.reserved_ct,
            t->delay.len,
            t->stat.buried_ct,
            t->stat.total_jobs_ct,
            t->using_ct,
            t->watching_ct,
            t->stat.waiting_ct,
            t->stat.total_delete_ct,
            t->stat.pause_ct,
            t->pause / 1000000000,
            time_left);
}

static void
maybe_enqueue_incoming_job(Conn *c) //校验job数据是否读取完毕，完了则入tube的队列
{
    job j = c->in_job;

    /* do we have a complete job? */
    if (c->in_job_read == j->r.body_size) return enqueue_incoming_job(c);//任务数据已经读取完毕，入队列（ready或者delay队列）

    /* otherwise we have incomplete data, so just keep waiting */
    c->state = STATE_WANTDATA;//任务数据没有读取完毕，则设置客户端conn状态未等待接收数据STATE_WANTDATA  继续从客户端读入数据
}

/* j can be NULL */
static job
remove_this_reserved_job(Conn *c, job j)
{
    j = job_remove(j);
    if (j) {
        global_stat.reserved_ct--;
        j->tube->stat.reserved_ct--;
        j->reserver = NULL;
    }
    c->soonest_job = NULL;
    return j;
}

static job
remove_reserved_job(Conn *c, job j)
{
    return remove_this_reserved_job(c, find_reserved_job_in_conn(c, j));
}

static int
name_is_ok(const char *name, size_t max)
{
    size_t len = strlen(name);
    return len > 0 && len <= max &&
        strspn(name, NAME_CHARS) == len && name[0] != '-';
}

void
prot_remove_tube(tube t)
{
    ms_remove(&tubes, t);
}

static void
dispatch_cmd(Conn *c)
{
    int r, i, timeout = -1;
    int z;
    uint count;
    job j = 0;
    byte type;
    char *size_buf, *delay_buf, *ttr_buf, *pri_buf, *end_buf, *name;
    uint pri, body_size;
    int64 delay, ttr;
    uint64 id;
    tube t = NULL;

    /* NUL-terminate this string so we can use strtol and friends */
    c->cmd[c->cmd_len - 2] = '\0';

    /* check for possible maliciousness */
    if (strlen(c->cmd) != c->cmd_len - 2) {
        return reply_msg(c, MSG_BAD_FORMAT);
    }

    type = which_cmd(c);//查找命令类型
    if (verbose >= 2) {
        printf("<%d command %s\n", c->sock.fd, op_names[type]);
    }

    switch (type) {//switch处理各个命令
    case OP_PUT:
        r = read_pri(&pri, c->cmd + 4, &delay_buf);//读取优先级大小
        if (r) return reply_msg(c, MSG_BAD_FORMAT);

        r = read_delay(&delay, delay_buf, &ttr_buf);//读取delay
        if (r) return reply_msg(c, MSG_BAD_FORMAT);

        r = read_ttr(&ttr, ttr_buf, &size_buf);//读取ttr
        if (r) return reply_msg(c, MSG_BAD_FORMAT);

        errno = 0;
        body_size = strtoul(size_buf, &end_buf, 10);// data body 大小 解析job字节数
        if (errno) return reply_msg(c, MSG_BAD_FORMAT);

        op_ct[type]++;//统计

        if (body_size > job_data_size_limit) {//job长度超过限制；返回  #define JOB_DATA_SIZE_LIMIT_DEFAULT ((1 << 16) - 1)
            /* throw away the job body and respond with JOB_TOO_BIG */
            return skip(c, body_size + 2, MSG_JOB_TOO_BIG);
        }

        /* don't allow trailing garbage */
        if (end_buf[0] != '\0') return reply_msg(c, MSG_BAD_FORMAT);

        connsetproducer(c);//put，说明是生产者，设置conn类型为生产者

        if (ttr < 1000000000) {
            ttr = 1000000000;
        }

        c->in_job = make_job(pri, delay, ttr, body_size + 2, c->use);//初始化job结构体，存储在hash表all_jobs中

        /* OOM? */
        if (!c->in_job) {
            /* throw away the job body and respond with OUT_OF_MEMORY */
            twarnx("server error: " MSG_OUT_OF_MEMORY);
            return skip(c, body_size + 2, MSG_OUT_OF_MEMORY);
        }

        fill_extra_data(c);//解析客户端发来的任务数据，存储在c->in_job的body数据字段

        /* it's possible we already have a complete job */
        maybe_enqueue_incoming_job(c); //校验job数据是否读取完毕，完了则入tube的队列

        break;
    case OP_PEEK_READY://所谓peek就是只是拿到，而不去删除或改变状态    获取当前处于reday的元素   注意所有peek的job都是copy的
        /* don't allow trailing garbage */
        if (c->cmd_len != CMD_PEEK_READY_LEN + 2) {
            return reply_msg(c, MSG_BAD_FORMAT);
        }
        op_ct[type]++;

        if (c->use->ready.len) {
            j = job_copy(c->use->ready.data[0]);
        }

        if (!j) return reply(c, MSG_NOTFOUND, MSG_NOTFOUND_LEN, STATE_SENDWORD);//NOT_FOUND\r\n

        reply_job(c, j, MSG_FOUND);
        break;
    case OP_PEEK_DELAYED://获取放在delay堆里面的一个job
        /* don't allow trailing garbage */
        if (c->cmd_len != CMD_PEEK_DELAYED_LEN + 2) {
            return reply_msg(c, MSG_BAD_FORMAT);
        }
        op_ct[type]++;

        if (c->use->delay.len) {
            j = job_copy(c->use->delay.data[0]);
        }

        if (!j) return reply(c, MSG_NOTFOUND, MSG_NOTFOUND_LEN, STATE_SENDWORD);

        reply_job(c, j, MSG_FOUND);
        break;
    case OP_PEEK_BURIED://获取已经被buried的job
        /* don't allow trailing garbage */
        if (c->cmd_len != CMD_PEEK_BURIED_LEN + 2) {
            return reply_msg(c, MSG_BAD_FORMAT);
        }
        op_ct[type]++;

        j = job_copy(buried_job_p(c->use)? j = c->use->buried.next : NULL);

        if (!j) return reply(c, MSG_NOTFOUND, MSG_NOTFOUND_LEN, STATE_SENDWORD);

        reply_job(c, j, MSG_FOUND);
        break;
    case OP_PEEKJOB:
        errno = 0;
        id = strtoull(c->cmd + CMD_PEEKJOB_LEN, &end_buf, 10);// 获取参数：job id
        if (errno) return reply_msg(c, MSG_BAD_FORMAT);
        op_ct[type]++;

        /* So, peek is annoying, because some other connection might free the
         * job while we are still trying to write it out. So we copy it and
         * then free the copy when it's done sending. */
        j = job_copy(peek_job(id));  // 把找到的job复制到新的内存，防止在peek的job，正在被删除

        if (!j) return reply(c, MSG_NOTFOUND, MSG_NOTFOUND_LEN, STATE_SENDWORD);

        reply_job(c, j, MSG_FOUND);
        break;
    case OP_RESERVE_TIMEOUT://reserve-with-timeout
        errno = 0;
        timeout = strtol(c->cmd + CMD_RESERVE_TIMEOUT_LEN, &end_buf, 10);
        if (errno) return reply_msg(c, MSG_BAD_FORMAT);
    case OP_RESERVE: /* FALLTHROUGH */ //reserve 其实和put是相反的
        /* don't allow trailing garbage */
        if (type == OP_RESERVE && c->cmd_len != CMD_RESERVE_LEN + 2) {
            return reply_msg(c, MSG_BAD_FORMAT);
        }

        op_ct[type]++;
        connsetworker(c);//RESERVE，说明是消费者，设置conn类型为消费者   (1) 首先把conn的类型设置为CONN_TYPE_WORKER。因为，执行reserve命令的客户端的状态此时被认为是worker，是自然的

        if (conndeadlinesoon(c) && !conn_ready(c)) { //当客户端有多个任务正在处理，处于reserved状态，且超时时间即将到达时；如果此时客户端监听的所有tube都没有ready状态的任务，则直接返回MSG_DEADLINE_SOON给客户端
            return reply_msg(c, MSG_DEADLINE_SOON);//a.从参数c(连接)的reserved_jobs链表中找到一个ttr最小的job(若没有设置ttr选择就是最先插入的job)，并判断该Job是否至少还有1秒过期。 b.检查当前conn watch的tube的ready job堆是否有准备好的job
        }//若存在满足条件a的reserved的job，并且watch的tube没有准备好的job，则直接返回MSG_DEADLINE_SOON消息，并把连接设置为等待’w’(写入)的状态(等待服务向连接写数据)，否则继续执行下面的步骤。

        /* try to get a new job for this guy */
        wait_for_job(c, timeout); //设置当前客户端正在等待job  该函数的功能是：修改当前连接的状态为STATE_WAIT，并把该连接添加到，watch的tube的waiting数组中。然后把当前连接添加到dirty链表中。
        process_queue();//检查所有tube 看是不是有已经准备好的job 分发给客户端
        break;
    case OP_DELETE://beanstalked删除其实 使用了哈希表来加快速度 通过id找到job 然后分别从 reserve reday buried delayed里面删除（从大顶堆或者链表里面删除）链表因为是双向链表 所以删除是O(1)  堆里面删除因为job记录了在队里的索引 所以删除也是O(1) 当然堆得调整排除在外
        errno = 0;
        id = strtoull(c->cmd + CMD_DELETE_LEN, &end_buf, 10); // 获取参数：job id
        if (errno) return reply_msg(c, MSG_BAD_FORMAT);
        op_ct[type]++;

        j = job_find(id);  //从全局job的hash表all_jobs中查找对应job id的job实体
        j = remove_reserved_job(c, j) ? ://先查看这个job是不是被当前的客户端reserve了 是的话直接在reserve里面删除
            remove_ready_job(j) ? ://检查是不是在reday队列里面 是的话也删除
            remove_buried_job(j) ? ://检查是不是在buried队列里面是的话也删除
            remove_delayed_job(j);//检查是不是在delayed队列里面 是的话删除

        if (!j) return reply(c, MSG_NOTFOUND, MSG_NOTFOUND_LEN, STATE_SENDWORD);

		j->tube->stat.total_delete_ct++;//统计该tube删除的job数量

        j->r.state = Invalid;//设置job的状态为Invalid，注意这是一个中间状态，因为马上就会释放该job的内存
        r = walwrite(&c->srv->wal, j);//若开启了wal选项，还需要把job写入到文件中
        walmaint(&c->srv->wal);//若开启了wal选项，写入binlog日志
        job_free(j);// 若job还没有从all_jobs中删除，则删除之，然后释放该job的内存

        if (!r) return reply_serr(c, MSG_INTERNAL_ERROR);

        reply(c, MSG_DELETED, MSG_DELETED_LEN, STATE_SENDWORD);
        break;
    case OP_RELEASE://release命令将一个reserved的job放回ready堆中。它通常在job执行失败时使用。 release <id> <pri> <delay>\r\n
        errno = 0;
        id = strtoull(c->cmd + CMD_RELEASE_LEN, &pri_buf, 10);//id：为job id
        if (errno) return reply_msg(c, MSG_BAD_FORMAT);

        r = read_pri(&pri, pri_buf, &delay_buf);//pri：为job的优先级
        if (r) return reply_msg(c, MSG_BAD_FORMAT);

        r = read_delay(&delay, delay_buf, NULL);//delay：为延迟ready的秒数
        if (r) return reply_msg(c, MSG_BAD_FORMAT);
        op_ct[type]++;

        j = remove_reserved_job(c, job_find(id));//从全局工作哈希表：all_jobs(保存所有job的全局hash表)中查找该job的指针 

        if (!j) return reply(c, MSG_NOTFOUND, MSG_NOTFOUND_LEN, STATE_SENDWORD);

        /* We want to update the delay deadline on disk, so reserve space for
         * that. */
        if (delay) {
            z = walresvupdate(&c->srv->wal, j);
            if (!z) return reply_serr(c, MSG_OUT_OF_MEMORY);
            j->walresv += z;
        }

        j->r.pri = pri;//把优先级设置成参数的值
        j->r.delay = delay;
        j->r.release_ct++;

        r = enqueue_job(c->srv, j, delay, !!delay);//若delay大于0，则放入tube的delay堆，否则放入tube的ready堆中
        if (r < 0) return reply_serr(c, MSG_INTERNAL_ERROR);
        if (r == 1) {
            return reply(c, MSG_RELEASED, MSG_RELEASED_LEN, STATE_SENDWORD);
        }

        /* out of memory trying to grow the queue, so it gets buried */
        bury_job(c->srv, j, 0);
        reply(c, MSG_BURIED, MSG_BURIED_LEN, STATE_SENDWORD);
        break;
    case OP_BURY:
        errno = 0;
        id = strtoull(c->cmd + CMD_BURY_LEN, &pri_buf, 10);  //获取参数：bury的job的id
        if (errno) return reply_msg(c, MSG_BAD_FORMAT);

        r = read_pri(&pri, pri_buf, NULL);
        if (r) return reply_msg(c, MSG_BAD_FORMAT);
        op_ct[type]++;

        j = remove_reserved_job(c, job_find(id));

        if (!j) return reply(c, MSG_NOTFOUND, MSG_NOTFOUND_LEN, STATE_SENDWORD);

        j->r.pri = pri;
        r = bury_job(c->srv, j, 1);
        if (!r) return reply_serr(c, MSG_INTERNAL_ERROR);
        reply(c, MSG_BURIED, MSG_BURIED_LEN, STATE_SENDWORD);
        break;
    case OP_KICK:
        errno = 0;
        count = strtoul(c->cmd + CMD_KICK_LEN, &end_buf, 10);
        if (end_buf == c->cmd + CMD_KICK_LEN) {
            return reply_msg(c, MSG_BAD_FORMAT);
        }
        if (errno) return reply_msg(c, MSG_BAD_FORMAT);

        op_ct[type]++;

        i = kick_jobs(c->srv, c->use, count);

        return reply_line(c, STATE_SENDWORD, "KICKED %u\r\n", i);
    case OP_JOBKICK:
        errno = 0;
        id = strtoull(c->cmd + CMD_JOBKICK_LEN, &end_buf, 10);
        if (errno) return twarn("strtoull"), reply_msg(c, MSG_BAD_FORMAT);

        op_ct[type]++;

        j = job_find(id);
        if (!j) return reply(c, MSG_NOTFOUND, MSG_NOTFOUND_LEN, STATE_SENDWORD);

        if ((j->r.state == Buried && kick_buried_job(c->srv, j)) ||
            (j->r.state == Delayed && kick_delayed_job(c->srv, j))) {
            reply(c, MSG_KICKED, MSG_KICKED_LEN, STATE_SENDWORD);
        } else {
            return reply(c, MSG_NOTFOUND, MSG_NOTFOUND_LEN, STATE_SENDWORD);
        }
        break;
    case OP_TOUCH://The touch command allows a worker to request more time to work on a job.  也就是说touch是一个worker在处理一个任务时，可能快要超时了 touch让这个job的超时时间推后   
        errno = 0;
        id = strtoull(c->cmd + CMD_TOUCH_LEN, &end_buf, 10);
        if (errno) return twarn("strtoull"), reply_msg(c, MSG_BAD_FORMAT);

        op_ct[type]++;

        j = touch_job(c, job_find(id));//job_find()函数的功能是：从全局变量all_jobs哈希表中查找对应id的job。  touch_job针对这个job 将dead时间增加ttr  延长RESERVE_TIMEOUT的时间

        if (j) {
            reply(c, MSG_TOUCHED, MSG_TOUCHED_LEN, STATE_SENDWORD);
        } else {
            return reply(c, MSG_NOTFOUND, MSG_NOTFOUND_LEN, STATE_SENDWORD);
        }
        break;
    case OP_STATS:
        /* don't allow trailing garbage */
        if (c->cmd_len != CMD_STATS_LEN + 2) {
            return reply_msg(c, MSG_BAD_FORMAT);
        }

        op_ct[type]++;

        do_stats(c, fmt_stats, c->srv);
        break;
    case OP_JOBSTATS:
        errno = 0;
        id = strtoull(c->cmd + CMD_JOBSTATS_LEN, &end_buf, 10);
        if (errno) return reply_msg(c, MSG_BAD_FORMAT);

        op_ct[type]++;

        j = peek_job(id);
        if (!j) return reply(c, MSG_NOTFOUND, MSG_NOTFOUND_LEN, STATE_SENDWORD);

        if (!j->tube) return reply_serr(c, MSG_INTERNAL_ERROR);
        do_stats(c, (fmt_fn) fmt_job_stats, j);
        break;
    case OP_STATS_TUBE:
        name = c->cmd + CMD_STATS_TUBE_LEN;
        if (!name_is_ok(name, 200)) return reply_msg(c, MSG_BAD_FORMAT);

        op_ct[type]++;

        t = tube_find(name);
        if (!t) return reply_msg(c, MSG_NOTFOUND);

        do_stats(c, (fmt_fn) fmt_stats_tube, t);
        t = NULL;
        break;
    case OP_LIST_TUBES://以job的形式返回所有的列表
        /* don't allow trailing garbage */
        if (c->cmd_len != CMD_LIST_TUBES_LEN + 2) {
            return reply_msg(c, MSG_BAD_FORMAT);
        }

        op_ct[type]++;
        do_list_tubes(c, &tubes);
        break;
    case OP_LIST_TUBE_USED://返回正在使用的tube的名字  USING default\r\n
        /* don't allow trailing garbage */
        if (c->cmd_len != CMD_LIST_TUBE_USED_LEN + 2) {
            return reply_msg(c, MSG_BAD_FORMAT);
        }

        op_ct[type]++;
        reply_line(c, STATE_SENDWORD, "USING %s\r\n", c->use->name);
        break;
    case OP_LIST_TUBES_WATCHED:
        /* don't allow trailing garbage */
        if (c->cmd_len != CMD_LIST_TUBES_WATCHED_LEN + 2) {
            return reply_msg(c, MSG_BAD_FORMAT);
        }

        op_ct[type]++;
        do_list_tubes(c, &c->watch);
        break;
    case OP_USE://使用指定的tube 如果没有这个tube，那么就创建这个tube，放入tube ms      use和using命令，影响的是put操作。而且只能有一个tube被use。生产者采用这个命令 消费者用的是watch
        name = c->cmd + CMD_USE_LEN;
        if (!name_is_ok(name, 200)) return reply_msg(c, MSG_BAD_FORMAT);
        op_ct[type]++;//记录命令被执行了多少次

        TUBE_ASSIGN(t, tube_find_or_make(name));
        if (!t) return reply_serr(c, MSG_OUT_OF_MEMORY);

        c->use->using_ct--;
        TUBE_ASSIGN(c->use, t);//减少上个tube的引用计数，新的tube赋值给use，然后增加当前的应用计数
        TUBE_ASSIGN(t, NULL);
        c->use->using_ct++;

        reply_line(c, STATE_SENDWORD, "USING %s\r\n", c->use->name);//回复客户端 USING xxxx  标记当前链接状态机为STATE_SENDWORD
        break;
    case OP_WATCH://监控指定tube    watch和watching，影响的是reserve操作，watch的tube可以是多个，只要其中有一个有数据，reserve都会返回该数据。 通过ingore来取消watch的tube。 
        name = c->cmd + CMD_WATCH_LEN;
        if (!name_is_ok(name, 200)) return reply_msg(c, MSG_BAD_FORMAT);
        op_ct[type]++;

        TUBE_ASSIGN(t, tube_find_or_make(name));
        if (!t) return reply_serr(c, MSG_OUT_OF_MEMORY);

        r = 1;
        if (!ms_contains(&c->watch, t)) r = ms_append(&c->watch, t);//在自己的监控列表里面增加 watch XXXX tube
        TUBE_ASSIGN(t, NULL);
        if (!r) return reply_serr(c, MSG_OUT_OF_MEMORY);

        reply_line(c, STATE_SENDWORD, "WATCHING %zu\r\n", c->watch.used);//返回当前已经监控了多少个 tube
        break;
    case OP_IGNORE://取消监听tube
        name = c->cmd + CMD_IGNORE_LEN;
        if (!name_is_ok(name, 200)) return reply_msg(c, MSG_BAD_FORMAT);
        op_ct[type]++;

        t = NULL;
        for (i = 0; i < c->watch.used; i++) {
            t = c->watch.items[i];
            if (strncmp(t->name, name, MAX_TUBE_NAME_LEN) == 0) break;
            t = NULL;
        }

        if (t && c->watch.used < 2) return reply_msg(c, MSG_NOT_IGNORED);

        if (t) ms_remove(&c->watch, t); /* may free t if refcount => 0 */
        t = NULL;

        reply_line(c, STATE_SENDWORD, "WATCHING %zu\r\n", c->watch.used);//返回当前已经监控了多少个 tube
        break;
    case OP_QUIT:
        c->state = STATE_CLOSE;
        break;
    case OP_PAUSE_TUBE://暂停指定的tube 暂停期间所有job都不能再被消费者消费
        op_ct[type]++;

        r = read_tube_name(&name, c->cmd + CMD_PAUSE_TUBE_LEN, &delay_buf);
        if (r) return reply_msg(c, MSG_BAD_FORMAT);

        r = read_delay(&delay, delay_buf, NULL);
        if (r) return reply_msg(c, MSG_BAD_FORMAT);

        *delay_buf = '\0';
        t = tube_find(name);
        if (!t) return reply_msg(c, MSG_NOTFOUND);

        // Always pause for a positive amount of time, to make sure
        // that waiting clients wake up when the deadline arrives.
        if (delay == 0) {
            delay = 1;
        }

        t->deadline_at = nanoseconds() + delay;
        t->pause = delay;
        t->stat.pause_ct++;

        reply_line(c, STATE_SENDWORD, "PAUSED\r\n");
        break;
    default:
        return reply_msg(c, MSG_UNKNOWN_COMMAND);
    }
}

/* There are three reasons this function may be called. We need to check for
 * all of them.
 *
 *  1. A reserved job has run out of time.
 *  2. A waiting client's reserved job has entered the safety margin.
 *  3. A waiting client's requested timeout has occurred.
 *
 * If any of these happen, we must do the appropriate thing. */
static void
conn_timeout(Conn *c)
{
    int r, should_timeout = 0;
    job j;

    /* Check if the client was trying to reserve a job. */
    if (conn_waiting(c) && conndeadlinesoon(c)) should_timeout = 1;//客户端正在被阻塞时，如果有reserved状态的job即将到期，则需要解除客户端阻塞 //conndeadlinesoon：查询到期时间最小的reserved job，校验其是否即将到期（1秒内到期）

    /* Check if any reserved jobs have run out of time. We should do this
     * whether or not the client is waiting for a new reservation. */
    while ((j = connsoonestjob(c))) {//connsoonestjob获取到期时间最近的reserved job
        if (j->r.deadline_at >= nanoseconds()) break;

        /* This job is in the middle of being written out. If we return it to
         * the ready queue, someone might free it before we finish writing it
         * out to the socket. So we'll copy it here and free the copy when it's
         * done sending. */
        if (j == c->out_job) {
            c->out_job = job_copy(c->out_job);
        }

        timeout_ct++; /* stats */
        j->r.timeout_ct++;
        r = enqueue_job(c->srv, remove_this_reserved_job(c, j), 0, 0);//从客户端的reserved_jobs链表移除job，重新入到tube的相应job队列
        if (r < 1) bury_job(c->srv, j, 0); /* out of memory, so bury it */
        connsched(c);//重新计算conn待处理事件的时间，入srv->conns堆
    }

    if (should_timeout) {
        return reply_msg(remove_waiting_conn(c), MSG_DEADLINE_SOON);//reserved即将到期，解除阻塞，返回MSG_DEADLINE_SOON消息
    } else if (conn_waiting(c) && c->pending_timeout >= 0) {//客户端阻塞超时，解除阻塞
        c->pending_timeout = -1;
        return reply_msg(remove_waiting_conn(c), MSG_TIMED_OUT);
    }
}

void
enter_drain_mode(int sig)
{
    drain_mode = 1;
}

static void
do_cmd(Conn *c)//命令执行的入口函数
{
    dispatch_cmd(c);//分发并执行命令
    fill_extra_data(c);//put命令时，不仅需要执行命令，还需要接收分析job body数据
}

static void
reset_conn(Conn *c)//改为需要读取fd 重置信息 重置为STATE_WANTCOMMAND
{
    connwant(c, 'r');//设置读取事件
    c->next = dirty;//加入dirty列表
    dirty = c;

    /* was this a peek or stats command? */
    if (c->out_job && c->out_job->r.state == Copy) job_free(c->out_job);//如果out_job是拷贝出来的job  那么要释放内存
    c->out_job = NULL;

    c->reply_sent = 0; /* now that we're done, reset this */
    c->state = STATE_WANTCOMMAND;
}

static void
conn_data(Conn *c)//客户端数据交互（根据客户端状态不同执行不同的读写操作）
{
    int r, to_read;
    job j;
    struct iovec iov[2];

    switch (c->state) {
    case STATE_WANTCOMMAND:
        r = read(c->sock.fd, c->cmd + c->cmd_read, LINE_BUF_SIZE - c->cmd_read);//读取命令到输入缓冲区cmd
        if (r == -1) return check_err(c, "read()");
        if (r == 0) {//说明终端已经close了
            c->state = STATE_CLOSE;
            return;
        }

        c->cmd_read += r; /* we got some bytes 我们读取到了 r字节数据 */

        c->cmd_len = cmd_len(c); /* find the EOL */ //定位\r\n，并返回命令请求开始位置到\r\n长度；如果没有\r\n说明命令请求不完全，返回0  协议格式在protocol.md里面有说明

        /* yay, complete command line */
        if (c->cmd_len) return do_cmd(c);//如果读取完整的命令，则处理；否则意味着命令不完全，需要下次继续接收    c->cmd_len>0表示已经读取完了   

        /* c->cmd_read > LINE_BUF_SIZE can't happen */

        /* command line too long? */
        if (c->cmd_read == LINE_BUF_SIZE) {
            c->cmd_read = 0; /* discard the input so far 如果命令行读取太长，那么就扔掉，重新读*/
            return reply_msg(c, MSG_BAD_FORMAT);//给客户端返回错误消息
        }

        /* otherwise we have an incomplete line, so just keep waiting */
        break;
    case STATE_BITBUCKET:
        /* Invert the meaning of in_job_read while throwing away data -- it
         * counts the bytes that remain to be thrown away. */
        to_read = min(c->in_job_read, BUCKET_BUF_SIZE);
        r = read(c->sock.fd, bucket, to_read);
        if (r == -1) return check_err(c, "read()");
        if (r == 0) {
            c->state = STATE_CLOSE;
            return;
        }

        c->in_job_read -= r; /* we got some bytes */

        /* (c->in_job_read < 0) can't happen */

        if (c->in_job_read == 0) {
            return reply(c, c->reply, c->reply_len, STATE_SENDWORD);
        }
        break;
    case STATE_WANTDATA://只有当使用put命令发布任务时，才会携带数据；客户端状态才会成为STATE_WANTDATA；而读取命令行时，已经携带了任务的必要参数（优先级 大小等信息），那时已经创建了任务，并存储在c->in_job字段
        j = c->in_job;

        r = read(c->sock.fd, j->body + c->in_job_read, j->r.body_size -c->in_job_read);//读取任务数据  这个buf最多也就读 j->r.body_size 这么大  读取这么大了，说明已经读取完毕了
        if (r == -1) return check_err(c, "read()");
        if (r == 0) {
            c->state = STATE_CLOSE;
            return;
        }

        c->in_job_read += r; /* we got some bytes *///记录任务读取了多少数据

        /* (j->in_job_read > j->r.body_size) can't happen */ // read(c->sock.fd, j->body + c->in_job_read, j->r.body_size -c->in_job_read);已经限制了

        maybe_enqueue_incoming_job(c); //函数会判断任务数据是否已经读取完全，完全则将任务写入tube的ready或delay队列；
        break;
    case STATE_SENDWORD:
        r= write(c->sock.fd, c->reply + c->reply_sent, c->reply_len - c->reply_sent);
        if (r == -1) return check_err(c, "write()");
        if (r == 0) {//说明终端已经断开连接了
            c->state = STATE_CLOSE;
            return;
        }

        c->reply_sent += r; /* we got some bytes */

        /* (c->reply_sent > c->reply_len) can't happen */

        if (c->reply_sent == c->reply_len) return reset_conn(c);//如果job的数据已经发完，则重置客户端为读取 状态机改为want_cmd，关心可读事件；否则继续待发送job

        /* otherwise we sent an incomplete reply, so just keep waiting */
        break;
    case STATE_SENDJOB://实际直接发送的就是 RESERVED <id> <bytes>\r\n<data>\r\n  放在两个参数里面发送
        j = c->out_job;

        iov[0].iov_base = (void *)(c->reply + c->reply_sent);
        iov[0].iov_len = c->reply_len - c->reply_sent; /* maybe 0   这个值可能是0 这种情况下只需要发job就好了*/
        iov[1].iov_base = j->body + c->out_job_sent;
        iov[1].iov_len = j->r.body_size - c->out_job_sent;

        r = writev(c->sock.fd, iov, 2);
        if (r == -1) return check_err(c, "writev()");
        if (r == 0) {
            c->state = STATE_CLOSE;
            return;
        }

        /* update the sent values */
        c->reply_sent += r;
        if (c->reply_sent >= c->reply_len) {
            c->out_job_sent += c->reply_sent - c->reply_len;
            c->reply_sent = c->reply_len;
        }

        /* (c->out_job_sent > j->r.body_size) can't happen */

        /* are we done? */
        if (c->out_job_sent == j->r.body_size) {
            if (verbose >= 2) {
                printf(">%d job %"PRIu64"\n", c->sock.fd, j->r.id);
            }
            return reset_conn(c);//如果job的数据已经发完，则重置客户端rw，关心可读事件；否则继续待发送job
        }

        /* otherwise we sent incomplete data, so just keep waiting */
        break;
    case STATE_WAIT:
        if (c->halfclosed) {
            c->pending_timeout = -1;
            return reply_msg(remove_waiting_conn(c), MSG_TIMED_OUT);
        }
        break;
    }
}

#define want_command(c) ((c)->sock.fd && ((c)->state == STATE_WANTCOMMAND))
#define cmd_data_ready(c) (want_command(c) && (c)->cmd_read)

static void
update_conns()//update_conns负责更新客户端socket的事件到epoll；其在每次循环开始，执行epoll_wait之前都会执
{
    int r;
    Conn *c;

    while (dirty) {//遍历dirty链表，更新每一个conn关心的socket事件
        c = dirty;
        dirty = dirty->next;
        c->next = NULL;
        r = sockwant(&c->sock, c->rw);
        if (r == -1) {
            twarn("sockwant");
            connclose(c);
        }
    }
}

static void
h_conn(const int fd/*客户端的fd*/, const short which/*读还是写...*/, Conn *c/*客户端指针*/)
{
    if (fd != c->sock.fd) {
        twarnx("Argh! event fd doesn't match conn fd.");
        close(fd);
        connclose(c);
        update_conns();
        return;
    }

    if (which == 'h') { //客户端断开链接，标记
        c->halfclosed = 1;
    }

    conn_data(c);//客户端数据交互（根据客户端状态不同执行不同的读写操作）
    while (cmd_data_ready(c) && (c->cmd_len = cmd_len(c))) do_cmd(c);//解析完命令时，执行命令
    if (c->state == STATE_CLOSE) {
        protrmdirty(c);
        connclose(c);
    }
    update_conns();
}

static void
prothandle(Conn *c, int ev/*读还是写....*/)
{
    h_conn(c->sock.fd, ev, c);
}

int64
prottick(Server *s)
{
    int r;
    job j;
    int64 now;
    int i;
    tube t;
    int64 period = 0x34630B8A000LL; /* 1 hour in nanoseconds */
    int64 d;

    now = nanoseconds();
    while ((j = delay_q_peek())) {//1）将状态为delay的且已经到期的job移到ready队列；  遍历所有tube的delay队列中过期时间已经到达或者即将的job（即将到达时间最小） peek的意思就是我只看一下
        d = j->r.deadline_at - now;
        if (d > 0) {
            period = min(period, d);//即将到达，更新period
            break;
        }
        j = delay_q_take();//d<0 说明已经有delay的任务到期了 delay_q_take的实现方式其实就是delay_q_peek获取时间最小的job 然后从delay堆里面删除这个job
        r = enqueue_job(s, j, 0, 0);
        if (r < 1) bury_job(s, j, 0); /* out of memory, so bury it */
    }

    for (i = 0; i < tubes.used; i++) {//2）tube暂停时间到达，如果tube存在消费者阻塞等待获取job，需要返回job给客户端；
        t = tubes.items[i];
        d = t->deadline_at - now;
        if (t->pause && d <= 0) {//tube暂停期限达到，process_queue同3.4.2节
            t->pause = 0;
            process_queue();//检查有没有消费者正在阻塞等待此tube产生job，若有需要返回job
        }
        else if (d > 0) {
            period = min(period, d);//tube还没有到期，更新period
        }
    }

    while (s->conns.len) {
        Conn *c = s->conns.data[0];//循环获取conn待执行事件发生时间最早的
        d = c->tickat - now;
        if (d > 0) {
            period = min(period, d);
            break;
        }

        heapremove(&s->conns, 0);
        conn_timeout(c);
    }

    update_conns();

    return period;
}

void
h_accept(const int fd, const short which/*读还是写 这儿没有使用*/, Server *s/*指向全局的srv*/)
{
    Conn *c;
    int cfd, flags, r;
    socklen_t addrlen;
    struct sockaddr_in6 addr;

    addrlen = sizeof addr;
    cfd = accept(fd, (struct sockaddr *)&addr, &addrlen);//获取客户端的fd   这个时候三次握手已经完成 cfd是客户端的通信fd
    if (cfd == -1) {
        if (errno != EAGAIN && errno != EWOULDBLOCK) twarn("accept()");
        update_conns();
        return;
    }
    if (verbose) {
        printf("accept %d\n", cfd);
    }

    flags = fcntl(cfd, F_GETFL, 0);//获得fd标识 获得fd已经存在的属性
    if (flags < 0) {
        twarn("getting flags");
        close(cfd);
        if (verbose) {
            printf("close %d\n", cfd);
        }
        update_conns();
        return;
    }

    r = fcntl(cfd, F_SETFL, flags | O_NONBLOCK);//设置fd非阻塞，使用epoll必须设置非阻塞，负责epoll无法同时监听多个fd
    if (r < 0) {
        twarn("setting O_NONBLOCK");
        close(cfd);
        if (verbose) {
            printf("close %d\n", cfd);
        }
        update_conns();
        return;
    }

    c = make_conn(cfd, STATE_WANTCOMMAND, default_tube, default_tube);//创建conn对象；默认监听default_tube（c->watch存储所有监听的tube）；默认使用default_tube（c->use）//注意：初始化conn对象时，客户端状态为STATE_WANTCOMMAND，即等待接收客户端命令；
    if (!c) { // 若创建Conn实体失败，关闭该连接
        twarnx("make_conn() failed");
        close(cfd);
        if (verbose) {
            printf("close %d\n", cfd);
        }
        update_conns();
        return;
    }
    c->srv = s;// 反向指针，指向该连接所在的Server结构实体。
    c->sock.x = c; // sock.x指向本Conn实体的指针
    c->sock.f = (Handle)prothandle;//设置客户端处理函数
    c->sock.fd = cfd;// 设置sock.fd描述符的值为cfd

    r = sockwant(&c->sock, 'r');//epoll注册，对每个客户端监听可读事件
    if (r == -1) {
        twarn("sockwant");
        close(cfd);
        if (verbose) {
            printf("close %d\n", cfd);
        }
        update_conns();
        return;
    }
    update_conns();
}

void
prot_init()
{
    started_at = nanoseconds();//纳秒
    memset(op_ct, 0, sizeof(op_ct));

    int dev_random = open("/dev/urandom", O_RDONLY);
    if (dev_random < 0) {
        twarn("open /dev/urandom");
        exit(50);
    }

    int i, r;
    byte rand_data[NumIdBytes];
    r = read(dev_random, &rand_data, NumIdBytes);
    if (r != NumIdBytes) {
        twarn("read /dev/urandom");
        exit(50);
    }
    for (i = 0; i < NumIdBytes; i++) {
        sprintf(id + (i * 2), "%02x", rand_data[i]);
    }
    close(dev_random);

    if (uname(&node_info) == -1) {
        warn("uname");
        exit(50);
    }

    ms_init(&tubes, NULL, NULL);

    TUBE_ASSIGN(default_tube, tube_find_or_make("default"));//tube_find_or_make 会把新创建的tube放入 tubes里面
    if (!default_tube) twarnx("Out of memory during startup!");
}

// For each job in list, inserts the job into the appropriate data
// structures and adds it to the log.
//
// Returns 1 on success, 0 on failure.
int
prot_replay(Server *s, job list)
{
    job j, nj;
    int64 t, delay;
    int r, z;

    for (j = list->next ; j != list ; j = nj) {
        nj = j->next;
        job_remove(j);
        z = walresvupdate(&s->wal, j);
        if (!z) {
            twarnx("failed to reserve space");
            return 0;
        }
        delay = 0;
        switch (j->r.state) {
        case Buried:
            bury_job(s, j, 0);
            break;
        case Delayed:
            t = nanoseconds();
            if (t < j->r.deadline_at) {
                delay = j->r.deadline_at - t;
            }
            /* fall through */
        default:
            r = enqueue_job(s, j, delay, 0);
            if (r < 1) twarnx("error recovering job %"PRIu64, j->r.id);
        }
    }
    return 1;
}
