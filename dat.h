// Requirements:
// #include <stdint.h>
// #include <stdlib.h>

typedef unsigned char uchar;
typedef uchar         byte;
typedef unsigned int  uint;
typedef int32_t       int32;
typedef uint32_t      uint32;
typedef int64_t       int64;
typedef uint64_t      uint64;

#define int8_t   do_not_use_int8_t
#define uint8_t  do_not_use_uint8_t
#define int32_t  do_not_use_int32_t
#define uint32_t do_not_use_uint32_t
#define int64_t  do_not_use_int64_t
#define uint64_t do_not_use_uint64_t

typedef struct ms     *ms;
typedef struct job    *job;
typedef struct tube   *tube;
typedef struct Conn   Conn;
typedef struct Heap   Heap;
typedef struct Jobrec Jobrec;
typedef struct File   File;
typedef struct Socket Socket;
typedef struct Server Server;
typedef struct Wal    Wal;

typedef void(*ms_event_fn)(ms a, void *item, size_t i);
typedef void(*Handle)(void*, int rw);
typedef int(*Less)(void*, void*);
typedef void(*Record)(void*, int);
typedef int(FAlloc)(int, int);

#if _LP64
#define NUM_PRIMES 48
#else
#define NUM_PRIMES 19
#endif

#define MAX_TUBE_NAME_LEN 201

/* A command can be at most LINE_BUF_SIZE chars, including "\r\n". This value
 * MUST be enough to hold the longest possible command or reply line, which is
 * currently "USING a{200}\r\n". */
#define LINE_BUF_SIZE 208//命令最大也就这么大了

/* CONN_TYPE_* are bit masks */
#define CONN_TYPE_PRODUCER 1//客户端是个消费者
#define CONN_TYPE_WORKER   2//客户端是个生产者
#define CONN_TYPE_WAITING  4//客户端处于正在等待job的状态  //客户端正在阻塞

#define min(a,b) ((a)<(b)?(a):(b))

#define URGENT_THRESHOLD 1024
#define JOB_DATA_SIZE_LIMIT_DEFAULT ((1 << 16) - 1)

extern const char version[];
extern int verbose;
extern struct Server srv;

// Replaced by tests to simulate failures.
extern FAlloc *falloc;

struct stats {
    uint urgent_ct;
    uint waiting_ct;
    uint buried_ct;
    uint reserved_ct;
    uint pause_ct;
    uint64   total_delete_ct;
    uint64   total_jobs_ct;
};


struct Heap {//存储的内容只是指针
    int     cap;//堆可以容纳的数量
    int     len;//堆当前的大小
    void    **data;//堆数组
    Less    less;// 入堆时需要用到less函数，来和堆中的父节点进行值的比较
    Record  rec;// 设置堆数组中的index的值
};
int   heapinsert(Heap *h, void *x);
void* heapremove(Heap *h, int k);


struct Socket {
    int    fd;
    Handle f;//fd的处理函数
    void   *x;
    int    added;//该fd是否已经添加到epoll里面
};
int sockinit(void);
int sockwant(Socket*, int);
int socknext(Socket**, int64);

struct ms {//可以理解成一个集合  一个数组  存储的内容就是指针
    size_t used, cap, last;
    void **items;
    ms_event_fn oninsert, onremove;
};

enum
{
    Walver = 7
};

enum // Jobrec.state
{
    Invalid,
    Ready,
    Reserved,
    Buried,
    Delayed,
    Copy
};

// if you modify this struct, you must increment Walver above
struct Jobrec {
    uint64 id;//job的id
    uint32 pri;
    int64  delay;//job的delay时间
    int64  ttr;//job的失效时间
    int32  body_size;//job的body的大小
    int64  created_at;//创建时间
    int64  deadline_at;//job的过期时间   job的处理时间，初始值是ttr+now()，若是越大job处于ready状态的时间就越长，否则job的状态会转换成其他状态。
    uint32 reserve_ct;
    uint32 timeout_ct;
    uint32 release_ct;
    uint32 bury_ct;
    uint32 kick_ct;
    byte   state;//当前状态 job的状态
};

struct job {
    Jobrec r; // persistent fields; these get written to the wal   其实这个地方才是真正保存job的地方

    /* bookeeping fields; these are in-memory only */
    char pad[6];
    tube tube;//指向自己属于哪个tube
    job prev, next; /* linked list of jobs */
    job ht_next; /* Next job in a hash table list */
    size_t heap_index; /* where is this job in its current heap 自己在tube堆里面的位置 */
    File *file;
    job  fnext;
    job  fprev;
    void *reserver;//记录job当前消费者是谁，也就是谁reserver了这个job
    int walresv;
    int walused;

    char body[]; // written separately to the wal
};

struct tube {
    uint refs;//引用计数
    char name[MAX_TUBE_NAME_LEN];//名称
    Heap ready;//存储状态未ready的job，按照优先级排序
    Heap delay;//存储状态未delayed的job，按照到期时间排序
    struct ms waiting; /* set of conns *///等待当前tube有job产生的消费者的集合
    struct stats stat;//登记状态的结构体
    uint using_ct;//应该是当前tube上等待的客户端链接数 
    uint watching_ct;//watch的个数
    int64 pause;//执行pause命令后，pause字段记录暂停时间   暂停多长时间
    int64 deadline_at;//deadline_at记录tube暂停到达时间    暂停到啥时候 time()+pause
    struct job buried;//存储状态为buried的job，是一个链表  保留的任务: 任务不会被执行，也不会消失，除非有人将他修改为其他状态；
};


#define twarn(fmt, args...) warn("%s:%d in %s: " fmt, \
                                 __FILE__, __LINE__, __func__, ##args)
#define twarnx(fmt, args...) warnx("%s:%d in %s: " fmt, \
                                   __FILE__, __LINE__, __func__, ##args)

void warn(const char *fmt, ...) __attribute__((format(printf, 1, 2)));
void warnx(const char *fmt, ...) __attribute__((format(printf, 1, 2)));
char* fmtalloc(char *fmt, ...) __attribute__((format(printf, 1, 2)));
void* zalloc(int n);
#define new(T) zalloc(sizeof(T))
void optparse(Server*, char**);

extern const char *progname;

int64 nanoseconds(void);
int   rawfalloc(int fd, int len);


void ms_init(ms a, ms_event_fn oninsert, ms_event_fn onremove);
void ms_clear(ms a);
int ms_append(ms a, void *item);
int ms_remove(ms a, void *item);
int ms_contains(ms a, void *item);
void *ms_take(ms a);


#define make_job(pri,delay,ttr,body_size,tube) make_job_with_id(pri,delay,ttr,body_size,tube,0)//初始化job结构体，存储在hash表all_jobs中

job allocate_job(int body_size);
job make_job_with_id(uint pri, int64 delay, int64 ttr,
             int body_size, tube tube, uint64 id);
void job_free(job j);

/* Lookup a job by job ID */
job job_find(uint64 job_id);

/* the void* parameters are really job pointers */
void job_setheappos(void*, int);
int job_pri_less(void*, void*);
int job_delay_less(void*, void*);

job job_copy(job j);

const char * job_state(job j);

int job_list_any_p(job head);
job job_remove(job j);
void job_insert(job head, job j);

uint64 total_jobs(void);

/* for unit tests */
size_t get_all_jobs_used(void);


extern struct ms tubes;

tube make_tube(const char *name);
void tube_dref(tube t);
void tube_iref(tube t);
tube tube_find(const char *name);
tube tube_find_or_make(const char *name);
#define TUBE_ASSIGN(a,b) (tube_dref(a), (a) = (b), tube_iref(a))


Conn *make_conn(int fd, char start_state, tube use, tube watch);

int count_cur_conns(void);
uint count_tot_conns(void);
int count_cur_producers(void);
int count_cur_workers(void);


extern size_t primes[];


extern size_t job_data_size_limit;

void prot_init(void);
int64 prottick(Server *s);

Conn *remove_waiting_conn(Conn *c);

void enqueue_reserved_jobs(Conn *c);

void enter_drain_mode(int sig);
void h_accept(const int fd, const short which, Server* srv);
void prot_remove_tube(tube t);
int  prot_replay(Server *s, job list);


int make_server_socket(char *host_addr, char *port);


struct Conn {
    Server *srv;//执行服务器
    Socket sock;//客户端socket
    char   state;//STATE_WANTCOMMAND  STATE_WANTDATA ...等状态  状态机标记
    char   type;//CONN_TYPE_PRODUCER CONN_TYPE_WORKER 还是CONN_TYPE_WAITING
    Conn   *next;// 下一个Conn的指针
    tube   use;//指向当前使用的tube put命令发布的job会插入到当前tube中  一个客户肯定use一个tube 默认是default tube
    int64  tickat;      // time at which to do more work     //客户端处理job的TTR到期时间；或者客户端阻塞（等待job的时间）的到期时间；  两者取较早的哪个时间
    int    tickpos;     // position in srv->conns   // 在srv->conns堆里的位置  //c->tickpos记录当前客户端在srv->conns堆的索引；（思考：tickpos在什么时候赋值的？答：heap的函数指针rec）
    job    soonest_job; // memoization of the soonest job  记录了j->r.deadline_at最小的那个job  应该是ttr最早要过期的job
    int    rw;          // currently want: 'r', 'w', or 'h'
    int    pending_timeout; //客户端获取job而阻塞（阻塞不太恰当 应当理解为等待reday job的最长时间）的到期时间 默认为-1  reserve-with-timeout命令可以设置超时时间
    char   halfclosed;//表示客户端断开连接

    char cmd[LINE_BUF_SIZE]; // this string is NOT NUL-terminated
    int  cmd_len;//命令加+\r\n的长度
    int  cmd_read;//读取命令的状态下  已经读了多少位了   cmd+cmd_read  就是读取的起始位置

    char *reply;//输出数据缓冲区
    int  reply_len;//输出数据缓存区的长度
    int  reply_sent;//输出缓存区已经发给客户端数据的长度 如果reply_sent==reply_len那么说明发送完毕了
    char reply_buf[LINE_BUF_SIZE]; // this string IS NUL-terminated  一个输出缓存区  上面的reply指针可能指向reply_buf

    // How many bytes of in_job->body have been read so far. If in_job is NULL
    // while in_job_read is nonzero, we are in bit bucket mode and
    // in_job_read's meaning is inverted -- then it counts the bytes that
    // remain to be thrown away.
    int in_job_read;//put命令发布job时，从客户端已经读入的job body的字节数
    job in_job; // a job to be read from the client   正在读取进入的job缓存

    job out_job;// 待返回给客户端的job
    int out_job_sent;//已经发送的job字节数

    struct ms  watch;//当前客户端监听的所有tube集合
    struct job reserved_jobs; // linked list header 这个链表是当前消费者已经获取ready的job   也就是用户获取正在处理但是还没有删除的job
};
int  connless(Conn *a, Conn *b);
void connrec(Conn *c, int i);
void connwant(Conn *c, int rw);
void connsched(Conn *c);
void connclose(Conn *c);
void connsetproducer(Conn *c);
void connsetworker(Conn *c);
job  connsoonestjob(Conn *c);
int  conndeadlinesoon(Conn *c);
int conn_ready(Conn *c);
#define conn_waiting(c) ((c)->type & CONN_TYPE_WAITING) //客户端正在阻塞




enum
{
    Filesizedef = (10 << 20)
};

struct Wal {
    int    filesize;//每个binlog的大小
    int    use;//是否开启binlog
    char   *dir;
    File   *head;//binlog文件列表
    File   *cur;
    File   *tail;
    int    nfile;//binlog的个数
    int    next;//下一个可以用的日志文件
    int    resv;  // bytes reserved
    int    alive; // bytes in use
    int64  nmig;  // migrations
    int64  nrec;  // records written ever
    int    wantsync; //0 不调用fsync  1 调用fsync
    int64  syncrate;//刷硬盘的时间
    int64  lastsync;
    int    nocomp; // disable binlog compaction?  是否收缩binlog
};
int  waldirlock(Wal*);
void walinit(Wal*, job list);
int  walwrite(Wal*, job);
void walmaint(Wal*);
int  walresvput(Wal*, job);
int  walresvupdate(Wal*, job);
void walgc(Wal*);


struct File {
    File *next;
    uint refs;//文件的引用计数  如果引用计数为0 那么这个binlog将会被删除
    int  seq;//第n个binlog
    int  iswopen; // is open for writing
    int  fd;//当前binlog的fd
    int  free;
    int  resv;
    char *path;//当前binlog的磁盘位置
    Wal  *w;//指向 Wal  *

    struct job jlist; // jobs written in this file
};
int  fileinit(File*, Wal*, int);
Wal* fileadd(File*, Wal*);
void fileincref(File*);
void filedecref(File*);
void fileaddjob(File*, job);
void filermjob(File*, job);
int  fileread(File*, job list);
void filewopen(File*);
void filewclose(File*);
int  filewrjobshort(File*, job);
int  filewrjobfull(File*, job);


#define Portdef "11300"

struct Server {
    char *port;//端口
    char *addr;//地址
    char *user;

    Wal    wal;
    Socket sock;//监听的socket
    Heap   conns;//存储即将有事件发生的客户端；按照事件发生的时间排序的最小堆； //例如：当客户端获取job后，若超过TTR时间没处理完，job会状态应重置为ready状态； //当客户端调用reserve获取job但当前tube没有ready状态的job时，客户端等待timeout的时间；
};
void srvserve(Server *srv);
void srvaccept(Server *s, int ev);
