#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include "dat.h"

static uint64 next_id = 1;

static int cur_prime = 0;

static job all_jobs_init[12289] = {0};//job哈希表  为的是较快的找到一个id job，O（1）时间范围内找到job 因为装载因子是4 所以平均查找2次
static job *all_jobs = all_jobs_init;
static size_t all_jobs_cap = 12289; /* == primes[0] */
static size_t all_jobs_used = 0;

static int hash_table_was_oom = 0;

static void rehash();

static int
_get_job_hash_index(uint64 job_id)
{
    return job_id % all_jobs_cap;
}

static void
store_job(job j)//将job放到一个装载因子为4的哈希表中
{
    int index = 0;

    index = _get_job_hash_index(j->r.id);

    j->ht_next = all_jobs[index];
    all_jobs[index] = j;
    all_jobs_used++;

    /* accept a load factor of 4 */
    if (all_jobs_used > (all_jobs_cap << 2)) rehash();
}

static void
rehash()//把旧的哈希表里面的数据捣腾到新的哈希表里面
{
    job *old = all_jobs;
    size_t old_cap = all_jobs_cap, old_used = all_jobs_used, i;

    if (cur_prime >= NUM_PRIMES) return;
    if (hash_table_was_oom) return;//哈希表已经out of memory

    all_jobs_cap = primes[++cur_prime];//获取新的大小 约等于*2
    all_jobs = calloc(all_jobs_cap, sizeof(job));
    if (!all_jobs) {
        twarnx("Failed to allocate %zu new hash buckets", all_jobs_cap);
        hash_table_was_oom = 1;
        --cur_prime;
        all_jobs = old;
        all_jobs_cap = old_cap;
        all_jobs_used = old_used;
        return;
    }
    all_jobs_used = 0;

    for (i = 0; i < old_cap; i++) {
        while (old[i]) {
            job j = old[i];
            old[i] = j->ht_next;
            j->ht_next = NULL;
            store_job(j);
        }
    }
    if (old != all_jobs_init) {
        free(old);
    }
}

job
job_find(uint64 job_id)//哈希表里面查找job
{
    job jh = NULL;
    int index = _get_job_hash_index(job_id);

    for (jh = all_jobs[index]; jh && jh->r.id != job_id; jh = jh->ht_next);

    return jh;
}

job
allocate_job(int body_size)//生成一个job  job的大小是  sizeof(struct job) + body_size
{
    job j;

    j = malloc(sizeof(struct job) + body_size);//生成job空间
    if (!j) return twarnx("OOM"), (job) 0;

    memset(j, 0, sizeof(struct job));//初始化为0
    j->r.created_at = nanoseconds();
    j->r.body_size = body_size;
    j->next = j->prev = j; /* not in a linked list */
    return j;
}

job
make_job_with_id(uint pri, int64 delay, int64 ttr,
                 int body_size, tube tube, uint64 id)
{
    job j;

    j = allocate_job(body_size);//生成一个适合大小的job
    if (!j) return twarnx("OOM"), (job) 0;//out of memory

    if (id) {//填充id
        j->r.id = id;
        if (id >= next_id) next_id = id + 1;
    } else {
        j->r.id = next_id++;
    }
    j->r.pri = pri;
    j->r.delay = delay;
    j->r.ttr = ttr;

    store_job(j);//job放在一个hash表中 这个哈希表是全局的 根据job id进行哈希

    TUBE_ASSIGN(j->tube, tube);

    return j;
}

static void
job_hash_free(job j)//哈希表中去除一个job
{
    job *slot;

    slot = &all_jobs[_get_job_hash_index(j->r.id)];
    while (*slot && *slot != j) slot = &(*slot)->ht_next;
    if (*slot) {
        *slot = (*slot)->ht_next;
        --all_jobs_used;
    }
}

void
job_free(job j)//销毁一个 job  1,哈希表中去除该job。 2释放改job的内存
{
    if (j) {
        TUBE_ASSIGN(j->tube, NULL);
        if (j->r.state != Copy) job_hash_free(j);
    }

    free(j);
}

void
job_setheappos(void *j, int pos)//设置jon在对应堆中的位置 快速获取
{
    ((job)j)->heap_index = pos;
}

int
job_pri_less(void *ax, void *bx)//job 在优先队列堆中的排序函数 根据优先级进行排序   优先级无法排序的情况下就使用id进行排序
{
    job a = ax, b = bx;
    if (a->r.pri < b->r.pri) return 1;
    if (a->r.pri > b->r.pri) return 0;
    return a->r.id < b->r.id;
}

int
job_delay_less(void *ax, void *bx)//job 在优先队列堆中的排序函数 根据销毁时间进行排序 销毁时间无法排序就根据id进行排序
{
    job a = ax, b = bx;
    if (a->r.deadline_at < b->r.deadline_at) return 1;
    if (a->r.deadline_at > b->r.deadline_at) return 0;
    return a->r.id < b->r.id;
}

job
job_copy(job j)//拷贝一个job  但是去除一些不能拷贝的属性
{
    job n;

    if (!j) return NULL;

    n = malloc(sizeof(struct job) + j->r.body_size);
    if (!n) return twarnx("OOM"), (job) 0;

    memcpy(n, j, sizeof(struct job) + j->r.body_size);
    n->next = n->prev = n; /* not in a linked list */

    n->file = NULL; /* copies do not have refcnt on the wal */

    n->tube = 0; /* Don't use memcpy for the tube, which we must refcount.    注意引用计数 所以要置为空格 然后用下方的宏来赋值*/
    TUBE_ASSIGN(n->tube, j->tube);

    /* Mark this job as a copy so it can be appropriately freed later on */
    n->r.state = Copy;

    return n;
}

const char *
job_state(job j)//获取job当前的状态
{
    if (j->r.state == Ready) return "ready";
    if (j->r.state == Reserved) return "reserved";
    if (j->r.state == Buried) return "buried";
    if (j->r.state == Delayed) return "delayed";
    return "invalid";
}

int
job_list_any_p(job head)//检查是否是在一个链表里面
{
    return head->next != head || head->prev != head;
}

job
job_remove(job j)//将一个job从单亲链表里面解除出来  比如从reverse 链表里面拿出来
{
    if (!j) return NULL;
    if (!job_list_any_p(j)) return NULL; /* not in a doubly-linked list */

    j->next->prev = j->prev;
    j->prev->next = j->next;

    j->prev = j->next = j;

    return j;
}

void
job_insert(job head, job j)//job放入 head所在的列表中
{
    if (job_list_any_p(j)) return; /* already in a linked list */

    j->prev = head->prev;
    j->next = head;
    head->prev->next = j;
    head->prev = j;
}

uint64
total_jobs()//job的个数
{
    return next_id - 1;
}

/* for unit tests */
size_t
get_all_jobs_used()
{
    return all_jobs_used;
}
