#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include "dat.h"

struct ms tubes;//tubes 是个ms 实际上就是个列表

tube
make_tube(const char *name)//生成一个名字叫做那么的tube
{
    tube t;

    t = new(struct tube);
    if (!t) return NULL;

    t->name[MAX_TUBE_NAME_LEN - 1] = '\0';
    strncpy(t->name, name, MAX_TUBE_NAME_LEN - 1);
    if (t->name[MAX_TUBE_NAME_LEN - 1] != '\0') twarnx("truncating tube name");

    t->ready.less = job_pri_less;// 入堆时需要用到less函数，来和堆中的父节点进行值的比较
    t->delay.less = job_delay_less;// 入堆时需要用到less函数，来和堆中的父节点进行值的比较
    t->ready.rec = job_setheappos;// 设置堆数组中的index的值
    t->delay.rec = job_setheappos;// 设置堆数组中的index的值
    t->buried = (struct job) { };// 初始化buried链表
    t->buried.prev = t->buried.next = &t->buried;// 初始化buried链表
    ms_init(&t->waiting, NULL, NULL);

    return t;
}

static void
tube_free(tube t)
{
    prot_remove_tube(t);//从全局变量集合中删除这个tube
    free(t->ready.data);//释放ready的空间
    free(t->delay.data);//释放delay的空间
    ms_clear(&t->waiting);//清除当前tube上消费者集合
    free(t);//释放当前tube占用内存
}

void
tube_dref(tube t)//减小tube的引用计数
{
    if (!t) return;
    if (t->refs < 1) return twarnx("refs is zero for tube: %s", t->name);

    --t->refs;
    if (t->refs < 1) tube_free(t);
}

void
tube_iref(tube t)//增加tube的引用计数
{
    if (!t) return;
    ++t->refs;
}

static tube
make_and_insert_tube(const char *name)//往全局tubes集合中增加一个名字叫做name的tube
{
    int r;
    tube t = NULL;

    t = make_tube(name);
    if (!t) return NULL;

    /* We want this global tube list to behave like "weak" refs, so don't
     * increment the ref count. */
    r = ms_append(&tubes, t);
    if (!r) return tube_dref(t), (tube) 0;

    return t;
}

tube
tube_find(const char *name)//全局tubes集合里面查找叫做name的tube
{
    tube t;
    size_t i;

    for (i = 0; i < tubes.used; i++) {
        t = tubes.items[i];
        if (strncmp(t->name, name, MAX_TUBE_NAME_LEN) == 0) return t;
    }
    return NULL;
}

tube
tube_find_or_make(const char *name)//全局tubes里面查找name的tube，找不到就创建一个放到里面
{
    return tube_find(name) ? : make_and_insert_tube(name);
}

