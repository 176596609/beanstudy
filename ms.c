#include <stdint.h>
#include <string.h>
#include <stdlib.h>
#include "dat.h"

//这个集合虽然用的是数组，但是有个特点，删除的时候依然比较高效，不会导致整个内存移动，当然查找元素的时候还是比较低效O(N)

void
ms_init(ms a, ms_event_fn oninsert, ms_event_fn onremove)//初始化一个集合
{
    a->used = a->cap = a->last = 0;
    a->items = NULL;
    a->oninsert = oninsert;
    a->onremove = onremove;
}

static void
grow(ms a)//集合扩容到原来的2倍
{
    void **nitems;
    size_t ncap = (a->cap << 1) ? : 1;//注意这个三目运算符的小语法糖

    nitems = malloc(ncap * sizeof(void *));
    if (!nitems) return;

    memcpy(nitems, a->items, a->used * sizeof(void *));
    free(a->items);
    a->items = nitems;
    a->cap = ncap;
}

int
ms_append(ms a, void *item)//集合增加一个元素
{
    if (a->used >= a->cap) grow(a);
    if (a->used >= a->cap) return 0;

    a->items[a->used++] = item;
    if (a->oninsert) a->oninsert(a, item, a->used - 1);
    return 1;
}

static int
ms_delete(ms a, size_t i)//集合删除一个元素
{
    void *item;

    if (i >= a->used) return 0;
    item = a->items[i];//取第i个元素
    a->items[i] = a->items[--a->used];//用最后一个元素 覆盖第i个元素  也就是删除第i个元素  集合总数减1  这是个小技巧，避免数组的整个内存移动。也符合集合的概念

    /* it has already been removed now */
    if (a->onremove) a->onremove(a, item, i);
    return 1;
}

void
ms_clear(ms a)//清空整个集合  释放内存
{
    while (ms_delete(a, 0));
    free(a->items);
    ms_init(a, a->oninsert, a->onremove);
}

int
ms_remove(ms a, void *item)//删除集合中某个指定的元素
{
    size_t i;

    for (i = 0; i < a->used; i++) {
        if (a->items[i] == item) return ms_delete(a, i);
    }
    return 0;
}

int
ms_contains(ms a, void *item)//检查集合中是否有某个特定元素
{
    size_t i;

    for (i = 0; i < a->used; i++) {
        if (a->items[i] == item) return 1;
    }
    return 0;
}

void *
ms_take(ms a)  //ms_take：将客户端从此job所属tube的waiting集合中删除；并返回客户端conn
{
    void *item;

    if (!a->used) return NULL;

    a->last = a->last % a->used;
    item = a->items[a->last];
    ms_delete(a, a->last);
    ++a->last;
    return item;
}
