#include <stdint.h>
#include <string.h>
#include <stdlib.h>
#include "dat.h"

//���������Ȼ�õ������飬�����и��ص㣬ɾ����ʱ����Ȼ�Ƚϸ�Ч�����ᵼ�������ڴ��ƶ�����Ȼ����Ԫ�ص�ʱ���ǱȽϵ�ЧO(N)

void
ms_init(ms a, ms_event_fn oninsert, ms_event_fn onremove)//��ʼ��һ������
{
    a->used = a->cap = a->last = 0;
    a->items = NULL;
    a->oninsert = oninsert;
    a->onremove = onremove;
}

static void
grow(ms a)//�������ݵ�ԭ����2��
{
    void **nitems;
    size_t ncap = (a->cap << 1) ? : 1;//ע�������Ŀ�������С�﷨��

    nitems = malloc(ncap * sizeof(void *));
    if (!nitems) return;

    memcpy(nitems, a->items, a->used * sizeof(void *));
    free(a->items);
    a->items = nitems;
    a->cap = ncap;
}

int
ms_append(ms a, void *item)//��������һ��Ԫ��
{
    if (a->used >= a->cap) grow(a);
    if (a->used >= a->cap) return 0;

    a->items[a->used++] = item;
    if (a->oninsert) a->oninsert(a, item, a->used - 1);
    return 1;
}

static int
ms_delete(ms a, size_t i)//����ɾ��һ��Ԫ��
{
    void *item;

    if (i >= a->used) return 0;
    item = a->items[i];//ȡ��i��Ԫ��
    a->items[i] = a->items[--a->used];//�����һ��Ԫ�� ���ǵ�i��Ԫ��  Ҳ����ɾ����i��Ԫ��  ����������1  ���Ǹ�С���ɣ���������������ڴ��ƶ���Ҳ���ϼ��ϵĸ���

    /* it has already been removed now */
    if (a->onremove) a->onremove(a, item, i);
    return 1;
}

void
ms_clear(ms a)//�����������  �ͷ��ڴ�
{
    while (ms_delete(a, 0));
    free(a->items);
    ms_init(a, a->oninsert, a->onremove);
}

int
ms_remove(ms a, void *item)//ɾ��������ĳ��ָ����Ԫ��
{
    size_t i;

    for (i = 0; i < a->used; i++) {
        if (a->items[i] == item) return ms_delete(a, i);
    }
    return 0;
}

int
ms_contains(ms a, void *item)//��鼯�����Ƿ���ĳ���ض�Ԫ��
{
    size_t i;

    for (i = 0; i < a->used; i++) {
        if (a->items[i] == item) return 1;
    }
    return 0;
}

void *
ms_take(ms a)  //ms_take�����ͻ��˴Ӵ�job����tube��waiting������ɾ���������ؿͻ���conn
{
    void *item;

    if (!a->used) return NULL;

    a->last = a->last % a->used;
    item = a->items[a->last];
    ms_delete(a, a->last);
    ++a->last;
    return item;
}
