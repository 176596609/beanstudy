#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include "dat.h"

struct ms tubes;//tubes �Ǹ�ms ʵ���Ͼ��Ǹ��б�

tube
make_tube(const char *name)//����һ�����ֽ�����ô��tube
{
    tube t;

    t = new(struct tube);
    if (!t) return NULL;

    t->name[MAX_TUBE_NAME_LEN - 1] = '\0';
    strncpy(t->name, name, MAX_TUBE_NAME_LEN - 1);
    if (t->name[MAX_TUBE_NAME_LEN - 1] != '\0') twarnx("truncating tube name");

    t->ready.less = job_pri_less;// ���ʱ��Ҫ�õ�less���������Ͷ��еĸ��ڵ����ֵ�ıȽ�
    t->delay.less = job_delay_less;// ���ʱ��Ҫ�õ�less���������Ͷ��еĸ��ڵ����ֵ�ıȽ�
    t->ready.rec = job_setheappos;// ���ö������е�index��ֵ
    t->delay.rec = job_setheappos;// ���ö������е�index��ֵ
    t->buried = (struct job) { };// ��ʼ��buried����
    t->buried.prev = t->buried.next = &t->buried;// ��ʼ��buried����
    ms_init(&t->waiting, NULL, NULL);

    return t;
}

static void
tube_free(tube t)
{
    prot_remove_tube(t);//��ȫ�ֱ���������ɾ�����tube
    free(t->ready.data);//�ͷ�ready�Ŀռ�
    free(t->delay.data);//�ͷ�delay�Ŀռ�
    ms_clear(&t->waiting);//�����ǰtube�������߼���
    free(t);//�ͷŵ�ǰtubeռ���ڴ�
}

void
tube_dref(tube t)//��Сtube�����ü���
{
    if (!t) return;
    if (t->refs < 1) return twarnx("refs is zero for tube: %s", t->name);

    --t->refs;
    if (t->refs < 1) tube_free(t);
}

void
tube_iref(tube t)//����tube�����ü���
{
    if (!t) return;
    ++t->refs;
}

static tube
make_and_insert_tube(const char *name)//��ȫ��tubes����������һ�����ֽ���name��tube
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
tube_find(const char *name)//ȫ��tubes����������ҽ���name��tube
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
tube_find_or_make(const char *name)//ȫ��tubes�������name��tube���Ҳ����ʹ���һ���ŵ�����
{
    return tube_find(name) ? : make_and_insert_tube(name);
}

