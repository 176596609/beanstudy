#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include "dat.h"
//�ܽᣬbeanstalkd��ʵ���Ǹ��ö�ʵ�ֵ����ȶ��У�������ÿ��Ԫ�ؼ�¼���Լ��ڶ������λ�ã��ﵽO(1)�ĸ�Ч���ʣ��ѵ���չ��*2���У���Ӧ���������

static void
set(Heap *h, int k, void *x)//���öѵĵ�K��λ��Ϊx
{
    h->data[k] = x;
    h->rec(x, k);
}


static void
swap(Heap *h, int a, int b)//��������������A��B�����ֵ
{
    void *tmp;

    tmp = h->data[a];
    set(h, a, h->data[b]);
    set(h, b, tmp);
}


static int
less(Heap *h, int a, int b)//�Ա�����a��b�����ֵ ����
{
    return h->less(h->data[a], h->data[b]);
}


static void
siftdown(Heap *h, int k)//�����ĸ��������γɴ󶥶�  k�����ϵ����ݺ��丸�ڵ�p�����ݽ��жԱ� ������ڵ������С���ӽڵ��������ô�ͽ����������ڵ������ k����ֵ��Ϊp���ϵݹ�
{
    for (;;) {
        int p = (k-1) / 2; /* parent */

        if (k == 0 || less(h, p, k)) {
            return;
        }

        swap(h, k, p);
        k = p;
    }
}


static void
siftup(Heap *h, int k)//�������·������γɴ󶥶�
{
    for (;;) {
        int l, r, s;

        l = k*2 + 1; /* left child ����������*/
        r = k*2 + 2; /* right child ����������*/

        /* find the smallest of the three */
        s = k;
        if (l < h->len && less(h, l, s)) s = l;//s����������С
        if (r < h->len && less(h, r, s)) s = r;//��������С
        //�������� s������� ��С�ڵ������
        if (s == k) {//s==k˵���Ѿ��Ǹ��󶥶�
            return; /* satisfies the heap property */
        }

        swap(h, k, s);//�����ڵ�λ��
        k = s;
    }
}


// Heapinsert inserts x into heap h according to h->less.
// It returns 1 on success, otherwise 0.
int
heapinsert(Heap *h, void *x)//����һ��Ԫ��
{
    int k;

    if (h->len == h->cap) {//���������������Ѿ��ﵽ�ѳ��صļ��� ��ô������2��
        void **ndata;
        int ncap = (h->len+1) * 2; /* allocate twice what we need */

        ndata = malloc(sizeof(void*) * ncap);
        if (!ndata) {
            return 0;
        }

        memcpy(ndata, h->data, sizeof(void*)*h->len);
        free(h->data);
        h->data = ndata;
        h->cap = ncap;
    }

    k = h->len;
    h->len++;
    set(h, k, x);//��ʵ�ǲ������һλ
    siftdown(h, k);//���ϵ����Ѵﵽƽ��
    return 1;
}


void *
heapremove(Heap *h, int k)//ɾ��һ��Ԫ��
{
    void *x;

    if (k >= h->len) {
        return 0;
    }

    x = h->data[k];//ȥ����k��Ԫ��
    h->len--;//��С�ѵĳ��� ��ʵcapû��
    set(h, k, h->data[h->len]);//�����һλ�ƶ�����һλ
    siftdown(h, k);//�����ĸ��������γɴ󶥶�
    siftup(h, k);//�������²������γɴ󶥶�
    h->rec(x, -1);
    return x;
}
