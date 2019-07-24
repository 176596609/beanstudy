#include <stdint.h>
#include <stdlib.h>
#include <string.h>
#include "dat.h"
//总结，beanstalkd其实就是个用堆实现的优先队列，亮点是每个元素记录了自己在堆里面的位置，达到O(1)的高效访问，堆的扩展以*2进行，适应任务的增加

static void
set(Heap *h, int k, void *x)//设置堆的第K个位置为x
{
    h->data[k] = x;
    h->rec(x, k);
}


static void
swap(Heap *h, int a, int b)//交换堆里面索引A和B里面的值
{
    void *tmp;

    tmp = h->data[a];
    set(h, a, h->data[b]);
    set(h, b, tmp);
}


static int
less(Heap *h, int a, int b)//对比索引a和b里面的值 函数
{
    return h->less(h->data[a], h->data[b]);
}


static void
siftdown(Heap *h, int k)//向树的根部调整形成大顶堆  k索引上的数据和其父节点p的数据进行对比 如果父节点的数据小于子节点的数据那么就交换这两个节点的数据 k索引值变为p向上递归
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
siftup(Heap *h, int k)//向树的下方调整形成大顶堆
{
    for (;;) {
        int l, r, s;

        l = k*2 + 1; /* left child 左子树索引*/
        r = k*2 + 2; /* right child 右子树索引*/

        /* find the smallest of the three */
        s = k;
        if (l < h->len && less(h, l, s)) s = l;//s比左子树还小
        if (r < h->len && less(h, r, s)) s = r;//右子树更小
        //到这里了 s里面存了 最小节点的索引
        if (s == k) {//s==k说明已经是个大顶堆
            return; /* satisfies the heap property */
        }

        swap(h, k, s);//调整节点位置
        k = s;
    }
}


// Heapinsert inserts x into heap h according to h->less.
// It returns 1 on success, otherwise 0.
int
heapinsert(Heap *h, void *x)//插入一个元素
{
    int k;

    if (h->len == h->cap) {//如果堆里面的数据已经达到堆承载的极限 那么堆扩容2倍
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
    set(h, k, x);//其实是插在最后一位
    siftdown(h, k);//向上调整堆达到平衡
    return 1;
}


void *
heapremove(Heap *h, int k)//删除一个元素
{
    void *x;

    if (k >= h->len) {
        return 0;
    }

    x = h->data[k];//去除第k个元素
    h->len--;//缩小堆的长度 其实cap没变
    set(h, k, h->data[h->len]);//将最后一位移动到第一位
    siftdown(h, k);//向树的根部调整形成大顶堆
    siftup(h, k);//向树的下部调整形成大顶堆
    h->rec(x, -1);
    return x;
}
