/* deque.c: VecDeque<T> ring buffer (alloc::collections::vec_deque), type-erased. */
#include "rt_internal.h"

#define QAT(q, i) ((q)->buf + (usize)(i) * (q)->elem)

void jz_deque_init(jz_deque *q, u32 elem) {
    q->buf = NULL;
    q->head = q->len = q->cap = 0;
    q->elem = elem;
}

static inline u32 wrap(const jz_deque *q, u32 i) { return i >= q->cap ? i - q->cap : i; }

/* grow, then make the contents contiguous again like VecDeque::handle_capacity_increase */
static void grow(jz_deque *q) {
    u32 old = q->cap;
    u32 cap = jz_grow_cap(old, old + 1, q->elem == 1 ? 8 : 4);
    q->buf = (u8 *)jz_realloc(q->buf, (usize)cap * q->elem);
    q->cap = cap;
    if (q->head <= old - q->len) return; /* was contiguous */
    u32 head_len = old - q->head;
    u32 tail_len = q->len - head_len;
    if (tail_len < head_len && tail_len <= cap - old) {
        /* copy the wrapped tail right after the old end */
        memcpy(QAT(q, old), q->buf, (usize)tail_len * q->elem);
    } else {
        /* move the head segment to the end of the new buffer */
        u32 nh = cap - head_len;
        memmove(QAT(q, nh), QAT(q, q->head), (usize)head_len * q->elem);
        q->head = nh;
    }
}

void jz_deque_push_back(jz_deque *q, const void *item) {
    if (q->len == q->cap) grow(q);
    memcpy(QAT(q, wrap(q, q->head + q->len)), item, q->elem);
    q->len++;
}

void jz_deque_push_front(jz_deque *q, const void *item) {
    if (q->len == q->cap) grow(q);
    q->head = q->head == 0 ? q->cap - 1 : q->head - 1;
    memcpy(QAT(q, q->head), item, q->elem);
    q->len++;
}

bool jz_deque_pop_front(jz_deque *q, void *out) {
    if (!q->len) return false;
    if (out) memcpy(out, QAT(q, q->head), q->elem);
    q->head = wrap(q, q->head + 1);
    q->len--;
    return true;
}

bool jz_deque_pop_back(jz_deque *q, void *out) {
    if (!q->len) return false;
    q->len--;
    if (out) memcpy(out, QAT(q, wrap(q, q->head + q->len)), q->elem);
    return true;
}

void *jz_deque_at(const jz_deque *q, u32 i) {
    if (i >= q->len) return NULL;
    return QAT(q, wrap(q, q->head + i));
}

void jz_deque_drop(jz_deque *q, void (*drop)(void *)) {
    if (drop)
        for (u32 i = 0; i < q->len; i++) drop(QAT(q, wrap(q, q->head + i)));
    jz_free(q->buf);
    q->buf = NULL;
    q->head = q->len = q->cap = 0;
}
