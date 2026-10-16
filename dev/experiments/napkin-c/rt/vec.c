/* vec.c: type-erased Vec<T> (alloc::vec). One copy for every element type. */
#include "rt_internal.h"

static inline u32 vec_min_cap(usize elem) { return elem == 1 ? 8 : elem <= 1024 ? 4 : 1; }

void jz_vec_reserve(void *vec, u32 extra, usize elem) {
    jz_vec *v = (jz_vec *)vec;
    u32 need = v->len + extra;
    if (need < v->len) jz_panic("capacity overflow");
    if (need <= v->cap) return;
    u32 cap = jz_grow_cap(v->cap, need, vec_min_cap(elem));
    u64 bytes = (u64)cap * elem;
    if (bytes > 0x7fffffffu) jz_panic("capacity overflow");
    v->ptr = jz_realloc(v->ptr, (usize)bytes);
    v->cap = cap;
}

void *jz_vec_push_slot(void *vec, usize elem) {
    jz_vec *v = (jz_vec *)vec;
    if (v->len == v->cap) jz_vec_reserve(v, 1, elem);
    return (u8 *)v->ptr + (usize)v->len++ * elem;
}

void jz_vec_push(void *vec, const void *item, usize elem) { memcpy(jz_vec_push_slot(vec, elem), item, elem); }

void jz_vec_insert(void *vec, u32 idx, const void *item, usize elem) {
    jz_vec *v = (jz_vec *)vec;
    if (idx > v->len) jz_panic("insertion index (is idx) should be <= len (is len)");
    if (v->len == v->cap) jz_vec_reserve(v, 1, elem);
    u8 *p = (u8 *)v->ptr + (usize)idx * elem;
    memmove(p + elem, p, (usize)(v->len - idx) * elem);
    memcpy(p, item, elem);
    v->len++;
}

void jz_vec_remove(void *vec, u32 idx, void *out, usize elem) {
    jz_vec *v = (jz_vec *)vec;
    if (idx >= v->len) jz_panic("removal index (is idx) should be < len (is len)");
    u8 *p = (u8 *)v->ptr + (usize)idx * elem;
    if (out) memcpy(out, p, elem);
    memmove(p, p + elem, (usize)(v->len - idx - 1) * elem);
    v->len--;
}

void jz_vec_swap_remove(void *vec, u32 idx, void *out, usize elem) {
    jz_vec *v = (jz_vec *)vec;
    if (idx >= v->len) jz_panic("swap_remove index (is idx) should be < len (is len)");
    u8 *p = (u8 *)v->ptr + (usize)idx * elem;
    if (out) memcpy(out, p, elem);
    v->len--;
    if (idx != v->len) memcpy(p, (u8 *)v->ptr + (usize)v->len * elem, elem);
}

void jz_vec_truncate(void *vec, u32 len, usize elem, void (*drop)(void *)) {
    jz_vec *v = (jz_vec *)vec;
    if (len >= v->len) return;
    u32 old = v->len;
    v->len = len;
    if (drop)
        for (u32 i = len; i < old; i++) drop((u8 *)v->ptr + (usize)i * elem);
}

void jz_vec_drop(void *vec, usize elem, void (*drop)(void *)) {
    jz_vec *v = (jz_vec *)vec;
    if (drop)
        for (u32 i = 0; i < v->len; i++) drop((u8 *)v->ptr + (usize)i * elem);
    if (v->cap) jz_free(v->ptr);
    v->ptr = NULL;
    v->len = v->cap = 0;
}

void jz_vec_clone(void *dst, const void *src, usize elem, void (*clone)(void *dst, const void *src)) {
    jz_vec *d = (jz_vec *)dst;
    const jz_vec *s = (const jz_vec *)src;
    d->ptr = NULL;
    d->len = d->cap = 0;
    if (!s->len) return;
    jz_vec_reserve(d, s->len, elem);
    if (clone) {
        for (u32 i = 0; i < s->len; i++) {
            clone((u8 *)d->ptr + (usize)i * elem, (const u8 *)s->ptr + (usize)i * elem);
            d->len = i + 1;
        }
    } else {
        memcpy(d->ptr, s->ptr, (usize)s->len * elem);
        d->len = s->len;
    }
}

void jz_vec_extend(void *vec, const void *items, u32 n, usize elem) {
    jz_vec *v = (jz_vec *)vec;
    if (!n) return;
    jz_vec_reserve(v, n, elem);
    memcpy((u8 *)v->ptr + (usize)v->len * elem, items, (usize)n * elem);
    v->len += n;
}

void jz_vec_dedup(void *vec, usize elem, int (*cmp)(const void *, const void *), void (*drop)(void *)) {
    jz_vec *v = (jz_vec *)vec;
    if (v->len < 2) return;
    u8 *base = (u8 *)v->ptr;
    u32 w = 1;
    for (u32 r = 1; r < v->len; r++) {
        u8 *cur = base + (usize)r * elem;
        u8 *prev = base + (usize)(w - 1) * elem;
        if (cmp(prev, cur) == 0) {
            if (drop) drop(cur);
        } else {
            if (w != r) memcpy(base + (usize)w * elem, cur, elem);
            w++;
        }
    }
    v->len = w;
}

void jz_vec_retain(void *vec, usize elem, bool (*keep)(void *item, void *ctx), void *ctx, void (*drop)(void *)) {
    jz_vec *v = (jz_vec *)vec;
    u8 *base = (u8 *)v->ptr;
    u32 w = 0;
    for (u32 r = 0; r < v->len; r++) {
        u8 *cur = base + (usize)r * elem;
        if (keep(cur, ctx)) {
            if (w != r) memcpy(base + (usize)w * elem, cur, elem);
            w++;
        } else if (drop) {
            drop(cur);
        }
    }
    v->len = w;
}
