/* rc.c: Rc<T>/Arc<T> (alloc::rc). A header with strong/weak counts and the payload's drop fn sits
 * in front of the payload; the implicit weak reference held by the strong refs follows std. */
#include "rt_internal.h"

#define RC_HDR_SIZE ((sizeof(jz_rc_hdr) + 7) & ~(usize)7)
static inline jz_rc_hdr *hdr(const void *p) { return (jz_rc_hdr *)((u8 *)p - RC_HDR_SIZE); }

void *jz_rc_new(usize size, void (*drop)(void *)) {
    jz_rc_hdr *h = (jz_rc_hdr *)jz_zalloc(RC_HDR_SIZE + size);
    h->strong = 1;
    h->weak = 1;
    h->drop = drop;
    return (u8 *)h + RC_HDR_SIZE;
}

void *jz_rc_clone(void *p) {
    if (!p) return NULL;
    jz_rc_hdr *h = hdr(p);
    if (++h->strong == 0) __builtin_trap(); /* refcount overflow aborts */
    return p;
}

void jz_rc_release(void *p) {
    if (!p) return;
    jz_rc_hdr *h = hdr(p);
    if (--h->strong != 0) return;
    if (h->drop) h->drop(p);
    if (--h->weak == 0) jz_free(h);
}

u32 jz_rc_strong(const void *p) { return p ? hdr(p)->strong : 0; }

void *jz_rc_make_mut(void **pp, usize size, void (*clone)(void *dst, const void *src)) {
    void *p = *pp;
    jz_rc_hdr *h = hdr(p);
    if (h->strong == 1 && h->weak == 1) return p; /* unique: mutate in place */
    void *n = jz_rc_new(size, h->drop);
    if (clone) clone(n, p);
    else memcpy(n, p, size);
    jz_rc_release(p);
    *pp = n;
    return n;
}
