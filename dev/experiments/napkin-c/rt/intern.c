/* intern.c: internment::Intern<T>: a global set of leaked, deduplicated values per type.
 * Open addressing (linear probing) table of (hash, pointer), grown at 3/4 load. Interned values
 * live forever (like internment), so returned pointers can be compared by address. */
#include "rt_internal.h"

typedef struct ient { u64 hash; const void *p; } ient;
struct jz_interner { ient *tab; u32 mask; u32 len; };

static void pool_grow(jz_interner *in) {
    u32 ncap = in->tab ? (in->mask + 1) * 2 : 64;
    ient *nt = (ient *)jz_zalloc(sizeof(ient) * ncap);
    if (in->tab) {
        for (u32 i = 0; i <= in->mask; i++) {
            if (!in->tab[i].p) continue;
            u32 j = (u32)in->tab[i].hash & (ncap - 1);
            while (nt[j].p) j = (j + 1) & (ncap - 1);
            nt[j] = in->tab[i];
        }
        jz_free(in->tab);
    }
    in->tab = nt;
    in->mask = ncap - 1;
}

const void *jz_intern_ex(jz_interner **pool, const void *val, usize size, u64 (*hash)(const void *),
                         bool (*eq)(const void *, const void *), bool *inserted) {
    jz_interner *in = *pool;
    if (!in) {
        in = JZ_NEW(jz_interner);
        *pool = in;
    }
    if (!in->tab || (in->len + 1) * 4 > (in->mask + 1) * 3) pool_grow(in);
    u64 h = hash ? hash(val) : jz_hash_bytes(val, size);
    u32 i = (u32)h & in->mask;
    for (;;) {
        ient *e = &in->tab[i];
        if (!e->p) break;
        if (e->hash == h && (eq ? eq(e->p, val) : memcmp(e->p, val, size) == 0)) {
            if (inserted) *inserted = false;
            return e->p;
        }
        i = (i + 1) & in->mask;
    }
    void *copy = jz_memdup(val, size); /* Box::leak(Box::new(val)) */
    in->tab[i].hash = h;
    in->tab[i].p = copy;
    in->len++;
    if (inserted) *inserted = true;
    return copy;
}

const void *jz_intern(jz_interner **pool, const void *val, usize size, u64 (*hash)(const void *), bool (*eq)(const void *, const void *)) {
    return jz_intern_ex(pool, val, size, hash, eq, NULL);
}
