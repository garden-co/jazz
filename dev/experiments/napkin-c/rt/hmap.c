/* hmap.c: HashMap / HashSet (hashbrown's SwissTable, portable "generic" group implementation).
 *
 * Layout: `ctrl` holds one control byte per bucket plus GROUP mirrored trailing bytes;
 * `slots` holds the (key, value) entries, each rounded to 8 bytes. Control bytes:
 * EMPTY = 0xFF, DELETED = 0x80, FULL = top 7 bits of the hash (h2). Probing is triangular over
 * groups of 8 control bytes matched with SWAR bit tricks, exactly like hashbrown without SSE2.
 * Load factor 7/8; tombstones are reused and cleared by an in-place rehash on growth pressure.
 * The hash is foldhash-like (folded 64x64->128 multiply). */
#include "rt_internal.h"

#define GROUP 8
#define EMPTY 0xFF
#define DELETED 0x80
#define LSB 0x0101010101010101ull
#define MSB 0x8080808080808080ull

static const u8 EMPTY_GROUP[GROUP] = {EMPTY, EMPTY, EMPTY, EMPTY, EMPTY, EMPTY, EMPTY, EMPTY};

/* ---------- hashing (foldhash-style) ---------- */
#define FH_SEED 0x243f6a8885a308d3ull
#define FH_K1 0x13198a2e03707344ull
#define FH_K2 0xa4093822299f31d0ull
#define FH_K3 0x082efa98ec4e6c89ull

static inline u64 folded_multiply(u64 a, u64 b) {
    unsigned __int128 r = (unsigned __int128)a * b;
    return (u64)r ^ (u64)(r >> 64);
}

static inline u64 rd64(const u8 *p) { u64 v; memcpy(&v, p, 8); return v; }
static inline u64 rd32(const u8 *p) { u32 v; memcpy(&v, p, 4); return v; }

u64 jz_hash_bytes(const void *vp, usize n) {
    const u8 *p = (const u8 *)vp;
    u64 s0 = FH_SEED ^ (u64)n, s1 = FH_K1;
    if (n <= 16) {
        u64 a = 0, b = 0;
        if (n >= 8) {
            a = rd64(p);
            b = rd64(p + n - 8);
        } else if (n >= 4) {
            a = rd32(p);
            b = rd32(p + n - 4);
        } else if (n > 0) {
            a = p[0];
            b = ((u64)p[n / 2] << 8) | p[n - 1];
        }
        return folded_multiply(a ^ s0, b ^ FH_K2);
    }
    while (n > 16) {
        s0 = folded_multiply(rd64(p) ^ s0, rd64(p + 8) ^ s1);
        s1 = (s1 << 23 | s1 >> 41) + FH_K3;
        p += 16;
        n -= 16;
    }
    /* last 16 bytes (may overlap) */
    s0 = folded_multiply(rd64(p + n - 16) ^ s0, rd64(p + n - 8) ^ FH_K2);
    return folded_multiply(s0, s1 ^ FH_K3);
}

u64 jz_hash_u64(u64 v) { return folded_multiply(v ^ FH_SEED, FH_K1); }

u64 jz_hash_combine(u64 h, u64 v) { return folded_multiply(h ^ v, FH_K2); }

/* ---------- group bit tricks ---------- */
static inline u64 grp_load(const u8 *p) { return rd64(p); }
static inline u64 grp_match_byte(u64 g, u8 b) {
    u64 x = g ^ (LSB * b);
    return (x - LSB) & ~x & MSB;
}
static inline u64 grp_match_empty(u64 g) { return g & (g << 1) & MSB; }
static inline u64 grp_match_empty_or_deleted(u64 g) { return g & MSB; }
static inline u32 bm_lowest(u64 m) { return (u32)__builtin_ctzll(m) / 8; }
static inline u32 bm_tz(u64 m) { return m ? (u32)__builtin_ctzll(m) / 8 : GROUP; }
static inline u32 bm_lz(u64 m) { return m ? (u32)__builtin_clzll(m) / 8 : GROUP; }

static inline u8 h2(u64 h) { return (u8)(h >> 57); }

typedef struct hl { usize ks, stride; } hl;
static inline hl mk_hl(const jz_hmap_desc *d) {
    hl l;
    l.ks = ((usize)d->key_size + 7) & ~(usize)7;
    l.stride = l.ks + (((usize)d->val_size + 7) & ~(usize)7);
    return l;
}
#define SLOT(m, L, i) ((m)->slots + (usize)(i) * (L)->stride)

static inline u32 buckets(const jz_hmap *m) { return m->ctrl ? m->mask + 1 : 0; }
static inline u32 cap_to_growth(u32 mask) {
    u32 b = mask + 1;
    return b < 8 ? mask : (b / 8) * 7;
}

static inline void set_ctrl(jz_hmap *m, u32 i, u8 v) {
    m->ctrl[i] = v;
    m->ctrl[((i - GROUP) & m->mask) + GROUP] = v;
}

void jz_hmap_init(jz_hmap *m, const jz_hmap_desc *d) {
    m->ctrl = NULL;
    m->slots = NULL;
    m->mask = 0;
    m->len = 0;
    m->growth_left = 0;
    m->d = d;
}

static bool find(const jz_hmap *m, const hl *L, u64 hash, const void *key, u32 *out) {
    if (!m->ctrl) return false;
    u8 tag = h2(hash);
    u32 pos = (u32)hash & m->mask, stride = 0;
    for (;;) {
        u64 g = grp_load(m->ctrl + pos);
        for (u64 bits = grp_match_byte(g, tag); bits; bits &= bits - 1) {
            u32 i = (pos + bm_lowest(bits)) & m->mask;
            if (m->d->eq(key, SLOT(m, L, i))) {
                *out = i;
                return true;
            }
        }
        if (grp_match_empty(g)) return false;
        stride += GROUP;
        pos = (pos + stride) & m->mask;
    }
}

static u32 find_insert_slot(const jz_hmap *m, u64 hash) {
    u32 pos = (u32)hash & m->mask, stride = 0;
    for (;;) {
        u64 g = grp_load(m->ctrl + pos);
        u64 bits = grp_match_empty_or_deleted(g);
        if (bits) {
            u32 i = (pos + bm_lowest(bits)) & m->mask;
            /* tables smaller than a group: the match may be in the mirrored tail */
            if ((i8)m->ctrl[i] >= 0) i = bm_lowest(grp_match_empty_or_deleted(grp_load(m->ctrl)));
            return i;
        }
        stride += GROUP;
        pos = (pos + stride) & m->mask;
    }
}

static void alloc_table(jz_hmap *m, const hl *L, u32 nbuckets) {
    m->ctrl = (u8 *)jz_alloc(nbuckets + GROUP);
    memset(m->ctrl, EMPTY, nbuckets + GROUP);
    m->slots = (u8 *)jz_alloc((usize)nbuckets * L->stride);
    m->mask = nbuckets - 1;
    m->growth_left = cap_to_growth(m->mask);
}

static u32 capacity_to_buckets(u32 cap) {
    if (cap < 8) return cap < 4 ? 4 : 8;
    u64 adj = (u64)cap * 8 / 7;
    if (adj > 0x40000000u) jz_panic("Hash table capacity overflow");
    u32 b = 1;
    while (b < adj) b <<= 1;
    return b;
}

static void resize(jz_hmap *m, const hl *L, u32 min_cap) {
    jz_hmap old = *m;
    u32 ob = buckets(&old);
    alloc_table(m, L, capacity_to_buckets(min_cap));
    for (u32 i = 0; i < ob; i++) {
        if ((i8)old.ctrl[i] < 0) continue;
        u8 *src = old.slots + (usize)i * L->stride;
        u64 h = m->d->hash(src);
        u32 j = find_insert_slot(m, h);
        set_ctrl(m, j, h2(h));
        memcpy(SLOT(m, L, j), src, L->stride);
    }
    m->growth_left -= old.len;
    m->len = old.len;
    if (old.ctrl) {
        jz_free(old.ctrl);
        jz_free(old.slots);
    }
}

/* rehash in place: reclaim tombstones without growing (hashbrown's rehash_in_place, done via a scratch copy) */
static void reserve_one(jz_hmap *m, const hl *L) {
    u32 full_cap = m->ctrl ? cap_to_growth(m->mask) : 0;
    u32 need = m->len + 1;
    if (need <= full_cap / 2) resize(m, L, full_cap); /* mostly tombstones: same size */
    else resize(m, L, need > full_cap + 1 ? need : full_cap + 1);
}

void *jz_hmap_get(const jz_hmap *m, const void *key) {
    if (!m->len) return NULL;
    hl L = mk_hl(m->d);
    u32 i;
    if (!find(m, &L, m->d->hash(key), key, &i)) return NULL;
    return SLOT(m, &L, i) + L.ks;
}

static u8 *insert_new(jz_hmap *m, const hl *L, u64 h, const void *key, const void *val) {
    u32 i = m->ctrl ? find_insert_slot(m, h) : 0;
    if (!m->ctrl || (m->growth_left == 0 && m->ctrl[i] == EMPTY)) {
        reserve_one(m, L);
        i = find_insert_slot(m, h);
    }
    m->growth_left -= (m->ctrl[i] == EMPTY);
    set_ctrl(m, i, h2(h));
    u8 *s = SLOT(m, L, i);
    memcpy(s, key, m->d->key_size);
    if (m->d->val_size) {
        if (val) memcpy(s + L->ks, val, m->d->val_size);
        else memset(s + L->ks, 0, m->d->val_size);
    }
    m->len++;
    return s;
}

bool jz_hmap_insert(jz_hmap *m, const void *key, const void *val, void *old_val) {
    hl L = mk_hl(m->d);
    u64 h = m->d->hash(key);
    u32 i;
    if (find(m, &L, h, key, &i)) {
        u8 *v = SLOT(m, &L, i) + L.ks;
        if (m->d->val_size) {
            if (old_val) memcpy(old_val, v, m->d->val_size);
            else if (m->d->val_drop) m->d->val_drop(v);
            memcpy(v, val, m->d->val_size);
        }
        if (m->d->key_drop) m->d->key_drop((void *)key); /* HashMap::insert keeps the old key */
        return true;
    }
    insert_new(m, &L, h, key, val);
    return false;
}

void *jz_hmap_entry(jz_hmap *m, const void *key, bool *inserted) {
    hl L = mk_hl(m->d);
    u64 h = m->d->hash(key);
    u32 i;
    if (find(m, &L, h, key, &i)) {
        if (inserted) *inserted = false;
        if (m->d->key_drop) m->d->key_drop((void *)key);
        return SLOT(m, &L, i) + L.ks;
    }
    if (inserted) *inserted = true;
    return insert_new(m, &L, h, key, NULL) + L.ks;
}

static void erase(jz_hmap *m, u32 i) {
    u32 before = (i - GROUP) & m->mask;
    u64 eb = grp_match_empty(grp_load(m->ctrl + before));
    u64 ea = grp_match_empty(grp_load(m->ctrl + i));
    u8 c;
    if (bm_lz(eb) + bm_tz(ea) >= GROUP) {
        c = DELETED;
    } else {
        m->growth_left++;
        c = EMPTY;
    }
    set_ctrl(m, i, c);
    m->len--;
}

bool jz_hmap_remove(jz_hmap *m, const void *key, void *out_key, void *out_val) {
    if (!m->len) return false;
    hl L = mk_hl(m->d);
    u32 i;
    if (!find(m, &L, m->d->hash(key), key, &i)) return false;
    u8 *s = SLOT(m, &L, i);
    if (out_key) memcpy(out_key, s, m->d->key_size);
    else if (m->d->key_drop) m->d->key_drop(s);
    if (m->d->val_size) {
        if (out_val) memcpy(out_val, s + L.ks, m->d->val_size);
        else if (m->d->val_drop) m->d->val_drop(s + L.ks);
    }
    erase(m, i);
    return true;
}

static void drop_elems(jz_hmap *m, const hl *L) {
    if (!m->d->key_drop && !m->d->val_drop) return;
    u32 nb = buckets(m);
    for (u32 i = 0; i < nb && m->len; i++) {
        if ((i8)m->ctrl[i] < 0) continue;
        u8 *s = SLOT(m, L, i);
        if (m->d->key_drop) m->d->key_drop(s);
        if (m->d->val_drop) m->d->val_drop(s + L->ks);
    }
}

void jz_hmap_clear(jz_hmap *m) {
    if (!m->ctrl) return;
    hl L = mk_hl(m->d);
    drop_elems(m, &L);
    memset(m->ctrl, EMPTY, buckets(m) + GROUP);
    m->len = 0;
    m->growth_left = cap_to_growth(m->mask);
}

void jz_hmap_drop(jz_hmap *m) {
    if (m->ctrl) {
        hl L = mk_hl(m->d);
        drop_elems(m, &L);
        jz_free(m->ctrl);
        jz_free(m->slots);
    }
    m->ctrl = NULL;
    m->slots = NULL;
    m->mask = m->len = m->growth_left = 0;
}

void jz_hmap_iter_begin(jz_hmap_iter *it, const jz_hmap *m) {
    it->m = m;
    it->i = 0;
}

bool jz_hmap_next(jz_hmap_iter *it, void **key, void **val) {
    const jz_hmap *m = it->m;
    if (!m->ctrl) return false;
    u32 nb = m->mask + 1;
    /* skip a whole group of empty/deleted control bytes at a time */
    while (it->i < nb) {
        if ((it->i & (GROUP - 1)) == 0 && it->i + GROUP <= nb && grp_match_empty_or_deleted(grp_load(m->ctrl + it->i)) == MSB) {
            it->i += GROUP;
            continue;
        }
        u32 i = it->i++;
        if ((i8)m->ctrl[i] >= 0) {
            hl L = mk_hl(m->d);
            u8 *s = SLOT(m, &L, i);
            if (key) *key = s;
            if (val) *val = s + L.ks;
            return true;
        }
    }
    return false;
}

void jz_hmap_retain(jz_hmap *m, bool (*keep)(const void *k, void *v, void *ctx), void *ctx) {
    if (!m->ctrl) return;
    hl L = mk_hl(m->d);
    u32 nb = m->mask + 1;
    for (u32 i = 0; i < nb; i++) {
        if ((i8)m->ctrl[i] < 0) continue;
        u8 *s = SLOT(m, &L, i);
        if (keep(s, s + L.ks, ctx)) continue;
        if (m->d->key_drop) m->d->key_drop(s);
        if (m->d->val_drop) m->d->val_drop(s + L.ks);
        erase(m, i);
    }
}
