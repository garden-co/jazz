/* btree.c: BTreeMap / BTreeSet (alloc::collections::btree), one type-erased B-tree for every K,V.
 *
 * B = 6 as in std: nodes hold up to CAP = 2B-1 = 11 keys, non-root nodes at least MIN_LEN = B-1 = 5.
 * Insertion inserts into the leaf and splits overflowing nodes on the way back up (nodes have room
 * for one extra key while being split). Removal of an internal key swaps in its in-order
 * predecessor; underfull children are fixed by stealing from a sibling (rotate) or by merging
 * with one. Iteration keeps an explicit stack of (node, index) so nodes need no parent links.
 * Keys and values are stored inline, each slot rounded up to 8 bytes so typed pointers into
 * nodes are aligned. */
#include "rt_internal.h"

#define B 6
#define CAP (2 * B - 1)
#define MIN_LEN (B - 1)
#define HDR 8

struct jz_btree_node {
    u16 len;
    u16 leaf;
    u32 _pad;
    /* keys[CAP+1], vals[CAP+1], edges[CAP+2] (internal nodes only) */
};

typedef struct lay { usize ks, vs, edges_off; const jz_btree_desc *d; } lay;

static inline lay mk_lay(const jz_btree_desc *d) {
    lay l;
    l.d = d;
    l.ks = ((usize)d->key_size + 7) & ~(usize)7;
    l.vs = ((usize)d->val_size + 7) & ~(usize)7;
    l.edges_off = HDR + (CAP + 1) * (l.ks + l.vs);
    return l;
}

#define KEY(L, n, i) ((u8 *)(n) + HDR + (usize)(i) * (L)->ks)
#define VAL(L, n, i) ((u8 *)(n) + HDR + (CAP + 1) * (L)->ks + (usize)(i) * (L)->vs)
#define EDGES(L, n) ((jz_btree_node **)((u8 *)(n) + (L)->edges_off))

static jz_btree_node *node_new(const lay *L, bool leaf) {
    usize sz = L->edges_off + (leaf ? 0 : (CAP + 2) * sizeof(jz_btree_node *));
    jz_btree_node *n = (jz_btree_node *)jz_alloc(sz);
    n->len = 0;
    n->leaf = leaf;
    return n;
}

/* linear search like std's search_node: returns true if found; *idx = position or descent edge */
static bool node_search(const lay *L, const jz_btree_node *n, const void *key, u32 *idx) {
    jz_cmp_fn cmp = L->d->cmp;
    u32 i = 0;
    for (; i < n->len; i++) {
        int c = cmp(key, KEY(L, n, i), NULL);
        if (c == 0) { *idx = i; return true; }
        if (c < 0) break;
    }
    *idx = i;
    return false;
}

void jz_btree_init(jz_btree *t, const jz_btree_desc *d) {
    t->root = NULL;
    t->height = 0;
    t->len = 0;
    t->d = d;
}

static void node_free_rec(const lay *L, jz_btree_node *n, u32 h, bool drop_items) {
    const jz_btree_desc *d = L->d;
    if (drop_items) {
        for (u32 i = 0; i < n->len; i++) {
            if (d->key_drop) d->key_drop(KEY(L, n, i));
            if (d->val_drop) d->val_drop(VAL(L, n, i));
        }
    }
    if (h > 0)
        for (u32 i = 0; i <= n->len; i++) node_free_rec(L, EDGES(L, n)[i], h - 1, drop_items);
    jz_free(n);
}

void jz_btree_clear(jz_btree *t) {
    if (t->root) {
        lay L = mk_lay(t->d);
        node_free_rec(&L, t->root, t->height, true);
    }
    t->root = NULL;
    t->height = 0;
    t->len = 0;
}

void jz_btree_drop(jz_btree *t) { jz_btree_clear(t); }

void *jz_btree_get(const jz_btree *t, const void *key) {
    jz_btree_node *n = t->root;
    if (!n) return NULL;
    lay L = mk_lay(t->d);
    u32 h = t->height;
    for (;;) {
        u32 i;
        if (node_search(&L, n, key, &i)) return VAL(&L, n, i);
        if (h == 0) return NULL;
        n = EDGES(&L, n)[i];
        h--;
    }
}

bool jz_btree_contains(const jz_btree *t, const void *key) {
    jz_btree_node *n = t->root;
    if (!n) return false;
    lay L = mk_lay(t->d);
    u32 h = t->height;
    for (;;) {
        u32 i;
        if (node_search(&L, n, key, &i)) return true;
        if (h == 0) return false;
        n = EDGES(&L, n)[i];
        h--;
    }
}

/* ---------- insertion ---------- */
static void node_insert_at(const lay *L, jz_btree_node *n, u32 i, const void *k, const void *v, jz_btree_node *right) {
    u32 len = n->len;
    memmove(KEY(L, n, i + 1), KEY(L, n, i), (usize)(len - i) * L->ks);
    memcpy(KEY(L, n, i), k, L->d->key_size);
    if (L->vs) {
        memmove(VAL(L, n, i + 1), VAL(L, n, i), (usize)(len - i) * L->vs);
        memcpy(VAL(L, n, i), v, L->d->val_size);
    }
    if (!n->leaf) {
        jz_btree_node **e = EDGES(L, n);
        memmove(&e[i + 2], &e[i + 1], (usize)(len - i) * sizeof(*e));
        e[i + 1] = right;
    }
    n->len = (u16)(len + 1);
}

/* split an overflowing node (len == CAP+1): median goes to mk/mv, upper half to a new node */
static jz_btree_node *node_split(const lay *L, jz_btree_node *n, void *mk, void *mv) {
    const u32 mid = B;            /* keys [0,B) stay, key B is the median, (B, CAP] move */
    u32 rlen = n->len - mid - 1;
    jz_btree_node *r = node_new(L, n->leaf);
    memcpy(mk, KEY(L, n, mid), L->d->key_size);
    if (L->vs) memcpy(mv, VAL(L, n, mid), L->d->val_size);
    memcpy(KEY(L, r, 0), KEY(L, n, mid + 1), (usize)rlen * L->ks);
    if (L->vs) memcpy(VAL(L, r, 0), VAL(L, n, mid + 1), (usize)rlen * L->vs);
    if (!n->leaf) memcpy(EDGES(L, r), &EDGES(L, n)[mid + 1], (usize)(rlen + 1) * sizeof(jz_btree_node *));
    r->len = (u16)rlen;
    n->len = (u16)mid;
    return r;
}

enum { INS_DONE, INS_REPLACED, INS_SPLIT };

static int insert_rec(const lay *L, jz_btree_node *n, u32 h, const void *key, const void *val, void *old_val,
                      void *mk, void *mv, jz_btree_node **split) {
    const jz_btree_desc *d = L->d;
    u32 i;
    if (node_search(L, n, key, &i)) {
        void *slot = VAL(L, n, i);
        if (d->val_size) {
            if (old_val) memcpy(old_val, slot, d->val_size);
            else if (d->val_drop) d->val_drop(slot);
            memcpy(slot, val, d->val_size);
        }
        if (d->key_drop) d->key_drop((void *)key); /* map keeps its original key */
        return INS_REPLACED;
    }
    if (h == 0) {
        node_insert_at(L, n, i, key, val, NULL);
    } else {
        jz_btree_node *right = NULL;
        int r = insert_rec(L, EDGES(L, n)[i], h - 1, key, val, old_val, mk, mv, &right);
        if (r != INS_SPLIT) return r;
        node_insert_at(L, n, i, mk, mv, right);
    }
    if (n->len > CAP) {
        *split = node_split(L, n, mk, mv);
        return INS_SPLIT;
    }
    return INS_DONE;
}

bool jz_btree_insert(jz_btree *t, const void *key, const void *val, void *old_val) {
    lay L = mk_lay(t->d);
    if (!t->root) {
        t->root = node_new(&L, true);
        t->height = 0;
    }
    void *mk = __builtin_alloca(L.ks + 8);
    void *mv = __builtin_alloca(L.vs + 8);
    jz_btree_node *right = NULL;
    int r = insert_rec(&L, t->root, t->height, key, val, old_val, mk, mv, &right);
    if (r == INS_REPLACED) return true;
    t->len++;
    if (r == INS_SPLIT) {
        jz_btree_node *root = node_new(&L, false);
        memcpy(KEY(&L, root, 0), mk, t->d->key_size);
        if (L.vs) memcpy(VAL(&L, root, 0), mv, t->d->val_size);
        EDGES(&L, root)[0] = t->root;
        EDGES(&L, root)[1] = right;
        root->len = 1;
        t->root = root;
        t->height++;
    }
    return false;
}

void *jz_btree_entry(jz_btree *t, const void *key, bool *inserted) {
    void *v = jz_btree_get(t, key);
    if (v) {
        if (inserted) *inserted = false;
        if (t->d->key_drop) t->d->key_drop((void *)key); /* entry(key) consumes key */
        return v;
    }
    void *zero = __builtin_alloca((usize)t->d->val_size + 8);
    memset(zero, 0, t->d->val_size);
    jz_btree_insert(t, key, zero, NULL);
    if (inserted) *inserted = true;
    return jz_btree_get(t, key);
}

/* ---------- removal ---------- */
static void node_remove_at(const lay *L, jz_btree_node *n, u32 i, void *ok, void *ov) {
    u32 len = n->len;
    if (ok) memcpy(ok, KEY(L, n, i), L->d->key_size);
    if (ov && L->vs) memcpy(ov, VAL(L, n, i), L->d->val_size);
    memmove(KEY(L, n, i), KEY(L, n, i + 1), (usize)(len - i - 1) * L->ks);
    if (L->vs) memmove(VAL(L, n, i), VAL(L, n, i + 1), (usize)(len - i - 1) * L->vs);
    n->len = (u16)(len - 1);
}

/* parent key i separates edges i and i+1: move left's last entry up, parent's down into child */
static void rotate_right(const lay *L, jz_btree_node *p, u32 i, jz_btree_node *left, jz_btree_node *child) {
    u32 cl = child->len;
    memmove(KEY(L, child, 1), KEY(L, child, 0), (usize)cl * L->ks);
    if (L->vs) memmove(VAL(L, child, 1), VAL(L, child, 0), (usize)cl * L->vs);
    memcpy(KEY(L, child, 0), KEY(L, p, i), L->ks);
    if (L->vs) memcpy(VAL(L, child, 0), VAL(L, p, i), L->vs);
    u32 ll = left->len - 1;
    memcpy(KEY(L, p, i), KEY(L, left, ll), L->ks);
    if (L->vs) memcpy(VAL(L, p, i), VAL(L, left, ll), L->vs);
    if (!child->leaf) {
        jz_btree_node **ce = EDGES(L, child);
        memmove(&ce[1], &ce[0], (usize)(cl + 1) * sizeof(*ce));
        ce[0] = EDGES(L, left)[ll + 1];
    }
    left->len = (u16)ll;
    child->len = (u16)(cl + 1);
}

static void rotate_left(const lay *L, jz_btree_node *p, u32 i, jz_btree_node *child, jz_btree_node *right) {
    u32 cl = child->len;
    memcpy(KEY(L, child, cl), KEY(L, p, i), L->ks);
    if (L->vs) memcpy(VAL(L, child, cl), VAL(L, p, i), L->vs);
    memcpy(KEY(L, p, i), KEY(L, right, 0), L->ks);
    if (L->vs) memcpy(VAL(L, p, i), VAL(L, right, 0), L->vs);
    u32 rl = right->len;
    memmove(KEY(L, right, 0), KEY(L, right, 1), (usize)(rl - 1) * L->ks);
    if (L->vs) memmove(VAL(L, right, 0), VAL(L, right, 1), (usize)(rl - 1) * L->vs);
    if (!child->leaf) {
        jz_btree_node **re = EDGES(L, right);
        EDGES(L, child)[cl + 1] = re[0];
        memmove(&re[0], &re[1], (usize)rl * sizeof(*re));
    }
    right->len = (u16)(rl - 1);
    child->len = (u16)(cl + 1);
}

/* merge edges i, i+1 of p with the separating key i into edge i */
static void merge_children(const lay *L, jz_btree_node *p, u32 i) {
    jz_btree_node **pe = EDGES(L, p);
    jz_btree_node *left = pe[i], *right = pe[i + 1];
    u32 ll = left->len, rl = right->len;
    memcpy(KEY(L, left, ll), KEY(L, p, i), L->ks);
    if (L->vs) memcpy(VAL(L, left, ll), VAL(L, p, i), L->vs);
    memcpy(KEY(L, left, ll + 1), KEY(L, right, 0), (usize)rl * L->ks);
    if (L->vs) memcpy(VAL(L, left, ll + 1), VAL(L, right, 0), (usize)rl * L->vs);
    if (!left->leaf) memcpy(&EDGES(L, left)[ll + 1], EDGES(L, right), (usize)(rl + 1) * sizeof(jz_btree_node *));
    left->len = (u16)(ll + 1 + rl);
    /* remove key i and edge i+1 from parent */
    u32 pl = p->len;
    memmove(KEY(L, p, i), KEY(L, p, i + 1), (usize)(pl - i - 1) * L->ks);
    if (L->vs) memmove(VAL(L, p, i), VAL(L, p, i + 1), (usize)(pl - i - 1) * L->vs);
    memmove(&pe[i + 1], &pe[i + 2], (usize)(pl - i - 1) * sizeof(*pe));
    p->len = (u16)(pl - 1);
    jz_free(right);
}

static void fix_child(const lay *L, jz_btree_node *p, u32 i) {
    jz_btree_node **pe = EDGES(L, p);
    jz_btree_node *child = pe[i];
    if (child->len >= MIN_LEN) return;
    if (i > 0 && pe[i - 1]->len > MIN_LEN) {
        rotate_right(L, p, i - 1, pe[i - 1], child);
    } else if (i < p->len && pe[i + 1]->len > MIN_LEN) {
        rotate_left(L, p, i, child, pe[i + 1]);
    } else if (i > 0) {
        merge_children(L, p, i - 1);
    } else {
        merge_children(L, p, i);
    }
}

static void remove_max(const lay *L, jz_btree_node *n, u32 h, void *ok, void *ov) {
    if (h == 0) {
        node_remove_at(L, n, n->len - 1, ok, ov);
        return;
    }
    u32 last = n->len;
    remove_max(L, EDGES(L, n)[last], h - 1, ok, ov);
    fix_child(L, n, last);
}

static bool remove_rec(const lay *L, jz_btree_node *n, u32 h, const void *key, void *ok, void *ov) {
    u32 i;
    if (node_search(L, n, key, &i)) {
        if (h == 0) {
            node_remove_at(L, n, i, ok, ov);
        } else {
            if (ok) memcpy(ok, KEY(L, n, i), L->d->key_size);
            if (ov && L->vs) memcpy(ov, VAL(L, n, i), L->d->val_size);
            remove_max(L, EDGES(L, n)[i], h - 1, KEY(L, n, i), VAL(L, n, i));
            fix_child(L, n, i);
        }
        return true;
    }
    if (h == 0) return false;
    if (!remove_rec(L, EDGES(L, n)[i], h - 1, key, ok, ov)) return false;
    fix_child(L, n, i);
    return true;
}

bool jz_btree_remove(jz_btree *t, const void *key, void *out_key, void *out_val) {
    if (!t->root) return false;
    lay L = mk_lay(t->d);
    const jz_btree_desc *d = t->d;
    void *kb = out_key, *vb = out_val;
    if (!kb && d->key_drop) kb = __builtin_alloca(L.ks + 8);
    if (!vb && d->val_drop && L.vs) vb = __builtin_alloca(L.vs + 8);
    if (!remove_rec(&L, t->root, t->height, key, kb, vb)) return false;
    if (!out_key && d->key_drop) d->key_drop(kb);
    if (!out_val && d->val_drop && L.vs) d->val_drop(vb);
    t->len--;
    jz_btree_node *root = t->root;
    if (root->len == 0) {
        if (t->height > 0) {
            t->root = EDGES(&L, root)[0];
            t->height--;
        } else {
            t->root = NULL;
        }
        jz_free(root);
    }
    return true;
}

bool jz_btree_first(const jz_btree *t, void **key, void **val) {
    jz_btree_node *n = t->root;
    if (!n || t->len == 0) return false;
    lay L = mk_lay(t->d);
    for (u32 h = t->height; h > 0; h--) n = EDGES(&L, n)[0];
    if (key) *key = KEY(&L, n, 0);
    if (val) *val = VAL(&L, n, 0);
    return true;
}

bool jz_btree_last(const jz_btree *t, void **key, void **val) {
    jz_btree_node *n = t->root;
    if (!n || t->len == 0) return false;
    lay L = mk_lay(t->d);
    for (u32 h = t->height; h > 0; h--) n = EDGES(&L, n)[n->len];
    if (key) *key = KEY(&L, n, n->len - 1);
    if (val) *val = VAL(&L, n, n->len - 1);
    return true;
}

bool jz_btree_pop_first(jz_btree *t, void *out_key, void *out_val) {
    void *k;
    if (!jz_btree_first(t, &k, NULL)) return false;
    void *tmp = __builtin_alloca((usize)t->d->key_size + 8);
    memcpy(tmp, k, t->d->key_size);
    return jz_btree_remove(t, tmp, out_key, out_val);
}

/* ---------- iteration ---------- */
static void iter_descend(jz_btree_iter *it, const lay *L, jz_btree_node *n) {
    for (;;) {
        it->depth++;
        it->stack[it->depth] = n;
        it->idx[it->depth] = 0;
        if (n->leaf) break;
        n = EDGES(L, n)[0];
    }
}

void jz_btree_iter_begin(jz_btree_iter *it, const jz_btree *t) {
    it->t = t;
    it->depth = -1;
    it->hi = NULL;
    it->hi_incl = false;
    if (t->root && t->len) {
        lay L = mk_lay(t->d);
        iter_descend(it, &L, t->root);
    }
}

void jz_btree_range(jz_btree_iter *it, const jz_btree *t, const void *lo, bool lo_incl, const void *hi, bool hi_incl) {
    it->t = t;
    it->depth = -1;
    it->hi = hi;
    it->hi_incl = hi_incl;
    if (!t->root || !t->len) return;
    lay L = mk_lay(t->d);
    if (lo && hi) {
        int c = t->d->cmp(lo, hi, NULL);
        if (c > 0) jz_panic("range start is greater than range end in BTreeMap");
        if (c == 0 && !lo_incl && !hi_incl) jz_panic("range start and end are equal and excluded in BTreeMap");
    }
    if (!lo) {
        iter_descend(it, &L, t->root);
        return;
    }
    jz_btree_node *n = t->root;
    for (;;) {
        u32 i = 0;
        for (; i < n->len; i++) {
            int c = t->d->cmp(KEY(&L, n, i), lo, NULL);
            if (c > 0 || (c == 0 && lo_incl)) break;
        }
        it->depth++;
        it->stack[it->depth] = n;
        it->idx[it->depth] = (u16)i;
        if (n->leaf) break;
        n = EDGES(&L, n)[i];
    }
}

bool jz_btree_next(jz_btree_iter *it, void **key, void **val) {
    if (it->depth < 0) return false;
    lay L = mk_lay(it->t->d);
    while (it->depth >= 0) {
        jz_btree_node *n = it->stack[it->depth];
        u32 i = it->idx[it->depth];
        if (i < n->len) {
            void *k = KEY(&L, n, i);
            if (it->hi) {
                int c = it->t->d->cmp(k, it->hi, NULL);
                if (c > 0 || (c == 0 && !it->hi_incl)) {
                    it->depth = -1;
                    return false;
                }
            }
            if (key) *key = k;
            if (val) *val = VAL(&L, n, i);
            it->idx[it->depth] = (u16)(i + 1);
            if (!n->leaf) iter_descend(it, &L, EDGES(&L, n)[i + 1]);
            return true;
        }
        it->depth--;
    }
    return false;
}

/* ---------- whole-tree operations ---------- */
static jz_btree_node *clone_rec(const lay *L, const jz_btree_node *s, u32 h, void (*kc)(void *, const void *),
                                void (*vc)(void *, const void *)) {
    jz_btree_node *n = node_new(L, h == 0);
    const jz_btree_desc *d = L->d;
    for (u32 i = 0; i < s->len; i++) {
        if (kc) kc(KEY(L, n, i), KEY(L, s, i));
        else memcpy(KEY(L, n, i), KEY(L, s, i), d->key_size);
        if (L->vs) {
            if (vc) vc(VAL(L, n, i), VAL(L, s, i));
            else memcpy(VAL(L, n, i), VAL(L, s, i), d->val_size);
        }
    }
    if (h > 0)
        for (u32 i = 0; i <= s->len; i++) EDGES(L, n)[i] = clone_rec(L, EDGES(L, s)[i], h - 1, kc, vc);
    n->len = s->len;
    return n;
}

void jz_btree_clone(jz_btree *dst, const jz_btree *src, void (*kclone)(void *, const void *), void (*vclone)(void *, const void *)) {
    jz_btree_init(dst, src->d);
    if (!src->root) return;
    lay L = mk_lay(src->d);
    dst->root = clone_rec(&L, src->root, src->height, kclone, vclone);
    dst->height = src->height;
    dst->len = src->len;
}

/* move every entry out (in order) into a packed (key|val) buffer and free the nodes */
static u8 *drain_all(jz_btree *t, u32 *count, usize *pair) {
    lay L = mk_lay(t->d);
    usize ps = L.ks + L.vs;
    u8 *buf = t->len ? (u8 *)jz_alloc(ps * t->len) : NULL;
    u32 n = 0;
    void *k, *v;
    JZ_BTREE_FOR(t, k, v) {
        memcpy(buf + ps * n, k, t->d->key_size);
        if (L.vs) memcpy(buf + ps * n + L.ks, v, t->d->val_size);
        n++;
    }
    if (t->root) node_free_rec(&L, t->root, t->height, false);
    t->root = NULL;
    t->height = 0;
    t->len = 0;
    *count = n;
    *pair = ps;
    return buf;
}

void jz_btree_retain(jz_btree *t, bool (*keep)(const void *k, void *v, void *ctx), void *ctx) {
    u32 n;
    usize ps;
    lay L = mk_lay(t->d);
    u8 *buf = drain_all(t, &n, &ps);
    for (u32 i = 0; i < n; i++) {
        u8 *k = buf + ps * i, *v = k + L.ks;
        if (keep(k, v, ctx)) {
            jz_btree_insert(t, k, v, NULL);
        } else {
            if (t->d->key_drop) t->d->key_drop(k);
            if (t->d->val_drop) t->d->val_drop(v);
        }
    }
    jz_free(buf);
}

void jz_btree_append(jz_btree *dst, jz_btree *src) {
    if (!src->len) return;
    if (!dst->len) {
        jz_btree tmp = *dst;
        *dst = *src;
        *src = tmp;
        return;
    }
    u32 n;
    usize ps;
    lay L = mk_lay(src->d);
    u8 *buf = drain_all(src, &n, &ps);
    for (u32 i = 0; i < n; i++) jz_btree_insert(dst, buf + ps * i, buf + ps * i + L.ks, NULL);
    jz_free(buf);
}

bool jz_btree_eq(const jz_btree *a, const jz_btree *b, bool (*veq)(const void *, const void *)) {
    if (a->len != b->len) return false;
    jz_btree_iter ia, ib;
    jz_btree_iter_begin(&ia, a);
    jz_btree_iter_begin(&ib, b);
    void *ka, *va, *kb, *vb;
    while (jz_btree_next(&ia, &ka, &va)) {
        if (!jz_btree_next(&ib, &kb, &vb)) return false;
        if (a->d->cmp(ka, kb, NULL) != 0) return false;
        if (a->d->val_size) {
            if (veq ? !veq(va, vb) : memcmp(va, vb, a->d->val_size) != 0) return false;
        }
    }
    return true;
}

void jz_btree_from_pairs_ex(jz_btree *t, const jz_btree_desc *d, void *pairs, u32 n, usize stride, usize val_off) {
    jz_btree_init(t, d);
    if (!n) return;
    /* stable sort by key (the key is at offset 0 of each pair), later duplicates win */
    jz_sort(pairs, n, stride, d->cmp, NULL);
    u8 *p = (u8 *)pairs;
    for (u32 i = 0; i < n; i++) jz_btree_insert(t, p + stride * i, p + stride * i + val_off, NULL);
}

void jz_btree_from_pairs(jz_btree *t, const jz_btree_desc *d, void *pairs, u32 n) {
    jz_btree_from_pairs_ex(t, d, pairs, n, (usize)d->key_size + d->val_size, d->key_size);
}
