/* positional_order.c: port of crates/jazz/layers/db/src/positional_order.rs.
 * Rust monomorphised PositionalOrder<K> (twice for insert/merge/pull/split in the
 * shipped build); here one copy parameterised by a key descriptor. */
#include "positional_order.h"

#define NIL JAZZ_DB_PO_NIL

static usize stride(const jazz_db_PositionalOrder *o) { return sizeof(jazz_db_PoNode) + o->k->key_size; }
static jazz_db_PoNode *node_at(const jazz_db_PositionalOrder *o, u32 i) { return (jazz_db_PoNode *)(o->nodes + (usize)i * stride(o)); }
static void *node_key(const jazz_db_PositionalOrder *o, u32 i) { return (u8 *)node_at(o, i) + sizeof(jazz_db_PoNode); }

static void occ_drop(void *p) { jazz_types__object__OutputOccurrenceId__drop(p); }
static const jz_btree_desc occ_slots_desc = {
    sizeof(jazz_types_OutputOccurrenceId), sizeof(u32), jazz_types__object__OutputOccurrenceId__cmp, occ_drop, NULL,
};
const jazz_db_PositionalOrderKey jazz_db__positional_order__occurrence_key = {
    sizeof(jazz_types_OutputOccurrenceId), jazz_types__object__OutputOccurrenceId__cmp,
    jazz_types__object__OutputOccurrenceId__clone, occ_drop, &occ_slots_desc,
};

static u64 jazz_db__positional_order__splitmix64(u64 state) {
    u64 z = state;
    z = (z ^ (z >> 30)) * 0xbf58476d1ce4e5b9ull;
    z = (z ^ (z >> 27)) * 0x94d049bb133111ebull;
    return z ^ (z >> 31);
}

void jazz_db__positional_order__PositionalOrder__default(jazz_db_PositionalOrder *o, const jazz_db_PositionalOrderKey *k) {
    memset(o, 0, sizeof *o);
    o->k = k;
    jz_btree_init(&o->slots, k->slots_desc);
    o->root = NIL;
    o->seed = 0x9e3779b97f4a7c15ull;
    o->anonymous = 0;
}

void jazz_db__positional_order__PositionalOrder__drop(jazz_db_PositionalOrder *o) {
    jz_btree_drop(&o->slots);
    for (u32 i = 0; i < o->nodes_len; i++)
        if (node_at(o, i)->has_key && o->k->drop) o->k->drop(node_key(o, i));
    jz_free(o->nodes);
    o->nodes = NULL;
    o->nodes_len = o->nodes_cap = 0;
}

static void slot_key_clone(void *d, const void *s);
static const jazz_db_PositionalOrderKey *clone_key_ctx;
static void slot_key_clone(void *d, const void *s) { clone_key_ctx->clone(d, s); }

void jazz_db__positional_order__PositionalOrder__clone(jazz_db_PositionalOrder *dst, const jazz_db_PositionalOrder *src) {
    *dst = *src;
    clone_key_ctx = src->k;
    jz_btree_clone(&dst->slots, &src->slots, slot_key_clone, NULL);
    dst->nodes = jz_alloc(stride(src) * (src->nodes_cap ? src->nodes_cap : 1));
    for (u32 i = 0; i < src->nodes_len; i++) {
        jazz_db_PoNode *d = (jazz_db_PoNode *)(dst->nodes + (usize)i * stride(src));
        *d = *node_at(src, i);
        if (d->has_key) src->k->clone((u8 *)d + sizeof *d, node_key(src, i));
    }
}

static void reserve_nodes(jazz_db_PositionalOrder *o, u32 extra) {
    if (o->nodes_len + extra <= o->nodes_cap) return;
    u32 cap = o->nodes_cap ? o->nodes_cap * 2 : 4;
    if (cap < o->nodes_len + extra) cap = o->nodes_len + extra;
    o->nodes = jz_realloc(o->nodes, (usize)cap * stride(o));
    o->nodes_cap = cap;
}

u32 jazz_db__positional_order__PositionalOrder__len(const jazz_db_PositionalOrder *o) { return o->nodes_len; }

static u32 jazz_db__positional_order__PositionalOrder__size(const jazz_db_PositionalOrder *o, u32 node) {
    return node == NIL ? 0 : node_at(o, node)->size;
}

bool jazz_db__positional_order__PositionalOrder__from_ordered(jazz_db_PositionalOrder *o, const jazz_db_PositionalOrderKey *k, void *keys, u32 n, void *dup) {
    jazz_db__positional_order__PositionalOrder__default(o, k);
    u32 lower = n;
    u32 slack = lower / 8;
    if (slack < 64) slack = 64;
    reserve_nodes(o, lower + slack);
    for (u32 i = 0; i < n; i++) {
        void *key = (u8 *)keys + (usize)i * k->key_size;
        u32 position = jazz_db__positional_order__PositionalOrder__len(o);
        u8 tmp[256];
        k->clone(tmp, key);
        if (!jazz_db__positional_order__PositionalOrder__insert(o, position, tmp)) {
            memcpy(dup, key, k->key_size);
            for (u32 j = i + 1; j < n; j++) k->drop((u8 *)keys + (usize)j * k->key_size);
            jazz_db__positional_order__PositionalOrder__drop(o);
            return false;
        }
        k->drop(key);
    }
    return true;
}

bool jazz_db__positional_order__PositionalOrder__position(const jazz_db_PositionalOrder *o, const void *key, u32 *out) {
    u32 *slot = jz_btree_get(&o->slots, key);
    if (!slot) return false;
    u32 node = *slot;
    u32 rank = jazz_db__positional_order__PositionalOrder__size(o, node_at(o, node)->left);
    for (;;) {
        u32 parent = node_at(o, node)->parent;
        if (parent == NIL) { *out = rank; return true; }
        if (node_at(o, parent)->right == node)
            rank += jazz_db__positional_order__PositionalOrder__size(o, node_at(o, parent)->left) + 1;
        node = parent;
    }
}

u32 jazz_db__positional_order__PositionalOrder__anonymous_len(const jazz_db_PositionalOrder *o) { return o->anonymous; }

bool jazz_db__positional_order__PositionalOrder__anonymize(jazz_db_PositionalOrder *o, const void *key, u32 *out) {
    u32 position;
    if (!jazz_db__positional_order__PositionalOrder__position(o, key, &position)) return false;
    u32 node;
    if (!jz_btree_remove(&o->slots, key, NULL, &node)) jz_panic("positioned key has a slot");
    jazz_db_PoNode *n = node_at(o, node);
    if (n->has_key) o->k->drop(node_key(o, node));
    n->has_key = false;
    o->anonymous += 1;
    if (out) *out = position;
    return true;
}

const void *jazz_db__positional_order__PositionalOrder__get(const jazz_db_PositionalOrder *o, u32 position) {
    u32 node = o->root;
    while (node != NIL) {
        const jazz_db_PoNode *current = node_at(o, node);
        u32 left = jazz_db__positional_order__PositionalOrder__size(o, current->left);
        if (position < left) {
            node = current->left;
        } else if (position == left) {
            return current->has_key ? node_key(o, node) : NULL;
        } else {
            position -= left + 1;
            node = current->right;
        }
    }
    return NULL;
}

static void jazz_db__positional_order__PositionalOrder__pull(jazz_db_PositionalOrder *o, u32 node) {
    u32 left = node_at(o, node)->left, right = node_at(o, node)->right;
    node_at(o, node)->size = 1 + jazz_db__positional_order__PositionalOrder__size(o, left) +
                             jazz_db__positional_order__PositionalOrder__size(o, right);
    if (left != NIL) node_at(o, left)->parent = node;
    if (right != NIL) node_at(o, right)->parent = node;
}

static void jazz_db__positional_order__PositionalOrder__split(jazz_db_PositionalOrder *o, u32 node, u32 count, u32 *before, u32 *after) {
    if (node == NIL) { *before = NIL; *after = NIL; return; }
    u32 left = node_at(o, node)->left;
    u32 left_size = jazz_db__positional_order__PositionalOrder__size(o, left);
    if (count <= left_size) {
        u32 b, a;
        jazz_db__positional_order__PositionalOrder__split(o, left, count, &b, &a);
        node_at(o, node)->left = a;
        jazz_db__positional_order__PositionalOrder__pull(o, node);
        *before = b; *after = node;
    } else {
        u32 right = node_at(o, node)->right;
        u32 b, a;
        jazz_db__positional_order__PositionalOrder__split(o, right, count - left_size - 1, &b, &a);
        node_at(o, node)->right = b;
        jazz_db__positional_order__PositionalOrder__pull(o, node);
        *before = node; *after = a;
    }
}

static u32 jazz_db__positional_order__PositionalOrder__merge(jazz_db_PositionalOrder *o, u32 left, u32 right) {
    if (left == NIL) return right;
    if (right == NIL) return left;
    if (node_at(o, left)->priority >= node_at(o, right)->priority) {
        u32 child = node_at(o, left)->right;
        u32 m = jazz_db__positional_order__PositionalOrder__merge(o, child, right);
        node_at(o, left)->right = m;
        jazz_db__positional_order__PositionalOrder__pull(o, left);
        return left;
    } else {
        u32 child = node_at(o, right)->left;
        u32 m = jazz_db__positional_order__PositionalOrder__merge(o, left, child);
        node_at(o, right)->left = m;
        jazz_db__positional_order__PositionalOrder__pull(o, right);
        return right;
    }
}

bool jazz_db__positional_order__PositionalOrder__insert(jazz_db_PositionalOrder *o, u32 position, void *key) {
    if (jz_btree_contains(&o->slots, key)) {
        o->k->drop(key);
        return false;
    }
    u32 len = jazz_db__positional_order__PositionalOrder__len(o);
    if (position > len) position = len;
    u32 node = o->nodes_len;
    if (node == NIL) jz_panic("positional order exceeds u32 nodes");
    o->seed += 0x9e3779b97f4a7c15ull;
    u64 priority = jazz_db__positional_order__splitmix64(o->seed);
    u8 slot_key[256];
    o->k->clone(slot_key, key);
    jz_btree_insert(&o->slots, slot_key, &node, NULL);
    reserve_nodes(o, 1);
    o->nodes_len++;
    jazz_db_PoNode *n = node_at(o, node);
    n->has_key = true;
    n->left = n->right = n->parent = NIL;
    n->size = 1;
    n->priority = priority;
    memcpy(node_key(o, node), key, o->k->key_size);
    u32 before, after;
    jazz_db__positional_order__PositionalOrder__split(o, o->root, position, &before, &after);
    u32 joined = jazz_db__positional_order__PositionalOrder__merge(o, before, node);
    o->root = jazz_db__positional_order__PositionalOrder__merge(o, joined, after);
    node_at(o, o->root)->parent = NIL;
    return true;
}

static void jazz_db__positional_order__PositionalOrder__release(jazz_db_PositionalOrder *o, u32 node) {
    u32 last = o->nodes_len - 1;
    /* swap_remove */
    if (node_at(o, node)->has_key) o->k->drop(node_key(o, node));
    if (node != last) memcpy(node_at(o, node), node_at(o, last), stride(o));
    o->nodes_len--;
    if (node == last) return;
    jazz_db_PoNode *moved = node_at(o, node);
    u32 parent = moved->parent, left = moved->left, right = moved->right;
    if (parent == NIL) {
        if (o->root == last) o->root = node;
    } else {
        jazz_db_PoNode *p = node_at(o, parent);
        if (p->left == last) p->left = node;
        else if (p->right == last) p->right = node;
    }
    if (left != NIL) node_at(o, left)->parent = node;
    if (right != NIL) node_at(o, right)->parent = node;
    if (node_at(o, node)->has_key) {
        u32 *slot = jz_btree_get(&o->slots, node_key(o, node));
        if (!slot) jz_panic("moved node retains its key slot");
        *slot = node;
    }
}

bool jazz_db__positional_order__PositionalOrder__remove(jazz_db_PositionalOrder *o, const void *key, u32 *out) {
    u32 position;
    if (!jazz_db__positional_order__PositionalOrder__position(o, key, &position)) return false;
    u32 node;
    if (!jz_btree_remove(&o->slots, key, NULL, &node)) jz_panic("positioned key has a slot");
    u32 before, rest, removed, after;
    jazz_db__positional_order__PositionalOrder__split(o, o->root, position, &before, &rest);
    jazz_db__positional_order__PositionalOrder__split(o, rest, 1, &removed, &after);
    (void)removed;
    o->root = jazz_db__positional_order__PositionalOrder__merge(o, before, after);
    if (o->root != NIL) node_at(o, o->root)->parent = NIL;
    jazz_db__positional_order__PositionalOrder__release(o, node);
    if (out) *out = position;
    return true;
}

static void jazz_db__positional_order__Iter__descend_left(jazz_db_PositionalOrderIter *it, u32 node) {
    while (node != NIL) {
        JZ_VEC_PUSH(it->stack, node);
        node = node_at(it->order, node)->left;
    }
}

void jazz_db__positional_order__PositionalOrder__iter(const jazz_db_PositionalOrder *o, jazz_db_PositionalOrderIter *it) {
    it->order = o;
    it->stack.ptr = NULL; it->stack.len = it->stack.cap = 0;
    jazz_db__positional_order__Iter__descend_left(it, o->root);
}

bool jazz_db__positional_order__Iter__next(jazz_db_PositionalOrderIter *it, const void **key) {
    if (it->stack.len == 0) return false;
    u32 node = it->stack.ptr[--it->stack.len];
    const jazz_db_PoNode *current = node_at(it->order, node);
    jazz_db__positional_order__Iter__descend_left(it, current->right);
    *key = current->has_key ? node_key(it->order, node) : NULL;
    return true;
}

void jazz_db__positional_order__Iter__drop(jazz_db_PositionalOrderIter *it) { jz_free(it->stack.ptr); }

const void *jazz_db__positional_order__PositionalOrder__index(const jazz_db_PositionalOrder *o, u32 position) {
    const void *k = jazz_db__positional_order__PositionalOrder__get(o, position);
    if (!k) {
        jz_str msg = jz_format("position %u out of bounds (len %u)", position, jazz_db__positional_order__PositionalOrder__len(o));
        jz_str_push_char(&msg, 0);
        jz_panic(msg.ptr);
    }
    return k;
}

bool jazz_db__positional_order__PositionalOrder__eq_vec(const jazz_db_PositionalOrder *o, const void *keys, u32 n) {
    if (jazz_db__positional_order__PositionalOrder__len(o) != n) return false;
    jazz_db_PositionalOrderIter it;
    jazz_db__positional_order__PositionalOrder__iter(o, &it);
    const void *key;
    u32 i = 0;
    bool eq = true;
    while (jazz_db__positional_order__Iter__next(&it, &key)) {
        if (!key || o->k->cmp(key, (const u8 *)keys + (usize)i * o->k->key_size, NULL) != 0) { eq = false; break; }
        i++;
    }
    jazz_db__positional_order__Iter__drop(&it);
    return eq;
}
