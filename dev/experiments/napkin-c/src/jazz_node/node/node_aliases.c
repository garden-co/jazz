/* Port of crates/jazz/layers/node/src/node/node_aliases.rs */
#include "jz_rt.h"
#include "jazz_node/_ext_node-core-b.h"
#include "jazz_node/node/mod.h"

static u64 alias_hash(const void *k) { return jz_hash_u64(*(const u64 *)k); }
static bool alias_eq(const void *a, const void *b) { return *(const u64 *)a == *(const u64 *)b; }
static const jz_hmap_desc desc_by_alias = { sizeof(jazz_types_NodeAlias), sizeof(jazz_types_NodeUuid), alias_hash, alias_eq, NULL, NULL };
static const jz_btree_desc desc_by_node = { sizeof(jazz_types_NodeUuid), sizeof(jazz_types_NodeAlias), jz_cmp_uuid, NULL, NULL };

void jazz_node__node__node_aliases__NodeAliases__init(jazz_node_NodeAliases *a) {
    jz_btree_init(&a->by_node, &desc_by_node);
    jz_hmap_init(&a->by_alias, &desc_by_alias);
    a->max_alias = 0;
    a->retargets = 0;
}

const jazz_types_NodeAlias *jazz_node__node__node_aliases__NodeAliases__get(const jazz_node_NodeAliases *a, const jazz_types_NodeUuid *n) {
    return jz_btree_get(&a->by_node, n);
}

bool jazz_node__node__node_aliases__NodeAliases__node_for_alias(const jazz_node_NodeAliases *a, jazz_types_NodeAlias alias, jazz_types_NodeUuid *out) {
    const jazz_types_NodeUuid *n = jz_hmap_get(&a->by_alias, &alias);
    if (!n) return false;
    *out = *n;
    return true;
}

bool jazz_node__node__node_aliases__NodeAliases__insert(jazz_node_NodeAliases *a, jazz_types_NodeUuid node, jazz_types_NodeAlias alias, jazz_types_NodeAlias *prev) {
    if (alias > a->max_alias) a->max_alias = alias;
    jazz_types_NodeUuid previous_node;
    if (jz_hmap_insert(&a->by_alias, &alias, &node, &previous_node) && !jz_uuid_eq(&previous_node, &node)) {
        jz_btree_remove(&a->by_node, &previous_node, NULL, NULL);
        a->retargets += 1;
    }
    jazz_types_NodeAlias previous_alias;
    bool had = jz_btree_insert(&a->by_node, &node, &alias, &previous_alias);
    if (had && previous_alias != alias) {
        jz_hmap_remove(&a->by_alias, &previous_alias, NULL, NULL);
        a->retargets += 1;
    }
    if (had && prev) *prev = previous_alias;
    return had;
}

u64 jazz_node__node__node_aliases__NodeAliases__retarget_count(const jazz_node_NodeAliases *a) { return a->retargets; }
u64 jazz_node__node__node_aliases__NodeAliases__max_alias(const jazz_node_NodeAliases *a) { return a->max_alias; }
usize jazz_node__node__node_aliases__NodeAliases__len(const jazz_node_NodeAliases *a) { return a->by_node.len; }

void jazz_node__node__node_aliases__NodeAliases__iter(jz_btree_iter *it, const jazz_node_NodeAliases *a) {
    jz_btree_iter_begin(it, &a->by_node);
}

static bool alias_val_eq(const void *a, const void *b) { return *(const u64 *)a == *(const u64 *)b; }
bool jazz_node__node__node_aliases__NodeAliases__PartialEq__eq(const jazz_node_NodeAliases *a, const jazz_node_NodeAliases *b) {
    return jz_btree_eq(&a->by_node, &b->by_node, alias_val_eq);
}

const jazz_types_NodeAlias *jazz_node__node__node_aliases__NodeAliases__Index__index(const jazz_node_NodeAliases *a, const jazz_types_NodeUuid *n) {
    const jazz_types_NodeAlias *v = jz_btree_get(&a->by_node, n);
    if (!v) jz_panic("key not found in map");
    return v;
}

void jazz_node__node__node_aliases__NodeAliases__FromIterator__from_iter(jazz_node_NodeAliases *out, const jazz_types_NodeUuid *nodes,
                                                                       const jazz_types_NodeAlias *aliases, u32 n) {
    jazz_node__node__node_aliases__NodeAliases__init(out);
    for (u32 i = 0; i < n; i++) jazz_node__node__node_aliases__NodeAliases__insert(out, nodes[i], aliases[i], NULL);
}

void jazz_node__node__node_aliases__NodeAliases__clone(jazz_node_NodeAliases *dst, const jazz_node_NodeAliases *src) {
    jazz_node__node__node_aliases__NodeAliases__init(dst);
    jazz_types_NodeUuid *k; jazz_types_NodeAlias *v;
    JZ_BTREE_FOR(&src->by_node, k, v) {
        jz_btree_insert(&dst->by_node, k, v, NULL);
        jz_hmap_insert(&dst->by_alias, v, k, NULL);
    }
    dst->max_alias = src->max_alias;
    dst->retargets = src->retargets;
}
