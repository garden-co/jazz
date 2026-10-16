/* routes.c: port of crates/groove/src/ivm/routes.rs.
 * Route index for prepared-shape bindings (#3288): a route barrier marks a
 * per-binding equality filter below a shared terminal; activation stops there
 * and the tick activates only barriers whose key occurs in the delta. */
#include "jz_rt.h"
#include "../_ext_groove-ivm-db.h"
#include "routes.h"

static u64 nodeid_hash_v(const void *k) { return jz_hash_u64(*(const u64 *)k); }
static bool nodeid_eq_v(const void *a, const void *b) { return *(const u64 *)a == *(const u64 *)b; }
static u64 bytes_hash_v(const void *k) { const jz_bytes *b = k; return jz_hash_bytes(b->ptr, b->len); }
static bool bytes_eq_v(const void *a, const void *b) { return jz_bytes_eq((const jz_bytes *)a, (const jz_bytes *)b); }
static void bytes_drop_v(void *p) { jz_bytes_drop((jz_bytes *)p); }
static void nodeset_drop_v(void *p) { jz_hmap_drop((jz_hmap *)p); }
static void route_table_drop_v(void *p) { groove__ivm__routes__RouteTable__drop((groove_RouteTable *)p); }
static void barrier_drop_v(void *p) { jz_bytes_drop(&((groove_RouteBarrier *)p)->key); }
static void value_type_drop_v(void *p) { groove__records__values__ValueType__drop((groove_ValueType *)p); }

static const jz_hmap_desc BY_KEY_DESC = { sizeof(jz_bytes), sizeof(jz_hmap), bytes_hash_v, bytes_eq_v, bytes_drop_v, nodeset_drop_v };
static const jz_hmap_desc TABLES_DESC = { sizeof(u64), sizeof(groove_RouteTable), nodeid_hash_v, nodeid_eq_v, NULL, route_table_drop_v };
static const jz_hmap_desc BARRIERS_DESC = { sizeof(u64), sizeof(groove_RouteBarrier), nodeid_hash_v, nodeid_eq_v, NULL, barrier_drop_v };

/* ---------------- RouteTable ---------------- */
static void RouteTable__Default__default(groove_RouteTable *t) {
    memset(t, 0, sizeof *t);
    jz_hmap_init(&t->by_key, &BY_KEY_DESC);
    jz_hmap_init(&t->root_ordering_nodes, &groove__ivm__NODESET_DESC);
}
void groove__ivm__routes__RouteTable__drop(groove_RouteTable *self) {
    jz_vec_drop(&self->field_indices, sizeof(u32), NULL);
    jz_vec_drop(&self->field_types, sizeof(groove_ValueType), value_type_drop_v);
    jz_hmap_drop(&self->by_key);
    jz_hmap_drop(&self->root_ordering_nodes);
}
bool groove__ivm__routes__RouteTable__key_of_record(const groove_RouteTable *self, const groove_BorrowedRecord *record, jz_bytes *out) {
    groove_ValueVec values = {0};
    jz_vec_reserve(&values, self->field_indices.len, sizeof(groove_Value));
    for (u32 i = 0; i < self->field_indices.len; i++) {
        groove_Value v;
        jz_err e = groove__records__BorrowedRecord__get_idx(record, self->field_indices.ptr[i], &v);
        if (e.code) {
            jz_err_drop(&e);
            jz_vec_drop(&values, sizeof(groove_Value), groove__records__values__Value__drop_v);
            return false;
        }
        JZ_VEC_PUSH(values, v);
    }
    bool ok = groove__ivm__routes__encode_route_key(values.ptr, values.len, self->field_types.ptr, self->field_types.len, out);
    jz_vec_drop(&values, sizeof(groove_Value), groove__records__values__Value__drop_v);
    return ok;
}
void groove__ivm__routes__RouteTable__barriers(const groove_RouteTable *self, groove_NodeIdVec *out) {
    memset(out, 0, sizeof *out);
    jz_bytes *key; jz_hmap *nodes;
    JZ_HMAP_FOR(&self->by_key, key, nodes) {
        u64 *id; void *unused;
        JZ_HMAP_FOR(nodes, id, unused) JZ_VEC_PUSH(*out, *id);
    }
}

/* ---------------- RouteIndex ---------------- */
void groove__ivm__routes__RouteIndex__Default__default(groove_RouteIndex *self) {
    jz_hmap_init(&self->tables, &TABLES_DESC);
    jz_hmap_init(&self->barriers, &BARRIERS_DESC);
}
void groove__ivm__routes__RouteIndex__drop(groove_RouteIndex *self) {
    jz_hmap_drop(&self->tables);
    jz_hmap_drop(&self->barriers);
}
bool groove__ivm__routes__RouteIndex__is_barrier(const groove_RouteIndex *self, groove_NodeId node) {
    return jz_hmap_get(&self->barriers, &node) != NULL;
}
const groove_RouteTable *groove__ivm__routes__RouteIndex__table(const groove_RouteIndex *self, groove_NodeId terminal) {
    return jz_hmap_get(&self->tables, &terminal);
}
bool groove__ivm__routes__RouteIndex__has_table(const groove_RouteIndex *self, groove_NodeId terminal) {
    return jz_hmap_get(&self->tables, &terminal) != NULL;
}
bool groove__ivm__routes__RouteIndex__add(groove_RouteIndex *self, groove_NodeId terminal, groove_NodeId barrier,
                                          groove_UsizeVec field_indices, groove_ValueTypeVec field_types, jz_bytes key,
                                          bool has_root_ordering_node, groove_NodeId root_ordering_node) {
    groove_RouteBarrier *existing = jz_hmap_get(&self->barriers, &barrier);
    if (existing) {
        existing->refs += 1;
        jz_vec_drop(&field_indices, sizeof(u32), NULL);
        jz_vec_drop(&field_types, sizeof(groove_ValueType), value_type_drop_v);
        jz_bytes_drop(&key);
        return false;
    }
    bool inserted;
    groove_RouteTable *table = jz_hmap_entry(&self->tables, &terminal, &inserted);
    if (inserted) RouteTable__Default__default(table);
    if (table->field_indices.len == 0) {
        jz_vec_drop(&table->field_indices, sizeof(u32), NULL);
        jz_vec_drop(&table->field_types, sizeof(groove_ValueType), value_type_drop_v);
        table->field_indices = field_indices;
        table->field_types = field_types;
    } else {
        jz_vec_drop(&field_indices, sizeof(u32), NULL);
        jz_vec_drop(&field_types, sizeof(groove_ValueType), value_type_drop_v);
    }
    jz_bytes key_copy = jz_bytes_clone(&key);
    bool key_inserted;
    jz_hmap *nodes = jz_hmap_entry(&table->by_key, &key_copy, &key_inserted);
    if (key_inserted) jz_hmap_init(nodes, &groove__ivm__NODESET_DESC);
    else jz_bytes_drop(&key_copy);
    jz_hmap_insert(nodes, &barrier, NULL, NULL);
    if (has_root_ordering_node) jz_hmap_insert(&table->root_ordering_nodes, &root_ordering_node, NULL, NULL);
    groove_RouteBarrier entry = { terminal, key, 1 };
    jz_hmap_insert(&self->barriers, &barrier, &entry, NULL);
    return true;
}
bool groove__ivm__routes__RouteIndex__release(groove_RouteIndex *self, groove_NodeId barrier) {
    groove_RouteBarrier *entry = jz_hmap_get(&self->barriers, &barrier);
    if (!entry) return false;
    entry->refs -= 1;
    if (entry->refs > 0) return false;
    return groove__ivm__routes__RouteIndex__remove(self, barrier);
}
bool groove__ivm__routes__RouteIndex__remove(groove_RouteIndex *self, groove_NodeId barrier) {
    groove_NodeId k;
    groove_RouteBarrier entry;
    if (!jz_hmap_remove(&self->barriers, &barrier, &k, &entry)) return false;
    groove_RouteTable *table = jz_hmap_get(&self->tables, &entry.terminal);
    if (table) {
        jz_hmap *nodes = jz_hmap_get(&table->by_key, &entry.key);
        if (nodes) {
            jz_hmap_remove(nodes, &barrier, NULL, NULL);
            if (nodes->len == 0) jz_hmap_remove(&table->by_key, &entry.key, NULL, NULL);
        }
        if (table->by_key.len == 0) jz_hmap_remove(&self->tables, &entry.terminal, NULL, NULL);
    }
    jz_bytes_drop(&entry.key);
    return true;
}

bool groove__ivm__routes__is_routable_type(const groove_ValueType *value_type) {
    switch (value_type->tag) {
    case GROOVE_VT_U8: case GROOVE_VT_U16: case GROOVE_VT_U32: case GROOVE_VT_U64: case GROOVE_VT_U48:
    case GROOVE_VT_I32: case GROOVE_VT_I64: case GROOVE_VT_BOOL: case GROOVE_VT_UUID:
        return true;
    case GROOVE_VT_NULLABLE:
        return groove__ivm__routes__is_routable_type(value_type->u.inner);
    default:
        return false;
    }
}

bool groove__ivm__routes__encode_route_key(const groove_Value *values, u32 n_values, const groove_ValueType *types, u32 n_types, jz_bytes *out) {
    if (n_values != n_types) return false;
    jz_bytes key = {0};
    for (u32 i = 0; i < n_values; i++) {
        jz_bytes encoded;
        jz_err e = groove__records__encode_single_field_value(&values[i], &types[i], &encoded);
        if (e.code) { jz_err_drop(&e); jz_bytes_drop(&key); return false; }
        u32 len = encoded.len; /* (len as u32).to_le_bytes(); wasm32 is little-endian */
        jz_bytes_push(&key, (const u8 *)&len, 4);
        jz_bytes_push(&key, encoded.ptr, encoded.len);
        jz_bytes_drop(&encoded);
    }
    *out = key;
    return true;
}
