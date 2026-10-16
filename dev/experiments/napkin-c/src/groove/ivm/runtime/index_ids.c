/* groove::ivm::runtime::index_ids: durable numeric identities for schema indexes. */
#include "mod.h"

const char groove__ivm__runtime__index_ids__INDICES_CF[] = "indices";
static const u8 INDEX_ID_REGISTRY_PREFIX[] = "\0groove-index-id\0";
#define INDEX_ID_REGISTRY_PREFIX_LEN 17u
static const u8 INDEX_LAYOUT_MARKER_KEY[] = "\0groove-index-layout";
#define INDEX_LAYOUT_MARKER_KEY_LEN 20u
static const u8 INDEX_LAYOUT_MARKER_VALUE[] = "groove-durable-index-v2";
#define INDEX_LAYOUT_MARKER_VALUE_LEN 23u

static int pair_cmp(const void *a, const void *b, void *ctx) {
    (void)ctx;
    const groove_StrPair *x = a, *y = b;
    int c = jz_str_cmp(&x->a, &y->a);
    return c ? c : jz_str_cmp(&x->b, &y->b);
}
static void pair_drop(void *p) { groove_StrPair *s = p; jz_str_drop(&s->a); jz_str_drop(&s->b); }
static void entry_drop(void *p) { jz_bytes_drop(&((groove_IndexIdEntry *)p)->definition); }
static const jz_btree_desc ENTRIES = { sizeof(groove_StrPair), sizeof(groove_IndexIdEntry), pair_cmp, pair_drop, entry_drop };
static const jz_btree_desc NAMES = { sizeof(u32), sizeof(groove_StrPair), jz_cmp_u32, NULL, pair_drop };
static const jz_btree_desc PAIR_SET = { sizeof(groove_StrPair), 0, pair_cmp, pair_drop, NULL };

static groove_StrPair pair_clone(const groove_StrPair *p) { groove_StrPair r = { jz_str_clone(&p->a), jz_str_clone(&p->b) }; return r; }
static jz_str cf_name(void) { return jz_str_from(groove__ivm__runtime__index_ids__INDICES_CF); }

void groove__ivm__runtime__index_ids__IndexIdRegistry__default(groove_IndexIdRegistry *r) {
    jz_btree_init(&r->entries, &ENTRIES);
    jz_btree_init(&r->names, &NAMES);
    r->max_id = 0;
    jz_btree_init(&r->unpersisted, &PAIR_SET);
    r->marker_persisted = false;
}

void groove_IndexIdRegistry_drop(void *p) {
    groove_IndexIdRegistry *r = p;
    jz_btree_drop(&r->entries); jz_btree_drop(&r->names); jz_btree_drop(&r->unpersisted);
}

groove_SharedIndexIds groove__ivm__runtime__index_ids__SharedIndexIds__default(void) {
    groove_IndexIdRegistry *r = jz_rc_new(sizeof(groove_IndexIdRegistry), groove_IndexIdRegistry_drop);
    groove__ivm__runtime__index_ids__IndexIdRegistry__default(r);
    return r;
}

bool groove__ivm__runtime__index_ids__PendingIndexIds__is_empty(const groove_PendingIndexIds *p) { return p->operations.len == 0; }
const groove_WriteOpVec *groove__ivm__runtime__index_ids__PendingIndexIds__operations(const groove_PendingIndexIds *p) { return &p->operations; }

void groove_PendingIndexIds_drop(groove_PendingIndexIds *p) {
    jz_vec_drop(&p->operations, sizeof(groove_OwnedWriteOperation), groove_OwnedWriteOperation_drop);
    for (u32 i = 0; i < p->entries.len; i++) pair_drop(&p->entries.ptr[i].key);
    jz_vec_drop(&p->entries, sizeof(groove_PendingIndexEntry), NULL);
}

static jz_err malformed_key(void) { return groove__storage__Error__invalid_layout(jz_str_from("malformed durable index id key")); }

static jz_err decode_registry_key(jz_slice key, groove_StrPair *out) {
    if (key.len < INDEX_ID_REGISTRY_PREFIX_LEN || memcmp(key.ptr, INDEX_ID_REGISTRY_PREFIX, INDEX_ID_REGISTRY_PREFIX_LEN) != 0) return malformed_key();
    const u8 *rest = key.ptr + INDEX_ID_REGISTRY_PREFIX_LEN;
    u32 rest_len = key.len - INDEX_ID_REGISTRY_PREFIX_LEN;
    if (rest_len < 2) return malformed_key();
    u32 len = ((u32)rest[0] << 8) | rest[1];
    rest += 2; rest_len -= 2;
    if (rest_len < len) return malformed_key();
    if (!jz_utf8_valid(rest, len) || !jz_utf8_valid(rest + len, rest_len - len)) return malformed_key();
    out->a = jz_str_from_slice(jz_slice_of(rest, len));
    out->b = jz_str_from_slice(jz_slice_of(rest + len, rest_len - len));
    return JZ_OK;
}

static jz_err decode_registry_value(jz_slice value, u32 *id, jz_bytes *definition) {
    if (value.len < 4) return groove__storage__Error__invalid_layout(jz_str_from("malformed durable index id value"));
    *id = ((u32)value.ptr[0] << 24) | ((u32)value.ptr[1] << 16) | ((u32)value.ptr[2] << 8) | value.ptr[3];
    if (*id == 0) return groove__storage__Error__invalid_layout(jz_str_from("durable index id 0 is reserved"));
    *definition = jz_bytes_from(value.ptr + 4, value.len - 4);
    return JZ_OK;
}

static jz_bytes encode_registry_key(const jz_str *table, const jz_str *index) {
    jz_bytes key = jz_bytes_from(INDEX_ID_REGISTRY_PREFIX, INDEX_ID_REGISTRY_PREFIX_LEN);
    if (table->len > 0xffff) jz_panic("table names fit u16");
    u8 be[2] = { (u8)(table->len >> 8), (u8)table->len };
    jz_bytes_push(&key, be, 2);
    jz_bytes_push(&key, (const u8 *)table->ptr, table->len);
    jz_bytes_push(&key, (const u8 *)index->ptr, index->len);
    return key;
}

static jz_bytes encode_registry_value(u32 id, const jz_bytes *definition) {
    u8 be[4] = { (u8)(id >> 24), (u8)(id >> 16), (u8)(id >> 8), (u8)id };
    jz_bytes value = jz_bytes_from(be, 4);
    jz_bytes_push(&value, definition->ptr, definition->len);
    return value;
}

/* Canonical bytes of an index definition: unique flag, u16 BE column count,
 * then each column name as u16 BE length plus UTF-8. */
static jz_bytes index_definition_bytes(const groove_IndexSchema *index) {
    jz_bytes bytes = {0};
    jz_bytes_push_u8(&bytes, index->unique ? 1 : 0);
    if (index->columns.len > 0xffff) jz_panic("index column count fits u16");
    u8 be[2] = { (u8)(index->columns.len >> 8), (u8)index->columns.len };
    jz_bytes_push(&bytes, be, 2);
    for (u32 i = 0; i < index->columns.len; i++) {
        const jz_str *column = &index->columns.ptr[i];
        if (column->len > 0xffff) jz_panic("column names fit u16");
        u8 cl[2] = { (u8)(column->len >> 8), (u8)column->len };
        jz_bytes_push(&bytes, cl, 2);
        jz_bytes_push(&bytes, (const u8 *)column->ptr, column->len);
    }
    return bytes;
}

jz_err groove__ivm__runtime__index_ids__IndexIdRegistry__load(groove_Storage storage, groove_IndexIdRegistry *out) {
    jz_str cf = cf_name();
    bool found = false;
    jz_bytes marker = {0};
    groove__ivm__runtime__index_ids__IndexIdRegistry__default(out);
    jz_err e = storage.vt->get(storage.self, &cf, jz_slice_of(INDEX_LAYOUT_MARKER_KEY, INDEX_LAYOUT_MARKER_KEY_LEN), &found, &marker);
    if (JZ_IS_ERR(e)) {
        jz_str_drop(&cf);
        if (e.code == GROOVE_STORAGE_ColumnFamilyNotFound) { jz_err_drop(&e); return JZ_OK; }
        return e;
    }
    if (found) {
        bool ok = marker.len == INDEX_LAYOUT_MARKER_VALUE_LEN && memcmp(marker.ptr, INDEX_LAYOUT_MARKER_VALUE, marker.len) == 0;
        jz_bytes_drop(&marker);
        if (!ok) { jz_str_drop(&cf); return groove__storage__Error__invalid_layout(jz_str_from("unsupported durable-index layout marker")); }
        out->marker_persisted = true;
    } else {
        /* Metadata keys start with 0x00 and sort first, so the last key is an
         * index entry whenever any exists. */
        groove_ScanRequest req = groove__storage__ScanRequest__prefix(cf, (jz_bytes){0});
        groove__storage__ScanRequest__with_max_items(&req, 1);
        groove__storage__ScanRequest__reversed(&req);
        groove_KvPairVec last = {0};
        e = storage.vt->scan(storage.self, &req, &last);
        groove_ScanRequest_drop(&req);
        JZ_TRY(e);
        bool any = false;
        for (u32 i = 0; i < last.len; i++) any |= last.ptr[i].key.len == 0 || last.ptr[i].key.ptr[0] != 0;
        groove_KvPairVec_drop(&last);
        if (any) return groove__storage__Error__invalid_layout(jz_str_from("durable-index entries without the groove-durable-index-v2 layout marker"));
        return JZ_OK;
    }
    groove_ScanRequest req = groove__storage__ScanRequest__prefix(cf, jz_bytes_from(INDEX_ID_REGISTRY_PREFIX, INDEX_ID_REGISTRY_PREFIX_LEN));
    groove_KvPairVec entries = {0};
    e = storage.vt->scan(storage.self, &req, &entries);
    groove_ScanRequest_drop(&req);
    JZ_TRY(e);
    for (u32 i = 0; i < entries.len; i++) {
        groove_StrPair key;
        u32 id; jz_bytes definition;
        e = decode_registry_key(jz_bytes_slice(&entries.ptr[i].key), &key);
        if (!JZ_IS_ERR(e)) {
            e = decode_registry_value(jz_bytes_slice(&entries.ptr[i].value), &id, &definition);
            if (JZ_IS_ERR(e)) pair_drop(&key);
        }
        if (JZ_IS_ERR(e)) { groove_KvPairVec_drop(&entries); return e; }
        groove_StrPair name = pair_clone(&key);
        groove_StrPair old;
        if (jz_btree_insert(&out->names, &id, &name, &old)) {
            pair_drop(&old); pair_drop(&key); jz_bytes_drop(&definition);
            groove_KvPairVec_drop(&entries);
            jz_str msg = jz_format("durable index id %u is assigned twice", id);
            return groove__storage__Error__invalid_layout(msg);
        }
        if (id > out->max_id) out->max_id = id;
        groove_IndexIdEntry entry = { id, definition };
        jz_btree_insert(&out->entries, &key, &entry, NULL);
    }
    groove_KvPairVec_drop(&entries);
    return JZ_OK;
}

/* Return the id for `index` on `table`, allocating a fresh id when the pair
 * is unknown or its stored definition differs. */
u32 groove__ivm__runtime__index_ids__IndexIdRegistry__resolve(groove_IndexIdRegistry *r, const char *table, u32 table_len, const groove_IndexSchema *index) {
    jz_bytes definition = index_definition_bytes(index);
    groove_StrPair key = { jz_str_from_slice(jz_slice_of(table, table_len)), jz_str_clone(&index->name) };
    groove_IndexIdEntry *entry = jz_btree_get(&r->entries, &key);
    if (entry && jz_bytes_eq(&entry->definition, &definition)) {
        u32 id = entry->id;
        pair_drop(&key); jz_bytes_drop(&definition);
        return id;
    }
    if (r->max_id == UINT32_MAX) jz_panic("durable index ids must not wrap");
    u32 id = r->max_id + 1;
    r->max_id = id;
    groove_IndexIdEntry fresh = { id, definition }, previous;
    groove_StrPair k1 = pair_clone(&key);
    if (jz_btree_insert(&r->entries, &k1, &fresh, &previous)) {
        /* The superseded id stays retired: max_id never decreases. */
        jz_btree_remove(&r->names, &previous.id, NULL, NULL);
        entry_drop(&previous);
    }
    groove_StrPair k2 = pair_clone(&key);
    jz_btree_insert(&r->names, &id, &k2, NULL);
    jz_btree_insert(&r->unpersisted, &key, NULL, NULL);
    return id;
}

void groove__ivm__runtime__index_ids__IndexIdRegistry__retire(groove_IndexIdRegistry *r, const char *table, u32 table_len, const jz_str *index) {
    groove_StrPair key = { jz_str_from_slice(jz_slice_of(table, table_len)), jz_str_clone(index) };
    groove_IndexIdEntry previous;
    if (jz_btree_remove(&r->entries, &key, NULL, &previous)) {
        jz_btree_remove(&r->names, &previous.id, NULL, NULL);
        jz_btree_remove(&r->unpersisted, &key, NULL, NULL);
        entry_drop(&previous);
    }
    pair_drop(&key);
}

bool groove__ivm__runtime__index_ids__IndexIdRegistry__names(const groove_IndexIdRegistry *r, u32 id, const jz_str **table, const jz_str **index) {
    const groove_StrPair *p = jz_btree_get(&r->names, &id);
    if (!p) return false;
    *table = &p->a; *index = &p->b;
    return true;
}

void groove__ivm__runtime__index_ids__IndexIdRegistry__pending(const groove_IndexIdRegistry *r, groove_PendingIndexIds *out) {
    memset(out, 0, sizeof(*out));
    if (r->unpersisted.len == 0) return;
    if (!r->marker_persisted) {
        out->marker = true;
        groove_OwnedWriteOperation op = { groove_WriteOp_Set, cf_name(),
            jz_bytes_from(INDEX_LAYOUT_MARKER_KEY, INDEX_LAYOUT_MARKER_KEY_LEN),
            jz_bytes_from(INDEX_LAYOUT_MARKER_VALUE, INDEX_LAYOUT_MARKER_VALUE_LEN) };
        JZ_VEC_PUSH(out->operations, op);
    }
    groove_StrPair *key; void *unit;
    JZ_BTREE_FOR(&r->unpersisted, key, unit) {
        const groove_IndexIdEntry *entry = jz_btree_get(&r->entries, key);
        if (!entry) jz_panic("unpersisted index id has an entry");
        groove_OwnedWriteOperation op = { groove_WriteOp_Set, cf_name(), encode_registry_key(&key->a, &key->b), encode_registry_value(entry->id, &entry->definition) };
        JZ_VEC_PUSH(out->operations, op);
        groove_PendingIndexEntry pe = { pair_clone(key), entry->id };
        JZ_VEC_PUSH(out->entries, pe);
    }
}

void groove__ivm__runtime__index_ids__IndexIdRegistry__mark_persisted(groove_IndexIdRegistry *r, const groove_PendingIndexIds *p) {
    if (p->marker) r->marker_persisted = true;
    for (u32 i = 0; i < p->entries.len; i++) {
        const groove_IndexIdEntry *entry = jz_btree_get(&r->entries, &p->entries.ptr[i].key);
        if (entry && entry->id == p->entries.ptr[i].id) jz_btree_remove(&r->unpersisted, &p->entries.ptr[i].key, NULL, NULL);
    }
}

/* The storage-key prefix of one durable index: the minimal unsigned LEB128
 * encoding of its id. */
jz_bytes groove__ivm__runtime__index_ids__durable_index_key_prefix(u32 id) {
    jz_bytes prefix = {0};
    jz_vec_reserve(&prefix, 5, 1);
    u32 rest = id;
    for (;;) {
        u8 byte = (u8)(rest & 0x7f);
        rest >>= 7;
        if (rest == 0) { jz_bytes_push_u8(&prefix, byte); return prefix; }
        jz_bytes_push_u8(&prefix, byte | 0x80);
    }
}

/* Split a durable index storage key into its id and logical key. Returns
 * false for metadata keys (first byte 0x00) and malformed or non-minimal ids. */
bool groove__ivm__runtime__index_ids__split_durable_index_key(jz_slice key, u32 *out_id, jz_slice *rest) {
    u32 id = 0;
    for (u32 position = 0; position < key.len && position < 5; position++) {
        u8 byte = key.ptr[position];
        u32 bits = byte & 0x7f;
        u32 shift = 7 * position;
        if (position == 4 && bits > 0x0f) return false;
        id |= bits << shift;
        if ((byte & 0x80) == 0) {
            if ((position > 0 && byte == 0) || id == 0) return false;
            *out_id = id;
            *rest = jz_slice_of(key.ptr + position + 1, key.len - position - 1);
            return true;
        }
    }
    return false;
}
