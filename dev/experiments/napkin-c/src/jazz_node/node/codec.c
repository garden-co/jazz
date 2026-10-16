/* codec.c: jazz_node::node::codec (node/codec.rs).
 * Storage and wire codec helpers for node-owned records: groove record
 * layouts (define_record! accessors/encoders are expanded by hand where used),
 * typed row wrappers, storage-key/value construction, the fixed catalogue
 * kernel payloads, and conversion between Jazz protocol records and groove
 * bytes. */
#include "codec.h"

static void jzn_str_drop_v(void *p) { jz_str_drop((jz_str *)p); }
static void jzn_value_drop_v(void *p) { groove_Value_drop(p); }
static void jzn_btree_drop_v(void *p) { jz_btree_drop((jz_btree *)p); }
static void jzn_values_drop(groove_ValueVec *v) { jz_vec_drop(v, sizeof(groove_Value), groove_Value_drop); }

/* define_record! PREFIX_FIELD_NAMES */
const char *const jzn_HistoryRowRecord_PREFIX_FIELD_NAMES[10] = {
    "branch_key", "row_uuid", "tx_time", "tx_node_id", "schema_version", "created_by", "created_at", "updated_by", "updated_at", "_deletion",
};
const char *const jzn_GlobalCurrentRowRecord_PREFIX_FIELD_NAMES[11] = {
    "branch_key", "row_uuid", "tx_time", "tx_node_id", "schema_version", "created_by", "created_at", "updated_by", "updated_at",
    "global_time", "_deletion",
};
const char *const jzn_RejectedVersionRowRecord_PREFIX_FIELD_NAMES[4] = { "tx_time", "tx_node_id", "row_uuid", "_deletion" };

/* Descriptor from (name, type) pairs: RecordDescriptor::new([...]). Types are moved. */
static groove_RecordDescriptor jzn_descriptor(const char *const *names, groove_ValueType *types, u32 n) {
    groove_NamedValueTypeVec fields = {0};
    for (u32 i = 0; i < n; i++) {
        groove_NamedValueType f = { jz_str_from(names[i]), types[i] };
        JZ_VEC_PUSH(fields, f);
    }
    return groove__records__RecordDescriptor__new(fields);
}

/* define_record!::encode: descriptor.create(values) -> OwnedRecord (values consumed). */
static jz_err jzn_encode_record(groove_RecordDescriptor d, groove_Value *values, u32 n, groove_OwnedRecord *out) {
    jz_bytes raw;
    jz_err err = groove__records__RecordDescriptor__create(d, values, n, &raw);
    for (u32 i = 0; i < n; i++) groove_Value_drop(&values[i]);
    if (JZ_IS_ERR(err)) return err;
    out->raw = raw;
    out->descriptor = d;
    return JZ_OK;
}

static groove_Value jzn_v_bytes(jz_bytes b) { groove_Value v; v.tag = GROOVE_V_BYTES; v.v.bytes = b; return v; }
static groove_Value jzn_v_uuid(jz_uuid u) { groove_Value v; v.tag = GROOVE_V_UUID; v.v.uuid = u; return v; }
static groove_Value jzn_v_u8(u8 x) { groove_Value v; v.tag = GROOVE_V_U8; v.v.u = x; return v; }
static groove_Value jzn_v_u32(u32 x) { groove_Value v; v.tag = GROOVE_V_U32; v.v.u = x; return v; }
static groove_Value jzn_v_bool(bool x) { groove_Value v; v.tag = GROOVE_V_BOOL; v.v.b = x; return v; }
static groove_Value jzn_v_string(jz_str s) { groove_Value v; v.tag = GROOVE_V_STRING; v.v.s = s; return v; }
static groove_Value jzn_v_array(groove_ValueVec a) { groove_Value v; v.tag = GROOVE_V_ARRAY; v.v.vals = a; return v; }
static groove_Value jzn_v_record(groove_OwnedRecord r) { groove_Value v; v.tag = GROOVE_V_RECORD; v.v.record = r; return v; }
static groove_Value jzn_v_enum(u32 tag, groove_OwnedRecord r) { groove_Value v; v.tag = GROOVE_V_ENUM; v.v.en.tag = tag; v.v.en.record = r; return v; }
static groove_Value jzn_v_enum_tag(u8 tag) { groove_Value v; v.tag = GROOVE_V_ENUM_TAG; v.v.u = 0; v.v.enum_tag = tag; return v; }

/* ======================= TouchedRows ======================= */

static int jzn_touched_key_cmp(const void *a, const void *b, void *ctx) {
    const jazz_node_TouchedRowsKey *x = a, *y = b;
    (void)ctx;
    if (x->table != y->table) return x->table < y->table ? -1 : 1;
    return jz_bytes_cmp(&x->branch_key, &y->branch_key);
}
static void jzn_touched_key_drop(void *p) { jz_bytes_drop(&((jazz_node_TouchedRowsKey *)p)->branch_key); }
static const jz_btree_desc jzn_desc_uuid_set = { sizeof(jz_uuid), 0, jz_cmp_uuid, NULL, NULL };
static const jz_btree_desc jzn_desc_touched = { sizeof(jazz_node_TouchedRowsKey), sizeof(jz_btree), jzn_touched_key_cmp, jzn_touched_key_drop, jzn_btree_drop_v };

void jazz_node_TouchedRows_default(jazz_node_TouchedRows *t) { jz_btree_init(&t->map, &jzn_desc_touched); }
void jazz_node_TouchedRows_drop(void *t) { jz_btree_drop(&((jazz_node_TouchedRows *)t)->map); }

static jz_btree *jzn_touched_group(jazz_node_TouchedRows *t, jazz_types_PhysicalTableId table, jz_bytes branch_key /* moved */) {
    jazz_node_TouchedRowsKey key = { table, branch_key };
    bool inserted;
    jz_btree *rows = jz_btree_entry(&t->map, &key, &inserted);
    if (inserted) jz_btree_init(rows, &jzn_desc_uuid_set);
    else jz_bytes_drop(&key.branch_key);
    return rows;
}

void jazz_node__node__codec__TouchedRows__insert(jazz_node_TouchedRows *t, jazz_types_PhysicalTableId table, jz_bytes branch_key,
                                                jazz_types_RowUuid row) {
    jz_btree_insert(jzn_touched_group(t, table, branch_key), &row, NULL, NULL);
}

void jazz_node__node__codec__TouchedRows__extend(jazz_node_TouchedRows *t, const jazz_node_TouchedRows *other) {
    const jazz_node_TouchedRowsKey *key; const jz_btree *rows;
    JZ_BTREE_FOR(&other->map, key, rows) {
        jz_btree *dst = jzn_touched_group(t, key->table, jz_bytes_clone(&key->branch_key));
        const jz_uuid *row; const void *unused;
        JZ_BTREE_FOR(rows, row, unused) jz_btree_insert(dst, row, NULL, NULL);
    }
}

/* Every touched row in (table, branch, row) order; `f` returns false to stop. */
void jazz_node__node__codec__TouchedRows__iter(const jazz_node_TouchedRows *t, jzn_touched_row_fn f, void *ctx) {
    const jazz_node_TouchedRowsKey *key; const jz_btree *rows;
    JZ_BTREE_FOR(&t->map, key, rows) {
        const jz_uuid *row; const void *unused;
        JZ_BTREE_FOR(rows, row, unused) {
            if (!f(ctx, key->table, jz_bytes_slice(&key->branch_key), *row)) return;
        }
    }
}

static groove_RecordDescriptor jazz_node__node__codec__TouchedRows__group_descriptor(void) {
    static const char *const names[3] = { "physical_table_id", "branch_key", "row_uuids" };
    groove_ValueType types[3] = {
        groove_vt_simple(GROOVE_VT_U64), groove_vt_simple(GROOVE_VT_BYTES),
        groove__records__values__ValueType__array_of(groove_vt_simple(GROOVE_VT_UUID)),
    };
    return jzn_descriptor(names, types, 3);
}

/* The canonical list. */
jz_err jazz_node__node__codec__TouchedRows__to_value(const jazz_node_TouchedRows *t, groove_Value *out) {
    groove_RecordDescriptor descriptor = jazz_node__node__codec__TouchedRows__group_descriptor();
    groove_ValueVec groups = {0};
    const jazz_node_TouchedRowsKey *key; const jz_btree *rows;
    JZ_BTREE_FOR(&t->map, key, rows) {
        groove_ValueVec uuids = {0};
        const jz_uuid *row; const void *unused;
        JZ_BTREE_FOR(rows, row, unused) JZ_VEC_PUSH(uuids, jzn_v_uuid(*row));
        groove_Value values[3] = { groove_value_u64(key->table), jzn_v_bytes(jz_bytes_clone(&key->branch_key)), jzn_v_array(uuids) };
        groove_OwnedRecord record;
        jz_err err = jzn_encode_record(descriptor, values, 3, &record);
        if (JZ_IS_ERR(err)) { jzn_values_drop(&groups); return err; }
        JZ_VEC_PUSH(groups, jzn_v_record(record));
    }
    *out = jzn_v_array(groups);
    return JZ_OK;
}

static jz_err jazz_node__node__codec__TouchedRows__from_value(const groove_Value *value, jazz_node_TouchedRows *out);

/* Decode a stored jazz_tx_touched_rows record's list, refusing any
 * non-canonical spelling. */
jz_err jazz_node__node__codec__TouchedRows__from_list_record(const groove_BorrowedRecord *r, jazz_node_TouchedRows *out) {
    groove_Value v;
    JZ_TRY(groove__records__BorrowedRecord__get_idx(r, 2 /* TX_TOUCHED_ROWS_LIST_IDX */, &v));
    jz_err err = jazz_node__node__codec__TouchedRows__from_value(&v, out);
    groove_Value_drop(&v);
    return err;
}

/* Values of the jazz_tx_touched_rows record listing these rows. */
jz_err jazz_node__node__codec__TouchedRows__list_record_values(const jazz_node_TouchedRows *t, jazz_types_TxTime tx_time,
                                                              jazz_types_NodeAlias alias, groove_ValueVec *out) {
    groove_Value list;
    JZ_TRY(jazz_node__node__codec__TouchedRows__to_value(t, &list));
    groove_ValueVec values = {0};
    JZ_VEC_PUSH(values, groove_value_u64(tx_time));
    JZ_VEC_PUSH(values, groove_value_u64(alias));
    JZ_VEC_PUSH(values, list);
    *out = values;
    return JZ_OK;
}

static jz_err jazz_node__node__codec__TouchedRows__from_value(const groove_Value *value, jazz_node_TouchedRows *out) {
    if (value->tag != GROOVE_V_ARRAY) return JZN_ISV("transaction touched_rows must be an array");
    jazz_node_TouchedRows touched;
    jazz_node_TouchedRows_default(&touched);
    bool has_previous = false;
    jazz_node_TouchedRowsKey previous = { 0, {0} };
    jz_err err = JZ_OK;
    for (u32 g = 0; g < value->v.vals.len; g++) {
        const groove_Value *group_value = &value->v.vals.ptr[g];
        if (group_value->tag != GROOVE_V_RECORD) { err = JZN_ISV("transaction touched_rows group must be a record"); break; }
        groove_BorrowedRecord group = groove_OwnedRecord_borrowed(&group_value->v.record);
        u64 table;
        jz_slice branch;
        err = groove__records__BorrowedRecord__get_u64(&group, 0, &table);
        if (!JZ_IS_ERR(err)) err = groove__records__BorrowedRecord__get_bytes(&group, 1, &branch);
        if (JZ_IS_ERR(err)) break;
        jazz_node_TouchedRowsKey key = { table, jz_bytes_from(branch.ptr, branch.len) };
        groove_Value rows;
        err = groove__records__BorrowedRecord__get_idx(&group, 2, &rows);
        if (JZ_IS_ERR(err)) { jz_bytes_drop(&key.branch_key); break; }
        if (rows.tag != GROOVE_V_ARRAY) {
            groove_Value_drop(&rows); jz_bytes_drop(&key.branch_key);
            err = JZN_ISV("transaction touched_rows group rows must be an array");
            break;
        }
        jz_btree uuids;
        jz_btree_init(&uuids, &jzn_desc_uuid_set);
        const jz_uuid *last = NULL;
        for (u32 i = 0; i < rows.v.vals.len; i++) {
            const groove_Value *row = &rows.v.vals.ptr[i];
            if (row->tag != GROOVE_V_UUID) { err = JZN_ISV("transaction touched row must be a UUID"); break; }
            if (last && jz_uuid_cmp(last, &row->v.uuid) >= 0) { err = JZN_ISV("transaction touched rows are not strictly increasing"); break; }
            last = &row->v.uuid;
            jz_btree_insert(&uuids, &row->v.uuid, NULL, NULL);
        }
        groove_Value_drop(&rows);
        if (!JZ_IS_ERR(err) && (uuids.len == 0 || (has_previous && jzn_touched_key_cmp(&previous, &key, NULL) >= 0)))
            err = JZN_ISV("transaction touched_rows groups are empty or not strictly increasing");
        if (JZ_IS_ERR(err)) { jz_btree_drop(&uuids); jz_bytes_drop(&key.branch_key); break; }
        if (has_previous) jz_bytes_drop(&previous.branch_key);
        previous.table = key.table;
        previous.branch_key = jz_bytes_clone(&key.branch_key);
        has_previous = true;
        jz_btree old;
        if (jz_btree_insert(&touched.map, &key, &uuids, &old)) jz_btree_drop(&old);
    }
    if (has_previous) jz_bytes_drop(&previous.branch_key);
    if (JZ_IS_ERR(err)) { jazz_node_TouchedRows_drop(&touched); return err; }
    *out = touched;
    return JZ_OK;
}

/* ======================= CatalogueRecordKind ======================= */

u64 jazz_node__node__codec__CatalogueRecordKind__key(u32 kind) {
    switch (kind) {
    case JZN_CRK_GENESIS: return 0;
    case JZN_CRK_SCHEMA: return 1;
    case JZN_CRK_LENS: return 2;
    case JZN_CRK_SCHEMA_LINEAGE_STAGED: return 3;
    case JZN_CRK_SCHEMA_LINEAGE_PENDING: return 4;
    case JZN_CRK_SCHEMA_LINEAGE_ACTIVE: return 5;
    case JZN_CRK_WRITE_POINTER_PENDING: return 6;
    case JZN_CRK_BOOTSTRAP_READY: return 7;
    case JZN_CRK_ACTIVE_SCHEMA: return 8;
    default: JZ_UNREACHABLE();
    }
}

jz_err jazz_node__node__codec__CatalogueRecordKind__from_key(u64 key, u32 *out) {
    if (key > 8) return jz_err_new(GROOVE_REC_ERR_NON_CANONICAL, "non-canonical record");
    *out = (u32)key; /* 0..=8 map onto the discriminants in order */
    return JZ_OK;
}

jz_err jazz_node__node__codec__CatalogueRecordKind__read(const groove_BorrowedRecord *r, u32 idx, u32 *out) {
    u64 key;
    JZ_TRY(groove__records__BorrowedRecord__get_u64(r, idx, &key));
    return jazz_node__node__codec__CatalogueRecordKind__from_key(key, out);
}

groove_Value jazz_node__node__codec__CatalogueRecordKind__to_value(u32 kind) {
    return groove_value_u64(jazz_node__node__codec__CatalogueRecordKind__key(kind));
}

/* ======================= catalogue kernel payload primitives ======================= */

#define CATALOGUE_BOOTSTRAP_READY_VERSION 1
#define CATALOGUE_WRITE_POINTER_VERSION 1
#define CATALOGUE_LINEAGE_ACTIVATION_VERSION 1
#define CATALOGUE_PUBLICATION_VERSION 2
#define CATALOGUE_STAGED_LINEAGE_VERSION 1
#define CATALOGUE_PENDING_LINEAGE_VERSION 1
#define PHYSICAL_MAPPING_VERSION 1
static const char PHYSICAL_MAPPING_CONTEXT[] = "invalid physical mapping payload";

static void jzn_put_u32(jz_bytes *p, u32 v) { u8 b[4]; for (int i = 0; i < 4; i++) b[i] = (u8)(v >> (8 * i)); jz_bytes_push(p, b, 4); }
static void jzn_put_u64(jz_bytes *p, u64 v) { u8 b[8]; for (int i = 0; i < 8; i++) b[i] = (u8)(v >> (8 * i)); jz_bytes_push(p, b, 8); }
static void jzn_put_uuid(jz_bytes *p, const jz_uuid *u) { jz_bytes_push(p, u->b, 16); }

static jz_err jazz_node__node__codec__put_len(jz_bytes *payload, usize length, const char *context) {
    if (length > 0xffffffffu) return JZN_ISV(context);
    jzn_put_u32(payload, (u32)length);
    return JZ_OK;
}

static jz_err jazz_node__node__codec__put_string(jz_bytes *payload, const jz_str *value, const char *context) {
    JZ_TRY(jazz_node__node__codec__put_len(payload, value->len, context));
    jz_bytes_push(payload, (const u8 *)value->ptr, value->len);
    return JZ_OK;
}

/* CataloguePayloadCursor */
typedef struct jzn_cursor { jz_slice payload; u32 offset; const char *context; } jzn_cursor;

static jz_err jazz_node__node__codec__CataloguePayloadCursor__new(jz_slice payload, u8 version, const char *context, jzn_cursor *out) {
    if (payload.len == 0 || payload.ptr[0] != version) return JZN_ISV(context);
    out->payload = payload;
    out->offset = 1;
    out->context = context;
    return JZ_OK;
}
static jz_err jazz_node__node__codec__CataloguePayloadCursor__bytes(jzn_cursor *c, u32 length, jz_slice *out) {
    u64 end = (u64)c->offset + length;
    if (end > c->payload.len) return JZN_ISV(c->context);
    *out = jz_slice_of(c->payload.ptr + c->offset, length);
    c->offset = (u32)end;
    return JZ_OK;
}
static jz_err jazz_node__node__codec__CataloguePayloadCursor__uuid(jzn_cursor *c, jz_uuid *out) {
    jz_slice b;
    JZ_TRY(jazz_node__node__codec__CataloguePayloadCursor__bytes(c, 16, &b));
    memcpy(out->b, b.ptr, 16);
    return JZ_OK;
}
static jz_err jazz_node__node__codec__CataloguePayloadCursor__u64(jzn_cursor *c, u64 *out) {
    jz_slice b;
    JZ_TRY(jazz_node__node__codec__CataloguePayloadCursor__bytes(c, 8, &b));
    u64 v = 0;
    for (int i = 7; i >= 0; i--) v = (v << 8) | b.ptr[i];
    *out = v;
    return JZ_OK;
}
static jz_err jazz_node__node__codec__CataloguePayloadCursor__u32(jzn_cursor *c, u32 *out) {
    jz_slice b;
    JZ_TRY(jazz_node__node__codec__CataloguePayloadCursor__bytes(c, 4, &b));
    *out = (u32)b.ptr[0] | (u32)b.ptr[1] << 8 | (u32)b.ptr[2] << 16 | (u32)b.ptr[3] << 24;
    return JZ_OK;
}
static jz_err jazz_node__node__codec__CataloguePayloadCursor__sized_bytes(jzn_cursor *c, jz_slice *out) {
    u32 length;
    JZ_TRY(jazz_node__node__codec__CataloguePayloadCursor__u32(c, &length));
    return jazz_node__node__codec__CataloguePayloadCursor__bytes(c, length, out);
}
static jz_err jazz_node__node__codec__CataloguePayloadCursor__string(jzn_cursor *c, jz_str *out) {
    jz_slice b;
    JZ_TRY(jazz_node__node__codec__CataloguePayloadCursor__sized_bytes(c, &b));
    if (!jz_utf8_valid(b.ptr, b.len)) return JZN_ISV(c->context);
    *out = jz_str_from_slice(b);
    return JZ_OK;
}
static jz_err jazz_node__node__codec__CataloguePayloadCursor__finish(const jzn_cursor *c) {
    return c->offset == c->payload.len ? JZ_OK : JZN_ISV(c->context);
}
#define CUR_U32(c, o) jazz_node__node__codec__CataloguePayloadCursor__u32((c), (o))
#define CUR_U64(c, o) jazz_node__node__codec__CataloguePayloadCursor__u64((c), (o))
#define CUR_UUID(c, o) jazz_node__node__codec__CataloguePayloadCursor__uuid((c), (o))
#define CUR_STR(c, o) jazz_node__node__codec__CataloguePayloadCursor__string((c), (o))
#define CUR_SIZED(c, o) jazz_node__node__codec__CataloguePayloadCursor__sized_bytes((c), (o))

static jz_err jazz_node__node__codec__require_strictly_increasing(const jz_str *previous /* NULL = None */, const jz_str *value, const char *context) {
    if (previous && jz_str_cmp(previous, value) >= 0) return JZN_ISV(context);
    return JZ_OK;
}

/* ---- enum case ids ---- */
static void jazz_node__node__codec__encode_scalar_case_id(jz_bytes *payload, const jazz_node_GlobalScalarEnumCaseId *c) {
    jzn_put_uuid(payload, &c->id);
    jzn_put_uuid(payload, &c->introducing_schema);
    jz_bytes_push_u8(payload, c->introducing_ordinal);
}
static void jazz_node__node__codec__encode_payload_case_id(jz_bytes *payload, const jazz_node_GlobalEnumCaseId *c) {
    jzn_put_uuid(payload, &c->id);
    jzn_put_uuid(payload, &c->introducing_schema);
    jzn_put_u32(payload, c->introducing_ordinal);
}
static jz_err jazz_node__node__codec__encode_scalar_cases(jz_bytes *payload, const jazz_node_GlobalScalarEnumCaseIdVec *cases) {
    JZ_TRY(jazz_node__node__codec__put_len(payload, cases->len, "physical mapping enum case count"));
    for (u32 i = 0; i < cases->len; i++) jazz_node__node__codec__encode_scalar_case_id(payload, &cases->ptr[i]);
    return JZ_OK;
}
static jz_err jazz_node__node__codec__encode_payload_cases(jz_bytes *payload, const jazz_node_GlobalEnumCaseIdVec *cases) {
    JZ_TRY(jazz_node__node__codec__put_len(payload, cases->len, "physical mapping enum case count"));
    for (u32 i = 0; i < cases->len; i++) jazz_node__node__codec__encode_payload_case_id(payload, &cases->ptr[i]);
    return JZ_OK;
}
static jz_err jazz_node__node__codec__encode_direct_scalar_enum_registries(jz_bytes *payload, const jz_btree *registries) {
    JZ_TRY(jazz_node__node__codec__put_len(payload, registries->len, "physical mapping direct registry count"));
    const u64 *column_id; const jazz_node_GlobalScalarEnumCaseIdVec *cases;
    JZ_BTREE_FOR(registries, column_id, cases) {
        jzn_put_u64(payload, *column_id);
        JZ_TRY(jazz_node__node__codec__encode_scalar_cases(payload, cases));
    }
    return JZ_OK;
}
static jz_err jazz_node__node__codec__encode_direct_payload_enum_registries(jz_bytes *payload, const jz_btree *registries) {
    JZ_TRY(jazz_node__node__codec__put_len(payload, registries->len, "physical mapping direct registry count"));
    const u64 *column_id; const jazz_node_GlobalEnumCaseIdVec *cases;
    JZ_BTREE_FOR(registries, column_id, cases) {
        jzn_put_u64(payload, *column_id);
        JZ_TRY(jazz_node__node__codec__encode_payload_cases(payload, cases));
    }
    return JZ_OK;
}
static jz_err jazz_node__node__codec__encode_nested_scalar_enum_registries(jz_bytes *payload, const jz_btree *registries) {
    JZ_TRY(jazz_node__node__codec__put_len(payload, registries->len, "physical mapping nested registry count"));
    const u64 *column_id; const jz_btree *paths;
    JZ_BTREE_FOR(registries, column_id, paths) {
        jzn_put_u64(payload, *column_id);
        JZ_TRY(jazz_node__node__codec__put_len(payload, paths->len, "physical mapping nested registry path count"));
        const jz_str *path; const jazz_node_GlobalScalarEnumCaseIdVec *cases;
        JZ_BTREE_FOR(paths, path, cases) {
            JZ_TRY(jazz_node__node__codec__put_string(payload, path, "physical mapping nested registry path"));
            JZ_TRY(jazz_node__node__codec__encode_scalar_cases(payload, cases));
        }
    }
    return JZ_OK;
}
static jz_err jazz_node__node__codec__encode_nested_payload_enum_registries(jz_bytes *payload, const jz_btree *registries) {
    JZ_TRY(jazz_node__node__codec__put_len(payload, registries->len, "physical mapping nested registry count"));
    const u64 *column_id; const jz_btree *paths;
    JZ_BTREE_FOR(registries, column_id, paths) {
        jzn_put_u64(payload, *column_id);
        JZ_TRY(jazz_node__node__codec__put_len(payload, paths->len, "physical mapping nested registry path count"));
        const jz_str *path; const jazz_node_GlobalEnumCaseIdVec *cases;
        JZ_BTREE_FOR(paths, path, cases) {
            JZ_TRY(jazz_node__node__codec__put_string(payload, path, "physical mapping nested registry path"));
            JZ_TRY(jazz_node__node__codec__encode_payload_cases(payload, cases));
        }
    }
    return JZ_OK;
}

static jz_err jazz_node__node__codec__decode_scalar_case_id(jzn_cursor *c, jazz_node_GlobalScalarEnumCaseId *out) {
    JZ_TRY(CUR_UUID(c, &out->id));
    JZ_TRY(CUR_UUID(c, &out->introducing_schema));
    jz_slice b;
    JZ_TRY(jazz_node__node__codec__CataloguePayloadCursor__bytes(c, 1, &b));
    out->introducing_ordinal = b.ptr[0];
    return JZ_OK;
}
static jz_err jazz_node__node__codec__decode_payload_case_id(jzn_cursor *c, jazz_node_GlobalEnumCaseId *out) {
    JZ_TRY(CUR_UUID(c, &out->id));
    JZ_TRY(CUR_UUID(c, &out->introducing_schema));
    return CUR_U32(c, &out->introducing_ordinal);
}

/* BTreeSet<GlobalScalarEnumCaseId> uniqueness uses the derived Ord over every field */
static int jzn_full_scalar_case_cmp(const void *a, const void *b, void *ctx) {
    const jazz_node_GlobalScalarEnumCaseId *x = a, *y = b;
    (void)ctx;
    int c = jz_uuid_cmp(&x->id, &y->id);
    if (c) return c;
    c = jz_uuid_cmp(&x->introducing_schema, &y->introducing_schema);
    if (c) return c;
    return (int)x->introducing_ordinal - (int)y->introducing_ordinal;
}
static int jzn_full_payload_case_cmp(const void *a, const void *b, void *ctx) {
    const jazz_node_GlobalEnumCaseId *x = a, *y = b;
    (void)ctx;
    int c = jz_uuid_cmp(&x->id, &y->id);
    if (c) return c;
    c = jz_uuid_cmp(&x->introducing_schema, &y->introducing_schema);
    if (c) return c;
    return x->introducing_ordinal < y->introducing_ordinal ? -1 : x->introducing_ordinal > y->introducing_ordinal;
}
static const jz_btree_desc jzn_desc_full_scalar_set = { sizeof(jazz_node_GlobalScalarEnumCaseId), 0, jzn_full_scalar_case_cmp, NULL, NULL };
static const jz_btree_desc jzn_desc_full_payload_set = { sizeof(jazz_node_GlobalEnumCaseId), 0, jzn_full_payload_case_cmp, NULL, NULL };

static jz_err jazz_node__node__codec__decode_scalar_cases(jzn_cursor *c, jazz_node_GlobalScalarEnumCaseIdVec *out) {
    u32 count;
    JZ_TRY(CUR_U32(c, &count));
    jazz_node_GlobalScalarEnumCaseIdVec cases = {0};
    jz_btree unique;
    jz_btree_init(&unique, &jzn_desc_full_scalar_set);
    jz_err err = JZ_OK;
    for (u32 i = 0; i < count; i++) {
        jazz_node_GlobalScalarEnumCaseId cs;
        err = jazz_node__node__codec__decode_scalar_case_id(c, &cs);
        if (JZ_IS_ERR(err)) break;
        if (jz_btree_contains(&unique, &cs)) { err = JZN_ISV(PHYSICAL_MAPPING_CONTEXT); break; }
        jz_btree_insert(&unique, &cs, NULL, NULL);
        JZ_VEC_PUSH(cases, cs);
    }
    jz_btree_drop(&unique);
    if (JZ_IS_ERR(err)) { jz_vec_drop(&cases, sizeof(jazz_node_GlobalScalarEnumCaseId), NULL); return err; }
    *out = cases;
    return JZ_OK;
}
static jz_err jazz_node__node__codec__decode_payload_cases(jzn_cursor *c, jazz_node_GlobalEnumCaseIdVec *out) {
    u32 count;
    JZ_TRY(CUR_U32(c, &count));
    jazz_node_GlobalEnumCaseIdVec cases = {0};
    jz_btree unique;
    jz_btree_init(&unique, &jzn_desc_full_payload_set);
    jz_err err = JZ_OK;
    for (u32 i = 0; i < count; i++) {
        jazz_node_GlobalEnumCaseId cs;
        err = jazz_node__node__codec__decode_payload_case_id(c, &cs);
        if (JZ_IS_ERR(err)) break;
        if (jz_btree_contains(&unique, &cs)) { err = JZN_ISV(PHYSICAL_MAPPING_CONTEXT); break; }
        jz_btree_insert(&unique, &cs, NULL, NULL);
        JZ_VEC_PUSH(cases, cs);
    }
    jz_btree_drop(&unique);
    if (JZ_IS_ERR(err)) { jz_vec_drop(&cases, sizeof(jazz_node_GlobalEnumCaseId), NULL); return err; }
    *out = cases;
    return JZ_OK;
}

/* direct registries: one copy over the case kind (Rust has two monomorphs) */
typedef jz_err (*jzn_decode_cases_fn)(jzn_cursor *c, void *out);
static jz_err jzn_decode_direct_registries(jzn_cursor *c, jzn_decode_cases_fn decode, jz_btree *out) {
    u32 count;
    JZ_TRY(CUR_U32(c, &count));
    jz_btree registries;
    jz_btree_init(&registries, &jzn_desc_u64_podvec);
    bool has_previous = false;
    u64 previous = 0;
    jz_err err = JZ_OK;
    for (u32 i = 0; i < count; i++) {
        u64 id;
        err = CUR_U64(c, &id);
        if (JZ_IS_ERR(err)) break;
        if (id == 0 || (has_previous && id <= previous)) { err = JZN_ISV(PHYSICAL_MAPPING_CONTEXT); break; }
        previous = id;
        has_previous = true;
        jz_vec cases;
        err = decode(c, &cases);
        if (JZ_IS_ERR(err)) break;
        jz_vec old;
        if (jz_btree_insert(&registries, &id, &cases, &old)) { jzn_pod_vec_drop(&old); err = JZN_ISV(PHYSICAL_MAPPING_CONTEXT); break; }
    }
    if (JZ_IS_ERR(err)) { jz_btree_drop(&registries); return err; }
    *out = registries;
    return JZ_OK;
}
static jz_err jzn_decode_nested_registries(jzn_cursor *c, jzn_decode_cases_fn decode, jz_btree *out) {
    u32 count;
    JZ_TRY(CUR_U32(c, &count));
    jz_btree registries;
    jz_btree_init(&registries, &jzn_desc_u64_strpodvec_map);
    bool has_previous = false;
    u64 previous_column = 0;
    jz_err err = JZ_OK;
    for (u32 i = 0; i < count && !JZ_IS_ERR(err); i++) {
        u64 id;
        err = CUR_U64(c, &id);
        if (JZ_IS_ERR(err)) break;
        if (id == 0 || (has_previous && id <= previous_column)) { err = JZN_ISV(PHYSICAL_MAPPING_CONTEXT); break; }
        previous_column = id;
        has_previous = true;
        u32 path_count;
        err = CUR_U32(c, &path_count);
        if (JZ_IS_ERR(err)) break;
        jz_btree paths;
        jz_btree_init(&paths, &jzn_desc_str_podvec);
        jz_str previous_path = {0};
        bool has_path = false;
        for (u32 p = 0; p < path_count; p++) {
            jz_str path;
            err = CUR_STR(c, &path);
            if (JZ_IS_ERR(err)) break;
            err = jazz_node__node__codec__require_strictly_increasing(has_path ? &previous_path : NULL, &path, PHYSICAL_MAPPING_CONTEXT);
            if (JZ_IS_ERR(err)) { jz_str_drop(&path); break; }
            jz_str_drop(&previous_path);
            previous_path = jz_str_clone(&path);
            has_path = true;
            jz_vec cases;
            err = decode(c, &cases);
            if (JZ_IS_ERR(err)) { jz_str_drop(&path); break; }
            jz_vec old;
            if (jz_btree_insert(&paths, &path, &cases, &old)) { jzn_pod_vec_drop(&old); err = JZN_ISV(PHYSICAL_MAPPING_CONTEXT); break; }
        }
        jz_str_drop(&previous_path);
        if (JZ_IS_ERR(err)) { jz_btree_drop(&paths); break; }
        jz_btree old;
        if (jz_btree_insert(&registries, &id, &paths, &old)) { jz_btree_drop(&old); err = JZN_ISV(PHYSICAL_MAPPING_CONTEXT); }
    }
    if (JZ_IS_ERR(err)) { jz_btree_drop(&registries); return err; }
    *out = registries;
    return JZ_OK;
}
static jz_err jzn_decode_scalar_cases_v(jzn_cursor *c, void *out) { return jazz_node__node__codec__decode_scalar_cases(c, out); }
static jz_err jzn_decode_payload_cases_v(jzn_cursor *c, void *out) { return jazz_node__node__codec__decode_payload_cases(c, out); }
static jz_err jazz_node__node__codec__decode_direct_scalar_enum_registries(jzn_cursor *c, jz_btree *out) {
    return jzn_decode_direct_registries(c, jzn_decode_scalar_cases_v, out);
}
static jz_err jazz_node__node__codec__decode_direct_payload_enum_registries(jzn_cursor *c, jz_btree *out) {
    return jzn_decode_direct_registries(c, jzn_decode_payload_cases_v, out);
}
static jz_err jazz_node__node__codec__decode_nested_scalar_enum_registries(jzn_cursor *c, jz_btree *out) {
    return jzn_decode_nested_registries(c, jzn_decode_scalar_cases_v, out);
}
static jz_err jazz_node__node__codec__decode_nested_payload_enum_registries(jzn_cursor *c, jz_btree *out) {
    return jzn_decode_nested_registries(c, jzn_decode_payload_cases_v, out);
}

/* ---- physical identity manifest ---- */
static jz_err jazz_node__node__codec__encode_physical_identity_manifest(jz_bytes *payload, const jazz_protocol_PhysicalIdentityManifest *manifest) {
    JZ_TRY(jazz_node__node__codec__put_len(payload, manifest->tables.len, "physical identity table count"));
    const jz_str *table_name; const jazz_protocol_PhysicalTableIdentity *table;
    JZ_BTREE_FOR(&manifest->tables, table_name, table) {
        JZ_TRY(jazz_node__node__codec__put_string(payload, table_name, "physical identity table name"));
        jzn_put_uuid(payload, &table->id);
        JZ_TRY(jazz_node__node__codec__put_len(payload, table->columns.len, "physical identity column count"));
        const jz_str *column_name; const jazz_protocol_PhysicalColumnIdentity *column;
        JZ_BTREE_FOR(&table->columns, column_name, column) {
            JZ_TRY(jazz_node__node__codec__put_string(payload, column_name, "physical identity column name"));
            jzn_put_uuid(payload, &column->id);
            JZ_TRY(jazz_node__node__codec__put_len(payload, column->enum_variants.len, "physical identity enum occurrence count"));
            const jz_str *path; const JZ_VEC(jz_uuid) *variants;
            JZ_BTREE_FOR(&column->enum_variants, path, variants) {
                JZ_TRY(jazz_node__node__codec__put_string(payload, path, "physical identity enum path"));
                JZ_TRY(jazz_node__node__codec__put_len(payload, variants->len, "physical identity enum case count"));
                for (u32 i = 0; i < variants->len; i++) jzn_put_uuid(payload, &variants->ptr[i]);
            }
        }
    }
    return JZ_OK;
}

static jz_err jazz_node__node__codec__decode_physical_identity_manifest(jzn_cursor *c, jazz_protocol_PhysicalIdentityManifest *out) {
    u32 table_count;
    JZ_TRY(CUR_U32(c, &table_count));
    jazz_protocol_PhysicalIdentityManifest manifest;
    jz_btree_init(&manifest.tables, &jazz_protocol_PhysicalTableIdentity_map_desc);
    jz_str previous_table = {0};
    bool has_table = false;
    jz_err err = JZ_OK;
    for (u32 t = 0; t < table_count && !JZ_IS_ERR(err); t++) {
        jz_str table_name;
        err = CUR_STR(c, &table_name);
        if (JZ_IS_ERR(err)) break;
        err = jazz_node__node__codec__require_strictly_increasing(has_table ? &previous_table : NULL, &table_name, PHYSICAL_MAPPING_CONTEXT);
        if (JZ_IS_ERR(err)) { jz_str_drop(&table_name); break; }
        jz_str_drop(&previous_table);
        previous_table = jz_str_clone(&table_name);
        has_table = true;
        jazz_protocol_PhysicalTableIdentity table;
        jz_btree_init(&table.columns, &jazz_protocol_PhysicalColumnIdentity_map_desc);
        u32 column_count = 0;
        err = CUR_UUID(c, &table.id);
        if (!JZ_IS_ERR(err)) err = CUR_U32(c, &column_count);
        jz_str previous_column = {0};
        bool has_column = false;
        for (u32 k = 0; k < column_count && !JZ_IS_ERR(err); k++) {
            jz_str column_name;
            err = CUR_STR(c, &column_name);
            if (JZ_IS_ERR(err)) break;
            err = jazz_node__node__codec__require_strictly_increasing(has_column ? &previous_column : NULL, &column_name, PHYSICAL_MAPPING_CONTEXT);
            if (JZ_IS_ERR(err)) { jz_str_drop(&column_name); break; }
            jz_str_drop(&previous_column);
            previous_column = jz_str_clone(&column_name);
            has_column = true;
            jazz_protocol_PhysicalColumnIdentity column;
            jz_btree_init(&column.enum_variants, &jazz_protocol_enum_variants_map_desc);
            u32 occurrence_count = 0;
            err = CUR_UUID(c, &column.id);
            if (!JZ_IS_ERR(err)) err = CUR_U32(c, &occurrence_count);
            jz_str previous_path = {0};
            bool has_path = false;
            for (u32 o = 0; o < occurrence_count && !JZ_IS_ERR(err); o++) {
                jz_str path;
                err = CUR_STR(c, &path);
                if (JZ_IS_ERR(err)) break;
                err = jazz_node__node__codec__require_strictly_increasing(has_path ? &previous_path : NULL, &path, PHYSICAL_MAPPING_CONTEXT);
                if (JZ_IS_ERR(err)) { jz_str_drop(&path); break; }
                jz_str_drop(&previous_path);
                previous_path = jz_str_clone(&path);
                has_path = true;
                u32 case_count;
                err = CUR_U32(c, &case_count);
                JZ_VEC(jz_uuid) variants = {0};
                for (u32 v = 0; v < case_count && !JZ_IS_ERR(err); v++) {
                    jz_uuid id;
                    err = CUR_UUID(c, &id);
                    if (!JZ_IS_ERR(err)) JZ_VEC_PUSH(variants, id);
                }
                if (JZ_IS_ERR(err)) { jz_vec_drop(&variants, sizeof(jz_uuid), NULL); jz_str_drop(&path); break; }
                jz_vec old;
                if (jz_btree_insert(&column.enum_variants, &path, &variants, &old)) jzn_pod_vec_drop(&old);
            }
            jz_str_drop(&previous_path);
            if (JZ_IS_ERR(err)) { jz_btree_drop(&column.enum_variants); jz_str_drop(&column_name); break; }
            jazz_protocol_PhysicalColumnIdentity old;
            if (jz_btree_insert(&table.columns, &column_name, &column, &old)) jz_btree_drop(&old.enum_variants);
        }
        jz_str_drop(&previous_column);
        if (JZ_IS_ERR(err)) { jz_btree_drop(&table.columns); jz_str_drop(&table_name); break; }
        jazz_protocol_PhysicalTableIdentity old;
        if (jz_btree_insert(&manifest.tables, &table_name, &table, &old)) jz_btree_drop(&old.columns);
    }
    jz_str_drop(&previous_table);
    if (JZ_IS_ERR(err)) { jz_btree_drop(&manifest.tables); return err; }
    *out = manifest;
    return JZ_OK;
}

/* ======================= physical mapping ======================= */

/* Encode node-local physical storage metadata as one typed, canonical value.
 * Table/column ids are opaque local handles; the case identities inside are
 * semantic and their vector order is the physical tag order. */
jz_err jazz_node__node__codec__encode_physical_mapping(const jazz_node_SchemaPhysicalMapping *mapping, jz_bytes *out) {
    jz_bytes payload = {0};
    jz_bytes_push_u8(&payload, PHYSICAL_MAPPING_VERSION);
    jz_err err = jazz_node__node__codec__encode_physical_identity_manifest(&payload, &mapping->identities);
    if (!JZ_IS_ERR(err)) err = jazz_node__node__codec__put_len(&payload, mapping->tables.len, "physical mapping table count");
    const jz_str *table_name; const jazz_node_TablePhysicalMapping *table;
    if (!JZ_IS_ERR(err)) JZ_BTREE_FOR(&mapping->tables, table_name, table) {
        if (JZ_IS_ERR(err = jazz_node__node__codec__put_string(&payload, table_name, "physical mapping table name"))) break;
        jzn_put_u64(&payload, table->table_id);
        if (JZ_IS_ERR(err = jazz_node__node__codec__put_len(&payload, table->columns.len, "physical mapping column count"))) break;
        const jz_str *column_name; const u64 *column_id;
        JZ_BTREE_FOR(&table->columns, column_name, column_id) {
            if (JZ_IS_ERR(err = jazz_node__node__codec__put_string(&payload, column_name, "physical mapping column name"))) break;
            jzn_put_u64(&payload, *column_id);
        }
        if (JZ_IS_ERR(err)) break;
        if (JZ_IS_ERR(err = jazz_node__node__codec__put_len(&payload, table->variant_cases.len, "physical mapping variant case count"))) break;
        for (u32 i = 0; i < table->variant_cases.len && !JZ_IS_ERR(err); i++) {
            const jazz_node_PhysicalVariantCase *cs = &table->variant_cases.ptr[i];
            jzn_put_u32(&payload, cs->tag);
            if (JZ_IS_ERR(err = jazz_node__node__codec__put_len(&payload, cs->fields.len, "physical mapping variant field count"))) break;
            const jz_str *field; const void *unused;
            JZ_BTREE_FOR(&cs->fields, field, unused) {
                if (JZ_IS_ERR(err = jazz_node__node__codec__put_string(&payload, field, "physical mapping variant field"))) break;
            }
        }
        if (JZ_IS_ERR(err)) break;
        if (JZ_IS_ERR(err = jazz_node__node__codec__encode_direct_scalar_enum_registries(&payload, &table->scalar_enum_cases))) break;
        if (JZ_IS_ERR(err = jazz_node__node__codec__encode_direct_payload_enum_registries(&payload, &table->payload_enum_cases))) break;
        if (JZ_IS_ERR(err = jazz_node__node__codec__encode_nested_scalar_enum_registries(&payload, &table->nested_scalar_enum_cases))) break;
        if (JZ_IS_ERR(err = jazz_node__node__codec__encode_nested_payload_enum_registries(&payload, &table->nested_payload_enum_cases))) break;
    }
    if (JZ_IS_ERR(err)) { jz_bytes_drop(&payload); return err; }
    *out = payload;
    return JZ_OK;
}

jz_err jazz_node__node__codec__decode_physical_mapping(jz_slice payload, jazz_node_SchemaPhysicalMapping *out) {
    const char *CONTEXT = PHYSICAL_MAPPING_CONTEXT;
    if (payload.len == 0) return JZN_ISV(CONTEXT);
    u8 version = payload.ptr[0];
    if (version != PHYSICAL_MAPPING_VERSION) return JZN_ISV(CONTEXT);
    jzn_cursor cursor;
    JZ_TRY(jazz_node__node__codec__CataloguePayloadCursor__new(payload, version, CONTEXT, &cursor));
    jazz_node_SchemaPhysicalMapping mapping;
    JZ_TRY(jazz_node__node__codec__decode_physical_identity_manifest(&cursor, &mapping.identities));
    jz_btree_init(&mapping.tables, &jzn_desc_str_table_mapping);
    u32 table_count = 0;
    jz_err err = CUR_U32(&cursor, &table_count);
    jz_str previous_table = {0};
    bool has_table = false;
    for (u32 t = 0; t < table_count && !JZ_IS_ERR(err); t++) {
        jz_str table_name;
        err = CUR_STR(&cursor, &table_name);
        if (JZ_IS_ERR(err)) break;
        err = jazz_node__node__codec__require_strictly_increasing(has_table ? &previous_table : NULL, &table_name, CONTEXT);
        if (JZ_IS_ERR(err)) { jz_str_drop(&table_name); break; }
        jz_str_drop(&previous_table);
        previous_table = jz_str_clone(&table_name);
        has_table = true;
        u64 table_id;
        err = CUR_U64(&cursor, &table_id);
        if (!JZ_IS_ERR(err) && table_id == 0) err = JZN_ISV(CONTEXT);
        if (JZ_IS_ERR(err)) { jz_str_drop(&table_name); break; }
        jazz_node_TablePhysicalMapping table;
        jazz_node_TablePhysicalMapping_init(&table, table_id);
        u32 column_count = 0;
        err = CUR_U32(&cursor, &column_count);
        jz_str previous_column = {0};
        bool has_column = false;
        for (u32 k = 0; k < column_count && !JZ_IS_ERR(err); k++) {
            jz_str name;
            err = CUR_STR(&cursor, &name);
            if (JZ_IS_ERR(err)) break;
            err = jazz_node__node__codec__require_strictly_increasing(has_column ? &previous_column : NULL, &name, CONTEXT);
            if (JZ_IS_ERR(err)) { jz_str_drop(&name); break; }
            jz_str_drop(&previous_column);
            previous_column = jz_str_clone(&name);
            has_column = true;
            u64 id;
            err = CUR_U64(&cursor, &id);
            if (JZ_IS_ERR(err)) { jz_str_drop(&name); break; }
            if (id == 0 || jz_btree_contains(&table.columns, &name)) { jz_str_drop(&name); err = JZN_ISV(CONTEXT); break; }
            jz_btree_insert(&table.columns, &name, &id, NULL);
        }
        jz_str_drop(&previous_column);
        u32 variant_count = 0;
        if (!JZ_IS_ERR(err)) err = CUR_U32(&cursor, &variant_count);
        /* Do not reserve an attacker-controlled count before checking that the
         * corresponding entries exist in the exact-consumed payload. */
        for (u32 v = 0; v < variant_count && !JZ_IS_ERR(err); v++) {
            jazz_node_PhysicalVariantCase cs;
            jz_btree_init(&cs.fields, &jzn_desc_str_set);
            u32 field_count = 0;
            err = CUR_U32(&cursor, &cs.tag);
            if (!JZ_IS_ERR(err)) err = CUR_U32(&cursor, &field_count);
            jz_str previous_field = {0};
            bool has_field = false;
            for (u32 f = 0; f < field_count && !JZ_IS_ERR(err); f++) {
                jz_str field;
                err = CUR_STR(&cursor, &field);
                if (JZ_IS_ERR(err)) break;
                err = jazz_node__node__codec__require_strictly_increasing(has_field ? &previous_field : NULL, &field, CONTEXT);
                if (JZ_IS_ERR(err)) { jz_str_drop(&field); break; }
                jz_str_drop(&previous_field);
                previous_field = jz_str_clone(&field);
                has_field = true;
                if (jz_btree_contains(&cs.fields, &field)) { jz_str_drop(&field); err = JZN_ISV(CONTEXT); break; }
                jz_btree_insert(&cs.fields, &field, NULL, NULL);
            }
            jz_str_drop(&previous_field);
            if (JZ_IS_ERR(err)) { jz_btree_drop(&cs.fields); break; }
            JZ_VEC_PUSH(table.variant_cases, cs);
        }
        if (!JZ_IS_ERR(err)) { jz_btree_drop(&table.scalar_enum_cases); err = jazz_node__node__codec__decode_direct_scalar_enum_registries(&cursor, &table.scalar_enum_cases); if (JZ_IS_ERR(err)) jz_btree_init(&table.scalar_enum_cases, &jzn_desc_u64_podvec); }
        if (!JZ_IS_ERR(err)) { jz_btree_drop(&table.payload_enum_cases); err = jazz_node__node__codec__decode_direct_payload_enum_registries(&cursor, &table.payload_enum_cases); if (JZ_IS_ERR(err)) jz_btree_init(&table.payload_enum_cases, &jzn_desc_u64_podvec); }
        if (!JZ_IS_ERR(err)) { jz_btree_drop(&table.nested_scalar_enum_cases); err = jazz_node__node__codec__decode_nested_scalar_enum_registries(&cursor, &table.nested_scalar_enum_cases); if (JZ_IS_ERR(err)) jz_btree_init(&table.nested_scalar_enum_cases, &jzn_desc_u64_strpodvec_map); }
        if (!JZ_IS_ERR(err)) { jz_btree_drop(&table.nested_payload_enum_cases); err = jazz_node__node__codec__decode_nested_payload_enum_registries(&cursor, &table.nested_payload_enum_cases); if (JZ_IS_ERR(err)) jz_btree_init(&table.nested_payload_enum_cases, &jzn_desc_u64_strpodvec_map); }
        if (JZ_IS_ERR(err)) { jazz_node_TablePhysicalMapping_drop(&table); jz_str_drop(&table_name); break; }
        if (jz_btree_contains(&mapping.tables, &table_name)) {
            jazz_node_TablePhysicalMapping_drop(&table); jz_str_drop(&table_name);
            err = JZN_ISV(CONTEXT);
            break;
        }
        jz_btree_insert(&mapping.tables, &table_name, &table, NULL);
    }
    jz_str_drop(&previous_table);
    if (!JZ_IS_ERR(err)) err = jazz_node__node__codec__CataloguePayloadCursor__finish(&cursor);
    if (JZ_IS_ERR(err)) { jazz_node_SchemaPhysicalMapping_drop(&mapping); return err; }
    *out = mapping;
    return JZ_OK;
}

/* ======================= catalogue records ======================= */

jz_err jazz_node__node__codec__encode_catalogue_schema(const jazz_model_SchemaVersion *schema, jz_bytes *out) {
    jz_err err = jazz_protocol__protocol__canonical_catalogue_schema_bytes(schema, out);
    if (JZ_IS_ERR(err)) { jz_err_drop(&err); return JZN_ISV("encode catalogue public schema"); }
    return JZ_OK;
}

jz_err jazz_node__node__codec__decode_catalogue_schema(jz_slice payload, jazz_model_SchemaVersion *out) {
    jzn_cursor cursor;
    JZ_TRY(jazz_node__node__codec__CataloguePayloadCursor__new(payload, JAZZ_PROTOCOL_CATALOGUE_SCHEMA_VERSION, "invalid catalogue schema payload", &cursor));
    jz_uuid id;
    jz_slice public_schema;
    JZ_TRY(CUR_UUID(&cursor, &id));
    JZ_TRY(CUR_SIZED(&cursor, &public_schema));
    JZ_TRY(jazz_node__node__codec__CataloguePayloadCursor__finish(&cursor));
    jazz_model_JazzSchema schema;
    jz_err err = jazz_model__model__public_schema_convert__decode_public_schema_json(public_schema, &schema);
    if (JZ_IS_ERR(err)) { jz_err_drop(&err); return JZN_ISV("invalid catalogue schema public schema"); }
    jz_bytes canonical_public_schema;
    err = jazz_model__schema__JazzSchema__public_schema_json(&schema, &canonical_public_schema);
    if (JZ_IS_ERR(err)) { jz_err_drop(&err); jazz_model_JazzSchema_drop(&schema); return JZN_ISV("encode catalogue public schema"); }
    bool canonical = jz_slice_cmp(jz_bytes_slice(&canonical_public_schema), public_schema) == 0;
    jz_bytes_drop(&canonical_public_schema);
    if (!canonical) { jazz_model_JazzSchema_drop(&schema); return JZN_ISV("non-canonical catalogue schema public schema"); }
    jz_uuid version_id = jazz_model__schema__JazzSchema__version_id(&schema);
    if (!jz_uuid_eq(&version_id, &id)) { jazz_model_JazzSchema_drop(&schema); return JZN_ISV("catalogue schema content id mismatch"); }
    out->id = id;
    out->schema = schema;
    return JZ_OK;
}

void jazz_node__node__codec__encode_catalogue_bootstrap_ready(const jazz_node_CatalogueBootstrapReady *ready, jz_bytes *out) {
    jz_bytes payload = {0};
    jz_vec_reserve(&payload, 1 + 16 + 8 + 16 + 8, 1);
    jz_bytes_push_u8(&payload, CATALOGUE_BOOTSTRAP_READY_VERSION);
    jzn_put_uuid(&payload, &ready->genesis);
    jzn_put_u64(&payload, ready->current_write_schema.revision);
    jzn_put_uuid(&payload, &ready->current_write_schema.schema);
    jzn_put_u64(&payload, ready->active_catalogue_seq);
    *out = payload;
}

jz_err jazz_node__node__codec__decode_catalogue_bootstrap_ready(jz_slice payload, jazz_node_CatalogueBootstrapReady *out) {
    jzn_cursor cursor;
    JZ_TRY(jazz_node__node__codec__CataloguePayloadCursor__new(payload, CATALOGUE_BOOTSTRAP_READY_VERSION,
                                                               "invalid catalogue bootstrap receipt payload", &cursor));
    JZ_TRY(CUR_UUID(&cursor, &out->genesis));
    JZ_TRY(CUR_U64(&cursor, &out->current_write_schema.revision));
    JZ_TRY(CUR_UUID(&cursor, &out->current_write_schema.schema));
    JZ_TRY(CUR_U64(&cursor, &out->active_catalogue_seq));
    return jazz_node__node__codec__CataloguePayloadCursor__finish(&cursor);
}

jz_uuid jazz_node__node__codec__catalogue_write_pointer_id(jazz_protocol_CurrentWriteSchema pointer) {
    u8 rev[8];
    for (int i = 0; i < 8; i++) rev[i] = (u8)(pointer.revision >> (8 * i));
    return jz_uuid_v5(&pointer.schema, rev, 8);
}

jz_err jazz_node__node__codec__decode_catalogue_write_pointer(jz_slice payload, jazz_protocol_CurrentWriteSchema *out) {
    jzn_cursor cursor;
    JZ_TRY(jazz_node__node__codec__CataloguePayloadCursor__new(payload, CATALOGUE_WRITE_POINTER_VERSION, "invalid catalogue write-pointer payload", &cursor));
    JZ_TRY(CUR_U64(&cursor, &out->revision));
    JZ_TRY(CUR_UUID(&cursor, &out->schema));
    return jazz_node__node__codec__CataloguePayloadCursor__finish(&cursor);
}

void jazz_node__node__codec__encode_catalogue_lineage_activation(jazz_node_SchemaLineageActivation activation, jz_bytes *out) {
    jz_bytes payload = {0};
    jz_vec_reserve(&payload, 1 + 16 + 8, 1);
    jz_bytes_push_u8(&payload, CATALOGUE_LINEAGE_ACTIVATION_VERSION);
    jzn_put_uuid(&payload, &activation.id);
    jzn_put_u64(&payload, activation.catalogue_seq);
    *out = payload;
}

jz_err jazz_node__node__codec__decode_catalogue_lineage_activation(jz_slice payload, jazz_node_SchemaLineageActivation *out) {
    jzn_cursor cursor;
    JZ_TRY(jazz_node__node__codec__CataloguePayloadCursor__new(payload, CATALOGUE_LINEAGE_ACTIVATION_VERSION,
                                                               "invalid catalogue lineage activation payload", &cursor));
    JZ_TRY(CUR_UUID(&cursor, &out->id));
    JZ_TRY(CUR_U64(&cursor, &out->catalogue_seq));
    return jazz_node__node__codec__CataloguePayloadCursor__finish(&cursor);
}

/* The protocol lens has a dedicated canonical grammar because it is a durable
 * sync/publication fact. */
void jazz_node__node__codec__encode_catalogue_lens(const jazz_protocol_MigrationLens *lens, jz_bytes *out) {
    jz_bytes payload = {0};
    jz_bytes_push_u8(&payload, 1);
    jz_bytes body;
    jazz_protocol__protocol__canonical_lens_bytes(lens, &body);
    jz_bytes_push(&payload, body.ptr, body.len);
    jz_bytes_drop(&body);
    *out = payload;
}

jz_err jazz_node__node__codec__decode_catalogue_lens(jz_slice payload, jazz_protocol_MigrationLens *out) {
    if (payload.len == 0 || payload.ptr[0] != 1) return JZN_ISV("invalid catalogue lens payload");
    jz_err err = jazz_protocol__protocol__decode_canonical_lens_bytes(jz_slice_of(payload.ptr + 1, payload.len - 1), out);
    if (JZ_IS_ERR(err)) { jz_err_drop(&err); return JZN_ISV("invalid catalogue lens payload"); }
    return JZ_OK;
}

static int jzn_predecessor_source_cmp(const void *a, const void *b, void *ctx) {
    (void)ctx;
    return jz_uuid_cmp(&(*(const jazz_protocol_SchemaPredecessor *const *)a)->lens.source,
                       &(*(const jazz_protocol_SchemaPredecessor *const *)b)->lens.source);
}

/* sort + reject duplicates + write the sorted table list */
static jz_err jzn_put_sorted_tables(jz_bytes *payload, const jzn_StrVec *tables, const char *duplicate, const char *count_ctx) {
    JZ_VEC(const jz_str *) sorted = {0};
    for (u32 i = 0; i < tables->len; i++) JZ_VEC_PUSH(sorted, &tables->ptr[i]);
    jz_sort(sorted.ptr, sorted.len, sizeof(const jz_str *), jz_cmp_str, NULL);
    jz_err err = JZ_OK;
    for (u32 i = 1; i < sorted.len; i++)
        if (jz_str_eq(sorted.ptr[i - 1], sorted.ptr[i])) { err = JZN_ISV(duplicate); break; }
    if (!JZ_IS_ERR(err)) err = jazz_node__node__codec__put_len(payload, sorted.len, count_ctx);
    for (u32 i = 0; i < sorted.len && !JZ_IS_ERR(err); i++)
        err = jazz_node__node__codec__put_string(payload, sorted.ptr[i], "catalogue publication table");
    jz_vec_drop(&sorted, sizeof(const jz_str *), NULL);
    return err;
}

static jz_err jazz_node__node__codec__encode_catalogue_publication(const jazz_protocol_SchemaLineagePublication *publication, jz_bytes *out) {
    jz_bytes schema;
    JZ_TRY(jazz_node__node__codec__encode_catalogue_schema(&publication->schema, &schema));
    jz_bytes payload = {0};
    jz_bytes_push_u8(&payload, CATALOGUE_PUBLICATION_VERSION);
    jzn_put_uuid(&payload, &publication->id);
    jz_err err = jazz_node__node__codec__put_len(&payload, schema.len, "catalogue publication schema length");
    jz_bytes_push(&payload, schema.ptr, schema.len);
    jz_bytes_drop(&schema);
    JZ_VEC(const jazz_protocol_SchemaPredecessor *) predecessors = {0};
    for (u32 i = 0; i < publication->predecessors.len; i++) JZ_VEC_PUSH(predecessors, &publication->predecessors.ptr[i]);
    jz_sort(predecessors.ptr, predecessors.len, sizeof(void *), jzn_predecessor_source_cmp, NULL);
    bool invalid = predecessors.len == 0;
    for (u32 i = 1; i < predecessors.len && !invalid; i++)
        if (jz_uuid_eq(&predecessors.ptr[i - 1]->lens.source, &predecessors.ptr[i]->lens.source)) invalid = true;
    if (!JZ_IS_ERR(err) && invalid) err = JZN_ISV("invalid schema predecessors");
    if (!JZ_IS_ERR(err)) err = jazz_node__node__codec__put_len(&payload, predecessors.len, "catalogue predecessor count");
    for (u32 i = 0; i < predecessors.len && !JZ_IS_ERR(err); i++) {
        const jazz_protocol_SchemaPredecessor *predecessor = predecessors.ptr[i];
        jz_bytes lens;
        jazz_node__node__codec__encode_catalogue_lens(&predecessor->lens, &lens);
        err = jazz_node__node__codec__put_len(&payload, lens.len, "catalogue publication lens length");
        jz_bytes_push(&payload, lens.ptr, lens.len);
        jz_bytes_drop(&lens);
        if (!JZ_IS_ERR(err))
            err = jzn_put_sorted_tables(&payload, &predecessor->new_tables, "catalogue publication duplicate new table", "catalogue publication new table count");
        if (!JZ_IS_ERR(err))
            err = jzn_put_sorted_tables(&payload, &predecessor->dropped_tables, "catalogue publication duplicate dropped table", "catalogue publication dropped table count");
    }
    jz_vec_drop(&predecessors, sizeof(void *), NULL);
    if (!JZ_IS_ERR(err)) err = jazz_node__node__codec__encode_physical_identity_manifest(&payload, &publication->physical_identities);
    if (JZ_IS_ERR(err)) { jz_bytes_drop(&payload); return err; }
    *out = payload;
    return JZ_OK;
}

static jz_err jazz_node__node__codec__decode_sorted_strings(jzn_cursor *c, const char *context, jzn_StrVec *out) {
    u32 count;
    JZ_TRY(CUR_U32(c, &count));
    jzn_StrVec values = {0};
    jz_err err = JZ_OK;
    for (u32 i = 0; i < count; i++) {
        jz_str value;
        err = CUR_STR(c, &value);
        if (JZ_IS_ERR(err)) break;
        err = jazz_node__node__codec__require_strictly_increasing(values.len ? &values.ptr[values.len - 1] : NULL, &value, context);
        if (JZ_IS_ERR(err)) { jz_str_drop(&value); break; }
        JZ_VEC_PUSH(values, value);
    }
    if (JZ_IS_ERR(err)) { jzn_str_vec_drop(&values); return err; }
    *out = values;
    return JZ_OK;
}

static void jzn_predecessor_drop(void *p) {
    jazz_protocol_SchemaPredecessor *x = p;
    jazz_protocol_MigrationLens_drop(&x->lens);
    jzn_str_vec_drop(&x->new_tables);
    jzn_str_vec_drop(&x->dropped_tables);
}

static jz_err jazz_node__node__codec__decode_catalogue_publication(jz_slice payload, jazz_protocol_SchemaLineagePublication *out) {
    const char *CONTEXT = "invalid catalogue protocol publication";
    jzn_cursor cursor;
    JZ_TRY(jazz_node__node__codec__CataloguePayloadCursor__new(payload, CATALOGUE_PUBLICATION_VERSION, CONTEXT, &cursor));
    jazz_protocol_SchemaLineagePublication publication;
    memset(&publication, 0, sizeof publication);
    jz_slice schema_bytes;
    JZ_TRY(CUR_UUID(&cursor, &publication.id));
    JZ_TRY(CUR_SIZED(&cursor, &schema_bytes));
    JZ_TRY(jazz_node__node__codec__decode_catalogue_schema(schema_bytes, &publication.schema));
    u32 count = 0;
    jz_err err = CUR_U32(&cursor, &count);
    if (!JZ_IS_ERR(err) && count == 0) err = JZN_ISV(CONTEXT);
    for (u32 i = 0; i < count && !JZ_IS_ERR(err); i++) {
        jazz_protocol_SchemaPredecessor predecessor;
        memset(&predecessor, 0, sizeof predecessor);
        jz_slice lens_bytes;
        err = CUR_SIZED(&cursor, &lens_bytes);
        if (!JZ_IS_ERR(err)) err = jazz_node__node__codec__decode_catalogue_lens(lens_bytes, &predecessor.lens);
        if (JZ_IS_ERR(err)) break;
        err = jazz_node__node__codec__decode_sorted_strings(&cursor, CONTEXT, &predecessor.new_tables);
        if (!JZ_IS_ERR(err)) err = jazz_node__node__codec__decode_sorted_strings(&cursor, CONTEXT, &predecessor.dropped_tables);
        if (JZ_IS_ERR(err)) { jzn_predecessor_drop(&predecessor); break; }
        JZ_VEC_PUSH(publication.predecessors, predecessor);
    }
    bool have_manifest = false;
    if (!JZ_IS_ERR(err)) { err = jazz_node__node__codec__decode_physical_identity_manifest(&cursor, &publication.physical_identities); have_manifest = !JZ_IS_ERR(err); }
    if (!JZ_IS_ERR(err)) err = jazz_node__node__codec__CataloguePayloadCursor__finish(&cursor);
    if (!JZ_IS_ERR(err)) {
        jz_uuid content_id = jazz_protocol__protocol__SchemaLineagePublication__content_id(&publication);
        jz_bytes again;
        err = jazz_node__node__codec__encode_catalogue_publication(&publication, &again);
        if (!JZ_IS_ERR(err)) {
            bool same = jz_slice_cmp(jz_bytes_slice(&again), payload) == 0;
            jz_bytes_drop(&again);
            if (!jz_uuid_eq(&publication.id, &content_id) || !same) err = JZN_ISV(CONTEXT);
        }
    }
    if (JZ_IS_ERR(err)) {
        if (!have_manifest) jz_btree_init(&publication.physical_identities.tables, &jazz_protocol_PhysicalTableIdentity_map_desc);
        jazz_protocol_SchemaLineagePublication_drop(&publication);
        return err;
    }
    *out = publication;
    return JZ_OK;
}

static jz_err jazz_node__node__codec__encode_catalogue_staged_lineage_with_mapping(const jazz_node_StagedSchemaLineage *staged, jz_slice mapping, jz_bytes *out);

jz_err jazz_node__node__codec__encode_catalogue_staged_lineage(const jazz_node_StagedSchemaLineage *staged, jz_bytes *out) {
    jz_bytes mapping;
    JZ_TRY(jazz_node__node__codec__encode_physical_mapping(&staged->mapping, &mapping));
    jz_err err = jazz_node__node__codec__encode_catalogue_staged_lineage_with_mapping(staged, jz_bytes_slice(&mapping), out);
    jz_bytes_drop(&mapping);
    return err;
}

static jz_err jazz_node__node__codec__encode_catalogue_staged_lineage_with_mapping(const jazz_node_StagedSchemaLineage *staged, jz_slice mapping, jz_bytes *out) {
    jz_bytes publication;
    JZ_TRY(jazz_node__node__codec__encode_catalogue_publication(&staged->publication, &publication));
    jz_bytes payload = {0};
    jz_bytes_push_u8(&payload, CATALOGUE_STAGED_LINEAGE_VERSION);
    jzn_put_u64(&payload, staged->catalogue_seq);
    jz_err err = jazz_node__node__codec__put_len(&payload, publication.len, "staged catalogue publication length");
    jz_bytes_push(&payload, publication.ptr, publication.len);
    jz_bytes_drop(&publication);
    jzn_put_u64(&payload, staged->alias);
    if (!JZ_IS_ERR(err)) err = jazz_node__node__codec__put_len(&payload, mapping.len, "staged catalogue mapping length");
    jz_bytes_push(&payload, mapping.ptr, mapping.len);
    if (JZ_IS_ERR(err)) { jz_bytes_drop(&payload); return err; }
    *out = payload;
    return JZ_OK;
}

jz_err jazz_node__node__codec__decode_catalogue_staged_lineage(jz_slice payload, jazz_node_StagedSchemaLineage *out) {
    const char *CONTEXT = "invalid staged catalogue lineage";
    jzn_cursor cursor;
    JZ_TRY(jazz_node__node__codec__CataloguePayloadCursor__new(payload, CATALOGUE_STAGED_LINEAGE_VERSION, CONTEXT, &cursor));
    jazz_node_StagedSchemaLineage staged;
    jz_slice publication_bytes, mapping_payload;
    JZ_TRY(CUR_U64(&cursor, &staged.catalogue_seq));
    JZ_TRY(CUR_SIZED(&cursor, &publication_bytes));
    JZ_TRY(jazz_node__node__codec__decode_catalogue_publication(publication_bytes, &staged.publication));
    jz_err err = CUR_U64(&cursor, &staged.alias);
    if (!JZ_IS_ERR(err)) err = CUR_SIZED(&cursor, &mapping_payload);
    if (JZ_IS_ERR(err)) { jazz_protocol_SchemaLineagePublication_drop(&staged.publication); return err; }
    err = jazz_node__node__codec__decode_physical_mapping(mapping_payload, &staged.mapping);
    if (JZ_IS_ERR(err)) { jazz_protocol_SchemaLineagePublication_drop(&staged.publication); return err; }
    err = jazz_node__node__codec__CataloguePayloadCursor__finish(&cursor);
    if (!JZ_IS_ERR(err)) {
        jz_bytes mapping_again, staged_again;
        err = jazz_node__node__codec__encode_physical_mapping(&staged.mapping, &mapping_again);
        if (!JZ_IS_ERR(err)) {
            bool mapping_same = jz_slice_cmp(jz_bytes_slice(&mapping_again), mapping_payload) == 0;
            jz_bytes_drop(&mapping_again);
            if (!mapping_same) err = JZN_ISV(CONTEXT);
            else {
                err = jazz_node__node__codec__encode_catalogue_staged_lineage_with_mapping(&staged, mapping_payload, &staged_again);
                if (!JZ_IS_ERR(err)) {
                    bool same = jz_slice_cmp(jz_bytes_slice(&staged_again), payload) == 0;
                    jz_bytes_drop(&staged_again);
                    if (!same) err = JZN_ISV(CONTEXT);
                }
            }
        }
    }
    if (JZ_IS_ERR(err)) {
        jazz_protocol_SchemaLineagePublication_drop(&staged.publication);
        jazz_node_SchemaPhysicalMapping_drop(&staged.mapping);
        return err;
    }
    *out = staged;
    return JZ_OK;
}

jz_err jazz_node__node__codec__encode_catalogue_pending_lineage(const jazz_node_PendingSchemaLineage *pending, jz_bytes *out) {
    jz_bytes publication;
    JZ_TRY(jazz_node__node__codec__encode_catalogue_publication(&pending->publication, &publication));
    jz_bytes payload = {0};
    jz_bytes_push_u8(&payload, CATALOGUE_PENDING_LINEAGE_VERSION);
    jzn_put_u64(&payload, pending->catalogue_seq);
    jz_err err = jazz_node__node__codec__put_len(&payload, publication.len, "pending catalogue publication length");
    jz_bytes_push(&payload, publication.ptr, publication.len);
    jz_bytes_drop(&publication);
    if (JZ_IS_ERR(err)) { jz_bytes_drop(&payload); return err; }
    *out = payload;
    return JZ_OK;
}

jz_err jazz_node__node__codec__decode_catalogue_pending_lineage(jz_slice payload, jazz_node_PendingSchemaLineage *out) {
    const char *CONTEXT = "invalid pending catalogue lineage";
    jzn_cursor cursor;
    JZ_TRY(jazz_node__node__codec__CataloguePayloadCursor__new(payload, CATALOGUE_PENDING_LINEAGE_VERSION, CONTEXT, &cursor));
    jazz_node_PendingSchemaLineage pending;
    jz_slice publication_bytes;
    JZ_TRY(CUR_U64(&cursor, &pending.catalogue_seq));
    JZ_TRY(CUR_SIZED(&cursor, &publication_bytes));
    JZ_TRY(jazz_node__node__codec__decode_catalogue_publication(publication_bytes, &pending.publication));
    jz_err err = jazz_node__node__codec__CataloguePayloadCursor__finish(&cursor);
    if (!JZ_IS_ERR(err)) {
        jz_bytes again;
        err = jazz_node__node__codec__encode_catalogue_pending_lineage(&pending, &again);
        if (!JZ_IS_ERR(err)) {
            bool same = jz_slice_cmp(jz_bytes_slice(&again), payload) == 0;
            jz_bytes_drop(&again);
            if (!same) err = JZN_ISV(CONTEXT);
        }
    }
    if (JZ_IS_ERR(err)) { jazz_protocol_SchemaLineagePublication_drop(&pending.publication); return err; }
    *out = pending;
    return JZ_OK;
}

jz_err jazz_node__node__codec__encode_active_schema(const jazz_node_ActiveSchema *active, jz_bytes *out) {
    jazz_model_SchemaVersion version;
    jazz_protocol__protocol__SchemaVersion__new(&active->compiled, &version);
    jz_bytes schema;
    jz_err err = jazz_node__node__codec__encode_catalogue_schema(&version, &schema);
    jazz_model_SchemaVersion_drop(&version);
    if (JZ_IS_ERR(err)) return err;
    if (schema.len > 0xffffffffu) { jz_bytes_drop(&schema); return JZN_ISV("active schema too large"); }
    jz_bytes bytes = {0};
    jz_bytes_push_u8(&bytes, 1);
    jzn_put_u64(&bytes, active->revision);
    jzn_put_u32(&bytes, schema.len);
    jz_bytes_push(&bytes, schema.ptr, schema.len);
    jz_bytes_drop(&schema);
    *out = bytes;
    return JZ_OK;
}

jz_err jazz_node__node__codec__decode_active_schema(jz_slice bytes, jazz_node_ActiveSchema *out) {
    jzn_cursor cursor;
    JZ_TRY(jazz_node__node__codec__CataloguePayloadCursor__new(bytes, 1, "invalid active schema payload", &cursor));
    u64 revision;
    u32 length;
    jz_slice body;
    JZ_TRY(CUR_U64(&cursor, &revision));
    JZ_TRY(CUR_U32(&cursor, &length));
    JZ_TRY(jazz_node__node__codec__CataloguePayloadCursor__bytes(&cursor, length, &body));
    jazz_model_SchemaVersion schema;
    JZ_TRY(jazz_node__node__codec__decode_catalogue_schema(body, &schema));
    jz_err err = jazz_node__node__codec__CataloguePayloadCursor__finish(&cursor);
    if (JZ_IS_ERR(err)) { jazz_model_SchemaVersion_drop(&schema); return err; }
    jazz_protocol_CurrentWriteSchema pointer = { revision, schema.id };
    return jazz_node__node__ActiveSchema__new(pointer, schema.schema, out);
}

/* ======================= VersionRecordFromNode ======================= */

jz_err jazz_node__node__codec__VersionRecordFromNode__from_commit(const jazz_node_MergeableCommit *commit, const jazz_model_TableSchema *table,
                                                                 jazz_types_SchemaVersionId schema_version, jazz_protocol_VersionRecord **out) {
    jazz_types_TxTime probe;
    jz_err err = jazz_types__time__TxTime__from_physical_ms(commit->now_ms, &probe);
    if (JZ_IS_ERR(err)) {
        jz_err_drop(&err);
        return jz_err_new(JZN_E_INVALID_MERGEABLE_COMMIT, "commit now_ms exceeds packed HLC physical-millisecond range");
    }
    groove_ValueVec positional;
    JZ_TRY(jazz_node__node__codec__positional_cells_from_map(table, &commit->cells, &positional));
    JZ_VEC(const groove_Value *) cells = {0};
    for (u32 i = 0; i < positional.len; i++) JZ_VEC_PUSH(cells, (const groove_Value *)positional.ptr[i].v.nullable);
    jazz_protocol_VersionRecord *record;
    err = jazz_protocol__protocol__VersionRecord__encode(table, schema_version, commit->row_uuid, commit->made_by, commit->now_ms,
                                                         commit->made_by, commit->now_ms, cells.ptr, cells.len, commit->deletion, &record);
    jz_vec_drop(&cells, sizeof(void *), NULL);
    jzn_values_drop(&positional);
    if (JZ_IS_ERR(err)) return err;
    if (commit->authored_columns) {
        jz_btree copy;
        jz_btree_clone(&copy, commit->authored_columns, NULL, NULL);
        jazz_protocol__protocol__VersionRecord__with_authored_columns(record, &copy);
    } else {
        jazz_protocol__protocol__VersionRecord__with_authored_columns(record, NULL);
    }
    *out = record;
    return JZ_OK;
}

typedef struct jzn_from_stored_ctx {
    const jazz_node_VersionRow *stored;
    groove_BorrowedRecord input;
    groove_RecordDescriptor descriptor;
} jzn_from_stored_ctx;

static jz_err jzn_from_stored_field(void *ctx, u32 index, jz_bytes *output) {
    jzn_from_stored_ctx *c = ctx;
    /* Both stored layouts share the provenance prefix. Timestamps are packed
     * HLC values in storage and milliseconds on wire. */
    bool has_source = true;
    u32 source = 0;
    if (index == 0) source = JZN_HISTORY_FIELD_ROW_UUID_IDX;
    else if (index == 1) source = JZN_HISTORY_FIELD_CREATED_BY_IDX;
    else if (index == 5) source = JZN_HISTORY_FIELD__DELETION_IDX;
    else if (index >= JZN_WIRE_USER_CELLS) source = JZN_HISTORY_USER_CELLS + index - JZN_WIRE_USER_CELLS;
    else has_source = false;
    if (has_source) {
        u32 start, end;
        JZ_TRY(groove__records__RecordDescriptor__field_span(*c->input.descriptor, c->input.raw, source, &start, &end));
        jz_bytes_push(output, c->input.raw.ptr + start, end - start);
        return JZ_OK;
    }
    groove_Value value;
    switch (index) {
    case 2: value = groove_value_u64(jazz_types__time__TxTime__physical_ms(jazz_node__node__codec__VersionRow__created_at(c->stored))); break;
    case 3: JZ_TRY(jazz_node__node__codec__row_author_value(jazz_node__node__codec__VersionRow__updated_by(c->stored), &value)); break;
    case 4: value = groove_value_u64(jazz_types__time__TxTime__physical_ms(jazz_node__node__codec__VersionRow__updated_at(c->stored))); break;
    default: value = groove_value_null(); break;
    }
    jz_err err = groove__records__RecordDescriptor__encode_field_into(c->descriptor, index, &value, output);
    groove_Value_drop(&value);
    return err;
}

jz_err jazz_node__node__codec__VersionRecordFromNode__from_stored(const jazz_node_VersionRow *stored, const jazz_model_TableSchema *table,
    jazz_types_SchemaVersionId schema_version, jz_btree *authored_columns, jazz_protocol_VersionRecord **out) {
    jzn_from_stored_ctx c;
    c.stored = stored;
    c.descriptor = jazz_node__node__codec__prepared_wire_record_descriptor(table);
    c.input = groove_OwnedRecord_borrowed(&stored->record);
    jz_bytes raw;
    jz_err err = groove__records__RecordDescriptor__create_with_encoded_fields(c.descriptor, c.input.raw.len, jzn_from_stored_field, &c, &raw);
    if (JZ_IS_ERR(err)) { if (authored_columns) jz_btree_drop(authored_columns); return err; }
    jazz_protocol_RowBase base;
    err = jazz_node__node__codec__VersionRow__base(stored, &base);
    jz_bytes signs = {0};
    if (!JZ_IS_ERR(err)) err = jazz_node__node__codec__VersionRow__counter_signs(stored, &signs);
    if (JZ_IS_ERR(err)) { jz_bytes_drop(&raw); if (authored_columns) jz_btree_drop(authored_columns); return err; }
    groove_OwnedRecord record = { raw, c.descriptor };
    jazz_protocol_VersionRecord *v = jazz_protocol__protocol__VersionRecord__new(jz_str_clone(&table->name), schema_version, record);
    jazz_model_BranchKey key;
    jazz_model_BranchKey_clone(&key, jazz_node__node__codec__VersionRow__branch_key(stored));
    jazz_protocol__protocol__VersionRecord__with_branch_key(v, key);
    jazz_protocol__protocol__VersionRecord__with_authored_columns(v, authored_columns);
    jazz_protocol__protocol__VersionRecord__with_base(v, base);
    jazz_protocol__protocol__VersionRecord__with_counter_signs(v, signs);
    *out = v;
    return JZ_OK;
}

/* Release builds compile the layout assertions out (cfg(debug_assertions)). */
void jazz_node__node__codec__debug_assert_lowered_layouts(const jazz_model_JazzSchema *schema) { (void)schema; }

/* ======================= StoredTransaction ======================= */

/* Merge incoming status without undoing an already known outcome, global
 * time, or durability. */
jz_err jazz_node__node__codec__StoredTransaction__reconcile_fate(jazz_node_StoredTransaction *t, jazz_model_Fate fate,
    bool has_global_time, jazz_types_GlobalTime global_time, bool has_durability, u32 durability) {
    if (t->has_global_time && has_global_time && global_time < t->global_time) {
        jazz_model_Fate_drop(&fate);
        return jz_err_new(JZN_E_NON_MONOTONE_STATE, "global seq cannot move backwards");
    }
    jazz_model_Fate next;
    JZ_TRY(jazz_node__node__codec__next_fate(&t->fate, fate, &next));
    jazz_model_Fate_drop(&t->fate);
    t->fate = next;
    if (has_global_time) { t->has_global_time = true; t->global_time = global_time; }
    if (has_durability && durability > t->durability) t->durability = durability;
    return JZ_OK;
}

void jazz_node__node__codec__StoredTransaction__to_record(const jazz_node_StoredTransaction *t, jazz_model_TransactionRecord *out) {
    jazz_model_Transaction copy;
    jazz_model_Transaction_clone(&copy, &t->tx);
    out->tx_id = t->tx.tx_id;
    out->made_by = t->tx.made_by;
    out->kind = t->tx.kind;
    out->n_total_writes = t->tx.n_total_writes;
    jazz_model_Fate_clone(&out->fate, &t->fate);
    out->has_global_time = t->has_global_time;
    out->global_time = t->global_time;
    out->durability = t->durability;
    out->user_metadata_json = copy.user_metadata_json;
    out->contribution_merge = copy.contribution_merge;
    copy.user_metadata_json = NULL;
    copy.contribution_merge = NULL;
    jazz_model_Transaction_drop(&copy);
}

/* ======================= VersionRow ======================= */

void jazz_node_VersionRow_clone(void *dst, const void *src) {
    jazz_node_VersionRow *d = dst; const jazz_node_VersionRow *s = src;
    d->table = s->table;
    jazz_model_BranchKey_clone(&d->branch_key, &s->branch_key);
    groove_OwnedRecord_clone(&d->record, &s->record);
}
void jazz_node_VersionRow_drop(void *p) {
    jazz_node_VersionRow *v = p;
    jazz_model_BranchKey_drop(&v->branch_key);
    groove_OwnedRecord_drop(&v->record);
}

/* Record layout depends on the table name, the ordered physical column shape
 * and each column's merge strategy. Thread-local bounded cache. */
typedef struct jzn_history_cache_column { jz_str name; groove_ValueType column_type; u32 kind; u32 strategy; } jzn_history_cache_column;
typedef struct jzn_history_cache_entry {
    jz_str table_name;
    JZ_VEC(jzn_history_cache_column) columns;
    groove_RecordDescriptor descriptor;
    groove_RecordDescriptor wire_descriptor;
} jzn_history_cache_entry;
static JZ_VEC(jzn_history_cache_entry) jzn_history_cache;

static void jzn_history_cache_entry_drop(void *p) {
    jzn_history_cache_entry *e = p;
    jz_str_drop(&e->table_name);
    for (u32 i = 0; i < e->columns.len; i++) {
        jz_str_drop(&e->columns.ptr[i].name);
        groove_ValueType_drop(&e->columns.ptr[i].column_type);
    }
    jz_vec_drop(&e->columns, sizeof(jzn_history_cache_column), NULL);
}

static void jazz_node__node__codec__version_record_descriptors(const jazz_model_TableSchema *table, groove_RecordDescriptor *history,
                                                              groove_RecordDescriptor *wire) {
    for (u32 i = 0; i < jzn_history_cache.len; i++) {
        const jzn_history_cache_entry *entry = &jzn_history_cache.ptr[i];
        if (!jz_str_eq(&entry->table_name, &table->name) || entry->columns.len != table->columns.len) continue;
        bool all = true;
        for (u32 c = 0; c < entry->columns.len && all; c++) {
            const jzn_history_cache_column *cc = &entry->columns.ptr[c];
            const jazz_model_ColumnSchema *column = &table->columns.ptr[c];
            all = jz_str_eq(&cc->name, &column->name) && groove__schema__ColumnType__eq(&cc->column_type, &column->column_type) &&
                  cc->kind == column->large_value_kind && cc->strategy == jazz_model__schema__TableSchema__merge_strategy(table, &column->name);
        }
        if (all) { *history = entry->descriptor; *wire = entry->wire_descriptor; return; }
    }
    groove_TableSchema authored = jazz_model__schema__TableSchema__authored_history_storage_table(table);
    groove_RecordDescriptor descriptor = groove__schema__TableSchema__record_schema(&authored);
    groove_TableSchema_drop(&authored);
    groove_RecordDescriptor wire_descriptor = jazz_model__schema__TableSchema__wire_record_descriptor(table);
    /* Bound retained preparation data even for applications with continual
     * schema changes. Eviction affects preparation cost only, never bytes. */
    if (jzn_history_cache.len == 128) {
        jzn_history_cache_entry old;
        jz_vec_remove(&jzn_history_cache, 0, &old, sizeof old);
        jzn_history_cache_entry_drop(&old);
    }
    jzn_history_cache_entry entry;
    memset(&entry, 0, sizeof entry);
    entry.table_name = jz_str_clone(&table->name);
    for (u32 c = 0; c < table->columns.len; c++) {
        jzn_history_cache_column cc;
        cc.name = jz_str_clone(&table->columns.ptr[c].name);
        groove_ValueType_clone(&cc.column_type, &table->columns.ptr[c].column_type);
        cc.kind = table->columns.ptr[c].large_value_kind;
        cc.strategy = jazz_model__schema__TableSchema__merge_strategy(table, &table->columns.ptr[c].name);
        JZ_VEC_PUSH(entry.columns, cc);
    }
    entry.descriptor = descriptor;
    entry.wire_descriptor = wire_descriptor;
    JZ_VEC_PUSH(jzn_history_cache, entry);
    *history = descriptor;
    *wire = wire_descriptor;
}

groove_RecordDescriptor jazz_node__node__codec__prepared_wire_record_descriptor(const jazz_model_TableSchema *t) {
    groove_RecordDescriptor h, w;
    jazz_node__node__codec__version_record_descriptors(t, &h, &w);
    return w;
}
groove_RecordDescriptor jazz_node__node__codec__history_record_descriptor(const jazz_model_TableSchema *t) {
    groove_RecordDescriptor h, w;
    jazz_node__node__codec__version_record_descriptors(t, &h, &w);
    return h;
}

/* The same row image with its record fields replaced, keeping the layout. */
jz_err jazz_node__node__codec__VersionRow__with_record_values(const jazz_node_VersionRow *v, groove_ValueVec values, jazz_node_VersionRow *out) {
    groove_OwnedRecord record;
    JZ_TRY(jazz_node__node__codec__owned_record_from_storage_values_with_descriptor(v->record.descriptor, values, &record));
    out->table = v->table;
    jazz_model_BranchKey_clone(&out->branch_key, &v->branch_key);
    out->record = record;
    return JZ_OK;
}

jz_err jazz_node__node__codec__VersionRow__from_parts_with_schema_version(const jazz_model_TableSchema *table, jazz_node_VersionRowParts parts,
    const groove_RecordDescriptor *history_descriptor, jazz_node_VersionRow *out) {
    groove_ValueVec values;
    jz_err err = jazz_node__node__codec__history_values_from_parts(table, &parts, &values);
    groove_OwnedRecord record;
    if (!JZ_IS_ERR(err))
        err = jazz_node__node__codec__owned_record_from_storage_values_with_descriptor(
            history_descriptor ? *history_descriptor : jazz_node__node__codec__history_record_descriptor(table), values, &record);
    jz_btree_drop(&parts.cells);
    if (parts.authored_columns) { jz_btree_drop(parts.authored_columns); jz_free(parts.authored_columns); }
    jz_bytes_drop(&parts.counter_signs);
    if (JZ_IS_ERR(err)) { jz_str_drop(&parts.table); jazz_model_BranchKey_drop(&parts.branch_key); return err; }
    out->table = groove__Intern__new_str(parts.table);
    out->branch_key = parts.branch_key;
    out->record = record;
    return JZ_OK;
}

static groove_Value jazz_node__node__codec__authored_column_ids_value(const jz_btree *columns);

typedef struct jzn_from_wire_ctx {
    const jazz_model_TableSchema *table;
    const jazz_protocol_VersionRecord *version;
    const jz_btree *authored_columns;
    groove_RecordDescriptor descriptor;
    groove_BorrowedRecord source;
    jazz_types_NodeAlias tx_node_alias;
    jazz_types_SchemaVersionAlias schema_version_alias;
    jazz_types_TxTime tx_time;
    jazz_types_GlobalTime seq;
    u64 created_at, updated_at;
    u32 deletion;
    u32 first_merge_field;
} jzn_from_wire_ctx;

static jz_err jzn_from_wire_field(void *ctx, u32 index, jz_bytes *output) {
    jzn_from_wire_ctx *c = ctx;
    u32 ncols = c->table->columns.len;
    bool has_source = true;
    u32 source_index = 0;
    if (index == 1) source_index = 0;
    else if (index == 5) source_index = 1;
    else if (index == 7) source_index = 3;
    else if (index == 9) source_index = 5;
    else if (index >= JZN_HISTORY_USER_CELLS && index < JZN_HISTORY_USER_CELLS + ncols) source_index = index - JZN_HISTORY_USER_CELLS + 6;
    else has_source = false;
    if (has_source) {
        const groove_DescriptorFieldVec *sf = groove__records__RecordDescriptor__fields(*c->source.descriptor);
        const groove_DescriptorFieldVec *df = groove__records__RecordDescriptor__fields(c->descriptor);
        if (source_index < sf->len && groove_ValueType_eq(&sf->ptr[source_index].value_type, &df->ptr[index].value_type)) {
            u32 start, end;
            JZ_TRY(groove__records__RecordDescriptor__field_span(*c->source.descriptor, c->source.raw, source_index, &start, &end));
            jz_bytes_push(output, c->source.raw.ptr + start, end - start);
            return JZ_OK;
        }
    }
    groove_Value value;
    switch (index) {
    case 0: {
        jz_bytes b;
        jazz_model__model__branch__BranchKey__canonical_bytes(jazz_protocol__protocol__VersionRecord__branch_key(c->version), &b);
        value = jzn_v_bytes(b);
        break;
    }
    case 1: value = jzn_v_uuid(jazz_protocol__protocol__VersionRecord__row_uuid(c->version)); break;
    case 2: value = groove_value_u64(c->tx_time); break;
    case 3: value = groove_value_u64(c->tx_node_alias); break;
    case 4: value = groove_value_u64(c->schema_version_alias); break;
    case 5: JZ_TRY(jazz_node__node__codec__row_author_value(jazz_protocol__protocol__VersionRecord__created_by(c->version), &value)); break;
    case 6: value = groove_value_u64(c->created_at); break;
    case 7: JZ_TRY(jazz_node__node__codec__history_updated_by_value(jazz_protocol__protocol__VersionRecord__updated_by(c->version), &value)); break;
    case 8: value = groove_value_u64(c->updated_at); break;
    case 9: value = jazz_node__node__codec__nullable_deletion_value(c->deletion); break;
    default:
        if (index < JZN_HISTORY_USER_CELLS + ncols) {
            u32 column = index - JZN_HISTORY_USER_CELLS;
            groove_Value cell;
            if (jazz_protocol__protocol__VersionRecord__optional_cell_at(c->version, column, &cell)) {
                jz_err err = jazz_node__node__codec__validate_cell_value(&c->table->columns.ptr[column], &cell);
                if (JZ_IS_ERR(err)) { groove_Value_drop(&cell); return err; }
                value = groove_value_some(cell);
            } else {
                value = groove_value_null();
            }
        } else if (index == JZN_HISTORY_USER_CELLS + ncols) {
            value = jazz_node__node__codec__authored_column_ids_value(c->authored_columns);
        } else if (index == JZN_HISTORY_USER_CELLS + ncols + 1) {
            jz_slice signs = jazz_protocol__protocol__VersionRecord__counter_signs(c->version);
            value = jzn_v_bytes(jz_bytes_from(signs.ptr, signs.len));
        } else {
            switch (index - c->first_merge_field) {
            case 0: value = groove_value_u64(c->seq); break;
            case 1: value = jazz_node__node__codec__base_seq_value(jazz_protocol__protocol__VersionRecord__base(c->version)); break;
            case 2: value = jazz_node__node__codec__base_pending_value(jazz_protocol__protocol__VersionRecord__base(c->version)); break;
            default: return JZN_ISV("history layout does not end with the merge fields");
            }
        }
        break;
    }
    jz_err err = groove__records__RecordDescriptor__encode_field_into(c->descriptor, index, &value, output);
    groove_Value_drop(&value);
    return err;
}

jz_err jazz_node__node__codec__VersionRow__from_wire_with_schema_version(const jazz_model_TableSchema *table, const jazz_protocol_VersionRecord *version,
    const jz_btree *authored_columns, jazz_types_NodeAlias tx_node_alias, jazz_types_SchemaVersionAlias schema_version_alias,
    jazz_types_TxTime tx_time, jazz_types_GlobalTime seq, jazz_node_VersionRow *out) {
    if (!jazz_model__model__branch__BranchKey__is_canonical(jazz_protocol__protocol__VersionRecord__branch_key(version)))
        return jz_err_new(JZN_E_INVALID_MERGEABLE_COMMIT, "row version branch key is not canonical");
    jzn_from_wire_ctx c;
    c.table = table;
    c.version = version;
    c.authored_columns = authored_columns;
    c.deletion = jazz_protocol__protocol__VersionRecord__deletion(version);
    JZ_TRY(jazz_node__node__merge_ops__validate_counter_signs(table, jazz_protocol__protocol__VersionRecord__counter_signs(version)));
    /* authored_columns and counter_signs precede the merge fields. */
    c.first_merge_field = JZN_HISTORY_USER_CELLS + table->columns.len + 2;
    c.descriptor = jazz_node__node__codec__history_record_descriptor(table);
    c.source = groove_OwnedRecord_borrowed(jazz_protocol__protocol__VersionRecord__record(version));
    c.tx_node_alias = tx_node_alias;
    c.schema_version_alias = schema_version_alias;
    c.tx_time = tx_time;
    c.seq = seq;
    /* Admission still requires an account-bearing row author, including the
     * reserved SYSTEM identity. */
    jazz_types_RowAuthor ra;
    jz_err err = jazz_types__ids__RowAuthor__from_persisted_subject(jazz_protocol__protocol__VersionRecord__created_by(version), &ra);
    if (JZ_IS_ERR(err)) { jz_err_drop(&err); return jz_err_new(JZN_E_UNADMITTED_WRITE_AUTHOR, "unadmitted write author"); }
    err = jazz_types__ids__RowAuthor__from_persisted_subject(jazz_protocol__protocol__VersionRecord__updated_by(version), &ra);
    if (JZ_IS_ERR(err)) { jz_err_drop(&err); return jz_err_new(JZN_E_UNADMITTED_WRITE_AUTHOR, "unadmitted write author"); }
    jazz_types_TxTime t;
    err = jazz_types__time__TxTime__from_physical_ms(jazz_protocol__protocol__VersionRecord__created_at_ms(version), &t);
    if (JZ_IS_ERR(err)) { jz_err_drop(&err); return JZN_ISV("wire created_at_ms exceeds packed HLC range"); }
    c.created_at = t;
    err = jazz_types__time__TxTime__from_physical_ms(jazz_protocol__protocol__VersionRecord__updated_at_ms(version), &t);
    if (JZ_IS_ERR(err)) { jz_err_drop(&err); return JZN_ISV("wire updated_at_ms exceeds packed HLC range"); }
    c.updated_at = t;
    jz_bytes raw;
    JZ_TRY(groove__records__RecordDescriptor__create_with_encoded_fields(c.descriptor, c.source.raw.len + 128, jzn_from_wire_field, &c, &raw));
    out->table = groove__Intern__new_str(jz_str_clone(jazz_protocol__protocol__VersionRecord__table(version)));
    jazz_model_BranchKey_clone(&out->branch_key, jazz_protocol__protocol__VersionRecord__branch_key(version));
    out->record.raw = raw;
    out->record.descriptor = c.descriptor;
    return JZ_OK;
}

const jazz_model_BranchKey *jazz_node__node__codec__VersionRow__branch_key(const jazz_node_VersionRow *v) { return &v->branch_key; }

static u64 jzn_row_u64(const jazz_node_VersionRow *v, u32 idx, const char *what) {
    groove_BorrowedRecord r = groove_OwnedRecord_borrowed(&v->record);
    u64 x;
    jz_err err = groove__records__BorrowedRecord__get_u64(&r, idx, &x);
    JZ_ASSERT(!JZ_IS_ERR(err), what);
    return x;
}

jazz_types_RowUuid jazz_node__node__codec__VersionRow__row_uuid(const jazz_node_VersionRow *v) {
    groove_BorrowedRecord r = groove_OwnedRecord_borrowed(&v->record);
    jz_uuid u;
    jz_err err = groove__records__BorrowedRecord__get_uuid(&r, JZN_HISTORY_FIELD_ROW_UUID_IDX, &u);
    JZ_ASSERT(!JZ_IS_ERR(err), "valid row_uuid");
    return u;
}
jazz_types_NodeAlias jazz_node__node__codec__VersionRow__tx_node_alias(const jazz_node_VersionRow *v) { return jzn_row_u64(v, JZN_HISTORY_FIELD_TX_NODE_ID_IDX, "valid tx_node_id"); }
jazz_types_TxTime jazz_node__node__codec__VersionRow__tx_time(const jazz_node_VersionRow *v) { return jzn_row_u64(v, JZN_HISTORY_FIELD_TX_TIME_IDX, "valid tx_time"); }
jz_err jazz_node__node__codec__VersionRow__validate_canonical(const jazz_node_VersionRow *v) {
    return jazz_node__node__codec__validate_canonical_version_parts(&v->branch_key);
}

jazz_types_AuthorSubject jazz_node__node__codec__VersionRow__created_by(const jazz_node_VersionRow *v) {
    groove_BorrowedRecord r = groove_OwnedRecord_borrowed(&v->record), sub;
    jz_err err = groove__records__BorrowedRecord__get_record(&r, JZN_HISTORY_FIELD_CREATED_BY_IDX, &sub);
    JZ_ASSERT(!JZ_IS_ERR(err), "valid created_by");
    jazz_types_RowAuthor a;
    err = jazz_types__ids__RowAuthor__from_record(&sub, &a);
    JZ_ASSERT(!JZ_IS_ERR(err), "canonical created_by");
    return jazz_types__ids__RowAuthor__as_author_subject(a);
}

jazz_types_TxTime jazz_node__node__codec__VersionRow__created_at(const jazz_node_VersionRow *v) { return jzn_row_u64(v, JZN_HISTORY_FIELD_CREATED_AT_IDX, "valid created_at"); }

/* History storage omits updated_by when it is the author of the image's own
 * transaction; every read path fills it in before a VersionRow leaves storage. */
jazz_types_AuthorSubject jazz_node__node__codec__VersionRow__updated_by(const jazz_node_VersionRow *v) {
    groove_BorrowedRecord r = groove_OwnedRecord_borrowed(&v->record);
    bool some;
    jazz_types_RowAuthor a;
    jz_err err = jazz_types__ids__RowAuthor__read_optional(&r, JZN_HISTORY_FIELD_UPDATED_BY_IDX, &some, &a);
    JZ_ASSERT(!JZ_IS_ERR(err), "valid updated_by");
    JZ_ASSERT(some, "history updated_by is resolved when the image is read");
    return jazz_types__ids__RowAuthor__as_author_subject(a);
}

jz_err jazz_node__node__codec__VersionRow__updated_by_is_implicit(const jazz_node_VersionRow *v, bool *out) {
    groove_BorrowedRecord r = groove_OwnedRecord_borrowed(&v->record);
    groove_Value value;
    JZ_TRY(groove__records__BorrowedRecord__get_idx(&r, JZN_HISTORY_FIELD_UPDATED_BY_IDX, &value));
    *out = value.tag == GROOVE_V_NULLABLE && value.v.nullable == NULL;
    groove_Value_drop(&value);
    return JZ_OK;
}

jazz_types_TxTime jazz_node__node__codec__VersionRow__updated_at(const jazz_node_VersionRow *v) { return jzn_row_u64(v, JZN_HISTORY_FIELD_UPDATED_AT_IDX, "valid updated_at"); }
jazz_types_SchemaVersionAlias jazz_node__node__codec__VersionRow__schema_version_alias(const jazz_node_VersionRow *v) {
    return jzn_row_u64(v, JZN_HISTORY_FIELD_SCHEMA_VERSION_IDX, "valid schema_version");
}

/* The row's deletion state, stamped into the row image like any cell. */
u32 jazz_node__node__codec__VersionRow__deletion(const jazz_node_VersionRow *v) {
    groove_BorrowedRecord r = groove_OwnedRecord_borrowed(&v->record);
    groove_Value value, inner;
    jz_err err = groove__records__BorrowedRecord__get_idx(&r, JZN_HISTORY_FIELD__DELETION_IDX, &value);
    JZ_ASSERT(!JZ_IS_ERR(err), "valid deletion");
    bool some;
    err = jazz_node__node__codec__nullable_value(value, &some, &inner);
    JZ_ASSERT(!JZ_IS_ERR(err), "valid deletion");
    if (!some) return JZN_DELETION_NONE;
    u32 d;
    err = jazz_node__node__codec__deletion_event_from_value(inner, &d);
    JZ_ASSERT(!JZ_IS_ERR(err), "valid deletion");
    return d;
}

bool jazz_node__node__codec__VersionRow__is_deleted(const jazz_node_VersionRow *v) {
    return jazz_node__node__codec__VersionRow__deletion(v) == JZN_DELETION_DELETED;
}

static const jz_btree_desc jzn_desc_str_value = { sizeof(jz_str), sizeof(groove_Value), jz_cmp_str, jzn_str_drop_v, jzn_value_drop_v };

jz_err jazz_node__node__codec__VersionRow__cells(const jazz_node_VersionRow *v, const jazz_model_TableSchema *table, jz_btree *out) {
    jz_btree cells;
    jz_btree_init(&cells, &jzn_desc_str_value);
    groove_BorrowedRecord borrowed = groove_OwnedRecord_borrowed(&v->record);
    for (u32 idx = 0; idx < table->columns.len; idx++) {
        groove_Value raw, value;
        bool some;
        jz_err err = groove__records__BorrowedRecord__get_idx(&borrowed, JZN_HISTORY_USER_CELLS + idx, &raw);
        if (!JZ_IS_ERR(err)) err = jazz_node__node__codec__nullable_value(raw, &some, &value);
        if (JZ_IS_ERR(err)) { jz_btree_drop(&cells); return err; }
        if (some) {
            jz_str key = jz_str_clone(&table->columns.ptr[idx].name);
            jz_btree_insert(&cells, &key, &value, NULL);
        }
    }
    *out = cells;
    return JZ_OK;
}

jz_err jazz_node__node__codec__VersionRow__cell(const jazz_node_VersionRow *v, const jazz_model_TableSchema *table, const jz_str *column,
                                               bool *some, groove_Value *out) {
    u32 pos = 0;
    for (; pos < table->columns.len; pos++) if (jz_str_eq(&table->columns.ptr[pos].name, column)) break;
    if (pos == table->columns.len) return JZN_ISV("missing user column field");
    groove_BorrowedRecord r = groove_OwnedRecord_borrowed(&v->record);
    groove_Value raw;
    JZ_TRY(groove__records__BorrowedRecord__get_idx(&r, JZN_HISTORY_USER_CELLS + pos, &raw));
    return jazz_node__node__codec__nullable_value(raw, some, out);
}

/* None is the legacy/lens fallback: every present cell is treated as
 * authored. Exact sets use strictly increasing node-local column ids. */
jz_err jazz_node__node__codec__VersionRow__authored_column_ids(const jazz_node_VersionRow *v, bool *some, jz_btree *out) {
    *some = false;
    u32 field;
    if (!groove__records__RecordDescriptor__field_index(v->record.descriptor, JZ_LIT("authored_columns"), &field)) return JZ_OK;
    groove_BorrowedRecord r = groove_OwnedRecord_borrowed(&v->record);
    groove_Value raw, value;
    bool present;
    JZ_TRY(groove__records__BorrowedRecord__get_idx(&r, field, &raw));
    JZ_TRY(jazz_node__node__codec__nullable_value(raw, &present, &value));
    if (!present) return JZ_OK;
    JZ_TRY(jazz_node__node__codec__authored_column_ids_from_value(value, out));
    *some = true;
    return JZ_OK;
}

static jz_err jazz_node__node__codec__VersionRow__trailing_field_index(const jazz_node_VersionRow *v, const char *name, u32 *out) {
    if (!groove__records__RecordDescriptor__field_index(v->record.descriptor, jz_slice_of(name, (u32)strlen(name)), out))
        return JZN_ISV("history merge field missing");
    return JZ_OK;
}
static jz_err jazz_node__node__codec__VersionRow__trailing_field(const jazz_node_VersionRow *v, const char *name, groove_Value *out) {
    u32 idx;
    JZ_TRY(jazz_node__node__codec__VersionRow__trailing_field_index(v, name, &idx));
    groove_BorrowedRecord r = groove_OwnedRecord_borrowed(&v->record);
    return groove__records__BorrowedRecord__get_idx(&r, idx, out);
}

/* The sign bits of this patch's counter ops: empty on a settled image. */
jz_err jazz_node__node__codec__VersionRow__counter_signs(const jazz_node_VersionRow *v, jz_bytes *out) {
    groove_Value value;
    JZ_TRY(jazz_node__node__codec__VersionRow__trailing_field(v, JAZZ_SCHEMA_COUNTER_SIGNS_FIELD, &value));
    if (value.tag != GROOVE_V_BYTES) { groove_Value_drop(&value); return JZN_ISV("counter_signs field must be bytes"); }
    *out = value.v.bytes;
    return JZ_OK;
}

/* The write's accepted seq; 0 while it has no accepted fate. */
jz_err jazz_node__node__codec__VersionRow__seq(const jazz_node_VersionRow *v, jazz_types_GlobalTime *out) {
    groove_Value value;
    JZ_TRY(jazz_node__node__codec__VersionRow__trailing_field(v, JAZZ_SCHEMA_SEQ_FIELD, &value));
    if (value.tag != GROOVE_V_U64) { groove_Value_drop(&value); return JZN_ISV("history seq must be u64"); }
    *out = value.v.u;
    return JZ_OK;
}

/* The write's base. */
jz_err jazz_node__node__codec__VersionRow__base(const jazz_node_VersionRow *v, jazz_protocol_RowBase *out) {
    groove_Value raw, value;
    bool some;
    jazz_protocol_RowBase base;
    memset(&base, 0, sizeof base);
    JZ_TRY(jazz_node__node__codec__VersionRow__trailing_field(v, JAZZ_SCHEMA_BASE_SEQ_FIELD, &raw));
    JZ_TRY(jazz_node__node__codec__nullable_value(raw, &some, &value));
    if (some) {
        if (value.tag != GROOVE_V_U64) { groove_Value_drop(&value); return JZN_ISV("history base_seq must be u64"); }
        base.has_seq = true;
        base.seq = value.v.u;
    }
    JZ_TRY(jazz_node__node__codec__VersionRow__trailing_field(v, JAZZ_SCHEMA_BASE_PENDING_FIELD, &raw));
    JZ_TRY(jazz_node__node__codec__nullable_value(raw, &some, &value));
    if (some) {
        JZ_TRY(jazz_node__node__codec__tx_id_from_value(value, &base.pending));
        base.has_pending = true;
    }
    *out = base;
    return JZ_OK;
}

/* This image with its required merge fields replaced. */
jz_err jazz_node__node__codec__VersionRow__with_merge_fields(const jazz_node_VersionRow *v, jazz_types_GlobalTime seq, jazz_protocol_RowBase base,
                                                            jazz_node_VersionRow *out) {
    u32 seq_idx, base_seq_idx, base_pending_idx;
    JZ_TRY(jazz_node__node__codec__VersionRow__trailing_field_index(v, JAZZ_SCHEMA_SEQ_FIELD, &seq_idx));
    JZ_TRY(jazz_node__node__codec__VersionRow__trailing_field_index(v, JAZZ_SCHEMA_BASE_SEQ_FIELD, &base_seq_idx));
    JZ_TRY(jazz_node__node__codec__VersionRow__trailing_field_index(v, JAZZ_SCHEMA_BASE_PENDING_FIELD, &base_pending_idx));
    groove_ValueVec values;
    JZ_TRY(groove__records__OwnedRecord__to_values(&v->record, &values));
    JZ_ASSERT(seq_idx < values.len && base_seq_idx < values.len && base_pending_idx < values.len, "index out of bounds");
    groove_Value_drop(&values.ptr[seq_idx]);
    values.ptr[seq_idx] = groove_value_u64(seq);
    groove_Value_drop(&values.ptr[base_seq_idx]);
    values.ptr[base_seq_idx] = jazz_node__node__codec__base_seq_value(base);
    groove_Value_drop(&values.ptr[base_pending_idx]);
    values.ptr[base_pending_idx] = jazz_node__node__codec__base_pending_value(base);
    return jazz_node__node__codec__VersionRow__with_record_values(v, values, out);
}

/* This image at `seq`, keeping its base. */
jz_err jazz_node__node__codec__VersionRow__with_seq(const jazz_node_VersionRow *v, jazz_types_GlobalTime seq, jazz_node_VersionRow *out) {
    jazz_types_GlobalTime current;
    JZ_TRY(jazz_node__node__codec__VersionRow__seq(v, &current));
    if (current == seq) { jazz_node_VersionRow_clone(out, v); return JZ_OK; }
    jazz_protocol_RowBase base;
    JZ_TRY(jazz_node__node__codec__VersionRow__base(v, &base));
    return jazz_node__node__codec__VersionRow__with_merge_fields(v, seq, base, out);
}

jazz_model_HistoryEntry *jazz_node__node__codec__VersionRow__to_history_entry(const jazz_node_VersionRow *v, const jazz_node_StoredTransaction *tx,
                                                                             bool is_locally_current, bool is_globally_current) {
    jazz_model_TransactionRecord record;
    jazz_node__node__codec__StoredTransaction__to_record(tx, &record);
    groove_OwnedRecord version;
    groove_OwnedRecord_clone(&version, &v->record);
    return jazz_model__tx__HistoryEntry__new(jz_str_clone(v->table), version, record, is_locally_current, is_globally_current);
}

jz_err jazz_node__node__codec__owned_record_from_storage_values(const groove_TableSchema *storage_table, groove_ValueVec values, groove_OwnedRecord *out) {
    return jazz_node__node__codec__owned_record_from_storage_values_with_descriptor(groove__schema__TableSchema__record_schema(storage_table), values, out);
}

jz_err jazz_node__node__codec__owned_record_from_storage_values_with_descriptor(groove_RecordDescriptor d, groove_ValueVec values, groove_OwnedRecord *out) {
    jz_bytes raw;
    jz_err err = groove__records__RecordDescriptor__create(d, values.ptr, values.len, &raw);
    jzn_values_drop(&values);
    if (JZ_IS_ERR(err)) return err;
    out->raw = raw;
    out->descriptor = d;
    return JZ_OK;
}

u32 jazz_node__node__codec__ParkedIngressRole__strongest(u32 a, u32 b) {
    return (a == JZN_PARKED_AUTHORITY || b == JZN_PARKED_AUTHORITY) ? JZN_PARKED_AUTHORITY : JZN_PARKED_RELAY;
}

/* max_by_key over (tx_time.sort_key(node)); the last maximum wins like Iterator::max_by_key. */
bool jazz_node__node__codec__current_version_index(const jazz_node_VersionRow *versions, const u32 *candidates, u32 n,
                                                  const jazz_node_NodeAliases *aliases, u32 *out) {
    bool found = false;
    jazz_types_TxTime best_time = 0;
    jazz_types_NodeUuid best_node;
    memset(&best_node, 0, sizeof best_node);
    for (u32 i = 0; i < n; i++) {
        u32 idx = candidates[i];
        jazz_types_TxId tx_id;
        bool ok = jazz_node__node__codec__version_tx_id_from_aliases(&versions[idx], aliases, &tx_id);
        JZ_ASSERT(ok, "valid version tx id");
        jazz_types_TxTime t = jazz_node__node__codec__VersionRow__tx_time(&versions[idx]);
        if (!found || jazz_types__time__TxTime__sort_key_cmp(t, tx_id.node, best_time, best_node) >= 0) {
            found = true;
            best_time = t;
            best_node = tx_id.node;
            *out = idx;
        }
    }
    return found;
}

bool jazz_node__node__codec__version_tx_id_from_aliases(const jazz_node_VersionRow *v, const jazz_node_NodeAliases *aliases, jazz_types_TxId *out) {
    jazz_types_NodeUuid node;
    if (!jazz_node__node__node_aliases__NodeAliases__node_for_alias(aliases, jazz_node__node__codec__VersionRow__tx_node_alias(v), &node)) return false;
    out->time = jazz_node__node__codec__VersionRow__tx_time(v);
    out->node = node;
    return true;
}

/* ======================= transactions ======================= */

#define MAX_RUNTIME_RESULT_IDENTITY_BYTES (1024u * 1024u)

static groove_RecordDescriptor jazz_node__node__codec__runtime_result_value_descriptor(void) {
    static const char *const names[2] = { "descriptor", "value" };
    groove_ValueType types[2] = { groove_vt_simple(GROOVE_VT_BYTES), groove_vt_simple(GROOVE_VT_BYTES) };
    return jzn_descriptor(names, types, 2);
}

/* Encode a runtime synthetic aggregate row/replacement identity. */
jz_err jazz_node__node__codec__runtime_result_identity_bytes(const groove_Value *value, const groove_ValueType *value_type, jz_bytes *out) {
    static const char *const names[1] = { "value" };
    groove_ValueType t;
    groove_ValueType_clone(&t, value_type);
    groove_RecordDescriptor value_descriptor = jzn_descriptor(names, &t, 1);
    jz_bytes descriptor, encoded_value, encoded;
    JZ_TRY(groove__records__encode_persisted_record_descriptor(value_descriptor, &descriptor));
    jz_err err = groove__records__RecordDescriptor__create(value_descriptor, value, 1, &encoded_value);
    if (JZ_IS_ERR(err)) { jz_bytes_drop(&descriptor); return err; }
    groove_Value outer[2] = { jzn_v_bytes(descriptor), jzn_v_bytes(encoded_value) };
    err = groove__records__RecordDescriptor__create(jazz_node__node__codec__runtime_result_value_descriptor(), outer, 2, &encoded);
    groove_Value_drop(&outer[0]);
    groove_Value_drop(&outer[1]);
    if (JZ_IS_ERR(err)) return err;
    if (encoded.len > MAX_RUNTIME_RESULT_IDENTITY_BYTES) { jz_bytes_drop(&encoded); return JZN_ISV("settled result value encoding is too large"); }
    *out = encoded;
    return JZ_OK;
}

jz_err jazz_node__node__codec__transaction_values(jazz_types_NodeAlias alias, const jazz_model_Transaction *tx, const jazz_model_Fate *fate,
    bool has_global_time, jazz_types_GlobalTime global_time, u32 durability, groove_Value contribution_merge, groove_ValueVec *out) {
    return jazz_node__node__codec__transaction_values_with_partial_flag(alias, tx, fate, has_global_time, global_time, durability, false,
                                                                       contribution_merge, out);
}

static jz_err jzn_row_author_of(jazz_types_AuthorSubject s, groove_Value *out) {
    jazz_types_RowAuthor a;
    jz_err err = jazz_types__ids__RowAuthor__from_persisted_subject(s, &a);
    if (JZ_IS_ERR(err)) { jz_err_drop(&err); return jz_err_new(JZN_E_UNADMITTED_WRITE_AUTHOR, "unadmitted write author"); }
    *out = jazz_types__ids__RowAuthor__to_value(a);
    return JZ_OK;
}

static groove_Value jzn_tx_kind_value(u32 kind) {
    return jzn_v_string(jz_str_from(kind == JAZZ_TX_KIND_MERGEABLE ? "mergeable" : "exclusive"));
}

static groove_Value jzn_nullable_string(const jz_str *s) {
    return s ? groove_value_some(jzn_v_string(jz_str_clone(s))) : groove_value_null();
}

jz_err jazz_node__node__codec__transaction_values_with_partial_flag(jazz_types_NodeAlias alias, const jazz_model_Transaction *tx,
    const jazz_model_Fate *fate, bool has_global_time, jazz_types_GlobalTime global_time, u32 durability, bool is_partial,
    groove_Value contribution_merge, groove_ValueVec *out) {
    /* Slots 5-8: jazz.exclusive-read-evidence.v1 while an exclusive fate is
     * pending, null otherwise. */
    groove_Value evidence[4];
    jz_err err = jazz_node__node__exclusive_read_evidence__evidence_slot_values(tx, fate->tag == JAZZ_FATE_PENDING, evidence);
    if (JZ_IS_ERR(err)) { groove_Value_drop(&contribution_merge); return err; }
    groove_Value author;
    err = jzn_row_author_of(tx->made_by, &author);
    if (JZ_IS_ERR(err)) {
        for (int i = 0; i < 4; i++) groove_Value_drop(&evidence[i]);
        groove_Value_drop(&contribution_merge);
        return err;
    }
    groove_Value reason = groove_value_null();
    if (fate->tag == JAZZ_FATE_REJECTED) {
        groove_Value r;
        err = jazz_model__tx__RejectionReason__to_storage_value(&fate->reason, &r);
        if (JZ_IS_ERR(err)) {
            for (int i = 0; i < 4; i++) groove_Value_drop(&evidence[i]);
            groove_Value_drop(&contribution_merge);
            groove_Value_drop(&author);
            return err;
        }
        reason = groove_value_some(r);
    }
    groove_ValueVec values = {0};
    JZ_VEC_PUSH(values, groove_value_u64(tx->tx_id.time));
    JZ_VEC_PUSH(values, groove_value_u64(alias));
    JZ_VEC_PUSH(values, jzn_tx_kind_value(tx->kind));
    JZ_VEC_PUSH(values, jzn_v_u32(tx->n_total_writes));
    JZ_VEC_PUSH(values, author);
    for (int i = 0; i < 4; i++) JZ_VEC_PUSH(values, evidence[i]);
    JZ_VEC_PUSH(values, jzn_nullable_string(tx->user_metadata_json));
    JZ_VEC_PUSH(values, contribution_merge);
    JZ_VEC_PUSH(values, tx->has_permission_subject ? groove_value_some(jazz_types__ids__AuthorSubject__to_value(tx->permission_subject)) : groove_value_null());
    JZ_VEC_PUSH(values, jzn_v_bool(is_partial));
    JZ_VEC_PUSH(values, jzn_v_string(jazz_node__node__codec__fate_string(fate)));
    JZ_VEC_PUSH(values, has_global_time ? groove_value_some(groove_value_u64(global_time)) : groove_value_null());
    JZ_VEC_PUSH(values, reason);
    JZ_VEC_PUSH(values, jzn_v_string(jz_str_from(jazz_node__node__codec__durability_string(durability))));
    *out = values;
    return JZ_OK;
}

jz_err jazz_node__node__codec__rejected_transaction_values(jazz_types_NodeAlias alias, const jazz_model_Transaction *tx,
                                                          const jazz_model_RejectionReason *reason, groove_ValueVec *out) {
    groove_Value author, r;
    JZ_TRY(jzn_row_author_of(tx->made_by, &author));
    jz_err err = jazz_model__tx__RejectionReason__to_storage_value(reason, &r);
    if (JZ_IS_ERR(err)) { groove_Value_drop(&author); return err; }
    groove_ValueVec values = {0};
    JZ_VEC_PUSH(values, groove_value_u64(tx->tx_id.time));
    JZ_VEC_PUSH(values, groove_value_u64(alias));
    JZ_VEC_PUSH(values, jzn_tx_kind_value(tx->kind));
    JZ_VEC_PUSH(values, author);
    JZ_VEC_PUSH(values, r);
    JZ_VEC_PUSH(values, jzn_nullable_string(tx->user_metadata_json));
    *out = values;
    return JZ_OK;
}

jz_err jazz_node__node__codec__rejected_version_values(const jazz_model_TableSchema *table_schema, const jazz_node_VersionRow *version, groove_ValueVec *out) {
    jz_btree cells;
    JZ_TRY(jazz_node__node__codec__VersionRow__cells(version, table_schema, &cells));
    groove_ValueVec values = {0};
    JZ_VEC_PUSH(values, groove_value_u64(jazz_node__node__codec__VersionRow__tx_time(version)));
    JZ_VEC_PUSH(values, groove_value_u64(jazz_node__node__codec__VersionRow__tx_node_alias(version)));
    JZ_VEC_PUSH(values, jzn_v_uuid(jazz_node__node__codec__VersionRow__row_uuid(version)));
    JZ_VEC_PUSH(values, jazz_node__node__codec__nullable_deletion_value(jazz_node__node__codec__VersionRow__deletion(version)));
    for (u32 i = 0; i < table_schema->columns.len; i++) {
        const groove_Value *cell = jz_btree_get(&cells, &table_schema->columns.ptr[i].name);
        if (cell) { groove_Value c; groove_Value_clone(&c, cell); JZ_VEC_PUSH(values, groove_value_some(c)); }
        else JZ_VEC_PUSH(values, groove_value_null());
    }
    jz_btree_drop(&cells);
    *out = values;
    return JZ_OK;
}

jz_str jazz_node__node__codec__fate_string(const jazz_model_Fate *fate) {
    switch (fate->tag) {
    case JAZZ_FATE_PENDING: return jz_str_from("pending");
    case JAZZ_FATE_ACCEPTED: return jz_str_from("accepted");
    default: return jz_str_from("rejected");
    }
}

const char *jazz_node__node__codec__durability_string(u32 durability) {
    switch (durability) {
    case JAZZ_DURABILITY_NONE: return "none";
    case JAZZ_DURABILITY_LOCAL: return "local";
    default: return "global";
    }
}

jz_err jazz_node__node__codec__next_fate(const jazz_model_Fate *current, jazz_model_Fate incoming, jazz_model_Fate *out) {
    if (current->tag == JAZZ_FATE_PENDING) { *out = incoming; return JZ_OK; }
    if (current->tag == JAZZ_FATE_ACCEPTED && incoming.tag != JAZZ_FATE_REJECTED) {
        jazz_model_Fate_drop(&incoming);
        out->tag = JAZZ_FATE_ACCEPTED;
        return JZ_OK;
    }
    if (current->tag == JAZZ_FATE_REJECTED && incoming.tag != JAZZ_FATE_ACCEPTED) {
        /* Pending, the same reason, or a different reason: keep the known one */
        jazz_model_Fate_drop(&incoming);
        jazz_model_Fate_clone(out, current);
        return JZ_OK;
    }
    jazz_model_Fate_drop(&incoming);
    return jz_err_new(JZN_E_CONFLICTING_FATE, "conflicting fate");
}

static void jzn_redact_reads(jazz_model_Transaction *t) {
    if (t->base_snapshot) { jazz_model_Snapshot_drop(t->base_snapshot); t->base_snapshot = NULL; }
    if (t->row_read_set) { jazz_model_ReadSet_drop(t->row_read_set); t->row_read_set = NULL; }
    if (t->absent_read_set) { jazz_model_ReadSet_drop(t->absent_read_set); t->absent_read_set = NULL; }
    if (t->predicate_read_set) { jazz_model_ReadSet_drop(t->predicate_read_set); t->predicate_read_set = NULL; }
}

bool jazz_node__node__codec__known_transaction_payload_matches(const jazz_model_Transaction *existing, const jazz_model_Transaction *incoming) {
    jazz_model_Transaction redacted_existing, redacted_incoming;
    jazz_model_Transaction_clone(&redacted_existing, existing);
    jzn_redact_reads(&redacted_existing);
    jazz_model_Transaction_clone(&redacted_incoming, incoming);
    jzn_redact_reads(&redacted_incoming);
    bool result = jazz_model_Transaction_eq(existing, incoming) || jazz_model_Transaction_eq(&redacted_existing, incoming) ||
                  jazz_model_Transaction_eq(existing, &redacted_incoming) || jazz_model_Transaction_eq(&redacted_existing, &redacted_incoming);
    jazz_model_Transaction_drop(&redacted_existing);
    jazz_model_Transaction_drop(&redacted_incoming);
    return result;
}

/* Copy a transaction without its local-only policy capability. */
void jazz_node__node__codec__transaction_without_permission_subject(const jazz_model_Transaction *tx, jazz_model_Transaction *out) {
    jazz_model_Transaction_clone(out, tx);
    out->has_permission_subject = false;
}

bool jazz_node__node__codec__known_transaction_payload_matches_redacted_permission_subject(const jazz_model_Transaction *existing,
                                                                                          const jazz_model_Transaction *incoming) {
    jazz_model_Transaction a, b;
    jazz_node__node__codec__transaction_without_permission_subject(existing, &a);
    jazz_node__node__codec__transaction_without_permission_subject(incoming, &b);
    bool r = jazz_node__node__codec__known_transaction_payload_matches(&a, &b);
    jazz_model_Transaction_drop(&a);
    jazz_model_Transaction_drop(&b);
    return r;
}

bool jazz_node__node__codec__known_transaction_payload_matches_redacted_cardinality(const jazz_model_Transaction *existing,
                                                                                   const jazz_model_Transaction *incoming) {
    jazz_model_Transaction a, b;
    jazz_model_Transaction_clone(&a, existing);
    a.n_total_writes = 0;
    jazz_model_Transaction_clone(&b, incoming);
    b.n_total_writes = 0;
    bool r = jazz_node__node__codec__known_transaction_payload_matches(&a, &b);
    jazz_model_Transaction_drop(&a);
    jazz_model_Transaction_drop(&b);
    return r;
}

static int jzn_version_record_ptr_cmp(const void *a, const void *b, void *ctx) {
    return jazz_protocol_VersionRecord_cmp(*(void *const *)a, *(void *const *)b, ctx);
}
void jazz_node__node__codec__canonical_versions(jazz_protocol_VersionRecordVec *versions) {
    jz_sort(versions->ptr, versions->len, sizeof(void *), jzn_version_record_ptr_cmp, NULL);
}

/* ======================= row image values ======================= */

jz_err jazz_node__node__codec__authored_column_ids_from_value(groove_Value value, jz_btree *out) {
    /* An intentional pre-v1 storage cut: no JSON-in-Bytes representation. */
    if (value.tag != GROOVE_V_ARRAY) { groove_Value_drop(&value); return JZN_ISV("authored columns must be an array of physical column ids"); }
    jz_btree ids;
    jz_btree_init(&ids, &jzn_desc_u64_set);
    bool has_previous = false;
    u64 previous = 0;
    jz_err err = JZ_OK;
    for (u32 i = 0; i < value.v.vals.len; i++) {
        const groove_Value *v = &value.v.vals.ptr[i];
        if (v->tag != GROOVE_V_U64) { err = JZN_ISV("authored columns must contain physical column ids"); break; }
        u64 id = v->v.u;
        if (id == 0) { err = JZN_ISV("authored physical column ids must be nonzero"); break; }
        if (has_previous && previous >= id) { err = JZN_ISV("authored physical column ids must be strictly increasing"); break; }
        previous = id;
        has_previous = true;
        jz_btree_insert(&ids, &id, NULL, NULL);
    }
    groove_Value_drop(&value);
    if (JZ_IS_ERR(err)) { jz_btree_drop(&ids); return err; }
    *out = ids;
    return JZ_OK;
}

static groove_Value jazz_node__node__codec__authored_column_ids_value(const jz_btree *columns) {
    if (!columns) return groove_value_null();
    groove_ValueVec ids = {0};
    const u64 *id; const void *unused;
    JZ_BTREE_FOR(columns, id, unused) JZ_VEC_PUSH(ids, groove_value_u64(*id));
    return groove_value_some(jzn_v_array(ids));
}

jz_err jazz_node__node__codec__row_author_value(jazz_types_AuthorSubject author, groove_Value *out) {
    return jzn_row_author_of(author, out);
}

/* updated_by of an in-memory history image: always present. */
jz_err jazz_node__node__codec__history_updated_by_value(jazz_types_AuthorSubject author, groove_Value *out) {
    groove_Value v;
    JZ_TRY(jazz_node__node__codec__row_author_value(author, &v));
    *out = groove_value_some(v);
    return JZ_OK;
}

jz_err jazz_node__node__codec__history_values_from_parts(const jazz_model_TableSchema *table, const jazz_node_VersionRowParts *version, groove_ValueVec *out) {
    groove_Value created_by, updated_by;
    JZ_TRY(jazz_node__node__codec__row_author_value(version->created_by, &created_by));
    jz_err err = jazz_node__node__codec__history_updated_by_value(version->updated_by, &updated_by);
    if (JZ_IS_ERR(err)) { groove_Value_drop(&created_by); return err; }
    groove_ValueVec values = {0};
    jz_bytes branch;
    jazz_model__model__branch__BranchKey__canonical_bytes(&version->branch_key, &branch);
    JZ_VEC_PUSH(values, jzn_v_bytes(branch));
    JZ_VEC_PUSH(values, jzn_v_uuid(version->row_uuid));
    JZ_VEC_PUSH(values, groove_value_u64(version->tx_time));
    JZ_VEC_PUSH(values, groove_value_u64(version->tx_node_alias));
    JZ_VEC_PUSH(values, groove_value_u64(version->schema_version_alias));
    JZ_VEC_PUSH(values, created_by);
    JZ_VEC_PUSH(values, groove_value_u64(version->created_at));
    JZ_VEC_PUSH(values, updated_by);
    JZ_VEC_PUSH(values, groove_value_u64(version->updated_at));
    JZ_VEC_PUSH(values, jazz_node__node__codec__nullable_deletion_value(version->deletion));
    for (u32 i = 0; i < table->columns.len; i++) {
        const groove_Value *cell = jz_btree_get(&version->cells, &table->columns.ptr[i].name);
        if (cell) { groove_Value c; groove_Value_clone(&c, cell); JZ_VEC_PUSH(values, groove_value_some(c)); }
        else JZ_VEC_PUSH(values, groove_value_null());
    }
    JZ_VEC_PUSH(values, jazz_node__node__codec__authored_column_ids_value(version->authored_columns));
    JZ_VEC_PUSH(values, jzn_v_bytes(jz_bytes_clone(&version->counter_signs)));
    JZ_VEC_PUSH(values, groove_value_u64(version->seq));
    JZ_VEC_PUSH(values, jazz_node__node__codec__base_seq_value(version->base));
    JZ_VEC_PUSH(values, jazz_node__node__codec__base_pending_value(version->base));
    *out = values;
    return JZ_OK;
}

groove_Value jazz_node__node__codec__base_seq_value(jazz_protocol_RowBase base) {
    return base.has_seq ? groove_value_some(groove_value_u64(base.seq)) : groove_value_null();
}
groove_Value jazz_node__node__codec__base_pending_value(jazz_protocol_RowBase base) {
    return base.has_pending ? groove_value_some(jazz_node__node__codec__tx_id_value(base.pending)) : groove_value_null();
}
groove_Value jazz_node__node__codec__nullable_deletion_value(u32 deletion) {
    return deletion == JZN_DELETION_NONE ? groove_value_null() : groove_value_some(jazz_node__node__codec__deletion_event_value(deletion));
}
groove_Value jazz_node__node__codec__deletion_event_value(u32 deletion) {
    return jzn_v_enum_tag(deletion == JZN_DELETION_DELETED ? 0 : 1);
}

/* The history key of `version`: accepted writes are keyed by seq, pending
 * ones (seq 0) by their transaction. */
jz_err jazz_node__node__codec__history_primary_key(const jazz_node_VersionRow *version, jazz_node_PrimaryKeyValue *out) {
    jazz_types_GlobalTime seq;
    JZ_TRY(jazz_node__node__codec__VersionRow__seq(version, &seq));
    *out = jazz_node__node__codec__history_primary_key_at(&version->branch_key, jazz_node__node__codec__VersionRow__row_uuid(version), seq,
                                                          jazz_node__node__codec__VersionRow__tx_time(version),
                                                          jazz_node__node__codec__VersionRow__tx_node_alias(version));
    return JZ_OK;
}

jazz_node_PrimaryKeyValue jazz_node__node__codec__history_primary_key_at(const jazz_model_BranchKey *branch_key, jazz_types_RowUuid row_uuid,
    jazz_types_GlobalTime seq, jazz_types_TxTime tx_time, jazz_types_NodeAlias tx_node_alias) {
    jz_bytes b;
    jazz_model__model__branch__BranchKey__canonical_bytes(branch_key, &b);
    groove_PrimaryKeyValue *items[5] = {
        groove__storage__PrimaryKeyValue__bytes(b), groove__storage__PrimaryKeyValue__uuid(row_uuid),
        groove__storage__PrimaryKeyValue__u64(seq), groove__storage__PrimaryKeyValue__u64(tx_time),
        groove__storage__PrimaryKeyValue__u64(tx_node_alias),
    };
    return groove__storage__PrimaryKeyValue__composite(items, 5);
}

/* The seqs a transaction's history record may be stored at, most likely first. */
void jazz_node__node__codec__history_seq_candidates(bool has_global_time, jazz_types_GlobalTime global_time, jzn_U64Vec *out) {
    JZ_VEC(u64) v = {0};
    if (has_global_time && global_time != 0) { JZ_VEC_PUSH(v, global_time); JZ_VEC_PUSH(v, (u64)0); }
    else JZ_VEC_PUSH(v, (u64)0);
    out->ptr = v.ptr; out->len = v.len; out->cap = v.cap;
}

void jazz_node__node__codec__history_key_values(jz_slice branch_key, jazz_types_RowUuid row_uuid, jazz_types_GlobalTime seq,
                                               jazz_types_TxTime tx_time, jazz_types_NodeAlias alias, groove_ValueVec *out) {
    groove_ValueVec v = {0};
    JZ_VEC_PUSH(v, jzn_v_bytes(jz_bytes_from(branch_key.ptr, branch_key.len)));
    JZ_VEC_PUSH(v, jzn_v_uuid(row_uuid));
    JZ_VEC_PUSH(v, groove_value_u64(seq));
    JZ_VEC_PUSH(v, groove_value_u64(tx_time));
    JZ_VEC_PUSH(v, groove_value_u64(alias));
    *out = v;
}

jazz_node_PrimaryKeyValue jazz_node__node__codec__global_current_primary_key(const jazz_model_BranchKey *branch_key, jazz_types_RowUuid row_uuid) {
    jz_bytes b;
    jazz_model__model__branch__BranchKey__canonical_bytes(branch_key, &b);
    groove_PrimaryKeyValue *items[2] = { groove__storage__PrimaryKeyValue__bytes(b), groove__storage__PrimaryKeyValue__uuid(row_uuid) };
    return groove__storage__PrimaryKeyValue__composite(items, 2);
}

/* The encoded branch-key part that leads every global_current_primary_key. */
void jazz_node__node__codec__global_current_branch_key_prefix(const jazz_model_BranchKey *branch_key, jz_bytes *out) {
    jz_bytes b;
    jazz_model__model__branch__BranchKey__canonical_bytes(branch_key, &b);
    groove__storage__PrimaryKeyValue__into_bytes(groove__storage__PrimaryKeyValue__bytes(b), out);
}

#define GLOBAL_CURRENT_KEY_ROW_UUID_LEN (1u + 16u)
/* The branch-key part of an encoded global_current_primary_key. */
jz_slice jazz_node__node__codec__global_current_key_branch_prefix(jz_slice key) {
    return jz_slice_of(key.ptr, key.len > GLOBAL_CURRENT_KEY_ROW_UUID_LEN ? key.len - GLOBAL_CURRENT_KEY_ROW_UUID_LEN : 0);
}

jazz_node_PrimaryKeyValue jazz_node__node__codec__rejected_transaction_primary_key(jazz_types_NodeAlias alias, jazz_types_TxId tx_id) {
    groove_PrimaryKeyValue *items[2] = { groove__storage__PrimaryKeyValue__u64(tx_id.time), groove__storage__PrimaryKeyValue__u64(alias) };
    return groove__storage__PrimaryKeyValue__composite(items, 2);
}

jz_err jazz_node__node__codec__rejected_version_primary_key_from_record(const groove_BorrowedRecord *record, jazz_node_PrimaryKeyValue *out) {
    groove_Value v;
    u64 tx_time, tx_node;
    jz_uuid row;
    JZ_TRY(groove__records__BorrowedRecord__get_idx(record, JZN_REJVER_FIELD_TX_TIME_IDX, &v));
    JZ_TRY(jazz_node__node__codec__expect_u64(v, "tx_time", &tx_time));
    JZ_TRY(groove__records__BorrowedRecord__get_idx(record, JZN_REJVER_FIELD_TX_NODE_ID_IDX, &v));
    JZ_TRY(jazz_node__node__codec__expect_u64(v, "tx_node_id", &tx_node));
    JZ_TRY(groove__records__BorrowedRecord__get_idx(record, JZN_REJVER_FIELD_ROW_UUID_IDX, &v));
    JZ_TRY(jazz_node__node__codec__expect_uuid(v, "row_uuid", &row));
    groove_PrimaryKeyValue *items[3] = {
        groove__storage__PrimaryKeyValue__u64(tx_time), groove__storage__PrimaryKeyValue__u64(tx_node), groove__storage__PrimaryKeyValue__uuid(row),
    };
    *out = groove__storage__PrimaryKeyValue__composite(items, 3);
    return JZ_OK;
}

/* ======================= current rows ======================= */

/* Predicate selecting row images that are not deleted; null `_deletion` is
 * matched explicitly since SQL comparison treats null as unknown. */
groove_PredicateExpr *jazz_node__node__codec__not_deleted_predicate(void) {
    groove_PredicateExpr *items[2] = {
        groove__ivm__op_types__PredicateExpr__is_null(jz_str_from("_deletion")),
        groove__ivm__op_types__PredicateExpr__neq(jz_str_from("_deletion"), groove__ivm__op_types__LiteralValue__from_value(jzn_v_enum_tag(0))),
    };
    return groove__ivm__op_types__PredicateExpr__or(items, 2);
}

typedef struct jzn_visible_ctx { const jazz_model_TableSchema *table; const jzn_StrVec *content_fields; } jzn_visible_ctx;
static groove_GraphBuilder *jzn_visible_layer(void *ctx, u32 ref) {
    const jzn_visible_ctx *c = ctx;
    jz_str table_name = ref == JZN_REF_LOCAL ? jazz_node__node__codec__ahead_current_table_name(&c->table->name)
                                             : jazz_node__node__codec__global_current_table_name(&c->table->name);
    return groove__db__query__GraphBuilder__project(groove__db__query__GraphBuilder__table(table_name), c->content_fields->ptr, c->content_fields->len);
}

groove_GraphBuilder *jazz_node__node__codec__visible_current_graph(const jazz_model_TableSchema *table, u32 settled) {
    jzn_StrVec user_fields = {0};
    for (u32 i = 0; i < table->columns.len; i++) JZ_VEC_PUSH(user_fields, jazz_node__node__query_engine__user_column_field(&table->columns.ptr[i].name));
    jzn_StrVec content_fields = {0};
    JZ_VEC_PUSH(content_fields, jz_str_from("row_uuid"));
    for (u32 i = 0; i < user_fields.len; i++) JZ_VEC_PUSH(content_fields, jz_str_clone(&user_fields.ptr[i]));
    static const char *const tail[7] = { "created_by", "created_at", "updated_by", "updated_at", "tx_time", "tx_node_id", "_deletion" };
    for (u32 i = 0; i < 7; i++) JZ_VEC_PUSH(content_fields, jz_str_from(tail[i]));
    /* The global-current table carries every user cell: select the winner
     * first, then drop deleted images. */
    jzn_visible_ctx ctx = { table, &content_fields };
    jzn_Layers *layers = jazz_node__node__layer_stack__ref_layers(settled, jzn_visible_layer, &ctx);
    groove_GraphBuilder *current = jazz_node__node__layer_stack__compose(layers);
    if (settled != JAZZ_DURABILITY_GLOBAL) current = groove__db__query__GraphBuilder__project(current, content_fields.ptr, content_fields.len);
    groove_ProjectFieldVec fields = {0};
    JZ_VEC_PUSH(fields, groove__ivm__op_types__ProjectField__named(jz_str_from("row_uuid")));
    for (u32 i = 0; i < user_fields.len; i++) {
        JZ_VEC_PUSH(fields, groove__ivm__op_types__ProjectField__named(user_fields.ptr[i]));
        user_fields.ptr[i] = (jz_str){0};
    }
    JZ_VEC_PUSH(fields, groove__ivm__op_types__ProjectField__renamed(jz_str_from("created_by"), jz_str_from("$createdBy")));
    JZ_VEC_PUSH(fields, groove__ivm__op_types__ProjectField__renamed(jz_str_from("created_at"), jz_str_from("$createdAt")));
    JZ_VEC_PUSH(fields, groove__ivm__op_types__ProjectField__renamed(jz_str_from("updated_by"), jz_str_from("$updatedBy")));
    JZ_VEC_PUSH(fields, groove__ivm__op_types__ProjectField__renamed(jz_str_from("updated_at"), jz_str_from("$updatedAt")));
    JZ_VEC_PUSH(fields, groove__ivm__op_types__ProjectField__named(jz_str_from("tx_time")));
    JZ_VEC_PUSH(fields, groove__ivm__op_types__ProjectField__named(jz_str_from("tx_node_id")));
    groove_GraphBuilder *out = groove__db__query__GraphBuilder__project_fields(
        groove__db__query__GraphBuilder__filter(current, jazz_node__node__codec__not_deleted_predicate()), fields);
    jzn_str_vec_drop(&user_fields);
    jzn_str_vec_drop(&content_fields);
    return out;
}

void jazz_node__node__codec__decode_current_row(const jazz_model_TableSchema *table, const groove_BorrowedRecord *record, jazz_model_CurrentRow *out) {
    groove_OwnedRecord r = { jz_bytes_from(record->raw.ptr, record->raw.len), *record->descriptor };
    jazz_model__row__CurrentRow__new(jz_str_clone(&table->name), r, out);
}

static int jzn_current_row_cmp(const void *a, const void *b, void *ctx) {
    const jazz_model_CurrentRow *l = a, *r = b;
    (void)ctx;
    jazz_types_RowUuid lu = jazz_model__row__CurrentRow__row_uuid(l), ru = jazz_model__row__CurrentRow__row_uuid(r);
    int c = memcmp(lu.b, ru.b, 16);
    return c ? c : jz_bytes_cmp(&l->record.raw, &r->record.raw);
}
void jazz_node__node__codec__sort_current_rows(jazz_model_CurrentRow *rows, u32 n) {
    jz_sort(rows, n, sizeof(jazz_model_CurrentRow), jzn_current_row_cmp, NULL);
}

static groove_RecordDescriptor jazz_node__node__codec__current_row_descriptor(const jazz_model_TableSchema *table);

static jz_err jazz_node__node__codec__current_row_prefix_and_cells_from_version(const jazz_model_TableSchema *table, const jazz_node_VersionRow *version,
                                                                               groove_ValueVec *out) {
    groove_ValueVec values = {0};
    jz_vec_reserve(&values, table->columns.len + 7, sizeof(groove_Value));
    JZ_VEC_PUSH(values, jzn_v_uuid(jazz_node__node__codec__VersionRow__row_uuid(version)));
    groove_BorrowedRecord borrowed = groove_OwnedRecord_borrowed(&version->record);
    for (u32 idx = 0; idx < table->columns.len; idx++) {
        groove_Value raw, value;
        bool some;
        jz_err err = groove__records__BorrowedRecord__get_idx(&borrowed, JZN_HISTORY_USER_CELLS + idx, &raw);
        if (!JZ_IS_ERR(err)) err = jazz_node__node__codec__nullable_value(raw, &some, &value);
        if (JZ_IS_ERR(err)) { jzn_values_drop(&values); return err; }
        JZ_VEC_PUSH(values, some ? groove_value_some(value) : groove_value_null());
    }
    *out = values;
    return JZ_OK;
}

static void jazz_node__node__codec__append_current_row_provenance(groove_ValueVec *values, const jazz_node_VersionRow *provenance) {
    jazz_types_RowAuthor a;
    jz_err err = jazz_types__ids__RowAuthor__from_persisted_subject(jazz_node__node__codec__VersionRow__created_by(provenance), &a);
    JZ_ASSERT(!JZ_IS_ERR(err), "stored provenance is a row author");
    JZ_VEC_PUSH(*values, jazz_types__ids__RowAuthor__to_value(a));
    JZ_VEC_PUSH(*values, groove_value_u64(jazz_types__time__TxTime__physical_ms(jazz_node__node__codec__VersionRow__created_at(provenance))));
    err = jazz_types__ids__RowAuthor__from_persisted_subject(jazz_node__node__codec__VersionRow__updated_by(provenance), &a);
    JZ_ASSERT(!JZ_IS_ERR(err), "stored provenance is a row author");
    JZ_VEC_PUSH(*values, jazz_types__ids__RowAuthor__to_value(a));
    JZ_VEC_PUSH(*values, groove_value_u64(jazz_types__time__TxTime__physical_ms(jazz_node__node__codec__VersionRow__updated_at(provenance))));
    JZ_VEC_PUSH(*values, groove_value_u64(jazz_node__node__codec__VersionRow__tx_time(provenance)));
    JZ_VEC_PUSH(*values, groove_value_u64(jazz_node__node__codec__VersionRow__tx_node_alias(provenance)));
}

static jz_err jzn_current_row_finish(const jazz_model_TableSchema *table, groove_RecordDescriptor descriptor, groove_ValueVec values, jazz_model_CurrentRow *out) {
    groove_OwnedRecord record;
    JZ_TRY(jazz_node__node__codec__owned_record_from_storage_values_with_descriptor(descriptor, values, &record));
    jazz_model__row__CurrentRow__new(jz_str_clone(&table->name), record, out);
    return JZ_OK;
}

jz_err jazz_node__node__codec__current_row_from_version_projection(const jazz_model_TableSchema *table, const jazz_node_VersionRow *version,
                                                                  jazz_model_CurrentRow *out) {
    groove_RecordDescriptor descriptor = jazz_node__node__codec__current_row_descriptor(table);
    groove_ValueVec values;
    JZ_TRY(jazz_node__node__codec__current_row_prefix_and_cells_from_version(table, version, &values));
    jazz_node__node__codec__append_current_row_provenance(&values, version);
    return jzn_current_row_finish(table, descriptor, values, out);
}

jz_err jazz_node__node__codec__current_row_from_materialized_cells(const jazz_model_TableSchema *t, const jazz_node_VersionRow *v, const jz_btree *cells,
                                                                  jazz_model_CurrentRow *out) {
    return jazz_node__node__codec__current_row_from_materialized_cells_with_provenance(t, v, v, cells, out);
}

jz_err jazz_node__node__codec__current_row_from_materialized_cells_with_provenance(const jazz_model_TableSchema *t, const jazz_node_VersionRow *content,
    const jazz_node_VersionRow *provenance, const jz_btree *cells, jazz_model_CurrentRow *out) {
    return jazz_node__node__codec__current_row_from_materialized_cells_with_layer_provenance(t, content, provenance, provenance, cells, out);
}

static void jzn_push_cells(groove_ValueVec *values, const jazz_model_TableSchema *table, const jz_btree *cells) {
    for (u32 i = 0; i < table->columns.len; i++) {
        const groove_Value *cell = jz_btree_get(cells, &table->columns.ptr[i].name);
        if (cell) { groove_Value c; groove_Value_clone(&c, cell); JZ_VEC_PUSH(*values, groove_value_some(c)); }
        else JZ_VEC_PUSH(*values, groove_value_null());
    }
}

/* Application cells and creation provenance from the content winner; update
 * provenance from the winner of the layer that most recently changed the row. */
jz_err jazz_node__node__codec__current_row_from_materialized_cells_with_layer_provenance(const jazz_model_TableSchema *table,
    const jazz_node_VersionRow *content, const jazz_node_VersionRow *created, const jazz_node_VersionRow *updated, const jz_btree *cells,
    jazz_model_CurrentRow *out) {
    groove_RecordDescriptor descriptor = jazz_node__node__codec__current_row_descriptor(table);
    groove_Value created_by, updated_by;
    JZ_TRY(jazz_node__node__codec__row_author_value(jazz_node__node__codec__VersionRow__created_by(created), &created_by));
    jz_err err = jazz_node__node__codec__row_author_value(jazz_node__node__codec__VersionRow__updated_by(updated), &updated_by);
    if (JZ_IS_ERR(err)) { groove_Value_drop(&created_by); return err; }
    groove_ValueVec values = {0};
    jz_vec_reserve(&values, table->columns.len + 7, sizeof(groove_Value));
    JZ_VEC_PUSH(values, jzn_v_uuid(jazz_node__node__codec__VersionRow__row_uuid(content)));
    jzn_push_cells(&values, table, cells);
    JZ_VEC_PUSH(values, created_by);
    JZ_VEC_PUSH(values, groove_value_u64(jazz_types__time__TxTime__physical_ms(jazz_node__node__codec__VersionRow__created_at(created))));
    JZ_VEC_PUSH(values, updated_by);
    JZ_VEC_PUSH(values, groove_value_u64(jazz_types__time__TxTime__physical_ms(jazz_node__node__codec__VersionRow__updated_at(updated))));
    JZ_VEC_PUSH(values, groove_value_u64(jazz_node__node__codec__VersionRow__tx_time(updated)));
    JZ_VEC_PUSH(values, groove_value_u64(jazz_node__node__codec__VersionRow__tx_node_alias(updated)));
    return jzn_current_row_finish(table, descriptor, values, out);
}

jz_err jazz_node__node__codec__current_row_from_cells_with_explicit_provenance(const jazz_model_TableSchema *table, jazz_types_RowUuid row_uuid,
    const jz_btree *cells, jazz_model_RowProvenance provenance, bool has_projected, jazz_types_TxTime tx_time, jazz_types_NodeAlias alias,
    jazz_model_CurrentRow *out) {
    groove_RecordDescriptor descriptor = jazz_node__node__codec__current_row_descriptor(table);
    groove_Value created_by, updated_by;
    JZ_TRY(jazz_node__node__codec__row_author_value(provenance.created_by, &created_by));
    jz_err err = jazz_node__node__codec__row_author_value(provenance.updated_by, &updated_by);
    if (JZ_IS_ERR(err)) { groove_Value_drop(&created_by); return err; }
    groove_ValueVec values = {0};
    jz_vec_reserve(&values, table->columns.len + 7, sizeof(groove_Value));
    JZ_VEC_PUSH(values, jzn_v_uuid(row_uuid));
    jzn_push_cells(&values, table, cells);
    JZ_VEC_PUSH(values, created_by);
    JZ_VEC_PUSH(values, groove_value_u64(provenance.created_at));
    JZ_VEC_PUSH(values, updated_by);
    JZ_VEC_PUSH(values, groove_value_u64(provenance.updated_at));
    JZ_VEC_PUSH(values, groove_value_u64(has_projected ? tx_time : 0));
    JZ_VEC_PUSH(values, groove_value_u64(has_projected ? alias : 0));
    return jzn_current_row_finish(table, descriptor, values, out);
}

/* current_row_column_type::json_type */
static groove_ValueType jazz_node__node__codec__current_row_column_type__json_type(const groove_ValueType *logical) {
    if (logical->tag == GROOVE_VT_NULLABLE)
        return groove_vt_box(GROOVE_VT_NULLABLE, jazz_node__node__codec__current_row_column_type__json_type(logical->v.inner));
    return groove__large_values__physical_storage_value_type(GROOVE_LARGE_VALUE_KIND_JSON);
}

/* Runtime carriers preserve logical nullable wrappers while retaining the
 * semantic kind of indirect JSON. */
groove_ValueType jazz_node__node__codec__current_row_column_type(const jazz_model_ColumnSchema *column) {
    if (column->large_value_kind == JAZZ_LARGE_KIND_JSON) return jazz_node__node__codec__current_row_column_type__json_type(&column->column_type);
    groove_ValueType t;
    groove_ValueType_clone(&t, &column->column_type);
    return t;
}

/* CurrentRowDescriptorCacheEntry: process-wide cache keyed by table name and
 * column (name, current-row type). */
typedef struct jzn_currow_cache_entry {
    jz_str table_name;
    JZ_VEC(groove_NamedValueType) columns;
    groove_RecordDescriptor descriptor;
} jzn_currow_cache_entry;
static JZ_VEC(jzn_currow_cache_entry) jzn_currow_cache;

static bool jazz_node__node__codec__CurrentRowDescriptorCacheEntry__matches(const jzn_currow_cache_entry *e, const jazz_model_TableSchema *table) {
    if (!jz_str_eq(&e->table_name, &table->name) || e->columns.len != table->columns.len) return false;
    for (u32 i = 0; i < e->columns.len; i++) {
        const jazz_model_ColumnSchema *column = &table->columns.ptr[i];
        if (!jz_str_eq(&e->columns.ptr[i].name, &column->name)) return false;
        groove_ValueType t = jazz_node__node__codec__current_row_column_type(column);
        bool eq = groove__schema__ColumnType__eq(&e->columns.ptr[i].value_type, &t);
        groove_ValueType_drop(&t);
        if (!eq) return false;
    }
    return true;
}

/* Runtime current rows can carry unresolved scalar references during policy
 * evaluation; these descriptors are not persisted history or wire formats. */
static groove_RecordDescriptor jazz_node__node__codec__build_current_row_descriptor(const jazz_model_TableSchema *table) {
    groove_DescriptorFieldVec fields = {0};
    groove_DescriptorField f;
    memset(&f, 0, sizeof f);
    f.has_name = true; f.name = jz_str_from("row_uuid"); f.identity.tag = GROOVE_FI_NONE; f.value_type = groove_vt_simple(GROOVE_VT_UUID);
    JZ_VEC_PUSH(fields, f);
    for (u32 i = 0; i < table->columns.len; i++) {
        const jazz_model_ColumnSchema *column = &table->columns.ptr[i];
        memset(&f, 0, sizeof f);
        f.has_name = true;
        f.name = jazz_node__node__query_engine__user_column_field(&column->name);
        f.value_type = groove_vt_box(GROOVE_VT_NULLABLE, jazz_node__node__codec__current_row_column_type(column));
        f.identity.tag = GROOVE_FI_NAME;
        f.identity.name = jz_str_clone(&column->name);
        JZ_VEC_PUSH(fields, f);
    }
    static const char *const tail[6] = { "$createdBy", "$createdAt", "$updatedBy", "$updatedAt", "tx_time", "tx_node_id" };
    for (u32 i = 0; i < 6; i++) {
        memset(&f, 0, sizeof f);
        f.has_name = true;
        f.name = jz_str_from(tail[i]);
        f.identity.tag = GROOVE_FI_NONE;
        f.value_type = (i == 0 || i == 2) ? jazz_types__ids__RowAuthor__value_type() : groove_vt_simple(GROOVE_VT_U64);
        JZ_VEC_PUSH(fields, f);
    }
    return groove__records__RecordDescriptor__new_with_fields(fields);
}

static groove_RecordDescriptor jazz_node__node__codec__current_row_descriptor(const jazz_model_TableSchema *table) {
    for (u32 i = 0; i < jzn_currow_cache.len; i++)
        if (jazz_node__node__codec__CurrentRowDescriptorCacheEntry__matches(&jzn_currow_cache.ptr[i], table)) return jzn_currow_cache.ptr[i].descriptor;
    groove_RecordDescriptor descriptor = jazz_node__node__codec__build_current_row_descriptor(table);
    jzn_currow_cache_entry entry;
    memset(&entry, 0, sizeof entry);
    entry.table_name = jz_str_clone(&table->name);
    for (u32 i = 0; i < table->columns.len; i++) {
        groove_NamedValueType c = { jz_str_clone(&table->columns.ptr[i].name), jazz_node__node__codec__current_row_column_type(&table->columns.ptr[i]) };
        JZ_VEC_PUSH(entry.columns, c);
    }
    entry.descriptor = descriptor;
    JZ_VEC_PUSH(jzn_currow_cache, entry);
    return descriptor;
}

jz_err jazz_node__node__codec__current_row_from_positional_cells(const jazz_model_TableSchema *table, jazz_types_RowUuid row_uuid,
                                                                const groove_Value *const *cells, u32 n, jazz_model_CurrentRow *out) {
    groove_NamedValueTypeVec fields = {0};
    groove_NamedValueType f = { jz_str_from("row_uuid"), groove_vt_simple(GROOVE_VT_UUID) };
    JZ_VEC_PUSH(fields, f);
    for (u32 i = 0; i < table->columns.len; i++) {
        groove_ValueType t;
        groove_ValueType_clone(&t, &table->columns.ptr[i].column_type);
        groove_NamedValueType c = { jz_str_clone(&table->columns.ptr[i].name), groove_vt_box(GROOVE_VT_NULLABLE, t) };
        JZ_VEC_PUSH(fields, c);
    }
    groove_RecordDescriptor descriptor = groove__records__RecordDescriptor__new(fields);
    groove_ValueVec values = {0};
    JZ_VEC_PUSH(values, jzn_v_uuid(row_uuid));
    for (u32 idx = 0; idx < table->columns.len; idx++) {
        if (idx < n && cells[idx]) { groove_Value c; groove_Value_clone(&c, cells[idx]); JZ_VEC_PUSH(values, groove_value_some(c)); }
        else JZ_VEC_PUSH(values, groove_value_null());
    }
    groove_OwnedRecord record;
    JZ_TRY(jazz_node__node__codec__owned_record_from_storage_values_with_descriptor(descriptor, values, &record));
    /* This app-facing positional projection uses logical schema names, not
     * private user_{column} storage carriers. */
    jazz_model__row__CurrentRow__new_with_binding_fields(jz_str_clone(&table->name), record, JAZZ_CURROW_BINDING_LOGICAL_FIELD, out);
    return JZ_OK;
}

/* Returns one Nullable-wrapped value per column (None -> Nullable(None)). */
jz_err jazz_node__node__codec__positional_cells_from_map(const jazz_model_TableSchema *table, const jz_btree *cells, groove_ValueVec *out) {
    const jz_str *column; const groove_Value *unused;
    JZ_BTREE_FOR(cells, column, unused) {
        bool known = false;
        for (u32 i = 0; i < table->columns.len && !known; i++) known = jz_str_eq(&table->columns.ptr[i].name, column);
        if (!known) return jz_err_new(JZN_E_INVALID_MERGEABLE_COMMIT, "unknown user cell column");
    }
    groove_ValueVec values = {0};
    for (u32 i = 0; i < table->columns.len; i++) {
        const jazz_model_ColumnSchema *c = &table->columns.ptr[i];
        const groove_Value *cell = jz_btree_get(cells, &c->name);
        if (!cell) { JZ_VEC_PUSH(values, groove_value_null()); continue; }
        groove_Value copy;
        groove_Value_clone(&copy, cell);
        groove_Value value = jazz_model__schema__ColumnSchema__storage_value(c, copy);
        jz_err err = jazz_node__node__codec__validate_cell_value(c, &value);
        if (JZ_IS_ERR(err)) { groove_Value_drop(&value); jzn_values_drop(&values); return err; }
        JZ_VEC_PUSH(values, groove_value_some(value));
    }
    *out = values;
    return JZ_OK;
}

void jazz_node__node__codec__cells_from_positional(const jazz_model_TableSchema *table, const groove_Value *const *cells, u32 n, jz_btree *out) {
    jz_btree_init(out, &jzn_desc_str_value);
    for (u32 idx = 0; idx < table->columns.len; idx++) {
        if (idx >= n || !cells[idx]) continue;
        jz_str key = jz_str_clone(&table->columns.ptr[idx].name);
        groove_Value v;
        groove_Value_clone(&v, cells[idx]);
        groove_Value old;
        if (jz_btree_insert(out, &key, &v, &old)) groove_Value_drop(&old);
    }
}

jz_err jazz_node__node__codec__nullable_value(groove_Value value, bool *some, groove_Value *out) {
    if (value.tag != GROOVE_V_NULLABLE) { groove_Value_drop(&value); return JZN_ISV("nullable value expected"); }
    if (!value.v.nullable) { *some = false; return JZ_OK; }
    *some = true;
    *out = *value.v.nullable;
    jz_free(value.v.nullable);
    return JZ_OK;
}

jz_err jazz_node__node__codec__validate_cell_value(const jazz_model_ColumnSchema *column, const groove_Value *value) {
    static const char *const names[1] = { "cell" };
    groove_ValueType t = jazz_model__schema__storage_column_type(column);
    groove_RecordDescriptor d = jzn_descriptor(names, &t, 1);
    jz_bytes raw;
    JZ_TRY(groove__records__RecordDescriptor__create(d, value, 1, &raw));
    jz_bytes_drop(&raw);
    return JZ_OK;
}

jz_err jazz_node__node__codec__expect_u64(groove_Value value, const char *field, u64 *out) {
    if (value.tag != GROOVE_V_U64) { groove_Value_drop(&value); return JZN_ISV(field); }
    *out = value.v.u;
    return JZ_OK;
}

jz_err jazz_node__node__codec__expect_uuid(groove_Value value, const char *field, jz_uuid *out) {
    if (value.tag != GROOVE_V_UUID) { groove_Value_drop(&value); return JZN_ISV(field); }
    *out = value.v.uuid;
    return JZ_OK;
}

jz_err jazz_node__node__codec__validate_canonical_version_parts(const jazz_model_BranchKey *branch_key) {
    jz_bytes b;
    jz_err err = jazz_model__model__branch__BranchKey__try_canonical_bytes(branch_key, &b);
    if (JZ_IS_ERR(err)) { jz_err_drop(&err); return jz_err_new(JZN_E_INVALID_MERGEABLE_COMMIT, "row version branch key is not canonical"); }
    jz_bytes_drop(&b);
    return JZ_OK;
}

jz_err jazz_node__node__codec__tx_id_from_value(groove_Value value, jazz_types_TxId *out) {
    jz_err err = JZ_OK;
    if (value.tag != GROOVE_V_TUPLE || value.v.vals.len != 2) err = JZN_ISV("tx id must be tuple(u64, uuid)");
    else if (value.v.vals.ptr[0].tag != GROOVE_V_U64) err = JZN_ISV("tx id time must be u64");
    else if (value.v.vals.ptr[1].tag != GROOVE_V_UUID) err = JZN_ISV("tx id node must be uuid");
    else { out->time = value.v.vals.ptr[0].v.u; out->node = value.v.vals.ptr[1].v.uuid; }
    groove_Value_drop(&value);
    return err;
}

jz_err jazz_node__node__codec__tx_kind_from_discriminant(u8 value, u32 *out) {
    if (value > 1) return JZN_ISV("unknown tx kind");
    *out = value == 0 ? JAZZ_TX_KIND_MERGEABLE : JAZZ_TX_KIND_EXCLUSIVE;
    return JZ_OK;
}

jz_err jazz_node__node__codec__fate_from_encoded_fields(const groove_BorrowedRecord *record, jazz_model_Fate *out) {
    u8 tag;
    JZ_TRY(groove__records__BorrowedRecord__get_enum(record, JZN_TXROW_FIELD_FATE_IDX, &tag));
    memset(out, 0, sizeof *out);
    switch (tag) {
    case 0: out->tag = JAZZ_FATE_PENDING; return JZ_OK;
    case 1: out->tag = JAZZ_FATE_ACCEPTED; return JZ_OK;
    case 2: out->tag = JAZZ_FATE_REJECTED; return jazz_node__node__codec__rejection_reason_from_encoded_fields(record, &out->reason);
    default: return JZN_ISV("unknown fate");
    }
}

jz_err jazz_node__node__codec__rejection_reason_from_encoded_fields(const groove_BorrowedRecord *record, jazz_model_RejectionReason *out) {
    groove_Value v;
    JZ_TRY(groove__records__BorrowedRecord__get_idx(record, JZN_TXROW_FIELD_REJECTION_REASON_IDX, &v));
    if (v.tag != GROOVE_V_NULLABLE || !v.v.nullable) { groove_Value_drop(&v); return JZN_ISV("rejected transaction missing reason"); }
    groove_Value reason = *v.v.nullable;
    jz_free(v.v.nullable);
    jz_err err = jazz_model__tx__RejectionReason__from_storage_value(reason, out);
    if (JZ_IS_ERR(err)) { jz_err_drop(&err); return JZN_ISV("invalid rejection reason"); }
    return JZ_OK;
}

jz_err jazz_node__node__codec__durability_from_discriminant(u8 value, u32 *out) {
    jz_err err = jazz_model__tx__DurabilityTier__from_discriminant(value, out);
    if (JZ_IS_ERR(err)) { jz_err_drop(&err); return JZN_ISV("unknown durability"); }
    return JZ_OK;
}

jz_err jazz_node__node__codec__deletion_event_from_value(groove_Value value, u32 *out) {
    if (value.tag == GROOVE_V_NULLABLE && value.v.nullable) {
        groove_Value inner = *value.v.nullable;
        jz_free(value.v.nullable);
        return jazz_node__node__codec__deletion_event_from_value(inner, out);
    }
    jz_err err = JZ_OK;
    if (value.tag == GROOVE_V_ENUM_TAG && value.v.enum_tag == 0) *out = JZN_DELETION_DELETED;
    else if (value.tag == GROOVE_V_ENUM_TAG && value.v.enum_tag == 1) *out = JZN_DELETION_RESTORED;
    else err = JZN_ISV("unknown deletion event");
    groove_Value_drop(&value);
    return err;
}

groove_Value jazz_node__node__codec__tx_id_value(jazz_types_TxId tx_id) {
    groove_ValueVec parts = {0};
    JZ_VEC_PUSH(parts, groove_value_u64(tx_id.time));
    JZ_VEC_PUSH(parts, jzn_v_uuid(tx_id.node));
    groove_Value v;
    v.tag = GROOVE_V_TUPLE;
    v.v.vals = parts;
    return v;
}

jz_str jazz_node__node__codec__global_current_table_name(const jz_str *table) { return jz_format("jazz_%.*s_global_current", JZN_S(*table)); }
jz_str jazz_node__node__codec__ahead_current_table_name(const jz_str *table) { return jz_format("jazz_%.*s_ahead_current", JZN_S(*table)); }

/* ======================================================================
 * Contribution-merge provenance / branch-view evidence storage codecs
 * ====================================================================== */

/* define_record! typed accessor: get_idx + RecordField::from_value type check. */
static jz_err jzn_rec_field(const groove_OwnedRecord *r, u32 idx, u32 vtag, groove_Value *out) {
    groove_Value v;
    JZ_TRY(groove__records__OwnedRecord__get_idx(r, idx, &v));
    if (v.tag != vtag) {
        groove_Value_drop(&v);
        return jz_err_new(JZN_E_RECORD, "record field type mismatch");
    }
    *out = v;
    return JZ_OK;
}
static jz_err jzn_rec_u64(const groove_OwnedRecord *r, u32 idx, u64 *out) {
    groove_Value v; JZ_TRY(jzn_rec_field(r, idx, GROOVE_V_U64, &v)); *out = v.v.u; return JZ_OK;
}
static jz_err jzn_rec_u8(const groove_OwnedRecord *r, u32 idx, u8 *out) {
    groove_Value v; JZ_TRY(jzn_rec_field(r, idx, GROOVE_V_U8, &v)); *out = (u8)v.v.u; return JZ_OK;
}
static jz_err jzn_rec_uuid(const groove_OwnedRecord *r, u32 idx, jz_uuid *out) {
    groove_Value v; JZ_TRY(jzn_rec_field(r, idx, GROOVE_V_UUID, &v)); *out = v.v.uuid; return JZ_OK;
}
static jz_err jzn_rec_bytes(const groove_OwnedRecord *r, u32 idx, jz_bytes *out) {
    groove_Value v; JZ_TRY(jzn_rec_field(r, idx, GROOVE_V_BYTES, &v)); *out = v.v.bytes; return JZ_OK;
}
static jz_err jzn_rec_string(const groove_OwnedRecord *r, u32 idx, jz_str *out) {
    groove_Value v; JZ_TRY(jzn_rec_field(r, idx, GROOVE_V_STRING, &v)); *out = v.v.s; return JZ_OK;
}
static jz_err jzn_rec_array(const groove_OwnedRecord *r, u32 idx, groove_ValueVec *out) {
    groove_Value v; JZ_TRY(jzn_rec_field(r, idx, GROOVE_V_ARRAY, &v)); *out = v.v.vals; return JZ_OK;
}
static jz_err jzn_rec_record(const groove_OwnedRecord *r, u32 idx, groove_OwnedRecord *out) {
    groove_Value v; JZ_TRY(jzn_rec_field(r, idx, GROOVE_V_RECORD, &v)); *out = v.v.record; return JZ_OK;
}
static jz_err jzn_rec_enum(const groove_OwnedRecord *r, u32 idx, groove_EnumValue *out) {
    groove_Value v; JZ_TRY(jzn_rec_field(r, idx, GROOVE_V_ENUM, &v)); *out = v.v.en; return JZ_OK;
}
static void jzn_value_vec_drop(groove_ValueVec *v) { jzn_values_drop(v); }

const groove_ValueType *jazz_node__node__codec__record_field_type(groove_RecordDescriptor descriptor, u32 index) {
    const groove_DescriptorFieldVec *fields = groove__records__RecordDescriptor__fields(descriptor);
    JZ_ASSERT(index < fields->len, "contribution storage descriptor field");
    return &fields->ptr[index].value_type;
}

static groove_RecordDescriptor jzn_nested_record_descriptor(const groove_ValueType *t) {
    if (t->tag != GROOVE_VT_RECORD) jz_panic("contribution storage field is a record");
    return *t->v.record;
}

static const groove_ValueType *jzn_array_element_type(const groove_ValueType *t) {
    if (t->tag != GROOVE_VT_ARRAY) jz_panic("contribution storage field is an array");
    return t->v.inner;
}

static groove_RecordDescriptor jzn_contribution_merge_descriptor(void) {
    static bool init;
    static groove_RecordDescriptor descriptor;
    if (!init) {
        groove_ValueType t = jazz_model__schema__contribution_merge_storage_type();
        if (t.tag != GROOVE_VT_NULLABLE) jz_panic("contribution merge storage column is nullable");
        if (t.v.inner->tag != GROOVE_VT_RECORD) jz_panic("contribution merge storage column contains a record");
        descriptor = *t.v.inner->v.record;
        groove_ValueType_drop(&t);
        init = true;
    }
    return descriptor;
}

static groove_RecordDescriptor jzn_array_record_field(groove_RecordDescriptor d, u32 idx, const char *what) {
    const groove_ValueType *t = jazz_node__node__codec__record_field_type(d, idx);
    if (t->tag != GROOVE_VT_ARRAY) jz_panic(what);
    if (t->v.inner->tag != GROOVE_VT_RECORD) jz_panic(what);
    return *t->v.inner->v.record;
}

static groove_RecordDescriptor jzn_branch_view_copy_descriptor(void) {
    return jzn_array_record_field(jzn_contribution_merge_descriptor(), 3, "branch-view copy is an array of records");
}

static groove_RecordDescriptor jzn_branch_write_intent_descriptor(void) {
    return jzn_array_record_field(jzn_contribution_merge_descriptor(), 4, "branch write intent is an array of records");
}

static const groove_EnumSchema *jzn_branch_write_operation_schema(void) {
    const groove_ValueType *t = jazz_node__node__codec__record_field_type(jzn_branch_write_intent_descriptor(), 5);
    if (t->tag != GROOVE_VT_ENUM) jz_panic("branch write operation is an enum");
    return t->v.enm;
}

static const groove_EnumSchema *jzn_branch_view_copy_base_schema(void) {
    const groove_ValueType *t = jazz_node__node__codec__record_field_type(jzn_branch_view_copy_descriptor(), 2);
    if (t->tag != GROOVE_VT_ENUM) jz_panic("branch-view copy base is an enum");
    return t->v.enm;
}

/* schema.tag(name).expect(..) + schema.case(tag).expect(..).payload */
static groove_RecordDescriptor jzn_declared_case(const groove_EnumSchema *schema, const char *name, const char *what, u32 *tag) {
    if (!groove__records__values__EnumSchema__tag(schema, name, tag)) jz_panic(what);
    const groove_EnumCase *c;
    jz_err err = groove__records__values__EnumSchema__case(schema, *tag, &c);
    if (JZ_IS_ERR(err)) jz_panic(what);
    return c->payload;
}

static jz_err jzn_branch_write_intent_storage_record(const jazz_model_BranchWriteIntent *intent, bool has_index, u32 evidence_index,
                                                     groove_OwnedRecord *out) {
    const groove_EnumSchema *schema = jzn_branch_write_operation_schema();
    const char *name;
    groove_Value payload_values[1];
    u32 n_payload = 0;
    if (intent->operation.tag == JAZZ_BWO_EXACT_HEAD_INSERT && !has_index) name = "exact_head_insert";
    else if (intent->operation.tag == JAZZ_BWO_EXACT_HEAD_UPDATE && !has_index) name = "exact_head_update";
    else if (intent->operation.tag == JAZZ_BWO_VIEW_UPDATE_COPY && has_index) {
        name = "view_update_copy";
        payload_values[0] = jzn_v_u32(evidence_index);
        n_payload = 1;
    } else {
        return JZN_ISV("branch write intent evidence binding is invalid");
    }
    u32 tag;
    groove_RecordDescriptor payload_d = jzn_declared_case(schema, name, "declared branch write operation", &tag);
    groove_OwnedRecord payload;
    JZ_TRY(jzn_encode_record(payload_d, payload_values, n_payload, &payload));
    jz_bytes head;
    jz_err err = jazz_model__model__branch__BranchKey__try_canonical_bytes(&intent->head, &head);
    if (JZ_IS_ERR(err)) {
        jz_err_drop(&err);
        groove_OwnedRecord_drop(&payload);
        return JZN_ISV("branch write intent head is invalid");
    }
    groove_Value values[6] = {
        jzn_v_u8(intent->version),
        groove_value_u64(intent->physical_table_id),
        jzn_v_uuid(intent->authored_schema),
        jzn_v_uuid(intent->row_uuid),
        jzn_v_bytes(head),
        jzn_v_enum(tag, payload),
    };
    return jzn_encode_record(jzn_branch_write_intent_descriptor(), values, 6, out);
}

typedef JZ_VEC(jazz_model_BranchViewCopyEvidence) jzn_CopyVec;

static jz_err jzn_branch_write_intent_from_storage_record(groove_OwnedRecord record, const jzn_CopyVec *copies,
                                                          jazz_model_BranchWriteIntent *out) {
    jz_err err = JZ_OK;
    groove_EnumValue op = {0};
    jz_bytes head = {0};
    memset(out, 0, sizeof *out);
    if (!groove_RecordDescriptor_eq(record.descriptor, jzn_branch_write_intent_descriptor())) {
        groove_OwnedRecord_drop(&record);
        return JZN_ISV("branch write intent must be a v1 record");
    }
    if (JZ_IS_ERR(err = jzn_rec_enum(&record, 5, &op))) goto done;
    const groove_EnumCase *c;
    err = groove__records__values__EnumSchema__case(jzn_branch_write_operation_schema(), op.tag, &c);
    if (JZ_IS_ERR(err)) { jz_err_drop(&err); err = JZN_ISV("branch write operation tag is invalid"); goto done; }
    if (jz_str_eq_cstr(&c->name, "exact_head_insert")) out->operation.tag = JAZZ_BWO_EXACT_HEAD_INSERT;
    else if (jz_str_eq_cstr(&c->name, "exact_head_update")) out->operation.tag = JAZZ_BWO_EXACT_HEAD_UPDATE;
    else if (jz_str_eq_cstr(&c->name, "view_update_copy")) {
        groove_Value index;
        err = groove__records__OwnedRecord__get_idx(&op.record, 0, &index);
        if (JZ_IS_ERR(err)) { jz_err_drop(&err); err = JZN_ISV("branch write copy index is invalid"); goto done; }
        if (index.tag != GROOVE_V_U32) {
            groove_Value_drop(&index);
            err = JZN_ISV("branch write copy index is invalid");
            goto done;
        }
        if (index.v.u >= copies->len) { err = JZN_ISV("branch write copy evidence is missing"); goto done; }
        out->operation.tag = JAZZ_BWO_VIEW_UPDATE_COPY;
        jazz_model_BranchViewCopyEvidence_clone(&out->operation.copy, &copies->ptr[index.v.u]);
    } else {
        err = JZN_ISV("branch write operation tag is invalid");
        goto done;
    }
    if (JZ_IS_ERR(err = jzn_rec_u8(&record, 0, &out->version))) goto fail;
    if (JZ_IS_ERR(err = jzn_rec_u64(&record, 1, &out->physical_table_id))) goto fail;
    if (JZ_IS_ERR(err = jzn_rec_uuid(&record, 2, &out->authored_schema))) goto fail;
    if (JZ_IS_ERR(err = jzn_rec_uuid(&record, 3, &out->row_uuid))) goto fail;
    if (JZ_IS_ERR(err = jzn_rec_bytes(&record, 4, &head))) goto fail;
    err = jazz_model__model__branch__BranchKey__from_canonical_bytes(jz_bytes_slice(&head), &out->head);
    if (JZ_IS_ERR(err)) { jz_err_drop(&err); err = JZN_ISV("branch write intent head is invalid"); goto fail; }
    goto done;
fail:
    if (out->operation.tag == JAZZ_BWO_VIEW_UPDATE_COPY) jazz_model_BranchViewCopyEvidence_drop(&out->operation.copy);
done:
    jz_bytes_drop(&head);
    groove_OwnedRecord_drop(&op.record);
    groove_OwnedRecord_drop(&record);
    return err;
}

static jz_err jzn_branch_view_copy_base_storage_value(const jazz_model_BranchViewCopyBase *base, groove_EnumValue *out) {
    const groove_EnumSchema *schema = jzn_branch_view_copy_base_schema();
    jz_bytes branch;
    jz_err err;
    if (base->tag == JAZZ_BVC_BASE_CURRENT) {
        u32 tag;
        groove_RecordDescriptor payload = jzn_declared_case(schema, "current", "declared current base", &tag);
        err = jazz_model__model__branch__BranchKey__try_canonical_bytes(&base->branch, &branch);
        if (JZ_IS_ERR(err)) { jz_err_drop(&err); return JZN_ISV("branch-view copy base is invalid"); }
        groove_Value values[1] = { jzn_v_bytes(branch) };
        out->tag = tag;
        return jzn_encode_record(payload, values, 1, &out->record);
    }
    u32 tag;
    groove_RecordDescriptor payload = jzn_declared_case(schema, "snapshot", "declared snapshot base", &tag);
    const groove_ValueType *dots_type = jazz_node__node__codec__record_field_type(payload, 4);
    groove_RecordDescriptor dot_descriptor = jzn_nested_record_descriptor(jzn_array_element_type(dots_type));
    groove_ValueVec dots = {0};
    for (u32 i = 0; i < base->snapshot.dots.len; i++) {
        const jazz_model_SnapshotDot *dot = &base->snapshot.dots.ptr[i];
        groove_Value dv[2] = { groove_value_u64(dot->time), jzn_v_uuid(dot->node) };
        groove_OwnedRecord r;
        err = jzn_encode_record(dot_descriptor, dv, 2, &r);
        if (JZ_IS_ERR(err)) { jzn_value_vec_drop(&dots); return err; }
        JZ_VEC_PUSH(dots, jzn_v_record(r));
    }
    err = jazz_model__model__branch__BranchKey__try_canonical_bytes(&base->branch, &branch);
    if (JZ_IS_ERR(err)) { jz_err_drop(&err); jzn_value_vec_drop(&dots); return JZN_ISV("branch-view copy base is invalid"); }
    groove_Value values[5] = {
        jzn_v_bytes(branch),
        jzn_v_uuid(base->snapshot.owner),
        groove_value_u64(base->snapshot.global_base),
        groove_value_u64(base->snapshot.local_base),
        jzn_v_array(dots),
    };
    out->tag = tag;
    return jzn_encode_record(payload, values, 5, &out->record);
}

static jz_err jzn_branch_view_copy_storage_record(const jazz_model_BranchViewCopyEvidence *e, groove_OwnedRecord *out) {
    groove_RecordDescriptor descriptor = jzn_branch_view_copy_descriptor();
    jz_bytes head;
    jz_err err = jazz_model__model__branch__BranchKey__try_canonical_bytes(&e->head, &head);
    if (JZ_IS_ERR(err)) { jz_err_drop(&err); return JZN_ISV("branch-view copy head is invalid"); }
    groove_EnumValue base;
    err = jzn_branch_view_copy_base_storage_value(&e->base, &base);
    if (JZ_IS_ERR(err)) { jz_bytes_drop(&head); return err; }
    groove_Value values[7] = {
        jzn_v_u8(e->version),
        jzn_v_bytes(head),
        jzn_v_enum(base.tag, base.record),
        jzn_v_string(jz_str_clone(&e->table)),
        jzn_v_uuid(e->row_uuid),
        groove_value_u64(e->source_version.time),
        jzn_v_uuid(e->source_version.node),
    };
    return jzn_encode_record(descriptor, values, 7, out);
}

static jz_err jzn_branch_key_from(const groove_OwnedRecord *r, u32 idx, const char *msg, jazz_model_BranchKey *out) {
    jz_bytes b;
    JZ_TRY(jzn_rec_bytes(r, idx, &b));
    jz_err err = jazz_model__model__branch__BranchKey__from_canonical_bytes(jz_bytes_slice(&b), out);
    jz_bytes_drop(&b);
    if (JZ_IS_ERR(err)) { jz_err_drop(&err); return JZN_ISV(msg); }
    return JZ_OK;
}

static jz_err jzn_branch_view_copy_from_storage_record(groove_OwnedRecord record, jazz_model_BranchViewCopyEvidence *out) {
    jz_err err = JZ_OK;
    groove_EnumValue base_value = {0};
    memset(out, 0, sizeof *out);
    if (!groove_RecordDescriptor_eq(record.descriptor, jzn_branch_view_copy_descriptor())) {
        groove_OwnedRecord_drop(&record);
        return JZN_ISV("branch-view copy evidence must be a v1 record");
    }
    const groove_EnumSchema *base_schema = jzn_branch_view_copy_base_schema();
    if (JZ_IS_ERR(err = jzn_rec_enum(&record, 2, &base_value))) goto done;
    const groove_EnumCase *c;
    err = groove__records__values__EnumSchema__case(base_schema, base_value.tag, &c);
    if (JZ_IS_ERR(err)) { jz_err_drop(&err); err = JZN_ISV("branch-view copy base tag is invalid"); goto done; }
    if (jz_str_eq_cstr(&c->name, "current")) {
        out->base.tag = JAZZ_BVC_BASE_CURRENT;
        if (JZ_IS_ERR(err = jzn_branch_key_from(&base_value.record, 0, "branch-view copy current branch is invalid", &out->base.branch)))
            goto done;
    } else if (jz_str_eq_cstr(&c->name, "snapshot")) {
        out->base.tag = JAZZ_BVC_BASE_SNAPSHOT;
        groove_ValueVec dots;
        if (JZ_IS_ERR(err = jzn_rec_array(&base_value.record, 4, &dots))) goto done;
        for (u32 i = 0; i < dots.len; i++) {
            if (dots.ptr[i].tag != GROOVE_V_RECORD) {
                jzn_value_vec_drop(&dots);
                err = JZN_ISV("branch-view copy snapshot dot must be a record");
                goto fail;
            }
            jazz_model_SnapshotDot d;
            if (JZ_IS_ERR(err = jzn_rec_u64(&dots.ptr[i].v.record, 0, &d.time)) ||
                JZ_IS_ERR(err = jzn_rec_uuid(&dots.ptr[i].v.record, 1, &d.node))) {
                jzn_value_vec_drop(&dots);
                goto fail;
            }
            JZ_VEC_PUSH(out->base.snapshot.dots, d);
        }
        jzn_value_vec_drop(&dots);
        if (JZ_IS_ERR(err = jzn_branch_key_from(&base_value.record, 0, "branch-view copy snapshot branch is invalid", &out->base.branch)))
            goto fail;
        if (JZ_IS_ERR(err = jzn_rec_uuid(&base_value.record, 1, &out->base.snapshot.owner)) ||
            JZ_IS_ERR(err = jzn_rec_u64(&base_value.record, 2, &out->base.snapshot.global_base)) ||
            JZ_IS_ERR(err = jzn_rec_u64(&base_value.record, 3, &out->base.snapshot.local_base)))
            goto fail;
    } else {
        err = JZN_ISV("branch-view copy base tag is invalid");
        goto done;
    }
    if (JZ_IS_ERR(err = jzn_rec_u8(&record, 0, &out->version))) goto fail;
    if (JZ_IS_ERR(err = jzn_branch_key_from(&record, 1, "branch-view copy head is invalid", &out->head))) goto fail;
    if (JZ_IS_ERR(err = jzn_rec_string(&record, 3, &out->table))) goto fail;
    if (JZ_IS_ERR(err = jzn_rec_uuid(&record, 4, &out->row_uuid))) goto fail;
    if (JZ_IS_ERR(err = jzn_rec_u64(&record, 5, &out->source_version.time))) goto fail;
    if (JZ_IS_ERR(err = jzn_rec_uuid(&record, 6, &out->source_version.node))) goto fail;
    goto done;
fail:
    jazz_model_BranchViewCopyEvidence_drop(out);
done:
    groove_OwnedRecord_drop(&base_value.record);
    groove_OwnedRecord_drop(&record);
    return err;
}

static jz_err jzn_contribution_component_storage_value(const jazz_model_ContributionCoordinate *coordinate,
                                                       const jazz_model_ContributionComponent *component,
                                                       const groove_ValueType *value_type,
                                                       jzn_contribution_id_resolver *r, groove_EnumValue *out) {
    if (value_type->tag != GROOVE_VT_ENUM) jz_panic("contribution component uses a Groove payload enum");
    const groove_EnumSchema *schema = value_type->v.enm;
    const char *case_name = component->tag == JAZZ_CC_COLUMN ? "column"
                          : component->tag == JAZZ_CC_OPERATION ? "operation" : "register";
    u32 tag;
    groove_RecordDescriptor payload = jzn_declared_case(schema, case_name, "declared contribution component case", &tag);
    out->tag = tag;
    if (component->tag == JAZZ_CC_COLUMN || component->tag == JAZZ_CC_OPERATION) {
        u64 id;
        JZ_TRY(r->column_id(r->ctx, &coordinate->table, &component->column, &id));
        if (id == 0) return JZN_ISV("contribution physical column id must be nonzero");
        if (component->tag == JAZZ_CC_COLUMN) {
            groove_Value values[1] = { groove_value_u64(id) };
            return jzn_encode_record(payload, values, 1, &out->record);
        }
        groove_Value values[2] = { groove_value_u64(id), jzn_v_bytes(jz_bytes_from(component->identity.ptr, component->identity.len)) };
        return jzn_encode_record(payload, values, 2, &out->record);
    }
    return jzn_encode_record(payload, NULL, 0, &out->record);
}

static jz_err jzn_contribution_coordinate_storage_record(const jazz_model_ContributionCoordinate *coordinate,
                                                         groove_RecordDescriptor descriptor,
                                                         jzn_contribution_id_resolver *r, groove_OwnedRecord *out) {
    u64 table_id;
    JZ_TRY(r->table_id(r->ctx, &coordinate->table, &table_id));
    if (table_id == 0) return JZN_ISV("contribution physical table id must be nonzero");
    groove_EnumValue component;
    JZ_TRY(jzn_contribution_component_storage_value(coordinate, &coordinate->component,
                                                    jazz_node__node__codec__record_field_type(descriptor, 4), r, &component));
    jz_bytes branch;
    jazz_model__model__branch__BranchKey__canonical_bytes(&coordinate->branch_key, &branch);
    groove_Value values[5] = {
        jzn_v_bytes(branch),
        groove_value_u64(table_id),
        jzn_v_uuid(coordinate->row_uuid),
        jzn_v_enum_tag((u8)coordinate->layer),
        jzn_v_enum(component.tag, component.record),
    };
    return jzn_encode_record(descriptor, values, 5, out);
}

static jz_err jzn_contribution_dot_storage_record(const jazz_model_ContributionDot *dot, groove_RecordDescriptor descriptor,
                                                  jzn_contribution_id_resolver *r, groove_OwnedRecord *out) {
    groove_RecordDescriptor coordinate = jzn_nested_record_descriptor(jazz_node__node__codec__record_field_type(descriptor, 2));
    groove_OwnedRecord c;
    JZ_TRY(jzn_contribution_coordinate_storage_record(&dot->coordinate, coordinate, r, &c));
    groove_Value values[3] = { groove_value_u64(dot->tx_id.time), jzn_v_uuid(dot->tx_id.node), jzn_v_record(c) };
    return jzn_encode_record(descriptor, values, 3, out);
}

static jz_err jzn_contribution_substitution_storage_record(const jazz_model_ContributionSubstitution *s,
                                                           groove_RecordDescriptor descriptor,
                                                           jzn_contribution_id_resolver *r, groove_OwnedRecord *out) {
    groove_RecordDescriptor target = jzn_nested_record_descriptor(jazz_node__node__codec__record_field_type(descriptor, 0));
    groove_RecordDescriptor source = jzn_nested_record_descriptor(
        jzn_array_element_type(jazz_node__node__codec__record_field_type(descriptor, 1)));
    groove_OwnedRecord t;
    JZ_TRY(jzn_contribution_coordinate_storage_record(&s->target, target, r, &t));
    groove_ValueVec sources = {0};
    for (u32 i = 0; i < s->sources.len; i++) {
        groove_OwnedRecord d;
        jz_err err = jzn_contribution_dot_storage_record(&s->sources.ptr[i], source, r, &d);
        if (JZ_IS_ERR(err)) { jzn_value_vec_drop(&sources); groove_OwnedRecord_drop(&t); return err; }
        JZ_VEC_PUSH(sources, jzn_v_record(d));
    }
    groove_Value values[2] = { jzn_v_record(t), jzn_v_array(sources) };
    return jzn_encode_record(descriptor, values, 2, out);
}

jz_err jazz_node__node__codec__contribution_merge_storage_value(const jazz_model_ContributionMergeProvenance *p,
                                                                jzn_contribution_id_resolver *r, groove_Value *out) {
    groove_RecordDescriptor descriptor = jzn_contribution_merge_descriptor();
    if (!p) { *out = groove_value_null(); return JZ_OK; }
    jz_err err = JZ_OK;
    groove_RecordDescriptor substitution = jzn_nested_record_descriptor(
        jzn_array_element_type(jazz_node__node__codec__record_field_type(descriptor, 2)));
    groove_ValueVec subs = {0}, copies = {0}, intents = {0};
    for (u32 i = 0; i < p->substitutions.len; i++) {
        groove_OwnedRecord rec;
        if (JZ_IS_ERR(err = jzn_contribution_substitution_storage_record(&p->substitutions.ptr[i], substitution, r, &rec))) goto fail;
        JZ_VEC_PUSH(subs, jzn_v_record(rec));
    }
    for (u32 i = 0; i < p->branch_view_copies.len; i++) {
        groove_OwnedRecord rec;
        if (JZ_IS_ERR(err = jzn_branch_view_copy_storage_record(&p->branch_view_copies.ptr[i], &rec))) goto fail;
        JZ_VEC_PUSH(copies, jzn_v_record(rec));
    }
    for (u32 i = 0; i < p->branch_write_intents.len; i++) {
        const jazz_model_BranchWriteIntent *intent = &p->branch_write_intents.ptr[i];
        bool has_index = false;
        u32 index = 0;
        if (intent->operation.tag == JAZZ_BWO_VIEW_UPDATE_COPY) {
            for (u32 j = 0; j < p->branch_view_copies.len; j++) {
                if (jazz_model_BranchViewCopyEvidence_eq(&p->branch_view_copies.ptr[j], &intent->operation.copy)) {
                    has_index = true; index = j; break;
                }
            }
        }
        groove_OwnedRecord rec;
        if (JZ_IS_ERR(err = jzn_branch_write_intent_storage_record(intent, has_index, index, &rec))) goto fail;
        JZ_VEC_PUSH(intents, jzn_v_record(rec));
    }
    jz_bytes source, target;
    jazz_model__model__branch__BranchKey__canonical_bytes(&p->source, &source);
    jazz_model__model__branch__BranchKey__canonical_bytes(&p->target, &target);
    groove_Value values[5] = { jzn_v_bytes(source), jzn_v_bytes(target), jzn_v_array(subs), jzn_v_array(copies), jzn_v_array(intents) };
    groove_OwnedRecord record;
    JZ_TRY(jzn_encode_record(descriptor, values, 5, &record));
    /* RecordField::to_value(&Some(record)) */
    *out = groove_value_some(jzn_v_record(record));
    return JZ_OK;
fail:
    jzn_value_vec_drop(&subs);
    jzn_value_vec_drop(&copies);
    jzn_value_vec_drop(&intents);
    return err;
}

jz_err jazz_node__node__codec__contribution_component_from_storage(groove_EnumValue value, const groove_EnumSchema *schema,
                                                                   const jz_str *table, jzn_contribution_name_resolver *r,
                                                                   jazz_model_ContributionComponent *out) {
    const groove_EnumCase *c;
    jz_err err = groove__records__values__EnumSchema__case(schema, value.tag, &c);
    memset(out, 0, sizeof *out);
    if (JZ_IS_ERR(err)) {
        jz_err_drop(&err);
        groove_OwnedRecord_drop(&value.record);
        return JZN_ISV("transaction contribution component tag is invalid");
    }
    groove_OwnedRecord payload = value.record;
    if (jz_str_eq_cstr(&c->name, "column") || jz_str_eq_cstr(&c->name, "operation")) {
        bool is_column = jz_str_eq_cstr(&c->name, "column");
        u64 id;
        if (JZ_IS_ERR(err = jzn_rec_u64(&payload, 0, &id))) goto done;
        if (id == 0) { err = JZN_ISV("stored contribution physical column id must be nonzero"); goto done; }
        if (JZ_IS_ERR(err = r->column_name(r->ctx, table, id, &out->column))) goto done;
        if (is_column) { out->tag = JAZZ_CC_COLUMN; goto done; }
        out->tag = JAZZ_CC_OPERATION;
        if (JZ_IS_ERR(err = jzn_rec_bytes(&payload, 1, &out->identity))) { jz_str_drop(&out->column); goto done; }
        goto done;
    }
    if (jz_str_eq_cstr(&c->name, "register") && groove__records__RecordDescriptor__fields(payload.descriptor)->len == 0) {
        groove_ValueVec vals;
        if (JZ_IS_ERR(err = groove__records__OwnedRecord__to_values(&payload, &vals))) goto done;
        jzn_value_vec_drop(&vals);
        out->tag = JAZZ_CC_REGISTER;
        goto done;
    }
    err = JZN_ISV("transaction contribution component payload is invalid");
done:
    groove_OwnedRecord_drop(&payload);
    return err;
}

static jz_err jzn_contribution_coordinate_from_storage(groove_OwnedRecord record, jzn_contribution_name_resolver *r,
                                                       jazz_model_ContributionCoordinate *out) {
    jz_err err = JZ_OK;
    memset(out, 0, sizeof *out);
    const groove_ValueType *ct = jazz_node__node__codec__record_field_type(record.descriptor, 4);
    if (ct->tag != GROOVE_VT_ENUM) {
        groove_OwnedRecord_drop(&record);
        return JZN_ISV("transaction contribution component descriptor is invalid");
    }
    const groove_EnumSchema *component_schema = ct->v.enm; /* descriptor is interned: no clone needed */
    u64 table_id;
    if (JZ_IS_ERR(err = jzn_rec_u64(&record, 1, &table_id))) goto done;
    if (table_id == 0) { err = JZN_ISV("stored contribution physical table id must be nonzero"); goto done; }
    if (JZ_IS_ERR(err = r->table_name(r->ctx, table_id, &out->table))) goto done;
    if (JZ_IS_ERR(err = jzn_branch_key_from(&record, 0, "transaction contribution branch key is invalid", &out->branch_key))) goto fail;
    if (JZ_IS_ERR(err = jzn_rec_uuid(&record, 2, &out->row_uuid))) goto fail;
    {
        groove_Value layer;
        if (JZ_IS_ERR(err = jzn_rec_field(&record, 3, GROOVE_V_ENUM_TAG, &layer))) goto fail;
        out->layer = layer.v.enum_tag;
    }
    groove_EnumValue component;
    if (JZ_IS_ERR(err = jzn_rec_enum(&record, 4, &component))) goto fail;
    if (JZ_IS_ERR(err = jazz_node__node__codec__contribution_component_from_storage(component, component_schema, &out->table, r,
                                                                                    &out->component)))
        goto fail;
    goto done;
fail:
    jz_str_drop(&out->table);
    jazz_model_BranchKey_drop(&out->branch_key);
done:
    groove_OwnedRecord_drop(&record);
    return err;
}

static jz_err jzn_contribution_dot_from_storage(groove_OwnedRecord record, jzn_contribution_name_resolver *r, jazz_model_ContributionDot *out) {
    jz_err err;
    groove_OwnedRecord coordinate;
    if (JZ_IS_ERR(err = jzn_rec_u64(&record, 0, &out->tx_id.time)) ||
        JZ_IS_ERR(err = jzn_rec_uuid(&record, 1, &out->tx_id.node)) ||
        JZ_IS_ERR(err = jzn_rec_record(&record, 2, &coordinate))) {
        groove_OwnedRecord_drop(&record);
        return err;
    }
    groove_OwnedRecord_drop(&record);
    return jzn_contribution_coordinate_from_storage(coordinate, r, &out->coordinate);
}

static jz_err jzn_contribution_substitution_from_storage(groove_OwnedRecord record, jzn_contribution_name_resolver *r,
                                                         jazz_model_ContributionSubstitution *out) {
    jz_err err;
    groove_OwnedRecord target;
    groove_ValueVec sources = {0};
    memset(out, 0, sizeof *out);
    if (JZ_IS_ERR(err = jzn_rec_record(&record, 0, &target))) { groove_OwnedRecord_drop(&record); return err; }
    if (JZ_IS_ERR(err = jzn_contribution_coordinate_from_storage(target, r, &out->target))) { groove_OwnedRecord_drop(&record); return err; }
    if (JZ_IS_ERR(err = jzn_rec_array(&record, 1, &sources))) goto fail;
    for (u32 i = 0; i < sources.len; i++) {
        if (sources.ptr[i].tag != GROOVE_V_RECORD) { err = JZN_ISV("transaction contribution dot must be a record"); goto fail; }
        jazz_model_ContributionDot d;
        groove_OwnedRecord rec = sources.ptr[i].v.record;
        sources.ptr[i] = groove_value_null();
        if (JZ_IS_ERR(err = jzn_contribution_dot_from_storage(rec, r, &d))) goto fail;
        JZ_VEC_PUSH(out->sources, d);
    }
    jzn_value_vec_drop(&sources);
    groove_OwnedRecord_drop(&record);
    return JZ_OK;
fail:
    jzn_value_vec_drop(&sources);
    jazz_model_ContributionSubstitution_drop(out);
    groove_OwnedRecord_drop(&record);
    return err;
}

jz_err jazz_node__node__codec__contribution_merge_from_storage_record(groove_OwnedRecord record, jzn_contribution_name_resolver *r,
                                                                      jazz_model_ContributionMergeProvenance *out) {
    jz_err err = JZ_OK;
    groove_ValueVec arr = {0};
    memset(out, 0, sizeof *out);
    if (!groove_RecordDescriptor_eq(record.descriptor, jzn_contribution_merge_descriptor())) {
        groove_OwnedRecord_drop(&record);
        return JZN_ISV("transaction contribution provenance must be a record");
    }
    if (JZ_IS_ERR(err = jzn_branch_key_from(&record, 0, "transaction contribution source branch key is invalid", &out->source))) goto fail;
    if (JZ_IS_ERR(err = jzn_branch_key_from(&record, 1, "transaction contribution target branch key is invalid", &out->target))) goto fail;

    if (JZ_IS_ERR(err = jzn_rec_array(&record, 2, &arr))) goto fail;
    for (u32 i = 0; i < arr.len; i++) {
        if (arr.ptr[i].tag != GROOVE_V_RECORD) { err = JZN_ISV("transaction contribution substitution must be a record"); goto fail; }
        groove_OwnedRecord rec = arr.ptr[i].v.record;
        arr.ptr[i] = groove_value_null();
        jazz_model_ContributionSubstitution s;
        if (JZ_IS_ERR(err = jzn_contribution_substitution_from_storage(rec, r, &s))) goto fail;
        JZ_VEC_PUSH(out->substitutions, s);
    }
    jzn_value_vec_drop(&arr);

    if (JZ_IS_ERR(err = jzn_rec_array(&record, 3, &arr))) goto fail;
    for (u32 i = 0; i < arr.len; i++) {
        if (arr.ptr[i].tag != GROOVE_V_RECORD) { err = JZN_ISV("branch-view copy evidence must be a record"); goto fail; }
        groove_OwnedRecord rec = arr.ptr[i].v.record;
        arr.ptr[i] = groove_value_null();
        jazz_model_BranchViewCopyEvidence e;
        if (JZ_IS_ERR(err = jzn_branch_view_copy_from_storage_record(rec, &e))) goto fail;
        JZ_VEC_PUSH(out->branch_view_copies, e);
    }
    jzn_value_vec_drop(&arr);

    if (JZ_IS_ERR(err = jzn_rec_array(&record, 4, &arr))) goto fail;
    for (u32 i = 0; i < arr.len; i++) {
        if (arr.ptr[i].tag != GROOVE_V_RECORD) { err = JZN_ISV("branch write intent must be a record"); goto fail; }
        groove_OwnedRecord rec = arr.ptr[i].v.record;
        arr.ptr[i] = groove_value_null();
        jazz_model_BranchWriteIntent intent;
        if (JZ_IS_ERR(err = jzn_branch_write_intent_from_storage_record(rec, (const jzn_CopyVec *)&out->branch_view_copies, &intent)))
            goto fail;
        JZ_VEC_PUSH(out->branch_write_intents, intent);
    }
    jzn_value_vec_drop(&arr);
    groove_OwnedRecord_drop(&record);

    err = jazz_model__tx__ContributionMergeProvenance__validate(out);
    if (JZ_IS_ERR(err)) {
        jz_err_drop(&err);
        jazz_model_ContributionMergeProvenance_drop(out);
        return JZN_ISV("transaction contribution provenance must be canonical");
    }
    return JZ_OK;
fail:
    jzn_value_vec_drop(&arr);
    jazz_model_ContributionMergeProvenance_drop(out);
    groove_OwnedRecord_drop(&record);
    return err;
}

/* #[derive(Clone)] StoredTransaction */
void jazz_node__node__codec__StoredTransaction__clone(jazz_node_StoredTransaction *dst, const jazz_node_StoredTransaction *src) {
    *dst = *src;
    jazz_model_Transaction_clone(&dst->tx, &src->tx);
    jazz_model_Fate_clone(&dst->fate, &src->fate);
}
void jazz_node__node__codec__StoredTransaction__drop(jazz_node_StoredTransaction *t) {
    jazz_model_Transaction_drop(&t->tx);
    jazz_model_Fate_drop(&t->fate);
}
