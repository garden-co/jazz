/* db/mod.c: port of crates/groove/src/db/mod.rs.
 * Schema-aware database facade over records, storage and the IVM runtime:
 * error type, the large-value metadata plane (keys, canonical metadata
 * records, node reference transitions), chunk-install observer/journal and
 * the resident publication lifecycle (AppliedBatch / PersistedBatch). */
#include "mod.h"
#include <stdarg.h>
#include <stdio.h>

const char groove__db__LARGE_VALUE_METADATA_CF[] = GROOVE_DB_LARGE_VALUE_METADATA_CF;

jz_str groove__db__metadata_cf(void) { return jz_str_from(GROOVE_DB_LARGE_VALUE_METADATA_CF); }
groove_OwnedWriteOperation groove__db__metadata_set(jz_bytes key, jz_bytes value) {
    groove_OwnedWriteOperation op = { GROOVE_WRITE_OP_SET, groove__db__metadata_cf(), key, value };
    return op;
}
groove_OwnedWriteOperation groove__db__metadata_delete(jz_bytes key) {
    groove_OwnedWriteOperation op = { GROOVE_WRITE_OP_DELETE, groove__db__metadata_cf(), key, {0} };
    return op;
}
void groove__db__WriteOpVec__drop(groove_WriteOpVec *v) { jz_vec_drop(v, sizeof(groove_OwnedWriteOperation), groove__storage__OwnedWriteOperation__drop); }

/* ======================= db::Error ======================= */
static const char *error_unit_message(u32 code) {
    switch (code) {
    case GROOVE_DB_ImmutableBatchConflict: return "immutable record conflict invalidated the batch";
    case GROOVE_DB_StaleImmutableBatch: return "immutable batch must be rebuilt against the current database state";
    case GROOVE_DB_DatabasePoisoned: return "database instance is poisoned after failure or storage extraction";
    case GROOVE_DB_UnsettledPublications: return "database storage extraction requires all publications to be settled";
    case GROOVE_DB_SubscriptionEnded: return "subscription ended";
    default: return "database error";
    }
}
jz_err groove__db__Error__unit(u32 code) { return jz_err_new(code, error_unit_message(code)); }
jz_err groove__db__Error__publication_not_found(u64 publication) {
    return jz_err_fmt(GROOVE_DB_PublicationNotFound, "publication does not belong to this database: PublicationId(%llu)", (unsigned long long)publication);
}
static void debug_byte_vec(jz_str *out, const u8 *key, u32 kn) {
    jz_str_push_char(out, '[');
    for (u32 i = 0; i < kn; i++) jz_str_appendf(out, i ? ", %u" : "%u", key[i]);
    jz_str_push_char(out, ']');
}
jz_err groove__db__Error__duplicate_primary_key(const char *table, u32 tn, const u8 *key, u32 kn) {
    jz_str k = {0};
    debug_byte_vec(&k, key, kn);
    jz_err e = jz_err_fmt(GROOVE_DB_DuplicatePrimaryKey, "duplicate primary key for table %.*s: %.*s", (int)tn, table, GROOVE_DB_S(k));
    jz_str_drop(&k);
    return e;
}
static const char *error_str_prefix(u32 code) {
    switch (code) {
    case GROOVE_DB_DuplicateParameter: return "duplicate query parameter binding: ";
    case GROOVE_DB_InvalidPersistedIndex: return "invalid persisted index contents: ";
    case GROOVE_DB_InvalidLargeValueMetadata: return "invalid persisted large-value metadata: ";
    case GROOVE_DB_MissingParameter: return "missing query parameter binding: ";
    case GROOVE_DB_MissingPrimaryKey: return "table has no primary key: ";
    case GROOVE_DB_DirectRecordStoreNotFound: return "direct record store not found: ";
    case GROOVE_DB_InvalidDirectRecordStoreKey: return "invalid direct record store key: ";
    case GROOVE_DB_DuplicateApplicationStorageName: return "application table/direct-record-store storage name is duplicated: ";
    case GROOVE_DB_TableNotFound: return "table not found: ";
    case GROOVE_DB_TableAlreadyExists: return "table already exists: ";
    case GROOVE_DB_UnsupportedSchemaVariantTableFeature: return "schema-variant table uses foreign keys, which are not supported yet: ";
    case GROOVE_DB_ReservedTableVariant: return "schema version 0 is reserved for Groove's implicit table layout: ";
    case GROOVE_DB_CannotPromoteLiveTableToSchemaVariants: return "cannot add the first explicit schema version to a live homogeneous table: ";
    case GROOVE_DB_UnknownParameter: return "unknown query parameter binding: ";
    default: return "";
    }
}
jz_err groove__db__Error__str(u32 code, const char *p, u32 n) { return jz_err_fmt(code, "%s%.*s", error_str_prefix(code), (int)n, p); }
static jz_err error_vstr(u32 code, const char *fmt, va_list ap) {
    char buf[512];
    int n = vsnprintf(buf, sizeof buf, fmt, ap);
    if (n < 0) n = 0;
    if ((u32)n >= sizeof buf) n = sizeof buf - 1;
    return groove__db__Error__str(code, buf, (u32)n);
}
jz_err groove__db__Error__invalid_large_value_metadata(const char *fmt, ...) {
    va_list ap; va_start(ap, fmt);
    jz_err e = error_vstr(GROOVE_DB_InvalidLargeValueMetadata, fmt, ap);
    va_end(ap);
    return e;
}
jz_err groove__db__Error__invalid_persisted_index(const char *fmt, ...) {
    va_list ap; va_start(ap, fmt);
    jz_err e = error_vstr(GROOVE_DB_InvalidPersistedIndex, fmt, ap);
    va_end(ap);
    return e;
}
jz_err groove__db__Error__table_version(u32 code, const char *table, u32 tn, u64 version) {
    unsigned long long v = (unsigned long long)version;
    switch (code) {
    case GROOVE_DB_DuplicateTableVariant: return jz_err_fmt(code, "duplicate schema version %llu for table %.*s", v, (int)tn, table);
    case GROOVE_DB_TableVariantTagOutOfRange: return jz_err_fmt(code, "table %.*s variant tag %llu exceeds the bounded u32 tag space", (int)tn, table, v);
    case GROOVE_DB_SchemaVersionDescriptorMismatch: return jz_err_fmt(code, "record descriptor does not match schema version %llu for table %.*s", v, (int)tn, table);
    default: return jz_err_fmt(GROOVE_DB_UnknownTableVariant, "unknown schema version %llu for table %.*s", v, (int)tn, table);
    }
}
jz_err groove__db__Error__pending_upload_limit(u32 limit) {
    return jz_err_fmt(GROOVE_DB_PendingLargeValueUploadLimitExceeded, "pending large-value upload limit reached: %u", limit);
}
jz_err groove__db__Error__arity(u32 code, const char *name, u32 n, u32 expected, u32 actual) {
    if (code == GROOVE_DB_IndexKeyArity)
        return jz_err_fmt(code, "index key arity mismatch for %.*s: expected at most %u, got %u", (int)n, name, expected, actual);
    return jz_err_fmt(GROOVE_DB_PrimaryKeyArity, "primary key arity mismatch for %.*s: expected at most %u, got %u", (int)n, name, expected, actual);
}
jz_err groove__db__Error__pair(u32 code, const char *a, u32 na, const char *b, u32 nb) {
    switch (code) {
    case GROOVE_DB_IndexNotFound: return jz_err_fmt(code, "index not found: %.*s.%.*s", (int)na, a, (int)nb, b);
    case GROOVE_DB_PrimaryKeyTypeMismatch: return jz_err_fmt(code, "primary key type mismatch for %.*s.%.*s", (int)na, a, (int)nb, b);
    case GROOVE_DB_InvalidApplicationStorageName: return jz_err_fmt(code, "invalid application storage name \"%.*s\": %.*s", (int)na, a, (int)nb, b);
    case GROOVE_DB_TableFieldDefinitionMismatch: return jz_err_fmt(code, "field definition does not match the live catalogue: %.*s.%.*s", (int)na, a, (int)nb, b);
    case GROOVE_DB_TableIndexDefinitionMismatch: return jz_err_fmt(code, "index definition does not match the live catalogue: %.*s.%.*s", (int)na, a, (int)nb, b);
    default:
        return jz_err_fmt(GROOVE_DB_TableIndexRegistrationWhilePublicationsResident,
                          "cannot register index %.*s.%.*s while database publications remain resident", (int)na, a, (int)nb, b);
    }
}
jz_err groove__db__Error__table_index_field_not_found(const char *table, u32 tn, const char *index, u32 in, const char *field, u32 fn) {
    return jz_err_fmt(GROOVE_DB_TableIndexFieldNotFound, "index %.*s.%.*s references unknown field %.*s", (int)tn, table, (int)in, index, (int)fn, field);
}
jz_err groove__db__Error__table_version_field(u32 code, const char *table, u32 tn, u64 version, const char *field, u32 fn) {
    unsigned long long v = (unsigned long long)version;
    if (code == GROOVE_DB_InvalidTableVariantField)
        return jz_err_fmt(code, "invalid field %.*s in schema version %llu for table %.*s", (int)fn, field, v, (int)tn, table);
    return jz_err_fmt(GROOVE_DB_SchemaVersionMissingPrimaryKey, "schema version %llu for table %.*s omits primary-key column %.*s", v, (int)tn, table, (int)fn, field);
}
void groove__db__Error__Debug__fmt(jz_str *out, const jz_err *error) {
    jz_str_appendf(out, "Error(%#x, \"%.*s\")", error->code, GROOVE_DB_E(*error));
}

/* inner.map_err(|error| Error::InvalidLargeValueMetadata(format!("{ctx}: {error}"))) */
static jz_err lv_meta_wrap(const char *ctx, jz_err inner) {
    jz_err e = groove__db__Error__invalid_large_value_metadata("%s: %.*s", ctx, GROOVE_DB_E(inner));
    jz_err_drop(&inner);
    return e;
}
/* map_err(|error| ChunkError::Backend(error.to_string())) */
static jz_err chunk_backend(jz_err inner) {
    jz_err e = groove__chunks__ChunkError__backend(inner.msg ? inner.msg->ptr : "", inner.msg ? inner.msg->len : 0);
    jz_err_drop(&inner);
    return e;
}

/* ======================= metadata keys ======================= */
static jz_bytes prefixed(const char *prefix, const u8 *p, u32 n) {
    jz_bytes key = jz_bytes_from((const u8 *)prefix, (u32)strlen(prefix));
    jz_bytes_push(&key, p, n);
    return key;
}
jz_bytes groove__db__staged_large_value_key(groove_StagedLargeValueId id) { return prefixed("staged/", id.b, 16); }
jz_bytes groove__db__pending_large_value_upload_key(groove_StagedLargeValueId id) { return prefixed("upload/", id.b, 16); }
jz_bytes groove__db__completed_large_value_upload_key(groove_StagedLargeValueId id) { return prefixed("completed-upload/", id.b, 16); }
jz_bytes groove__db__completed_large_value_receipt_key(groove_StagedLargeValueId id) { return prefixed("completed-receipt/", id.b, 16); }

/* Bind an engine-owned lifecycle record to the fixed-width opaque identity in
 * its metadata key. */
static jz_err staged_large_value_id_from_metadata_key(const u8 *key, u32 n, const char *prefix, const char *record_name, groove_StagedLargeValueId *out) {
    u32 pn = (u32)strlen(prefix);
    if (n < pn || memcmp(key, prefix, pn) != 0)
        return groove__db__Error__invalid_large_value_metadata("%s has an invalid metadata key prefix", record_name);
    if (n - pn != 16)
        return groove__db__Error__invalid_large_value_metadata("%s metadata key must contain exactly 16 staging-id bytes", record_name);
    memcpy(out->b, key + pn, 16);
    return JZ_OK;
}

static jz_err node_ref_key(const char *prefix, const groove_NodeRef *node_ref, const char *ctx, jz_bytes *out) {
    jz_bytes encoded;
    jz_err e = groove__large_values__encode_node_ref(node_ref, &encoded);
    if (e.code) return lv_meta_wrap(ctx, e);
    *out = prefixed(prefix, encoded.ptr, encoded.len);
    jz_bytes_drop(&encoded);
    return JZ_OK;
}
jz_err groove__db__large_value_root_key(const groove_NodeRef *node_ref, jz_bytes *out) { return node_ref_key("root/", node_ref, "cannot encode root identity", out); }
jz_err groove__db__large_value_node_key(const groove_NodeRef *node_ref, jz_bytes *out) { return node_ref_key("node/", node_ref, "cannot encode node identity", out); }
jz_err groove__db__large_value_reclaim_key(const groove_NodeRef *node_ref, jz_bytes *out) { return node_ref_key("reclaim/", node_ref, "cannot encode reclaim identity", out); }
/* Durable marker for a remotely hydrated immutable node whose byte write has
 * happened but whose Groove reference metadata still needs installation. */
jz_err groove__db__large_value_pending_install_key(const groove_NodeRef *node_ref, jz_bytes *out) {
    return node_ref_key("install/", node_ref, "cannot encode pending install identity", out);
}
/* Decode the identity embedded in a reclaim-work key. */
jz_err groove__db__large_value_reclaim_node_ref_from_key(const u8 *key, u32 n, groove_NodeRef *out) {
    if (n < 8 || memcmp(key, "reclaim/", 8) != 0) return groove__db__Error__invalid_large_value_metadata("reclaim entry has an invalid key prefix");
    jz_err e = groove__large_values__decode_node_ref(key + 8, n - 8, out);
    if (e.code) return lv_meta_wrap("cannot decode reclaim key identity", e);
    return JZ_OK;
}

/* ======================= reference views ======================= */
void groove__db__LargeValueNodeReferences__drop(groove_LargeValueNodeReferences *r) { jz_vec_drop(&r->children, sizeof(groove_NodeRef), NULL); }
static void node_refs_drop_v(void *p) { groove__db__LargeValueNodeReferences__drop(p); }
void groove__db__CompletedLargeValueUpload__drop(groove_CompletedLargeValueUpload *u) { groove__large_values__LargeValueRef__drop(&u->descriptor); }

/* Combine Groove's two reference views without creating another reclamation
 * authority: durable zero is the sole authorization, and resident references
 * can only veto it. */
u8 groove__db__large_value_reclaim_decision(const groove_LargeValueNodeReferences *durable, const groove_LargeValueNodeReferences *resident) {
    bool durable_retained = durable->references != 0 || durable->upload_references != 0;
    bool resident_retained = resident->references != 0 || resident->upload_references != 0;
    if (durable_retained && resident_retained) return GROOVE_RECLAIM_CLEAR_STALE_QUEUE;
    if (durable_retained) return GROOVE_RECLAIM_WAIT_FOR_DURABLE_ZERO;
    if (resident_retained) return GROOVE_RECLAIM_RESIDENT_VETO;
    return GROOVE_RECLAIM_DELETE;
}

int groove__db__node_ref_cmp(const void *a, const void *b, void *ctx) { return memcmp(a, b, sizeof(groove_NodeRef)); }
static int node_ref_cmp2(const void *a, const void *b) { return memcmp(a, b, sizeof(groove_NodeRef)); }

groove_NodeRefVec groove__db__unique_large_value_children(const groove_ChunkNode *node) {
    groove_NodeRefVec children = {0};
    if (node->tag == GROOVE_CHUNK_NODE_LEAF) return children;
    for (u32 i = 0; i < node->children.len; i++) JZ_VEC_PUSH(children, node->children.ptr[i].node_ref);
    return groove__db__canonical_large_value_children(children);
}
/* Metadata child edges describe physical ownership rather than a logical byte
 * order: collect::<BTreeSet<_>>() == sort + dedup. */
groove_NodeRefVec groove__db__canonical_large_value_children(groove_NodeRefVec children) {
    jz_sort_unstable(children.ptr, children.len, sizeof(groove_NodeRef), groove__db__node_ref_cmp, NULL);
    jz_vec_dedup(&children, sizeof(groove_NodeRef), node_ref_cmp2, NULL);
    return children;
}
static bool node_ref_vec_eq(const groove_NodeRefVec *a, const groove_NodeRefVec *b) {
    return a->len == b->len && (a->len == 0 || memcmp(a->ptr, b->ptr, a->len * sizeof(groove_NodeRef)) == 0);
}

/* ======================= durable metadata record schemas ======================= */
enum { ROOT_REF_DURABLE_FIELD = 1, ROOT_REF_STAGED_FIELD = 2, ROOT_REF_NODE_ACTIVE_FIELD = 3 };
enum { NODE_REF_REFERENCES_FIELD = 1, NODE_REF_UPLOAD_REFERENCES_FIELD = 2, NODE_REF_CHILDREN_FIELD = 3 };
enum { STAGED_VALUE_ID_FIELD = 1, STAGED_VALUE_REF_FIELD, STAGED_VALUE_ENCODED_BYTES_FIELD, STAGED_VALUE_NODE_COUNT_FIELD, STAGED_VALUE_CREATED_AT_MS_FIELD };
enum { PENDING_UPLOAD_ID_FIELD = 1, PENDING_UPLOAD_DESCRIPTOR_FIELD, PENDING_UPLOAD_RECEIPT_ID_FIELD, PENDING_UPLOAD_ENCODED_BYTES_FIELD,
       PENDING_UPLOAD_NODE_COUNT_FIELD, PENDING_UPLOAD_CREATED_AT_MS_FIELD, PENDING_UPLOAD_CHUNKS_FIELD };
enum { COMPLETED_UPLOAD_ID_FIELD = 1, COMPLETED_UPLOAD_RECEIPT_ID_FIELD, COMPLETED_UPLOAD_DESCRIPTOR_FIELD, COMPLETED_UPLOAD_ENCODED_BYTES_FIELD,
       COMPLETED_UPLOAD_NODE_COUNT_FIELD };

typedef struct DurableMetadataRecordSchema { JZ_VEC(u16) field_ids; groove_RecordDescriptor descriptor; bool init; } DurableMetadataRecordSchema;
typedef struct MetaField { u16 id; const char *name; groove_ValueType value_type; } MetaField;
static int meta_field_cmp(const void *a, const void *b, void *ctx) {
    u16 x = ((const MetaField *)a)->id, y = ((const MetaField *)b)->id;
    return x < y ? -1 : x > y;
}
/* Construct a fixed engine-owned record layout from permanent, one-based
 * positional field IDs. Gaps and duplicate IDs are rejected. */
static void durable_metadata_record_schema(DurableMetadataRecordSchema *out, MetaField *fields, u32 n) {
    jz_sort(fields, n, sizeof(MetaField), meta_field_cmp, NULL);
    JZ_ASSERT(n != 0, "durable metadata record must have at least one field");
    for (u32 i = 0; i < n; i++)
        JZ_ASSERT(fields[i].id == i + 1, "durable metadata record field IDs must be contiguous starting at one");
    memset(&out->field_ids, 0, sizeof out->field_ids);
    jz_str names[8];
    groove_ValueType types[8];
    JZ_ASSERT(n <= 8, "metadata record too wide");
    for (u32 i = 0; i < n; i++) {
        JZ_VEC_PUSH(out->field_ids, fields[i].id);
        names[i] = jz_format("f%04u_%s", (unsigned)fields[i].id, fields[i].name);
        types[i] = fields[i].value_type;
    }
    out->descriptor = groove__records__RecordDescriptor__new(names, types, n);
    for (u32 i = 0; i < n; i++) jz_str_drop(&names[i]);
    out->init = true;
}
static groove_ValueType vt_scalar(u8 tag) { groove_ValueType t; memset(&t, 0, sizeof t); t.tag = tag; return t; }
static groove_ValueType vt_nullable(groove_ValueType inner) {
    groove_ValueType t = vt_scalar(GROOVE_VT_NULLABLE);
    t.u.inner = jz_memdup(&inner, sizeof inner);
    return t;
}
static groove_ValueType vt_node_ref_array(void) {
    groove_ValueType rec = vt_scalar(GROOVE_VT_RECORD);
    groove_RecordDescriptor d = groove__large_values__node_ref_descriptor();
    rec.u.record = jz_memdup(&d, sizeof d);
    groove_ValueType arr = vt_scalar(GROOVE_VT_ARRAY);
    arr.u.inner = jz_memdup(&rec, sizeof rec);
    return arr;
}

static const DurableMetadataRecordSchema *large_value_root_references_schema(void) {
    static DurableMetadataRecordSchema schema;
    if (!schema.init) {
        MetaField f[] = { { ROOT_REF_DURABLE_FIELD, "durable", vt_scalar(GROOVE_VT_U64) },
                          { ROOT_REF_STAGED_FIELD, "staged", vt_scalar(GROOVE_VT_U64) },
                          { ROOT_REF_NODE_ACTIVE_FIELD, "node_active", vt_scalar(GROOVE_VT_BOOL) } };
        durable_metadata_record_schema(&schema, f, 3);
    }
    return &schema;
}
static const DurableMetadataRecordSchema *large_value_node_references_schema(void) {
    static DurableMetadataRecordSchema schema;
    if (!schema.init) {
        MetaField f[] = { { NODE_REF_REFERENCES_FIELD, "references", vt_scalar(GROOVE_VT_U64) },
                          { NODE_REF_UPLOAD_REFERENCES_FIELD, "upload_references", vt_scalar(GROOVE_VT_U64) },
                          { NODE_REF_CHILDREN_FIELD, "children", vt_node_ref_array() } };
        durable_metadata_record_schema(&schema, f, 3);
    }
    return &schema;
}
static const DurableMetadataRecordSchema *staged_large_value_schema(void) {
    static DurableMetadataRecordSchema schema;
    if (!schema.init) {
        /* Keep the canonical descriptor as opaque bytes at the metadata
         * envelope boundary. */
        MetaField f[] = { { STAGED_VALUE_ID_FIELD, "id", groove__records__values__ValueType__raw_bytes() },
                          { STAGED_VALUE_REF_FIELD, "value_ref", groove__records__values__ValueType__raw_bytes() },
                          { STAGED_VALUE_ENCODED_BYTES_FIELD, "encoded_bytes", vt_scalar(GROOVE_VT_U64) },
                          { STAGED_VALUE_NODE_COUNT_FIELD, "node_count", vt_scalar(GROOVE_VT_U64) },
                          { STAGED_VALUE_CREATED_AT_MS_FIELD, "created_at_ms", vt_scalar(GROOVE_VT_U64) } };
        durable_metadata_record_schema(&schema, f, 5);
    }
    return &schema;
}
static const DurableMetadataRecordSchema *pending_large_value_upload_schema(void) {
    static DurableMetadataRecordSchema schema;
    if (!schema.init) {
        MetaField f[] = { { PENDING_UPLOAD_ID_FIELD, "id", groove__records__values__ValueType__raw_bytes() },
                          { PENDING_UPLOAD_DESCRIPTOR_FIELD, "descriptor", vt_nullable(groove__records__values__ValueType__raw_bytes()) },
                          { PENDING_UPLOAD_RECEIPT_ID_FIELD, "receipt_id", vt_nullable(groove__records__values__ValueType__raw_bytes()) },
                          { PENDING_UPLOAD_ENCODED_BYTES_FIELD, "encoded_bytes", vt_scalar(GROOVE_VT_U64) },
                          { PENDING_UPLOAD_NODE_COUNT_FIELD, "node_count", vt_scalar(GROOVE_VT_U64) },
                          { PENDING_UPLOAD_CREATED_AT_MS_FIELD, "created_at_ms", vt_scalar(GROOVE_VT_U64) },
                          { PENDING_UPLOAD_CHUNKS_FIELD, "chunks", vt_node_ref_array() } };
        durable_metadata_record_schema(&schema, f, 7);
    }
    return &schema;
}
static const DurableMetadataRecordSchema *completed_large_value_upload_schema(void) {
    static DurableMetadataRecordSchema schema;
    if (!schema.init) {
        /* Preserve descriptor-led format dispatch at the metadata boundary. */
        MetaField f[] = { { COMPLETED_UPLOAD_ID_FIELD, "id", groove__records__values__ValueType__raw_bytes() },
                          { COMPLETED_UPLOAD_RECEIPT_ID_FIELD, "receipt_id", groove__records__values__ValueType__raw_bytes() },
                          { COMPLETED_UPLOAD_DESCRIPTOR_FIELD, "descriptor", groove__records__values__ValueType__raw_bytes() },
                          { COMPLETED_UPLOAD_ENCODED_BYTES_FIELD, "encoded_bytes", vt_scalar(GROOVE_VT_U64) },
                          { COMPLETED_UPLOAD_NODE_COUNT_FIELD, "node_count", vt_scalar(GROOVE_VT_U64) } };
        durable_metadata_record_schema(&schema, f, 5);
    }
    return &schema;
}

/* ---- metadata values: BTreeMap<u16, Value> ---- */
static int u16_cmp(const void *a, const void *b, void *ctx) { u16 x = *(const u16 *)a, y = *(const u16 *)b; return x < y ? -1 : x > y; }
static const jz_btree_desc FIELD_VALUES_DESC = { sizeof(u16), sizeof(groove_Value), u16_cmp, NULL, groove__records__values__Value__drop_v };
typedef struct MetaValue { u16 id; groove_Value value; } MetaValue;

static groove_Value v_u64(u64 x) { groove_Value v; memset(&v, 0, sizeof v); v.tag = GROOVE_V_U64; v.u.u64_ = x; return v; }
static groove_Value v_bool(bool x) { groove_Value v; memset(&v, 0, sizeof v); v.tag = GROOVE_V_BOOL; v.u.b = x; return v; }
static groove_Value v_bytes(jz_bytes b) { groove_Value v; memset(&v, 0, sizeof v); v.tag = GROOVE_V_BYTES; v.u.bytes = b; return v; }
static groove_Value v_nullable(groove_Value *inner_or_null) {
    groove_Value v; memset(&v, 0, sizeof v);
    v.tag = GROOVE_V_NULLABLE;
    v.u.nullable = inner_or_null ? jz_memdup(inner_or_null, sizeof *inner_or_null) : NULL;
    return v;
}
static groove_Value staged_large_value_id_value(groove_StagedLargeValueId id) { return v_bytes(jz_bytes_from(id.b, 16)); }
static jz_err staged_large_value_id_from_value(const groove_Value *value, groove_StagedLargeValueId *out) {
    if (value->tag != GROOVE_V_BYTES) return groove__db__Error__invalid_large_value_metadata("large-value staging id must be bytes");
    if (value->u.bytes.len != 16) return groove__db__Error__invalid_large_value_metadata("large-value staging id must be 16 bytes");
    memcpy(out->b, value->u.bytes.ptr, 16);
    return JZ_OK;
}
static groove_Value canonical_node_ref_values(const groove_NodeRefVec *values) {
    groove_NodeRefVec copy = {0};
    jz_vec_extend(&copy, values->ptr, values->len, sizeof(groove_NodeRef));
    copy = groove__db__canonical_large_value_children(copy);
    groove_Value array; memset(&array, 0, sizeof array);
    array.tag = GROOVE_V_ARRAY;
    for (u32 i = 0; i < copy.len; i++) JZ_VEC_PUSH(array.u.items, groove__large_values__node_ref_value(&copy.ptr[i]));
    jz_vec_drop(&copy, sizeof(groove_NodeRef), NULL);
    return array;
}
static jz_err canonical_node_refs_from_value(const groove_Value *value, const char *name, groove_NodeRefVec *out) {
    if (value->tag != GROOVE_V_ARRAY) return groove__db__Error__invalid_large_value_metadata("%s must be an array", name);
    groove_NodeRefVec refs = {0};
    jz_vec_reserve(&refs, value->u.items.len, sizeof(groove_NodeRef));
    for (u32 i = 0; i < value->u.items.len; i++) {
        groove_NodeRef node_ref;
        jz_err e = groove__large_values__node_ref_from_value(&value->u.items.ptr[i], &node_ref);
        if (e.code) {
            jz_vec_drop(&refs, sizeof(groove_NodeRef), NULL);
            jz_err w = groove__db__Error__invalid_large_value_metadata("cannot decode %s: %.*s", name, GROOVE_DB_E(e));
            jz_err_drop(&e);
            return w;
        }
        if (refs.len && groove__db__node_ref_cmp(&refs.ptr[refs.len - 1], &node_ref, NULL) >= 0) {
            jz_vec_drop(&refs, sizeof(groove_NodeRef), NULL);
            return groove__db__Error__invalid_large_value_metadata("%s must be strictly increasing", name);
        }
        JZ_VEC_PUSH(refs, node_ref);
    }
    *out = refs;
    return JZ_OK;
}
/* consumes input values */
static jz_err encode_large_value_metadata_record(const DurableMetadataRecordSchema *schema, MetaValue *input, u32 n, const char *name, jz_bytes *out) {
    jz_btree values;
    jz_btree_init(&values, &FIELD_VALUES_DESC);
    jz_err err = JZ_OK;
    u32 i = 0;
    for (; i < n; i++) {
        if (jz_btree_contains(&values, &input[i].id)) {
            err = groove__db__Error__invalid_large_value_metadata("cannot encode %s: duplicate field id %u", name, (unsigned)input[i].id);
            break;
        }
        jz_btree_insert(&values, &input[i].id, &input[i].value, NULL);
    }
    if (err.code) {
        for (; i < n; i++) groove__records__values__Value__drop(&input[i].value);
        jz_btree_drop(&values);
        return err;
    }
    groove_ValueVec ordered = {0};
    jz_vec_reserve(&ordered, schema->field_ids.len, sizeof(groove_Value));
    for (u32 k = 0; k < schema->field_ids.len; k++) {
        u16 id = schema->field_ids.ptr[k];
        groove_Value v;
        if (!jz_btree_remove(&values, &id, NULL, &v)) {
            err = groove__db__Error__invalid_large_value_metadata("cannot encode %s: missing field id %u", name, (unsigned)id);
            goto done;
        }
        JZ_VEC_PUSH(ordered, v);
    }
    void *k0, *v0;
    if (jz_btree_first(&values, &k0, &v0)) {
        err = groove__db__Error__invalid_large_value_metadata("cannot encode %s: unknown field id %u", name, (unsigned)*(u16 *)k0);
        goto done;
    }
    {
        jz_err e = groove__records__RecordDescriptor__create(schema->descriptor, ordered.ptr, ordered.len, out);
        if (e.code) {
            err = groove__db__Error__invalid_large_value_metadata("cannot encode %s: %.*s", name, GROOVE_DB_E(e));
            jz_err_drop(&e);
        }
    }
done:
    groove__records__values__ValueVec__drop(&ordered);
    jz_btree_drop(&values);
    return err;
}
static jz_err decode_large_value_metadata_record(const u8 *encoded, u32 n, const DurableMetadataRecordSchema *schema, const char *name, jz_btree *out) {
    groove_BorrowedRecord record = { jz_slice_of(encoded, n), schema->descriptor };
    groove_ValueVec values;
    jz_err e = groove__records__BorrowedRecord__to_values(&record, &values);
    if (e.code) {
        jz_err w = groove__db__Error__invalid_large_value_metadata("cannot decode %s: %.*s", name, GROOVE_DB_E(e));
        jz_err_drop(&e);
        return w;
    }
    jz_bytes canonical;
    e = groove__records__RecordDescriptor__create(schema->descriptor, values.ptr, values.len, &canonical);
    if (e.code) {
        groove__records__values__ValueVec__drop(&values);
        jz_err w = groove__db__Error__invalid_large_value_metadata("cannot decode %s: %.*s", name, GROOVE_DB_E(e));
        jz_err_drop(&e);
        return w;
    }
    bool same = canonical.len == n && memcmp(canonical.ptr, encoded, n) == 0;
    jz_bytes_drop(&canonical);
    if (!same) {
        groove__records__values__ValueVec__drop(&values);
        return groove__db__Error__invalid_large_value_metadata("cannot decode %s: non-canonical record", name);
    }
    jz_btree_init(out, &FIELD_VALUES_DESC);
    u32 count = values.len < schema->field_ids.len ? values.len : schema->field_ids.len;
    for (u32 i = 0; i < count; i++) jz_btree_insert(out, &schema->field_ids.ptr[i], &values.ptr[i], NULL);
    for (u32 i = count; i < values.len; i++) groove__records__values__Value__drop(&values.ptr[i]);
    jz_vec_drop(&values, sizeof(groove_Value), NULL);
    return JZ_OK;
}
static jz_err take_metadata_field(jz_btree *values, u16 id, const char *name, groove_Value *out) {
    if (!jz_btree_remove(values, &id, NULL, out))
        return groove__db__Error__invalid_large_value_metadata("cannot decode %s: missing field id %u", name, (unsigned)id);
    return JZ_OK;
}
/* let records::Value::<tag>(x) = take_metadata_field(..)? else { return Err(..) } */
static jz_err take_typed(jz_btree *values, u16 id, const char *name, u8 tag, const char *invalid, groove_Value *out) {
    JZ_TRY(take_metadata_field(values, id, name, out));
    if (out->tag != tag) {
        groove__records__values__Value__drop(out);
        return groove__db__Error__invalid_large_value_metadata("%s", invalid);
    }
    return JZ_OK;
}

/* ---- completed upload ---- */
jz_err groove__db__encode_completed_large_value_upload(const groove_CompletedLargeValueUpload *upload, jz_bytes *out) {
    jz_bytes descriptor;
    jz_err e = groove__large_values__encode_large_value_ref(&upload->descriptor, &descriptor);
    if (e.code) return lv_meta_wrap("cannot encode completed upload descriptor", e);
    MetaValue v[] = {
        { COMPLETED_UPLOAD_ID_FIELD, staged_large_value_id_value(upload->id) },
        { COMPLETED_UPLOAD_RECEIPT_ID_FIELD, staged_large_value_id_value(upload->receipt_id) },
        { COMPLETED_UPLOAD_DESCRIPTOR_FIELD, v_bytes(descriptor) },
        { COMPLETED_UPLOAD_ENCODED_BYTES_FIELD, v_u64(upload->accounting.encoded_bytes) },
        { COMPLETED_UPLOAD_NODE_COUNT_FIELD, v_u64(upload->accounting.node_count) },
    };
    return encode_large_value_metadata_record(completed_large_value_upload_schema(), v, 5, "completed upload", out);
}
static jz_err decode_completed_large_value_upload(const u8 *encoded, u32 n, groove_CompletedLargeValueUpload *out) {
    const char *name = "completed upload";
    jz_btree fields;
    JZ_TRY(decode_large_value_metadata_record(encoded, n, completed_large_value_upload_schema(), name, &fields));
    groove_Value id = {0}, receipt_id = {0}, descriptor = {0}, encoded_bytes = {0}, node_count = {0};
    jz_err e;
    if ((e = take_metadata_field(&fields, COMPLETED_UPLOAD_ID_FIELD, name, &id)).code) goto fail0;
    if ((e = take_metadata_field(&fields, COMPLETED_UPLOAD_RECEIPT_ID_FIELD, name, &receipt_id)).code) goto fail1;
    if ((e = take_typed(&fields, COMPLETED_UPLOAD_DESCRIPTOR_FIELD, name, GROOVE_V_BYTES, "invalid completed upload descriptor", &descriptor)).code) goto fail2;
    if ((e = take_typed(&fields, COMPLETED_UPLOAD_ENCODED_BYTES_FIELD, name, GROOVE_V_U64, "invalid completed upload accounting", &encoded_bytes)).code) goto fail3;
    if ((e = take_typed(&fields, COMPLETED_UPLOAD_NODE_COUNT_FIELD, name, GROOVE_V_U64, "invalid completed upload accounting", &node_count)).code) goto fail3;
    if ((e = staged_large_value_id_from_value(&id, &out->id)).code) goto fail3;
    if ((e = staged_large_value_id_from_value(&receipt_id, &out->receipt_id)).code) goto fail3;
    e = groove__large_values__decode_large_value_ref(descriptor.u.bytes.ptr, descriptor.u.bytes.len, &out->descriptor);
    if (e.code) { e = lv_meta_wrap("cannot decode completed upload descriptor", e); goto fail3; }
    out->accounting.encoded_bytes = encoded_bytes.u.u64_;
    out->accounting.node_count = node_count.u.u64_;
fail3:
    groove__records__values__Value__drop(&descriptor);
fail2:
    groove__records__values__Value__drop(&receipt_id);
fail1:
    groove__records__values__Value__drop(&id);
fail0:
    jz_btree_drop(&fields);
    return e;
}

/* ---- root references ---- */
jz_err groove__db__encode_large_value_root_references(const groove_LargeValueRootReferences *r, jz_bytes *out) {
    MetaValue v[] = {
        { ROOT_REF_DURABLE_FIELD, v_u64(r->durable) },
        { ROOT_REF_STAGED_FIELD, v_u64(r->staged) },
        { ROOT_REF_NODE_ACTIVE_FIELD, v_bool(r->node_active) },
    };
    return encode_large_value_metadata_record(large_value_root_references_schema(), v, 3, "root references", out);
}
jz_err groove__db__decode_large_value_root_references(const u8 *p, u32 n, groove_LargeValueRootReferences *out) {
    const char *name = "root references", *invalid = "cannot decode root references: invalid fields";
    jz_btree values;
    JZ_TRY(decode_large_value_metadata_record(p, n, large_value_root_references_schema(), name, &values));
    groove_Value durable, staged, node_active;
    jz_err e;
    if ((e = take_typed(&values, ROOT_REF_DURABLE_FIELD, name, GROOVE_V_U64, invalid, &durable)).code) goto done;
    if ((e = take_typed(&values, ROOT_REF_STAGED_FIELD, name, GROOVE_V_U64, invalid, &staged)).code) goto done;
    if ((e = take_typed(&values, ROOT_REF_NODE_ACTIVE_FIELD, name, GROOVE_V_BOOL, invalid, &node_active)).code) goto done;
    out->durable = durable.u.u64_;
    out->staged = staged.u.u64_;
    out->node_active = node_active.u.b;
done:
    jz_btree_drop(&values);
    return e;
}

/* ---- node references ---- */
jz_err groove__db__encode_large_value_node_references(const groove_LargeValueNodeReferences *r, jz_bytes *out) {
    MetaValue v[] = {
        { NODE_REF_REFERENCES_FIELD, v_u64(r->references) },
        { NODE_REF_UPLOAD_REFERENCES_FIELD, v_u64(r->upload_references) },
        { NODE_REF_CHILDREN_FIELD, canonical_node_ref_values(&r->children) },
    };
    return encode_large_value_metadata_record(large_value_node_references_schema(), v, 3, "large-value node references", out);
}
jz_err groove__db__decode_large_value_node_references(const u8 *p, u32 n, groove_LargeValueNodeReferences *out) {
    const char *name = "large-value node references", *invalid = "cannot decode large-value node references: invalid fields";
    jz_btree values;
    JZ_TRY(decode_large_value_metadata_record(p, n, large_value_node_references_schema(), name, &values));
    groove_Value references, upload_references, children;
    jz_err e;
    if ((e = take_typed(&values, NODE_REF_REFERENCES_FIELD, name, GROOVE_V_U64, invalid, &references)).code) goto done;
    if ((e = take_typed(&values, NODE_REF_UPLOAD_REFERENCES_FIELD, name, GROOVE_V_U64, invalid, &upload_references)).code) goto done;
    if ((e = take_metadata_field(&values, NODE_REF_CHILDREN_FIELD, name, &children)).code) goto done;
    out->references = references.u.u64_;
    out->upload_references = upload_references.u.u64_;
    e = canonical_node_refs_from_value(&children, "large-value node children", &out->children);
    groove__records__values__Value__drop(&children);
done:
    jz_btree_drop(&values);
    return e;
}

/* ---- staged large value ---- */
jz_err groove__db__encode_staged_large_value(const groove_StagedLargeValue *staged, jz_bytes *out) {
    jz_bytes value_ref;
    jz_err e = groove__large_values__encode_large_value_ref(&staged->value_ref, &value_ref);
    if (e.code) return lv_meta_wrap("cannot encode staged large value", e);
    MetaValue v[] = {
        { STAGED_VALUE_ID_FIELD, staged_large_value_id_value(staged->id) },
        { STAGED_VALUE_REF_FIELD, v_bytes(value_ref) },
        { STAGED_VALUE_ENCODED_BYTES_FIELD, v_u64(staged->accounting.encoded_bytes) },
        { STAGED_VALUE_NODE_COUNT_FIELD, v_u64(staged->accounting.node_count) },
        { STAGED_VALUE_CREATED_AT_MS_FIELD, v_u64(staged->created_at_ms) },
    };
    return encode_large_value_metadata_record(staged_large_value_schema(), v, 5, "staged large value", out);
}
static jz_err decode_staged_large_value(const u8 *p, u32 n, groove_StagedLargeValue *out) {
    const char *name = "staged large value", *invalid = "cannot decode staged large value: invalid fields";
    jz_btree values;
    JZ_TRY(decode_large_value_metadata_record(p, n, staged_large_value_schema(), name, &values));
    groove_Value id = {0}, value_ref = {0}, encoded_bytes, node_count, created_at_ms;
    jz_err e;
    if ((e = take_metadata_field(&values, STAGED_VALUE_ID_FIELD, name, &id)).code) goto done;
    if ((e = take_typed(&values, STAGED_VALUE_REF_FIELD, name, GROOVE_V_BYTES, "cannot decode staged large value: invalid descriptor bytes", &value_ref)).code) goto done;
    if ((e = take_typed(&values, STAGED_VALUE_ENCODED_BYTES_FIELD, name, GROOVE_V_U64, invalid, &encoded_bytes)).code) goto done;
    if ((e = take_typed(&values, STAGED_VALUE_NODE_COUNT_FIELD, name, GROOVE_V_U64, invalid, &node_count)).code) goto done;
    if ((e = take_typed(&values, STAGED_VALUE_CREATED_AT_MS_FIELD, name, GROOVE_V_U64, invalid, &created_at_ms)).code) goto done;
    if ((e = staged_large_value_id_from_value(&id, &out->id)).code) goto done;
    e = groove__large_values__decode_large_value_ref(value_ref.u.bytes.ptr, value_ref.u.bytes.len, &out->value_ref);
    if (e.code) { e = lv_meta_wrap("cannot decode staged large value descriptor", e); goto done; }
    out->accounting.encoded_bytes = encoded_bytes.u.u64_;
    out->accounting.node_count = node_count.u.u64_;
    out->created_at_ms = created_at_ms.u.u64_;
done:
    groove__records__values__Value__drop(&id);
    groove__records__values__Value__drop(&value_ref);
    jz_btree_drop(&values);
    return e;
}
jz_err groove__db__decode_staged_large_value_at_key(const u8 *key, u32 kn, const u8 *p, u32 n, groove_StagedLargeValue *out) {
    groove_StagedLargeValueId key_id;
    JZ_TRY(staged_large_value_id_from_metadata_key(key, kn, "staged/", "staged large value", &key_id));
    JZ_TRY(decode_staged_large_value(p, n, out));
    if (memcmp(out->id.b, key_id.b, 16) != 0) {
        groove__large_values__StagedLargeValue__drop(out);
        return groove__db__Error__invalid_large_value_metadata("staged large value key and receipt id differ");
    }
    return JZ_OK;
}

/* ---- pending upload ---- */
jz_err groove__db__encode_pending_large_value_upload(const groove_PendingLargeValueUpload *upload, jz_bytes *out) {
    bool has_descriptor = upload->has_descriptor;
    jz_bytes descriptor = {0};
    if (has_descriptor) {
        jz_err e = groove__large_values__encode_large_value_ref(&upload->descriptor, &descriptor);
        if (e.code) return lv_meta_wrap("cannot encode pending large-value upload descriptor", e);
    }
    groove_Value descriptor_inner = v_bytes(descriptor);
    groove_Value receipt_inner = upload->has_receipt_id ? staged_large_value_id_value(upload->receipt_id) : v_u64(0);
    MetaValue v[] = {
        { PENDING_UPLOAD_ID_FIELD, staged_large_value_id_value(upload->id) },
        { PENDING_UPLOAD_DESCRIPTOR_FIELD, v_nullable(has_descriptor ? &descriptor_inner : NULL) },
        { PENDING_UPLOAD_RECEIPT_ID_FIELD, v_nullable(upload->has_receipt_id ? &receipt_inner : NULL) },
        { PENDING_UPLOAD_ENCODED_BYTES_FIELD, v_u64(upload->accounting.encoded_bytes) },
        { PENDING_UPLOAD_NODE_COUNT_FIELD, v_u64(upload->accounting.node_count) },
        { PENDING_UPLOAD_CREATED_AT_MS_FIELD, v_u64(upload->created_at_ms) },
        { PENDING_UPLOAD_CHUNKS_FIELD, canonical_node_ref_values(&upload->chunks) },
    };
    return encode_large_value_metadata_record(pending_large_value_upload_schema(), v, 7, "pending large-value upload", out);
}
static jz_err decode_pending_large_value_upload(const u8 *p, u32 n, groove_PendingLargeValueUpload *out) {
    const char *name = "pending large-value upload", *invalid = "cannot decode pending large-value upload: invalid fields";
    jz_btree values;
    JZ_TRY(decode_large_value_metadata_record(p, n, pending_large_value_upload_schema(), name, &values));
    groove_Value id = {0}, descriptor = {0}, receipt_id = {0}, encoded_bytes, node_count, created_at_ms, chunks = {0};
    memset(out, 0, sizeof *out);
    jz_err e;
    if ((e = take_metadata_field(&values, PENDING_UPLOAD_ID_FIELD, name, &id)).code) goto done;
    if ((e = take_typed(&values, PENDING_UPLOAD_DESCRIPTOR_FIELD, name, GROOVE_V_NULLABLE, invalid, &descriptor)).code) goto done;
    if ((e = take_typed(&values, PENDING_UPLOAD_RECEIPT_ID_FIELD, name, GROOVE_V_NULLABLE, invalid, &receipt_id)).code) goto done;
    if ((e = take_typed(&values, PENDING_UPLOAD_ENCODED_BYTES_FIELD, name, GROOVE_V_U64, invalid, &encoded_bytes)).code) goto done;
    if ((e = take_typed(&values, PENDING_UPLOAD_NODE_COUNT_FIELD, name, GROOVE_V_U64, invalid, &node_count)).code) goto done;
    if ((e = take_typed(&values, PENDING_UPLOAD_CREATED_AT_MS_FIELD, name, GROOVE_V_U64, invalid, &created_at_ms)).code) goto done;
    if ((e = take_metadata_field(&values, PENDING_UPLOAD_CHUNKS_FIELD, name, &chunks)).code) goto done;
    if (descriptor.u.nullable) {
        const groove_Value *inner = descriptor.u.nullable;
        if (inner->tag == GROOVE_V_BYTES) e = groove__large_values__decode_large_value_ref(inner->u.bytes.ptr, inner->u.bytes.len, &out->descriptor);
        else e = groove__large_values__Error__new(GROOVE_EXT_LV_E_MALFORMED_SCALAR);
        if (e.code) { e = lv_meta_wrap("cannot decode pending large-value upload descriptor", e); goto done; }
        out->has_descriptor = true;
    }
    if (receipt_id.u.nullable) {
        if ((e = staged_large_value_id_from_value(receipt_id.u.nullable, &out->receipt_id)).code) goto fail_desc;
        out->has_receipt_id = true;
    }
    if ((e = staged_large_value_id_from_value(&id, &out->id)).code) goto fail_desc;
    out->accounting.encoded_bytes = encoded_bytes.u.u64_;
    out->accounting.node_count = node_count.u.u64_;
    out->created_at_ms = created_at_ms.u.u64_;
    if ((e = canonical_node_refs_from_value(&chunks, "pending large-value upload chunks", &out->chunks)).code) goto fail_desc;
    goto done;
fail_desc:
    if (out->has_descriptor) groove__large_values__LargeValueRef__drop(&out->descriptor);
    out->has_descriptor = false;
done:
    groove__records__values__Value__drop(&id);
    groove__records__values__Value__drop(&descriptor);
    groove__records__values__Value__drop(&receipt_id);
    groove__records__values__Value__drop(&chunks);
    jz_btree_drop(&values);
    return e;
}
jz_err groove__db__decode_pending_large_value_upload_at_key(const u8 *key, u32 kn, const u8 *p, u32 n, groove_PendingLargeValueUpload *out) {
    groove_StagedLargeValueId key_id;
    JZ_TRY(staged_large_value_id_from_metadata_key(key, kn, "upload/", "pending upload", &key_id));
    JZ_TRY(decode_pending_large_value_upload(p, n, out));
    if (memcmp(out->id.b, key_id.b, 16) != 0) {
        groove__large_values__PendingLargeValueUpload__drop(out);
        return groove__db__Error__invalid_large_value_metadata("pending upload key and journal id differ");
    }
    return JZ_OK;
}
/* Decode the forward completed-upload journal only when its embedded upload
 * identity is the canonical fixed-width suffix of its metadata key. */
jz_err groove__db__decode_completed_large_value_upload_at_key(const u8 *key, u32 kn, const u8 *p, u32 n, groove_CompletedLargeValueUpload *out) {
    groove_StagedLargeValueId key_id;
    JZ_TRY(staged_large_value_id_from_metadata_key(key, kn, "completed-upload/", "completed upload", &key_id));
    JZ_TRY(decode_completed_large_value_upload(p, n, out));
    if (memcmp(out->id.b, key_id.b, 16) != 0) {
        groove__db__CompletedLargeValueUpload__drop(out);
        return groove__db__Error__invalid_large_value_metadata("completed upload key and journal id differ");
    }
    return JZ_OK;
}
/* Decode the reverse binding's fixed-width upload ID after validating the
 * receipt key. */
jz_err groove__db__decode_completed_large_value_receipt_at_key(const u8 *key, u32 kn, const u8 *p, u32 n, groove_StagedLargeValueId *out) {
    groove_StagedLargeValueId ignored;
    JZ_TRY(staged_large_value_id_from_metadata_key(key, kn, "completed-receipt/", "completed receipt", &ignored));
    if (n != 16) return groove__db__Error__invalid_large_value_metadata("completed receipt binding must contain exactly 16 upload-id bytes");
    memcpy(out->b, p, 16);
    return JZ_OK;
}

/* ======================= btree descriptors ======================= */
static void str_drop_v(void *p) { jz_str_drop(p); }
static void bytes_drop_v(void *p) { jz_bytes_drop(p); }
static void btree_drop_v(void *p) { jz_btree_drop(p); }
static void rc_release_v(void *p) { if (*(void **)p) jz_rc_release(*(void **)p); }
static void waker_drop_v(void *p) { (void)p; }
static int exact_key_cmp(const void *a, const void *b, void *ctx) {
    const groove_ExactKey *x = a, *y = b;
    int c = jz_str_cmp(&x->table, &y->table);
    return c ? c : jz_bytes_cmp(&x->key, &y->key);
}
static void exact_key_drop(void *p) { groove_ExactKey *k = p; jz_str_drop(&k->table); jz_bytes_drop(&k->key); }
const jz_btree_desc groove__db__NODE_UPDATES_DESC = { sizeof(groove_NodeRef), sizeof(groove_LargeValueNodeReferences), groove__db__node_ref_cmp, NULL, node_refs_drop_v };
const jz_btree_desc groove__db__NODE_SET_DESC = { sizeof(groove_NodeRef), 0, groove__db__node_ref_cmp, NULL, NULL };
const jz_btree_desc groove__db__NODE_I64_DESC = { sizeof(groove_NodeRef), sizeof(i64), groove__db__node_ref_cmp, NULL, NULL };
const jz_btree_desc groove__db__NODE_U64_DESC = { sizeof(groove_NodeRef), sizeof(u64), groove__db__node_ref_cmp, NULL, NULL };
const jz_btree_desc groove__db__EXACT_KEYS_DESC = { sizeof(groove_ExactKey), sizeof(jz_bytes), exact_key_cmp, exact_key_drop, bytes_drop_v };
const jz_btree_desc groove__db__STR_SET_DESC = { sizeof(jz_str), 0, jz_cmp_str, str_drop_v, NULL };
const jz_btree_desc groove__db__STORED_DESCRIPTORS_DESC = { sizeof(jz_str), sizeof(jz_btree), jz_cmp_str, str_drop_v, btree_drop_v };
const jz_btree_desc groove__db__VARIANT_DESCRIPTORS_DESC = { sizeof(u32), sizeof(groove_RecordDescriptor), jz_cmp_u32, NULL, NULL };
const jz_btree_desc groove__db__PUBLICATION_STATES_DESC = { sizeof(u64), sizeof(void *), jz_cmp_u64, NULL, rc_release_v };
const jz_btree_desc groove__db__U64_SET_DESC = { sizeof(u64), 0, jz_cmp_u64, NULL, NULL };
static const jz_btree_desc WAITERS_DESC = { sizeof(u64), sizeof(jz_waker), jz_cmp_u64, NULL, waker_drop_v };

/* ======================= node reference transitions ======================= */
/* Apply physical-node ownership transitions against one read-your-own-write
 * overlay. Each active parent contributes one reference to each distinct child
 * node, regardless of how many logical occurrences of that child the parent's
 * branch contains. */
jz_err groove__db__large_value_node_transition_operations(groove_DynStorage storage, jz_btree node_updates, groove_NodeTransitionVec pending,
                                                          bool allow_missing_positive_metadata, groove_WriteOpVec *out) {
    jz_err e = JZ_OK;
    jz_btree reclaim_candidates;
    jz_btree_init(&reclaim_candidates, &groove__db__NODE_SET_DESC);
    groove_WriteOpVec operations = {0};
    groove_PhysicalTraversalNodeBudget node_budget = groove__large_values__PhysicalTraversalNodeBudget__new();
    if ((e = groove__large_values__PhysicalTraversalNodeBudget__consume_many(&node_budget, node_updates.len)).code) goto done;
    while (pending.len) {
        groove_NodeTransition t = pending.ptr[--pending.len];
        groove_NodeRef node_ref = t.node_ref;
        i8 delta = t.delta;
        groove_LargeValueNodeReferences metadata;
        if (!jz_btree_remove(&node_updates, &node_ref, NULL, &metadata)) {
            if ((e = groove__large_values__PhysicalTraversalNodeBudget__consume(&node_budget)).code) goto done;
            jz_bytes key;
            if ((e = groove__db__large_value_node_key(&node_ref, &key)).code) goto done;
            bool found;
            jz_bytes encoded;
            if ((e = groove__storage__OrderedKvStorage__get(storage, groove__db__metadata_cf(), key, &found, &encoded)).code) goto done;
            if (found) {
                e = groove__db__decode_large_value_node_references(encoded.ptr, encoded.len, &metadata);
                jz_bytes_drop(&encoded);
                if (e.code) goto done;
            } else if (delta > 0 && allow_missing_positive_metadata) {
                memset(&metadata, 0, sizeof metadata);
            } else {
                e = groove__db__Error__invalid_large_value_metadata("active node reference metadata is missing");
                goto done;
            }
        }
        metadata.children = groove__db__canonical_large_value_children(metadata.children);
        bool crossed_zero;
        if (delta > 0) {
            bool crossed = metadata.references == 0;
            if (metadata.references == UINT64_MAX) {
                groove__db__LargeValueNodeReferences__drop(&metadata);
                e = groove__db__Error__invalid_large_value_metadata("node reference count overflow");
                goto done;
            }
            metadata.references += 1;
            jz_btree_remove(&reclaim_candidates, &node_ref, NULL, NULL);
            crossed_zero = crossed;
        } else {
            if (metadata.references == 0) {
                groove__db__LargeValueNodeReferences__drop(&metadata);
                e = groove__db__Error__invalid_large_value_metadata("node reference count underflow");
                goto done;
            }
            metadata.references -= 1;
            bool crossed = metadata.references == 0;
            if (crossed) jz_btree_insert(&reclaim_candidates, &node_ref, NULL, NULL);
            crossed_zero = crossed;
        }
        if (crossed_zero)
            for (u32 i = 0; i < metadata.children.len; i++) {
                groove_NodeTransition child = { metadata.children.ptr[i], delta };
                JZ_VEC_PUSH(pending, child);
            }
        jz_btree_insert(&node_updates, &node_ref, &metadata, NULL);
    }
    groove_NodeRef *node_ref;
    groove_LargeValueNodeReferences *metadata;
    JZ_BTREE_FOR(&node_updates, node_ref, metadata) {
        jz_bytes key, value;
        if ((e = groove__db__large_value_node_key(node_ref, &key)).code) goto done;
        if ((e = groove__db__encode_large_value_node_references(metadata, &value)).code) { jz_bytes_drop(&key); goto done; }
        JZ_VEC_PUSH(operations, groove__db__metadata_set(key, value));
        if (metadata->references == 0 && jz_btree_contains(&reclaim_candidates, node_ref)) {
            if ((e = groove__db__large_value_reclaim_key(node_ref, &key)).code) goto done;
            jz_err ee = groove__large_values__encode_node_ref(node_ref, &value);
            if (ee.code) { jz_bytes_drop(&key); e = lv_meta_wrap("cannot encode reclaim entry", ee); goto done; }
            JZ_VEC_PUSH(operations, groove__db__metadata_set(key, value));
        }
    }
done:
    jz_btree_drop(&reclaim_candidates);
    jz_btree_drop(&node_updates);
    jz_vec_drop(&pending, sizeof(groove_NodeTransition), NULL);
    if (e.code) { groove__db__WriteOpVec__drop(&operations); return e; }
    *out = operations;
    return JZ_OK;
}

/* ======================= small Rc helpers ======================= */
void *groove__db__unit_rc_new(void) { return jz_rc_new(1, NULL); }
bool *groove__db__cell_bool_new(bool v) { bool *c = jz_rc_new(sizeof(bool), NULL); *c = v; return c; }

groove_LifecycleMutex *groove__db__LifecycleMutex__new(void) { groove_LifecycleMutex *m = jz_rc_new(sizeof *m, NULL); m->locked = false; return m; }
/* lock_owned().await: the synchronous port cannot suspend on contention. */
void groove__db__LifecycleMutex__lock(groove_LifecycleMutex *m) {
    if (m->locked) jz_panic("large-value lifecycle mutex is contended");
    m->locked = true;
}
bool groove__db__LifecycleMutex__try_lock(groove_LifecycleMutex *m) {
    if (m->locked) return false;
    m->locked = true;
    return true;
}
void groove__db__LifecycleMutex__unlock(groove_LifecycleMutex *m) { m->locked = false; }

/* ======================= chunk install journal ======================= */
/* The recovery journal is intentionally separate from byte storage and from
 * metadata installation. It only answers whether a particular exact chunk
 * needs reconciliation. */
static jz_err journal_mark_pending(void *selfp, groove_NodeRef node_ref) {
    groove_MetadataChunkInstallJournal *self = selfp;
    groove_LayoutStorage *storage = groove__db__weak_upgrade(self->storage);
    if (!storage) {
        static const char msg[] = "database storage closed while recording chunk installation";
        return groove__chunks__ChunkError__backend(msg, sizeof msg - 1);
    }
    jz_bytes key;
    jz_err e = groove__db__large_value_pending_install_key(&node_ref, &key);
    if (e.code) { jz_rc_release(storage); return chunk_backend(e); }
    bool found;
    jz_bytes existing = {0};
    jz_bytes empty = {0};
    e = groove__storage__OrderedKvStorage__put_if_absent(groove__storage__LayoutStorage__as_dyn(storage), groove__db__metadata_cf(), key, empty, &found, &existing);
    jz_rc_release(storage);
    if (e.code) return chunk_backend(e);
    bool non_empty = found && existing.len != 0;
    if (found) jz_bytes_drop(&existing);
    if (non_empty) return groove__chunks__ChunkError__integrity();
    return JZ_OK;
}
static jz_err journal_is_pending(void *selfp, groove_NodeRef node_ref, bool *out) {
    groove_MetadataChunkInstallJournal *self = selfp;
    groove_LayoutStorage *storage = groove__db__weak_upgrade(self->storage);
    if (!storage) {
        static const char msg[] = "database storage closed while reading chunk installation journal";
        return groove__chunks__ChunkError__backend(msg, sizeof msg - 1);
    }
    jz_bytes key;
    jz_err e = groove__db__large_value_pending_install_key(&node_ref, &key);
    if (e.code) { jz_rc_release(storage); return chunk_backend(e); }
    bool found;
    jz_bytes value;
    e = groove__storage__OrderedKvStorage__get(groove__storage__LayoutStorage__as_dyn(storage), groove__db__metadata_cf(), key, &found, &value);
    jz_rc_release(storage);
    if (e.code) return chunk_backend(e);
    if (!found) { *out = false; return JZ_OK; }
    bool empty = value.len == 0;
    jz_bytes_drop(&value);
    if (empty) { *out = true; return JZ_OK; }
    return groove__chunks__ChunkError__integrity();
}
static jz_err journal_complete(void *selfp, groove_NodeRef node_ref) {
    groove_MetadataChunkInstallJournal *self = selfp;
    groove_LayoutStorage *storage = groove__db__weak_upgrade(self->storage);
    if (!storage) {
        static const char msg[] = "database storage closed while completing chunk installation";
        return groove__chunks__ChunkError__backend(msg, sizeof msg - 1);
    }
    jz_bytes key;
    jz_err e = groove__db__large_value_pending_install_key(&node_ref, &key);
    if (e.code) { jz_rc_release(storage); return chunk_backend(e); }
    bool removed;
    jz_bytes empty = {0};
    e = groove__storage__OrderedKvStorage__compare_and_delete(groove__storage__LayoutStorage__as_dyn(storage), groove__db__metadata_cf(), key, empty, &removed);
    jz_rc_release(storage);
    if (e.code) return chunk_backend(e);
    return JZ_OK;
}
static void journal_drop(void *selfp) { jz_rc_release(selfp); }
static const groove_ChunkInstallJournalVt METADATA_JOURNAL_VT = { journal_mark_pending, journal_is_pending, journal_complete, journal_drop };
groove_DynChunkInstallJournal groove__db__MetadataChunkInstallJournal__new_dyn(groove_LayoutStorage *storage) {
    groove_MetadataChunkInstallJournal *j = jz_rc_new(sizeof *j, NULL);
    j->storage = storage;
    groove_DynChunkInstallJournal d = { j, &METADATA_JOURNAL_VT };
    return d;
}

/* ======================= chunk install observer ======================= */
static void resident_install_clone(groove_ResidentLifecycleInstall *dst, const groove_ResidentLifecycleInstall *src) {
    dst->storage = groove__storage__OwnedStorage__clone(&src->storage);
    dst->staged = jz_rc_clone(src->staged);
    dst->lifecycle_held = jz_rc_clone(src->lifecycle_held);
    dst->durable = jz_rc_clone(src->durable);
    dst->install_failures = groove__chunks__PublicationInstallFailures__clone(&src->install_failures);
}
static void resident_install_drop(groove_ResidentLifecycleInstall *r) {
    groove__storage__OwnedStorage__drop(&r->storage);
    jz_rc_release(r->staged);
    jz_rc_release(r->lifecycle_held);
    jz_rc_release(r->durable);
    groove__chunks__PublicationInstallFailures__drop(&r->install_failures);
}
static void observer_payload_drop(void *p) {
    groove_MetadataChunkInstallObserver *o = p;
    if (o->has_resident_install) resident_install_drop(&o->resident_install);
}

static jz_err observer_installed(void *selfp, groove_NodeRef node_ref, jz_bytes encoded) {
    groove_MetadataChunkInstallObserver *self = selfp;
    groove_LayoutStorage *storage = groove__db__weak_upgrade(self->storage);
    if (!storage) {
        jz_bytes_drop(&encoded);
        static const char msg[] = "database storage closed during chunk installation";
        return groove__chunks__ChunkError__backend(msg, sizeof msg - 1);
    }
    jz_err e = JZ_OK;
    bool has_install = self->has_resident_install;
    groove_ResidentLifecycleInstall install;
    if (has_install) resident_install_clone(&install, &self->resident_install);
    groove_LifecycleMutex *lifecycle = NULL;
    groove_ChunkNode node;
    bool have_node = false;
    groove_NodeRefVec children = {0};
    groove_LargeValueNodeReferences metadata = {0};
    groove_NodeRefVec existing_children = {0};
    jz_bytes root_key = {0};
    groove_WriteOpVec operations = {0};
    if (!has_install || !*install.lifecycle_held) {
        lifecycle = groove__db__weak_upgrade(self->lifecycle);
        if (!lifecycle) {
            static const char msg[] = "database lifecycle closed during chunk installation";
            e = groove__chunks__ChunkError__backend(msg, sizeof msg - 1);
            goto done;
        }
        groove__db__LifecycleMutex__lock(lifecycle);
    }
    groove_DynStorage read_storage = (has_install && !*install.durable) ? install.storage.storage : groove__storage__LayoutStorage__as_dyn(storage);
    if (groove__large_values__decode_node_untyped_authenticated(node_ref.object_hash, encoded.ptr, encoded.len, &node).code) {
        e = groove__chunks__ChunkError__integrity();
        goto done;
    }
    have_node = true;
    children = groove__db__unique_large_value_children(&node);
    jz_bytes node_key;
    if ((e = groove__db__large_value_node_key(&node_ref, &node_key)).code) { e = chunk_backend(e); goto done; }
    bool found;
    jz_bytes existing;
    if ((e = groove__storage__OrderedKvStorage__get(read_storage, groove__db__metadata_cf(), node_key, &found, &existing)).code) { e = chunk_backend(e); goto done; }
    if (found) {
        e = groove__db__decode_large_value_node_references(existing.ptr, existing.len, &metadata);
        jz_bytes_drop(&existing);
        if (e.code) { e = chunk_backend(e); goto done; }
    }
    existing_children = groove__db__canonical_large_value_children(metadata.children);
    memset(&metadata.children, 0, sizeof metadata.children);
    if (existing_children.len != 0 && !node_ref_vec_eq(&existing_children, &children)) {
        e = groove__chunks__ChunkError__integrity();
        goto done;
    }
    bool newly_discovered_active_children = metadata.references > 0 && existing_children.len == 0 && children.len != 0;
    jz_vec_extend(&metadata.children, children.ptr, children.len, sizeof(groove_NodeRef));

    if ((e = groove__db__large_value_root_key(&node_ref, &root_key)).code) { e = chunk_backend(e); goto done; }
    jz_bytes root_encoded;
    if ((e = groove__storage__OrderedKvStorage__get(read_storage, groove__db__metadata_cf(), jz_bytes_clone(&root_key), &found, &root_encoded)).code) { e = chunk_backend(e); goto done; }
    groove_LargeValueRootReferences root_references = {0};
    if (found) {
        e = groove__db__decode_large_value_root_references(root_encoded.ptr, root_encoded.len, &root_references);
        jz_bytes_drop(&root_encoded);
        if (e.code) { e = chunk_backend(e); goto done; }
    }
    u64 total = root_references.durable + root_references.staged;
    if (total < root_references.durable) total = UINT64_MAX; /* saturating_add */
    bool activate_root = total > 0 && !root_references.node_active;
    if (activate_root) root_references.node_active = true;
    jz_btree initial;
    jz_btree_init(&initial, &groove__db__NODE_UPDATES_DESC);
    jz_btree_insert(&initial, &node_ref, &metadata, NULL);
    memset(&metadata, 0, sizeof metadata);
    groove_NodeTransitionVec transitions = {0};
    if (activate_root) { groove_NodeTransition t = { node_ref, 1 }; JZ_VEC_PUSH(transitions, t); }
    if (newly_discovered_active_children)
        for (u32 i = 0; i < children.len; i++) { groove_NodeTransition t = { children.ptr[i], 1 }; JZ_VEC_PUSH(transitions, t); }
    if ((e = groove__db__large_value_node_transition_operations(read_storage, initial, transitions, true, &operations)).code) { e = chunk_backend(e); goto done; }
    if (activate_root) {
        jz_bytes value;
        if ((e = groove__db__encode_large_value_root_references(&root_references, &value)).code) { e = chunk_backend(e); goto done; }
        JZ_VEC_PUSH(operations, groove__db__metadata_set(root_key, value));
        memset(&root_key, 0, sizeof root_key);
    }
    /* The recovery marker and all node/root metadata share the same durability
     * boundary. */
    jz_bytes install_key;
    if ((e = groove__db__large_value_pending_install_key(&node_ref, &install_key)).code) { e = chunk_backend(e); goto done; }
    JZ_VEC_PUSH(operations, groove__db__metadata_delete(install_key));
    if (has_install) {
        if (*install.durable) {
            jz_err w = groove__storage__OrderedKvStorage__write_many(groove__storage__LayoutStorage__as_dyn(storage), operations);
            memset(&operations, 0, sizeof operations);
            if (w.code) {
                jz_err recorded = groove__chunks__ChunkError__backend(w.msg ? w.msg->ptr : "", w.msg ? w.msg->len : 0);
                groove__chunks__PublicationInstallFailures__record(&install.install_failures, node_ref, recorded);
                e = chunk_backend(w);
            }
        } else {
            groove__storage__StagedWriteState__extend(install.staged, operations);
            memset(&operations, 0, sizeof operations);
        }
    } else {
        jz_err w = groove__storage__OrderedKvStorage__write_many(groove__storage__LayoutStorage__as_dyn(storage), operations);
        memset(&operations, 0, sizeof operations);
        if (w.code) e = chunk_backend(w);
    }
done:
    groove__db__WriteOpVec__drop(&operations);
    jz_bytes_drop(&root_key);
    jz_vec_drop(&existing_children, sizeof(groove_NodeRef), NULL);
    groove__db__LargeValueNodeReferences__drop(&metadata);
    jz_vec_drop(&children, sizeof(groove_NodeRef), NULL);
    if (have_node) groove__large_values__ChunkNode__drop(&node);
    if (lifecycle) { groove__db__LifecycleMutex__unlock(lifecycle); jz_rc_release(lifecycle); }
    if (has_install) resident_install_drop(&install);
    jz_bytes_drop(&encoded);
    jz_rc_release(storage);
    return e;
}
static bool observer_completes_install_journal(void *selfp) { return true; }
static void observer_drop(void *selfp) { jz_rc_release(selfp); }
static const groove_ChunkInstallObserverVt METADATA_OBSERVER_VT = { observer_installed, observer_completes_install_journal, observer_drop };
groove_DynChunkInstallObserver groove__db__MetadataChunkInstallObserver__new_dyn(groove_LayoutStorage *storage, groove_LifecycleMutex *lifecycle,
                                                                                 const groove_ResidentLifecycleInstall *resident_install) {
    groove_MetadataChunkInstallObserver *o = jz_rc_new(sizeof *o, observer_payload_drop);
    o->storage = storage;
    o->lifecycle = lifecycle;
    o->has_resident_install = resident_install != NULL;
    if (resident_install) resident_install_clone(&o->resident_install, resident_install);
    groove_DynChunkInstallObserver d = { o, &METADATA_OBSERVER_VT };
    return d;
}

/* ======================= publication lifecycle ======================= */
static void persistence_order_drop(void *p) {
    groove_PersistenceOrder *o = p;
    jz_btree_drop(&o->waiters);
    if (o->has_failure) jz_str_drop(&o->failure);
}
groove_PersistenceOrder *groove__db__PersistenceOrder__new(void) {
    groove_PersistenceOrder *o = jz_rc_new(sizeof *o, persistence_order_drop);
    memset(o, 0, sizeof *o);
    jz_btree_init(&o->waiters, &WAITERS_DESC);
    return o;
}
static void wake_all_waiters(groove_PersistenceOrder *order) {
    jz_btree waiters = order->waiters;
    jz_btree_init(&order->waiters, &WAITERS_DESC);
    u64 *k; jz_waker *w;
    JZ_BTREE_FOR(&waiters, k, w) jz_waker_wake(w);
    jz_btree_drop(&waiters);
}
void groove__db__fail_publication_order(groove_PersistenceOrder *order, jz_str message) {
    if (order->has_failure) jz_str_drop(&order->failure);
    order->has_failure = true;
    order->failure = message;
    wake_all_waiters(order);
}
void groove__db__wake_publication_owner(groove_PersistenceOrder *order) {
    if (order->has_owner_waiter) {
        jz_waker w = order->owner_waiter;
        order->has_owner_waiter = false;
        jz_waker_wake(&w);
    }
}

groove_PublicationId groove__db__AppliedBatch__publication(const groove_AppliedBatch *b) { return b->publication; }
/* Physical tables whose resident rows changed in this publication. */
const jz_str *groove__db__AppliedBatch__changed_tables(const groove_AppliedBatch *b, u32 *len) { *len = b->changed_tables.len; return b->changed_tables.ptr; }

typedef struct PersistenceAttempt {
    u8 *lifecycle;
    groove_PersistenceOrder *order;
    groove_PublicationId publication;
    bool *abandoned_application;
    bool write_started;
    bool completed;
} PersistenceAttempt;
static void PersistenceAttempt__drop(PersistenceAttempt *self) {
    if (!self->completed && *self->lifecycle == GROOVE_APPLIED_PERSISTING) {
        if (self->write_started) {
            *self->lifecycle = GROOVE_APPLIED_ABANDONED;
            *self->abandoned_application = true;
            groove__db__fail_publication_order(self->order,
                jz_format("publication PublicationId(%llu) persistence was cancelled after its atomic write started", (unsigned long long)self->publication));
            groove__db__wake_publication_owner(self->order);
        } else {
            *self->lifecycle = GROOVE_APPLIED_APPLIED;
        }
    }
}

static void set_flag_wake(void *ctx) { *(bool *)ctx = true; }

/* poll_fn turn: Ready(Ok) when it is this publication's turn, Ready(Err) on a
 * recorded order failure, otherwise registers a waiter (Pending). */
static bool persistence_turn_poll(const groove_AppliedBatch *self, const jz_waker *cx, jz_err *out) {
    groove_PersistenceOrder *order = self->order;
    if (order->has_failure) {
        *out = groove__storage__Error__backend("publication order", &order->failure);
        return true;
    }
    if (order->next == self->publication) { *out = JZ_OK; return true; }
    jz_btree_insert(&order->waiters, &self->publication, cx, NULL);
    return false;
}

groove_PersistedBatch groove__db__AppliedBatch__persist(const groove_AppliedBatch *self) {
    u8 previous = *self->lifecycle;
    *self->lifecycle = GROOVE_APPLIED_PERSISTING;
    if (previous != GROOVE_APPLIED_APPLIED) jz_panic("an applied batch may have only one persistence attempt at a time");
    if (!self->storage->storage) jz_panic("an applied publication retains storage until settlement");
    groove_LayoutStorage *storage = jz_rc_clone(self->storage->storage);
    PersistenceAttempt attempt = { self->lifecycle, self->order, self->publication, self->abandoned_application, false, false };
    /* await the publication's turn: the synchronous port drains ready tasks
     * while it is not yet this publication's turn. */
    jz_err turn;
    bool woken = false;
    jz_waker cx = { set_flag_wake, &woken };
    while (!persistence_turn_poll(self, &cx, &turn)) {
        woken = false;
        jz_task_run_ready();
        if (!woken) jz_panic("publication persistence turn cannot make progress");
    }
    groove_StagedSnapshot *snapshot = groove__storage__StagedWriteState__snapshot(self->operations);
    groove_WriteOperationVec operations = {0};
    for (u32 b = 0; b < groove__storage__StagedSnapshot__blocks(snapshot); b++) {
        u32 len;
        const groove_OwnedWriteOperation *block = groove__storage__StagedSnapshot__block(snapshot, b, &len);
        for (u32 i = 0; i < len; i++) JZ_VEC_PUSH(operations, groove__storage__OwnedWriteOperation__as_write_operation(&block[i]));
    }
    groove_StorageWriteMetrics storage_writes = groove__db__StorageWriteMetrics__from_operations(operations.ptr, operations.len, self->index_ids);
    u64 storage_start = jz_now_ms();
    u8 outcome;
    jz_err error = JZ_OK;
    if (!turn.code) {
        attempt.write_started = true;
        outcome = groove__storage__OrderedKvStorage__write_many_borrowed_outcome(groove__storage__LayoutStorage__as_dyn(storage), operations, &error);
    } else {
        jz_vec_drop(&operations, sizeof(groove_WriteOperation), NULL);
        outcome = GROOVE_WRITE_MANY_UNCOMMITTED;
        error = turn;
    }
    jz_err result = JZ_OK;
    switch (outcome) {
    case GROOVE_WRITE_MANY_COMMITTED: result = JZ_OK; break;
    case GROOVE_WRITE_MANY_UNCOMMITTED: result = error; break;
    default:
        /* Runtime state already reflects this publication. Without a definite
         * non-commit receipt it cannot be rolled back or retried safely. */
        *self->abandoned_application = true;
        result = error;
        break;
    }
    groove__storage__StagedSnapshot__release(snapshot);
    u64 storage_write_time = jz_now_ms() - storage_start;
    if (!result.code && self->resident_install_durable) *self->resident_install_durable = true;
    *self->lifecycle = GROOVE_APPLIED_PERSISTENCE_COMPLETE;
    attempt.completed = true;
    bool has_waiter = false;
    jz_waker waiter;
    {
        groove_PersistenceOrder *order = self->order;
        if (!result.code) {
            order->next = order->next == UINT64_MAX ? UINT64_MAX : order->next + 1;
            u64 next = order->next;
            has_waiter = jz_btree_remove(&order->waiters, &next, NULL, &waiter);
        } else {
            if (order->has_failure) jz_str_drop(&order->failure);
            order->has_failure = true;
            order->failure = jz_format("%.*s", GROOVE_DB_E(result));
            wake_all_waiters(order);
        }
    }
    if (has_waiter) jz_waker_wake(&waiter);
    if (*self->abandoned_application) groove__db__wake_publication_owner(self->order);
    PersistenceAttempt__drop(&attempt);
    jz_rc_release(storage);
    groove_PersistedBatch persisted;
    memset(&persisted, 0, sizeof persisted);
    persisted.publication = self->publication;
    persisted.result = result;
    persisted.notifications_deferred = self->notifications_deferred;
    persisted.metrics.storage_write_time_ms = storage_write_time;
    persisted.metrics.ivm_tick_time_ms = self->ivm_tick_time_ms;
    persisted.metrics.storage_write_count = storage_writes.total.count;
    persisted.metrics.storage_write_bytes = storage_writes.total.bytes;
    persisted.metrics.storage_writes = storage_writes;
    persisted.metrics.tick = groove__ivm__runtime__TickMetrics__clone(self->tick);
    persisted.receipt.storage = jz_rc_clone(self->storage);
    persisted.receipt.lifecycle = jz_rc_clone(self->lifecycle);
    persisted.receipt.order = jz_rc_clone(self->order);
    persisted.receipt.abandoned_application = jz_rc_clone(self->abandoned_application);
    return persisted;
}

void groove__db__AppliedBatch__drop(groove_AppliedBatch *b) {
    if (*b->lifecycle == GROOVE_APPLIED_APPLIED) {
        *b->lifecycle = GROOVE_APPLIED_ABANDONED;
        *b->abandoned_application = true;
        groove__db__fail_publication_order(b->order,
            jz_format("publication PublicationId(%llu) application was abandoned before persistence", (unsigned long long)b->publication));
        groove__db__wake_publication_owner(b->order);
    }
    jz_rc_release(b->storage);
    jz_rc_release(b->operations);
    if (b->resident_install_durable) jz_rc_release(b->resident_install_durable);
    jz_rc_release(b->order);
    groove__ivm__runtime__TickMetrics__drop(b->tick);
    jz_vec_drop(&b->changed_tables, sizeof(jz_str), str_drop_v);
    jz_rc_release(b->lifecycle);
    jz_rc_release(b->abandoned_application);
    jz_rc_release(b->index_ids);
}

/* Fail closed if a host-owned durable/runtime application is abandoned. */
void groove__db__HostApplicationGuard__complete(groove_HostApplicationGuard guard) {
    guard.completed = true;
    groove__db__HostApplicationGuard__drop(&guard);
}
void groove__db__HostApplicationGuard__drop(groove_HostApplicationGuard *guard) {
    if (!guard->completed) {
        *guard->abandoned_application = true;
        groove__db__wake_publication_owner(guard->order);
    }
    jz_rc_release(guard->order);
    jz_rc_release(guard->abandoned_application);
}

static void layout_storage_cell_take(groove_LayoutStorageCell *cell) {
    if (cell->storage) { jz_rc_release(cell->storage); cell->storage = NULL; }
}
void groove__db__PersistenceReceipt__finish(const groove_PersistenceReceipt *r) {
    *r->lifecycle = GROOVE_APPLIED_FINISHED;
    /* A settled handle may still be retained for its id or metrics, but can
     * never persist again. It must not pin a replaced database runtime. */
    layout_storage_cell_take(r->storage);
}
void groove__db__PersistenceReceipt__drop(groove_PersistenceReceipt *r) {
    if (*r->lifecycle == GROOVE_APPLIED_PERSISTENCE_COMPLETE) {
        *r->lifecycle = GROOVE_APPLIED_ABANDONED;
        *r->abandoned_application = true;
        groove__db__wake_publication_owner(r->order);
    }
    jz_rc_release(r->storage);
    jz_rc_release(r->lifecycle);
    jz_rc_release(r->order);
    jz_rc_release(r->abandoned_application);
}
void groove__db__PersistedBatch__drop(groove_PersistedBatch *b) {
    jz_err_drop(&b->result);
    groove__db__CommitMetrics__drop(&b->metrics);
    groove__db__PersistenceReceipt__drop(&b->receipt);
}

/* ======================= Database (field drops) ======================= */
void groove__db__Database__drop(groove_Database *db) {
    jz_rc_release(db->storage);
    if (db->chunk_storage.vt) groove__chunks__DynChunkStorage__drop(&db->chunk_storage);
    groove__chunks__DynMissingChunkResolver__drop(&db->chunk_resolver);
    if (db->ivm_runtime) groove__ivm__runtime__IvmRuntime__drop(db->ivm_runtime); /* NULL after into_storage */
    if (db->has_last_commit_metrics) groove__db__CommitMetrics__drop(&db->last_commit_metrics);
    if (db->last_tick_metrics) groove__ivm__runtime__TickMetrics__drop(db->last_tick_metrics);
    jz_rc_release(db->storage_read_metrics);
    jz_btree_drop(&db->stored_record_descriptors);
    jz_rc_release(db->immutable_batch_owner);
    jz_rc_release(db->batch_preparation_owner);
    jz_btree_drop(&db->resident_publications);
    jz_btree_drop(&db->persisted_publications);
    jz_rc_release(db->resident_writes);
    jz_rc_release(db->publication_persistence);
    if (db->large_value_publication_lifecycle_guard) {
        groove__db__LifecycleMutex__unlock(db->large_value_publication_lifecycle_guard);
        jz_rc_release(db->large_value_publication_lifecycle_guard);
    }
    jz_rc_release(db->large_value_lifecycle);
    jz_rc_release(db->large_value_lifecycle_held);
    jz_btree_drop(&db->large_value_lifecycle_publications);
    jz_rc_release(db->abandoned_application);
}
