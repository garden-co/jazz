/* _ext_node-core-a.h: mirror declarations of types and functions that the
 * node-core-a unit (node/ingest*, node/physical*, node/codec,
 * node/maintained_subscription_view) uses but that are OWNED BY OTHER UNITS
 * (groove, jazz_types, jazz_model, jazz_protocol, jazz_engine, and the other
 * jazz_node units: node/mod.rs, state/.., query_eval/.., policy, ...).
 * Layouts are approximate mirrors of the Rust fields. */
#ifndef JZ_EXT_NODE_CORE_A_H
#define JZ_EXT_NODE_CORE_A_H
#include "jz_rt.h"

/* ======================= jazz_types ids / time ======================= */
typedef u64 jazz_types_TxTime;
typedef u64 jazz_types_GlobalTime;
typedef jz_uuid jazz_types_NodeUuid;
typedef u64 jazz_types_NodeAlias;
typedef u64 jazz_types_SchemaVersionAlias;
typedef u64 jazz_types_PhysicalTableId;
typedef u64 jazz_types_PhysicalColumnId;
typedef jz_uuid jazz_types_GlobalPhysicalTableId;
typedef jz_uuid jazz_types_GlobalPhysicalColumnId;
typedef jz_uuid jazz_types_GlobalPhysicalEnumVariantId;
typedef jz_uuid jazz_types_SchemaVersionId;
typedef jz_uuid jazz_types_MigrationLensId;
typedef jz_uuid jazz_types_RowUuid;
typedef JZ_VEC(u64) jzn_U64Vec;
typedef struct jazz_types_TxId { jazz_types_TxTime time; jazz_types_NodeUuid node; } jazz_types_TxId;
int jazz_types__ids__TxId__cmp(const void *a, const void *b, void *ctx);
void jazz_types__ids__TxId__debug(jz_str *out, const jazz_types_TxId *t);

/* AuthorSubject: System | SystemAt(Intern<str>) | Authenticated(Intern<str>) */
enum { JAZZ_AUTHOR_SYSTEM = 0, JAZZ_AUTHOR_SYSTEM_AT = 1, JAZZ_AUTHOR_AUTHENTICATED = 2 };
typedef struct jazz_types_AuthorSubject { u32 tag; const jz_str *interned; } jazz_types_AuthorSubject;
typedef struct jazz_types_RowAuthor { u32 tag; const jz_str *interned; } jazz_types_RowAuthor;
int jazz_types__ids__AuthorSubject__cmp(const void *a, const void *b, void *ctx);
bool jazz_types__ids__AuthorSubject__eq(const jazz_types_AuthorSubject *a, const jazz_types_AuthorSubject *b);
jz_err jazz_types__ids__RowAuthor__from_persisted_subject(jazz_types_AuthorSubject s, jazz_types_RowAuthor *out);
jazz_types_AuthorSubject jazz_types__ids__RowAuthor__as_author_subject(jazz_types_RowAuthor a);
u64 jazz_types__time__TxTime__physical_ms(jazz_types_TxTime t);
jz_err jazz_types__time__TxTime__from_physical_ms(u64 ms, jazz_types_TxTime *out);
jz_err jazz_types__time__TxTime__tick_after(jazz_types_TxTime t, jazz_types_TxTime *out);

/* ======================= groove::records ======================= */
typedef struct groove_RecordDescriptorData groove_RecordDescriptorData;
/* RecordDescriptor(Intern<RecordDescriptorData>): a copyable interned handle */
typedef struct groove_RecordDescriptor { const groove_RecordDescriptorData *p; } groove_RecordDescriptor;
typedef struct groove_OwnedRecord { jz_bytes raw; groove_RecordDescriptor descriptor; } groove_OwnedRecord;
typedef struct groove_BorrowedRecord { jz_slice raw; const groove_RecordDescriptor *descriptor; } groove_BorrowedRecord;
typedef struct groove_LargeValueRef groove_LargeValueRef;
typedef struct groove_Value groove_Value;
typedef JZ_VEC(groove_Value) groove_ValueVec;
enum {
    GROOVE_V_U8, GROOVE_V_U16, GROOVE_V_U32, GROOVE_V_U64, GROOVE_V_F64, GROOVE_V_BOOL,
    GROOVE_V_STRING, GROOVE_V_BYTES, GROOVE_V_LARGE, GROOVE_V_UUID, GROOVE_V_ENUM_TAG,
    GROOVE_V_TUPLE, GROOVE_V_ARRAY, GROOVE_V_NULLABLE, GROOVE_V_I64, GROOVE_V_I32,
    GROOVE_V_RECORD, GROOVE_V_ENUM, GROOVE_V_U48
};
typedef struct groove_EnumValue { u32 tag; groove_OwnedRecord record; } groove_EnumValue;
struct groove_Value {
    u32 tag;
    union {
        u64 u; i64 i; f64 f; bool b; u8 enum_tag; jz_str s; jz_bytes bytes; jz_uuid uuid;
        groove_LargeValueRef *large;
        groove_ValueVec vals;      /* Tuple / Array */
        groove_Value *nullable;    /* NULL = None */
        groove_OwnedRecord record;
        groove_EnumValue en;
    } v;
};
void groove_Value_clone(void *dst, const void *src);
void groove_Value_drop(void *v);
bool groove_Value_eq(const groove_Value *a, const groove_Value *b);
int groove_Value_cmp(const void *a, const void *b, void *ctx);
void groove_Value_debug(jz_str *out, const groove_Value *v);
static inline groove_Value groove_value_u64(u64 x) { groove_Value v; v.tag = GROOVE_V_U64; v.v.u = x; return v; }
static inline groove_Value groove_value_null(void) { groove_Value v; v.tag = GROOVE_V_NULLABLE; v.v.nullable = NULL; return v; }
static inline groove_Value groove_value_some(groove_Value inner) {
    groove_Value v; v.tag = GROOVE_V_NULLABLE; v.v.nullable = (groove_Value *)jz_memdup(&inner, sizeof inner); return v;
}

typedef struct groove_ValueType groove_ValueType;
typedef JZ_VEC(groove_ValueType) groove_ValueTypeVec;
typedef struct groove_ScalarEnumSchema { u64 registry_id; jz_str name; JZ_VEC(jz_str) variants; } groove_ScalarEnumSchema;
typedef struct groove_EnumCase { jz_str name; groove_RecordDescriptor payload; } groove_EnumCase;
typedef JZ_VEC(groove_EnumCase) groove_EnumCaseVec;
typedef struct groove_EnumSchema { u64 registry_id; jz_str name; groove_EnumCaseVec cases; } groove_EnumSchema;
enum {
    GROOVE_VT_U8, GROOVE_VT_U16, GROOVE_VT_U32, GROOVE_VT_U64, GROOVE_VT_I32, GROOVE_VT_I64,
    GROOVE_VT_F64, GROOVE_VT_BOOL, GROOVE_VT_STRING, GROOVE_VT_BYTES, GROOVE_VT_INTERNAL,
    GROOVE_VT_UUID, GROOVE_VT_ENUM_TAG, GROOVE_VT_TUPLE, GROOVE_VT_ARRAY, GROOVE_VT_NULLABLE,
    GROOVE_VT_RECORD, GROOVE_VT_ENUM, GROOVE_VT_U48
};
struct groove_ValueType {
    u32 tag;
    union {
        groove_ScalarEnumSchema enum_tag;
        groove_ValueTypeVec tuple;
        groove_ValueType *inner;             /* Box: Array / Nullable */
        groove_RecordDescriptor *record;     /* Box<RecordDescriptor> */
        groove_EnumSchema *enm;              /* Box<EnumSchema> */
        u32 internal;
    } v;
};
void groove_ValueType_clone(void *dst, const void *src);
void groove_ValueType_drop(void *v);
bool groove_ValueType_eq(const groove_ValueType *a, const groove_ValueType *b);
groove_ValueType groove__records__values__ValueType__nullable(groove_ValueType t);
groove_ValueType groove__records__values__ValueType__array_of(groove_ValueType t);
bool groove__records__values__ValueType__is_internal_storage_type(const groove_ValueType *t);
groove_ValueType groove__records__values__ValueType__rebind_variant_registries(groove_ValueType t, const jz_str *path);
static inline groove_ValueType groove_vt_simple(u32 tag) { groove_ValueType t; memset(&t, 0, sizeof t); t.tag = tag; return t; }
static inline groove_ValueType groove_vt_box(u32 tag, groove_ValueType inner) {
    groove_ValueType t; t.tag = tag; t.v.inner = (groove_ValueType *)jz_memdup(&inner, sizeof inner); return t;
}
static inline groove_ValueType groove_vt_record(groove_RecordDescriptor d) {
    groove_ValueType t; t.tag = GROOVE_VT_RECORD; t.v.record = (groove_RecordDescriptor *)jz_memdup(&d, sizeof d); return t;
}

jz_err groove__records__values__ScalarEnumSchema__new(jz_str name, jz_str *variants, u32 n, groove_ScalarEnumSchema *out);
void groove__records__values__ScalarEnumSchema__with_registry_id(groove_ScalarEnumSchema *s, u64 id);
u64 groove__records__values__ScalarEnumSchema__registry_id(const groove_ScalarEnumSchema *s);
void groove_ScalarEnumSchema_drop(void *s);
jz_err groove__records__values__EnumSchema__new(jz_str name, groove_EnumCaseVec cases, groove_EnumSchema *out);
void groove__records__values__EnumSchema__with_registry_id(groove_EnumSchema *s, u64 id);
jz_err groove__records__values__EnumSchema__case(const groove_EnumSchema *s, u32 tag, const groove_EnumCase **out);
void groove_EnumSchema_drop(void *s);
void groove_EnumCase_clone(void *dst, const void *src);
void groove_EnumCase_drop(void *c);
u64 groove__records__values__variant_registry_id_for_path(const jz_str *path);

/* FieldIdentity: Name | Slot(u64) | NamedSlot { name, slot } */
enum { GROOVE_FI_NONE = 0, GROOVE_FI_NAME, GROOVE_FI_SLOT, GROOVE_FI_NAMED_SLOT };
typedef struct groove_FieldIdentity { u32 tag; jz_str name; u64 slot; } groove_FieldIdentity;
typedef struct groove_DescriptorField {
    bool has_name; jz_str name;
    groove_FieldIdentity identity;
    groove_ValueType value_type;
} groove_DescriptorField;
typedef JZ_VEC(groove_DescriptorField) groove_DescriptorFieldVec;
typedef struct groove_NamedValueType { jz_str name; groove_ValueType value_type; } groove_NamedValueType;
typedef JZ_VEC(groove_NamedValueType) groove_NamedValueTypeVec;
void groove_NamedValueType_drop(void *p);
void groove_FieldIdentity_clone(groove_FieldIdentity *dst, const groove_FieldIdentity *src);
void groove_DescriptorField_drop(void *f);
bool groove__records__DescriptorField__logical_name(const groove_DescriptorField *f, jz_slice *out);

/* RecordDescriptor methods (groove::records) */
groove_RecordDescriptor groove__records__RecordDescriptor__new(groove_NamedValueTypeVec fields);
groove_RecordDescriptor groove__records__RecordDescriptor__new_with_fields(groove_DescriptorFieldVec fields);
const groove_DescriptorFieldVec *groove__records__RecordDescriptor__fields(groove_RecordDescriptor d);
bool groove__records__RecordDescriptor__field_index(groove_RecordDescriptor d, jz_slice name, u32 *out);
jz_err groove__records__RecordDescriptor__create(groove_RecordDescriptor d, const groove_Value *vals, u32 n, jz_bytes *out);
jz_err groove__records__RecordDescriptor__field_span(groove_RecordDescriptor d, jz_slice raw, u32 idx, u32 *start, u32 *end);
jz_err groove__records__RecordDescriptor__encode_field_into(groove_RecordDescriptor d, u32 idx, const groove_Value *v, jz_bytes *out);
jz_err groove__records__RecordDescriptor__get_idx(groove_RecordDescriptor d, jz_slice raw, u32 idx, groove_Value *out);
static inline bool groove_RecordDescriptor_eq(groove_RecordDescriptor a, groove_RecordDescriptor b) { return a.p == b.p; }
/* fill callback for create_with_encoded_fields: (ctx, index, output) */
typedef jz_err (*groove_encode_field_fn)(void *ctx, u32 index, jz_bytes *output);
jz_err groove__records__RecordDescriptor__create_with_encoded_fields(groove_RecordDescriptor d, u32 cap, groove_encode_field_fn f, void *ctx, jz_bytes *out);

static inline groove_BorrowedRecord groove_OwnedRecord_borrowed(const groove_OwnedRecord *r) {
    groove_BorrowedRecord b; b.raw = jz_bytes_slice(&r->raw); b.descriptor = &r->descriptor; return b;
}
void groove_OwnedRecord_clone(void *dst, const void *src);
void groove_OwnedRecord_drop(void *r);
bool groove_OwnedRecord_eq(const groove_OwnedRecord *a, const groove_OwnedRecord *b);
jz_err groove__records__OwnedRecord__to_values(const groove_OwnedRecord *r, groove_ValueVec *out);
jz_err groove__records__BorrowedRecord__get_idx(const groove_BorrowedRecord *r, u32 idx, groove_Value *out);
jz_err groove__records__BorrowedRecord__get_u64(const groove_BorrowedRecord *r, u32 idx, u64 *out);
jz_err groove__records__BorrowedRecord__get_u32(const groove_BorrowedRecord *r, u32 idx, u32 *out);
jz_err groove__records__BorrowedRecord__get_u8(const groove_BorrowedRecord *r, u32 idx, u8 *out);
jz_err groove__records__BorrowedRecord__get_bool(const groove_BorrowedRecord *r, u32 idx, bool *out);
jz_err groove__records__BorrowedRecord__get_enum(const groove_BorrowedRecord *r, u32 idx, u8 *out);
jz_err groove__records__BorrowedRecord__get_bytes(const groove_BorrowedRecord *r, u32 idx, jz_slice *out);
jz_err groove__records__BorrowedRecord__get_str(const groove_BorrowedRecord *r, u32 idx, jz_slice *out);
jz_err groove__records__BorrowedRecord__get_uuid(const groove_BorrowedRecord *r, u32 idx, jz_uuid *out);
jz_err groove__records__BorrowedRecord__get_record(const groove_BorrowedRecord *r, u32 idx, groove_BorrowedRecord *out);
jz_err groove__records__BorrowedRecord__get_nullable_u64(const groove_BorrowedRecord *r, u32 idx, bool *some, u64 *out);
jz_err groove__records__BorrowedRecord__get_nullable_enum(const groove_BorrowedRecord *r, u32 idx, bool *some, u8 *out);
jz_err groove__records__BorrowedRecord__get_nullable_string(const groove_BorrowedRecord *r, u32 idx, bool *some, jz_slice *out);
jz_err groove__records__BorrowedRecord__get_nullable_bytes(const groove_BorrowedRecord *r, u32 idx, bool *some, jz_slice *out);
jz_err groove__records__BorrowedRecord__to_values(const groove_BorrowedRecord *r, groove_ValueVec *out);

/* VariantRecord / ValidatedVariantRecord */
typedef struct groove_VariantRecord { u32 variant_tag; groove_OwnedRecord record; } groove_VariantRecord;
typedef struct groove_ValidatedVariantRecord { u32 variant_tag; groove_OwnedRecord record; } groove_ValidatedVariantRecord;
jz_err groove__records__ValidatedVariantRecord__create_with_encoded_fields(u32 tag, groove_RecordDescriptor d, u32 cap,
    groove_encode_field_fn f, void *ctx, groove_ValidatedVariantRecord *out);

/* groove::records::Error codes (subset used here) */
enum {
    GROOVE_REC_ERR_NON_CANONICAL = 0x6101, GROOVE_REC_ERR_INVALID_ENUM_DISCRIMINANT,
    GROOVE_REC_ERR_UNKNOWN_ENUM_TAG, GROOVE_REC_ERR_TYPE_MISMATCH
};

/* groove::Intern<String> */
typedef const jz_str *groove_InternStr;
groove_InternStr groove__Intern__new_str(jz_str s);

/* groove::ivm::RecursiveEnumRemaps */
typedef struct groove_RecursiveEnumRemaps {
    jz_btree scalar;           /* String -> Vec<Option<u8>>  (opt encoded as u16: 0x100 = None) */
    jz_btree payload;          /* String -> Vec<Option<u32>> (opt encoded as i64: -1 = None) */
    jz_btree payload_children; /* String -> Vec<Option<String>> (jz_str, ptr NULL = None) */
} groove_RecursiveEnumRemaps;
typedef JZ_VEC(u16) groove_OptU8Vec;
typedef JZ_VEC(i64) groove_OptU32Vec;
typedef JZ_VEC(jz_str) groove_OptStrVec;
#define GROOVE_OPT_U8_NONE ((u16)0x100)
void groove_RecursiveEnumRemaps_init(groove_RecursiveEnumRemaps *r);
void groove_RecursiveEnumRemaps_drop(void *r);
void groove_RecursiveEnumRemaps_clone(void *dst, const void *src);

/* groove::ivm::LiteralValue / StaticScanSpec / ProjectField */
typedef struct groove_LiteralValue { u32 tag; groove_Value value; } groove_LiteralValue;
typedef JZ_VEC(groove_LiteralValue) groove_LiteralValueVec;
groove_LiteralValue groove__ivm__op_types__LiteralValue__from_value(groove_Value v);
void groove_LiteralValue_clone(void *dst, const void *src);
void groove_LiteralValue_drop(void *v);
enum { GROOVE_SCAN_POINT, GROOVE_SCAN_PREFIX, GROOVE_SCAN_PREFIX_LIMIT, GROOVE_SCAN_REVERSE_PREFIX_LIMIT, GROOVE_SCAN_RANGE };
typedef struct groove_StaticScanSpec {
    u32 tag;
    groove_LiteralValueVec a;  /* Point/Prefix values, PrefixLimit prefix, Range start */
    groove_LiteralValueVec b;  /* Range end */
    u32 max_items;
} groove_StaticScanSpec;
void groove_StaticScanSpec_drop(void *s);
typedef struct groove_ProjectField groove_ProjectField; /* opaque, by-pointer */
typedef JZ_VEC(groove_ProjectField *) groove_ProjectFieldVec;
groove_ProjectField *groove__ivm__op_types__ProjectField__named(jz_str name);
groove_ProjectField *groove__ivm__op_types__ProjectField__renamed(jz_str source, jz_str output);
groove_ProjectField *groove__ivm__op_types__ProjectField__literal_typed(jz_str output, groove_Value v, groove_ValueType t);
groove_ProjectField *groove__ivm__op_types__ProjectField__recursive_enum_remap(jz_str source, jz_str output, groove_ValueType t, groove_RecursiveEnumRemaps remaps);
groove_ProjectField *groove__ivm__op_types__ProjectField__recursive_enum_remap_omitting_unrepresentable(jz_str source, jz_str output, groove_ValueType t, groove_RecursiveEnumRemaps remaps);
groove_ProjectField *groove_ProjectField_clone(const groove_ProjectField *f);
void groove_ProjectField_drop(groove_ProjectField *f);
void groove_ProjectFieldVec_drop(groove_ProjectFieldVec *v);
void groove_ProjectFieldVec_clone(groove_ProjectFieldVec *dst, const groove_ProjectFieldVec *src);

/* groove::db::GraphBuilder (opaque) */
typedef struct groove_GraphBuilder groove_GraphBuilder;
groove_GraphBuilder *groove__db__query__GraphBuilder__variant_source_scan(jz_str table, jz_str target, groove_StaticScanSpec scan);
groove_GraphBuilder *groove__db__query__GraphBuilder__variant_source(jz_str table, jz_str target);
groove_GraphBuilder *groove__db__query__GraphBuilder__variant_index_scan(jz_str table, jz_str index, jz_str target, groove_StaticScanSpec scan);

/* groove::schema */
typedef struct groove_ColumnSchema { jz_str name; groove_ValueType column_type; } groove_ColumnSchema;
typedef JZ_VEC(groove_ColumnSchema) groove_ColumnSchemaVec;
typedef struct groove_IndexSchema { jz_str name; JZ_VEC(jz_str) columns; bool unique; } groove_IndexSchema;
typedef JZ_VEC(groove_IndexSchema) groove_IndexSchemaVec;
typedef struct groove_TableVariantField { jz_str name; groove_ValueType value_type; bool has_shared; jz_str shared_column; } groove_TableVariantField;
typedef JZ_VEC(groove_TableVariantField) groove_TableVariantFieldVec;
typedef struct groove_TableVariant { u32 tag; JZ_VEC(jz_str) fields; groove_TableVariantFieldVec payload_fields; } groove_TableVariant;
typedef struct groove_PrimaryKey groove_PrimaryKey;
typedef struct groove_TableSchema {
    jz_str name;
    u64 variant_registry_id;
    jz_btree value_variant_registries;
    groove_ColumnSchemaVec columns;
    groove_PrimaryKey *primary_key; /* Option<PrimaryKey> */
    groove_IndexSchemaVec indices;
    JZ_VEC(groove_TableVariant) variants;
} groove_TableSchema;
typedef JZ_VEC(groove_TableSchema) groove_TableSchemaVec;
void groove_ColumnSchema_clone(void *dst, const void *src);
void groove_ColumnSchema_drop(void *c);
void groove_TableSchema_clone(void *dst, const void *src);
void groove_TableSchema_drop(void *t);
void groove_IndexSchema_clone(void *dst, const void *src);
void groove_IndexSchema_drop(void *i);
groove_PrimaryKey *groove_PrimaryKey_clone(const groove_PrimaryKey *p);
void groove__schema__TableSchema__new_with_bound_registries(groove_TableSchema *out, jz_str name, groove_ColumnSchemaVec columns);
void groove__schema__TableSchema__with_index(groove_TableSchema *t, groove_IndexSchema index);
void groove__schema__TableSchema__with_variant_payload(groove_TableSchema *t, u32 tag, groove_TableVariantFieldVec fields);
const groove_TableVariant *groove__schema__TableSchema__variant(const groove_TableSchema *t, u32 tag);
groove_RecordDescriptor groove__schema__TableSchema__record_schema(const groove_TableSchema *t);
groove_IndexSchema groove__schema__IndexSchema__new(jz_str name, jz_str *cols, u32 n);
groove_TableVariantField groove__schema__TableVariantField__shared(jz_str name, groove_ValueType t, jz_str column);
groove_ValueType groove__large_values__physical_storage_value_type(u32 kind);
enum { GROOVE_LARGE_VALUE_KIND_JSON = 2 };

/* groove::db::Database (schema admission + storage) */
typedef struct groove_Database groove_Database;
typedef struct groove_RuntimeRegistryCheckpoint groove_RuntimeRegistryCheckpoint;
enum { GROOVE_DB_ERR_TABLE_NOT_FOUND = 0x6201 };
jz_err groove__db__schema_admission__Database__table_schema(const groove_Database *db, const jz_str *table, const groove_TableSchema **out);
groove_RuntimeRegistryCheckpoint *groove__db__schema_admission__Database__runtime_registry_checkpoint(const groove_Database *db);
void groove__db__schema_admission__Database__restore_runtime_registry(groove_Database *db, groove_RuntimeRegistryCheckpoint *c);
void groove_RuntimeRegistryCheckpoint_drop(groove_RuntimeRegistryCheckpoint *c);
jz_err groove__db__schema_admission__Database__register_table(groove_Database *db, groove_TableSchema table);
jz_err groove__db__schema_admission__Database__register_table_variant_with_columns(groove_Database *db, const jz_str *table, groove_ColumnSchemaVec cols, groove_TableVariant variant);
jz_err groove__db__schema_admission__Database__evolve_table_variant_registries(groove_Database *db, const jz_str *table, const groove_ColumnSchemaVec *cols);
jz_err groove__db__schema_admission__Database__register_table_index(groove_Database *db, const jz_str *table, groove_IndexSchema index);
jz_err groove__db__schema_admission__Database__define_variant_projection(groove_Database *db, const jz_str *table, const jz_str *target, groove_RecordDescriptor output);
jz_err groove__db__schema_admission__Database__register_variant_case(groove_Database *db, const jz_str *table, const jz_str *target, u32 tag, groove_ProjectFieldVec fields);
jz_err groove__db__schema_admission__Database__register_variant_ignore_case(groove_Database *db, const jz_str *table, const jz_str *target, u32 tag);
jz_err groove__db__schema_admission__Database__register_variant_projection_case_omitting_unrepresentable_enums(groove_Database *db, const jz_str *table, const jz_str *target, u32 tag, groove_ProjectFieldVec fields);
jz_err groove__db__schema_admission__Database__refresh_variant_case_for_registry_evolution(groove_Database *db, const jz_str *table, const jz_str *target, u32 tag, groove_ProjectFieldVec fields);

/* ======================= jazz_model::schema ======================= */
enum { JAZZ_LARGE_KIND_NONE = 0, JAZZ_LARGE_KIND_TEXT, JAZZ_LARGE_KIND_BYTES, JAZZ_LARGE_KIND_JSON };
typedef struct jazz_model_ColumnSchema {
    jz_str name;
    groove_ValueType column_type;
    u32 large_value_kind;
    groove_Value *default_; /* Option<Value> */
} jazz_model_ColumnSchema;
typedef JZ_VEC(jazz_model_ColumnSchema) jazz_model_ColumnSchemaVec;
typedef struct jazz_model_TableSchema {
    jz_str name;
    jazz_model_ColumnSchemaVec columns;
    JZ_VEC(jz_str) branch_by;
    jz_btree references;
    void *read_policy;
    void *write_policies;
    jz_btree indexed_columns;   /* BTreeSet<String> */
    jz_btree composite_indexes; /* BTreeSet<Vec<String>> */
    jz_btree merge_strategies;
} jazz_model_TableSchema;
typedef JZ_VEC(jazz_model_TableSchema) jazz_model_TableSchemaVec;
typedef struct jazz_model_JazzSchema { void *public_schema; jazz_model_TableSchemaVec tables; } jazz_model_JazzSchema;
/* jazz_protocol::SchemaVersion { id, schema } (kept under the jazz_model prefix used by the physical code) */
typedef struct jazz_model_SchemaVersion { jz_uuid id; jazz_model_JazzSchema schema; } jazz_model_SchemaVersion;
void jazz_model_JazzSchema_drop(void *s);
void jazz_model_SchemaVersion_clone(void *dst, const void *src);
void jazz_model_SchemaVersion_drop(void *s);
jz_uuid jazz_model__schema__JazzSchema__version_id(const jazz_model_JazzSchema *s);
/* serde_json::to_vec(schema.public_schema()) */
jz_err jazz_model__schema__JazzSchema__public_schema_json(const jazz_model_JazzSchema *s, jz_bytes *out);
jz_err jazz_model__model__public_schema_convert__decode_public_schema_json(jz_slice json, jazz_model_JazzSchema *out);
typedef JZ_VEC(jz_str) jzn_StrVec;
void jazz_model_TableSchema_clone(void *dst, const void *src);
void jazz_model_TableSchema_drop(void *t);
/* lowered storage tables of one Jazz table (return owned groove tables) */
groove_TableSchema jazz_model__schema__TableSchema__history_storage_table(const jazz_model_TableSchema *t);
groove_TableSchema jazz_model__schema__TableSchema__global_current_storage_table(const jazz_model_TableSchema *t);
groove_TableSchema jazz_model__schema__TableSchema__ahead_current_storage_table(const jazz_model_TableSchema *t);
groove_TableSchema jazz_model__schema__TableSchema__rejected_versions_storage_table(const jazz_model_TableSchema *t);
void jazz_model__schema__TableSchema__global_current_indexed_columns(const jazz_model_TableSchema *t, JZ_VEC(jz_str) *out);
#define JAZZ_SCHEMA_GLOBAL_CURRENT_BY_SEQ_INDEX "by_branch_global_time"
#define JAZZ_SCHEMA_COUNTER_SIGNS_FIELD "counter_signs"
#define JAZZ_SCHEMA_SEQ_FIELD "seq"
#define JAZZ_SCHEMA_BASE_SEQ_FIELD "base_seq"
#define JAZZ_SCHEMA_BASE_PENDING_FIELD "base_pending"

/* BranchKey */
typedef struct jazz_model_BranchKey { JZ_VEC(void) values; } jazz_model_BranchKey;
void jazz_model__model__branch__BranchKey__canonical_bytes(const jazz_model_BranchKey *k, jz_bytes *out);
void jazz_model_BranchKey_default(jazz_model_BranchKey *k);
void jazz_model_BranchKey_drop(void *k);

/* compiled migration lens path */
enum { JAZZ_LENS_OP_RENAME, JAZZ_LENS_OP_COPY, JAZZ_LENS_OP_ADD, JAZZ_LENS_OP_DROP };
typedef struct jazz_node_CompiledLensOp { u32 tag; jz_str a; jz_str b; groove_Value default_; } jazz_node_CompiledLensOp;
typedef struct jazz_node_CompiledLensPath { jz_str target_table; JZ_VEC(jazz_node_CompiledLensOp) ops; } jazz_node_CompiledLensPath;
void jazz_node_CompiledLensPath_drop(void *p);

/* ======================= jazz_protocol ======================= */
typedef struct jazz_protocol_PhysicalColumnIdentity {
    jazz_types_GlobalPhysicalColumnId id;
    jz_btree enum_variants; /* String -> Vec<GlobalPhysicalEnumVariantId> */
} jazz_protocol_PhysicalColumnIdentity;
typedef struct jazz_protocol_PhysicalTableIdentity {
    jazz_types_GlobalPhysicalTableId id;
    jz_btree columns; /* String -> PhysicalColumnIdentity */
} jazz_protocol_PhysicalTableIdentity;
typedef struct jazz_protocol_PhysicalIdentityManifest { jz_btree tables; /* String -> PhysicalTableIdentity */ } jazz_protocol_PhysicalIdentityManifest;
void jazz_protocol_PhysicalIdentityManifest_clone(void *dst, const void *src);
void jazz_protocol_PhysicalIdentityManifest_drop(void *m);
bool jazz_protocol_PhysicalIdentityManifest_eq(const jazz_protocol_PhysicalIdentityManifest *a, const jazz_protocol_PhysicalIdentityManifest *b);
/* catalogue publication types */
typedef struct jazz_protocol_MigrationLens { jz_uuid id; jz_uuid source; jz_uuid target; jz_vec table_lenses; } jazz_protocol_MigrationLens;
void jazz_protocol_MigrationLens_clone(void *dst, const void *src);
void jazz_protocol_MigrationLens_drop(void *l);
typedef struct jazz_protocol_SchemaPredecessor { jazz_protocol_MigrationLens lens; jzn_StrVec new_tables; jzn_StrVec dropped_tables; } jazz_protocol_SchemaPredecessor;
typedef JZ_VEC(jazz_protocol_SchemaPredecessor) jazz_protocol_SchemaPredecessorVec;
typedef struct jazz_protocol_SchemaLineagePublication {
    jz_uuid id;
    jazz_model_SchemaVersion schema;
    jazz_protocol_SchemaPredecessorVec predecessors;
    jazz_protocol_PhysicalIdentityManifest physical_identities;
} jazz_protocol_SchemaLineagePublication;
void jazz_protocol_SchemaLineagePublication_drop(void *p);
bool jazz_protocol_SchemaLineagePublication_eq(const jazz_protocol_SchemaLineagePublication *a, const jazz_protocol_SchemaLineagePublication *b);
jz_uuid jazz_protocol__protocol__SchemaLineagePublication__content_id(const jazz_protocol_SchemaLineagePublication *p);
typedef struct jazz_protocol_CurrentWriteSchema { u64 revision; jz_uuid schema; } jazz_protocol_CurrentWriteSchema;
jz_err jazz_protocol__protocol__canonical_catalogue_schema_bytes(const jazz_model_SchemaVersion *s, jz_bytes *out);
void jazz_protocol__protocol__canonical_lens_bytes(const jazz_protocol_MigrationLens *l, jz_bytes *out);
jz_err jazz_protocol__protocol__decode_canonical_lens_bytes(jz_slice bytes, jazz_protocol_MigrationLens *out);
#define JAZZ_PROTOCOL_CATALOGUE_SCHEMA_VERSION 1
void jazz_protocol__protocol__SchemaVersion__new(const jazz_model_JazzSchema *schema /* cloned */, jazz_model_SchemaVersion *out);
/* Result<(), &'static str>: NULL = Ok */
const char *jazz_protocol__protocol__PhysicalIdentityManifest__validate_for_schema(const jazz_protocol_PhysicalIdentityManifest *m, const jazz_model_JazzSchema *s);
extern const jz_btree_desc jazz_protocol_PhysicalTableIdentity_map_desc;
extern const jz_btree_desc jazz_protocol_PhysicalColumnIdentity_map_desc;
extern const jz_btree_desc jazz_protocol_enum_variants_map_desc;

/* ======================= jazz_node::node (mod.rs, state/..) ======================= */
/* node::Error discriminants */
enum {
    JZN_E_GROOVE = 0x7001, JZN_E_CHUNK_STORAGE, JZN_E_LARGE_VALUE_UPLOAD_CHUNK_UNAVAILABLE, JZN_E_LARGE_VALUE,
    JZN_E_LARGE_VALUE_REACHABILITY, JZN_E_LARGE_VALUE_INGRESS_RATE_LIMITED, JZN_E_LARGE_VALUE_STAGE_EXPIRED,
    JZN_E_RECORD, JZN_E_MALFORMED_CURRENT_ROW, JZN_E_STORAGE, JZN_E_CLOCK_OVERFLOW, JZN_E_QUERY,
    JZN_E_QUERY_LOWERING, JZN_E_QUERY_CAPABILITY, JZN_E_AUTH_SUPPORT_MISSING_CLAIM, JZN_E_POLICY_PROOF_CYCLE,
    JZN_E_TABLE_NOT_FOUND, JZN_E_UNSUPPORTED_COLUMN_TYPE, JZN_E_INVALID_MERGEABLE_COMMIT,
    JZN_E_UNADMITTED_WRITE_AUTHOR, JZN_E_INVALID_BRANCH_KEY, JZN_E_UNSUPPORTED_EXCLUSIVE_READ,
    JZN_E_TRANSACTION_CONFLICT, JZN_E_INVALID_STORED_VALUE, JZN_E_INVALID_AUTHORITY_SOURCE_CLOSURE,
    JZN_E_MISSING_TRANSACTION, JZN_E_MALFORMED_VIEW_UPDATE, JZN_E_MAINTAINED_VIEW_MISSING_BUNDLE_WITNESS,
    JZN_E_MISSING_OPEN_BATCH, JZN_E_DUPLICATE_OPEN_BATCH, JZN_E_OPEN_TX_IDENTITY_MISMATCH,
    JZN_E_NON_MONOTONE_STATE, JZN_E_CONFLICTING_COMMIT_UNIT, JZN_E_CONFLICTING_FATE,
    JZN_E_UNSUPPORTED_COMMIT_UNIT, JZN_E_UNSUPPORTED_SYNC_MESSAGE, JZN_E_UNAUTHORIZED_CATALOGUE_UPDATE,
    JZN_E_INVALID_CATALOGUE_UPDATE, JZN_E_CATALOGUE_ACTIVATION_FAILED, JZN_E_CATALOGUE_UNINITIALIZED,
    JZN_E_CATALOGUE_CODEC, JZN_E_HISTORICAL_READ_REQUIRES_SERVER, JZN_E_AUTHORIZATION_DENIED,
    JZN_E_SUBSCRIPTION_CLOSED
};
#define JZN_ISV(msg) jz_err_new(JZN_E_INVALID_STORED_VALUE, (msg))
static inline jz_err jzn_table_not_found(const char *p, u32 n) { return jz_err_fmt(JZN_E_TABLE_NOT_FOUND, "table not found: %.*s", (int)n, p); }

typedef struct jazz_node_SchemaPhysicalMapping jazz_node_SchemaPhysicalMapping;
typedef struct jazz_node_PreparedPhysicalWritePlan jazz_node_PreparedPhysicalWritePlan;

typedef struct jazz_node_ActiveSchema { jazz_types_SchemaVersionId schema; u64 revision; jazz_model_JazzSchema compiled; } jazz_node_ActiveSchema;
void jazz_node_ActiveSchema_clone(jazz_node_ActiveSchema *dst, const jazz_node_ActiveSchema *src);
void jazz_node_ActiveSchema_drop(jazz_node_ActiveSchema *a);

typedef struct jazz_node_SchemaCatalogue {
    jazz_types_SchemaVersionId local_schema_version_id;
    bool has_local_schema_version_alias; jazz_types_SchemaVersionAlias local_schema_version_alias;
    jazz_model_JazzSchema schema;
    jz_btree schema_version_aliases;   /* SchemaVersionId -> SchemaVersionAlias */
    jz_btree catalogue_schemas;        /* SchemaVersionId -> SchemaVersion */
    jz_btree catalogue_lenses;         /* MigrationLensId -> MigrationLens */
    jz_btree physical_mappings;        /* SchemaVersionId -> SchemaPhysicalMapping */
    jz_btree physical_current_winner_projections; /* SchemaVersionId -> BTreeMap<String,(String,Vec<String>)> */
    jz_btree staged_lineages;
    jz_btree pending_lineages;
    jz_btree active_lineages_by_target;
    u64 active_catalogue_seq;
    u64 next_physical_table_id;
    u64 next_physical_column_id;
    jz_btree lens_path_cache;
    jz_btree compiled_lens_cache;
    jz_btree physical_write_plan_cache; /* SchemaVersionId -> String -> PhysicalWriteTarget -> Arc<Plan> */
    jazz_node_ActiveSchema active_schema;
} jazz_node_SchemaCatalogue;

typedef struct jazz_node_DatabaseSlot { groove_Database *db; } jazz_node_DatabaseSlot;

/* NodeState sub-states (node/mod.rs, mirrored) */
typedef struct jazz_node_Clock {
    jazz_types_TxTime tx_time;
    jazz_types_TxTime *reservation_high_water;    /* Rc<Cell<TxTime>> */
    jazz_types_GlobalTime global_time_register;
    jz_btree locally_minted_global_times;          /* set<GlobalTime> */
    jazz_types_GlobalTime committed_global_time;
    jz_btree applied_global_times_after_frontier;  /* set<GlobalTime> */
    jz_btree frontier_dots;                        /* GlobalTime -> Vec<TxId> */
} jazz_node_Clock;
void jazz_node_Clock_clone(jazz_node_Clock *dst, const jazz_node_Clock *src);
void jazz_node_Clock_drop(jazz_node_Clock *c);
bool jazz_node_u64_set_eq(const jz_btree *a, const jz_btree *b);
jz_err jazz_node__node__Clock__allocate_global_time(jazz_node_Clock *c, u64 physical_ms, jazz_types_GlobalTime *out);

typedef struct jazz_node_Parking {
    jz_btree parked_shape_registrations;    /* ShapeId -> ShapeAst */
    jz_btree parked_binding_deltas;         /* ShapeId -> Vec<ParkedSubscription> */
    jz_btree parked_commit_units;           /* TxId -> ParkedCommitUnit */
    jz_btree parked_catalogue_commit_units; /* set<TxId> */
    u64 newest_ingest_ms;
} jazz_node_Parking;

typedef struct jazz_node_QueryServing {
    jz_btree query_shape_cache;
    jz_btree read_policy_authorization_request_cache;
    jz_btree compiled_query_program_cache;
    jz_btree query_program_templates;
    jz_deque supported_query_program_requests;
    jz_btree policy_authorization_graph_cache;
    jz_btree policy_authorization_graph_replacements;
    jz_btree tx_versions_cache;
    jz_btree version_storage_sources_cache;
    jz_btree registered_shapes;
    jz_btree authority_results;
} jazz_node_QueryServing;

typedef struct jazz_node_OpenTxState {
    jz_btree open_transactions;
    jz_btree closed_batches;
    jz_btree local_permission_subjects;   /* TxId -> AuthorSubject */
    jz_vec released_large_values;
    jz_btree pending_foreign_transactions; /* set<TxId> */
} jazz_node_OpenTxState;
typedef struct jazz_node_RejectionTracking { jz_btree rejected_transactions; /* TxId -> RejectedTransaction */ } jazz_node_RejectionTracking;
typedef struct jazz_node_AheadCurrentKeys { jz_hmap keys; jz_hmap rows_per_branch; } jazz_node_AheadCurrentKeys;
typedef struct jazz_node_LargeValueIngressState { u64 window_started_ms, admitted_bytes; } jazz_node_LargeValueIngressState;
typedef struct jazz_node_SyncMetrics {
    u64 parked_orphans, parked_orphans_resolved, parked_catalogue_orphans, parked_catalogue_orphans_resolved,
        dropped_malformed_relay_commit_units, parked_catalogue_shapes, parked_catalogue_shapes_resolved,
        receiver_bulk_bundle_ingests, receiver_per_bundle_ingests, receiver_bulk_ingest_commits,
        receiver_history_table_probes, receiver_history_rebuild_rows_avoided,
        authoritative_reset_missing_payload_fallbacks, peer_payload_inventory_missing_fallbacks;
} jazz_node_SyncMetrics;
struct jazz_node_NodeAliases { jz_btree by_node; jz_hmap by_alias; u64 max_alias; u64 retargets; };

typedef struct jazz_node_NodeState {
    jazz_types_NodeUuid node_uuid;
    bool has_self_node_alias; jazz_types_NodeAlias self_node_alias;
    jazz_node_SchemaCatalogue catalogue;
    u32 catalogue_bootstrap_state;
    bool catalogue_bootstrap_marker;
    jazz_node_Clock clock;
    jazz_node_Parking parking;
    jazz_node_QueryServing query;
    jazz_node_OpenTxState open_tx;
    jazz_node_RejectionTracking rejections;
    jazz_node_DatabaseSlot database;
    jazz_node_LargeValueIngressState large_value_ingress;
    u64 groove_runtime_token;
    u64 physical_identity_generation;
    bool history_complete;
    u32 authored_commit_durability;
    jz_btree pending_persistence;           /* set<TxId> */
    struct jazz_node_NodeAliases node_aliases;
    jazz_node_AheadCurrentKeys ahead_current_keys;
    jz_btree tx_touched_dirty;
    jz_btree history_tx_authors;
    jz_hmap history_tx_seqs;
    bool minting_global_time;
    jazz_node_SyncMetrics sync_metrics;
    jz_btree session_claims;
    bool permissions_ready;
    bool catalogue_activation_failed;
} jazz_node_NodeState;
static inline groove_Database *jzn_db(jazz_node_NodeState *s) { return s->database.db; }

/* mod.rs free fns */
jz_str jazz_node__node__global_case_path(const jz_str *path, const void /*GlobalEnumCaseId*/ *c);
jz_err jazz_node__node__hydrate_nested_scalar_enum_cases(const groove_ValueType *t, const jz_btree *enum_variants,
    jazz_types_SchemaVersionId schema, const char *identity_path, const char *path, jz_btree *out);
jz_err jazz_node__node__hydrate_nested_payload_enum_cases(const groove_ValueType *t, const jz_btree *enum_variants,
    jazz_types_SchemaVersionId schema, const char *identity_path, const char *path, jz_btree *out);
jz_str jazz_node__node__query_engine__user_column_field(const jz_str *name);

/* state/.. NodeState methods */
jz_err jazz_node__node__state__read_payload__NodeState__table_in_schema(jazz_node_NodeState *s, const jz_str *table, jazz_types_SchemaVersionId v, jazz_model_TableSchema *out);
jz_err jazz_node__node__state__read_payload__NodeState__table_in_schema_ref(jazz_node_NodeState *s, const jz_str *table, jazz_types_SchemaVersionId v, const jazz_model_TableSchema **out);
jz_err jazz_node__node__state__read_payload__NodeState__compiled_lens_path(jazz_node_NodeState *s, jazz_types_SchemaVersionId from,
    jazz_types_SchemaVersionId to, const jz_str *table, bool *some, jazz_node_CompiledLensPath *out);
bool jazz_node__node__state__catalogue__NodeState__schema_version_for_alias(const jazz_node_NodeState *s, jazz_types_SchemaVersionAlias a, jazz_types_SchemaVersionId *out);
typedef struct jazz_node_VersionRow jazz_node_VersionRow;
typedef JZ_VEC(jazz_node_VersionRow) jazz_node_VersionRowVec;
void jazz_node__node__state__commit__NodeState__mark_tx_touched_row(jazz_node_NodeState *s, jazz_types_PhysicalTableId t, const jazz_node_VersionRow *v);

/* shared btree descs over common key kinds (owned by another unit) */
int jazz_cmp_uuid_v(const void *a, const void *b, void *ctx);


/* ======================= jazz_model::tx / jazz_protocol rows (mirrors) ======================= */
enum { JAZZ_TX_KIND_MERGEABLE = 0, JAZZ_TX_KIND_EXCLUSIVE = 1 };
enum { JAZZ_DURABILITY_NONE = 0, JAZZ_DURABILITY_LOCAL = 1, JAZZ_DURABILITY_GLOBAL = 2 };
enum { JAZZ_MERGE_ASPECT_CONTENT = 0, JAZZ_MERGE_ASPECT_DELETION = 1 };
/* RejectionReason */
enum {
    JAZZ_REJ_CLIENT_CLOCK_TOO_FAR_AHEAD, JAZZ_REJ_AUTHORIZATION_DENIED, JAZZ_REJ_EXCLUSIVE_CONFLICT,
    JAZZ_REJ_CAUSALITY_VIOLATION, JAZZ_REJ_CASCADE, JAZZ_REJ_MALFORMED_COMMIT
};
typedef struct jazz_model_RejectionReason { u32 tag; jazz_types_TxId root; jz_str message; } jazz_model_RejectionReason;
void jazz_model_RejectionReason_clone(void *dst, const void *src);
void jazz_model_RejectionReason_drop(void *r);
bool jazz_model_RejectionReason_eq(const jazz_model_RejectionReason *a, const jazz_model_RejectionReason *b);
jz_err jazz_model__tx__RejectionReason__to_storage_value(const jazz_model_RejectionReason *r, groove_Value *out);
jz_err jazz_model__tx__RejectionReason__from_storage_record(groove_EnumValue v, jazz_model_RejectionReason *out);
/* Fate: Pending | Accepted | Rejected(reason) */
enum { JAZZ_FATE_PENDING = 0, JAZZ_FATE_ACCEPTED = 1, JAZZ_FATE_REJECTED = 2 };
typedef struct jazz_model_Fate { u32 tag; jazz_model_RejectionReason reason; } jazz_model_Fate;
void jazz_model_Fate_clone(void *dst, const void *src);
void jazz_model_Fate_drop(void *f);

typedef struct jazz_model_Snapshot jazz_model_Snapshot;
typedef struct jazz_model_ReadSet jazz_model_ReadSet;
typedef struct jazz_model_ContributionMergeProvenance jazz_model_ContributionMergeProvenance;
typedef struct jazz_model_Transaction {
    jazz_types_TxId tx_id;
    u32 kind;
    u32 n_total_writes;
    jazz_types_AuthorSubject made_by;
    bool has_permission_subject; jazz_types_AuthorSubject permission_subject;
    jazz_model_Snapshot *base_snapshot;       /* Option */
    jazz_model_ReadSet *row_read_set;         /* Option<Vec<RowRead>> */
    jazz_model_ReadSet *absent_read_set;      /* Option<Vec<AbsentRead>> */
    jazz_model_ReadSet *predicate_read_set;   /* Option<Vec<PredicateRead>> */
    jz_str *user_metadata_json;               /* Option<String> */
    jazz_model_ContributionMergeProvenance *contribution_merge; /* Option */
} jazz_model_Transaction;
void jazz_model_Transaction_clone(void *dst, const void *src);
void jazz_model_Transaction_drop(void *t);
bool jazz_model_Transaction_eq(const jazz_model_Transaction *a, const jazz_model_Transaction *b);
void jazz_model_Snapshot_drop(jazz_model_Snapshot *s);
void jazz_model_ReadSet_drop(jazz_model_ReadSet *s);

typedef struct jazz_model_TransactionRecord {
    jazz_types_TxId tx_id;
    jazz_types_AuthorSubject made_by;
    u32 kind;
    u32 n_total_writes;
    jazz_model_Fate fate;
    bool has_global_time; jazz_types_GlobalTime global_time;
    u32 durability;
    jz_str *user_metadata_json;
    jazz_model_ContributionMergeProvenance *contribution_merge;
} jazz_model_TransactionRecord;
typedef struct jazz_model_HistoryEntry jazz_model_HistoryEntry;
jazz_model_HistoryEntry *jazz_model__tx__HistoryEntry__new(jz_str table, groove_OwnedRecord version, jazz_model_TransactionRecord tx,
    bool is_locally_current, bool is_globally_current);

/* contribution provenance (jazz_model::tx) */
typedef struct jazz_model_ContributionComponent { u32 tag; jz_str column; jz_bytes identity; } jazz_model_ContributionComponent;
enum { JAZZ_CC_COLUMN = 0, JAZZ_CC_OPERATION = 1, JAZZ_CC_REGISTER = 2 };
typedef struct jazz_model_ContributionCoordinate {
    jazz_model_BranchKey branch_key; jz_str table; jazz_types_RowUuid row_uuid; u32 layer; jazz_model_ContributionComponent component;
} jazz_model_ContributionCoordinate;
typedef struct jazz_model_ContributionDot { jazz_types_TxId tx_id; jazz_model_ContributionCoordinate coordinate; } jazz_model_ContributionDot;
typedef JZ_VEC(jazz_model_ContributionDot) jazz_model_ContributionDotVec;
typedef struct jazz_model_ContributionSubstitution { jazz_model_ContributionCoordinate target; jazz_model_ContributionDotVec sources; } jazz_model_ContributionSubstitution;
/* BranchViewCopyBase: Current(branch) | Snapshot { branch, snapshot } */
typedef struct jazz_model_SnapshotDot { u64 time; jz_uuid node; } jazz_model_SnapshotDot;
typedef struct jazz_model_SnapshotRef { jz_uuid owner; u64 global_base; u64 local_base; JZ_VEC(jazz_model_SnapshotDot) dots; } jazz_model_SnapshotRef;
enum { JAZZ_BVC_BASE_CURRENT = 0, JAZZ_BVC_BASE_SNAPSHOT = 1 };
typedef struct jazz_model_BranchViewCopyBase { u32 tag; jazz_model_BranchKey branch; jazz_model_SnapshotRef snapshot; } jazz_model_BranchViewCopyBase;
typedef struct jazz_model_BranchViewCopyEvidence {
    u8 version; jazz_model_BranchKey head; jazz_model_BranchViewCopyBase base; jz_str table; jazz_types_RowUuid row_uuid; jazz_types_TxId source_version;
} jazz_model_BranchViewCopyEvidence;
enum { JAZZ_BWO_EXACT_HEAD_INSERT = 0, JAZZ_BWO_EXACT_HEAD_UPDATE = 1, JAZZ_BWO_VIEW_UPDATE_COPY = 2 };
typedef struct jazz_model_BranchWriteOperation { u32 tag; jazz_model_BranchViewCopyEvidence copy; } jazz_model_BranchWriteOperation;
typedef struct jazz_model_BranchWriteIntent {
    u8 version; jazz_types_PhysicalTableId physical_table_id; jazz_types_SchemaVersionId authored_schema; jazz_types_RowUuid row_uuid;
    jazz_model_BranchKey head; jazz_model_BranchWriteOperation operation;
} jazz_model_BranchWriteIntent;
struct jazz_model_ContributionMergeProvenance {
    jazz_model_BranchKey source;
    jazz_model_BranchKey target;
    JZ_VEC(jazz_model_ContributionSubstitution) substitutions;
    JZ_VEC(jazz_model_BranchViewCopyEvidence) branch_view_copies;
    JZ_VEC(jazz_model_BranchWriteIntent) branch_write_intents;
};
void jazz_model_ContributionMergeProvenance_drop(void *p);
void jazz_model_ContributionCoordinate_drop(void *c);
void jazz_model_ContributionDot_drop(void *d);
void jazz_model_ContributionSubstitution_drop(void *s);
void jazz_model_BranchViewCopyEvidence_drop(void *e);
void jazz_model_BranchViewCopyEvidence_clone(void *dst, const void *src);
void jazz_model_BranchWriteIntent_drop(void *i);
bool jazz_model_BranchViewCopyEvidence_eq(const jazz_model_BranchViewCopyEvidence *a, const jazz_model_BranchViewCopyEvidence *b);
jz_err jazz_model__tx__ContributionMergeProvenance__validate(const jazz_model_ContributionMergeProvenance *p);
jz_err jazz_model__model__branch__BranchKey__from_canonical_bytes(jz_slice b, jazz_model_BranchKey *out);
bool jazz_model__model__branch__BranchKey__is_canonical(const jazz_model_BranchKey *k);
void jazz_model_BranchKey_clone(void *dst, const void *src);
bool jazz_model_BranchKey_eq(const jazz_model_BranchKey *a, const jazz_model_BranchKey *b);
groove_ValueType jazz_model__schema__contribution_merge_storage_type(void);

/* jazz_protocol::RowBase { seq: Option<GlobalTime>, pending: Option<TxId> } */
typedef struct jazz_protocol_RowBase { bool has_seq; jazz_types_GlobalTime seq; bool has_pending; jazz_types_TxId pending; } jazz_protocol_RowBase;
/* jazz_protocol::VersionRecord (opaque; accessors) */
typedef struct jazz_protocol_VersionRecord jazz_protocol_VersionRecord;
typedef JZ_VEC(jazz_protocol_VersionRecord *) jazz_protocol_VersionRecordVec;
const jz_str *jazz_protocol__protocol__VersionRecord__table(const jazz_protocol_VersionRecord *v);
const jazz_model_BranchKey *jazz_protocol__protocol__VersionRecord__branch_key(const jazz_protocol_VersionRecord *v);
u32 jazz_protocol__protocol__VersionRecord__deletion(const jazz_protocol_VersionRecord *v); /* JZN_DELETION_* (0 = None) */
jz_slice jazz_protocol__protocol__VersionRecord__counter_signs(const jazz_protocol_VersionRecord *v);
const groove_OwnedRecord *jazz_protocol__protocol__VersionRecord__record(const jazz_protocol_VersionRecord *v);
jazz_types_AuthorSubject jazz_protocol__protocol__VersionRecord__created_by(const jazz_protocol_VersionRecord *v);
jazz_types_AuthorSubject jazz_protocol__protocol__VersionRecord__updated_by(const jazz_protocol_VersionRecord *v);
u64 jazz_protocol__protocol__VersionRecord__created_at_ms(const jazz_protocol_VersionRecord *v);
u64 jazz_protocol__protocol__VersionRecord__updated_at_ms(const jazz_protocol_VersionRecord *v);
jazz_types_RowUuid jazz_protocol__protocol__VersionRecord__row_uuid(const jazz_protocol_VersionRecord *v);
bool jazz_protocol__protocol__VersionRecord__optional_cell_at(const jazz_protocol_VersionRecord *v, u32 column, groove_Value *out);
jazz_protocol_RowBase jazz_protocol__protocol__VersionRecord__base(const jazz_protocol_VersionRecord *v);
jz_err jazz_protocol__protocol__VersionRecord__encode(const jazz_model_TableSchema *t, jazz_types_SchemaVersionId v, jazz_types_RowUuid row,
    jazz_types_AuthorSubject created_by, u64 created_at_ms, jazz_types_AuthorSubject updated_by, u64 updated_at_ms,
    const groove_Value *const *cells /* Option<Value> each, NULL = None */, u32 n, u32 deletion, jazz_protocol_VersionRecord **out);
jazz_protocol_VersionRecord *jazz_protocol__protocol__VersionRecord__new(jz_str table, jazz_types_SchemaVersionId v, groove_OwnedRecord record);
void jazz_protocol__protocol__VersionRecord__with_authored_columns(jazz_protocol_VersionRecord *v, const jz_btree *columns /* NULL = None; moved */);
void jazz_protocol__protocol__VersionRecord__with_branch_key(jazz_protocol_VersionRecord *v, jazz_model_BranchKey k);
void jazz_protocol__protocol__VersionRecord__with_base(jazz_protocol_VersionRecord *v, jazz_protocol_RowBase base);
void jazz_protocol__protocol__VersionRecord__with_counter_signs(jazz_protocol_VersionRecord *v, jz_bytes signs);
int jazz_protocol_VersionRecord_cmp(const void *a, const void *b, void *ctx);
void jazz_protocol_VersionRecord_drop(jazz_protocol_VersionRecord *v);

/* jazz_node MergeableCommit (node/commit.rs, other unit) */
typedef struct jazz_node_MergeableCommit {
    jazz_types_RowUuid row_uuid;
    jazz_types_AuthorSubject made_by;
    u64 now_ms;
    jz_btree cells;              /* String -> Value */
    u32 deletion;                /* JZN_DELETION_* (0 = None) */
    jz_btree *authored_columns;  /* Option<BTreeSet<String>> */
} jazz_node_MergeableCommit;

/* jazz_model TableSchema extras used by the codec */
enum { JAZZ_MERGE_STRATEGY_LWW = 0 };
u32 jazz_model__schema__TableSchema__merge_strategy(const jazz_model_TableSchema *t, const jz_str *column);
groove_TableSchema jazz_model__schema__TableSchema__authored_history_storage_table(const jazz_model_TableSchema *t);
groove_RecordDescriptor jazz_model__schema__TableSchema__wire_record_descriptor(const jazz_model_TableSchema *t);
bool groove__schema__ColumnType__eq(const groove_ValueType *a, const groove_ValueType *b);

/* other jazz_node units */
jz_err jazz_node__node__merge_ops__validate_counter_signs(const jazz_model_TableSchema *t, jz_slice signs);
typedef struct jazz_node_NodeAliases jazz_node_NodeAliases;
bool jazz_node__node__node_aliases__NodeAliases__node_for_alias(const jazz_node_NodeAliases *a, jazz_types_NodeAlias alias, jazz_types_NodeUuid *out);
/* exclusive_read_evidence::evidence_slot_values -> [Value; 4] */
jz_err jazz_node__node__exclusive_read_evidence__evidence_slot_values(const jazz_model_Transaction *tx, bool pending, groove_Value out[4]);
/* RowAuthor */
jz_err jazz_types__ids__RowAuthor__from_record(const groove_BorrowedRecord *r, jazz_types_RowAuthor *out);
groove_Value jazz_types__ids__RowAuthor__to_value(jazz_types_RowAuthor a);
jz_err jazz_types__ids__RowAuthor__read_optional(const groove_BorrowedRecord *r, u32 idx, bool *some, jazz_types_RowAuthor *out);
groove_Value jazz_types__ids__AuthorSubject__to_value(jazz_types_AuthorSubject a);
/* TxTime::sort_key(node) ordering */
int jazz_types__time__TxTime__sort_key_cmp(jazz_types_TxTime a, jazz_types_NodeUuid an, jazz_types_TxTime b, jazz_types_NodeUuid bn);


/* ---- more groove / model surface used by the codec ---- */
bool groove__records__values__EnumSchema__tag(const groove_EnumSchema *s, const char *name, u32 *out);
jz_err groove__records__encode_persisted_record_descriptor(groove_RecordDescriptor d, jz_bytes *out);
jz_err groove__records__OwnedRecord__get_idx(const groove_OwnedRecord *r, u32 idx, groove_Value *out);
typedef struct groove_PrimaryKeyValue groove_PrimaryKeyValue; /* opaque, heap */
groove_PrimaryKeyValue *groove__storage__PrimaryKeyValue__bytes(jz_bytes b);
groove_PrimaryKeyValue *groove__storage__PrimaryKeyValue__uuid(jz_uuid u);
groove_PrimaryKeyValue *groove__storage__PrimaryKeyValue__u64(u64 v);
groove_PrimaryKeyValue *groove__storage__PrimaryKeyValue__composite(groove_PrimaryKeyValue **items, u32 n);
void groove__storage__PrimaryKeyValue__into_bytes(groove_PrimaryKeyValue *pk, jz_bytes *out);
typedef struct groove_PredicateExpr groove_PredicateExpr; /* opaque, heap */
groove_PredicateExpr *groove__ivm__op_types__PredicateExpr__is_null(jz_str field);
groove_PredicateExpr *groove__ivm__op_types__PredicateExpr__neq(jz_str field, groove_LiteralValue value);
groove_PredicateExpr *groove__ivm__op_types__PredicateExpr__or(groove_PredicateExpr **items, u32 n);
groove_GraphBuilder *groove__db__query__GraphBuilder__table(jz_str table);
groove_GraphBuilder *groove__db__query__GraphBuilder__project(groove_GraphBuilder *g, const jz_str *fields, u32 n);
groove_GraphBuilder *groove__db__query__GraphBuilder__filter(groove_GraphBuilder *g, groove_PredicateExpr *p);
groove_GraphBuilder *groove__db__query__GraphBuilder__project_fields(groove_GraphBuilder *g, groove_ProjectFieldVec fields);
/* node::layer_stack (other unit): Ref::Local | Ref::Remote(Remote::Origin) */
enum { JZN_REF_LOCAL = 0, JZN_REF_REMOTE_ORIGIN = 1 };
typedef groove_GraphBuilder *(*jzn_layer_fn)(void *ctx, u32 ref);
typedef struct jzn_Layers jzn_Layers;
jzn_Layers *jazz_node__node__layer_stack__ref_layers(u32 settled, jzn_layer_fn f, void *ctx);
groove_GraphBuilder *jazz_node__node__layer_stack__compose(jzn_Layers *layers);
/* CurrentRow (jazz_model::row) */
enum { JAZZ_CURROW_BINDING_STORAGE = 0, JAZZ_CURROW_BINDING_LOGICAL_FIELD = 1 };
typedef struct jazz_model_CurrentRow { jz_str table; groove_OwnedRecord record; u32 binding; } jazz_model_CurrentRow;
void jazz_model__row__CurrentRow__new(jz_str table, groove_OwnedRecord record, jazz_model_CurrentRow *out);
void jazz_model__row__CurrentRow__new_with_binding_fields(jz_str table, groove_OwnedRecord record, u32 role, jazz_model_CurrentRow *out);
jazz_types_RowUuid jazz_model__row__CurrentRow__row_uuid(const jazz_model_CurrentRow *r);
typedef struct jazz_model_RowProvenance { jazz_types_AuthorSubject created_by; u64 created_at; jazz_types_AuthorSubject updated_by; u64 updated_at; } jazz_model_RowProvenance;
groove_ValueType jazz_types__ids__RowAuthor__value_type(void);
groove_Value jazz_model__schema__ColumnSchema__storage_value(const jazz_model_ColumnSchema *c, groove_Value v);
groove_ValueType jazz_model__schema__storage_column_type(const jazz_model_ColumnSchema *c);
jz_err jazz_model__model__branch__BranchKey__try_canonical_bytes(const jazz_model_BranchKey *k, jz_bytes *out);
jz_err jazz_model__tx__DurabilityTier__from_discriminant(u8 v, u32 *out);
jz_err jazz_model__tx__RejectionReason__from_storage_value(groove_Value v, jazz_model_RejectionReason *out);

/* ======================= ingest mirrors (node/mod.rs, state/.., protocol) ======================= */
typedef jazz_node_NodeState NSA; /* short alias for long prototypes */
jazz_types_SchemaVersionId jazz_protocol__protocol__VersionRecord__schema_version(const jazz_protocol_VersionRecord *v);
const jz_btree *jazz_protocol__protocol__VersionRecord__authored_columns(const jazz_protocol_VersionRecord *v); /* NULL = None */
bool jazz_protocol__protocol__VersionRecord__deletes_row(const jazz_protocol_VersionRecord *v);
bool jazz_protocol_VersionRecord_eq(const jazz_protocol_VersionRecord *a, const jazz_protocol_VersionRecord *b);
jazz_protocol_VersionRecord *jazz_protocol_VersionRecord_clone(const jazz_protocol_VersionRecord *v);
void jazz_protocol_VersionRecordVec_drop(jazz_protocol_VersionRecordVec *v);
void jazz_protocol_VersionRecordVec_clone(jazz_protocol_VersionRecordVec *dst, const jazz_protocol_VersionRecordVec *src);
jz_err jazz_protocol__protocol__validate_version_records(const jazz_protocol_VersionRecordVec *v);
/* protocol_limits: Option<String> */
bool jazz_node__protocol_limits__commit_unit_limit_violation(const jazz_protocol_VersionRecordVec *v, jz_str *reason);
typedef struct jazz_protocol_ShapeAst jazz_protocol_ShapeAst;
typedef struct jazz_protocol_ShapeOptions jazz_protocol_ShapeOptions;
jz_err jazz_node__protocol_limits__validate_shape_registration_size(const jazz_protocol_ShapeAst *ast, const jazz_protocol_ShapeOptions *opts);

/* Large-value upload protocol */
typedef struct groove_LargeValueRef groove_LargeValueRef;
bool groove_LargeValueRef_eq(const groove_LargeValueRef *a, const groove_LargeValueRef *b);
groove_LargeValueRef *groove_LargeValueRef_clone(const groove_LargeValueRef *r);
typedef struct jazz_protocol_ChunkNode { jz_bytes encoded; } jazz_protocol_ChunkNode;
typedef JZ_VEC(jazz_protocol_ChunkNode) jazz_protocol_ChunkNodeVec;
typedef JZ_VEC(jz_uuid) jzn_ChunkIdVec;
enum { JZP_CHUNK_STATUS_NEED, JZP_CHUNK_STATUS_STAGED, JZP_CHUNK_STATUS_REJECTED, JZP_CHUNK_STATUS_RATE_LIMITED };
typedef struct jazz_protocol_ChunkUploadResult { groove_LargeValueRef *value_ref; u32 status; jzn_ChunkIdVec need; } jazz_protocol_ChunkUploadResult;
enum { GROOVE_LV_PROGRESS_MISSING, GROOVE_LV_PROGRESS_STAGED };
typedef struct groove_LargeValueUploadProgress { u32 tag; jzn_ChunkIdVec missing; } groove_LargeValueUploadProgress;
typedef struct groove_PendingLargeValueUpload { groove_LargeValueRef *descriptor; /* Option */ } groove_PendingLargeValueUpload;
typedef JZ_VEC(groove_PendingLargeValueUpload) groove_PendingLargeValueUploadVec;
enum {
    GROOVE_DB_ERR_INVALID_LARGE_VALUE_METADATA = 0x6210, GROOVE_DB_ERR_IVM_LARGE_VALUE, GROOVE_DB_ERR_IVM_CHUNK_INTEGRITY,
    GROOVE_DB_ERR_PENDING_LARGE_VALUE_UPLOAD_LIMIT, GROOVE_DB_ERR_UNSETTLED_PUBLICATIONS
};
jz_err groove__db__Database__ensure_usable(const groove_Database *db);
bool groove__db__Database__has_unsettled_publications(const groove_Database *db);
jz_err groove__db__Database__begin_large_value_upload_with_pending_limit(groove_Database *db, groove_LargeValueRef *r, usize limit, groove_LargeValueUploadProgress *out);
jz_err groove__db__Database__pending_large_value_uploads(groove_Database *db, groove_PendingLargeValueUploadVec *out);
jz_err groove__db__Database__continue_large_value_upload_if_current(groove_Database *db, groove_LargeValueRef *r, jazz_protocol_ChunkNodeVec chunks,
    bool *some, groove_LargeValueUploadProgress *out);
jz_err groove__db__Database__prune_dropped_subscriptions(groove_Database *db);
usize groove__db__Database__runtime_stats_active_subscriptions(const groove_Database *db);
jz_err groove__db__Database__table_has_stored_rows(groove_Database *db, const jz_str *table, bool *out);

/* DatabaseBatch */
typedef struct groove_DatabaseBatch groove_DatabaseBatch;
typedef struct groove_AppliedBatch groove_AppliedBatch;
typedef struct groove_PersistProgress groove_PersistProgress;
groove_DatabaseBatch *groove__db__Database__open_batch(groove_Database *db);
void groove__db__DatabaseBatch__drop(groove_DatabaseBatch *b);
void groove__db__DatabaseBatch__reserve(groove_DatabaseBatch *b, usize n);
void groove__db__DatabaseBatch__insert(groove_DatabaseBatch *b, const char *table, groove_ValueVec values);
void groove__db__DatabaseBatch__insert_raw(groove_DatabaseBatch *b, groove_InternStr table, groove_PrimaryKeyValue *key, groove_ValidatedVariantRecord record);
void groove__db__DatabaseBatch__update_raw(groove_DatabaseBatch *b, groove_InternStr table, groove_PrimaryKeyValue *key, groove_ValidatedVariantRecord record);
void groove__db__DatabaseBatch__delete(groove_DatabaseBatch *b, groove_InternStr table, groove_PrimaryKeyValue *key);
void groove__db__DatabaseBatch__delete_str(groove_DatabaseBatch *b, const char *table, groove_PrimaryKeyValue *key);
enum { GROOVE_ENSURE_INSERTED, GROOVE_ENSURE_ALREADY_IDENTICAL, GROOVE_ENSURE_CONFLICT };
jz_err groove__db__DatabaseBatch__ensure_exact(groove_DatabaseBatch *b, groove_Database *db, groove_InternStr table, groove_PrimaryKeyValue *key,
    groove_ValidatedVariantRecord record, u32 *outcome);
jz_err groove__db__Database__apply_batch(groove_Database *db, groove_DatabaseBatch *b, groove_AppliedBatch **out);
groove_PersistProgress *groove__db__Database__persist_with_progress(groove_Database *db, const groove_AppliedBatch *applied);
jz_err groove__db__Database__finish_persistence(groove_Database *db, groove_PersistProgress *p);
void groove_AppliedBatch_drop(groove_AppliedBatch *a);

/* SyncMessage (jazz_protocol::SyncMessage) */
enum {
    JZP_SM_COMMIT_UNIT, JZP_SM_FATE_UPDATE, JZP_SM_VIEW_UPDATE, JZP_SM_VIEW_UPDATE_PART, JZP_SM_REGISTER_SHAPE,
    JZP_SM_SUBSCRIBE, JZP_SM_SUBSCRIBE_REJECTED, JZP_SM_UNSUBSCRIBE, JZP_SM_PUBLISH_SCHEMA, JZP_SM_PUBLISH_SCHEMA_WITH_LENS,
    JZP_SM_PUBLISH_LENS, JZP_SM_CATALOGUE_ACK, JZP_SM_CATALOGUE_SNAPSHOT, JZP_SM_READ_BARRIER, JZP_SM_CHUNK_UPLOAD_START,
    JZP_SM_CHUNK_UPLOAD_NODES, JZP_SM_CHUNK_UPLOAD_RESULT, JZP_SM_CHUNK_REQUEST_BATCH, JZP_SM_CHUNK_RESPONSE_BATCH,
    JZP_SM_SESSION_CLAIMS, JZP_SM_CURRENT_ROWS_REQUEST, JZP_SM_CURRENT_ROWS_RECEIPT, JZP_SM_CURRENT_ROWS_CANCEL,
    JZP_SM_AUTHZ_SCOPE_INTENT, JZP_SM_AUTHZ_SCOPE_VIEW, JZP_SM_AUTHZ_SCOPE_AGGREGATE_RECEIPT, JZP_SM_AUTHZ_SCOPE_UNAVAILABLE,
    JZP_SM_AUTHZ_SCOPE_DECISION, JZP_SM_RESERVED14, JZP_SM_RESERVED15, JZP_SM_RESERVED29
};
typedef struct jazz_protocol_CatalogueAck {
    bool has_revision; u64 revision;
    bool has_schema; jazz_types_SchemaVersionId schema;
    bool has_lens; jazz_types_MigrationLensId lens;
    bool applied;
} jazz_protocol_CatalogueAck;
typedef struct jazz_protocol_ViewUpdatePayload jazz_protocol_ViewUpdatePayload;
typedef struct jazz_protocol_SubscribePayload jazz_protocol_SubscribePayload;
typedef struct jazz_protocol_SyncMessage {
    u32 tag;
    union {
        struct { jazz_model_Transaction tx; jazz_protocol_VersionRecordVec versions; } commit_unit;
        struct { jazz_types_TxId tx_id; jazz_model_Fate fate; bool has_global_time; jazz_types_GlobalTime global_time;
                 bool has_durability; u32 durability; } fate_update;
        jazz_protocol_ViewUpdatePayload *view_update;
        struct { u64 shape_id; jazz_protocol_ShapeAst *ast; jazz_protocol_ShapeOptions *opts; } register_shape;
        jazz_protocol_SubscribePayload *subscribe;
        struct { u64 subscription; } unsubscribe;
        struct { jazz_types_AuthorSubject author; jazz_model_SchemaVersion *schema; } publish_schema;
        struct { jazz_types_AuthorSubject author; u64 catalogue_seq; jazz_protocol_SchemaLineagePublication *publication; } publish_schema_with_lens;
        struct { jazz_types_AuthorSubject author; jazz_protocol_MigrationLens lens; } publish_lens;
        jazz_protocol_CatalogueAck catalogue_ack;
        struct { groove_LargeValueRef *value_ref; } chunk_upload_start;
        struct { groove_LargeValueRef *value_ref; jazz_protocol_ChunkNodeVec chunks; } chunk_upload_nodes;
        jazz_protocol_ChunkUploadResult chunk_upload_result;
        struct { jazz_types_AuthorSubject identity; jz_btree claims; } session_claims;
    } v;
} jazz_protocol_SyncMessage;
typedef JZ_VEC(jazz_protocol_SyncMessage) jazz_protocol_SyncMessageVec;
void jazz_protocol_SyncMessage_drop(void *m);
void jazz_protocol_SyncMessageVec_drop(jazz_protocol_SyncMessageVec *v);
bool jazz_node__node__is_catalogue_mutation(const jazz_protocol_SyncMessage *m);

/* PublicationOutcome<T> (mod.rs). T is carried in `value` (Vec<SyncMessage>) or by the caller. */
typedef struct jazz_node_PublishedTransaction { jazz_types_TxId tx_id; groove_AppliedBatch *persistence; } jazz_node_PublishedTransaction;
typedef struct jazz_node_PublicationOutcome {
    jazz_protocol_SyncMessageVec value;
    JZ_VEC(jazz_node_PublishedTransaction) publications;
    jz_deque post_settlement_work; /* SyncMessage */
} jazz_node_PublicationOutcome;
void jazz_node__node__PublicationOutcome__settled(jazz_node_PublicationOutcome *out, jazz_protocol_SyncMessageVec value);
void jazz_node__node__PublicationOutcome__append_outcome(jazz_node_PublicationOutcome *self, jazz_node_PublicationOutcome *other);
void jazz_node_PublicationOutcome_drop(jazz_node_PublicationOutcome *o);

/* CommitUnitIngestContext / Trust */
enum { JZN_TRUST_SESSION, JZN_TRUST_RELAY, JZN_TRUST_BACKEND, JZN_TRUST_AUTHORITY, JZN_TRUST_ADMIN };
typedef struct jazz_node_CommitUnitIngestContext {
    jazz_types_AuthorSubject identity;
    u32 trust;
    bool admitted_write_authorization;
    bool version_receipts_validated;
} jazz_node_CommitUnitIngestContext;
bool jazz_node__node__CommitUnitTrust__is_trusted(u32 trust);
extern const jazz_types_AuthorSubject jazz_types_AuthorSubject_SYSTEM;
#define JZN_SKEW_TOLERANCE_MS 30000ull
#define JZN_LARGE_VALUE_UPLOAD_START_INGRESS_CHARGE_BYTES (1ull << 20)
u64 jazz_node__node__next_groove_runtime_token(void);
jazz_types_GlobalTime jazz_types__time__GlobalTime__authority_now_ms(u64 now_ms, u64 tx_ms);

/* JazzSchema permissions helpers */
void jazz_model__schema__JazzSchema__without_permissions(const jazz_model_JazzSchema *s, jazz_model_JazzSchema *out);
void jazz_model__schema__JazzSchema__with_permissions_from(const jazz_model_JazzSchema *s, const jazz_model_JazzSchema *from, jazz_model_JazzSchema *out);
jz_uuid jazz_protocol__protocol__MigrationLens__content_id(const jazz_protocol_MigrationLens *l);
typedef struct jazz_protocol_TableLens { jz_str source_table; jz_str target_table; jz_vec ops; } jazz_protocol_TableLens;

/* mod.rs */
typedef JZ_VEC(groove_LargeValueRef *) jzn_LargeValueRefVec;
void jazz_node__node__version_indirect_descriptors(const jazz_protocol_VersionRecordVec *v, jzn_LargeValueRefVec *out);

/* state/durable.rs, state/lifecycle.rs */
jz_err jazz_node__node__state__durable__NodeState__require_catalogue_ready(const NSA *s);
jz_err jazz_node__node__state__lifecycle__NodeState__current_staged_ids_for_descriptors(NSA *s, const jzn_LargeValueRefVec *d, bool b);
bool jazz_node__node__state__lifecycle__NodeState__admit_large_value_ingress(NSA *s, u64 bytes);
void jazz_node__node__state__lifecycle__NodeState__set_session_claims(NSA *s, jazz_types_AuthorSubject identity, jz_btree claims);
jz_err jazz_node__node__state__lifecycle__NodeState__rebuild_database_slot(NSA *s);
void jazz_node__node__state__lifecycle__NodeState__invalidate_runtime_handles_after_database_rebuild(NSA *s);

/* state/catalogue.rs */
jz_err jazz_node__node__state__catalogue__NodeState__persist_catalogue_schema(NSA *s, const jazz_model_SchemaVersion *v);
jz_err jazz_node__node__state__catalogue__NodeState__persist_active_schema(NSA *s, const jazz_node_ActiveSchema *a);
void jazz_node__node__state__catalogue__NodeState__install_active_schema(NSA *s, jazz_node_ActiveSchema a);
jz_err jazz_node__node__state__catalogue__NodeState__ensure_provisional_physical_mapping(NSA *s, jazz_types_SchemaVersionId v);
jz_err jazz_node__node__state__catalogue__NodeState__ensure_schema_version_alias(NSA *s, jazz_types_SchemaVersionId v, jazz_types_SchemaVersionAlias *out);
typedef struct jazz_node_PhysicalIdentityHistory jazz_node_PhysicalIdentityHistory;
jazz_node_PhysicalIdentityHistory *jazz_node__node__state__catalogue__NodeState__physical_identity_history_for_candidate(const NSA *s,
    jazz_types_SchemaVersionId schema, const jz_uuid *publication /* NULL = None */);
jz_err jazz_node__node__state__catalogue__NodeState__validate_publication_sources(const jazz_protocol_SchemaLineagePublication *p,
    const jz_btree *schemas, const jz_btree *mappings, jazz_node_PhysicalIdentityHistory *history);
struct jazz_node_PendingSchemaLineage;
struct jazz_node_StagedSchemaLineage;
jz_err jazz_node__node__state__catalogue__NodeState__validate_pending_schema_lineage_physical_mapping(const NSA *s, const struct jazz_node_PendingSchemaLineage *p);
jz_err jazz_node__node__state__catalogue__NodeState__remove_pending_schema_lineage(NSA *s, u64 seq, jz_uuid publication);
jz_err jazz_node__node__state__catalogue__NodeState__reconcile_publication_mapping(const jazz_node_SchemaCatalogue *c,
    const jazz_protocol_SchemaLineagePublication *p, const jazz_node_SchemaPhysicalMapping *fresh, jazz_node_SchemaPhysicalMapping *out);
jz_err jazz_node__node__state__catalogue__NodeState__next_schema_version_alias(const NSA *s, jazz_types_SchemaVersionAlias *out);
jz_err jazz_node__node__state__catalogue__NodeState__persist_catalogue_schema_lineage(NSA *s, const struct jazz_node_StagedSchemaLineage *st);
jz_err jazz_node__node__state__catalogue__NodeState__persist_pending_schema_lineage(NSA *s, const struct jazz_node_PendingSchemaLineage *p);
jz_err jazz_node__node__state__catalogue__NodeState__write_active_schema_lineage_to_batch(groove_DatabaseBatch *b, const struct jazz_node_StagedSchemaLineage *st);
jz_err jazz_node__node__state__catalogue__NodeState__reconcile_physical_mapping_for_lens(const NSA *s, const jazz_protocol_MigrationLens *l, jazz_node_SchemaPhysicalMapping *out);
jz_err jazz_node__node__state__catalogue__NodeState__persist_catalogue_lens_with_physical_metadata(NSA *s, const jazz_protocol_MigrationLens *l, const void *meta);
void jazz_node_StagedSchemaLineage_clone(struct jazz_node_StagedSchemaLineage *dst, const struct jazz_node_StagedSchemaLineage *src);
void jazz_node_StagedSchemaLineage_drop(void *p);
void jazz_node_PendingSchemaLineage_clone(struct jazz_node_PendingSchemaLineage *dst, const struct jazz_node_PendingSchemaLineage *src);
void jazz_node_PendingSchemaLineage_drop(void *p);
void jazz_protocol_SchemaLineagePublication_clone(void *dst, const void *src);
extern const jz_btree_desc jzn_desc_uuid_schema_version; /* SchemaVersionId -> SchemaVersion */
extern const jz_btree_desc jzn_desc_uuid_alias;          /* SchemaVersionId -> SchemaVersionAlias */

/* migration_validation.rs */
jz_err jazz_node__node__migration_validation__validate_migration_lens_between(const jazz_protocol_MigrationLens *l,
    const jazz_model_SchemaVersion *source, const jazz_model_SchemaVersion *target);
jz_err jazz_node__node__migration_validation__validate_lineage_table_partition(const jazz_model_JazzSchema *source,
    const jazz_model_JazzSchema *target, const jazz_protocol_MigrationLens *l, const jzn_StrVec *new_tables, const jzn_StrVec *dropped_tables);

/* query_eval/subscriptions.rs, views.rs */
jz_err jazz_node__node__query_eval__subscriptions__NodeState__drain_parked_shape_registrations(NSA *s);
jz_err jazz_node__node__query_eval__subscriptions__NodeState__register_shape_with_options(NSA *s, u64 shape, jazz_protocol_ShapeAst *ast, jazz_protocol_ShapeOptions *opts);
jz_err jazz_node__node__query_eval__subscriptions__NodeState__apply_subscribe(NSA *s, jazz_protocol_SubscribePayload *p);
void jazz_node__node__query_eval__subscriptions__NodeState__apply_unsubscribe(NSA *s, u64 subscription);
typedef struct jazz_node_ViewUpdateParts jazz_node_ViewUpdateParts;
jazz_node_ViewUpdateParts *jazz_node__node__ViewUpdateParts__from_payload(jazz_protocol_ViewUpdatePayload *p, bool defer_settlement, bool reset_input_set);
jz_err jazz_node__node__views__NodeState__apply_view_update(NSA *s, jazz_node_ViewUpdateParts *parts);

/* state/commit.rs, currency.rs, open_tx.rs, global_state.rs */
struct jazz_node_StoredTransaction;
jz_err jazz_node__node__currency__NodeState__query_transaction(NSA *s, jazz_types_TxId tx, bool *found, struct jazz_node_StoredTransaction *out);
jz_err jazz_node__node__currency__NodeState__query_versions_for_tx(NSA *s, jazz_types_TxId tx, jazz_node_VersionRowVec *out);

jz_err jazz_node__node__state__read_payload__NodeState__version_record_from_row(NSA *s, const jazz_node_VersionRow *v, jazz_protocol_VersionRecord **out);
void jazz_node__node__state__read_payload__NodeState__merge_tx_time(NSA *s, jazz_types_TxTime t);
void jazz_node__node__open_tx__NodeState__record_applied_global_time(NSA *s, jazz_types_GlobalTime gt, jazz_types_TxId tx);
jz_err jazz_node__node__state__catalogue__NodeState__ensure_node_alias(NSA *s, jazz_types_NodeUuid node, jazz_types_NodeAlias *out);
jz_err jazz_node__node__state__commit__NodeState__apply_node_batch(NSA *s, groove_DatabaseBatch *b, groove_AppliedBatch **out);
void jazz_node__node__state__commit__NodeState__invalidate_tx_version_tables_cache(NSA *s, jazz_types_TxId tx);
jz_err jazz_node__node__state__read_payload__NodeState__version_tx_id(const NSA *s, const jazz_node_VersionRow *v, jazz_types_TxId *out);
jz_err jazz_node__node__state__contribution_merge__NodeState__admit_contribution_merge_for_storage(NSA *s, const jazz_model_Transaction *tx, groove_Value *out);
jz_err jazz_node__node__state__contribution_merge__NodeState__validate_contribution_merge_operation_identities(NSA *s, const jazz_model_Transaction *tx);

/* ---- read sets / snapshots (jazz_model::tx) ---- */
typedef struct jazz_model_ShapeId { u8 b[32]; } jazz_model_ShapeId;
typedef struct jazz_model_BindingId { u8 b[32]; } jazz_model_BindingId;
static inline bool jazz_model_ShapeId_eq(const jazz_model_ShapeId *a, const jazz_model_ShapeId *b) { return memcmp(a->b, b->b, 32) == 0; }
static inline bool jazz_model_BindingId_eq(const jazz_model_BindingId *a, const jazz_model_BindingId *b) { return memcmp(a->b, b->b, 32) == 0; }
typedef struct jazz_model_Query jazz_model_Query;
typedef struct jazz_model_ValidatedQuery jazz_model_ValidatedQuery;
typedef struct jazz_model_Binding jazz_model_Binding;
struct jazz_model_Snapshot { jazz_types_NodeUuid owner; jazz_types_GlobalTime global_base; jazz_types_TxTime local_base; JZ_VEC(jazz_types_TxId) dots; };
typedef struct jazz_model_RowRead { jz_str table; jazz_types_RowUuid row_uuid; jazz_types_TxId version; } jazz_model_RowRead;
typedef struct jazz_model_AbsentRead { jz_str table; jazz_types_RowUuid row_uuid; } jazz_model_AbsentRead;
typedef struct jazz_model_PredicateRead {
    jz_str table; jazz_model_ShapeId shape_id; jazz_model_Query *shape; jazz_model_BindingId binding_id; jz_btree binding_values;
} jazz_model_PredicateRead;
struct jazz_model_ReadSet { u32 len; union { jazz_model_RowRead *rows; jazz_model_AbsentRead *absent; jazz_model_PredicateRead *predicates; } v; };
jazz_model_Query *jazz_model__query__Query__from_table(const jz_str *table);
void jazz_model__query__Query__drop(jazz_model_Query *q);
jz_err jazz_model__query__Query__validate(const jazz_model_Query *q, const jazz_model_JazzSchema *schema, jazz_model_ValidatedQuery **out);
jz_err jazz_model__query__ValidatedQuery__bind(const jazz_model_ValidatedQuery *v, jz_btree values, jazz_model_Binding **out);
jazz_model_ShapeId jazz_model__query__ValidatedQuery__shape_id(const jazz_model_ValidatedQuery *v);
jazz_model_BindingId jazz_model__query__Binding__binding_id(const jazz_model_Binding *b);
const jz_str *jazz_model__query__ValidatedQuery__table(const jazz_model_ValidatedQuery *v);
bool jazz_model__query__ValidatedQuery__has_aggregate(const jazz_model_ValidatedQuery *v);
const jazz_model_Query *jazz_model__query__ValidatedQuery__aggregate_input(const jazz_model_ValidatedQuery *v); /* NULL = None */
jazz_types_SchemaVersionId jazz_model__query__ValidatedQuery__schema_version(const jazz_model_ValidatedQuery *v);
void jazz_model__query__ValidatedQuery__drop(jazz_model_ValidatedQuery *v);
void jazz_model__query__Binding__drop(jazz_model_Binding *b);
void jazz_model__query__binding_values_clone(jz_btree *dst, const jz_btree *src);
bool jazz_types__ids__AuthorSubject__is_anonymous(const jazz_types_AuthorSubject *a);
bool jazz_node__node__CommitUnitIngestContext__same_parked_authority(const jazz_node_CommitUnitIngestContext *existing,
    const jazz_node_CommitUnitIngestContext *resent);

/* CurrentRow helpers (query results) */
typedef JZ_VEC(jazz_model_CurrentRow) jazz_model_CurrentRowVec;
bool jazz_model__row__CurrentRow__aggregate_payload_equivalent(const jazz_model_CurrentRow *a, const jazz_model_CurrentRow *b);
void jazz_model_CurrentRowVec_drop(jazz_model_CurrentRowVec *v);

/* query_eval.rs / authorization.rs / open_tx.rs / policy.rs / global_state.rs */
jz_err jazz_node__node__query_eval__NodeState__query_rows_with_prepared_plan_for_identity(NSA *s, const jazz_model_ValidatedQuery *q,
    const jazz_model_Binding *b, u32 durability, const void *opts, jazz_types_AuthorSubject identity, jazz_model_CurrentRowVec *out);
jz_err jazz_node__node__query_eval__NodeState__query_rows_at_snapshot(NSA *s, const jazz_model_ValidatedQuery *q, const jazz_model_Binding *b,
    const jazz_model_Snapshot *snapshot, jazz_model_CurrentRowVec *out);
jz_err jazz_node__node__query_eval__authorization__NodeState__query_rows(NSA *s, const jazz_model_ValidatedQuery *q, const jazz_model_Binding *b,
    u32 durability, jazz_model_CurrentRowVec *out);
bool jazz_node__node__open_tx__NodeState__snapshot_content_witness(NSA *s, jazz_types_SchemaVersionId v, const jz_str *table,
    jazz_types_RowUuid row, const jazz_model_Snapshot *snapshot, jazz_types_TxId *out);
bool jazz_node__node__open_tx__NodeState__snapshot_covers(NSA *s, jazz_types_TxId tx, const jazz_model_Snapshot *snapshot);
bool jazz_node__node__global_state__NodeState__visible_global_row_tx_id_now(NSA *s, jazz_types_SchemaVersionId v, const jz_str *table,
    jazz_types_RowUuid row, jazz_types_TxId *out);
bool jazz_node__node__global_state__NodeState__visible_global_tx_id_for_physical_table_now(NSA *s, jazz_types_PhysicalTableId t,
    jazz_types_RowUuid row, jazz_types_TxId *out);
bool jazz_node__node__global_state__NodeState__visible_global_content_tx_id_in_schema_now(NSA *s, jazz_types_SchemaVersionId v,
    const jz_str *table, jazz_types_RowUuid row, jazz_types_TxId *out);
jz_err jazz_node__node__global_state__NodeState__global_current_updates_for_versions(NSA *s, const groove_DatabaseBatch *b, jazz_types_TxId tx,
    jazz_types_GlobalTime gt, const jazz_node_VersionRowVec *versions, jazz_node_VersionRowVec *out);
enum { JZN_UNIT_POLICY_ALLOWED, JZN_UNIT_POLICY_DENIED, JZN_UNIT_POLICY_UNSUPPORTED };
jz_err jazz_node__node__policy__NodeState__commit_unit_write_policies_allow(NSA *s, const jazz_protocol_VersionRecordVec *versions,
    jazz_types_AuthorSubject author, jazz_types_TxId candidate, u32 *decision, jz_str *unsupported_reason);
jz_err jazz_node__node__policy__NodeState__version_satisfies_read_for_write_visibility(NSA *s, const jazz_protocol_VersionRecord *v,
    jazz_types_AuthorSubject subject, const jazz_types_TxId *candidate, bool *out);
jz_err jazz_node__node__policy__NodeState__branch_view_copy_satisfies_read_for_write_visibility(NSA *s, const jazz_model_BranchViewCopyEvidence *e,
    jazz_types_SchemaVersionId authored_schema, jazz_types_AuthorSubject subject, const jazz_types_TxId *candidate, bool *out);

/* currency.rs / state/commit.rs */
jz_err jazz_node__node__currency__NodeState__query_row_versions_in_branch(NSA *s, const jz_str *table, const jazz_model_BranchKey *branch_key,
    jazz_types_RowUuid row, jazz_node_VersionRowVec *out);
/* query_transaction_state: Option<(Fate, Option<GlobalTime>, DurabilityTier)> */
jz_err jazz_node__node__currency__NodeState__query_transaction_state(NSA *s, jazz_types_TxId tx, bool *some, jazz_model_Fate *fate,
    bool *has_gt, jazz_types_GlobalTime *gt, u32 *durability);
jz_err jazz_node__node__currency__NodeState__version_storage_sources(NSA *s, const jz_str *table, jzn_StrVec *out);
jz_err jazz_node__node__currency__NodeState__decode_stored_history_record(NSA *s, const groove_DatabaseBatch *b /* NULL = None */, const jz_str *table,
    const jz_str *storage_table, groove_OwnedRecord record, jazz_node_VersionRow *out);
jz_err jazz_node__node__currency__NodeState__decode_history_owned_record(NSA *s, const char *table, const jz_str *storage_table,
    groove_OwnedRecord record, jazz_node_VersionRow *out);
/* history_tx_seq: Option<GlobalTime> */
jz_err jazz_node__node__currency__NodeState__history_tx_seq(NSA *s, const groove_DatabaseBatch *b, jazz_types_TxTime t,
    jazz_types_NodeAlias alias, bool *has, jazz_types_GlobalTime *out);
jz_err jazz_node__node__currency__NodeState__history_image_from_current_record(NSA *s, jazz_types_SchemaVersionId v, const jz_str *table,
    u32 variant_tag, groove_BorrowedRecord record, bool *some, jazz_node_VersionRow *out);
void jazz_node__node__state__commit__NodeState__cache_tx_versions(NSA *s, jazz_types_TxId tx, jazz_node_VersionRowVec versions);
bool jazz_node__node__state__read_payload__NodeState__node_for_alias(const NSA *s, jazz_types_NodeAlias alias, jazz_types_NodeUuid *out);

/* raw storage reads (groove::db) */
typedef struct groove_RawRow { u32 variant_tag; groove_OwnedRecord record; } groove_RawRow;
typedef JZ_VEC(groove_RawRow) groove_RawRowVec;
void groove_RawRowVec_drop(groove_RawRowVec *v);
jz_err groove__db__Database__primary_key_get_raw_in_batch(groove_Database *db, const groove_DatabaseBatch *b, const jz_str *table,
    const groove_Value *key, u32 n, bool *some, groove_RawRow *out);
jz_err groove__db__Database__primary_key_scan_raw_in_batch(groove_Database *db, const groove_DatabaseBatch *b, const jz_str *table,
    const groove_Value *prefix, u32 n, groove_RawRowVec *out);
jz_err groove__db__Database__primary_key_scan_range_raw_in_batch(groove_Database *db, const groove_DatabaseBatch *b, const jz_str *table,
    const groove_Value *start, const groove_Value *end, u32 n, groove_RawRowVec *out);
jz_err groove__db__Database__index_scan_raw(groove_Database *db, const char *table, const char *index, const groove_Value *key, u32 n,
    groove_RawRowVec *out);
jz_err groove__records__BorrowedRecord__get_u64(const groove_BorrowedRecord *r, u32 idx, u64 *out);
jz_err groove__records__BorrowedRecord__get_nullable_u64(const groove_BorrowedRecord *r, u32 idx, bool *some, u64 *out);
typedef struct groove_DirectRecordStore groove_DirectRecordStore;
jz_err groove__db__Database__direct_record_store(groove_Database *db, const char *name, groove_DirectRecordStore **out);
jz_err groove__db__DirectRecordStore__get(groove_DirectRecordStore *st, const groove_Value *key, u32 n, bool *some, groove_OwnedRecord *out);
jz_err groove__db__DirectRecordStore__set(groove_DirectRecordStore *st, const groove_Value *key, u32 n, const groove_Value *values, u32 nv);
#define JAZZ_SCHEMA_SUPERSEDED_WRITES_STORE "jazz_superseded_writes"
void groove__db__DatabaseBatch__update(groove_DatabaseBatch *b, const char *table, groove_ValueVec values);
void groove__db__DatabaseBatch__accept_large_value(groove_DatabaseBatch *b, u64 staged_id);
enum { GROOVE_NOTIFY_AFTER_PERSISTENCE = 1 };
void groove__db__DatabaseBatch__deliver_notifications(groove_DatabaseBatch *b, u32 timing);
typedef JZ_VEC(u64) jzn_StagedIdVec;
jz_err jazz_node__node__state__lifecycle__NodeState__current_staged_ids_for_descriptors_vec(NSA *s, const jzn_LargeValueRefVec *d, bool b,
    jzn_StagedIdVec *out);

/* rejected payloads (state/recovery.rs) */
typedef struct jazz_node_RejectedVersion jazz_node_RejectedVersion;
typedef JZ_VEC(jazz_node_RejectedVersion *) jazz_node_RejectedVersionVec;
jazz_node_RejectedVersion *jazz_node__node__RejectedVersion__new(jz_str table, groove_OwnedRecord record);
int jazz_node__node__RejectedVersion__sort_key_cmp(const void *a, const void *b, void *ctx); /* (table, row_uuid, deletion.is_some()) */
struct jazz_node_RejectedTransaction;
struct jazz_node_RejectedTransaction *jazz_node__node__RejectedTransaction__new(jazz_types_TxId tx, groove_OwnedRecord record,
    jazz_node_RejectedVersionVec versions);
jazz_types_TxId jazz_node__node__RejectedTransaction__tx_id(const struct jazz_node_RejectedTransaction *r);
extern const jz_btree_desc jzn_desc_txid_rejected; /* TxId -> RejectedTransaction* */
void jazz_model__schema__JazzSchema__storage_tables(const jazz_model_JazzSchema *s, groove_TableSchemaVec *out);

/* merge_ops.rs */
void jazz_node__node__merge_ops__clear_counter_signs(groove_ValueVec *values, groove_RecordDescriptor d);
bool jazz_node__node__merge_ops__counter_sign(const jazz_model_TableSchema *t, jz_slice signs, u32 index);
jz_err jazz_node__node__merge_ops__counter_after_op(const groove_ValueType *t, const groove_Value *previous, const groove_Value *op,
    bool negative, bool *some, groove_Value *out);
jz_err jazz_node__node__merge_ops__apply_merge_op(u32 strategy, const groove_ValueType *t, const groove_Value *previous,
    const groove_Value *op, bool negative, groove_Value *out);
jz_err jazz_node__node__merge_ops__apply_pending_merge_op(u32 strategy, const groove_ValueType *t, const groove_Value *base,
    const groove_Value *op, bool negative, groove_Value *out);
jz_err jazz_node__node__merge_ops__counter_to_i128(const groove_Value *v, __int128 *out);
jz_err jazz_node__node__merge_ops__counter_op_delta(const groove_ValueType *t, const groove_Value *op, bool negative, __int128 *out);
enum { JAZZ_MERGE_STRATEGY_COUNTER = 1, JAZZ_MERGE_STRATEGY_GSET = 2 };
void groove_ValueType_debug(jz_str *out, const groove_ValueType *t);

/* mod.rs lens helpers */
void jazz_node__node__apply_compiled_lens_path(const jazz_node_CompiledLensPath *path, jz_btree *cells, jz_str *out_table /* NULL ok */);

/* JazzSchema::validate_authored_branch_key: Ok(Vec<(column, value)>) or Err(reason) */
typedef struct jazz_model_BranchCell { jz_str column; groove_Value value; } jazz_model_BranchCell;
typedef JZ_VEC(jazz_model_BranchCell) jazz_model_BranchCellVec;
jz_err jazz_model__schema__JazzSchema__validate_authored_branch_key(const jazz_model_JazzSchema *s, const jazz_model_TableSchema *t,
    const jazz_model_BranchKey *k, jazz_model_BranchCellVec *out, jz_str *reason);
bool jazz_protocol__protocol__VersionRecord__cell_at(const jazz_protocol_VersionRecord *v, u32 position, groove_Value *out);

/* fates.rs / view_updates.rs extras */
int jazz_model_BranchKey_cmp(const void *a, const void *b, void *ctx);
void groove__db__DatabaseBatch__insert_variant(groove_DatabaseBatch *b, groove_InternStr table, groove_VariantRecord record);
/* record_applied_global_time -> Vec<GlobalTime> (globals the frontier advanced over) */
void jazz_node__node__open_tx__NodeState__record_applied_global_time_collect(NSA *s, jazz_types_GlobalTime gt, jazz_types_TxId tx,
    jzn_U64Vec *advanced);


/* ======================= maintained_subscription_view.rs mirrors ======================= */
/* --- jazz_types object ids --- */
typedef struct jazz_types_OutputOccurrenceId { void *repr; } jazz_types_OutputOccurrenceId;
typedef struct jazz_types_ObjectId { jz_uuid uuid; } jazz_types_ObjectId;
static inline jazz_types_ObjectId jazz_types__object__ObjectId__from_uuid(jz_uuid u) { jazz_types_ObjectId o; o.uuid = u; return o; }
typedef struct jzn_UnionArm { usize position; jz_str label; } jzn_UnionArm;
typedef JZ_VEC(jzn_UnionArm) jzn_UnionArmVec;
typedef JZ_VEC(jazz_types_ObjectId) jzn_ObjectIdVec;
/* OutputOccurrenceId::with_union_arms(root, joined, arms) -> Option */
bool jazz_types__object__OutputOccurrenceId__with_union_arms(jazz_types_ObjectId root, jzn_ObjectIdVec joined, jzn_UnionArmVec arms,
    jazz_types_OutputOccurrenceId *out);
typedef struct jazz_types_ResultKey { void *repr; } jazz_types_ResultKey;

/* --- jazz_protocol result members (mirror of jazz_protocol/protocol.h) --- */
typedef groove_InternStr jazz_protocol_InternStr;
enum { JAZZ_LAYER_CONTENT = 0, JAZZ_LAYER_DELETION = 1, JAZZ_LAYER_CONTENT_OR_DELETION = 2 };
typedef struct jazz_protocol_OptBytes { bool some; jz_bytes v; } jazz_protocol_OptBytes;
typedef struct jazz_protocol_OptTxId { bool some; jazz_types_TxId v; } jazz_protocol_OptTxId;
typedef struct jazz_protocol_ReadViewKey { jz_uuid id; } jazz_protocol_ReadViewKey;
typedef struct jazz_protocol_RowVersionRefEntry {
    jazz_types_TxId tx;
    bool has_schema_version; jazz_types_SchemaVersionId schema_version;
    u32 layer;
    jazz_protocol_OptTxId batch;
    jazz_protocol_OptBytes branch_or_prefix;
    jazz_protocol_OptBytes row_digest;
} jazz_protocol_RowVersionRefEntry;
typedef struct jazz_protocol_SupportingRow {
    jazz_types_GlobalPhysicalTableId physical_table;
    jazz_protocol_InternStr version_table;
    jazz_types_RowUuid row;
    jazz_protocol_RowVersionRefEntry version;
} jazz_protocol_SupportingRow;
typedef JZ_VEC(jazz_protocol_SupportingRow) jazz_protocol_SupportingRowVec;
int jazz_protocol__protocol__SupportingRow__cmp(const void *a, const void *b, void *ctx);
void jazz_protocol__protocol__SupportingRow__clone(void *dst, const void *src);
void jazz_protocol__protocol__SupportingRow__drop(void *r);
typedef struct jazz_protocol_ResultRowSource { u32 tag; void *inner; } jazz_protocol_ResultRowSource;
typedef struct jazz_protocol_RealRowMemberEntry {
    jazz_protocol_InternStr table;
    jazz_types_RowUuid row_uuid;
    bool has_occurrence_id; jazz_types_OutputOccurrenceId occurrence_id;
    jazz_protocol_OptTxId content_tx;
    u32 layer;
    jazz_protocol_OptTxId deletion_tx;
    jazz_protocol_ResultRowSource source;
    jazz_protocol_ReadViewKey read_view;
    bool has_schema_version; jazz_types_SchemaVersionId schema_version;
    jazz_protocol_OptBytes branch_or_prefix;
    jazz_protocol_OptBytes row_digest;
    jazz_protocol_OptTxId batch;
    bool has_settle_position; jazz_types_GlobalTime settle_position;
} jazz_protocol_RealRowMemberEntry;
typedef struct jazz_protocol_ResultRowEntry { jazz_protocol_InternStr table; jazz_types_RowUuid row; jazz_types_TxId tx; } jazz_protocol_ResultRowEntry;
enum { JAZZ_RME_ROW = 0, JAZZ_RME_SYNTHETIC, JAZZ_RME_PATH_TUPLE, JAZZ_RME_TYPED_ROW };
typedef struct jazz_protocol_ResultMemberEntry {
    u32 tag;
    union {
        jazz_protocol_RealRowMemberEntry row;
        struct { jz_str table; jz_bytes row; jz_bytes replacement; } synthetic;
        struct {
            jz_str path; jazz_protocol_InternStr source_table; jazz_types_RowUuid source_row;
            jazz_protocol_InternStr target_table; jazz_types_RowUuid target_row;
            jazz_protocol_OptBytes edge_id; jz_bytes revision;
        } path_tuple;
        struct { jazz_protocol_RealRowMemberEntry row; jazz_types_ResultKey occurrence_key; } typed;
    } u;
} jazz_protocol_ResultMemberEntry;
typedef JZ_VEC(jazz_protocol_ResultMemberEntry) jazz_protocol_ResultMemberEntryVec;
int jazz_protocol__protocol__ResultMemberEntry__cmp(const void *a, const void *b, void *ctx);
bool jazz_protocol__protocol__ResultMemberEntry__eq(const jazz_protocol_ResultMemberEntry *a, const jazz_protocol_ResultMemberEntry *b);
void jazz_protocol__protocol__ResultMemberEntry__clone(void *dst, const void *src);
void jazz_protocol__protocol__ResultMemberEntry__drop(void *e);
bool jazz_protocol__protocol__ResultMemberEntry__as_row(const jazz_protocol_ResultMemberEntry *e, jazz_protocol_ResultRowEntry *out);
const jazz_protocol_RealRowMemberEntry *jazz_protocol__protocol__ResultMemberEntry__as_real_row(const jazz_protocol_ResultMemberEntry *e);
void jazz_protocol__protocol__ResultMemberEntry__from_real(jazz_protocol_ResultMemberEntry *out, jazz_protocol_RealRowMemberEntry row);
void jazz_protocol__protocol__RealRowMemberEntry__current_content(jazz_protocol_RealRowMemberEntry *out, jazz_protocol_ResultRowEntry row);
void jazz_protocol__protocol__RealRowMemberEntry__with_occurrence_id(jazz_protocol_RealRowMemberEntry *m, jazz_types_OutputOccurrenceId id);
void jazz_protocol__protocol__RealRowMemberEntry__with_settle_position(jazz_protocol_RealRowMemberEntry *m, bool some, jazz_types_GlobalTime pos);
void jazz_protocol__protocol__RealRowMemberEntry__with_row_digest(jazz_protocol_RealRowMemberEntry *m, jz_bytes digest);
/* SyntheticReplacementToken::from_encoded_record */
jz_bytes jazz_protocol__protocol__SyntheticReplacementToken__from_encoded_record(jz_bytes encoded);
typedef struct jazz_protocol_ResultMemberPayloadEntry { jazz_protocol_ResultMemberEntry member; jz_bytes descriptor; jz_bytes record; } jazz_protocol_ResultMemberPayloadEntry;
void jazz_protocol__protocol__ResultMemberPayloadEntry__clone(void *dst, const void *src);
void jazz_protocol__protocol__ResultMemberPayloadEntry__drop(void *e);
typedef struct jazz_protocol_MemberPayloadPair { jazz_protocol_ResultMemberEntry member; jazz_protocol_ResultMemberPayloadEntry payload; } jazz_protocol_MemberPayloadPair;
typedef JZ_VEC(jazz_protocol_MemberPayloadPair) jazz_protocol_MemberPayloadPairVec;
enum { JAZZ_PSR_ROOT = 0, JAZZ_PSR_ALIAS, JAZZ_PSR_RECURSIVE_SEED, JAZZ_PSR_RECURSIVE_STEP, JAZZ_PSR_CORRELATED_CHILD, JAZZ_PSR_POLICY };
typedef struct jazz_protocol_ProgramSourceRole { u32 tag; jz_str name; } jazz_protocol_ProgramSourceRole;
typedef struct jazz_protocol_ProgramSourceId { jazz_protocol_InternStr table; JZ_VEC(jazz_protocol_ProgramSourceRole) path; } jazz_protocol_ProgramSourceId;
int jazz_protocol__protocol__ProgramSourceId__cmp(const void *a, const void *b, void *ctx);
void jazz_protocol__protocol__ProgramSourceId__clone(void *dst, const void *src);
void jazz_protocol__protocol__ProgramSourceId__drop(void *s);
void jazz_protocol__protocol__ProgramSourceId__debug(jz_str *out, const jazz_protocol_ProgramSourceId *s);
typedef struct jazz_protocol_OptU32Enum { bool some; u32 v; } jazz_protocol_OptU32Enum;
enum { JAZZ_EDGE_INCLUDE = 0, JAZZ_EDGE_JOIN, JAZZ_EDGE_RELATION, JAZZ_EDGE_RECURSIVE, JAZZ_EDGE_POLICY };
enum { JAZZ_ROLE_INTERMEDIATE = 0, JAZZ_ROLE_FRONTIER, JAZZ_ROLE_TERMINAL };
typedef struct jazz_protocol_RelationEdgeEntry {
    jz_str path;
    jazz_protocol_InternStr source_table; jazz_types_RowUuid source_row;
    jazz_protocol_InternStr target_table; jazz_types_RowUuid target_row;
    jazz_protocol_OptU32Enum kind;
    bool has_source_version; jazz_protocol_RowVersionRefEntry source_version;
    bool has_target_version; jazz_protocol_RowVersionRefEntry target_version;
    bool has_depth; u32 depth;
    jazz_protocol_OptBytes edge_id;
    jazz_protocol_OptBytes branch;
    jazz_protocol_OptU32Enum role;
    jazz_protocol_OptBytes order;
    jazz_protocol_OptU32Enum hole_state;
} jazz_protocol_RelationEdgeEntry;
typedef struct jazz_protocol_ProgramSourceCoverageEntry { jazz_protocol_ProgramSourceId source; bool complete; } jazz_protocol_ProgramSourceCoverageEntry;
enum { JAZZ_PFE_RESULT_PAYLOAD = 0, JAZZ_PFE_RELATION_EDGE, JAZZ_PFE_PATH_CORRELATION_COVERAGE, JAZZ_PFE_PROGRAM_SOURCE_COVERAGE };
/* only the arms this unit constructs; other arms are opaque here */
typedef struct jazz_protocol_ProgramFactEntry {
    u32 tag;
    union {
        jazz_protocol_ResultMemberPayloadEntry result_payload;
        jazz_protocol_RelationEdgeEntry relation_edge;
        jazz_protocol_ProgramSourceCoverageEntry program_source;
        void *other;
    } u;
} jazz_protocol_ProgramFactEntry;
typedef JZ_VEC(jazz_protocol_ProgramFactEntry) jazz_protocol_ProgramFactEntryVec;
int jazz_protocol__protocol__ProgramFactEntry__cmp(const void *a, const void *b, void *ctx);
void jazz_protocol__protocol__ProgramFactEntry__clone(void *dst, const void *src);
void jazz_protocol__protocol__ProgramFactEntry__drop(void *e);
void jazz_protocol__protocol__ProgramFactEntry__debug(jz_str *out, const jazz_protocol_ProgramFactEntry *e);
void jazz_protocol__protocol__ResultMemberEntry__debug(jz_str *out, const jazz_protocol_ResultMemberEntry *e);

/* --- jazz_engine query_engine schemas (mirror of jazz_engine/query_engine/schemas.h) --- */
typedef JZ_VEC(jz_str) jazz_engine_StrVec;
typedef struct jazz_engine_OptStr { bool some; jz_str s; } jazz_engine_OptStr;
enum { JAZZ_ENGINE_CARRIER_CURRENT_ROW, JAZZ_ENGINE_CARRIER_LOGICAL };
/* CurrentRowPublicationField: ResultField { name, visibility } | UnresolvedSourceCell | StoredColumn */
enum { JZ_CRPF_RESULT_FIELD = 0, JZ_CRPF_UNRESOLVED_SOURCE_CELL = 1, JZ_CRPF_STORED_COLUMN = 2 };
enum { JZ_CRRV_APPLICATION_CELL = 0, JZ_CRRV_PUBLIC_PROVENANCE = 1, JZ_CRRV_HIDDEN_METADATA = 2 };
typedef struct jazz_engine_CurrentRowPublicationField { u32 tag; jz_str name; u32 visibility; jazz_types_PhysicalColumnId id; } jazz_engine_CurrentRowPublicationField;
void jazz_engine_CurrentRowPublicationField_clone(void *dst, const void *src);
void jazz_engine_CurrentRowPublicationField_drop(void *f);
bool jazz_engine__query_engine__publication__CurrentRowPublicationField__public_name(const jazz_engine_CurrentRowPublicationField *f, jz_slice *out);
typedef struct jazz_engine_TypedOutputField { jz_str name; groove_ValueType ty; } jazz_engine_TypedOutputField;
typedef JZ_VEC(jazz_engine_TypedOutputField) jazz_engine_TypedOutputFieldVec;
typedef struct jazz_engine_SyntheticResultMembershipSchema {
    jz_str table_field, row_field, replacement_field;
    jz_btree routing_param_fields;
} jazz_engine_SyntheticResultMembershipSchema;
void jazz_engine_SyntheticResultMembershipSchema_clone(void *dst, const void *src);
void jazz_engine_SyntheticResultMembershipSchema_drop(void *s);
typedef struct jazz_engine_AggregateResultSchema {
    jazz_engine_SyntheticResultMembershipSchema synthetic;
    groove_DescriptorFieldVec group_key_fields;
    jazz_engine_StrVec group_names;
    groove_DescriptorFieldVec value_fields;
    jazz_engine_StrVec value_names;
    jz_btree routing_param_fields;
} jazz_engine_AggregateResultSchema;
enum { JAZZ_ENGINE_ART_DIRECT, JAZZ_ENGINE_ART_ROOT_COLLECTOR, JAZZ_ENGINE_ART_AGGREGATE };
typedef struct jazz_engine_AppRowTerminal { u8 tag; jazz_engine_AggregateResultSchema aggregate; } jazz_engine_AppRowTerminal;
typedef struct jazz_engine_AppRowSchema {
    jz_btree publication_fields; /* String -> CurrentRowPublicationField */
    groove_RecordDescriptor descriptor;
    jz_btree hidden_fields;      /* BTreeSet<String> */
    u8 carrier;
    jz_btree field_carriers;     /* String -> AppRowCarrier(u8) */
    jz_btree public_field_names; /* String -> String */
    jazz_engine_AppRowTerminal terminal;
    bool root_union_arm;
} jazz_engine_AppRowSchema;
void jazz_engine_AppRowSchema_clone(void *dst, const void *src);
void jazz_engine_AppRowSchema_drop(void *s);
typedef struct jazz_engine_ContentVersionFields { jz_str tx_time_field, tx_node_field; } jazz_engine_ContentVersionFields;
typedef struct jazz_engine_VersionIdentityFields {
    jz_str table_field, row_field, tx_time_field, tx_node_field;
    jazz_engine_OptStr batch_id_field, branch_or_prefix_field, row_digest_field;
    jz_str schema_field, layer_field;
} jazz_engine_VersionIdentityFields;
enum { JAZZ_ENGINE_RMV_CONTENT, JAZZ_ENGINE_RMV_CONTENT_OR_DELETION };
typedef struct jazz_engine_ResultMembershipVersionSchema {
    u8 tag;
    jazz_engine_ContentVersionFields content;
    jazz_engine_VersionIdentityFields deletion;
    jz_str deletion_state_field;
} jazz_engine_ResultMembershipVersionSchema;
typedef struct jazz_engine_RowRefSchema { jz_str source_field, table_field, row_field; } jazz_engine_RowRefSchema;
typedef struct jazz_engine_VersionedRowRefSchema {
    jazz_engine_RowRefSchema row;
    bool has_version; jazz_engine_ResultMembershipVersionSchema version;
    jazz_engine_OptStr branch_or_prefix_field;
} jazz_engine_VersionedRowRefSchema;
typedef struct jazz_engine_ResultMembershipSchema {
    jz_str table_field, row_field;
    jazz_engine_StrVec occurrence_id_fields;
    jz_btree occurrence_union_arm_fields; /* usize -> String */
    jazz_engine_TypedOutputFieldVec payload_fields;
    jz_btree payload_publication_fields;
    jazz_engine_OptStr branch_or_prefix_field;
    jazz_engine_ResultMembershipVersionSchema version;
    jazz_engine_OptStr settle_position_field;
    jz_btree routing_param_fields;
} jazz_engine_ResultMembershipSchema;
typedef struct jazz_engine_RelationEdgeSchema {
    jazz_engine_VersionedRowRefSchema source;
    jz_str path_field;
    jazz_engine_VersionedRowRefSchema target;
    jz_str kind_field;
    jazz_engine_OptStr depth_field, edge_id_field, branch_field, role_field, order_field, hole_state_field;
} jazz_engine_RelationEdgeSchema;
typedef struct jazz_engine_VersionWitnessSchema {
    jazz_protocol_ProgramSourceId source;
    groove_RecordDescriptor descriptor;
    jazz_engine_VersionIdentityFields identity;
    jz_str created_by_field, created_at_field, updated_by_field, updated_at_field, authored_columns_field, deletion_field;
    jz_btree user_fields; /* String -> String */
} jazz_engine_VersionWitnessSchema;
typedef struct jazz_engine_VersionWitnessSchemas {
    jz_str role_field;
    bool has_content; jazz_engine_VersionWitnessSchema content;
    bool has_deletion; jazz_engine_VersionWitnessSchema deletion;
    jz_btree routing_param_fields;
} jazz_engine_VersionWitnessSchemas;
typedef struct jazz_engine_ProgramSourceCoverageSchema { jazz_protocol_ProgramSourceId source; bool complete; jz_btree routing_param_fields; } jazz_engine_ProgramSourceCoverageSchema;
/* clone/drop helpers (derive(Clone)) for the schemas this unit retains */
void jazz_engine_ResultMembershipSchema_clone(void *dst, const void *src);
void jazz_engine_ResultMembershipSchema_drop(void *s);
void jazz_engine_AggregateResultSchema_clone(void *dst, const void *src);
void jazz_engine_AggregateResultSchema_drop(void *s);
void jazz_engine_VersionWitnessSchema_clone(void *dst, const void *src);
void jazz_engine_VersionWitnessSchema_drop(void *s);
void jazz_engine_ProgramSourceCoverageSchema_clone(void *dst, const void *src);
void jazz_engine_ProgramSourceCoverageSchema_drop(void *s);
void jazz_engine_RelationEdgeSchema_clone(void *dst, const void *src);
void jazz_engine_RelationEdgeSchema_drop(void *s);
/* ProgramFactKey / ProgramFactTerminal / ProgramFactSchema tags */
enum {
    JAZZ_ENGINE_PFK_RESULT_MEMBERSHIP = 0, JAZZ_ENGINE_PFK_PROGRAM_SOURCE_COVERAGE, JAZZ_ENGINE_PFK_RELATION_EDGES,
    JAZZ_ENGINE_PFK_VERSION_WITNESSES, JAZZ_ENGINE_PFK_REPLACEMENT_WITNESSES, JAZZ_ENGINE_PFK_OTHER
};
typedef struct jazz_engine_ProgramFactKey { u32 tag; void *payload; } jazz_engine_ProgramFactKey;
enum {
    JAZZ_ENGINE_PFT_PRIMARY, JAZZ_ENGINE_PFT_VERSION_WITNESS_CONTENT, JAZZ_ENGINE_PFT_VERSION_WITNESS_DELETION,
    JAZZ_ENGINE_PFT_REPLACEMENT_WITNESS_CONTENT, JAZZ_ENGINE_PFT_REPLACEMENT_WITNESS_DELETION
};
enum {
    JAZZ_ENGINE_PFS_AUTHORIZED_ROWS, JAZZ_ENGINE_PFS_RESULT_MEMBERSHIP, JAZZ_ENGINE_PFS_AGGREGATE_RESULT,
    JAZZ_ENGINE_PFS_RELATION_EDGES, JAZZ_ENGINE_PFS_PATH_CORRELATION_COVERAGE, JAZZ_ENGINE_PFS_PROGRAM_SOURCE_COVERAGE,
    JAZZ_ENGINE_PFS_READ_FRONTIER_SETTLED, JAZZ_ENGINE_PFS_COMPLETE_TX_PAYLOAD_COVERAGE, JAZZ_ENGINE_PFS_VIEW_COMPLETE_EXCLUSIVE_COVERAGE,
    JAZZ_ENGINE_PFS_VERSION_WITNESSES, JAZZ_ENGINE_PFS_REPLACEMENT_WITNESSES
};
typedef struct jazz_engine_ProgramFactSchema {
    u8 tag;
    union {
        jazz_engine_ResultMembershipSchema result_membership;
        jazz_engine_AggregateResultSchema aggregate_result;
        jazz_engine_RelationEdgeSchema relation_edges;
        jazz_engine_ProgramSourceCoverageSchema program_source_coverage;
        jazz_engine_VersionWitnessSchemas witnesses;
        void *other;
    } u;
} jazz_engine_ProgramFactSchema;
typedef struct jazz_engine_ProgramFactOutput { jazz_engine_ProgramFactKey key; u8 terminal; jazz_engine_ProgramFactSchema schema; } jazz_engine_ProgramFactOutput;
enum { JAZZ_ENGINE_OTS_APP_ROWS, JAZZ_ENGINE_OTS_FACT };
typedef struct jazz_engine_OutputTerminalSchema {
    u8 tag;
    union { jazz_engine_AppRowSchema app_rows; jazz_engine_ProgramFactOutput fact; } u;
} jazz_engine_OutputTerminalSchema;
typedef struct jazz_engine_LoweredTerminal { jz_str sink; void *graph; jazz_engine_OutputTerminalSchema output; } jazz_engine_LoweredTerminal;
typedef struct jazz_engine_LoweredProgram {
    JZ_VEC(jazz_engine_LoweredTerminal) terminals;
    jz_btree shared_witness_sinks; /* key -> String sink */
} jazz_engine_LoweredProgram;
typedef struct jazz_engine_QueryProgram { jazz_engine_LoweredProgram lowered; } jazz_engine_QueryProgram;

/* --- groove::ivm terminal output / multisink deltas --- */
enum { GROOVE_TPS_COLLECTION, GROOVE_TPS_KEY };
typedef struct groove_TerminalPathSegment { u32 tag; jz_str name; jz_bytes key; } groove_TerminalPathSegment;
enum { GROOVE_TE_INSERT, GROOVE_TE_UPDATE, GROOVE_TE_REMOVE, GROOVE_TE_MOVE };
typedef struct groove_TerminalEdit { u32 tag; jz_bytes key; jz_bytes value; usize index; } groove_TerminalEdit;
typedef struct groove_TerminalOperation {
    jz_bytes root_key;
    groove_RecordDescriptor root_descriptor;
    JZ_VEC(groove_TerminalPathSegment) path;
    groove_TerminalEdit edit;
} groove_TerminalOperation;
typedef JZ_VEC(groove_TerminalOperation) groove_TerminalOperationVec;
void groove_TerminalOperation_drop(void *t);
typedef struct groove_RecordDeltas groove_RecordDeltas;
typedef struct groove_RecordDeltasIter { const groove_RecordDeltas *d; u32 i; } groove_RecordDeltasIter;
bool groove__ivm__RecordDeltas__next(groove_RecordDeltasIter *it, groove_BorrowedRecord *record, i64 *weight);
bool groove__ivm__RecordDeltas__is_empty(const groove_RecordDeltas *d);
u32 groove__ivm__RecordDeltas__len(const groove_RecordDeltas *d);
/* MultisinkDeltas { sinks: BTreeMap<String, RecordDeltas>, terminal_sinks: BTreeMap<String, TerminalDeltas> } */
typedef struct groove_MultisinkBatch { u32 sinks_len; u32 terminal_sinks_len; void *repr; } groove_MultisinkBatch;
/* take the i-th terminal sink (moves its operations out) */
void groove__ivm__MultisinkBatch__take_terminal_sink(groove_MultisinkBatch *b, u32 i, const jz_str **sink, groove_TerminalOperationVec *ops);
void groove__ivm__MultisinkBatch__sink(const groove_MultisinkBatch *b, u32 i, const jz_str **sink, const groove_RecordDeltas **deltas);
void groove_MultisinkBatch_drop(groove_MultisinkBatch *b);

/* --- groove::records extras --- */
jz_err groove__records__decode_persisted_record_descriptor(jz_slice bytes, groove_RecordDescriptor *out);
void groove__records__DescriptorField__new(groove_DescriptorField *out, jz_str name, groove_ValueType t);
void groove_DescriptorField_clone(void *dst, const void *src);
typedef struct groove_RecordProjector groove_RecordProjector;
typedef struct { u32 from, to; } groove_FieldPair;
jz_err groove__records__RecordProjector__new_registry_rebound(groove_RecordDescriptor src, groove_RecordDescriptor dst,
    const groove_FieldPair *pairs, u32 n, groove_RecordProjector **out);
void groove_RecordProjector_drop(groove_RecordProjector *p);
jz_err groove__records__values__EnumSchema__case(const groove_EnumSchema *s, u32 tag, const groove_EnumCase **out);
void groove__records__EnumValue__new(groove_EnumValue *out, u32 tag, groove_OwnedRecord record);
u64 groove__records__RecordDescriptor__hash(groove_RecordDescriptor d);

/* --- blake3 --- */
void jz_blake3_derive_key(const char *context, const u8 *p, usize n, u8 out[32]);

/* --- jazz_model runtime schema --- */
jz_err jazz_model__schema__RuntimeSchema__decode_persisted_branch_key(const jazz_model_TableSchema *t, jz_slice bytes, jazz_model_BranchKey *out);
bool jazz_model_BranchKey_eq(const jazz_model_BranchKey *a, const jazz_model_BranchKey *b);

/* --- node::node_aliases --- */
u64 jazz_node__node__node_aliases__NodeAliases__retarget_count(const jazz_node_NodeAliases *a);
usize jazz_node__node__node_aliases__NodeAliases__len(const jazz_node_NodeAliases *a);

/* --- node::supporting_frontier (core-b) --- */
typedef struct jazz_node_SupportingFrontier {
    jz_btree weights;          /* SupportingRow -> [i64; 4] */
    bool has_unpublished;
    jz_btree unpublished;      /* SupportingRow -> bool */
} jazz_node_SupportingFrontier;
void jazz_node__node__supporting_frontier__SupportingFrontier__default(jazz_node_SupportingFrontier *f);
u32 jazz_node__node__supporting_frontier__SupportingFrontier__apply(jazz_node_SupportingFrontier *f, usize origin, jazz_protocol_SupportingRow row, i64 weight);
typedef JZ_VEC(const jazz_protocol_SupportingRow *) jzn_SupportingRowRefVec;
void jazz_node__node__supporting_frontier__SupportingFrontier__rows(const jazz_node_SupportingFrontier *f, jzn_SupportingRowRefVec *out);
void jazz_node__node__supporting_frontier__SupportingFrontier__acknowledged_rows(const jazz_node_SupportingFrontier *f, jzn_SupportingRowRefVec *out);
bool jazz_node__node__supporting_frontier__SupportingFrontier__delta(const jazz_node_SupportingFrontier *f, jazz_protocol_SupportingRowVec *adds, jazz_protocol_SupportingRowVec *removes);
void jazz_node__node__supporting_frontier__SupportingFrontier__acknowledge(jazz_node_SupportingFrontier *f);
void jazz_node__node__supporting_frontier__SupportingFrontier__forget_predecessor(jazz_node_SupportingFrontier *f);
usize jazz_node__node__supporting_frontier__SupportingFrontier__retained_bytes(const jazz_node_SupportingFrontier *f);
void jazz_node__node__supporting_frontier__SupportingFrontier__clone(jazz_node_SupportingFrontier *dst, const jazz_node_SupportingFrontier *src);
void jazz_node__node__supporting_frontier__SupportingFrontier__drop(jazz_node_SupportingFrontier *f);

/* --- node::terminal_root / terminal_record (core-b) --- */
typedef enum { JZN_CARRIER_CURRENT_ROW, JZN_CARRIER_LOGICAL } jazz_node_TerminalRootCarrier;
typedef struct jazz_node_TerminalRootPublicField {
    jazz_engine_CurrentRowPublicationField publication;
    jz_str name;
    jz_str descriptor_field_name;
    usize slot;
    jazz_node_TerminalRootCarrier carrier;
} jazz_node_TerminalRootPublicField;
typedef JZ_VEC(jazz_node_TerminalRootPublicField) jazz_node_TerminalRootPublicFieldVec;
typedef struct jazz_node_TerminalRootLayout {
    jz_str id;
    groove_RecordDescriptor root_descriptor;
    usize root_key_slot;
    jz_str root_key_field_name;
    jazz_node_TerminalRootPublicFieldVec public_fields;
    jazz_node_TerminalRootCarrier carrier;
    bool root_union_arm;
} jazz_node_TerminalRootLayout;
void jazz_node_TerminalRootLayout_clone(void *dst, const void *src);
void jazz_node_TerminalRootLayout_drop(void *l);
typedef struct jazz_node_TerminalRecordState jazz_node_TerminalRecordState;
jz_err jazz_node__node__terminal_record__TerminalRecordState__new(groove_OwnedRecord record, jazz_node_TerminalRecordState **out);
jz_err jazz_node__node__terminal_record__TerminalRecordState__apply(jazz_node_TerminalRecordState *s, const groove_TerminalOperation *op);
jz_err jazz_node__node__terminal_record__TerminalRecordState__update_record(jazz_node_TerminalRecordState *s, groove_OwnedRecord record);
jz_err jazz_node__node__terminal_record__TerminalRecordState__record(const jazz_node_TerminalRecordState *s, groove_OwnedRecord *out);
usize jazz_node__node__terminal_record__TerminalRecordState__retained_bytes(const jazz_node_TerminalRecordState *s);
void jazz_node__node__terminal_record__TerminalRecordState__clone_box(void *dst, const void *src); /* Box clone */
void jazz_node__node__terminal_record__TerminalRecordState__drop_box(void *p);              /* Box drop */

/* --- node::descriptor_roles (core-b) --- */
typedef struct jazz_node_PreparedAggregatePayload { jz_bytes descriptor; jz_bytes record; jz_bytes row_identity; jz_bytes replacement_identity; } jazz_node_PreparedAggregatePayload;
jz_err jazz_node__node__descriptor_roles__encode_aggregate_payload_record(const groove_BorrowedRecord *record, const jazz_engine_AggregateResultSchema *schema,
    jazz_node_PreparedAggregatePayload *out);
typedef struct jazz_node_CurrentPayloadEncodePlan { groove_RecordProjector *projector; jz_bytes descriptor; } jazz_node_CurrentPayloadEncodePlan;
jz_err jazz_node__node__descriptor_roles__CurrentPayloadEncodePlan__new(groove_RecordDescriptor descriptor, const jazz_engine_ResultMembershipSchema *schema,
    jazz_node_CurrentPayloadEncodePlan *out);
jz_err jazz_node__node__descriptor_roles__CurrentPayloadEncodePlan__encode(const jazz_node_CurrentPayloadEncodePlan *p, const groove_BorrowedRecord *record,
    jz_bytes *descriptor_out, jz_bytes *record_out);
void jazz_node__node__descriptor_roles__CurrentPayloadEncodePlan__drop(void *p);

/* --- debug_env --- */
bool jazz_node__debug_env__covered_input_trace(void);
void jz_eprint(const jz_str *line);

#endif
