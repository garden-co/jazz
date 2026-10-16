/* Mirror declarations of types and functions that unit groove-ivm-rt-b uses
 * from OTHER units (records, schema, storage, chunks, large_values, ivm graph /
 * op_types / template, and the sibling unit groove-ivm-rt-a: subscriptions,
 * runtime_tick, evaluator, record_projection). Layouts follow the Rust fields
 * roughly; names follow PORTING.md so links resolve. */
#ifndef GROOVE_EXT_GROOVE_IVM_RT_B_H
#define GROOVE_EXT_GROOVE_IVM_RT_B_H
#include "jz_rt.h"

/* ======================= groove::records ======================= */
typedef struct groove_RecordDescriptorData groove_RecordDescriptorData;
/* RecordDescriptor(Intern<RecordDescriptorData>): Copy, compared by pointer. */
typedef const groove_RecordDescriptorData *groove_RecordDescriptor;
typedef struct groove_Value groove_Value;
typedef struct groove_ValueType groove_ValueType;
typedef struct groove_EnumSchema groove_EnumSchema;
typedef struct groove_ScalarEnumSchema groove_ScalarEnumSchema;
typedef struct groove_LargeValueRef groove_LargeValueRef;
typedef struct groove_PreparedProjection groove_PreparedProjection;
typedef struct groove_RawProjectionScratch groove_RawProjectionScratch;

typedef struct { u32 a, b; } groove_UsizePair;
typedef JZ_VEC(u32) groove_U32Vec;
typedef JZ_VEC(jz_bytes) groove_BytesVec;
typedef JZ_VEC(jz_str) groove_StrVec;
typedef struct groove_OwnedRecord { jz_bytes raw; groove_RecordDescriptor descriptor; } groove_OwnedRecord;
typedef struct groove_BorrowedRecord { jz_slice raw; groove_RecordDescriptor descriptor; } groove_BorrowedRecord;
typedef struct groove_EnumValue { u32 tag; groove_OwnedRecord record; } groove_EnumValue;

typedef enum {
    groove_Value_U8, groove_Value_U16, groove_Value_U32, groove_Value_U64, groove_Value_F64,
    groove_Value_Bool, groove_Value_String, groove_Value_Bytes, groove_Value_Large, groove_Value_Uuid,
    groove_Value_EnumTag, groove_Value_Tuple, groove_Value_Array, groove_Value_Nullable,
    groove_Value_I64, groove_Value_I32, groove_Value_Record, groove_Value_Enum, groove_Value_U48,
} groove_ValueTag;
typedef JZ_VEC(groove_Value) groove_ValueVec;
struct groove_Value {
    u8 tag;
    union {
        u8 u8v; u16 u16v; u32 u32v; u64 u64v; f64 f64v; bool b; i64 i64v; i32 i32v;
        jz_str s; jz_bytes bytes; groove_LargeValueRef *large; jz_uuid uuid;
        groove_ValueVec vals;        /* Tuple / Array */
        groove_Value *nullable;      /* Nullable: NULL = None */
        groove_OwnedRecord record;
        groove_EnumValue enm;
    } v;
};
void groove_Value_drop(groove_Value *v);
void groove_Value_drop_v(void *v);
void groove_Value_clone(groove_Value *dst, const groove_Value *src);
bool groove_Value_eq(const groove_Value *a, const groove_Value *b);
void groove_Value_debug(jz_str *out, const groove_Value *v);

typedef enum {
    groove_ValueType_U8, groove_ValueType_U16, groove_ValueType_U32, groove_ValueType_U64,
    groove_ValueType_I32, groove_ValueType_I64, groove_ValueType_F64, groove_ValueType_Bool,
    groove_ValueType_String, groove_ValueType_Bytes, groove_ValueType_Internal, groove_ValueType_Uuid,
    groove_ValueType_EnumTag, groove_ValueType_Tuple, groove_ValueType_Array, groove_ValueType_Nullable,
    groove_ValueType_Record, groove_ValueType_Enum, groove_ValueType_U48,
} groove_ValueTypeTag;
struct groove_ValueType {
    u8 tag;
    union {
        u8 internal; /* InternalValueTypeRepr */
        groove_ScalarEnumSchema *enum_tag;
        JZ_VEC(groove_ValueType) tuple;
        groove_ValueType *inner;            /* Array / Nullable */
        groove_RecordDescriptor *record;
        groove_EnumSchema *enm;
    } v;
};
void groove_ValueType_drop(groove_ValueType *t);
void groove_ValueType_clone(groove_ValueType *dst, const groove_ValueType *src);
bool groove_ValueType_eq(const groove_ValueType *a, const groove_ValueType *b);
void groove_ValueType_debug(jz_str *out, const groove_ValueType *t);
u64 groove_ValueType_hash(const groove_ValueType *t, u64 h);

typedef struct groove_EnumCase { jz_str name; groove_RecordDescriptor payload; } groove_EnumCase;
struct groove_EnumSchema { u64 registry_id; jz_str name; JZ_VEC(groove_EnumCase) cases; };
bool groove_EnumSchema_eq(const groove_EnumSchema *a, const groove_EnumSchema *b);
jz_err groove__records__values__EnumSchema__tag(const groove_EnumSchema *s, const char *name, u32 len, u32 *out);

typedef enum { groove_FieldIdentity_Name, groove_FieldIdentity_Slot, groove_FieldIdentity_NamedSlot } groove_FieldIdentityTag;
typedef struct groove_FieldIdentity { u8 tag; jz_str name; u64 slot; } groove_FieldIdentity;
void groove_FieldIdentity_drop(groove_FieldIdentity *f);
void groove_FieldIdentity_clone(groove_FieldIdentity *dst, const groove_FieldIdentity *src);
bool groove_FieldIdentity_eq(const groove_FieldIdentity *a, const groove_FieldIdentity *b);
u64 groove_FieldIdentity_hash(const groove_FieldIdentity *f, u64 h);

typedef struct groove_DescriptorField {
    bool has_name; jz_str name;
    bool has_identity; groove_FieldIdentity identity;
    groove_ValueType value_type;
} groove_DescriptorField;
typedef JZ_VEC(groove_DescriptorField) groove_DescriptorFieldVec;

/* RecordDescriptor methods */
groove_RecordDescriptor groove__records__RecordDescriptor__new(const groove_DescriptorField *fields, u32 n);
groove_RecordDescriptor groove__records__RecordDescriptor__empty(void);
u32 groove__records__RecordDescriptor__len(groove_RecordDescriptor d);
const groove_DescriptorField *groove__records__RecordDescriptor__fields(groove_RecordDescriptor d, u32 *n);
const groove_DescriptorField *groove__records__RecordDescriptor__field(groove_RecordDescriptor d, u32 idx);
bool groove__records__RecordDescriptor__field_index(groove_RecordDescriptor d, const char *name, u32 name_len, u32 *idx);
const groove_ValueType *groove__records__RecordDescriptor__field_type(groove_RecordDescriptor d, u32 idx);
bool groove__records__RecordDescriptor__can_evolve_registry_to(groove_RecordDescriptor a, groove_RecordDescriptor b);
bool groove__records__RecordDescriptor__registry_compatible(groove_RecordDescriptor a, groove_RecordDescriptor b);
jz_err groove__records__RecordDescriptor__create(groove_RecordDescriptor d, const groove_Value *values, u32 n, jz_bytes *out);
jz_err groove__records__RecordDescriptor__create_into(groove_RecordDescriptor d, const groove_Value *values, u32 n, jz_bytes *out);
void groove__records__RecordDescriptor__debug(jz_str *out, groove_RecordDescriptor d);
u64 groove__records__RecordDescriptor__hash(groove_RecordDescriptor d, u64 h);
void groove__records__RecordDescriptor__encode_postcard(groove_RecordDescriptor d, jz_pc_w *w);
jz_err groove__records__RecordDescriptor__validate(groove_RecordDescriptor d, jz_slice raw);
groove_RecordDescriptor groove__records__RecordDescriptor__with_fields(groove_RecordDescriptor d, groove_DescriptorFieldVec *fields);
groove_RecordDescriptor groove__records__RecordDescriptor__concat(groove_RecordDescriptor a, groove_RecordDescriptor b);

/* BorrowedRecord / OwnedRecord */
groove_BorrowedRecord groove__records__BorrowedRecord__new(jz_slice raw, groove_RecordDescriptor d);
jz_err groove__records__BorrowedRecord__get_idx(const groove_BorrowedRecord *r, u32 idx, groove_Value *out);
jz_err groove__records__BorrowedRecord__field_bytes(const groove_BorrowedRecord *r, u32 idx, jz_slice *out);
jz_err groove__records__BorrowedRecord__is_null_idx(const groove_BorrowedRecord *r, u32 idx, bool *out);
jz_err groove__records__BorrowedRecord__to_values(const groove_BorrowedRecord *r, groove_ValueVec *out);
jz_err groove__records__BorrowedRecord__get_u64(const groove_BorrowedRecord *r, u32 idx, u64 *out);
jz_err groove__records__BorrowedRecord__get_str(const groove_BorrowedRecord *r, u32 idx, jz_slice *out);
jz_err groove__records__BorrowedRecord__get_bytes(const groove_BorrowedRecord *r, u32 idx, jz_slice *out);
void groove_OwnedRecord_drop(groove_OwnedRecord *r);
void groove_OwnedRecord_clone(groove_OwnedRecord *dst, const groove_OwnedRecord *src);
jz_err groove__records__concat_records(groove_RecordDescriptor out_desc, const groove_BorrowedRecord *left, const groove_BorrowedRecord *right, jz_bytes *out);
jz_err groove__records__project_raw(const groove_PreparedProjection *p, jz_slice raw, groove_RawProjectionScratch *scratch, jz_bytes *out);
jz_err groove__records__collect_by_ordered_scalar(const groove_Value *v, jz_bytes *out);
jz_err groove__records__encode_value_into(const groove_ValueType *t, const groove_Value *v, jz_bytes *out);
jz_err groove__records__value_matches_type(const groove_Value *v, const groove_ValueType *t, bool *out);

/* typed BorrowedRecord getters (records/mod.rs) */
#define GROOVE_GETTER(name, T) jz_err groove__records__BorrowedRecord__get_##name(const groove_BorrowedRecord *r, u32 idx, T *out); \
    jz_err groove__records__BorrowedRecord__get_nullable_##name(const groove_BorrowedRecord *r, u32 idx, bool *some, T *out);
GROOVE_GETTER(u8, u8)
GROOVE_GETTER(u16, u16)
GROOVE_GETTER(u32, u32)
GROOVE_GETTER(u48, u64)
GROOVE_GETTER(i32, i32)
GROOVE_GETTER(i64, i64)
GROOVE_GETTER(f64, f64)
GROOVE_GETTER(bool, bool)
GROOVE_GETTER(uuid, jz_uuid)
GROOVE_GETTER(enum, u8)
#undef GROOVE_GETTER
jz_err groove__records__BorrowedRecord__get_nullable_u64(const groove_BorrowedRecord *r, u32 idx, bool *some, u64 *out);
jz_err groove__records__BorrowedRecord__get_nullable_string(const groove_BorrowedRecord *r, u32 idx, bool *some, jz_slice *out);
jz_err groove__records__BorrowedRecord__get_nullable_bytes(const groove_BorrowedRecord *r, u32 idx, bool *some, jz_slice *out);
jz_err groove__records__BorrowedRecord__field(const groove_BorrowedRecord *r, u32 idx, const groove_DescriptorField **out);
jz_err groove__records__u48_be_bytes(u64 v, u8 out[6]);
jz_err groove__records__RecordDescriptor__project_record_raw(groove_RecordDescriptor out, const groove_RecordDescriptor *inputs, const jz_slice *raws, u32 ninputs, const groove_UsizePair *mapping, u32 nmap, jz_bytes *result);
jz_err groove__records__RecordDescriptor__from_postcard(jz_slice bytes, groove_RecordDescriptor *out);
jz_err groove__records__Error__invalid_f64_nan(void);
jz_err groove__records__Error__field_not_found(const char *p, u32 n);
/* records::Error codes (only those this unit raises directly) */
enum { GROOVE_RECORDS_ERR_FIELD_NOT_FOUND = 0x0300, GROOVE_RECORDS_ERR_TYPE_MISMATCH, GROOVE_RECORDS_ERR_UNEXPECTED_EOF, GROOVE_RECORDS_ERR_INVALID_UTF8 };

/* ======================= groove::schema ======================= */
typedef struct groove_IndexSchema { jz_str name; JZ_VEC(jz_str) columns; bool unique; } groove_IndexSchema;
typedef struct groove_TableVariantField { jz_str name; groove_ValueType value_type; bool has_shared; jz_str shared_column; } groove_TableVariantField;
typedef struct groove_TableVariant { u32 tag; JZ_VEC(jz_str) fields; JZ_VEC(groove_TableVariantField) payload_fields; } groove_TableVariant;
typedef struct groove_ColumnSchema { jz_str name; groove_ValueType column_type; } groove_ColumnSchema;
typedef struct groove_PrimaryKeyColumn { jz_str column; u8 key_type; } groove_PrimaryKeyColumn;
typedef struct groove_PrimaryKey { JZ_VEC(groove_PrimaryKeyColumn) columns; bool generated; } groove_PrimaryKey;
typedef struct groove_TableSchema {
    jz_str name;
    u64 variant_registry_id;
    jz_btree value_variant_registries;
    JZ_VEC(groove_ColumnSchema) columns;
    bool has_primary_key; groove_PrimaryKey primary_key;
    JZ_VEC(groove_IndexSchema) indices;
    jz_vec foreign_keys;
    JZ_VEC(groove_TableVariant) variants;
} groove_TableSchema;
typedef struct groove_DatabaseSchema { JZ_VEC(groove_TableSchema) tables; jz_vec direct_record_stores; } groove_DatabaseSchema;
void groove_DatabaseSchema_drop(groove_DatabaseSchema *s);
void groove_DatabaseSchema_clone(groove_DatabaseSchema *dst, const groove_DatabaseSchema *src);
void groove_TableSchema_clone(groove_TableSchema *dst, const groove_TableSchema *src);
void groove_TableSchema_drop(groove_TableSchema *t);
void groove_IndexSchema_clone(groove_IndexSchema *dst, const groove_IndexSchema *src);
void groove_IndexSchema_drop(groove_IndexSchema *t);
bool groove_IndexSchema_eq(const groove_IndexSchema *a, const groove_IndexSchema *b);
const groove_TableSchema *groove__schema__DatabaseSchema__table(const groove_DatabaseSchema *s, const char *name, u32 len);
groove_TableSchema *groove__schema__DatabaseSchema__table_mut(groove_DatabaseSchema *s, const char *name, u32 len);
groove_RecordDescriptor groove__schema__TableSchema__record_schema(const groove_TableSchema *t);
bool groove__schema__TableSchema__record_schema_for_variant(const groove_TableSchema *t, u32 tag, groove_RecordDescriptor *out);
bool groove__schema__TableSchema__has_variants(const groove_TableSchema *t);
const groove_IndexSchema *groove__schema__TableSchema__index(const groove_TableSchema *t, const char *name, u32 len);
jz_err groove__schema__TableSchema__validate(const groove_TableSchema *t);
bool groove__schema__TableSchema__primary_key_columns(const groove_TableSchema *t, groove_StrVec *out);
const groove_TableVariant *groove__schema__TableSchema__variant(const groove_TableSchema *t, u32 tag);

/* ======================= groove::storage ======================= */
typedef enum { groove_ScanDirection_Forward, groove_ScanDirection_Reverse } groove_ScanDirection;
typedef enum { groove_ScanBounds_Prefix, groove_ScanBounds_Range } groove_ScanBoundsTag;
typedef struct groove_ScanBounds { u8 tag; jz_bytes start; /* prefix */ jz_bytes end; } groove_ScanBounds;
typedef struct groove_ScanRequest {
    jz_str cf;
    groove_ScanBounds bounds;
    groove_ScanDirection direction;
    bool has_max_items; u32 max_items;
} groove_ScanRequest;
void groove_ScanRequest_drop(groove_ScanRequest *r);
void groove_ScanRequest_clone(groove_ScanRequest *dst, const groove_ScanRequest *src);
bool groove_ScanRequest_eq(const groove_ScanRequest *a, const groove_ScanRequest *b);
int groove_ScanRequest_cmp(const groove_ScanRequest *a, const groove_ScanRequest *b);
u64 groove_ScanRequest_hash(const groove_ScanRequest *r, u64 h);
groove_ScanRequest groove__storage__ScanRequest__prefix(jz_str cf, jz_bytes prefix);
groove_ScanRequest groove__storage__ScanRequest__range(jz_str cf, jz_bytes start, jz_bytes end);
void groove__storage__ScanRequest__with_max_items(groove_ScanRequest *r, u32 n);
void groove__storage__ScanRequest__reversed(groove_ScanRequest *r);
typedef struct groove_KvPair { jz_bytes key; jz_bytes value; } groove_KvPair;
typedef JZ_VEC(groove_KvPair) groove_KvPairVec;
void groove_KvPair_drop(void *p);
void groove_KvPairVec_drop(groove_KvPairVec *v);
typedef enum { groove_WriteOp_Set, groove_WriteOp_Delete } groove_WriteOpTag;
typedef struct groove_OwnedWriteOperation { u8 tag; jz_str cf; jz_bytes key; jz_bytes value; } groove_OwnedWriteOperation;
typedef JZ_VEC(groove_OwnedWriteOperation) groove_WriteOpVec;
void groove_OwnedWriteOperation_drop(void *op);
/* dyn OrderedKvStorage as a vtable (one C copy for every S: Storage).
 * Async methods complete synchronously (host imports). */
typedef struct groove_StorageVt {
    jz_err (*get)(void *self, const jz_str *cf, jz_slice key, bool *found, jz_bytes *out);
    jz_err (*scan)(void *self, const groove_ScanRequest *req, groove_KvPairVec *out); /* scan + collect_scan */
    jz_err (*set)(void *self, const jz_str *cf, jz_slice key, jz_slice value);
    jz_err (*del)(void *self, const jz_str *cf, jz_slice key);
    jz_err (*write_batch)(void *self, const groove_WriteOpVec *ops);
} groove_StorageVt;
typedef struct groove_Storage { const groove_StorageVt *vt; void *self; } groove_Storage;
/* RecordStore (per-table record view) */
typedef struct groove_RecordStoreVt {
    jz_err (*get)(void *self, jz_slice key, bool *found, jz_bytes *out);
    jz_err (*scan)(void *self, const groove_ScanRequest *req, groove_KvPairVec *out);
} groove_RecordStoreVt;
typedef struct groove_RecordStore { const groove_RecordStoreVt *vt; void *self; } groove_RecordStore;
enum { GROOVE_STORAGE_ERR_BASE = 0x0400, GROOVE_STORAGE_ColumnFamilyNotFound, GROOVE_STORAGE_InvalidStorageLayout, GROOVE_STORAGE_InvalidStorageKey, GROOVE_STORAGE_Record, GROOVE_STORAGE_Backend };
jz_err groove__storage__Error__invalid_layout(jz_str message); /* "invalid storage layout: {0}" */

/* ======================= groove::chunks / large_values ======================= */
typedef struct groove_ChunkProviderVt {
    bool (*get)(void *self, jz_slice chunk_id, jz_bytes *out);
    void (*drop)(void *self);
} groove_ChunkProviderVt;
typedef struct groove_OwnedChunkProvider { const groove_ChunkProviderVt *vt; void *rc; } groove_OwnedChunkProvider;
groove_OwnedChunkProvider groove__chunks__OwnedChunkProvider__default(void);
groove_OwnedChunkProvider groove__chunks__OwnedChunkProvider__new(const groove_ChunkProviderVt *vt, void *rc);
groove_OwnedChunkProvider groove__chunks__OwnedChunkProvider__clone(const groove_OwnedChunkProvider *p);
void groove_OwnedChunkProvider_drop(groove_OwnedChunkProvider *p);
bool groove__chunks__OwnedChunkProvider__get(const groove_OwnedChunkProvider *p, jz_slice chunk_id, jz_bytes *out);
typedef struct groove_ChunkId { u8 b[32]; } groove_ChunkId;
jz_err groove__large_values__LargeValueRef__chunk_ids(const groove_LargeValueRef *r, void *out);
jz_err groove__large_values__resolve(const groove_LargeValueRef *r, const groove_OwnedChunkProvider *chunks, groove_Value *out);

/* ======================= groove::ivm (graph, op_types, template) ======================= */
typedef u64 groove_NodeId;
u64 groove_NodeId_hash(const void *k);
bool groove_NodeId_eq(const void *a, const void *b);
typedef struct groove_FrontierName { jz_str name; } groove_FrontierName;
typedef struct groove_InputSourceId { u64 runtime_namespace; u64 local; } groove_InputSourceId;
typedef enum { groove_BindingSourceKey_Prepared, groove_BindingSourceKey_RuntimeInput } groove_BindingSourceKeyTag;
typedef struct groove_BindingSourceKey { u8 tag; jz_str prepared; u64 runtime_namespace; u64 local; } groove_BindingSourceKey;
void groove_BindingSourceKey_clone(groove_BindingSourceKey *dst, const groove_BindingSourceKey *src);
void groove_BindingSourceKey_drop(groove_BindingSourceKey *k);
bool groove_BindingSourceKey_eq(const groove_BindingSourceKey *a, const groove_BindingSourceKey *b);
int groove_BindingSourceKey_cmp(const groove_BindingSourceKey *a, const groove_BindingSourceKey *b);
u64 groove_BindingSourceKey_hash(const groove_BindingSourceKey *k);
void groove_BindingSourceKey_debug(jz_str *out, const groove_BindingSourceKey *k);
groove_BindingSourceKey groove__ivm__graph__InputSourceId__binding_key(groove_InputSourceId id);
bool groove__ivm__graph__InputSourceId__belongs_to(groove_InputSourceId id, u64 ns);
bool groove__ivm__graph__InputSourceId__was_allocated_by(groove_InputSourceId id, u64 ns, u64 next_local);
jz_str groove__ivm__graph__InputSourceId__diagnostic_name(groove_InputSourceId id);

typedef struct groove_LiteralValue groove_LiteralValue;
typedef JZ_VEC(groove_LiteralValue) groove_LiteralVec;
typedef enum {
    groove_Lit_U8, groove_Lit_U16, groove_Lit_U32, groove_Lit_U64, groove_Lit_U48, groove_Lit_I64,
    groove_Lit_I32, groove_Lit_F64, groove_Lit_Bool, groove_Lit_EnumTag, groove_Lit_String,
    groove_Lit_Bytes, groove_Lit_Uuid, groove_Lit_Tuple, groove_Lit_Array, groove_Lit_Nullable,
    groove_Lit_NativeRecord, groove_Lit_Record,
} groove_LiteralTag;
struct groove_LiteralValue {
    u8 tag;
    union {
        u64 u; i64 i; bool b; jz_str s; jz_bytes bytes; jz_uuid uuid;
        groove_LiteralVec vals; groove_LiteralValue *nullable;
        struct { jz_bytes descriptor; jz_bytes raw; } native;
    } v;
};
void groove_LiteralValue_drop(groove_LiteralValue *l);
void groove_LiteralValue_clone(groove_LiteralValue *dst, const groove_LiteralValue *src);
bool groove_LiteralValue_eq(const groove_LiteralValue *a, const groove_LiteralValue *b);
void groove__ivm__op_types__LiteralValue__to_value(const groove_LiteralValue *l, groove_Value *out);
void groove__ivm__op_types__LiteralValue__from_value(groove_Value v, groove_LiteralValue *out);
bool groove__ivm__op_types__LiteralValue__value_type(const groove_LiteralValue *l, groove_ValueType *out);

typedef enum {
    groove_Scan_Point, groove_Scan_Prefix, groove_Scan_PrefixLimit, groove_Scan_ReversePrefixLimit, groove_Scan_Range,
} groove_StaticScanTag;
typedef struct groove_StaticScanSpec { u8 tag; groove_LiteralVec a; groove_LiteralVec b; u32 max_items; } groove_StaticScanSpec;
void groove_StaticScanSpec_clone(groove_StaticScanSpec *dst, const groove_StaticScanSpec *src);
void groove_StaticScanSpec_drop(groove_StaticScanSpec *s);
bool groove_StaticScanSpec_eq(const groove_StaticScanSpec *a, const groove_StaticScanSpec *b);

typedef enum { groove_VPT_Named, groove_VPT_SchemaIndex } groove_VariantProjectionTargetTag;
typedef struct groove_VariantProjectionTarget { u8 tag; jz_str name; } groove_VariantProjectionTarget;
void groove_VariantProjectionTarget_clone(groove_VariantProjectionTarget *dst, const groove_VariantProjectionTarget *src);
bool groove_VariantProjectionTarget_eq(const groove_VariantProjectionTarget *a, const groove_VariantProjectionTarget *b);

typedef enum { groove_ValueComparison_Exact, groove_ValueComparison_Policy } groove_ValueComparison;

typedef enum { groove_FieldRef_Name, groove_FieldRef_StoredName, groove_FieldRef_Resolved } groove_FieldRefTag;
typedef struct groove_FieldRef { u8 tag; jz_str name; u32 idx; } groove_FieldRef;
typedef JZ_VEC(groove_FieldRef) groove_FieldRefVec;
void groove_FieldRef_clone(groove_FieldRef *dst, const groove_FieldRef *src);
void groove_FieldRef_drop(groove_FieldRef *f);
jz_str groove__ivm__graph__FieldRef__display_name(const groove_FieldRef *f);

typedef struct groove_RecursiveEnumRemaps { jz_btree scalar; jz_btree payload; jz_btree payload_children; } groove_RecursiveEnumRemaps;
typedef enum {
    groove_PlanExpr_Field, groove_PlanExpr_Literal, groove_PlanExpr_Null, groove_PlanExpr_Nullable,
    groove_PlanExpr_NullableFlat, groove_PlanExpr_EnumTagRemap, groove_PlanExpr_EnumRemap, groove_PlanExpr_RecursiveEnumRemap,
} groove_PlanExprTag;
typedef struct groove_PlanExpr {
    u8 tag;
    jz_str field;
    groove_LiteralValue literal;
    groove_ValueType null_type;
    JZ_VEC(i16) tags8;   /* Option<u8>: -1 = None */
    JZ_VEC(i64) tags32;  /* Option<u32>: -1 = None */
    groove_RecursiveEnumRemaps *remaps;
    bool omit_unrepresentable;
} groove_PlanExpr;
typedef JZ_VEC(groove_PlanExpr) groove_PlanExprVec;
void groove_PlanExpr_clone(groove_PlanExpr *dst, const groove_PlanExpr *src);
void groove_PlanExpr_drop(groove_PlanExpr *e);

typedef struct groove_PredicateExpr groove_PredicateExpr;
typedef enum {
    groove_Pred_TemplateArgument, groove_Pred_Eq, groove_Pred_Neq, groove_Pred_Contains, groove_Pred_EqField,
    groove_Pred_ContainsField, groove_Pred_NeqField, groove_Pred_Gt, groove_Pred_GtEq, groove_Pred_Lt,
    groove_Pred_LtEq, groove_Pred_IsNull, groove_Pred_IsNotNull, groove_Pred_EnumMatch, groove_Pred_And, groove_Pred_Or,
} groove_PredicateTag;
struct groove_PredicateExpr {
    u8 tag;
    u32 slot;                  /* TemplateArgument */
    JZ_VEC(jz_str) fields;     /* TemplateArgument */
    jz_str field;
    jz_str value_field;        /* EqField/ContainsField/NeqField */
    groove_LiteralValue value;
    u32 case_tag; groove_PredicateExpr *payload; /* EnumMatch */
    JZ_VEC(groove_PredicateExpr) children;       /* And / Or */
};
void groove_PredicateExpr_clone(groove_PredicateExpr *dst, const groove_PredicateExpr *src);
void groove_PredicateExpr_drop(groove_PredicateExpr *p);
bool groove_PredicateExpr_eq(const groove_PredicateExpr *a, const groove_PredicateExpr *b);
void groove_PredicateExpr_debug(jz_str *out, const groove_PredicateExpr *p);
void groove__ivm__op_types__PredicateExpr__canonicalize(groove_PredicateExpr *p);
bool groove__ivm__op_types__PredicateExpr__has_template_arguments(const groove_PredicateExpr *p);
bool groove__ivm__op_types__PredicateExpr__accepts_template_argument(const groove_PredicateExpr *p, const groove_PredicateExpr *arg);

typedef enum { groove_ProjectExpr_Field, groove_ProjectExpr_Literal, groove_ProjectExpr_Null, groove_ProjectExpr_Nullable, groove_ProjectExpr_NullableFlat,
    groove_ProjectExpr_EnumTagRemap, groove_ProjectExpr_EnumRemap, groove_ProjectExpr_RecursiveEnumRemap,
    groove_ProjectExpr_TemplateArgument, groove_ProjectExpr_RecordField, groove_ProjectExpr_TypedLiteral } groove_ProjectExprTag;
typedef struct groove_ProjectExpr {
    u8 tag; groove_FieldRef field; groove_LiteralValue literal; groove_ValueType null_type;
    JZ_VEC(i16) tags8; JZ_VEC(i64) tags32; groove_RecursiveEnumRemaps *remaps; bool omit_unrepresentable;
} groove_ProjectExpr;
void groove_ProjectExpr_clone(groove_ProjectExpr *dst, const groove_ProjectExpr *src);
void groove_ProjectExpr_drop(groove_ProjectExpr *e);
bool groove_ProjectExpr_eq(const groove_ProjectExpr *a, const groove_ProjectExpr *b);

typedef struct groove_ProjectField {
    groove_ProjectExpr expression; jz_str output_name;
    bool has_identity; groove_FieldIdentity output_identity;
    bool has_value_type; groove_ValueType value_type;
} groove_ProjectField;
typedef JZ_VEC(groove_ProjectField) groove_ProjectFieldVec;
void groove_ProjectField_clone(groove_ProjectField *dst, const groove_ProjectField *src);
void groove_ProjectField_drop(groove_ProjectField *f);
u64 groove_ProjectField_hash(const groove_ProjectField *f, u64 h);
bool groove_ProjectField_eq(const groove_ProjectField *a, const groove_ProjectField *b);
const groove_FieldRef *groove__ivm__graph__ProjectField__source(const groove_ProjectField *f);
groove_ProjectField groove__ivm__graph__ProjectField__renamed_resolved(u32 idx, const char *name, u32 len);

typedef struct groove_ProjectionExpr { groove_ProjectExpr expression; bool has_output_name; jz_str output_name; bool has_identity; groove_FieldIdentity output_identity; } groove_ProjectionExpr;
typedef JZ_VEC(groove_ProjectionExpr) groove_ProjectionExprVec;
void groove_ProjectionExpr_clone(groove_ProjectionExpr *dst, const groove_ProjectionExpr *src);
void groove_ProjectionExpr_drop(groove_ProjectionExpr *e);

typedef struct groove_MapProjectOp { groove_ProjectionExprVec expressions; JZ_VEC(groove_UsizePair) mapping; } groove_MapProjectOp;
void groove_MapProjectOp_clone(groove_MapProjectOp *dst, const groove_MapProjectOp *src);
void groove_MapProjectOp_drop(groove_MapProjectOp *m);
bool groove_MapProjectOp_eq(const groove_MapProjectOp *a, const groove_MapProjectOp *b);

typedef struct groove_DurableStorage { jz_str column_family; jz_bytes key_prefix; jz_str name; } groove_DurableStorage;
void groove_DurableStorage_clone(groove_DurableStorage *dst, const groove_DurableStorage *src);
void groove_DurableStorage_drop(groove_DurableStorage *d);

typedef struct groove_TableSourceOp { jz_str table; bool has_scan; groove_StaticScanSpec scan; bool has_vp; groove_VariantProjectionTarget variant_projection; } groove_TableSourceOp;
typedef struct groove_LookupOp { groove_TableSourceOp source; groove_RecordDescriptor key_descriptor; JZ_VEC(u32) key_fields; } groove_LookupOp;
typedef struct groove_IndexSourceDurableIds { u32 index; JZ_VEC(u32) intersections; bool has_candidate; u32 candidate; } groove_IndexSourceDurableIds;
typedef struct groove_IndexCandidateFilter { jz_str table; jz_str index; groove_StaticScanSpec scan; jz_str source_column; jz_str candidate_column; } groove_IndexCandidateFilter;
typedef struct groove_IndexIntersection { jz_str index; groove_StaticScanSpec scan; } groove_IndexIntersection;
typedef struct groove_IndexSourceOp {
    jz_str table; jz_str index; groove_IndexSourceDurableIds *durable_ids;
    JZ_VEC(groove_IndexIntersection) intersections;
    bool has_candidate_filter; groove_IndexCandidateFilter candidate_filter;
    groove_RecordDescriptor input_descriptor;
    bool has_vp; groove_VariantProjectionTarget variant_projection;
    bool has_row_projection; groove_VariantProjectionTarget row_projection;
    JZ_VEC(u32) key_fields; JZ_VEC(u32) value_fields;
    bool unique, append_value_to_key, store_value;
    bool has_scan; groove_StaticScanSpec scan;
} groove_IndexSourceOp;
typedef struct groove_InlineRecordsOp { JZ_VEC(jz_bytes) records; } groove_InlineRecordsOp;
typedef struct groove_FrontierSourceOp { groove_FrontierName binding; } groove_FrontierSourceOp;
typedef struct groove_BindingSourceOp { groove_BindingSourceKey key; } groove_BindingSourceOp;
typedef struct groove_PersistOp { jz_str name; groove_DurableStorage storage; JZ_VEC(u32) key_fields; bool unique; } groove_PersistOp;
typedef struct groove_FilterOp { groove_PredicateExpr predicate; groove_ValueComparison comparison; } groove_FilterOp;
typedef struct groove_StreamingChecksumOp { jz_str field; u32 field_idx; jz_str output_field; u32 window_bytes; u32 max_bytes_per_turn; } groove_StreamingChecksumOp;
typedef struct groove_UnwrapNullableOp { jz_str field; u32 field_idx; } groove_UnwrapNullableOp;
typedef struct groove_UnnestOp { jz_str array_field; u32 array_field_idx; jz_str element_field; } groove_UnnestOp;
typedef struct groove_VariantProjectOp { jz_str field; u32 field_idx; u32 tag; } groove_VariantProjectOp;
typedef struct groove_IndexByOp {
    groove_PlanExprVec key_expressions; groove_PlanExprVec value_expressions;
    bool has_explicit_index; groove_IndexSchema explicit_index;
    JZ_VEC(u32) key_fields; JZ_VEC(u32) value_fields;
    bool unique, append_value_to_key, store_value;
    bool has_scan; groove_StaticScanSpec scan;
} groove_IndexByOp;
typedef enum { groove_JoinOpKind_Inner, groove_JoinOpKind_Left, groove_JoinOpKind_Right, groove_JoinOpKind_Full } groove_JoinOpKind;
typedef struct groove_JoinOp {
    groove_JoinOpKind kind; groove_PlanExprVec left_key; groove_PlanExprVec right_key;
    groove_RecordDescriptor left_descriptor, right_descriptor;
    bool has_residual; groove_PlanExpr residual_predicate;
    groove_ValueComparison comparison;
} groove_JoinOp;
typedef struct groove_RecursiveOp {
    groove_FrontierName frontier; u32 max_iters; bool truncate_at_max_iters;
    JZ_VEC(jz_str) read_tables; bool has_step_witness_output; groove_RecordDescriptor step_witness_output;
} groove_RecursiveOp;
typedef struct groove_ArgByOp { JZ_VEC(jz_str) group_fields; JZ_VEC(jz_str) order_fields; JZ_VEC(u32) group_field_indices; JZ_VEC(u32) comparison_field_indices; } groove_ArgByOp;
typedef enum { groove_TopByDirection_Asc, groove_TopByDirection_Desc } groove_TopByDirection;
typedef struct groove_TopByLimit { bool finite; u64 n; } groove_TopByLimit;
typedef struct groove_TopByOrderField { jz_str field; groove_TopByDirection direction; } groove_TopByOrderField;
typedef struct groove_TopByOp {
    JZ_VEC(jz_str) group_fields; JZ_VEC(u32) group_field_indices; JZ_VEC(groove_TopByOrderField) order_fields;
    JZ_VEC(jz_str) tie_fields; JZ_VEC(u32) sort_field_indices; JZ_VEC(u8) sort_directions;
    u64 offset; groove_TopByLimit limit;
} groove_TopByOp;
typedef struct groove_CollectByProjection { jz_str field; u32 field_idx; jz_str output_name; bool unwrap_nullable; } groove_CollectByProjection;
typedef struct groove_CollectBySlot groove_CollectBySlot;
struct groove_CollectBySlot {
    JZ_VEC(jz_str) group_fields; JZ_VEC(u32) group_field_indices; JZ_VEC(groove_CollectByProjection) child_fields;
    groove_RecordDescriptor child_descriptor; jz_str collection_field; u32 collection_field_index;
    JZ_VEC(groove_CollectBySlot) slots; JZ_VEC(groove_TopByOrderField) order_fields; JZ_VEC(jz_str) tie_fields;
    bool has_presence; u32 presence_field_index; bool has_reference_array; u32 reference_array_field_index; bool reference_order;
    JZ_VEC(u32) sort_field_indices; JZ_VEC(u8) sort_directions; u64 offset; groove_TopByLimit limit;
};
typedef enum { groove_CollectByMode_Collect, groove_CollectByMode_Root, groove_CollectByMode_Expand } groove_CollectByMode;
typedef struct groove_CollectByOp {
    groove_CollectByMode mode; JZ_VEC(jz_str) group_fields; JZ_VEC(u32) group_field_indices;
    JZ_VEC(groove_CollectByProjection) parent_fields; JZ_VEC(groove_CollectByProjection) child_fields;
    groove_RecordDescriptor child_descriptor; jz_str collection_field; u32 collection_field_index;
    JZ_VEC(groove_CollectBySlot) slots; JZ_VEC(groove_CollectByProjection) tuple_fields;
    JZ_VEC(jz_str) occurrence_id_fields; JZ_VEC(u32) occurrence_id_field_indices;
    JZ_VEC(groove_TopByOrderField) order_fields; JZ_VEC(jz_str) tie_fields;
    JZ_VEC(u32) sort_field_indices; JZ_VEC(u8) sort_directions; u64 offset; groove_TopByLimit limit;
} groove_CollectByOp;
typedef enum { groove_AggFn_Count, groove_AggFn_Sum, groove_AggFn_Avg, groove_AggFn_Min, groove_AggFn_Max } groove_AggregateFunction;
typedef struct groove_AggregateExpr {
    groove_AggregateFunction function; bool has_expression; groove_PlanExpr expression; bool distinct;
    bool has_output_name; jz_str output_name; bool has_identity; groove_FieldIdentity output_identity;
} groove_AggregateExpr;
typedef struct groove_AggregateOp { groove_PlanExprVec group_key; JZ_VEC(u32) group_field_indices; JZ_VEC(groove_AggregateExpr) aggregates; } groove_AggregateOp;
typedef struct groove_ArrangeOp { JZ_VEC(jz_str) fields; groove_ValueComparison comparison; } groove_ArrangeOp;

typedef enum {
    groove_Op_TableSource, groove_Op_IndexSource, groove_Op_InlineRecords, groove_Op_FrontierSource,
    groove_Op_BindingSource, groove_Op_Arrange, groove_Op_ArgMaxBy, groove_Op_ArgMinBy, groove_Op_TopBy,
    groove_Op_CollectBy, groove_Op_Recursive, groove_Op_RecursiveStepWitness, groove_Op_Persist,
    groove_Op_Filter, groove_Op_MapProject, groove_Op_StreamingChecksum, groove_Op_UnwrapNullable,
    groove_Op_Unnest, groove_Op_VariantProject, groove_Op_IndexBy, groove_Op_Join, groove_Op_SemiJoin,
    groove_Op_AntiJoin, groove_Op_Lookup, groove_Op_Union, groove_Op_Negate, groove_Op_Distinct, groove_Op_Aggregate,
} groove_OpTag;
typedef struct groove_OpType {
    u8 tag;
    union {
        groove_TableSourceOp table_source; groove_IndexSourceOp index_source; groove_InlineRecordsOp inline_records;
        groove_FrontierSourceOp frontier_source; groove_BindingSourceOp binding_source; groove_ArrangeOp arrange;
        groove_ArgByOp arg_by; groove_TopByOp top_by; groove_CollectByOp *collect_by; groove_RecursiveOp recursive;
        groove_PersistOp persist; groove_FilterOp filter; groove_MapProjectOp map_project;
        groove_StreamingChecksumOp streaming_checksum; groove_UnwrapNullableOp unwrap_nullable; groove_UnnestOp unnest;
        groove_VariantProjectOp variant_project; groove_IndexByOp index_by; groove_JoinOp join; groove_LookupOp lookup;
        groove_AggregateOp aggregate;
    } v;
} groove_OpType;
void groove_OpType_drop(groove_OpType *op);
void groove_OpType_clone(groove_OpType *dst, const groove_OpType *src);
bool groove_OpType_eq(const groove_OpType *a, const groove_OpType *b);

typedef enum { groove_NodeOutput_Records, groove_NodeOutput_Arrangement } groove_NodeOutputTag;
typedef struct groove_NodeOutput { u8 tag; groove_RecordDescriptor records; } groove_NodeOutput;
static inline groove_RecordDescriptor groove__ivm__graph__NodeOutput__records(groove_NodeOutput o) { return o.records; }
typedef JZ_VEC(groove_NodeId) groove_NodeIdVec;
typedef struct groove_NodeDescriptor { groove_OpType operator_; groove_NodeIdVec inputs; groove_NodeOutput output; } groove_NodeDescriptor;
void groove_NodeDescriptor_drop(groove_NodeDescriptor *d);
typedef enum { groove_NodeDurability_Ephemeral, groove_NodeDurability_Durable } groove_NodeDurabilityTag;
typedef struct groove_NodeDurability { u8 tag; groove_DurableStorage storage; } groove_NodeDurability;
void groove_NodeDurability_drop(groove_NodeDurability *d);
void groove_NodeDurability_clone(groove_NodeDurability *dst, const groove_NodeDurability *src);
typedef struct groove_GraphNode { groove_NodeId id; groove_NodeDescriptor descriptor; groove_NodeDurability durability; jz_hmap children; } groove_GraphNode;
typedef enum { groove_Retainer_Subscription, groove_Retainer_Hydration, groove_Retainer_PreparedShape, groove_Retainer_DurableSchemaObject } groove_RetainerTag;
typedef struct groove_Retainer { u8 tag; jz_str name; } groove_Retainer;
void groove_Retainer_clone(groove_Retainer *dst, const groove_Retainer *src);
void groove_Retainer_drop(groove_Retainer *r);
void groove_Retainer_drop_v(void *r);
bool groove_Retainer_eq(const void *a, const void *b);
u64 groove_Retainer_hash(const void *r);

typedef struct groove_IvmGraph groove_IvmGraph; /* opaque: owned by unit groove-ivm (graph.rs) */
typedef struct groove_ExecutionLayout { groove_NodeIdVec nodes; jz_hmap slots; /* NodeId -> u32 */ } groove_ExecutionLayout;
groove_IvmGraph *groove__ivm__graph__IvmGraph__new(void);
groove_IvmGraph *groove__ivm__graph__IvmGraph__clone(const groove_IvmGraph *g);
void groove_IvmGraph_drop(groove_IvmGraph *g);
groove_NodeId groove__ivm__graph__IvmGraph__dedup_node(groove_IvmGraph *g, groove_NodeDescriptor descriptor, groove_NodeDurability durability);
jz_err groove__ivm__graph__IvmGraph__validate_node(const groove_IvmGraph *g, const groove_NodeDescriptor *d);
const groove_GraphNode *groove__ivm__graph__IvmGraph__node(const groove_IvmGraph *g, groove_NodeId id);
groove_GraphNode *groove__ivm__graph__IvmGraph__node_mut(groove_IvmGraph *g, groove_NodeId id);
const jz_hmap *groove__ivm__graph__IvmGraph__nodes(const groove_IvmGraph *g);
void groove__ivm__graph__IvmGraph__remove_node(groove_IvmGraph *g, groove_NodeId id);
void groove__ivm__graph__IvmGraph__take_added_nodes(groove_IvmGraph *g, groove_NodeIdVec *out);
void groove__ivm__graph__IvmGraph__mark_ancestors(const groove_IvmGraph *g, groove_NodeId id, jz_hmap *retained);
jz_err groove__ivm__graph__IvmGraph__execution_layout(groove_IvmGraph *g, const groove_NodeId *roots, u32 n, groove_ExecutionLayout **out);
void groove__ivm__graph__IvmGraph__execution_layout_counters(const groove_IvmGraph *g, u64 *builds, u64 *hits);
void groove__ivm__graph__IvmGraph__table_source_nodes(const groove_IvmGraph *g, const char *table, u32 len, groove_NodeIdVec *out);
void groove__ivm__graph__IvmGraph__affected_nodes(const groove_IvmGraph *g, const jz_btree *tables, groove_NodeIdVec *out);
groove_NodeId groove__ivm__graph__NodeDescriptor__node_id_of(const groove_OpType *op, const groove_NodeId *inputs, u32 n, const groove_NodeOutput *out);
groove_NodeId groove__ivm__graph__NodeDescriptor__node_id(const groove_NodeDescriptor *d);
jz_err groove__ivm__graph__NodeDescriptor__validate(const groove_NodeDescriptor *d, const groove_NodeOutput *inputs, u32 n);
enum { GROOVE_GRAPH_ERR_VALIDATION = 0x0500 };

/* GraphBuilder (Arc-shared enum) and template types */
typedef struct groove_GraphBuilder groove_GraphBuilder;
typedef struct groove_TypedGraphTemplate groove_TypedGraphTemplate;
typedef struct groove_TemplateScalarArgument groove_TemplateScalarArgument;
typedef struct groove_CollectByBuilder groove_CollectByBuilder;
typedef struct groove_TopByOrder { groove_FieldRef field; groove_TopByDirection direction; } groove_TopByOrder;
typedef enum {
    groove_GB_TypedTemplate, groove_GB_TemplateInput, groove_GB_Table, groove_GB_InlineRecords, groove_GB_InputSource,
    groove_GB_Index, groove_GB_FrontierSource, groove_GB_BindingSource, groove_GB_Recursive, groove_GB_RecursiveStepWitness,
    groove_GB_Filter, groove_GB_UnwrapNullable, groove_GB_Unnest, groove_GB_VariantProject, groove_GB_Project,
    groove_GB_StreamingChecksum, groove_GB_Union, groove_GB_Join, groove_GB_SemiJoin, groove_GB_AntiJoin, groove_GB_Lookup,
    groove_GB_ArgMaxBy, groove_GB_ArgMinBy, groove_GB_TopBy, groove_GB_CollectBy, groove_GB_Aggregate,
} groove_GraphBuilderTag;
typedef JZ_VEC(groove_GraphBuilder *) groove_GraphBuilderVec; /* Arc<GraphBuilder> = jz_rc payload ptr */
struct groove_GraphBuilder {
    u8 tag;
    /* TypedTemplate */
    groove_TypedGraphTemplate *program; groove_GraphBuilderVec inputs; JZ_VEC(groove_PredicateExpr) predicates;
    groove_TemplateScalarArgument *scalars; u32 scalars_len;
    /* TemplateInput */
    u32 slot; groove_RecordDescriptor output; groove_GraphBuilder *input; /* also unary input */
    /* sources */
    jz_str table; bool has_scan; groove_StaticScanSpec scan; bool has_variant_projection; jz_str variant_projection;
    JZ_VEC(jz_bytes) records; groove_InputSourceId id; jz_str index;
    JZ_VEC(groove_IndexIntersection) intersections; bool has_candidate_filter; groove_IndexCandidateFilter candidate_filter;
    bool has_row_projection; jz_str row_projection;
    groove_FrontierName binding; jz_str shape;
    /* Recursive */
    groove_GraphBuilder *seed, *step, *step_witness; groove_FrontierName frontier; u32 max_iters; bool truncate_at_max_iters;
    groove_GraphBuilder *recursive;
    /* unary ops */
    groove_PredicateExpr predicate; groove_ValueComparison comparison;
    groove_FieldRef field; jz_str element_field; jz_str case_name; groove_ProjectFieldVec fields;
    jz_str output_field; u32 window_bytes; u32 max_bytes_per_turn;
    /* binary */
    groove_GraphBuilder *left, *right; groove_FieldRefVec left_on, right_on;
    groove_GraphBuilder *keys; groove_FieldRefVec keys_on;
    /* windows / aggregates */
    groove_FieldRefVec group_cols, order_cols; JZ_VEC(groove_TopByOrder) top_order_cols; groove_FieldRefVec tie_cols;
    u64 offset; groove_TopByLimit limit; groove_CollectByBuilder *collect; JZ_VEC(groove_AggregateExpr) aggregates;
};
void groove__ivm__graph__GraphBuilder__visit_inputs(const groove_GraphBuilder *g, void (*visit)(groove_GraphBuilder *const *input, void *ctx), void *ctx);
void groove__ivm__graph__GraphBuilder__postorder(const groove_GraphBuilder *g, void *out);
bool groove_GraphBuilder_eq(const groove_GraphBuilder *a, const groove_GraphBuilder *b);
u64 groove_GraphBuilder_hash(const groove_GraphBuilder *g, u64 h);

/* ===================== sibling unit groove-ivm-rt-a ===================== */
typedef struct groove_RecordDelta { jz_bytes record; i64 weight; } groove_RecordDelta;
typedef JZ_VEC(groove_RecordDelta) groove_RecordDeltaVec;
void groove_RecordDelta_drop(void *d);
void groove_RecordDelta_clone(void *dst, const void *src);
static inline groove_BorrowedRecord groove_RecordDelta_borrowed(const groove_RecordDelta *d, groove_RecordDescriptor desc) {
    groove_BorrowedRecord r = { jz_bytes_slice(&d->record), desc }; return r;
}
typedef struct groove_RecordDeltas { groove_RecordDescriptor descriptor; groove_RecordDeltaVec deltas; } groove_RecordDeltas;
void groove_RecordDeltas_drop(groove_RecordDeltas *d);
void groove_RecordDeltas_drop_v(void *d);
void groove_RecordDeltas_clone(groove_RecordDeltas *dst, const groove_RecordDeltas *src);
bool groove_RecordDeltas_eq(const groove_RecordDeltas *a, const groove_RecordDeltas *b);
typedef struct groove_TableDelta { jz_str table; u32 variant_tag; groove_RecordDescriptor descriptor; groove_RecordDeltaVec deltas; } groove_TableDelta;
typedef JZ_VEC(groove_TableDelta) groove_TableDeltaVec;
typedef struct groove_CompiledNode { groove_RecordDescriptor output; groove_NodeId node; bool has_root_ordering_node; groove_NodeId root_ordering_node; } groove_CompiledNode;
static inline bool groove_CompiledNode_eq(const groove_CompiledNode *a, const groove_CompiledNode *b) {
    return a->output == b->output && a->node == b->node && a->has_root_ordering_node == b->has_root_ordering_node &&
           (!a->has_root_ordering_node || a->root_ordering_node == b->root_ordering_node);
}
typedef struct groove_BindingSourceState { groove_RecordDescriptor descriptor; jz_hmap refcounts; bool initialized; } groove_BindingSourceState;
typedef struct groove_BindingDelta { groove_BindingSourceKey key; groove_RecordDescriptor descriptor; groove_RecordDeltaVec deltas; bool initializes_snapshot; } groove_BindingDelta;
typedef struct groove_BindingSources { jz_hmap states; void *snapshot; jz_hmap dirty; } groove_BindingSources;
void groove_BindingSources_init(groove_BindingSources *b);
void groove_BindingSources_drop(groove_BindingSources *b);
groove_BindingSourceState *groove__ivm__runtime__subscriptions__BindingSources__entry_vacant(groove_BindingSources *b, groove_BindingSourceKey key, bool *was_vacant);
const groove_BindingSourceState *groove__ivm__runtime__subscriptions__BindingSources__get(const groove_BindingSources *b, const groove_BindingSourceKey *key);
const jz_hmap *groove__ivm__runtime__subscriptions__BindingSources__snapshot(groove_BindingSources *b);
typedef struct groove_PendingIncrementalEvaluation { void *rc; } groove_PendingIncrementalEvaluation;
groove_PendingIncrementalEvaluation groove__ivm__runtime__runtime_tick__PendingIncrementalEvaluation__default(void);
void groove_PendingIncrementalEvaluation_drop(groove_PendingIncrementalEvaluation *p);
bool groove__ivm__runtime__runtime_tick__PendingIncrementalEvaluation__is_idle(const groove_PendingIncrementalEvaluation *p);
void groove_MultisinkSubscriptionState_drop_v(void *s);
void groove_RoutedMultisinkShapeState_drop_v(void *s);
u64 groove__ivm__runtime__evaluator__record_deltas_digest(const groove_RecordDeltas *d);
u32 groove__ivm__runtime__subscriptions__record_deltas_encoded_bytes(const groove_RecordDeltas *d);
bool groove__ivm__runtime__evaluator__builder_contains_recursive(const groove_GraphBuilder *g);
u64 groove__ivm__runtime__subscriptions__graph_builder_fingerprint(const groove_GraphBuilder *g);

/* OperatorState (evaluator.rs): tagged union. Payload types owned by rt-a are opaque. */
typedef enum {
    groove_OperatorState_Stateless, groove_OperatorState_Join, groove_OperatorState_SemiJoin, groove_OperatorState_AntiJoin,
    groove_OperatorState_ArgBy, groove_OperatorState_TopBy, groove_OperatorState_Recursive, groove_OperatorState_CollectBy,
    groove_OperatorState_StreamingChecksum, groove_OperatorState_Lookup,
} groove_OperatorStateTag;
struct groove_RecursiveState;
/* Layout follows rt-a's union; only the Recursive arm (AsOf<RecursiveState, Tick>) is named here. */
typedef struct groove_OperatorState {
    u32 tag;
    union {
        void *payload;
        struct { struct groove_RecursiveState *value; struct { bool has; u64 tick; u64 sub_tick; } as_of; } recursive;
        u64 opaque[8];
    } v;
} groove_OperatorState;
void groove_OperatorState_drop(groove_OperatorState *s);
void groove_OperatorState_drop_v(void *s);
void groove__ivm__runtime__evaluator__operator_state_for(const groove_OpType *op, groove_OperatorState *out);

/* records: prepared raw projections (records/mod.rs) */
typedef enum { groove_RPF_Copy, groove_RPF_WrapNullable, groove_RPF_Nested, groove_RPF_Error, groove_RPF_Encoded, groove_RPF_Evaluate } groove_RawProjectionFieldTag;
typedef struct groove_DescIdx { groove_RecordDescriptor descriptor; u32 index; } groove_DescIdx;
typedef struct groove_RawProjectionField { u8 tag; u32 source_idx; JZ_VEC(groove_DescIdx) path; jz_err error; jz_bytes bytes; } groove_RawProjectionField;
struct groove_PreparedProjection { JZ_VEC(groove_RawProjectionField) fields; bool reuses_input; };
groove_PreparedProjection *groove__records__PreparedProjection__new(groove_RecordDescriptor source, groove_RecordDescriptor target, JZ_VEC(groove_RawProjectionField) *fields); /* Arc payload */
typedef struct groove_PreparedRecordCopy groove_PreparedRecordCopy;
jz_err groove__records__RecordDescriptor__field_span(groove_RecordDescriptor d, jz_slice raw, u32 index, u32 *start, u32 *end);
typedef jz_err (*groove_WriteFieldFn)(u32 index, jz_bytes *out, void *ctx);
jz_err groove__records__RecordDescriptor__write_projected_fields_into(groove_RecordDescriptor d, jz_bytes *out, groove_WriteFieldFn f, void *ctx, u32 *start, u32 *end);
jz_err groove__records__decode_single_field_value(jz_slice bytes, const groove_ValueType *t, groove_Value *out);
bool groove__records__ValueType__may_contain_stored_scalar(const groove_ValueType *t);

/* evaluator.rs (rt-a): TickEvaluator mirror and the methods this unit calls */
typedef struct groove_EvaluationInputs groove_EvaluationInputs;
typedef struct groove_EvaluationMemo groove_EvaluationMemo;
/* subscriptions.rs PredicateExpr helpers (rt-a) */
struct groove_PredicateRecord;
jz_err groove__ivm__runtime__subscriptions__PredicateExpr__matches(const groove_PredicateExpr *p, struct groove_PredicateRecord record, groove_ValueComparison comparison, bool *out);
void groove__ivm__runtime__subscriptions__PredicateExpr__referenced_fields(const groove_PredicateExpr *p, jz_btree *out);

/* record_projection.rs (rt-a) */
jz_err groove__ivm__runtime__record_projection__project_field_expr(groove_RecordDescriptor input, const groove_ProjectField *field, groove_ProjectExpr *out);
jz_err groove__ivm__runtime__record_projection__project_descriptor(groove_RecordDescriptor input, const groove_ProjectField *fields, u32 n, groove_RecordDescriptor *out);
jz_err groove__ivm__runtime__record_projection__project_record(const groove_ProjectionExpr *exprs, u32 n, const usize (*mapping)[2], u32 nmap, groove_RecordDescriptor output_desc, groove_RecordDescriptor input_desc, const u8 *record, u32 len, jz_bytes *out);
jz_err groove__ivm__runtime__record_projection__project_field_value(const groove_ProjectionExpr *expr, usize output_idx, groove_RecordDescriptor output_desc, groove_RecordDescriptor input_desc, const u8 *record, u32 len, groove_Value *out);
jz_err groove__ivm__runtime__record_projection__encode_projection_field_value(groove_RecordDescriptor output_desc, usize field_idx, groove_Value value, jz_bytes *out);
bool groove__ivm__runtime__record_projection__resolve_field_name(groove_RecordDescriptor d, const char *name, u32 len, u32 *idx);
jz_err groove__ivm__runtime__record_projection__resolve_field_ref(groove_RecordDescriptor d, const groove_FieldRef *f, u32 *idx);
jz_err groove__ivm__runtime__record_projection__field_ref_name(groove_RecordDescriptor d, const groove_FieldRef *f, jz_str *out);
jz_err groove__ivm__runtime__record_projection__field_name_at(groove_RecordDescriptor d, u32 idx, jz_str *out);
jz_err groove__ivm__runtime__record_projection__unwrap_nullable_descriptor(groove_RecordDescriptor d, u32 idx, groove_RecordDescriptor *out);
jz_err groove__ivm__runtime__record_projection__unnest_descriptor(groove_RecordDescriptor d, u32 idx, const char *element_field, u32 len, groove_RecordDescriptor *out);
jz_err groove__ivm__runtime__record_projection__variant_project_descriptor(groove_RecordDescriptor d, u32 idx, u32 tag, groove_RecordDescriptor *out);
jz_err groove__ivm__runtime__record_projection__aggregate_descriptor(groove_RecordDescriptor d, const groove_AggregateOp *op, groove_RecordDescriptor *out);
groove_RecordDescriptor groove__ivm__runtime__record_projection__index_record_descriptor(void);
const jz_str *groove__ivm__runtime__record_projection__variant_projection_target_name(const groove_VariantProjectionTarget *t);
jz_err groove__ivm__runtime__record_projection__remap_enum_tag(groove_Value v, const i16 *tags, u32 n, groove_Value *out);
jz_err groove__ivm__runtime__record_projection__remap_enum(groove_Value v, const i64 *tags, u32 n, groove_Value *out);
jz_err groove__ivm__runtime__record_projection__remap_recursive_enum_value(groove_Value v, const groove_RecursiveEnumRemaps *r, bool omit, bool *omitted, groove_Value *out);
jz_err groove__ivm__runtime__record_projection__apply_index_by(const groove_IndexByOp *op, groove_RecordDescriptor input, const groove_RecordDelta *deltas, u32 n, groove_RecordDeltaVec *out);
bool groove__ivm__runtime__record_projection__projection_uses_raw_copy(const groove_MapProjectOp *p);
jz_err groove__ivm__runtime__record_projection__raw_projection_fields(const groove_MapProjectOp *project, groove_RecordDescriptor input_desc, groove_RecordDescriptor output_desc, groove_PreparedProjection **out /* NULL = None */);
jz_err groove__ivm__runtime__record_projection__schema_index_input_fields(const groove_TableSchema *t, const groove_IndexSchema *idx, groove_StrVec *out);
jz_err groove__ivm__runtime__record_projection__schema_index_input_descriptor(const groove_TableSchema *t, const groove_IndexSchema *idx, groove_RecordDescriptor *out);
typedef enum { groove_StaticScanBounds_Prefix, groove_StaticScanBounds_Range } groove_StaticScanBoundsTag;
typedef struct groove_StaticScanBounds { u32 tag; jz_bytes a; /* prefix = start */ jz_bytes b; } groove_StaticScanBounds;
void groove__ivm__runtime__record_projection__StaticScanBounds__drop(groove_StaticScanBounds *b);
jz_err groove__ivm__runtime__record_projection__scan_bounds(const groove_StaticScanSpec *scan, groove_StaticScanBounds *out);
bool groove__ivm__runtime__record_projection__scan_max_items(const groove_StaticScanSpec *scan /* nullable */, usize *out);
bool groove__ivm__runtime__record_projection__scan_reversed(const groove_StaticScanSpec *scan);
jz_err groove__ivm__runtime__record_projection__key_matches_static_scan(const u8 *key, u32 len, const groove_StaticScanSpec *scan, bool *out);
jz_err groove__ivm__runtime__record_projection__persisted_index_scan_bounds(u32 index_id, const groove_StaticScanSpec *scan /* nullable */, groove_StaticScanBounds *out);
jz_err groove__ivm__runtime__record_projection__collect_by_descriptor(groove_RecordDescriptor input, const groove_CollectByBuilder *b, groove_RecordDescriptor *out);
jz_err groove__ivm__runtime__record_projection__validate_collect_by_terminality(const groove_GraphBuilder *g);
jz_err groove__ivm__runtime__record_projection__validate_collect_by_key_types(groove_RecordDescriptor d, const u32 *idx, u32 n);


/* chunks.rs: ChunkRequest / ChunkLease and the tracked provider API used by evaluation_session.rs */
typedef struct groove_ChunkRequest { u8 object_hash[32]; jz_bytes locator; } groove_ChunkRequest;
void groove_ChunkRequest_clone(groove_ChunkRequest *dst, const groove_ChunkRequest *src);
void groove_ChunkRequest_drop(groove_ChunkRequest *r);
int groove_ChunkRequest_cmp(const groove_ChunkRequest *a, const groove_ChunkRequest *b);
typedef struct groove_ChunkLease { jz_bytes bytes; void *lease; /* Rc */ } groove_ChunkLease;
void groove_ChunkLease_drop(groove_ChunkLease *l);
bool groove__chunks__ChunkLease__verifies_object_hash(const groove_ChunkLease *l, const u8 expected[32]);
bool groove__chunks__OwnedChunkProvider__permits_eager_read_retry(const groove_OwnedChunkProvider *p);
/* get_tracked(..).await, completed synchronously; on error reports ChunkError::into_parts */
jz_err groove__chunks__OwnedChunkProvider__get_tracked(const groove_OwnedChunkProvider *p, const groove_ChunkRequest *req, groove_ChunkLease *out, bool *publication_metadata_durability);
jz_err groove__chunks__ChunkError__unavailable(void);
bool groove__storage__Storage__permits_eager_read_retry(groove_Storage s);
/* db/encoding.rs */
jz_err groove__db__encoding__persisted_index_primary_key(const groove_TableSchema *t, const char *index, u32 ilen, const groove_IndexSchema *idx, jz_slice storage_key, jz_slice stored_value, jz_bytes *out);
jz_err groove__db__encoding__persisted_index_column_value(const groove_TableSchema *t, const char *index, u32 ilen, const groove_IndexSchema *idx, jz_slice storage_key, jz_slice stored_value, const char *column, u32 clen, groove_Value *out);


/* records: raw field projection helpers used by operator_updates.rs */
bool groove__records__RecordDescriptor__registry_compatible_with(groove_RecordDescriptor a, groove_RecordDescriptor b);
jz_err groove__records__split_variant_record(jz_slice stored, u32 *variant_tag, jz_slice *payload);
typedef jz_err (*groove_EvaluateFieldFn)(u32 index, jz_bytes *output, void *ctx);
jz_err groove__records__RecordDescriptor__project_raw_fields_into(groove_RecordDescriptor out_desc, groove_RecordDescriptor in_desc, jz_slice raw,
    const groove_RawProjectionField *fields, u32 n, jz_bytes *output, groove_EvaluateFieldFn eval, void *ctx, u32 *start, u32 *end);
jz_err groove__records__RecordDescriptor__unwrap_nullable_field_into(groove_RecordDescriptor out_desc, groove_RecordDescriptor in_desc, jz_slice raw,
    u32 field_idx, jz_bytes *output, groove_RawProjectionScratch *scratch, bool *some, u32 *start, u32 *end);
groove_RawProjectionScratch *groove__records__RawProjectionScratch__default(void);
void groove_RawProjectionScratch_drop(groove_RawProjectionScratch *s);


/* records: PreparedRecordCopy (join output) */
jz_err groove__records__PreparedRecordCopy__project_into(const groove_PreparedRecordCopy *p, const jz_slice *inputs, u32 n, jz_bytes *out, u32 *start, u32 *end);
groove_PreparedRecordCopy *groove__records__PreparedRecordCopy__new(groove_RecordDescriptor output, const groove_RecordDescriptor *inputs, u32 n, const groove_UsizePair *mapping, u32 nmap);


/* large_values.rs: LargeValueRef layout (mirror; used by windows.rs identity keys) */
typedef struct groove_ReplaceEdit_m { u64 offset; u64 delete_length; jz_bytes insert_bytes; u64 utf16_offset; u64 delete_utf16_length; u64 insert_utf16_length; } groove_ReplaceEdit_m;
struct groove_LargeValueRef {
    u8 kind; u8 format_version;
    u8 logical_hash[32];
    struct { u8 object_hash[32]; u8 locator[32]; } root;
    u64 byte_length;
    bool has_utf16_length; u64 utf16_length;
    JZ_VEC(groove_ReplaceEdit_m) edit_tail;
};
/* records helpers used by windows.rs */
bool groove__records__ValueType__collect_by_ordered_scalar(const groove_ValueType *t);
const groove_ValueType *groove__records__ValueType__non_nullable(const groove_ValueType *t);
jz_err groove__records__encode_record_descriptor(groove_RecordDescriptor d, jz_bytes *out);
jz_err groove__records__BorrowedRecord__get_bool(const groove_BorrowedRecord *r, u32 idx, bool *out);
groove_RecordDescriptor groove__records__RecordDescriptor__new_named(const jz_str *names, const groove_ValueType *types, u32 n);

#endif
