/* _ext_protocol-types.h: mirror declarations of types and functions owned by
 * OTHER units (groove, jazz_model, the vendored ruzstd decoder) that the
 * protocol-types unit (crates jazz_protocol, jazz_types, jazz_compression)
 * uses. Layouts are "roughly like the Rust fields". Names follow PORTING.md.
 * Shared by src/jazz_protocol, src/jazz_types and src/jazz_compression. */
#ifndef JAZZ_EXT_PROTOCOL_TYPES_H
#define JAZZ_EXT_PROTOCOL_TYPES_H
#include "jz_rt.h"

/* ============================ groove::records ============================ */
typedef struct groove_RecordDescriptorData groove_RecordDescriptorData;
/* RecordDescriptor(Intern<RecordDescriptorData>): Copy handle, eq/hash by pointer. */
typedef const groove_RecordDescriptorData *groove_RecordDescriptor;
typedef struct groove_LargeValueRef groove_LargeValueRef;

enum {
    GROOVE_VT_U8, GROOVE_VT_U16, GROOVE_VT_U32, GROOVE_VT_U64, GROOVE_VT_I32, GROOVE_VT_I64,
    GROOVE_VT_F64, GROOVE_VT_BOOL, GROOVE_VT_STRING, GROOVE_VT_BYTES, GROOVE_VT_INTERNAL,
    GROOVE_VT_UUID, GROOVE_VT_ENUM_TAG, GROOVE_VT_TUPLE, GROOVE_VT_ARRAY, GROOVE_VT_NULLABLE,
    GROOVE_VT_RECORD, GROOVE_VT_ENUM, GROOVE_VT_U48
};
typedef struct groove_ValueType groove_ValueType;
typedef struct groove_ScalarEnumSchema { JZ_VEC(jz_str) variants; u64 registry_id; } groove_ScalarEnumSchema;
typedef struct groove_EnumCase { jz_str name; groove_RecordDescriptor payload; } groove_EnumCase;
typedef struct groove_EnumSchema { jz_str name; JZ_VEC(groove_EnumCase) cases; u64 registry_id; } groove_EnumSchema;
struct groove_ValueType {
    u8 tag;
    union {
        struct { u8 repr; u8 kind; } internal;
        groove_ScalarEnumSchema *enum_tag;
        JZ_VEC(groove_ValueType) tuple;
        groove_ValueType *inner;          /* Array / Nullable: Box<ValueType> */
        groove_RecordDescriptor *record;  /* Box<RecordDescriptor> */
        groove_EnumSchema *enum_schema;   /* Box<EnumSchema> */
    } u;
};
void groove__records__values__ValueType__drop(groove_ValueType *v);

typedef struct groove_DescriptorField {
    bool has_name; jz_str name;
    bool has_identity; u8 identity[32];
    groove_ValueType value_type;
} groove_DescriptorField;
const groove_DescriptorField *groove__records__RecordDescriptor__fields(groove_RecordDescriptor d, u32 *len);
bool groove__records__RecordDescriptor__field_index(groove_RecordDescriptor d, const char *p, u32 n, u32 *out);
/* new(fields): takes ownership of the value types */
groove_RecordDescriptor groove__records__RecordDescriptor__new(const jz_str *names, groove_ValueType *types, u32 n);

enum {
    GROOVE_V_U8, GROOVE_V_U16, GROOVE_V_U32, GROOVE_V_U64, GROOVE_V_F64, GROOVE_V_BOOL,
    GROOVE_V_STRING, GROOVE_V_BYTES, GROOVE_V_LARGE, GROOVE_V_UUID, GROOVE_V_ENUM_TAG,
    GROOVE_V_TUPLE, GROOVE_V_ARRAY, GROOVE_V_NULLABLE, GROOVE_V_I64, GROOVE_V_I32,
    GROOVE_V_RECORD, GROOVE_V_ENUM, GROOVE_V_U48
};
typedef struct groove_OwnedRecord { jz_bytes raw; groove_RecordDescriptor descriptor; } groove_OwnedRecord;
typedef struct groove_EnumValue { u32 tag; groove_OwnedRecord record; } groove_EnumValue;
typedef struct groove_Value groove_Value;
typedef JZ_VEC(groove_Value) groove_ValueVec;
struct groove_Value {
    u8 tag;
    union {
        u8 u8_; u16 u16_; u32 u32_; u64 u64_; f64 f64_; bool b; i64 i64_; i32 i32_;
        jz_str s; jz_bytes bytes; groove_LargeValueRef *large; jz_uuid uuid; u8 enum_tag;
        groove_ValueVec items;      /* Tuple / Array */
        groove_Value *nullable;     /* Nullable: NULL = None */
        groove_OwnedRecord record;
        groove_EnumValue enum_;
    } u;
};
void groove__records__values__Value__Clone__clone(groove_Value *dst, const groove_Value *src);
void groove__records__values__Value__drop(groove_Value *v);
void groove__records__values__Value__drop_v(void *v);
bool groove__records__values__Value__PartialEq__eq(const groove_Value *a, const groove_Value *b);
void groove__records__values__Value__Debug__fmt(jz_str *out, const groove_Value *v);
void groove__records__values__Value__Serialize__serialize(jz_pc_w *w, const groove_Value *v);
jz_err groove__records__values__Value__Deserialize__deserialize(jz_pc_r *r, groove_Value *out);
jz_err groove__records__RecordDescriptor__create(groove_RecordDescriptor d, const groove_Value *values, u32 n, jz_bytes *out);
jz_err groove__records__RecordDescriptor__field_span(groove_RecordDescriptor d, jz_slice record, u32 idx, u32 *start, u32 *end);
typedef struct groove_BorrowedRecord { jz_slice raw; groove_RecordDescriptor descriptor; } groove_BorrowedRecord;
groove_BorrowedRecord groove__records__RecordDescriptor__bind(groove_RecordDescriptor d, jz_slice raw);
jz_err groove__records__BorrowedRecord__get_idx(const groove_BorrowedRecord *r, u32 idx, groove_Value *out);
jz_err groove__records__BorrowedRecord__get_uuid(const groove_BorrowedRecord *r, u32 idx, jz_uuid *out);
jz_err groove__records__BorrowedRecord__get_nullable_uuid(const groove_BorrowedRecord *r, u32 idx, bool *some, jz_uuid *out);
jz_err groove__records__BorrowedRecord__get_str(const groove_BorrowedRecord *r, u32 idx, jz_slice *out);
jz_err groove__records__BorrowedRecord__get_u64(const groove_BorrowedRecord *r, u32 idx, u64 *out);
jz_err groove__records__BorrowedRecord__get_record(const groove_BorrowedRecord *r, u32 idx, groove_BorrowedRecord *out);
groove_OwnedRecord groove__records__OwnedRecord__new(jz_bytes raw, groove_RecordDescriptor d);
void groove__records__OwnedRecord__drop(groove_OwnedRecord *r);
void groove__records__OwnedRecord__Clone__clone(groove_OwnedRecord *dst, const groove_OwnedRecord *src);
jz_err groove__records__OwnedRecord__get_idx(const groove_OwnedRecord *r, u32 idx, groove_Value *out);
static inline groove_BorrowedRecord groove_OwnedRecord_borrowed(const groove_OwnedRecord *r) {
    groove_BorrowedRecord b = { jz_bytes_slice(&r->raw), r->descriptor }; return b;
}
jz_err groove__records__values__encode_persisted_record_descriptor(groove_RecordDescriptor d, jz_bytes *out);
jz_err groove__records__values__decode_persisted_record_descriptor(jz_slice bytes, groove_RecordDescriptor *out);
jz_err groove__records__values__encode_record_descriptor(groove_RecordDescriptor d, jz_bytes *out);
void groove__records__Error__Display__fmt(jz_str *out, const jz_err *e);
/* groove::records::Error discriminants used here */
enum { GROOVE_REC_ERR_NON_CANONICAL_RECORD = 0x0d01, GROOVE_REC_ERR_LENGTH_OVERFLOW = 0x0d02 };

/* ============================ groove::Intern<String> ============================ */
typedef const jz_str *groove_InternStr;
groove_InternStr groove__Intern__new_str(jz_str s);        /* Intern::new (moves s) */
groove_InternStr groove__Intern__from_ref_str(jz_slice s); /* Intern::from_ref */

/* ============================ groove::large_values ============================ */
typedef struct groove_Locator { u8 b[32]; } groove_Locator;
typedef struct groove_NodeRef { u8 b[48]; } groove_NodeRef;
typedef struct groove_StagedChunk { groove_NodeRef node; jz_bytes bytes; } groove_StagedChunk;
groove_LargeValueRef *groove__large_values__LargeValueRef__clone(const groove_LargeValueRef *a);
void groove__large_values__LargeValueRef__drop(groove_LargeValueRef *a);
bool groove__large_values__LargeValueRef__eq(const groove_LargeValueRef *a, const groove_LargeValueRef *b);
u8 groove__large_values__LargeValueRef__kind(const groove_LargeValueRef *a);
void groove__large_values__LargeValueRef__Serialize__serialize(jz_pc_w *w, const groove_LargeValueRef *a);
jz_err groove__large_values__LargeValueRef__Deserialize__deserialize(jz_pc_r *r, groove_LargeValueRef **out);
void groove__large_values__Locator__Serialize__serialize(jz_pc_w *w, const groove_Locator *a);
jz_err groove__large_values__Locator__Deserialize__deserialize(jz_pc_r *r, groove_Locator *out);
void groove__large_values__NodeRef__Serialize__serialize(jz_pc_w *w, const groove_NodeRef *a);
jz_err groove__large_values__NodeRef__Deserialize__deserialize(jz_pc_r *r, groove_NodeRef *out);
void groove__large_values__StagedChunk__Serialize__serialize(jz_pc_w *w, const groove_StagedChunk *a);
jz_err groove__large_values__StagedChunk__Deserialize__deserialize(jz_pc_r *r, groove_StagedChunk *out);
/* encode_stored_scalar(kind, &StoredScalar::Chunked(value.clone())) */
jz_err groove__large_values__encode_stored_scalar_chunked(u8 kind, const groove_LargeValueRef *v, jz_bytes *out);

/* ============================ groove::storage ============================ */
typedef struct groove_StorageCodecProfile { jz_btree codec_ids; } groove_StorageCodecProfile;
extern const char groove__storage__GROOVE_DURABLE_INDEX_V2_CODEC[];
void groove__storage__StorageCodecProfile__groove_epoch_1(groove_StorageCodecProfile *out);
jz_err groove__storage__StorageCodecProfile__with_additional_codecs(const groove_StorageCodecProfile *p, const char *const *codecs, u32 n, groove_StorageCodecProfile *out);
void groove__storage__StorageCodecProfile__drop(groove_StorageCodecProfile *p);

/* ============================ jazz_model::tx ============================ */
typedef struct jazz_model_TxId { u64 time; jz_uuid node; } jazz_model_TxId;
int jazz_model__tx__TxId__cmp(const void *a, const void *b, void *ctx);
bool jazz_model__tx__TxId__PartialEq__eq(const jazz_model_TxId *a, const jazz_model_TxId *b);
void jazz_model__tx__TxId__Serialize__serialize(jz_pc_w *w, const jazz_model_TxId *v);
jz_err jazz_model__tx__TxId__Deserialize__deserialize(jz_pc_r *r, jazz_model_TxId *out);
void jazz_model__tx__TxId__Debug__fmt(jz_str *out, const jazz_model_TxId *v);
typedef enum { JAZZ_TX_MERGEABLE = 0, JAZZ_TX_EXCLUSIVE = 1 } jazz_model_TxKind;
typedef enum { JAZZ_DUR_NONE = 0, JAZZ_DUR_LOCAL = 1, JAZZ_DUR_GLOBAL = 2 } jazz_model_DurabilityTier;
typedef enum { JAZZ_DELETION_DELETED = 0, JAZZ_DELETION_RESTORED = 1 } jazz_model_DeletionEvent;
typedef struct jazz_model_RejectionReason { u32 tag; jazz_model_TxId root; jz_str detail; } jazz_model_RejectionReason;
typedef enum { JAZZ_FATE_PENDING = 0, JAZZ_FATE_ACCEPTED = 1, JAZZ_FATE_REJECTED = 2 } jazz_model_FateTag;
typedef struct jazz_model_Fate { u32 tag; jazz_model_RejectionReason rejected; } jazz_model_Fate;
void jazz_model__tx__Fate__Clone__clone(jazz_model_Fate *dst, const jazz_model_Fate *src);
void jazz_model__tx__Fate__drop(jazz_model_Fate *p);
bool jazz_model__tx__Fate__PartialEq__eq(const jazz_model_Fate *a, const jazz_model_Fate *b);
void jazz_model__tx__DurabilityTier__Debug__fmt(jz_str *out, u32 tier);
void jazz_model__tx__Fate__Serialize__serialize(jz_pc_w *w, const jazz_model_Fate *v);
jz_err jazz_model__tx__Fate__Deserialize__deserialize(jz_pc_r *r, jazz_model_Fate *out);
/* AuthorSubject is defined in jazz_types (this unit); Transaction holds two. */
typedef struct jazz_model_TransactionRest jazz_model_TransactionRest; /* snapshot/read sets/metadata */
typedef struct jazz_model_Transaction {
    jazz_model_TxId tx_id;
    jazz_model_TxKind kind;
    u32 n_total_writes;
    u32 made_by_tag; const void *made_by_interned;          /* AuthorSubject */
    bool has_permission_subject; u32 ps_tag; const void *ps_interned;
    jazz_model_TransactionRest *rest; /* base_snapshot, read sets, user_metadata_json, contribution_merge */
} jazz_model_Transaction;
void jazz_model__tx__Transaction__Clone__clone(jazz_model_Transaction *dst, const jazz_model_Transaction *src);
void jazz_model__tx__Transaction__drop(jazz_model_Transaction *p);
bool jazz_model__tx__Transaction__PartialEq__eq(const jazz_model_Transaction *a, const jazz_model_Transaction *b);
void jazz_model__tx__Transaction__Serialize__serialize(jz_pc_w *w, const jazz_model_Transaction *v);
jz_err jazz_model__tx__Transaction__Deserialize__deserialize(jz_pc_r *r, jazz_model_Transaction *out);

/* ============================ jazz_model::query ============================ */
typedef struct jazz_model_ShapeId { jz_uuid u; } jazz_model_ShapeId;
typedef struct jazz_model_BindingId { jz_uuid u; } jazz_model_BindingId;
typedef struct jazz_model_Query jazz_model_Query;               /* boxed, opaque */
typedef struct jazz_model_RelationQuery jazz_model_RelationQuery; /* boxed, opaque */
typedef struct jazz_model_ValidatedQuery jazz_model_ValidatedQuery;
jazz_model_Query *jazz_model__query__ast__Query__Clone__clone(const jazz_model_Query *q);
void jazz_model__query__ast__Query__drop(jazz_model_Query *q);
bool jazz_model__query__ast__Query__PartialEq__eq(const jazz_model_Query *a, const jazz_model_Query *b);
void jazz_model__query__ast__Query__Serialize__serialize(jz_pc_w *w, const jazz_model_Query *q);
jz_err jazz_model__query__ast__Query__Deserialize__deserialize(jz_pc_r *r, jazz_model_Query **out);
jazz_model_RelationQuery *jazz_model__query__ast__RelationQuery__Clone__clone(const jazz_model_RelationQuery *q);
void jazz_model__query__ast__RelationQuery__drop(jazz_model_RelationQuery *q);
bool jazz_model__query__ast__RelationQuery__PartialEq__eq(const jazz_model_RelationQuery *a, const jazz_model_RelationQuery *b);
void jazz_model__query__ast__relation_query_wire__serialize_required(jz_pc_w *w, const jazz_model_RelationQuery *q);
jz_err jazz_model__query__ast__relation_query_wire__deserialize_required(jz_pc_r *r, jazz_model_RelationQuery **out);
const jazz_model_Query *jazz_model__query__validation__ValidatedQuery__query(const jazz_model_ValidatedQuery *v);
jz_uuid jazz_model__query__validation__ValidatedQuery__schema_version(const jazz_model_ValidatedQuery *v);
/* query::policy_limits::{MAX_POLICY_EXPRESSION_DEPTH, MAX_POLICY_EXPRESSION_NODES} are re-exported only */

/* ============================ jazz_model::schema ============================ */
typedef struct jazz_model_ColumnSchema {
    jz_str name;
    groove_ValueType column_type;
    u8 large_value_kind;
    bool has_default; groove_Value default_;
} jazz_model_ColumnSchema;
typedef struct jazz_model_TableSchema {
    jz_str name;
    JZ_VEC(jazz_model_ColumnSchema) columns;
    /* branch_by, references, policies, indexes: not touched here */
    void *rest;
} jazz_model_TableSchema;
typedef struct jazz_model_PublicSchema jazz_model_PublicSchema;
typedef struct jazz_model_JazzSchema {
    jazz_model_PublicSchema *public_schema;
    JZ_VEC(jazz_model_TableSchema) tables; /* runtime (Deref<Target = RuntimeSchema>) */
    void *runtime_rest;
} jazz_model_JazzSchema;
void jazz_model__schema__JazzSchema__Clone__clone(jazz_model_JazzSchema *dst, const jazz_model_JazzSchema *src);
void jazz_model__schema__JazzSchema__drop(jazz_model_JazzSchema *s);
bool jazz_model__schema__JazzSchema__PartialEq__eq(const jazz_model_JazzSchema *a, const jazz_model_JazzSchema *b);
jz_uuid jazz_model__schema__JazzSchema__version_id(const jazz_model_JazzSchema *s);
groove_RecordDescriptor jazz_model__schema__TableSchema__wire_record_descriptor(const jazz_model_TableSchema *t);
bool jazz_model__schema__physical_column_epoch_is_compatible(const jazz_model_TableSchema *st, jz_slice sc, const jazz_model_TableSchema *tt, jz_slice tc);
extern const char jazz_model__schema__APP_COLUMN_PREFIX[]; /* "_app_" */
/* serde_json::to_vec(schema.public_schema()) */
jz_err jazz_model__model__public_schema__PublicSchema__Serialize__to_json(const jazz_model_PublicSchema *s, jz_str *out);
jz_err jazz_model__model__public_schema_convert__decode_public_schema_json(jz_slice bytes, jazz_model_JazzSchema *out);

/* ============================ jazz_model::model::branch ============================ */
typedef struct jazz_model_BranchKeyEntry { jz_str name; jz_bytes value; } jazz_model_BranchKeyEntry;
typedef struct jazz_model_BranchKey { JZ_VEC(jazz_model_BranchKeyEntry) values; } jazz_model_BranchKey;
void jazz_model__model__branch__BranchKey__Clone__clone(jazz_model_BranchKey *dst, const jazz_model_BranchKey *src);
void jazz_model__model__branch__BranchKey__drop(jazz_model_BranchKey *k);
int jazz_model__model__branch__BranchKey__Ord__cmp(const jazz_model_BranchKey *a, const jazz_model_BranchKey *b);
void jazz_model__model__branch__BranchKey__Serialize__serialize(jz_pc_w *w, const jazz_model_BranchKey *k);
jz_err jazz_model__model__branch__BranchKey__Deserialize__deserialize(jz_pc_r *r, jazz_model_BranchKey *out);
/* BranchSelector { values: BTreeMap<String, BranchColumnValue(Vec<u8>)> } */
typedef struct jazz_model_BranchSelector { jz_btree values; } jazz_model_BranchSelector;
void jazz_model__model__branch__BranchSelector__Clone__clone(jazz_model_BranchSelector *dst, const jazz_model_BranchSelector *src);
void jazz_model__model__branch__BranchSelector__drop(jazz_model_BranchSelector *s);
int jazz_model__model__branch__BranchSelector__Ord__cmp(const jazz_model_BranchSelector *a, const jazz_model_BranchSelector *b);
void jazz_model__model__branch__BranchSelector__Hash__hash(const jazz_model_BranchSelector *s, u64 *h);
void jazz_model__model__branch__BranchSelector__Serialize__serialize(jz_pc_w *w, const jazz_model_BranchSelector *s);
jz_err jazz_model__model__branch__BranchSelector__Deserialize__deserialize(jz_pc_r *r, jazz_model_BranchSelector *out);
typedef struct jazz_model_SnapshotRef {
    jz_uuid owner; u64 global_base; u64 local_base; JZ_VEC(jazz_model_TxId) dots;
} jazz_model_SnapshotRef;
void jazz_model__model__branch__SnapshotRef__Clone__clone(jazz_model_SnapshotRef *dst, const jazz_model_SnapshotRef *src);
void jazz_model__model__branch__SnapshotRef__drop(jazz_model_SnapshotRef *s);
int jazz_model__model__branch__SnapshotRef__Ord__cmp(const jazz_model_SnapshotRef *a, const jazz_model_SnapshotRef *b);
void jazz_model__model__branch__SnapshotRef__Debug__fmt(jz_str *out, const jazz_model_SnapshotRef *s);
void jazz_model__model__branch__SnapshotRef__Serialize__serialize(jz_pc_w *w, const jazz_model_SnapshotRef *s);
jz_err jazz_model__model__branch__SnapshotRef__Deserialize__deserialize(jz_pc_r *r, jazz_model_SnapshotRef *out);
enum { JAZZ_BVB_CURRENT = 0, JAZZ_BVB_SNAPSHOT = 1 };
typedef struct jazz_model_BranchViewBase { u32 tag; jazz_model_BranchSelector branch; jazz_model_SnapshotRef snapshot; } jazz_model_BranchViewBase;
void jazz_model__model__branch__BranchViewBase__Clone__clone(jazz_model_BranchViewBase *dst, const jazz_model_BranchViewBase *src);
void jazz_model__model__branch__BranchViewBase__drop(jazz_model_BranchViewBase *b);
int jazz_model__model__branch__BranchViewBase__Ord__cmp(const jazz_model_BranchViewBase *a, const jazz_model_BranchViewBase *b);
void jazz_model__model__branch__BranchViewBase__Serialize__serialize(jz_pc_w *w, const jazz_model_BranchViewBase *b);
jz_err jazz_model__model__branch__BranchViewBase__Deserialize__deserialize(jz_pc_r *r, jazz_model_BranchViewBase *out);

/* ============================ jazz_model::model::policy_* ============================ */
/* canonical_policy_binding_claims(&author, provider_claims): BTreeMap<String, Value> (desc: str -> Value) */
void jazz_model__model__policy_claims__canonical_policy_binding_claims(const void *author /* AuthorSubject */, jz_btree *provider_claims_moved, jz_btree *out);
jz_err jazz_model__model__policy_directory__policy_binding_directory_claims_value(const jz_btree *claims, groove_Value *out);
jz_err jazz_model__model__policy_directory__policy_binding_directory_claims_from_value(groove_Value value, jz_btree *out);
groove_RecordDescriptor jazz_model__model__policy_directory__policy_directory_descriptor(void);
jz_err jazz_model__model__policy_directory__policy_directory_payload(u8 presence, groove_Value claims, groove_Value *out);
/* jz_btree_desc for BTreeMap<String, Value>, owned by jazz_model */
extern const jz_btree_desc jazz_model__btree_desc_str_value;

/* ============================ ruzstd (vendored decoder) ============================ */
/* The runtime provides whole-frame jz_zstd_decompress; the persistent streaming
 * FrameDecoder used by jazz_compression::stream is declared here. */
typedef struct ruzstd_FrameDecoder ruzstd_FrameDecoder;
ruzstd_FrameDecoder *ruzstd__decoding__frame_decoder__FrameDecoder__default(void);
void ruzstd__decoding__frame_decoder__FrameDecoder__drop(ruzstd_FrameDecoder *d);
jz_err ruzstd__decoding__frame_decoder__FrameDecoder__init(ruzstd_FrameDecoder *d, const u8 *p, u32 n);
jz_err ruzstd__decoding__frame_decoder__FrameDecoder__set_max_block_output(ruzstd_FrameDecoder *d, u32 limit);
jz_err ruzstd__decoding__frame_decoder__FrameDecoder__decode_blocks_upto(ruzstd_FrameDecoder *d, const u8 *p, u32 n, u32 blocks);
u32 ruzstd__decoding__frame_decoder__FrameDecoder__read_streaming(ruzstd_FrameDecoder *d, u8 *out, u32 n);
u32 ruzstd__decoding__frame_decoder__FrameDecoder__streaming_available(const ruzstd_FrameDecoder *d);
void ruzstd__decoding__errors__Display__fmt(jz_str *out, const jz_err *e);
/* ruzstd::decoding::StreamingDecoder over a byte slice + Read::take/read_to_end */
typedef struct ruzstd_StreamingDecoder ruzstd_StreamingDecoder;
jz_err ruzstd__decoding__streaming_decoder__StreamingDecoder__new(const u8 *p, u32 n, ruzstd_StreamingDecoder **out);
jz_err ruzstd__decoding__streaming_decoder__StreamingDecoder__read_to_end_take(ruzstd_StreamingDecoder *d, u64 limit, jz_bytes *out);
void ruzstd__decoding__streaming_decoder__StreamingDecoder__drop(ruzstd_StreamingDecoder *d);

#endif
