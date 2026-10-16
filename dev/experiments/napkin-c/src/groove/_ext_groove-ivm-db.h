/* _ext_groove-ivm-db.h: mirror declarations of types and functions owned by
 * OTHER units (groove records/schema/storage/large_values/chunks/ivm::runtime)
 * that the groove-ivm-db unit (crates/groove/src/ivm/{graph,op_types,template,
 * execution_layout,activation,routes}.rs + crates/groove/src/db/) uses.
 * Layouts are "roughly like the Rust fields". Names follow PORTING.md. */
#ifndef GROOVE_EXT_GROOVE_IVM_DB_H
#define GROOVE_EXT_GROOVE_IVM_DB_H
#include "jz_rt.h"

typedef JZ_VEC(jz_str) groove_StrVec; /* Vec<String> */

/* ===================== generic Hasher (core::hash::Hasher) ===================== */
typedef struct groove_Hasher {
    void (*write)(void *self, const u8 *p, u32 n);
    void *self;
} groove_Hasher;
static inline void groove_hash_bytes_raw(groove_Hasher *h, const void *p, u32 n) { h->write(h->self, (const u8 *)p, n); }
static inline void groove_hash_u8(groove_Hasher *h, u8 v) { groove_hash_bytes_raw(h, &v, 1); }
static inline void groove_hash_u16(groove_Hasher *h, u16 v) { groove_hash_bytes_raw(h, &v, 2); }
static inline void groove_hash_u32(groove_Hasher *h, u32 v) { groove_hash_bytes_raw(h, &v, 4); }
static inline void groove_hash_u64(groove_Hasher *h, u64 v) { groove_hash_bytes_raw(h, &v, 8); }
static inline void groove_hash_usize(groove_Hasher *h, u32 v) { groove_hash_bytes_raw(h, &v, 4); }
static inline void groove_hash_discr(groove_Hasher *h, u32 v) { groove_hash_bytes_raw(h, &v, 4); } /* isize discriminant on wasm32 */
static inline void groove_hash_bool(groove_Hasher *h, bool v) { groove_hash_u8(h, v ? 1 : 0); }
static inline void groove_hash_str(groove_Hasher *h, const jz_str *s) { groove_hash_bytes_raw(h, s->ptr, s->len); groove_hash_u8(h, 0xff); }
static inline void groove_hash_bytes(groove_Hasher *h, const jz_bytes *b) { groove_hash_usize(h, b->len); groove_hash_bytes_raw(h, b->ptr, b->len); }

/* ===================== Debug formatting helpers (core::fmt::DebugStruct etc.) =====
 * Owned by this unit (defined in ivm/op_types.c), declared here so every file
 * of the unit shares them. */
typedef struct groove_DebugStruct { jz_str *out; bool has_fields; } groove_DebugStruct;
groove_DebugStruct groove__fmt__debug_struct(jz_str *out, const char *name);
void groove__fmt__DebugStruct__field(groove_DebugStruct *d, const char *name);
void groove__fmt__DebugStruct__finish(groove_DebugStruct *d);
void groove__fmt__debug_tuple_begin(jz_str *out, const char *name);
void groove__fmt__debug_tuple_next(jz_str *out, u32 index);
void groove__fmt__debug_tuple_end(jz_str *out);
void groove__fmt__debug_str(jz_str *out, const jz_str *s);
void groove__fmt__debug_bytes(jz_str *out, const u8 *p, u32 n);
void groove__fmt__debug_usize_list(jz_str *out, const u32 *p, u32 n);
void groove__fmt__debug_str_list(jz_str *out, const jz_str *p, u32 n);

/* ===================== records (crates/groove/src/records) ===================== */
typedef struct groove_RecordDescriptorData groove_RecordDescriptorData;
/* RecordDescriptor(Intern<RecordDescriptorData>): Copy handle, eq/hash by pointer. */
typedef const groove_RecordDescriptorData *groove_RecordDescriptor;
typedef struct groove_ScalarEnumSchema groove_ScalarEnumSchema;
typedef struct groove_EnumSchema groove_EnumSchema;
typedef struct groove_LargeValueRef groove_LargeValueRef;

enum {
    GROOVE_VT_U8, GROOVE_VT_U16, GROOVE_VT_U32, GROOVE_VT_U64, GROOVE_VT_I32, GROOVE_VT_I64,
    GROOVE_VT_F64, GROOVE_VT_BOOL, GROOVE_VT_STRING, GROOVE_VT_BYTES, GROOVE_VT_INTERNAL,
    GROOVE_VT_UUID, GROOVE_VT_ENUM_TAG, GROOVE_VT_TUPLE, GROOVE_VT_ARRAY, GROOVE_VT_NULLABLE,
    GROOVE_VT_RECORD, GROOVE_VT_ENUM, GROOVE_VT_U48
};
typedef struct groove_ValueType groove_ValueType;
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
void groove__records__values__ValueType__Clone__clone(groove_ValueType *dst, const groove_ValueType *src);
void groove__records__values__ValueType__drop(groove_ValueType *v);
bool groove__records__values__ValueType__PartialEq__eq(const groove_ValueType *a, const groove_ValueType *b);
void groove__records__values__ValueType__Hash__hash(const groove_ValueType *v, groove_Hasher *h);
void groove__records__values__ValueType__Debug__fmt(jz_str *out, const groove_ValueType *v);
bool groove__records__values__ValueType__contains_record(const groove_ValueType *v);
groove_ValueType groove__records__values__ValueType__raw_bytes(void);
bool groove__records__values__collect_by_ordered_scalar(const groove_ValueType *v);

enum { GROOVE_FIELD_ID_NAME, GROOVE_FIELD_ID_SLOT, GROOVE_FIELD_ID_NAMED_SLOT };
typedef struct groove_FieldIdentity {
    u8 tag;
    jz_str name; /* Name / NamedSlot.name */
    u64 slot;    /* Slot / NamedSlot.slot */
} groove_FieldIdentity;
void groove__records__FieldIdentity__Clone__clone(groove_FieldIdentity *dst, const groove_FieldIdentity *src);
void groove__records__FieldIdentity__drop(groove_FieldIdentity *v);
bool groove__records__FieldIdentity__PartialEq__eq(const groove_FieldIdentity *a, const groove_FieldIdentity *b);
void groove__records__FieldIdentity__Hash__hash(const groove_FieldIdentity *v, groove_Hasher *h);
void groove__records__FieldIdentity__Debug__fmt(jz_str *out, const groove_FieldIdentity *v);

typedef struct groove_DescriptorField {
    bool has_name; jz_str name;
    bool has_identity; groove_FieldIdentity identity;
    groove_ValueType value_type;
} groove_DescriptorField;
typedef struct groove_DescriptorFieldInit { const char *name; groove_ValueType value_type; } groove_DescriptorFieldInit;

const groove_DescriptorField *groove__records__RecordDescriptor__fields(groove_RecordDescriptor d, u32 *len);
bool groove__records__RecordDescriptor__registry_compatible_with(groove_RecordDescriptor a, groove_RecordDescriptor b);
bool groove__records__RecordDescriptor__can_evolve_registry_to(groove_RecordDescriptor a, groove_RecordDescriptor b);
bool groove__records__RecordDescriptor__field_index(groove_RecordDescriptor d, const char *p, u32 n, u32 *out);
/* new(fields): takes ownership of the value types */
groove_RecordDescriptor groove__records__RecordDescriptor__new(const jz_str *names, groove_ValueType *types, u32 n);
groove_RecordDescriptor groove__records__RecordDescriptor__new_with_fields(groove_DescriptorField *fields, u32 n);
void groove__records__RecordDescriptor__Debug__fmt(jz_str *out, groove_RecordDescriptor d);
void groove__records__RecordDescriptor__Serialize__serialize(jz_pc_w *w, groove_RecordDescriptor d);
jz_err groove__records__RecordDescriptor__Deserialize__deserialize(jz_pc_r *r, groove_RecordDescriptor *out);

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
jz_err groove__records__encode_single_field_value(const groove_Value *v, const groove_ValueType *t, jz_bytes *out);
jz_err groove__records__RecordDescriptor__create(groove_RecordDescriptor d, const groove_Value *values, u32 n, jz_bytes *out);
/* BorrowedRecord<'a> */
typedef struct groove_BorrowedRecord { jz_slice raw; groove_RecordDescriptor descriptor; } groove_BorrowedRecord;
jz_err groove__records__BorrowedRecord__get_idx(const groove_BorrowedRecord *r, u32 idx, groove_Value *out);
jz_err groove__records__BorrowedRecord__to_values(const groove_BorrowedRecord *r, groove_ValueVec *out);
jz_err groove__records__BorrowedRecord__validate(const groove_BorrowedRecord *r);
jz_err groove__records__BorrowedRecord__validate_canonical(const groove_BorrowedRecord *r);
jz_err groove__records__BorrowedRecord__get_bytes(const groove_BorrowedRecord *r, u32 idx, jz_slice *out);
groove_OwnedRecord groove__records__OwnedRecord__new(jz_bytes raw, groove_RecordDescriptor d);
void groove__records__OwnedRecord__drop(groove_OwnedRecord *r);
void groove__records__OwnedRecord__Clone__clone(groove_OwnedRecord *dst, const groove_OwnedRecord *src);
/* VariantRecord / ValidatedVariantRecord */
typedef struct groove_VariantRecord { u32 variant_tag; groove_OwnedRecord record; } groove_VariantRecord;
typedef groove_VariantRecord groove_ValidatedVariantRecord;
void groove__records__encode_variant_record(u32 variant_tag, const u8 *payload, u32 n, jz_bytes *out);
void groove__records__append_variant_record(jz_bytes *stored, u32 variant_tag, const u8 *payload, u32 n);
jz_err groove__records__split_variant_record(const u8 *stored, u32 n, u32 *tag, jz_slice *payload);
jz_err groove__records__VariantRecord__validate(const groove_VariantRecord *r);
/* records::Error: codes owned by records unit; we only propagate jz_err. */
enum { GROOVE_RECORDS_ERR_ARITY_MISMATCH = 0x0300, GROOVE_RECORDS_ERR_TYPE_MISMATCH };

/* ===================== schema (crates/groove/src/schema.rs) ===================== */
typedef struct groove_IndexSchema { jz_str name; groove_StrVec columns; bool unique; } groove_IndexSchema;
void groove__schema__IndexSchema__Clone__clone(groove_IndexSchema *dst, const groove_IndexSchema *src);
void groove__schema__IndexSchema__drop(groove_IndexSchema *v);
bool groove__schema__IndexSchema__PartialEq__eq(const groove_IndexSchema *a, const groove_IndexSchema *b);
void groove__schema__IndexSchema__Hash__hash(const groove_IndexSchema *v, groove_Hasher *h);
void groove__schema__IndexSchema__Debug__fmt(jz_str *out, const groove_IndexSchema *v);


/* ===================== records (additional, used by db/) ===================== */
jz_err groove__records__RecordDescriptor__get(groove_RecordDescriptor d, const u8 *raw, u32 n, const char *name, u32 name_len, groove_Value *out);
/* visit_large_value_refs(record, |r| -> bool stop): *found = whether f returned true */
typedef bool (*groove_LargeValueRefVisitor)(void *ctx, const groove_LargeValueRef *reference);
jz_err groove__records__RecordDescriptor__visit_large_value_refs(groove_RecordDescriptor d, const u8 *raw, u32 n, groove_LargeValueRefVisitor f, void *ctx, bool *found);
jz_err groove__records__ensure_value_type(const groove_Value *value, const groove_ValueType *value_type);
jz_err groove__records__u48_be_bytes(u64 value, u8 out[6]);
u64 groove__records__u48_from_be_bytes(const u8 bytes[6]);
jz_err groove__records__Error__arity_mismatch(u32 expected, u32 actual);   /* records::Error::ArityMismatch */
jz_err groove__records__Error__field_not_found(const char *p, u32 n);     /* records::Error::FieldNotFound */
jz_err groove__records__ScalarEnumSchema__variant(const groove_ScalarEnumSchema *s, u8 discriminant);
jz_err groove__records__ScalarEnumSchema__discriminant(const groove_ScalarEnumSchema *s, const jz_str *variant, u8 *out);
jz_err groove__records__VariantRecord__validate_record(const groove_VariantRecord *r); /* record.record().validate() */
void groove__records__VariantRecord__drop(groove_VariantRecord *r);
void groove__records__VariantRecord__Clone__clone(groove_VariantRecord *dst, const groove_VariantRecord *src);
void groove__records__values__ValueVec__drop(groove_ValueVec *v);
void groove__records__values__ValueVec__Clone__clone(groove_ValueVec *dst, const groove_ValueVec *src);

/* ===================== large_values (crates/groove/src/large_values.rs) =====================
 * Layouts mirror src/groove/large_values.h (groove-storage unit). */
typedef struct groove_ContentHash { u8 b[32]; } groove_ContentHash;
typedef struct groove_Locator { u8 b[32]; } groove_Locator;
typedef struct groove_NodeRef { groove_ContentHash object_hash; groove_Locator locator; } groove_NodeRef;
typedef JZ_VEC(groove_NodeRef) groove_NodeRefVec;
typedef struct groove_NodeMetrics { u64 byte_length; bool has_utf16; u64 utf16_length; } groove_NodeMetrics;
typedef struct groove_BranchChild { groove_NodeRef node_ref; groove_NodeMetrics metrics; groove_ContentHash logical_hash; } groove_BranchChild;
typedef JZ_VEC(groove_BranchChild) groove_BranchChildVec;
enum { GROOVE_CHUNK_NODE_LEAF = 0, GROOVE_CHUNK_NODE_BRANCH = 1 };
typedef struct groove_ChunkNode { u8 tag; u8 format; u8 kind; jz_bytes bytes; groove_BranchChildVec children; } groove_ChunkNode;
typedef struct groove_ReplaceEdit { u64 offset, delete_length; jz_bytes insert_bytes; u64 utf16_offset, delete_utf16_length, insert_utf16_length; } groove_ReplaceEdit;
typedef JZ_VEC(groove_ReplaceEdit) groove_ReplaceEditVec;
enum { GROOVE_LVK_BYTES = 0, GROOVE_LVK_STRING = 1, GROOVE_LVK_JSON = 2 };
struct groove_LargeValueRef {
    u8 kind; u8 format_version;
    groove_ContentHash logical_hash;
    groove_NodeRef root;
    u64 byte_length;
    bool has_utf16_length; u64 utf16_length;
    groove_ReplaceEditVec edit_tail;
};
typedef struct groove_StagedChunk { groove_NodeRef node_ref; jz_bytes encoded; } groove_StagedChunk;
typedef JZ_VEC(groove_StagedChunk) groove_StagedChunkVec;
typedef struct groove_PreparedLargeValue { groove_LargeValueRef value_ref; groove_StagedChunkVec staged_chunks; } groove_PreparedLargeValue;
typedef struct groove_StagedLargeValueId { u8 b[16]; } groove_StagedLargeValueId;
typedef JZ_VEC(groove_StagedLargeValueId) groove_StagedLargeValueIdVec;
typedef struct groove_StagedLargeValueAccounting { u64 encoded_bytes; u64 node_count; } groove_StagedLargeValueAccounting;
typedef struct groove_StagedLargeValue { groove_StagedLargeValueId id; groove_LargeValueRef value_ref; groove_StagedLargeValueAccounting accounting; u64 created_at_ms; } groove_StagedLargeValue;
typedef JZ_VEC(groove_StagedLargeValue) groove_StagedLargeValueVec;
typedef struct groove_PendingLargeValueUpload {
    groove_StagedLargeValueId id;
    bool has_descriptor; groove_LargeValueRef descriptor;
    bool has_receipt_id; groove_StagedLargeValueId receipt_id;
    groove_StagedLargeValueAccounting accounting;
    u64 created_at_ms;
    groove_NodeRefVec chunks;
} groove_PendingLargeValueUpload;
typedef JZ_VEC(groove_PendingLargeValueUpload) groove_PendingLargeValueUploadVec;
enum { GROOVE_UPLOAD_MISSING = 0, GROOVE_UPLOAD_STAGED = 1 };
typedef struct groove_LargeValueUploadProgress { u8 tag; groove_NodeRefVec missing; groove_StagedLargeValue staged; } groove_LargeValueUploadProgress;
typedef struct groove_StreamingExecutionStats { u64 logical_bytes_consumed; u64 windows_consumed; u64 cooperative_yields; } groove_StreamingExecutionStats;
typedef struct groove_LargeValueCursor { groove_LargeValueRef value; u64 offset; u64 window_bytes; } groove_LargeValueCursor;
typedef struct groove_StreamingChecksum groove_StreamingChecksum;           /* opaque: storage unit */
typedef struct groove_LogicalValueValidator groove_LogicalValueValidator;   /* opaque: storage unit */
typedef struct groove_ConsolidationContinuation groove_ConsolidationContinuation; /* opaque */
enum { GROOVE_TAIL_UPDATED = 0, GROOVE_TAIL_CONSOLIDATION_REQUIRED = 1 };
typedef struct groove_TailOutcome { u8 tag; groove_LargeValueRef value; } groove_TailOutcome;
typedef struct groove_PhysicalTraversalNodeBudget { u32 remaining; } groove_PhysicalTraversalNodeBudget;
enum { GROOVE_JSON_PTR_FOUND = 0, GROOVE_JSON_PTR_NEED_MORE = 1, GROOVE_JSON_PTR_REQUIRES_FULL = 2 };
typedef struct groove_JsonPointerPrefix { u8 tag; jz_json *found; /* Found(Option<Value>): NULL = None */ } groove_JsonPointerPrefix;
/* large_values::Error codes (large_values.h): base 0x1100 */
enum { GROOVE_EXT_LV_E_INVALID_UTF8 = 0x1101, GROOVE_EXT_LV_E_INVALID_JSON = 0x1102, GROOVE_EXT_LV_E_MALFORMED_SCALAR = 0x110B };
#define GROOVE_EXT_LV_LEAF_MIN_BYTES (16u * 1024u)
jz_err groove__large_values__Error__new(u32 code);
/* ReachabilityError { LargeValue(Error) | Chunk(ChunkError) } is transparent over its inner codes */
int groove__large_values__NodeRef__Ord__cmp(const void *a, const void *b, void *ctx);
void groove__large_values__LargeValueRef__drop(groove_LargeValueRef *r);
void groove__large_values__LargeValueRef__Clone__clone(groove_LargeValueRef *dst, const groove_LargeValueRef *src);
bool groove__large_values__LargeValueRef__PartialEq__eq(const groove_LargeValueRef *a, const groove_LargeValueRef *b);
void groove__large_values__ChunkNode__drop(groove_ChunkNode *n);
void groove__large_values__PreparedLargeValue__drop(groove_PreparedLargeValue *p);
void groove__large_values__StagedChunk__drop(void *chunk);
void groove__large_values__StagedLargeValue__drop(groove_StagedLargeValue *s);
void groove__large_values__PendingLargeValueUpload__drop(groove_PendingLargeValueUpload *u);
void groove__large_values__PendingLargeValueUpload__Clone__clone(groove_PendingLargeValueUpload *dst, const groove_PendingLargeValueUpload *src);
jz_err groove__large_values__encode_node_ref(const groove_NodeRef *node_ref, jz_bytes *out);
jz_err groove__large_values__decode_node_ref(const u8 *encoded, u32 n, groove_NodeRef *out);
groove_RecordDescriptor groove__large_values__node_ref_descriptor(void);
groove_Value groove__large_values__node_ref_value(const groove_NodeRef *node_ref);
jz_err groove__large_values__node_ref_from_value(const groove_Value *value, groove_NodeRef *out);
jz_err groove__large_values__encode_large_value_ref(const groove_LargeValueRef *value, jz_bytes *out);
jz_err groove__large_values__decode_large_value_ref(const u8 *encoded, u32 n, groove_LargeValueRef *out);
jz_err groove__large_values__decode_node_untyped_authenticated(groove_ContentHash expected_hash, const u8 *p, u32 n, groove_ChunkNode *out);
jz_err groove__large_values__decode_authenticated_node(groove_ContentHash expected_hash, const u8 *p, u32 n, groove_ChunkNode *out);
jz_err groove__large_values__validate_staged_chunk_batch(u8 kind, const groove_StagedChunk *chunks, u32 n);
jz_err groove__large_values__prepare(u8 kind, const u8 *p, u32 n, groove_PreparedLargeValue *out);
groove_PhysicalTraversalNodeBudget groove__large_values__PhysicalTraversalNodeBudget__new(void);
jz_err groove__large_values__PhysicalTraversalNodeBudget__consume(groove_PhysicalTraversalNodeBudget *b);
jz_err groove__large_values__PhysicalTraversalNodeBudget__consume_many(groove_PhysicalTraversalNodeBudget *b, u32 count);
void groove__large_values__record_finalize_validation_bytes(u32 bytes);
typedef struct groove_EvaluationInputs groove_EvaluationInputs; /* ivm::runtime::evaluation_session */
typedef struct groove_LocalChunkReader { void *storage; /* Rc<RefCell<Rc<dyn ChunkStorage>>> */ } groove_LocalChunkReader;
jz_err groove__large_values__missing_upload_frontier(const groove_LargeValueRef *value, const groove_LocalChunkReader *reader, u32 limit, groove_NodeRefVec *out);
/* uploaded_chunks: BTreeSet<NodeRef> */
jz_err groove__large_values__validate_finalized_upload(const groove_LargeValueRef *value, const groove_LocalChunkReader *reader, const jz_btree *uploaded_chunks, bool descriptor_was_bound_before_completion);
jz_err groove__large_values__validate_derived_upload(const groove_LargeValueRef *value, const groove_LargeValueRef *base, const groove_LocalChunkReader *reader, const jz_btree *staged_chunks, bool *ok);
jz_err groove__large_values__LogicalValueValidator__new(const groove_LargeValueRef *value, groove_LogicalValueValidator **out);
jz_err groove__large_values__LogicalValueValidator__push(groove_LogicalValueValidator *v, const u8 *bytes, u32 n);
jz_err groove__large_values__LogicalValueValidator__finish(groove_LogicalValueValidator *v, const groove_LargeValueRef *value);
void groove__large_values__LogicalValueValidator__drop(groove_LogicalValueValidator *v);
jz_err groove__large_values__validate_edit_tail_attempt(const groove_LargeValueRef *value, groove_EvaluationInputs *inputs);
jz_err groove__large_values__byte_range_attempt(const groove_LargeValueRef *value, u64 start, u64 end, groove_EvaluationInputs *inputs, jz_bytes *out);
jz_err groove__large_values__utf16_range_attempt(const groove_LargeValueRef *value, u64 start, u64 end, groove_EvaluationInputs *inputs, jz_bytes *out);
jz_err groove__large_values__utf16_offset_to_byte_attempt(const groove_LargeValueRef *value, u64 offset, groove_EvaluationInputs *inputs, u64 *out);
jz_err groove__large_values__replace_tail_attempt(const groove_LargeValueRef *value, u64 offset, u64 delete_length, jz_bytes insert_bytes, groove_EvaluationInputs *inputs, groove_TailOutcome *out);
jz_err groove__large_values__append_tail(const groove_LargeValueRef *value, jz_bytes insert_bytes, groove_TailOutcome *out);
jz_err groove__large_values__ConsolidationContinuation__new(groove_LargeValueRef source, groove_ConsolidationContinuation **out);
jz_err groove__large_values__ConsolidationContinuation__step(groove_ConsolidationContinuation *c, groove_EvaluationInputs *inputs, bool *has_out, groove_PreparedLargeValue *out);
void groove__large_values__ConsolidationContinuation__drop(groove_ConsolidationContinuation *c);
jz_err groove__large_values__StreamingChecksum__new(groove_LargeValueRef value, u32 window_bytes, u32 max_bytes_per_turn, groove_StreamingChecksum **out);
const groove_LargeValueCursor *groove__large_values__StreamingChecksum__cursor(const groove_StreamingChecksum *s);
jz_err groove__large_values__StreamingChecksum__consume_window(groove_StreamingChecksum *s, const u8 *bytes, u32 n, bool *yield);
jz_err groove__large_values__StreamingChecksum__record_yield(groove_StreamingChecksum *s);
jz_err groove__large_values__StreamingChecksum__finish(groove_StreamingChecksum *s, groove_ContentHash *hash, groove_StreamingExecutionStats *stats);
void groove__large_values__StreamingChecksum__drop(groove_StreamingChecksum *s);
u64 groove__large_values__LargeValueCursor__remaining_bytes(const groove_LargeValueCursor *c);
const groove_LargeValueRef *groove__large_values__LargeValueCursor__value(const groove_LargeValueCursor *c);
bool groove__large_values__LargeValueCursor__next_range(const groove_LargeValueCursor *c, u64 *start, u64 *end);
void groove__large_values__LargeValueCursor__advance_to(groove_LargeValueCursor *c, u64 offset);
jz_err groove__large_values__json_pointer_prefix(const u8 *source, u32 n, const char *pointer, u32 pointer_len, groove_JsonPointerPrefix *out);

/* ===================== chunks (crates/groove/src/chunks.rs) ===================== */
enum { GROOVE_EXT_CHUNK_E_UNAVAILABLE = 0x1211, GROOVE_EXT_CHUNK_E_RETRYABLE = 0x1212, GROOVE_EXT_CHUNK_E_BACKEND = 0x1213, GROOVE_EXT_CHUNK_E_INTEGRITY = 0x1214 };
jz_err groove__chunks__ChunkError__backend(const char *p, u32 n);
jz_err groove__chunks__ChunkError__integrity(void);
jz_err groove__chunks__ChunkError__From__from(jz_err storage_error); /* From<ChunkStorageError> */
typedef struct groove_ChunkRequest { u8 object_hash[32]; groove_Locator locator; } groove_ChunkRequest;
typedef JZ_VEC(groove_ChunkRequest) groove_ChunkRequestVec;
/* Rc<dyn ChunkStorage> */
typedef struct groove_ChunkStorageVtbl {
    jz_err (*get)(void *self, groove_Locator locator, groove_ContentHash expected_hash, jz_bytes *out);
    jz_err (*stage)(void *self, groove_StagedChunkVec chunks, groove_StagedLargeValueAccounting *out);
    jz_err (*delete_)(void *self, groove_Locator locator, groove_ContentHash expected_hash);
    void (*drop)(void *self);
} groove_ChunkStorageVtbl;
typedef struct groove_DynChunkStorage { void *self; const groove_ChunkStorageVtbl *vt; } groove_DynChunkStorage;
jz_err groove__chunks__ChunkStorage__delete(groove_DynChunkStorage s, groove_Locator locator, groove_ContentHash expected_hash);
groove_DynChunkStorage groove__chunks__DynChunkStorage__clone(const groove_DynChunkStorage *s);
void groove__chunks__DynChunkStorage__drop(groove_DynChunkStorage *s);
/* Rc<dyn MissingChunkResolver> */
typedef struct groove_DynMissingChunkResolver { void *self; const void *vt; } groove_DynMissingChunkResolver;
groove_DynMissingChunkResolver groove__chunks__DynMissingChunkResolver__clone(const groove_DynMissingChunkResolver *r);
void groove__chunks__DynMissingChunkResolver__drop(groove_DynMissingChunkResolver *r);
groove_DynMissingChunkResolver groove__chunks__UnavailableChunkResolver__new(void);
typedef struct groove_LayoutStorage groove_LayoutStorage;
/* ManagedChunkStorage::new(Rc::new(OrderedChunkStorage::new(Rc::downgrade(&storage)))) */
groove_DynChunkStorage groove__chunks__ManagedChunkStorage__new_ordered(groove_LayoutStorage *weak_storage);
/* trait ChunkInstallObserver / ChunkInstallJournal as vtables (Rc<dyn ...>) */
typedef struct groove_ChunkInstallObserverVt {
    jz_err (*installed)(void *self, groove_NodeRef node_ref, jz_bytes encoded);
    bool (*completes_install_journal)(void *self);
    void (*drop)(void *self);
} groove_ChunkInstallObserverVt;
typedef struct groove_DynChunkInstallObserver { void *self; const groove_ChunkInstallObserverVt *vt; } groove_DynChunkInstallObserver;
typedef struct groove_ChunkInstallJournalVt {
    jz_err (*mark_pending)(void *self, groove_NodeRef node_ref);
    jz_err (*is_pending)(void *self, groove_NodeRef node_ref, bool *out);
    jz_err (*complete)(void *self, groove_NodeRef node_ref);
    void (*drop)(void *self);
} groove_ChunkInstallJournalVt;
typedef struct groove_DynChunkInstallJournal { void *self; const groove_ChunkInstallJournalVt *vt; } groove_DynChunkInstallJournal;
/* PublicationInstallFailures: Rc<RefCell<..>> handle */
typedef struct groove_PublicationInstallFailures { void *inner; } groove_PublicationInstallFailures;
groove_PublicationInstallFailures groove__chunks__PublicationInstallFailures__default(void);
groove_PublicationInstallFailures groove__chunks__PublicationInstallFailures__clone(const groove_PublicationInstallFailures *f);
void groove__chunks__PublicationInstallFailures__drop(groove_PublicationInstallFailures *f);
void groove__chunks__PublicationInstallFailures__record(const groove_PublicationInstallFailures *f, groove_NodeRef node_ref, jz_err error);
/* Rc<dyn ChunkProvider> */
typedef struct groove_DynChunkProvider { void *self; const void *vt; } groove_DynChunkProvider;
groove_DynChunkProvider groove__chunks__StorageChunkProvider__with_resolver_observer_and_journal(
    groove_DynChunkStorage storage, groove_DynMissingChunkResolver resolver,
    groove_DynChunkInstallObserver observer, groove_DynChunkInstallJournal journal);
/* OwnedChunkProvider (verified-cache context) */
typedef struct groove_OwnedChunkProvider { void *inner; } groove_OwnedChunkProvider;
typedef struct groove_ChunkLease groove_ChunkLease;
jz_err groove__chunks__OwnedChunkProvider__get(const groove_OwnedChunkProvider *p, groove_ChunkRequest request, groove_ChunkLease **out);
const jz_bytes *groove__chunks__ChunkLease__bytes(const groove_ChunkLease *l);
/* try_begin_reclamation() -> Option<ChunkReclamationGuard>; NULL = None */
void *groove__chunks__OwnedChunkProvider__try_begin_reclamation(const groove_OwnedChunkProvider *p);
void groove__chunks__ChunkReclamationGuard__drop(void *guard);
void groove__chunks__OwnedChunkProvider__drop(groove_OwnedChunkProvider *p);
groove_LocalChunkReader groove__chunks__LocalChunkReader__new(groove_DynChunkStorage storage);
void groove__chunks__LocalChunkReader__drop(groove_LocalChunkReader *r);
jz_err groove__chunks__LocalChunkReader__get(const groove_LocalChunkReader *r, groove_Locator locator, groove_ContentHash expected_hash, jz_bytes *out);

/* ===================== storage (crates/groove/src/storage/mod.rs) ===================== */
enum { GROOVE_EXT_STORAGE_ERR_BASE = 0x0400, GROOVE_EXT_STORAGE_ERR_END = 0x04ff };
jz_err groove__storage__Error__backend(const char *backend, const jz_str *message); /* Error::Backend { backend, message } */
typedef struct groove_KvPair { jz_bytes key; jz_bytes value; } groove_KvPair;
typedef JZ_VEC(groove_KvPair) groove_KvPairVec;
void groove__storage__KvPair__drop(void *p);
enum { GROOVE_SCAN_BOUNDS_PREFIX = 0, GROOVE_SCAN_BOUNDS_RANGE = 1 };
typedef struct groove_ScanRequest {
    jz_str cf;
    u8 bounds_tag; jz_bytes prefix_or_start; jz_bytes end;
    u8 direction; /* 0 forward, 1 reverse */
    bool has_max_items; u32 max_items;
} groove_ScanRequest;
groove_ScanRequest groove__storage__ScanRequest__prefix(jz_str cf, jz_bytes prefix);
void groove__storage__ScanRequest__with_max_items(groove_ScanRequest *r, u32 max_items);
enum { GROOVE_WRITE_OP_SET = 0, GROOVE_WRITE_OP_DELETE = 1 };
typedef struct groove_OwnedWriteOperation { u8 tag; jz_str cf; jz_bytes key; jz_bytes value; } groove_OwnedWriteOperation;
typedef JZ_VEC(groove_OwnedWriteOperation) groove_WriteOpVec;
void groove__storage__OwnedWriteOperation__drop(void *op);
void groove__storage__OwnedWriteOperation__Clone__clone(void *dst, const void *src);
/* WriteOperation<'a> borrowed view */
typedef struct groove_WriteOperation { u8 tag; jz_slice cf; jz_slice key; jz_slice value; } groove_WriteOperation;
typedef JZ_VEC(groove_WriteOperation) groove_WriteOperationVec;
groove_WriteOperation groove__storage__OwnedWriteOperation__as_write_operation(const groove_OwnedWriteOperation *op);
enum { GROOVE_VALUE_CMP_ABSENT = 0, GROOVE_VALUE_CMP_IDENTICAL = 1, GROOVE_VALUE_CMP_DIFFERENT = 2 };
enum { GROOVE_WRITE_MANY_COMMITTED = 0, GROOVE_WRITE_MANY_UNCOMMITTED = 1, GROOVE_WRITE_MANY_POSSIBLY_COMMITTED = 2 };
/* Box<dyn StorageCursor> */
typedef struct groove_StorageCursorVt {
    jz_err (*next_batch)(void *self, bool *some, groove_KvPairVec *out);
    void (*drop)(void *self);
} groove_StorageCursorVt;
typedef struct groove_StorageCursor { void *self; const groove_StorageCursorVt *vt; } groove_StorageCursor;
jz_err groove__storage__StorageCursor__next_batch(groove_StorageCursor *c, bool *some, groove_KvPairVec *out);
void groove__storage__StorageCursor__drop(groove_StorageCursor *c);
/* trait OrderedKvStorage as a vtable; NULL entries use the trait default
 * (dispatched by groove__storage__OrderedKvStorage__* below). Async -> sync. */
typedef struct groove_OrderedKvStorageVt {
    bool (*permits_eager_read_retry)(const void *self);
    jz_err (*get)(const void *self, jz_str cf, jz_bytes key, bool *found, jz_bytes *out);
    jz_err (*compare_value)(const void *self, jz_str cf, jz_bytes key, jz_bytes expected, u8 *out);
    jz_err (*put_if_absent)(const void *self, jz_str cf, jz_bytes key, jz_bytes value, bool *found, jz_bytes *existing);
    jz_err (*compare_and_delete)(const void *self, jz_str cf, jz_bytes key, jz_bytes expected, bool *removed);
    jz_err (*set)(const void *self, jz_str cf, jz_bytes key, jz_bytes value);
    jz_err (*delete_)(const void *self, jz_str cf, jz_bytes key);
    jz_err (*close)(const void *self);
    jz_err (*set_write_flush_cadence)(const void *self, u32 every);
    jz_err (*flush_write_boundary)(const void *self);
    jz_err (*approximate_class_bytes)(const void *self, jz_str cf, bool *some, u64 *out);
    jz_err (*scan)(const void *self, groove_ScanRequest request, groove_StorageCursor *out);
    jz_err (*last_with_prefix)(const void *self, jz_str cf, jz_bytes prefix, bool *some, groove_KvPair *out);
    jz_err (*last_with_prefix_before_or_at)(const void *self, jz_str cf, jz_bytes prefix, jz_bytes upper, bool *some, groove_KvPair *out);
    jz_err (*write_many)(const void *self, groove_WriteOpVec operations);
    u8 (*write_many_outcome)(const void *self, groove_WriteOpVec operations, jz_err *error);
    u8 (*write_many_borrowed_outcome)(const void *self, groove_WriteOperationVec operations, jz_err *error);
    bool (*column_family_names)(const void *self, groove_StrVec *out);
} groove_OrderedKvStorageVt;
/* &dyn OrderedKvStorage */
typedef struct groove_DynStorage { const void *self; const groove_OrderedKvStorageVt *vt; } groove_DynStorage;
bool groove__storage__OrderedKvStorage__permits_eager_read_retry(groove_DynStorage s);
jz_err groove__storage__OrderedKvStorage__get(groove_DynStorage s, jz_str cf, jz_bytes key, bool *found, jz_bytes *out);
jz_err groove__storage__OrderedKvStorage__compare_value(groove_DynStorage s, jz_str cf, jz_bytes key, jz_bytes expected, u8 *out);
jz_err groove__storage__OrderedKvStorage__put_if_absent(groove_DynStorage s, jz_str cf, jz_bytes key, jz_bytes value, bool *found, jz_bytes *existing);
jz_err groove__storage__OrderedKvStorage__compare_and_delete(groove_DynStorage s, jz_str cf, jz_bytes key, jz_bytes expected, bool *removed);
jz_err groove__storage__OrderedKvStorage__set(groove_DynStorage s, jz_str cf, jz_bytes key, jz_bytes value);
jz_err groove__storage__OrderedKvStorage__delete(groove_DynStorage s, jz_str cf, jz_bytes key);
jz_err groove__storage__OrderedKvStorage__close(groove_DynStorage s);
jz_err groove__storage__OrderedKvStorage__set_write_flush_cadence(groove_DynStorage s, u32 every);
jz_err groove__storage__OrderedKvStorage__flush_write_boundary(groove_DynStorage s);
jz_err groove__storage__OrderedKvStorage__approximate_class_bytes(groove_DynStorage s, jz_str cf, bool *some, u64 *out);
jz_err groove__storage__OrderedKvStorage__scan(groove_DynStorage s, groove_ScanRequest request, groove_StorageCursor *out);
jz_err groove__storage__OrderedKvStorage__last_with_prefix(groove_DynStorage s, jz_str cf, jz_bytes prefix, bool *some, groove_KvPair *out);
jz_err groove__storage__OrderedKvStorage__last_with_prefix_before_or_at(groove_DynStorage s, jz_str cf, jz_bytes prefix, jz_bytes upper, bool *some, groove_KvPair *out);
jz_err groove__storage__OrderedKvStorage__write_many(groove_DynStorage s, groove_WriteOpVec operations);
u8 groove__storage__OrderedKvStorage__write_many_borrowed_outcome(groove_DynStorage s, groove_WriteOperationVec operations, jz_err *error);
bool groove__storage__OrderedKvStorage__column_family_names(groove_DynStorage s, groove_StrVec *out);
jz_err groove__storage__validate_application_storage_name(const char *name, u32 len);
/* LayoutStorage: jz_rc payload */
typedef struct groove_BoxedStorage { void *self; const groove_OrderedKvStorageVt *vt; } groove_BoxedStorage;
void groove__storage__BoxedStorage__drop(groove_BoxedStorage *b);
typedef struct groove_StorageLayout { u8 tag; } groove_StorageLayout; /* Identity | JazzClassV2 */
jz_err groove__storage__LayoutStorage__new_boxed(groove_BoxedStorage inner, groove_StorageLayout layout, groove_LayoutStorage **out);
groove_DynStorage groove__storage__LayoutStorage__as_dyn(const groove_LayoutStorage *s);
groove_BoxedStorage groove__storage__LayoutStorage__into_inner(groove_LayoutStorage *s);
jz_err groove__storage__LayoutStorage__approximate_class_bytes_for_logical_families(const groove_LayoutStorage *s, const jz_slice *families, u32 n, bool *some, u64 *out);
/* StagedWriteState: Rc<RefCell<StagedWriteState>> is a jz_rc payload */
typedef struct groove_StagedWriteState groove_StagedWriteState;
typedef struct groove_StagedSnapshot groove_StagedSnapshot; /* Arc<[Arc<[OwnedWriteOperation]>]> */
groove_StagedWriteState *groove__storage__StagedWriteState__default(void);
groove_StagedWriteState *groove__storage__StagedWriteState__from_ops(groove_WriteOpVec operations);
void groove__storage__StagedWriteState__reset(groove_StagedWriteState *s); /* *s = Default::default() */
void groove__storage__StagedWriteState__stage(groove_StagedWriteState *s, groove_OwnedWriteOperation op);
void groove__storage__StagedWriteState__extend(groove_StagedWriteState *s, groove_WriteOpVec ops);
void groove__storage__StagedWriteState__extend_shared(groove_StagedWriteState *s, groove_StagedSnapshot *snapshot);
groove_StagedSnapshot *groove__storage__StagedWriteState__snapshot(const groove_StagedWriteState *s);
void groove__storage__StagedSnapshot__release(groove_StagedSnapshot *s);
u32 groove__storage__StagedSnapshot__blocks(const groove_StagedSnapshot *s);
const groove_OwnedWriteOperation *groove__storage__StagedSnapshot__block(const groove_StagedSnapshot *s, u32 block, u32 *len);
groove_WriteOpVec groove__storage__StagedWriteState__into_operations(groove_StagedWriteState *s);
bool groove__storage__StagedWriteState__contains_key(groove_StagedWriteState *s, const char *cf, u32 cf_len, const u8 *key, u32 key_len);
/* StagedWriteOverlay<'a, S> (read-your-own-writes view) */
typedef struct groove_StagedWriteOverlay groove_StagedWriteOverlay;
groove_StagedWriteOverlay *groove__storage__StagedWriteOverlay__new(groove_DynStorage base, groove_StagedWriteState *state);
/* new_owned(Rc<S>, Rc<RefCell<StagedWriteState>>): base_rc is the jz_rc owning `base` */
groove_StagedWriteOverlay *groove__storage__StagedWriteOverlay__new_owned(void *base_rc, groove_DynStorage base, groove_StagedWriteState *state);
groove_DynStorage groove__storage__StagedWriteOverlay__as_dyn(const groove_StagedWriteOverlay *o);
void groove__storage__StagedWriteOverlay__release(groove_StagedWriteOverlay *o); /* Rc / drop */
enum { GROOVE_STAGED_POINT_MISS = 0, GROOVE_STAGED_POINT_SET = 1, GROOVE_STAGED_POINT_DELETE = 2 };
u8 groove__storage__StagedWriteOverlay__staged_point_value(const groove_StagedWriteOverlay *o, const char *cf, u32 cf_len, const u8 *key, u32 key_len, jz_slice *value);
/* OwnedStorage<'a>(Rc<dyn OrderedKvStorage>) */
typedef struct groove_OwnedStorage { void *rc; groove_DynStorage storage; } groove_OwnedStorage;
groove_OwnedStorage groove__storage__OwnedStorage__new(void *rc, groove_DynStorage storage);
groove_OwnedStorage groove__storage__OwnedStorage__clone(const groove_OwnedStorage *s);
void groove__storage__OwnedStorage__drop(groove_OwnedStorage *s);
/* RecordStore<'a, S> */
typedef struct groove_RecordStore { groove_DynStorage storage; const char *column_family; u32 cf_len; groove_RecordDescriptor descriptor; } groove_RecordStore;
groove_RecordStore groove__storage__RecordStore__new(groove_DynStorage storage, const char *cf, u32 cf_len, groove_RecordDescriptor descriptor);
jz_err groove__storage__RecordStore__get_raw(const groove_RecordStore *s, const u8 *key, u32 n, bool *found, jz_bytes *out);
jz_err groove__storage__RecordStore__range(const groove_RecordStore *s, const u8 *start, u32 ns, const u8 *end, u32 ne, groove_KvPairVec *out);
jz_err groove__storage__RecordStore__prefix(const groove_RecordStore *s, const u8 *prefix, u32 n, groove_KvPairVec *out);
jz_err groove__storage__RecordStore__last_with_prefix(const groove_RecordStore *s, const u8 *prefix, u32 n, bool *some, groove_KvPair *out);
jz_err groove__storage__RecordStore__last_with_prefix_before_or_at(const groove_RecordStore *s, const u8 *prefix, u32 n, const u8 *upper, u32 nu, bool *some, groove_KvPair *out);

/* ===================== schema (crates/groove/src/schema.rs) ===================== */
typedef groove_ValueType groove_ColumnType;
typedef struct groove_ColumnSchema { jz_str name; groove_ColumnType column_type; } groove_ColumnSchema;
typedef JZ_VEC(groove_ColumnSchema) groove_ColumnSchemaVec;
enum { GROOVE_PK_INTEGER = 0, GROOVE_PK_BOOL, GROOVE_PK_STRING, GROOVE_PK_BYTES, GROOVE_PK_UUID };
enum { GROOVE_INT_KEY_U8 = 0, GROOVE_INT_KEY_U16, GROOVE_INT_KEY_U32, GROOVE_INT_KEY_U64 };
typedef struct groove_PrimaryKeyType { u8 tag; u8 integer; } groove_PrimaryKeyType;
typedef struct groove_PrimaryKeyColumn { jz_str column; groove_PrimaryKeyType key_type; } groove_PrimaryKeyColumn;
typedef struct groove_PrimaryKey { JZ_VEC(groove_PrimaryKeyColumn) columns; bool generated; } groove_PrimaryKey;
groove_ColumnType groove__schema__PrimaryKeyType__column_type(const groove_PrimaryKeyType *t);
typedef struct groove_TableVariantField { jz_str name; groove_ValueType value_type; bool has_shared_column; jz_str shared_column; } groove_TableVariantField;
typedef struct groove_TableVariant { u32 tag; groove_StrVec fields; JZ_VEC(groove_TableVariantField) payload_fields; } groove_TableVariant;
void groove__schema__TableVariant__Clone__clone(groove_TableVariant *dst, const groove_TableVariant *src);
void groove__schema__TableVariant__drop(groove_TableVariant *v);
const jz_str *groove__schema__TableVariant__payload_name_for_shared(const groove_TableVariant *v, const jz_str *shared);
typedef struct groove_TableSchema {
    jz_str name;
    u64 variant_registry_id;
    jz_btree value_variant_registries;
    groove_ColumnSchemaVec columns;
    bool has_primary_key; groove_PrimaryKey primary_key;
    JZ_VEC(groove_IndexSchema) indices;
    jz_vec foreign_keys;
    JZ_VEC(groove_TableVariant) variants;
} groove_TableSchema;
typedef struct groove_KeyField { jz_str name; groove_ValueType value_type; } groove_KeyField;
typedef struct groove_DirectRecordStoreSchema { jz_str name; JZ_VEC(groove_KeyField) key; JZ_VEC(groove_KeyField) value; } groove_DirectRecordStoreSchema;
typedef struct groove_DatabaseSchema { JZ_VEC(groove_TableSchema) tables; JZ_VEC(groove_DirectRecordStoreSchema) direct_record_stores; } groove_DatabaseSchema;
void groove__schema__TableSchema__Clone__clone(groove_TableSchema *dst, const groove_TableSchema *src);
void groove__schema__TableSchema__drop(groove_TableSchema *t);
void groove__schema__DatabaseSchema__Clone__clone(groove_DatabaseSchema *dst, const groove_DatabaseSchema *src);
void groove__schema__DatabaseSchema__drop(groove_DatabaseSchema *s);
groove_RecordDescriptor groove__schema__TableSchema__record_schema(const groove_TableSchema *t);
bool groove__schema__TableSchema__record_schema_for_variant(const groove_TableSchema *t, u32 tag, groove_RecordDescriptor *out);
bool groove__schema__TableSchema__has_variants(const groove_TableSchema *t);
const groove_TableVariant *groove__schema__TableSchema__variant(const groove_TableSchema *t, u32 tag);
const groove_TableSchema *groove__schema__DatabaseSchema__table(const groove_DatabaseSchema *s, const char *name, u32 len);
groove_RecordDescriptor groove__schema__DirectRecordStoreSchema__key_descriptor(const groove_DirectRecordStoreSchema *s);
groove_RecordDescriptor groove__schema__DirectRecordStoreSchema__value_descriptor(const groove_DirectRecordStoreSchema *s);
bool groove__schema__ColumnSchema__PartialEq__eq(const groove_ColumnSchema *a, const groove_ColumnSchema *b);
void groove__schema__ColumnSchema__Clone__clone(groove_ColumnSchema *dst, const groove_ColumnSchema *src);
void groove__schema__ColumnSchema__drop(void *c);
bool groove__records__values__ValueType__registry_compatible_with(const groove_ValueType *a, const groove_ValueType *b);
bool groove__records__values__ValueType__can_evolve_registry_to(const groove_ValueType *a, const groove_ValueType *b);

/* ===================== ivm::runtime (owned by groove-ivm-rt-a/b) ===================== */
typedef struct groove_ProjectField groove_ProjectField;     /* ivm/graph.h (this unit) */
typedef struct groove_InputSourceId groove_InputSourceId; /* ivm/graph.h (this unit) */
typedef struct groove_IvmRuntime groove_IvmRuntime;
/* IvmRuntimeError codes (ivm/runtime/mod.h, base 0x0600). LargeValue, Chunk,
 * Storage and RecordEncoding wrap their inner error transparently. */
enum {
    GROOVE_EXT_IVM_BINDING_SOURCE_DESCRIPTOR_MISMATCH = 0x0621,
    GROOVE_EXT_IVM_FOREIGN_INPUT_SOURCE = 0x0622,
    GROOVE_EXT_IVM_INPUT_SOURCE_RETIRED = 0x0623,
    GROOVE_EXT_IVM_RECORD_ENCODING = 0x0625,
    GROOVE_EXT_IVM_EVALUATION_BLOCKED = 0x062B,
    GROOVE_EXT_IVM_UNIQUE_INDEX_VIOLATION = 0x0643,
};
jz_err groove__ivm__runtime__IvmRuntimeError__evaluation_blocked(void);
bool groove__ivm__runtime__IvmRuntimeError__is_record_encoding(const jz_err *e);
typedef struct groove_IndexIdRegistry groove_IndexIdRegistry;
typedef groove_IndexIdRegistry *groove_SharedIndexIds; /* Rc<RefCell<IndexIdRegistry>> (jz_rc payload) */
typedef struct groove_IndexIdRegistrations groove_IndexIdRegistrations;
jz_err groove__ivm__runtime__IndexIdRegistry__load(groove_DynStorage storage, groove_SharedIndexIds *out);
groove_IndexIdRegistrations *groove__ivm__runtime__IndexIdRegistry__pending(const groove_IndexIdRegistry *r);
void groove__ivm__runtime__IndexIdRegistry__mark_persisted(groove_IndexIdRegistry *r, const groove_IndexIdRegistrations *reg);
bool groove__ivm__runtime__IndexIdRegistry__names(const groove_IndexIdRegistry *r, u32 id, jz_slice *table, jz_slice *index);
bool groove__ivm__runtime__IndexIdRegistrations__is_empty(const groove_IndexIdRegistrations *reg);
const groove_OwnedWriteOperation *groove__ivm__runtime__IndexIdRegistrations__operations(const groove_IndexIdRegistrations *reg, u32 *len);
void groove__ivm__runtime__IndexIdRegistrations__drop(groove_IndexIdRegistrations *reg);
void groove__ivm__runtime__durable_index_key_prefix(u32 index_id, jz_bytes *out);
bool groove__ivm__runtime__split_durable_index_key(const u8 *key, u32 n, u32 *id, jz_slice *logical);
jz_err groove__ivm__runtime__encode_key_part(jz_bytes *key, const groove_Value *value);

typedef struct groove_TickMetrics groove_TickMetrics; /* heap, Clone */
groove_TickMetrics *groove__ivm__runtime__TickMetrics__clone(const groove_TickMetrics *m);
void groove__ivm__runtime__TickMetrics__drop(groove_TickMetrics *m);
typedef struct groove_ResidentTick groove_ResidentTick;
groove_WriteOpVec groove__ivm__runtime__ResidentTick__take_durable_writes(groove_ResidentTick *t);
typedef struct groove_RuntimeStats groove_RuntimeStats;
typedef struct groove_ExecutionLayoutStats { u64 builds, hits; } groove_ExecutionLayoutStats;
typedef struct { u64 runtime_namespace, sequence; } groove_SubscriptionId;
typedef u64 groove_PreparedShapeId;
typedef u64 groove_PublicationId;
typedef struct groove_RecordDeltas groove_RecordDeltas;       /* Arc */
typedef struct groove_MultisinkDeltas groove_MultisinkDeltas; /* heap */
typedef struct groove_Subscription groove_Subscription;
typedef struct groove_MultisinkSubscription groove_MultisinkSubscription;
typedef struct groove_RoutedMultisinkTerminal groove_RoutedMultisinkTerminal;
typedef struct groove_InputSourceReplacement groove_InputSourceReplacement;
typedef struct groove_InputSourceDelta groove_InputSourceDelta;
typedef struct groove_IvmPreparedShape groove_IvmPreparedShape; /* crate::ivm::PreparedShape */
groove_PreparedShapeId groove__ivm__runtime__PreparedShape__id(const groove_IvmPreparedShape *s);
void groove__ivm__runtime__PreparedShape__drop(groove_IvmPreparedShape *s);
typedef struct groove_LiveAttach groove_LiveAttach; /* Option<LiveAttach>: NULL = None */
enum { GROOVE_LIFETIME_FIRST_RESULT = 0, GROOVE_LIFETIME_RETAINED = 1 };
enum { GROOVE_ROOT_MATERIALIZE = 0, GROOVE_ROOT_PHYSICAL = 1, GROOVE_ROOT_PHYSICAL_FIELDS = 2 };
typedef struct groove_RootIndirectValues { u8 tag; jz_btree *physical; /* Arc<BTreeSet<String>> */ } groove_RootIndirectValues;
/* (K, GraphBuilder) sinks */
typedef struct groove_GraphBuilder groove_GraphBuilder;
/* SubscriptionEvent<T> / PublicationUpdate<T> */
enum { GROOVE_SUB_EVENT_UPDATE = 0, GROOVE_SUB_EVENT_ERROR = 1 };
enum { GROOVE_SUB_ERROR_EVALUATION = 0, GROOVE_SUB_ERROR_ENDED = 1 };
typedef struct groove_PublicationUpdate { bool has_publication; groove_PublicationId publication; void *deltas; /* T */ } groove_PublicationUpdate;
typedef struct groove_SubscriptionEvent { u8 tag; groove_PublicationUpdate update; u8 error_tag; jz_err error; } groove_SubscriptionEvent;
/* Poll: returns true when Ready */
bool groove__ivm__runtime__Subscription__poll_next_event(const groove_Subscription *s, const jz_waker *cx, groove_SubscriptionEvent *out);
bool groove__ivm__runtime__MultisinkSubscription__poll_next_event(const groove_MultisinkSubscription *s, const jz_waker *cx, groove_SubscriptionEvent *out);
jz_err groove__ivm__runtime__SubscriptionError__into_error(u8 error_tag, jz_err error); /* Error::SubscriptionFailed: transparent */

jz_err groove__ivm__runtime__IvmRuntime__new(groove_DatabaseSchema schema, groove_IvmRuntime **out);
jz_err groove__ivm__runtime__IvmRuntime__new_with_index_ids(groove_DatabaseSchema schema, groove_SharedIndexIds index_ids, groove_IvmRuntime **out);
groove_IvmRuntime *groove__ivm__runtime__IvmRuntime__Clone__clone(const groove_IvmRuntime *rt);
void groove__ivm__runtime__IvmRuntime__drop(groove_IvmRuntime *rt);
const groove_DatabaseSchema *groove__ivm__runtime__IvmRuntime__schema(const groove_IvmRuntime *rt);
groove_SharedIndexIds groove__ivm__runtime__IvmRuntime__index_ids(const groove_IvmRuntime *rt);
const groove_TableSchema *groove__ivm__runtime__IvmRuntime__table(const groove_IvmRuntime *rt, const char *table, u32 len);
const groove_RecordDescriptor *groove__ivm__runtime__IvmRuntime__table_storage_descriptor(const groove_IvmRuntime *rt, const char *table, u32 len);
const groove_RecordDescriptor *groove__ivm__runtime__IvmRuntime__record_descriptor(const groove_IvmRuntime *rt, const char *table, u32 len, u32 variant_tag);
const groove_IndexSchema *groove__ivm__runtime__IvmRuntime__index(const groove_IvmRuntime *rt, const char *table, u32 len, const char *index, u32 ilen);
u32 groove__ivm__runtime__IvmRuntime__index_storage_id(const groove_IvmRuntime *rt, const char *table, u32 len, const groove_IndexSchema *index);
const groove_DirectRecordStoreSchema *groove__ivm__runtime__IvmRuntime__direct_record_store(const groove_IvmRuntime *rt, const char *name, u32 len);
jz_err groove__ivm__runtime__IvmRuntime__drive_pending_incremental(groove_IvmRuntime *rt);
jz_err groove__ivm__runtime__IvmRuntime__tick(groove_IvmRuntime *rt, void *table_deltas, u32 n, groove_DynStorage storage, groove_TickMetrics **out);
jz_err groove__ivm__runtime__IvmRuntime__tick_resident_staged(groove_IvmRuntime *rt, void *table_deltas /* Vec<TableDelta> moved */, groove_OwnedStorage storage, bool defer_notifications,
                                                           bool has_observer, groove_DynChunkInstallObserver observer, groove_PublicationInstallFailures failures, groove_ResidentTick **out);
groove_TickMetrics *groove__ivm__runtime__IvmRuntime__assign_resident_publication(groove_IvmRuntime *rt, groove_ResidentTick *tick, groove_PublicationId publication);
void groove__ivm__runtime__IvmRuntime__discard_deferred_notifications(groove_IvmRuntime *rt, groove_PublicationId publication);
void groove__ivm__runtime__IvmRuntime__settle_deferred_notifications(groove_IvmRuntime *rt, groove_PublicationId publication);
void groove__ivm__runtime__IvmRuntime__set_chunk_provider(groove_IvmRuntime *rt, groove_DynChunkProvider provider);
void groove__ivm__runtime__IvmRuntime__set_owned_chunk_provider(groove_IvmRuntime *rt, groove_OwnedChunkProvider provider);
groove_OwnedChunkProvider groove__ivm__runtime__IvmRuntime__chunk_provider(const groove_IvmRuntime *rt);
/* poll_* return true when Ready */
bool groove__ivm__runtime__IvmRuntime__poll_storage_extraction(groove_IvmRuntime *rt, const groove_OwnedStorage *storage, const jz_waker *cx, jz_err *result);
void groove__ivm__runtime__IvmRuntime__set_plain_output_root_positions_enabled(groove_IvmRuntime *rt, bool enabled);
bool groove__ivm__runtime__IvmRuntime__plain_output_root_positions_enabled(const groove_IvmRuntime *rt);
bool groove__ivm__runtime__IvmRuntime__has_pending_storage_writes(const groove_IvmRuntime *rt);
void groove__ivm__runtime__IvmRuntime__set_auto_direct_family_enabled(groove_IvmRuntime *rt, bool enabled);
void groove__ivm__runtime__IvmRuntime__set_tick_runtime_stats_enabled(groove_IvmRuntime *rt, bool enabled);
groove_RuntimeStats *groove__ivm__runtime__IvmRuntime__stats(const groove_IvmRuntime *rt);
groove_ExecutionLayoutStats groove__ivm__runtime__IvmRuntime__execution_layout_stats(const groove_IvmRuntime *rt);
bool groove__ivm__runtime__IvmRuntime__prepared_binding_source_is_bound(const groove_IvmRuntime *rt, const char *shape, u32 len);
jz_err groove__ivm__runtime__IvmRuntime__infer_builder_output(const groove_IvmRuntime *rt, const groove_GraphBuilder *graph, groove_RecordDescriptor *out);
groove_InputSourceId groove__ivm__runtime__IvmRuntime__allocate_input_source(groove_IvmRuntime *rt, groove_RecordDescriptor descriptor);
jz_err groove__ivm__runtime__IvmRuntime__replace_input_sources(groove_IvmRuntime *rt, groove_InputSourceReplacement *items, u32 n, bool detach_cold, const groove_OwnedStorage *storage, groove_TickMetrics **out);
jz_err groove__ivm__runtime__IvmRuntime__apply_input_source_deltas(groove_IvmRuntime *rt, groove_InputSourceDelta *items, u32 n, bool detach_cold, const groove_OwnedStorage *storage, groove_TickMetrics **out);
jz_err groove__ivm__runtime__IvmRuntime__retire_input_sources(groove_IvmRuntime *rt, const groove_InputSourceId *ids, u32 n, const groove_OwnedStorage *storage, groove_TickMetrics **out);
bool groove__ivm__runtime__IvmRuntime__has_resident_continuation(const groove_IvmRuntime *rt);
bool groove__ivm__runtime__IvmRuntime__has_pending_incremental(const groove_IvmRuntime *rt);
bool groove__ivm__runtime__IvmRuntime__subscription_has_pending_progress(const groove_IvmRuntime *rt, groove_SubscriptionId id);
bool groove__ivm__runtime__IvmRuntime__subscription_has_pending_evaluation(const groove_IvmRuntime *rt, groove_SubscriptionId id);
bool groove__ivm__runtime__IvmRuntime__poll_pending_incremental(groove_IvmRuntime *rt, const jz_waker *cx, jz_err *result);
bool groove__ivm__runtime__IvmRuntime__poll_resident_incremental(groove_IvmRuntime *rt, const jz_waker *cx, jz_err *result);
void groove__ivm__runtime__IvmRuntime__retain_owner_progress_waker(groove_IvmRuntime *rt, const jz_waker *waker);
jz_err groove__ivm__runtime__IvmRuntime__subscribe_one_sink_with_waker(groove_IvmRuntime *rt, groove_GraphBuilder graph, const groove_OwnedStorage *storage, const jz_waker *waker, groove_Subscription **out);
jz_err groove__ivm__runtime__IvmRuntime__subscribe_with_lifetime(groove_IvmRuntime *rt, const jz_str *names, groove_GraphBuilder *graphs, u32 n, const groove_OwnedStorage *storage,
                                                               u8 lifetime, groove_RootIndirectValues root_indirect_values, const jz_waker *waker, groove_MultisinkSubscription **out);
jz_err groove__ivm__runtime__IvmRuntime__prepare_live_attach(groove_IvmRuntime *rt, groove_PreparedShapeId shape, const groove_Value *values, u32 n, const groove_OwnedStorage *storage, groove_LiveAttach **out);
jz_err groove__ivm__runtime__IvmRuntime__bind_shape_one_sink_with_waker(groove_IvmRuntime *rt, groove_PreparedShapeId shape, const groove_Value *values, u32 n, const groove_OwnedStorage *storage,
                                                                      const jz_waker *waker, groove_LiveAttach *live, groove_Subscription **out);
jz_err groove__ivm__runtime__IvmRuntime__bind_shape_one_sink_with_output_and_waker(groove_IvmRuntime *rt, groove_PreparedShapeId shape, const groove_Value *values, u32 n,
                                                                                 groove_RecordDescriptor public_output, const groove_OwnedStorage *storage,
                                                                                 const jz_waker *waker, groove_LiveAttach *live, groove_Subscription **out);
jz_err groove__ivm__runtime__IvmRuntime__prepare_one_sink(groove_IvmRuntime *rt, groove_GraphBuilder graph, jz_str binding_source_shape, groove_RecordDescriptor binding_descriptor,
                                                        jz_str *output_key_fields, u32 n, groove_DynStorage storage, groove_IvmPreparedShape **out);
jz_err groove__ivm__runtime__IvmRuntime__prepare_one_sink_with_routing(groove_IvmRuntime *rt, groove_GraphBuilder output_graph, groove_GraphBuilder routing_graph, jz_str binding_source_shape,
                                                                     groove_RecordDescriptor binding_descriptor, jz_str *routing_key_fields, u32 n, groove_DynStorage storage,
                                                                     groove_IvmPreparedShape **out);
jz_err groove__ivm__runtime__IvmRuntime__prepare(groove_IvmRuntime *rt, groove_RoutedMultisinkTerminal **terminals, u32 n, jz_str binding_source_shape,
                                               groove_RecordDescriptor binding_descriptor, groove_DynStorage storage, groove_IvmPreparedShape **out);
jz_err groove__ivm__runtime__IvmRuntime__prepare_shared(groove_IvmRuntime *rt, groove_RoutedMultisinkTerminal **terminals, u32 n, jz_str binding_source_shape,
                                                      groove_RecordDescriptor binding_descriptor, groove_DynStorage storage, groove_IvmPreparedShape **out);
void groove__ivm__runtime__IvmRuntime__release_shared_prepared_shape(groove_IvmRuntime *rt, groove_PreparedShapeId shape);
jz_err groove__ivm__runtime__IvmRuntime__bind_shape_with_lifetime(groove_IvmRuntime *rt, groove_PreparedShapeId shape, const groove_Value *values, u32 n, const groove_OwnedStorage *storage,
                                                                u8 lifetime, groove_RootIndirectValues root_indirect_values, const jz_waker *waker, groove_LiveAttach *live,
                                                                groove_MultisinkSubscription **out);
void groove__ivm__runtime__IvmRuntime__transfer_prepared_shape_to_bindings(groove_IvmRuntime *rt, groove_PreparedShapeId shape);
jz_err groove__ivm__runtime__IvmRuntime__query_snapshot_with_root_values(groove_IvmRuntime *rt, groove_GraphBuilder graph, groove_DynStorage storage, groove_RootIndirectValues root_indirect_values, groove_RecordDeltas **out);
jz_err groove__ivm__runtime__IvmRuntime__query_snapshots(groove_IvmRuntime *rt, const jz_str *names, groove_GraphBuilder *graphs, u32 n, groove_DynStorage storage, groove_MultisinkDeltas **out);
bool groove__ivm__runtime__IvmRuntime__unsubscribe(groove_IvmRuntime *rt, groove_SubscriptionId id);
jz_err groove__ivm__runtime__IvmRuntime__retire_prepared_shape(groove_IvmRuntime *rt, groove_PreparedShapeId shape);
jz_err groove__ivm__runtime__IvmRuntime__prune_dropped_subscriptions_with_storage(groove_IvmRuntime *rt, groove_DynStorage storage, u32 *out);
bool groove__ivm__runtime__IvmRuntime__declared_index_repair_is_idle(const groove_IvmRuntime *rt);
jz_err groove__ivm__runtime__IvmRuntime__rebuild_declared_indexes(groove_IvmRuntime *rt, groove_DynStorage storage);
jz_err groove__ivm__runtime__IvmRuntime__register_table(groove_IvmRuntime *rt, groove_TableSchema table);
jz_err groove__ivm__runtime__IvmRuntime__register_table_variant_with_columns(groove_IvmRuntime *rt, const char *table, u32 len, groove_ColumnSchemaVec added_columns, groove_TableVariant variant);
jz_err groove__ivm__runtime__IvmRuntime__evolve_table_variant_registries(groove_IvmRuntime *rt, const char *table, u32 len, const groove_ColumnSchema *columns, u32 n);
jz_err groove__ivm__runtime__IvmRuntime__register_table_index(groove_IvmRuntime *rt, const char *table, u32 len, groove_IndexSchema index, groove_DynStorage storage);
jz_err groove__ivm__runtime__IvmRuntime__define_variant_projection(groove_IvmRuntime *rt, const char *table, u32 len, const char *target, u32 tlen, groove_RecordDescriptor output);
jz_err groove__ivm__runtime__IvmRuntime__register_variant_projection_case(groove_IvmRuntime *rt, const char *table, u32 len, const char *target, u32 tlen, u32 variant_tag,
                                                                        const groove_ProjectField *fields, u32 n);
jz_err groove__ivm__runtime__IvmRuntime__register_variant_projection_case_omitting_unrepresentable_enums(groove_IvmRuntime *rt, const char *table, u32 len, const char *target, u32 tlen,
                                                                                                       u32 variant_tag, const groove_ProjectField *fields, u32 n);
jz_err groove__ivm__runtime__IvmRuntime__refresh_variant_projection_case_for_registry_evolution(groove_IvmRuntime *rt, const char *table, u32 len, const char *target, u32 tlen,
                                                                                              u32 variant_tag, const groove_ProjectField *fields, u32 n);
jz_err groove__ivm__runtime__IvmRuntime__register_variant_projection_enum_case(groove_IvmRuntime *rt, const char *table, u32 len, const char *target, u32 tlen, u32 variant_tag,
                                                                             const char *enum_field, u32 flen, const groove_EnumSchema *enum_schema, const char *case_, u32 clen,
                                                                             const groove_ProjectField *fields, u32 n);
jz_err groove__ivm__runtime__IvmRuntime__register_variant_projection_ignore_case(groove_IvmRuntime *rt, const char *table, u32 len, const char *target, u32 tlen, u32 variant_tag);
jz_err groove__ivm__runtime__IvmRuntime__project_variant_record(const groove_IvmRuntime *rt, const char *table, u32 len, const char *target, u32 tlen, const groove_VariantRecord *record,
                                                              bool *some, groove_OwnedRecord *out);
/* evaluation_session::EvaluationInputs */
groove_EvaluationInputs *groove__ivm__runtime__evaluation_session__EvaluationInputs__default(void);
void groove__ivm__runtime__evaluation_session__EvaluationInputs__drop(groove_EvaluationInputs *in);
groove_ChunkRequestVec groove__ivm__runtime__evaluation_session__EvaluationInputs__take_missing_chunks(groove_EvaluationInputs *in);
void groove__ivm__runtime__evaluation_session__EvaluationInputs__install_chunk_from_provider(groove_EvaluationInputs *in, groove_ChunkRequest request, groove_ChunkLease *bytes);

/* TableDelta / RecordDelta (ivm runtime) */
typedef struct groove_RecordDelta { jz_bytes record; /* bytes::Bytes */ i64 weight; } groove_RecordDelta;
typedef struct groove_TableDelta { jz_str table; u32 variant_tag; groove_RecordDescriptor descriptor; JZ_VEC(groove_RecordDelta) deltas; } groove_TableDelta;
typedef JZ_VEC(groove_TableDelta) groove_TableDeltaVec;
void groove__ivm__runtime__TableDelta__drop(void *d);

/* ===================== ivm::planner / queries (not this unit) ===================== */
typedef struct groove_Query groove_Query;
typedef struct groove_QueryParameter { jz_str name; groove_ValueType value_type; } groove_QueryParameter;
typedef JZ_VEC(groove_QueryParameter) groove_QueryParameterVec;
typedef struct groove_PlannedQuery { groove_GraphBuilder *graph; } groove_PlannedQuery;
typedef struct groove_PublicOutputField { jz_str name; groove_ValueType value_type; } groove_PublicOutputField;
typedef struct groove_PlannedPreparedShape {
    groove_PlannedQuery planned;
    jz_str shape;
    groove_RecordDescriptor binding_descriptor;
    groove_StrVec output_key_fields;
    groove_QueryParameterVec parameters;
    JZ_VEC(groove_PublicOutputField) public_output;
} groove_PlannedPreparedShape;
jz_err groove__ivm__planner__plan_query(const groove_Query *query, const groove_DatabaseSchema *schema, groove_PlannedQuery *out);
jz_err groove__ivm__planner__plan_prepared_shape(const groove_Query *query, const groove_DatabaseSchema *schema, groove_PlannedPreparedShape *out);
void groove__ivm__planner__QueryParameter__Clone__clone(void *dst, const void *src);

/* blake3::derive_key(context, material) */
void jz_blake3_derive_key(const char *context, const u8 *material, u32 n, u8 out[32]);

#endif
