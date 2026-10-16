/* groove::large_values (crates/groove/src/large_values.rs) */
#ifndef GROOVE_LARGE_VALUES_H
#define GROOVE_LARGE_VALUES_H

#include "jz_rt.h"
#include "records/values.h"

/* large_values::Error */
enum {
    GROOVE_LV_E_BASE = 0x1100,
    GROOVE_LV_E_InvalidUtf8,
    GROOVE_LV_E_InvalidJson,
    GROOVE_LV_E_UnsupportedFormat,
    GROOVE_LV_E_ObjectHashMismatch,
    GROOVE_LV_E_MalformedNode,
    GROOVE_LV_E_MetricOverflow,
    GROOVE_LV_E_DescriptorMismatch,
    GROOVE_LV_E_InvalidTree,
    GROOVE_LV_E_TraversalWorkLimitExceeded,
    GROOVE_LV_E_PhysicalTraversalNodeLimitExceeded,
    GROOVE_LV_E_MalformedScalar,
    GROOVE_LV_E_RequiresEvaluation,
    GROOVE_LV_E_END
};
jz_err groove__large_values__Error__new(u32 code);
jz_err groove__large_values__Error__unsupported_format(u8 version);
static inline bool groove__large_values__is_error(u32 code) { return code > GROOVE_LV_E_BASE && code < GROOVE_LV_E_END; }

#define GROOVE_LV_FORMAT_V1 1
#define GROOVE_LV_FORMAT_VERSION GROOVE_LV_FORMAT_V1
#define GROOVE_LV_INLINE_VALUE_MAX_BYTES (64u * 1024u)
#define GROOVE_LV_LEAF_MIN_BYTES (16u * 1024u)
#define GROOVE_LV_LEAF_TARGET_BYTES (64u * 1024u)
#define GROOVE_LV_LEAF_MAX_BYTES (256u * 1024u)
#define GROOVE_LV_LEAF_SHORT_MASK ((u64)0x1ffff)
#define GROOVE_LV_LEAF_LONG_MASK ((u64)0x7fff)
#define GROOVE_LV_MAX_ENCODED_NODE_BYTES (GROOVE_LV_LEAF_MAX_BYTES + 16u * 1024u)
#define GROOVE_LV_BRANCH_MIN_CHILDREN 4u
#define GROOVE_LV_BRANCH_TARGET_CHILDREN 16u
#define GROOVE_LV_BRANCH_MAX_CHILDREN 64u
#define GROOVE_LV_BRANCH_SHORT_MASK ((u64)0x1f)
#define GROOVE_LV_BRANCH_LONG_MASK ((u64)0x7)
#define GROOVE_LV_MAX_TREE_DEPTH 32u
#define GROOVE_LV_MAX_LOGICAL_TRAVERSAL_STEPS (128u * 1024u)
#define GROOVE_LV_MAX_PHYSICAL_TRAVERSAL_NODES GROOVE_LV_MAX_LOGICAL_TRAVERSAL_STEPS
#define GROOVE_LV_MAX_JSON_NESTING_DEPTH 128u
#define GROOVE_LV_MAX_EDIT_COUNT 64u
#define GROOVE_LV_MAX_EDIT_TAIL_BYTES (256u * 1024u)
#define GROOVE_LV_LOCATOR_BYTES 32u

typedef struct groove_ContentHash { u8 b[32]; } groove_ContentHash;
typedef struct groove_Locator { u8 b[32]; } groove_Locator;
typedef struct groove_NodeRef { groove_ContentHash object_hash; groove_Locator locator; } groove_NodeRef;
typedef JZ_VEC(groove_NodeRef) groove_NodeRefVec;
typedef struct groove_NodeMetrics { u64 byte_length; bool has_utf16; u64 utf16_length; } groove_NodeMetrics;
typedef struct groove_BranchChild { groove_NodeRef node_ref; groove_NodeMetrics metrics; groove_ContentHash logical_hash; } groove_BranchChild;
typedef JZ_VEC(groove_BranchChild) groove_BranchChildVec;

enum { GROOVE_CHUNK_NODE_LEAF = 0, GROOVE_CHUNK_NODE_BRANCH = 1 };
typedef struct groove_ChunkNode {
    u8 tag;
    u8 format;
    u8 kind; /* groove_LargeValueKind */
    jz_bytes bytes;                  /* Leaf */
    groove_BranchChildVec children;  /* Branch */
} groove_ChunkNode;

typedef struct groove_ReplaceEdit {
    u64 offset;
    u64 delete_length;
    jz_bytes insert_bytes;
    u64 utf16_offset;
    u64 delete_utf16_length;
    u64 insert_utf16_length;
} groove_ReplaceEdit;
typedef JZ_VEC(groove_ReplaceEdit) groove_ReplaceEditVec;

struct groove_LargeValueRef {
    u8 kind; /* groove_LargeValueKind */
    u8 format_version;
    groove_ContentHash logical_hash;
    groove_NodeRef root;
    u64 byte_length;
    bool has_utf16_length;
    u64 utf16_length;
    groove_ReplaceEditVec edit_tail;
};

typedef struct groove_StagedChunk { groove_NodeRef node_ref; jz_bytes encoded; } groove_StagedChunk;
typedef JZ_VEC(groove_StagedChunk) groove_StagedChunkVec;
typedef struct groove_PreparedLargeValue { groove_LargeValueRef value_ref; groove_StagedChunkVec staged_chunks; } groove_PreparedLargeValue;


/* StagedLargeValueId([u8;16]) / StagedLargeValueAccounting / StagedLargeValue */
typedef struct groove_StagedLargeValueId { u8 b[16]; } groove_StagedLargeValueId;
typedef struct groove_StagedLargeValueAccounting { u64 encoded_bytes; u64 node_count; } groove_StagedLargeValueAccounting;
typedef struct groove_StagedLargeValue {
    groove_StagedLargeValueId id;
    groove_LargeValueRef value_ref;
    groove_StagedLargeValueAccounting accounting;
    u64 created_at_ms;
} groove_StagedLargeValue;
typedef struct groove_PendingLargeValueUpload {
    groove_StagedLargeValueId id;
    bool has_descriptor; groove_LargeValueRef descriptor; /* #[serde(default)] */
    bool has_receipt_id; groove_StagedLargeValueId receipt_id; /* #[serde(default)] */
    groove_StagedLargeValueAccounting accounting;
    u64 created_at_ms;
    groove_NodeRefVec chunks;
} groove_PendingLargeValueUpload;
/* LargeValueUploadProgress { Missing(Vec<NodeRef>) | Staged(StagedLargeValue) } */
enum { GROOVE_UPLOAD_MISSING = 0, GROOVE_UPLOAD_STAGED = 1 };
typedef struct groove_LargeValueUploadProgress { u8 tag; groove_NodeRefVec missing; groove_StagedLargeValue staged; } groove_LargeValueUploadProgress;

typedef struct groove_StreamingPrepareStats { u64 input_bytes; u32 peak_leaf_buffer_bytes; u32 peak_frontier_nodes; u64 staged_chunk_count; } groove_StreamingPrepareStats;
typedef struct groove_StreamingExecutionStats { u64 logical_bytes_consumed; u64 windows_consumed; u64 cooperative_yields; } groove_StreamingExecutionStats;

typedef struct groove_LargeValueCursor { groove_LargeValueRef value; u64 offset; u64 window_bytes; } groove_LargeValueCursor;
typedef struct groove_StreamingChecksum {
    groove_LargeValueCursor cursor;
    jz_blake3_hasher *hasher;
    u32 max_bytes_per_turn;
    u32 bytes_this_turn;
    groove_StreamingExecutionStats stats;
} groove_StreamingChecksum;

#include "large_values/json_syntax.h"
typedef struct groove_LogicalValueValidator {
    u8 kind;
    jz_bytes utf8_tail;
    u64 utf16_length;
    bool has_json; groove_StreamingJsonValidator json;
} groove_LogicalValueValidator;

/* BuiltNode (private; here because StreamingLevel embeds it) */
typedef struct groove_BuiltNode { groove_NodeRef node_ref; groove_NodeMetrics metrics; groove_ContentHash structural_hash; } groove_BuiltNode;
typedef struct groove_StreamingLevel { JZ_VEC(groove_BuiltNode) pending; u64 hash; u32 total; } groove_StreamingLevel;
/* L: FnMut(ContentHash) -> Locator ; S: FnMut(StagedChunk) -> Result<(), Error> (takes ownership) */
typedef groove_Locator (*groove_LocatorForFn)(void *ctx, groove_ContentHash hash);
typedef jz_err (*groove_StageFn)(void *ctx, groove_StagedChunk chunk);
typedef struct groove_StreamingTreeBuilder {
    u8 kind;
    groove_LocatorForFn locator_for; void *locator_ctx;
    groove_StageFn stage; void *stage_ctx;
    jz_bytes leaf;
    u32 leaf_scan;
    u64 leaf_hash;
    JZ_VEC(groove_StreamingLevel) levels;
    groove_StreamingPrepareStats stats;
} groove_StreamingTreeBuilder;
typedef struct groove_PushStreamingPreparation {
    groove_StreamingTreeBuilder builder;
    bool has_json; groove_StreamingJsonValidator json;
} groove_PushStreamingPreparation;

typedef struct groove_ConsolidationContinuation {
    groove_LargeValueRef source;
    bool has_current; groove_LargeValueRef current;
    u32 next_edit;
    groove_StagedChunkVec staged_chunks;
} groove_ConsolidationContinuation;

/* TailOutcome { Updated(LargeValueRef) | ConsolidationRequired(LargeValueRef) } */
enum { GROOVE_TAIL_UPDATED = 0, GROOVE_TAIL_CONSOLIDATION_REQUIRED = 1 };
typedef struct groove_TailOutcome { u8 tag; groove_LargeValueRef value; } groove_TailOutcome;

typedef struct groove_PhysicalTraversalNodeBudget { u32 remaining; } groove_PhysicalTraversalNodeBudget;
void groove__large_values__record_finalize_validation_bytes(u32 bytes);

/* JsonPointerPrefix { Found(Option<serde_json::Value>) | NeedMore | RequiresFullDocument } */
enum { GROOVE_JSON_POINTER_FOUND = 0, GROOVE_JSON_POINTER_NEED_MORE = 1, GROOVE_JSON_POINTER_REQUIRES_FULL_DOCUMENT = 2 };
typedef struct groove_JsonPointerPrefix { u8 tag; jz_json *found; /* NULL = None */ } groove_JsonPointerPrefix;

/* StoredScalar { Primitive(Vec<u8>) | Chunked(LargeValueRef) } */
enum { GROOVE_STORED_PRIMITIVE = 0, GROOVE_STORED_CHUNKED = 1 };
typedef struct groove_StoredScalar { u8 tag; jz_bytes primitive; groove_LargeValueRef chunked; } groove_StoredScalar;
void groove__large_values__StoredScalar__drop(groove_StoredScalar *s);

/* LargeValueRef derives */
void groove__large_values__LargeValueRef__drop(groove_LargeValueRef *r);
void groove__large_values__LargeValueRef__Clone__clone(groove_LargeValueRef *dst, const groove_LargeValueRef *src);
bool groove__large_values__LargeValueRef__PartialEq__eq(const groove_LargeValueRef *a, const groove_LargeValueRef *b);
void groove__large_values__LargeValueRef__Debug__fmt(jz_str *out, const groove_LargeValueRef *r);
void groove__large_values__LargeValueRef__Serialize__serialize(jz_pc_w *w, const groove_LargeValueRef *r);
jz_err groove__large_values__LargeValueRef__Deserialize__deserialize(jz_pc_r *r, groove_LargeValueRef *out);
void groove__large_values__LargeValueRef__Serialize__serialize_json(jz_json_w *w, const groove_LargeValueRef *r);
jz_err groove__large_values__LargeValueRef__Deserialize__deserialize_json(const jz_json *j, groove_LargeValueRef *out);
void groove__large_values__LargeValueKind__Debug__fmt(jz_str *out, u8 kind);
void groove__large_values__LargeValueKind__Serialize__serialize_json(jz_json_w *w, u8 kind);
jz_err groove__large_values__LargeValueKind__Deserialize__deserialize_json(const jz_json *j, u8 *out);
void groove__large_values__ReplaceEdit__drop(groove_ReplaceEdit *e);
void groove__large_values__ReplaceEdit__Clone__clone(groove_ReplaceEdit *dst, const groove_ReplaceEdit *src);
bool groove__large_values__ReplaceEdit__PartialEq__eq(const groove_ReplaceEdit *a, const groove_ReplaceEdit *b);
void groove__large_values__ChunkNode__drop(groove_ChunkNode *n);

/* stored scalar codecs used by records::values */
jz_err groove__large_values__encode_primitive_stored_scalar_into(groove_LargeValueKind kind, const u8 *p, u32 n, jz_bytes *out);
jz_err groove__large_values__encode_stored_scalar(groove_LargeValueKind kind, const groove_StoredScalar *s, jz_bytes *out);
jz_err groove__large_values__decode_stored_scalar(groove_LargeValueKind kind, const u8 *p, u32 n, groove_StoredScalar *out);
jz_err groove__large_values__inline_scalar_bytes(groove_LargeValueKind kind, const u8 *p, u32 n, jz_slice *out);
groove_ValueType groove__large_values__physical_storage_value_type(groove_LargeValueKind kind);

/* ---- remaining public / pub(crate) API (large_values.c) ---- */
typedef struct groove_EvaluationInputs groove_EvaluationInputs;   /* groove::ivm::runtime::evaluation_session */
typedef struct groove_LocalChunkReader groove_LocalChunkReader;   /* chunks.h */

void groove__large_values__Error__Debug__fmt(jz_str *out, const jz_err *e);
void groove__large_values__ContentHash__Debug__fmt(jz_str *out, const groove_ContentHash *h);
void groove__large_values__Locator__Debug__fmt(jz_str *out, const groove_Locator *l);
void groove__large_values__NodeRef__Debug__fmt(jz_str *out, const groove_NodeRef *r);
void groove__large_values__ContentHash__Serialize__serialize(jz_pc_w *w, const groove_ContentHash *h);
jz_err groove__large_values__ContentHash__Deserialize__deserialize(jz_pc_r *r, groove_ContentHash *out);
void groove__large_values__ContentHash__Serialize__serialize_json(jz_json_w *w, const groove_ContentHash *h);
jz_err groove__large_values__ContentHash__Deserialize__deserialize_json(const jz_json *j, groove_ContentHash *out);
void groove__large_values__Locator__Serialize__serialize(jz_pc_w *w, const groove_Locator *l);
jz_err groove__large_values__Locator__Deserialize__deserialize(jz_pc_r *r, groove_Locator *out);
void groove__large_values__Locator__Serialize__serialize_json(jz_json_w *w, const groove_Locator *l);
jz_err groove__large_values__Locator__Deserialize__deserialize_json(const jz_json *j, groove_Locator *out);
void groove__large_values__NodeRef__Serialize__serialize(jz_pc_w *w, const groove_NodeRef *r);
jz_err groove__large_values__NodeRef__Deserialize__deserialize(jz_pc_r *r, groove_NodeRef *out);
void groove__large_values__NodeRef__Serialize__serialize_json(jz_json_w *w, const groove_NodeRef *r);
jz_err groove__large_values__NodeRef__Deserialize__deserialize_json(const jz_json *j, groove_NodeRef *out);
groove_Locator groove__large_values__Locator__random(void);
const u8 *groove__large_values__Locator__as_bytes(const groove_Locator *l);
jz_err groove__large_values__LargeValueCursor__new(groove_LargeValueRef value, u32 window_bytes, groove_LargeValueCursor *out);
u64 groove__large_values__LargeValueCursor__offset(const groove_LargeValueCursor *c);
u64 groove__large_values__LargeValueCursor__remaining_bytes(const groove_LargeValueCursor *c);
const groove_LargeValueRef *groove__large_values__LargeValueCursor__value(const groove_LargeValueCursor *c);
bool groove__large_values__LargeValueCursor__next_range(const groove_LargeValueCursor *c, u64 *start, u64 *end);
void groove__large_values__LargeValueCursor__advance_to(groove_LargeValueCursor *c, u64 offset);
void groove__large_values__LargeValueCursor__drop(groove_LargeValueCursor *c);
jz_err groove__large_values__StreamingChecksum__new(groove_LargeValueRef value, u32 window_bytes, u32 max_bytes_per_turn, groove_StreamingChecksum *out);
const groove_LargeValueCursor *groove__large_values__StreamingChecksum__cursor(const groove_StreamingChecksum *s);
groove_StreamingExecutionStats groove__large_values__StreamingChecksum__stats(const groove_StreamingChecksum *s);
jz_err groove__large_values__StreamingChecksum__consume_window(groove_StreamingChecksum *s, const u8 *bytes, u32 n, bool *yield);
jz_err groove__large_values__StreamingChecksum__record_yield(groove_StreamingChecksum *s);
void groove__large_values__StreamingChecksum__drop(groove_StreamingChecksum *s);
jz_err groove__large_values__StreamingChecksum__finish(groove_StreamingChecksum *s, groove_ContentHash *hash, groove_StreamingExecutionStats *stats);
jz_err groove__large_values__validate_staged_chunk_batch(u8 kind, const groove_StagedChunk *chunks, u32 n);
jz_err groove__large_values__LogicalValueValidator__new(const groove_LargeValueRef *value, groove_LogicalValueValidator *out);
void groove__large_values__LogicalValueValidator__drop(groove_LogicalValueValidator *v);
jz_err groove__large_values__LogicalValueValidator__push(groove_LogicalValueValidator *v, const u8 *bytes, u32 n);
jz_err groove__large_values__LogicalValueValidator__finish(groove_LogicalValueValidator *v, const groove_LargeValueRef *value);
void groove__large_values__PushStreamingPreparation__new(groove_PushStreamingPreparation *p, u8 kind, groove_StageFn stage, void *stage_ctx);
jz_err groove__large_values__PushStreamingPreparation__push(groove_PushStreamingPreparation *p, const u8 *bytes, u32 n);
jz_err groove__large_values__PushStreamingPreparation__finish(groove_PushStreamingPreparation *p, groove_LargeValueRef *out, groove_StreamingPrepareStats *stats);
void groove__large_values__PushStreamingPreparation__drop(groove_PushStreamingPreparation *p);
void groove__large_values__PreparedLargeValue__drop(groove_PreparedLargeValue *p);
jz_err groove__large_values__ConsolidationContinuation__new(groove_LargeValueRef source, groove_ConsolidationContinuation *out);
void groove__large_values__ConsolidationContinuation__drop(groove_ConsolidationContinuation *c);
jz_err groove__large_values__ConsolidationContinuation__step(groove_ConsolidationContinuation *c, groove_EvaluationInputs *inputs, bool *has_out, groove_PreparedLargeValue *out);
jz_err groove__large_values__append_tail(const groove_LargeValueRef *value, jz_bytes insert_bytes, groove_TailOutcome *out);
jz_err groove__large_values__replace_tail_attempt(const groove_LargeValueRef *value, u64 offset, u64 delete_length, jz_bytes insert_bytes, groove_EvaluationInputs *inputs, groove_TailOutcome *out);
jz_err groove__large_values__consolidate_appends_attempt(const groove_LargeValueRef *value, groove_EvaluationInputs *inputs, groove_LocatorForFn fresh_locator, void *fresh_ctx, groove_PreparedLargeValue *out);
jz_err groove__large_values__consolidate_single_edit_attempt(const groove_LargeValueRef *value, groove_EvaluationInputs *inputs, groove_LocatorForFn fresh_locator, void *fresh_ctx, groove_PreparedLargeValue *out);
groove_PhysicalTraversalNodeBudget groove__large_values__PhysicalTraversalNodeBudget__new(void);
jz_err groove__large_values__PhysicalTraversalNodeBudget__consume(groove_PhysicalTraversalNodeBudget *b);
jz_err groove__large_values__PhysicalTraversalNodeBudget__consume_many(groove_PhysicalTraversalNodeBudget *b, u32 count);
jz_err groove__large_values__missing_upload_frontier(const groove_LargeValueRef *value, const groove_LocalChunkReader *reader, u32 limit, groove_NodeRefVec *out);
jz_err groove__large_values__validate_finalized_upload(const groove_LargeValueRef *value, const groove_LocalChunkReader *reader, const jz_btree *uploaded_chunks, bool descriptor_was_bound_before_completion);
jz_err groove__large_values__validate_derived_upload(const groove_LargeValueRef *value, const groove_LargeValueRef *base, const groove_LocalChunkReader *reader, const jz_btree *staged_chunks, bool *ok);
jz_err groove__large_values__prepare(u8 kind, const u8 *p, u32 n, groove_PreparedLargeValue *out);
groove_RecordDescriptor groove__large_values__node_ref_descriptor(void);
groove_Value groove__large_values__node_ref_value(const groove_NodeRef *node_ref);
jz_err groove__large_values__node_ref_from_value(const groove_Value *value, groove_NodeRef *out);
jz_err groove__large_values__encode_node_ref(const groove_NodeRef *node_ref, jz_bytes *out);
jz_err groove__large_values__decode_node_ref(const u8 *encoded, u32 n, groove_NodeRef *out);
jz_err groove__large_values__encode_large_value_ref(const groove_LargeValueRef *value, jz_bytes *out);
jz_err groove__large_values__decode_large_value_ref(const u8 *encoded, u32 n, groove_LargeValueRef *out);
jz_err groove__large_values__validate_edit_tail_attempt(const groove_LargeValueRef *value, groove_EvaluationInputs *inputs);
jz_err groove__large_values__decode_node(u8 kind, groove_ContentHash expected_hash, const u8 *p, u32 n, groove_ChunkNode *out);
jz_err groove__large_values__decode_node_untyped_authenticated(groove_ContentHash expected_hash, const u8 *p, u32 n, groove_ChunkNode *out);
jz_err groove__large_values__decode_authenticated_node(groove_ContentHash expected_hash, const u8 *p, u32 n, groove_ChunkNode *out);
jz_err groove__large_values__encode_node(const groove_ChunkNode *node, jz_bytes *out);
groove_ContentHash groove__large_values__object_hash(const u8 *p, u32 n);
jz_err groove__large_values__materialize_attempt(const groove_LargeValueRef *value, groove_EvaluationInputs *inputs, jz_bytes *out);
jz_err groove__large_values__materialize_record_borrowed_attempt(groove_RecordDescriptor descriptor, const u8 *raw, u32 n, const u32 *fields, u32 nfields, groove_EvaluationInputs *inputs, bool *owned, jz_bytes *out);
jz_err groove__large_values__materialize_record_attempt(groove_RecordDescriptor descriptor, const u8 *raw, u32 n, groove_EvaluationInputs *inputs, jz_bytes *out);
jz_err groove__large_values__materialize_record_fields_attempt(groove_RecordDescriptor descriptor, const u8 *raw, u32 n, const u32 *field_indices, u32 nfields, groove_EvaluationInputs *inputs, jz_bytes *out);
jz_err groove__large_values__byte_range_attempt(const groove_LargeValueRef *value, u64 start, u64 end, groove_EvaluationInputs *inputs, jz_bytes *out);
jz_err groove__large_values__utf16_range_attempt(const groove_LargeValueRef *value, u64 start, u64 end, groove_EvaluationInputs *inputs, jz_bytes *out);
jz_err groove__large_values__utf16_offset_to_byte_attempt(const groove_LargeValueRef *value, u64 offset, groove_EvaluationInputs *inputs, u64 *out);
jz_err groove__large_values__compare_inline_attempt(const groove_LargeValueRef *value, const u8 *inline_, u32 n, groove_EvaluationInputs *inputs, int *out);
jz_err groove__large_values__json_pointer_prefix(const u8 *source, u32 n, const char *pointer, u32 plen, groove_JsonPointerPrefix *out);

#endif
