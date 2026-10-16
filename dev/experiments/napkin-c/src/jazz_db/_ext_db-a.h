/* _ext_db-a.h: local mirror declarations of types and functions from other
 * units (groove, jazz_types, jazz_model, jazz_protocol, jazz_node and the
 * rest of jazz_db: db.rs, lifecycle.rs, reads.rs, transactions.rs,
 * mutation_errors.rs, row_availability.rs, first_load.rs, read_barrier.rs,
 * wire_transport.rs, ...) used by unit db-a (peer_connection.rs,
 * node_runtime.rs, mutations.rs).
 *
 * Layouts approximate the Rust fields; only what db-a touches is spelled
 * out, everything else is an opaque pointer.  Function names follow the
 * PORTING.md scheme so the final link resolves against the defining unit. */
#ifndef JAZZ_DB_EXT_DB_A_H
#define JAZZ_DB_EXT_DB_A_H
#include "jz_rt.h"

/* ======================= api error (jazz_node::node::api_error) =======================
 * Error { code: ErrorCode, message: String } maps to jz_err with
 * code = ErrorCode discriminant + 1 (0 is success). */
enum {
    JAZZ_EC_SCHEMA = 1,
    JAZZ_EC_QUERY,
    JAZZ_EC_WRITE_REJECTED,
    JAZZ_EC_TRANSACTION_CONFLICT,
    JAZZ_EC_STORAGE,
    JAZZ_EC_PROTOCOL,
    JAZZ_EC_BACKPRESSURE,
    JAZZ_EC_NOT_OBSERVED,
    JAZZ_EC_HISTORICAL_READ_REQUIRES_SERVER,
};
/* crate::node::Error variants that db-a matches on (node-error code space). */
enum {
    JAZZ_NODE_ERR_BASE = 0x1000,
    JAZZ_NODE_ERR_MISSING_OPEN_BATCH,
    JAZZ_NODE_ERR_MISSING_TRANSACTION,
    JAZZ_NODE_ERR_GROOVE_DATABASE_POISONED,
    JAZZ_NODE_ERR_INVALID_AUTHORITY_SOURCE_CLOSURE,
    JAZZ_NODE_ERR_LARGE_VALUE_UPLOAD_CHUNK_UNAVAILABLE,
    JAZZ_NODE_ERR_GROOVE_LARGE_VALUE,
    JAZZ_NODE_ERR_TX_TIME,
    JAZZ_NODE_ERR_QUERY_CAPABILITY,
    JAZZ_NODE_ERR_UNSUPPORTED_SYNC_MESSAGE,
    JAZZ_NODE_ERR_OTHER,
};
/* impl From<crate::node::Error> for Error */
jz_err jazz_node__node__api_error__Error__From__from(jz_err node_error);
/* impl From<groove::large_values::Error> for crate::node::Error */
jz_err jazz_node__node__Error__From_groove_large_values_Error__from(jz_err groove_error);
/* impl From<TransportError> for Error (transport_error() in peer_connection uses Display) */
void jazz_node__node__Error__display(jz_str *out, const jz_err *e);
void jazz_node__node__api_error__Error__display(jz_str *out, const jz_err *e);
#define JZ_TRY_NODE(expr) do { jz_err _ne = (expr); if (_ne.code) return jazz_node__node__api_error__Error__From__from(_ne); } while (0)
static inline jz_err jazz_db_err_clone(const jz_err *e) {
    jz_err out = {e->code, NULL};
    if (e->msg) { out.msg = (jz_str *)jz_alloc(sizeof(jz_str)); *out.msg = jz_str_clone(e->msg); }
    return out;
}
static inline bool jazz_db_err_msg_contains(const jz_err *e, const char *needle) {
    if (!e->msg) return false;
    u32 n = (u32)strlen(needle);
    for (u32 i = 0; i + n <= e->msg->len; ++i)
        if (memcmp(e->msg->ptr + i, needle, n) == 0) return true;
    return false;
}

/* ======================= jazz_types ======================= */
typedef u64 jazz_types_TxTime;
typedef u64 jazz_types_GlobalTime;
typedef jz_uuid jazz_types_NodeUuid;
typedef jz_uuid jazz_types_RowUuid;
typedef struct jazz_types_SchemaVersionId { u8 b[32]; } jazz_types_SchemaVersionId;
typedef struct jazz_types_GlobalPhysicalTableId { u8 b[32]; } jazz_types_GlobalPhysicalTableId;
typedef struct jazz_types_AuthorSubject { u32 tag; const void *interned; } jazz_types_AuthorSubject;
enum { JAZZ_AUTHOR_SYSTEM = 0, JAZZ_AUTHOR_SYSTEM_AT = 1, JAZZ_AUTHOR_AUTHENTICATED = 2 };
static inline bool jazz_types_AuthorSubject_eq(jazz_types_AuthorSubject a, jazz_types_AuthorSubject b) { return a.tag == b.tag && a.interned == b.interned; }
static inline bool jazz_types_AuthorSubject_is_system(jazz_types_AuthorSubject a) { return a.tag == JAZZ_AUTHOR_SYSTEM; }
int jazz_types__ids__AuthorSubject__cmp(const void *a, const void *b, void *ctx);
extern const jazz_types_AuthorSubject jazz_types__ids__AuthorSubject__SYSTEM;
jz_err jazz_types__time__TxTime__tick(jazz_types_TxTime previous, u64 now_ms, jazz_types_TxTime *out);
bool jazz_types__debug_env__covered_input_trace(void);
#define JAZZ_TYPES_HLC_MAX_PHYSICAL_MS ((u64)0xFFFFFFFFFFFFull)

/* web_time::Instant as monotonic ms */
typedef u64 jazz_db_Instant;
static inline jazz_db_Instant jazz_db_instant_now(void) { return jz_now_ms(); }

/* ======================= jazz_model ======================= */
typedef struct jazz_model_TxId { jazz_types_TxTime time; jz_uuid node; } jazz_model_TxId;
int jazz_model__tx__TxId__cmp(const void *a, const void *b, void *ctx);
u64 jazz_model__tx__TxId__hash(const void *a);
bool jazz_model__tx__TxId__eq(const void *a, const void *b);
void jazz_model__tx__TxId__debug(jz_str *out, const jazz_model_TxId *t);
void jazz_model__tx__TxId__new(jazz_model_TxId *out, jazz_types_TxTime time, jazz_types_NodeUuid node);
static inline bool jazz_model_TxId_eq(jazz_model_TxId a, jazz_model_TxId b) { return a.time == b.time && jz_uuid_eq(&a.node, &b.node); }
typedef enum { JAZZ_TX_MERGEABLE = 0, JAZZ_TX_EXCLUSIVE = 1 } jazz_model_TxKind;
typedef enum { JAZZ_DUR_NONE = 0, JAZZ_DUR_LOCAL = 1, JAZZ_DUR_GLOBAL = 2 } jazz_model_DurabilityTier;
void jazz_model__tx__DurabilityTier__debug(jz_str *out, jazz_model_DurabilityTier t);
typedef struct jazz_model_RejectionReason { u32 tag; jazz_model_TxId root; jz_str detail; } jazz_model_RejectionReason;
void jazz_model__tx__RejectionReason__clone(void *dst, const void *src);
void jazz_model__tx__RejectionReason__drop(void *p);
typedef enum { JAZZ_FATE_PENDING = 0, JAZZ_FATE_ACCEPTED = 1, JAZZ_FATE_REJECTED = 2 } jazz_model_FateTag;
typedef struct jazz_model_Fate { u32 tag; jazz_model_RejectionReason rejected; } jazz_model_Fate;
void jazz_model__tx__Fate__drop(void *p);
typedef enum { JAZZ_DELETION_DELETED = 0, JAZZ_DELETION_RESTORED = 1 } jazz_model_DeletionEvent;
typedef struct jazz_model_OpenTransactionId { u64 v; } jazz_model_OpenTransactionId;
int jazz_model__transaction__OpenTransactionId__cmp(const void *a, const void *b, void *ctx);
static inline bool jazz_model_OpenTransactionId_eq(jazz_model_OpenTransactionId a, jazz_model_OpenTransactionId b) { return a.v == b.v; }

typedef struct jazz_model_ShapeId { jz_uuid u; } jazz_model_ShapeId;
typedef struct jazz_model_BindingId { jz_uuid u; } jazz_model_BindingId;
typedef struct jazz_model_ValidatedQuery jazz_model_ValidatedQuery; /* Rc-shared */
typedef struct jazz_model_Binding jazz_model_Binding;               /* Rc-shared */
typedef struct jazz_model_Query jazz_model_Query;
/* query().table / query().aggregate / query().flat_join */
typedef struct jazz_model_QueryAst { jz_str table; void *aggregate; void *flat_join; } jazz_model_QueryAst;
jazz_model_ShapeId jazz_model__query__ValidatedQuery__shape_id(const jazz_model_ValidatedQuery *q);
const jazz_model_QueryAst *jazz_model__query__ValidatedQuery__query(const jazz_model_ValidatedQuery *q);
jazz_types_SchemaVersionId jazz_model__query__ValidatedQuery__schema_version(const jazz_model_ValidatedQuery *q);
jazz_model_ValidatedQuery *jazz_model__query__ValidatedQuery__clone(const jazz_model_ValidatedQuery *q);
void jazz_model__query__ValidatedQuery__drop(jazz_model_ValidatedQuery *q);
bool jazz_model__query__ValidatedQuery__eq(const jazz_model_ValidatedQuery *a, const jazz_model_ValidatedQuery *b);
jazz_model_BindingId jazz_model__query__Binding__binding_id(const jazz_model_Binding *b);
jazz_model_Binding *jazz_model__query__Binding__clone(const jazz_model_Binding *b);
void jazz_model__query__Binding__drop(jazz_model_Binding *b);
/* binding values as wire values */
typedef struct jazz_model_ValueVec { void *ptr; u32 len; u32 cap; } jazz_model_ValueVec;
jazz_model_Query *jazz_model__query__Query__from_table(jz_slice table);
void jazz_model__query__Query__drop(jazz_model_Query *q);
extern const jz_uuid jazz_model__query__QUERY_NAMESPACE;
bool jazz_node__node__simple_scalar_exit_query(const jazz_model_QueryAst *q);

/* ======================= groove ======================= */
typedef enum { GROOVE_LV_BYTES = 0, GROOVE_LV_STRING = 1, GROOVE_LV_JSON = 2 } groove_LargeValueKind;
typedef struct groove_NodeRef { u8 locator[32]; u8 object_hash[32]; } groove_NodeRef;
typedef struct groove_LargeValueRef {
    u8 kind; /* groove_LargeValueKind */
    u64 byte_length;
    bool has_utf16_length;
    u64 utf16_length;
    groove_NodeRef root;
    u32 height;
} groove_LargeValueRef;
static inline bool groove_LargeValueRef_eq(const groove_LargeValueRef *a, const groove_LargeValueRef *b) { return memcmp(a, b, sizeof(*a)) == 0; }
typedef struct groove_StagedLargeValueId { u8 b[16]; } groove_StagedLargeValueId;
int groove__large_values__StagedLargeValueId__cmp(const void *a, const void *b, void *ctx);
typedef struct groove_StagedLargeValue { groove_StagedLargeValueId id; groove_LargeValueRef value_ref; } groove_StagedLargeValue;
typedef struct groove_StagedChunk { groove_NodeRef node_ref; jz_bytes encoded; } groove_StagedChunk;
void groove__large_values__StagedChunk__drop(void *p);
/* PushStreamingPreparation<fn(ContentHash)->Locator, Box<dyn FnMut(StagedChunk)->Result>> */
typedef struct groove_ChunkSink { jz_err (*stage)(void *ctx, groove_StagedChunk chunk); void *ctx; void (*drop)(void *ctx); } groove_ChunkSink;
typedef struct groove_PushStreamingPreparation groove_PushStreamingPreparation;
groove_PushStreamingPreparation *groove__large_values__PushStreamingPreparation__new(groove_LargeValueKind kind, groove_ChunkSink sink);
jz_err groove__large_values__PushStreamingPreparation__push(groove_PushStreamingPreparation *p, const u8 *bytes, u32 n);
jz_err groove__large_values__PushStreamingPreparation__finish(groove_PushStreamingPreparation *p, groove_LargeValueRef *out_ref);
void groove__large_values__PushStreamingPreparation__drop(groove_PushStreamingPreparation *p);
enum { GROOVE_LV_ERR_MALFORMED_SCALAR = 0x2001 };
typedef struct groove_SubscriptionId { u64 v; } groove_SubscriptionId;
typedef struct groove_RecordDescriptor groove_RecordDescriptor;
typedef struct groove_OwnedRecord { jz_bytes raw; const groove_RecordDescriptor *descriptor; } groove_OwnedRecord;

/* groove::records::Value (variant order as in Rust) */
typedef enum {
    GROOVE_V_U8, GROOVE_V_U16, GROOVE_V_U32, GROOVE_V_U64, GROOVE_V_F64, GROOVE_V_BOOL,
    GROOVE_V_STRING, GROOVE_V_BYTES, GROOVE_V_LARGE, GROOVE_V_UUID, GROOVE_V_ENUM_TAG,
    GROOVE_V_TUPLE, GROOVE_V_ARRAY, GROOVE_V_NULLABLE, GROOVE_V_I64, GROOVE_V_I32,
    GROOVE_V_RECORD, GROOVE_V_ENUM, GROOVE_V_U48
} groove_ValueTag;
typedef struct groove_Value groove_Value;
typedef struct groove_EnumValue { u8 tag; groove_OwnedRecord record; } groove_EnumValue;
struct groove_Value {
    u8 tag;
    union {
        u64 u; i64 i; f64 f; bool b; jz_str s; jz_bytes bytes; jz_uuid uuid;
        groove_LargeValueRef *large; /* Box */
        groove_Value *nullable;      /* Option<Box<Value>>, NULL = None */
        JZ_VEC(groove_Value) values; /* Tuple / Array */
        groove_OwnedRecord record;
        groove_EnumValue enum_value;
    } v;
};
void groove__records__Value__clone(void *dst, const void *src);
void groove__records__Value__drop(void *p);
bool groove__records__Value__eq(const void *a, const void *b);
typedef JZ_VEC(groove_Value) groove_ValueVec;
jz_err groove__records__OwnedRecord__to_values(const groove_OwnedRecord *r, groove_ValueVec *out);
void groove__records__ValueVec__drop(groove_ValueVec *v);
/* groove::records::ValueType and groove::schema::ColumnType: tag + boxed inner */
typedef struct groove_ValueType { u32 tag; struct groove_ValueType *inner; } groove_ValueType;
enum { GROOVE_VT_STRING = 6, GROOVE_VT_BYTES = 7, GROOVE_VT_NULLABLE = 13 };
typedef groove_ValueType groove_ColumnType;
enum { GROOVE_COLTYPE_NULLABLE = 13 };

/* ======================= jazz_node::schema ======================= */
typedef enum { JAZZ_LV_SEM_NOT_LARGE = 0, JAZZ_LV_SEM_BYTES, JAZZ_LV_SEM_STRING, JAZZ_LV_SEM_JSON } jazz_node_LargeValueSemanticKind;
typedef struct jazz_node_ColumnSchema {
    jz_str name;
    groove_ColumnType column_type;
    groove_Value *default_value; /* Option<Value> */
    u8 large_value_kind;         /* LargeValueSemanticKind */
} jazz_node_ColumnSchema;
typedef struct jazz_node_TableSchema {
    jz_str name;
    JZ_VEC(jazz_node_ColumnSchema) columns;
    JZ_VEC(jz_str) branch_by;
    void *read_policy; /* Option<Policy> */
} jazz_node_TableSchema;
typedef struct jazz_node_JazzSchema { JZ_VEC(jazz_node_TableSchema) tables; void *public_schema; } jazz_node_JazzSchema;
jazz_node_JazzSchema *jazz_node__schema__JazzSchema__clone(const jazz_node_JazzSchema *s);
void jazz_node__schema__TableSchema__clone(jazz_node_TableSchema *dst, const jazz_node_TableSchema *src);
void jazz_node__schema__TableSchema__drop(jazz_node_TableSchema *t);
groove_ColumnType jazz_node__schema__storage_column_type(const jazz_node_ColumnSchema *c);

/* RowCells = BTreeMap<String, Value>; claims maps share the same desc. */
extern const jz_btree_desc jazz_db_str_value_desc;
/* BTreeSet<String> */
extern const jz_btree_desc jazz_db_str_set_desc;

/* ======================= jazz_protocol ======================= */
typedef struct jazz_protocol_ReadViewKey { jz_uuid id; u8 tier; } jazz_protocol_ReadViewKey;
typedef struct jazz_protocol_SubscriptionKey {
    jazz_model_ShapeId shape_id;
    jazz_model_BindingId binding_id;
    jazz_protocol_ReadViewKey read_view;
} jazz_protocol_SubscriptionKey;
int jazz_protocol__protocol__SubscriptionKey__cmp(const void *a, const void *b, void *ctx);
static inline bool jazz_protocol_SubscriptionKey_eq(const jazz_protocol_SubscriptionKey *a, const jazz_protocol_SubscriptionKey *b) { return memcmp(a, b, sizeof(*a)) == 0; }
void jazz_protocol__protocol__SubscriptionKey__debug(jz_str *out, const jazz_protocol_SubscriptionKey *k);
typedef struct jazz_protocol_BindingViewKey {
    jazz_model_ShapeId shape_id;
    jazz_model_BindingId binding_id;
    jazz_protocol_ReadViewKey read_view;
} jazz_protocol_BindingViewKey;
typedef struct jazz_protocol_PermissionAdviceRequestId { u8 b[16]; } jazz_protocol_PermissionAdviceRequestId;
int jazz_protocol__protocol__PermissionAdviceRequestId__cmp(const void *a, const void *b, void *ctx);
typedef enum { JAZZ_ADVICE_ALLOWED = 0, JAZZ_ADVICE_DENIED = 1, JAZZ_ADVICE_UNKNOWN = 2 } jazz_protocol_PermissionAdvice;
typedef struct jazz_protocol_PermissionAdviceAction jazz_protocol_PermissionAdviceAction; /* opaque */
jazz_protocol_PermissionAdviceAction *jazz_protocol__protocol__PermissionAdviceAction__clone(const jazz_protocol_PermissionAdviceAction *a);
bool jazz_protocol__protocol__PermissionAdviceAction__eq(const jazz_protocol_PermissionAdviceAction *a, const jazz_protocol_PermissionAdviceAction *b);
void jazz_protocol__protocol__PermissionAdviceAction__drop(jazz_protocol_PermissionAdviceAction *a);
typedef struct jazz_protocol_DelegatedSessionBinding { jazz_types_AuthorSubject identity; jz_btree claims; } jazz_protocol_DelegatedSessionBinding;
void jazz_protocol__protocol__DelegatedSessionBinding__clone(void *dst, const void *src);
void jazz_protocol__protocol__DelegatedSessionBinding__drop(void *p);
bool jazz_protocol__protocol__DelegatedSessionBinding__eq(const jazz_protocol_DelegatedSessionBinding *a, const jazz_protocol_DelegatedSessionBinding *b);
typedef struct jazz_protocol_BranchSelector { jz_str name; } jazz_protocol_BranchSelector;
typedef struct jazz_protocol_BranchViewBase jazz_protocol_BranchViewBase; /* opaque */
void jazz_protocol__protocol__BranchSelector__default(jazz_protocol_BranchSelector *out);
void jazz_protocol__protocol__BranchSelector__clone(jazz_protocol_BranchSelector *dst, const jazz_protocol_BranchSelector *src);
void jazz_protocol__protocol__BranchSelector__drop(jazz_protocol_BranchSelector *b);
jazz_protocol_BranchViewBase *jazz_protocol__protocol__BranchViewBase__clone(const jazz_protocol_BranchViewBase *b);
typedef struct jazz_protocol_ReadViewSpec jazz_protocol_ReadViewSpec;
jazz_protocol_ReadViewSpec *jazz_protocol__protocol__ReadViewSpec__clone(const jazz_protocol_ReadViewSpec *r);
jazz_protocol_ReadViewSpec *jazz_protocol__protocol__ReadViewSpec__default(void);
bool jazz_protocol__protocol__ReadViewSpec__is_default(const jazz_protocol_ReadViewSpec *r);
void jazz_protocol__protocol__ReadViewSpec__drop(jazz_protocol_ReadViewSpec *r);
typedef enum { JAZZ_BINDING_SOURCE_DIRECT = 0, JAZZ_BINDING_SOURCE_RELAY_AUTHORITY_SESSION = 1 } jazz_protocol_BindingSource;
typedef struct jazz_protocol_RegisterShapeOptions {
    jazz_model_DurabilityTier tier;
    jazz_protocol_ReadViewSpec *read_view;
    u8 binding_source;
    u8 extra[16];
} jazz_protocol_RegisterShapeOptions;
jazz_protocol_ReadViewKey jazz_protocol__protocol__RegisterShapeOptions__read_view_key(const jazz_protocol_RegisterShapeOptions *o);
void jazz_protocol__protocol__RegisterShapeOptions__clone(jazz_protocol_RegisterShapeOptions *dst, const jazz_protocol_RegisterShapeOptions *src);
void jazz_protocol__protocol__RegisterShapeOptions__default(jazz_protocol_RegisterShapeOptions *out);
void jazz_protocol__protocol__RegisterShapeOptions__drop(jazz_protocol_RegisterShapeOptions *o);
bool jazz_protocol__protocol__RegisterShapeOptions__eq(const jazz_protocol_RegisterShapeOptions *a, const jazz_protocol_RegisterShapeOptions *b);
/* PolicyBindingKey { identity, canonical_claims } */
typedef struct jazz_protocol_PolicyBindingKey { jazz_types_AuthorSubject identity; jz_btree canonical_claims; } jazz_protocol_PolicyBindingKey;
void jazz_protocol__protocol__PolicyBindingKey__from_canonical_parts(jazz_protocol_PolicyBindingKey *out, jazz_types_AuthorSubject identity, jz_btree claims);
void jazz_protocol__protocol__CanonicalPolicyClaims__new(jz_btree *out, jz_btree claims);
const jz_btree *jazz_protocol__protocol__PolicyBindingKey__claims(const jazz_protocol_PolicyBindingKey *k);
void jazz_protocol__protocol__PolicyBindingKey__clone(void *dst, const void *src);
void jazz_protocol__protocol__PolicyBindingKey__drop(void *p);
bool jazz_protocol__protocol__PolicyBindingKey__eq(const jazz_protocol_PolicyBindingKey *a, const jazz_protocol_PolicyBindingKey *b);
/* CoverageKey { shape_id, binding_id, opts, policy_binding: Option<PolicyBindingKey> } */
typedef struct jazz_db_CoverageKey {
    jazz_model_ShapeId shape_id;
    jazz_model_BindingId binding_id;
    jazz_protocol_RegisterShapeOptions opts;
    bool has_policy_binding;
    jazz_protocol_PolicyBindingKey policy_binding;
} jazz_db_CoverageKey;
int jazz_db__db__CoverageKey__cmp(const void *a, const void *b, void *ctx);
bool jazz_db__db__CoverageKey__eq(const jazz_db_CoverageKey *a, const jazz_db_CoverageKey *b);
void jazz_db__db__CoverageKey__clone(void *dst, const void *src);
void jazz_db__db__CoverageKey__drop(void *p);
/* AuthorityResultKey { binding_view, policy } */
typedef struct jazz_protocol_AuthorityResultKey { jazz_protocol_BindingViewKey binding_view; bool has_policy; jazz_protocol_PolicyBindingKey policy; } jazz_protocol_AuthorityResultKey;
void jazz_protocol__protocol__AuthorityResultKey__policy_scoped(jazz_protocol_AuthorityResultKey *out, jazz_protocol_BindingViewKey view, jazz_protocol_PolicyBindingKey policy);
void jazz_protocol__protocol__AuthorityResultKey__clone(void *dst, const void *src);
void jazz_protocol__protocol__AuthorityResultKey__drop(void *p);
bool jazz_protocol__protocol__AuthorityResultKey__eq(const jazz_protocol_AuthorityResultKey *a, const jazz_protocol_AuthorityResultKey *b);
int jazz_protocol__protocol__AuthorityResultKey__cmp(const void *a, const void *b, void *ctx);
/* AuthorityContext (Copy) */
typedef struct jazz_protocol_AuthorityContext {
    u8 authority[16];
    jazz_types_AuthorSubject link;
    u64 connection_id;
    u64 connection_epoch;
    u64 claims_revision;
    u64 policy_epoch;
    u64 authorization_progress;
    u64 settled_through;
} jazz_protocol_AuthorityContext;
static inline bool jazz_protocol_AuthorityContext_eq(const jazz_protocol_AuthorityContext *a, const jazz_protocol_AuthorityContext *b) { return memcmp(a, b, sizeof(*a)) == 0; }
/* CurrentRowCoordinate */
typedef struct jazz_protocol_CurrentRowCoordinate {
    jazz_types_SchemaVersionId schema;
    jz_str table;
    jazz_types_GlobalPhysicalTableId physical_table;
    jazz_types_RowUuid row;
} jazz_protocol_CurrentRowCoordinate;
void jazz_protocol__protocol__CurrentRowCoordinate__clone(void *dst, const void *src);
void jazz_protocol__protocol__CurrentRowCoordinate__drop(void *p);
bool jazz_protocol__protocol__CurrentRowCoordinate__eq(const jazz_protocol_CurrentRowCoordinate *a, const jazz_protocol_CurrentRowCoordinate *b);
typedef enum { JAZZ_CRO_READABLE = 0, JAZZ_CRO_CURRENT_UNAVAILABLE = 1, JAZZ_CRO_UNKNOWN = 2 } jazz_protocol_CurrentRowOutcome;
#define JAZZ_PROTOCOL_MAX_CURRENT_ROWS 64u
typedef struct jazz_protocol_CurrentRowsReceipt {
    JZ_VEC(jazz_protocol_CurrentRowCoordinate) rows;
    JZ_VEC(u8) outcomes;
    jazz_protocol_PolicyBindingKey context;
    u8 core[16];
    u64 core_epoch, claims_revision, policy_epoch, settled_through, authorization_progress;
} jazz_protocol_CurrentRowsReceipt;
/* SubscribeRejectReason (opaque, cloneable) */
typedef struct jazz_protocol_SubscribeRejectReason { u32 tag; u32 code; void *detail; } jazz_protocol_SubscribeRejectReason;
enum { JAZZ_REJECT_SERVER_FAILURE = 3, JAZZ_REJECT_INVALID_AUTHORITY_SOURCE_CLOSURE = 7 };
enum { JAZZ_SERVER_FAILURE_INTERNAL = 0 };
void jazz_protocol__protocol__SubscribeRejectReason__clone(void *dst, const void *src);
void jazz_protocol__protocol__SubscribeRejectReason__drop(void *p);

/* ---------- SyncMessage (externally tagged enum; variant order as in Rust) ---------- */
typedef enum {
    JAZZ_SM_CHUNK_REQUEST_BATCH, JAZZ_SM_CHUNK_RESPONSE_BATCH, JAZZ_SM_SESSION_CLAIMS, JAZZ_SM_COMMIT_UNIT,
    JAZZ_SM_FATE_UPDATE, JAZZ_SM_REGISTER_SHAPE, JAZZ_SM_SUBSCRIBE, JAZZ_SM_SUBSCRIBE_REJECTED,
    JAZZ_SM_UNSUBSCRIBE, JAZZ_SM_PUBLISH_SCHEMA, JAZZ_SM_PUBLISH_SCHEMA_WITH_LENS, JAZZ_SM_PUBLISH_LENS,
    JAZZ_SM_CATALOGUE_ACK, JAZZ_SM_VIEW_UPDATE, JAZZ_SM_RESERVED14, JAZZ_SM_RESERVED15,
    JAZZ_SM_CATALOGUE_SNAPSHOT, JAZZ_SM_AUTHORIZATION_SCOPE_INTENT, JAZZ_SM_AUTHORIZATION_SCOPE_VIEW,
    JAZZ_SM_AUTHORIZATION_SCOPE_AGGREGATE_RECEIPT, JAZZ_SM_AUTHORIZATION_SCOPE_UNAVAILABLE,
    JAZZ_SM_AUTHORIZATION_SCOPE_DECISION, JAZZ_SM_CHUNK_UPLOAD_START, JAZZ_SM_CHUNK_UPLOAD_NODES,
    JAZZ_SM_CHUNK_UPLOAD_RESULT, JAZZ_SM_CURRENT_ROWS_REQUEST, JAZZ_SM_CURRENT_ROWS_RECEIPT,
    JAZZ_SM_CURRENT_ROWS_CANCEL, JAZZ_SM_VIEW_UPDATE_PART, JAZZ_SM_RESERVED29, JAZZ_SM_READ_BARRIER
} jazz_protocol_SyncMessageTag;
typedef struct jazz_protocol_Transaction jazz_protocol_Transaction;
typedef struct jazz_protocol_TransactionHdr {
    jazz_model_TxId tx_id;
    jazz_types_AuthorSubject made_by;
    bool has_permission_subject;
    jazz_types_AuthorSubject permission_subject;
} jazz_protocol_TransactionHdr;
typedef struct jazz_protocol_VersionRecord jazz_protocol_VersionRecord;
typedef JZ_VEC(jazz_protocol_VersionRecord *) jazz_protocol_VersionVec;
typedef struct jazz_protocol_ViewUpdatePayload jazz_protocol_ViewUpdatePayload;
typedef struct jazz_protocol_Subscribe {
    jazz_model_ShapeId shape_id;
    jazz_protocol_SubscriptionKey subscription;
    jazz_model_ValueVec values;
    void *known_state;
    bool has_delegated_session;
    jazz_protocol_DelegatedSessionBinding delegated_session;
} jazz_protocol_Subscribe;
typedef struct jazz_protocol_ShapeAst jazz_protocol_ShapeAst;
typedef struct jazz_protocol_ChunkResponse jazz_protocol_ChunkResponse;
typedef struct jazz_protocol_ChunkUploadStatus { u32 tag; JZ_VEC(groove_NodeRef) need; jz_str failed; } jazz_protocol_ChunkUploadStatus;
enum { JAZZ_CHUNK_UPLOAD_NEED = 0, JAZZ_CHUNK_UPLOAD_STAGED = 1, JAZZ_CHUNK_UPLOAD_RATE_LIMITED = 2, JAZZ_CHUNK_UPLOAD_FAILED = 3 };
typedef struct jazz_protocol_AuthorizationSupportScopeKey jazz_protocol_AuthorizationSupportScopeKey;
typedef struct jazz_protocol_AuthorizationScopeReceipt jazz_protocol_AuthorizationScopeReceipt;
typedef struct jazz_protocol_SyncMessage jazz_protocol_SyncMessage;
struct jazz_protocol_SyncMessage {
    u32 tag;
    union {
        struct { JZ_VEC(void *) requests; } chunk_request_batch;
        struct { JZ_VEC(jazz_protocol_ChunkResponse *) responses; } chunk_response_batch;
        struct { jazz_types_AuthorSubject identity; jz_btree claims; } session_claims;
        struct { jazz_protocol_Transaction *tx; jazz_protocol_VersionVec versions; } commit_unit;
        struct { jazz_model_TxId tx_id; jazz_model_Fate fate; bool has_global_time; jazz_types_GlobalTime global_time; bool has_durability; u8 durability; } fate_update;
        struct { jazz_model_ShapeId shape_id; jazz_protocol_ShapeAst *ast; jazz_protocol_RegisterShapeOptions opts; } register_shape;
        jazz_protocol_Subscribe subscribe;
        struct { jazz_protocol_SubscriptionKey subscription; jazz_protocol_SubscribeRejectReason reason; } subscribe_rejected;
        struct { jazz_protocol_SubscriptionKey subscription; } unsubscribe;
        struct { jazz_types_AuthorSubject author; void *schema; u64 catalogue_seq; } publish;
        void *catalogue_ack;
        jazz_protocol_ViewUpdatePayload *view_update; /* ViewUpdate / ViewUpdatePart */
        void *catalogue_snapshot;
        struct { jazz_protocol_PermissionAdviceRequestId request_id; jazz_protocol_PermissionAdviceAction *action; bool has_delegated_session; jazz_protocol_DelegatedSessionBinding delegated_session; } scope_intent;
        struct { jazz_protocol_PermissionAdviceRequestId request_id; jazz_protocol_AuthorizationSupportScopeKey *key; u16 clause_index; u16 clause_count; jazz_protocol_ViewUpdatePayload *view; } scope_view;
        struct { jazz_protocol_PermissionAdviceRequestId request_id; jazz_protocol_AuthorizationScopeReceipt *receipt; } scope_receipt;
        struct { jazz_protocol_PermissionAdviceRequestId request_id; } scope_unavailable;
        struct { jazz_protocol_PermissionAdviceRequestId request_id; u8 advice; } scope_decision;
        struct { groove_LargeValueRef value_ref; } chunk_upload_start;
        struct { groove_LargeValueRef value_ref; JZ_VEC(groove_StagedChunk) chunks; } chunk_upload_nodes;
        struct { groove_LargeValueRef value_ref; jazz_protocol_ChunkUploadStatus status; } chunk_upload_result;
        void *current_rows_request;
        jazz_protocol_CurrentRowsReceipt *current_rows_receipt;
        struct { jazz_protocol_PermissionAdviceRequestId request_id; } current_rows_cancel;
        struct { u64 writes_through; } read_barrier;
    } v;
};
void jazz_protocol__protocol__SyncMessage__clone(void *dst, const void *src);
void jazz_protocol__protocol__SyncMessage__drop(void *p);
const jazz_protocol_TransactionHdr *jazz_protocol__protocol__Transaction__hdr(const jazz_protocol_Transaction *tx);
jazz_protocol_ShapeAst *jazz_protocol__protocol__ShapeAst__from_validated(const jazz_model_ValidatedQuery *q);
/* SyncMessage::uploaded_versions(), VersionRecord::application_cell_count/cell_at */
void jazz_protocol__protocol__SyncMessage__uploaded_versions(const jazz_protocol_SyncMessage *m, jazz_protocol_VersionVec *out);
u32 jazz_protocol__protocol__VersionRecord__application_cell_count(const jazz_protocol_VersionRecord *v);
bool jazz_protocol__protocol__VersionRecord__cell_at(const jazz_protocol_VersionRecord *v, u32 position, groove_Value *out);
/* postcard size helper (serialized_sync_message_len) */
u32 jazz_protocol__protocol__SyncMessage__postcard_len(const jazz_protocol_SyncMessage *m);
jz_err jazz_protocol__protocol__ViewUpdatePayload__from_view_update(const jazz_protocol_SyncMessage *view, jazz_protocol_ViewUpdatePayload **out);
jazz_protocol_AuthorizationSupportScopeKey *jazz_protocol__protocol__AuthorizationSupportScopeKey__clone(const jazz_protocol_AuthorizationSupportScopeKey *k);
jazz_protocol_AuthorizationScopeReceipt *jazz_protocol__protocol__AuthorizationScopeReceipt__clone(const jazz_protocol_AuthorizationScopeReceipt *r);

/* CommitUnitTrust / CommitUnitIngestContext (jazz_node::node) */
typedef enum {
    JAZZ_TRUST_SESSION = 0, JAZZ_TRUST_RELAY, JAZZ_TRUST_TRUSTED_BACKEND, JAZZ_TRUST_TRUSTED_AUTHORITY, JAZZ_TRUST_TRUSTED_ADMIN
} jazz_node_CommitUnitTrust;
bool jazz_node__node__CommitUnitTrust__is_trusted(jazz_node_CommitUnitTrust t);
typedef struct jazz_node_CommitUnitIngestContext {
    jazz_types_AuthorSubject identity;
    u8 trust; /* CommitUnitTrust */
    bool admitted_write_authorization;
    bool version_receipts_validated;
} jazz_node_CommitUnitIngestContext;
typedef enum { JAZZ_QAM_CLIENT_LOCAL = 0, JAZZ_QAM_LINK = 1, JAZZ_QAM_TRUSTED = 2 } jazz_node_QueryAuthorizationMode;

/* ======================= jazz_node: NodeState and friends ======================= */
typedef struct jazz_node_NodeState jazz_node_NodeState;
/* SharedNodeState = Rc<futures::lock::Mutex<NodeState>> */
typedef struct jazz_db_NodeMutex { u32 locked; jazz_node_NodeState *state; } jazz_db_NodeMutex;
static inline jazz_node_NodeState *jazz_db_node_lock(jazz_db_NodeMutex *m) { m->locked++; return m->state; }
static inline jazz_node_NodeState *jazz_db_node_try_lock(jazz_db_NodeMutex *m) { if (m->locked) return NULL; m->locked++; return m->state; }
static inline void jazz_db_node_unlock(jazz_db_NodeMutex *m) { if (m->locked) m->locked--; }
/* LocalMutexBorrow::borrow: try_lock or panic */
jazz_node_NodeState *jazz_db__db__LocalMutexBorrow__borrow(jazz_db_NodeMutex *m);

typedef struct jazz_node_CurrentRow {
    const jz_str *table;
    groove_OwnedRecord *record;
    bool deleted;
    void *publication_fields;
} jazz_node_CurrentRow;
void jazz_node__node__CurrentRow__drop(void *p);
jazz_types_RowUuid jazz_node__node__CurrentRow__row_uuid(const jazz_node_CurrentRow *r);
bool jazz_node__node__CurrentRow__cell(const jazz_node_CurrentRow *r, const jazz_node_TableSchema *schema, jz_slice column, groove_Value *out);
typedef JZ_VEC(jazz_node_CurrentRow) jazz_node_CurrentRowVec;
typedef struct jazz_node_RelationSnapshot {
    u32 root_count;
    jazz_node_CurrentRowVec rows;
    JZ_VEC(void *) edges;
} jazz_node_RelationSnapshot;
void jazz_node__node__RelationSnapshot__clone(jazz_node_RelationSnapshot *dst, const jazz_node_RelationSnapshot *src);
void jazz_node__node__RelationSnapshot__drop(jazz_node_RelationSnapshot *s);
bool jazz_node__node__RelationSnapshot__eq(const jazz_node_RelationSnapshot *a, const jazz_node_RelationSnapshot *b);
typedef struct jazz_db_OutputOccurrenceVec { void *ptr; u32 len; u32 cap; } jazz_db_OutputOccurrenceVec;
typedef struct jazz_node_RelationSnapshotIndex {
    JZ_VEC(void *) roots; /* Vec<Option<OutputOccurrenceId>> */
    jz_btree terminal_records;
    void *rows;
} jazz_node_RelationSnapshotIndex;
void jazz_node__node__RelationSnapshotIndex__clone(jazz_node_RelationSnapshotIndex *dst, const jazz_node_RelationSnapshotIndex *src);
void jazz_node__node__RelationSnapshotIndex__drop(jazz_node_RelationSnapshotIndex *s);
void jazz_node__node__RelationSnapshotIndex__with_root_occurrences(jazz_node_RelationSnapshotIndex *out, jazz_db_OutputOccurrenceVec roots);
void jazz_node__node__RelationSnapshotIndex__flatten_roots(const jazz_node_RelationSnapshotIndex *s, jazz_db_OutputOccurrenceVec *out);

/* LocalMaintainedViewSubscription */
typedef struct jazz_node_LocalMaintainedViewSubscription jazz_node_LocalMaintainedViewSubscription;
groove_SubscriptionId jazz_node__node__LocalMaintainedViewSubscription__subscription_id(const jazz_node_LocalMaintainedViewSubscription *m);
bool jazz_node__node__LocalMaintainedViewSubscription__initial_snapshot_received(const jazz_node_LocalMaintainedViewSubscription *m);
bool jazz_node__node__LocalMaintainedViewSubscription__has_covered_input_sources(const jazz_node_LocalMaintainedViewSubscription *m);
bool jazz_node__node__LocalMaintainedViewSubscription__has_installed_covered_closure(const jazz_node_LocalMaintainedViewSubscription *m, const jazz_protocol_AuthorityResultKey *key, u64 generation);
bool jazz_node__node__LocalMaintainedViewSubscription__uses_logical_tables(const jazz_node_LocalMaintainedViewSubscription *m, const jz_hmap *tables);
const void *jazz_node__node__LocalMaintainedViewSubscription__terminal_root_layout(const jazz_node_LocalMaintainedViewSubscription *m);
const jazz_db_OutputOccurrenceVec *jazz_node__node__LocalMaintainedViewSubscription__root_occurrence_ids(const jazz_node_LocalMaintainedViewSubscription *m);
jz_err jazz_node__node__LocalMaintainedViewSubscription__decoded_terminal_records(const jazz_node_LocalMaintainedViewSubscription *m, jz_btree *out);
void jazz_node__node__LocalMaintainedViewSubscription__drop(jazz_node_LocalMaintainedViewSubscription *m);
/* LocalMaintainedViewSubscriptionUpdate */
typedef enum { JAZZ_LMU_STRUCTURED = 0, JAZZ_LMU_FLAT = 1, JAZZ_LMU_ORDERED_WINDOW = 2 } jazz_node_LmuTag;
typedef struct jazz_node_LocalMaintainedUpdate { u32 tag; JZ_VEC(void *) terminal_operations; void *flat; } jazz_node_LocalMaintainedUpdate;
void jazz_node__node__LocalMaintainedUpdate__drop(jazz_node_LocalMaintainedUpdate *u);
typedef struct jazz_node_MaterializedSnapshot { jazz_node_RelationSnapshot snapshot; jazz_db_OutputOccurrenceVec root_occurrence_ids; } jazz_node_MaterializedSnapshot;

/* NodeState methods (async ones are synchronous in the port). Node errors. */
#define NS jazz_node_NodeState
bool jazz_node__node__NodeState__is_history_complete(NS *n);
void *jazz_node__node__NodeState__local_chunk_reader_handle(NS *n);
void *jazz_node__node__NodeState__tx_time_reservation_clock(NS *n); /* Rc<Cell<TxTime>> */
jazz_types_NodeUuid jazz_node__node__NodeState__node_uuid(NS *n);
void jazz_node__node__NodeState__set_missing_chunk_resolver(NS *n, void *resolver);
bool *jazz_node__node__NodeState__detach_covered_chunk_waits_handle(NS *n);
void jazz_node__node__NodeState__rejected_transactions(NS *n, JZ_VEC(jazz_model_TxId) *out);
const void *jazz_node__node__NodeState__rejected_transaction(NS *n, jazz_model_TxId tx);
jz_err jazz_node__node__NodeState__abandon_tx(NS *n, jazz_model_OpenTransactionId id);
bool jazz_node__node__NodeState__has_released_large_values(NS *n);
void jazz_node__node__NodeState__evict_released_large_values(NS *n);
void jazz_node__node__NodeState__abandon_all_open_transactions(NS *n);
jz_err jazz_node__node__NodeState__evict_expired_staged_large_values(NS *n, u32 *out);
void jazz_node__node__NodeState__set_non_durable_client(NS *n);
jz_err jazz_node__node__NodeState__configure_scope_isolated_client_relay(NS *n, void *scope);
void jazz_node__node__NodeState__set_permissions_ready(NS *n, bool ready);
void jazz_node__node__NodeState__set_large_value_staging_policy(NS *n, const void *policy);
jz_err jazz_node__node__NodeState__settle_published_transaction(NS *n, jazz_model_TxId tx, jz_err persistence);
jz_err jazz_node__node__NodeState__pending_transaction_ids_for(NS *n, jazz_types_NodeUuid node, jazz_types_AuthorSubject author, JZ_VEC(jazz_model_TxId) *out);
jz_err jazz_node__node__NodeState__synchronizing_transaction_ids_for_node(NS *n, jazz_types_NodeUuid node, JZ_VEC(jazz_model_TxId) *out);
jz_err jazz_node__node__NodeState__pending_transaction_ids_for_author(NS *n, jazz_types_AuthorSubject author, JZ_VEC(jazz_model_TxId) *out);
bool jazz_node__node__NodeState__groove_runtime_token_eq(NS *n, u64 token);
u64 jazz_node__node__NodeState__groove_runtime_token(NS *n);
void jazz_node__node__NodeState__unsubscribe_groove_subscription(NS *n, groove_SubscriptionId id);
void jazz_node__node__NodeState__apply_unsubscribe(NS *n, jazz_protocol_SubscriptionKey s);
jz_err jazz_node__node__NodeState__discard_rejection(NS *n, jazz_model_TxId tx);
/* transaction_state -> Option<(Fate, Option<GlobalTime>, DurabilityTier)> */
bool jazz_node__node__NodeState__transaction_state(NS *n, jazz_model_TxId tx, jazz_model_Fate *fate, bool *has_gt, jazz_types_GlobalTime *gt, jazz_model_DurabilityTier *dur);
typedef struct jazz_node_TransactionRecord { jazz_model_Fate fate; u8 durability; bool has_global_time; jazz_types_GlobalTime global_time; u8 kind; } jazz_node_TransactionRecord;
bool jazz_node__node__NodeState__transaction_record(NS *n, jazz_model_TxId tx, jazz_node_TransactionRecord *out);
bool jazz_node__node__NodeState__is_history_complete(NS *n);
jazz_types_GlobalTime jazz_node__node__NodeState__committed_global_time(NS *n);
void *jazz_node__node__NodeState__client_relay_scope(NS *n);
bool jazz_node__node__ClientRelayScope__admits_session(void *scope, jazz_types_AuthorSubject identity);
/* session_claims_with_revisions -> Vec<(AuthorSubject, BTreeMap, u64)> */
typedef struct jazz_node_SessionClaimsRev { jazz_types_AuthorSubject identity; jz_btree claims; u64 revision; } jazz_node_SessionClaimsRev;
typedef JZ_VEC(jazz_node_SessionClaimsRev) jazz_node_SessionClaimsRevVec;
void jazz_node__node__NodeState__session_claims_with_revisions(NS *n, jazz_node_SessionClaimsRevVec *out);
void jazz_node__node__SessionClaimsRevVec__drop(jazz_node_SessionClaimsRevVec *v);
void jazz_node__node__NodeState__session_claims_for(NS *n, jazz_types_AuthorSubject a, jz_btree *out);
u64 jazz_node__node__NodeState__session_claim_revision(NS *n, jazz_types_AuthorSubject a);
void jazz_node__node__NodeState__set_session_claims(NS *n, jazz_types_AuthorSubject a, jz_btree claims);
jz_err jazz_node__node__NodeState__authority_result_key_for_subscription(NS *n, jazz_protocol_SubscriptionKey s, jazz_protocol_AuthorityResultKey *out);
void jazz_node__node__NodeState__invalidate_authority_result_settlement(NS *n, const jazz_protocol_AuthorityResultKey *k);
bool jazz_node__node__NodeState__has_settled_authority_result(NS *n, const jazz_protocol_AuthorityResultKey *k);
bool jazz_node__node__NodeState__opening_pending_for_authority_result(NS *n, const jazz_protocol_AuthorityResultKey *k);
bool jazz_node__node__NodeState__publication_deferred_for_authority_result(NS *n, const jazz_protocol_AuthorityResultKey *k);
bool jazz_node__node__NodeState__authority_source_closure_generation(NS *n, const jazz_protocol_AuthorityResultKey *k, u64 *out);
void jazz_node__node__NodeState__scalar_authority_input_rows(NS *n, const jazz_protocol_AuthorityResultKey *k, const jz_str *table, jz_btree *out /* BTreeSet<RowUuid> */);
bool jazz_node__node__NodeState__has_pending_query_runtime(NS *n);
bool jazz_node__node__NodeState__subscription_has_pending_query_runtime(NS *n, groove_SubscriptionId id);
jz_err jazz_node__node__NodeState__drive_query_runtime(NS *n);
jz_err jazz_node__node__NodeState__drive_ready_query_runtime_with_waker(NS *n, const jz_waker *w);
void jazz_node__node__NodeState__snapshot_pending_authoritative_resets(NS *n, jz_btree *out /* AuthorityResultKey -> u64 */);
void jazz_node__node__NodeState__acknowledge_authoritative_reset(NS *n, const jazz_protocol_AuthorityResultKey *k, u64 generation);
void jazz_node__node__NodeState__logical_table_names_for_storage_tables(NS *n, const jz_hmap *tables, jz_hmap *out);
void jazz_node__node__NodeState__release_shapes_for_peer(NS *n, u64 epoch);
void jazz_node__node__NodeState__record_authoritative_reset_missing_payload_fallback(NS *n);
bool jazz_node__node__NodeState__covered_receiver_evaluation_pending(NS *n, const jazz_node_LocalMaintainedViewSubscription *m);
jz_err jazz_node__node__NodeState__relation_snapshot_has_materialized_required_cells(NS *n, const jazz_model_ValidatedQuery *shape, const jazz_node_RelationSnapshot *s, bool *out);
jz_err jazz_node__node__NodeState__open_maintained_view_subscription_in_authorization_mode_with_waker(
    NS *n, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding, jazz_types_AuthorSubject author,
    jazz_model_DurabilityTier tier, const jazz_protocol_ReadViewSpec *view, const void *window, u8 mode, bool pending_overlay,
    const jz_waker *waker, jazz_node_LocalMaintainedViewSubscription **out_maintained, jazz_node_RelationSnapshot *out_snapshot);
jz_err jazz_node__node__NodeState__drain_local_maintained_view_subscription_preserving_rows_with_waker(
    NS *n, jazz_node_LocalMaintainedViewSubscription *m, const jazz_protocol_AuthorityResultKey *authority, const jz_btree *overlay_rows,
    const jz_waker *waker, bool *has_update, jazz_node_LocalMaintainedUpdate *update, bool *suppressed);
jz_err jazz_node__node__NodeState__materialize_local_maintained_relation_snapshot_with_occurrences(NS *n, const jazz_node_LocalMaintainedViewSubscription *m, jazz_node_MaterializedSnapshot *out);
jz_err jazz_node__node__NodeState__subscription_snapshot_in_authorization_mode(NS *n, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding, jazz_model_DurabilityTier tier, jazz_types_AuthorSubject author, const jazz_protocol_ReadViewSpec *view, u8 mode, jazz_node_RelationSnapshot *out);
jz_err jazz_node__node__NodeState__query_binding_for_link(NS *n, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding, jazz_model_ValidatedQuery **out_shape, jazz_model_Binding **out_binding);
jz_err jazz_node__node__NodeState__local_content_winner_tx_id(NS *n, const jz_str *table, jazz_types_RowUuid row, bool *has, jazz_model_TxId *out);
jz_err jazz_node__node__NodeState__current_row_coordinate(NS *n, const jz_str *table, jazz_types_RowUuid row, jazz_protocol_CurrentRowCoordinate *out);
jz_err jazz_node__node__NodeState__query_rows_for_client(NS *n, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding, jazz_model_DurabilityTier tier, jazz_types_AuthorSubject identity, jazz_node_CurrentRowVec *out);
/* Scoped claims: owner.scoped_optional_session_claims / scoped_active_session_claims return a guard
 * that derefs to NodeState; modelled as a push/pop pair on the node. */
NS *jazz_node__node__NodeState__scoped_optional_session_claims(NS *n, jazz_types_AuthorSubject a, const jz_btree *claims /* NULL = None */);
NS *jazz_node__node__NodeState__scoped_active_session_claims(NS *n, jazz_types_AuthorSubject a, jz_btree claims);
void jazz_node__node__NodeState__end_scoped_claims(NS *n);
typedef struct jazz_node_LocalReadPolicyBinding jazz_node_LocalReadPolicyBinding;
jazz_node_LocalReadPolicyBinding *jazz_node__node__NodeState__local_read_policy_binding(NS *n, jazz_types_AuthorSubject a);
jz_err jazz_node__node__NodeState__activate_local_availability_authority(NS *n, const jazz_node_LocalReadPolicyBinding *scope, const u8 core[16], u64 core_epoch);
typedef struct jazz_node_LocalAvailabilityWatermark { u8 core[16]; u64 core_epoch, claims_revision, policy_epoch, settled_through, authorization_progress; } jazz_node_LocalAvailabilityWatermark;
typedef struct jazz_node_RowAvailabilityOutcome { jazz_types_GlobalPhysicalTableId table; jazz_types_RowUuid row; u8 status; } jazz_node_RowAvailabilityOutcome;
enum { JAZZ_LRA_READABLE = 0, JAZZ_LRA_CURRENT_UNAVAILABLE = 1 };
jz_err jazz_node__node__NodeState__apply_verified_local_row_availability(NS *n, const jazz_node_LocalReadPolicyBinding *scope, jazz_node_LocalAvailabilityWatermark wm, const void *outcomes, u32 n_outcomes, bool *changed);
jz_err jazz_node__node__NodeState__local_availability_table_id(NS *n, jazz_types_SchemaVersionId schema, const jz_str *table, u64 *out);
bool jazz_node__node__NodeState__is_local_row_unavailable(NS *n, const jazz_node_LocalReadPolicyBinding *scope, u64 table_id, jazz_types_RowUuid row);

/* mutations.rs node API */
typedef struct jazz_node_MergeableCommit jazz_node_MergeableCommit;
jazz_node_MergeableCommit *jazz_node__node__MergeableCommit__new(jz_slice table, jazz_types_RowUuid row, u64 now_ms);
void jazz_node__node__MergeableCommit__made_by(jazz_node_MergeableCommit *c, jazz_types_AuthorSubject a);
void jazz_node__node__MergeableCommit__cells(jazz_node_MergeableCommit *c, jz_btree cells);
void jazz_node__node__MergeableCommit__authored_columns(jazz_node_MergeableCommit *c, jz_btree columns);
void jazz_node__node__MergeableCommit__branch(jazz_node_MergeableCommit *c, jazz_protocol_BranchSelector b);
void jazz_node__node__MergeableCommit__permission_subject(jazz_node_MergeableCommit *c, jazz_types_AuthorSubject a);
void jazz_node__node__MergeableCommit__deletion(jazz_node_MergeableCommit *c, jazz_model_DeletionEvent d);
void jazz_node__node__MergeableCommit__verified_inherited_large_cells(jazz_node_MergeableCommit *c, const jz_btree *cells);
bool jazz_node__node__MergeableCommit__needs_large_value_staging(const jazz_node_MergeableCommit *c);
void jazz_node__node__MergeableCommit__staged_large_cell(jazz_node_MergeableCommit *c, jz_slice column, groove_StagedLargeValue staged, bool nullable);
void jazz_node__node__MergeableCommit__drop(jazz_node_MergeableCommit *c);
typedef struct jazz_node_PublishedTransaction jazz_node_PublishedTransaction;
jazz_model_TxId jazz_node__node__PublishedTransaction__tx_id(const jazz_node_PublishedTransaction *p);
const JZ_VEC(jz_str) *jazz_node__node__PublishedTransaction__changed_tables(const jazz_node_PublishedTransaction *p);
jz_err jazz_node__node__PublishedTransaction__persist(const jazz_node_PublishedTransaction *p);
void jazz_node__node__PublishedTransaction__drop(void *p);
/* PublicationOutcome<T>: value is type-erased (pointer to caller-owned storage) */
typedef struct jazz_node_PublicationOutcome {
    JZ_VEC(jazz_node_PublishedTransaction *) publications;
    jz_deque post_settlement_work; /* VecDeque<SyncMessage> */
    void *value;
    u32 value_size;
} jazz_node_PublicationOutcome;
void jazz_node__node__PublicationOutcome__published(jazz_node_PublicationOutcome *out, void *value, u32 value_size, jazz_node_PublishedTransaction *p);
void jazz_node__node__PublicationOutcome__settled(jazz_node_PublicationOutcome *out, void *value, u32 value_size);
void jazz_node__node__PublicationOutcome__drop(jazz_node_PublicationOutcome *o);
jz_err jazz_node__node__NodeState__apply_sync_message_with_ingest_context(NS *n, jazz_protocol_SyncMessage msg, const jazz_node_CommitUnitIngestContext *ctx, jazz_node_PublicationOutcome *out);
jz_err jazz_node__node__NodeState__apply_sync_message(NS *n, jazz_protocol_SyncMessage msg, jazz_node_PublicationOutcome *out);
jz_err jazz_node__node__NodeState__seal_inherited_large_values(NS *n, jazz_node_MergeableCommit *c, jazz_types_SchemaVersionId schema, bool allow_inherited);
typedef struct jazz_db_NamedStaged { jz_str column; groove_StagedLargeValue staged; } jazz_db_NamedStaged;
jz_err jazz_node__node__NodeState__seal_large_value_updates(NS *n, jazz_node_MergeableCommit *c, const jazz_db_NamedStaged *staged, u32 n_staged, jazz_types_SchemaVersionId schema);
jz_err jazz_node__node__NodeState__commit_mergeable_many_in_schema_at(NS *n, jazz_types_SchemaVersionId schema, jazz_node_MergeableCommit **commits, u32 n_commits, jazz_model_TxId reserved, jazz_node_PublishedTransaction **out);
jz_err jazz_node__node__NodeState__commit_mergeable_many_in_schema(NS *n, jazz_types_SchemaVersionId schema, jazz_node_MergeableCommit **commits, u32 n_commits, jazz_node_PublishedTransaction **out);
jz_err jazz_node__node__NodeState__evict_staged_large_value(NS *n, groove_StagedLargeValueId id);
jz_err jazz_node__node__NodeState__read_large_value_range(NS *n, const groove_LargeValueRef *v, u64 from, u64 to, jz_bytes *out);
jz_err jazz_node__node__NodeState__large_text_utf16_offset_to_byte(NS *n, const groove_LargeValueRef *v, u64 offset, u64 *out);
jz_err jazz_node__node__NodeState__edit_and_stage_large_value(NS *n, groove_LargeValueRef v, u64 offset, u64 delete_length, jz_bytes insert, groove_StagedLargeValue *out);
jz_err jazz_node__node__NodeState__prepare_and_stage_large_scalar(NS *n, groove_Value *value, u8 semantic_kind, bool *has, groove_StagedLargeValue *out);
jz_err jazz_node__node__NodeState__tx_write_in_schema_with_staged_large_values(NS *n, jazz_model_OpenTransactionId tx, jazz_types_SchemaVersionId schema, jz_slice table, jazz_types_RowUuid row, jz_btree cells, const void *deletion, bool has_now_ms, u64 now_ms, jz_btree staged);
jz_err jazz_node__node__NodeState__tx_patch_mergeable_with_staged_large_values(NS *n, jazz_model_OpenTransactionId tx, jazz_types_SchemaVersionId schema, jz_slice table, jazz_types_RowUuid row, jz_btree cells, bool has_now_ms, u64 now_ms, jazz_protocol_BranchSelector branch, bool flag, jz_btree staged);
jz_err jazz_node__node__NodeState__begin_streaming_large_value_upload(NS *n, groove_StagedLargeValueId id, groove_LargeValueKind kind);
jz_err jazz_node__node__NodeState__evict_pending_large_value_upload(NS *n, groove_StagedLargeValueId id);
jz_err jazz_node__node__NodeState__stage_large_value_chunk_batch(NS *n, groove_StagedLargeValueId id, groove_LargeValueKind kind, JZ_VEC(groove_StagedChunk) *chunks);
jz_err jazz_node__node__NodeState__finalize_large_value_upload(NS *n, groove_StagedLargeValueId id, groove_LargeValueRef value_ref, groove_StagedLargeValue *out);
jz_err jazz_node__node__NodeState__visible_current_cells_in_branch(NS *n, jz_slice table, const jazz_protocol_BranchSelector *head, jazz_types_RowUuid row, bool *has, jz_btree *out);
jz_err jazz_node__node__NodeState__visible_current_cells_in_branch_view(NS *n, jz_slice table, const jazz_protocol_BranchSelector *head, const jazz_protocol_BranchViewBase *base, jazz_types_RowUuid row, bool *has, jz_btree *out);
jz_err jazz_node__node__NodeState__local_content_winner_tx_id_in_branch(NS *n, jz_slice table, const jazz_protocol_BranchSelector *head, jazz_types_RowUuid row, bool *has, jazz_model_TxId *out);
jz_err jazz_node__node__NodeState__local_deletion_winner_tx_id_in_branch(NS *n, jz_slice table, const jazz_protocol_BranchSelector *head, jazz_types_RowUuid row, bool *has, jazz_model_TxId *out);
jz_err jazz_node__node__NodeState__local_content_winner_tx_id_in_schema(NS *n, jazz_types_SchemaVersionId s, jz_slice table, jazz_types_RowUuid row, bool *has, jazz_model_TxId *out);
jz_err jazz_node__node__NodeState__local_deletion_winner_tx_id_in_schema(NS *n, jazz_types_SchemaVersionId s, jz_slice table, jazz_types_RowUuid row, bool *has, jazz_model_TxId *out);
jz_err jazz_node__node__NodeState__local_current_row(NS *n, jz_slice table, jazz_types_RowUuid row, bool *has, jazz_node_CurrentRow *out);
jz_err jazz_node__node__NodeState__local_current_row_in_schema(NS *n, jz_slice table, jazz_types_RowUuid row, jazz_types_SchemaVersionId s, bool *has, jazz_node_CurrentRow *out);
bool jazz_node__node__NodeState__current_row_tx_id(NS *n, const jazz_node_CurrentRow *r, jazz_model_TxId *out);
jz_err jazz_node__node__NodeState__current_physical_cells_in_schema(NS *n, jazz_types_SchemaVersionId s, jz_slice table, jazz_types_RowUuid row, bool *has, jz_btree *out);
jz_err jazz_node__node__NodeState__current_physical_cells_and_winner_in_schema(NS *n, jazz_types_SchemaVersionId s, jz_slice table, jazz_types_RowUuid row, bool *has, jz_btree *out, jazz_model_TxId *winner);
jz_err jazz_node__node__NodeState__query_rows_for_client_physical_row(NS *n, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding, jazz_model_DurabilityTier tier, jazz_types_AuthorSubject identity, jazz_types_RowUuid row, jazz_node_CurrentRowVec *out);
jz_err jazz_node__node__NodeState__query_rows_for_link_physical_row(NS *n, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding, jazz_model_DurabilityTier tier, jazz_types_AuthorSubject identity, jazz_types_RowUuid row, jazz_node_CurrentRowVec *out);
jz_err jazz_node__node__NodeState__dry_run_read_current_allows(NS *n, jz_slice table, jazz_types_RowUuid row, jazz_types_AuthorSubject author, bool *allowed);
jazz_model_DurabilityTier jazz_node__node__NodeState__authored_commit_durability(NS *n);
/* peer_connection.rs node API */
jz_err jazz_node__node__NodeState__require_staged_large_values_for_versions(NS *n, const jazz_protocol_VersionVec *versions);
jz_err jazz_node__node__NodeState__ingest_relay_commit_unit_with_encoder_trust(NS *n, jazz_protocol_Transaction *tx, jazz_protocol_VersionVec versions, bool trusted);
jz_err jazz_node__node__NodeState__known_untrusted_commit_unit_matches(NS *n, const jazz_protocol_Transaction *tx, const jazz_protocol_VersionVec *versions, bool *out);
jz_err jazz_node__node__NodeState__commit_unit_for(NS *n, jazz_model_TxId tx, jazz_protocol_SyncMessage *out);
jz_err jazz_node__node__NodeState__local_chunk(NS *n, const u8 locator[32], const u8 object_hash[32], jz_bytes *out);
jz_err jazz_node__node__NodeState__known_state_declaration_for_subscription(NS *n, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding, jazz_protocol_SubscriptionKey sub, const jazz_model_ValueVec *values, jazz_types_AuthorSubject identity, const void *policy_binding, void **out);
#undef NS

/* ======================= jazz_node::peer (PeerState) ======================= */
typedef struct jazz_node_PeerState jazz_node_PeerState;
typedef enum { JAZZ_PEER_ROLE_CLIENT_LINK = 0, JAZZ_PEER_ROLE_RELAY = 1 } jazz_node_PeerRoleTag;
typedef struct jazz_node_PeerRole { u32 tag; jazz_types_AuthorSubject identity; } jazz_node_PeerRole;
jazz_node_PeerRole jazz_node__peer__PeerState__role(const jazz_node_PeerState *p);
jazz_node_PeerState *jazz_node__peer__PeerState__client_link(jazz_types_AuthorSubject identity);
jazz_node_PeerState *jazz_node__peer__PeerState__relay(void);
void jazz_node__peer__PeerState__drop(jazz_node_PeerState *p);
void jazz_node__peer__PeerState__reset_catalogue_snapshot_announcement(jazz_node_PeerState *p);
bool jazz_node__peer__PeerState__admits_relay_binding(const jazz_node_PeerState *p, const jazz_types_AuthorSubject *identity, const jz_btree *claims);
jz_err jazz_node__peer__PeerState__prove_terminal_commit_authorization(jazz_node_PeerState *p, jazz_node_NodeState *n, jazz_types_AuthorSubject subject, jz_btree claims, const jazz_protocol_VersionVec *versions, jazz_model_TxId tx, bool *out);
typedef struct jazz_node_ScopeRelayBinding { jazz_types_AuthorSubject identity; jz_btree claims; } jazz_node_ScopeRelayBinding;
const jazz_node_ScopeRelayBinding *jazz_node__peer__PeerState__admitted_scope_relay_binding(const jazz_node_PeerState *p);
void jazz_node__peer__PeerState__advance_authorization_progress(jazz_node_PeerState *p);
void jazz_node__peer__PeerState__forget_subscription_with_node(jazz_node_PeerState *p, jazz_node_NodeState *n, jazz_protocol_SubscriptionKey s);
void jazz_node__peer__PeerState__note_claim_refresh_full_diff(jazz_node_PeerState *p, jazz_protocol_SubscriptionKey s);
u64 jazz_node__peer__PeerState__authorization_progress_for_subscription(const jazz_node_PeerState *p, jazz_protocol_SubscriptionKey s);
void jazz_node__peer__PeerState__set_subscription_policy_binding(jazz_node_PeerState *p, jazz_protocol_SubscriptionKey s, jazz_types_AuthorSubject identity, jz_btree claims);
void jazz_node__peer__PeerState__retain_authorization_progress_for_subscription(jazz_node_PeerState *p, jazz_protocol_SubscriptionKey s, u64 progress);
void jazz_node__peer__PeerState__set_subscription_authority_result_source(jazz_node_PeerState *p, jazz_protocol_SubscriptionKey s, jazz_protocol_AuthorityResultKey key);
void jazz_node__peer__PeerState__set_subscription_awaiting_selected_authority_source(jazz_node_PeerState *p, jazz_protocol_SubscriptionKey s, bool awaiting);
jz_err jazz_node__peer__PeerState__rehydrate_query_for_subscription_with_opts_and_waker(jazz_node_PeerState *p, jazz_node_NodeState *n, jazz_protocol_SubscriptionKey s, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding, jazz_protocol_RegisterShapeOptions opts, const jz_waker *w, bool *has, jazz_protocol_SyncMessage *out);
typedef struct jazz_node_FullDiffFallbackMetrics { u64 count; u64 rows; } jazz_node_FullDiffFallbackMetrics;
jazz_node_FullDiffFallbackMetrics jazz_node__peer__PeerState__full_diff_fallbacks(const jazz_node_PeerState *p);

/* ======================= jazz_db (other units) ======================= */
typedef enum { JAZZ_TICK_IMMEDIATE = 0, JAZZ_TICK_DEFERRED = 1, JAZZ_TICK_AFTER_CURRENT_TURN = 2 } jazz_db_TickUrgency;
/* dyn TickScheduler */
typedef struct jazz_db_TickSchedulerVt {
    void (*schedule_tick)(void *self, jazz_db_TickUrgency u);
    void (*schedule_tick_after)(void *self, u64 delay_ms);
    bool (*query_runtime_waker)(void *self, jz_waker *out);
    bool (*drops_pending_ticks)(void *self);
} jazz_db_TickSchedulerVt;
typedef struct jazz_db_TickScheduler { void *self; const jazz_db_TickSchedulerVt *vt; } jazz_db_TickScheduler;
/* SharedTickScheduler = Rc<RefCell<Option<Rc<dyn TickScheduler>>>> */
typedef struct jazz_db_SharedTickScheduler { bool has; jazz_db_TickScheduler s; } jazz_db_SharedTickScheduler;
/* dyn UploadRetryClock */
typedef struct jazz_db_UploadRetryClock { void *self; u64 (*now_ms)(void *self); } jazz_db_UploadRetryClock;
jazz_db_UploadRetryClock jazz_db__db__MonotonicUploadRetryClock__new(void);
/* DbTickStats */
typedef struct jazz_db_DbTickStats { u32 subscription_events; u32 remote_sync_applied; } jazz_db_DbTickStats;

/* Boxed local futures: poll returns true when ready, writing its output to `out`. */
typedef struct jazz_db_LocalFuture {
    bool (*poll)(void *self, const jz_waker *cx, void *out);
    void (*drop)(void *self);
    void *self;
} jazz_db_LocalFuture;
static inline bool jazz_db_future_poll(jazz_db_LocalFuture *f, const jz_waker *cx, void *out) { return f->poll(f->self, cx, out); }
static inline void jazz_db_future_drop(jazz_db_LocalFuture *f) { if (f->drop && f->self) f->drop(f->self); f->self = NULL; }
/* futures::channel::oneshot */
typedef struct jazz_db_Oneshot { u32 refs; bool sent; bool receiver_dropped; bool sender_dropped; u32 elem; void *value; jz_waker waker; } jazz_db_Oneshot;
jazz_db_Oneshot *jazz_db__oneshot__channel(u32 elem);
bool jazz_db__oneshot__send(jazz_db_Oneshot *c, const void *value); /* false when the receiver is gone */
bool jazz_db__oneshot__try_recv(jazz_db_Oneshot *c, void *out);
bool jazz_db__oneshot__is_canceled(const jazz_db_Oneshot *c);
void jazz_db__oneshot__drop_sender(jazz_db_Oneshot *c);
void jazz_db__oneshot__drop_receiver(jazz_db_Oneshot *c);

/* MutationOwnerLifecycle */
typedef enum { JAZZ_OWNER_OPEN = 0, JAZZ_OWNER_CLOSING = 1 } jazz_db_MutationOwnerLifecycle;
/* QueuedMutationStatus */
typedef enum { JAZZ_QMS_PENDING = 0, JAZZ_QMS_PUBLISHED = 1, JAZZ_QMS_FAILED = 2 } jazz_db_QueuedMutationStatusTag;
typedef struct jazz_db_QueuedMutationStatus { u32 tag; jz_err error; } jazz_db_QueuedMutationStatus;
void jazz_db__db__QueuedMutationStatus__drop(void *p);
/* QueuedMutationAlias = Rc<RefCell<Option<TxId>>> */
typedef struct jazz_db_QueuedMutationAlias { bool has; jazz_model_TxId tx_id; } jazz_db_QueuedMutationAlias;
/* QueuedMutationCompletion = Box<dyn FnOnce(Result<(), Error>)> */
typedef struct jazz_db_QueuedMutationCompletion { void (*call)(void *ctx, jz_err result); void *ctx; void (*drop)(void *ctx); } jazz_db_QueuedMutationCompletion;
typedef struct jazz_db_QueuedMutationOperation {
    bool has_transaction;
    jazz_model_TxId transaction_tx_id;
    u8 transaction_kind;
    bool has_open_tx_id;
    jazz_model_OpenTransactionId open_tx_id;
    jazz_db_LocalFuture future; /* output: jz_err */
    jazz_db_QueuedMutationStatus *status; /* Option<Rc<RefCell<..>>> */
    bool has_completion;
    jazz_db_QueuedMutationCompletion completion;
} jazz_db_QueuedMutationOperation;
void jazz_db__db__QueuedMutationOperation__drop(void *p);

/* WriteStateWaiters = Rc<RefCell<BTreeMap<TxId, Vec<WriteStateWaiter>>>> */
typedef struct jazz_db_WriteStateWaiter { u64 id; jazz_db_Oneshot *notify; } jazz_db_WriteStateWaiter;
typedef JZ_VEC(jazz_db_WriteStateWaiter) jazz_db_WriteStateWaiterVec;
extern const jz_btree_desc jazz_db_write_state_waiters_desc;
typedef struct jazz_db_WriteStateChange { jz_btree *waiters; jazz_model_TxId tx_id; u64 waiter_id; jazz_db_Oneshot *receiver; } jazz_db_WriteStateChange;
void jazz_db__db__WriteStateChange__drop(jazz_db_WriteStateChange *c);
void jazz_db__db__WriteStateChange__await(jazz_db_WriteStateChange *c);
typedef struct jazz_db_WriteWaitOptions { u8 tier; bool observe_only; } jazz_db_WriteWaitOptions;
typedef struct jazz_db_WriteState { jazz_model_Fate fate; bool has_global_time; jazz_types_GlobalTime global_time; u8 durability; } jazz_db_WriteState;
bool jazz_db__db__transaction_satisfies_wait(const jazz_model_Fate *fate, bool has_gt, jazz_types_GlobalTime gt, jazz_model_DurabilityTier durability, jazz_model_DurabilityTier tier);
jz_err jazz_db__db__write_rejected(jazz_model_TxId tx, const jazz_model_RejectionReason *reason);
jz_err jazz_db__db__transaction_abandoned(jazz_model_OpenTransactionId id);
jz_err jazz_db__db__row_already_deleted(jazz_types_RowUuid row);
jz_err jazz_db__db__read_for_write_denied(const char *operation, jz_slice table);
jz_err jazz_db__db__update_target_not_loaded(const char *operation, jz_slice table, jazz_types_RowUuid row);
jz_err jazz_db__db__pending_open_schema_error(void);
groove_Value jazz_db__db__default_cell_for_column_type(const groove_ColumnType *t, const groove_Value *def);

/* PendingOpenSchema / OpenSchemaAdmission = Rc<RefCell<Option<PendingOpenSchema>>> */
typedef struct jazz_db_SharedResultFuture jazz_db_SharedResultFuture;
typedef struct jazz_db_PendingOpenSchema {
    bool has_connection_epoch;
    u64 connection_epoch;
    bool authoritative_catalogue_received;
    jazz_types_SchemaVersionId schema;
    bool has_result;
    jz_err result;
    jazz_db_Oneshot *sender;
    jazz_db_SharedResultFuture *wait;
} jazz_db_PendingOpenSchema;
typedef struct jazz_db_OpenSchemaAdmission { bool has; jazz_db_PendingOpenSchema pending; } jazz_db_OpenSchemaAdmission;
void jazz_db__db__finish_open_schema_admission(jazz_db_OpenSchemaAdmission *state, jz_err result);
void jazz_db__db__finish_open_schema_connection(jazz_db_OpenSchemaAdmission *state, u64 connection_epoch, jz_err result);
void jazz_db__db__begin_open_schema_connection(jazz_db_OpenSchemaAdmission *state, u64 connection_epoch);
jz_err jazz_db__db__SharedResultFuture__await(jazz_db_SharedResultFuture *f);
jazz_db_SharedResultFuture *jazz_db__db__SharedResultFuture__clone(jazz_db_SharedResultFuture *f);

/* UploadOutbox (db.rs) */
typedef struct jazz_db_PendingUpload { jazz_model_TxId tx_id; bool has_unit; jazz_protocol_SyncMessage unit; } jazz_db_PendingUpload;
typedef struct jazz_db_UploadOutbox jazz_db_UploadOutbox;
void jazz_db__db__UploadOutbox__mark_upstream_attached(jazz_db_UploadOutbox *o);
void jazz_db__db__UploadOutbox__declare_root(jazz_db_UploadOutbox *o);
void jazz_db__db__UploadOutbox__forget_on_wire(jazz_db_UploadOutbox *o);
void jazz_db__db__UploadOutbox__mark_on_wire(jazz_db_UploadOutbox *o, jazz_model_TxId tx);
void jazz_db__db__UploadOutbox__note_upload_progress(jazz_db_UploadOutbox *o);
u32 jazz_db__db__UploadOutbox__len(const jazz_db_UploadOutbox *o);
const jazz_db_PendingUpload *jazz_db__db__UploadOutbox__at(const jazz_db_UploadOutbox *o, u32 i);
void jazz_db__db__UploadOutbox__remove_released(jazz_db_UploadOutbox *o, jz_hmap *remaining, jz_hmap *out_released);
bool jazz_db__db__UploadOutbox__settles_uploads_locally(const jazz_db_UploadOutbox *o);
jazz_db_UploadOutbox *jazz_db__db__UploadOutbox__default(void);
bool jazz_db__db__queue_pending_upload_in(jazz_db_UploadOutbox *outbox, jazz_model_TxId tx, bool has_unit, jazz_protocol_SyncMessage unit);

/* PendingUpstreamSubscription / PendingUpstreamCommand (db.rs) */
typedef struct jazz_db_PolicyBinding { jazz_types_AuthorSubject identity; jz_btree claims; } jazz_db_PolicyBinding; /* (AuthorSubject, BTreeMap<String, Value>) */
void jazz_db__db__PolicyBinding__clone(jazz_db_PolicyBinding *dst, const jazz_db_PolicyBinding *src);
void jazz_db__db__PolicyBinding__drop(jazz_db_PolicyBinding *p);
bool jazz_db__db__PolicyBinding__eq(const jazz_db_PolicyBinding *a, const jazz_db_PolicyBinding *b);
typedef struct jazz_db_PendingUpstreamSubscription {
    jazz_protocol_SubscriptionKey subscription;
    jazz_model_ValidatedQuery *shape;
    jazz_model_Binding *binding;
    jazz_protocol_RegisterShapeOptions opts;
    jazz_types_AuthorSubject identity;
    bool has_policy_binding;
    jazz_db_PolicyBinding policy_binding;
} jazz_db_PendingUpstreamSubscription;
void jazz_db__db__PendingUpstreamSubscription__clone(void *dst, const void *src);
void jazz_db__db__PendingUpstreamSubscription__drop(void *p);
typedef enum { JAZZ_PUC_SUBSCRIBE = 0, JAZZ_PUC_UNSUBSCRIBE = 1, JAZZ_PUC_AUTHORIZATION_SCOPE_INTENT = 2 } jazz_db_PendingUpstreamCommandTag;
typedef struct jazz_db_PendingUpstreamCommand {
    u32 tag;
    jazz_db_PendingUpstreamSubscription subscribe;
    jazz_protocol_SubscriptionKey unsubscribe;
    struct {
        jazz_protocol_PermissionAdviceRequestId request_id;
        jazz_protocol_PermissionAdviceAction *action;
        bool has_session_claim_binding;
        jazz_db_PolicyBinding session_claim_binding;
        bool has_delegated_session;
        jazz_protocol_DelegatedSessionBinding delegated_session;
    } intent;
} jazz_db_PendingUpstreamCommand;
void jazz_db__db__PendingUpstreamCommand__clone(void *dst, const void *src);
void jazz_db__db__PendingUpstreamCommand__drop(void *p);
typedef JZ_VEC(jazz_db_PendingUpstreamCommand) jazz_db_PendingUpstreamCommands;

typedef struct jazz_db_QueryCoverageRegistration { jazz_db_CoverageKey coverage; jazz_db_PendingUpstreamSubscription subscription; u32 ref_count; } jazz_db_QueryCoverageRegistration;
typedef struct jazz_db_UpstreamCoverageHandle { jazz_db_CoverageKey coverage; jazz_protocol_SubscriptionKey subscription; } jazz_db_UpstreamCoverageHandle;
void jazz_db__db__UpstreamCoverageHandle__clone(void *dst, const void *src);
void jazz_db__db__UpstreamCoverageHandle__drop(void *p);
typedef JZ_VEC(jazz_db_UpstreamCoverageHandle) jazz_db_UpstreamCoverageHandleVec;

/* Scalar / deletion reconciliation (db.rs) */
typedef struct jazz_db_CurrentRowsResult { u32 tag; jazz_protocol_CurrentRowsReceipt *receipt; } jazz_db_CurrentRowsResult; /* 0 = Applied, 1 = Unknown */
enum { JAZZ_CRR_APPLIED = 0, JAZZ_CRR_UNKNOWN = 1 };
void jazz_db__row_availability__CurrentRowsResult__drop(jazz_db_CurrentRowsResult *r);
typedef struct jazz_db_ScalarProbe { jazz_db_Instant deadline; JZ_VEC(jazz_types_RowUuid) rows; jazz_db_LocalFuture future; } jazz_db_ScalarProbe;
typedef struct jazz_db_ScalarGeneration { u64 epoch; jazz_protocol_SubscriptionKey subscription; u64 revision; } jazz_db_ScalarGeneration;
typedef struct jazz_db_ScalarReconciliation {
    bool has_generation;
    jazz_db_ScalarGeneration generation;
    jz_deque pending; /* VecDeque<RowUuid> */
    bool has_active;
    jazz_db_ScalarProbe active;
    bool has_retry_at;
    jazz_db_Instant retry_at;
    u64 retry_delay_ms;
} jazz_db_ScalarReconciliation;
void jazz_db__db__ScalarReconciliation__default(jazz_db_ScalarReconciliation *out);
void jazz_db__db__ScalarReconciliation__drop(jazz_db_ScalarReconciliation *s);
#define JAZZ_DELETION_DISCOVERY_LIMIT_MS 5000u
typedef struct jazz_db_DeletionDiscoverySpec {
    jazz_model_ValidatedQuery *shape;
    jazz_model_Binding *binding;
    jazz_types_AuthorSubject author;
    jz_btree claims;
    jazz_protocol_PolicyBindingKey context;
    jz_btree authoritative; /* BTreeSet<RowUuid> */
} jazz_db_DeletionDiscoverySpec;
void jazz_db__db__DeletionDiscoverySpec__drop(jazz_db_DeletionDiscoverySpec *s);
typedef enum {
    JAZZ_DR_DISABLED = 0, JAZZ_DR_UNCHECKED, JAZZ_DR_DEFERRED, JAZZ_DR_DISCOVERING, JAZZ_DR_PENDING, JAZZ_DR_PROBING, JAZZ_DR_DONE
} jazz_db_DeletionReconciliationTag;
typedef struct jazz_db_HeldRowKey { jz_str table; jazz_types_RowUuid row; } jazz_db_HeldRowKey;
typedef struct jazz_db_DeletionReconciliation {
    u32 tag;
    jazz_db_Instant since;
    jazz_db_DeletionDiscoverySpec *spec;                 /* Deferred / Discovering */
    jazz_node_LocalMaintainedViewSubscription *maintained; /* Discovering */
    u64 runtime_token;                                   /* Discovering */
    JZ_VEC(jazz_protocol_CurrentRowCoordinate) rows;     /* Pending */
    jazz_protocol_PolicyBindingKey context;              /* Pending */
    JZ_VEC(jazz_db_HeldRowKey) probing_rows;             /* Probing */
} jazz_db_DeletionReconciliation;
void jazz_db__db__DeletionReconciliation__drop(void *p);
bool jazz_db__db__DeletionReconciliation__waiting_since(const jazz_db_DeletionReconciliation *d, jazz_db_Instant *out);
typedef struct jazz_db_HeldRowCheck { jazz_protocol_CurrentRowCoordinate coordinate; jazz_protocol_PolicyBindingKey context; u32 attempts; bool has_retry_at; jazz_db_Instant retry_at; } jazz_db_HeldRowCheck;
typedef struct jazz_db_HeldRowProbe { JZ_VEC(jazz_db_HeldRowKey) rows; jazz_db_Instant deadline; jazz_db_LocalFuture future; } jazz_db_HeldRowProbe;
typedef struct jazz_db_HeldRowChecks {
    jz_btree rows; /* (String, RowUuid) -> HeldRowCheck */
    bool has_active;
    jazz_db_HeldRowProbe active;
    JZ_VEC(jazz_db_DeletionReconciliation) discoveries;
    bool has_upstream_connection;
    u64 upstream_connection;
} jazz_db_HeldRowChecks;
void jazz_db__db__HeldRowChecks__default(jazz_db_HeldRowChecks *out);
void jazz_db__db__HeldRowChecks__enqueue(jazz_db_HeldRowChecks *c, void *rows /* Vec<CurrentRowCoordinate>, moved */, const jazz_protocol_PolicyBindingKey *context, void *out_keys /* Vec<(String,RowUuid)> */);
bool jazz_db__db__HeldRowChecks__adopt_discovery(jazz_db_HeldRowChecks *c, jazz_db_DeletionReconciliation reconciliation, jazz_db_DeletionReconciliation *out_redundant);
u64 jazz_db__db__HeldRowChecks__retry_delay(u32 attempts);
void jazz_db__db__HeldRowKey__clone(void *dst, const void *src);
void jazz_db__db__HeldRowKey__drop(void *p);
int jazz_db__db__HeldRowKey__cmp(const void *a, const void *b, void *ctx);
extern const jz_btree_desc jazz_db_held_row_key_set_desc; /* BTreeSet<(String, RowUuid)> */

/* SubscriptionState (db.rs) */
typedef enum { JAZZ_SNAP_LOCAL_MAINTAINED = 0, JAZZ_SNAP_LINK_SNAPSHOT = 1 } jazz_db_SubscriptionSnapshotSource;
typedef struct jazz_db_SubscriptionSender jazz_db_SubscriptionSender;
typedef struct jazz_db_PublicationCheckpoint { u64 a; u64 b; } jazz_db_PublicationCheckpoint;
typedef struct jazz_db_SubscriptionEvent jazz_db_SubscriptionEvent;
typedef struct jazz_db_LocalCleanupCell { bool has; u64 token; groove_SubscriptionId id; } jazz_db_LocalCleanupCell; /* Rc<Cell<Option<(u64, SubscriptionId)>>> */
typedef struct jazz_db_SubscriptionState {
    bool *closed; /* Rc<Cell<bool>> */
    bool terminal_rows;
    jazz_db_DeletionReconciliation deletion_reconciliation;
    bool scalar_reconciliation_enabled;
    u64 scalar_authority_revision;
    jazz_db_ScalarReconciliation scalar_reconciliation;
    /* kind: SubscriptionKind::Prepared { shape, binding, maintained_subscription } */
    jazz_model_ValidatedQuery *shape;
    jazz_model_Binding *binding;
    jazz_node_LocalMaintainedViewSubscription *maintained_subscription;
    u64 groove_runtime_token;
    jazz_db_LocalCleanupCell *local_subscription_cleanup;
    jazz_db_UpstreamCoverageHandleVec upstream_subscription_handles;
    bool propagates_upstream;
    bool has_request_identity_claims;
    jazz_db_PolicyBinding request_identity_claims;
    jazz_types_AuthorSubject author;
    u8 authorization_mode;
    u8 read_tier;
    bool pending_overlay;
    bool has_remote_read_tier;
    u8 remote_read_tier;
    bool requires_authority_receipt;
    jazz_protocol_ReadViewSpec *read_view;
    jazz_node_RelationSnapshot snapshot;
    jazz_node_RelationSnapshotIndex snapshot_index;
    u8 snapshot_source;
    bool settled;
    bool pending_initial_local_snapshot;
    bool pending_initial_owner_result;
    jazz_db_UpstreamCoverageHandleVec authority_witness;
    jazz_db_SubscriptionSender *sender;
} jazz_db_SubscriptionState;
/* SubscriptionList = Rc<RefCell<Vec<Weak<RefCell<SubscriptionState>>>>> */
typedef JZ_VEC(jazz_db_SubscriptionState *) jazz_db_SubscriptionList; /* entries are weak Rc payload ptrs */
static inline jazz_db_SubscriptionState *jazz_db_weak_upgrade(jazz_db_SubscriptionState *w) {
    if (!w) return NULL;
    jz_rc_hdr *h = (jz_rc_hdr *)w - 1;
    if (h->strong == 0) return NULL;
    return (jazz_db_SubscriptionState *)jz_rc_clone(w);
}
static inline u32 jazz_db_weak_strong_count(jazz_db_SubscriptionState *w) { return w ? ((jz_rc_hdr *)w - 1)->strong : 0; }
static inline jazz_db_SubscriptionState *jazz_db_rc_downgrade(jazz_db_SubscriptionState *s) { ((jz_rc_hdr *)s - 1)->weak++; return s; }
static inline void jazz_db_weak_release(jazz_db_SubscriptionState *w) { if (w) ((jz_rc_hdr *)w - 1)->weak--; }
jz_err jazz_db__first_load__SubscriptionState__send_rejection(const jazz_db_SubscriptionState *s, jazz_protocol_SubscribeRejectReason reason);
jazz_db_SubscriptionSender *jazz_db__db__SubscriptionSender__clone(const jazz_db_SubscriptionSender *s);
void jazz_db__db__SubscriptionSender__drop(jazz_db_SubscriptionSender *s);
jz_err jazz_db__db__SubscriptionSender__checkpoint(jazz_db_SubscriptionSender *s, u8 tier, bool settled, const jazz_node_RelationSnapshot *snap, const jazz_node_RelationSnapshotIndex *idx, jazz_db_PublicationCheckpoint *out);
jz_err jazz_db__db__SubscriptionSender__materialized(jazz_db_SubscriptionSender *s, jazz_node_NodeState *n, const jazz_model_ValidatedQuery *shape, const jazz_db_SubscriptionEvent *event, void **out);
jz_err jazz_db__db__SubscriptionSender__publish(jazz_db_SubscriptionSender *s, jazz_db_SubscriptionEvent *event, jazz_db_PublicationCheckpoint before, const jazz_node_RelationSnapshot *snap, const jazz_node_RelationSnapshotIndex *idx, void *materialized, bool *out);
/* SubscriptionEvent::Delta field access */
typedef struct jazz_db_DeltaFields { bool *reset; bool *publishable; void *added; void *updated; void *removed; bool *settled; } jazz_db_DeltaFields;
bool jazz_db__db__SubscriptionEvent__as_delta(jazz_db_SubscriptionEvent *e, jazz_db_DeltaFields *out);
bool jazz_db__db__OutputVec__is_empty(const void *v);
void jazz_db__db__OutputVec__clear(void *v);
void jazz_db__db__OutputVec__assign(void *dst, void *src_moved);
void jazz_db__db__SubscriptionEvent__drop(jazz_db_SubscriptionEvent *e);
jazz_db_SubscriptionEvent *jazz_db__db__subscription_delta_event_with_reset(u8 tier, bool settled, const jazz_node_RelationSnapshot *prev, const jazz_node_RelationSnapshot *next, bool reset, bool terminal_rows);
jazz_db_SubscriptionEvent *jazz_db__db__subscription_delta_event(u8 tier, bool settled, const jazz_node_RelationSnapshot *prev, const jazz_node_RelationSnapshot *next, bool terminal_rows);
jz_err jazz_db__db__subscription_outputs_with_occurrence_sidecar(const jazz_node_RelationSnapshot *s, const jazz_db_OutputOccurrenceVec *roots, void **out);
void *jazz_db__db__reset_removed_roots(const jazz_node_RelationSnapshot *prev, const jazz_node_RelationSnapshotIndex *prev_idx, const jazz_db_OutputOccurrenceVec *roots);
void jazz_db__db__relation_snapshot_with_delta_slack(jazz_node_RelationSnapshot *out, const jazz_node_RelationSnapshot *s);
jz_err jazz_db__db__materialized_subscription_snapshot(const jazz_node_RelationSnapshot *s, const jazz_node_RelationSnapshotIndex *idx, jazz_node_RelationSnapshot *out);
jz_err jazz_db__db__relation_snapshot_index_with_root_occurrences(const jazz_node_RelationSnapshot *s, const jazz_db_OutputOccurrenceVec *roots, jazz_node_RelationSnapshotIndex *out);
jz_err jazz_db__db__maintained_snapshot_index_or_row_index(jazz_node_NodeState *n, const jazz_db_SubscriptionState *kind, const jazz_node_RelationSnapshot *s, jazz_node_RelationSnapshotIndex *out);
jz_err jazz_db__db__apply_maintained_update_to_snapshot(jazz_node_RelationSnapshot *s, jazz_node_RelationSnapshotIndex *idx, jazz_node_LocalMaintainedUpdate update, jz_slice table, u8 tier, bool previous_settled, const void *layout, jazz_db_SubscriptionEvent **out);
jz_err jazz_db__db__apply_terminal_operations_to_subscription_snapshot(jazz_node_RelationSnapshot *s, jazz_node_RelationSnapshotIndex *idx, void *ops, const void *window, const void *layout, jz_slice table, u8 tier, bool settled, jazz_db_SubscriptionEvent **out);
jz_err jazz_db__db__materialize_subscription_terminal_records(jazz_node_RelationSnapshot *s, const jazz_node_RelationSnapshotIndex *idx);
typedef struct jazz_db_ActiveAuthorityViewReceipts jazz_db_ActiveAuthorityViewReceipts;
bool jazz_db__db__subscription_is_settled(jazz_node_NodeState *n, const jazz_db_ActiveAuthorityViewReceipts *receipts, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding, u8 tier, const jazz_protocol_ReadViewSpec *view, bool requires_receipt, const jazz_protocol_AuthorityResultKey *settled_key);
void jazz_db__db__demote_authority_receipt_subscriptions(jazz_db_SubscriptionList *subscriptions, const jz_btree *keep);
jz_err jazz_db__db__restore_local_subscriber_replay(jazz_db_NodeMutex *node, jazz_db_UploadOutbox *outbox, jz_btree *local_fate_routes, jazz_types_AuthorSubject author, void *downstream_fates);
void jazz_db__db__upstream_register_shape_options(jazz_protocol_RegisterShapeOptions *out, jazz_model_DurabilityTier tier, jazz_protocol_ReadViewSpec *view, jazz_model_DurabilityTier floor);
void jazz_db__db__request_coverage_key(jazz_db_CoverageKey *out, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding, jazz_protocol_RegisterShapeOptions opts, const jazz_db_PolicyBinding *claims /* NULL = None */);

/* AuthorityViewReceipts */
struct jazz_db_ActiveAuthorityViewReceipts {
    bool has;
    u64 connection_epoch;
    jazz_types_GlobalTime confirmation_floor;
    jz_btree subscriptions;  /* BTreeSet<SubscriptionKey> */
    jz_btree binding_views;  /* BTreeSet<BindingViewKey> */
};
extern const jz_btree_desc jazz_db_subscription_key_set_desc;
extern const jz_btree_desc jazz_db_binding_view_key_set_desc;
extern const jz_btree_desc jazz_db_txid_set_desc;
extern const jz_btree_desc jazz_db_row_uuid_set_desc; /* BTreeSet<RowUuid> */
extern const jz_btree_desc jazz_db_txid_err_desc;
extern const jz_btree_desc jazz_db_open_tx_set_desc;
extern const jz_btree_desc jazz_db_open_tx_err_desc;
extern const jz_hmap_desc jazz_db_txid_hset_desc;
extern const jz_hmap_desc jazz_db_str_hset_desc;

/* StagedInboundMessage */
typedef struct jazz_db_BufferLease jazz_db_BufferLease;
jazz_db_BufferLease *jazz_db__wire__BufferLease__clone(jazz_db_BufferLease *l);
void jazz_db__wire__BufferLease__drop(jazz_db_BufferLease *l);
typedef struct jazz_db_StagedInboundMessage {
    jazz_protocol_SyncMessage message;
    jazz_db_BufferLease *lease;
    bool authority_receipt_eligible;
    bool receipts_validated;
} jazz_db_StagedInboundMessage;
void jazz_db__db__StagedInboundMessage__drop(void *p);
/* ReceivedSyncMessage (routed_messages.rs) */
typedef struct jazz_db_ReceivedSyncMessage { jazz_protocol_SyncMessage message; jazz_db_BufferLease *lease; bool receipts_validated; } jazz_db_ReceivedSyncMessage;
void jazz_db__db__routed_messages__ReceivedSyncMessage__unleased(jazz_db_ReceivedSyncMessage *out, jazz_protocol_SyncMessage m);

/* PendingLocalPublication */
typedef struct jazz_db_PendingLocalPublication {
    jazz_node_PublishedTransaction *published; /* Rc */
    bool has_upload_unit;
    jazz_protocol_SyncMessage upload_unit;
    jazz_db_LocalFuture settlement; /* output: jz_err + TxId (jazz_db_TxIdResult) */
} jazz_db_PendingLocalPublication;
typedef struct jazz_db_TxIdResult { jz_err err; jazz_model_TxId tx_id; } jazz_db_TxIdResult;

/* LocalFateRoutes = Rc<RefCell<BTreeMap<TxId, Vec<LocalFateRoute>>>> */
typedef struct jazz_db_LocalFateRoute {
    void *queue; /* Weak<RefCell<Vec<SyncMessage>>> */
    bool local_acknowledged;
    bool replay_ready;
    bool has_replay_author;
    jazz_types_AuthorSubject replay_author;
    void *replay_unit;
    void *held_fate;
} jazz_db_LocalFateRoute;
typedef JZ_VEC(jazz_db_LocalFateRoute) jazz_db_LocalFateRouteVec;
/* PendingDownstreamFates = Rc<RefCell<Vec<SyncMessage>>> */
typedef JZ_VEC(jazz_protocol_SyncMessage) jazz_db_SyncMessageVec;
void jazz_db__db__register_local_fate_route(jz_btree *routes, jazz_model_TxId tx, jazz_db_SyncMessageVec *downstream_fates);
bool jazz_db__db__local_fate_is_terminal(const jazz_protocol_SyncMessage *fate);
void jazz_db__db__route_local_fate(jz_btree *routes, jazz_model_TxId tx, const jazz_protocol_SyncMessage *fate);
void jazz_db__db__release_local_replay_fates(jz_btree *routes);

/* MutationErrorState (db.rs) / mutation_errors.rs */
typedef struct jazz_db_MutationErrorEvent jazz_db_MutationErrorEvent;
typedef struct jazz_db_MutationErrorCallback { void (*call)(void *ctx, const jazz_db_MutationErrorEvent *e); void *ctx; } jazz_db_MutationErrorCallback;
typedef struct jazz_db_MutationErrorState {
    bool has_callback;
    jazz_db_MutationErrorCallback callback;
    jz_btree pending; /* TxId -> MutationErrorEvent */
    jz_btree application_waiters;
} jazz_db_MutationErrorState;
extern const jz_btree_desc jazz_db_mutation_error_event_desc;
jazz_db_MutationErrorEvent *jazz_db__mutation_errors__mutation_error_event(const void *rejected);
jazz_db_MutationErrorEvent *jazz_db__mutation_errors__mutation_error_event_for(jazz_model_TxId tx, const jazz_model_RejectionReason *reason);
jazz_db_MutationErrorEvent *jazz_db__mutation_errors__queued_mutation_error_event(jazz_model_TxId tx, jazz_model_TxKind kind, const jz_err *error);
void jazz_db__mutation_errors__queue_mutation_error(jazz_db_MutationErrorState *s, jazz_db_SharedTickScheduler *sched, jazz_model_TxId tx, jazz_db_MutationErrorEvent *event);
typedef struct jazz_db_MutationErrorWait { jazz_db_MutationErrorState *errors; jazz_db_SharedTickScheduler *scheduler; jazz_model_TxId tx_id; } jazz_db_MutationErrorWait;
void jazz_db__mutation_errors__MutationErrorWait__new(jazz_db_MutationErrorWait *out, jazz_db_MutationErrorState *s, jazz_db_SharedTickScheduler *sched, jazz_model_TxId tx);
void jazz_db__mutation_errors__MutationErrorWait__drop(jazz_db_MutationErrorWait *w);
typedef struct jazz_db_MutationErrorDelivery { jazz_db_MutationErrorCallback callback; JZ_VEC(struct { jazz_model_TxId tx; jazz_db_MutationErrorEvent *event; }) events; } jazz_db_MutationErrorDelivery;
bool jazz_db__mutation_errors__take_pending_mutation_error_delivery(jazz_db_MutationErrorState *s, jazz_db_MutationErrorDelivery *out);

/* PermissionAdviceWaiters = BTreeMap<PermissionAdviceRequestId, oneshot::Sender<PermissionAdvice>> */
extern const jz_btree_desc jazz_db_permission_waiters_desc;
extern const jz_btree_desc jazz_db_permission_request_id_set_desc; /* BTreeSet<PermissionAdviceRequestId> */
typedef struct jazz_db_PermissionAdviceFuture { jz_btree *waiters; jazz_protocol_PermissionAdviceRequestId request_id; jazz_db_Oneshot *receiver; } jazz_db_PermissionAdviceFuture;

/* RemoteLinkTracker (first_load.rs) */
typedef struct jazz_db_RemoteLinkTracker jazz_db_RemoteLinkTracker;
jazz_db_RemoteLinkTracker *jazz_db__first_load__RemoteLinkTracker__new(jazz_db_SharedTickScheduler *scheduler);
void jazz_db__first_load__RemoteLinkTracker__upstream_attached(jazz_db_RemoteLinkTracker *t);
void jazz_db__first_load__RemoteLinkTracker__upstream_detached(jazz_db_RemoteLinkTracker *t);
void jazz_db__first_load__RemoteLinkTracker__on_tick(jazz_db_RemoteLinkTracker *t);
bool jazz_db__first_load__RemoteLinkTracker__has_witnesses(const jazz_db_RemoteLinkTracker *t);
void jazz_db__first_load__RemoteLinkTracker__resolve_witnesses(jazz_db_RemoteLinkTracker *t, bool (*settled)(void *ctx, const jazz_db_UpstreamCoverageHandleVec *handles), void *ctx, jazz_db_UpstreamCoverageHandleVec *retired);

/* row_availability.rs */
typedef struct jazz_db_Node jazz_db_Node;
/* impl Node { fn request_current_rows(rows, context) -> impl Future<Output = CurrentRowsResult> } */
jazz_db_LocalFuture jazz_db__db__row_availability__Node__request_current_rows(jazz_db_Node *self, void *rows /* Vec<CurrentRowCoordinate>, moved */, jazz_protocol_PolicyBindingKey context);
typedef struct jazz_db_CurrentRowsRouter jazz_db_CurrentRowsRouter;
jazz_db_CurrentRowsRouter *jazz_db__row_availability__CurrentRowsRouter__default(void);
void jazz_db__row_availability__CurrentRowsRouter__disconnect(jazz_db_CurrentRowsRouter *r, u64 epoch);

/* chunk resolver / auxiliary pump (db.rs) */
typedef struct jazz_db_PeerChunkResolver jazz_db_PeerChunkResolver;
jazz_db_PeerChunkResolver *jazz_db__db__PeerChunkResolver__default(void);
jazz_db_PeerChunkResolver *jazz_db__db__PeerChunkResolver__clone(jazz_db_PeerChunkResolver *r);
u64 jazz_db__db__PeerChunkResolver__completion_generation(const jazz_db_PeerChunkResolver *r);
bool jazz_db__db__PeerChunkResolver__has_pending_local_demand(const jazz_db_PeerChunkResolver *r);
void jazz_db__db__PeerChunkResolver__complete(jazz_db_PeerChunkResolver *r, jazz_protocol_ChunkResponse *resp);
typedef enum { JAZZ_PUMP_UPSTREAM = 0, JAZZ_PUMP_SUBSCRIBER = 1 } jazz_db_PeerIoPumpRole;
typedef struct jazz_db_PeerIoPump { void *inner; jazz_db_PeerChunkResolver *resolver; } jazz_db_PeerIoPump;
jazz_db_PeerIoPump jazz_db__db__PeerIoPump__new(jazz_db_PeerChunkResolver *resolver, void *local_chunk_reader, u64 epoch, jazz_db_PeerIoPumpRole role, void *wire_inbound_context);
void jazz_db__db__PeerIoPump__with_shared_auxiliary_endpoint(jazz_db_PeerIoPump *p, void *endpoint);
void jazz_db__db__PeerIoPump__disconnect(jazz_db_PeerIoPump *p);
void jazz_db__db__PeerIoPump__claim_binding_output(const jazz_db_PeerIoPump *p);
jazz_db_PeerIoPump jazz_db__db__PeerIoPump__clone(const jazz_db_PeerIoPump *p);
bool jazz_db__db__PeerIoPump__take_outbound(jazz_db_PeerIoPump *p, u32 max, jazz_protocol_SyncMessage *out);
void jazz_db__db__PeerIoPump__restore_outbound(jazz_db_PeerIoPump *p, jazz_protocol_SyncMessage m);
void jazz_db__db__PeerIoPump__acknowledge_outbound(jazz_db_PeerIoPump *p, const jazz_protocol_SyncMessage *m);
void *jazz_db__db__PeerIoPump__chunk_requests_for_responder(jazz_db_PeerIoPump *p);
bool jazz_db__db__PeerIoPump__serve_local_request(jazz_db_PeerIoPump *p, void *request, jazz_protocol_SyncMessage *out);
void *jazz_node__node__NodeState__local_chunk_reader_clone(void *reader);

/* read_barrier.rs */
typedef struct jazz_db_ReadBarrierSender { u64 sent; u64 accepted; } jazz_db_ReadBarrierSender;
void jazz_db__read_barrier__ReadBarrierSender__commit_unit_sent(jazz_db_ReadBarrierSender *s);
bool jazz_db__read_barrier__ReadBarrierSender__pending_barrier(jazz_db_ReadBarrierSender *s, jazz_protocol_SyncMessage *out);
void jazz_db__read_barrier__ReadBarrierSender__barrier_accepted(jazz_db_ReadBarrierSender *s);
typedef struct jazz_db_ReadBarrierReceiver { u64 writes_through; u64 answered; jz_deque held; } jazz_db_ReadBarrierReceiver;
void jazz_db__read_barrier__ReadBarrierReceiver__default(jazz_db_ReadBarrierReceiver *out);
void jazz_db__read_barrier__ReadBarrierReceiver__drop(jazz_db_ReadBarrierReceiver *r);

/* wire */
typedef enum { JAZZ_TRANSPORT_BACKPRESSURE = 0, JAZZ_TRANSPORT_FAILED = 1 } jazz_db_TransportErrorTag;
typedef struct jazz_db_TransportError { u32 tag; jz_str message; } jazz_db_TransportError;
void jazz_protocol__wire__TransportError__display(jz_str *out, const jazz_db_TransportError *e);
void jazz_protocol__wire__TransportError__drop(jazz_db_TransportError *e);
typedef enum { JAZZ_FLUSH_IDLE = 0, JAZZ_FLUSH_MORE_READY = 1, JAZZ_FLUSH_BACKPRESSURED = 2 } jazz_db_WireFlushStatus;
typedef struct jazz_db_WireAuthorityEndpoint { jazz_types_NodeUuid node; u64 epoch; } jazz_db_WireAuthorityEndpoint;
typedef u32 jazz_db_WireFeatures;
#define JAZZ_FEATURE_AUTHORIZATION_SCOPE_VIEWS 0x8u
typedef struct jazz_db_WireError jazz_db_WireError;

/* Db (db.rs) mirror, as seen from mutations.rs */
typedef struct jazz_db_Node jazz_db_Node;
typedef struct jazz_db_DbIdentity { jazz_types_NodeUuid node; jazz_types_AuthorSubject author; } jazz_db_DbIdentity;
typedef struct jazz_db_RowIdSourceVt { jazz_types_RowUuid (*next_row_id)(void *self); } jazz_db_RowIdSourceVt;
typedef struct jazz_db_RowIdSource { void *self; const jazz_db_RowIdSourceVt *vt; } jazz_db_RowIdSource;
typedef struct jazz_db_WriteClock { u64 next_counter_ms; bool wall; } jazz_db_WriteClock;
u64 jazz_db__db__WriteClock__next_now_ms(jazz_db_WriteClock *c);
typedef struct jazz_db_Db {
    jazz_node_JazzSchema *schema;
    jazz_types_SchemaVersionId schema_version_id;
    bool schema_view_is_fixed;
    bool requires_open_schema_admission;
    void *schema_views;
    jazz_db_DbIdentity identity;
    jazz_db_Node *node; /* Rc<Node> */
    jazz_db_RowIdSource *row_id_source;
    bool row_id_source_guarantees_fresh;
    jazz_db_WriteClock *next_now_ms;
    bool has_reserved_tx_id;
    jazz_model_TxId reserved_tx_id;
    bool owner_operation_admitted;
    bool backend_attribution;
} jazz_db_Db;
void jazz_db__db__Db__drop(jazz_db_Db *db);
/* lifecycle.rs */
jz_err jazz_db__db__lifecycle__Db__reserve_transaction_id_at_ms(const jazz_db_Db *db, u64 now_ms, jazz_model_TxId *out);
jazz_db_Db *jazz_db__db__lifecycle__Db__clone_for_reserved_transaction(const jazz_db_Db *db, jazz_model_TxId tx);
jz_err jazz_db__db__lifecycle__Db__ensure_mutation_operation_admitted(const jazz_db_Db *db);
jz_err jazz_db__db__lifecycle__Db__refresh_subscriptions_for_tables(const jazz_db_Db *db, const jz_hmap *changed);
jz_err jazz_db__db__lifecycle__Db__write_state(const jazz_db_Db *db, jazz_model_TxId tx, jazz_db_WriteState *out);
/* transactions.rs */
jz_err jazz_db__db__transactions__Db__transaction_is_exclusive(const jazz_db_Db *db, jazz_model_OpenTransactionId tx, bool *out);
jz_err jazz_db__db__transactions__Db__transaction_target_for_write(const jazz_db_Db *db, jazz_model_OpenTransactionId tx, jz_slice table, jazz_types_RowUuid row, const char *operation, bool allow_missing, bool *has, jz_btree *out);
jz_err jazz_db__db__transactions__Db__lock_for_transaction_operation(const jazz_db_Db *db, jazz_model_OpenTransactionId tx, jazz_node_NodeState **out);
/* reads.rs */
typedef struct jazz_db_PreparedQuery { jazz_model_ValidatedQuery *shape; jazz_model_Binding *binding; } jazz_db_PreparedQuery;
jz_err jazz_db__db__reads__Db__prepare_query(const jazz_db_Db *db, const jazz_model_Query *q, jazz_db_PreparedQuery *out);
void jazz_db__db__PreparedQuery__drop(jazz_db_PreparedQuery *q);
typedef struct jazz_db_ReadOpts jazz_db_ReadOpts;
jazz_db_ReadOpts *jazz_db__db__ReadOpts__local_only(void);
void jazz_db__db__ReadOpts__branch_view(jazz_db_ReadOpts *o, jazz_protocol_BranchSelector head, jazz_protocol_BranchViewBase *base);
jz_err jazz_db__db__reads__Db__all_for_identity(const jazz_db_Db *db, const jazz_db_PreparedQuery *q, jazz_db_ReadOpts *opts, jazz_types_AuthorSubject identity, jazz_node_CurrentRowVec *out);

/* WriteHandle (db.rs) */
typedef struct jazz_db_WriteHandle {
    jazz_db_NodeMutex *node; /* Weak<LocalMutex<NodeState>> */
    jazz_types_RowUuid row_uuid;
    jazz_model_TxId tx_id;
    u8 local_tier;
    jazz_db_QueuedMutationStatus *queued_status; /* Option<Rc<RefCell<..>>> */
    jazz_db_QueuedMutationAlias *queued_alias;   /* Option<Rc<RefCell<Option<TxId>>>> */
} jazz_db_WriteHandle;
jazz_model_TxId jazz_db__db__WriteHandle__mergeable_tx_id(const jazz_db_WriteHandle *w);
void jazz_db__db__WriteHandle__drop(jazz_db_WriteHandle *w);

/* WriteIdentity / write targets / options (db.rs) */
typedef enum { JAZZ_WID_DATABASE = 0, JAZZ_WID_SESSION = 1, JAZZ_WID_ATTRIBUTION = 2 } jazz_db_WriteIdentityTag;
typedef struct jazz_db_WriteIdentity { u32 tag; jazz_types_AuthorSubject author; } jazz_db_WriteIdentity;
typedef struct jazz_db_ResolvedWriteIdentity { jazz_types_AuthorSubject made_by; bool has_permission_subject; jazz_types_AuthorSubject permission_subject; } jazz_db_ResolvedWriteIdentity;
typedef struct jazz_db_ExactWriteTarget { bool is_branch; jazz_protocol_BranchSelector branch; } jazz_db_ExactWriteTarget;
void jazz_db__db__ExactWriteTarget__branch(const jazz_db_ExactWriteTarget *t, jazz_protocol_BranchSelector *out);
typedef struct jazz_db_WriteTarget { bool is_branch_view; jazz_protocol_BranchSelector head; jazz_protocol_BranchViewBase *base; } jazz_db_WriteTarget;
typedef struct jazz_db_InsertOptions { bool has_row_id; jazz_types_RowUuid row_id; jazz_db_WriteIdentity identity; jazz_db_ExactWriteTarget target; bool has_updated_at_ms; u64 updated_at_ms; } jazz_db_InsertOptions;
typedef struct jazz_db_UpdateOptions { jazz_db_WriteIdentity identity; jazz_db_WriteTarget target; bool has_updated_at_ms; u64 updated_at_ms; } jazz_db_UpdateOptions;
typedef jazz_db_UpdateOptions jazz_db_UpsertOptions;
typedef jazz_db_UpdateOptions jazz_db_DeleteOptions;
typedef struct jazz_db_RestoreOptions { jazz_db_WriteIdentity identity; jazz_db_ExactWriteTarget target; bool has_updated_at_ms; u64 updated_at_ms; } jazz_db_RestoreOptions;

/* ---------- connection-shared db.rs types ---------- */
/* UpstreamUploadDestination { remote_node, link_identity } (Copy, Ord) */
typedef struct jazz_db_UpstreamUploadDestination { u8 remote_node[16]; jazz_types_AuthorSubject link_identity; } jazz_db_UpstreamUploadDestination;
int jazz_db__db__UpstreamUploadDestination__cmp(const void *a, const void *b, void *ctx);
/* ShapeRegistrationKey = (ShapeId, ReadViewKey) */
typedef struct jazz_db_ShapeRegistrationKey { jazz_model_ShapeId shape_id; jazz_protocol_ReadViewKey read_view; } jazz_db_ShapeRegistrationKey;
extern const jz_btree_desc jazz_db_shape_registration_key_set_desc;
/* RelayUpstreamSubscriptionOwners key (SubscriptionKey, u64, ReadViewKey) */
typedef struct jazz_db_RelayOwnerKey { jazz_protocol_SubscriptionKey subscription; u64 downstream_connection_epoch; jazz_protocol_ReadViewKey read_view; } jazz_db_RelayOwnerKey;
typedef struct jazz_db_RelayUpstreamSubscriptionOwner {
    jazz_db_PendingUpstreamSubscription request;
    u64 scalar_authority_revision;
    jazz_db_ScalarReconciliation scalar_reconciliation;
    u64 downstream_connection_epoch;
    jazz_db_CoverageKey coverage;
    jazz_db_PolicyBinding policy_binding;
    jz_btree downstream_subscriptions; /* BTreeSet<SubscriptionKey> */
} jazz_db_RelayUpstreamSubscriptionOwner;
extern const jz_btree_desc jazz_db_relay_owner_desc;          /* RelayOwnerKey -> RelayUpstreamSubscriptionOwner */
void jazz_db__db__RelayUpstreamSubscriptionOwner__drop(void *p);
typedef struct jazz_db_RelaySubscriptionRejection {
    jazz_db_CoverageKey coverage;
    jazz_db_PolicyBinding policy_binding;
    jz_btree downstream_subscriptions;
    jazz_protocol_SubscribeRejectReason reason;
} jazz_db_RelaySubscriptionRejection;
void jazz_db__db__RelaySubscriptionRejection__drop(void *p);
extern const jz_btree_desc jazz_db_relay_rejections_desc;     /* u64 -> jz_deque<RelaySubscriptionRejection> */
/* UpstreamSubscriptionOwners: SubscriptionKey -> Vec<Weak<SubscriptionState>> */
typedef JZ_VEC(jazz_db_SubscriptionState *) jazz_db_WeakStateVec;
extern const jz_btree_desc jazz_db_upstream_owners_desc;
/* CoverageKey maps */
extern const jz_btree_desc jazz_db_coverage_subscription_desc;  /* CoverageKey -> SubscriptionKey */
extern const jz_btree_desc jazz_db_coverage_count_desc;         /* CoverageKey -> usize / u64 */
extern const jz_btree_desc jazz_db_coverage_set_desc;           /* BTreeSet<CoverageKey> */
extern const jz_btree_desc jazz_db_query_coverage_registrations_desc; /* SubscriptionKey -> QueryCoverageRegistration */
extern const jz_btree_desc jazz_db_txid_u64_desc;               /* TxId -> u64 */
extern const jz_btree_desc jazz_db_upload_destination_queues_desc; /* UpstreamUploadDestination -> LargeValueUploadQueues */
extern const jz_btree_desc jazz_db_local_fate_routes_desc;      /* TxId -> Vec<LocalFateRoute> */
/* CoverageGroup (db.rs) */
typedef enum { JAZZ_CPBO_DIRECT_ADMITTED = 0, JAZZ_CPBO_DELEGATED = 1 } jazz_db_CoveragePolicyBindingOrigin;
typedef struct jazz_db_KeyedSyncMessage { jazz_protocol_SubscriptionKey subscription; jazz_protocol_SyncMessage message; } jazz_db_KeyedSyncMessage;
typedef struct jazz_db_CoverageGroup {
    jazz_model_ValidatedQuery *shape;
    jazz_model_Binding *binding;
    jazz_db_PolicyBinding policy_binding;
    u8 policy_binding_origin;
    jz_btree subscribers;                 /* BTreeSet<SubscriptionKey> */
    jz_btree pending_initial_subscribers; /* BTreeSet<SubscriptionKey> */
    bool has_pending_initial_update;
    jazz_db_KeyedSyncMessage pending_initial_update;
    jz_deque pending_incremental_updates; /* VecDeque<KeyedSyncMessage> */
    bool has_publication_runtime_token;
    u64 publication_runtime_token;
    bool has_pending_claim_refresh_revision;
    u64 pending_claim_refresh_revision;
    bool initialized;
    jazz_protocol_SubscriptionKey authority_result_subscription;
    jazz_protocol_SubscriptionKey upstream_subscription;
    jazz_protocol_RegisterShapeOptions upstream_opts;
    bool awaiting_upstream_settlement;
} jazz_db_CoverageGroup;
void jazz_db__db__CoverageGroup__drop(void *p);
extern const jz_btree_desc jazz_db_coverage_groups_desc;  /* CoverageKey -> CoverageGroup */
extern const jz_btree_desc jazz_db_served_desc;           /* SubscriptionKey -> CoverageKey */
/* SubscriberShapeRegistration (db.rs) */
enum { JAZZ_SSR_REGISTERED = 0, JAZZ_SSR_PENDING_CATALOGUE_ADMISSION = 1, JAZZ_SSR_REJECTED_UNSUPPORTED_CAPABILITY = 2 };
typedef struct jazz_db_SubscriberShapeRegistration { u32 tag; jazz_protocol_RegisterShapeOptions opts; jz_str detail; } jazz_db_SubscriberShapeRegistration;
bool jazz_db__db__SubscriberShapeRegistration__owns_node_shape(const jazz_db_SubscriberShapeRegistration *r);
extern const jz_btree_desc jazz_db_shape_registrations_desc; /* ShapeRegistrationKey -> SubscriberShapeRegistration */
/* ServedAuthorizationScopeHydration / Clause */
typedef struct jazz_db_ServedAuthorizationScopeClause {
    jazz_protocol_SubscriptionKey subscription;
    jazz_protocol_SyncMessage register_;
    jazz_protocol_SyncMessage subscribe;
    jazz_protocol_SyncMessage view;
} jazz_db_ServedAuthorizationScopeClause;
typedef struct jazz_db_ServedAuthorizationScopeHydration {
    JZ_VEC(jazz_db_ServedAuthorizationScopeClause) clauses;
    jazz_protocol_AuthorizationScopeReceipt *receipt;
} jazz_db_ServedAuthorizationScopeHydration;
void jazz_db__db__ServedAuthorizationScopeHydration__clone(void *dst, const void *src);
void jazz_db__db__ServedAuthorizationScopeHydration__drop(void *p);
extern const jz_btree_desc jazz_db_scope_hydrations_desc; /* AuthorizationSupportScopeKey* -> Hydration */
/* AuthorizationScopeLeaseRequest (db.rs) */
typedef struct jazz_db_AuthorizationScopeLeaseRequest {
    jazz_protocol_PermissionAdviceAction *action;
    jazz_db_PolicyBinding session_claim_binding;
    bool has_delegated_session;
    jazz_protocol_DelegatedSessionBinding delegated_session;
    jz_btree waiters; /* BTreeSet<PermissionAdviceRequestId> */
    bool intent_sent;
    jazz_protocol_AuthorizationSupportScopeKey *key; /* Option */
    void *lease;                                     /* Option<AuthorizationScopeLease> */
    void *owner;                                     /* Option<AuthorizationScopeOwnerToken> */
    bool has_clause_count;
    u16 clause_count;
    jz_btree applied_clauses;                        /* u16 -> (SubscriptionKey, GlobalTime, u64) */
} jazz_db_AuthorizationScopeLeaseRequest;
/* AuthorizationScopeLeaseManager (opaque registry + requests map) */
typedef struct jazz_db_AuthorizationScopeLeaseManager { void *registry; jz_btree requests; } jazz_db_AuthorizationScopeLeaseManager;
void jazz_db__db__AuthorizationScopeLeaseManager__default(jazz_db_AuthorizationScopeLeaseManager *out);
void jazz_db__db__AuthorizationScopeLeaseManager__drop(jazz_db_AuthorizationScopeLeaseManager *m);
/* PendingSubscriptionFinalization */
typedef struct jazz_db_OpeningLocal { bool has; u64 token; groove_SubscriptionId id; } jazz_db_OpeningLocal;
typedef struct jazz_db_PendingSubscriptionFinalization {
    jazz_db_SubscriptionState *state; /* Option<Rc<..>> */
    jazz_db_UpstreamCoverageHandleVec opening_upstream;
    jazz_db_OpeningLocal opening_local;
    jazz_db_Oneshot *acknowledgement;
} jazz_db_PendingSubscriptionFinalization;
void jazz_db__db__PendingSubscriptionFinalization__drop(void *p);
/* TransactionWaitObserver = Pin<Box<dyn Future<Output=()>>> */
typedef jazz_db_LocalFuture jazz_db_TransactionWaitObserver;
/* AuthorityViewReceipts::new */
void jazz_db__db__AuthorityViewReceipts__set(jazz_db_ActiveAuthorityViewReceipts *r, u64 epoch, jazz_types_GlobalTime floor);
void jazz_db__db__AuthorityViewReceipts__clear(jazz_db_ActiveAuthorityViewReceipts *r);

/* current_write_schema / catalogue_schemas (NodeState) */
typedef struct jazz_node_CurrentWriteSchema { jazz_types_SchemaVersionId schema; } jazz_node_CurrentWriteSchema;
jz_err jazz_node__node__NodeState__current_write_schema(jazz_node_NodeState *n, jazz_node_CurrentWriteSchema *out);
const jazz_node_JazzSchema *jazz_node__node__NodeState__catalogue_schema(jazz_node_NodeState *n, jazz_types_SchemaVersionId id);
void jazz_types__ids__SchemaVersionId__debug(jz_str *out, const jazz_types_SchemaVersionId *id);


/* ======================= peer_connection.rs needs ======================= */
void groove__records__values__Value__Serialize__serialize(jz_pc_w *w, const groove_Value *v);
void jazz_types__ids__AuthorSubject__Serialize__serialize(jz_pc_w *w, const jazz_types_AuthorSubject *s);
void jazz_protocol__protocol__SubscriptionKey__Serialize__serialize(jz_pc_w *w, const jazz_protocol_SubscriptionKey *k);
void jazz_protocol__protocol__ReadViewKey__Serialize__serialize(jz_pc_w *w, const jazz_protocol_ReadViewKey *k);
void jazz_protocol__protocol__PolicyBindingKey__Serialize__serialize(jz_pc_w *w, const jazz_protocol_PolicyBindingKey *k);
/* crate::node::Error::InvalidAuthoritySourceClosure { subscription, transition } */
bool jazz_node__node__Error__as_invalid_authority_source_closure(const jz_err *e, jazz_protocol_SubscriptionKey *subscription, const void **transition);
void jazz_protocol__protocol__SubscribeRejectReason__invalid_authority_source_closure(jazz_protocol_SubscribeRejectReason *out, const void *transition);
void jazz_protocol__protocol__SubscribeRejectReason__server_failure(jazz_protocol_SubscribeRejectReason *out, u32 code);
void jazz_protocol__protocol__AuthorizationSupportScopeKey__drop(jazz_protocol_AuthorizationSupportScopeKey *k);
void jazz_protocol__protocol__Subscribe__drop(jazz_protocol_Subscribe *s);
void jazz_db__db__KeyedSyncMessage__drop(void *p);
extern const jz_btree_desc jazz_db_coverage_authority_result_desc; /* CoverageKey -> AuthorityResultKey */

/* ViewUpdatePayload (field layout mirrors jazz_protocol::protocol::ViewUpdatePayload) */
typedef struct jazz_protocol_VersionCarrier jazz_protocol_VersionCarrier;
typedef struct jazz_protocol_SupportingRowsUpdate jazz_protocol_SupportingRowsUpdate;
typedef struct jazz_protocol_PeerPayloadInventory {
    JZ_VEC(jazz_model_TxId) complete_tx_payloads;
    bool has_authorization_progress; u64 authorization_progress;
    bool opening_pending;
} jazz_protocol_PeerPayloadInventory;
typedef JZ_VEC(jazz_protocol_VersionCarrier *) jazz_protocol_VersionCarrierVec;
struct jazz_protocol_ViewUpdatePayload {
    jazz_protocol_SubscriptionKey subscription;
    jazz_types_GlobalTime settled_through;
    jazz_protocol_VersionCarrierVec version_carriers;
    jazz_protocol_PeerPayloadInventory peer_payload_inventory;
    jazz_protocol_SupportingRowsUpdate *supporting_rows;
};
void jazz_protocol__protocol__ViewUpdatePayload__drop(jazz_protocol_ViewUpdatePayload *v);
typedef struct jazz_protocol_BundleRef { jazz_model_TxId tx_id; u32 fate_tag; } jazz_protocol_BundleRef;
typedef JZ_VEC(jazz_protocol_BundleRef) jazz_protocol_BundleRefVec;
jz_err jazz_protocol__protocol__VersionCarrier__bundle_refs(const jazz_protocol_VersionCarrier *c, jazz_protocol_BundleRefVec *out);
jazz_protocol_VersionCarrier *jazz_protocol__protocol__VersionCarrier__clone(const jazz_protocol_VersionCarrier *c);
void jazz_protocol__protocol__VersionCarrier__drop(jazz_protocol_VersionCarrier *c);
u32 jazz_protocol__protocol__SupportingRowsUpdate__added_rows_len(const jazz_protocol_SupportingRowsUpdate *u);
u32 jazz_protocol__protocol__SupportingRowsUpdate__removed_rows_len(const jazz_protocol_SupportingRowsUpdate *u);
/* jazz_node::node::ViewUpdateParts */
typedef struct jazz_node_ViewUpdateParts {
    jazz_protocol_SupportingRowsUpdate *wire_rows; /* Option */
    jazz_protocol_SubscriptionKey subscription;
    jazz_types_GlobalTime settled_through;
    bool defer_settlement;
    bool reset_input_set;
    jazz_protocol_VersionCarrierVec version_carriers;
    JZ_VEC(jazz_model_TxId) peer_complete_tx_payload_refs;
    bool has_authorization_progress; u64 authorization_progress;
    bool opening_pending;
    JZ_VEC(void *) result_member_adds;
    JZ_VEC(void *) result_member_removes;
} jazz_node_ViewUpdateParts;
u32 jazz_node__node__ViewUpdateParts__supporting_adds_len(const jazz_node_ViewUpdateParts *p);
u32 jazz_node__node__ViewUpdateParts__supporting_removes_len(const jazz_node_ViewUpdateParts *p);
void jazz_node__node__ViewUpdateParts__drop(void *p);
typedef JZ_VEC(jazz_node_ViewUpdateParts) jazz_node_ViewUpdatePartsVec;
/* db.rs PendingAuthorityViewUpdate */
typedef struct jazz_db_PendingAuthorityViewUpdate { jazz_node_ViewUpdateParts parts; bool authority_receipt_eligible; } jazz_db_PendingAuthorityViewUpdate;
typedef JZ_VEC(jazz_db_PendingAuthorityViewUpdate) jazz_db_PendingAuthorityViewUpdateVec;

/* ---- peer_connection.rs needs (part 2): node/peer/protocol mirrors ---- */
#define NS jazz_node_NodeState
bool jazz_db__wire__encoded_sync_message_len(const jazz_protocol_SyncMessage *m, u32 *len);
jz_err jazz_node__node__NodeState__binding_view_key_for_subscription(NS *n, jazz_protocol_SubscriptionKey s, jazz_protocol_BindingViewKey *out);
jz_err jazz_node__node__NodeState__ingest_unselected_authority_view_bundles(NS *n, jazz_protocol_VersionCarrierVec *carriers);
jz_err jazz_node__node__NodeState__apply_view_updates_in_batch(NS *n, jazz_node_ViewUpdatePartsVec updates);
void jazz_node__node__NodeState__record_authoritative_settled_through(NS *n, jazz_types_GlobalTime cut);
jz_err jazz_node__node__NodeState__table(NS *n, jz_slice table, const jazz_node_TableSchema **out);
const jazz_model_Query *jazz_node__node__TableSchema__insert_check(const jazz_node_TableSchema *t);
jz_err jazz_node__node__NodeState__dry_run_insert_allows(NS *n, jazz_node_MergeableCommit *commit, bool *allowed);
jz_err jazz_node__node__NodeState__local_current_row_exists(NS *n, jz_slice table, jazz_types_RowUuid row, bool *exists);
jz_err jazz_node__node__NodeState__dry_run_delete_current_allows(NS *n, jz_slice table, jazz_types_RowUuid row, jazz_types_AuthorSubject author, bool *allowed);
bool jazz_node__node__NodeState__permissions_ready(NS *n);
bool jazz_db__db__subscriber_permissions_ready(bool permissions_ready, u8 trust);
u64 jazz_node__node__NodeState__active_catalogue_seq(NS *n);
void jazz_node__node__NodeState__record_dropped_peer_request(NS *n);
void jazz_node__node__NodeState__record_transport_backpressure_retry(NS *n);
enum { JAZZ_QAM_TRUSTED_SERVING = 3 };
jz_err jazz_node__node__NodeState__ensure_peer_maintained_subscription_view_supported(NS *n, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding,
    jazz_model_DurabilityTier tier, jazz_types_AuthorSubject subject, const jazz_protocol_ReadViewSpec *view, u8 mode);

/* PermissionAdviceAction (protocol.rs) */
enum { JAZZ_PAA_INSERT = 0, JAZZ_PAA_READ = 1, JAZZ_PAA_UPDATE = 2, JAZZ_PAA_DELETE = 3 };
struct jazz_protocol_PermissionAdviceAction {
    u32 tag;
    jz_str table;
    jz_btree cells;  /* Insert: String -> Value */
    jazz_types_RowUuid row;
    jz_btree patch;  /* Update: String -> Value */
};

/* AuthorizationSupportScope (node) */
typedef struct jazz_node_ScopeSubscription { jazz_model_ValidatedQuery *shape; jazz_model_Binding *binding; } jazz_node_ScopeSubscription;
typedef struct jazz_node_AuthorizationSupportScope {
    jazz_protocol_AuthorizationSupportScopeKey *key;
    JZ_VEC(jazz_node_ScopeSubscription) subscriptions;
    jazz_protocol_RegisterShapeOptions options;
} jazz_node_AuthorizationSupportScope;
jz_err jazz_node__node__NodeState__authorization_support_scope_for_session(NS *n, jazz_types_AuthorSubject identity, const jz_btree *claims,
    const jazz_protocol_PermissionAdviceAction *action, jazz_node_AuthorizationSupportScope *out);
void jazz_node__node__AuthorizationSupportScope__drop(jazz_node_AuthorizationSupportScope *s);
void jazz_db__db__retain_current_scope_hydrations(jz_btree *hydrations, u64 claims_revision, u64 policy_epoch, jazz_types_GlobalTime cut);
enum { JAZZ_DB_MAX_AUTHORIZATION_SCOPES = 256 };

/* authorization_scope.rs */
typedef struct jazz_protocol_AuthorityScopeAggregate jazz_protocol_AuthorityScopeAggregate;
jazz_protocol_AuthorityScopeAggregate *jazz_protocol__authorization_scope__AuthorityScopeAggregate__new(const void *shape_binding_ids, u32 len);
bool jazz_protocol__authorization_scope__AuthorityScopeAggregate__register(jazz_protocol_AuthorityScopeAggregate *a, jazz_protocol_SubscriptionKey s, const void *shape_binding_id);
bool jazz_protocol__authorization_scope__AuthorityScopeAggregate__apply(jazz_protocol_AuthorityScopeAggregate *a, jazz_protocol_SubscriptionKey s, jazz_types_GlobalTime cut, u64 progress);
bool jazz_protocol__authorization_scope__AuthorityScopeAggregate__bounds(const jazz_protocol_AuthorityScopeAggregate *a, jazz_types_GlobalTime *settled_through, u64 *progress);
void jazz_protocol__authorization_scope__AuthorityScopeAggregate__drop(jazz_protocol_AuthorityScopeAggregate *a);
typedef struct jazz_protocol_AuthorizationScopeReceiptFields {
    jazz_protocol_AuthorizationSupportScopeKey *key;
    u8 authority[16];
    jazz_types_AuthorSubject link;
    u64 authority_epoch;
    u64 claims_revision;
    u64 policy_epoch;
    jazz_types_GlobalTime settled_through;
    u64 authorization_progress;
} jazz_protocol_AuthorizationScopeReceiptFields;
jazz_protocol_AuthorizationScopeReceipt *jazz_protocol__protocol__AuthorizationScopeReceipt__new(const jazz_protocol_AuthorizationScopeReceiptFields *f);
void jazz_protocol__protocol__AuthorizationScopeReceipt__fields(const jazz_protocol_AuthorizationScopeReceipt *r, jazz_protocol_AuthorizationScopeReceiptFields *out);
jazz_types_AuthorSubject jazz_protocol__protocol__AuthorizationSupportScopeKey__subject(const jazz_protocol_AuthorizationSupportScopeKey *k);
bool jazz_protocol__protocol__AuthorityContext__same_admitted_link(const jazz_protocol_AuthorityContext *a, const jazz_protocol_AuthorityContext *b);

/* PeerState */
void jazz_node__peer__PeerState__declare_known_state(jazz_node_PeerState *p, jazz_protocol_SubscriptionKey s, void *known_state);
void jazz_node__peer__PeerState__forget_subscription(jazz_node_PeerState *p, jazz_protocol_SubscriptionKey s);
jz_err jazz_node__peer__PeerState__rehydrate_authorization_support_query_for_identity(jazz_node_PeerState *p, NS *n, jazz_types_AuthorSubject identity, jz_btree claims,
    jazz_protocol_SubscriptionKey s, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding, jazz_protocol_RegisterShapeOptions opts, jazz_protocol_SyncMessage *out);
bool jazz_node__peer__PeerState__subscription_awaits_selected_authority_source(const jazz_node_PeerState *p, jazz_protocol_SubscriptionKey s);
const jazz_protocol_AuthorityResultKey *jazz_node__peer__PeerState__subscription_authority_result_source(const jazz_node_PeerState *p, jazz_protocol_SubscriptionKey s);
bool jazz_node__peer__PeerState__needs_catalogue_snapshot(const jazz_node_PeerState *p, const u8 fingerprint[32]);
void jazz_node__peer__PeerState__mark_catalogue_snapshot_announced(jazz_node_PeerState *p, const u8 fingerprint[32]);

/* Catalogue snapshot */
typedef struct jazz_protocol_CatalogueSnapshot jazz_protocol_CatalogueSnapshot;
jz_err jazz_node__node__NodeState__catalogue_snapshot(NS *n, jazz_protocol_CatalogueSnapshot **out);
void jazz_protocol__protocol__CatalogueSnapshot__to_json_vec(const jazz_protocol_CatalogueSnapshot *s, jz_str *out);
void jazz_protocol__protocol__CatalogueSnapshot__drop(jazz_protocol_CatalogueSnapshot *s);

/* Chunk responses, fates, barriers */
typedef struct jazz_db_ChunkResponseBatch jazz_db_ChunkResponseBatch;
jazz_db_ChunkResponseBatch *jazz_protocol__protocol__ChunkResponseBatch__clone(const jazz_db_ChunkResponseBatch *b);
void jazz_protocol__protocol__ChunkResponseBatch__into_message(jazz_db_ChunkResponseBatch *b, jazz_protocol_SyncMessage *out);
void jazz_protocol__protocol__ChunkResponseBatch__drop(jazz_db_ChunkResponseBatch *b);
void jazz_db__db__register_local_fate_observer(jz_btree *local_fate_routes, jazz_model_TxId tx, jazz_db_SyncMessageVec *downstream_fates);
bool jazz_db__db__read_barrier__ReadBarrierReceiver__answer_sent(jazz_db_ReadBarrierReceiver *r, const jazz_protocol_SyncMessage *m);
jazz_db_MutationErrorEvent *jazz_db__mutation_errors__mutation_error_event_for_kind(jazz_model_TxId tx, jazz_model_TxKind kind, const jazz_model_RejectionReason *reason);
void jazz_types__ids__NodeUuid__debug(jz_str *out, const jazz_types_NodeUuid *n);

/* Query mirrors (jazz_model::query) used by the authority id-reference check */
const jz_btree *jazz_model__query__ValidatedQuery__params(const jazz_model_ValidatedQuery *q);  /* name -> param */
const jz_btree *jazz_model__query__Binding__values(const jazz_model_Binding *b);              /* name -> Value */
enum { JAZZ_OPERAND_COLUMN = 0, JAZZ_OPERAND_PARAM = 1, JAZZ_OPERAND_LITERAL = 2 };
typedef struct jazz_model_Operand { u32 tag; jz_str column; } jazz_model_Operand;
enum { JAZZ_PRED_ALL, JAZZ_PRED_ANY, JAZZ_PRED_NOT, JAZZ_PRED_EQ, JAZZ_PRED_NE, JAZZ_PRED_GT, JAZZ_PRED_GTE, JAZZ_PRED_LT, JAZZ_PRED_LTE,
       JAZZ_PRED_CONTAINS, JAZZ_PRED_IN, JAZZ_PRED_IS_NULL, JAZZ_PRED_ENUM_MATCH };
typedef struct jazz_model_Predicate {
    u32 tag;
    JZ_VEC(struct jazz_model_Predicate) items;
    struct jazz_model_Predicate *item;
    jazz_model_Operand left, right;
    JZ_VEC(jazz_model_Operand) values;
    jz_str column;
} jazz_model_Predicate;
typedef struct jazz_model_CorrelatedFilter { jz_str source_column; } jazz_model_CorrelatedFilter;
typedef struct jazz_model_JoinVia {
    bool has_source_column; jz_str source_column;
    bool has_source_lookup; jz_str source_lookup_row_id_source_column;
    JZ_VEC(jazz_model_CorrelatedFilter) correlated_filters;
} jazz_model_JoinVia;
typedef struct jazz_model_QueryPolicyBranch {
    JZ_VEC(jazz_model_Predicate) filters;
    u32 reachable_len;
    JZ_VEC(jazz_model_JoinVia) joins;
} jazz_model_QueryPolicyBranch;
typedef struct jazz_model_QueryPolicyParts {
    JZ_VEC(jazz_model_Predicate) filters;
    u32 reachable_len;
    JZ_VEC(jazz_model_JoinVia) joins;
    JZ_VEC(jazz_model_QueryPolicyBranch) policy_branches;
} jazz_model_QueryPolicyParts;
const jazz_model_QueryPolicyParts *jazz_model__query__Query__policy_parts(const jazz_model_Query *q);
#undef NS

/* tick_inner support */
void jazz_db__db__AuthorizationScopeLeaseRequest__drop(void *p);
extern const jz_btree_desc jazz_db_scope_applied_clauses_desc; /* u16 -> (SubscriptionKey, GlobalTime, u64) */

#define NS jazz_node_NodeState
/* upstream inbound */
void jazz_db__db__UploadOutbox__retain(jazz_db_UploadOutbox *o, bool (*keep)(const jazz_db_PendingUpload *p, void *ctx), void *ctx);
void jazz_db__db__UploadOutbox__mark_upload_failed(jazz_db_UploadOutbox *o, jazz_model_TxId tx, const char *reason);
enum { JAZZ_REJECTION_MALFORMED_COMMIT = 4 };
jz_err jazz_node__node__NodeState__defer_catalogue_for_persistence(NS *n, const jz_waker *waker, bool *deferred);
jz_err jazz_node__node__NodeState__apply_trusted_catalogue_snapshot(NS *n, jazz_protocol_CatalogueSnapshot *snapshot, jazz_node_PublicationOutcome *out);
const jz_btree *jazz_node__node__NodeState__catalogue_schemas(NS *n); /* SchemaVersionId -> schema */
jz_err jazz_db__db__pending_open_schema_error(void);
jz_err jazz_db__db__row_availability__receive_current_rows(jazz_db_NodeMutex *node, jazz_db_CurrentRowsRouter *router, const jazz_protocol_AuthorityContext *selected,
    const jazz_protocol_AuthorityContext *expected, bool eligible, jazz_protocol_CurrentRowsReceipt *receipt);
jz_err jazz_node__node__NodeState__remember_discarded_pending_view_transactions(NS *n, const jazz_protocol_VersionCarrierVec *carriers);
jz_err jazz_node__node__NodeState__missing_known_state_row_version_refs(NS *n, const jazz_protocol_SyncMessage *message, u32 *missing_len);
bool jazz_protocol__protocol__SupportingRowsUpdate__is_snapshot(const jazz_protocol_SupportingRowsUpdate *u);
bool jazz_protocol__protocol__SupportingRowsUpdate__catch_up_predecessor(const jazz_protocol_SupportingRowsUpdate *u, u8 out[16]);
bool jazz_node__node__NodeState__holds_supporting_revision(NS *n, jazz_protocol_SubscriptionKey s, const u8 revision[16]);
void jazz_node__node__NodeState__forget_supporting_revision(NS *n, jazz_protocol_SubscriptionKey s);
void jazz_node__node__NodeState__forget_declared_known_state(NS *n, jazz_protocol_SubscriptionKey s);
bool jazz_protocol__protocol__AuthorizationSupportScopeKey__eq(const jazz_protocol_AuthorizationSupportScopeKey *a, const jazz_protocol_AuthorizationSupportScopeKey *b);
void jazz_protocol__protocol__AuthorizationScopeReceipt__drop(jazz_protocol_AuthorizationScopeReceipt *r);
/* AuthorizationScopeRegistry (authorization_scope.rs) */
enum { JAZZ_SCOPE_ACQ_OWNER = 0, JAZZ_SCOPE_ACQ_WAITING = 1, JAZZ_SCOPE_ACQ_PROVEN = 2 };
bool jazz_protocol__authorization_scope__AuthorizationScopeRegistry__acquire(void *registry, jazz_protocol_AuthorizationSupportScopeKey *key, void **lease, u32 *acquisition, void **owner);
bool jazz_protocol__authorization_scope__AuthorizationScopeRegistry__install(void *registry, void *lease, void *owner, jazz_protocol_AuthorityContext authority, jazz_protocol_AuthorizationScopeReceipt *receipt);
bool jazz_protocol__authorization_scope__AuthorizationScopeRegistry__receipt(void *registry, void *lease, jazz_protocol_AuthorityContext authority, u64 authorization_progress, u64 settled_through);
void jazz_protocol__authorization_scope__AuthorizationScopeLease__drop(void *lease);
typedef struct jazz_db_AppliedScopeClause { jazz_protocol_SubscriptionKey subscription; jazz_types_GlobalTime cut; u64 progress; } jazz_db_AppliedScopeClause;
bool jazz_node__node__is_catalogue_mutation(const jazz_protocol_SyncMessage *m);
jz_err jazz_node__node__NodeState__ingest_relay_commit_unit_with_encoder_trust(NS *n, jazz_protocol_Transaction *tx, jazz_protocol_VersionVec versions, bool encoder_trust);
void jazz_db__db__route_local_fate(jz_btree *local_fate_routes, jazz_model_TxId tx, const jazz_protocol_SyncMessage *fate);
typedef struct jazz_db_PeerConnection jazz_db_PeerConnection;
jz_err jazz_db__db__row_availability__PeerConnection__pump_current_rows(jazz_db_PeerConnection *self);
#undef NS

#define NS jazz_node_NodeState
/* subscriber tick */
enum { JAZZ_CATALOGUE_BOOTSTRAP_READY = 2 };
u32 jazz_node__node__NodeState__catalogue_bootstrap_state(NS *n);
bool jazz_node__peer__PeerState__has_announced_catalogue_snapshot(const jazz_node_PeerState *p);
void jazz_node__node__NodeState__apply_unsubscribe_with_admitted_policy_binding(NS *n, jazz_protocol_SubscriptionKey s, jazz_protocol_PolicyBindingKey binding);
/* read_barrier.rs */
typedef struct jazz_db_HeldRequest { jazz_protocol_SyncMessage message; jazz_db_BufferLease *lease; bool receipts_validated; } jazz_db_HeldRequest;
bool jazz_db__db__read_barrier__ReadBarrierReceiver__pop_released(jazz_db_ReadBarrierReceiver *r, jazz_db_HeldRequest *out);
bool jazz_db__db__read_barrier__ReadBarrierReceiver__is_holding(const jazz_db_ReadBarrierReceiver *r);
bool jazz_db__db__read_barrier__ReadBarrierReceiver__has_released(const jazz_db_ReadBarrierReceiver *r);
bool jazz_db__db__read_barrier__ReadBarrierReceiver__admit(jazz_db_ReadBarrierReceiver *r, jazz_db_HeldRequest request, jazz_db_HeldRequest *out);
void jazz_db__db__read_barrier__ReadBarrierReceiver__commit_unit_received(jazz_db_ReadBarrierReceiver *r, jazz_model_TxId tx);
/* db.rs helpers */
void jazz_db__db__queue_local_acknowledgements(jz_btree *local_fate_routes, jazz_db_NodeMutex *node);
jazz_model_ValidatedQuery *jazz_node__node__NodeState__registered_shape(NS *n, jazz_model_ShapeId shape_id); /* Option<Rc>, owned */
void jazz_model__query__ValidatedQuery__drop(jazz_model_ValidatedQuery *q);
jazz_model_ValidatedQuery *jazz_model__query__ValidatedQuery__clone(const jazz_model_ValidatedQuery *q);
bool jazz_db__db__subscriber_inbound_message_is_authority_only(const jazz_protocol_SyncMessage *m, jazz_node_CommitUnitIngestContext ctx, const jazz_node_PeerState *peer);
bool jazz_db__db__admitted_request_policy_binding(jazz_node_CommitUnitIngestContext ctx, const jazz_node_PeerState *peer, const jazz_db_PolicyBinding *session /* NULL = None */,
    const jazz_protocol_DelegatedSessionBinding *delegated /* NULL = None */, jazz_db_PolicyBinding *out);
/* chunk requests */
typedef struct jazz_protocol_ChunkRequest { u64 request_id; u8 locator[32]; u8 expected_hash[32]; } jazz_protocol_ChunkRequest;
enum { JAZZ_CHUNK_RESPONSE_FOUND = 0, JAZZ_CHUNK_RESPONSE_UNAVAILABLE = 1 };
typedef struct jazz_protocol_ChunkResponseEntry { u64 request_id; u32 tag; jz_bytes bytes; } jazz_protocol_ChunkResponseEntry;
typedef JZ_VEC(jazz_protocol_ChunkResponseEntry) jazz_protocol_ChunkResponseEntryVec;
jazz_db_ChunkResponseBatch *jazz_protocol__protocol__ChunkResponseBatch__new(jazz_protocol_ChunkResponseEntryVec responses);
void jazz_protocol__protocol__ChunkRequest__drop(jazz_protocol_ChunkRequest *r);
enum { GROOVE_CHUNK_STORAGE_UNAVAILABLE = 1 };
bool jazz_db__db__PeerIoPump__is_disconnected(const jazz_db_PeerIoPump *p);
void jazz_db__db__PeerChunkResolver__enqueue_relay(jazz_db_PeerChunkResolver *r, u64 connection_epoch, jazz_protocol_ChunkRequest *request);
/* current rows */
typedef struct jazz_protocol_CurrentRowsRequest {
    jazz_protocol_PermissionAdviceRequestId request_id;
    JZ_VEC(jazz_protocol_CurrentRowCoordinate) rows;
    bool has_delegated_session;
    jazz_protocol_DelegatedSessionBinding delegated_session;
} jazz_protocol_CurrentRowsRequest;
void jazz_protocol__protocol__CurrentRowsRequest__clone(jazz_protocol_CurrentRowsRequest *dst, const jazz_protocol_CurrentRowsRequest *src);
void jazz_protocol__protocol__CurrentRowsRequest__drop(jazz_protocol_CurrentRowsRequest *r);
void jazz_db__row_availability__CurrentRowsRouter__cancel_downstream(jazz_db_CurrentRowsRouter *r, u64 epoch, jazz_protocol_PermissionAdviceRequestId request_id);
bool jazz_db__row_availability__valid_request(const jazz_protocol_CurrentRowsRequest *r);
bool jazz_node__node__NodeState__can_mint_current_row_receipts(NS *n);
u64 *jazz_db__row_availability__CurrentRowsRouter__progress_entry(jazz_db_CurrentRowsRouter *r, u64 epoch);
jz_err jazz_node__node__NodeState__evaluate_current_rows(NS *n, const jazz_protocol_CurrentRowsRequest *request, jazz_protocol_PolicyBindingKey context, u64 epoch, u64 progress,
    jazz_protocol_CurrentRowsReceipt **out);
typedef struct jazz_db_CurrentRowsRoute {
    jazz_protocol_CurrentRowsRequest request;
    jazz_protocol_PolicyBindingKey context;
    bool has_upstream; jazz_protocol_PermissionAdviceRequestId upstream;
    bool has_downstream; u64 downstream_epoch; jazz_protocol_PermissionAdviceRequestId downstream_request_id;
    jazz_db_Oneshot *sender;
} jazz_db_CurrentRowsRoute;
bool jazz_db__row_availability__CurrentRowsRouter__admit(jazz_db_CurrentRowsRouter *r, jazz_db_CurrentRowsRoute route);
jazz_protocol_CurrentRowsReceipt *jazz_db__row_availability__unknown_receipt(const jazz_protocol_CurrentRowsRequest *request, jazz_protocol_PolicyBindingKey context);
void jazz_protocol__protocol__PolicyBindingKey__clone(void *dst, const void *src);
void jazz_protocol__protocol__PolicyBindingKey__drop(void *p);
/* shape registration */
enum { JAZZ_MAX_SHAPE_REGISTRATIONS_PER_PEER = 1024, JAZZ_MAX_SHAPE_REGISTRATION_BYTES = 64 * 1024 };
bool jazz_db__db__validate_shape_registration_size(const jazz_protocol_ShapeAst *ast, const jazz_protocol_RegisterShapeOptions *opts, const char **message);
void jazz_db__db__server_subscription_failure_rejection_message(jazz_protocol_SyncMessage *out, jazz_protocol_SubscriptionKey s, const jz_err *error);
void jazz_db__db__unsupported_shape_capability_rejection_message(jazz_protocol_SyncMessage *out, jazz_protocol_SubscriptionKey s, jz_str detail);
jazz_protocol_SubscriptionKey jazz_db__db__register_shape_rejection_subscription(jazz_model_ShapeId shape_id, jazz_protocol_ReadViewKey read_view);
u8 jazz_db__db__delegated_session_capability(jazz_node_CommitUnitIngestContext ctx, jazz_node_PeerRole role);
jz_err jazz_db__db__ensure_supported_register_shape_options(const jazz_protocol_RegisterShapeOptions *opts, bool local_receiver, jazz_node_PeerRole role, u8 delegated_capability);
jz_err jazz_db__db__validate_shape_ast_for_registration(NS *n, jazz_model_ShapeId shape_id, const jazz_protocol_ShapeAst *ast, jazz_model_ValidatedQuery **out /* Option */);
bool jazz_db__db__is_server_shape_validation_failure(const jz_err *error);
bool jazz_db__db__subscriber_permission_subject(jazz_node_CommitUnitIngestContext ctx, jazz_types_AuthorSubject *out);
bool jazz_model__query__ValidatedQuery__params_is_empty(const jazz_model_ValidatedQuery *q);
u32 jazz_model__query__ValidatedQuery__params_len(const jazz_model_ValidatedQuery *q);
jz_err jazz_model__query__ValidatedQuery__bind(const jazz_model_ValidatedQuery *q, jz_btree values /* String -> Value */, jazz_model_Binding **out);
u8 jazz_node__node__NodeState__peer_query_authorization_mode(NS *n);
jz_err jazz_node__node__NodeState__register_shape_for_peer_with_options(NS *n, u64 connection_epoch, jazz_model_ShapeId shape_id, jazz_protocol_ShapeAst *ast, jazz_protocol_RegisterShapeOptions opts);
void jazz_protocol__protocol__ShapeAst__drop(jazz_protocol_ShapeAst *a);
/* subscribe admission */
void jazz_protocol__protocol__Subscribe__clone(jazz_protocol_Subscribe *dst, const jazz_protocol_Subscribe *src);
bool jazz_protocol__protocol__Subscribe__eq(const jazz_protocol_Subscribe *a, const jazz_protocol_Subscribe *b);
jz_err jazz_db__db__pending_catalogue_subscription_size(const jazz_protocol_Subscribe *s, const jazz_db_PolicyBinding *b, u32 *out);
void jazz_db__db__coverage_key(jazz_db_CoverageKey *out, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding, jazz_protocol_RegisterShapeOptions opts);
void jazz_db__db__CoverageKey__clone(void *dst, const void *src);
void jazz_protocol__protocol__RegisterShapeOptions__drop(jazz_protocol_RegisterShapeOptions *o);
void jazz_protocol__protocol__ReadViewSpec__clone_into(jazz_protocol_ReadViewSpec **dst, const jazz_protocol_ReadViewSpec *src);
enum { JAZZ_KNOWN_STATE_WATERMARK = 1 };
u32 jazz_protocol__protocol__KnownStateDeclaration__tag(const void *known_state);
void *jazz_protocol__protocol__KnownStateDeclaration__clone(const void *known_state);
jz_err jazz_node__node__NodeState__apply_subscribe_with_admitted_policy_binding(NS *n, jazz_protocol_Subscribe subscribe, jazz_protocol_PolicyBindingKey binding);
void jazz_node__node__NodeState__invalidate_ownerless_settled_result_view(NS *n, jazz_protocol_BindingViewKey view);
jazz_protocol_SupportingRowsUpdate *jazz_protocol__protocol__SupportingRowsUpdate__snapshot_empty(void);
jazz_protocol_ViewUpdatePayload *jazz_protocol__protocol__ViewUpdatePayload__new(jazz_protocol_SubscriptionKey s, jazz_types_GlobalTime settled_through,
    jazz_protocol_PeerPayloadInventory inventory, jazz_protocol_SupportingRowsUpdate *rows);
/* unsubscribe */
bool jazz_node__peer__PeerState__subscription_policy_binding(const jazz_node_PeerState *p, jazz_protocol_SubscriptionKey s, jazz_types_AuthorSubject *identity, jz_btree *claims);
void jazz_node__node__NodeState__release_shape_for_peer(NS *n, u64 connection_epoch, jazz_model_ShapeId shape_id);
/* other */
jz_err jazz_db__db__PublicationOutcome__responses(jazz_node_PublicationOutcome *o, jazz_db_SyncMessageVec *out);
bool jazz_db__db__UploadOutbox__contains(const jazz_db_UploadOutbox *o, jazz_model_TxId tx);
/* serving */
bool jazz_node__peer__PeerState__has_maintained_subscription(const jazz_node_PeerState *p, jazz_protocol_SubscriptionKey s);
bool jazz_node__peer__PeerState__has_pending_initial_maintained_subscription(const jazz_node_PeerState *p, jazz_protocol_SubscriptionKey s);
typedef struct jazz_node_ReconciledMaintainedSubscription jazz_node_ReconciledMaintainedSubscription;
const jazz_protocol_SyncMessage *jazz_node__peer__ReconciledMaintainedSubscription__canonical_update(const jazz_node_ReconciledMaintainedSubscription *r);
jz_err jazz_node__peer__PeerState__reconcile_maintained_subscription_for_clone(jazz_node_PeerState *p, NS *n, jazz_protocol_SubscriptionKey s, const jazz_model_ValidatedQuery *shape,
    const jazz_model_Binding *binding, const jazz_protocol_RegisterShapeOptions *opts, const jz_waker *waker, jazz_node_ReconciledMaintainedSubscription **out /* Option */);
jz_err jazz_node__peer__PeerState__rehydrate_query_for_subscription_from_reconciled_maintained_subscription(jazz_node_PeerState *p, NS *n, jazz_protocol_SubscriptionKey group,
    jazz_protocol_SubscriptionKey subscription, const jazz_model_ValidatedQuery *shape, jazz_node_ReconciledMaintainedSubscription *reconciled, jazz_protocol_SyncMessage *out);
jz_err jazz_node__peer__PeerState__query_update_for_subscription_with_opts_and_waker(jazz_node_PeerState *p, NS *n, jazz_protocol_SubscriptionKey s, const jazz_model_ValidatedQuery *shape,
    const jazz_model_Binding *binding, jazz_protocol_RegisterShapeOptions opts, const jz_waker *waker, bool *ready, bool *has, jazz_protocol_SyncMessage *out);
#undef NS

#endif
