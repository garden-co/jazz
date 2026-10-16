/* _ext_wasm-bindings.h: local mirror declarations of types and functions from
 * other units (groove, jazz_types, jazz_model, jazz_protocol, jazz_node,
 * jazz_db) that the wasm-bindings unit (crates/jazz-wasm) calls.
 * Layouts approximate the Rust fields; opaque where only pointers pass. */
#ifndef JAZZ_WASM_EXT_WASM_BINDINGS_H
#define JAZZ_WASM_EXT_WASM_BINDINGS_H
#include "jz_rt.h"
#include "idb_tree/idb_tree.h"

/* ======================= jazz_types ======================= */
typedef jz_uuid jazz_types_NodeUuid;
typedef jz_uuid jazz_types_RowUuid;
typedef u64 jazz_types_TxTime;
typedef u64 jazz_types_GlobalTime;
typedef struct jazz_types_AuthorSubject { u32 tag; const void *interned; } jazz_types_AuthorSubject;
extern const jazz_types_AuthorSubject jazz_types__ids__AuthorSubject__SYSTEM;
jz_slice jazz_types__ids__AuthorSubject__canonical(const jazz_types_AuthorSubject *a);
/* Err(AuthorSubjectError): message is its Display text */
jz_err jazz_types__ids__AuthorSubject__from_canonical(jz_slice canonical, jazz_types_AuthorSubject *out);
jz_err jazz_types__ids__AuthorSubject__from_untrusted_canonical(jz_slice canonical, jazz_types_AuthorSubject *out);
/* verify_client_runtime_author_at -> Result<AuthorSubject, String> */
jz_err jazz_types__identity__verify_client_runtime_author_at(jz_slice token, jz_slice app_id, jz_slice claimed_author, u64 now_seconds,
                                                             jazz_types_AuthorSubject *out);

/* ======================= groove ======================= */
enum { WB_GROOVE_V_LARGE = 8, WB_GROOVE_V_NULLABLE = 13 };
typedef struct groove_Value groove_Value;
struct groove_Value {
    u8 tag;
    union {
        groove_Value *nullable; /* Nullable(Option<Box<Value>>), NULL = None */
        void *large;            /* Large(Box<LargeValueRef>) */
        u8 raw[24];
    } v;
};
void groove__records__values__Value__drop(void *p);
typedef JZ_VEC(groove_Value) groove_ValueVec;
typedef struct groove_RecordDescriptor groove_RecordDescriptor;
typedef struct groove_BorrowedRecord { const groove_RecordDescriptor *descriptor; jz_slice raw; } groove_BorrowedRecord;
groove_BorrowedRecord groove__records__RecordDescriptor__bind(const groove_RecordDescriptor *d, jz_slice raw);
jz_err groove__records__BorrowedRecord__to_values(const groove_BorrowedRecord *r, groove_ValueVec *out);
/* RowCells = BTreeMap<String, Value>; also the claims maps */
extern const jz_btree_desc groove__STRING_VALUE_BTREE;

/* Storage implementations erased into the node's BoxedStorage */
typedef struct groove_BoxedStorage { const void *vt; void *self; } groove_BoxedStorage;
jz_err groove__storage__memory__MemoryStorage__new(const jz_slice *families, u32 n, groove_BoxedStorage *out);
jz_err groove__storage__idb__IdbStorage__open(idb_tree_PageStore store, const jz_slice *families, u32 n, groove_BoxedStorage *out);

/* groove::ivm::TerminalOperation */
enum { GROOVE_TPATH_COLLECTION = 0, GROOVE_TPATH_KEY = 1 };
typedef struct groove_TerminalPathSegment { u32 tag; jz_str collection; jz_bytes key; } groove_TerminalPathSegment;
enum { GROOVE_TEDIT_INSERT = 0, GROOVE_TEDIT_UPDATE = 1, GROOVE_TEDIT_REMOVE = 2, GROOVE_TEDIT_MOVE = 3 };
typedef struct groove_TerminalEdit { u32 tag; usize index; jz_bytes key; jz_bytes value; } groove_TerminalEdit;
typedef struct groove_TerminalOperation {
    const groove_RecordDescriptor *root_descriptor;
    jz_bytes root_key;
    JZ_VEC(groove_TerminalPathSegment) path;
    groove_TerminalEdit edit;
} groove_TerminalOperation;

/* ======================= jazz_model ======================= */
typedef struct jazz_model_TxId { jazz_types_TxTime time; jazz_types_NodeUuid node; } jazz_model_TxId;
typedef enum { JAZZ_DUR_NONE = 0, JAZZ_DUR_LOCAL = 1, JAZZ_DUR_GLOBAL = 2 } jazz_model_DurabilityTier;
enum {
    JAZZ_REJ_CLIENT_CLOCK_TOO_FAR_AHEAD, JAZZ_REJ_AUTHORIZATION_DENIED, JAZZ_REJ_EXCLUSIVE_CONFLICT,
    JAZZ_REJ_CAUSALITY_VIOLATION, JAZZ_REJ_CASCADE, JAZZ_REJ_MALFORMED_COMMIT
};
typedef struct jazz_model_RejectionReason { u32 tag; jazz_model_TxId root; jz_str message; } jazz_model_RejectionReason;
enum { JAZZ_FATE_PENDING = 0, JAZZ_FATE_ACCEPTED = 1, JAZZ_FATE_REJECTED = 2 };
typedef struct jazz_model_Fate { u32 tag; jazz_model_RejectionReason rejected; } jazz_model_Fate;
typedef struct jazz_model_TransactionId { u8 b[16]; } jazz_model_TransactionId;
typedef struct jazz_model_OpenTransactionId { u8 b[16]; } jazz_model_OpenTransactionId;
/* <OpenTransactionId as FromStr>::from_str -> Result<Self, String> */
jz_err jazz_model__model__transaction__OpenTransactionId__FromStr__from_str(jz_slice raw, jazz_model_OpenTransactionId *out);
void jazz_model__model__transaction__TransactionId__from_committed_tx(const jazz_model_TxId *tx, jazz_model_TransactionId *out);
void jazz_model__model__transaction__TransactionId__Display__fmt(const jazz_model_TransactionId *t, jz_str *out);

typedef struct jazz_model_JazzSchema jazz_model_JazzSchema; /* Arc-shared, opaque */
void jazz_model__schema__JazzSchema__column_families(const jazz_model_JazzSchema *s, JZ_VEC(jz_str) *out);
void jazz_model__schema__JazzSchema__drop(jazz_model_JazzSchema *s);
jz_err jazz_model__model__public_schema_convert__decode_public_schema_json(jz_slice bytes, jazz_model_JazzSchema **out);

/* model::branch */
typedef struct jazz_model_BranchSelector { jz_btree values; /* String -> BranchColumnValue(Vec<u8>) */ } jazz_model_BranchSelector;
extern const jz_btree_desc jazz_model__branch__STRING_BYTES_BTREE;
void jazz_model__model__branch__BranchSelector__drop(jazz_model_BranchSelector *s);
typedef struct jazz_model_SnapshotRef {
    jazz_types_NodeUuid owner;
    jazz_types_GlobalTime global_base;
    jazz_types_TxTime local_base;
    JZ_VEC(jazz_model_TxId) dots;
} jazz_model_SnapshotRef;
enum { JAZZ_BASE_CURRENT = 0, JAZZ_BASE_SNAPSHOT = 1 };
typedef struct jazz_model_BranchViewBase { u32 tag; jazz_model_BranchSelector branch; jazz_model_SnapshotRef snapshot; } jazz_model_BranchViewBase;
void jazz_model__model__branch__BranchViewBase__drop(jazz_model_BranchViewBase *b);

/* model::json_schema host validator traits */
typedef struct jazz_model_HostCompiledJsonSchemaVtbl {
    jz_err (*validate)(void *self, jz_slice instance_json, bool *has_reason, jz_str *reason);
    void (*drop)(void *self);
} jazz_model_HostCompiledJsonSchemaVtbl;
typedef struct jazz_model_HostCompiledJsonSchema { const jazz_model_HostCompiledJsonSchemaVtbl *vt; void *self; } jazz_model_HostCompiledJsonSchema;
typedef struct jazz_model_HostJsonSchemaValidatorVtbl {
    jz_err (*compile)(void *self, jz_slice schema_json, jazz_model_HostCompiledJsonSchema *out);
    void (*drop)(void *self);
} jazz_model_HostJsonSchemaValidatorVtbl;
typedef struct jazz_model_HostJsonSchemaValidator { const jazz_model_HostJsonSchemaValidatorVtbl *vt; void *self; } jazz_model_HostJsonSchemaValidator;
void jazz_model__model__json_schema__install_host_validator(jazz_model_HostJsonSchemaValidator validator);

/* ======================= jazz_protocol ======================= */
enum { JAZZ_RVS_CURRENT = 0, JAZZ_RVS_BRANCH_VIEW = 1, JAZZ_RVS_SNAPSHOT = 2 };
typedef struct jazz_protocol_ReadViewSpec {
    u32 source_tag;
    jazz_model_BranchSelector head;
    bool has_base;
    jazz_model_BranchViewBase base;
    jazz_model_SnapshotRef snapshot;
} jazz_protocol_ReadViewSpec;
void jazz_protocol__protocol__ReadViewSpec__drop(jazz_protocol_ReadViewSpec *r);
enum { JAZZ_ADVICE_ALLOWED = 0, JAZZ_ADVICE_DENIED = 1, JAZZ_ADVICE_UNKNOWN = 2 };
enum { JAZZ_PAA_INSERT = 0, JAZZ_PAA_READ = 1, JAZZ_PAA_UPDATE = 2, JAZZ_PAA_DELETE = 3 };
typedef struct jazz_protocol_PermissionAdviceAction {
    u32 tag;
    jz_str table;
    jz_btree cells; /* Insert cells / Update patch */
    jazz_types_RowUuid row;
} jazz_protocol_PermissionAdviceAction;
enum { JAZZ_SRR_UNSUPPORTED_SHAPE_CAPABILITY = 0, JAZZ_SRR_SHAPE_REGISTRATION_PENDING = 1, JAZZ_SRR_SERVER_FAILURE = 2,
       JAZZ_SRR_INVALID_AUTHORITY_SOURCE_CLOSURE = 3 };
typedef struct jazz_protocol_SubscribeRejectReason { u32 tag; jz_str detail; u32 code; jz_str transition; } jazz_protocol_SubscribeRejectReason;
void jazz_protocol__protocol__SubscribeServerFailureCode__Debug__fmt(u32 code, jz_str *out);
void jazz_protocol__protocol__SubscribeRejectReason__drop(jazz_protocol_SubscribeRejectReason *r);

typedef u64 jazz_protocol_WireFeatures;
#define JAZZ_WIRE_PROTOCOL_VERSION ((u16)6)
#define JAZZ_FEATURE_AUTHORIZATION_SCOPE_VIEWS ((u64)1 << 7)
#define JAZZ_MAX_WIRE_FRAME_BYTES (2u * 1024u * 1024u)
jazz_protocol_WireFeatures jazz_protocol__wire__current_wire_features(void);
jz_err jazz_protocol__wire__decode_websocket_frame_batch(jz_slice bytes, JZ_VEC(jz_bytes) *out);
jz_err jazz_protocol__wire__validate_frames_for_artifact_corpus(const jz_bytes *frames, u32 n, jazz_protocol_WireFeatures features);
typedef struct jazz_protocol_WireAuthorityEndpoint { jazz_types_NodeUuid node; u64 epoch; } jazz_protocol_WireAuthorityEndpoint;

/* ======================= jazz_node ======================= */
/* api_error::Error { code: ErrorCode, message } is carried as jz_err whose code is
 * the ErrorCode discriminant + 1 (always < 0x100); the msg is its Display text. */
#define JAZZ_NODE_ERROR_CODE_MAX 0x100u
enum { JAZZ_ERRCODE_PROTOCOL = 7 };
const char *jazz_node__node__api_error__ErrorCode__as_str(u32 code);
typedef struct jazz_node_CurrentRow jazz_node_CurrentRow;
void jazz_node__node__CurrentRow__encoded_record(const jazz_node_CurrentRow *r, const groove_RecordDescriptor **d, jz_slice *raw);
void jazz_node__node__CurrentRow__drop(void *p);
extern const u32 jazz_node__node__CurrentRow__SIZE;
typedef struct jazz_node_RelationSnapshot jazz_node_RelationSnapshot;
void jazz_node__node__RelationSnapshot__drop(jazz_node_RelationSnapshot *s);
typedef struct jazz_node_LargeValueStagingPolicy { u64 incoming_bytes_per_window; u64 window_ms; u64 max_age_ms; } jazz_node_LargeValueStagingPolicy;
void jazz_node__node__LargeValueStagingPolicy__Default__default(jazz_node_LargeValueStagingPolicy *out);
typedef struct jazz_node_ClientRelayScope { jz_str storage_owner; bool has_admitted_session; jazz_types_AuthorSubject admitted_session; } jazz_node_ClientRelayScope;
void jazz_node__node__relay_scope__ClientRelayScope__from_admitted_storage_owner(jz_str storage_owner, jazz_types_AuthorSubject author,
                                                                                jazz_node_ClientRelayScope *out);

/* ======================= jazz_db ======================= */
typedef struct jazz_db_Db jazz_db_Db;                 /* Rc<Db> payload */
typedef struct jazz_db_WriteHandle jazz_db_WriteHandle;
typedef struct jazz_db_PeerConnection jazz_db_PeerConnection; /* Rc<LocalMutex<PeerConnection>> */
typedef struct jazz_db_PeerIoPump jazz_db_PeerIoPump;
typedef struct jazz_db_SubscriptionStream jazz_db_SubscriptionStream;
typedef struct jazz_db_StreamingValueUpload jazz_db_StreamingValueUpload;
typedef struct jazz_db_PermissionAdviceFuture jazz_db_PermissionAdviceFuture;
typedef struct jazz_db_QueryAttachment jazz_db_QueryAttachment;
typedef struct jazz_db_BoxTransport jazz_db_BoxTransport; /* Box<dyn Transport> */
void jazz_db__db__BoxTransport__drop(jazz_db_BoxTransport *t);
void jazz_db__db__PermissionAdviceFuture__drop(jazz_db_PermissionAdviceFuture *f);
typedef struct jazz_db_SubscriptionOutputRow jazz_db_SubscriptionOutputRow;
typedef struct jazz_db_RemovedRow jazz_db_RemovedRow;

typedef struct jazz_db_DbIdentity { jazz_types_NodeUuid node; jazz_types_AuthorSubject author; } jazz_db_DbIdentity;
typedef struct jazz_db_DbConfig jazz_db_DbConfig;
jazz_db_DbConfig *jazz_db__db__config__DbConfig__new(jazz_model_JazzSchema *schema, groove_BoxedStorage storage, jazz_db_DbIdentity identity);
typedef struct jazz_db_RowIdSourceVtbl { jazz_types_RowUuid (*next_row_id)(void *self); void (*drop)(void *self); } jazz_db_RowIdSourceVtbl;
void jazz_db__db__config__DbConfig__with_id_source(jazz_db_DbConfig *c, const jazz_db_RowIdSourceVtbl *vt, void *source);
void *jazz_db__db__config__SeededRowIdSource__new(u64 seed);
extern const jazz_db_RowIdSourceVtbl jazz_db__db__config__SeededRowIdSource__RowIdSource__VTABLE;

jz_err jazz_db__db__lifecycle__Db__open(jazz_db_DbConfig *config, jazz_db_Db **out);
jz_err jazz_db__db__lifecycle__Db__open_history_complete(jazz_db_DbConfig *config, jazz_db_Db **out);
jz_err jazz_db__db__lifecycle__Db__open_with_backend_attribution(jazz_db_DbConfig *config, jazz_db_Db **out);
jz_err jazz_db__db__lifecycle__Db__open_history_complete_with_backend_attribution(jazz_db_DbConfig *config, jazz_db_Db **out);
jz_err jazz_db__db__lifecycle__Db__open_scope_isolated_client_relay(jazz_db_DbConfig *config, jazz_node_ClientRelayScope scope, jazz_db_Db **out);
jz_err jazz_db__db__lifecycle__Db__set_initial_sync_flush_cadence(jazz_db_Db *db, usize every /* InitialSyncFlushCadence::every */);
jz_err jazz_db__db__lifecycle__Db__restore_browser_relay_pending_uploads(jazz_db_Db *db);
void jazz_db__db__lifecycle__Db__set_deferred_local_persistence(jazz_db_Db *db, bool deferred);
void jazz_db__db__lifecycle__Db__drive_queued_mutation_once(jazz_db_Db *db);
jz_err jazz_db__db__lifecycle__Db__register_schema_view(jazz_db_Db *db, jazz_model_JazzSchema *schema, jazz_db_Db **out);
jazz_db_DbIdentity jazz_db__db__lifecycle__Db__identity(const jazz_db_Db *db);
jz_err jazz_db__db__lifecycle__Db__close(jazz_db_Db *db);
jz_err jazz_db__db__lifecycle__Db__tick(jazz_db_Db *db);
void jazz_db__db__lifecycle__Db__set_non_durable_client(jazz_db_Db *db);
void jazz_db__db__lifecycle__Db__set_large_value_staging_policy(jazz_db_Db *db, jazz_node_LargeValueStagingPolicy policy);
jz_err jazz_db__db__lifecycle__Db__evict_expired_staged_large_values(jazz_db_Db *db, usize *evicted);
jz_err jazz_db__db__lifecycle__Db__wait_for_pending_writes(jazz_db_Db *db, jazz_model_DurabilityTier tier);
jazz_types_TxTime jazz_db__db__lifecycle__Db__foreground_tx_time_high_water(jazz_db_Db *db);
void jazz_db__db__lifecycle__Db__seed_foreground_tx_time_high_water(jazz_db_Db *db, jazz_types_TxTime high_water);
jazz_db_PeerConnection *jazz_db__db__lifecycle__Db__connect_upstream(jazz_db_Db *db, jazz_db_BoxTransport *transport);
jz_err jazz_db__db__lifecycle__Db__accept_subscriber_with_claims_async(jazz_db_Db *db, jazz_db_BoxTransport *transport, jazz_types_AuthorSubject identity,
                                                                    jz_btree claims, jazz_db_PeerConnection **out);
bool jazz_db__db__lifecycle__Db__detach_connection(jazz_db_Db *db, jazz_db_PeerConnection *connection);
jazz_db_PermissionAdviceFuture *jazz_db__db__lifecycle__Db__request_permission_advice(jazz_db_Db *db, jazz_protocol_PermissionAdviceAction action);
void jazz_db__db__lifecycle__Db__cancel_permission_advice_request(jazz_db_Db *db, const u8 request_id[16]);
void jazz_db__db__PermissionAdviceFuture__request_id(const jazz_db_PermissionAdviceFuture *f, u8 out[16]);
u32 jazz_db__db__PermissionAdviceFuture__Future__await(jazz_db_PermissionAdviceFuture *f); /* consumes, -> PermissionAdvice */
/* queued_mutation_barrier: Ok(Err(e)) -> e, Err(Canceled) -> *cancelled */
jz_err jazz_db__db__lifecycle__Db__queued_mutation_barrier(jazz_db_Db *db, bool *cancelled);
/* enqueue_transaction_read(tx, future): the read closure runs as the owner turn admits it */
jz_err jazz_db__db__lifecycle__Db__enqueue_transaction_read(jazz_db_Db *db, jazz_model_OpenTransactionId tx, jz_err (*read)(void *ctx), void *ctx,
                                                           bool *cancelled);
typedef struct jazz_db_MutationErrorEvent jazz_db_MutationErrorEvent;
typedef struct jazz_db_MutationErrorCallback { void (*call)(void *ctx, const jazz_db_MutationErrorEvent *event); void *ctx; void (*drop)(void *ctx); } jazz_db_MutationErrorCallback;
void jazz_db__db__lifecycle__Db__on_mutation_error(jazz_db_Db *db, jazz_db_MutationErrorCallback *callback /* Rc */);

/* TickScheduler trait object */
enum { JAZZ_TICK_IMMEDIATE = 0, JAZZ_TICK_DEFERRED = 1, JAZZ_TICK_AFTER_CURRENT_TURN = 2 };
typedef struct jazz_db_TickSchedulerVtbl {
    void (*schedule_tick)(void *self, u32 urgency);
    void (*schedule_tick_after)(void *self, u64 delay_ms);
    bool (*query_runtime_waker)(void *self, jz_waker *out);
    void (*drop)(void *self);
} jazz_db_TickSchedulerVtbl;
void jazz_db__db__lifecycle__Db__set_tick_scheduler(jazz_db_Db *db, const jazz_db_TickSchedulerVtbl *vt, void *scheduler /* Rc, NULL = None */);

typedef struct jazz_db_WriteWaitOptions { jazz_model_DurabilityTier tier; bool observe_only; } jazz_db_WriteWaitOptions;
void jazz_db__db__lifecycle__Db__wait_for_write_with(jazz_db_Db *db, const jazz_db_WriteHandle *write, jazz_db_WriteWaitOptions options,
                                                      void (*callback)(void *ctx, jz_err result, const jazz_model_TxId *tx), void *ctx);
/* WriteHandle */
jazz_types_RowUuid jazz_db__db__WriteHandle__row_uuid(const jazz_db_WriteHandle *w);
jazz_model_TxId jazz_db__db__WriteHandle__mergeable_tx_id(const jazz_db_WriteHandle *w);
void jazz_db__db__WriteHandle__drop(jazz_db_WriteHandle *w);
typedef struct jazz_db_WriteState {
    jazz_model_Fate fate;
    bool has_global_time;
    jazz_types_GlobalTime global_time;
    jazz_model_DurabilityTier durability;
} jazz_db_WriteState;
/* WriteHandle::write_state().poll(noop): returns false while Pending */
bool jazz_db__db__WriteHandle__write_state_poll(const jazz_db_WriteHandle *w, jz_err *result, jazz_db_WriteState *out);
void jazz_db__db__WriteState__drop(jazz_db_WriteState *s);

/* MutationErrorEvent (Serialize, camelCase) */
enum { JAZZ_TXKIND_MERGEABLE = 0, JAZZ_TXKIND_EXCLUSIVE = 1 };
struct jazz_db_MutationErrorEvent {
    jz_str code;
    jz_str reason;
    struct {
        jazz_model_TransactionId transaction_id;
        u32 kind;
        bool sealed;
        struct { jazz_model_TransactionId transaction_id; jz_str code; jz_str reason; } latest_settlement; /* TransactionFate::Rejected */
    } transaction;
};

/* write targets / options */
enum { JAZZ_WID_DATABASE = 0, JAZZ_WID_SESSION = 1, JAZZ_WID_ATTRIBUTION = 2 };
typedef struct jazz_db_WriteIdentity { u32 tag; jazz_types_AuthorSubject author; } jazz_db_WriteIdentity;
enum { JAZZ_EWT_ROOT = 0, JAZZ_EWT_BRANCH = 1 };
typedef struct jazz_db_ExactWriteTarget { u32 tag; jazz_model_BranchSelector branch; } jazz_db_ExactWriteTarget;
enum { JAZZ_WT_ROOT = 0, JAZZ_WT_BRANCH_VIEW = 1 };
typedef struct jazz_db_WriteTarget { u32 tag; jazz_model_BranchSelector head; bool has_base; jazz_model_BranchViewBase base; } jazz_db_WriteTarget;
typedef struct jazz_db_InsertOptions {
    bool has_row_id; jazz_types_RowUuid row_id;
    jazz_db_WriteIdentity identity;
    jazz_db_ExactWriteTarget target;
    bool has_updated_at_ms; u64 updated_at_ms;
} jazz_db_InsertOptions;
typedef struct jazz_db_UpdateOptions {
    jazz_db_WriteIdentity identity;
    jazz_db_WriteTarget target;
    bool has_updated_at_ms; u64 updated_at_ms;
} jazz_db_UpdateOptions;
typedef jazz_db_UpdateOptions jazz_db_UpsertOptions;
typedef jazz_db_UpdateOptions jazz_db_DeleteOptions;
typedef struct jazz_db_RestoreOptions {
    jazz_db_WriteIdentity identity;
    jazz_db_ExactWriteTarget target;
    bool has_updated_at_ms; u64 updated_at_ms;
} jazz_db_RestoreOptions;

/* LargeValueUpdate (serde tag = "kind", snake_case) */
enum { JAZZ_LVP_BYTES = 0, JAZZ_LVP_TEXT_UTF16 = 1, JAZZ_LVP_TEXT_UTF8 = 2 };
typedef struct jazz_db_LargeValueUpdatePage { u32 tag; u64 from; u64 to; } jazz_db_LargeValueUpdatePage;
typedef struct jazz_db_LargeValueUpdateSplice { u64 at; u64 delete_; jz_bytes insert; } jazz_db_LargeValueUpdateSplice;
typedef struct jazz_db_JsonSetEdit { jz_str at; jz_json *value; } jazz_db_JsonSetEdit;
enum { JAZZ_LVU_SPLICE = 0, JAZZ_LVU_JSON_SET = 1 };
typedef struct jazz_db_LargeValueUpdate {
    u32 tag;
    jz_str column;
    jazz_db_LargeValueUpdatePage within;
    JZ_VEC(jazz_db_LargeValueUpdateSplice) splices;
    JZ_VEC(jazz_db_JsonSetEdit) edits;
} jazz_db_LargeValueUpdate;
typedef JZ_VEC(jazz_db_LargeValueUpdate) jazz_db_LargeValueUpdateVec;

/* mutations.rs / transactions.rs */
jz_err jazz_db__db__mutations__Db__enqueue_insert(jazz_db_Db *db, jz_str table, jz_btree cells, jazz_db_InsertOptions options, jazz_db_WriteHandle **out);
jz_err jazz_db__db__mutations__Db__enqueue_update(jazz_db_Db *db, jz_str table, jazz_types_RowUuid row, jz_btree patch, jazz_db_UpdateOptions options,
                                                  jazz_db_WriteHandle **out);
jz_err jazz_db__db__mutations__Db__enqueue_upsert(jazz_db_Db *db, jz_str table, jazz_types_RowUuid row, jz_btree cells, jazz_db_UpsertOptions options,
                                                  jazz_db_WriteHandle **out);
jz_err jazz_db__db__mutations__Db__enqueue_delete(jazz_db_Db *db, jz_str table, jazz_types_RowUuid row, jazz_db_DeleteOptions options,
                                                  jazz_db_WriteHandle **out);
jz_err jazz_db__db__mutations__Db__enqueue_restore(jazz_db_Db *db, jz_str table, jazz_types_RowUuid row, bool has_cells, jz_btree cells,
                                                   jazz_db_RestoreOptions options, jazz_db_WriteHandle **out);
jz_err jazz_db__db__mutations__Db__enqueue_large_value_update(jazz_db_Db *db, jz_str table, jazz_types_RowUuid row, jz_btree patch,
                                                              jazz_db_LargeValueUpdateVec mutations, bool has_updated_at_ms, u64 updated_at_ms,
                                                              jazz_db_WriteHandle **out);
void jazz_db__db__mutations__Db__set_identity_claims(jazz_db_Db *db, jazz_types_AuthorSubject identity, jz_btree claims);
void jazz_db__db__mutations__Db__set_identity_claims_async(jazz_db_Db *db, jazz_types_AuthorSubject identity, jz_btree claims);
jz_err jazz_db__db__mutations__Db__can_insert(jazz_db_Db *db, jz_slice table, jz_btree cells, u32 *advice);
jz_err jazz_db__db__mutations__Db__local_current_row(jazz_db_Db *db, jz_slice table, jazz_types_RowUuid row, bool *some, jazz_node_CurrentRow **out);
jz_err jazz_db__db__mutations__Db__begin_streaming_value_upload(jazz_db_Db *db, jz_slice table, const jz_btree *cells, jz_slice column,
                                                                jazz_db_StreamingValueUpload **out);
jz_err jazz_db__db__mutations__Db__push_streaming_value_upload(jazz_db_Db *db, jazz_db_StreamingValueUpload *upload, jz_slice bytes);
enum { JAZZ_SMK_INSERT = 0, JAZZ_SMK_UPDATE = 1, JAZZ_SMK_UPSERT = 2 };
jz_err jazz_db__db__mutations__Db__finish_streaming_value_upload(jazz_db_Db *db, jazz_db_StreamingValueUpload *upload, u32 mutation, jz_slice table,
                                                                 jazz_types_RowUuid row, jz_btree cells, jz_slice column, jazz_db_WriteIdentity identity,
                                                                 bool has_now_ms, u64 now_ms, bool has_head, jazz_model_BranchSelector head,
                                                                 bool has_base, jazz_model_BranchViewBase base, jazz_db_WriteHandle **out);
jz_err jazz_db__db__mutations__Db__abort_streaming_value_upload(jazz_db_Db *db, jazz_db_StreamingValueUpload *upload);

jz_err jazz_db__db__transactions__Db__enqueue_begin_exclusive(jazz_db_Db *db, jazz_model_OpenTransactionId id, const jazz_types_AuthorSubject *author,
                                                              const jazz_types_AuthorSubject *attribution);
jz_err jazz_db__db__transactions__Db__enqueue_begin_mergeable(jazz_db_Db *db, jazz_model_OpenTransactionId id, const jazz_types_AuthorSubject *author,
                                                              const jazz_types_AuthorSubject *attribution);
void jazz_db__db__transactions__Db__enqueue_abandon_transaction_handle(jazz_db_Db *db, jazz_model_OpenTransactionId id);
jz_err jazz_db__db__transactions__Db__enqueue_commit_exclusive_handle_at_ms(jazz_db_Db *db, jazz_model_OpenTransactionId id, u64 now_ms,
                                                                            jazz_db_WriteHandle **out);
jz_err jazz_db__db__transactions__Db__enqueue_commit_mergeable_handle_at_ms(jazz_db_Db *db, jazz_model_OpenTransactionId id, u64 now_ms,
                                                                            jazz_db_WriteHandle **out);
jz_err jazz_db__db__transactions__Db__enqueue_transaction_insert(jazz_db_Db *db, jazz_model_OpenTransactionId id, jz_str table, jz_btree cells,
                                                                 jazz_db_InsertOptions options, jazz_types_RowUuid *out);
jz_err jazz_db__db__transactions__Db__enqueue_transaction_update(jazz_db_Db *db, jazz_model_OpenTransactionId id, jz_str table, jazz_types_RowUuid row,
                                                                 jz_btree patch, jazz_db_UpdateOptions options);
jz_err jazz_db__db__transactions__Db__enqueue_transaction_large_value_update(jazz_db_Db *db, jazz_model_OpenTransactionId id, jz_str table,
                                                                             jazz_types_RowUuid row, jz_btree patch, jazz_db_LargeValueUpdateVec mutations,
                                                                             bool has_updated_at_ms, u64 updated_at_ms);
jz_err jazz_db__db__transactions__Db__enqueue_transaction_upsert(jazz_db_Db *db, jazz_model_OpenTransactionId id, jz_str table, jazz_types_RowUuid row,
                                                                 jz_btree cells, jazz_db_UpsertOptions options);
jz_err jazz_db__db__transactions__Db__enqueue_transaction_delete(jazz_db_Db *db, jazz_model_OpenTransactionId id, jz_str table, jazz_types_RowUuid row,
                                                                 jazz_db_DeleteOptions options);
jz_err jazz_db__db__transactions__Db__enqueue_transaction_restore(jazz_db_Db *db, jazz_model_OpenTransactionId id, jz_str table, jazz_types_RowUuid row,
                                                                  bool has_cells, jz_btree cells, jazz_db_RestoreOptions options);

/* reads.rs / subscriptions.rs */
enum { JAZZ_READ_TIER_LOCAL_FIRST = 0, JAZZ_READ_TIER_REMOTE = 1, JAZZ_READ_TIER_LOCAL_ONLY = 2 };
enum { JAZZ_FIRST_LOAD_DELIVER = 0, JAZZ_FIRST_LOAD_WAIT_FOR_REMOTE = 1 };
typedef struct jazz_db_FirstLoad { u32 tag; u64 timeout_ms; } jazz_db_FirstLoad;
typedef struct jazz_db_ReadOpts {
    u32 tier;
    bool include_deleted;
    jazz_protocol_ReadViewSpec read_view;
    jazz_db_FirstLoad first_load;
} jazz_db_ReadOpts;
void jazz_db__db__ReadOpts__Default__default(jazz_db_ReadOpts *out);
void jazz_db__db__ReadOpts__drop(jazz_db_ReadOpts *o);
typedef struct jazz_db_RequestScope { bool some; jazz_types_AuthorSubject author; jz_btree claims; } jazz_db_RequestScope;
enum { JAZZ_SRR_ROWS = 0, JAZZ_SRR_RELATION = 1 };
typedef struct jazz_db_SerializedReadResult {
    u32 tag;
    JZ_VEC(jazz_node_CurrentRow *) rows;
    jazz_node_RelationSnapshot *relation;
} jazz_db_SerializedReadResult;
void jazz_db__db__SerializedReadResult__drop(jazz_db_SerializedReadResult *r);
jz_err jazz_db__db__reads__Db__all_serialized_query(jazz_db_Db *db, jz_slice query, jazz_db_ReadOpts opts, bool has_open_tx,
                                                    jazz_model_OpenTransactionId open_tx, jazz_db_RequestScope request_scope,
                                                    const jazz_types_AuthorSubject *author, bool require_coverage,
                                                    bool (*coverage_expired)(void *ctx), void *expired_ctx,
                                                    void (*release_coverage)(void *ctx, jazz_db_QueryAttachment *attachment), void *release_ctx,
                                                    jazz_db_SerializedReadResult *out);
enum { JAZZ_SSA_CLIENT_LOCAL = 0, JAZZ_SSA_TRUSTED_SERVING = 1, JAZZ_SSA_TRUSTED_CLIENT = 2 };
typedef struct jazz_db_SerializedSubscriptionAuthorization { u32 tag; jazz_types_AuthorSubject author; } jazz_db_SerializedSubscriptionAuthorization;
jz_err jazz_db__db__subscriptions__Db__subscribe_serialized_query(jazz_db_Db *db, jz_slice query, jazz_db_ReadOpts opts, jazz_db_RequestScope request_scope,
                                                                  jazz_db_SerializedSubscriptionAuthorization authorization,
                                                                  jazz_db_SubscriptionStream **out);
void jazz_db__db__subscriptions__Db__detach_query(jazz_db_Db *db, jazz_db_QueryAttachment *attachment);

enum { JAZZ_SUB_EVENT_DELTA = 0, JAZZ_SUB_EVENT_REJECTED = 1, JAZZ_SUB_EVENT_CLOSED = 2 };
typedef struct jazz_db_SubscriptionEvent {
    u32 tag;
    bool reset;
    bool publishable;
    JZ_VEC(jazz_db_SubscriptionOutputRow *) added;
    JZ_VEC(jazz_db_SubscriptionOutputRow *) updated;
    JZ_VEC(jazz_db_RemovedRow *) removed;
    JZ_VEC(groove_TerminalOperation) terminal_operations;
    bool settled;
    jazz_model_DurabilityTier tier;
    jazz_protocol_SubscribeRejectReason reason;
} jazz_db_SubscriptionEvent;
void jazz_db__db__SubscriptionEvent__drop(jazz_db_SubscriptionEvent *e);
/* <SubscriptionStream as Stream>::poll_next; false = end of stream */
bool jazz_db__db__SubscriptionStream__Stream__next(jazz_db_SubscriptionStream *s, jazz_db_SubscriptionEvent *out);
void jazz_db__db__SubscriptionStream__drop(jazz_db_SubscriptionStream *s);
/* BindingHydrationError: code JAZZ_BINDING_RETRYABLE carries retry_after_ms; anything else is Error(e) */
#define JAZZ_BINDING_RETRYABLE 0xd00du
jz_err jazz_db__db__reads__Db__hydrate_subscription_event_for_binding_outcome(jazz_db_Db *db, jazz_db_SubscriptionEvent *event, u32 *retry_after_ms);

/* first_load.rs */
enum { JAZZ_LINK_NO_SERVER = 0, JAZZ_LINK_ATTEMPTING = 1, JAZZ_LINK_LIVE = 2, JAZZ_LINK_FAILED = 3 };
bool jazz_db__db__first_load__RemoteLinkHint__from_host_str(jz_slice value, u32 *out);
void jazz_db__db__first_load__Db__set_remote_link_hint(jazz_db_Db *db, u32 hint);

/* peer_connection.rs / db.rs PeerIoPump */
typedef struct jazz_db_DbTickStats { usize subscription_events; usize remote_sync_applied; } jazz_db_DbTickStats;
jazz_db_PeerIoPump *jazz_db__db__peer_connection__PeerConnection__io_pump(jazz_db_PeerConnection *c);
void jazz_db__db__peer_connection__PeerConnection__update_authenticated_session_claims(jazz_db_PeerConnection *c, jz_btree claims);
jz_err jazz_db__db__peer_connection__PeerConnection__tick(jazz_db_PeerConnection *c, jazz_db_DbTickStats *out);
void jazz_db__db__PeerConnection__rc_release(jazz_db_PeerConnection *c);
jazz_db_PeerIoPump *jazz_db__db__PeerIoPump__clone(const jazz_db_PeerIoPump *p);
void jazz_db__db__PeerIoPump__drop(jazz_db_PeerIoPump *p);
/* Result<Option<Vec<u8>>, String> */
jz_err jazz_db__db__PeerIoPump__route_incoming_wire_frame(jazz_db_PeerIoPump *p, jz_bytes frame, bool *some, jz_bytes *out);
jz_err jazz_db__db__PeerIoPump__take_outbound_wire_frames(jazz_db_PeerIoPump *p, usize max_frames, usize max_bytes, JZ_VEC(jz_bytes) *out);
bool jazz_db__db__PeerIoPump__incomplete_receive_timeout_ms(jazz_db_PeerIoPump *p, u64 *out);
jz_err jazz_db__db__PeerIoPump__expire_incomplete_receive(jazz_db_PeerIoPump *p);
void jazz_db__db__PeerIoPump__disconnect(jazz_db_PeerIoPump *p);
void jazz_db__db__PeerIoPump__outbound_ready(jazz_db_PeerIoPump *p); /* .await */
void jazz_db__db__PeerIoPump__set_trace_enabled(jazz_db_PeerIoPump *p, bool enabled);
typedef struct jazz_db_PeerIoTraceEntry {
    const char *event;
    const char *role;
    u64 connection;
    u64 request_id;
    u8 remaining_hops;
    jz_str object_hash;
    jz_str locator_fingerprint;
    const char *response;      /* Option<&'static str> */
    const char *storage_error; /* Option<&'static str> */
} jazz_db_PeerIoTraceEntry;
void jazz_db__db__PeerIoPump__take_trace(jazz_db_PeerIoPump *p, JZ_VEC(jazz_db_PeerIoTraceEntry) *out);
void jazz_db__db__PeerIoTraceEntry__drop(void *p);

/* wire_transport.rs */
typedef struct jazz_db_WireTransportVtbl {
    jz_err (*send_frame)(void *self, jz_bytes frame);
    bool (*try_recv_frame)(void *self, jz_bytes *out);
    void (*drop)(void *self);
} jazz_db_WireTransportVtbl;
typedef struct jazz_db_ConnectionSessionContext {
    jazz_protocol_WireAuthorityEndpoint local;
    bool has_remote;
    jazz_protocol_WireAuthorityEndpoint remote;
    jazz_types_AuthorSubject link_identity;
    jazz_protocol_WireFeatures negotiated_features;
} jazz_db_ConnectionSessionContext;
jazz_db_BoxTransport *jazz_db__db__wire_transport__WireTransportAdapter__new(const jazz_db_WireTransportVtbl *vt, void *inner, u16 protocol_version,
                                                                             jazz_protocol_WireFeatures features, const void *session /* None */);
jazz_db_BoxTransport *jazz_db__db__wire_transport__WireTransportAdapter__new_with_session_context(
    const jazz_db_WireTransportVtbl *vt, void *inner, u16 protocol_version, jazz_protocol_WireFeatures features, const void *session,
    const jazz_db_ConnectionSessionContext *session_context /* NULL = None */);

/* binding_args.rs: Result<_, String> */
jz_err jazz_db__binding_args__row_uuid(jz_slice bytes, jazz_types_RowUuid *out);
jz_err jazz_db__binding_args__checked_u64(f64 value, jz_slice name, u64 *out);
jz_err jazz_db__binding_args__author_subject(jz_slice bytes, jazz_types_AuthorSubject *out);
jz_err jazz_db__binding_args__durability_tier(jz_slice tier, jazz_model_DurabilityTier *out);
jz_err jazz_db__binding_args__read_tier(jz_slice tier, u32 *out);
jz_err jazz_db__binding_args__policy_claims(const jazz_types_AuthorSubject *author, jz_json *claims, jz_btree *out);
/* binding_codec.rs */
extern const char jazz_db__binding_codec__BINDING_CODEC_GOLDEN_FIXTURE[];
jz_err jazz_db__binding_codec__decode_named_cells(jz_slice bytes, jz_btree *out);
jz_err jazz_db__binding_codec__encode_rows(jazz_node_CurrentRow *const *rows, u32 n, jz_bytes *out);
jz_err jazz_db__binding_codec__encode_relation_snapshot(const jazz_node_RelationSnapshot *s, jz_bytes *out);
jz_err jazz_db__binding_codec__encode_subscription_delta(jazz_db_SubscriptionOutputRow *const *added, u32 na, jazz_db_SubscriptionOutputRow *const *updated,
                                                         u32 nu, jazz_db_RemovedRow *const *removed, u32 nr, jz_bytes *out);
/* Rc<Db> */
jazz_db_Db *jazz_db__db__Db__rc_clone(jazz_db_Db *db);
void jazz_db__db__Db__rc_release(jazz_db_Db *db);
#endif
