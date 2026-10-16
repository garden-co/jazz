/* db.h: types defined in crates/jazz/layers/db/src/db.rs (and shared by its
 * db-b submodules). Grown file by file. */
#ifndef JAZZ_DB_DB_H
#define JAZZ_DB_DB_H
#include "_ext_db-b.h"
#include "result_tree.h"
#include "positional_order.h"

/* api_error::ErrorCode (+1: 0 is success in jz_err) */
enum {
    JAZZ_EC_SCHEMA = 1, JAZZ_EC_QUERY, JAZZ_EC_WRITE_REJECTED, JAZZ_EC_TRANSACTION_CONFLICT, JAZZ_EC_STORAGE,
    JAZZ_EC_PROTOCOL, JAZZ_EC_BACKPRESSURE, JAZZ_EC_NOT_OBSERVED, JAZZ_EC_HISTORICAL_READ_REQUIRES_SERVER,
};

typedef enum { JAZZ_DB_READ_LOCAL_FIRST = 0, JAZZ_DB_READ_REMOTE = 1, JAZZ_DB_READ_LOCAL_ONLY = 2 } jazz_db_ReadTier;

/* RemovedRow / SubscriptionOutputRow (db.rs) */
typedef struct jazz_db_RemovedRow {
    jz_str table;
    jazz_types_RowUuid row_uuid;
    jazz_types_OutputOccurrenceId occurrence_id;
    u32 index;
} jazz_db_RemovedRow;
typedef struct jazz_db_SubscriptionOutputRow {
    jazz_types_OutputOccurrenceId occurrence_id;
    jazz_node_CurrentRow row;
    bool has_previous_index;
    u32 previous_index;
    u32 index;
} jazz_db_SubscriptionOutputRow;
void jazz_db__db__RemovedRow__drop(void *p);
void jazz_db__db__RemovedRow__clone(void *dst, const void *src);
void jazz_db__db__SubscriptionOutputRow__drop(void *p);
void jazz_db__db__SubscriptionOutputRow__clone(void *dst, const void *src);

#include "db/wire_transport.h"
#include "db/config.h"

/* ---------- peer chunk routing (db.rs:98-1447) ---------- */
#define JAZZ_DB_DEFAULT_CHUNK_FORWARD_HOPS 8u
#define JAZZ_DB_MAX_PENDING_CHUNK_DEMANDS 4096u
#define JAZZ_DB_MAX_RELAY_CHUNK_OBLIGATIONS JAZZ_DB_MAX_PENDING_CHUNK_DEMANDS
#define JAZZ_DB_MAX_CHUNK_RELAY_TRACE_EVENTS_PER_CONNECTION 64u

typedef struct jazz_db_PeerIoTraceEntry {
    const char *event;
    const char *role;
    u64 connection;
    u64 request_id;
    u8 remaining_hops;
    jz_str object_hash;
    jz_str locator_fingerprint;
    const char *response;      /* Option<&'static str>: NULL = None */
    const char *storage_error; /* Option<&'static str> */
} jazz_db_PeerIoTraceEntry;
void jazz_db__db__PeerIoTraceEntry__drop(void *p);
void jazz_db__db__PeerIoTraceEntry__clone(void *dst, const void *src);
typedef JZ_VEC(jazz_db_PeerIoTraceEntry) jazz_db_PeerIoTraceVec;

typedef enum { JAZZ_CDW_LOCAL, JAZZ_CDW_RELAY } jazz_db_ChunkDemandWaiterTag;
typedef struct jazz_db_ChunkDemandWaiter {
    u32 tag;
    u64 waiter_id; jz_chan *sender;    /* Local: oneshot::Sender<Result<Bytes, ChunkError>> */
    u64 connection; u64 request_id;    /* Relay */
} jazz_db_ChunkDemandWaiter;
typedef JZ_VEC(jazz_db_ChunkDemandWaiter) jazz_db_ChunkDemandWaiterVec;
typedef struct jazz_db_PendingChunkDemand { u64 upstream_id; u8 remaining_hops; jazz_db_ChunkDemandWaiterVec waiters; } jazz_db_PendingChunkDemand;
typedef JZ_VEC(jazz_protocol_ChunkRequestEntry) jazz_db_ChunkRequestEntryVec;
typedef JZ_VEC(jazz_protocol_ChunkResponseEntry) jazz_db_ChunkResponseEntryVec;

typedef struct jazz_db_ChunkDemandState {
    u64 next_request_id;
    u64 next_waiter_id;
    usize relay_chunk_obligations;
    jz_btree pending_by_chunk;          /* ChunkRequest -> PendingChunkDemand */
    jz_btree chunk_by_upstream_id;      /* u64 -> ChunkRequest */
    jz_deque outbound;                  /* ChunkRequestEntry */
    jz_btree relay_responses;           /* u64 -> Vec<ChunkResponseEntry> */
    jz_btree inflight_relay_responses;  /* u64 -> VecDeque<Vec<ChunkResponseEntry>> */
    jz_btree outbound_wakers;           /* u64 -> jz_waker */
    jz_btree disconnected_connections;  /* BTreeSet<u64> */
    bool has_upstream_connection; u64 upstream_connection;
    jz_btree upstream_connections;      /* BTreeSet<u64> */
    u64 completion_generation;
    jz_btree trace_by_connection;       /* u64 -> VecDeque<PeerIoTraceEntry> */
    jz_btree traced_connections;        /* BTreeSet<u64> */
} jazz_db_ChunkDemandState;

/* PeerChunkResolver { state: Rc<RefCell<ChunkDemandState>> } */
typedef struct jazz_db_PeerChunkResolver { jazz_db_ChunkDemandState *state; } jazz_db_PeerChunkResolver;
void jazz_db__db__PeerChunkResolver__default(jazz_db_PeerChunkResolver *out);
jazz_db_PeerChunkResolver jazz_db__db__PeerChunkResolver__clone(const jazz_db_PeerChunkResolver *r);
void jazz_db__db__PeerChunkResolver__drop(jazz_db_PeerChunkResolver *r);
bool jazz_db__db__PeerChunkResolver__has_pending_local_demand(const jazz_db_PeerChunkResolver *r);
u64 jazz_db__db__PeerChunkResolver__completion_generation(const jazz_db_PeerChunkResolver *r);
void jazz_db__db__PeerChunkResolver__fail_local_demand_for_close(const jazz_db_PeerChunkResolver *r);
groove_ChunkFuture jazz_db__db__PeerChunkResolver__MissingChunkResolver__resolve(const void *self, groove_ChunkRequest request);
extern const groove_MissingChunkResolverVt jazz_db_peer_chunk_resolver_vt;

typedef enum { JAZZ_PIO_UPSTREAM, JAZZ_PIO_SUBSCRIBER } jazz_db_PeerIoPumpRole;
typedef struct jazz_db_RcBoolCell { bool v; } jazz_db_RcBoolCell;
typedef struct jazz_db_RcMessageCell { bool has; jazz_protocol_SyncMessage m; } jazz_db_RcMessageCell;

typedef struct jazz_db_PeerIoPump {
    jazz_db_PeerChunkResolver resolver;
    groove_LocalChunkReader local_chunks;
    u64 connection;
    u32 role;
    jazz_protocol_WireInboundContext *wire_inbound_context; /* Option<Rc<..>> */
    jazz_db_SharedAuxiliaryEndpoint *auxiliary_endpoint;    /* Option<Arc<Mutex<..>>> */
    jazz_db_RcBoolCell *wire_reservation_active;
    jazz_db_RcMessageCell *wire_outbound_obligation;
    jazz_db_RcBoolCell *canonical_credit_progress;
} jazz_db_PeerIoPump;

typedef struct jazz_db_ReservedOutboundWireFrame { jazz_db_PeerIoPump pump; jz_bytes frame; bool credit_grant; } jazz_db_ReservedOutboundWireFrame;
typedef struct jazz_db_PeerIoOutboundReady { jazz_db_PeerIoPump pump; } jazz_db_PeerIoOutboundReady;

#define PIO_FN(n) jazz_db__db__PeerIoPump__##n
void PIO_FN(new)(jazz_db_PeerChunkResolver resolver, groove_LocalChunkReader local_chunks, u64 connection, u32 role, jazz_protocol_WireInboundContext *wire_inbound_context, jazz_db_PeerIoPump *out);
jazz_db_PeerIoPump PIO_FN(clone)(const jazz_db_PeerIoPump *p);
void PIO_FN(drop)(jazz_db_PeerIoPump *p);
void PIO_FN(with_shared_auxiliary_endpoint)(jazz_db_PeerIoPump *self, jazz_db_SharedAuxiliaryEndpoint *endpoint);
void PIO_FN(claim_binding_output)(const jazz_db_PeerIoPump *self);
bool PIO_FN(incomplete_receive_timeout_ms)(const jazz_db_PeerIoPump *self, u64 *out);
jz_err PIO_FN(expire_incomplete_receive)(const jazz_db_PeerIoPump *self);
/* Ok(()) -> true; Err(message) -> false and message handed back in *back */
bool PIO_FN(route_incoming)(const jazz_db_PeerIoPump *self, jazz_protocol_SyncMessage message, jazz_protocol_SyncMessage *back);
jz_err PIO_FN(route_incoming_payload)(const jazz_db_PeerIoPump *self, jz_bytes payload, bool *has, jz_bytes *out);
jz_err PIO_FN(route_incoming_wire_frame)(const jazz_db_PeerIoPump *self, jz_bytes frame, bool *has, jz_bytes *out);
jz_err PIO_FN(take_outbound_wire_frame)(const jazz_db_PeerIoPump *self, bool *has, jz_bytes *out);
jz_err PIO_FN(reserve_outbound_wire_frame)(const jazz_db_PeerIoPump *self, bool *has, jazz_db_ReservedOutboundWireFrame *out);
typedef JZ_VEC(jz_bytes) jazz_db_BytesVec;
jz_err PIO_FN(take_outbound_wire_frames)(const jazz_db_PeerIoPump *self, usize max_frames, usize max_bytes, jazz_db_BytesVec *out);
bool PIO_FN(take_outbound)(const jazz_db_PeerIoPump *self, usize limit, jazz_protocol_SyncMessage *out);
jz_err PIO_FN(send_outbound)(const jazz_db_PeerIoPump *self, jazz_db_Transport *transport, usize limit, bool *sent);
void PIO_FN(acknowledge_outbound)(const jazz_db_PeerIoPump *self, const jazz_protocol_SyncMessage *message);
jz_err PIO_FN(take_outbound_payload)(const jazz_db_PeerIoPump *self, usize limit, bool *has, jz_bytes *out);
jazz_db_PeerIoOutboundReady PIO_FN(outbound_ready)(const jazz_db_PeerIoPump *self);
bool PIO_FN(outbound_is_ready)(const jazz_db_PeerIoPump *self);
void PIO_FN(disconnect)(const jazz_db_PeerIoPump *self);
bool PIO_FN(is_disconnected)(const jazz_db_PeerIoPump *self);
jazz_db_PeerIoTraceVec PIO_FN(take_trace)(const jazz_db_PeerIoPump *self);
void PIO_FN(set_trace_enabled)(const jazz_db_PeerIoPump *self, bool enabled);
jz_bytes jazz_db__db__ReservedOutboundWireFrame__take_frame(jazz_db_ReservedOutboundWireFrame *r);
void jazz_db__db__ReservedOutboundWireFrame__commit(jazz_db_ReservedOutboundWireFrame *r);
void jazz_db__db__ReservedOutboundWireFrame__drop(jazz_db_ReservedOutboundWireFrame *r);
bool jazz_db__db__PeerIoOutboundReady__poll(jazz_db_PeerIoOutboundReady *f, const jz_waker *cx);

/* ---------- facade state types (db.rs:1448-3290) ---------- */
/* SharedNodeState = Rc<LocalMutex<NodeState>> */
typedef struct jazz_db_SharedNodeState { bool locked; jazz_node_NodeState *state; } jazz_db_SharedNodeState;
/* lock().await in the synchronous port */
jazz_node_NodeState *jazz_db__db__SharedNodeState__lock(jazz_db_SharedNodeState *s);
void jazz_db__db__SharedNodeState__unlock(jazz_db_SharedNodeState *s);
/* LocalMutexBorrow::borrow / borrow_mut: try_lock or panic */
jazz_node_NodeState *jazz_db__db__LocalMutexBorrow__borrow(jazz_db_SharedNodeState *s, const char *caller);

/* Result<(), Error> slot shared between oneshot sender and Shared future */
typedef struct jazz_db_SharedResultCell { bool ready; jz_err result; bool sender_dropped; } jazz_db_SharedResultCell;
typedef struct jazz_db_PendingOpenSchema {
    bool has_connection_epoch; u64 connection_epoch;
    bool authoritative_catalogue_received;
    jazz_types_SchemaVersionId schema;
    bool has_result; jz_err result;
    jazz_db_SharedResultCell *sender; /* Option<oneshot::Sender>: NULL = taken */
    jazz_db_SharedResultCell *wait;   /* Shared<LocalBoxFuture<Result<(), Error>>> */
} jazz_db_PendingOpenSchema;
/* OpenSchemaAdmission = Rc<RefCell<Option<PendingOpenSchema>>> */
typedef struct jazz_db_OpenSchemaAdmissionCell { bool has; jazz_db_PendingOpenSchema pending; } jazz_db_OpenSchemaAdmissionCell;
typedef jazz_db_OpenSchemaAdmissionCell *jazz_db_OpenSchemaAdmission;
void jazz_db__db__PendingOpenSchema__new(jazz_db_PendingOpenSchema *out, jazz_types_SchemaVersionId schema);
void jazz_db__db__PendingOpenSchema__drop(jazz_db_PendingOpenSchema *p);
/* poll the shared wait: true when ready, *out = clone of result */
bool jazz_db__db__PendingOpenSchema__poll_wait(jazz_db_SharedResultCell *wait, jz_err *out);
void jazz_db__db__finish_open_schema_admission(jazz_db_OpenSchemaAdmission state, jz_err result);
void jazz_db__db__finish_open_schema_connection(jazz_db_OpenSchemaAdmission state, u64 connection_epoch, jz_err result);
void jazz_db__db__begin_open_schema_connection(jazz_db_OpenSchemaAdmission state, u64 connection_epoch);
jz_err jazz_db__db__pending_open_schema_error(void);

typedef enum { JAZZ_TICK_IMMEDIATE, JAZZ_TICK_DEFERRED, JAZZ_TICK_AFTER_CURRENT_TURN } jazz_db_TickUrgency;
typedef struct jazz_db_TickSchedulerVt {
    void (*schedule_tick)(const void *self, u32 urgency);
    void (*schedule_tick_after)(const void *self, u64 delay_ms);
    bool (*query_runtime_waker)(const void *self, jz_waker *out); /* default: None */
    bool (*drops_pending_ticks)(const void *self);                 /* default: false */
    void (*drop)(void *self);
} jazz_db_TickSchedulerVt;
typedef struct jazz_db_TickScheduler { void *self; const jazz_db_TickSchedulerVt *vt; } jazz_db_TickScheduler; /* Rc<dyn TickScheduler> */
/* SharedTickScheduler = Rc<RefCell<Option<Rc<dyn TickScheduler>>>> */
typedef struct jazz_db_SharedTickSchedulerCell { bool has; jazz_db_TickScheduler scheduler; } jazz_db_SharedTickSchedulerCell;
typedef jazz_db_SharedTickSchedulerCell *jazz_db_SharedTickScheduler;

typedef enum { JAZZ_TXK_MERGEABLE, JAZZ_TXK_EXCLUSIVE } jazz_db_TransactionKind;
typedef struct jazz_db_TransactionFate { jazz_model_TransactionId transaction_id; jz_str code; jz_str reason; } jazz_db_TransactionFate; /* Rejected */
typedef struct jazz_db_LocalTransactionRecord {
    jazz_model_TransactionId transaction_id;
    u32 kind;
    bool sealed;
    jazz_db_TransactionFate latest_settlement;
} jazz_db_LocalTransactionRecord;
typedef struct jazz_db_MutationErrorEvent { jz_str code; jz_str reason; jazz_db_LocalTransactionRecord transaction; } jazz_db_MutationErrorEvent;
void jazz_db__db__MutationErrorEvent__clone(void *dst, const void *src);
void jazz_db__db__MutationErrorEvent__drop(void *p);
bool jazz_db__db__MutationErrorEvent__eq(const jazz_db_MutationErrorEvent *a, const jazz_db_MutationErrorEvent *b);
void jazz_db__db__MutationErrorEvent__Serialize__serialize(const jazz_db_MutationErrorEvent *e, jz_json_w *w);
u32 jazz_db__db__TransactionKind__from_tx_kind(u32 kind);
/* MutationErrorCallback = Rc<dyn Fn(&MutationErrorEvent)> */
typedef struct jazz_db_MutationErrorCallback { void *self; void (*call)(void *self, const jazz_db_MutationErrorEvent *e); } jazz_db_MutationErrorCallback;
jazz_db_MutationErrorCallback jazz_db__db__MutationErrorCallback__clone(const jazz_db_MutationErrorCallback *c);
void jazz_db__db__MutationErrorCallback__drop(jazz_db_MutationErrorCallback *c);

/* sync_autopsy (feature off): no-ops */
void jazz_db__db__sync_autopsy__enable(void);
void jazz_db__db__sync_autopsy__clear(void);
jz_str jazz_db__db__sync_autopsy__dump(void);

typedef enum { JAZZ_MOL_OPEN, JAZZ_MOL_CLOSING } jazz_db_MutationOwnerLifecycle;

typedef struct jazz_db_SchemaViewId { u8 b[32]; } jazz_db_SchemaViewId;
const u8 *jazz_db__db__SchemaViewId__as_bytes(const jazz_db_SchemaViewId *id);
jazz_db_SchemaViewId jazz_db__db__SchemaViewId__for_schema(const jazz_model_JazzSchema *schema);

typedef struct jazz_db_WriteClock { u64 next_counter_ms; bool wall; } jazz_db_WriteClock;
void jazz_db__db__WriteClock__default(jazz_db_WriteClock *c);
u64 jazz_db__db__WriteClock__next_now_ms(jazz_db_WriteClock *c);
typedef struct jazz_db_UploadRetryClockVt { u64 (*now_ms)(const void *self); void (*drop)(void *self); } jazz_db_UploadRetryClockVt;
typedef struct jazz_db_UploadRetryClock { void *self; const jazz_db_UploadRetryClockVt *vt; } jazz_db_UploadRetryClock;
jazz_db_UploadRetryClock jazz_db__db__MonotonicUploadRetryClock__new(void);
/* SharedUploadRetryClock = Rc<RefCell<Rc<dyn UploadRetryClock>>> */
typedef struct jazz_db_SharedUploadRetryClockCell { jazz_db_UploadRetryClock clock; } jazz_db_SharedUploadRetryClockCell;

typedef struct jazz_db_UpstreamUploadDestination { u8 remote_node[16]; jazz_types_AuthorSubject link_identity; } jazz_db_UpstreamUploadDestination;
int jazz_db__db__UpstreamUploadDestination__cmp(const void *a, const void *b, void *ctx);

/* PendingDownstreamFates = Rc<RefCell<Vec<SyncMessage>>> */
typedef struct jazz_db_PendingDownstreamFatesCell { jazz_db_SyncMessageVec v; } jazz_db_PendingDownstreamFatesCell;
typedef jazz_db_PendingDownstreamFatesCell *jazz_db_PendingDownstreamFates;
jazz_db_PendingDownstreamFates jazz_db__db__PendingDownstreamFates__new(void);

typedef struct jazz_db_LocalFateRoute {
    jazz_db_PendingDownstreamFatesCell *queue; /* Weak */
    bool local_acknowledged;
    bool replay_ready;
    bool has_replay_author; jazz_types_AuthorSubject replay_author;
    bool has_replay_unit; jazz_protocol_SyncMessage replay_unit;
    bool has_held_fate; jazz_protocol_SyncMessage held_fate;
} jazz_db_LocalFateRoute;
typedef JZ_VEC(jazz_db_LocalFateRoute) jazz_db_LocalFateRouteVec;
/* LocalFateRoutes = Rc<RefCell<BTreeMap<TxId, Vec<LocalFateRoute>>>> */
typedef struct jazz_db_LocalFateRoutesCell { jz_btree map; } jazz_db_LocalFateRoutesCell;
typedef jazz_db_LocalFateRoutesCell *jazz_db_LocalFateRoutes;
jazz_db_LocalFateRoutes jazz_db__db__LocalFateRoutes__new(void);
void jazz_db__db__register_local_fate_route(jazz_db_LocalFateRoutes routes, jazz_model_TxId tx_id, jazz_db_PendingDownstreamFates queue);
void jazz_db__db__register_local_fate_observer(jazz_db_LocalFateRoutes routes, jazz_model_TxId tx_id, jazz_db_PendingDownstreamFates queue);
void jazz_db__db__register_local_replay_route(jazz_db_LocalFateRoutes routes, jazz_model_TxId tx_id, jazz_db_PendingDownstreamFates queue, jazz_types_AuthorSubject author, const jazz_protocol_SyncMessage *replay_unit);
void jazz_db__db__queue_local_acknowledgements(jazz_db_LocalFateRoutes routes, jazz_db_SharedNodeState *node);
bool jazz_db__db__local_fate_is_terminal(const jazz_protocol_SyncMessage *fate);
void jazz_db__db__release_local_replay_fates(jazz_db_LocalFateRoutes routes);
void jazz_db__db__route_local_fate(jazz_db_LocalFateRoutes routes, jazz_model_TxId tx_id, const jazz_protocol_SyncMessage *fate);

typedef struct jazz_db_PendingUpload { jazz_model_TxId tx_id; bool has_unit; jazz_protocol_SyncMessage unit; } jazz_db_PendingUpload;
typedef struct jazz_db_UploadFailure { jazz_model_TxId tx_id; const char *reason; } jazz_db_UploadFailure;
#define JAZZ_DB_RECENT_UPLOAD_FAILURES 64u
typedef struct jazz_db_UploadOutbox {
    jz_deque entries;          /* PendingUpload */
    jz_hmap tx_ids;            /* HashSet<TxId> */
    bool declared_root;
    bool upstream_attached;
    jz_hmap on_wire;           /* HashSet<TxId> */
    jz_deque upload_failures;  /* (TxId, &'static str) */
    u64 upload_progress;
} jazz_db_UploadOutbox;
typedef jazz_db_UploadOutbox *jazz_db_Outbox; /* Rc<RefCell<UploadOutbox>> */
jazz_db_Outbox jazz_db__db__Outbox__new(void);
#define OUTBOX_FN(n) jazz_db__db__UploadOutbox__##n
bool OUTBOX_FN(settles_uploads_locally)(const jazz_db_UploadOutbox *o);
void OUTBOX_FN(mark_upstream_attached)(jazz_db_UploadOutbox *o);
bool OUTBOX_FN(push)(jazz_db_UploadOutbox *o, jazz_db_PendingUpload pending);
u32 OUTBOX_FN(len)(const jazz_db_UploadOutbox *o);
const jazz_db_PendingUpload *OUTBOX_FN(at)(const jazz_db_UploadOutbox *o, u32 i); /* iter() */
bool OUTBOX_FN(contains)(const jazz_db_UploadOutbox *o, jazz_model_TxId tx_id);
void OUTBOX_FN(retain)(jazz_db_UploadOutbox *o, bool (*keep)(const jazz_db_PendingUpload *p, void *ctx), void *ctx);
void OUTBOX_FN(mark_on_wire)(jazz_db_UploadOutbox *o, jazz_model_TxId tx_id);
void OUTBOX_FN(note_upload_progress)(jazz_db_UploadOutbox *o);
u64 OUTBOX_FN(upload_progress)(const jazz_db_UploadOutbox *o);
void OUTBOX_FN(forget_on_wire)(jazz_db_UploadOutbox *o);
bool OUTBOX_FN(awaits_wire)(const jazz_db_UploadOutbox *o, jazz_model_TxId tx_id);
void OUTBOX_FN(mark_upload_failed)(jazz_db_UploadOutbox *o, jazz_model_TxId tx_id, const char *reason);
const char *OUTBOX_FN(upload_failure)(const jazz_db_UploadOutbox *o, jazz_model_TxId tx_id);
void OUTBOX_FN(remove_released)(jazz_db_UploadOutbox *o, jz_hmap *released, jz_hmap *completed);
bool jazz_db__db__queue_pending_upload_in(jazz_db_Outbox outbox, jazz_model_TxId tx_id, const jazz_protocol_SyncMessage *unit);
extern const jz_hmap_desc jazz_db_txid_set_desc;

jz_err jazz_db__db__restore_local_subscriber_replay(jazz_db_SharedNodeState *node, jazz_db_Outbox outbox, jazz_db_LocalFateRoutes routes,
                                                    jazz_types_AuthorSubject author, jazz_db_PendingDownstreamFates downstream_fates);

bool jazz_db__db__transaction_satisfies_wait(const jazz_model_Fate *fate, bool has_global_time, u32 durability, u32 tier);
typedef struct jazz_db_WriteState { jazz_model_Fate fate; bool has_global_time; jazz_types_GlobalTime global_time; u32 durability; } jazz_db_WriteState;
typedef struct jazz_db_InitialSyncFlushCadence { usize writes; } jazz_db_InitialSyncFlushCadence;
#define JAZZ_DB_INITIAL_SYNC_FLUSH_CADENCE_DEFAULT 512u

typedef struct jazz_db_BindingViewRequirement { jazz_protocol_BindingViewKey view; u64 generation; } jazz_db_BindingViewRequirement;
typedef struct jazz_db_CoverageRefresh { jazz_protocol_CoverageKey coverage; u64 generation; } jazz_db_CoverageRefresh;
typedef struct jazz_db_QueryAttachment {
    JZ_VEC(jazz_protocol_SubscriptionKey) subscriptions;
    JZ_VEC(jazz_db_BindingViewRequirement) required_after;
    bool requires_delivery_receipt;
    bool requires_current_authority_receipt;
    JZ_VEC(jazz_protocol_SubscriptionKey) registrations;
    JZ_VEC(jazz_db_CoverageRefresh) refreshes;
} jazz_db_QueryAttachment;
jazz_protocol_SubscriptionKey jazz_db__db__QueryAttachment__subscription(const jazz_db_QueryAttachment *a);
void jazz_db__db__QueryAttachment__drop(jazz_db_QueryAttachment *a);
void jazz_db__db__QueryAttachment__clone(jazz_db_QueryAttachment *dst, const jazz_db_QueryAttachment *src);

typedef struct jazz_db_WriteStateWaiter { u64 id; jz_chan *notify; /* Future(oneshot::Sender<()>) */ } jazz_db_WriteStateWaiter;
typedef JZ_VEC(jazz_db_WriteStateWaiter) jazz_db_WriteStateWaiterVec;
typedef struct jazz_db_WriteStateWaitersCell { jz_btree map; /* TxId -> Vec<WriteStateWaiter> */ } jazz_db_WriteStateWaitersCell;
typedef jazz_db_WriteStateWaitersCell *jazz_db_WriteStateWaiters;
typedef struct jazz_db_WriteStateChange { jazz_db_WriteStateWaiters waiters; jazz_model_TxId tx_id; u64 waiter_id; jz_chan *receiver; } jazz_db_WriteStateChange;
bool jazz_db__db__WriteStateChange__poll(jazz_db_WriteStateChange *f, const jz_waker *cx);
void jazz_db__db__WriteStateChange__drop(jazz_db_WriteStateChange *f);
typedef struct jazz_db_WriteWaitOptions { u32 tier; bool observe_only; } jazz_db_WriteWaitOptions;
jazz_db_WriteWaitOptions jazz_db__db__WriteWaitOptions__from_tier(u32 tier);

typedef struct jazz_db_PermissionAdviceWaitersCell { jz_btree map; /* RequestId -> oneshot::Sender<PermissionAdvice> */ } jazz_db_PermissionAdviceWaitersCell;
typedef jazz_db_PermissionAdviceWaitersCell *jazz_db_PermissionAdviceWaiters;
typedef struct jazz_db_PermissionAdviceFuture { jazz_db_PermissionAdviceWaiters waiters; jazz_protocol_PermissionAdviceRequestId request_id; jz_chan *receiver; } jazz_db_PermissionAdviceFuture;
bool jazz_db__db__PermissionAdviceFuture__poll(jazz_db_PermissionAdviceFuture *f, const jz_waker *cx, u32 *out);
jazz_protocol_PermissionAdviceRequestId jazz_db__db__PermissionAdviceFuture__request_id(const jazz_db_PermissionAdviceFuture *f);
void jazz_db__db__PermissionAdviceFuture__drop(jazz_db_PermissionAdviceFuture *f);

typedef struct jazz_db_MutationErrorState {
    bool has_callback; jazz_db_MutationErrorCallback callback;
    jz_btree pending;              /* TxId -> MutationErrorEvent */
    jz_btree application_waiters;  /* TxId -> usize */
} jazz_db_MutationErrorState;
typedef jazz_db_MutationErrorState *jazz_db_SharedMutationErrors;
jazz_db_SharedMutationErrors jazz_db__db__SharedMutationErrors__new(void);
extern const jz_btree_desc jazz_db_mutation_error_pending_desc;

typedef struct jazz_db_StreamingValueUploadCleanupTicket { jazz_db_SharedResultCell *result; } jazz_db_StreamingValueUploadCleanupTicket;
bool jazz_db__db__StreamingValueUploadCleanupTicket__result(const jazz_db_StreamingValueUploadCleanupTicket *t, jz_err *out);

typedef struct jazz_db_DbTickStats { usize subscription_events; usize remote_sync_applied; } jazz_db_DbTickStats;

/* first_load.rs FirstLoad { Deliver, WaitForRemote { timeout_ms } } */
typedef enum { JAZZ_FL_DELIVER, JAZZ_FL_WAIT_FOR_REMOTE } jazz_db_FirstLoadTag;
typedef struct jazz_db_FirstLoad { u32 tag; u64 timeout_ms; } jazz_db_FirstLoad;
typedef struct jazz_db_ReadOpts {
    u32 tier;
    bool include_deleted;
    jazz_protocol_ReadViewSpec read_view;
    jazz_db_FirstLoad first_load;
} jazz_db_ReadOpts;
void jazz_db__db__ReadOpts__default(jazz_db_ReadOpts *out);
void jazz_db__db__ReadOpts__for_read_tier(jazz_db_ReadOpts *out, u32 tier);
void jazz_db__db__ReadOpts__branch_view(jazz_db_ReadOpts *self, jazz_model_BranchSelector head, const jazz_protocol_BranchViewBase *base);

jz_err jazz_db__db__transaction_abandoned(jazz_model_OpenTransactionId open_tx_id);
jz_err jazz_db__db__row_already_deleted(jazz_types_RowUuid row);
jz_err jazz_db__db__row_already_exists(jz_slice table, jazz_types_RowUuid row);
jz_err jazz_db__db__read_for_write_denied(const char *operation, jz_slice table);
jz_err jazz_db__db__update_target_not_loaded(const char *operation, jz_slice table, jazz_types_RowUuid row);

/* lens for schema views */
typedef JZ_VEC(jz_str) jazz_db_StrVec;
groove_Value jazz_db__db__default_cell_for_column_type(const groove_ColumnType *column_type, const groove_Value *default_);
jz_err jazz_db__db__direct_schema_view_lens(const jazz_model_JazzSchema *source, const jazz_model_JazzSchema *target,
                                            jazz_protocol_MigrationLens **lens, jazz_db_StrVec *new_tables, jazz_db_StrVec *dropped_tables);


/* ---------- db.rs part 3 types (lines 1806-1910, 2700-2830, 3726-7166) ---------- */
/* RowCells = BTreeMap<String, Value> (jazz_db_str_value_desc) */
typedef jz_btree jazz_db_RowCells;
void jazz_db__db__RowCells__clone(jz_btree *dst, const jz_btree *src);
jazz_db_PolicyBinding jazz_db__db__PolicyBinding__clone(const jazz_db_PolicyBinding *b);
void jazz_db__db__PolicyBinding__drop(jazz_db_PolicyBinding *b);

typedef enum { JAZZ_WI_DATABASE, JAZZ_WI_SESSION, JAZZ_WI_ATTRIBUTION } jazz_db_WriteIdentityTag;
typedef struct jazz_db_WriteIdentity { u32 tag; jazz_types_AuthorSubject subject; } jazz_db_WriteIdentity;
typedef struct jazz_db_ResolvedWriteIdentity { jazz_types_AuthorSubject made_by; bool has_permission_subject; jazz_types_AuthorSubject permission_subject; } jazz_db_ResolvedWriteIdentity;
typedef enum { JAZZ_EWT_ROOT, JAZZ_EWT_BRANCH } jazz_db_ExactWriteTargetTag;
typedef struct jazz_db_ExactWriteTarget { u32 tag; jazz_model_BranchSelector branch; } jazz_db_ExactWriteTarget;
typedef enum { JAZZ_WT_ROOT, JAZZ_WT_BRANCH_VIEW } jazz_db_WriteTargetTag;
typedef struct jazz_db_WriteTarget { u32 tag; jazz_model_BranchSelector head; bool has_base; jazz_protocol_BranchViewBase base; } jazz_db_WriteTarget;
typedef struct jazz_db_InsertOptions {
    bool has_row_id; jazz_types_RowUuid row_id;
    jazz_db_WriteIdentity identity;
    jazz_db_ExactWriteTarget target;
    bool has_updated_at_ms; u64 updated_at_ms;
} jazz_db_InsertOptions;
typedef struct jazz_db_UpdateOptions { jazz_db_WriteIdentity identity; jazz_db_WriteTarget target; bool has_updated_at_ms; u64 updated_at_ms; } jazz_db_UpdateOptions;
typedef jazz_db_UpdateOptions jazz_db_UpsertOptions;
typedef jazz_db_UpdateOptions jazz_db_DeleteOptions;
typedef struct jazz_db_RestoreOptions { jazz_db_WriteIdentity identity; jazz_db_ExactWriteTarget target; bool has_updated_at_ms; u64 updated_at_ms; } jazz_db_RestoreOptions;
jazz_model_BranchSelector jazz_db__db__ExactWriteTarget__branch(const jazz_db_ExactWriteTarget *t);
jz_err jazz_db__db__ensure_transaction_identity(jazz_db_WriteIdentity identity);
jz_err jazz_db__db__ensure_exclusive_target(const jazz_db_ExactWriteTarget *target);
jz_err jazz_db__db__ensure_exclusive_view_target(const jazz_db_WriteTarget *target);

/* Generic local future (Pin<Box<dyn Future<Output = T>>>), polled synchronously. */
typedef struct jazz_db_LocalFuture { void *self; bool (*poll)(void *self, const jz_waker *cx, void *out); void (*drop)(void *self); } jazz_db_LocalFuture;
void jazz_db__db__LocalFuture__drop(jazz_db_LocalFuture *f);
/* block_on: poll until ready (the port has no real suspension) */
void jazz_db__db__LocalFuture__block_on(jazz_db_LocalFuture *f, void *out);

/* QueuedMutationStatus / QueuedMutationAlias (db.rs:1887-1907) */
typedef enum { JAZZ_QMS_PENDING, JAZZ_QMS_PUBLISHED, JAZZ_QMS_FAILED } jazz_db_QueuedMutationStatusTag;
typedef struct jazz_db_QueuedMutationStatus { u32 tag; jz_err error; } jazz_db_QueuedMutationStatus;
typedef struct jazz_db_QueuedMutationAlias { bool has; jazz_model_TxId tx_id; } jazz_db_QueuedMutationAlias;

/* SubscriptionEvent */
typedef enum { JAZZ_SE_DELTA, JAZZ_SE_REJECTED, JAZZ_SE_CLOSED } jazz_db_SubscriptionEventTag;
typedef JZ_VEC(jazz_db_SubscriptionOutputRow) jazz_db_OutputRowVec;
typedef JZ_VEC(jazz_db_RemovedRow) jazz_db_RemovedRowVec;
typedef struct jazz_db_SubscriptionEvent {
    u32 tag;
    bool reset, publishable;
    jazz_db_OutputRowVec added, updated;
    jazz_db_RemovedRowVec removed;
    jazz_db_TerminalOperationVec terminal_operations;
    bool settled;
    u32 tier;
    jazz_protocol_SubscribeRejectReason reason; /* Rejected */
} jazz_db_SubscriptionEvent;
void jazz_db__db__SubscriptionEvent__drop(void *p);
void jazz_db__db__SubscriptionEvent__clone(void *dst, const void *src);
jazz_db_SubscriptionEvent jazz_db__db__SubscriptionEvent__delta(bool reset, u32 tier, bool settled);

typedef struct jazz_db_RelationSnapshotIndex {
    jazz_db_PositionalOrder roots;  /* PositionalOrder<OutputOccurrenceId> */
    jz_btree terminal_records;      /* OutputOccurrenceId -> TerminalRecordState* */
} jazz_db_RelationSnapshotIndex;
void jazz_db__db__RelationSnapshotIndex__default(jazz_db_RelationSnapshotIndex *out);
void jazz_db__db__RelationSnapshotIndex__clone(jazz_db_RelationSnapshotIndex *dst, const jazz_db_RelationSnapshotIndex *src);
void jazz_db__db__RelationSnapshotIndex__drop(jazz_db_RelationSnapshotIndex *i);
void jazz_db__db__RelationSnapshotIndex__from_snapshot(jazz_db_RelationSnapshotIndex *out, const jazz_node_RelationSnapshot *snapshot);
void jazz_db__db__RelationSnapshotIndex__with_root_occurrences(jazz_db_RelationSnapshotIndex *out, jazz_types_OutputOccurrenceId *occurrences, u32 n);

typedef enum { JAZZ_SSS_LOCAL_MAINTAINED, JAZZ_SSS_LINK_SNAPSHOT } jazz_db_SubscriptionSnapshotSource;
typedef struct jazz_db_SubscriptionKind {
    jazz_model_ValidatedQuery *shape;
    jazz_model_Binding *binding;
    jazz_node_LocalMaintainedViewSubscription *maintained_subscription; /* NULL = None */
} jazz_db_SubscriptionKind;

/* first_load.rs OpeningGate */
typedef enum { JAZZ_OR_LOCAL_FIRST, JAZZ_OR_REMOTE_WINDOW } jazz_db_OpeningRoute;
typedef struct jazz_db_OpeningGate { u64 epoch; u64 deadline; u32 route; bool withheld, witnessed, witness_answered; } jazz_db_OpeningGate;
bool jazz_db__db__first_load__OpeningGate__awaits_answer(const jazz_db_OpeningGate *g, bool settled);

typedef struct jazz_db_SubscriptionPublicationSnapshot { jazz_node_RelationSnapshot snapshot; jazz_db_OccurrenceVec occurrences; } jazz_db_SubscriptionPublicationSnapshot;
void jazz_db__db__SubscriptionPublicationSnapshot__drop(jazz_db_SubscriptionPublicationSnapshot *s);
typedef struct jazz_db_SubscriptionPublication {
    bool opened;
    bool has_deferred; jazz_db_SubscriptionPublicationSnapshot deferred;
    bool reset;
    jz_btree unresolved;  /* BTreeSet<OutputOccurrenceId> */
    bool has_opening_gate; jazz_db_OpeningGate opening_gate;
    bool remote_window;
    bool window_fell_back;
} jazz_db_SubscriptionPublication;
typedef struct jazz_db_SubscriptionSender {
    jz_chan *sender;                              /* UnboundedSender<SubscriptionEvent> */
    jazz_db_SubscriptionPublication *publication; /* Rc<RefCell<..>> */
    u32 requested_tier;
} jazz_db_SubscriptionSender;
jazz_db_SubscriptionSender jazz_db__db__SubscriptionSender__clone(const jazz_db_SubscriptionSender *s);
void jazz_db__db__SubscriptionSender__drop(jazz_db_SubscriptionSender *s);
jz_err jazz_db__db__SubscriptionSender__materialized(const jazz_db_SubscriptionSender *self, jazz_node_NodeState *node, const jazz_model_ValidatedQuery *shape, const jazz_db_SubscriptionEvent *event, bool *out);
jz_err jazz_db__db__SubscriptionSender__checkpoint(const jazz_db_SubscriptionSender *self, u32 tier, bool settled, const jazz_node_RelationSnapshot *snapshot, const jazz_db_RelationSnapshotIndex *index, bool *has, jazz_db_SubscriptionPublicationSnapshot *out);
jz_err jazz_db__db__SubscriptionSender__publish(const jazz_db_SubscriptionSender *self, jazz_db_SubscriptionEvent event, bool has_before, jazz_db_SubscriptionPublicationSnapshot before, const jazz_node_RelationSnapshot *snapshot, const jazz_db_RelationSnapshotIndex *index, bool materialized, bool *out);
bool jazz_db__db__SubscriptionSender__unbounded_send(const jazz_db_SubscriptionSender *self, jazz_db_SubscriptionEvent event);

/* ScalarReconciliation / deletion reconciliation */
typedef struct jazz_db_ScalarProbe { u64 deadline; JZ_VEC(jazz_types_RowUuid) rows; jazz_db_LocalFuture future; /* Output = CurrentRowsResult */ } jazz_db_ScalarProbe;
typedef struct jazz_db_ScalarReconciliation {
    bool has_generation; u64 generation_epoch; jazz_protocol_SubscriptionKey generation_subscription; u64 generation_revision;
    jz_deque pending;   /* RowUuid */
    bool has_active; jazz_db_ScalarProbe active;
    bool has_retry_at; u64 retry_at;
    u64 retry_delay_ms;
} jazz_db_ScalarReconciliation;
void jazz_db__db__ScalarReconciliation__default(jazz_db_ScalarReconciliation *out);
void jazz_db__db__ScalarReconciliation__drop(jazz_db_ScalarReconciliation *s);
#define JAZZ_DB_DELETION_DISCOVERY_LIMIT_MS 5000u
typedef struct jazz_db_DeletionDiscoverySpec {
    jazz_model_ValidatedQuery *shape;
    jazz_model_Binding *binding;
    jazz_types_AuthorSubject author;
    jz_btree claims;
    jazz_protocol_PolicyBindingKey context;
    jz_btree authoritative; /* BTreeSet<RowUuid> */
} jazz_db_DeletionDiscoverySpec;
bool jazz_db__db__DeletionDiscoverySpec__same_discovery(const jazz_db_DeletionDiscoverySpec *a, const jazz_db_DeletionDiscoverySpec *b);
void jazz_db__db__DeletionDiscoverySpec__drop(jazz_db_DeletionDiscoverySpec *s);
typedef JZ_VEC(jazz_protocol_CurrentRowCoordinate) jazz_db_CoordinateVec;
typedef struct jazz_db_TableRow { jz_str table; jazz_types_RowUuid row; } jazz_db_TableRow;
typedef JZ_VEC(jazz_db_TableRow) jazz_db_TableRowVec;
int jazz_db__db__TableRow__cmp(const void *a, const void *b, void *ctx);
void jazz_db__db__TableRow__drop(void *p);
typedef enum { JAZZ_DR_DISABLED, JAZZ_DR_UNCHECKED, JAZZ_DR_DEFERRED, JAZZ_DR_DISCOVERING, JAZZ_DR_PENDING, JAZZ_DR_PROBING, JAZZ_DR_DONE } jazz_db_DeletionReconciliationTag;
typedef struct jazz_db_DeletionReconciliation {
    u32 tag;
    u64 since;
    jazz_db_DeletionDiscoverySpec *spec;                    /* Deferred / Discovering */
    jazz_node_LocalMaintainedViewSubscription *maintained;  /* Discovering */
    u64 runtime_token;                                      /* Discovering */
    jazz_db_CoordinateVec rows;        /* Pending */
    jazz_protocol_PolicyBindingKey context;                 /* Pending */
    jazz_db_TableRowVec probing;                            /* Probing */
} jazz_db_DeletionReconciliation;
bool jazz_db__db__DeletionReconciliation__waiting_since(const jazz_db_DeletionReconciliation *d, u64 *out);
void jazz_db__db__DeletionReconciliation__drop(void *p);
typedef struct jazz_db_HeldRowCheck {
    jazz_protocol_CurrentRowCoordinate coordinate;
    jazz_protocol_PolicyBindingKey context;
    u32 attempts;
    bool has_retry_at; u64 retry_at;
} jazz_db_HeldRowCheck;
typedef struct jazz_db_HeldRowProbe { jazz_db_TableRowVec rows; u64 deadline; jazz_db_LocalFuture future; } jazz_db_HeldRowProbe;
typedef struct jazz_db_HeldRowChecks {
    jz_btree rows;  /* (String, RowUuid) -> HeldRowCheck */
    bool has_active; jazz_db_HeldRowProbe active;
    JZ_VEC(jazz_db_DeletionReconciliation) discoveries;
    bool has_upstream_connection; u64 upstream_connection;
} jazz_db_HeldRowChecks;
void jazz_db__db__HeldRowChecks__default(jazz_db_HeldRowChecks *out);
void jazz_db__db__HeldRowChecks__enqueue(jazz_db_HeldRowChecks *self, jazz_db_CoordinateVec rows, const jazz_protocol_PolicyBindingKey *context, jazz_db_TableRowVec *out);
bool jazz_db__db__HeldRowChecks__adopt_discovery(jazz_db_HeldRowChecks *self, jazz_db_DeletionReconciliation reconciliation, jazz_db_DeletionReconciliation *redundant);
u64 jazz_db__db__HeldRowChecks__retry_delay(u32 attempts);

/* Upstream coverage */
typedef struct jazz_db_UpstreamCoverageHandle { jazz_protocol_CoverageKey coverage; jazz_protocol_SubscriptionKey subscription; } jazz_db_UpstreamCoverageHandle;
typedef JZ_VEC(jazz_db_UpstreamCoverageHandle) jazz_db_UpstreamCoverageHandleVec;
void jazz_db__db__UpstreamCoverageHandle__clone(void *dst, const void *src);
void jazz_db__db__UpstreamCoverageHandle__drop(void *p);
typedef struct jazz_db_PendingUpstreamSubscription {
    jazz_protocol_SubscriptionKey subscription;
    jazz_model_ValidatedQuery *shape;
    jazz_model_Binding *binding;
    jazz_protocol_RegisterShapeOptions opts;
    jazz_types_AuthorSubject identity;
    bool has_policy_binding; jazz_db_PolicyBinding policy_binding;
} jazz_db_PendingUpstreamSubscription;
void jazz_db__db__PendingUpstreamSubscription__clone(void *dst, const void *src);
void jazz_db__db__PendingUpstreamSubscription__drop(void *p);
typedef enum { JAZZ_PUC_SUBSCRIBE, JAZZ_PUC_UNSUBSCRIBE, JAZZ_PUC_AUTHORIZATION_SCOPE_INTENT } jazz_db_PendingUpstreamCommandTag;
typedef struct jazz_db_PendingUpstreamCommand {
    u32 tag;
    jazz_db_PendingUpstreamSubscription subscribe;
    jazz_protocol_SubscriptionKey unsubscribe;
    jazz_protocol_PermissionAdviceRequestId request_id;
    jazz_protocol_PermissionAdviceAction action;
    bool has_session_claim_binding; jazz_db_PolicyBinding session_claim_binding;
    bool has_delegated_session; jazz_protocol_DelegatedSessionBinding delegated_session;
} jazz_db_PendingUpstreamCommand;
void jazz_db__db__PendingUpstreamCommand__drop(void *p);
typedef struct jazz_db_PendingUpstreamCommandsCell { JZ_VEC(jazz_db_PendingUpstreamCommand) v; } jazz_db_PendingUpstreamCommandsCell;
typedef struct jazz_db_QueryCoverageRegistration { jazz_protocol_CoverageKey coverage; jazz_db_PendingUpstreamSubscription subscription; usize ref_count; } jazz_db_QueryCoverageRegistration;
typedef struct jazz_db_AuthorityViewReceiptsCell { bool has; jz_btree binding_views; /* BTreeSet<BindingViewKey> */ jz_btree subscriptions; /* BTreeSet<SubscriptionKey> */ } jazz_db_AuthorityViewReceiptsCell;
typedef jazz_db_AuthorityViewReceiptsCell *jazz_db_ActiveAuthorityViewReceipts;

/* SubscriptionState (RefCell payload of Rc<RefCell<SubscriptionState>>) */
typedef struct jazz_db_LocalCleanupCell { bool has; u64 token; groove_SubscriptionId id; } jazz_db_LocalCleanupCell; /* Rc<Cell<Option<(u64, SubscriptionId)>>> */
typedef struct jazz_db_SubscriptionState {
    jazz_db_RcBoolCell *closed;
    bool terminal_rows;
    jazz_db_DeletionReconciliation deletion_reconciliation;
    bool scalar_reconciliation_enabled;
    u64 scalar_authority_revision;
    jazz_db_ScalarReconciliation scalar_reconciliation;
    jazz_db_SubscriptionKind kind;
    u64 groove_runtime_token;
    jazz_db_LocalCleanupCell *local_subscription_cleanup;
    jazz_db_UpstreamCoverageHandleVec upstream_subscription_handles;
    bool propagates_upstream;
    bool has_request_identity_claims; jazz_db_PolicyBinding request_identity_claims;
    jazz_types_AuthorSubject author;
    u32 authorization_mode;
    u32 read_tier;
    bool pending_overlay;
    bool has_remote_read_tier; u32 remote_read_tier;
    bool requires_authority_receipt;
    jazz_protocol_ReadViewSpec read_view;
    jazz_node_RelationSnapshot snapshot;
    jazz_db_RelationSnapshotIndex snapshot_index;
    u32 snapshot_source;
    bool settled;
    bool pending_initial_local_snapshot;
    bool pending_initial_owner_result;
    jazz_db_UpstreamCoverageHandleVec authority_witness;
    jazz_db_SubscriptionSender sender;
    i32 borrow_flag; /* RefCell borrow state: >0 shared, <0 exclusive */
} jazz_db_SubscriptionState;
/* Rc<RefCell<SubscriptionState>>: rc payload pointer; Weak = same pointer, upgrade = strong > 0 */
typedef jazz_db_SubscriptionState *jazz_db_SubscriptionStateRc;
static inline jazz_db_SubscriptionState *jazz_db_weak_state_upgrade(jazz_db_SubscriptionState *w) { return (w && jz_rc_strong(w) > 0) ? (jazz_db_SubscriptionState *)jz_rc_clone(w) : NULL; }
/* SubscriptionList = Rc<RefCell<Vec<Weak<RefCell<SubscriptionState>>>>> */
typedef JZ_VEC(jazz_db_SubscriptionState *) jazz_db_WeakStateVec;
typedef struct jazz_db_SubscriptionListCell { jazz_db_WeakStateVec v; } jazz_db_SubscriptionListCell;
typedef jazz_db_SubscriptionListCell *jazz_db_SubscriptionList;

typedef struct jazz_db_PendingSubscriptionFinalization {
    jazz_db_SubscriptionState *state; /* Option<Rc<..>> */
    jazz_db_UpstreamCoverageHandleVec opening_upstream;
    bool has_opening_local; u64 opening_local_token; groove_SubscriptionId opening_local_id;
    jz_chan *acknowledgement;         /* Option<oneshot::Sender<()>> */
} jazz_db_PendingSubscriptionFinalization;

/* SubscriptionStream */
typedef struct jazz_db_SubscriptionCleanup {
    /* FnOnce(Option<oneshot::Sender<()>>) -> Option<SubscriptionFinalizationFuture> */
    bool (*call)(void *ctx, jz_chan *ack /* NULL = None */, jazz_db_LocalFuture *out);
    void *ctx;
    void (*drop)(void *ctx);
} jazz_db_SubscriptionCleanup;
typedef enum { JAZZ_SF_NONE, JAZZ_SF_PENDING, JAZZ_SF_FAILED } jazz_db_SubscriptionFinalizationTag;
typedef struct jazz_db_SubscriptionStream jazz_db_SubscriptionStream;
struct jazz_db_SubscriptionStream {
    jz_chan *receiver;
    jazz_db_SubscriptionState *state;
    bool has_cleanup; jazz_db_SubscriptionCleanup cleanup;
    u32 finalization_tag; jazz_db_LocalFuture finalization_pending; u32 failed_code; jz_str failed_message;
    bool terminated;
    jazz_db_SubscriptionStream *window_fallback; /* Option<Box<..>> */
    bool serving_fallback;
};
jz_err jazz_db__db__SubscriptionStream__close(jazz_db_SubscriptionStream *s);
bool jazz_db__db__SubscriptionStream__next_event(jazz_db_SubscriptionStream *s, jazz_db_SubscriptionEvent *out);
bool jazz_db__db__SubscriptionStream__is_remote_window(const jazz_db_SubscriptionStream *s);
jz_err jazz_db__db__SubscriptionStream__settled_receiver_local_snapshot(const jazz_db_SubscriptionStream *s, jazz_node_RelationSnapshot *out);
bool jazz_db__db__SubscriptionStream__try_next_event(jazz_db_SubscriptionStream *s, jazz_db_SubscriptionEvent *out);
/* Poll<Option<Item>>: returns true when Ready; *has false = stream end */
bool jazz_db__db__SubscriptionStream__Stream__poll_next(jazz_db_SubscriptionStream *s, const jz_waker *cx, bool *has, jazz_db_SubscriptionEvent *out);
void jazz_db__db__SubscriptionStream__Drop__drop(jazz_db_SubscriptionStream *s);
typedef struct jazz_db_CleanupGuard { bool has; void (*call)(void *ctx); void *ctx; } jazz_db_CleanupGuard;
void jazz_db__db__CleanupGuard__new(jazz_db_CleanupGuard *out, void (*call)(void *ctx), void *ctx);
void jazz_db__db__CleanupGuard__take(jazz_db_CleanupGuard *g, void (**call)(void *ctx), void **ctx);
void jazz_db__db__CleanupGuard__Drop__drop(jazz_db_CleanupGuard *g);

typedef enum { JAZZ_SRR_ROWS, JAZZ_SRR_RELATION } jazz_db_SerializedReadResultTag;
typedef struct jazz_db_SerializedReadResult { u32 tag; JZ_VEC(jazz_node_CurrentRow) rows; jazz_node_RelationSnapshot relation; } jazz_db_SerializedReadResult;
typedef enum { JAZZ_SSA_CLIENT_LOCAL, JAZZ_SSA_TRUSTED_SERVING, JAZZ_SSA_TRUSTED_CLIENT } jazz_db_SerializedSubscriptionAuthorizationTag;
typedef struct jazz_db_SerializedSubscriptionAuthorization { u32 tag; jazz_types_AuthorSubject author; } jazz_db_SerializedSubscriptionAuthorization;

typedef struct jazz_db_PreparedQuery {
    bool has_request_identity_claims; jazz_db_PolicyBinding request_identity_claims;
    jazz_model_ValidatedQuery *shape;
    jazz_model_Binding *binding;
    jazz_node_PreparedQueryPlanHandle *local_plan;  /* NULL = None */
    jazz_node_PreparedQueryPlanHandle *global_plan;
    u64 groove_runtime_token;
} jazz_db_PreparedQuery;
void jazz_db__db__PreparedQuery__clone(jazz_db_PreparedQuery *dst, const jazz_db_PreparedQuery *src);
void jazz_db__db__PreparedQuery__drop(jazz_db_PreparedQuery *q);
bool jazz_db__db__PreparedQuery__request_identity(const jazz_db_PreparedQuery *q, jazz_types_AuthorSubject *out);
void jazz_db__db__PreparedQuery__with_identity_claims(jazz_db_PreparedQuery *self, jazz_types_AuthorSubject author, jz_btree claims);
jz_err jazz_db__db__PreparedQuery__request_policy_binding(const jazz_db_PreparedQuery *q, jazz_types_AuthorSubject author, bool *has, jazz_db_PolicyBinding *out);
jz_err jazz_db__db__PreparedQuery__scoped_node(const jazz_db_PreparedQuery *q, jazz_node_NodeState *node, jazz_types_AuthorSubject author, jazz_node_ActiveSessionClaimsScope **out);
const jazz_model_ValidatedQuery *jazz_db__db__PreparedQuery__shape(const jazz_db_PreparedQuery *q);
const jazz_model_Binding *jazz_db__db__PreparedQuery__binding(const jazz_db_PreparedQuery *q);
jazz_node_PreparedQueryPlanHandle *jazz_db__db__PreparedQuery__plan_for_tier(const jazz_db_PreparedQuery *q, u32 tier, u64 groove_runtime_token);
bool jazz_db__db__should_install_prepared_plan(const jazz_model_ValidatedQuery *shape);

/* Db handle (db.rs:1806) */
typedef struct jazz_db_Node jazz_db_Node;
typedef struct jazz_db_SchemaViewsCell { jz_btree map; /* SchemaViewId -> JazzSchema */ } jazz_db_SchemaViewsCell;
typedef struct jazz_db_RowIdSourceCell { jazz_db_RowIdSource source; } jazz_db_RowIdSourceCell;
typedef struct jazz_db_Db {
    jazz_model_JazzSchema schema;
    jazz_types_SchemaVersionId schema_version_id;
    bool schema_view_is_fixed;
    bool requires_open_schema_admission;
    jazz_db_SchemaViewsCell *schema_views;
    jazz_db_DbIdentity identity;
    jazz_db_Node *node;                       /* Rc<Node> */
    jazz_db_RowIdSourceCell *row_id_source;   /* Rc<RefCell<Box<dyn RowIdSource>>> */
    bool row_id_source_guarantees_fresh;
    jazz_db_WriteClock *next_now_ms;          /* Rc<WriteClock> */
    bool has_reserved_tx_id; jazz_model_TxId reserved_tx_id;
    bool owner_operation_admitted;
    bool backend_attribution;
} jazz_db_Db;
jazz_types_RowUuid jazz_db__db__Db__next_row_id(const jazz_db_Db *db);

/* MergeableTx / ExclusiveTx (one C copy for the Tx and TxRef impls) */
typedef struct jazz_db_TxHandle { const jazz_db_Db *db; jazz_model_OpenTransactionId tx_id; bool committed; } jazz_db_TxHandle;
typedef JZ_VEC(jazz_node_CurrentRow) jazz_db_CurrentRowVec;
#define MTX_FN(n) jazz_db__db__MergeableTxOps__##n
#define XTX_FN(n) jazz_db__db__ExclusiveTxOps__##n
jz_err MTX_FN(insert)(const jazz_db_TxHandle *t, jz_slice table, jazz_db_RowCells cells, jazz_db_InsertOptions options, jazz_types_RowUuid *out);
jz_err MTX_FN(update)(const jazz_db_TxHandle *t, jz_slice table, jazz_types_RowUuid row, jazz_db_RowCells patch, jazz_db_UpdateOptions options);
jz_err MTX_FN(upsert)(const jazz_db_TxHandle *t, jz_slice table, jazz_types_RowUuid row, jazz_db_RowCells cells, jazz_db_UpsertOptions options);
jz_err MTX_FN(delete)(const jazz_db_TxHandle *t, jz_slice table, jazz_types_RowUuid row, jazz_db_DeleteOptions options);
jz_err MTX_FN(restore)(const jazz_db_TxHandle *t, jz_slice table, jazz_types_RowUuid row, bool has_cells, jazz_db_RowCells cells, jazz_db_RestoreOptions options);
jz_err MTX_FN(move_between_branches)(const jazz_db_TxHandle *t, jz_slice table, jazz_model_BranchSelector source, jazz_model_BranchSelector target, jazz_types_RowUuid row);
jz_err jazz_db__db__MergeableTx__commit(jazz_db_TxHandle *t, jazz_model_TxId *out);
void jazz_db__db__MergeableTx__Drop__drop(jazz_db_TxHandle *t);
jz_err XTX_FN(insert)(const jazz_db_TxHandle *t, jz_slice table, jazz_db_RowCells cells, jazz_db_InsertOptions options, jazz_types_RowUuid *out);
jz_err XTX_FN(update)(const jazz_db_TxHandle *t, jz_slice table, jazz_types_RowUuid row, jazz_db_RowCells patch, jazz_db_UpdateOptions options);
jz_err XTX_FN(upsert)(const jazz_db_TxHandle *t, jz_slice table, jazz_types_RowUuid row, jazz_db_RowCells cells, jazz_db_UpsertOptions options);
jz_err XTX_FN(delete)(const jazz_db_TxHandle *t, jz_slice table, jazz_types_RowUuid row, jazz_db_DeleteOptions options);
jz_err XTX_FN(restore)(const jazz_db_TxHandle *t, jz_slice table, jazz_types_RowUuid row, bool has_cells, jazz_db_RowCells cells, jazz_db_RestoreOptions options);
jz_err jazz_db__db__ExclusiveTx__commit(jazz_db_TxHandle *t, jazz_model_TxId *out);
void jazz_db__db__ExclusiveTx__Drop__drop(jazz_db_TxHandle *t);

/* WriteHandle */
typedef struct jazz_db_WriteHandle {
    jazz_db_SharedNodeState *node;                 /* WeakNodeState */
    jazz_types_RowUuid row_uuid;
    jazz_model_TxId tx_id;
    u32 local_tier;
    jazz_db_QueuedMutationStatus *queued_status;   /* Option<Rc<RefCell<..>>> */
    jazz_db_QueuedMutationAlias *queued_alias;     /* Option<QueuedMutationAlias> */
} jazz_db_WriteHandle;
jazz_types_RowUuid jazz_db__db__WriteHandle__row_uuid(const jazz_db_WriteHandle *w);
jazz_model_TxId jazz_db__db__WriteHandle__mergeable_tx_id(const jazz_db_WriteHandle *w);
jz_err jazz_db__db__WriteHandle__wait(const jazz_db_WriteHandle *w, u32 tier, jazz_model_TxId *out);
jz_err jazz_db__db__WriteHandle__write_state(const jazz_db_WriteHandle *w, jazz_db_WriteState *out);
jz_err jazz_db__db__write_rejected_tx(jazz_model_TxId tx_id, const jazz_model_RejectionReason *reason);

/* db.rs free functions used by submodules */
bool jazz_db__db__subscriber_inbound_message_is_authority_only(const jazz_protocol_SyncMessage *message, jazz_node_CommitUnitIngestContext ingest, const jazz_node_PeerState *peer);
bool jazz_db__db__delegated_session_capability(jazz_node_CommitUnitIngestContext ingest, jazz_node_PeerRole role);
bool jazz_db__db__admitted_request_policy_binding(jazz_node_CommitUnitIngestContext ingest, const jazz_node_PeerState *peer, const jazz_db_PolicyBinding *direct, const jazz_protocol_DelegatedSessionBinding *delegated, jazz_db_PolicyBinding *out);
bool jazz_db__db__subscriber_permission_subject(jazz_node_CommitUnitIngestContext ingest, jazz_types_AuthorSubject *out);
jz_err jazz_db__db__materialize_result_tree(const jazz_model_Query *query, jazz_node_RelationSnapshot snapshot, jazz_db_ResultTree *out);
jazz_db_SubscriptionEvent jazz_db__db__subscription_delta_event(u32 tier, bool settled, const jazz_node_RelationSnapshot *previous, const jazz_node_RelationSnapshot *current, bool terminal_rows);
jazz_db_SubscriptionEvent jazz_db__db__subscription_delta_event_with_reset(u32 tier, bool settled, const jazz_node_RelationSnapshot *previous, const jazz_node_RelationSnapshot *current, bool reset, bool terminal_rows);
void jazz_db__db__demote_authority_receipt_subscriptions(jazz_db_SubscriptionList subscriptions, const jz_btree *publishing_subscriptions);
jz_err jazz_db__db__subscription_terminal_delta_event(u32 tier, bool settled, const jazz_node_RelationSnapshot *previous, const jazz_types_OutputOccurrenceId *previous_occurrences, u32 n_prev,
                                                     const jazz_node_RelationSnapshot *current, const jazz_types_OutputOccurrenceId *current_occurrences, u32 n_cur, jazz_db_SubscriptionEvent *out);
jz_err jazz_db__db__apply_maintained_update_to_snapshot(jazz_node_RelationSnapshot *snapshot, jazz_db_RelationSnapshotIndex *index, jazz_node_LocalMaintainedViewSubscriptionUpdate update,
                                                       jz_slice table, u32 tier, bool settled, const jazz_node_TerminalRootLayout *terminal_layout, jazz_db_SubscriptionEvent *out);
jz_err jazz_db__db__materialized_subscription_snapshot(const jazz_node_RelationSnapshot *snapshot, const jazz_db_RelationSnapshotIndex *index, jazz_node_RelationSnapshot *out);
jz_err jazz_db__db__snapshot_root_occurrences(const jazz_node_RelationSnapshot *snapshot, const jazz_db_RelationSnapshotIndex *index, jazz_db_OccurrenceVec *out);
jz_err jazz_db__db__relation_snapshot_index_with_root_occurrences(const jazz_node_RelationSnapshot *snapshot, const jazz_types_OutputOccurrenceId *occurrences, u32 n, jazz_db_RelationSnapshotIndex *out);
jz_err jazz_db__db__maintained_snapshot_index_or_row_index(jazz_node_NodeState *node, const jazz_db_SubscriptionKind *kind, const jazz_node_RelationSnapshot *snapshot, jazz_db_RelationSnapshotIndex *out);
void jazz_db__db__relation_snapshot_with_delta_slack(const jazz_node_RelationSnapshot *snapshot, jazz_node_RelationSnapshot *out);
void jazz_db__db__reserve_relation_snapshot_delta_slack(jazz_node_RelationSnapshot *snapshot);
bool jazz_db__db__subscription_is_settled(const jazz_node_NodeState *node, jazz_db_ActiveAuthorityViewReceipts receipts, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding,
                                          u32 tier, jazz_protocol_ReadViewSpec read_view, bool requires_authority_receipt, const jazz_protocol_AuthorityResultKey *authority_result_key);
void jazz_db__db__subscription_row_occurrence_id(const jazz_node_CurrentRow *row, jazz_types_OutputOccurrenceId *out);
jz_err jazz_db__db__subscription_outputs_with_occurrence_sidecar(const jazz_node_RelationSnapshot *snapshot, const jazz_types_OutputOccurrenceId *occurrence_ids, u32 n, jazz_db_OutputRowVec *out);
void jazz_db__db__reset_removed_roots(const jazz_node_RelationSnapshot *previous, const jazz_db_RelationSnapshotIndex *previous_index, const jazz_types_OutputOccurrenceId *current, u32 n, jazz_db_RemovedRowVec *out);
void jazz_db__db__subscription_output_row(jazz_node_CurrentRow row, bool has_previous_index, u32 previous_index, u32 index, jazz_db_SubscriptionOutputRow *out);
bool jazz_db__db__subscription_event_is_publishable(const jazz_db_SubscriptionEvent *e);
extern const jz_btree_desc jazz_db_occurrence_set_desc;


/* db/transactions.rs (impl Db) used by the Tx-ops defaults */
#define DBTX_FN(n) jazz_db__db__transactions__Db__##n
jz_err DBTX_FN(stage_mergeable_insert)(const jazz_db_Db *db, jazz_model_OpenTransactionId tx_id, jz_slice table, jazz_types_RowUuid row, jazz_db_RowCells cells, bool has_now, u64 now_ms, bool known_fresh_row);
jz_err DBTX_FN(stage_mergeable_insert_in_branch)(const jazz_db_Db *db, jazz_model_OpenTransactionId tx_id, jz_slice table, jazz_model_BranchSelector branch, jazz_types_RowUuid row, jazz_db_RowCells cells, bool has_now, u64 now_ms, bool known_fresh_row);
jz_err DBTX_FN(stage_mergeable_update)(const jazz_db_Db *db, jazz_model_OpenTransactionId tx_id, jz_slice table, jazz_types_RowUuid row, jazz_db_RowCells patch, bool has_now, u64 now_ms);
jz_err DBTX_FN(stage_mergeable_update_in_branch_view)(const jazz_db_Db *db, jazz_model_OpenTransactionId tx_id, jz_slice table, jazz_model_BranchSelector head, bool has_base, jazz_protocol_BranchViewBase base, jazz_types_RowUuid row, jazz_db_RowCells patch, bool has_now, u64 now_ms);
jz_err DBTX_FN(stage_mergeable_upsert)(const jazz_db_Db *db, jazz_model_OpenTransactionId tx_id, jz_slice table, jazz_types_RowUuid row, jazz_db_RowCells cells, jazz_db_UpsertOptions options, bool require_complete_insert);
jz_err DBTX_FN(stage_mergeable_delete)(const jazz_db_Db *db, jazz_model_OpenTransactionId tx_id, jz_slice table, jazz_types_RowUuid row, bool has_now, u64 now_ms);
jz_err DBTX_FN(stage_mergeable_delete_in_branch_view)(const jazz_db_Db *db, jazz_model_OpenTransactionId tx_id, jz_slice table, jazz_model_BranchSelector head, bool has_base, jazz_protocol_BranchViewBase base, jazz_types_RowUuid row, bool has_now, u64 now_ms);
jz_err DBTX_FN(stage_mergeable_restore)(const jazz_db_Db *db, jazz_model_OpenTransactionId tx_id, jz_slice table, jazz_types_RowUuid row, jazz_db_RowCells cells, bool has_now, u64 now_ms);
jz_err DBTX_FN(stage_mergeable_restore_in_branch)(const jazz_db_Db *db, jazz_model_OpenTransactionId tx_id, jz_slice table, jazz_model_BranchSelector branch, jazz_types_RowUuid row, jazz_db_RowCells cells, bool has_now, u64 now_ms);
jz_err DBTX_FN(commit_mergeable_handle)(const jazz_db_Db *db, jazz_model_OpenTransactionId tx_id, jazz_model_TxId *out);
jz_err DBTX_FN(transaction_read)(const jazz_db_Db *db, jazz_model_OpenTransactionId tx_id, jz_slice table, jazz_types_RowUuid row, bool *has, jazz_db_RowCells *out);
jz_err DBTX_FN(transaction_all)(const jazz_db_Db *db, jazz_model_OpenTransactionId tx_id, const jazz_db_PreparedQuery *prepared, jazz_db_ReadOpts opts, jazz_db_CurrentRowVec *out);
jz_err DBTX_FN(transaction_all_for_identity)(const jazz_db_Db *db, jazz_model_OpenTransactionId tx_id, const jazz_db_PreparedQuery *prepared, jazz_types_AuthorSubject author, jazz_db_ReadOpts opts, jazz_db_CurrentRowVec *out);
jz_err DBTX_FN(transaction_relation_snapshot)(const jazz_db_Db *db, jazz_model_OpenTransactionId tx_id, const jazz_db_PreparedQuery *prepared, jazz_db_ReadOpts opts, jazz_node_RelationSnapshot *out);
jz_err DBTX_FN(transaction_relation_snapshot_for_identity)(const jazz_db_Db *db, jazz_model_OpenTransactionId tx_id, const jazz_db_PreparedQuery *prepared, jazz_types_AuthorSubject author, jazz_db_ReadOpts opts, jazz_node_RelationSnapshot *out);
jz_err DBTX_FN(transaction_current_rows)(const jazz_db_Db *db, jazz_model_OpenTransactionId tx_id, jz_slice table, jazz_db_CurrentRowVec *out);
jz_err DBTX_FN(stage_exclusive_insert)(const jazz_db_Db *db, jazz_model_OpenTransactionId tx_id, jz_slice table, jazz_types_RowUuid row, jazz_db_RowCells cells, bool has_now, u64 now_ms, bool explicit_row_id);
jz_err DBTX_FN(stage_exclusive_update)(const jazz_db_Db *db, jazz_model_OpenTransactionId tx_id, jz_slice table, jazz_types_RowUuid row, jazz_db_RowCells patch, bool has_now, u64 now_ms);
jz_err DBTX_FN(stage_exclusive_upsert)(const jazz_db_Db *db, jazz_model_OpenTransactionId tx_id, jz_slice table, jazz_types_RowUuid row, jazz_db_RowCells cells, bool has_now, u64 now_ms, bool require_complete_insert);
jz_err DBTX_FN(stage_exclusive_delete)(const jazz_db_Db *db, jazz_model_OpenTransactionId tx_id, jz_slice table, jazz_types_RowUuid row, bool has_now, u64 now_ms);
jz_err DBTX_FN(stage_exclusive_restore)(const jazz_db_Db *db, jazz_model_OpenTransactionId tx_id, jz_slice table, jazz_types_RowUuid row, jazz_db_RowCells cells, bool has_now, u64 now_ms);
jz_err DBTX_FN(commit_exclusive_handle)(const jazz_db_Db *db, jazz_model_OpenTransactionId tx_id, jazz_model_TxId *out);

/* ---- db.rs:3509-3726 free helpers + SubscriptionStream::compiled_authorization_mode ---- */
jazz_model_DurabilityTier jazz_db__db__read_coverage_tier(const jazz_db_ReadOpts *opts);
void jazz_db__db__upstream_register_shape_options(jazz_model_DurabilityTier tier, jazz_protocol_ReadViewSpec read_view, jazz_model_DurabilityTier upstream_durability_floor, jazz_protocol_RegisterShapeOptions *out);
jazz_model_DurabilityTier jazz_db__db__remote_subscription_tier(jazz_model_DurabilityTier tier, jazz_model_DurabilityTier upstream_durability_floor);
jz_err jazz_db__db__ensure_default_read_view(const jazz_db_ReadOpts *opts);
jz_err jazz_db__db__ensure_supported_read_view(const jazz_db_ReadOpts *opts);
jz_err jazz_db__db__ensure_supported_subscription_read_opts(const jazz_db_ReadOpts *opts);
jz_err jazz_db__db__ensure_supported_register_shape_read_view(const jazz_protocol_RegisterShapeOptions *opts);
jz_err jazz_db__db__ensure_supported_register_shape_options(const jazz_protocol_RegisterShapeOptions *opts, bool local_receiver, jazz_node_PeerRole peer_role, bool delegated_session_capability);
jz_err jazz_db__db__validate_shape_ast_for_registration(jazz_node_NodeState *node, jazz_model_ShapeId shape_id, const jazz_model_ShapeAst *ast, bool *has, jazz_model_ValidatedQuery *out);
void jazz_db__db__unsupported_shape_capability_rejection_message(jazz_protocol_SubscriptionKey subscription, jz_str detail, jazz_protocol_SyncMessage *out);
void jazz_db__db__server_subscription_failure_rejection_message(jazz_protocol_SubscriptionKey subscription, const jz_err *error, jazz_protocol_SyncMessage *out);
void jazz_db__db__subscription_rejection_message(jazz_protocol_SubscriptionKey subscription, jazz_protocol_SubscribeRejectReason reason, jazz_protocol_SyncMessage *out);
u32 jazz_db__db__server_failure_code(const jz_err *error);
bool jazz_db__db__is_server_shape_validation_failure(const jz_err *error);
jazz_protocol_SubscriptionKey jazz_db__db__register_shape_rejection_subscription(jazz_model_ShapeId shape_id, jazz_protocol_ReadViewKey read_view);
void jazz_db__db__coverage_key(const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding, jazz_protocol_RegisterShapeOptions opts, jazz_protocol_CoverageKey *out);
void jazz_db__db__request_coverage_key(const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding, jazz_protocol_RegisterShapeOptions opts, const jazz_db_PolicyBinding *policy_binding /* NULL = None */, jazz_protocol_CoverageKey *out);
bool jazz_db__db__subscriber_permissions_ready(bool permissions_ready, jazz_node_CommitUnitTrust trust);
bool jazz_db__db__SubscriptionStream__compiled_authorization_mode(const jazz_db_SubscriptionStream *s, u32 *out);

/* SubscriberShapeRegistration (db.rs:2575) */
typedef enum { JAZZ_SSR_REGISTERED, JAZZ_SSR_PENDING_CATALOGUE_ADMISSION, JAZZ_SSR_REJECTED_UNSUPPORTED_CAPABILITY } jazz_db_SubscriberShapeRegistrationTag;
typedef struct jazz_db_SubscriberShapeRegistration { u32 tag; jazz_protocol_RegisterShapeOptions opts; jz_str detail; } jazz_db_SubscriberShapeRegistration;
bool jazz_db__db__SubscriberShapeRegistration__owns_node_shape(const jazz_db_SubscriberShapeRegistration *r);
bool jazz_db__db__TickScheduler__query_runtime_waker_default(const void *self, jz_waker *out);
bool jazz_db__db__TickScheduler__drops_pending_ticks_default(const void *self);
jz_err jazz_db__db__Db__reserve_minted_tx_time_after(const jazz_db_Db *db, jazz_types_TxTime high_water);
jazz_types_TxTime jazz_db__db__Db__minted_tx_time_high_water(const jazz_db_Db *db);
const jazz_node_CurrentRow *jazz_db__db__SubscriptionOutputRow__Deref__deref(const jazz_db_SubscriptionOutputRow *r);
jz_err jazz_db__db__materialize_subscription_terminal_records(jazz_node_RelationSnapshot *snapshot, const jazz_db_RelationSnapshotIndex *index);

#include "_ext_db-b-node.h"
#include "_ext_db-b-tx.h"
#endif
