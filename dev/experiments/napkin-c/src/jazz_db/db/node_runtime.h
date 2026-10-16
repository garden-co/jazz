/* node_runtime.h: port of crates/jazz/layers/db/src/db/node_runtime.rs
 * Node-owned orchestration for upstream and subscriber connections. */
#ifndef JAZZ_DB_NODE_RUNTIME_H
#define JAZZ_DB_NODE_RUNTIME_H
#include "jz_rt.h"
#include "../_ext_db-a.h"

typedef struct jazz_db_PeerConnection jazz_db_PeerConnection;
/* Rc<LocalMutex<PeerConnection>> */
typedef struct jazz_db_PeerMutex { u32 locked; jazz_db_PeerConnection *conn; } jazz_db_PeerMutex;
static inline jazz_db_PeerConnection *jazz_db_peer_try_lock(jazz_db_PeerMutex *m) { if (m->locked) return NULL; m->locked++; return m->conn; }
static inline jazz_db_PeerConnection *jazz_db_peer_lock(jazz_db_PeerMutex *m) { m->locked++; return m->conn; }
static inline void jazz_db_peer_unlock(jazz_db_PeerMutex *m) { if (m->locked) m->locked--; }
typedef JZ_VEC(jazz_db_PeerMutex *) jazz_db_PeerMutexVec;

/* ---------- Transport trait (dyn Transport) ---------- */
typedef struct jazz_db_ConnectionSessionContext {
    jazz_db_WireAuthorityEndpoint local;
    bool has_remote;
    jazz_db_WireAuthorityEndpoint remote;
    jazz_types_AuthorSubject link_identity;
    jazz_db_WireFeatures negotiated_features;
} jazz_db_ConnectionSessionContext;
typedef struct jazz_db_TransportVt {
    jz_err (*send)(void *self, jazz_protocol_SyncMessage message, jazz_db_TransportError *err);
    /* defaults below are used when the slot is NULL */
    jz_err (*try_send)(void *self, jazz_protocol_SyncMessage message, bool *returned, jazz_protocol_SyncMessage *back, jazz_db_TransportError *err);
    bool (*try_recv)(void *self, jazz_protocol_SyncMessage *out);
    jz_err (*try_recv_result)(void *self, bool *has, jazz_protocol_SyncMessage *out, jazz_db_TransportError *err);
    jz_err (*try_recv_owned_result)(void *self, bool *has, jazz_db_ReceivedSyncMessage *out, jazz_db_TransportError *err);
    jz_err (*poll_flush)(void *self, jazz_db_WireFlushStatus *out, jazz_db_TransportError *err);
    bool (*has_terminal_failure)(const void *self);
    jazz_db_WireError *(*remote_wire_error)(const void *self);
    bool (*incomplete_receive_timeout_ms)(const void *self, u64 *out);
    void *(*shared_auxiliary_endpoint)(const void *self);
    void (*set_trusted_encoder)(void *self, bool trusted);
    void *(*wire_inbound_context)(const void *self);
    bool (*connection_session_context)(const void *self, jazz_db_ConnectionSessionContext *out);
    bool (*permits_delegated_sessions)(const void *self);
    void (*drop)(void *self);
} jazz_db_TransportVt;
typedef struct jazz_db_Transport { void *self; const jazz_db_TransportVt *vt; } jazz_db_Transport;
/* Transport errors are returned as jz_err with code JAZZ_DB_TRANSPORT_ERR and detail in *err */
enum { JAZZ_DB_TRANSPORT_ERR = 0x3001 };
jz_err jazz_db__db__node_runtime__Transport__send(jazz_db_Transport *t, jazz_protocol_SyncMessage m, jazz_db_TransportError *err);
jz_err jazz_db__db__node_runtime__Transport__try_send(jazz_db_Transport *t, jazz_protocol_SyncMessage m, bool *returned, jazz_protocol_SyncMessage *back, jazz_db_TransportError *err);
bool jazz_db__db__node_runtime__Transport__try_recv(jazz_db_Transport *t, jazz_protocol_SyncMessage *out);
jz_err jazz_db__db__node_runtime__Transport__try_recv_result(jazz_db_Transport *t, bool *has, jazz_protocol_SyncMessage *out, jazz_db_TransportError *err);
jz_err jazz_db__db__node_runtime__Transport__try_recv_owned_result(jazz_db_Transport *t, bool *has, jazz_db_ReceivedSyncMessage *out, jazz_db_TransportError *err);
jz_err jazz_db__db__node_runtime__Transport__poll_flush(jazz_db_Transport *t, jazz_db_WireFlushStatus *out, jazz_db_TransportError *err);
bool jazz_db__db__node_runtime__Transport__has_terminal_failure(const jazz_db_Transport *t);
jazz_db_WireError *jazz_db__db__node_runtime__Transport__remote_wire_error(const jazz_db_Transport *t);
bool jazz_db__db__node_runtime__Transport__incomplete_receive_timeout_ms(const jazz_db_Transport *t, u64 *out);
void *jazz_db__db__node_runtime__Transport__shared_auxiliary_endpoint(const jazz_db_Transport *t);
void jazz_db__db__node_runtime__Transport__set_trusted_encoder(jazz_db_Transport *t, bool trusted);
void *jazz_db__db__node_runtime__Transport__wire_inbound_context(const jazz_db_Transport *t);
bool jazz_db__db__node_runtime__Transport__connection_session_context(const jazz_db_Transport *t, jazz_db_ConnectionSessionContext *out);
bool jazz_db__db__node_runtime__Transport__permits_delegated_sessions(const jazz_db_Transport *t);

/* ---------- QueryRuntimeWake (Arc<..> as Waker) ---------- */
typedef struct jazz_db_QueryRuntimeWake { bool *pending; jz_waker scheduler; } jazz_db_QueryRuntimeWake;
/* Rc<RefCell<Option<Waker>>> */
typedef struct jazz_db_WakerCache { bool has; jz_waker waker; } jazz_db_WakerCache;
bool jazz_db__db__node_runtime__make_query_runtime_waker(jazz_db_SharedTickScheduler *scheduler, bool *pending, jazz_db_WakerCache *cache, jz_waker *out);
void jazz_db__db__node_runtime__notify_write_state_waiters(jz_btree *waiters, jazz_model_TxId tx_id);

/* ---------- Node ---------- */
typedef JZ_VEC(jazz_db_TransactionWaitObserver) jazz_db_TransactionWaitObserverVec;
typedef JZ_VEC(jazz_protocol_AuthorityContext) jazz_db_AuthorityContextVec;
typedef struct jazz_db_HeldAuthority { bool has; jazz_protocol_AuthorityContext ctx; } jazz_db_HeldAuthority; /* Rc<RefCell<Option<AuthorityContext>>> */

struct jazz_db_Node {
    jazz_db_NodeMutex *node;                        /* SharedNodeState */
    bool has_owner_release_wait;                    /* RefCell<Option<LocalBoxFuture<()>>> */
    jazz_db_LocalFuture owner_release_wait;
    u8 mutation_owner_lifecycle;                    /* Cell<MutationOwnerLifecycle> */
    u32 close_owner;                                /* futures::lock::Mutex<()> */
    jazz_types_TxTime *tx_time_reservation_clock;   /* Rc<Cell<TxTime>> */
    jazz_types_NodeUuid node_uuid;
    bool receives_commits_as_local;
    jazz_db_SubscriptionList *subscriptions;
    jazz_db_UploadOutbox *outbox;
    jz_deque *pending_local_publications;           /* VecDeque<PendingLocalPublication> */
    jz_deque queued_mutations;                      /* VecDeque<QueuedMutationOperation> */
    u32 queued_mutation_active_leases;
    jazz_db_TransactionWaitObserverVec transaction_wait_observers;
    jz_btree queued_mutation_failures;              /* TxId -> Error */
    jz_btree deferred_rejection_discards;           /* BTreeSet<TxId> */
    jz_btree queued_open_transaction_failures;      /* OpenTransactionId -> Error */
    jz_btree reserved_mutations;                    /* BTreeSet<TxId> */
    jz_btree *pending_transaction_abandonments;     /* BTreeSet<OpenTransactionId> */
    bool transaction_abandonments_closed;
    bool transaction_abandonment_shutdown_pending;
    jazz_db_PendingUpstreamCommands *upstream_subscriptions;
    jz_deque *pending_subscription_finalizations;   /* VecDeque<PendingSubscriptionFinalization> */
    bool subscription_finalizations_closed;
    bool subscription_runtime_retired;
    jz_btree *latest_coverage_subscriptions;        /* CoverageKey -> SubscriptionKey */
    jz_btree *upstream_coverage_refcounts;          /* CoverageKey -> usize */
    jz_btree *awaiting_initial_authority_coverage;  /* BTreeSet<CoverageKey> */
    jazz_db_ActiveAuthorityViewReceipts *active_authority_view_receipts;
    jz_btree *coverage_refresh_generations;         /* CoverageKey -> u64 */
    jz_btree *query_coverage_registrations;         /* SubscriptionKey -> QueryCoverageRegistration */
    jz_btree *upstream_subscription_owners;         /* SubscriptionKey -> Vec<Weak<SubscriptionState>> */
    jz_btree *relay_upstream_subscription_owners;   /* RelayOwnerKey -> RelayUpstreamSubscriptionOwner */
    jz_btree *pending_relay_subscription_rejections;/* u64 -> VecDeque<RelaySubscriptionRejection> */
    jazz_db_PeerMutexVec connections;
    jazz_db_PeerMutexVec pending_detaches;
    jazz_db_SharedTickScheduler *scheduler;
    jazz_db_RemoteLinkTracker *remote_link;
    bool *query_runtime_wake_pending;               /* Arc<AtomicBool> */
    jazz_db_WakerCache *query_runtime_waker;
    jazz_db_UploadRetryClock *upload_retry_clock;   /* Rc<RefCell<Rc<dyn UploadRetryClock>>> */
    jz_btree *detached_large_value_uploads;         /* UpstreamUploadDestination -> LargeValueUploadQueues */
    jz_btree *large_value_upload_retry_deadlines;   /* TxId -> u64 */
    jz_btree *write_state_waiters;                  /* TxId -> Vec<WriteStateWaiter> */
    jazz_db_OpenSchemaAdmission *open_schema_admission;
    jz_btree *permission_advice_waiters;
    jazz_db_CurrentRowsRouter *current_rows;
    jazz_db_HeldRowChecks held_row_checks;
    jz_btree *local_fate_routes;                    /* TxId -> Vec<LocalFateRoute> */
    jazz_db_AuthorityContextVec *admitted_upstream_authorities;
    jazz_db_HeldAuthority *admitted_upstream_authority;
    jazz_db_MutationErrorState *mutation_errors;
    jz_btree *browser_relay_recovered_tx_ids;       /* BTreeSet<TxId> */
    u64 next_write_state_waiter_id;
    u64 next_subscription_nonce;
    u64 *subscriber_dirty_epoch;                    /* Rc<Cell<u64>> */
    u8 upstream_durability_floor;
    bool defer_local_persistence;
    jazz_db_PeerChunkResolver *chunk_resolver;
    bool *detach_covered_chunk_waits;               /* Rc<Cell<bool>> */
    void *local_chunk_reader;
    u64 observed_chunk_completion_generation;
    u64 local_subscription_dirty_generation;
    u64 observed_local_subscription_dirty_generation;
};
typedef struct jazz_db_Node jazz_db_Node;

/* QueuedMutationFuture = Pin<Box<dyn Future<Output = Result<(), Error>>>> */
typedef jazz_db_LocalFuture jazz_db_QueuedMutationFuture;

#define NODE_FN(name) jazz_db__db__node_runtime__Node__##name
jazz_db_Node *NODE_FN(new)(jazz_node_NodeState *node);
jz_err NODE_FN(reserve_transaction_id)(const jazz_db_Node *self, u64 now_ms, jazz_model_TxId *out);
void NODE_FN(begin_mutation_shutdown)(jazz_db_Node *self);
void NODE_FN(lock_close_owner)(jazz_db_Node *self);
void NODE_FN(unlock_close_owner)(jazz_db_Node *self);
jz_err NODE_FN(ensure_mutation_admission_open)(const jazz_db_Node *self);
jz_err NODE_FN(ensure_subscription_finalization_open)(const jazz_db_Node *self);
jazz_db_QueuedMutationStatus *NODE_FN(enqueue_mutation)(jazz_db_Node *self, jazz_model_TxId tx_id, jazz_db_QueuedMutationFuture future);
void NODE_FN(enqueue_reserved_write)(jazz_db_Node *self, jazz_model_TxId tx_id, jazz_db_QueuedMutationFuture future);
jz_err NODE_FN(enqueue_transaction_operation)(jazz_db_Node *self, jazz_model_OpenTransactionId open_tx_id, jazz_db_QueuedMutationFuture future);
/* read: a future whose output is a jz_err followed by `out_size` bytes of T; result arrives on the oneshot */
jazz_db_Oneshot *NODE_FN(enqueue_transaction_read)(jazz_db_Node *self, jazz_model_OpenTransactionId open_tx_id, jazz_db_LocalFuture read, u32 out_size);
u32 NODE_FN(queued_mutation_count)(const jazz_db_Node *self);
jazz_db_Oneshot *NODE_FN(queued_mutation_barrier)(jazz_db_Node *self);
jazz_db_QueuedMutationStatus *NODE_FN(enqueue_transaction_commit)(jazz_db_Node *self, jazz_model_OpenTransactionId open_tx_id, jazz_model_TxId tx_id, jazz_model_TxKind kind, jazz_db_QueuedMutationFuture future);
void NODE_FN(enqueue_transaction_cleanup)(jazz_db_Node *self, jazz_db_QueuedMutationFuture future);
void NODE_FN(enqueue_owner_operation)(jazz_db_Node *self, jazz_db_QueuedMutationFuture future);
bool NODE_FN(owner_queue_is_quiescent)(const jazz_db_Node *self);
void NODE_FN(enqueue_transaction_cleanup_with_completion)(jazz_db_Node *self, jazz_db_QueuedMutationFuture future, const jazz_db_QueuedMutationCompletion *completion);
bool NODE_FN(owner_is_available)(const jazz_db_Node *self);
bool NODE_FN(owner_is_available_or_wake_when_released)(jazz_db_Node *self);
bool NODE_FN(queued_transaction_error)(const jazz_db_Node *self, jazz_model_OpenTransactionId id, jz_err *out);
void NODE_FN(retire_queued_transaction_error)(jazz_db_Node *self, jazz_model_OpenTransactionId id);
bool NODE_FN(poll_queued_mutation_once)(jazz_db_Node *self);
void NODE_FN(drain_queued_mutations)(jazz_db_Node *self);
void NODE_FN(upstream_register_shape_options)(const jazz_db_Node *self, jazz_model_DurabilityTier tier, jazz_protocol_ReadViewSpec *read_view, jazz_protocol_RegisterShapeOptions *out);
bool NODE_FN(receives_commits_as_local)(const jazz_db_Node *self);
jazz_db_NodeMutex *NODE_FN(node)(const jazz_db_Node *self);
void NODE_FN(set_large_value_staging_policy)(jazz_db_Node *self, const void *policy);
jz_err NODE_FN(evict_expired_staged_large_values)(jazz_db_Node *self, u32 *out);
void NODE_FN(set_non_durable_client)(jazz_db_Node *self);
jz_err NODE_FN(configure_scope_isolated_client_relay)(jazz_db_Node *self, void *scope);
void NODE_FN(set_deferred_local_persistence)(jazz_db_Node *self, bool deferred);
jz_err NODE_FN(set_permissions_ready)(jazz_db_Node *self, bool ready);
void NODE_FN(queue_pending_upload)(jazz_db_Node *self, jazz_model_TxId tx_id, bool has_unit, jazz_protocol_SyncMessage unit);
void NODE_FN(queue_local_publication)(jazz_db_Node *self, jazz_node_PublishedTransaction *published, bool has_unit, jazz_protocol_SyncMessage upload_unit);
bool NODE_FN(has_pending_local_publications)(const jazz_db_Node *self);
typedef struct jazz_db_TxTables { jazz_model_TxId tx_id; JZ_VEC(jz_str) tables; } jazz_db_TxTables;
typedef JZ_VEC(jazz_db_TxTables) jazz_db_TxTablesVec;
void NODE_FN(pending_local_publication_tables)(const jazz_db_Node *self, jazz_db_TxTablesVec *out);
bool NODE_FN(is_pending_local_publication)(const jazz_db_Node *self, jazz_model_TxId tx_id);
jz_err NODE_FN(poll_local_publication_settlement_once)(jazz_db_Node *self, bool *pending);
jz_err NODE_FN(lock_for_large_value_staging)(jazz_db_Node *self, jazz_node_NodeState **out);
jz_err NODE_FN(settle_local_publications)(jazz_db_Node *self);
jz_err NODE_FN(restore_pending_uploads)(jazz_db_Node *self, jazz_db_DbIdentity identity);
jz_err NODE_FN(restore_backend_pending_uploads)(jazz_db_Node *self, jazz_types_NodeUuid node_id);
jz_err NODE_FN(restore_browser_relay_pending_uploads)(jazz_db_Node *self, jazz_types_AuthorSubject author);
void NODE_FN(mark_subscriber_connections_dirty)(jazz_db_Node *self);
void NODE_FN(mark_subscriber_connections_dirty_after_query_runtime_wake)(jazz_db_Node *self);
jazz_protocol_SubscriptionKey NODE_FN(next_subscription_key)(jazz_db_Node *self, const jazz_model_ValidatedQuery *shape, jazz_protocol_ReadViewKey read_view);
void NODE_FN(set_scheduler)(jazz_db_Node *self, const jazz_db_TickScheduler *scheduler /* NULL = None */);
void NODE_FN(schedule_tick)(const jazz_db_Node *self, jazz_db_TickUrgency urgency);
bool NODE_FN(query_runtime_waker)(const jazz_db_Node *self, jz_waker *out);
jz_err NODE_FN(lock_for_transaction_open)(jazz_db_Node *self, jazz_model_OpenTransactionId open_tx_id, bool owner_operation_admitted, jazz_node_NodeState **out);
jz_err NODE_FN(lock_for_transaction_operation)(jazz_db_Node *self, jazz_model_OpenTransactionId open_tx_id, bool owner_operation_admitted, jazz_node_NodeState **out);
bool NODE_FN(mark_transaction_abandoned)(jazz_db_Node *self, jazz_model_OpenTransactionId id);
void NODE_FN(clear_transaction_abandonment)(jazz_db_Node *self, jazz_model_OpenTransactionId id);
void NODE_FN(abandon_or_enqueue_transaction)(jazz_db_Node *self, jazz_model_OpenTransactionId id);
void NODE_FN(begin_transaction_abandonment_shutdown)(jazz_db_Node *self);
jz_err NODE_FN(finish_transaction_abandonment_shutdown)(jazz_db_Node *self, u32 *out);
void NODE_FN(evict_released_large_values)(jazz_db_Node *self);
void NODE_FN(enqueue_subscription_finalization)(jazz_db_Node *self, jazz_db_PendingSubscriptionFinalization command);
jz_err NODE_FN(drain_subscription_finalizations)(jazz_db_Node *self, u32 *out);
void NODE_FN(begin_subscription_finalization_shutdown)(jazz_db_Node *self);
void NODE_FN(retire_subscription_runtime_after_close)(jazz_db_Node *self);
void NODE_FN(set_mutation_error_callback)(jazz_db_Node *self, const jazz_db_MutationErrorCallback *cb);
jz_err NODE_FN(flush_deferred_rejection_discards)(jazz_db_Node *self);
/* Option<Result<TxId, Error>>: returns true when an outcome exists (err.code != 0 for Err) */
bool NODE_FN(transaction_wait_outcome)(jazz_db_Node *self, jazz_model_TxId tx_id, jazz_model_DurabilityTier tier, jz_err *err, jazz_model_TxId *out);
bool NODE_FN(queued_mutation_write_state)(const jazz_db_Node *self, jazz_model_TxId tx_id, jazz_db_WriteState *out, jz_err *err);
bool NODE_FN(take_queued_mutation_failure)(jazz_db_Node *self, jazz_model_TxId tx_id, jz_err *out);
/* Box<dyn FnOnce(Result<TxId, Error>)> */
typedef struct jazz_db_TxIdCallback { void (*call)(void *ctx, jz_err result, jazz_model_TxId tx_id); void *ctx; void (*drop)(void *ctx); } jazz_db_TxIdCallback;
void NODE_FN(wait_for_transaction_with)(jazz_db_Node *self, jazz_model_TxId tx_id, jazz_db_WriteWaitOptions opts, jazz_db_TxIdCallback callback);
void NODE_FN(wait_for_write_with)(jazz_db_Node *self, jazz_model_TxId tx_id, jazz_db_QueuedMutationAlias *alias /* NULL = None */, jazz_db_WriteWaitOptions opts, jazz_db_TxIdCallback callback);
jz_err NODE_FN(wait_for_transaction)(jazz_db_Node *self, jazz_model_TxId tx_id, jazz_db_WriteWaitOptions opts, jazz_model_TxId *out);
void NODE_FN(poll_transaction_wait_observers)(jazz_db_Node *self);
void NODE_FN(drain_transaction_wait_observers)(jazz_db_Node *self);
void NODE_FN(deliver_pending_mutation_errors)(jazz_db_Node *self);
jazz_db_PermissionAdviceFuture NODE_FN(request_permission_advice)(jazz_db_Node *self, jazz_protocol_PermissionAdviceAction *action);
jazz_db_PermissionAdviceFuture NODE_FN(request_permission_advice_with_delegated_session)(jazz_db_Node *self, jazz_protocol_PermissionAdviceAction *action, jazz_protocol_DelegatedSessionBinding session);
void NODE_FN(cancel_permission_advice_request)(jazz_db_Node *self, jazz_protocol_PermissionAdviceRequestId request_id);
jazz_db_WriteStateChange NODE_FN(register_write_state_waiter)(jazz_db_Node *self, jazz_model_TxId tx_id);
jz_err NODE_FN(refresh_subscriptions)(jazz_db_Node *self, u32 *out);
jz_err NODE_FN(refresh_subscriptions_with_tables)(jazz_db_Node *self, const jz_hmap *changed_tables, u32 *out);
jz_err NODE_FN(connect_upstream)(jazz_db_Node *self, jazz_db_Transport transport, jazz_db_PeerMutex **out);
jz_err NODE_FN(try_connect_upstream)(jazz_db_Node *self, jazz_db_Transport transport, jazz_db_PeerMutex **out);
jz_err NODE_FN(accept_subscriber_with_claims_async)(jazz_db_Node *self, jazz_db_Transport transport, jazz_types_AuthorSubject identity, jz_btree claims, jazz_node_CommitUnitTrust trust, jazz_db_PeerMutex **out);
bool NODE_FN(detach_connection)(jazz_db_Node *self, jazz_db_PeerMutex *connection);
jz_err NODE_FN(detach_connection_async)(jazz_db_Node *self, jazz_db_PeerMutex *connection, bool *out);
jz_err NODE_FN(tick)(jazz_db_Node *self, jazz_db_DbTickStats *out);
#undef NODE_FN

jz_err jazz_db__db__node_runtime__refresh_subscriptions_in(jazz_db_NodeMutex *node, jazz_db_SubscriptionList *subscriptions, jazz_db_ActiveAuthorityViewReceipts *receipts, const jz_waker *waker, u32 *out);
jz_err jazz_db__db__node_runtime__refresh_subscriptions_in_with_tables(jazz_db_NodeMutex *node, jazz_db_SubscriptionList *subscriptions, jazz_db_ActiveAuthorityViewReceipts *receipts, const jz_hmap *changed_tables /* NULL = None */, const jz_waker *waker /* NULL = None */, u32 *out);
void jazz_db__db__node_runtime__register_upstream_subscription_owner(jz_btree *owners, const jazz_db_UpstreamCoverageHandle *handles, u32 n, jazz_db_SubscriptionState *state);
void jazz_db__db__node_runtime__unregister_upstream_subscription_owner(jz_btree *owners, jazz_protocol_SubscriptionKey subscription, jazz_db_SubscriptionState *weak);
bool jazz_db__db__node_runtime__retire_relay_upstream_subscription(jz_btree *owners, jazz_protocol_SubscriptionKey subscription, u64 downstream_connection_epoch, const jazz_db_CoverageKey *coverage, jazz_db_RelayUpstreamSubscriptionOwner *out);
typedef struct jazz_db_KeyedRelayOwner { jazz_protocol_SubscriptionKey subscription; jazz_db_RelayUpstreamSubscriptionOwner owner; } jazz_db_KeyedRelayOwner;
typedef JZ_VEC(jazz_db_KeyedRelayOwner) jazz_db_KeyedRelayOwnerVec;
void jazz_db__db__node_runtime__take_relay_upstream_subscription_owner(jz_btree *owners, jazz_protocol_SubscriptionKey subscription, jazz_db_KeyedRelayOwnerVec *out);
void jazz_db__db__node_runtime__retire_relay_upstream_subscriptions_for_connection(jz_btree *owners, u64 downstream_connection_epoch, jazz_db_KeyedRelayOwnerVec *out);
bool jazz_db__db__node_runtime__register_shape_rejection_matches(jazz_protocol_SubscriptionKey subscription, const jazz_model_ValidatedQuery *shape, const jazz_protocol_RegisterShapeOptions *opts);
u32 jazz_db__db__node_runtime__route_upstream_subscription_rejection(jazz_db_SubscriptionList *subscriptions, jz_btree *owners, jazz_protocol_SubscriptionKey subscription, const jazz_protocol_SubscribeRejectReason *reason);

#endif
