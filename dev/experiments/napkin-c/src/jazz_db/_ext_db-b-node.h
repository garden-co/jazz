/* _ext_db-b-node.h: mirrors of unit db-a symbols (db/node_runtime.rs,
 * db/peer_connection.rs, db/mutations.rs) that db-b calls and whose
 * signatures use db.rs types. Included at the end of db.h. */
#ifndef JAZZ_DB_EXT_DB_B_NODE_H
#define JAZZ_DB_EXT_DB_B_NODE_H
#include "db/read_barrier.h"

/* peer_connection.rs */
void jazz_db__db__peer_connection__schedule_tick_in(jazz_db_SharedTickScheduler scheduler, u32 urgency);


/* node_runtime.rs: Node (node_runtime.rs:224-307), mirrored with db-b type names */
typedef struct jazz_db_RemoteLinkTracker jazz_db_RemoteLinkTracker;
struct jazz_db_Node {
    jazz_db_SharedNodeState *node;
    bool has_owner_release_wait; jazz_db_LocalFuture owner_release_wait;
    u8 mutation_owner_lifecycle;
    u32 close_owner;
    jazz_types_TxTime *tx_time_reservation_clock;
    jazz_types_NodeUuid node_uuid;
    bool receives_commits_as_local;
    jazz_db_SubscriptionList subscriptions;
    jazz_db_Outbox outbox;
    jz_deque *pending_local_publications;
    jz_deque queued_mutations;
    u32 queued_mutation_active_leases;
    jz_vec transaction_wait_observers;
    jz_btree queued_mutation_failures;
    jz_btree deferred_rejection_discards;
    jz_btree queued_open_transaction_failures;
    jz_btree reserved_mutations;
    jz_btree *pending_transaction_abandonments;
    bool transaction_abandonments_closed;
    bool transaction_abandonment_shutdown_pending;
    jazz_db_PendingUpstreamCommandsCell *upstream_subscriptions;
    jz_deque *pending_subscription_finalizations;
    bool subscription_finalizations_closed;
    bool subscription_runtime_retired;
    jz_btree *latest_coverage_subscriptions;
    jz_btree *upstream_coverage_refcounts;
    jz_btree *awaiting_initial_authority_coverage;
    jazz_db_ActiveAuthorityViewReceipts active_authority_view_receipts;
    jz_btree *coverage_refresh_generations;
    jz_btree *query_coverage_registrations;
    jz_btree *upstream_subscription_owners;
    jz_btree *relay_upstream_subscription_owners;
    jz_btree *pending_relay_subscription_rejections;
    jz_vec connections;
    jz_vec pending_detaches;
    jazz_db_SharedTickScheduler scheduler;
    jazz_db_RemoteLinkTracker *remote_link;
    bool *query_runtime_wake_pending;
    void *query_runtime_waker;
    jazz_db_SharedUploadRetryClockCell *upload_retry_clock;
    jz_btree *detached_large_value_uploads;
    jz_btree *large_value_upload_retry_deadlines;
    jazz_db_WriteStateWaiters write_state_waiters;
    jazz_db_OpenSchemaAdmission open_schema_admission;
    jazz_db_PermissionAdviceWaiters permission_advice_waiters;
    void *current_rows;            /* row_availability::CurrentRowsRouter */
    jazz_db_HeldRowChecks *held_row_checks; /* Rc<RefCell<HeldRowChecks>> */
    jazz_db_LocalFateRoutes local_fate_routes;
    void *admitted_upstream_authorities;
    void *admitted_upstream_authority;
    jazz_db_SharedMutationErrors mutation_errors;
    jz_btree *browser_relay_recovered_tx_ids;
    u64 next_write_state_waiter_id;
    u64 next_subscription_nonce;
    u64 *subscriber_dirty_epoch;
    u8 upstream_durability_floor;
    bool defer_local_persistence;
    jazz_db_PeerChunkResolver chunk_resolver;
    bool *detach_covered_chunk_waits;
    groove_LocalChunkReader local_chunk_reader;
    u64 observed_chunk_completion_generation;
    u64 local_subscription_dirty_generation;
    u64 observed_local_subscription_dirty_generation;
};
#define NODE_RT_FN(name) jazz_db__db__node_runtime__Node__##name
void NODE_RT_FN(abandon_or_enqueue_transaction)(jazz_db_Node *self, jazz_model_OpenTransactionId id);
#undef NODE_RT_FN


/* peer_connection.rs (db-a) helpers used by db-b */
void jazz_db__db__peer_connection__queue_sync_context_control(jz_deque *pending, jazz_protocol_SyncMessage message);
jz_err jazz_db__db__peer_connection__transport_error(jz_err error);
bool jazz_db__db__peer_connection__handle_transport_backpressure(jazz_db_SharedNodeState *node, jazz_db_SharedTickScheduler scheduler, const jz_err *error);

/* PeerConnection (peer_connection.rs, db-a): mirrored with the fields db-b touches */
typedef struct jazz_db_UpstreamLinkMirror {
    bool has_expected_scope_authority; jazz_protocol_AuthorityContext expected_scope_authority;
    jazz_db_ReadBarrierSender read_barrier;
} jazz_db_UpstreamLinkMirror;
typedef struct jazz_db_SubscriberLinkMirror { jazz_db_ReadBarrierReceiver read_barrier; } jazz_db_SubscriberLinkMirror;
typedef enum { JAZZ_LINK_UPSTREAM = 0, JAZZ_LINK_SUBSCRIBER = 1 } jazz_db_ConnectionLinkTag;
typedef struct jazz_db_ConnectionLink { u32 tag; union { jazz_db_UpstreamLinkMirror upstream; jazz_db_SubscriberLinkMirror subscriber; } v; } jazz_db_ConnectionLink;
typedef struct jazz_db_HeldAuthorityCell { bool has; jazz_protocol_AuthorityContext ctx; } jazz_db_HeldAuthorityCell;
typedef struct jazz_db_PeerConnection {
    jazz_db_Transport transport;
    jz_deque staged_inbound;
    jazz_db_SharedNodeState *node;
    jazz_db_SubscriptionList subscriptions;
    jz_btree *upstream_subscription_owners;
    jz_btree *relay_upstream_subscription_owners;
    jz_btree *pending_relay_subscription_rejections;
    jz_btree *latest_coverage_subscriptions;
    jz_btree *awaiting_initial_authority_coverage;
    jz_btree *query_coverage_registrations;
    jazz_db_ActiveAuthorityViewReceipts active_authority_view_receipts;
    jz_btree *coverage_refresh_generations;
    jazz_db_SharedTickScheduler scheduler;
    bool *query_runtime_wake_pending;
    void *query_runtime_waker;
    jazz_db_SharedUploadRetryClockCell *upload_retry_clock;
    bool has_upstream_upload_destination; jazz_db_UpstreamUploadDestination upstream_upload_destination;
    jz_btree *large_value_upload_retry_deadlines;
    jazz_db_WriteStateWaiters write_state_waiters;
    jazz_db_OpenSchemaAdmission open_schema_admission;
    jazz_db_PermissionAdviceWaiters permission_advice_waiters;
    struct jazz_db_CurrentRowsRouter *current_rows;
    jazz_db_LocalFateRoutes local_fate_routes;
    jazz_db_HeldAuthorityCell *admitted_upstream_authority;
    jazz_db_PendingDownstreamFates downstream_fates;
    jazz_db_SharedMutationErrors mutation_errors;
    jz_btree *browser_relay_recovered_tx_ids;
    u64 *subscriber_dirty_epoch;
    u64 observed_subscriber_dirty_epoch;
    u64 local_replay_epoch;
    u64 observed_session_claim_revision;
    bool inbound_authority_receipt_quarantine;
    u64 connection_epoch;
    bool has_startup_error; jz_err startup_error;
    JZ_VEC(jazz_model_TxId) released_outbox_tx_ids;
    void *pending_chunk_response;
    jz_deque pending_control_responses;
    jazz_db_ConnectionLink link;
    bool has_last_resume_bytes; u32 last_resume_bytes;
    jazz_db_PeerIoPump auxiliary_pump;
} jazz_db_PeerConnection;

#endif
