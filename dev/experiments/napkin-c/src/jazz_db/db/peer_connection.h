/* peer_connection.h: port of crates/jazz/layers/db/src/db/peer_connection.rs
 * Per-peer synchronization, repair, and resume machinery. */
#ifndef JAZZ_DB_PEER_CONNECTION_H
#define JAZZ_DB_PEER_CONNECTION_H
#include "jz_rt.h"
#include "../_ext_db-a.h"
#include "node_runtime.h"

typedef JZ_VEC(jazz_model_TxId) jazz_db_TxIdVec;

/* PendingAuthorizationScopeSequence */
typedef struct jazz_db_PendingAuthorizationScopeSequence {
    jazz_protocol_PermissionAdviceRequestId request_id;
    jazz_protocol_AuthorizationSupportScopeKey *key;
    jazz_db_ServedAuthorizationScopeHydration hydration;
    u32 next_step;
} jazz_db_PendingAuthorizationScopeSequence;

/* PendingSubscriberControlResponse */
typedef enum { JAZZ_PSCR_DIRECT = 0, JAZZ_PSCR_WITH_SYNC_CONTEXT = 1, JAZZ_PSCR_AUTHORIZATION_SCOPE_SEQUENCE = 2 } jazz_db_PendingSubscriberControlResponseTag;
typedef struct jazz_db_PendingSubscriberControlResponse {
    u32 tag;
    jazz_protocol_SyncMessage message;                 /* Direct / WithSyncContext */
    jazz_db_PendingAuthorizationScopeSequence sequence; /* AuthorizationScopeSequence */
} jazz_db_PendingSubscriberControlResponse;
void jazz_db__db__peer_connection__PendingSubscriberControlResponse__drop(void *p);
void jazz_db__db__peer_connection__PendingSubscriberControlResponse__clone(void *dst, const void *src);
void jazz_db__db__peer_connection__queue_sync_context_control(jz_deque *pending, jazz_protocol_SyncMessage message);

/* PendingLargeValueUpload */
typedef struct jazz_db_PendingLargeValueUpload {
    groove_LargeValueRef value_ref;
    jz_deque requested;  /* VecDeque<NodeRef> */
    jz_deque in_flight;  /* VecDeque<NodeRef> */
    bool has_retry_not_before_ms;
    u64 retry_not_before_ms;
    bool started;
} jazz_db_PendingLargeValueUpload;
void jazz_db__db__peer_connection__PendingLargeValueUpload__drop(void *p);
/* LargeValueUploadQueues = BTreeMap<TxId, VecDeque<PendingLargeValueUpload>> */
extern const jz_btree_desc jazz_db_large_value_upload_queues_desc;
void jazz_db__db__peer_connection__take_reconnectable_large_value_uploads(jz_btree *uploads, jz_btree *awaiting, jz_btree *out);
void jazz_db__db__peer_connection__merge_reconnectable_large_value_uploads(jz_btree *destination_uploads, jz_btree uploads);

/* map descriptors for connection-state maps (defined in peer_connection.c) */
extern const jz_btree_desc jazz_db_sent_subscriptions_desc;            /* SubscriptionKey -> PendingUpstreamSubscription */
extern const jz_btree_desc jazz_db_subscription_global_time_desc;     /* SubscriptionKey -> GlobalTime */
extern const jz_btree_desc jazz_db_author_u64_desc;                   /* AuthorSubject -> u64 */
extern const jz_btree_desc jazz_db_txid_large_value_ref_desc;         /* TxId -> LargeValueRef */
extern const jz_btree_desc jazz_db_pending_catalogue_subscriptions_desc; /* SubscriptionKey -> PendingCatalogueSubscription */

/* ConnectionLink */
typedef struct jazz_db_UpstreamConnectionState {
    bool local_receiver;
    jazz_db_PendingUpstreamCommands pending;
    jazz_db_PendingUpstreamCommands *upstream_subscriptions;
    jz_btree announced_shapes;                /* BTreeSet<ShapeRegistrationKey> */
    jz_btree sent_subscriptions;              /* SubscriptionKey -> PendingUpstreamSubscription */
    jz_btree awaiting_support_snapshots;      /* SubscriptionKey -> GlobalTime */
    jz_btree sent_session_claim_revisions;    /* AuthorSubject -> u64 */
    jazz_db_UploadOutbox *outbox;
    jz_btree uploaded;                        /* BTreeSet<TxId> */
    jz_btree large_value_uploads;             /* LargeValueUploadQueues */
    jz_btree awaiting_large_value_uploads;    /* TxId -> LargeValueRef */
    jz_btree failed_large_value_uploads;      /* BTreeSet<TxId> */
    jz_btree missing_body_resends;            /* BTreeSet<SubscriptionKey> */
    bool has_expected_scope_authority;
    jazz_protocol_AuthorityContext expected_scope_authority;
    jazz_db_AuthorizationScopeLeaseManager scope_lease_manager;
    jazz_db_ReadBarrierSender read_barrier;
} jazz_db_UpstreamConnectionState;

typedef struct jazz_db_SubscriberConnectionState {
    jazz_node_PeerState *peer;
    jazz_node_CommitUnitIngestContext ingest_context;
    jz_btree session_claims;                  /* BTreeMap<String, Value> */
    u64 session_claim_revision;
    bool local_receiver;
    jazz_db_UploadOutbox *outbox;
    jazz_db_PendingUpstreamCommands *upstream_subscriptions;
    jz_btree served;                          /* SubscriptionKey -> CoverageKey */
    jz_btree coverage_groups;                 /* CoverageKey -> CoverageGroup */
    jz_btree shape_registrations;             /* ShapeRegistrationKey -> SubscriberShapeRegistration */
    jz_deque deferred_subscribe_rejections;   /* VecDeque<PendingSubscriberControlResponse> */
    jz_btree pending_catalogue_subscriptions; /* SubscriptionKey -> PendingCatalogueSubscription */
    jz_btree authority_scope_hydrations;      /* AuthorizationSupportScopeKey -> Hydration */
    u64 authority_scope_hydration_count;
    bool serve_dirty;
    jazz_db_ReadBarrierReceiver read_barrier;
} jazz_db_SubscriberConnectionState;

typedef enum { JAZZ_LINK_UPSTREAM = 0, JAZZ_LINK_SUBSCRIBER = 1 } jazz_db_ConnectionLinkTag;
typedef struct jazz_db_ConnectionLink {
    u32 tag;
    union { jazz_db_UpstreamConnectionState upstream; jazz_db_SubscriberConnectionState subscriber; } v;
} jazz_db_ConnectionLink;

/* PendingCatalogueSubscription */
typedef struct jazz_db_PendingCatalogueSubscription {
    jazz_protocol_Subscribe subscribe;
    jazz_db_PolicyBinding policy_binding;
    u32 encoded_bytes;
} jazz_db_PendingCatalogueSubscription;
void jazz_db__db__peer_connection__PendingCatalogueSubscription__drop(void *p);

/* ResumeCursor */
typedef struct jazz_db_ResumeCursor {
    jazz_node_PeerState *peer;
    jazz_node_CommitUnitIngestContext ingest_context;
    jz_btree session_claims;
    u64 session_claim_revision;
} jazz_db_ResumeCursor;

/* ChunkResponseBatch (opaque, owned by the protocol crate) */
typedef struct jazz_db_ChunkResponseBatch jazz_db_ChunkResponseBatch;

struct jazz_db_PeerConnection {
    jazz_db_Transport transport;
    jz_deque staged_inbound;                       /* VecDeque<StagedInboundMessage> */
    jazz_db_NodeMutex *node;
    jazz_db_SubscriptionList *subscriptions;
    jz_btree *upstream_subscription_owners;
    jz_btree *relay_upstream_subscription_owners;
    jz_btree *pending_relay_subscription_rejections;
    jz_btree *latest_coverage_subscriptions;
    jz_btree *awaiting_initial_authority_coverage;
    jz_btree *query_coverage_registrations;
    jazz_db_ActiveAuthorityViewReceipts *active_authority_view_receipts;
    jz_btree *coverage_refresh_generations;
    jazz_db_SharedTickScheduler *scheduler;
    bool *query_runtime_wake_pending;
    jazz_db_WakerCache *query_runtime_waker;
    jazz_db_UploadRetryClock *upload_retry_clock;
    bool has_upstream_upload_destination;
    jazz_db_UpstreamUploadDestination upstream_upload_destination;
    jz_btree *large_value_upload_retry_deadlines;
    jz_btree *write_state_waiters;
    jazz_db_OpenSchemaAdmission *open_schema_admission;
    jz_btree *permission_advice_waiters;
    jazz_db_CurrentRowsRouter *current_rows;
    jz_btree *local_fate_routes;
    jazz_db_HeldAuthority *admitted_upstream_authority;
    jazz_db_SyncMessageVec *downstream_fates;     /* PendingDownstreamFates */
    jazz_db_MutationErrorState *mutation_errors;
    jz_btree *browser_relay_recovered_tx_ids;
    u64 *subscriber_dirty_epoch;
    u64 observed_subscriber_dirty_epoch;
    u64 local_replay_epoch;
    u64 observed_session_claim_revision;
    bool inbound_authority_receipt_quarantine;
    u64 connection_epoch;
    bool has_startup_error;
    jz_err startup_error;
    jazz_db_TxIdVec released_outbox_tx_ids;
    jazz_db_ChunkResponseBatch *pending_chunk_response; /* Option */
    jz_deque pending_control_responses;           /* VecDeque<PendingSubscriberControlResponse> */
    jazz_db_ConnectionLink link;
    bool has_last_resume_bytes;
    u32 last_resume_bytes;
    jazz_db_PeerIoPump auxiliary_pump;
};

#define PC_FN(name) jazz_db__db__peer_connection__PeerConnection__##name
void PC_FN(take_released_outbox_tx_ids)(jazz_db_PeerConnection *self, jazz_db_TxIdVec *out);
void PC_FN(forget_released_outbox_tx_ids)(jazz_db_PeerConnection *self, const jz_hmap *released);
jazz_db_PeerIoPump PC_FN(io_pump)(const jazz_db_PeerConnection *self);
void PC_FN(update_authenticated_session_claims)(jazz_db_PeerConnection *self, jz_btree claims);
jazz_db_WireError *PC_FN(remote_wire_error)(const jazz_db_PeerConnection *self);
bool PC_FN(last_resume_bytes)(const jazz_db_PeerConnection *self, u32 *out);
jazz_node_FullDiffFallbackMetrics PC_FN(full_diff_fallbacks)(const jazz_db_PeerConnection *self);
bool PC_FN(take_resume_cursor)(jazz_db_PeerConnection *self, jazz_db_ResumeCursor *out);
jz_err PC_FN(rehydrate_subscriber_views)(jazz_db_PeerConnection *self);
void PC_FN(stage_inbound_without_authority_receipt)(jazz_db_PeerConnection *self);
jz_err PC_FN(tick)(jazz_db_PeerConnection *self, jazz_db_DbTickStats *out);
bool PC_FN(mark_subscriber_dirty)(jazz_db_PeerConnection *self);
void jazz_db__db__peer_connection__PeerConnection__drop(jazz_db_PeerConnection *self);
#undef PC_FN

u32 jazz_db__db__peer_connection__route_subscription_refresh_failure(jazz_db_SubscriptionList *subscriptions, const jz_err *error);
jazz_protocol_SubscriptionKey jazz_db__db__peer_connection__coverage_group_subscription_key(const jazz_db_CoverageKey *coverage);
void jazz_db__db__peer_connection__schedule_tick_in(jazz_db_SharedTickScheduler *scheduler, jazz_db_TickUrgency urgency);
jz_err jazz_db__db__peer_connection__transport_error(jazz_db_TransportError error);
jz_err jazz_db__db__peer_connection__send_subscriber_with_sync_context(jazz_db_NodeMutex *node, jazz_node_PeerState *peer, jazz_db_Transport *transport, jz_btree *local_fate_routes, jazz_db_SyncMessageVec *downstream_fates, jazz_protocol_SyncMessage message);
bool jazz_db__db__peer_connection__authorization_scope_receipt_matches_transport_context(const jazz_protocol_AuthorizationScopeReceipt *receipt, jazz_protocol_AuthorityContext expected, jazz_types_AuthorSubject expected_subject, bool has_applied_cut, jazz_types_GlobalTime applied_cut);

#endif
