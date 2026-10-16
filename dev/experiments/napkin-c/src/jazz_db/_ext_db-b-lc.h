/* _ext_db-b-lc.h: externs used by db/lifecycle.c (unit db-b).
 * Mirrors of jazz_node NodeState, jazz_model and db-a node_runtime /
 * mutations symbols, declared with db-b's own type names. */
#ifndef JAZZ_DB_EXT_DB_B_LC_H
#define JAZZ_DB_EXT_DB_B_LC_H
#include "db.h"
#include "db/config.h"

/* ---- opaque mirrors of db-a / node types ---- */
typedef struct jazz_db_PeerMutex jazz_db_PeerMutex;          /* Rc<LocalMutex<PeerConnection>> */
typedef struct jazz_db_Oneshot jazz_db_Oneshot;              /* oneshot::Receiver<Result<T,Error>> */
typedef struct jazz_db_bTxIdCallback { void (*call)(void *ctx, jz_err result, jazz_model_TxId tx_id); void *ctx; void (*drop)(void *ctx); } jazz_db_bTxIdCallback;
typedef struct jazz_db_bQueuedMutationCompletion { void (*call)(void *ctx, jz_err result); void *ctx; void (*drop)(void *ctx); } jazz_db_bQueuedMutationCompletion;
/* PublicationOutcome<T> (value type-erased, caller-owned) */
typedef struct jazz_db_bPublicationOutcome {
    JZ_VEC(jazz_node_PublishedTransaction *) publications;
    jz_deque post_settlement_work;
    void *value;
    u32 value_size;
} jazz_db_bPublicationOutcome;
void jazz_node__node__PublicationOutcome__published(jazz_db_bPublicationOutcome *out, void *value, u32 value_size, jazz_node_PublishedTransaction *p);
typedef struct groove_StagedLargeValueId { u8 b[16]; } groove_StagedLargeValueId;
typedef struct jazz_db_StreamingValueUpload jazz_db_StreamingValueUpload;
groove_StagedLargeValueId jazz_db__db__mutations__StreamingValueUpload__cleanup_id(jazz_db_StreamingValueUpload *upload);
typedef struct jazz_node_LargeValueStagingPolicy jazz_node_LargeValueStagingPolicy;
typedef struct jazz_protocol_ResumeCursor jazz_protocol_ResumeCursor;
typedef struct jazz_node_SchemaLineagePublication jazz_node_SchemaLineagePublication;
typedef struct jazz_node_MergeableCommit jazz_node_MergeableCommit;
typedef JZ_VEC(jazz_model_TxId) jazz_db_TxIdVec;

/* ---- jazz_model ---- */
void jazz_model__schema__JazzSchema__empty(jazz_model_JazzSchema *out);
bool jazz_model__schema__JazzSchema__eq(const jazz_model_JazzSchema *a, const jazz_model_JazzSchema *b);
bool jazz_model__schema__TableSchema__read_policy_eq(const jazz_model_TableSchema *a, const jazz_model_TableSchema *b);
bool jazz_model__schema__TableSchema__write_policies_eq(const jazz_model_TableSchema *a, const jazz_model_TableSchema *b);
bool jazz_model__schema__TableSchema__indexed_columns_eq(const jazz_model_TableSchema *a, const jazz_model_TableSchema *b);
bool jazz_model__schema__TableSchema__composite_indexes_eq(const jazz_model_TableSchema *a, const jazz_model_TableSchema *b);
void jazz_db__db__SchemaViewId__debug(jz_str *out, const jazz_db_SchemaViewId *id);
typedef struct jazz_model_SchemaVersion jazz_model_SchemaVersion;
jazz_model_SchemaVersion *jazz_model__schema__SchemaVersion__new(jazz_model_JazzSchema schema);

/* ---- jazz_node NodeState ---- */
#define LNS(n) jazz_node__node__NodeState__##n
jz_err LNS(new_client)(jazz_types_NodeUuid node, jazz_model_JazzSchema schema, groove_BoxedStorage *storage, bool history_complete, jazz_node_NodeState **out);
jz_err LNS(new_history_complete)(jazz_types_NodeUuid node, jazz_model_JazzSchema schema, groove_BoxedStorage *storage, jazz_node_NodeState **out);
u32 LNS(catalogue_schemas_len)(const jazz_node_NodeState *n);
const jazz_model_JazzSchema *LNS(schema_with_active_permissions)(const jazz_node_NodeState *n, jazz_types_SchemaVersionId id);
jz_err LNS(author_schema_lineage_publication)(jazz_node_NodeState *n, jazz_model_SchemaVersion *version, jazz_protocol_MigrationLens *lens,
                                             jazz_db_StrVec new_tables, jazz_db_StrVec dropped_tables, jazz_node_SchemaLineagePublication **out);
void jazz_protocol__protocol__SyncMessage__publish_schema_with_lens(jazz_protocol_SyncMessage *out, jazz_types_AuthorSubject author, u64 catalogue_seq, jazz_node_SchemaLineagePublication *publication);
jz_err LNS(apply_trusted_catalogue_message)(jazz_node_NodeState *n, jazz_protocol_SyncMessage message, jazz_db_bPublicationOutcome *out);
jz_err LNS(activate_schema)(jazz_node_NodeState *n, u64 seq, jazz_model_JazzSchema schema);
jz_err LNS(close)(jazz_node_NodeState *n);
void LNS(seed_tx_time_high_water)(jazz_node_NodeState *n, jazz_types_TxTime high_water);
jz_err LNS(set_initial_sync_flush_cadence)(jazz_node_NodeState *n, usize writes);
jazz_node_MergeableCommit *jazz_node__node__MergeableCommit__new(jz_slice table, jazz_types_RowUuid row, u64 now_ms);
void jazz_node__node__MergeableCommit__made_by(jazz_node_MergeableCommit *c, jazz_types_AuthorSubject made_by);
void jazz_node__node__MergeableCommit__cells(jazz_node_MergeableCommit *c, jz_btree cells);
jz_err LNS(commit_mergeable_in_schema)(jazz_node_NodeState *n, jazz_types_SchemaVersionId schema, jazz_node_MergeableCommit *commit, jazz_node_PublishedTransaction *out);
jz_err LNS(finalize_local_mergeable_commit)(jazz_node_NodeState *n, jazz_model_TxId tx_id, jazz_db_bPublicationOutcome *out);
jz_err LNS(synchronizing_transaction_ids_for_node)(jazz_node_NodeState *n, jazz_types_NodeUuid node, jazz_db_TxIdVec *out);
jz_err LNS(synchronizing_transaction_ids_for_author)(jazz_node_NodeState *n, jazz_types_AuthorSubject author, jazz_db_TxIdVec *out);
jz_err LNS(evict_pending_large_value_upload)(jazz_node_NodeState *n, groove_StagedLargeValueId id);
#undef LNS

/* ---- db-a node_runtime.rs Node (mirrors) ---- */
#define LNR(n) jazz_db__db__node_runtime__Node__##n
jazz_db_Node *LNR(new)(jazz_node_NodeState *node);
void LNR(declare_upload_root)(jazz_db_Node *self);
void LNR(set_large_value_staging_policy)(jazz_db_Node *self, const jazz_node_LargeValueStagingPolicy *policy);
jz_err LNR(evict_expired_staged_large_values)(jazz_db_Node *self, u32 *out);
jz_err LNR(restore_pending_uploads)(jazz_db_Node *self, jazz_db_DbIdentity identity);
jz_err LNR(configure_scope_isolated_client_relay)(jazz_db_Node *self, jazz_node_ClientRelayScope *scope);
jz_err LNR(restore_backend_pending_uploads)(jazz_db_Node *self, jazz_types_NodeUuid node_id);
void LNR(begin_mutation_shutdown)(jazz_db_Node *self);
void LNR(begin_transaction_abandonment_shutdown)(jazz_db_Node *self);
void LNR(begin_subscription_finalization_shutdown)(jazz_db_Node *self);
void LNR(lock_close_owner)(jazz_db_Node *self);
void LNR(release_close_owner)(jazz_db_Node *self);
void LNR(drain_queued_mutations)(jazz_db_Node *self);
void LNR(drain_transaction_wait_observers)(jazz_db_Node *self);
jz_err LNR(flush_deferred_rejection_discards)(jazz_db_Node *self);
jz_err LNR(finish_transaction_abandonment_shutdown)(jazz_db_Node *self, u32 *out);
void LNR(retire_subscription_runtime_after_close)(jazz_db_Node *self);
void LNR(set_non_durable_client)(jazz_db_Node *self);
bool LNR(poll_queued_mutation_once)(jazz_db_Node *self);
u32 LNR(queued_mutation_count)(const jazz_db_Node *self);
jazz_db_Oneshot *LNR(queued_mutation_barrier)(jazz_db_Node *self);
bool LNR(queued_transaction_error)(const jazz_db_Node *self, jazz_model_OpenTransactionId id, jz_err *out);
void LNR(enqueue_transaction_cleanup_with_completion)(jazz_db_Node *self, jazz_db_LocalFuture future, const jazz_db_bQueuedMutationCompletion *completion);
jazz_db_Oneshot *LNR(enqueue_transaction_read)(jazz_db_Node *self, jazz_model_OpenTransactionId open_tx_id, jazz_db_LocalFuture read, u32 out_size);
jz_err LNR(restore_browser_relay_pending_uploads)(jazz_db_Node *self, jazz_types_AuthorSubject author);
void LNR(set_deferred_local_persistence)(jazz_db_Node *self, bool deferred);
void LNR(mark_subscriber_connections_dirty)(jazz_db_Node *self);
bool LNR(queued_mutation_write_state)(const jazz_db_Node *self, jazz_model_TxId tx_id, jazz_db_WriteState *out, jz_err *err);
bool LNR(take_queued_mutation_failure)(jazz_db_Node *self, jazz_model_TxId tx_id, jz_err *out);
jz_err LNR(wait_for_transaction)(jazz_db_Node *self, jazz_model_TxId tx_id, jazz_db_WriteWaitOptions opts, jazz_model_TxId *out);
void LNR(wait_for_transaction_with)(jazz_db_Node *self, jazz_model_TxId tx_id, jazz_db_WriteWaitOptions opts, jazz_db_bTxIdCallback callback);
void LNR(wait_for_write_with)(jazz_db_Node *self, jazz_model_TxId tx_id, jazz_db_QueuedMutationAlias *alias, jazz_db_WriteWaitOptions opts, jazz_db_bTxIdCallback callback);
jazz_db_WriteStateChange LNR(register_write_state_waiter)(jazz_db_Node *self, jazz_model_TxId tx_id);
void LNR(set_mutation_error_callback)(jazz_db_Node *self, const jazz_db_MutationErrorCallback *cb);
jz_err LNR(connect_upstream)(jazz_db_Node *self, jazz_db_Transport transport, jazz_db_PeerMutex **out);
jz_err LNR(try_connect_upstream)(jazz_db_Node *self, jazz_db_Transport transport, jazz_db_PeerMutex **out);
void LNR(set_scheduler)(jazz_db_Node *self, const jazz_db_TickScheduler *scheduler);
void LNR(mark_subscriber_connections_dirty_after_query_runtime_wake)(jazz_db_Node *self);
jazz_db_PermissionAdviceFuture LNR(request_permission_advice)(jazz_db_Node *self, jazz_protocol_PermissionAdviceAction *action);
jazz_db_PermissionAdviceFuture LNR(request_permission_advice_with_delegated_session)(jazz_db_Node *self, jazz_protocol_PermissionAdviceAction *action, jazz_protocol_DelegatedSessionBinding session);
void LNR(cancel_permission_advice_request)(jazz_db_Node *self, jazz_protocol_PermissionAdviceRequestId request_id);
jazz_db_PeerMutex *LNR(accept_subscriber)(jazz_db_Node *self, jazz_db_Transport transport, jazz_types_AuthorSubject identity);
jazz_db_PeerMutex *LNR(accept_scope_isolated_relay_subscriber)(jazz_db_Node *self, jazz_db_Transport transport, jazz_types_AuthorSubject identity, jz_btree claims, u64 admission_epoch);
jazz_db_PeerMutex *LNR(accept_subscriber_with_claims)(jazz_db_Node *self, jazz_db_Transport transport, jazz_types_AuthorSubject identity, jz_btree claims);
jz_err LNR(accept_subscriber_with_claims_async)(jazz_db_Node *self, jazz_db_Transport transport, jazz_types_AuthorSubject identity, jz_btree claims, jazz_node_CommitUnitTrust trust, jazz_db_PeerMutex **out);
jazz_db_PeerMutex *LNR(accept_subscriber_with_claims_and_trust)(jazz_db_Node *self, jazz_db_Transport transport, jazz_types_AuthorSubject identity, jz_btree claims, jazz_node_CommitUnitTrust trust);
jazz_db_PeerMutex *LNR(accept_subscriber_with_resume)(jazz_db_Node *self, jazz_db_Transport transport, jazz_types_AuthorSubject identity, jazz_protocol_ResumeCursor *cursor);
bool LNR(detach_connection)(jazz_db_Node *self, jazz_db_PeerMutex *connection);
jz_err LNR(detach_connection_async)(jazz_db_Node *self, jazz_db_PeerMutex *connection, bool *out);
bool LNR(owner_is_available)(const jazz_db_Node *self);
void LNR(deliver_pending_mutation_errors)(jazz_db_Node *self);
jz_err LNR(poll_local_publication_settlement_once)(jazz_db_Node *self, bool *pending);
void LNR(poll_transaction_wait_observers)(jazz_db_Node *self);
bool LNR(owner_is_available_or_wake_when_released)(jazz_db_Node *self);
jz_err LNR(tick)(jazz_db_Node *self, jazz_db_DbTickStats *out);
jz_err LNR(refresh_subscriptions_with_tables)(jazz_db_Node *self, const jz_hmap *changed_tables, u32 *out);
#undef LNR

/* ---- db-a mutations.rs (mirror) ---- */
jz_err jazz_db__db__mutations__Db__finish_publication_outcome(const jazz_db_Db *self, jazz_db_bPublicationOutcome outcome);

/* ---- lifecycle.rs public surface (this unit) ---- */
#define LCX(n) jazz_db__db__lifecycle__Db__##n
jz_err LCX(open)(jazz_db_DbConfig config, jazz_db_Db *out);
jz_err LCX(open_history_complete)(jazz_db_DbConfig config, jazz_db_Db *out);
jz_err LCX(register_schema_view)(const jazz_db_Db *db, jazz_model_JazzSchema schema, jazz_db_Db *out);
jz_err LCX(close)(const jazz_db_Db *db);
jz_err LCX(write_state)(const jazz_db_Db *db, jazz_model_TxId tx_id, jazz_db_WriteState *out);
jz_err LCX(tick)(const jazz_db_Db *db);
jz_err LCX(refresh_subscriptions_for_tables)(const jazz_db_Db *db, const jz_hmap *changed_tables, u32 *out);
#undef LCX

/* ---- catalogue.rs dependencies ---- */
#include "db/view_update_parts.h"
jz_err jazz_db__db__mutations__Db__check_catalogue_admin(const jazz_db_Db *self);
/* PublicationOutcome<Vec<SyncMessage>>::value moved out (db-a helper) */
jz_err jazz_db__db__PublicationOutcome__responses(jazz_db_bPublicationOutcome *o, jazz_db_SyncMessageVec *out);
void jazz_protocol__protocol__SyncMessage__publish_schema(jazz_protocol_SyncMessage *out, jazz_types_AuthorSubject author, jazz_model_SchemaVersion *schema);
void jazz_protocol__protocol__SyncMessage__publish_lens(jazz_protocol_SyncMessage *out, jazz_types_AuthorSubject author, jazz_protocol_MigrationLens *lens);
void jazz_protocol__protocol__SyncMessage__drop(void *m);
jz_err jazz_node__node__NodeState__validate_schema_activation(jazz_node_NodeState *n, u64 revision, jazz_model_JazzSchema schema);
/* public_schema: BTreeMap<TableName (jz_str), PublicTable> */
typedef struct jazz_model_PublicTable jazz_model_PublicTable;
typedef struct jazz_model_TablePolicies { void *p; } jazz_model_TablePolicies;
void jazz_model__schema__JazzSchema__public_schema_clone(const jazz_model_JazzSchema *s, jz_btree *out);
void jazz_model__public_schema__TablePolicies__clone(void *dst, const void *src);
void jazz_model__public_schema__TablePolicies__default(jazz_model_TablePolicies *out);
void jazz_model__public_schema__PublicTable__set_policies(jazz_model_PublicTable *t, jazz_model_TablePolicies policies);
extern const jz_hmap_desc jazz_model_table_policies_map_desc; /* HashMap<TableName, TablePolicies> */
jz_err jazz_model__schema__JazzSchema__new(const jz_btree *source, jazz_model_JazzSchema *out);
jz_err jazz_db__db__node_runtime__Node__set_permissions_ready(jazz_db_Node *self, bool ready);
const jazz_protocol_MigrationLens *jazz_node__node__NodeState__catalogue_lens(const jazz_node_NodeState *n, const void *lens_id);
void jazz_protocol__protocol__MigrationLens__clone(jazz_protocol_MigrationLens **dst, const jazz_protocol_MigrationLens *src);

#endif
