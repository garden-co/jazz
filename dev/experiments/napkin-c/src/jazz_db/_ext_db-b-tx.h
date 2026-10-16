/* _ext_db-b-tx.h: mirrors used by db/transactions.c, db/reads.c, db/subscriptions.c
 * and db/lifecycle.c:
 *  - jazz_node NodeState transaction/read API (crate::node, other units)
 *  - db-a's node_runtime.rs Node methods and mutations.rs Db helpers, declared
 *    with db-b type names (same symbols as db-a's own headers).
 * Included at the end of db.h (after the Db / Node types). */
#ifndef JAZZ_DB_EXT_DB_B_TX_H
#define JAZZ_DB_EXT_DB_B_TX_H

/* ---- crate::node types ---- */
typedef enum { JAZZ_DELETION_DELETED = 0, JAZZ_DELETION_RESTORED = 1 } jazz_node_DeletionEvent;
typedef enum { JAZZ_TBRS_VISIBLE, JAZZ_TBRS_PENDING_DELETION, JAZZ_TBRS_ABSENT } jazz_node_TransactionBranchRowStateTag;
typedef enum { JAZZ_TITS_ABSENT, JAZZ_TITS_VISIBLE, JAZZ_TITS_DELETED } jazz_node_TransactionInsertTargetState;
typedef struct jazz_node_BranchViewCopyEvidence jazz_node_BranchViewCopyEvidence; /* crate::tx */
void jazz_node__tx__BranchViewCopyEvidence__drop(jazz_node_BranchViewCopyEvidence *e);
typedef struct jazz_node_PublishedTransaction { jazz_model_TxId tx_id; void *rest; } jazz_node_PublishedTransaction;
typedef struct jazz_node_TableSchema jazz_node_TableSchema;
typedef struct jazz_node_PolicyQuery jazz_node_PolicyQuery;
const jazz_node_PolicyQuery *jazz_node__schema__TableSchema__read_policy(const jazz_node_TableSchema *t); /* NULL = None */
jazz_node_PolicyQuery *jazz_node__schema__PolicyQuery__clone(const jazz_node_PolicyQuery *p);
void jazz_node__schema__PolicyQuery__drop(jazz_node_PolicyQuery *p);
typedef struct jazz_model_Query jazz_model_Query;
typedef JZ_VEC(jazz_model_Query *) jazz_db_QueryVec;
jazz_model_OpenTransactionId jazz_model__model__transaction__OpenTransactionId__new(void);
/* `|| self.next_now_ms()` */
typedef struct jazz_db_NowFn { u64 (*call)(const void *ctx); const void *ctx; } jazz_db_NowFn;

/* ---- NodeState transaction API (node errors are 0x500 jz_err) ---- */
#define NS_FN(n) jazz_node__node__NodeState__##n
jz_err NS_FN(transaction_needs_large_value_staging)(jazz_node_NodeState *n, jazz_model_OpenTransactionId id, bool *out);
jz_err NS_FN(transaction_is_exclusive)(jazz_node_NodeState *n, jazz_model_OpenTransactionId id, bool *out);
jz_err NS_FN(transaction_is_attributed)(jazz_node_NodeState *n, jazz_model_OpenTransactionId id, bool *out);
jz_err NS_FN(exclusive_source_hydration_queries)(jazz_node_NodeState *n, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding, bool include_deleted, jazz_db_QueryVec *out);
jz_err NS_FN(open_mergeable)(jazz_node_NodeState *n, jazz_model_OpenTransactionId id, jazz_types_AuthorSubject made_by, bool has_permission_subject, jazz_types_AuthorSubject permission_subject);
jz_err NS_FN(open_exclusive_with_identity)(jazz_node_NodeState *n, jazz_model_OpenTransactionId id, jazz_types_AuthorSubject made_by, jazz_types_AuthorSubject permission_subject);
jz_err NS_FN(open_exclusive_for_identity)(jazz_node_NodeState *n, jazz_model_OpenTransactionId id, jazz_types_AuthorSubject author);
jz_err NS_FN(tx_write_mergeable_in_schema)(jazz_node_NodeState *n, jazz_model_OpenTransactionId id, jazz_types_SchemaVersionId schema, jz_slice table, jazz_types_RowUuid row, jz_btree cells, bool has_deletion, jazz_node_DeletionEvent deletion, bool has_now, u64 now_ms, bool known_fresh_row);
jz_err NS_FN(tx_write_mergeable_in_schema_and_branch)(jazz_node_NodeState *n, jazz_model_OpenTransactionId id, jazz_types_SchemaVersionId schema, jz_slice table, jazz_types_RowUuid row, jz_btree cells, bool has_deletion, jazz_node_DeletionEvent deletion, bool has_now, u64 now_ms, jazz_model_BranchSelector branch, bool known_fresh_row);
jz_err NS_FN(tx_write_mergeable_in_schema_and_branch_with_verified_inherited_cells)(jazz_node_NodeState *n, jazz_model_OpenTransactionId id, jazz_types_SchemaVersionId schema, jz_slice table, jazz_types_RowUuid row, jz_btree cells, bool has_deletion, jazz_node_DeletionEvent deletion, bool has_now, u64 now_ms, jazz_model_BranchSelector branch, bool known_fresh_row, bool has_verified, jz_btree verified_inherited_cells, bool replace_pending_deletion, jazz_node_BranchViewCopyEvidence *branch_view_copy /* NULL = None */);
jz_err NS_FN(tx_patch_mergeable_in_schema)(jazz_node_NodeState *n, jazz_model_OpenTransactionId id, jazz_types_SchemaVersionId schema, jz_slice table, jazz_types_RowUuid row, jz_btree patch, bool has_now, u64 now_ms);
jz_err NS_FN(tx_patch_mergeable_in_schema_and_branch)(jazz_node_NodeState *n, jazz_model_OpenTransactionId id, jazz_types_SchemaVersionId schema, jz_slice table, jazz_types_RowUuid row, jz_btree patch, bool has_now, u64 now_ms, jazz_model_BranchSelector branch, bool replace_pending_deletion);
jz_err NS_FN(tx_visible_current_cells_in_branch)(jazz_node_NodeState *n, jazz_model_OpenTransactionId id, jz_slice table, jazz_types_RowUuid row, const jazz_model_BranchSelector *branch, bool *has, jz_btree *out);
jz_err NS_FN(local_deletion_winner_tx_id_in_schema)(jazz_node_NodeState *n, jazz_types_SchemaVersionId schema, jz_slice table, jazz_types_RowUuid row, bool *has, jazz_model_TxId *out);
jz_err NS_FN(visible_current_cells_in_branch_view)(jazz_node_NodeState *n, jz_slice table, const jazz_model_BranchSelector *head, const jazz_protocol_BranchViewBase *base /* NULL = None */, jazz_types_RowUuid row, bool *has, jz_btree *out);
jz_err NS_FN(inherited_branch_view_copy_evidence)(jazz_node_NodeState *n, jz_slice table, const jazz_model_BranchSelector *head, const jazz_protocol_BranchViewBase *base, jazz_types_RowUuid row, jazz_node_BranchViewCopyEvidence **out /* NULL = None */);
jz_err NS_FN(tx_current_row_state_in_branch)(jazz_node_NodeState *n, jazz_model_OpenTransactionId id, jz_slice table, jazz_types_RowUuid row, const jazz_model_BranchSelector *branch, u32 *out);
jz_err NS_FN(commit_mergeable_open)(jazz_node_NodeState *n, jazz_model_OpenTransactionId id, jazz_db_NowFn now, jazz_node_PublishedTransaction *out);
jz_err NS_FN(commit_mergeable_open_at)(jazz_node_NodeState *n, jazz_model_OpenTransactionId id, jazz_model_TxId tx_id, jazz_db_NowFn now, jazz_node_PublishedTransaction *out);
jz_err NS_FN(commit_exclusive_bound)(jazz_node_NodeState *n, jazz_model_OpenTransactionId id, u64 now_ms, jazz_node_PublishedTransaction *published, jazz_protocol_SyncMessage *unit);
jz_err NS_FN(commit_exclusive_bound_at)(jazz_node_NodeState *n, jazz_model_OpenTransactionId id, jazz_model_TxId tx_id, jazz_node_PublishedTransaction *published, jazz_protocol_SyncMessage *unit);
jz_err NS_FN(commit_exclusive)(jazz_node_NodeState *n, jazz_model_OpenTransactionId id, jazz_types_AuthorSubject author, u64 now_ms, jazz_node_PublishedTransaction *published, jazz_protocol_SyncMessage *unit);
jz_err NS_FN(abandon_tx)(jazz_node_NodeState *n, jazz_model_OpenTransactionId id);
bool NS_FN(has_released_large_values)(const jazz_node_NodeState *n);
jz_err NS_FN(tx_read_in_schema)(jazz_node_NodeState *n, jazz_model_OpenTransactionId id, jazz_types_SchemaVersionId schema, jz_slice table, jazz_types_RowUuid row, bool *has, jz_btree *out);
jz_err NS_FN(hydrate_large_value_cells)(jazz_node_NodeState *n, jz_btree *cells);
jz_err NS_FN(hydrate_current_rows)(jazz_node_NodeState *n, jazz_db_CurrentRowVec *rows);
jz_err NS_FN(tx_relation_snapshot_with_options)(jazz_node_NodeState *n, jazz_model_OpenTransactionId id, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding, bool include_deleted, jazz_node_RelationSnapshot *out);
jz_err NS_FN(tx_relation_snapshot_for_identity_with_options)(jazz_node_NodeState *n, jazz_model_OpenTransactionId id, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding, jazz_types_AuthorSubject author, bool include_deleted, jazz_node_RelationSnapshot *out);
jz_err NS_FN(tx_query_with_options)(jazz_node_NodeState *n, jazz_model_OpenTransactionId id, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding, bool include_deleted, jazz_db_CurrentRowVec *out);
jz_err NS_FN(tx_query_for_identity_with_options)(jazz_node_NodeState *n, jazz_model_OpenTransactionId id, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding, jazz_types_AuthorSubject author, bool include_deleted, jazz_db_CurrentRowVec *out);
jz_err NS_FN(tx_current_rows)(jazz_node_NodeState *n, jazz_model_OpenTransactionId id, jz_slice table, jazz_db_CurrentRowVec *out);
jz_err NS_FN(tx_insert_target_state_in_schema)(jazz_node_NodeState *n, jazz_model_OpenTransactionId id, jazz_types_SchemaVersionId schema, jz_slice table, jazz_types_RowUuid row, u32 *out);
jz_err NS_FN(tx_write_in_schema_at_ms)(jazz_node_NodeState *n, jazz_model_OpenTransactionId id, jazz_types_SchemaVersionId schema, jz_slice table, jazz_types_RowUuid row, jz_btree cells, bool has_deletion, jazz_node_DeletionEvent deletion, bool has_now, u64 now_ms);
jz_err NS_FN(exclusive_transaction_permission_subject)(jazz_node_NodeState *n, jazz_model_OpenTransactionId id, jazz_types_AuthorSubject *out);
jz_err NS_FN(transaction_permission_subject)(jazz_node_NodeState *n, jazz_model_OpenTransactionId id, jazz_types_AuthorSubject *out);
jz_err NS_FN(read_policy_query_allows_open_tx_row)(jazz_node_NodeState *n, jazz_model_OpenTransactionId id, const jazz_node_PolicyQuery *policy, jazz_types_SchemaVersionId schema, jazz_types_RowUuid row, jazz_types_AuthorSubject identity, bool *out);
#undef NS_FN

/* ---- db-a node_runtime.rs Node methods ---- */
#define NODE_RT_FN(name) jazz_db__db__node_runtime__Node__##name
jz_err NODE_RT_FN(lock_for_transaction_open)(jazz_db_Node *self, jazz_model_OpenTransactionId open_tx_id, bool owner_operation_admitted, jazz_node_NodeState **out);
jz_err NODE_RT_FN(lock_for_transaction_operation)(jazz_db_Node *self, jazz_model_OpenTransactionId open_tx_id, bool owner_operation_admitted, jazz_node_NodeState **out);
bool NODE_RT_FN(has_pending_local_publications)(const jazz_db_Node *self);
jz_err NODE_RT_FN(settle_local_publications)(jazz_db_Node *self);
jz_err NODE_RT_FN(enqueue_transaction_operation)(jazz_db_Node *self, jazz_model_OpenTransactionId open_tx_id, jazz_db_LocalFuture future);
jazz_db_QueuedMutationStatus *NODE_RT_FN(enqueue_transaction_commit)(jazz_db_Node *self, jazz_model_OpenTransactionId open_tx_id, jazz_model_TxId tx_id, jazz_model_TxKind kind, jazz_db_LocalFuture future);
void NODE_RT_FN(enqueue_transaction_cleanup)(jazz_db_Node *self, jazz_db_LocalFuture future);
void NODE_RT_FN(retire_queued_transaction_error)(jazz_db_Node *self, jazz_model_OpenTransactionId id);
bool NODE_RT_FN(mark_transaction_abandoned)(jazz_db_Node *self, jazz_model_OpenTransactionId id);
void NODE_RT_FN(clear_transaction_abandonment)(jazz_db_Node *self, jazz_model_OpenTransactionId id);
void NODE_RT_FN(evict_released_large_values)(jazz_db_Node *self);
void NODE_RT_FN(schedule_tick)(const jazz_db_Node *self, u32 urgency);
jz_err NODE_RT_FN(reserve_transaction_id)(const jazz_db_Node *self, u64 now_ms, jazz_model_TxId *out);
jz_err NODE_RT_FN(ensure_mutation_admission_open)(const jazz_db_Node *self);
#undef NODE_RT_FN

/* ---- db-a mutations.rs Db helpers ---- */
#define MUT_FN(name) jazz_db__db__mutations__Db__##name
void MUT_FN(queued_write_handle)(const jazz_db_Db *self, jazz_types_RowUuid row, jazz_model_TxId tx_id, jazz_db_QueuedMutationStatus *status, jazz_db_QueuedMutationAlias *alias, jazz_db_WriteHandle *out);
jz_err MUT_FN(stage_transaction_large_value_update)(const jazz_db_Db *self, jazz_model_OpenTransactionId tx_id, jz_slice table, jazz_types_RowUuid row, jz_btree patch, jz_vec mutations, bool has_updated_at_ms, u64 updated_at_ms);
jz_err MUT_FN(settle_local_publication)(const jazz_db_Db *self, jazz_node_PublishedTransaction *published, bool has_unit, jazz_protocol_SyncMessage unit, jazz_model_DurabilityTier *out);
jz_err MUT_FN(resolve_write_identity)(const jazz_db_Db *self, jazz_db_WriteIdentity identity, jazz_db_ResolvedWriteIdentity *out);
u64 MUT_FN(next_now_ms)(const jazz_db_Db *self);
jz_err MUT_FN(table_schema)(const jazz_db_Db *self, jz_slice table, const jazz_node_TableSchema **out);
jz_err MUT_FN(validate_complete_insert)(const jazz_db_Db *self, jz_slice table, const jz_btree *cells);
jz_err MUT_FN(apply_insert_defaults)(const jazz_db_Db *self, jz_slice table, jz_btree *cells);
jz_err MUT_FN(ensure_row_not_deleted)(const jazz_db_Db *self, jz_slice table, jazz_types_RowUuid row);
jz_err MUT_FN(ensure_branch_view_row_not_deleted)(const jazz_db_Db *self, jz_slice table, const jazz_model_BranchSelector *head, const jazz_protocol_BranchViewBase *base, jazz_types_RowUuid row);
jz_err MUT_FN(ensure_open_schema_admitted)(const jazz_db_Db *self);
#undef MUT_FN
jz_err jazz_db__db__mutations__validate_updated_at_ms(bool has, u64 updated_at_ms);

/* ---- lifecycle.rs (this unit) ---- */
#define LC_FN(n) jazz_db__db__lifecycle__Db__##n
jz_err LC_FN(reserve_transaction_id_at_ms)(const jazz_db_Db *db, u64 now_ms, jazz_model_TxId *out);
void LC_FN(clone_for_reserved_transaction)(const jazz_db_Db *db, jazz_model_TxId tx_id, jazz_db_Db *out);
void LC_FN(clone_for_owner_operation)(const jazz_db_Db *db, jazz_db_Db *out);
jz_err LC_FN(ensure_mutation_operation_admitted)(const jazz_db_Db *db);
#undef LC_FN
/* Db drop glue (Rc releases of the shared handles) */
void jazz_db__db__Db__drop(jazz_db_Db *db);

/* ---- reads.rs dependencies ---- */
jz_err jazz_node__node__api_error__Error__from_query_error(jz_err query_error);
typedef struct jazz_model_RelationQuery jazz_model_RelationQuery;
jz_err jazz_model__query__relation_resolution__relation_query_to_query(const jazz_model_RelationQuery *q, jazz_model_Query **out);
jz_err jazz_model__query__Query__validate_with_schema_version(const jazz_model_Query *q, const jazz_model_JazzSchema *schema, jazz_types_SchemaVersionId version, jazz_model_ValidatedQuery **out);
jz_err jazz_model__query__ValidatedQuery__bind(const jazz_model_ValidatedQuery *q, jz_btree params, jazz_model_Binding **out);
void jazz_model__query__Query__drop(jazz_model_Query *q);
jz_slice jazz_model__query__Query__table(const jazz_model_Query *q);
u32 jazz_model__query__Query__policy_branches_len(const jazz_model_Query *q);
u32 jazz_model__query__Query__inherits_len(const jazz_model_Query *q);
u32 jazz_model__query__Query__includes_len(const jazz_model_Query *q);
bool jazz_model__query__Query__has_aggregate(const jazz_model_Query *q);
bool jazz_model__query__Query__has_relation(const jazz_model_Query *q);
const jazz_model_Query *jazz_node__schema__PolicyQuery__as_query(const jazz_node_PolicyQuery *p);
/* crate::wire::decode_postcard_exact::<Query>; the error message is the Display */
jz_err jazz_db__wire__decode_postcard_exact_query(jz_slice bytes, jazz_model_Query **out);
void jazz_protocol__protocol__SubscribeRejectReason__debug(jz_str *out, const jazz_protocol_SubscribeRejectReason *r);
typedef struct jazz_node_RowProvenance { jazz_types_AuthorSubject created_by; jazz_types_AuthorSubject updated_by; u64 created_at; u64 updated_at; } jazz_node_RowProvenance;
typedef struct jazz_node_QueryReadProfile { u64 counters[8]; } jazz_node_QueryReadProfile;
typedef struct jazz_db_StorageTables { jazz_model_TxId tx_id; jz_btree tables; /* BTreeSet<String> */ } jazz_db_StorageTables;
typedef JZ_VEC(jazz_db_StorageTables) jazz_db_TxTablesVec;
typedef struct jazz_node_CatalogueSchemaEntry { jazz_model_JazzSchema schema; } jazz_node_CatalogueSchemaEntry;
enum { GROOVE_TPS_COLLECTION = 0, GROOVE_TPS_KEY = 1 };

#define NS_FN(n) jazz_node__node__NodeState__##n
jz_err NS_FN(current_write_schema)(jazz_node_NodeState *n, jazz_types_SchemaVersionId *out);
const jazz_node_CatalogueSchemaEntry *NS_FN(catalogue_schema)(const jazz_node_NodeState *n, const jazz_types_SchemaVersionId *id); /* catalogue_schemas().get() */
u64 NS_FN(groove_runtime_token)(const jazz_node_NodeState *n);
bool NS_FN(uses_schema_projected_read)(const jazz_node_NodeState *n, const jazz_model_ValidatedQuery *shape);
jz_err NS_FN(prepared_query_plan)(jazz_node_NodeState *n, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding, u32 tier, jazz_types_AuthorSubject author, jazz_node_PreparedQueryPlanHandle **out);
jz_err NS_FN(query_rows_local_preview)(jazz_node_NodeState *n, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding, const jazz_node_PreparedQueryPlanHandle *plan, jazz_db_CurrentRowVec *out);
jz_err NS_FN(query_rows_local_preview_profiled)(jazz_node_NodeState *n, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding, const jazz_node_PreparedQueryPlanHandle *plan, jazz_db_CurrentRowVec *out, jazz_node_QueryReadProfile *profile);
jz_err NS_FN(row_provenance)(jazz_node_NodeState *n, const jazz_node_CurrentRow *row, bool *has, jazz_node_RowProvenance *out);
/* node.at(position).read(shape, binding) */
jz_err NS_FN(at_read)(jazz_node_NodeState *n, jazz_types_GlobalTime position, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding, jazz_db_CurrentRowVec *out);
jz_err NS_FN(query_relation_snapshot_for_serving_in_read_view)(jazz_node_NodeState *n, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding, u32 tier, jazz_types_AuthorSubject author, const jazz_protocol_ReadViewSpec *read_view, jazz_node_RelationSnapshot *out);
jz_err NS_FN(query_relation_snapshot_for_client)(jazz_node_NodeState *n, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding, u32 tier, jazz_types_AuthorSubject author, const jazz_protocol_ReadViewSpec *read_view, jazz_node_RelationSnapshot *out);
jz_err NS_FN(query_rows_including_deleted_in_authorization_mode)(jazz_node_NodeState *n, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding, u32 tier, const void *limit /* None */, jazz_types_AuthorSubject author, u32 mode, jazz_db_CurrentRowVec *out);
jz_err NS_FN(query_rows_with_prepared_plan_for_identity)(jazz_node_NodeState *n, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding, u32 tier, const void *limit /* None */, jazz_types_AuthorSubject author, jazz_db_CurrentRowVec *out);
jz_err NS_FN(query_rows_for_client)(jazz_node_NodeState *n, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding, u32 tier, jazz_types_AuthorSubject author, jazz_db_CurrentRowVec *out);
jz_err NS_FN(hydrate_encoded_record)(jazz_node_NodeState *n, const groove_RecordDescriptor *descriptor, jz_bytes *value);
void NS_FN(logical_table_names_for_storage_tables)(jazz_node_NodeState *n, const jz_btree *storage_tables, jz_btree *out /* BTreeSet<String> */);
jz_err NS_FN(commit_unit_for)(jazz_node_NodeState *n, jazz_model_TxId tx_id, jazz_protocol_SyncMessage *out);
#undef NS_FN
/* SyncMessage::uploaded_versions().map(|v| v.table()) collected into a set */
void jazz_protocol__protocol__SyncMessage__uploaded_version_tables(const jazz_protocol_SyncMessage *m, jz_btree *out);
bool jazz_db__db__node_runtime__Node__is_pending_local_publication(const jazz_db_Node *self, jazz_model_TxId tx_id);
void jazz_db__db__node_runtime__Node__pending_local_publication_tables(const jazz_db_Node *self, jazz_db_TxTablesVec *out);
bool jazz_db__db__node_runtime__Node__receives_commits_as_local(const jazz_db_Node *self);
jz_err jazz_db__db__mutations__Db__await_open_schema_for_coverage(const jazz_db_Db *self, jazz_model_DurabilityTier coverage_tier);
jz_err jazz_db__db__mutations__Db__current_write_schema_for_query(const jazz_db_Db *self, jazz_model_JazzSchema **schema_out, jazz_types_SchemaVersionId *out);
extern const jz_btree_desc jazz_db_str_set_desc;

/* binding_hydration_error classification of crate::node::Error */
bool jazz_node__node__Error__chunk_retryable(const jz_err *e, u32 *retry_after_ms); /* LargeValueReachability(Chunk(Retryable)) | Groove(IvmRuntime(Chunk(Retryable))) */
bool jazz_node__node__Error__chunk_unavailable(const jz_err *e);                    /* ChunkStorage(Unavailable) | ..Chunk(Unavailable) */
jazz_model_Query *jazz_model__query__Query__from(jz_str table);

/* ---- subscriptions.rs dependencies ---- */
#define NS_FN(n) jazz_node__node__NodeState__##n
jz_err NS_FN(open_transaction_snapshot)(jazz_node_NodeState *n, jazz_model_OpenTransactionId id, void **snapshot_out);
jz_err NS_FN(prepare_query_binding_for_link)(jazz_node_NodeState *n, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding, u32 tier, jazz_types_AuthorSubject author, jazz_model_ValidatedQuery **shape_out, jazz_model_Binding **binding_out);
jz_err NS_FN(query_binding_for_link)(jazz_node_NodeState *n, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding, jazz_model_ValidatedQuery **shape_out, jazz_model_Binding **binding_out);
u64 NS_FN(applied_authority_result_generation)(const jazz_node_NodeState *n, const jazz_protocol_AuthorityResultKey *key);
jz_err NS_FN(authority_result_key_for_subscription)(const jazz_node_NodeState *n, jazz_protocol_SubscriptionKey subscription, jazz_protocol_AuthorityResultKey *out);
void NS_FN(apply_unsubscribe)(jazz_node_NodeState *n, jazz_protocol_SubscriptionKey subscription);
jz_err NS_FN(open_maintained_view_subscription_in_authorization_mode_with_waker)(jazz_node_NodeState *n, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding, jazz_types_AuthorSubject author, u32 tier, const jazz_protocol_ReadViewSpec *read_view, const void *limit /* None */, u32 mode, bool pending_overlay, const jz_waker *waker /* NULL = None */, jazz_node_LocalMaintainedViewSubscription **subscription, jazz_node_RelationSnapshot *snapshot);
jz_err NS_FN(drain_local_maintained_view_subscription_preserving_rows_with_waker)(jazz_node_NodeState *n, jazz_node_LocalMaintainedViewSubscription *subscription, const jazz_protocol_AuthorityResultKey *key /* NULL = None */, const jz_btree *preserve, const jz_waker *waker, bool *has_update, jazz_node_LocalMaintainedViewSubscriptionUpdate *update);
bool NS_FN(covered_receiver_evaluation_pending)(const jazz_node_NodeState *n, const jazz_node_LocalMaintainedViewSubscription *subscription);
jz_err NS_FN(ensure_peer_maintained_subscription_view_supported)(jazz_node_NodeState *n, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding, u32 tier, jazz_types_AuthorSubject identity, const jazz_protocol_ReadViewSpec *read_view, u32 mode);
#undef NS_FN
#define LMVS_FN(n) jazz_node__node__LocalMaintainedViewSubscription__##n
groove_SubscriptionId LMVS_FN(subscription_id)(const jazz_node_LocalMaintainedViewSubscription *m);
bool LMVS_FN(has_root_collector)(const jazz_node_LocalMaintainedViewSubscription *m);
const jazz_types_OutputOccurrenceId *LMVS_FN(root_occurrence_ids)(const jazz_node_LocalMaintainedViewSubscription *m, u32 *n);
const jazz_node_TerminalRootLayout *LMVS_FN(terminal_root_layout)(const jazz_node_LocalMaintainedViewSubscription *m); /* NULL = None */
bool LMVS_FN(has_installed_covered_closure)(const jazz_node_LocalMaintainedViewSubscription *m, const jazz_protocol_AuthorityResultKey *key, u64 generation);
bool LMVS_FN(has_covered_input_sources)(const jazz_node_LocalMaintainedViewSubscription *m);
jz_err LMVS_FN(decoded_terminal_records)(const jazz_node_LocalMaintainedViewSubscription *m, jz_btree *out);
bool LMVS_FN(initial_snapshot_received)(const jazz_node_LocalMaintainedViewSubscription *m);
#undef LMVS_FN
jazz_model_ShapeAst *jazz_model__shape__ShapeAst__from_validated(const jazz_model_ValidatedQuery *q);
void jazz_model__shape__ShapeAst__drop(jazz_model_ShapeAst *a);
void jazz_protocol__protocol__AuthorityResultKey__policy_scoped(jazz_protocol_AuthorityResultKey *out, jazz_protocol_BindingViewKey key, jazz_protocol_PolicyBindingKey binding);
bool jazz_protocol__protocol__AuthorityResultKey__eq(const jazz_protocol_AuthorityResultKey *a, const jazz_protocol_AuthorityResultKey *b);
void jazz_protocol__protocol__AuthorityResultKey__drop(void *p);
void jazz_protocol__protocol__AuthorityResultKey__clone(void *dst, const void *src);
bool jazz_node__node__simple_scalar_exit_query(const jazz_model_Query *q);
bool jazz_node__node__single_table_scalar_query(const jazz_model_Query *q);
void jazz_node__node__LocalMaintainedViewSubscriptionUpdate__drop(jazz_node_LocalMaintainedViewSubscriptionUpdate *u);
/* db-a node_runtime.rs */
#define NODE_RT_FN(name) jazz_db__db__node_runtime__Node__##name
void NODE_RT_FN(upstream_register_shape_options)(const jazz_db_Node *self, jazz_model_DurabilityTier tier, jazz_protocol_ReadViewSpec *read_view, jazz_protocol_RegisterShapeOptions *out);
jazz_protocol_SubscriptionKey NODE_RT_FN(next_subscription_key)(jazz_db_Node *self, const jazz_model_ValidatedQuery *shape, jazz_protocol_ReadViewKey read_view);
bool NODE_RT_FN(query_runtime_waker)(const jazz_db_Node *self, jz_waker *out);
void NODE_RT_FN(enqueue_subscription_finalization)(jazz_db_Node *self, jazz_db_PendingSubscriptionFinalization command);
jz_err NODE_RT_FN(drain_subscription_finalizations)(jazz_db_Node *self, u32 *out);
jz_err NODE_RT_FN(ensure_subscription_finalization_open)(const jazz_db_Node *self);
#undef NODE_RT_FN
void jazz_db__db__node_runtime__register_upstream_subscription_owner(jz_btree *owners, const jazz_db_UpstreamCoverageHandle *handles, u32 n, jazz_db_SubscriptionState *state);
void jazz_db__db__SubscriptionState__drop(jazz_db_SubscriptionState *s);

/* ---- subscriptions.rs (this unit) used by reads.rs ---- */
#define SUBS_FN(n) jazz_db__db__subscriptions__Db__##n
jz_err SUBS_FN(attach_query_with_coverage_async)(const jazz_db_Db *db, const jazz_db_PreparedQuery *prepared, jazz_db_ReadOpts opts, u32 coverage_tier, bool has_open_tx, jazz_model_OpenTransactionId open_tx_id, bool has_author, jazz_types_AuthorSubject author, jazz_db_QueryAttachment *out);
bool SUBS_FN(query_attachment_is_covered)(const jazz_db_Db *db, const jazz_db_QueryAttachment *attachment);
jz_err SUBS_FN(subscribe_one_shot_with_coverage)(const jazz_db_Db *db, const jazz_db_PreparedQuery *prepared, jazz_db_ReadOpts opts, bool has_author, jazz_types_AuthorSubject author, u32 coverage_tier, jazz_db_SubscriptionStream *out);
#undef SUBS_FN

#endif
