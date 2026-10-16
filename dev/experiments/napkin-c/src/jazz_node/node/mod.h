/* jazz_node::node (mod.rs): NodeState, shared error type, CurrentRow and the
 * cross-cutting in-memory indexes. Header owned by unit node-core-b. */
#ifndef JZ_NODE_MOD_H
#define JZ_NODE_MOD_H
#include "jz_rt.h"
#include "jazz_node/_ext_node-core-b.h"

/* ---------------- jazz_node::node::Error ---------------- */
/* Codes for the thiserror enum. Transparent variants keep the source
 * error's own code (groove / storage / records / serde_json / hlc). */
enum {
    JZN_E_BASE = 0x4e00,
    JZN_E_GROOVE,
    JZN_E_CHUNK_STORAGE,
    JZN_E_LARGE_VALUE_UPLOAD_CHUNK_UNAVAILABLE,
    JZN_E_LARGE_VALUE,
    JZN_E_LARGE_VALUE_REACHABILITY,
    JZN_E_LARGE_VALUE_INGRESS_RATE_LIMITED,
    JZN_E_LARGE_VALUE_STAGE_EXPIRED,
    JZN_E_RECORD,
    JZN_E_MALFORMED_CURRENT_ROW,
    JZN_E_STORAGE,
    JZN_E_CLOCK_OVERFLOW,
    JZN_E_QUERY,
    JZN_E_QUERY_LOWERING,
    JZN_E_QUERY_CAPABILITY,
    JZN_E_AUTHORIZATION_SUPPORT_MISSING_CLAIM,
    JZN_E_POLICY_PROOF_CYCLE,
    JZN_E_TABLE_NOT_FOUND,
    JZN_E_UNSUPPORTED_COLUMN_TYPE,
    JZN_E_INVALID_MERGEABLE_COMMIT,
    JZN_E_UNADMITTED_WRITE_AUTHOR,
    JZN_E_INVALID_BRANCH_KEY,
    JZN_E_UNSUPPORTED_EXCLUSIVE_READ,
    JZN_E_TRANSACTION_CONFLICT,
    JZN_E_INVALID_STORED_VALUE,
    JZN_E_INVALID_AUTHORITY_SOURCE_CLOSURE,
    JZN_E_MISSING_TRANSACTION,
    JZN_E_MALFORMED_VIEW_UPDATE,
    JZN_E_MAINTAINED_VIEW_MISSING_BUNDLE_WITNESS,
    JZN_E_MISSING_OPEN_BATCH,
    JZN_E_DUPLICATE_OPEN_BATCH,
    JZN_E_OPEN_TRANSACTION_IDENTITY_MISMATCH,
    JZN_E_NON_MONOTONE_STATE,
    JZN_E_CONFLICTING_COMMIT_UNIT,
    JZN_E_CONFLICTING_FATE,
    JZN_E_UNSUPPORTED_COMMIT_UNIT,
    JZN_E_UNSUPPORTED_SYNC_MESSAGE,
    JZN_E_UNAUTHORIZED_CATALOGUE_UPDATE,
    JZN_E_INVALID_CATALOGUE_UPDATE,
    JZN_E_CATALOGUE_ACTIVATION_FAILED,
    JZN_E_CATALOGUE_UNINITIALIZED,
    JZN_E_CATALOGUE_CODEC,
    JZN_E_HISTORICAL_READ_REQUIRES_SERVER,
    JZN_E_AUTHORIZATION_DENIED,
    JZN_E_SUBSCRIPTION_CLOSED,
    JZN_E_END
};
/* &'static str payload variants: the payload is the message; Display adds the prefix. */
#define JZN_ERR(code, detail) jz_err_new((code), (detail))
#define JZN_INVALID_STORED(d) JZN_ERR(JZN_E_INVALID_STORED_VALUE, (d))
#define JZN_INVALID_CATALOGUE(d) JZN_ERR(JZN_E_INVALID_CATALOGUE_UPDATE, (d))
#define JZN_MALFORMED_VIEW(d) JZN_ERR(JZN_E_MALFORMED_VIEW_UPDATE, (d))
#define JZN_INVALID_MERGEABLE(d) JZN_ERR(JZN_E_INVALID_MERGEABLE_COMMIT, (d))
#define JZN_NON_MONOTONE(d) JZN_ERR(JZN_E_NON_MONOTONE_STATE, (d))
#define JZN_UNSUPPORTED_SYNC(d) JZN_ERR(JZN_E_UNSUPPORTED_SYNC_MESSAGE, (d))
void jazz_node__node__Error__Display__fmt(const jz_err *e, jz_str *out);
bool jazz_node__node__is_unrepresentable_enum_projection(const jz_err *e);
jz_err jazz_node__node__Error__missing_transaction(const jazz_model_TxId *tx);
jz_err jazz_node__node__Error__conflicting_commit_unit(const jazz_model_TxId *tx);

/* ---------------- jazz_node::node::api_error ---------------- */
typedef enum {
    JZN_API_SCHEMA, JZN_API_QUERY, JZN_API_WRITE_REJECTED, JZN_API_TRANSACTION_CONFLICT, JZN_API_STORAGE,
    JZN_API_PROTOCOL, JZN_API_BACKPRESSURE, JZN_API_NOT_OBSERVED, JZN_API_HISTORICAL_READ_REQUIRES_SERVER
} jazz_node_ErrorCode;
typedef struct jazz_node_ApiError { jazz_node_ErrorCode code; jz_str message; } jazz_node_ApiError;

jazz_node_ApiError jazz_node__node__api_error__Error__new(jazz_node_ErrorCode code, jz_str message);
jazz_node_ApiError jazz_node__node__api_error__Error__From_NodeError__from(jz_err error);
/* ApiError carried through jz_err: code = JZN_API_ERR_BASE + ErrorCode, msg = message */
#define JZN_API_ERR_BASE 0x4f00u
jz_err jazz_node__node__api_error__Error__into_err(jazz_node_ErrorCode code, jz_str message);

/* ---------------- small enums / structs from mod.rs ---------------- */
typedef enum { JZN_LENS_FORWARD, JZN_LENS_REVERSE } jazz_node_LensPathDirection;
typedef struct jazz_node_LensPathCacheKey { jazz_types_SchemaVersionId source, target; } jazz_node_LensPathCacheKey;
typedef struct jazz_node_CompiledLensCacheKey { jazz_types_SchemaVersionId source, target; jz_str table; } jazz_node_CompiledLensCacheKey;
typedef enum { JZN_CLO_RENAME, JZN_CLO_COPY, JZN_CLO_ADD, JZN_CLO_DROP } jazz_node_CompiledLensOpTag;
typedef struct jazz_node_CompiledLensOp {
    jazz_node_CompiledLensOpTag tag;
    jz_str a;           /* from / column */
    jz_str b;           /* to */
    groove_Value deflt; /* Add */
} jazz_node_CompiledLensOp;
typedef struct jazz_node_CompiledLensPath { jz_str target_table; JZ_VEC(jazz_node_CompiledLensOp) ops; } jazz_node_CompiledLensPath;
void jazz_node__node__CompiledLensOp__drop(void *p);
void jazz_node__node__CompiledLensPath__drop(void *p);

typedef struct jazz_node_LargeValueStagingPolicy { u64 incoming_bytes_per_window, window_ms, max_age_ms; } jazz_node_LargeValueStagingPolicy;
typedef struct jazz_node_LargeValueIngressState { u64 window_started_ms, admitted_bytes; } jazz_node_LargeValueIngressState;
#define JZN_LARGE_VALUE_UPLOAD_START_INGRESS_CHARGE_BYTES (1ull << 20)
#define JZN_SKEW_TOLERANCE_MS 30000ull
#define JZN_TX_VERSION_TABLE_CACHE_MAX_ENTRIES 4096u
#define JZN_HISTORY_TX_AUTHOR_CACHE_MAX_ENTRIES 4096u

typedef struct jazz_node_ActiveSchema {
    jazz_types_SchemaVersionId schema;
    u64 revision;
    jazz_model_JazzSchema *compiled;
} jazz_node_ActiveSchema;

typedef struct jazz_node_SchemaCatalogue {
    jazz_types_SchemaVersionId local_schema_version_id;
    bool has_local_schema_version_alias; jazz_types_SchemaVersionAlias local_schema_version_alias;
    jazz_model_JazzSchema *schema;
    jz_btree schema_version_aliases;      /* SchemaVersionId -> SchemaVersionAlias */
    jz_btree catalogue_schemas;           /* SchemaVersionId -> SchemaVersion* */
    jz_btree catalogue_lenses;            /* MigrationLensId -> MigrationLens* */
    jz_btree physical_mappings;           /* SchemaVersionId -> SchemaPhysicalMapping */
    jz_btree physical_current_winner_projections;
    jz_btree staged_lineages;             /* u64 -> StagedSchemaLineage */
    jz_btree pending_lineages;            /* u64 -> PendingSchemaLineage */
    jz_btree active_lineages_by_target;   /* SchemaVersionId -> StagedSchemaLineage */
    u64 active_catalogue_seq;
    u64 next_physical_table_id;
    u64 next_physical_column_id;
    jz_btree lens_path_cache;             /* LensPathCacheKey -> Option<Vec<(lens, dir)>> */
    jz_btree compiled_lens_cache;         /* CompiledLensCacheKey -> Option<CompiledLensPath> */
    jz_btree physical_write_plan_cache;
    jazz_node_ActiveSchema active_schema;
} jazz_node_SchemaCatalogue;
const jazz_model_JazzSchema *jazz_node__node__SchemaCatalogue__schema_for_version(const jazz_node_SchemaCatalogue *c, jazz_types_SchemaVersionId v);

typedef enum { JZN_BOOTSTRAP_UNINITIALIZED, JZN_BOOTSTRAP_READY } jazz_node_CatalogueBootstrapState;

typedef struct jazz_node_Clock {
    jazz_types_TxTime tx_time;
    jazz_types_TxTime *reservation_high_water; /* Rc<Cell<TxTime>> */
    jazz_types_GlobalTime global_time_register;
    jz_btree locally_minted_global_times;          /* set<GlobalTime> */
    jazz_types_GlobalTime committed_global_time;
    jz_btree applied_global_times_after_frontier;  /* set<GlobalTime> */
    jz_btree frontier_dots;                        /* GlobalTime -> Vec<TxId> */
} jazz_node_Clock;

typedef struct jazz_node_Parking {
    jz_btree parked_shape_registrations;   /* ShapeId -> ShapeAst */
    jz_btree parked_binding_deltas;        /* ShapeId -> Vec<ParkedSubscription> */
    jz_btree parked_commit_units;          /* TxId -> ParkedCommitUnit */
    jz_btree parked_catalogue_commit_units;/* set<TxId> */
    u64 newest_ingest_ms;
} jazz_node_Parking;

typedef struct jazz_node_QueryServing {
    jz_btree watermark_restore_seen;
    jz_btree persisted_watermarks;
    bool watermarks_invalidated;
    jz_btree local_availability_records;
    jz_btree local_availability_authorities;
    jz_btree local_unavailable_inputs;
    jz_btree query_shape_cache;
    jz_btree read_policy_authorization_request_cache;
    jz_btree compiled_query_program_cache;
    void *query_program_templates;
    jz_deque supported_query_program_requests;
    jz_btree policy_authorization_graph_cache;
    jz_btree policy_authorization_graph_replacements;
    JZ_VEC(struct { jz_str table; void *lease; }) policy_proof_stack;
    jz_btree tx_versions_cache;              /* TxId -> CachedTransactionVersions */
    jz_deque tx_version_tables_cache_order;  /* TxId */
    jz_btree tx_version_tables_cache_order_set;
    jz_btree version_storage_sources_cache;
    jz_btree registered_shapes;              /* ShapeId -> ValidatedQuery* */
    jz_btree registered_shape_options;
    jz_btree peer_shape_owners;
    jz_btree locally_registered_shapes;
    jz_btree outbound_shape_owners;
    jz_btree outbound_binding_owners;
    jz_btree registered_bindings;
    jz_btree authority_results;              /* AuthorityResultKey -> AuthorityResultState */
    jz_btree applied_view_update_generations;
    jz_btree retained_root_window_sources;
} jazz_node_QueryServing;

typedef struct jazz_node_OpenTxState {
    jz_btree open_transactions;          /* OpenTransactionId -> OpenTransaction* */
    jz_btree closed_batches;             /* set<OpenTransactionId> */
    jz_btree local_permission_subjects;  /* TxId -> AuthorSubject */
    JZ_VEC(groove_StagedLargeValueId) released_large_values;
    jz_btree pending_foreign_transactions; /* set<TxId> */
} jazz_node_OpenTxState;

typedef struct jazz_node_RejectionTracking { jz_btree rejected_transactions; } jazz_node_RejectionTracking;

typedef struct jazz_node_AheadCurrentKeys {
    jz_hmap keys;            /* (PhysicalTableId, Vec<u8>) -> TxId */
    jz_hmap rows_per_branch; /* PhysicalTableId -> HashMap<Vec<u8>, usize> */
} jazz_node_AheadCurrentKeys;
typedef struct jazz_node_AheadKey { jazz_types_PhysicalTableId table; jz_bytes key; } jazz_node_AheadKey;
const jazz_model_TxId *jazz_node__node__AheadCurrentKeys__get(const jazz_node_AheadCurrentKeys *k, const jazz_node_AheadKey *key);
bool jazz_node__node__AheadCurrentKeys__contains_key(const jazz_node_AheadCurrentKeys *k, const jazz_node_AheadKey *key);
bool jazz_node__node__AheadCurrentKeys__insert(jazz_node_AheadCurrentKeys *k, jazz_node_AheadKey key, jazz_model_TxId tx, jazz_model_TxId *prev);
bool jazz_node__node__AheadCurrentKeys__remove(jazz_node_AheadCurrentKeys *k, const jazz_node_AheadKey *key, jazz_model_TxId *removed);
void jazz_node__node__AheadCurrentKeys__clear(jazz_node_AheadCurrentKeys *k);
usize jazz_node__node__AheadCurrentKeys__branch_rows(const jazz_node_AheadCurrentKeys *k, jazz_types_PhysicalTableId t, const jazz_model_BranchKey *b);
void jazz_node__node__AheadCurrentKeys__init(jazz_node_AheadCurrentKeys *k);

typedef struct jazz_node_NodeAliases {
    jz_btree by_node;  /* NodeUuid -> NodeAlias */
    jz_hmap by_alias;  /* NodeAlias -> NodeUuid */
    u64 max_alias;
    u64 retargets;
} jazz_node_NodeAliases;
void jazz_node__node__node_aliases__NodeAliases__init(jazz_node_NodeAliases *a);
const jazz_types_NodeAlias *jazz_node__node__node_aliases__NodeAliases__get(const jazz_node_NodeAliases *a, const jazz_types_NodeUuid *n);
bool jazz_node__node__node_aliases__NodeAliases__node_for_alias(const jazz_node_NodeAliases *a, jazz_types_NodeAlias alias, jazz_types_NodeUuid *out);
bool jazz_node__node__node_aliases__NodeAliases__insert(jazz_node_NodeAliases *a, jazz_types_NodeUuid node, jazz_types_NodeAlias alias, jazz_types_NodeAlias *prev);
u64 jazz_node__node__node_aliases__NodeAliases__retarget_count(const jazz_node_NodeAliases *a);
u64 jazz_node__node__node_aliases__NodeAliases__max_alias(const jazz_node_NodeAliases *a);
usize jazz_node__node__node_aliases__NodeAliases__len(const jazz_node_NodeAliases *a);
bool jazz_node__node__node_aliases__NodeAliases__PartialEq__eq(const jazz_node_NodeAliases *a, const jazz_node_NodeAliases *b);
const jazz_types_NodeAlias *jazz_node__node__node_aliases__NodeAliases__Index__index(const jazz_node_NodeAliases *a, const jazz_types_NodeUuid *n);

typedef struct jazz_node_DatabaseSlot { groove_Database *database; } jazz_node_DatabaseSlot;
jazz_node_DatabaseSlot jazz_node__node__database_slot__DatabaseSlot__new(groove_Database *db);
groove_Database *jazz_node__node__database_slot__DatabaseSlot__take(jazz_node_DatabaseSlot *s);
groove_Database *jazz_node__node__database_slot__DatabaseSlot__into_inner(jazz_node_DatabaseSlot s);
groove_Database *jazz_node__node__database_slot__DatabaseSlot__Deref__deref(const jazz_node_DatabaseSlot *s);
groove_Database *jazz_node__node__database_slot__DatabaseSlot__DerefMut__deref_mut(jazz_node_DatabaseSlot *s);

typedef struct jazz_node_ClientRelayScope {
    jz_str storage_owner;
    bool has_admitted_session; jazz_types_AuthorSubject admitted_session;
} jazz_node_ClientRelayScope;

typedef struct jazz_node_SyncMetrics {
    u64 parked_orphans, parked_orphans_resolved, parked_catalogue_orphans, parked_catalogue_orphans_resolved,
        dropped_malformed_relay_commit_units, parked_catalogue_shapes, parked_catalogue_shapes_resolved,
        dropped_detached_subscription_messages, dropped_peer_request_messages, transport_backpressure_retries,
        receiver_bulk_bundle_ingests, receiver_per_bundle_ingests, receiver_bulk_ingest_commits,
        receiver_history_table_probes, receiver_history_rebuild_rows_avoided,
        authoritative_reset_missing_payload_fallbacks, peer_payload_inventory_missing_fallbacks;
} jazz_node_SyncMetrics;
typedef struct jazz_node_QueryEngineReadMetrics {
    u64 policy_authorization_graphs, policy_authorized_source_joins, source_primary_key_scans,
        source_index_probes, source_global_time_range_scans, source_full_scans,
        source_branch_register_full_scans, local_mask_count_fallbacks;
} jazz_node_QueryEngineReadMetrics;

/* ---------------- NodeState ---------------- */
typedef struct jazz_node_NodeState {
    jazz_types_NodeUuid node_uuid;
    bool has_self_node_alias; jazz_types_NodeAlias self_node_alias;
    jazz_node_SchemaCatalogue catalogue;
    jazz_node_CatalogueBootstrapState catalogue_bootstrap_state;
    bool catalogue_bootstrap_marker;
    jazz_node_Clock clock;
    jazz_node_Parking parking;
    jazz_node_QueryServing query;
    jazz_node_OpenTxState open_tx;
    jazz_node_RejectionTracking rejections;
    jazz_node_DatabaseSlot database;
    groove_LocalChunkReader *local_chunk_reader;
    void *chunk_resolver; const groove_MissingChunkResolverVt *chunk_resolver_vt;
    bool *detach_covered_chunk_waits;
    jazz_node_LargeValueStagingPolicy large_value_staging_policy;
    jazz_node_LargeValueIngressState large_value_ingress;
    groove_OwnedChunkProvider *content_runtime_provider;
    u64 groove_runtime_token;
    u64 physical_identity_generation;
    u64 next_authoritative_reset_generation;
    bool history_complete;
    bool authoritative_scalar_exit_refresh;
    jz_hmap client_local_literal_shapes;
    jazz_model_DurabilityTier authored_commit_durability;
    bool has_relay_authority_session_owner; jazz_node_ClientRelayScope relay_authority_session_owner;
    jz_btree pending_persistence;          /* set<TxId> */
    jazz_node_NodeAliases node_aliases;
    bool has_absent_node_alias; jazz_types_NodeUuid absent_node_alias;
    jazz_node_AheadCurrentKeys ahead_current_keys;
    jz_btree tx_touched_dirty;
    jz_btree history_tx_authors;
    jz_hmap history_tx_seqs;
    bool minting_global_time;
    jazz_node_SyncMetrics sync_metrics;
    jazz_node_QueryEngineReadMetrics query_engine_read_metrics;
    jz_btree session_claims;             /* AuthorSubject -> BTreeMap<String, Value> */
    jz_btree session_claim_revisions;    /* AuthorSubject -> u64 */
    bool has_active_session_claims; jazz_types_AuthorSubject active_session_identity; jz_btree active_session_claims;
    bool permissions_ready;
    bool catalogue_activation_failed;
    bool has_initial_sync_flush_cadence; usize initial_sync_flush_cadence;
    bool initial_sync_flush_active;
    bool initial_sync_flush_completed;
} jazz_node_NodeState;
typedef jazz_node_NodeState NS;

/* ---------------- CurrentRow ---------------- */
typedef JZ_VEC(jz_str *) jzn_strptr_vec;
typedef struct jazz_node_CurrentRow {
    groove_InternStr table;
    groove_OwnedRecord *record;                  /* Arc<OwnedRecord> */
    bool deleted;
    jazz_engine_CurrentRowPublicationFieldVec *publication_fields; /* Arc<Vec<..>> */
} jazz_node_CurrentRow;
typedef struct jazz_node_CurrentRowVec_ { jazz_node_CurrentRow *ptr; u32 len; u32 cap; } jazz_node_CurrentRowVec;
void jazz_node__node__CurrentRow__clone(void *dst, const void *src);
void jazz_node__node__CurrentRow__drop(void *p);
jazz_types_RowUuid jazz_node__node__CurrentRow__row_uuid(const jazz_node_CurrentRow *r);
bool jazz_node__node__CurrentRow__cell_at(const jazz_node_CurrentRow *r, u32 pos, groove_Value *out);
bool jazz_node__node__CurrentRow__cell(const jazz_node_CurrentRow *r, const jazz_model_TableSchema *t, const jz_str *column, groove_Value *out);
jazz_node_CurrentRow jazz_node__node__CurrentRow__new_with_publication_fields(jz_str table, groove_OwnedRecord *rec, jazz_engine_CurrentRowPublicationFieldVec fields);
jazz_node_CurrentRow jazz_node__node__CurrentRow__new_with_explicit_binding_fields_and_names(jz_str table, groove_OwnedRecord *rec, jazz_engine_CurrentRowBindingRoleVec roles, jzn_strptr_vec names);
jazz_node_CurrentRow jazz_node__node__CurrentRow__new_with_explicit_binding_fields(jz_str table, groove_OwnedRecord *rec, jazz_engine_CurrentRowBindingRoleVec roles);
jazz_node_CurrentRow jazz_node__node__CurrentRow__new_with_binding_fields(jz_str table, groove_OwnedRecord *rec, jazz_engine_CurrentRowBindingRole role);
jazz_node_CurrentRow jazz_node__node__CurrentRow__new(jz_str table, groove_OwnedRecord *rec);
jz_err jazz_node__node__CurrentRow__project(const jazz_node_CurrentRow *r, const jazz_model_TableSchema *t, const jz_str *cols, u32 ncols, jazz_node_CurrentRow *out);
bool jazz_node__node__CurrentRow__PartialEq__eq(const jazz_node_CurrentRow *a, const jazz_node_CurrentRow *b);
bool jazz_node__node__CurrentRow__subscription_equivalent(const jazz_node_CurrentRow *a, const jazz_node_CurrentRow *b);
bool jazz_node__node__CurrentRow__aggregate_payload_equivalent(const jazz_node_CurrentRow *a, const jazz_node_CurrentRow *b);
bool jazz_node__node__CurrentRow__raw_field(const jazz_node_CurrentRow *r, const char *field, groove_Value *out);
bool jazz_node__node__CurrentRow__projected_tx_alias(const jazz_node_CurrentRow *r, jazz_types_TxTime *t, jazz_types_NodeAlias *a);

typedef struct jazz_node_RowProvenance {
    jazz_types_AuthorSubject created_by; u64 created_at;
    jazz_types_AuthorSubject updated_by; u64 updated_at;
} jazz_node_RowProvenance;
jz_err jazz_node__node__CurrentRow__provenance(const jazz_node_CurrentRow *r, bool *has, jazz_node_RowProvenance *out);

/* ---------------- MergeableCommit ---------------- */
typedef struct jazz_node_MergeableCommit {
    jz_str table;
    jazz_types_RowUuid row_uuid;
    jazz_model_BranchSelector *branch;
    jazz_types_AuthorSubject made_by;
    bool has_permission_subject; jazz_types_AuthorSubject permission_subject;
    u64 now_ms;
    jz_btree cells;              /* String -> Value */
    bool has_authored_columns; jz_btree authored_columns; /* set<String> */
    bool has_deletion; jazz_model_DeletionEvent deletion;
    bool has_user_metadata_json; jz_str user_metadata_json;
    jz_btree prepared_large_columns;   /* set<String> */
    JZ_VEC(groove_StagedLargeValueId) staged_large_values;
    bool known_fresh_row;
} jazz_node_MergeableCommit;
void jazz_node__node__MergeableCommit__new(jazz_node_MergeableCommit *out, jz_str table, jazz_types_RowUuid row, u64 now_ms);
jz_err jazz_node__node__MergeableCommit__validate(const jazz_node_MergeableCommit *c);
bool jazz_node__node__MergeableCommit__needs_large_value_staging(const jazz_node_MergeableCommit *c);
jazz_types_AuthorSubject jazz_node__node__MergeableCommit__effective_permission_subject(const jazz_node_MergeableCommit *c);
void jazz_node__node__MergeableCommit__drop(void *p);
bool jazz_node__node__value_contains_indirect_descriptor(const groove_Value *v);
void jazz_node__node__collect_indirect_descriptors(const groove_Value *v, void *descriptors /* Vec<LargeValueRef*> */);

/* ---------------- authority result state ---------------- */
typedef struct jazz_node_AuthoritySourceIncremental {
    u64 predecessor_generation, generation;
    jz_btree adds, removes; /* set<SupportingRow> */
} jazz_node_AuthoritySourceIncremental;
typedef struct jazz_node_AuthorityResultState {
    u64 applied_view_update_generation;
    u32 source_closure_tag; u64 source_closure_generation; /* Pending | Claimed{generation} */
    JZ_VEC(jazz_node_AuthoritySourceIncremental) source_incrementals;
    bool live_settled;
    bool has_supporting_revision; u8 supporting_revision[16];
    jz_btree covered_input_versions;
    bool has_compiled_covered_input_sources; jz_btree compiled_covered_input_sources;
    bool has_settled_through; jazz_types_GlobalTime settled_through;
    bool has_authorization_progress; u64 authorization_progress;
    bool known_state_declared, initial_hydration, deferred_publication;
    bool has_pending_authoritative_reset; u64 pending_authoritative_reset;
    bool pending_opening;
} jazz_node_AuthorityResultState;
enum { JZN_SOURCE_CLOSURE_PENDING, JZN_SOURCE_CLOSURE_CLAIMED };
void jazz_node__node__AuthorityResultState__default(jazz_node_AuthorityResultState *out);
typedef struct jazz_node_CoveredInputCoordinate {
    jazz_types_GlobalPhysicalTableId physical_table;
    jazz_types_RowUuid row;
    jazz_protocol_ResultRowLayer layer;
    jz_bytes branch;
} jazz_node_CoveredInputCoordinate;
void jazz_node__node__CoveredInputCoordinate__From__from(jazz_node_CoveredInputCoordinate *out, const jazz_protocol_SupportingRow *input);
int jazz_node__node__CoveredInputCoordinate__cmp(const void *a, const void *b, void *ctx);
void jazz_node__node__CoveredInputCoordinate__drop(void *p);
extern const jz_btree_desc jzn_desc_covered_inputs;          /* CoveredInputCoordinate -> SupportingRow */
extern const jz_btree_desc jzn_desc_compiled_scope_tables;   /* CompiledScopeTables: GlobalPhysicalTableId -> Intern<String> */
void jazz_node__node__record_maintained_view_stream_b_add_bundle(void);
void jazz_node__node__record_maintained_view_removal_stream_bundle(void);
void jazz_node__node__AuthorityResultState__drop(void *p);

/* ---------------- ViewUpdateParts / publication ---------------- */
JZ_NVEC(jazz_protocol_ResultMemberEntryVec, jazz_protocol_ResultMemberEntry);
typedef struct jazz_node_ViewUpdateParts {
    jazz_protocol_SupportingRowsUpdate *wire_rows; /* Option */
    jazz_protocol_SubscriptionKey subscription;
    jazz_types_GlobalTime settled_through;
    bool defer_settlement, reset_input_set;
    jazz_protocol_VersionCarrierPtrVec version_carriers;
    jazz_model_TxIdVec peer_complete_tx_payload_refs;
    bool has_authorization_progress; u64 authorization_progress;
    bool opening_pending;
    jazz_protocol_ResultMemberEntryVec result_member_adds;
    jazz_protocol_ResultMemberEntryVec result_member_removes;
} jazz_node_ViewUpdateParts;
JZ_NVEC(jazz_node_ViewUpdatePartsVec, jazz_node_ViewUpdateParts);
const jazz_protocol_SupportingRow *jazz_node__node__ViewUpdateParts__supporting_adds(const jazz_node_ViewUpdateParts *p, u32 *n);
const jazz_protocol_SupportingRow *jazz_node__node__ViewUpdateParts__supporting_removes(const jazz_node_ViewUpdateParts *p, u32 *n);
void jazz_node__node__ViewUpdateParts__drop(void *p);

typedef struct jazz_node_PublishedTransaction { jazz_model_TxId tx_id; groove_AppliedBatch *persistence; } jazz_node_PublishedTransaction;
typedef struct jazz_node_PublicationOutcome {
    void *value; /* T, type-erased */
    JZ_VEC(jazz_node_PublishedTransaction) publications;
    jz_deque post_settlement_work; /* SyncMessage* */
} jazz_node_PublicationOutcome;
void jazz_node__node__PublicationOutcome__append_outcome(jazz_node_PublicationOutcome *self, jazz_node_PublicationOutcome *other, usize elem);

/* Parked subscription / commit-unit ingest context */
typedef enum { JZN_TRUST_SESSION, JZN_TRUST_RELAY, JZN_TRUST_BACKEND, JZN_TRUST_AUTHORITY, JZN_TRUST_ADMIN } jazz_node_CommitUnitTrust;
typedef struct jazz_node_CommitUnitIngestContext {
    jazz_types_AuthorSubject identity;
    jazz_node_CommitUnitTrust trust;
    bool admitted_write_authorization;
    bool version_receipts_validated;
} jazz_node_CommitUnitIngestContext;

/* shared map descriptors (defined in mod.c) */
extern const jz_btree_desc jzn_desc_str_value;    /* String -> Value */
extern const jz_btree_desc jzn_desc_str_set;      /* set<String> */
extern const jz_btree_desc jzn_desc_txid_set;     /* set<TxId> */
extern const jz_btree_desc jzn_desc_u64_set;      /* set<u64> */
extern const jz_btree_desc jzn_desc_uuid_u64;     /* Uuid -> u64 */
extern const jz_btree_desc jzn_desc_uuid_set;     /* set<Uuid> */
extern const jz_btree_desc jzn_desc_str_ptr;      /* String -> T* (opaque) */
extern const jz_btree_desc jzn_desc_uuid_ptr;     /* Uuid -> T* (opaque) */
extern const jz_btree_desc jzn_desc_txid_ptr;     /* TxId -> T* (opaque) */
int jzn_cmp_txid(const void *a, const void *b, void *ctx);
void jzn_str_drop_v(void *p);

/* lens helpers */
void jazz_node__node__apply_compiled_lens_path(const jazz_node_CompiledLensPath *path, jz_btree *cells, jz_str *out_table);
jz_err jazz_node__node__validate_registered_transform(const jz_str *transform);
jz_err jazz_node__node__push_compiled_forward_lens_op(const jazz_protocol_LensOp *op, void *compiled);
jz_err jazz_node__node__push_compiled_reverse_lens_op(const jazz_protocol_LensOp *op, void *compiled);
jz_err jazz_node__node__validate_mergeable_write_shape(bool cells_empty, bool deletion_present);
u64 jazz_node__node__next_groove_runtime_token(void);
bool jazz_node__node__is_catalogue_mutation(const jazz_protocol_SyncMessage *m);

/* ---------------- NodeState methods defined in node-core-b files ---------------- */
jz_err jazz_node__node__currency__NodeState__query_transaction(NS *s, jazz_model_TxId tx, bool *found, jazz_node_StoredTransaction *out);
jz_err jazz_node__node__currency__NodeState__query_versions_for_tx(NS *s, jazz_model_TxId tx, jazz_node_VersionRowVec *out);
jz_err jazz_node__node__currency__NodeState__query_table_versions(NS *s, const jz_str *table, jazz_node_VersionRowVec *out);
jz_err jazz_node__node__views__NodeState__whole_table_shape_binding(NS *s, const jz_str *table, jazz_model_ValidatedQuery **shape, jazz_model_Binding **binding);
jz_err jazz_node__node__views__NodeState__version_bundle_for_maintained_view_versions_with_tx(NS *s, const jazz_node_StoredTransaction *tx,
    jazz_node_VersionRow *const *versions, u32 n, jazz_protocol_VersionBundle *out);
bool jazz_node__node__global_state__NodeState__visible_global_tx_id_for_physical_table_now(NS *s, jazz_types_PhysicalTableId t, jazz_types_RowUuid row, jazz_model_TxId *out);
bool jazz_node__node__global_state__NodeState__visible_global_current_now(NS *s, jazz_types_SchemaVersionId v, const jz_str *table, jazz_types_RowUuid row, jazz_model_TxId *tx, bool *deleted);
bool jazz_node__node__global_state__NodeState__visible_global_row_tx_id_now(NS *s, jazz_types_SchemaVersionId v, const jz_str *table, jazz_types_RowUuid row, jazz_model_TxId *out);
bool jazz_node__node__global_state__NodeState__visible_global_content_tx_id_in_schema_now(NS *s, jazz_types_SchemaVersionId v, const jz_str *table, jazz_types_RowUuid row, jazz_model_TxId *out);
jz_err jazz_node__node__global_state__NodeState__global_rows_changed_after(NS *s, const jz_str *table, jazz_types_GlobalTime pos, jz_btree *out);
jz_err jazz_node__node__global_state__NodeState__supporting_catch_up_after(NS *s, const jz_str *table, jazz_types_GlobalPhysicalTableId pt,
    const jazz_protocol_SupportingRow *const *current, u32 n, jazz_types_GlobalTime pos, const u8 predecessor[16], jazz_protocol_SupportingRowsUpdate **out);
jz_err jazz_node__node__global_state__NodeState__global_current_updates_for_versions(NS *s, const groove_DatabaseBatch *b, jazz_model_TxId tx,
    jazz_types_GlobalTime gt, jazz_node_VersionRow *const *versions, u32 n, jazz_node_VersionRowVec *out);

jz_err jazz_node__node__open_tx__NodeState__record_applied_global_time(NS *s, jazz_types_GlobalTime gt, jazz_model_TxId tx, jazz_types_GlobalTime *newly, bool *has_newly);
jazz_node_CurrentRow jazz_node__node__CurrentRow__into_deleted(jazz_node_CurrentRow r);
jz_err jazz_node__node__open_tx__NodeState__tx_visible_current_cells_in_branch(NS *self, jazz_model_OpenTransactionId tx_id, const jz_str *table, jazz_types_RowUuid row_uuid,
                                                                              const jazz_model_BranchSelector *branch, bool *some, jz_btree *out);
jazz_node_VersionRow *jazz_node__node__open_tx__NodeState__snapshot_winner(NS *self, jazz_types_SchemaVersionId schema_version, const jz_str *table,
                                                                 jazz_types_RowUuid row_uuid, const jazz_model_Snapshot *snapshot);
jz_err jazz_node__node__recovery__NodeState__rejected_versions_for(NS *s, jazz_types_NodeAlias alias, jazz_model_TxId tx, jzn_ptrvec *out);
jz_err jazz_node__node__recovery__NodeState__recover_from_storage(NS *s);
static inline groove_Database *jzn_db(NS *s) { return jazz_node__node__database_slot__DatabaseSlot__DerefMut__deref_mut(&s->database); }

/* ---------------- jazz_node::node::exclusive_read_evidence ---------------- */
typedef struct jazz_node_StoredEvidence {
    bool has_base_snapshot; jazz_model_Snapshot base_snapshot;
    bool has_row_read_set; jazz_model_RowReadVec row_read_set;
    bool has_absent_read_set; jazz_model_AbsentReadVec absent_read_set;
    bool has_predicate_read_set; jazz_model_PredicateReadVec predicate_read_set;
} jazz_node_StoredEvidence;
groove_RecordDescriptor *jazz_node__node__exclusive_read_evidence__base_snapshot_descriptor(void);
groove_RecordDescriptor *jazz_node__node__exclusive_read_evidence__row_reads_descriptor(void);
groove_RecordDescriptor *jazz_node__node__exclusive_read_evidence__absent_reads_descriptor(void);
groove_RecordDescriptor *jazz_node__node__exclusive_read_evidence__predicate_reads_descriptor(void);
jz_err jazz_node__node__exclusive_read_evidence__evidence_slot_values(const jazz_model_Transaction *tx, bool pending, groove_Value out[4]);
jz_err jazz_node__node__exclusive_read_evidence__decode_evidence_slots(const jz_slice *base_snapshot, const jz_slice *row_read_set,
    const jz_slice *absent_read_set, const jz_slice *predicate_read_set, jazz_node_StoredEvidence *out);
void jazz_node__node__exclusive_read_evidence__StoredEvidence__drop(jazz_node_StoredEvidence *s);

/* ---------------- NodeState methods in currency.rs / open_tx.rs / source_resolution.rs ---------------- */
jz_err jazz_node__node__currency__NodeState__query_table_versions_in_branch(NS *s, const jz_str *table, const jazz_model_BranchKey *branch_key, jazz_node_VersionRowVec *out);
bool jazz_node__node__open_tx__NodeState__snapshot_covers(NS *s, jazz_model_TxId tx_id, const jazz_model_Snapshot *snapshot);
extern const jz_btree_desc jzn_desc_branch_key_set; /* set<BranchKey*> */
jz_err jazz_node__node__source_resolution__NodeState__equivalent_stored_branch_keys(NS *self, jz_slice table, jazz_types_SchemaVersionId read_schema_version,
    const jazz_model_BranchKey *selected, jz_btree *out);
jz_err jazz_node__node__source_resolution__NodeState__inherited_branch_view_copy_evidence(NS *self, const jz_str *table, const jazz_model_BranchSelector *head,
    const jazz_protocol_BranchViewBase *base, jazz_types_RowUuid row_uuid, bool *some, jazz_model_BranchViewCopyEvidence *out);
jz_err jazz_node__node__source_resolution__NodeState__resolve_branch_view_copy_evidence(NS *self, const jazz_model_BranchViewCopyEvidence *evidence,
    jazz_types_SchemaVersionId authored_schema, const jazz_model_TxId *candidate_tx_id, jazz_node_VersionRow **out);
jz_err jazz_node__node__source_resolution__NodeState__visible_current_cells_in_branch_view(NS *self, const jz_str *table, const jazz_model_BranchSelector *head,
    const jazz_protocol_BranchViewBase *base, jazz_types_RowUuid row_uuid, bool *some, jz_btree *out);
jz_err jazz_node__node__source_resolution__NodeState__translate_cells(NS *self, jazz_types_SchemaVersionId source, jazz_types_SchemaVersionId target,
    const jz_str *table, jz_btree *cells, bool *some, jz_str *out_table);
void jazz_node__node__source_resolution__keep_latest_version(jz_btree *content, jz_btree *tx_ids, jazz_model_TxId tx_id, jazz_node_VersionRow *version);

const void *jazz_node__node__layer_stack__top_wins(const jz_btree *const *layers, u32 n, const void *key);
jz_err jazz_node__node__layer_stack__branch_view_stack(const jazz_model_JazzSchema *schema, const jazz_model_TableSchema *table,
    const jazz_model_BranchSelector *head_sel, const jazz_protocol_BranchViewBase *base,
    jazz_model_BranchKey **head_out, bool *has_base, jazz_engine_BranchViewSourceBase *base_out, jz_str *err_msg);

/* ---------------- catalogue lineage records (mod.rs) ---------------- */
typedef struct jazz_node_StagedSchemaLineage {
    u64 catalogue_seq;
    jazz_protocol_SchemaLineagePublication publication;
    jazz_types_SchemaVersionAlias alias;
    jazz_node_SchemaPhysicalMapping mapping;
} jazz_node_StagedSchemaLineage;
typedef struct jazz_node_PendingSchemaLineage { u64 catalogue_seq; jazz_protocol_SchemaLineagePublication publication; } jazz_node_PendingSchemaLineage;
typedef struct jazz_node_CatalogueBootstrapReady {
    jazz_types_SchemaVersionId genesis;
    jazz_protocol_CurrentWriteSchema current_write_schema;
    u64 active_catalogue_seq;
} jazz_node_CatalogueBootstrapReady;
void jazz_node__node__StagedSchemaLineage__Clone__clone(void *dst, const void *src);
void jazz_node__node__StagedSchemaLineage__drop(void *p);
void jazz_node__node__PendingSchemaLineage__Clone__clone(void *dst, const void *src);
void jazz_node__node__PendingSchemaLineage__drop(void *p);
void jazz_node__node__SchemaCatalogue__Clone__clone(jazz_node_SchemaCatalogue *dst, const jazz_node_SchemaCatalogue *src);
void jazz_node__node__SchemaCatalogue__drop(jazz_node_SchemaCatalogue *c);
/* defined by other units (state/catalogue.rs, codec.rs, ingest.rs) */
jz_err jazz_node__node__NodeState__write_active_schema_lineage_to_batch(groove_DatabaseBatch *b, const jazz_node_StagedSchemaLineage *staged);
jz_err jazz_node__node__NodeState__write_schema_version_mapping_to_batch(groove_DatabaseBatch *b, jazz_types_SchemaVersionAlias alias,
    jazz_types_SchemaVersionId id, const jazz_node_SchemaPhysicalMapping *mapping);
jz_err jazz_node__node__NodeState__write_active_schema_to_batch(groove_DatabaseBatch *b, const jazz_node_ActiveSchema *active);
jz_err jazz_node__node__NodeState__reconcile_publication_mapping(const jazz_node_SchemaCatalogue *planned, const jazz_protocol_SchemaLineagePublication *publication,
    const jazz_node_SchemaPhysicalMapping *fresh, jazz_node_SchemaPhysicalMapping *out);
jz_bytes jazz_node__node__codec__encode_catalogue_bootstrap_ready(const jazz_node_CatalogueBootstrapReady *ready);
jz_err jazz_node__node__ingest__NodeState__drain_parked_commit_units(NS *s, jazz_node_PublicationOutcome *out);
jz_err jazz_node__node__ingest__NodeState__drain_parked_relay_commit_units(NS *s);
jz_err jazz_node__node__ActiveSchema__new(jazz_protocol_CurrentWriteSchema pointer, jazz_model_JazzSchema *compiled, jazz_node_ActiveSchema *out);
jazz_protocol_CurrentWriteSchema jazz_node__node__ActiveSchema__wire_pointer(const jazz_node_ActiveSchema *s);
bool jazz_node__node__ActiveSchema__same_permissions(const jazz_node_ActiveSchema *a, const jazz_node_ActiveSchema *b);
bool jazz_node__node__ActiveSchema__same_authorization_source(const jazz_node_ActiveSchema *self, const jazz_node_ActiveSchema *other,
    const jazz_node_SchemaPhysicalMapping *self_mapping, const jazz_node_SchemaPhysicalMapping *other_mapping);
/* catalogue_ingest.rs */
bool jazz_node__node__catalogue_ingest__active_runtime_layouts_equal(const jazz_node_SchemaCatalogue *left, const jazz_node_SchemaCatalogue *right);
jz_err jazz_node__node__catalogue_ingest__NodeState__apply_trusted_catalogue_snapshot(NS *self, jazz_protocol_CatalogueSnapshot snapshot, jazz_node_PublicationOutcome *out);

/* policy.rs: ViewEvaluationContext { tx_rows: BTreeMap<TxId, Option<StoredTransaction>> } */
typedef struct { bool some; jazz_node_StoredTransaction tx; } jazz_node_OptStoredTransaction;
typedef struct jazz_node_ViewEvaluationContext { jz_btree tx_rows; } jazz_node_ViewEvaluationContext;
void jazz_node__node__policy__ViewEvaluationContext__default(jazz_node_ViewEvaluationContext *out);
void jazz_node__node__policy__ViewEvaluationContext__drop(jazz_node_ViewEvaluationContext *c);
/* currency.rs */
jz_err jazz_node__node__currency__NodeState__query_row_versions_in_branch(NS *self, const jz_str *table, const jazz_model_BranchKey *branch_key, jazz_types_RowUuid row_uuid, jazz_node_VersionRowVec *out);
jz_err jazz_node__node__currency__NodeState__query_row_versions(NS *self, const jz_str *table, jazz_types_RowUuid row_uuid, jazz_node_VersionRowVec *out);
jz_err jazz_node__node__currency__NodeState__query_versions_in_schema(NS *self, jazz_types_SchemaVersionId schema_version, const jz_str *table, const jazz_types_RowUuid *row_uuid, jazz_node_VersionRowVec *out);
jz_err jazz_node__node__currency__NodeState__query_table_versions_in_branch(NS *self, const jz_str *table, const jazz_model_BranchKey *branch_key, jazz_node_VersionRowVec *out);
jz_err jazz_node__node__currency__NodeState__query_local_winner(NS *self, const jz_str *table, jazz_types_RowUuid row_uuid, jazz_node_VersionRow **out);
jz_err jazz_node__node__currency__NodeState__query_local_winner_in_branch(NS *self, const jz_str *table, const jazz_model_BranchKey *branch_key, jazz_types_RowUuid row_uuid, jazz_node_VersionRow **out);
jz_err jazz_node__node__currency__NodeState__query_global_winner_in_branch(NS *self, const jz_str *table, const jazz_model_BranchKey *branch_key, jazz_types_RowUuid row_uuid, jazz_node_VersionRow **out);
jz_err jazz_node__node__currency__NodeState__query_local_view_winner_in_branch(NS *self, const jz_str *table, const jazz_model_BranchKey *branch_key, jazz_types_RowUuid row_uuid, jazz_node_VersionRow **out);
jz_err jazz_node__node__currency__NodeState__query_local_winner_in_branch_excluding_tx(NS *self, const jz_str *table, const jazz_model_BranchKey *branch_key, jazz_types_RowUuid row_uuid, jazz_model_TxId excluded_tx_id, jazz_node_VersionRow **out);
jz_err jazz_node__node__currency__NodeState__query_global_winner(NS *self, const jz_str *table, jazz_types_RowUuid row_uuid, jazz_node_VersionRow **out);
jz_err jazz_node__node__currency__NodeState__query_global_winner_in_schema_and_branch(NS *self, jazz_types_SchemaVersionId schema_version, const jz_str *table, const jazz_model_BranchKey *branch_key, jazz_types_RowUuid row_uuid, jazz_node_VersionRow **out);
jz_err jazz_node__node__currency__NodeState__query_global_winner_in_schema(NS *self, jazz_types_SchemaVersionId schema_version, const jz_str *table, jazz_types_RowUuid row_uuid, jazz_node_VersionRow **out);
jz_err jazz_node__node__currency__NodeState__history_image_from_current_record(NS *self, jazz_types_SchemaVersionId schema_version, const jz_str *table, u32 variant_tag, const groove_OwnedRecord *current, jazz_node_VersionRow **out);
jz_err jazz_node__node__currency__NodeState__query_winner_from_pk(NS *self, const jz_str *table, jazz_types_RowUuid row_uuid, jazz_node_VersionRow **out);
jz_err jazz_node__node__currency__NodeState__query_winner_from_pk_in_branch(NS *self, const jz_str *table, const jazz_model_BranchKey *branch_key, jazz_types_RowUuid row_uuid, jazz_node_VersionRow **out);
jz_err jazz_node__node__currency__NodeState__query_table_versions(NS *self, const jz_str *table, jazz_node_VersionRowVec *out);
jz_err jazz_node__node__currency__NodeState__query_versions_for_tx(NS *self, jazz_model_TxId tx_id, jazz_node_VersionRowVec *out);
jz_err jazz_node__node__currency__NodeState__query_versions_for_tx_rows_by_alias(NS *self, jazz_model_TxId tx_id, jazz_types_NodeAlias tx_node_alias, const jz_btree *rows, jazz_node_VersionRowVec *out);
jz_err jazz_node__node__currency__NodeState__version_storage_sources(NS *self, const jz_str *table, jzn_strvec *out);
jz_err jazz_node__node__currency__NodeState__decode_history_record(NS *self, const jz_str *table, const groove_OwnedRecord *record, jazz_node_VersionRow **out);
jz_err jazz_node__node__currency__NodeState__decode_stored_history_record(NS *self, const groove_DatabaseBatch *batch, const jz_str *requested_table, const jz_str *storage_table, groove_OwnedRecord *record, jazz_node_VersionRow **out);
jz_err jazz_node__node__currency__NodeState__remember_history_tx_author(NS *self, jazz_types_TxTime tx_time, jazz_types_NodeAlias tx_node_alias, jazz_types_AuthorSubject made_by);
jz_err jazz_node__node__currency__NodeState__decode_stored_physical_history_record(NS *self, const jz_str *storage_table, groove_OwnedRecord *record, jazz_node_VersionRow **out);
jz_err jazz_node__node__currency__NodeState__resolve_history_updated_by(NS *self, const groove_DatabaseBatch *batch, jazz_node_VersionRow *version, jazz_node_VersionRow **out);
jz_err jazz_node__node__currency__NodeState__history_tx_seq(const NS *self, const groove_DatabaseBatch *batch, jazz_types_TxTime tx_time, jazz_types_NodeAlias tx_node_alias, bool *some, jazz_types_GlobalTime *out);
jz_err jazz_node__node__currency__NodeState__decode_history_owned_record(NS *self, const jz_str *requested_table, const jz_str *storage_table, groove_OwnedRecord *record, jazz_node_VersionRow **out);
jz_err jazz_node__node__currency__NodeState__query_transaction(NS *self, jazz_model_TxId tx_id, bool *found, jazz_node_StoredTransaction *out);
jz_err jazz_node__node__currency__NodeState__query_transaction_state(NS *self, jazz_model_TxId tx_id, bool *found, jazz_model_Fate *fate, bool *has_global_time, jazz_types_GlobalTime *global_time, jazz_model_DurabilityTier *durability);
jz_err jazz_node__node__currency__NodeState__query_transaction_global_time(NS *self, jazz_model_TxId tx_id, bool *found, bool *has_global_time, jazz_types_GlobalTime *global_time);
jz_err jazz_node__node__currency__NodeState__preload_transaction_memo(NS *self, const jazz_model_TxId *tx_ids, u32 n, jazz_node_ViewEvaluationContext *context);
jz_err jazz_node__node__currency__NodeState__stored_transaction_from_record(const NS *self, jazz_model_TxId tx_id, jazz_types_NodeAlias expected_alias, const groove_OwnedRecord *record, jazz_node_StoredTransaction *out);
jz_err jazz_node__node__currency__NodeState__transaction_exists(const NS *self, jazz_model_TxId tx_id, bool *out);
jz_err jazz_node__node__currency__NodeState__query_version_by_alias(NS *self, const jz_str *table, jazz_types_RowUuid row_uuid, jazz_types_TxTime tx_time, jazz_types_NodeAlias tx_node_alias, jazz_node_VersionRow **out);
jz_err jazz_node__node__currency__NodeState__query_version_by_alias_in_branch(NS *self, jazz_types_SchemaVersionId schema_version, const jz_str *table, const jazz_model_BranchKey *branch_key, jazz_types_RowUuid row_uuid, jazz_types_TxTime tx_time, jazz_types_NodeAlias tx_node_alias, jazz_node_VersionRow **out);
jz_err jazz_node__node__currency__NodeState__query_version_by_alias_with_storage(NS *self, const jz_str *table, const jz_str *storage_table, jazz_types_RowUuid row_uuid, jazz_types_TxTime tx_time, jazz_types_NodeAlias tx_node_alias, jazz_node_VersionRow **out);
jz_err jazz_node__node__currency__NodeState__query_version_by_alias_with_storage_in_schema(NS *self, jazz_types_SchemaVersionId schema_version, const jz_str *table, const jz_str *storage_table, const jazz_model_BranchKey *branch_key, jazz_types_RowUuid row_uuid, jazz_types_TxTime tx_time, jazz_types_NodeAlias tx_node_alias, jazz_node_VersionRow **out);

/* policy.rs */
#define JZN_MAX_TRANSACTION_POLICY_EVIDENCE_ROWS ((usize)1 << 18)
enum { JZN_UNIT_POLICY_ALLOWED, JZN_UNIT_POLICY_DENIED, JZN_UNIT_POLICY_UNSUPPORTED };
struct jazz_engine_NormalizedRowSetShape;
jz_err jazz_node__node__policy__NodeState__write_policy_allows_version_record(NS *self, const jazz_protocol_VersionRecord *version, jazz_types_AuthorSubject author,
    const jazz_model_TxId *candidate_tx_id, const jazz_protocol_VersionRecordVec *candidate_versions, bool *out);
jz_err jazz_node__node__policy__NodeState__commit_unit_write_policies_allow(NS *self, const jazz_protocol_VersionRecordVec *versions,
    jazz_types_AuthorSubject author, jazz_model_TxId candidate_tx_id, u32 *decision, jz_str *unsupported_reason);
jz_err jazz_node__node__policy__NodeState__version_satisfies_read_for_write_visibility(NS *self, const jazz_protocol_VersionRecord *version,
    jazz_types_AuthorSubject author, const jazz_model_TxId *candidate_tx_id, bool *out);
jz_err jazz_node__node__policy__NodeState__branch_view_copy_satisfies_read_for_write_visibility(NS *self, const jazz_model_BranchViewCopyEvidence *evidence,
    jazz_types_SchemaVersionId authored_schema, jazz_types_AuthorSubject author, const jazz_model_TxId *candidate_tx_id, bool *out);
jz_err jazz_node__node__policy__NodeState__dry_run_insert_allows(NS *self, jazz_node_MergeableCommit commit, bool *out);
jz_err jazz_node__node__policy__NodeState__dry_run_mergeable_write_allows_in_schema(NS *self, jazz_types_SchemaVersionId write_schema_version,
    jazz_node_MergeableCommit commit, bool *out);
jz_err jazz_node__node__policy__NodeState__dry_run_read_current_allows(NS *self, jz_slice table_name, jazz_types_RowUuid row_uuid,
    jazz_types_AuthorSubject identity, bool *out);
jz_err jazz_node__node__policy__NodeState__dry_run_read_current_allows_in_schema(NS *self, jz_slice table_name, jazz_types_RowUuid row_uuid,
    jazz_types_SchemaVersionId schema_version, jazz_types_AuthorSubject identity, bool include_deleted, bool *out);
jz_err jazz_node__node__policy__NodeState__dry_run_delete_current_allows(NS *self, jz_slice table_name, jazz_types_RowUuid row_uuid,
    jazz_types_AuthorSubject author, bool *out);
jz_err jazz_node__node__policy__NodeState__local_current_row_exists(NS *self, jz_slice table_name, jazz_types_RowUuid row_uuid, bool *out);
jazz_types_SchemaVersionId jazz_node__node__policy__NodeState__read_policy_schema_for_table_name(const NS *self, const jz_str *table,
    jazz_types_SchemaVersionId query_schema, const struct jazz_engine_NormalizedRowSetShape *shape);
jz_err jazz_node__node__policy__NodeState__query_transaction_memo(NS *self, jazz_model_TxId tx_id, jazz_node_ViewEvaluationContext *context,
    bool *found, jazz_node_StoredTransaction *out);

/* MergeableCommit builder (mod.rs) */
void jazz_node__node__MergeableCommit__known_fresh_row(jazz_node_MergeableCommit *c);
void jazz_node__node__MergeableCommit__branch(jazz_node_MergeableCommit *c, jazz_model_BranchSelector *b);
void jazz_node__node__MergeableCommit__made_by(jazz_node_MergeableCommit *c, jazz_types_AuthorSubject s);
void jazz_node__node__MergeableCommit__permission_subject(jazz_node_MergeableCommit *c, jazz_types_AuthorSubject s);
void jazz_node__node__MergeableCommit__cells(jazz_node_MergeableCommit *c, jz_btree *cells /* moved: String -> Value */);
void jazz_node__node__MergeableCommit__cell(jazz_node_MergeableCommit *c, jz_str column, groove_Value value);
void jazz_node__node__MergeableCommit__staged_large_cell(jazz_node_MergeableCommit *c, jz_str column, groove_StagedLargeValue staged, bool nullable);
void jazz_node__node__MergeableCommit__verified_inherited_large_cells(jazz_node_MergeableCommit *c, const jz_btree *inherited);
void jazz_node__node__MergeableCommit__authored_columns(jazz_node_MergeableCommit *c, jz_btree columns);
void jazz_node__node__MergeableCommit__deletion(jazz_node_MergeableCommit *c, jazz_model_DeletionEvent d);
void jazz_node__node__MergeableCommit__user_metadata(jazz_node_MergeableCommit *c, jz_str json);
bool jazz_node__node__value_needs_large_value_staging(const groove_Value *value);

/* ---------------- open_tx.rs ---------------- */
enum { JZN_OTX_KIND_EXCLUSIVE, JZN_OTX_KIND_MERGEABLE };
/* Exclusive { made_by, permission_subject } | Mergeable { made_by, permission_subject: Option } */
typedef struct jazz_node_OpenTransactionKind {
    u32 tag;
    jazz_types_AuthorSubject made_by;
    bool has_permission_subject; jazz_types_AuthorSubject permission_subject; /* always Some for Exclusive */
} jazz_node_OpenTransactionKind;
enum { JZN_PENDING_CELLS_REPLACE, JZN_PENDING_CELLS_PATCH };
typedef struct jazz_node_PendingCells { u32 tag; jz_btree cells; /* String -> Value */ } jazz_node_PendingCells;
/* Pending row write inside an open transaction. */
typedef struct jazz_node_PendingWrite {
    jz_str table;
    jazz_types_RowUuid row_uuid;
    jazz_types_SchemaVersionId schema_version;
    jazz_model_BranchSelector *branch;
    jazz_node_PendingCells cells;
    bool has_deletion; jazz_model_DeletionEvent deletion;
    bool has_now_ms; u64 now_ms;
    bool known_fresh_row;
    bool has_verified_inherited_cells; jz_btree verified_inherited_cells;
    jazz_model_BranchViewCopyEvidence *branch_view_copy; /* NULL = None */
    jz_btree staged_large_cells;                          /* String -> StagedLargeValue */
} jazz_node_PendingWrite;
JZ_NVEC(jazz_node_PendingWriteSlots, jazz_node_PendingWrite *); /* Vec<Option<PendingWrite>>: NULL = None */
typedef struct jazz_node_StagedWrites {
    jazz_node_PendingWriteSlots slots;
    jz_btree positions; /* String -> BTreeMap<RowUuid, Vec<usize>> */
    usize live;
} jazz_node_StagedWrites;
JZ_NVEC(jazz_node_OptValueVec, groove_Value *); /* Vec<Option<Value>> */
typedef struct jazz_node_SnapshotRow {
    bool has_content_cells; jazz_node_OptValueVec content_cells;
    bool has_content_version; jazz_model_TxId content_version;
    bool has_deletion_version; jazz_model_TxId deletion_version;
    bool has_read_version; jazz_model_TxId read_version;
    bool deleted;
    jazz_node_VersionRow *provenance_created, *provenance_updated; /* Option<(VersionRow, VersionRow)>: NULL = None */
} jazz_node_SnapshotRow;
JZ_NVEC(jazz_node_NarrowedSourceReadVec, jazz_node_NarrowedSourceRead);
typedef struct jazz_node_SourceNarrowing {
    bool recording;
    jz_btree offered; /* SourceId -> NarrowedSourceRead */
    jz_btree payload; /* set<SourceId> */
    jazz_node_NarrowedSourceReadVec claimed;
} jazz_node_SourceNarrowing;
JZ_NVEC(jazz_node_StagedLargeValueIdVec, groove_StagedLargeValueId);
typedef struct jazz_node_OpenTransaction {
    jazz_node_OpenTransactionKind kind;
    jazz_types_AuthorSubject provisional_author;
    jazz_model_Snapshot base_snapshot;
    jz_btree base_snapshot_rows; /* (String, RowUuid) -> BTreeMap<SchemaVersionId, SnapshotRow> */
    jazz_model_RowReadVec row_reads;
    jazz_model_AbsentReadVec absent_reads;
    jazz_model_PredicateReadVec predicate_reads;
    jazz_node_SourceNarrowing source_narrowing;
    jz_btree narrowed_predicate_reads; /* set<(ShapeId, BindingId)> */
    jazz_node_StagedWrites writes;
    jazz_node_StagedLargeValueIdVec superseded_large_values;
    bool has_user_metadata_json; jz_str user_metadata_json;
} jazz_node_OpenTransaction;
enum { JZN_TBRS_VISIBLE, JZN_TBRS_PENDING_DELETION, JZN_TBRS_ABSENT };
typedef struct jazz_node_TransactionBranchRowState { u32 tag; jz_btree cells; bool staged; } jazz_node_TransactionBranchRowState;
enum { JZN_TITS_ABSENT, JZN_TITS_DELETED };
void jazz_node__node__open_tx__OpenTransaction__drop(void *p);
void jazz_node__node__open_tx__SnapshotRow__drop(jazz_node_SnapshotRow *r);

/* state/commit.rs, ingest.rs (other units) */
typedef struct jazz_node_SchemaCommit { jazz_types_SchemaVersionId schema; jazz_node_MergeableCommit commit; } jazz_node_SchemaCommit;
JZ_NVEC(jazz_node_SchemaCommitVec, jazz_node_SchemaCommit);
jz_err jazz_node__node__NodeState__commit_mergeable_many_at_with_schema_versions_and_provenance(NS *self, jazz_node_SchemaCommitVec commits,
    jazz_types_TxTime made_at, jazz_model_ContributionMergeProvenance *contribution_merge /* moved; NULL = None */, jazz_node_PublishedTransaction *out);
jz_err jazz_node__node__ingest__NodeState__publish_pending_transaction_and_versions(NS *self, jazz_model_Transaction *tx, jazz_protocol_VersionRecordVec versions,
    jazz_model_DurabilityTier durability, jazz_node_PublishedTransaction *out);


/* ---------------- views.rs ---------------- */
/* MaintainedViewBundleInputs<'a> */
typedef struct jazz_node_MaintainedViewBundleInputs {
    jazz_protocol_SupportingRowsUpdate *supporting_update;  /* Option, moved */
    const jazz_model_ValidatedQuery *shape;
    bool has_default_read_view;
    bool allow_authoritative_scalar_exit_refresh;
    jazz_protocol_SubscriptionKey subscription;
    jazz_types_GlobalTime settled_through;
    jz_btree peer_complete_tx_payloads;                     /* set<TxId> */
    const jazz_protocol_KnownStateDeclaration *known_state; /* Option */
    bool has_leave_scan_after; jazz_types_GlobalTime leave_scan_after;
    bool complete_exclusive_payloads;
    jz_btree previous_result_set;                           /* set<TxId> */
    jazz_protocol_ResultMemberEntryVec result_member_adds;
    jazz_protocol_ResultMemberEntryVec result_member_removes;
    jazz_types_AuthorSubject identity;
    jazz_model_DurabilityTier tier;
    const jazz_node_MaintainedSubscriptionView *maintained_facts;
    bool allow_storage_witness_fallback;
} jazz_node_MaintainedViewBundleInputs;
bool jazz_node__node__views__simple_scalar_exit_query(const jazz_model_Query *query);
bool jazz_node__node__views__single_table_scalar_query(const jazz_model_Query *query);
jz_err jazz_node__node__views__NodeState__subscribe_history(NS *self, const jz_str *table, groove_Subscription **out);
jz_err jazz_node__node__views__NodeState__scope_physical_table(const NS *self, jazz_types_SchemaVersionId schema, const jz_str *table, jazz_types_GlobalPhysicalTableId *out);
jz_err jazz_node__node__views__NodeState__scope_logical_table(const NS *self, jazz_types_SchemaVersionId schema, jazz_types_GlobalPhysicalTableId physical, groove_InternStr *out);
jz_err jazz_node__node__views__NodeState__view_update_for_maintained_result_members(NS *self, jazz_node_MaintainedViewBundleInputs inputs, jazz_protocol_SyncMessage **out);
jz_err jazz_node__node__views__NodeState__apply_view_update(NS *self, jazz_node_ViewUpdateParts update);
jz_err jazz_node__node__views__NodeState__apply_view_updates_in_batch(NS *self, jazz_node_ViewUpdatePartsVec updates);
jz_err jazz_node__node__views__NodeState__remember_discarded_pending_view_transactions(NS *self, jazz_protocol_VersionCarrier *const *carriers, u32 n);
jz_err jazz_node__node__views__NodeState__ingest_unselected_authority_view_bundles(NS *self, jazz_protocol_VersionCarrier *const *carriers, u32 n);
jz_err jazz_node__node__views__NodeState__covered_input_version(NS *self, const jazz_protocol_SupportingRow *input, jazz_types_SchemaVersionId read_schema, jazz_node_VersionRow **out);
jz_err jazz_node__node__views__NodeState__whole_table_subscription_key(NS *self, const jz_str *table, jazz_protocol_SubscriptionKey *out);
jz_err jazz_node__node__views__NodeState__canonical_history_version_for_maintained_witness(NS *self, const jazz_node_VersionRow *version, jazz_node_VersionRow **out);
bool jazz_node__peer__read_policy_admits_every_row(const jazz_model_Query *policy);
jz_err jazz_node__node__query_eval__NodeState__query_rows_for_link_physical_row(NS *s, const jazz_model_ValidatedQuery *shape, const jazz_model_Binding *binding,
    jazz_model_DurabilityTier tier, jazz_types_AuthorSubject identity, jazz_types_RowUuid row_uuid, jazz_node_CurrentRowVec *out);

#endif
