/* ingest.h: jazz_node::node::ingest (ingest.rs + ingest/{..}.rs, one Rust module via include!).
 * node-core-a unit. */
#ifndef JZ_NODE_INGEST_H
#define JZ_NODE_INGEST_H
#include "jz_rt.h"
#include "../_ext_node-core-a.h"
#include "physical.h"
#include "codec.h"

/* ---- ingest.rs ---- */
#define JZN_PARKED_PREDECESSOR_WAIT_MS 60000ull
#define JZN_MAX_SCHEMA_LINEAGE_DECLARATIONS 4096u
#define JZN_MAX_SCHEMA_LINEAGE_NAME_BYTES 1024u
#define JZN_MAX_SCHEMA_LINEAGE_OPS 16384u

typedef struct jazz_node_CommitUnitParkMode {
    bool has_ingest_context; jazz_node_CommitUnitIngestContext ingest_context;
    u32 ingress_role; /* JZN_PARKED_* */
} jazz_node_CommitUnitParkMode;
jazz_node_CommitUnitParkMode jazz_node__node__ingest__CommitUnitParkMode__default(void);

jz_err jazz_node__node__ingest__authority_wall_clock_ms(u64 *out);
jz_err jazz_node__node__ingest__validate_received_fate_update_global_time_durability(bool has_gt, jazz_types_GlobalTime gt,
    bool has_durability, u32 durability);
jz_err jazz_node__node__ingest__validate_received_view_bundle_global_time_durability(bool has_gt, jazz_types_GlobalTime gt, u32 durability);
bool jazz_node__node__ingest__fate_update_durability_claim(const jazz_model_Fate *fate, u32 durability, u32 *out);
bool jazz_node__node__ingest__commit_unit_write_count_matches(const jazz_model_Transaction *tx, usize version_count);
typedef struct jazz_node_ViewVersionKey { jz_str table; jazz_model_BranchKey branch_key; jazz_types_RowUuid row_uuid; } jazz_node_ViewVersionKey;
void jazz_node__node__ingest__view_version_key_for_ingest(const jazz_protocol_VersionRecord *v, jazz_node_ViewVersionKey *out);
int jazz_node_ViewVersionKey_cmp(const void *a, const void *b, void *ctx);
void jazz_node_ViewVersionKey_drop(void *k);

/* FateUpdate message helper */
void jazz_node__node__ingest__fate_update_message(jazz_types_TxId tx_id, jazz_model_Fate fate, bool has_gt, jazz_types_GlobalTime gt,
    bool has_durability, u32 durability, jazz_protocol_SyncMessage *out);
/* PublicationOutcome::settled(vec![msg]) */
void jazz_node__node__ingest__settled_one(jazz_node_PublicationOutcome *out, jazz_protocol_SyncMessage msg);
void jazz_node__node__ingest__settled_empty(jazz_node_PublicationOutcome *out);
/* Fate::Rejected(RejectionReason::MalformedCommit(reason)) */
jazz_model_Fate jazz_node__node__ingest__malformed_commit_fate(jz_str reason);
jazz_model_Fate jazz_node__node__ingest__rejected_fate(u32 reason_tag);

/* container descriptors defined in ingest.c */
extern const jz_btree_desc jzn_desc_uuid_set_a;  /* set<Uuid> */
extern const jz_btree_desc jzn_desc_txid_set_a;  /* set<TxId> */

/* ---- ingest/catalogue.rs ---- */
enum { JZN_CATALOGUE_ACTIVATION_COLD_OPEN = 0, JZN_CATALOGUE_ACTIVATION_LIVE = 1 };
#define JZN_MAX_PENDING_LARGE_VALUE_UPLOADS 1024u
bool jazz_node__node__ingest__large_value_upload_is_rejected(const jz_err *error);
jz_err jazz_node__node__ingest__NodeState__apply_sync_message(NSA *s, jazz_protocol_SyncMessage message, jazz_node_PublicationOutcome *out);
jz_err jazz_node__node__ingest__NodeState__apply_trusted_catalogue_message(NSA *s, jazz_protocol_SyncMessage message, jazz_node_PublicationOutcome *out);
jz_err jazz_node__node__ingest__NodeState__apply_sync_message_with_ingest_context(NSA *s, jazz_protocol_SyncMessage message,
    const jazz_node_CommitUnitIngestContext *ingest_context, jazz_node_PublicationOutcome *out);
jz_err jazz_node__node__ingest__NodeState__recover_pending_schema_lineages(NSA *s);
jz_err jazz_node__node__ingest__NodeState__drain_pending_schema_lineages(NSA *s, jazz_node_PublicationOutcome *out);
jz_err jazz_node__node__ingest__NodeState__validate_migration_lens_between(const jazz_protocol_MigrationLens *lens,
    const jazz_model_SchemaVersion *source, const jazz_model_SchemaVersion *target);
jz_err jazz_node__node__ingest__NodeState__validate_lineage_table_partition(const jazz_model_JazzSchema *source, const jazz_model_JazzSchema *target,
    const jazz_protocol_MigrationLens *lens, const jzn_StrVec *new_tables, const jzn_StrVec *dropped_tables);
jz_err jazz_node__node__ingest__NodeState__validate_schema_lineage_publication_bounds(const jazz_protocol_SchemaLineagePublication *p);
jz_err jazz_node__node__ingest__NodeState__validate_schema_lineage_publication(const jazz_protocol_SchemaLineagePublication *p);
void jazz_node__node__ingest__NodeState__remove_staged_schema_lineage_from_memory(NSA *s, const jazz_node_StagedSchemaLineage *staged);

/* ---- ingest/commit_bundles.rs ---- */
typedef struct jazz_node_GlobalTimeTx { jazz_types_GlobalTime global_time; jazz_types_TxId tx_id; } jazz_node_GlobalTimeTx;
typedef JZ_VEC(jazz_node_GlobalTimeTx) jazz_node_GlobalTimeTxVec;
typedef struct jazz_node_RejectedTransaction jazz_node_RejectedTransaction;
typedef JZ_VEC(jazz_node_RejectedTransaction *) jazz_node_RejectedTransactionVec;
jz_err jazz_node__node__ingest__NodeState__ingest_commit_unit(NSA *s, jazz_model_Transaction tx, jazz_protocol_VersionRecordVec versions,
    u64 now_ms, jazz_node_PublicationOutcome *out);
jz_err jazz_node__node__ingest__NodeState__ingest_commit_unit_with_context(NSA *s, jazz_model_Transaction tx, jazz_protocol_VersionRecordVec versions,
    u64 now_ms, const jazz_node_CommitUnitIngestContext *ingest_context, jazz_node_PublicationOutcome *out);
jz_err jazz_node__node__ingest__NodeState__finalize_local_mergeable_commit(NSA *s, jazz_types_TxId tx_id, jazz_node_PublicationOutcome *out);
jz_err jazz_node__node__ingest__NodeState__finalize_local_exclusive_commit(NSA *s, jazz_model_Transaction tx, jazz_protocol_VersionRecordVec versions,
    jazz_model_Fate *fate_out, jazz_node_PublicationOutcome *out);
jz_err jazz_node__node__ingest__NodeState__ingest_relay_commit_unit(NSA *s, jazz_model_Transaction tx, jazz_protocol_VersionRecordVec versions);
jz_err jazz_node__node__ingest__NodeState__ingest_relay_commit_unit_with_encoder_trust(NSA *s, jazz_model_Transaction tx,
    jazz_protocol_VersionRecordVec versions, bool trusted_encoder);
jz_err jazz_node__node__ingest__NodeState__known_untrusted_commit_unit_matches(NSA *s, const jazz_model_Transaction *tx,
    const jazz_protocol_VersionRecordVec *versions, bool *out);
jz_err jazz_node__node__ingest__NodeState__ingest_relay_commit_unit_once(NSA *s, jazz_model_Transaction tx, jazz_protocol_VersionRecordVec versions);
jz_err jazz_node__node__ingest__NodeState__ingest_commit_unit_once(NSA *s, jazz_model_Transaction tx, jazz_protocol_VersionRecordVec versions,
    u64 now_ms, const jazz_node_CommitUnitIngestContext *ingest_context, jazz_node_PublicationOutcome *out);
jz_err jazz_node__node__ingest__NodeState__ingest_known_transaction(NSA *s, jazz_model_Transaction tx, jazz_protocol_VersionRecordVec versions,
    jazz_model_Fate fate, bool has_gt, jazz_types_GlobalTime gt, u32 durability);
jz_err jazz_node__node__ingest__NodeState__stage_known_transaction(NSA *s, groove_DatabaseBatch *batch, jazz_model_Transaction tx,
    jazz_protocol_VersionRecordVec versions, jazz_model_Fate fate, bool has_gt, jazz_types_GlobalTime gt, u32 durability,
    jazz_node_GlobalTimeTxVec *staged_global_times, jazz_node_VersionRowVec *staged_content_versions, jazz_node_RejectedTransactionVec *staged_rejections);
/* VersionBundleRef<'_> */
enum { JZP_BUNDLE_SCOPE_COMPLETE = 0, JZP_BUNDLE_SCOPE_VIEW_SCOPED = 1 };
typedef struct jazz_protocol_VersionBundleRef {
    const jazz_model_Transaction *tx;
    const jazz_protocol_VersionRecordVec *versions;
    const jazz_model_Fate *fate;
    bool has_global_time; jazz_types_GlobalTime global_time;
    u32 durability;
    u32 scope;
} jazz_protocol_VersionBundleRef;
jz_err jazz_node__node__ingest__NodeState__ingest_reset_view_bundle_refs_in_bulk(NSA *s, const jazz_protocol_VersionBundleRef *bundles, u32 n,
    const jz_btree *preflight_persisted_tx_ids /* NULL = None */, jz_btree *out /* set<TxId> */);
bool jazz_node__node__ingest__same_addressed_rows(const jazz_protocol_VersionRecordVec *stored, const jazz_protocol_VersionRecordVec *resent);

/* ---- ingest/validation.rs ---- */
jz_err jazz_node__node__ingest__NodeState__ingest_transaction_and_versions(NSA *s, jazz_model_Transaction tx, jazz_protocol_VersionRecordVec versions,
    jazz_model_Fate fate, bool has_gt, jazz_types_GlobalTime gt, u32 durability);
jz_err jazz_node__node__ingest__NodeState__ingest_transaction_fragment_without_current_indexes(NSA *s, jazz_model_Transaction tx,
    jazz_protocol_VersionRecordVec versions, jazz_model_Fate fate, bool has_gt, jazz_types_GlobalTime gt, u32 durability);
jz_err jazz_node__node__ingest__NodeState__ingest_view_scoped_transaction_with_current_indexes(NSA *s, jazz_model_Transaction tx,
    jazz_protocol_VersionRecordVec versions, jazz_model_Fate fate, bool has_gt, jazz_types_GlobalTime gt, u32 durability);
jz_err jazz_node__node__ingest__NodeState__stage_view_scoped_transaction_with_current_indexes(NSA *s, groove_DatabaseBatch *batch,
    jazz_model_Transaction tx, jazz_protocol_VersionRecordVec versions, jazz_model_Fate fate, bool has_gt, jazz_types_GlobalTime gt, u32 durability,
    jazz_node_GlobalTimeTxVec *staged_global_times, jazz_node_VersionRowVec *staged_content_versions, jazz_node_RejectedTransactionVec *staged_rejections);
jz_err jazz_node__node__ingest__NodeState__publish_pending_transaction_and_versions(NSA *s, jazz_model_Transaction tx,
    jazz_protocol_VersionRecordVec versions, u32 durability, jazz_node_PublishedTransaction *out);
jz_err jazz_node__node__ingest__NodeState__prepare_exact_history_version(NSA *s, jazz_types_NodeAlias alias, jazz_types_TxTime tx_time,
    jazz_types_GlobalTime seq, const jazz_protocol_VersionRecord *version, jazz_node_VersionRow *out);
jz_err jazz_node__node__ingest__NodeState__ingest_transaction_and_versions_with_current_indexes_in_batch(NSA *s, groove_DatabaseBatch *batch,
    jazz_model_Transaction tx, jazz_protocol_VersionRecordVec versions, jazz_node_VersionRowVec previously_stored, jazz_model_Fate fate,
    bool has_gt, jazz_types_GlobalTime gt, u32 durability, bool update_current_indexes, bool is_partial);
/* stage_*: returns (staged versions, fate, global_time, rejected payload) */
typedef struct jazz_node_StagedTransaction {
    jazz_node_VersionRowVec versions;
    jazz_model_Fate fate;
    bool has_global_time; jazz_types_GlobalTime global_time;
    jazz_node_RejectedTransaction *rejected; /* Option */
} jazz_node_StagedTransaction;
jz_err jazz_node__node__ingest__NodeState__stage_transaction_and_versions_with_current_indexes(NSA *s, groove_DatabaseBatch *batch,
    jazz_model_Transaction tx, jazz_protocol_VersionRecordVec versions, jazz_model_Fate fate, bool has_gt, jazz_types_GlobalTime gt,
    u32 durability, bool update_current_indexes, bool is_partial, jazz_node_VersionRowVec *staged_content_versions /* NULL = None */,
    jazz_node_StagedTransaction *out);
jz_err jazz_node__node__ingest__NodeState__finalize_staged_transaction_ingest(NSA *s, groove_DatabaseBatch *batch, jazz_types_TxId tx_id,
    const jazz_model_Fate *fate, bool has_gt, jazz_types_GlobalTime gt, jazz_node_GlobalTimeTxVec *staged_global_times,
    const jazz_node_VersionRowVec *staged_versions);
/* Option<String>: true + reason when malformed */
bool jazz_node__node__ingest__NodeState__malformed_authored_version_reason(const NSA *s, const jazz_protocol_VersionRecord *const *versions,
    u32 n, jz_str *reason);
bool jazz_node__node__ingest__NodeState__malformed_authored_branch_key_reason(const jazz_model_JazzSchema *schema, const jazz_model_TableSchema *table,
    const jazz_protocol_VersionRecord *version, jz_str *reason);
jz_err jazz_node__node__ingest__NodeState__validate_view_payload_versions(const NSA *s, const jazz_protocol_VersionRecordVec *versions);
jz_err jazz_node__node__ingest__NodeState__validate_view_payload_versions_prepared(const NSA *s, const jazz_protocol_VersionRecordVec *versions,
    jz_btree *descriptors /* (SchemaVersionId, table) -> RecordDescriptor */);
typedef struct jzn_SchemaTableKey { jazz_types_SchemaVersionId schema; const jz_str *table; /* borrowed */ } jzn_SchemaTableKey;
extern const jz_btree_desc jzn_desc_schema_table_descriptor;
jz_err jazz_node__node__ingest__NodeState__reject_malformed_commit(NSA *s, jazz_model_Transaction tx, jz_str reason, jazz_protocol_SyncMessageVec *out);
jz_err jazz_node__node__ingest__NodeState__prepare_authored_schema_variants_for_commit(NSA *s, const jazz_protocol_VersionRecord *const *versions, u32 n);
jz_err jazz_node__node__ingest__NodeState__ingest_rejected_transaction(NSA *s, jazz_model_Transaction tx, jazz_model_Fate fate);

/* ---- ingest/view_updates.rs ---- */
jz_err jazz_node__node__ingest__NodeState__merged_global_post_image(NSA *s, const groove_DatabaseBatch *batch, jazz_types_SchemaVersionId v,
    const jazz_model_TableSchema *table_schema, const jazz_node_VersionRow *incoming, jazz_types_TxId incoming_tx, jazz_types_GlobalTime gt,
    bool authority, bool *some, jazz_node_VersionRow *out);
jz_err jazz_node__node__ingest__NodeState__merge_op_rejection(NSA *s, jazz_types_TxId tx_id, const jazz_protocol_VersionRecordVec *versions,
    bool *some, jazz_model_RejectionReason *out);
typedef struct jzn_FieldValue { u32 field; groove_Value value; } jzn_FieldValue;
typedef JZ_VEC(jzn_FieldValue) jzn_FieldValueVec;
/* Result<Result<Vec<(usize, Value)>, String>, Error>: *ok=false + column on the inner Err */
jz_err jazz_node__node__ingest__NodeState__cross_schema_settled_merge_cells(NSA *s, jazz_types_SchemaVersionId v, const jazz_model_TableSchema *t,
    const jz_btree *authored /* NULL = None */, const jazz_node_VersionRow *previous, bool *ok, jzn_FieldValueVec *carried, jz_str *column);
jz_err jazz_node__node__ingest__NodeState__global_current_seq_in_batch(NSA *s, const groove_DatabaseBatch *batch, jazz_types_SchemaVersionId v,
    const jz_str *table, const jazz_model_BranchKey *branch_key, jazz_types_RowUuid row, bool *has, jazz_types_GlobalTime *out);
jz_err jazz_node__node__ingest__NodeState__global_current_seq_and_writer_in_batch(NSA *s, const groove_DatabaseBatch *batch,
    jazz_types_SchemaVersionId v, const jz_str *table, const jazz_model_BranchKey *branch_key, jazz_types_RowUuid row,
    bool *found, bool *has_seq, jazz_types_GlobalTime *seq, bool *has_writer, jazz_types_TxId *writer);
jz_err jazz_node__node__ingest__NodeState__write_history_post_image(NSA *s, groove_DatabaseBatch *batch, const jazz_node_VersionRow *v,
    jazz_types_AuthorSubject tx_author);
jz_err jazz_node__node__ingest__NodeState__write_global_current_update(NSA *s, groove_DatabaseBatch *batch, const jazz_node_VersionRow *v,
    jazz_types_GlobalTime gt);
jz_err jazz_node__node__ingest__NodeState__write_ahead_current_insert(NSA *s, groove_DatabaseBatch *batch, const jazz_node_VersionRow *v);
jz_err jazz_node__node__ingest__NodeState__write_ahead_current_delete(NSA *s, groove_DatabaseBatch *batch, const jazz_node_VersionRow *v, bool *out);
jz_err jazz_node__node__ingest__NodeState__restore_ahead_overlay_after_reject(NSA *s, groove_DatabaseBatch *batch, const jazz_node_VersionRow *rejected);
jz_err jazz_node__node__ingest__NodeState__recompute_ahead_overlay(NSA *s, groove_DatabaseBatch *batch, const jazz_node_VersionRow *row,
    const jz_btree *settling /* set<TxId> */);
jz_err jazz_node__node__ingest__NodeState__rebase_ahead_overlays(NSA *s, groove_DatabaseBatch *batch, const jazz_node_VersionRowVec *synced,
    const jz_btree *settling);
typedef struct jzn_AheadOverlayKey { jazz_types_PhysicalTableId table; jz_bytes key; } jzn_AheadOverlayKey;
jz_err jazz_node__node__ingest__NodeState__ahead_overlay_key(const NSA *s, const jazz_node_VersionRow *v, jzn_AheadOverlayKey *out);
jz_err jazz_node__node__ingest__NodeState__has_foreign_ahead_overlay(const NSA *s, const jazz_node_VersionRow *v, jazz_types_TxId tx_id, bool *out);
jz_err jazz_node__node__ingest__NodeState__cleanup_fated_ahead_current_for_versions(NSA *s, groove_DatabaseBatch *batch,
    const jazz_node_VersionRowVec *versions);
jz_err jazz_node__node__ingest__NodeState__prune_ahead_current_for_global_time(NSA *s, groove_DatabaseBatch *batch, jazz_types_GlobalTime gt);
/* AheadCurrentKeys (HashMap<(PhysicalTableId, Vec<u8>), TxId>): mod.rs, owned by node-core-b (mirrored) */
const jazz_types_TxId *jazz_node__node__AheadCurrentKeys__get(const jazz_node_AheadCurrentKeys *k, const jzn_AheadOverlayKey *key);
bool jazz_node__node__AheadCurrentKeys__contains_key(const jazz_node_AheadCurrentKeys *k, const jzn_AheadOverlayKey *key);
bool jazz_node__node__AheadCurrentKeys__insert(jazz_node_AheadCurrentKeys *k, jzn_AheadOverlayKey key, jazz_types_TxId tx, jazz_types_TxId *prev);
bool jazz_node__node__AheadCurrentKeys__remove(jazz_node_AheadCurrentKeys *k, const jzn_AheadOverlayKey *key, jazz_types_TxId *removed);

/* ---- ingest/write_base.rs ---- */
/* PhysicalCells = BTreeMap<PhysicalColumnId, (ValueType, Option<Value>)> */
typedef struct jzn_PhysicalCell { groove_ValueType column_type; bool has_value; groove_Value value; } jzn_PhysicalCell;
extern const jz_btree_desc jzn_desc_physical_cells;
groove_ValueType jazz_node__node__ingest__deletion_cell_type(void);
const char *jazz_node__node__ingest__pending_predecessor_refusal(jazz_types_TxId predecessor, jazz_types_TxId writer);
/* Result<Result<(), String>, Error>: *refusal set (len>0 / has) on the inner Err */
jz_err jazz_node__node__ingest__NodeState__validate_write_base(NSA *s, const groove_DatabaseBatch *batch, jazz_types_SchemaVersionId v,
    const jz_str *table, const jazz_model_BranchKey *branch_key, jazz_types_RowUuid row, jazz_protocol_RowBase base, jazz_types_TxId writer,
    bool has_current_seq, jazz_types_GlobalTime current_seq, bool *refused, jz_str *reason);
jz_err jazz_node__node__ingest__NodeState__late_write_refusal(NSA *s, const groove_DatabaseBatch *batch, jazz_types_SchemaVersionId v,
    const jz_str *table, const jazz_model_BranchKey *branch_key, jazz_types_RowUuid row, jazz_protocol_RowBase base, jazz_types_TxId writer,
    bool has_current_seq, jazz_types_GlobalTime current_seq, bool has_current_writer, jazz_types_TxId current_writer, bool *refused, jz_str *reason);
jz_err jazz_node__node__ingest__NodeState__passed_over_predecessor(NSA *s, jazz_protocol_RowBase base, jazz_types_TxId writer,
    bool has_current_writer, jazz_types_TxId current_writer, bool *some, jazz_types_TxTime *out);
typedef struct jzn_SupersededPredecessor {
    jazz_types_SchemaVersionId schema_version; jz_str table; jazz_model_BranchKey branch_key; jazz_types_RowUuid row_uuid; jazz_types_TxId pending;
} jzn_SupersededPredecessor;
typedef JZ_VEC(jzn_SupersededPredecessor) jzn_SupersededPredecessorVec;
jz_err jazz_node__node__ingest__NodeState__record_superseded_by_refusal(NSA *s, jazz_types_TxId writer, jzn_SupersededPredecessorVec predecessors);
jz_err jazz_node__node__ingest__NodeState__record_superseded_writes(NSA *s, jazz_types_PhysicalTableId table, const jazz_model_BranchKey *branch_key,
    jazz_types_RowUuid row, jazz_types_NodeUuid node, jazz_types_TxTime through);
jz_err jazz_node__node__ingest__NodeState__newest_own_pending_write_in_batch(NSA *s, const groove_DatabaseBatch *batch, const jz_str *table,
    const jazz_model_BranchKey *branch_key, jazz_types_RowUuid row, bool *some, jazz_types_TxId *out);
jz_err jazz_node__node__ingest__NodeState__image_physical_cells(const NSA *s, const jazz_node_VersionRow *image, jz_btree *out);

/* ---- ingest/fates.rs ---- */
jz_err jazz_node__node__ingest__NodeState__validate_exclusive_commit_unit(NSA *s, const jazz_model_Transaction *tx,
    const jazz_protocol_VersionRecordVec *versions, bool *out);
jz_err jazz_node__node__ingest__NodeState__predicate_read_is_degenerate_whole_table(const NSA *s, const jazz_model_PredicateRead *p, bool *out);
jz_err jazz_node__node__ingest__NodeState__shape_predicate_outputs_differ(NSA *s, const jazz_model_PredicateRead *p, const jazz_model_Snapshot *snapshot,
    const jazz_model_Snapshot *comparison /* NULL = None */, bool *out);
jz_err jazz_node__node__ingest__NodeState__commit_unit_write_policy_rejection(NSA *s, const jazz_model_Transaction *tx,
    const jazz_protocol_VersionRecordVec *versions, const jazz_node_CommitUnitIngestContext *ctx, bool *some, jazz_model_RejectionReason *out);
jz_err jazz_node__node__ingest__NodeState__commit_unit_satisfies_write_policy(NSA *s, const jazz_protocol_VersionRecordVec *versions,
    jazz_types_AuthorSubject author, jazz_types_TxId candidate, bool *out);
jz_err jazz_node__node__ingest__NodeState__park_commit_unit_if_missing_schema_versions_with_mode(NSA *s, const jazz_model_Transaction *tx,
    const jazz_protocol_VersionRecordVec *versions, u64 now_ms, jazz_node_CommitUnitParkMode mode, bool *out);
jz_err jazz_node__node__ingest__NodeState__remove_rejected_local_versions(NSA *s, jazz_types_TxId tx_id, const jazz_node_StoredTransaction *tx,
    groove_DatabaseBatch *batch, jazz_node_RejectedTransaction **out /* NULL = None */);
/* Result<Option<AuthorSubject>, RejectionReason>: returns false + reason on Err */
bool jazz_node__node__ingest__commit_unit_admission_subject(const jazz_model_Transaction *tx, const jazz_node_CommitUnitIngestContext *ctx,
    bool *some, jazz_types_AuthorSubject *subject, jazz_model_RejectionReason *reason);
extern const jz_btree_desc jzn_desc_txid_parked; /* TxId -> ParkedCommitUnit */
jz_err jazz_node__node__ingest__NodeState__apply_fate_update(NSA *s, jazz_types_TxId tx_id, jazz_model_Fate fate, bool has_gt,
    jazz_types_GlobalTime gt, bool has_durability, u32 durability);
jz_err jazz_node__node__ingest__NodeState__drain_parked_commit_units(NSA *s, jazz_node_PublicationOutcome *out);
jz_err jazz_node__node__ingest__NodeState__drain_parked_relay_commit_units(NSA *s);

#endif
