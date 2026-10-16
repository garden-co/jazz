/* codec.h: jazz_node::node::codec (node-core-a unit). */
#ifndef JZ_NODE_CODEC_H
#define JZ_NODE_CODEC_H
#include "jz_rt.h"
#include "../_ext_node-core-a.h"
#include "physical.h"

/* ---- groove::define_record! layouts ---- */
#define JZN_HISTORY_FIELD_BRANCH_KEY_IDX 0u
#define JZN_HISTORY_FIELD_ROW_UUID_IDX 1u
#define JZN_HISTORY_FIELD_TX_TIME_IDX 2u
#define JZN_HISTORY_FIELD_TX_NODE_ID_IDX 3u
#define JZN_HISTORY_FIELD_SCHEMA_VERSION_IDX 4u
#define JZN_HISTORY_FIELD_CREATED_BY_IDX 5u
#define JZN_HISTORY_FIELD_CREATED_AT_IDX 6u
#define JZN_HISTORY_FIELD_UPDATED_BY_IDX 7u
#define JZN_HISTORY_FIELD_UPDATED_AT_IDX 8u
#define JZN_HISTORY_FIELD__DELETION_IDX 9u
#define JZN_HISTORY_USER_CELLS 10u
extern const char *const jzn_HistoryRowRecord_PREFIX_FIELD_NAMES[10];

#define JZN_GCUR_FIELD_BRANCH_KEY_IDX 0u
#define JZN_GCUR_FIELD_ROW_UUID_IDX 1u
#define JZN_GCUR_FIELD_TX_TIME_IDX 2u
#define JZN_GCUR_FIELD_TX_NODE_ID_IDX 3u
#define JZN_GCUR_FIELD_SCHEMA_VERSION_IDX 4u
#define JZN_GCUR_FIELD_CREATED_BY_IDX 5u
#define JZN_GCUR_FIELD_CREATED_AT_IDX 6u
#define JZN_GCUR_FIELD_UPDATED_BY_IDX 7u
#define JZN_GCUR_FIELD_UPDATED_AT_IDX 8u
#define JZN_GCUR_FIELD_GLOBAL_TIME_IDX 9u
#define JZN_GCUR_FIELD__DELETION_IDX 10u
#define JZN_GCUR_USER_CELLS 11u
extern const char *const jzn_GlobalCurrentRowRecord_PREFIX_FIELD_NAMES[11];

#define JZN_CURROW_USER_CELLS 1u
#define JZN_WIRE_FIELD_ROW_UUID_IDX 0u
#define JZN_WIRE_FIELD_CREATED_BY_IDX 1u
#define JZN_WIRE_FIELD_CREATED_AT_IDX 2u
#define JZN_WIRE_FIELD_UPDATED_BY_IDX 3u
#define JZN_WIRE_FIELD_UPDATED_AT_IDX 4u
#define JZN_WIRE_FIELD__DELETION_IDX 5u
#define JZN_WIRE_USER_CELLS 6u

#define JZN_REJVER_USER_CELLS 4u
extern const char *const jzn_RejectedVersionRowRecord_PREFIX_FIELD_NAMES[4];

#define JZN_DELETION_COLUMN_ID ((jazz_types_PhysicalColumnId)UINT64_MAX)
#define JZN_DELETION_COLUMN_NAME "_deletion"

/* ---- VersionRow ---- */
struct jazz_node_VersionRow {
    groove_InternStr table;
    jazz_model_BranchKey branch_key;
    groove_OwnedRecord record;
};
void jazz_node_VersionRow_clone(void *dst, const void *src);
void jazz_node_VersionRow_drop(void *v);

/* DeletionEvent */
enum { JZN_DELETION_NONE = 0, JZN_DELETION_DELETED = 1, JZN_DELETION_RESTORED = 2 };

jazz_types_SchemaVersionAlias jazz_node__node__codec__VersionRow__schema_version_alias(const jazz_node_VersionRow *v);
static inline const jz_str *jazz_node__node__codec__VersionRow__table(const jazz_node_VersionRow *v) { return v->table; }
jazz_types_TxTime jazz_node__node__codec__VersionRow__created_at(const jazz_node_VersionRow *v);
jazz_types_TxTime jazz_node__node__codec__VersionRow__updated_at(const jazz_node_VersionRow *v);
jazz_types_AuthorSubject jazz_node__node__codec__VersionRow__updated_by(const jazz_node_VersionRow *v);
jz_err jazz_node__node__codec__VersionRow__authored_column_ids(const jazz_node_VersionRow *v, bool *some, jz_btree *out);

jz_err jazz_node__node__codec__row_author_value(jazz_types_AuthorSubject a, groove_Value *out);
jz_err jazz_node__node__codec__history_updated_by_value(jazz_types_AuthorSubject a, groove_Value *out);
typedef groove_PrimaryKeyValue *jazz_node_PrimaryKeyValue;
jz_err jazz_node__node__codec__history_primary_key(const jazz_node_VersionRow *v, jazz_node_PrimaryKeyValue *out);

/* ---- TouchedRows: BTreeMap<(PhysicalTableId, Vec<u8>), BTreeSet<RowUuid>> ---- */
#define JZN_TX_TOUCHED_ROWS_TABLE "jazz_tx_touched_rows"
typedef struct jazz_node_TouchedRowsKey { jazz_types_PhysicalTableId table; jz_bytes branch_key; } jazz_node_TouchedRowsKey;
typedef struct jazz_node_TouchedRows { jz_btree map; } jazz_node_TouchedRows;
void jazz_node_TouchedRows_default(jazz_node_TouchedRows *t);
void jazz_node_TouchedRows_drop(void *t);
void jazz_node__node__codec__TouchedRows__insert(jazz_node_TouchedRows *t, jazz_types_PhysicalTableId table, jz_bytes branch_key, jazz_types_RowUuid row);
void jazz_node__node__codec__TouchedRows__extend(jazz_node_TouchedRows *t, const jazz_node_TouchedRows *other);
typedef bool (*jzn_touched_row_fn)(void *ctx, jazz_types_PhysicalTableId table, jz_slice branch_key, jazz_types_RowUuid row);
void jazz_node__node__codec__TouchedRows__iter(const jazz_node_TouchedRows *t, jzn_touched_row_fn f, void *ctx);
jz_err jazz_node__node__codec__TouchedRows__to_value(const jazz_node_TouchedRows *t, groove_Value *out);
jz_err jazz_node__node__codec__TouchedRows__from_list_record(const groove_BorrowedRecord *r, jazz_node_TouchedRows *out);
jz_err jazz_node__node__codec__TouchedRows__list_record_values(const jazz_node_TouchedRows *t, jazz_types_TxTime tx_time,
    jazz_types_NodeAlias alias, groove_ValueVec *out);

/* ---- CatalogueRecordKind ---- */
enum {
    JZN_CRK_GENESIS = 0, JZN_CRK_SCHEMA, JZN_CRK_LENS, JZN_CRK_SCHEMA_LINEAGE_STAGED, JZN_CRK_SCHEMA_LINEAGE_PENDING,
    JZN_CRK_SCHEMA_LINEAGE_ACTIVE, JZN_CRK_WRITE_POINTER_PENDING, JZN_CRK_BOOTSTRAP_READY, JZN_CRK_ACTIVE_SCHEMA
};
u64 jazz_node__node__codec__CatalogueRecordKind__key(u32 kind);
jz_err jazz_node__node__codec__CatalogueRecordKind__from_key(u64 key, u32 *out);
jz_err jazz_node__node__codec__CatalogueRecordKind__read(const groove_BorrowedRecord *r, u32 idx, u32 *out);
groove_Value jazz_node__node__codec__CatalogueRecordKind__to_value(u32 kind);

/* ---- catalogue kernel payloads (types owned by node/mod.rs, mirrored here) ---- */
typedef struct jazz_node_StagedSchemaLineage {
    u64 catalogue_seq;
    jazz_protocol_SchemaLineagePublication publication;
    jazz_types_SchemaVersionAlias alias;
    jazz_node_SchemaPhysicalMapping mapping;
} jazz_node_StagedSchemaLineage;
typedef struct jazz_node_PendingSchemaLineage { u64 catalogue_seq; jazz_protocol_SchemaLineagePublication publication; } jazz_node_PendingSchemaLineage;
typedef struct jazz_node_SchemaLineageActivation { jz_uuid id; u64 catalogue_seq; } jazz_node_SchemaLineageActivation;
typedef struct jazz_node_CatalogueBootstrapReady {
    jazz_types_SchemaVersionId genesis;
    jazz_protocol_CurrentWriteSchema current_write_schema;
    u64 active_catalogue_seq;
} jazz_node_CatalogueBootstrapReady;

/* ActiveSchema::new (node/mod.rs); `compiled` is moved */
jz_err jazz_node__node__ActiveSchema__new(jazz_protocol_CurrentWriteSchema pointer, jazz_model_JazzSchema compiled, jazz_node_ActiveSchema *out);
jz_err jazz_node__node__codec__encode_physical_mapping(const jazz_node_SchemaPhysicalMapping *m, jz_bytes *out);
jz_err jazz_node__node__codec__decode_physical_mapping(jz_slice payload, jazz_node_SchemaPhysicalMapping *out);
jz_err jazz_node__node__codec__encode_catalogue_schema(const jazz_model_SchemaVersion *s, jz_bytes *out);
jz_err jazz_node__node__codec__decode_catalogue_schema(jz_slice payload, jazz_model_SchemaVersion *out);
void jazz_node__node__codec__encode_catalogue_bootstrap_ready(const jazz_node_CatalogueBootstrapReady *r, jz_bytes *out);
jz_err jazz_node__node__codec__decode_catalogue_bootstrap_ready(jz_slice payload, jazz_node_CatalogueBootstrapReady *out);
jz_uuid jazz_node__node__codec__catalogue_write_pointer_id(jazz_protocol_CurrentWriteSchema p);
jz_err jazz_node__node__codec__decode_catalogue_write_pointer(jz_slice payload, jazz_protocol_CurrentWriteSchema *out);
void jazz_node__node__codec__encode_catalogue_lineage_activation(jazz_node_SchemaLineageActivation a, jz_bytes *out);
jz_err jazz_node__node__codec__decode_catalogue_lineage_activation(jz_slice payload, jazz_node_SchemaLineageActivation *out);
void jazz_node__node__codec__encode_catalogue_lens(const jazz_protocol_MigrationLens *l, jz_bytes *out);
jz_err jazz_node__node__codec__decode_catalogue_lens(jz_slice payload, jazz_protocol_MigrationLens *out);
jz_err jazz_node__node__codec__encode_catalogue_staged_lineage(const jazz_node_StagedSchemaLineage *s, jz_bytes *out);
jz_err jazz_node__node__codec__decode_catalogue_staged_lineage(jz_slice payload, jazz_node_StagedSchemaLineage *out);
jz_err jazz_node__node__codec__encode_catalogue_pending_lineage(const jazz_node_PendingSchemaLineage *p, jz_bytes *out);
jz_err jazz_node__node__codec__decode_catalogue_pending_lineage(jz_slice payload, jazz_node_PendingSchemaLineage *out);
jz_err jazz_node__node__codec__encode_active_schema(const jazz_node_ActiveSchema *a, jz_bytes *out);
jz_err jazz_node__node__codec__decode_active_schema(jz_slice bytes, jazz_node_ActiveSchema *out);

/* ---- StoredTransaction / VersionRowParts ---- */
typedef struct jazz_node_StoredTransaction {
    jazz_model_Transaction tx;
    jazz_types_NodeAlias node_alias;
    jazz_model_Fate fate;
    bool has_global_time; jazz_types_GlobalTime global_time;
    u32 durability;
    bool is_partial;
} jazz_node_StoredTransaction;
jz_err jazz_node__node__codec__StoredTransaction__reconcile_fate(jazz_node_StoredTransaction *t, jazz_model_Fate fate,
    bool has_global_time, jazz_types_GlobalTime global_time, bool has_durability, u32 durability);
void jazz_node__node__codec__StoredTransaction__to_record(const jazz_node_StoredTransaction *t, jazz_model_TransactionRecord *out);
void jazz_node__node__codec__StoredTransaction__clone(jazz_node_StoredTransaction *dst, const jazz_node_StoredTransaction *src);
void jazz_node__node__codec__StoredTransaction__drop(jazz_node_StoredTransaction *t);
typedef struct jazz_node_VersionRowParts {
    jz_str table;
    jazz_model_BranchKey branch_key;
    jazz_types_RowUuid row_uuid;
    jazz_types_NodeAlias tx_node_alias;
    jazz_types_SchemaVersionAlias schema_version_alias;
    jazz_types_TxTime tx_time;
    jazz_types_AuthorSubject created_by;
    jazz_types_TxTime created_at;
    jazz_types_AuthorSubject updated_by;
    jazz_types_TxTime updated_at;
    jz_btree cells;              /* String -> Value */
    jz_btree *authored_columns;  /* Option<BTreeSet<PhysicalColumnId>> */
    u32 deletion;
    jazz_types_GlobalTime seq;
    jazz_protocol_RowBase base;
    jz_bytes counter_signs;
} jazz_node_VersionRowParts;
enum { JZN_PARKED_RELAY = 0, JZN_PARKED_AUTHORITY = 1 };
/* ParkedCommitUnit (codec.rs) */
typedef struct jazz_node_ParkedCommitUnit {
    jazz_model_Transaction tx;
    jazz_protocol_VersionRecordVec versions;
    u64 now_ms;
    bool has_ingest_context; jazz_node_CommitUnitIngestContext ingest_context;
    u32 ingress_role;
} jazz_node_ParkedCommitUnit;

jz_err jazz_node__node__codec__VersionRecordFromNode__from_commit(const jazz_node_MergeableCommit *c, const jazz_model_TableSchema *t,
    jazz_types_SchemaVersionId v, jazz_protocol_VersionRecord **out);
jz_err jazz_node__node__codec__VersionRecordFromNode__from_stored(const jazz_node_VersionRow *stored, const jazz_model_TableSchema *t,
    jazz_types_SchemaVersionId v, jz_btree *authored_columns /* NULL = None; moved */, jazz_protocol_VersionRecord **out);
void jazz_node__node__codec__debug_assert_lowered_layouts(const jazz_model_JazzSchema *s);
groove_RecordDescriptor jazz_node__node__codec__prepared_wire_record_descriptor(const jazz_model_TableSchema *t);
groove_RecordDescriptor jazz_node__node__codec__history_record_descriptor(const jazz_model_TableSchema *t);
jz_err jazz_node__node__codec__VersionRow__with_record_values(const jazz_node_VersionRow *v, groove_ValueVec values, jazz_node_VersionRow *out);
jz_err jazz_node__node__codec__VersionRow__from_parts_with_schema_version(const jazz_model_TableSchema *t, jazz_node_VersionRowParts parts,
    const groove_RecordDescriptor *history_descriptor /* NULL = None */, jazz_node_VersionRow *out);
jz_err jazz_node__node__codec__VersionRow__from_wire_with_schema_version(const jazz_model_TableSchema *t, const jazz_protocol_VersionRecord *version,
    const jz_btree *authored_columns, jazz_types_NodeAlias tx_node_alias, jazz_types_SchemaVersionAlias alias, jazz_types_TxTime tx_time,
    jazz_types_GlobalTime seq, jazz_node_VersionRow *out);
const jazz_model_BranchKey *jazz_node__node__codec__VersionRow__branch_key(const jazz_node_VersionRow *v);
jazz_types_RowUuid jazz_node__node__codec__VersionRow__row_uuid(const jazz_node_VersionRow *v);
jazz_types_NodeAlias jazz_node__node__codec__VersionRow__tx_node_alias(const jazz_node_VersionRow *v);
jazz_types_TxTime jazz_node__node__codec__VersionRow__tx_time(const jazz_node_VersionRow *v);
jz_err jazz_node__node__codec__VersionRow__validate_canonical(const jazz_node_VersionRow *v);
jazz_types_AuthorSubject jazz_node__node__codec__VersionRow__created_by(const jazz_node_VersionRow *v);
jz_err jazz_node__node__codec__VersionRow__updated_by_is_implicit(const jazz_node_VersionRow *v, bool *out);
u32 jazz_node__node__codec__VersionRow__deletion(const jazz_node_VersionRow *v);
bool jazz_node__node__codec__VersionRow__is_deleted(const jazz_node_VersionRow *v);
jz_err jazz_node__node__codec__VersionRow__cells(const jazz_node_VersionRow *v, const jazz_model_TableSchema *t, jz_btree *out);
jz_err jazz_node__node__codec__VersionRow__cell(const jazz_node_VersionRow *v, const jazz_model_TableSchema *t, const jz_str *column, bool *some, groove_Value *out);
jz_err jazz_node__node__codec__VersionRow__counter_signs(const jazz_node_VersionRow *v, jz_bytes *out);
jz_err jazz_node__node__codec__VersionRow__seq(const jazz_node_VersionRow *v, jazz_types_GlobalTime *out);
jz_err jazz_node__node__codec__VersionRow__base(const jazz_node_VersionRow *v, jazz_protocol_RowBase *out);
jz_err jazz_node__node__codec__VersionRow__with_merge_fields(const jazz_node_VersionRow *v, jazz_types_GlobalTime seq, jazz_protocol_RowBase base, jazz_node_VersionRow *out);
jz_err jazz_node__node__codec__VersionRow__with_seq(const jazz_node_VersionRow *v, jazz_types_GlobalTime seq, jazz_node_VersionRow *out);
jazz_model_HistoryEntry *jazz_node__node__codec__VersionRow__to_history_entry(const jazz_node_VersionRow *v, const jazz_node_StoredTransaction *tx,
    bool is_locally_current, bool is_globally_current);
jz_err jazz_node__node__codec__owned_record_from_storage_values(const groove_TableSchema *t, groove_ValueVec values, groove_OwnedRecord *out);
jz_err jazz_node__node__codec__owned_record_from_storage_values_with_descriptor(groove_RecordDescriptor d, groove_ValueVec values, groove_OwnedRecord *out);
u32 jazz_node__node__codec__ParkedIngressRole__strongest(u32 a, u32 b);
bool jazz_node__node__codec__current_version_index(const jazz_node_VersionRow *versions, const u32 *candidates, u32 n,
    const jazz_node_NodeAliases *aliases, u32 *out);
bool jazz_node__node__codec__version_tx_id_from_aliases(const jazz_node_VersionRow *v, const jazz_node_NodeAliases *aliases, jazz_types_TxId *out);
jz_err jazz_node__node__codec__runtime_result_identity_bytes(const groove_Value *value, const groove_ValueType *t, jz_bytes *out);
jz_err jazz_node__node__codec__transaction_values(jazz_types_NodeAlias alias, const jazz_model_Transaction *tx, const jazz_model_Fate *fate,
    bool has_global_time, jazz_types_GlobalTime global_time, u32 durability, groove_Value contribution_merge, groove_ValueVec *out);
jz_err jazz_node__node__codec__transaction_values_with_partial_flag(jazz_types_NodeAlias alias, const jazz_model_Transaction *tx, const jazz_model_Fate *fate,
    bool has_global_time, jazz_types_GlobalTime global_time, u32 durability, bool is_partial, groove_Value contribution_merge, groove_ValueVec *out);
jz_err jazz_node__node__codec__rejected_transaction_values(jazz_types_NodeAlias alias, const jazz_model_Transaction *tx, const jazz_model_RejectionReason *reason, groove_ValueVec *out);
jz_err jazz_node__node__codec__rejected_version_values(const jazz_model_TableSchema *t, const jazz_node_VersionRow *v, groove_ValueVec *out);
jz_str jazz_node__node__codec__fate_string(const jazz_model_Fate *f);
const char *jazz_node__node__codec__durability_string(u32 d);
jz_err jazz_node__node__codec__next_fate(const jazz_model_Fate *current, jazz_model_Fate incoming, jazz_model_Fate *out);
bool jazz_node__node__codec__known_transaction_payload_matches(const jazz_model_Transaction *a, const jazz_model_Transaction *b);
void jazz_node__node__codec__transaction_without_permission_subject(const jazz_model_Transaction *tx, jazz_model_Transaction *out);
bool jazz_node__node__codec__known_transaction_payload_matches_redacted_permission_subject(const jazz_model_Transaction *a, const jazz_model_Transaction *b);
bool jazz_node__node__codec__known_transaction_payload_matches_redacted_cardinality(const jazz_model_Transaction *a, const jazz_model_Transaction *b);
void jazz_node__node__codec__canonical_versions(jazz_protocol_VersionRecordVec *versions);
jz_err jazz_node__node__codec__authored_column_ids_from_value(groove_Value value, jz_btree *out);
jz_err jazz_node__node__codec__history_values_from_parts(const jazz_model_TableSchema *t, const jazz_node_VersionRowParts *p, groove_ValueVec *out);
groove_Value jazz_node__node__codec__base_seq_value(jazz_protocol_RowBase base);
groove_Value jazz_node__node__codec__base_pending_value(jazz_protocol_RowBase base);
groove_Value jazz_node__node__codec__nullable_deletion_value(u32 deletion);
groove_Value jazz_node__node__codec__deletion_event_value(u32 deletion);
jazz_node_PrimaryKeyValue jazz_node__node__codec__history_primary_key_at(const jazz_model_BranchKey *k, jazz_types_RowUuid row,
    jazz_types_GlobalTime seq, jazz_types_TxTime tx_time, jazz_types_NodeAlias alias);
void jazz_node__node__codec__history_seq_candidates(bool has_global_time, jazz_types_GlobalTime global_time, jzn_U64Vec *out);
void jazz_node__node__codec__history_key_values(jz_slice branch_key, jazz_types_RowUuid row, jazz_types_GlobalTime seq,
    jazz_types_TxTime tx_time, jazz_types_NodeAlias alias, groove_ValueVec *out);
jazz_node_PrimaryKeyValue jazz_node__node__codec__global_current_primary_key(const jazz_model_BranchKey *k, jazz_types_RowUuid row);
void jazz_node__node__codec__global_current_branch_key_prefix(const jazz_model_BranchKey *k, jz_bytes *out);
jz_slice jazz_node__node__codec__global_current_key_branch_prefix(jz_slice key);
jazz_node_PrimaryKeyValue jazz_node__node__codec__rejected_transaction_primary_key(jazz_types_NodeAlias alias, jazz_types_TxId tx_id);
jz_err jazz_node__node__codec__rejected_version_primary_key_from_record(const groove_BorrowedRecord *r, jazz_node_PrimaryKeyValue *out);
groove_PredicateExpr *jazz_node__node__codec__not_deleted_predicate(void);
groove_GraphBuilder *jazz_node__node__codec__visible_current_graph(const jazz_model_TableSchema *t, u32 settled);
void jazz_node__node__codec__decode_current_row(const jazz_model_TableSchema *t, const groove_BorrowedRecord *r, jazz_model_CurrentRow *out);
void jazz_node__node__codec__sort_current_rows(jazz_model_CurrentRow *rows, u32 n);
jz_err jazz_node__node__codec__current_row_from_version_projection(const jazz_model_TableSchema *t, const jazz_node_VersionRow *v, jazz_model_CurrentRow *out);
jz_err jazz_node__node__codec__current_row_from_materialized_cells(const jazz_model_TableSchema *t, const jazz_node_VersionRow *v, const jz_btree *cells, jazz_model_CurrentRow *out);
jz_err jazz_node__node__codec__current_row_from_materialized_cells_with_provenance(const jazz_model_TableSchema *t, const jazz_node_VersionRow *content,
    const jazz_node_VersionRow *provenance, const jz_btree *cells, jazz_model_CurrentRow *out);
jz_err jazz_node__node__codec__current_row_from_materialized_cells_with_layer_provenance(const jazz_model_TableSchema *t, const jazz_node_VersionRow *content,
    const jazz_node_VersionRow *created, const jazz_node_VersionRow *updated, const jz_btree *cells, jazz_model_CurrentRow *out);
jz_err jazz_node__node__codec__current_row_from_cells_with_explicit_provenance(const jazz_model_TableSchema *t, jazz_types_RowUuid row,
    const jz_btree *cells, jazz_model_RowProvenance provenance, bool has_projected, jazz_types_TxTime tx_time, jazz_types_NodeAlias alias, jazz_model_CurrentRow *out);
groove_ValueType jazz_node__node__codec__current_row_column_type(const jazz_model_ColumnSchema *c);
jz_err jazz_node__node__codec__current_row_from_positional_cells(const jazz_model_TableSchema *t, jazz_types_RowUuid row,
    const groove_Value *const *cells, u32 n, jazz_model_CurrentRow *out);
jz_err jazz_node__node__codec__positional_cells_from_map(const jazz_model_TableSchema *t, const jz_btree *cells, groove_ValueVec *out /* Nullable-wrapped Option<Value> per column */);
void jazz_node__node__codec__cells_from_positional(const jazz_model_TableSchema *t, const groove_Value *const *cells, u32 n, jz_btree *out);
jz_err jazz_node__node__codec__nullable_value(groove_Value value, bool *some, groove_Value *out);
jz_err jazz_node__node__codec__validate_cell_value(const jazz_model_ColumnSchema *c, const groove_Value *v);
jz_err jazz_node__node__codec__expect_u64(groove_Value v, const char *field, u64 *out);
jz_err jazz_node__node__codec__expect_uuid(groove_Value v, const char *field, jz_uuid *out);
jz_err jazz_node__node__codec__validate_canonical_version_parts(const jazz_model_BranchKey *k);
jz_err jazz_node__node__codec__tx_id_from_value(groove_Value v, jazz_types_TxId *out);
jz_err jazz_node__node__codec__tx_kind_from_discriminant(u8 v, u32 *out);
jz_err jazz_node__node__codec__fate_from_encoded_fields(const groove_BorrowedRecord *r, jazz_model_Fate *out);
jz_err jazz_node__node__codec__rejection_reason_from_encoded_fields(const groove_BorrowedRecord *r, jazz_model_RejectionReason *out);
jz_err jazz_node__node__codec__durability_from_discriminant(u8 v, u32 *out);
jz_err jazz_node__node__codec__deletion_event_from_value(groove_Value v, u32 *out);
groove_Value jazz_node__node__codec__tx_id_value(jazz_types_TxId t);
jz_str jazz_node__node__codec__global_current_table_name(const jz_str *table);
jz_str jazz_node__node__codec__ahead_current_table_name(const jz_str *table);
/* TransactionRowRecord field indices */
#define JZN_TXROW_FIELD_TIME_IDX 0u
#define JZN_TXROW_FIELD_NODE_ID_IDX 1u
#define JZN_TXROW_FIELD_FATE_IDX 13u
#define JZN_TXROW_FIELD_REJECTION_REASON_IDX 15u
#define JZN_REJVER_FIELD_TX_TIME_IDX 0u
#define JZN_REJVER_FIELD_TX_NODE_ID_IDX 1u
#define JZN_REJVER_FIELD_ROW_UUID_IDX 2u

/* ---- contribution merge provenance at the storage boundary ---- */
typedef struct jazz_model_ContributionMergeProvenance jazz_model_ContributionMergeProvenance;
/* `impl FnMut(&str) -> PhysicalTableId` / `impl FnMut(&str, &str) -> PhysicalColumnId` */
typedef struct jzn_contribution_id_resolver {
    void *ctx;
    jz_err (*table_id)(void *ctx, const jz_str *table, u64 *out);
    jz_err (*column_id)(void *ctx, const jz_str *table, const jz_str *column, u64 *out);
} jzn_contribution_id_resolver;
typedef struct jzn_contribution_name_resolver {
    void *ctx;
    jz_err (*table_name)(void *ctx, u64 table, jz_str *out);
    jz_err (*column_name)(void *ctx, const jz_str *table, u64 column, jz_str *out);
} jzn_contribution_name_resolver;
jz_err jazz_node__node__codec__contribution_merge_storage_value(const jazz_model_ContributionMergeProvenance *p /* NULL = None */,
    jzn_contribution_id_resolver *r, groove_Value *out);
jz_err jazz_node__node__codec__contribution_merge_from_storage_record(groove_OwnedRecord record, jzn_contribution_name_resolver *r,
    jazz_model_ContributionMergeProvenance *out);
const groove_ValueType *jazz_node__node__codec__record_field_type(groove_RecordDescriptor descriptor, u32 index);
jz_err jazz_node__node__codec__contribution_component_from_storage(groove_EnumValue value, const groove_EnumSchema *schema,
    const jz_str *table, jzn_contribution_name_resolver *r, jazz_model_ContributionComponent *out);
jz_err jazz_node__node__physical__NodeState__contribution_merge_storage_value(const jazz_node_NodeState *s,
    const jazz_model_ContributionMergeProvenance *provenance, groove_Value *out);
jz_err jazz_node__node__physical__NodeState__contribution_merge_from_storage_record(const jazz_node_NodeState *s,
    groove_OwnedRecord record, jazz_model_ContributionMergeProvenance *out);
jz_err jazz_node__node__physical__NodeState__version_storage_primary_key(const jazz_node_NodeState *s, const jazz_node_VersionRow *v,
    jazz_node_PrimaryKeyValue *out);
extern const jz_btree_desc jzn_desc_uuid_plan_map; /* physical_write_plan_cache */

#endif
