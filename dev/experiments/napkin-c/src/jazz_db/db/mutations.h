/* mutations.h: port of crates/jazz/layers/db/src/db/mutations.rs
 * Row insertion, update, deletion, restoration, and authorization. */
#ifndef JAZZ_DB_MUTATIONS_H
#define JAZZ_DB_MUTATIONS_H
#include "jz_rt.h"
#include "../_ext_db-a.h"
#include "node_runtime.h"
#include "peer_connection.h"

/* #[serde(tag = "kind", rename_all = "snake_case")] enum LargeValueUpdatePage */
typedef enum { JAZZ_LVP_BYTES = 0, JAZZ_LVP_TEXT_UTF16 = 1, JAZZ_LVP_TEXT_UTF8 = 2 } jazz_db_LargeValueUpdatePageTag;
typedef struct jazz_db_LargeValueUpdatePage { u32 tag; u64 from; u64 to; } jazz_db_LargeValueUpdatePage;

typedef struct jazz_db_LargeValueUpdateSplice { u64 at; u64 delete_; jz_bytes insert; } jazz_db_LargeValueUpdateSplice;
void jazz_db__db__mutations__LargeValueUpdateSplice__drop(void *p);
void jazz_db__db__mutations__LargeValueUpdateSplice__clone(void *dst, const void *src);

typedef struct jazz_db_JsonSetEdit { jz_str at; jz_json *value; } jazz_db_JsonSetEdit;
void jazz_db__db__mutations__JsonSetEdit__drop(void *p);

typedef enum { JAZZ_LVU_SPLICE = 0, JAZZ_LVU_JSON_SET = 1 } jazz_db_LargeValueUpdateTag;
typedef struct jazz_db_LargeValueUpdate {
    u32 tag;
    jz_str column;
    jazz_db_LargeValueUpdatePage within;                   /* Splice */
    JZ_VEC(jazz_db_LargeValueUpdateSplice) splices;        /* Splice */
    JZ_VEC(jazz_db_JsonSetEdit) edits;                     /* JsonSet */
} jazz_db_LargeValueUpdate;
typedef JZ_VEC(jazz_db_LargeValueUpdate) jazz_db_LargeValueUpdateVec;
void jazz_db__db__mutations__LargeValueUpdate__drop(void *p);

/* serde Deserialize (serde_json, internally tagged) */
jz_err jazz_db__db__mutations__LargeValueUpdatePage__Deserialize__deserialize(const jz_json *v, jazz_db_LargeValueUpdatePage *out);
jz_err jazz_db__db__mutations__LargeValueUpdateSplice__Deserialize__deserialize(const jz_json *v, jazz_db_LargeValueUpdateSplice *out);
jz_err jazz_db__db__mutations__JsonSetEdit__Deserialize__deserialize(const jz_json *v, jazz_db_JsonSetEdit *out);
jz_err jazz_db__db__mutations__LargeValueUpdate__Deserialize__deserialize(const jz_json *v, jazz_db_LargeValueUpdate *out);

typedef enum { JAZZ_SMK_INSERT = 0, JAZZ_SMK_UPDATE = 1, JAZZ_SMK_UPSERT = 2 } jazz_db_StreamingMutationKind;

typedef struct jazz_db_StreamingValueUpload {
    groove_StagedLargeValueId id;
    groove_LargeValueKind kind;
    bool initialized;
    groove_PushStreamingPreparation *preparation; /* Option */
    JZ_VEC(groove_StagedChunk) *emitted;          /* Rc<RefCell<Vec<StagedChunk>>> */
} jazz_db_StreamingValueUpload;
groove_StagedLargeValueId jazz_db__db__mutations__StreamingValueUpload__cleanup_id(jazz_db_StreamingValueUpload *self);

jz_err jazz_db__db__mutations__validate_updated_at_ms(bool has, u64 updated_at_ms);
jz_err jazz_db__db__mutations__apply_json_set(jz_json **value, jz_slice pointer, jz_json *replacement);

#define DB_FN(name) jazz_db__db__mutations__Db__##name
typedef jazz_db_Db DB;
void DB_FN(queued_write_handle)(const DB *self, jazz_types_RowUuid row, jazz_model_TxId tx_id, jazz_db_QueuedMutationStatus *status, jazz_db_QueuedMutationAlias *alias, jazz_db_WriteHandle *out);
jz_err DB_FN(enqueue_insert)(const DB *self, jz_str table, jz_btree cells, jazz_db_InsertOptions options, jazz_db_WriteHandle *out);
jz_err DB_FN(enqueue_update)(const DB *self, jz_str table, jazz_types_RowUuid row, jz_btree patch, jazz_db_UpdateOptions options, jazz_db_WriteHandle *out);
jz_err DB_FN(enqueue_upsert)(const DB *self, jz_str table, jazz_types_RowUuid row, jz_btree cells, jazz_db_UpsertOptions options, jazz_db_WriteHandle *out);
jz_err DB_FN(enqueue_delete)(const DB *self, jz_str table, jazz_types_RowUuid row, jazz_db_DeleteOptions options, jazz_db_WriteHandle *out);
jz_err DB_FN(enqueue_restore)(const DB *self, jz_str table, jazz_types_RowUuid row, bool has_cells, jz_btree cells, jazz_db_RestoreOptions options, jazz_db_WriteHandle *out);
jz_err DB_FN(enqueue_large_value_update)(const DB *self, jz_str table, jazz_types_RowUuid row, jz_btree patch, jazz_db_LargeValueUpdateVec mutations, bool has_updated_at_ms, u64 updated_at_ms, jazz_db_WriteHandle *out);
jz_err DB_FN(update_with_large_value_mutations_at_ms)(const DB *self, jz_slice table, jazz_types_RowUuid row, jz_btree patch, jazz_db_LargeValueUpdateVec mutations, u64 now_ms, jazz_db_WriteHandle *out);
jz_err DB_FN(stage_transaction_large_value_update)(const DB *self, jazz_model_OpenTransactionId tx_id, jz_slice table, jazz_types_RowUuid row, jz_btree patch, jazz_db_LargeValueUpdateVec mutations, bool has_updated_at_ms, u64 updated_at_ms);
jz_err DB_FN(insert)(const DB *self, jz_slice table, jz_btree cells, jazz_db_InsertOptions options, jazz_db_WriteHandle *out);
jz_err DB_FN(begin_streaming_value_upload)(const DB *self, jz_slice table, const jz_btree *cells, jz_slice column, jazz_db_StreamingValueUpload *out);
jz_err DB_FN(push_streaming_value_upload)(const DB *self, jazz_db_StreamingValueUpload *upload, const u8 *bytes, u32 n);
jz_err DB_FN(abort_streaming_value_upload)(const DB *self, jazz_db_StreamingValueUpload upload);
jz_err DB_FN(finish_streaming_value_upload)(const DB *self, jazz_db_StreamingValueUpload upload, jazz_db_StreamingMutationKind mutation, jz_slice table, jazz_types_RowUuid row, jz_btree cells, jz_slice column, jazz_db_WriteIdentity identity, bool has_now_ms, u64 now_ms, const jazz_protocol_BranchSelector *head, jazz_protocol_BranchViewBase *base, jazz_db_WriteHandle *out);
jz_err DB_FN(can_insert)(const DB *self, jz_slice table, jz_btree cells, jazz_protocol_PermissionAdvice *out);
jz_err DB_FN(update)(const DB *self, jz_slice table, jazz_types_RowUuid row, jz_btree patch, jazz_db_UpdateOptions options, jazz_db_WriteHandle *out);
jz_err DB_FN(upsert_with_insert_validation)(const DB *self, jz_slice table, jazz_types_RowUuid row, jz_btree cells, jazz_db_UpsertOptions options, bool require_complete_insert, jazz_db_WriteHandle *out);
jz_err DB_FN(upsert)(const DB *self, jz_slice table, jazz_types_RowUuid row, jz_btree cells, jazz_db_UpsertOptions options, jazz_db_WriteHandle *out);
jz_err DB_FN(delete)(const DB *self, jz_slice table, jazz_types_RowUuid row, jazz_db_DeleteOptions options, jazz_db_WriteHandle *out);
jz_err DB_FN(can_read)(const DB *self, jz_slice table, jazz_types_RowUuid row, jazz_protocol_PermissionAdvice *out);
jz_err DB_FN(authorize_read_for_identity)(const DB *self, jz_slice table, jazz_types_RowUuid row, jazz_types_AuthorSubject identity, jazz_protocol_PermissionAdvice *out);
jz_err DB_FN(can_update)(const DB *self, jz_slice table, jazz_types_RowUuid row, jazz_protocol_PermissionAdvice *out);
void DB_FN(set_identity_claims)(const DB *self, jazz_types_AuthorSubject identity, jz_btree claims);
void DB_FN(set_identity_claims_async)(const DB *self, jazz_types_AuthorSubject identity, jz_btree claims);
jz_err DB_FN(can_delete)(const DB *self, jz_slice table, jazz_types_RowUuid row, jazz_protocol_PermissionAdvice *out);
jz_err DB_FN(restore)(const DB *self, jz_slice table, jazz_types_RowUuid row, bool has_cells, jz_btree cells, jazz_db_RestoreOptions options, jazz_db_WriteHandle *out);
jz_err DB_FN(settle_local_publication)(const DB *self, jazz_node_PublishedTransaction *published, bool has_unit, jazz_protocol_SyncMessage unit, jazz_model_DurabilityTier *out);
jz_err DB_FN(finish_deferred_local_publication)(const DB *self, jazz_node_PublishedTransaction *published, bool has_unit, jazz_protocol_SyncMessage upload_unit);
/* finish_publication_outcome: the boxed future runs synchronously; outcome.value stays with the caller */
jz_err DB_FN(finish_publication_outcome)(const DB *self, jazz_node_PublicationOutcome outcome);
jz_err DB_FN(resolve_write_identity)(const DB *self, jazz_db_WriteIdentity identity, jazz_db_ResolvedWriteIdentity *out);
jz_err DB_FN(check_catalogue_admin)(const DB *self);
jz_err DB_FN(finalize_local_commit)(const DB *self, jazz_model_TxId tx_id, jazz_model_DurabilityTier *out);
u64 DB_FN(next_now_ms)(const DB *self);
jz_err DB_FN(ensure_open_schema_admitted)(const DB *self);
jz_err DB_FN(await_open_schema_for_coverage)(const DB *self, jazz_model_DurabilityTier coverage_tier);
jz_err DB_FN(current_write_schema_for_query)(const DB *self, jazz_node_JazzSchema **schema_out, jazz_types_SchemaVersionId *out);
jz_err DB_FN(table_schema)(const DB *self, jz_slice table, const jazz_node_TableSchema **out);
jz_err DB_FN(validate_complete_insert)(const DB *self, jz_slice table, const jz_btree *cells);
jz_err DB_FN(apply_insert_defaults)(const DB *self, jz_slice table, jz_btree *cells);
jz_err DB_FN(local_current_row)(const DB *self, jz_slice table, jazz_types_RowUuid row, bool *has, jazz_node_CurrentRow *out);
jz_err DB_FN(ensure_row_not_deleted)(const DB *self, jz_slice table, jazz_types_RowUuid row);
jz_err DB_FN(precheck_mutation_admission)(const DB *self, jz_slice table);
jz_err DB_FN(ensure_branch_view_row_not_deleted)(const DB *self, jz_slice table, const jazz_protocol_BranchSelector *head, const jazz_protocol_BranchViewBase *base, jazz_types_RowUuid row);
jz_err DB_FN(local_row_for_trusted_identity)(const DB *self, jz_slice table, jazz_types_RowUuid row, jazz_types_AuthorSubject identity, bool *has, jazz_node_CurrentRow *out);
jz_err DB_FN(visible_branch_view_cells_for_identity)(const DB *self, jz_slice table, const jazz_protocol_BranchSelector *head, const jazz_protocol_BranchViewBase *base, jazz_types_RowUuid row, jazz_types_AuthorSubject identity, bool *has, jz_btree *out);
#undef DB_FN

#endif
