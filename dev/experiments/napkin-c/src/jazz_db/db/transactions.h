/* transactions.h: port of crates/jazz/layers/db/src/db/transactions.rs */
#ifndef JAZZ_DB_TRANSACTIONS_H
#define JAZZ_DB_TRANSACTIONS_H
#include "jz_rt.h"
#include "../db.h"
#define TX_FN(n) jazz_db__db__transactions__Db__##n
/* AsyncFnOnce(&mut MergeableTx) -> Result<T, Error> */
typedef jz_err (*jazz_db_TxCallback)(void *ctx, jazz_db_TxHandle *tx, void *value_out);
jz_err TX_FN(lock_for_transaction_operation)(const jazz_db_Db *db, jazz_model_OpenTransactionId open_tx_id, jazz_node_NodeState **out);
jz_err TX_FN(transaction_is_exclusive)(const jazz_db_Db *db, jazz_model_OpenTransactionId id, bool *out);
jz_err TX_FN(exclusive_source_hydration_queries)(const jazz_db_Db *db, jazz_model_OpenTransactionId id, const jazz_db_PreparedQuery *prepared, bool include_deleted, jazz_db_QueryVec *out);
jz_err TX_FN(mergeable_tx)(const jazz_db_Db *db, jazz_db_TxHandle *out);
jz_err TX_FN(transaction)(const jazz_db_Db *db, jazz_db_TxCallback callback, void *ctx, void *value_out, jazz_model_TxId *tx_id_out);
jz_err TX_FN(mergeable_tx_for_identity)(const jazz_db_Db *db, jazz_types_AuthorSubject author, jazz_db_TxHandle *out);
jz_err TX_FN(transaction_for_identity)(const jazz_db_Db *db, jazz_types_AuthorSubject author, jazz_db_TxCallback callback, void *ctx, void *value_out, jazz_model_TxId *tx_id_out);
jz_err TX_FN(begin_mergeable)(const jazz_db_Db *db, jazz_model_OpenTransactionId id);
jz_err TX_FN(begin_mergeable_for_identity)(const jazz_db_Db *db, jazz_model_OpenTransactionId id, jazz_types_AuthorSubject author);
jz_err TX_FN(begin_mergeable_with_identity)(const jazz_db_Db *db, jazz_model_OpenTransactionId id, jazz_db_WriteIdentity identity);
jz_err TX_FN(enqueue_begin_mergeable)(const jazz_db_Db *db, jazz_model_OpenTransactionId id, bool has_author, jazz_types_AuthorSubject author, bool has_attribution, jazz_types_AuthorSubject attribution);
jazz_db_TxHandle TX_FN(mergeable_tx_ref)(const jazz_db_Db *db, jazz_model_OpenTransactionId tx_id);
jz_err TX_FN(mergeable_transaction_upsert_exists)(const jazz_db_Db *db, jazz_model_OpenTransactionId tx_id, jz_slice table, jazz_types_RowUuid row, bool *out);
jz_err TX_FN(stage_mergeable_upsert_in_branch_view)(const jazz_db_Db *db, jazz_model_OpenTransactionId tx_id, jz_slice table, jazz_model_BranchSelector head, bool has_base, jazz_protocol_BranchViewBase base, jazz_types_RowUuid row, jz_btree cells, bool has_now, u64 now_ms, bool require_complete_insert);
jz_err TX_FN(enqueue_commit_mergeable_handle_at_ms)(const jazz_db_Db *db, jazz_model_OpenTransactionId open_tx_id, u64 now_ms, jazz_db_WriteHandle *out);
jz_err TX_FN(enqueue_commit_mergeable_handle)(const jazz_db_Db *db, jazz_model_OpenTransactionId open_tx_id, jazz_db_WriteHandle *out);
jz_err TX_FN(abandon_transaction_handle)(const jazz_db_Db *db, jazz_model_OpenTransactionId open_tx_id);
void TX_FN(enqueue_abandon_transaction_handle)(const jazz_db_Db *db, jazz_model_OpenTransactionId open_tx_id);
jz_err TX_FN(exclusive_tx)(const jazz_db_Db *db, jazz_db_TxHandle *out);
jz_err TX_FN(begin_exclusive)(const jazz_db_Db *db, jazz_model_OpenTransactionId id);
jz_err TX_FN(begin_exclusive_for_identity)(const jazz_db_Db *db, jazz_model_OpenTransactionId id, jazz_types_AuthorSubject author);
jz_err TX_FN(begin_exclusive_with_identity)(const jazz_db_Db *db, jazz_model_OpenTransactionId id, jazz_db_WriteIdentity identity);
jz_err TX_FN(enqueue_begin_exclusive)(const jazz_db_Db *db, jazz_model_OpenTransactionId id, bool has_author, jazz_types_AuthorSubject author, bool has_attribution, jazz_types_AuthorSubject attribution);
jazz_db_TxHandle TX_FN(exclusive_tx_ref)(const jazz_db_Db *db, jazz_model_OpenTransactionId tx_id);
jz_err TX_FN(enqueue_transaction_insert)(const jazz_db_Db *db, jazz_model_OpenTransactionId id, jz_str table, jazz_db_RowCells cells, jazz_db_InsertOptions options, jazz_types_RowUuid *out);
jz_err TX_FN(enqueue_transaction_update)(const jazz_db_Db *db, jazz_model_OpenTransactionId id, jz_str table, jazz_types_RowUuid row, jazz_db_RowCells patch, jazz_db_UpdateOptions options);
jz_err TX_FN(enqueue_transaction_large_value_update)(const jazz_db_Db *db, jazz_model_OpenTransactionId id, jz_str table, jazz_types_RowUuid row, jazz_db_RowCells patch, jz_vec mutations, bool has_updated_at_ms, u64 updated_at_ms);
jz_err TX_FN(enqueue_transaction_upsert)(const jazz_db_Db *db, jazz_model_OpenTransactionId id, jz_str table, jazz_types_RowUuid row, jazz_db_RowCells cells, jazz_db_UpsertOptions options);
jz_err TX_FN(enqueue_transaction_delete)(const jazz_db_Db *db, jazz_model_OpenTransactionId id, jz_str table, jazz_types_RowUuid row, jazz_db_DeleteOptions options);
jz_err TX_FN(enqueue_transaction_restore)(const jazz_db_Db *db, jazz_model_OpenTransactionId id, jz_str table, jazz_types_RowUuid row, bool has_cells, jazz_db_RowCells cells, jazz_db_RestoreOptions options);
jz_err TX_FN(all_in_open_transaction)(const jazz_db_Db *db, jazz_model_OpenTransactionId tx_id, const jazz_db_PreparedQuery *prepared, jazz_db_ReadOpts opts, bool has_author, jazz_types_AuthorSubject author, jazz_db_CurrentRowVec *out);
jz_err TX_FN(relation_snapshot_in_open_transaction)(const jazz_db_Db *db, jazz_model_OpenTransactionId tx_id, const jazz_db_PreparedQuery *prepared, jazz_db_ReadOpts opts, bool has_author, jazz_types_AuthorSubject author, jazz_node_RelationSnapshot *out);
jz_err TX_FN(transaction_target_for_write)(const jazz_db_Db *db, jazz_model_OpenTransactionId tx_id, jz_slice table, jazz_types_RowUuid row, const char *operation, bool allow_absent, bool *has, jz_btree *out);
jz_err TX_FN(enqueue_commit_exclusive_handle_at_ms)(const jazz_db_Db *db, jazz_model_OpenTransactionId open_tx_id, u64 now_ms, jazz_db_WriteHandle *out);
jz_err TX_FN(enqueue_commit_exclusive_handle)(const jazz_db_Db *db, jazz_model_OpenTransactionId open_tx_id, jazz_db_WriteHandle *out);
jz_err TX_FN(commit_exclusive_handle_for_identity)(const jazz_db_Db *db, jazz_model_OpenTransactionId open_tx_id, jazz_types_AuthorSubject author, jazz_model_TxId *out);
jz_err TX_FN(abandon_exclusive_handle)(const jazz_db_Db *db, jazz_model_OpenTransactionId open_tx_id);
jz_err TX_FN(open_exclusive_handle)(const jazz_db_Db *db, jazz_model_OpenTransactionId id);
#undef TX_FN
#endif
