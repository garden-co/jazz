use super::*;
use crate::batch_fate::{
    BatchFate, CapturedFrontierMember, SealedBatchMember, SealedBatchSubmission,
};
use crate::metadata::{MetadataKey, RowProvenance};
use crate::query_manager::encoding::encode_row;
use crate::query_manager::policy::Operation;
use crate::query_manager::query::QueryBuilder;
use crate::query_manager::types::{ColumnType, SchemaBuilder, SchemaHash, TableSchema, Value};
use crate::row_histories::{BatchId, StoredRowBatch, VisibleRowEntry};
use crate::storage::{MemoryStorage, Storage};
use crate::test_support::{create_test_row_with_id, persist_test_schema};
use std::collections::{HashMap, HashSet};

fn users_test_schema() -> crate::query_manager::types::Schema {
    SchemaBuilder::new()
        .table(TableSchema::builder("users").column("value", ColumnType::Text))
        .build()
}

fn users_schema_hash() -> SchemaHash {
    SchemaHash::compute(&users_test_schema())
}

fn seed_users_schema(storage: &mut MemoryStorage) {
    persist_test_schema(storage, &users_test_schema());
}

fn row_metadata(table: &str) -> HashMap<String, String> {
    HashMap::from([
        (MetadataKey::Table.to_string(), table.to_string()),
        (
            MetadataKey::OriginSchemaHash.to_string(),
            users_schema_hash().to_string(),
        ),
    ])
}

fn visible_row(
    row_id: ObjectId,
    branch: &str,
    parents: Vec<BatchId>,
    updated_at: u64,
    data: &[u8],
) -> crate::row_histories::StoredRowBatch {
    let payload = std::str::from_utf8(data).expect("sync-manager test row payload should be utf8");
    crate::row_histories::StoredRowBatch::new(
        row_id,
        branch,
        parents,
        encode_row(
            &users_test_schema()[&"users".into()].columns,
            &[Value::Text(payload.to_string())],
        )
        .expect("sync-manager test row should encode"),
        RowProvenance::for_insert(row_id.to_string(), updated_at),
        HashMap::new(),
        crate::row_histories::RowState::VisibleDirect,
        None,
    )
}

fn row_with_batch_state(
    row: crate::row_histories::StoredRowBatch,
    batch_id: BatchId,
    state: crate::row_histories::RowState,
    confirmed_tier: Option<DurabilityTier>,
) -> crate::row_histories::StoredRowBatch {
    crate::row_histories::StoredRowBatch::new_with_batch_id(
        batch_id,
        row.row_id,
        row.branch.as_str(),
        row.parents.iter().copied(),
        row.data.as_ref().to_vec(),
        row.row_provenance(),
        row.metadata
            .iter()
            .map(|(key, value)| (key.to_string(), value.to_string()))
            .collect(),
        state,
        confirmed_tier,
    )
}

fn row_with_state(
    row: crate::row_histories::StoredRowBatch,
    state: crate::row_histories::RowState,
    confirmed_tier: Option<DurabilityTier>,
) -> crate::row_histories::StoredRowBatch {
    let batch_id = row.batch_id;
    row_with_batch_state(row, batch_id, state, confirmed_tier)
}

fn seed_visible_row(
    _sm: &mut SyncManager,
    io: &mut MemoryStorage,
    table: &str,
    row: crate::row_histories::StoredRowBatch,
) {
    seed_users_schema(io);
    create_test_row_with_id(io, row.row_id, Some(row_metadata(table)));
    io.append_history_region_rows(table, std::slice::from_ref(&row))
        .unwrap();
    io.upsert_visible_region_rows(
        table,
        std::slice::from_ref(&VisibleRowEntry::rebuild(
            row.clone(),
            std::slice::from_ref(&row),
        )),
    )
    .unwrap();
}

fn persist_visible_row_settlement(
    io: &mut MemoryStorage,
    row_id: ObjectId,
    row: &crate::row_histories::StoredRowBatch,
) {
    let Some(confirmed_tier) = row.confirmed_tier else {
        return;
    };
    let settlement = match row.state {
        crate::row_histories::RowState::VisibleDirect => BatchFate::DurableDirect {
            batch_id: row.batch_id,
            confirmed_tier,
        },
        crate::row_histories::RowState::VisibleTransactional => BatchFate::AcceptedTransaction {
            batch_id: row.batch_id,
            confirmed_tier,
        },
        crate::row_histories::RowState::StagingPending
        | crate::row_histories::RowState::Superseded
        | crate::row_histories::RowState::Rejected => return,
    };
    io.upsert_authoritative_batch_fate(&settlement).unwrap();
}

struct FailingHistoryPatchStorage {
    inner: MemoryStorage,
    fail_history_load: bool,
    fail_authoritative_settlement_upsert: bool,
    fail_sealed_submission_upsert: bool,
}

impl FailingHistoryPatchStorage {
    fn new() -> Self {
        Self {
            inner: MemoryStorage::new(),
            fail_history_load: false,
            fail_authoritative_settlement_upsert: false,
            fail_sealed_submission_upsert: false,
        }
    }

    fn inner_mut(&mut self) -> &mut MemoryStorage {
        &mut self.inner
    }
}

impl Storage for FailingHistoryPatchStorage {
    fn apply_encoded_row_mutation(
        &mut self,
        table: &str,
        history_rows: &[crate::storage::OwnedHistoryRowBytes],
        visible_rows: &[crate::storage::OwnedVisibleRowBytes],
        index_mutations: &[crate::storage::IndexMutation<'_>],
    ) -> Result<(), crate::storage::StorageError> {
        self.inner
            .apply_encoded_row_mutation(table, history_rows, visible_rows, index_mutations)
    }

    fn apply_prepared_row_mutation(
        &mut self,
        table: &str,
        history_rows: &[StoredRowBatch],
        visible_entries: &[crate::row_histories::VisibleRowEntry],
        encoded_history_rows: &[crate::storage::OwnedHistoryRowBytes],
        encoded_visible_rows: &[crate::storage::OwnedVisibleRowBytes],
        index_mutations: &[crate::storage::IndexMutation<'_>],
    ) -> Result<(), crate::storage::StorageError> {
        self.inner.apply_prepared_row_mutation(
            table,
            history_rows,
            visible_entries,
            encoded_history_rows,
            encoded_visible_rows,
            index_mutations,
        )
    }

    fn raw_table_put(
        &mut self,
        table: &str,
        key: &str,
        value: &[u8],
    ) -> Result<(), crate::storage::StorageError> {
        self.inner.raw_table_put(table, key, value)
    }

    fn raw_table_delete(
        &mut self,
        table: &str,
        key: &str,
    ) -> Result<(), crate::storage::StorageError> {
        self.inner.raw_table_delete(table, key)
    }

    fn raw_table_get(
        &self,
        table: &str,
        key: &str,
    ) -> Result<Option<Vec<u8>>, crate::storage::StorageError> {
        self.inner.raw_table_get(table, key)
    }

    fn raw_table_scan_prefix(
        &self,
        table: &str,
        prefix: &str,
    ) -> Result<crate::storage::RawTableRows, crate::storage::StorageError> {
        self.inner.raw_table_scan_prefix(table, prefix)
    }

    fn raw_table_scan_range(
        &self,
        table: &str,
        start: Option<&str>,
        end: Option<&str>,
    ) -> Result<crate::storage::RawTableRows, crate::storage::StorageError> {
        self.inner.raw_table_scan_range(table, start, end)
    }

    fn load_history_row_batch(
        &self,
        table: &str,
        branch: &str,
        row_id: ObjectId,
        batch_id: BatchId,
    ) -> Result<Option<StoredRowBatch>, crate::storage::StorageError> {
        if self.fail_history_load {
            return Err(crate::storage::StorageError::IoError(format!(
                "simulated load_history_row_batch failure for {table}/{branch}/{row_id}/{batch_id:?}"
            )));
        }
        self.inner
            .load_history_row_batch(table, branch, row_id, batch_id)
    }

    fn upsert_authoritative_batch_fate(
        &mut self,
        settlement: &BatchFate,
    ) -> Result<(), crate::storage::StorageError> {
        if self.fail_authoritative_settlement_upsert {
            return Err(crate::storage::StorageError::IoError(format!(
                "simulated authoritative settlement persist failure for {:?}",
                settlement.batch_id()
            )));
        }
        self.inner.upsert_authoritative_batch_fate(settlement)
    }

    fn upsert_sealed_batch_submission(
        &mut self,
        submission: &SealedBatchSubmission,
    ) -> Result<(), crate::storage::StorageError> {
        if self.fail_sealed_submission_upsert {
            return Err(crate::storage::StorageError::IoError(format!(
                "simulated sealed submission persist failure for {:?}",
                submission.batch_id
            )));
        }
        self.inner.upsert_sealed_batch_submission(submission)
    }
}

fn sealed_submission(
    batch_id: BatchId,
    target_branch_name: &str,
    members: Vec<SealedBatchMember>,
    captured_frontier: Vec<CapturedFrontierMember>,
) -> SealedBatchSubmission {
    let mode = if captured_frontier.is_empty() {
        crate::batch_fate::BatchMode::Direct
    } else {
        crate::batch_fate::BatchMode::Transactional
    };
    sealed_submission_with_mode(
        batch_id,
        mode,
        target_branch_name,
        members,
        captured_frontier,
    )
}

fn transactional_sealed_submission(
    batch_id: BatchId,
    target_branch_name: &str,
    members: Vec<SealedBatchMember>,
    captured_frontier: Vec<CapturedFrontierMember>,
) -> SealedBatchSubmission {
    sealed_submission_with_mode(
        batch_id,
        crate::batch_fate::BatchMode::Transactional,
        target_branch_name,
        members,
        captured_frontier,
    )
}

fn sealed_submission_with_mode(
    batch_id: BatchId,
    mode: crate::batch_fate::BatchMode,
    target_branch_name: &str,
    members: Vec<SealedBatchMember>,
    captured_frontier: Vec<CapturedFrontierMember>,
) -> SealedBatchSubmission {
    SealedBatchSubmission::new(
        batch_id,
        mode,
        BranchName::new(target_branch_name),
        members,
        captured_frontier,
    )
}

fn add_client(sm: &mut SyncManager, io: &MemoryStorage, client_id: ClientId) {
    sm.add_client_with_storage(io, client_id);
}

fn add_server(sm: &mut SyncManager, io: &MemoryStorage, server_id: ServerId) {
    sm.add_server_with_storage(server_id, false, io);
}

fn set_client_query_scope(
    sm: &mut SyncManager,
    io: &MemoryStorage,
    client_id: ClientId,
    query_id: QueryId,
    scope: HashSet<(ObjectId, BranchName)>,
    session: Option<crate::query_manager::session::Session>,
) {
    sm.set_client_query_scope_with_storage(io, client_id, query_id, scope, session);
}

fn load_visible_row(
    storage: &MemoryStorage,
    table: &str,
    row_id: ObjectId,
    branch: &str,
) -> StoredRowBatch {
    storage
        .load_visible_region_row(table, branch, row_id)
        .unwrap()
        .expect("visible row should exist")
}

fn push_query_subscription(
    sm: &mut SyncManager,
    client_id: ClientId,
    payload_session: Option<crate::query_manager::session::Session>,
) -> Vec<PendingQuerySubscription> {
    let query = QueryBuilder::new("messages").branch("main").build();
    sm.push_inbox(InboxEntry {
        source: Source::Client(client_id),
        payload: SyncPayload::QuerySubscription {
            query_id: QueryId(1),
            query: Box::new(query),
            session: payload_session,
            required_tier: None,
            propagation: QueryPropagation::Full,
            policy_context_tables: vec![],
        },
    });
    sm.process_inbox(&mut MemoryStorage::new());
    sm.take_pending_query_subscriptions()
}

/// On client reconnect, the client replays its entire OPFS row history to the
/// server as `RowBatchCreated`, including rows originally authored by *other*
/// users. If the server re-runs permission checks classifying these as
/// `Operation::Update` (because a row with that id already exists), tables
/// without an `allowUpdate` policy end up rejecting the replay and the client
/// retracts the row from its view on the Rejected settlement — making data
/// "disappear on reload".
///
/// When the incoming row batch exactly matches an already-stored history
/// member, the server has nothing new to learn: it should short-circuit the
/// permission check and re-emit the cached settlement so the client can
/// reconcile.
mod basic;
mod client_lifecycle;
mod permissions;
mod query_scope;
mod server_sync;
mod settlements;
mod subscriptions;
mod transaction_sealing;
