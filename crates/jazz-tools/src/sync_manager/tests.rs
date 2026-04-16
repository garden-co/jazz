use super::*;
use crate::batch_fate::{
    BatchSettlement, CapturedFrontierMember, SealedBatchMember, SealedBatchSubmission,
    VisibleBatchMember,
};
use crate::metadata::{MetadataKey, RowProvenance};
use crate::query_manager::encoding::encode_row;
use crate::query_manager::query::QueryBuilder;
use crate::query_manager::types::{ColumnType, SchemaBuilder, SchemaHash, TableSchema, Value};
use crate::row_histories::{BatchId, StoredRowBatch, VisibleRowEntry};
use crate::storage::{MemoryStorage, Storage};
use crate::test_row_history::{create_test_row_with_id, persist_test_schema};
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

struct FailingHistoryPatchStorage {
    inner: MemoryStorage,
    fail_history_load: bool,
}

impl FailingHistoryPatchStorage {
    fn new() -> Self {
        Self {
            inner: MemoryStorage::new(),
            fail_history_load: false,
        }
    }

    fn inner_mut(&mut self) -> &mut MemoryStorage {
        &mut self.inner
    }
}

impl Storage for FailingHistoryPatchStorage {
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
}

fn sealed_submission(
    batch_id: BatchId,
    target_branch_name: &str,
    members: Vec<SealedBatchMember>,
    captured_frontier: Vec<CapturedFrontierMember>,
) -> SealedBatchSubmission {
    SealedBatchSubmission::new(
        batch_id,
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

#[test]
fn can_create_sync_manager() {
    let sm = SyncManager::new();
    assert!(sm.servers.is_empty());
    assert!(sm.clients.is_empty());
}

#[test]
fn set_query_scope_stores_session() {
    let mut sm = SyncManager::new();
    let io = MemoryStorage::new();
    let client_id = ClientId::new();
    let row_id = ObjectId::new();

    add_client(&mut sm, &io, client_id);
    set_client_query_scope(
        &mut sm,
        &io,
        client_id,
        QueryId(1),
        HashSet::from([(row_id, BranchName::new("main"))]),
        Some(crate::query_manager::session::Session::new("alice")),
    );

    let query = sm
        .get_client(client_id)
        .expect("client should exist")
        .queries
        .get(&QueryId(1))
        .expect("query should exist");
    assert_eq!(query.scope.len(), 1);
    assert_eq!(
        query
            .session
            .as_ref()
            .map(|session| session.user_id.as_str()),
        Some("alice")
    );
}

#[test]
fn set_query_scope_emits_query_scope_snapshot_to_client() {
    let mut sm = SyncManager::new();
    let io = MemoryStorage::new();
    let client_id = ClientId::new();
    let row_id = ObjectId::new();
    let query_id = QueryId(7);

    add_client(&mut sm, &io, client_id);
    sm.take_outbox();

    set_client_query_scope(
        &mut sm,
        &io,
        client_id,
        query_id,
        HashSet::from([(row_id, BranchName::new("main"))]),
        None,
    );

    assert!(sm.take_outbox().into_iter().any(|entry| matches!(
        entry,
        OutboxEntry {
            destination: Destination::Client(id),
            payload: SyncPayload::QueryScopeSnapshot { query_id: snapshot_query_id, scope },
        } if id == client_id
            && snapshot_query_id == query_id
            && scope == vec![(row_id, BranchName::new("main"))]
    )));
}

#[test]
fn query_scope_snapshot_from_server_is_stored_for_query() {
    let mut sm = SyncManager::new();
    let mut io = MemoryStorage::new();
    let server_id = ServerId::new();
    let query_id = QueryId(11);
    let first_row_id = ObjectId::from_uuid(uuid::Uuid::from_u128(1));
    let second_row_id = ObjectId::from_uuid(uuid::Uuid::from_u128(2));

    sm.process_from_server(
        &mut io,
        server_id,
        SyncPayload::QueryScopeSnapshot {
            query_id,
            scope: vec![
                (second_row_id, BranchName::new("main")),
                (first_row_id, BranchName::new("main")),
            ],
        },
    );

    assert_eq!(
        sm.remote_query_scope(query_id),
        HashSet::from([
            (first_row_id, BranchName::new("main")),
            (second_row_id, BranchName::new("main")),
        ])
    );
}

#[test]
fn query_scope_snapshot_from_server_relays_to_interested_clients() {
    let mut sm = SyncManager::new();
    let mut io = MemoryStorage::new();
    let client_id = ClientId::new();
    let server_id = ServerId::new();
    let query_id = QueryId(15);
    let row_id = ObjectId::from_uuid(uuid::Uuid::from_u128(15));

    add_client(&mut sm, &io, client_id);
    sm.take_outbox();
    sm.query_origin
        .entry(query_id)
        .or_default()
        .insert(client_id);

    sm.process_from_server(
        &mut io,
        server_id,
        SyncPayload::QueryScopeSnapshot {
            query_id,
            scope: vec![(row_id, BranchName::new("main"))],
        },
    );

    assert!(sm.take_outbox().into_iter().any(|entry| matches!(
        entry,
        OutboxEntry {
            destination: Destination::Client(id),
            payload: SyncPayload::QueryScopeSnapshot { query_id: relayed_query_id, scope },
        } if id == client_id
            && relayed_query_id == query_id
            && scope == vec![(row_id, BranchName::new("main"))]
    )));
}

#[test]
fn remove_server_clears_remote_query_scope() {
    let mut sm = SyncManager::new();
    let mut io = MemoryStorage::new();
    let server_id = ServerId::new();
    let query_id = QueryId(23);
    let row_id = ObjectId::from_uuid(uuid::Uuid::from_u128(23));

    sm.process_from_server(
        &mut io,
        server_id,
        SyncPayload::QueryScopeSnapshot {
            query_id,
            scope: vec![(row_id, BranchName::new("main"))],
        },
    );
    assert_eq!(
        sm.remote_query_scope(query_id),
        HashSet::from([(row_id, BranchName::new("main"))])
    );

    sm.remove_server(server_id);

    assert!(sm.remote_query_scope(query_id).is_empty());
}

#[test]
fn send_query_subscription_includes_session() {
    let mut sm = SyncManager::new();
    let io = MemoryStorage::new();
    let server_id = ServerId::new();
    add_server(&mut sm, &io, server_id);
    sm.take_outbox();

    let query = QueryBuilder::new("users").branch("main").build();
    let session = crate::query_manager::session::Session::new("alice");
    sm.send_query_subscription_to_servers(
        QueryId(7),
        query.clone(),
        Some(session.clone()),
        QueryPropagation::Full,
    );

    let outbox = sm.take_outbox();
    assert_eq!(outbox.len(), 1);
    match &outbox[0] {
        OutboxEntry {
            destination: Destination::Server(id),
            payload:
                SyncPayload::QuerySubscription {
                    query_id,
                    query: sent_query,
                    session: sent_session,
                    propagation,
                },
        } => {
            assert_eq!(*id, server_id);
            assert_eq!(*query_id, QueryId(7));
            assert_eq!(sent_query.table, query.table);
            assert_eq!(*propagation, QueryPropagation::Full);
            assert_eq!(
                sent_session
                    .as_ref()
                    .map(|session| session.user_id.as_str()),
                Some("alice")
            );
        }
        other => panic!("expected QuerySubscription to server, got {other:?}"),
    }
}

#[test]
fn schema_warning_from_server_relays_to_interested_clients() {
    let mut sm = SyncManager::new();
    let mut io = MemoryStorage::new();
    let client_id = ClientId::new();
    let server_id = ServerId::new();
    let query_id = QueryId(42);

    add_client(&mut sm, &io, client_id);
    sm.take_outbox();
    sm.query_origin
        .entry(query_id)
        .or_default()
        .insert(client_id);

    sm.process_from_server(
        &mut io,
        server_id,
        SyncPayload::SchemaWarning(SchemaWarning {
            query_id,
            table_name: "users".to_string(),
            row_count: 3,
            from_hash: crate::query_manager::types::SchemaHash([0xAA; 32]),
            to_hash: crate::query_manager::types::SchemaHash([0xBB; 32]),
        }),
    );

    let outbox = sm.take_outbox();
    assert_eq!(outbox.len(), 1);
    match &outbox[0] {
        OutboxEntry {
            destination: Destination::Client(id),
            payload: SyncPayload::SchemaWarning(warning),
        } => {
            assert_eq!(*id, client_id);
            assert_eq!(warning.query_id, query_id);
            assert_eq!(warning.table_name, "users");
        }
        other => panic!("expected relayed schema warning, got {other:?}"),
    }
}

#[test]
fn row_batch_created_emits_row_batch_state_changed_to_source() {
    let mut sm = SyncManager::new().with_durability_tier(DurabilityTier::Worker);
    let mut io = MemoryStorage::new();
    let server_id = ServerId::new();
    let row_id = ObjectId::new();
    let row = visible_row(row_id, "main", Vec::new(), 1_000, b"alice");

    sm.process_from_server(
        &mut io,
        server_id,
        SyncPayload::RowBatchCreated {
            metadata: Some(RowMetadata {
                id: row_id,
                metadata: row_metadata("users"),
            }),
            row: row.clone(),
        },
    );

    let outbox = sm.take_outbox();
    assert_eq!(outbox.len(), 1);
    match &outbox[0] {
        OutboxEntry {
            destination: Destination::Server(id),
            payload:
                SyncPayload::RowBatchStateChanged {
                    row_id: ack_row_id,
                    branch_name,
                    batch_id,
                    state,
                    confirmed_tier,
                },
        } => {
            assert_eq!(*id, server_id);
            assert_eq!(*ack_row_id, row_id);
            assert_eq!(*branch_name, BranchName::new("main"));
            assert_eq!(*batch_id, row.batch_id);
            assert_eq!(*state, None);
            assert_eq!(*confirmed_tier, Some(DurabilityTier::Worker));
        }
        other => panic!("expected RowBatchStateChanged to server, got {other:?}"),
    }
}

#[test]
fn row_batch_created_stamps_local_durability_into_storage() {
    let mut sm = SyncManager::new().with_durability_tier(DurabilityTier::EdgeServer);
    let mut io = MemoryStorage::new();
    let server_id = ServerId::new();
    let row_id = ObjectId::new();
    let row = visible_row(row_id, "main", Vec::new(), 1_000, b"alice");
    seed_users_schema(&mut io);

    sm.process_from_server(
        &mut io,
        server_id,
        SyncPayload::RowBatchCreated {
            metadata: Some(RowMetadata {
                id: row_id,
                metadata: row_metadata("users"),
            }),
            row,
        },
    );

    let visible = io
        .load_visible_region_row("users", "main", row_id)
        .unwrap()
        .expect("visible row");
    let history = io
        .scan_history_region(
            "users",
            "main",
            crate::row_histories::HistoryScan::Row { row_id },
        )
        .unwrap();

    assert_eq!(visible.confirmed_tier, Some(DurabilityTier::EdgeServer));
    assert_eq!(history.len(), 1);
    assert_eq!(history[0].confirmed_tier, Some(DurabilityTier::EdgeServer));
    assert_eq!(
        load_visible_row(&io, "users", row_id, "main").confirmed_tier,
        Some(DurabilityTier::EdgeServer)
    );
}

#[test]
fn row_batch_state_changed_updates_row_region_confirmed_tier_monotonically() {
    let mut sm = SyncManager::new();
    let mut io = MemoryStorage::new();
    let row_id = ObjectId::new();
    let row = visible_row(row_id, "main", Vec::new(), 1_000, b"alice");
    let batch_id = row.batch_id;
    seed_visible_row(&mut sm, &mut io, "users", row.clone());

    sm.process_from_server(
        &mut io,
        ServerId::new(),
        SyncPayload::RowBatchStateChanged {
            row_id,
            branch_name: BranchName::new("main"),
            batch_id,
            state: None,
            confirmed_tier: Some(DurabilityTier::EdgeServer),
        },
    );
    sm.process_from_server(
        &mut io,
        ServerId::new(),
        SyncPayload::RowBatchStateChanged {
            row_id,
            branch_name: BranchName::new("main"),
            batch_id,
            state: None,
            confirmed_tier: Some(DurabilityTier::Worker),
        },
    );

    let visible = io.scan_visible_region("users", "main").unwrap();
    let history = io
        .scan_history_region(
            "users",
            "main",
            crate::row_histories::HistoryScan::Row { row_id },
        )
        .unwrap();
    assert_eq!(visible.len(), 1);
    assert_eq!(history.len(), 1);
    assert_eq!(visible[0].confirmed_tier, Some(DurabilityTier::EdgeServer));
    assert_eq!(history[0].confirmed_tier, Some(DurabilityTier::EdgeServer));
}

#[test]
fn row_batch_state_changed_enqueues_pending_row_update_for_visible_row() {
    let mut sm = SyncManager::new();
    let mut io = MemoryStorage::new();
    let row_id = ObjectId::new();
    let row = visible_row(row_id, "main", Vec::new(), 1_000, b"alice");
    let batch_id = row.batch_id;
    seed_visible_row(&mut sm, &mut io, "users", row.clone());

    sm.process_from_server(
        &mut io,
        ServerId::new(),
        SyncPayload::RowBatchStateChanged {
            row_id,
            branch_name: BranchName::new("main"),
            batch_id,
            state: None,
            confirmed_tier: Some(DurabilityTier::EdgeServer),
        },
    );

    let updates = sm.take_pending_row_visibility_changes();
    assert_eq!(updates.len(), 1);
    assert_eq!(updates[0].object_id, row_id);
    assert_eq!(
        updates[0].row.confirmed_tier,
        Some(DurabilityTier::EdgeServer)
    );
}

#[test]
fn row_batch_state_changed_does_not_ack_when_storage_patch_fails() {
    let mut sm = SyncManager::new();
    let mut io = FailingHistoryPatchStorage::new();
    let row_id = ObjectId::new();
    let row = visible_row(row_id, "main", Vec::new(), 1_000, b"alice");
    let batch_id = row.batch_id;
    seed_visible_row(&mut sm, io.inner_mut(), "users", row.clone());
    io.fail_history_load = true;

    sm.process_from_server(
        &mut io,
        ServerId::new(),
        SyncPayload::RowBatchStateChanged {
            row_id,
            branch_name: BranchName::new("main"),
            batch_id,
            state: None,
            confirmed_tier: Some(DurabilityTier::EdgeServer),
        },
    );

    assert!(sm.take_received_row_batch_acks().is_empty());
    assert!(sm.take_pending_row_visibility_changes().is_empty());
    let loaded = io
        .inner
        .load_visible_region_row("users", "main", row_id)
        .unwrap()
        .expect("visible row should remain present");
    assert_eq!(loaded.confirmed_tier, None);
}

#[test]
fn row_batch_state_changed_relays_to_clients_that_received_row_batch_needed() {
    let mut sm = SyncManager::new();
    let mut io = MemoryStorage::new();
    let client_id = ClientId::new();
    let row_id = ObjectId::new();
    let row = visible_row(row_id, "main", Vec::new(), 1_000, b"alice");
    let batch_id = row.batch_id;

    add_client(&mut sm, &io, client_id);
    sm.take_outbox();
    seed_visible_row(&mut sm, &mut io, "users", row.clone());

    set_client_query_scope(
        &mut sm,
        &io,
        client_id,
        QueryId(1),
        HashSet::from([(row_id, BranchName::new("main"))]),
        None,
    );

    let initial = sm.take_outbox();
    assert!(initial.iter().any(|entry| matches!(
        entry,
        OutboxEntry {
            destination: Destination::Client(id),
            payload: SyncPayload::RowBatchNeeded { row: needed, .. },
        } if *id == client_id && needed.row_id == row_id
    )));

    sm.process_from_server(
        &mut io,
        ServerId::new(),
        SyncPayload::RowBatchStateChanged {
            row_id,
            branch_name: BranchName::new("main"),
            batch_id,
            state: None,
            confirmed_tier: Some(DurabilityTier::Worker),
        },
    );

    assert!(sm.take_outbox().into_iter().any(|entry| matches!(
        entry,
        OutboxEntry {
            destination: Destination::Client(id),
            payload:
                SyncPayload::RowBatchStateChanged {
                    row_id: changed_row_id,
                    batch_id: changed_batch_id,
                    confirmed_tier: Some(DurabilityTier::Worker),
                    ..
                },
        } if id == client_id && changed_row_id == row_id && changed_batch_id == batch_id
    )));
}

#[test]
fn initial_query_sync_replays_current_direct_batch_settlement() {
    let mut sm = SyncManager::new();
    let mut io = MemoryStorage::new();
    let client_id = ClientId::new();
    let row_id = ObjectId::new();
    let mut row = visible_row(row_id, "main", Vec::new(), 1_000, b"alice");
    row.confirmed_tier = Some(DurabilityTier::Worker);

    add_client(&mut sm, &io, client_id);
    sm.take_outbox();
    seed_visible_row(&mut sm, &mut io, "users", row.clone());

    set_client_query_scope(
        &mut sm,
        &io,
        client_id,
        QueryId(1),
        HashSet::from([(row_id, BranchName::new("main"))]),
        None,
    );

    let outbox = sm.take_outbox();
    assert!(outbox.iter().any(|entry| matches!(
        entry,
        OutboxEntry {
            destination: Destination::Client(id),
            payload: SyncPayload::BatchSettlement { settlement },
        } if *id == client_id && *settlement == BatchSettlement::DurableDirect {
            batch_id: row.batch_id,
            confirmed_tier: DurabilityTier::Worker,
            visible_members: vec![VisibleBatchMember {
                object_id: row_id,
                branch_name: BranchName::new("main"),
                batch_id: row.batch_id,
            }],
        }
    )));
}

#[test]
fn initial_query_sync_sends_only_current_row_for_deep_history() {
    let mut sm = SyncManager::new();
    let mut io = MemoryStorage::new();
    let client_id = ClientId::new();
    let row_id = ObjectId::new();
    let older = visible_row(row_id, "main", Vec::new(), 1_000, b"older");
    let newer = visible_row(row_id, "main", vec![older.batch_id()], 2_000, b"newer");

    add_client(&mut sm, &io, client_id);
    sm.take_outbox();
    seed_users_schema(&mut io);
    create_test_row_with_id(&mut io, row_id, Some(row_metadata("users")));
    io.append_history_region_rows("users", &[older.clone(), newer.clone()])
        .unwrap();
    io.upsert_visible_region_rows(
        "users",
        std::slice::from_ref(&VisibleRowEntry::rebuild(
            newer.clone(),
            &[older.clone(), newer.clone()],
        )),
    )
    .unwrap();

    set_client_query_scope(
        &mut sm,
        &io,
        client_id,
        QueryId(1),
        HashSet::from([(row_id, BranchName::new("main"))]),
        None,
    );

    let row_payloads: Vec<_> = sm
        .take_outbox()
        .into_iter()
        .filter_map(|entry| match entry {
            OutboxEntry {
                destination: Destination::Client(id),
                payload: SyncPayload::RowBatchNeeded { row, .. },
            } if id == client_id => Some(row),
            _ => None,
        })
        .collect();

    assert_eq!(
        row_payloads.len(),
        1,
        "initial sync should send only the current row"
    );
    assert_eq!(row_payloads[0].batch_id(), newer.batch_id());
    assert_eq!(row_payloads[0].data, newer.data);
}

#[test]
fn initial_query_sync_replays_current_accepted_transaction_settlement() {
    let mut sm = SyncManager::new();
    let mut io = MemoryStorage::new();
    let client_id = ClientId::new();
    let row_id = ObjectId::new();
    let row = row_with_state(
        visible_row(row_id, "main", Vec::new(), 1_000, b"alice"),
        crate::row_histories::RowState::VisibleTransactional,
        Some(DurabilityTier::Worker),
    );

    add_client(&mut sm, &io, client_id);
    sm.take_outbox();
    seed_visible_row(&mut sm, &mut io, "users", row.clone());

    set_client_query_scope(
        &mut sm,
        &io,
        client_id,
        QueryId(1),
        HashSet::from([(row_id, BranchName::new("main"))]),
        None,
    );

    let outbox = sm.take_outbox();
    assert!(outbox.iter().any(|entry| matches!(
        entry,
        OutboxEntry {
            destination: Destination::Client(id),
            payload: SyncPayload::BatchSettlement { settlement },
        } if *id == client_id && *settlement == BatchSettlement::AcceptedTransaction {
            batch_id: row.batch_id,
            confirmed_tier: DurabilityTier::Worker,
            visible_members: vec![VisibleBatchMember {
                object_id: row_id,
                branch_name: BranchName::new("main"),
                batch_id: row.batch_id,
            }],
        }
    )));
}

#[test]
fn batch_settlement_needed_returns_current_accepted_transaction() {
    let mut sm = SyncManager::new();
    let mut io = MemoryStorage::new();
    let client_id = ClientId::new();
    let row_id = ObjectId::new();
    let row = row_with_state(
        visible_row(row_id, "main", Vec::new(), 1_000, b"alice"),
        crate::row_histories::RowState::VisibleTransactional,
        Some(DurabilityTier::Worker),
    );

    add_client(&mut sm, &io, client_id);
    sm.take_outbox();
    seed_visible_row(&mut sm, &mut io, "users", row.clone());

    sm.process_from_client(
        &mut io,
        client_id,
        SyncPayload::BatchSettlementNeeded {
            batch_ids: vec![row.batch_id],
        },
    );

    assert!(sm.take_outbox().into_iter().any(|entry| matches!(
        entry,
        OutboxEntry {
            destination: Destination::Client(id),
            payload: SyncPayload::BatchSettlement { settlement },
        } if id == client_id && settlement == BatchSettlement::AcceptedTransaction {
            batch_id: row.batch_id,
            confirmed_tier: DurabilityTier::Worker,
            visible_members: vec![VisibleBatchMember {
                object_id: row_id,
                branch_name: BranchName::new("main"),
                batch_id: row.batch_id,
            }],
        }
    )));
}

#[test]
fn batch_settlement_needed_returns_missing_for_unknown_batch() {
    let mut sm = SyncManager::new();
    let mut io = MemoryStorage::new();
    let client_id = ClientId::new();
    let batch_id = crate::row_histories::BatchId::new();

    add_client(&mut sm, &io, client_id);
    sm.take_outbox();

    sm.process_from_client(
        &mut io,
        client_id,
        SyncPayload::BatchSettlementNeeded {
            batch_ids: vec![batch_id],
        },
    );

    assert!(sm.take_outbox().into_iter().any(|entry| matches!(
        entry,
        OutboxEntry {
            destination: Destination::Client(id),
            payload: SyncPayload::BatchSettlement { settlement },
        } if id == client_id && settlement == BatchSettlement::Missing { batch_id }
    )));
}

#[test]
fn batch_settlement_needed_returns_persisted_rejected_without_visible_rows() {
    let mut sm = SyncManager::new();
    let mut io = MemoryStorage::new();
    let client_id = ClientId::new();
    let batch_id = crate::row_histories::BatchId::new();
    let settlement = BatchSettlement::Rejected {
        batch_id,
        code: "permission_denied".to_string(),
        reason: "writer lacks publish rights".to_string(),
    };

    add_client(&mut sm, &io, client_id);
    sm.take_outbox();
    io.upsert_authoritative_batch_settlement(&settlement)
        .unwrap();

    sm.process_from_client(
        &mut io,
        client_id,
        SyncPayload::BatchSettlementNeeded {
            batch_ids: vec![batch_id],
        },
    );

    assert!(sm.take_outbox().into_iter().any(|entry| matches!(
        entry,
        OutboxEntry {
            destination: Destination::Client(id),
            payload: SyncPayload::BatchSettlement { settlement: returned },
        } if id == client_id && returned == settlement
    )));
}

#[test]
fn row_batch_state_changed_relays_direct_batch_settlement_to_interested_clients() {
    let mut sm = SyncManager::new();
    let mut io = MemoryStorage::new();
    let client_id = ClientId::new();
    let row_id = ObjectId::new();
    let row = visible_row(row_id, "main", Vec::new(), 1_000, b"alice");
    let batch_id = row.batch_id;

    add_client(&mut sm, &io, client_id);
    sm.take_outbox();
    seed_visible_row(&mut sm, &mut io, "users", row.clone());

    set_client_query_scope(
        &mut sm,
        &io,
        client_id,
        QueryId(1),
        HashSet::from([(row_id, BranchName::new("main"))]),
        None,
    );
    let _ = sm.take_outbox();

    sm.process_from_server(
        &mut io,
        ServerId::new(),
        SyncPayload::RowBatchStateChanged {
            row_id,
            branch_name: BranchName::new("main"),
            batch_id,
            state: None,
            confirmed_tier: Some(DurabilityTier::Worker),
        },
    );

    assert!(sm.take_outbox().into_iter().any(|entry| matches!(
        entry,
        OutboxEntry {
            destination: Destination::Client(id),
            payload: SyncPayload::BatchSettlement { settlement },
        } if id == client_id && settlement == BatchSettlement::DurableDirect {
            batch_id: row.batch_id,
            confirmed_tier: DurabilityTier::Worker,
            visible_members: vec![VisibleBatchMember {
                object_id: row_id,
                branch_name: BranchName::new("main"),
                batch_id: row.batch_id,
            }],
        }
    )));
}

#[test]
fn row_batch_state_changed_relays_accepted_transaction_settlement_to_interested_clients() {
    let mut sm = SyncManager::new();
    let mut io = MemoryStorage::new();
    let client_id = ClientId::new();
    let row_id = ObjectId::new();
    let row = row_with_state(
        visible_row(row_id, "main", Vec::new(), 1_000, b"alice"),
        crate::row_histories::RowState::VisibleTransactional,
        None,
    );
    let batch_id = row.batch_id;

    add_client(&mut sm, &io, client_id);
    sm.take_outbox();
    seed_visible_row(&mut sm, &mut io, "users", row.clone());

    set_client_query_scope(
        &mut sm,
        &io,
        client_id,
        QueryId(1),
        HashSet::from([(row_id, BranchName::new("main"))]),
        None,
    );
    let _ = sm.take_outbox();

    sm.process_from_server(
        &mut io,
        ServerId::new(),
        SyncPayload::RowBatchStateChanged {
            row_id,
            branch_name: BranchName::new("main"),
            batch_id,
            state: None,
            confirmed_tier: Some(DurabilityTier::Worker),
        },
    );

    assert!(sm.take_outbox().into_iter().any(|entry| matches!(
        entry,
        OutboxEntry {
            destination: Destination::Client(id),
            payload: SyncPayload::BatchSettlement { settlement },
        } if id == client_id && settlement == BatchSettlement::AcceptedTransaction {
            batch_id: row.batch_id,
            confirmed_tier: DurabilityTier::Worker,
            visible_members: vec![VisibleBatchMember {
                object_id: row_id,
                branch_name: BranchName::new("main"),
                batch_id: row.batch_id,
            }],
        }
    )));
}

#[test]
fn row_batch_state_changed_persists_accepted_transaction_tier_upgrade_authoritatively() {
    let mut sm = SyncManager::new();
    let mut io = MemoryStorage::new();
    let row_id = ObjectId::new();
    let row = row_with_state(
        visible_row(row_id, "main", Vec::new(), 1_000, b"alice"),
        crate::row_histories::RowState::VisibleTransactional,
        Some(DurabilityTier::Worker),
    );
    let batch_id = row.batch_id;

    seed_visible_row(&mut sm, &mut io, "users", row.clone());
    io.upsert_authoritative_batch_settlement(&BatchSettlement::AcceptedTransaction {
        batch_id: row.batch_id,
        confirmed_tier: DurabilityTier::Worker,
        visible_members: vec![VisibleBatchMember {
            object_id: row_id,
            branch_name: BranchName::new("main"),
            batch_id: row.batch_id,
        }],
    })
    .unwrap();

    sm.process_from_server(
        &mut io,
        ServerId::new(),
        SyncPayload::RowBatchStateChanged {
            row_id,
            branch_name: BranchName::new("main"),
            batch_id,
            state: None,
            confirmed_tier: Some(DurabilityTier::EdgeServer),
        },
    );

    assert_eq!(
        io.load_authoritative_batch_settlement(row.batch_id)
            .unwrap(),
        Some(BatchSettlement::AcceptedTransaction {
            batch_id: row.batch_id,
            confirmed_tier: DurabilityTier::EdgeServer,
            visible_members: vec![VisibleBatchMember {
                object_id: row_id,
                branch_name: BranchName::new("main"),
                batch_id: row.batch_id,
            }],
        })
    );
}

#[test]
fn row_batch_state_changed_stops_relaying_after_scope_removal() {
    let mut sm = SyncManager::new();
    let mut io = MemoryStorage::new();
    let client_id = ClientId::new();
    let row_id = ObjectId::new();
    let row = visible_row(row_id, "main", Vec::new(), 1_000, b"alice");

    add_client(&mut sm, &io, client_id);
    sm.take_outbox();
    seed_visible_row(&mut sm, &mut io, "users", row.clone());

    set_client_query_scope(
        &mut sm,
        &io,
        client_id,
        QueryId(1),
        HashSet::from([(row_id, BranchName::new("main"))]),
        None,
    );
    let _ = sm.take_outbox();

    set_client_query_scope(&mut sm, &io, client_id, QueryId(1), HashSet::new(), None);
    sm.process_from_server(
        &mut io,
        ServerId::new(),
        SyncPayload::RowBatchStateChanged {
            row_id,
            branch_name: BranchName::new("main"),
            batch_id: row.batch_id,
            state: None,
            confirmed_tier: Some(DurabilityTier::Worker),
        },
    );

    assert!(sm.take_outbox().into_iter().all(|entry| !matches!(
        entry,
        OutboxEntry {
            destination: Destination::Client(id),
            payload: SyncPayload::RowBatchStateChanged { row_id: changed_row_id, .. },
        } if id == client_id && changed_row_id == row_id
    )));
}

#[test]
fn stale_row_batch_from_client_replays_upstream_without_regressing_visible_row() {
    let mut sm = SyncManager::new();
    let mut io = MemoryStorage::new();
    let client_id = ClientId::new();
    let server_id = ServerId::new();
    let row_id = ObjectId::new();

    add_client(&mut sm, &io, client_id);
    sm.set_client_role(client_id, ClientRole::Peer);
    add_server(&mut sm, &io, server_id);

    let newer = visible_row(row_id, "main", Vec::new(), 2_000, b"newer");
    seed_visible_row(&mut sm, &mut io, "users", newer.clone());

    let older = visible_row(row_id, "main", Vec::new(), 1_000, b"older");
    sm.process_from_client(
        &mut io,
        client_id,
        SyncPayload::RowBatchCreated {
            metadata: Some(RowMetadata {
                id: row_id,
                metadata: row_metadata("users"),
            }),
            row: older.clone(),
        },
    );

    let visible = load_visible_row(&io, "users", row_id, "main");
    assert_eq!(visible.batch_id(), newer.batch_id());

    assert!(sm.take_outbox().into_iter().any(|entry| matches!(
        entry,
        OutboxEntry {
            destination: Destination::Server(id),
            payload: SyncPayload::RowBatchCreated { row, .. },
        } if id == server_id && row.batch_id() == older.batch_id()
    )));
}

#[test]
fn transactional_row_from_client_stays_staged_until_batch_is_sealed() {
    let mut sm = SyncManager::new().with_durability_tier(DurabilityTier::Worker);
    let mut io = MemoryStorage::new();
    let client_id = ClientId::new();
    let row_id = ObjectId::new();
    let row = row_with_state(
        visible_row(row_id, "main", Vec::new(), 1_000, b"alice"),
        crate::row_histories::RowState::StagingPending,
        None,
    );
    seed_users_schema(&mut io);

    add_client(&mut sm, &io, client_id);
    sm.set_client_role(client_id, ClientRole::Peer);
    sm.take_outbox();

    sm.process_from_client(
        &mut io,
        client_id,
        SyncPayload::RowBatchCreated {
            metadata: Some(RowMetadata {
                id: row_id,
                metadata: row_metadata("users"),
            }),
            row: row.clone(),
        },
    );

    let history_rows = io.scan_history_row_batches("users", row_id).unwrap();
    assert_eq!(history_rows.len(), 1);
    assert_eq!(
        history_rows[0].state,
        crate::row_histories::RowState::StagingPending
    );
    assert_eq!(
        io.load_visible_region_row("users", "main", row_id).unwrap(),
        None,
        "staging rows should not become visible until the batch is sealed"
    );
    assert_eq!(
        io.load_authoritative_batch_settlement(row.batch_id)
            .unwrap(),
        None,
        "authority should not decide a transactional batch before it is sealed"
    );

    assert!(sm.take_outbox().into_iter().all(|entry| !matches!(
        entry,
        OutboxEntry {
            destination: Destination::Client(id),
            payload:
                SyncPayload::BatchSettlement { .. }
                | SyncPayload::RowBatchStateChanged { .. },
        } if id == client_id
    )));
}

#[test]
fn seal_batch_accepts_all_staged_transactional_rows_as_one_settlement() {
    let mut sm = SyncManager::new().with_durability_tier(DurabilityTier::Worker);
    let mut io = MemoryStorage::new();
    let client_id = ClientId::new();
    let batch_id = crate::row_histories::BatchId::new();
    let first_row_id = ObjectId::new();
    let second_row_id = ObjectId::new();
    seed_users_schema(&mut io);

    add_client(&mut sm, &io, client_id);
    sm.set_client_role(client_id, ClientRole::Peer);
    sm.take_outbox();

    let first_row = row_with_batch_state(
        visible_row(first_row_id, "main", Vec::new(), 1_000, b"alice"),
        batch_id,
        crate::row_histories::RowState::StagingPending,
        None,
    );
    let second_row = row_with_batch_state(
        visible_row(second_row_id, "main", Vec::new(), 1_100, b"bob"),
        batch_id,
        crate::row_histories::RowState::StagingPending,
        None,
    );

    for row in [first_row.clone(), second_row.clone()] {
        sm.process_from_client(
            &mut io,
            client_id,
            SyncPayload::RowBatchCreated {
                metadata: Some(RowMetadata {
                    id: row.row_id,
                    metadata: row_metadata("users"),
                }),
                row,
            },
        );
    }
    sm.take_outbox();

    sm.process_from_client(
        &mut io,
        client_id,
        SyncPayload::SealBatch {
            submission: sealed_submission(
                batch_id,
                "main",
                vec![
                    SealedBatchMember {
                        object_id: first_row_id,
                        row_digest: first_row.content_digest(),
                    },
                    SealedBatchMember {
                        object_id: second_row_id,
                        row_digest: second_row.content_digest(),
                    },
                ],
                Vec::new(),
            ),
        },
    );

    let settlement = io
        .load_authoritative_batch_settlement(batch_id)
        .unwrap()
        .expect("sealed transactional batch should persist an authoritative settlement");
    let BatchSettlement::AcceptedTransaction {
        batch_id: settled_batch_id,
        confirmed_tier,
        visible_members,
    } = settlement
    else {
        panic!("expected accepted transactional settlement, got {settlement:?}");
    };
    assert_eq!(settled_batch_id, batch_id);
    assert_eq!(confirmed_tier, DurabilityTier::Worker);
    assert_eq!(visible_members.len(), 2);
    assert!(visible_members.contains(&VisibleBatchMember {
        object_id: first_row_id,
        branch_name: BranchName::new("main"),
        batch_id,
    }));
    assert!(visible_members.contains(&VisibleBatchMember {
        object_id: second_row_id,
        branch_name: BranchName::new("main"),
        batch_id,
    }));

    let first_visible = io
        .load_visible_region_row("users", "main", first_row_id)
        .unwrap()
        .expect("first row should become visible after seal");
    let second_visible = io
        .load_visible_region_row("users", "main", second_row_id)
        .unwrap()
        .expect("second row should become visible after seal");
    assert_eq!(
        first_visible.state,
        crate::row_histories::RowState::VisibleTransactional
    );
    assert_eq!(
        second_visible.state,
        crate::row_histories::RowState::VisibleTransactional
    );

    let outbox = sm.take_outbox();
    assert!(outbox.iter().any(|entry| matches!(
        entry,
        OutboxEntry {
            destination: Destination::Client(id),
            payload: SyncPayload::BatchSettlement { settlement: returned },
        } if *id == client_id && *returned == BatchSettlement::AcceptedTransaction {
            batch_id,
            confirmed_tier: DurabilityTier::Worker,
            visible_members: visible_members.clone(),
        }
    )));
}

#[test]
fn seal_batch_collapses_same_row_to_latest_visible_member() {
    let mut sm = SyncManager::new().with_durability_tier(DurabilityTier::Worker);
    let mut io = MemoryStorage::new();
    let client_id = ClientId::new();
    let batch_id = crate::row_histories::BatchId::new();
    let row_id = ObjectId::new();
    seed_users_schema(&mut io);

    add_client(&mut sm, &io, client_id);
    sm.set_client_role(client_id, ClientRole::Peer);
    sm.take_outbox();

    // client
    //   first staged version  -> same row, same batch
    //   second staged version -> same row, same batch
    //   seal batch
    //
    // authority
    //   settles one visible member for that row
    //   publishes only the latest staged content
    let first_row = row_with_batch_state(
        visible_row(row_id, "main", Vec::new(), 1_000, b"alice"),
        batch_id,
        crate::row_histories::RowState::StagingPending,
        None,
    );

    let second_row = row_with_batch_state(
        visible_row(row_id, "main", Vec::new(), 1_100, b"alice-updated"),
        batch_id,
        crate::row_histories::RowState::StagingPending,
        None,
    );

    for row in [first_row.clone(), second_row.clone()] {
        sm.process_from_client(
            &mut io,
            client_id,
            SyncPayload::RowBatchCreated {
                metadata: Some(RowMetadata {
                    id: row.row_id,
                    metadata: row_metadata("users"),
                }),
                row,
            },
        );
    }
    sm.take_outbox();

    sm.process_from_client(
        &mut io,
        client_id,
        SyncPayload::SealBatch {
            submission: sealed_submission(
                batch_id,
                "main",
                vec![SealedBatchMember {
                    object_id: row_id,
                    row_digest: second_row.content_digest(),
                }],
                Vec::new(),
            ),
        },
    );

    let settlement = io
        .load_authoritative_batch_settlement(batch_id)
        .unwrap()
        .expect("sealed transactional batch should persist an authoritative settlement");
    let BatchSettlement::AcceptedTransaction {
        visible_members, ..
    } = settlement
    else {
        panic!("expected accepted transactional settlement, got {settlement:?}");
    };
    assert_eq!(
        visible_members,
        vec![VisibleBatchMember {
            object_id: row_id,
            branch_name: BranchName::new("main"),
            batch_id,
        }]
    );

    let visible = io
        .load_visible_region_row("users", "main", row_id)
        .unwrap()
        .expect("latest row should become visible after seal");
    assert_eq!(visible.batch_id(), batch_id);
    assert!(visible.parents.is_empty());
    assert_eq!(visible.data, second_row.data);
    assert_eq!(visible.batch_id, batch_id);
    assert_eq!(
        visible.state,
        crate::row_histories::RowState::VisibleTransactional
    );

    let history_rows = io.scan_history_row_batches("users", row_id).unwrap();
    assert_eq!(history_rows.len(), 1);
    assert_eq!(history_rows[0].batch_id(), batch_id);
    assert_eq!(
        history_rows[0].state,
        crate::row_histories::RowState::VisibleTransactional
    );
    assert_eq!(history_rows[0].data, second_row.data);

    let outbox = sm.take_outbox();
    assert!(outbox.iter().any(|entry| matches!(
        entry,
        OutboxEntry {
            destination: Destination::Client(id),
            payload: SyncPayload::RowBatchStateChanged {
                row_id: changed_row_id,
                branch_name,
                batch_id: changed_batch_id,
                state: Some(crate::row_histories::RowState::VisibleTransactional),
                confirmed_tier: Some(DurabilityTier::Worker),
            },
        } if *id == client_id
            && *changed_row_id == row_id
            && *branch_name == BranchName::new("main")
            && *changed_batch_id == batch_id
    )));
    assert!(!outbox.iter().any(|entry| matches!(
        entry,
        OutboxEntry {
            destination: Destination::Client(id),
            payload: SyncPayload::RowBatchNeeded { row, .. },
        } if *id == client_id && row.row_id == row_id
    )));
}

#[test]
fn seal_batch_same_row_preserves_pre_transaction_parent_frontier() {
    let mut sm = SyncManager::new().with_durability_tier(DurabilityTier::Worker);
    let mut io = MemoryStorage::new();
    let client_id = ClientId::new();
    let batch_id = crate::row_histories::BatchId::new();
    let row_id = ObjectId::new();
    let base_row = visible_row(row_id, "main", Vec::new(), 900, b"base");
    seed_visible_row(&mut sm, &mut io, "users", base_row.clone());

    add_client(&mut sm, &io, client_id);
    sm.set_client_role(client_id, ClientRole::Peer);
    sm.take_outbox();

    let first_row = row_with_batch_state(
        visible_row(row_id, "main", vec![base_row.batch_id()], 1_000, b"alice"),
        batch_id,
        crate::row_histories::RowState::StagingPending,
        None,
    );

    let second_row = row_with_batch_state(
        visible_row(
            row_id,
            "main",
            vec![base_row.batch_id()],
            1_100,
            b"alice-updated",
        ),
        batch_id,
        crate::row_histories::RowState::StagingPending,
        None,
    );

    for row in [first_row.clone(), second_row.clone()] {
        sm.process_from_client(
            &mut io,
            client_id,
            SyncPayload::RowBatchCreated {
                metadata: Some(RowMetadata {
                    id: row.row_id,
                    metadata: row_metadata("users"),
                }),
                row,
            },
        );
    }
    sm.take_outbox();

    sm.process_from_client(
        &mut io,
        client_id,
        SyncPayload::SealBatch {
            submission: sealed_submission(
                batch_id,
                "main",
                vec![SealedBatchMember {
                    object_id: row_id,
                    row_digest: second_row.content_digest(),
                }],
                vec![CapturedFrontierMember {
                    object_id: row_id,
                    branch_name: BranchName::new("main"),
                    batch_id: base_row.batch_id(),
                }],
            ),
        },
    );

    let visible = io
        .load_visible_region_row("users", "main", row_id)
        .unwrap()
        .expect("sealed batch should publish the accepted row");
    assert_eq!(visible.batch_id(), batch_id);
    assert_eq!(visible.parents.as_slice(), [base_row.batch_id()]);
    assert_eq!(visible.data, second_row.data);
}

#[test]
fn seal_batch_waits_for_all_declared_rows_before_accepting() {
    let mut sm = SyncManager::new().with_durability_tier(DurabilityTier::Worker);
    let mut io = MemoryStorage::new();
    let client_id = ClientId::new();
    let batch_id = crate::row_histories::BatchId::new();
    let first_row_id = ObjectId::new();
    let second_row_id = ObjectId::new();
    seed_users_schema(&mut io);

    add_client(&mut sm, &io, client_id);
    sm.set_client_role(client_id, ClientRole::Peer);
    sm.take_outbox();

    let first_row = row_with_batch_state(
        visible_row(first_row_id, "main", Vec::new(), 1_000, b"alice"),
        batch_id,
        crate::row_histories::RowState::StagingPending,
        None,
    );
    let second_row = row_with_batch_state(
        visible_row(second_row_id, "main", Vec::new(), 1_100, b"bob"),
        batch_id,
        crate::row_histories::RowState::StagingPending,
        None,
    );
    let first_row_batch_id = first_row.content_digest();
    let second_row_batch_id = second_row.content_digest();

    sm.process_from_client(
        &mut io,
        client_id,
        SyncPayload::RowBatchCreated {
            metadata: Some(RowMetadata {
                id: first_row.row_id,
                metadata: row_metadata("users"),
            }),
            row: first_row,
        },
    );
    sm.take_outbox();

    sm.process_from_client(
        &mut io,
        client_id,
        SyncPayload::SealBatch {
            submission: sealed_submission(
                batch_id,
                "main",
                vec![
                    SealedBatchMember {
                        object_id: first_row_id,
                        row_digest: first_row_batch_id,
                    },
                    SealedBatchMember {
                        object_id: second_row_id,
                        row_digest: second_row_batch_id,
                    },
                ],
                Vec::new(),
            ),
        },
    );

    assert_eq!(
        io.load_authoritative_batch_settlement(batch_id).unwrap(),
        None,
        "authority should wait for all declared rows before settling the batch"
    );
    assert_eq!(
        io.load_visible_region_row("users", "main", first_row_id)
            .unwrap(),
        None,
        "partial sealed batches should not publish visible rows yet"
    );

    sm.process_from_client(
        &mut io,
        client_id,
        SyncPayload::RowBatchCreated {
            metadata: Some(RowMetadata {
                id: second_row.row_id,
                metadata: row_metadata("users"),
            }),
            row: second_row,
        },
    );

    let settlement = io
        .load_authoritative_batch_settlement(batch_id)
        .unwrap()
        .expect("authority should settle once all declared rows have arrived");
    let BatchSettlement::AcceptedTransaction {
        visible_members, ..
    } = settlement
    else {
        panic!("expected accepted transactional settlement, got {settlement:?}");
    };
    assert_eq!(visible_members.len(), 2);
}

#[test]
fn seal_batch_waits_for_declared_latest_row_batch_before_accepting() {
    let mut sm = SyncManager::new().with_durability_tier(DurabilityTier::Worker);
    let mut io = MemoryStorage::new();
    let client_id = ClientId::new();
    let batch_id = crate::row_histories::BatchId::new();
    let row_id = ObjectId::new();
    seed_users_schema(&mut io);

    add_client(&mut sm, &io, client_id);
    sm.set_client_role(client_id, ClientRole::Peer);
    sm.take_outbox();

    let first_row = row_with_batch_state(
        visible_row(row_id, "main", Vec::new(), 1_000, b"alice"),
        batch_id,
        crate::row_histories::RowState::StagingPending,
        None,
    );

    let second_row = row_with_batch_state(
        visible_row(row_id, "main", Vec::new(), 1_100, b"alice-updated"),
        batch_id,
        crate::row_histories::RowState::StagingPending,
        None,
    );

    sm.process_from_client(
        &mut io,
        client_id,
        SyncPayload::RowBatchCreated {
            metadata: Some(RowMetadata {
                id: first_row.row_id,
                metadata: row_metadata("users"),
            }),
            row: first_row.clone(),
        },
    );
    sm.take_outbox();

    sm.process_from_client(
        &mut io,
        client_id,
        SyncPayload::SealBatch {
            submission: sealed_submission(
                batch_id,
                "main",
                vec![SealedBatchMember {
                    object_id: row_id,
                    row_digest: second_row.content_digest(),
                }],
                Vec::new(),
            ),
        },
    );

    assert_eq!(
        io.load_authoritative_batch_settlement(batch_id).unwrap(),
        None,
        "authority should wait for the declared final row batch entry, not just any row for that object"
    );
    assert_eq!(
        io.load_visible_region_row("users", "main", row_id).unwrap(),
        None,
        "earlier staged versions should not become visible just because the object id was declared"
    );

    sm.process_from_client(
        &mut io,
        client_id,
        SyncPayload::RowBatchCreated {
            metadata: Some(RowMetadata {
                id: second_row.row_id,
                metadata: row_metadata("users"),
            }),
            row: second_row.clone(),
        },
    );

    let settlement = io
        .load_authoritative_batch_settlement(batch_id)
        .unwrap()
        .expect("authority should settle once the declared final row batch entry arrives");
    let BatchSettlement::AcceptedTransaction {
        visible_members, ..
    } = settlement
    else {
        panic!("expected accepted transactional settlement, got {settlement:?}");
    };
    assert_eq!(
        visible_members,
        vec![VisibleBatchMember {
            object_id: row_id,
            branch_name: BranchName::new("main"),
            batch_id,
        }]
    );

    let visible = io
        .load_visible_region_row("users", "main", row_id)
        .unwrap()
        .expect("declared final row batch entry should become visible");
    assert_eq!(visible.batch_id(), batch_id);
}

#[test]
fn same_row_staging_in_one_batch_keeps_only_latest_live_pending_member_before_seal() {
    let mut sm = SyncManager::new().with_durability_tier(DurabilityTier::Worker);
    let mut io = MemoryStorage::new();
    let client_id = ClientId::new();
    let batch_id = crate::row_histories::BatchId::new();
    let row_id = ObjectId::new();
    seed_users_schema(&mut io);

    add_client(&mut sm, &io, client_id);
    sm.set_client_role(client_id, ClientRole::Peer);
    sm.take_outbox();

    let first_row = row_with_batch_state(
        visible_row(row_id, "main", Vec::new(), 1_000, b"alice"),
        batch_id,
        crate::row_histories::RowState::StagingPending,
        None,
    );

    let second_row = row_with_batch_state(
        visible_row(row_id, "main", Vec::new(), 1_100, b"alice-updated"),
        batch_id,
        crate::row_histories::RowState::StagingPending,
        None,
    );

    sm.process_from_client(
        &mut io,
        client_id,
        SyncPayload::RowBatchCreated {
            metadata: Some(RowMetadata {
                id: first_row.row_id,
                metadata: row_metadata("users"),
            }),
            row: first_row.clone(),
        },
    );
    sm.process_from_client(
        &mut io,
        client_id,
        SyncPayload::RowBatchCreated {
            metadata: Some(RowMetadata {
                id: second_row.row_id,
                metadata: row_metadata("users"),
            }),
            row: second_row.clone(),
        },
    );

    let history_rows = io.scan_history_row_batches("users", row_id).unwrap();
    let live_pending_rows: Vec<_> = history_rows
        .iter()
        .filter(|row| matches!(row.state, crate::row_histories::RowState::StagingPending))
        .collect();
    assert_eq!(
        live_pending_rows.len(),
        1,
        "authority staging should keep one live pending member for a same-row batch rewrite"
    );
    assert_eq!(history_rows.len(), 1);
    assert_eq!(live_pending_rows[0].batch_id(), batch_id);
    assert_eq!(live_pending_rows[0].data, second_row.data);
    assert_eq!(
        io.load_visible_region_row("users", "main", row_id).unwrap(),
        None,
        "pre-seal transactional rewrites should remain non-visible"
    );
}

#[test]
fn seal_batch_rejects_members_spanning_multiple_target_branches() {
    let mut sm = SyncManager::new().with_durability_tier(DurabilityTier::Worker);
    let mut io = MemoryStorage::new();
    let client_id = ClientId::new();
    let batch_id = crate::row_histories::BatchId::new();
    let main_row_id = ObjectId::new();
    let draft_row_id = ObjectId::new();
    seed_users_schema(&mut io);

    add_client(&mut sm, &io, client_id);
    sm.set_client_role(client_id, ClientRole::Peer);
    sm.take_outbox();

    let main_row = row_with_batch_state(
        visible_row(main_row_id, "main", Vec::new(), 1_000, b"alice"),
        batch_id,
        crate::row_histories::RowState::StagingPending,
        None,
    );

    let draft_row = row_with_batch_state(
        visible_row(draft_row_id, "draft", Vec::new(), 1_100, b"bob"),
        batch_id,
        crate::row_histories::RowState::StagingPending,
        None,
    );

    sm.process_from_client(
        &mut io,
        client_id,
        SyncPayload::RowBatchCreated {
            metadata: Some(RowMetadata {
                id: main_row.row_id,
                metadata: row_metadata("users"),
            }),
            row: main_row.clone(),
        },
    );
    sm.process_from_client(
        &mut io,
        client_id,
        SyncPayload::RowBatchCreated {
            metadata: Some(RowMetadata {
                id: draft_row.row_id,
                metadata: row_metadata("users"),
            }),
            row: draft_row.clone(),
        },
    );
    sm.take_outbox();

    sm.process_from_client(
        &mut io,
        client_id,
        SyncPayload::SealBatch {
            submission: sealed_submission(
                batch_id,
                "main",
                vec![
                    SealedBatchMember {
                        object_id: main_row_id,
                        row_digest: main_row.content_digest(),
                    },
                    SealedBatchMember {
                        object_id: draft_row_id,
                        row_digest: draft_row.content_digest(),
                    },
                ],
                Vec::new(),
            ),
        },
    );

    assert_eq!(
        io.load_authoritative_batch_settlement(batch_id).unwrap(),
        Some(BatchSettlement::Rejected {
            batch_id,
            code: "invalid_batch_submission".to_string(),
            reason: "sealed transactional batch rows must belong to the declared target branch"
                .to_string(),
        })
    );
    assert_eq!(
        io.load_visible_region_row("users", "main", main_row_id)
            .unwrap(),
        None
    );
    assert_eq!(
        io.load_visible_region_row("users", "draft", draft_row_id)
            .unwrap(),
        None
    );

    let main_history_rows = io.scan_history_row_batches("users", main_row_id).unwrap();
    let draft_history_rows = io.scan_history_row_batches("users", draft_row_id).unwrap();
    assert_eq!(main_history_rows.len(), 1);
    assert_eq!(draft_history_rows.len(), 1);
    assert_eq!(
        main_history_rows[0].state,
        crate::row_histories::RowState::Rejected
    );
    assert_eq!(
        draft_history_rows[0].state,
        crate::row_histories::RowState::Rejected
    );
}

#[test]
fn seal_batch_rejects_when_batch_digest_does_not_match_members() {
    let mut sm = SyncManager::new().with_durability_tier(DurabilityTier::Worker);
    let mut io = MemoryStorage::new();
    let client_id = ClientId::new();
    let batch_id = crate::row_histories::BatchId::new();
    let row_id = ObjectId::new();
    seed_users_schema(&mut io);

    add_client(&mut sm, &io, client_id);
    sm.set_client_role(client_id, ClientRole::Peer);
    sm.take_outbox();

    let staged_row = row_with_batch_state(
        visible_row(row_id, "main", Vec::new(), 1_000, b"alice"),
        batch_id,
        crate::row_histories::RowState::StagingPending,
        None,
    );

    sm.process_from_client(
        &mut io,
        client_id,
        SyncPayload::RowBatchCreated {
            metadata: Some(RowMetadata {
                id: staged_row.row_id,
                metadata: row_metadata("users"),
            }),
            row: staged_row.clone(),
        },
    );
    sm.take_outbox();

    let mut submission = sealed_submission(
        batch_id,
        "main",
        vec![SealedBatchMember {
            object_id: row_id,
            row_digest: staged_row.content_digest(),
        }],
        Vec::new(),
    );
    submission.batch_digest = crate::digest::Digest32([255; 32]);

    sm.process_from_client(&mut io, client_id, SyncPayload::SealBatch { submission });

    assert_eq!(
        io.load_authoritative_batch_settlement(batch_id).unwrap(),
        Some(BatchSettlement::Rejected {
            batch_id,
            code: "invalid_batch_submission".to_string(),
            reason: "sealed transactional batch digest does not match declared members".to_string(),
        })
    );
    assert_eq!(
        io.load_visible_region_row("users", "main", row_id).unwrap(),
        None,
        "invalid batch digests should be rejected before publication"
    );
}

#[test]
fn seal_batch_rejects_when_family_visible_frontier_changed() {
    let mut sm = SyncManager::new().with_durability_tier(DurabilityTier::Worker);
    let mut io = MemoryStorage::new();
    let client_id = ClientId::new();
    let batch_id = crate::row_histories::BatchId::new();
    let existing_row_id = ObjectId::new();
    let conflicting_row_id = ObjectId::new();
    let staged_row_id = ObjectId::new();
    let target_branch = "dev-aaaaaaaaaaaa-main";
    let sibling_branch = "dev-bbbbbbbbbbbb-main";
    seed_users_schema(&mut io);

    add_client(&mut sm, &io, client_id);
    sm.set_client_role(client_id, ClientRole::Peer);
    sm.take_outbox();

    let existing_row = visible_row(existing_row_id, target_branch, Vec::new(), 900, b"seen");
    seed_visible_row(&mut sm, &mut io, "users", existing_row.clone());

    let staged_row = row_with_batch_state(
        visible_row(staged_row_id, target_branch, Vec::new(), 1_000, b"alice"),
        batch_id,
        crate::row_histories::RowState::StagingPending,
        None,
    );

    sm.process_from_client(
        &mut io,
        client_id,
        SyncPayload::RowBatchCreated {
            metadata: Some(RowMetadata {
                id: staged_row.row_id,
                metadata: row_metadata("users"),
            }),
            row: staged_row.clone(),
        },
    );
    sm.take_outbox();

    let conflicting_row = visible_row(
        conflicting_row_id,
        sibling_branch,
        Vec::new(),
        1_050,
        b"bob",
    );
    sm.process_from_client(
        &mut io,
        client_id,
        SyncPayload::RowBatchCreated {
            metadata: Some(RowMetadata {
                id: conflicting_row.row_id,
                metadata: row_metadata("users"),
            }),
            row: conflicting_row,
        },
    );
    sm.take_outbox();

    sm.process_from_client(
        &mut io,
        client_id,
        SyncPayload::SealBatch {
            submission: sealed_submission(
                batch_id,
                target_branch,
                vec![SealedBatchMember {
                    object_id: staged_row_id,
                    row_digest: staged_row.content_digest(),
                }],
                vec![CapturedFrontierMember {
                    object_id: existing_row_id,
                    branch_name: BranchName::new(target_branch),
                    batch_id: existing_row.batch_id(),
                }],
            ),
        },
    );

    assert_eq!(
        io.load_authoritative_batch_settlement(batch_id).unwrap(),
        Some(BatchSettlement::Rejected {
            batch_id,
            code: "transaction_conflict".to_string(),
            reason: "family-visible frontier changed since batch was sealed".to_string(),
        })
    );
    assert_eq!(
        io.load_visible_region_row("users", target_branch, staged_row_id)
            .unwrap(),
        None,
        "conflicted sealed batch should not publish its staged row"
    );

    let history_rows = io.scan_history_row_batches("users", staged_row_id).unwrap();
    assert_eq!(history_rows.len(), 1);
    assert_eq!(
        history_rows[0].state,
        crate::row_histories::RowState::Rejected
    );
}

#[test]
fn seal_batch_accepts_when_family_visible_frontier_matches() {
    let mut sm = SyncManager::new().with_durability_tier(DurabilityTier::Worker);
    let mut io = MemoryStorage::new();
    let client_id = ClientId::new();
    let batch_id = crate::row_histories::BatchId::new();
    let existing_row_id = ObjectId::new();
    let staged_row_id = ObjectId::new();
    let target_branch = "dev-aaaaaaaaaaaa-main";
    seed_users_schema(&mut io);

    add_client(&mut sm, &io, client_id);
    sm.set_client_role(client_id, ClientRole::Peer);
    sm.take_outbox();

    let existing_row = visible_row(existing_row_id, target_branch, Vec::new(), 900, b"seen");
    seed_visible_row(&mut sm, &mut io, "users", existing_row.clone());

    let staged_row = row_with_batch_state(
        visible_row(staged_row_id, target_branch, Vec::new(), 1_000, b"alice"),
        batch_id,
        crate::row_histories::RowState::StagingPending,
        None,
    );

    sm.process_from_client(
        &mut io,
        client_id,
        SyncPayload::RowBatchCreated {
            metadata: Some(RowMetadata {
                id: staged_row.row_id,
                metadata: row_metadata("users"),
            }),
            row: staged_row.clone(),
        },
    );
    sm.take_outbox();

    sm.process_from_client(
        &mut io,
        client_id,
        SyncPayload::SealBatch {
            submission: sealed_submission(
                batch_id,
                target_branch,
                vec![SealedBatchMember {
                    object_id: staged_row_id,
                    row_digest: staged_row.content_digest(),
                }],
                vec![CapturedFrontierMember {
                    object_id: existing_row_id,
                    branch_name: BranchName::new(target_branch),
                    batch_id: existing_row.batch_id(),
                }],
            ),
        },
    );

    assert!(matches!(
        io.load_authoritative_batch_settlement(batch_id).unwrap(),
        Some(BatchSettlement::AcceptedTransaction {
            batch_id: settled_batch_id,
            confirmed_tier: DurabilityTier::Worker,
            ref visible_members,
        }) if settled_batch_id == batch_id
            && *visible_members == vec![VisibleBatchMember {
                object_id: staged_row_id,
                branch_name: BranchName::new(target_branch),
                batch_id,
            }]
    ));
}

#[test]
fn forward_update_to_servers_with_storage_replays_row_history_without_visible_region() {
    let mut sm = SyncManager::new();
    let mut io = MemoryStorage::new();
    let server_id = ServerId::new();
    let row_id = ObjectId::new();
    let row = visible_row(row_id, "main", Vec::new(), 1_000, b"history-only");

    add_server(&mut sm, &io, server_id);
    sm.take_outbox();
    seed_users_schema(&mut io);
    create_test_row_with_id(&mut io, row_id, Some(row_metadata("users")));
    io.append_history_region_rows("users", std::slice::from_ref(&row))
        .unwrap();

    sm.forward_update_to_servers_with_storage(&io, row_id, BranchName::new("main"));

    assert!(sm.take_outbox().into_iter().any(|entry| matches!(
        entry,
        OutboxEntry {
            destination: Destination::Server(id),
            payload: SyncPayload::RowBatchCreated { row: created, metadata, .. },
        } if id == server_id && created.batch_id() == row.batch_id() && metadata.is_some()
    )));
}

#[test]
fn add_server_with_storage_syncs_full_row_history_to_server() {
    let mut io = MemoryStorage::new();
    let row_id = ObjectId::new();
    let older = visible_row(row_id, "main", Vec::new(), 1_000, b"older");
    let newer = visible_row(row_id, "main", vec![older.batch_id()], 2_000, b"newer");

    seed_users_schema(&mut io);
    io.put_metadata(row_id, row_metadata("users")).unwrap();
    io.append_history_region_rows("users", &[older.clone(), newer.clone()])
        .unwrap();
    io.upsert_visible_region_rows(
        "users",
        std::slice::from_ref(&VisibleRowEntry::rebuild(
            newer.clone(),
            &[older.clone(), newer.clone()],
        )),
    )
    .unwrap();

    let mut sm = SyncManager::new();
    let server_id = ServerId::new();
    sm.add_server_with_storage(server_id, false, &io);

    let outbox = sm.take_outbox();
    let schema_syncs = outbox
        .iter()
        .filter(|entry| {
            matches!(
                entry,
                OutboxEntry {
                    destination: Destination::Server(id),
                    payload: SyncPayload::CatalogueEntryUpdated { .. },
                } if *id == server_id
            )
        })
        .count();
    assert_eq!(schema_syncs, 1);

    let row_syncs = outbox
        .iter()
        .filter(|entry| {
            matches!(
                entry,
                OutboxEntry {
                    destination: Destination::Server(id),
                    payload: SyncPayload::RowBatchCreated { .. },
                } if *id == server_id
            )
        })
        .collect::<Vec<_>>();
    assert_eq!(row_syncs.len(), 2);
    assert!(matches!(
        row_syncs[0],
        OutboxEntry {
            destination: Destination::Server(id),
            payload: SyncPayload::RowBatchCreated { row, metadata, .. },
        } if *id == server_id && row.batch_id() == older.batch_id() && metadata.is_some()
    ));
    assert!(matches!(
        row_syncs[1],
        OutboxEntry {
            destination: Destination::Server(id),
            payload: SyncPayload::RowBatchCreated { row, metadata, .. },
        } if *id == server_id && row.batch_id() == newer.batch_id() && metadata.is_none()
    ));
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
            propagation: QueryPropagation::Full,
        },
    });
    sm.process_inbox(&mut MemoryStorage::new());
    sm.take_pending_query_subscriptions()
}

#[test]
fn query_subscription_falls_back_to_client_session_when_payload_omits_it() {
    let mut sm = SyncManager::new();
    let io = MemoryStorage::new();
    let client_id = ClientId::new();
    add_client(&mut sm, &io, client_id);
    sm.set_client_session(
        client_id,
        crate::query_manager::session::Session::new("alice"),
    );

    let pending = push_query_subscription(&mut sm, client_id, None);
    assert_eq!(pending.len(), 1);
    assert_eq!(
        pending[0]
            .session
            .as_ref()
            .map(|session| session.user_id.as_str()),
        Some("alice")
    );
}

#[test]
fn remove_client_cleans_pending_query_subscriptions() {
    let mut sm = SyncManager::new();
    let io = MemoryStorage::new();
    let alice = ClientId::new();
    let bob = ClientId::new();
    add_client(&mut sm, &io, alice);
    add_client(&mut sm, &io, bob);

    let query = QueryBuilder::new("users").build();
    sm.pending_query_subscriptions
        .push(PendingQuerySubscription {
            client_id: alice,
            query_id: QueryId(1),
            query: query.clone(),
            session: None,
            propagation: QueryPropagation::Full,
        });
    sm.pending_query_subscriptions
        .push(PendingQuerySubscription {
            client_id: bob,
            query_id: QueryId(2),
            query,
            session: None,
            propagation: QueryPropagation::Full,
        });

    sm.remove_client(alice);

    assert_eq!(sm.pending_query_subscriptions.len(), 1);
    assert_eq!(sm.pending_query_subscriptions[0].client_id, bob);
}

#[test]
fn remove_client_cleans_outbox_entries() {
    let mut sm = SyncManager::new();
    let io = MemoryStorage::new();
    let alice = ClientId::new();
    let bob = ClientId::new();
    add_client(&mut sm, &io, alice);
    add_client(&mut sm, &io, bob);

    let row = visible_row(ObjectId::new(), "main", Vec::new(), 1_000, b"alice");
    sm.outbox.push(OutboxEntry {
        destination: Destination::Client(alice),
        payload: SyncPayload::RowBatchCreated {
            metadata: None,
            row: row.clone(),
        },
    });
    sm.outbox.push(OutboxEntry {
        destination: Destination::Client(bob),
        payload: SyncPayload::RowBatchCreated {
            metadata: None,
            row: row.clone(),
        },
    });
    let server_id = ServerId::new();
    sm.outbox.push(OutboxEntry {
        destination: Destination::Server(server_id),
        payload: SyncPayload::RowBatchCreated {
            metadata: None,
            row,
        },
    });

    sm.remove_client(alice);

    assert_eq!(sm.outbox.len(), 2);
    assert!(sm.outbox.iter().all(|entry| match entry.destination {
        Destination::Client(id) => id != alice,
        Destination::Server(_) => true,
    }));
}

#[test]
fn remove_client_skips_when_inbox_entries_exist() {
    let mut sm = SyncManager::new();
    let io = MemoryStorage::new();
    let alice = ClientId::new();
    add_client(&mut sm, &io, alice);

    sm.push_inbox(InboxEntry {
        source: Source::Client(alice),
        payload: SyncPayload::RowBatchCreated {
            metadata: None,
            row: visible_row(ObjectId::new(), "main", Vec::new(), 1_000, b"alice"),
        },
    });

    assert!(!sm.remove_client(alice));
    assert!(sm.get_client(alice).is_some());
}
