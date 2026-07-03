use std::collections::BTreeMap;

use jazz::groove::records::Value;
use jazz::groove::schema::{ColumnSchema, ColumnType};
use jazz::groove::storage::RocksDbStorage;
use jazz::ids::{AuthorId, NodeUuid, RowUuid};
use jazz::node::{MergeableCommit, NodeState, SKEW_TOLERANCE_MS};
use jazz::peer::{PeerRole, PeerState};
use jazz::protocol::{SubscriptionKey, SyncMessage, VersionBundle};
use jazz::query::{Query, claim, col, eq, param};
use jazz::schema::{JazzSchema, Policy, TableSchema};
use jazz::tx::{DeletionEvent, DurabilityTier, Fate, RejectionReason, TxId};

fn node(byte: u8) -> NodeUuid {
    NodeUuid::from_bytes([byte; 16])
}

fn row(byte: u8) -> RowUuid {
    RowUuid::from_bytes([byte; 16])
}

fn schema() -> JazzSchema {
    JazzSchema::new([TableSchema::new(
        "todos",
        [
            ColumnSchema::new("title", ColumnType::String),
            ColumnSchema::new("owner", ColumnType::Uuid),
        ],
    )
    .with_read_policy(Policy::owner_only("todos", "owner"))])
}

fn read_write_policy_schema() -> JazzSchema {
    JazzSchema::new([TableSchema::new(
        "todos",
        [
            ColumnSchema::new("title", ColumnType::String),
            ColumnSchema::new("owner", ColumnType::Uuid),
        ],
    )
    .with_read_policy(Policy::owner_only("todos", "owner"))
    .with_write_policy(Policy::owner_only("todos", "owner"))])
}

fn access_write_policy_schema() -> JazzSchema {
    let canvas_policy = Policy::shape(Query::from("canvases").join_via(
        "canvasInvites",
        "canvas",
        [eq(col("userID"), claim("sub"))],
    ));
    JazzSchema::new([
        TableSchema::new("canvases", [ColumnSchema::new("title", ColumnType::String)])
            .with_read_policy(canvas_policy.clone())
            .with_write_policy(canvas_policy),
        TableSchema::new(
            "canvasInvites",
            [
                ColumnSchema::new("canvas", ColumnType::Uuid),
                ColumnSchema::new("userID", ColumnType::Uuid),
            ],
        )
        .with_reference("canvas", "canvases"),
    ])
}

fn open_node(
    node_uuid: NodeUuid,
    schema: JazzSchema,
) -> (tempfile::TempDir, NodeState<RocksDbStorage>) {
    let temp_dir = tempfile::tempdir().unwrap();
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = RocksDbStorage::open(temp_dir.path(), &refs).unwrap();
    let node = NodeState::new(node_uuid, schema, storage).unwrap();
    (temp_dir, node)
}

fn reopen_node(
    temp_dir: &tempfile::TempDir,
    node_uuid: NodeUuid,
    schema: JazzSchema,
) -> NodeState<RocksDbStorage> {
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = RocksDbStorage::open(temp_dir.path(), &refs).unwrap();
    NodeState::new(node_uuid, schema, storage).unwrap()
}

fn cells(title: &str, owner: AuthorId) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("title".to_owned(), Value::String(title.to_owned())),
        ("owner".to_owned(), Value::Uuid(owner.0)),
    ])
}

fn title_only_cells(title: &str) -> BTreeMap<String, Value> {
    BTreeMap::from([("title".to_owned(), Value::String(title.to_owned()))])
}

fn permission_scope_key(schema: &JazzSchema, table: &str, writer: AuthorId) -> SubscriptionKey {
    let _policy = schema
        .tables
        .iter()
        .find(|candidate| candidate.name == table)
        .and_then(|table| table.write_policies.insert_check.clone())
        .expect("table should have a write policy");
    let mut values = BTreeMap::new();
    values.insert("__jazz_claim_sub".to_owned(), Value::Uuid(writer.0));
    let shape = Query::from(table)
        .filter(eq(col("owner"), param("__jazz_claim_sub")))
        .validate(schema)
        .expect("policy should validate as a scope shape");
    let binding = shape
        .bind(values)
        .expect("writer claim should bind scope shape");
    SubscriptionKey {
        shape_id: shape.shape_id(),
        binding_id: binding.binding_id(),
        read_view: Default::default(),
    }
}

fn whole_table_key(schema: &JazzSchema, table: &str) -> SubscriptionKey {
    let shape = Query::from(table).validate(schema).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    SubscriptionKey {
        shape_id: shape.shape_id(),
        binding_id: binding.binding_id(),
        read_view: Default::default(),
    }
}

fn invite_cells(canvas: RowUuid, user: AuthorId) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("canvas".to_owned(), Value::Uuid(canvas.0)),
        ("userID".to_owned(), Value::Uuid(user.0)),
    ])
}

fn commit_local_global(node: &mut NodeState<RocksDbStorage>, commit: MergeableCommit) -> TxId {
    let tx_id = node.commit_mergeable(commit).unwrap();
    node.finalize_local_mergeable_commit(tx_id).unwrap();
    tx_id
}

fn commit(
    ui: &mut NodeState<RocksDbStorage>,
    row_uuid: RowUuid,
    made_at: u64,
    title: &str,
    owner: AuthorId,
    parents: impl IntoIterator<Item = TxId>,
) -> (TxId, SyncMessage) {
    ui.commit_mergeable_unit(
        MergeableCommit::new("todos", row_uuid, made_at)
            .made_by(AuthorId::from_bytes([7; 16]))
            .parents(parents.into_iter().collect())
            .cells(cells(title, owner)),
    )
    .unwrap()
}

fn deletion(
    ui: &mut NodeState<RocksDbStorage>,
    row_uuid: RowUuid,
    made_at: u64,
    event: DeletionEvent,
) -> (TxId, SyncMessage) {
    ui.commit_mergeable_unit(
        MergeableCommit::new("todos", row_uuid, made_at)
            .made_by(AuthorId::from_bytes([7; 16]))
            .deletion(event),
    )
    .unwrap()
}

fn relay_ingest(node: &mut NodeState<RocksDbStorage>, message: &SyncMessage) {
    let SyncMessage::CommitUnit { tx, versions } = message else {
        panic!("expected commit unit");
    };
    node.ingest_relay_commit_unit(tx.clone(), versions.clone())
        .unwrap();
}

fn core_ingest(
    node: &mut NodeState<RocksDbStorage>,
    message: &SyncMessage,
    now: u64,
) -> SyncMessage {
    let SyncMessage::CommitUnit { tx, versions } = message else {
        panic!("expected commit unit");
    };
    let [fate] = node
        .ingest_commit_unit(tx.clone(), versions.clone(), now)
        .unwrap()
        .try_into()
        .unwrap();
    fate
}

fn apply_fate(node: &mut NodeState<RocksDbStorage>, fate: &SyncMessage) {
    node.apply_sync_message(fate.clone()).unwrap();
    node.apply_sync_message(fate.clone()).unwrap();
}

fn refresh(
    upstream: &mut NodeState<RocksDbStorage>,
    downstream: &mut NodeState<RocksDbStorage>,
    peer: &mut PeerState,
) {
    let update = peer.current_rows_update(upstream, "todos").unwrap();
    downstream.apply_sync_message(update).unwrap();
}

fn rows(node: &mut NodeState<RocksDbStorage>) -> Vec<(RowUuid, Value)> {
    let schema = schema();
    let table = &schema.tables[0];
    node.current_rows("todos", DurabilityTier::Global)
        .unwrap()
        .into_iter()
        .map(|row| (row.row_uuid(), row.cell(table, "title").expect("title")))
        .collect()
}

fn edge_rows(node: &mut NodeState<RocksDbStorage>) -> Vec<(RowUuid, Value)> {
    let schema = schema();
    let table = &schema.tables[0];
    node.current_rows("todos", DurabilityTier::Edge)
        .unwrap()
        .into_iter()
        .map(|row| (row.row_uuid(), row.cell(table, "title").expect("title")))
        .collect()
}

fn subscription_rows(node: &mut NodeState<RocksDbStorage>) -> Vec<(RowUuid, Value)> {
    let schema = schema();
    let table = &schema.tables[0];
    node.subscription_current_rows("todos", DurabilityTier::Global)
        .unwrap()
        .into_iter()
        .map(|row| (row.row_uuid(), row.cell(table, "title").expect("title")))
        .collect()
}

#[test]
fn four_tier_topology_relays_pending_units_and_core_fates() {
    let schema = schema();
    let ui_author = AuthorId::from_bytes([7; 16]);
    let ui_owner = ui_author;
    let other_owner = AuthorId::from_bytes([8; 16]);

    let (_ui_dir, mut ui) = open_node(node(1), schema.clone());
    let (worker_dir, mut worker) = open_node(node(2), schema.clone());
    let (_edge_dir, mut edge) = open_node(node(3), schema.clone());
    let (_core_dir, mut core) = open_node(node(4), schema.clone());

    let mut core_to_edge = PeerState::new();
    let mut edge_to_worker = PeerState::new();
    let mut worker_to_ui = PeerState::for_author(ui_author);

    let owned_row = row(1);
    let other_row = row(2);
    let deleted_row = row(3);
    let exclusive_row = row(4);
    let skewed_row = row(5);

    let mut units = Vec::new();
    let (owned_v1, unit) = commit(&mut ui, owned_row, 10, "owned v1", ui_owner, []);
    units.push((owned_v1, unit, u64::MAX - SKEW_TOLERANCE_MS));
    let (owned_v2, unit) = commit(&mut ui, owned_row, 11, "owned v2", ui_owner, [owned_v1]);
    units.push((owned_v2, unit, u64::MAX - SKEW_TOLERANCE_MS));
    let (other_tx, unit) = commit(&mut ui, other_row, 12, "other", other_owner, []);
    units.push((other_tx, unit, u64::MAX - SKEW_TOLERANCE_MS));
    let (deleted_base, unit) = commit(&mut ui, deleted_row, 13, "delete me", ui_owner, []);
    units.push((deleted_base, unit, u64::MAX - SKEW_TOLERANCE_MS));
    let (delete_tx, unit) = deletion(&mut ui, deleted_row, 14, DeletionEvent::Deleted);
    units.push((delete_tx, unit, u64::MAX - SKEW_TOLERANCE_MS));
    let (restore_tx, unit) = deletion(&mut ui, deleted_row, 15, DeletionEvent::Restored);
    units.push((restore_tx, unit, u64::MAX - SKEW_TOLERANCE_MS));
    let (exclusive_seed, unit) = commit(&mut ui, exclusive_row, 16, "exclusive base", ui_owner, []);
    units.push((exclusive_seed, unit, u64::MAX - SKEW_TOLERANCE_MS));

    let mut fates = BTreeMap::new();
    for (idx, (tx_id, unit, now)) in units.iter().enumerate() {
        relay_ingest(&mut worker, unit);
        relay_ingest(&mut edge, unit);
        let fate = core_ingest(&mut core, unit, *now);
        apply_fate(&mut edge, &fate);
        apply_fate(&mut worker, &fate);
        apply_fate(&mut ui, &fate);
        fates.insert(*tx_id, fate);

        if idx == 3 {
            drop(worker);
            worker = reopen_node(&worker_dir, node(2), schema.clone());
        }
    }

    refresh(&mut core, &mut edge, &mut core_to_edge);
    refresh(&mut edge, &mut worker, &mut edge_to_worker);
    refresh(&mut worker, &mut ui, &mut worker_to_ui);

    let tx_id = ui.open_exclusive().unwrap();
    assert_eq!(
        ui.tx_read(tx_id, "todos", exclusive_row).unwrap(),
        Some(cells("exclusive base", ui_owner))
    );
    ui.tx_write(
        tx_id,
        "todos",
        exclusive_row,
        cells("exclusive committed", ui_owner),
        None,
    )
    .unwrap();
    let (exclusive_tx, exclusive_unit) = ui.commit_exclusive(tx_id, ui_author, 17).unwrap();

    let (skewed_tx, skewed_unit) = commit(&mut ui, skewed_row, 100_000, "too new", ui_owner, []);
    let tail = [
        (exclusive_tx, exclusive_unit, u64::MAX - SKEW_TOLERANCE_MS),
        (skewed_tx, skewed_unit, 0),
    ];
    for (tx_id, unit, now) in tail {
        relay_ingest(&mut worker, &unit);
        relay_ingest(&mut edge, &unit);
        let fate = core_ingest(&mut core, &unit, now);
        apply_fate(&mut edge, &fate);
        apply_fate(&mut worker, &fate);
        apply_fate(&mut ui, &fate);
        fates.insert(tx_id, fate);
    }

    refresh(&mut core, &mut edge, &mut core_to_edge);
    refresh(&mut edge, &mut worker, &mut edge_to_worker);
    refresh(&mut worker, &mut ui, &mut worker_to_ui);
    refresh(&mut core, &mut edge, &mut core_to_edge);
    refresh(&mut edge, &mut worker, &mut edge_to_worker);
    refresh(&mut worker, &mut ui, &mut worker_to_ui);

    let expected_all = vec![
        (owned_row, Value::String("owned v2".to_owned())),
        (other_row, Value::String("other".to_owned())),
        (deleted_row, Value::String("delete me".to_owned())),
        (
            exclusive_row,
            Value::String("exclusive committed".to_owned()),
        ),
    ];
    let expected_ui = vec![
        (owned_row, Value::String("owned v2".to_owned())),
        (deleted_row, Value::String("delete me".to_owned())),
        (
            exclusive_row,
            Value::String("exclusive committed".to_owned()),
        ),
    ];
    assert_eq!(rows(&mut core), expected_all);
    assert_eq!(rows(&mut edge), expected_all);
    assert_eq!(rows(&mut worker), expected_all);
    assert_eq!(subscription_rows(&mut ui), expected_ui);

    for node in [&mut ui, &mut worker, &mut edge, &mut core] {
        assert_eq!(
            node.transaction_state(skewed_tx).unwrap().0,
            Fate::Rejected(RejectionReason::ClientClockTooFarAhead)
        );
    }

    let (_, global_seq, _) = core.transaction_state(exclusive_tx).unwrap();
    for node in [&mut ui, &mut worker, &mut edge, &mut core] {
        assert_eq!(
            node.transaction_state(exclusive_tx).unwrap(),
            (Fate::Accepted, global_seq, DurabilityTier::Global)
        );
    }

    assert!(core_to_edge.metrics.version_bundles_out > 0);
    assert!(edge_to_worker.metrics.version_bundles_out > 0);
    assert!(worker_to_ui.metrics.version_bundles_out > 0);
    assert_eq!(
        worker.sync_metrics().parked_orphans,
        worker.sync_metrics().parked_orphans_resolved
    );
    assert_eq!(
        edge.sync_metrics().parked_orphans,
        edge.sync_metrics().parked_orphans_resolved
    );
}

#[test]
fn edge_peer_terminates_client_identity_and_relays_upstream() {
    let schema = schema();
    let client_author = AuthorId::from_bytes([7; 16]);
    let other_owner = AuthorId::from_bytes([8; 16]);

    let (_client_dir, mut client) = open_node(node(1), schema.clone());
    let (_edge_dir, mut edge) = open_node(node(3), schema.clone());
    let (_core_dir, mut core) = open_node(node(4), schema.clone());

    let mut core_to_edge = PeerState::relay();
    let mut edge_to_client = PeerState::edge_client(client_author);

    assert_eq!(core_to_edge.role(), PeerRole::Relay);
    assert_eq!(
        edge_to_client.role(),
        PeerRole::EdgeClient {
            identity: client_author
        }
    );
    assert_eq!(edge_to_client.identity(), client_author);

    let client_row = row(1);
    let other_row = row(2);
    let (client_tx, client_unit) = commit(
        &mut client,
        client_row,
        10,
        "client visible",
        client_author,
        [],
    );
    let (other_tx, other_unit) = commit(&mut client, other_row, 11, "core only", other_owner, []);

    for (tx_id, unit) in [(client_tx, client_unit), (other_tx, other_unit)] {
        relay_ingest(&mut edge, &unit);
        let fate = core_ingest(&mut core, &unit, u64::MAX - SKEW_TOLERANCE_MS);
        apply_fate(&mut edge, &fate);
        apply_fate(&mut client, &fate);
        assert_eq!(
            client.transaction_state(tx_id).unwrap().0,
            Fate::Accepted,
            "test setup should accept all relayed units"
        );
    }

    refresh(&mut core, &mut edge, &mut core_to_edge);
    refresh(&mut edge, &mut client, &mut edge_to_client);

    let expected_all = vec![
        (client_row, Value::String("client visible".to_owned())),
        (other_row, Value::String("core only".to_owned())),
    ];
    let expected_client = vec![(client_row, Value::String("client visible".to_owned()))];

    assert_eq!(rows(&mut core), expected_all);
    assert_eq!(rows(&mut edge), expected_all);
    assert_eq!(subscription_rows(&mut client), expected_client);
    assert_eq!(core_to_edge.identity(), AuthorId::SYSTEM);
}

#[test]
fn edge_defers_mergeable_fate_until_permission_scope_settles() {
    let schema = read_write_policy_schema();
    let client_author = AuthorId::from_bytes([7; 16]);

    let (_client_dir, mut client) = open_node(node(1), schema.clone());
    let (_edge_dir, mut edge) = open_node(node(3), schema);
    let mut edge_to_client = PeerState::edge_client(client_author);

    let row_uuid = row(9);
    let (tx_id, unit) = commit(
        &mut client,
        row_uuid,
        10,
        "edge accepted after scope",
        client_author,
        [],
    );
    let SyncMessage::CommitUnit { tx, versions } = unit else {
        panic!("expected commit unit");
    };

    let first = edge_to_client
        .ingest_edge_mergeable_commit_unit(
            &mut edge,
            tx.clone(),
            versions.clone(),
            u64::MAX - SKEW_TOLERANCE_MS,
        )
        .unwrap();
    assert!(
        first.is_empty(),
        "edge must not assign fate before scope settles"
    );
    assert_eq!(edge_to_client.deferred_edge_fate_count(), 1);
    assert_eq!(edge_to_client.edge_scope_subscription_count(), 1);
    assert_eq!(edge.transaction_state(tx_id).unwrap().0, Fate::Pending);

    let [fate] = edge_to_client
        .drain_deferred_edge_fates(&mut edge, u64::MAX - SKEW_TOLERANCE_MS)
        .unwrap()
        .try_into()
        .unwrap();
    assert_eq!(edge_to_client.deferred_edge_fate_count(), 0);
    assert_eq!(edge_to_client.edge_scope_subscription_count(), 0);
    assert_eq!(
        fate,
        SyncMessage::FateUpdate {
            tx_id,
            fate: Fate::Accepted,
            global_seq: None,
            durability: Some(DurabilityTier::Edge),
        }
    );
    assert_eq!(
        edge.transaction_state(tx_id).unwrap(),
        (Fate::Accepted, None, DurabilityTier::Edge)
    );
}

#[test]
fn edge_permission_scope_is_write_policy_claim_not_whole_table() {
    let schema = read_write_policy_schema();
    let client_author = AuthorId::from_bytes([7; 16]);

    let (_client_dir, mut client) = open_node(node(1), schema.clone());
    let (_edge_dir, mut edge) = open_node(node(3), schema.clone());
    let mut edge_to_client = PeerState::edge_client(client_author);

    let (tx_id, unit) = commit(&mut client, row(19), 10, "narrow scope", client_author, []);
    let SyncMessage::CommitUnit { tx, versions } = unit else {
        panic!("expected commit unit");
    };

    assert!(
        edge_to_client
            .ingest_edge_mergeable_commit_unit(
                &mut edge,
                tx,
                versions,
                u64::MAX - SKEW_TOLERANCE_MS,
            )
            .unwrap()
            .is_empty()
    );

    let scope_key = permission_scope_key(&schema, "todos", client_author);
    let whole_table = whole_table_key(&schema, "todos");
    assert_ne!(scope_key, whole_table);
    assert!(edge_to_client.subscription_result_sets(scope_key).is_some());
    assert!(
        edge_to_client
            .subscription_result_sets(whole_table)
            .is_none()
    );
    assert_eq!(edge_to_client.deferred_edge_fate_count(), 1);
    assert_eq!(edge.transaction_state(tx_id).unwrap().0, Fate::Pending);
}

#[test]
fn edge_permission_scope_uses_link_identity_not_made_by_provenance() {
    let schema = read_write_policy_schema();
    let backend_author = AuthorId::from_bytes([0xb0; 16]);
    let attributed_user = AuthorId::from_bytes([0xa1; 16]);

    let (_backend_dir, mut backend) = open_node(node(1), schema.clone());
    let (_edge_dir, mut edge) = open_node(node(3), schema.clone());
    let mut edge_to_backend = PeerState::edge_client(backend_author);

    let row_uuid = row(20);
    let (tx_id, unit) = backend
        .commit_mergeable_unit(
            MergeableCommit::new("todos", row_uuid, 10)
                .made_by(attributed_user)
                .permission_subject(backend_author)
                .cells(cells("attributed via backend", backend_author)),
        )
        .unwrap();
    let SyncMessage::CommitUnit { tx, versions } = unit else {
        panic!("expected commit unit");
    };

    assert_ne!(tx.made_by, edge_to_backend.identity());
    assert!(
        edge_to_backend
            .ingest_edge_mergeable_commit_unit(
                &mut edge,
                tx.clone(),
                versions.clone(),
                u64::MAX - SKEW_TOLERANCE_MS,
            )
            .unwrap()
            .is_empty()
    );

    let backend_scope = permission_scope_key(&schema, "todos", backend_author);
    let attributed_scope = permission_scope_key(&schema, "todos", attributed_user);
    assert!(
        edge_to_backend
            .subscription_result_sets(backend_scope)
            .is_some()
    );
    assert!(
        edge_to_backend
            .subscription_result_sets(attributed_scope)
            .is_none()
    );

    let [fate] = edge_to_backend
        .drain_deferred_edge_fates(&mut edge, u64::MAX - SKEW_TOLERANCE_MS)
        .unwrap()
        .try_into()
        .unwrap();
    assert_eq!(
        fate,
        SyncMessage::FateUpdate {
            tx_id,
            fate: Fate::Accepted,
            global_seq: None,
            durability: Some(DurabilityTier::Edge),
        }
    );
    assert_eq!(
        edge.transaction_state(tx_id).unwrap(),
        (Fate::Accepted, None, DurabilityTier::Edge)
    );
    let SyncMessage::CommitUnit { tx: stored_tx, .. } = edge.commit_unit_for(tx_id).unwrap() else {
        panic!("expected stored commit unit");
    };
    assert_eq!(stored_tx.made_by, attributed_user);
}

#[test]
fn edge_deduplicates_scope_subscription_for_repeated_deferred_units() {
    let schema = read_write_policy_schema();
    let client_author = AuthorId::from_bytes([7; 16]);

    let (_client_dir, mut client) = open_node(node(1), schema.clone());
    let (_edge_dir, mut edge) = open_node(node(3), schema);
    let mut edge_to_client = PeerState::edge_client(client_author);

    for (idx, row_uuid) in [row(21), row(22)].into_iter().enumerate() {
        let (tx_id, unit) = commit(
            &mut client,
            row_uuid,
            10 + idx as u64,
            "shared scope",
            client_author,
            [],
        );
        let SyncMessage::CommitUnit { tx, versions } = unit else {
            panic!("expected commit unit");
        };
        assert!(
            edge_to_client
                .ingest_edge_mergeable_commit_unit(
                    &mut edge,
                    tx,
                    versions,
                    u64::MAX - SKEW_TOLERANCE_MS,
                )
                .unwrap()
                .is_empty(),
            "{tx_id:?} should defer behind the shared scope"
        );
    }

    assert_eq!(edge_to_client.deferred_edge_fate_count(), 2);
    assert_eq!(edge_to_client.edge_scope_subscription_count(), 1);
}

#[test]
fn edge_releases_scope_subscription_after_last_deferred_unit_resolves() {
    let schema = read_write_policy_schema();
    let client_author = AuthorId::from_bytes([7; 16]);

    let (_client_dir, mut client) = open_node(node(1), schema.clone());
    let (_edge_dir, mut edge) = open_node(node(3), schema);
    let mut edge_to_client = PeerState::edge_client(client_author);

    for (idx, row_uuid) in [row(23), row(24)].into_iter().enumerate() {
        let (_tx_id, unit) = commit(
            &mut client,
            row_uuid,
            10 + idx as u64,
            "released scope",
            client_author,
            [],
        );
        let SyncMessage::CommitUnit { tx, versions } = unit else {
            panic!("expected commit unit");
        };
        edge_to_client
            .ingest_edge_mergeable_commit_unit(
                &mut edge,
                tx,
                versions,
                u64::MAX - SKEW_TOLERANCE_MS,
            )
            .unwrap();
    }
    assert_eq!(edge_to_client.edge_scope_subscription_count(), 1);

    let updates = edge_to_client
        .drain_deferred_edge_fates(&mut edge, u64::MAX - SKEW_TOLERANCE_MS)
        .unwrap();
    assert_eq!(updates.len(), 2);
    assert_eq!(edge_to_client.deferred_edge_fate_count(), 0);
    assert_eq!(edge_to_client.edge_scope_subscription_count(), 0);
}

#[test]
fn edge_restart_recovers_deferred_fate_from_client_outbox_redelivery() {
    let schema = read_write_policy_schema();
    let client_author = AuthorId::from_bytes([7; 16]);

    let (_client_dir, mut client) = open_node(node(1), schema.clone());
    let (edge_dir, mut edge) = open_node(node(3), schema.clone());
    let mut edge_to_client = PeerState::edge_client(client_author);

    let row_uuid = row(26);
    let (tx_id, unit) = commit(
        &mut client,
        row_uuid,
        10,
        "redelivered after edge restart",
        client_author,
        [],
    );
    let SyncMessage::CommitUnit { tx, versions } = unit.clone() else {
        panic!("expected commit unit");
    };

    assert!(
        edge_to_client
            .ingest_edge_mergeable_commit_unit(
                &mut edge,
                tx,
                versions,
                u64::MAX - SKEW_TOLERANCE_MS,
            )
            .unwrap()
            .is_empty(),
        "edge must defer until the permission scope settles"
    );
    assert_eq!(edge_to_client.deferred_edge_fate_count(), 1);
    assert_eq!(edge_to_client.edge_scope_subscription_count(), 1);
    assert_eq!(edge.transaction_state(tx_id).unwrap().0, Fate::Pending);
    drop(edge);
    drop(edge_to_client);

    let mut edge = reopen_node(&edge_dir, node(3), schema.clone());
    let edge_to_client = PeerState::edge_client(client_author);
    assert_eq!(
        edge_to_client.deferred_edge_fate_count(),
        0,
        "deferred edge-fate gates are in-memory and must not survive restart"
    );
    assert_eq!(
        edge_to_client.edge_scope_subscription_count(),
        0,
        "permission-scope gate refs are in-memory and must not survive restart"
    );
    let scope_key = permission_scope_key(&schema, "todos", client_author);
    assert!(
        edge_to_client.subscription_result_sets(scope_key).is_none(),
        "scope subscription result state must not survive through a fresh peer after restart"
    );
    assert_eq!(
        edge.transaction_state(tx_id).unwrap().0,
        Fate::Pending,
        "the pending relay history survives restart, but not the in-memory gate"
    );

    let SyncMessage::CommitUnit { tx, versions } = unit else {
        panic!("expected redelivered commit unit");
    };
    let mut redelivered_edge_to_client = PeerState::edge_client(client_author);
    assert!(
        redelivered_edge_to_client
            .ingest_edge_mergeable_commit_unit(
                &mut edge,
                tx,
                versions,
                u64::MAX - SKEW_TOLERANCE_MS,
            )
            .unwrap()
            .is_empty(),
        "redelivered unit reopens the permission-scope gate after restart"
    );
    let [fate] = redelivered_edge_to_client
        .drain_deferred_edge_fates(&mut edge, u64::MAX - SKEW_TOLERANCE_MS)
        .unwrap()
        .try_into()
        .unwrap();
    assert_eq!(
        fate,
        SyncMessage::FateUpdate {
            tx_id,
            fate: Fate::Accepted,
            global_seq: None,
            durability: Some(DurabilityTier::Edge),
        }
    );
    assert_eq!(
        edge.transaction_state(tx_id).unwrap(),
        (Fate::Accepted, None, DurabilityTier::Edge)
    );
    assert_eq!(
        edge_rows(&mut edge),
        vec![(
            row_uuid,
            Value::String("redelivered after edge restart".to_owned())
        )]
    );
}

#[test]
fn edge_restart_preserves_edge_accepted_unit_without_redelivery() {
    let schema = schema();
    let client_author = AuthorId::from_bytes([7; 16]);

    let (_client_dir, mut client) = open_node(node(1), schema.clone());
    let (edge_dir, mut edge) = open_node(node(3), schema.clone());
    let mut edge_to_client = PeerState::edge_client(client_author);

    let row_uuid = row(27);
    let (tx_id, unit) = commit(
        &mut client,
        row_uuid,
        10,
        "accepted before edge restart",
        client_author,
        [],
    );
    let SyncMessage::CommitUnit { tx, versions } = unit else {
        panic!("expected commit unit");
    };

    let [fate] = edge_to_client
        .ingest_edge_mergeable_commit_unit(&mut edge, tx, versions, u64::MAX - SKEW_TOLERANCE_MS)
        .unwrap()
        .try_into()
        .unwrap();
    assert_eq!(
        fate,
        SyncMessage::FateUpdate {
            tx_id,
            fate: Fate::Accepted,
            global_seq: None,
            durability: Some(DurabilityTier::Edge),
        }
    );
    assert_eq!(
        edge.transaction_state(tx_id).unwrap(),
        (Fate::Accepted, None, DurabilityTier::Edge)
    );
    drop(edge);
    drop(edge_to_client);

    let mut reopened = reopen_node(&edge_dir, node(3), schema);
    assert_eq!(
        reopened.transaction_state(tx_id).unwrap(),
        (Fate::Accepted, None, DurabilityTier::Edge),
        "edge-accepted fate must persist in edge storage across restart"
    );
    assert_eq!(
        edge_rows(&mut reopened),
        vec![(
            row_uuid,
            Value::String("accepted before edge restart".to_owned())
        )],
        "edge-accepted row must be readable after restart without client redelivery"
    );
}

#[test]
fn edge_public_write_table_settles_without_deferral_or_scope() {
    let schema = schema();
    let client_author = AuthorId::from_bytes([7; 16]);

    let (_client_dir, mut client) = open_node(node(1), schema.clone());
    let (_edge_dir, mut edge) = open_node(node(3), schema);
    let mut edge_to_client = PeerState::edge_client(client_author);

    let (tx_id, unit) = commit(&mut client, row(25), 10, "public write", client_author, []);
    let SyncMessage::CommitUnit { tx, versions } = unit else {
        panic!("expected commit unit");
    };
    let [fate] = edge_to_client
        .ingest_edge_mergeable_commit_unit(&mut edge, tx, versions, u64::MAX - SKEW_TOLERANCE_MS)
        .unwrap()
        .try_into()
        .unwrap();

    assert_eq!(
        fate,
        SyncMessage::FateUpdate {
            tx_id,
            fate: Fate::Accepted,
            global_seq: None,
            durability: Some(DurabilityTier::Edge),
        }
    );
    assert_eq!(edge_to_client.deferred_edge_fate_count(), 0);
    assert_eq!(edge_to_client.edge_scope_subscription_count(), 0);
}

#[test]
fn edge_accepted_mergeable_is_final_at_core_after_policy_revocation() {
    let schema = access_write_policy_schema();
    let client_author = AuthorId::from_bytes([7; 16]);
    let canvas_row = row(31);
    let invite_row = row(32);

    let (_client_dir, mut client) = open_node(node(1), schema.clone());
    let (_edge_dir, mut edge) = open_node(node(3), schema.clone());
    let (_core_dir, mut core) = open_node(node(4), schema.clone());
    let (_control_dir, mut control_core) = open_node(node(5), schema.clone());

    commit_local_global(
        &mut core,
        MergeableCommit::new("canvasInvites", invite_row, 10)
            .cells(invite_cells(canvas_row, client_author)),
    );
    commit_local_global(
        &mut control_core,
        MergeableCommit::new("canvasInvites", invite_row, 10)
            .cells(invite_cells(canvas_row, client_author)),
    );

    let mut core_to_edge = PeerState::relay();
    let grant_update = core_to_edge
        .current_rows_update(&mut core, "canvasInvites")
        .unwrap();
    edge.apply_sync_message(grant_update).unwrap();

    let (tx_id, unit) = client
        .commit_mergeable_unit(
            MergeableCommit::new("canvases", canvas_row, 20)
                .made_by(client_author)
                .cells(title_only_cells("edge final")),
        )
        .unwrap();
    let SyncMessage::CommitUnit { tx, versions } = unit.clone() else {
        panic!("expected commit unit");
    };

    let mut edge_to_client = PeerState::edge_client(client_author);
    let first = edge_to_client
        .ingest_edge_mergeable_commit_unit(
            &mut edge,
            tx.clone(),
            versions.clone(),
            u64::MAX - SKEW_TOLERANCE_MS,
        )
        .unwrap();
    assert!(first.is_empty());
    let [edge_fate] = edge_to_client
        .drain_deferred_edge_fates(&mut edge, u64::MAX - SKEW_TOLERANCE_MS)
        .unwrap()
        .try_into()
        .unwrap();
    assert_eq!(
        edge_fate,
        SyncMessage::FateUpdate {
            tx_id,
            fate: Fate::Accepted,
            global_seq: None,
            durability: Some(DurabilityTier::Edge),
        }
    );

    commit_local_global(
        &mut core,
        MergeableCommit::new("canvasInvites", invite_row, 30).deletion(DeletionEvent::Deleted),
    );
    commit_local_global(
        &mut control_core,
        MergeableCommit::new("canvasInvites", invite_row, 30).deletion(DeletionEvent::Deleted),
    );

    let [control_fate] = control_core
        .apply_sync_message(unit)
        .unwrap()
        .try_into()
        .unwrap();
    assert!(matches!(
        control_fate,
        SyncMessage::FateUpdate {
            fate: Fate::Rejected(RejectionReason::AuthorizationDenied),
            ..
        }
    ));

    let shape = Query::from("canvases").validate(&schema).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    core.apply_sync_message(SyncMessage::ViewUpdate {
        subscription: SubscriptionKey {
            shape_id: shape.shape_id(),
            binding_id: binding.binding_id(),
            read_view: Default::default(),
        },
        settled_through: jazz::time::GlobalSeq(0),
        reset_result_set: false,
        version_bundles: vec![VersionBundle {
            tx,
            versions,
            fate: Fate::Accepted,
            global_seq: None,
            durability: DurabilityTier::Edge,
        }],
        peer_payload_inventory: PeerPayloadInventory::default(),
        result_member_adds: Vec::new(),
        result_member_removes: Vec::new(),
        program_fact_adds: Vec::new(),
        program_fact_removes: Vec::new(),
    })
    .unwrap();

    let (fate, global_seq, durability) = core.transaction_state(tx_id).unwrap();
    assert_eq!(fate, Fate::Accepted);
    assert!(global_seq.is_none());
    assert_eq!(durability, DurabilityTier::Edge);
    let canvas_table = &schema.tables[0];
    assert_eq!(
        core.current_rows("canvases", DurabilityTier::Edge)
            .unwrap()
            .into_iter()
            .map(|row| (
                row.row_uuid(),
                row.cell(canvas_table, "title").expect("title")
            ))
            .collect::<Vec<_>>(),
        vec![(canvas_row, Value::String("edge final".to_owned()))]
    );
}
use jazz::protocol::PeerPayloadInventory;
