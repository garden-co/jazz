use std::collections::BTreeMap;

mod common;

use jazz::block_on;
use jazz::groove::records::Value;
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::node::{MergeableCommit, NodeState, SKEW_TOLERANCE_MS};
use jazz::peer::{PeerRole, PeerState};
use jazz::protocol::{SubscriptionKey, SyncMessage, VersionBundle};
use jazz::query::{Query, col, eq, param};
use jazz::schema::JazzSchema;
use jazz::tools::{
    ColumnType, OpenTransactionId, SchemaBuilder, TablePolicies, TableSchemaBuilder,
};
use jazz::tx::{DeletionEvent, DurabilityTier, Fate, RejectionReason, TxId};
use jazz_storage_rocksdb::RocksDbStorage;

use common::{compile_schema, exists, outer_eq, session_eq};

fn node(byte: u8) -> NodeUuid {
    NodeUuid::from_bytes([byte; 16])
}

fn row(byte: u8) -> RowUuid {
    RowUuid::from_bytes([byte; 16])
}

fn schema() -> JazzSchema {
    // These topology tests are about identity-scoped delivery.  Keep their
    // fixture writes intentionally public; declaring SELECT alone now closes
    // the remaining operation clauses.
    let policies = TablePolicies::new()
        .with_select(session_eq("owner", &["user"]))
        .with_insert(jazz::tools::PolicyExpr::True)
        .with_update(
            Some(jazz::tools::PolicyExpr::True),
            jazz::tools::PolicyExpr::True,
        )
        .with_delete(jazz::tools::PolicyExpr::True);
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("todos")
                    .column("title", ColumnType::Text)
                    .column("owner", ColumnType::Text)
                    .policies(policies),
            )
            .build(),
    )
}

/// A genuinely policy-free table is the public-write control case: it must
/// settle at an edge without authorization support or deferral.
fn public_write_schema() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("todos")
                    .column("title", ColumnType::Text)
                    .column("owner", ColumnType::Text),
            )
            .build(),
    )
}

fn read_write_policy_schema() -> JazzSchema {
    let owner = session_eq("owner", &["user"]);
    let policies = TablePolicies::new()
        .with_select(owner.clone())
        .with_insert(owner.clone())
        .with_update(Some(owner.clone()), owner.clone())
        .with_delete(owner);
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("todos")
                    .column("title", ColumnType::Text)
                    .column("owner", ColumnType::Text)
                    .policies(policies),
            )
            .build(),
    )
}

fn access_write_policy_schema() -> JazzSchema {
    let canvas = exists(
        "canvasInvites",
        vec![outer_eq("canvas", "id"), session_eq("userID", &["user"])],
    );
    let policies = TablePolicies::new()
        .with_select(canvas.clone())
        .with_insert(canvas.clone())
        .with_update(Some(canvas.clone()), canvas.clone())
        .with_delete(canvas);
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("canvases")
                    .column("title", ColumnType::Text)
                    .policies(policies),
            )
            .table(
                TableSchemaBuilder::new("canvasInvites")
                    .fk_column("canvas", "canvases")
                    .column("userID", ColumnType::Text),
            )
            .build(),
    )
}

fn open_node(
    node_uuid: NodeUuid,
    schema: JazzSchema,
) -> (tempfile::TempDir, NodeState<RocksDbStorage>) {
    let temp_dir = tempfile::tempdir().unwrap();
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = RocksDbStorage::open(temp_dir.path(), &refs).unwrap();
    let node = block_on(NodeState::new(node_uuid, schema, storage)).unwrap();
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
    block_on(NodeState::new(node_uuid, schema, storage)).unwrap()
}

fn cells(title: &str, owner: AuthorSubject) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("title".to_owned(), Value::String(title.to_owned())),
        (
            "owner".to_owned(),
            Value::String(owner.canonical().to_owned()),
        ),
    ])
}

fn title_only_cells(title: &str) -> BTreeMap<String, Value> {
    BTreeMap::from([("title".to_owned(), Value::String(title.to_owned()))])
}

fn permission_scope_key(
    schema: &JazzSchema,
    table: &str,
    writer: AuthorSubject,
) -> SubscriptionKey {
    let _policy = schema
        .tables
        .iter()
        .find(|candidate| candidate.name == table)
        .and_then(|table| table.write_policies.insert_check.clone())
        .expect("table should have a write policy");
    let mut values = BTreeMap::new();
    values.insert(
        "__jazz_claim_user".to_owned(),
        Value::String(writer.canonical().to_owned()),
    );
    let shape = Query::from(table)
        .filter(eq(col("owner"), param("__jazz_claim_user")))
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

fn invite_cells(canvas: RowUuid, user: AuthorSubject) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("canvas".to_owned(), Value::Uuid(canvas.0)),
        (
            "userID".to_owned(),
            Value::String(user.canonical().to_owned()),
        ),
    ])
}

fn commit_local_global(node: &mut NodeState<RocksDbStorage>, commit: MergeableCommit) -> TxId {
    block_on(async {
        let published = node.commit_mergeable(commit).await.unwrap();
        let tx_id = node
            .persist_and_settle_transaction(published)
            .await
            .unwrap();
        let outcome = node.finalize_local_mergeable_commit(tx_id).await.unwrap();
        node.persist_and_settle_outcome(outcome).await.unwrap();
        tx_id
    })
}

fn commit(
    ui: &mut NodeState<RocksDbStorage>,
    row_uuid: RowUuid,
    made_at: u64,
    title: &str,
    owner: AuthorSubject,
    parents: impl IntoIterator<Item = TxId>,
) -> (TxId, SyncMessage) {
    let writer = AuthorSubject::for_test_bytes([7; 16]);
    block_on(async {
        let (published, unit) = ui
            .commit_mergeable_unit(
                MergeableCommit::new("todos", row_uuid, made_at)
                    .made_by(writer)
                    .parents(parents.into_iter().collect())
                    .cells(cells(title, owner)),
            )
            .await
            .unwrap();
        let tx_id = ui.persist_and_settle_transaction(published).await.unwrap();
        (tx_id, unit)
    })
}

fn commit_as(
    ui: &mut NodeState<RocksDbStorage>,
    row_uuid: RowUuid,
    made_at: u64,
    title: &str,
    writer: AuthorSubject,
    parents: impl IntoIterator<Item = TxId>,
) -> (TxId, SyncMessage) {
    block_on(async {
        let (published, unit) = ui
            .commit_mergeable_unit(
                MergeableCommit::new("todos", row_uuid, made_at)
                    .made_by(writer)
                    .permission_subject(writer)
                    .parents(parents.into_iter().collect())
                    .cells(cells(title, writer)),
            )
            .await
            .unwrap();
        let tx_id = ui.persist_and_settle_transaction(published).await.unwrap();
        (tx_id, unit)
    })
}

fn deletion(
    ui: &mut NodeState<RocksDbStorage>,
    row_uuid: RowUuid,
    made_at: u64,
    event: DeletionEvent,
) -> (TxId, SyncMessage) {
    block_on(async {
        let (published, unit) = ui
            .commit_mergeable_unit(
                MergeableCommit::new("todos", row_uuid, made_at)
                    .made_by(AuthorSubject::for_test_bytes([7; 16]))
                    .deletion(event),
            )
            .await
            .unwrap();
        let tx_id = ui.persist_and_settle_transaction(published).await.unwrap();
        (tx_id, unit)
    })
}

fn relay_ingest(node: &mut NodeState<RocksDbStorage>, message: &SyncMessage) {
    let SyncMessage::CommitUnit { tx, versions } = message else {
        panic!("expected commit unit");
    };
    if let Some(identity) = tx.permission_subject {
        install_uuid_sub_claim(node, identity);
    }
    block_on(async {
        node.ingest_relay_commit_unit(tx.clone(), versions.clone())
            .await
            .unwrap();
    });
}

fn apply_message(node: &mut NodeState<RocksDbStorage>, message: SyncMessage) -> Vec<SyncMessage> {
    block_on(async {
        let outcome = node.apply_sync_message(message).await.unwrap();
        node.persist_and_settle_outcome(outcome).await.unwrap()
    })
}

fn transaction_state(
    node: &mut NodeState<RocksDbStorage>,
    tx_id: TxId,
) -> (Fate, Option<jazz::time::GlobalTime>, DurabilityTier) {
    block_on(node.transaction_state(tx_id)).unwrap()
}

fn edge_ingest(
    peer: &mut PeerState,
    node: &mut NodeState<RocksDbStorage>,
    tx: jazz::tx::Transaction,
    versions: Vec<jazz::protocol::VersionRecord>,
    now_ms: u64,
) -> Vec<SyncMessage> {
    install_uuid_sub_claim(node, peer.identity());
    block_on(async {
        let outcome = peer
            .ingest_edge_mergeable_commit_unit(node, tx, versions, now_ms)
            .await
            .unwrap();
        node.persist_and_settle_outcome(outcome).await.unwrap()
    })
}

fn drain_edge_fates(
    peer: &mut PeerState,
    node: &mut NodeState<RocksDbStorage>,
    now_ms: u64,
) -> Vec<SyncMessage> {
    block_on(async {
        let outcome = peer.drain_deferred_edge_fates(node, now_ms).await.unwrap();
        node.persist_and_settle_outcome(outcome).await.unwrap()
    })
}

fn core_ingest(
    node: &mut NodeState<RocksDbStorage>,
    message: &SyncMessage,
    now: u64,
) -> SyncMessage {
    let SyncMessage::CommitUnit { tx, versions } = message else {
        panic!("expected commit unit");
    };
    if let Some(identity) = tx.permission_subject {
        install_uuid_sub_claim(node, identity);
    }
    let [fate] = block_on(async {
        let outcome = node
            .ingest_commit_unit(tx.clone(), versions.clone(), now)
            .await
            .unwrap();
        node.persist_and_settle_outcome(outcome).await.unwrap()
    })
    .try_into()
    .unwrap();
    fate
}

fn install_uuid_sub_claim(node: &mut NodeState<RocksDbStorage>, identity: AuthorSubject) {
    if identity != AuthorSubject::SYSTEM {
        node.admit_test_session_claims(identity, BTreeMap::new());
    }
}

fn apply_fate(node: &mut NodeState<RocksDbStorage>, fate: &SyncMessage) {
    for message in [fate.clone(), fate.clone()] {
        apply_message(node, message);
    }
}

fn refresh(
    upstream: &mut NodeState<RocksDbStorage>,
    downstream: &mut NodeState<RocksDbStorage>,
    peer: &mut PeerState,
) {
    install_uuid_sub_claim(upstream, peer.identity());
    let update = block_on(peer.current_rows_update(upstream, "todos")).unwrap();
    apply_message(downstream, update);
}

fn rows(node: &mut NodeState<RocksDbStorage>) -> Vec<(RowUuid, Value)> {
    let schema = schema();
    let table = &schema.tables[0];
    block_on(node.current_rows("todos", DurabilityTier::Global))
        .unwrap()
        .into_iter()
        .map(|row| (row.row_uuid(), row.cell(table, "title").expect("title")))
        .collect()
}

fn edge_rows(node: &mut NodeState<RocksDbStorage>) -> Vec<(RowUuid, Value)> {
    let schema = schema();
    let table = &schema.tables[0];
    block_on(node.current_rows("todos", DurabilityTier::Edge))
        .unwrap()
        .into_iter()
        .map(|row| (row.row_uuid(), row.cell(table, "title").expect("title")))
        .collect()
}

fn subscription_rows(node: &mut NodeState<RocksDbStorage>) -> Vec<(RowUuid, Value)> {
    let schema = schema();
    let table = &schema.tables[0];
    block_on(node.subscription_current_rows("todos", DurabilityTier::Global))
        .unwrap()
        .into_iter()
        .map(|row| (row.row_uuid(), row.cell(table, "title").expect("title")))
        .collect()
}

#[test]
fn four_tier_topology_relays_pending_units_and_core_fates() {
    let schema = schema();
    let ui_author = AuthorSubject::for_test_bytes([7; 16]);
    let ui_owner = ui_author;
    let other_owner = AuthorSubject::for_test_bytes([8; 16]);

    let (_ui_dir, mut ui) = open_node(node(1), schema.clone());
    let (worker_dir, mut worker) = open_node(node(2), schema.clone());
    let (_edge_dir, mut edge) = open_node(node(3), schema.clone());
    let (_core_dir, mut core) = open_node(node(4), schema.clone());

    let mut core_to_edge = PeerState::new();
    let mut edge_to_worker = PeerState::new();
    let mut worker_to_ui = PeerState::client_link(ui_author);

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

    let tx_id = OpenTransactionId::new();
    block_on(ui.open_exclusive_for_test(tx_id, ui_author)).unwrap();
    assert_eq!(
        block_on(ui.tx_read(tx_id, "todos", exclusive_row)).unwrap(),
        Some(cells("exclusive base", ui_owner))
    );
    block_on(ui.tx_write(
        tx_id,
        "todos",
        exclusive_row,
        cells("exclusive committed", ui_owner),
        None,
    ))
    .unwrap();
    let (exclusive_tx, exclusive_unit) = block_on(async {
        let (published, unit) = ui.commit_exclusive(tx_id, ui_author, 17).await.unwrap();
        let tx_id = ui.persist_and_settle_transaction(published).await.unwrap();
        (tx_id, unit)
    });

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
            transaction_state(node, skewed_tx).0,
            Fate::Rejected(RejectionReason::ClientClockTooFarAhead)
        );
    }

    let (_, global_time, _) = transaction_state(&mut core, exclusive_tx);
    for node in [&mut ui, &mut worker, &mut edge, &mut core] {
        assert_eq!(
            transaction_state(node, exclusive_tx),
            (Fate::Accepted, global_time, DurabilityTier::Global)
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
    let client_author = AuthorSubject::for_test_bytes([7; 16]);
    let other_owner = AuthorSubject::for_test_bytes([8; 16]);

    let (_client_dir, mut client) = open_node(node(1), schema.clone());
    let (_edge_dir, mut edge) = open_node(node(3), schema.clone());
    let (_core_dir, mut core) = open_node(node(4), schema.clone());

    let mut core_to_edge = PeerState::relay();
    let mut edge_to_client = PeerState::edge_client(client_author);

    assert_eq!(core_to_edge.role(), PeerRole::Relay);
    assert_eq!(
        edge_to_client.role(),
        PeerRole::ClientLink {
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
            transaction_state(&mut client, tx_id).0,
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
    assert_eq!(core_to_edge.identity(), AuthorSubject::SYSTEM);
}

#[test]
fn edge_defers_mergeable_fate_until_permission_scope_settles() {
    let schema = read_write_policy_schema();
    let client_author = AuthorSubject::for_test_bytes([7; 16]);

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

    let first = edge_ingest(
        &mut edge_to_client,
        &mut edge,
        tx.clone(),
        versions.clone(),
        u64::MAX - SKEW_TOLERANCE_MS,
    );
    assert!(
        first.is_empty(),
        "edge must not assign fate before scope settles"
    );
    assert_eq!(edge_to_client.deferred_edge_fate_count(), 1);
    assert_eq!(edge_to_client.edge_scope_subscription_count(), 1);
    assert!(
        block_on(edge.transaction_state(tx_id)).is_none(),
        "an unresolved permission scope must keep the unit outside edge history"
    );

    let [fate] = drain_edge_fates(&mut edge_to_client, &mut edge, u64::MAX - SKEW_TOLERANCE_MS)
        .try_into()
        .unwrap();
    assert_eq!(edge_to_client.deferred_edge_fate_count(), 0);
    assert_eq!(edge_to_client.edge_scope_subscription_count(), 0);
    assert_eq!(
        fate,
        SyncMessage::FateUpdate {
            tx_id,
            fate: Fate::Accepted,
            global_time: None,
            durability: Some(DurabilityTier::Edge),
        }
    );
    assert_eq!(
        transaction_state(&mut edge, tx_id),
        (Fate::Accepted, None, DurabilityTier::Edge)
    );
}

#[test]
fn edge_permission_scope_is_write_policy_claim_not_whole_table() {
    let schema = read_write_policy_schema();
    let client_author = AuthorSubject::for_test_bytes([7; 16]);

    let (_client_dir, mut client) = open_node(node(1), schema.clone());
    let (_edge_dir, mut edge) = open_node(node(3), schema.clone());
    let mut edge_to_client = PeerState::edge_client(client_author);

    let (tx_id, unit) = commit(&mut client, row(19), 10, "narrow scope", client_author, []);
    let SyncMessage::CommitUnit { tx, versions } = unit else {
        panic!("expected commit unit");
    };

    assert!(
        edge_ingest(
            &mut edge_to_client,
            &mut edge,
            tx,
            versions,
            u64::MAX - SKEW_TOLERANCE_MS,
        )
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
    assert!(
        block_on(edge.transaction_state(tx_id)).is_none(),
        "the narrow write-policy scope must not create pending table history"
    );
}

#[test]
fn edge_permission_scope_uses_link_identity_not_made_by_provenance() {
    let schema = read_write_policy_schema();
    let backend_author = AuthorSubject::for_test_bytes([0xb0; 16]);
    let attributed_user = AuthorSubject::for_test_bytes([0xa1; 16]);

    let (_backend_dir, mut backend) = open_node(node(1), schema.clone());
    let (_edge_dir, mut edge) = open_node(node(3), schema.clone());
    let mut edge_to_backend = PeerState::edge_client(backend_author);

    let row_uuid = row(20);
    let (tx_id, unit) = block_on(async {
        let (published, unit) = backend
            .commit_mergeable_unit(
                MergeableCommit::new("todos", row_uuid, 10)
                    .made_by(attributed_user)
                    .permission_subject(backend_author)
                    .cells(cells("attributed via backend", backend_author)),
            )
            .await
            .unwrap();
        let tx_id = backend
            .persist_and_settle_transaction(published)
            .await
            .unwrap();
        (tx_id, unit)
    });
    let SyncMessage::CommitUnit { tx, versions } = unit else {
        panic!("expected commit unit");
    };

    assert_ne!(tx.made_by, edge_to_backend.identity());
    assert!(
        edge_ingest(
            &mut edge_to_backend,
            &mut edge,
            tx.clone(),
            versions.clone(),
            u64::MAX - SKEW_TOLERANCE_MS,
        )
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

    let [fate] = drain_edge_fates(
        &mut edge_to_backend,
        &mut edge,
        u64::MAX - SKEW_TOLERANCE_MS,
    )
    .try_into()
    .unwrap();
    assert_eq!(
        fate,
        SyncMessage::FateUpdate {
            tx_id,
            fate: Fate::Accepted,
            global_time: None,
            durability: Some(DurabilityTier::Edge),
        }
    );
    assert_eq!(
        transaction_state(&mut edge, tx_id),
        (Fate::Accepted, None, DurabilityTier::Edge)
    );
    let SyncMessage::CommitUnit { tx: stored_tx, .. } =
        block_on(edge.commit_unit_for(tx_id)).unwrap()
    else {
        panic!("expected stored commit unit");
    };
    assert_eq!(stored_tx.made_by, attributed_user);
}

#[test]
fn edge_deduplicates_scope_subscription_for_repeated_deferred_units() {
    let schema = read_write_policy_schema();
    let client_author = AuthorSubject::for_test_bytes([7; 16]);

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
            edge_ingest(
                &mut edge_to_client,
                &mut edge,
                tx,
                versions,
                u64::MAX - SKEW_TOLERANCE_MS
            )
            .is_empty(),
            "{tx_id:?} should defer behind the shared scope"
        );
    }

    assert_eq!(edge_to_client.deferred_edge_fate_count(), 2);
    assert_eq!(edge_to_client.edge_scope_subscription_count(), 1);
}

#[test]
fn edge_permission_scopes_are_keyed_by_policy_shape_and_writer_claim() {
    let schema = read_write_policy_schema();
    let writer_a = AuthorSubject::for_test_bytes([0xa1; 16]);
    let writer_b = AuthorSubject::for_test_bytes([0xb2; 16]);

    let (_client_a_dir, mut client_a) = open_node(node(1), schema.clone());
    let (_client_b_dir, mut client_b) = open_node(node(2), schema.clone());
    let (_edge_dir, mut edge) = open_node(node(3), schema.clone());
    let mut edge_to_a = PeerState::edge_client(writer_a);
    let mut edge_to_b = PeerState::edge_client(writer_b);

    for (idx, row_uuid) in [row(41), row(42)].into_iter().enumerate() {
        let (_tx_id, unit) = commit_as(
            &mut client_a,
            row_uuid,
            10 + idx as u64,
            "same claim",
            writer_a,
            [],
        );
        let SyncMessage::CommitUnit { tx, versions } = unit else {
            panic!("expected commit unit");
        };
        assert!(
            edge_ingest(
                &mut edge_to_a,
                &mut edge,
                tx,
                versions,
                u64::MAX - SKEW_TOLERANCE_MS
            )
            .is_empty()
        );
    }

    let (_tx_id, unit) = commit_as(&mut client_b, row(43), 20, "different claim", writer_b, []);
    let SyncMessage::CommitUnit { tx, versions } = unit else {
        panic!("expected commit unit");
    };
    assert!(
        edge_ingest(
            &mut edge_to_b,
            &mut edge,
            tx,
            versions,
            u64::MAX - SKEW_TOLERANCE_MS
        )
        .is_empty()
    );

    let scope_a = permission_scope_key(&schema, "todos", writer_a);
    let scope_b = permission_scope_key(&schema, "todos", writer_b);
    assert_eq!(
        scope_a.shape_id, scope_b.shape_id,
        "same write-policy shape must be reused across writer claims"
    );
    assert_ne!(
        scope_a.binding_id, scope_b.binding_id,
        "writer claim must remain part of scope identity"
    );
    assert!(edge_to_a.subscription_result_sets(scope_a).is_some());
    assert!(edge_to_a.subscription_result_sets(scope_b).is_none());
    assert!(edge_to_b.subscription_result_sets(scope_b).is_some());
    assert!(edge_to_b.subscription_result_sets(scope_a).is_none());
    assert_eq!(
        edge_to_a.edge_scope_subscription_count(),
        1,
        "same-claim deferred writes share one retained scope"
    );
    assert_eq!(
        edge_to_b.edge_scope_subscription_count(),
        1,
        "different claims use a separate retained scope"
    );
}

#[test]
fn settled_permission_scope_for_one_writer_claim_does_not_unlock_whole_table() {
    let schema = read_write_policy_schema();
    let writer_a = AuthorSubject::for_test_bytes([0xa1; 16]);
    let writer_b = AuthorSubject::for_test_bytes([0xb2; 16]);

    let (_client_a_dir, mut client_a) = open_node(node(1), schema.clone());
    let (_client_b_dir, mut client_b) = open_node(node(2), schema.clone());
    let (_edge_dir, mut edge) = open_node(node(3), schema);
    let mut edge_to_a = PeerState::edge_client(writer_a);
    let mut edge_to_b = PeerState::edge_client(writer_b);

    let (first_a, unit) = commit_as(&mut client_a, row(44), 10, "a first", writer_a, []);
    let SyncMessage::CommitUnit { tx, versions } = unit else {
        panic!("expected commit unit");
    };
    assert!(
        edge_ingest(
            &mut edge_to_a,
            &mut edge,
            tx,
            versions,
            u64::MAX - SKEW_TOLERANCE_MS
        )
        .is_empty()
    );
    let [_fate] = drain_edge_fates(&mut edge_to_a, &mut edge, u64::MAX - SKEW_TOLERANCE_MS)
        .try_into()
        .unwrap();
    assert_eq!(
        transaction_state(&mut edge, first_a),
        (Fate::Accepted, None, DurabilityTier::Edge)
    );

    let (second_a, unit) = commit_as(&mut client_a, row(45), 20, "a second", writer_a, []);
    let SyncMessage::CommitUnit { tx, versions } = unit else {
        panic!("expected commit unit");
    };
    let [a_fate] = edge_ingest(
        &mut edge_to_a,
        &mut edge,
        tx,
        versions,
        u64::MAX - SKEW_TOLERANCE_MS,
    )
    .try_into()
    .unwrap();
    assert_eq!(
        a_fate,
        SyncMessage::FateUpdate {
            tx_id: second_a,
            fate: Fate::Accepted,
            global_time: None,
            durability: Some(DurabilityTier::Edge),
        }
    );

    let (first_b, unit) = commit_as(&mut client_b, row(46), 30, "b first", writer_b, []);
    let SyncMessage::CommitUnit { tx, versions } = unit else {
        panic!("expected commit unit");
    };
    assert!(
        edge_ingest(
            &mut edge_to_b,
            &mut edge,
            tx,
            versions,
            u64::MAX - SKEW_TOLERANCE_MS
        )
        .is_empty()
    );
    assert_eq!(edge_to_b.deferred_edge_fate_count(), 1);
    assert!(
        block_on(edge.transaction_state(first_b)).is_none(),
        "settled writer-A scope must not admit writer-B into edge history"
    );
}

#[test]
fn edge_releases_scope_subscription_after_last_deferred_unit_resolves() {
    let schema = read_write_policy_schema();
    let client_author = AuthorSubject::for_test_bytes([7; 16]);

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
        edge_ingest(
            &mut edge_to_client,
            &mut edge,
            tx,
            versions,
            u64::MAX - SKEW_TOLERANCE_MS,
        );
    }
    assert_eq!(edge_to_client.edge_scope_subscription_count(), 1);

    let updates = drain_edge_fates(&mut edge_to_client, &mut edge, u64::MAX - SKEW_TOLERANCE_MS);
    assert_eq!(updates.len(), 2);
    assert_eq!(edge_to_client.deferred_edge_fate_count(), 0);
    assert_eq!(edge_to_client.edge_scope_subscription_count(), 0);
}

#[test]
fn edge_restart_recovers_deferred_fate_from_client_outbox_redelivery() {
    let schema = read_write_policy_schema();
    let client_author = AuthorSubject::for_test_bytes([7; 16]);

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
        edge_ingest(
            &mut edge_to_client,
            &mut edge,
            tx,
            versions,
            u64::MAX - SKEW_TOLERANCE_MS
        )
        .is_empty(),
        "edge must defer until the permission scope settles"
    );
    assert_eq!(edge_to_client.deferred_edge_fate_count(), 1);
    assert_eq!(edge_to_client.edge_scope_subscription_count(), 1);
    assert!(
        block_on(edge.transaction_state(tx_id)).is_none(),
        "a deferred edge upload must not persist before its permission scope settles"
    );
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
    assert!(
        block_on(edge.transaction_state(tx_id)).is_none(),
        "the unresolved upload must leave no durable edge history after restart"
    );

    let SyncMessage::CommitUnit { tx, versions } = unit else {
        panic!("expected redelivered commit unit");
    };
    let mut redelivered_edge_to_client = PeerState::edge_client(client_author);
    assert!(
        edge_ingest(
            &mut redelivered_edge_to_client,
            &mut edge,
            tx,
            versions,
            u64::MAX - SKEW_TOLERANCE_MS
        )
        .is_empty(),
        "redelivered unit reopens the permission-scope gate after restart"
    );
    let [fate] = drain_edge_fates(
        &mut redelivered_edge_to_client,
        &mut edge,
        u64::MAX - SKEW_TOLERANCE_MS,
    )
    .try_into()
    .unwrap();
    assert_eq!(
        fate,
        SyncMessage::FateUpdate {
            tx_id,
            fate: Fate::Accepted,
            global_time: None,
            durability: Some(DurabilityTier::Edge),
        }
    );
    assert_eq!(
        transaction_state(&mut edge, tx_id),
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
    let schema = public_write_schema();
    let client_author = AuthorSubject::for_test_bytes([7; 16]);

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

    let [fate] = edge_ingest(
        &mut edge_to_client,
        &mut edge,
        tx,
        versions,
        u64::MAX - SKEW_TOLERANCE_MS,
    )
    .try_into()
    .unwrap();
    assert_eq!(
        fate,
        SyncMessage::FateUpdate {
            tx_id,
            fate: Fate::Accepted,
            global_time: None,
            durability: Some(DurabilityTier::Edge),
        }
    );
    assert_eq!(
        transaction_state(&mut edge, tx_id),
        (Fate::Accepted, None, DurabilityTier::Edge)
    );
    drop(edge);
    drop(edge_to_client);

    let mut reopened = reopen_node(&edge_dir, node(3), schema);
    assert_eq!(
        transaction_state(&mut reopened, tx_id),
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
    let schema = public_write_schema();
    let client_author = AuthorSubject::for_test_bytes([7; 16]);

    let (_client_dir, mut client) = open_node(node(1), schema.clone());
    let (_edge_dir, mut edge) = open_node(node(3), schema);
    let mut edge_to_client = PeerState::edge_client(client_author);

    let (tx_id, unit) = commit(&mut client, row(25), 10, "public write", client_author, []);
    let SyncMessage::CommitUnit { tx, versions } = unit else {
        panic!("expected commit unit");
    };
    let [fate] = edge_ingest(
        &mut edge_to_client,
        &mut edge,
        tx,
        versions,
        u64::MAX - SKEW_TOLERANCE_MS,
    )
    .try_into()
    .unwrap();

    assert_eq!(
        fate,
        SyncMessage::FateUpdate {
            tx_id,
            fate: Fate::Accepted,
            global_time: None,
            durability: Some(DurabilityTier::Edge),
        }
    );
    assert_eq!(edge_to_client.deferred_edge_fate_count(), 0);
    assert_eq!(edge_to_client.edge_scope_subscription_count(), 0);
}

#[test]
fn edge_accepted_mergeable_is_final_at_core_after_policy_revocation() {
    let schema = access_write_policy_schema();
    let client_author = AuthorSubject::for_test_bytes([7; 16]);
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
    let grant_update =
        block_on(core_to_edge.current_rows_update(&mut core, "canvasInvites")).unwrap();
    apply_message(&mut edge, grant_update);

    let (tx_id, unit) = block_on(async {
        let (published, unit) = client
            .commit_mergeable_unit(
                MergeableCommit::new("canvases", canvas_row, 20)
                    .made_by(client_author)
                    .cells(title_only_cells("edge final")),
            )
            .await
            .unwrap();
        let tx_id = client
            .persist_and_settle_transaction(published)
            .await
            .unwrap();
        (tx_id, unit)
    });
    let SyncMessage::CommitUnit { tx, versions } = unit.clone() else {
        panic!("expected commit unit");
    };

    let mut edge_to_client = PeerState::edge_client(client_author);
    let first = edge_ingest(
        &mut edge_to_client,
        &mut edge,
        tx.clone(),
        versions.clone(),
        u64::MAX - SKEW_TOLERANCE_MS,
    );
    assert!(first.is_empty());
    let [edge_fate] =
        drain_edge_fates(&mut edge_to_client, &mut edge, u64::MAX - SKEW_TOLERANCE_MS)
            .try_into()
            .unwrap();
    assert_eq!(
        edge_fate,
        SyncMessage::FateUpdate {
            tx_id,
            fate: Fate::Accepted,
            global_time: None,
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

    let [control_fate] = apply_message(&mut control_core, unit).try_into().unwrap();
    assert!(matches!(
        control_fate,
        SyncMessage::FateUpdate {
            fate: Fate::Rejected(RejectionReason::AuthorizationDenied),
            ..
        }
    ));

    let shape = Query::from("canvases").validate(&schema).unwrap();
    let binding = shape.bind(BTreeMap::new()).unwrap();
    apply_message(
        &mut core,
        SyncMessage::ViewUpdate(jazz::protocol::ViewUpdatePayload {
            subscription: SubscriptionKey {
                shape_id: shape.shape_id(),
                binding_id: binding.binding_id(),
                read_view: Default::default(),
            },
            settled_through: jazz::time::GlobalTime(0),
            reset_result_set: false,
            version_carriers: Vec::new(),
            version_bundles: vec![VersionBundle {
                tx,
                versions,
                scope: jazz::protocol::VersionBundleScope::CompleteTransaction,
                fate: Fate::Accepted,
                global_time: None,
                durability: DurabilityTier::Edge,
            }],
            peer_payload_inventory: PeerPayloadInventory::default(),
            result_member_adds: Vec::new(),
            result_member_removes: Vec::new(),
            terminal_operations: Vec::new(),
            program_fact_adds: Vec::new(),
            program_fact_removes: Vec::new(),
        }),
    );

    let (fate, global_time, durability) = transaction_state(&mut core, tx_id);
    assert_eq!(fate, Fate::Accepted);
    assert!(global_time.is_none());
    assert_eq!(durability, DurabilityTier::Edge);
    let canvas_table = schema
        .tables
        .iter()
        .find(|table| table.name == "canvases")
        .expect("canvases schema");
    assert_eq!(
        block_on(core.current_rows("canvases", DurabilityTier::Edge))
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
