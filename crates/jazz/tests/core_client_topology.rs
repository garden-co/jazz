use std::collections::BTreeMap;

mod common;

use jazz::block_on;
use jazz::groove::records::Value;
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::node::{MergeableCommit, NodeState, SKEW_TOLERANCE_MS};
use jazz::peer::{PeerRole, PeerState};
use jazz::protocol::SyncMessage;
use jazz::schema::JazzSchema;
use jazz::tools::{
    ColumnType, OpenTransactionId, SchemaBuilder, TablePolicies, TableSchemaBuilder,
};
use jazz::tx::{DeletionEvent, DurabilityTier, Fate, RejectionReason, TxId};
use jazz_storage_rocksdb::RocksDbStorage;

use common::{compile_schema, session_eq};

fn node(byte: u8) -> NodeUuid {
    NodeUuid::from_bytes([byte; 16])
}

fn row(byte: u8) -> RowUuid {
    RowUuid::from_bytes([byte; 16])
}

// All user actors below share the synthetic test issuer. Text ownership
// addresses its subject explicitly; session.user is the complete author record.
fn schema() -> JazzSchema {
    // These topology tests are about identity-scoped delivery.  Keep their
    // fixture writes intentionally public; declaring SELECT alone now closes
    // the remaining operation clauses.
    let policies = TablePolicies::new()
        .with_select(session_eq("owner", &["user", "identity", "subject"]))
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

fn open_node(
    node_uuid: NodeUuid,
    schema: JazzSchema,
) -> (tempfile::TempDir, NodeState<RocksDbStorage>) {
    let temp_dir = tempfile::tempdir().unwrap();
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = RocksDbStorage::open(temp_dir.path(), &refs).unwrap();
    let node = block_on(NodeState::new_with_shared_test_catalogue(
        node_uuid, schema, storage,
    ))
    .unwrap();
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
    block_on(NodeState::new_with_shared_test_catalogue(
        node_uuid, schema, storage,
    ))
    .unwrap()
}

fn cells(title: &str, owner: AuthorSubject) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("title".to_owned(), Value::String(title.to_owned())),
        ("owner".to_owned(), Value::String(owner.principal_parts().1)),
    ])
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
                    .cells(cells(title, owner)),
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
    let update = block_on(common::direct_query_update(
        upstream,
        peer,
        &schema(),
        "todos",
    ));
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
fn local_persistence_forwards_pending_writes_and_core_fates() {
    let schema = schema();
    let ui_author = AuthorSubject::for_test_bytes([7; 16]);
    let ui_owner = ui_author;
    let other_owner = AuthorSubject::for_test_bytes([8; 16]);

    let (_ui_dir, mut ui) = open_node(node(1), schema.clone());
    let (worker_dir, mut worker) = open_node(node(2), schema.clone());
    let (_core_dir, mut core) = open_node(node(4), schema.clone());

    let mut core_to_ui = PeerState::client_link(ui_author);

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
        let fate = core_ingest(&mut core, unit, *now);
        apply_fate(&mut worker, &fate);
        apply_fate(&mut ui, &fate);
        fates.insert(*tx_id, fate);

        if idx == 3 {
            drop(worker);
            worker = reopen_node(&worker_dir, node(2), schema.clone());
        }
    }

    // Upload persistence is exercised through the local worker. Query
    // authorization is exercised at Core; the worker is never a serving
    // authority. Real transport/relay integration is covered by native-relay.
    block_on(common::register_direct_receiver(
        &mut ui,
        &schema,
        "todos",
        jazz::protocol::DelegatedSessionBinding {
            identity: ui_author,
            claims: BTreeMap::new(),
        },
    ));
    refresh(&mut core, &mut ui, &mut core_to_ui);

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
        let fate = core_ingest(&mut core, &unit, now);
        apply_fate(&mut worker, &fate);
        apply_fate(&mut ui, &fate);
        fates.insert(tx_id, fate);
    }

    refresh(&mut core, &mut ui, &mut core_to_ui);
    refresh(&mut core, &mut ui, &mut core_to_ui);

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
    assert_eq!(rows(&mut worker), expected_all);
    assert_eq!(subscription_rows(&mut ui), expected_ui);

    for node in [&mut ui, &mut worker, &mut core] {
        assert_eq!(
            transaction_state(node, skewed_tx).0,
            Fate::Rejected(RejectionReason::ClientClockTooFarAhead)
        );
    }

    let (_, global_time, _) = transaction_state(&mut core, exclusive_tx);
    for node in [&mut ui, &mut worker, &mut core] {
        assert_eq!(
            transaction_state(node, exclusive_tx),
            (Fate::Accepted, global_time, DurabilityTier::Global)
        );
    }

    assert!(core_to_ui.metrics.version_bundles_out > 0);
    assert_eq!(
        worker.sync_metrics().parked_orphans,
        worker.sync_metrics().parked_orphans_resolved
    );
}

#[test]
fn core_peer_terminates_client_identity_and_narrows_reads() {
    let schema = schema();
    let client_author = AuthorSubject::for_test_bytes([7; 16]);
    let other_owner = AuthorSubject::for_test_bytes([8; 16]);

    let (_client_dir, mut client) = open_node(node(1), schema.clone());
    let (_core_dir, mut core) = open_node(node(4), schema.clone());

    let mut core_to_client = PeerState::client_link(client_author);

    assert_eq!(
        core_to_client.role(),
        PeerRole::ClientLink {
            identity: client_author
        }
    );
    assert_eq!(core_to_client.identity(), client_author);

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
        let fate = core_ingest(&mut core, &unit, u64::MAX - SKEW_TOLERANCE_MS);
        apply_fate(&mut client, &fate);
        assert_eq!(
            transaction_state(&mut client, tx_id).0,
            Fate::Accepted,
            "test setup should accept all client units"
        );
    }

    block_on(common::register_direct_receiver(
        &mut client,
        &schema,
        "todos",
        jazz::protocol::DelegatedSessionBinding {
            identity: client_author,
            claims: BTreeMap::new(),
        },
    ));
    refresh(&mut core, &mut client, &mut core_to_client);

    let expected_all = vec![
        (client_row, Value::String("client visible".to_owned())),
        (other_row, Value::String("core only".to_owned())),
    ];
    let expected_client = vec![(client_row, Value::String("client visible".to_owned()))];

    assert_eq!(rows(&mut core), expected_all);
    assert_eq!(subscription_rows(&mut client), expected_client);
}
