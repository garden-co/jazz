use std::collections::BTreeMap;
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::thread;
use std::time::Duration;

mod common;

use jazz::block_on;
use jazz::groove::records::Value;
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::node::{CurrentRow, MergeableCommit, NodeState, SKEW_TOLERANCE_MS};
use jazz::peer::{PeerMetrics, PeerState};
use jazz::protocol::SyncMessage;
use jazz::schema::{JazzSchema, TableSchema};
use jazz::tools::{
    ColumnType, OpenTransactionId, SchemaBuilder, TablePolicies, TableSchemaBuilder,
};
use jazz::tx::{DeletionEvent, DurabilityTier, Fate, TxId};
use jazz::wire::{
    FEATURE_SYNC_MESSAGE_PAYLOAD, WIRE_PROTOCOL_VERSION, WireEnvelope, WireFrame, decode_frame,
    decode_sync_message, encode_frame, encode_sync_message,
};
use jazz_storage_rocksdb::RocksDbStorage;

use common::{compile_schema, session_eq};

const TABLE: &str = "todos";
const REFRESH_EVERY: usize = 25;

#[derive(Clone, Debug)]
enum Wire {
    #[allow(dead_code)]
    Sync(Box<SyncMessage>),
    Frame(Vec<u8>),
    Stop,
}

impl Wire {
    fn encoded(message: SyncMessage) -> Self {
        let payload = encode_sync_message(&message).unwrap();
        let frame = WireFrame::Message(WireEnvelope::new(
            WIRE_PROTOCOL_VERSION,
            FEATURE_SYNC_MESSAGE_PAYLOAD,
            payload,
        ));
        Wire::Frame(encode_frame(&frame).unwrap())
    }

    fn into_sync(self) -> Option<Box<SyncMessage>> {
        match self {
            Wire::Sync(sync) => Some(sync),
            Wire::Frame(bytes) => {
                let frame = decode_frame(&bytes).unwrap();
                let WireFrame::Message(envelope) = frame else {
                    panic!("expected wire message frame");
                };
                assert_eq!(envelope.protocol_version, WIRE_PROTOCOL_VERSION);
                assert_eq!(envelope.features, FEATURE_SYNC_MESSAGE_PAYLOAD);
                assert!(envelope.session.is_none());
                Some(Box::new(decode_sync_message(&envelope.payload).unwrap()))
            }
            Wire::Stop => None,
        }
    }
}

fn send_sync(tx: &Sender<Wire>, message: SyncMessage) {
    tx.send(Wire::encoded(message)).unwrap();
}

#[derive(Clone, Debug, Default)]
struct LinkSummary {
    metrics: PeerMetrics,
    shipped_complete_tx_payloads: usize,
}

struct ThreadResult {
    global_rows: BTreeMap<RowUuid, BTreeMap<String, Value>>,
    local_rows: BTreeMap<RowUuid, BTreeMap<String, Value>>,
    subscription_rows: BTreeMap<RowUuid, BTreeMap<String, Value>>,
    transaction_states: BTreeMap<TxId, (Fate, Option<jazz::time::GlobalTime>, DurabilityTier)>,
    sync_metrics: jazz::node::SyncMetrics,
    downstream_peer: Option<LinkSummary>,
}

struct UiResult {
    tx_ids: Vec<TxId>,
    receipt: ThreadResult,
}

fn node(byte: u8) -> NodeUuid {
    NodeUuid::from_bytes([byte; 16])
}

fn row(idx: u8) -> RowUuid {
    RowUuid::from_bytes([idx; 16])
}

fn schema() -> JazzSchema {
    // The concurrent topology itself is under test, not write authorization.
    // Make every fixture mutation explicit now that one declared policy closes
    // omitted operations.
    let policies = TablePolicies::new()
        .with_select(session_eq("owner", &["claims", "sub"]))
        .with_insert(jazz::tools::PolicyExpr::True)
        .with_update(
            Some(jazz::tools::PolicyExpr::True),
            jazz::tools::PolicyExpr::True,
        )
        .with_delete(jazz::tools::PolicyExpr::True);
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new(TABLE)
                    .column("title", ColumnType::Text)
                    .column("owner", ColumnType::Uuid)
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
    let node = block_on(NodeState::new(node_uuid, schema, storage)).unwrap();
    (temp_dir, node)
}

fn cells(title: impl Into<String>, owner: AuthorSubject) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("title".to_owned(), Value::String(title.into())),
        ("owner".to_owned(), Value::Uuid(owner.test_uuid())),
    ])
}

fn install_uuid_sub_claim(node: &mut NodeState<RocksDbStorage>, identity: AuthorSubject) {
    if identity != AuthorSubject::SYSTEM {
        node.admit_test_session_claims(
            identity,
            BTreeMap::from([("sub".to_owned(), Value::Uuid(identity.test_uuid()))]),
        );
    }
}

fn peer_summary(peer: &PeerState) -> LinkSummary {
    LinkSummary {
        metrics: peer.metrics.clone(),
        shipped_complete_tx_payloads: peer.shipped_complete_tx_payloads().len(),
    }
}

fn apply_message(node: &mut NodeState<RocksDbStorage>, message: SyncMessage) -> Vec<SyncMessage> {
    block_on(async {
        let outcome = node.apply_sync_message(message).await.unwrap();
        node.persist_and_settle_outcome(outcome).await.unwrap()
    })
}

fn commit_unit(
    node: &mut NodeState<RocksDbStorage>,
    commit: MergeableCommit,
) -> (TxId, SyncMessage) {
    block_on(async {
        let (published, unit) = node.commit_mergeable_unit(commit).await.unwrap();
        let tx_id = node
            .persist_and_settle_transaction(published)
            .await
            .unwrap();
        (tx_id, unit)
    })
}

fn send_view(node: &mut NodeState<RocksDbStorage>, peer: &mut PeerState, tx: &Sender<Wire>) {
    install_uuid_sub_claim(node, peer.identity());
    let update = block_on(peer.current_rows_update(node, TABLE)).unwrap();
    send_sync(tx, update);
}

fn relay_ingest(node: &mut NodeState<RocksDbStorage>, message: &SyncMessage) {
    let SyncMessage::CommitUnit { tx, versions } = message else {
        panic!("expected commit unit");
    };
    if let Some(identity) = tx.permission_subject {
        install_uuid_sub_claim(node, identity);
    }
    block_on(node.ingest_relay_commit_unit(tx.clone(), versions.clone())).unwrap();
}

fn process_downstream(
    node: &mut NodeState<RocksDbStorage>,
    message: Wire,
    downstream_tx: &Sender<Wire>,
    downstream_peer: &mut PeerState,
) -> bool {
    match message {
        Wire::Sync(_) | Wire::Frame(_) => {
            let sync = message.into_sync().unwrap();
            let forward_fate =
                matches!(&*sync, SyncMessage::FateUpdate { .. }).then(|| sync.clone());
            apply_message(node, *sync);
            if let Some(fate) = forward_fate {
                send_sync(downstream_tx, *fate);
            }
            send_view(node, downstream_peer, downstream_tx);
            false
        }
        Wire::Stop => {
            send_view(node, downstream_peer, downstream_tx);
            downstream_tx.send(Wire::Stop).unwrap();
            true
        }
    }
}

fn finish_node(
    node: &mut NodeState<RocksDbStorage>,
    tx_ids: impl IntoIterator<Item = TxId>,
    downstream_peer: Option<LinkSummary>,
) -> ThreadResult {
    let transaction_states = tx_ids
        .into_iter()
        .map(|tx_id| (tx_id, block_on(node.transaction_state(tx_id)).unwrap()))
        .collect();
    ThreadResult {
        global_rows: global_rows(node),
        local_rows: local_rows(node),
        subscription_rows: subscription_rows(node),
        transaction_states,
        sync_metrics: node.sync_metrics().clone(),
        downstream_peer,
    }
}

fn core_thread(
    schema: JazzSchema,
    from_edge: Receiver<Wire>,
    to_edge: Sender<Wire>,
) -> ThreadResult {
    let (_dir, mut core) = open_node(node(4), schema);
    let mut peer = PeerState::new();
    let mut ingests = 0_usize;
    let mut tx_ids = Vec::new();
    loop {
        match from_edge.recv().unwrap() {
            message @ (Wire::Sync(_) | Wire::Frame(_)) => {
                let sync = message.into_sync().unwrap();
                let SyncMessage::CommitUnit { tx, versions } = *sync else {
                    panic!("core expected commit unit");
                };
                tx_ids.push(tx.tx_id);
                if let Some(identity) = tx.permission_subject {
                    install_uuid_sub_claim(&mut core, identity);
                }
                let updates = block_on(async {
                    let outcome = core
                        .ingest_commit_unit(tx, versions, u64::MAX - SKEW_TOLERANCE_MS)
                        .await
                        .unwrap();
                    core.persist_and_settle_outcome(outcome).await.unwrap()
                });
                for update in updates {
                    send_sync(&to_edge, update);
                }
                ingests += 1;
                if ingests.is_multiple_of(REFRESH_EVERY) {
                    send_view(&mut core, &mut peer, &to_edge);
                }
            }
            Wire::Stop => {
                send_view(&mut core, &mut peer, &to_edge);
                to_edge.send(Wire::Stop).unwrap();
                break;
            }
        }
    }
    finish_node(&mut core, tx_ids, Some(peer_summary(&peer)))
}

fn relay_thread(
    node_byte: u8,
    schema: JazzSchema,
    upstream_rx: Receiver<Wire>,
    upstream_tx: Sender<Wire>,
    downstream_rx: Receiver<Wire>,
    downstream_tx: Sender<Wire>,
    mut downstream_peer: PeerState,
) -> ThreadResult {
    let (_dir, mut node) = open_node(node(node_byte), schema);
    let mut forwarded = 0_usize;
    let mut tx_ids = Vec::new();
    let mut upstream_stopped = false;
    loop {
        while let Ok(message) = downstream_rx.try_recv() {
            if process_downstream(&mut node, message, &downstream_tx, &mut downstream_peer) {
                return finish_node(&mut node, tx_ids, Some(peer_summary(&downstream_peer)));
            }
        }

        if upstream_stopped {
            let message = downstream_rx.recv().unwrap();
            if process_downstream(&mut node, message, &downstream_tx, &mut downstream_peer) {
                return finish_node(&mut node, tx_ids, Some(peer_summary(&downstream_peer)));
            }
            continue;
        }

        match upstream_rx.recv_timeout(Duration::from_millis(1)) {
            Ok(message @ (Wire::Sync(_) | Wire::Frame(_))) => {
                let sync = message.into_sync().unwrap();
                if let SyncMessage::CommitUnit { tx, .. } = &*sync {
                    tx_ids.push(tx.tx_id);
                }
                relay_ingest(&mut node, &sync);
                send_sync(&upstream_tx, *sync);
                forwarded += 1;
                if forwarded.is_multiple_of(REFRESH_EVERY) {
                    send_view(&mut node, &mut downstream_peer, &downstream_tx);
                }
            }
            Ok(Wire::Stop) => {
                upstream_tx.send(Wire::Stop).unwrap();
                upstream_stopped = true;
            }
            Err(RecvTimeoutError::Timeout) => {}
            Err(RecvTimeoutError::Disconnected) => panic!("upstream channel disconnected"),
        }
    }
}

fn drain_ui_downstream(node: &mut NodeState<RocksDbStorage>, rx: &Receiver<Wire>) {
    while let Ok(message) = rx.try_recv() {
        match message {
            Wire::Sync(_) | Wire::Frame(_) => {
                let sync = message.into_sync().unwrap();
                apply_message(node, *sync);
            }
            Wire::Stop => panic!("ui received early stop"),
        }
    }
}

fn ui_thread(
    schema: JazzSchema,
    to_worker: Sender<Wire>,
    from_worker: Receiver<Wire>,
    ui_author: AuthorSubject,
    ui_owner: AuthorSubject,
) -> UiResult {
    let (_dir, mut ui) = open_node(node(1), schema);
    let mut tx_ids = Vec::new();
    let mut parents = BTreeMap::<RowUuid, TxId>::new();

    for idx in 0..180_u64 {
        drain_ui_downstream(&mut ui, &from_worker);
        let row_uuid = row((idx % 24) as u8 + 1);
        let mut commit = MergeableCommit::new(TABLE, row_uuid, 10 + idx).made_by(ui_author);
        if let Some(parent) = parents.get(&row_uuid).copied() {
            commit = commit.parents(vec![parent]);
        }
        let title = format!("merge-{idx}");
        let (tx_id, unit) = commit_unit(&mut ui, commit.cells(cells(title, ui_owner)));
        parents.insert(row_uuid, tx_id);
        tx_ids.push(tx_id);
        send_sync(&to_worker, unit);

        if idx == 20 || idx == 70 {
            let deleted = row(30 + (idx / 50) as u8);
            let (base_tx, base_unit) = commit_unit(
                &mut ui,
                MergeableCommit::new(TABLE, deleted, 500 + idx)
                    .made_by(ui_author)
                    .cells(cells(format!("delete-base-{idx}"), ui_owner)),
            );
            parents.insert(deleted, base_tx);
            tx_ids.push(base_tx);
            send_sync(&to_worker, base_unit);

            let (delete_tx, delete_unit) = commit_unit(
                &mut ui,
                MergeableCommit::new(TABLE, deleted, 501 + idx)
                    .made_by(ui_author)
                    .deletion(DeletionEvent::Deleted),
            );
            tx_ids.push(delete_tx);
            send_sync(&to_worker, delete_unit);

            let (restore_tx, restore_unit) = commit_unit(
                &mut ui,
                MergeableCommit::new(TABLE, deleted, 502 + idx)
                    .made_by(ui_author)
                    .deletion(DeletionEvent::Restored),
            );
            tx_ids.push(restore_tx);
            send_sync(&to_worker, restore_unit);
        }

        if idx % 18 == 12 {
            drain_ui_downstream(&mut ui, &from_worker);
            let row_uuid = row(40 + ((idx / 18) % 8) as u8);
            let tx_id = OpenTransactionId::new();
            block_on(ui.open_exclusive_for_test(tx_id, ui_author)).unwrap();
            let _ = block_on(ui.tx_read(tx_id, TABLE, row_uuid)).unwrap();
            let title = format!("exclusive-{idx}");
            block_on(ui.tx_write(tx_id, TABLE, row_uuid, cells(title, ui_owner), None)).unwrap();
            let (tx_id, unit) = block_on(async {
                let (published, unit) = ui
                    .commit_exclusive(tx_id, ui_author, 1_000 + idx)
                    .await
                    .unwrap();
                let tx_id = ui.persist_and_settle_transaction(published).await.unwrap();
                (tx_id, unit)
            });
            tx_ids.push(tx_id);
            send_sync(&to_worker, unit);
        }
    }

    to_worker.send(Wire::Stop).unwrap();
    while let Some(sync) = from_worker.recv().unwrap().into_sync() {
        apply_message(&mut ui, *sync);
    }

    let receipt = finish_node(&mut ui, tx_ids.iter().copied(), None);
    UiResult { tx_ids, receipt }
}

fn global_rows(node: &mut NodeState<RocksDbStorage>) -> BTreeMap<RowUuid, BTreeMap<String, Value>> {
    let schema = schema();
    let table = &schema.tables[0];
    block_on(node.current_rows(TABLE, DurabilityTier::Global))
        .unwrap()
        .into_iter()
        .map(|row| (row.row_uuid(), row_cells(&row, table)))
        .collect()
}

fn local_rows(node: &mut NodeState<RocksDbStorage>) -> BTreeMap<RowUuid, BTreeMap<String, Value>> {
    let schema = schema();
    let table = &schema.tables[0];
    block_on(node.current_rows(TABLE, DurabilityTier::Local))
        .unwrap()
        .into_iter()
        .map(|row| (row.row_uuid(), row_cells(&row, table)))
        .collect()
}

fn subscription_rows(
    node: &mut NodeState<RocksDbStorage>,
) -> BTreeMap<RowUuid, BTreeMap<String, Value>> {
    let schema = schema();
    let table = &schema.tables[0];
    block_on(node.subscription_current_rows(TABLE, DurabilityTier::Global))
        .unwrap()
        .into_iter()
        .map(|row| (row.row_uuid(), row_cells(&row, table)))
        .collect()
}

fn row_cells(row: &CurrentRow, table: &TableSchema) -> BTreeMap<String, Value> {
    table
        .columns
        .iter()
        .filter_map(|column| {
            row.cell(table, column.name())
                .map(|value| (column.name().to_owned(), value))
        })
        .collect()
}

fn assert_link_dedup(summary: LinkSummary) {
    assert!(summary.metrics.view_updates_out > 0);
    assert_eq!(summary.metrics.duplicate_version_bundles_out, 0);
    assert_eq!(summary.metrics.complete_tx_payload_refs_out, 0);
    assert_eq!(summary.shipped_complete_tx_payloads, 0);
}

#[test]
fn threaded_four_tier_converges_with_fifo_links() {
    let schema = schema();
    let ui_author = AuthorSubject::for_test_bytes([7; 16]);
    let ui_owner = ui_author;

    let (ui_to_worker_tx, ui_to_worker_rx) = mpsc::channel::<Wire>();
    let (worker_to_ui_tx, worker_to_ui_rx) = mpsc::channel::<Wire>();
    let (worker_to_edge_tx, worker_to_edge_rx) = mpsc::channel::<Wire>();
    let (edge_to_worker_tx, edge_to_worker_rx) = mpsc::channel::<Wire>();
    let (edge_to_core_tx, edge_to_core_rx) = mpsc::channel::<Wire>();
    let (core_to_edge_tx, core_to_edge_rx) = mpsc::channel::<Wire>();

    let core_schema = schema.clone();
    let core_handle =
        thread::spawn(move || core_thread(core_schema, edge_to_core_rx, core_to_edge_tx));
    let edge_schema = schema.clone();
    let edge_handle = thread::spawn(move || {
        relay_thread(
            3,
            edge_schema,
            worker_to_edge_rx,
            edge_to_core_tx,
            core_to_edge_rx,
            edge_to_worker_tx,
            PeerState::new(),
        )
    });
    let worker_schema = schema.clone();
    let worker_handle = thread::spawn(move || {
        relay_thread(
            2,
            worker_schema,
            ui_to_worker_rx,
            worker_to_edge_tx,
            edge_to_worker_rx,
            worker_to_ui_tx,
            PeerState::client_link(ui_author),
        )
    });
    let ui_handle = thread::spawn(move || {
        ui_thread(
            schema,
            ui_to_worker_tx,
            worker_to_ui_rx,
            ui_author,
            ui_owner,
        )
    });

    let ui_result = ui_handle.join().unwrap();
    let worker_result = worker_handle.join().unwrap();
    let edge_result = edge_handle.join().unwrap();
    let core_result = core_handle.join().unwrap();

    let core_global = &core_result.global_rows;
    let edge_global = &edge_result.global_rows;
    let worker_global = &worker_result.global_rows;
    let ui_global = &ui_result.receipt.global_rows;
    assert_eq!(edge_global, core_global);
    assert_eq!(worker_global, core_global);
    assert_eq!(ui_global, core_global);

    assert_eq!(&core_result.local_rows, core_global);
    assert_eq!(&edge_result.local_rows, core_global);
    assert_eq!(&worker_result.local_rows, core_global);
    assert_eq!(&ui_result.receipt.local_rows, core_global);

    let ui_policy_rows = &ui_result.receipt.subscription_rows;
    assert!(!ui_policy_rows.is_empty());
    assert!(
        ui_policy_rows
            .values()
            .all(|cells| { cells.get("owner") == Some(&Value::Uuid(ui_author.test_uuid())) })
    );

    for tx_id in ui_result.tx_ids {
        let core_fact = core_result.transaction_states.get(&tx_id).unwrap();
        assert_eq!(
            edge_result.transaction_states.get(&tx_id).unwrap(),
            core_fact
        );
        assert_eq!(
            worker_result.transaction_states.get(&tx_id).unwrap(),
            core_fact
        );
        assert_eq!(
            ui_result.receipt.transaction_states.get(&tx_id).unwrap(),
            core_fact
        );
        assert!(!matches!(core_fact.0, Fate::Pending));
    }

    assert_link_dedup(core_result.downstream_peer.unwrap());
    assert_link_dedup(edge_result.downstream_peer.unwrap());
    assert_link_dedup(worker_result.downstream_peer.unwrap());

    assert_eq!(
        edge_result.sync_metrics.parked_orphans,
        edge_result.sync_metrics.parked_orphans_resolved
    );
    assert_eq!(
        worker_result.sync_metrics.parked_orphans,
        worker_result.sync_metrics.parked_orphans_resolved
    );
}
