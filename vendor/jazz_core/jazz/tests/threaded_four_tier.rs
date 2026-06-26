use std::collections::BTreeMap;
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::thread;
use std::time::Duration;

use jazz::groove::records::Value;
use jazz::groove::schema::{ColumnSchema, ColumnType};
use jazz::groove::storage::RocksDbStorage;
use jazz::ids::{AuthorId, NodeUuid, RowUuid};
use jazz::node::{CurrentRow, MergeableCommit, NodeState, SKEW_TOLERANCE_MS};
use jazz::peer::{PeerMetrics, PeerState};
use jazz::protocol::SyncMessage;
use jazz::schema::{JazzSchema, Policy, TableSchema};
use jazz::tx::{DeletionEvent, DurabilityTier, Fate, TxId};
use jazz::wire::{
    FEATURE_SYNC_MESSAGE_PAYLOAD, WIRE_PROTOCOL_VERSION, WireEnvelope, WireFrame, decode_frame,
    decode_sync_message, encode_frame, encode_sync_message,
};

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
    node: NodeState<RocksDbStorage>,
    downstream_peer: Option<LinkSummary>,
}

struct UiResult {
    node: NodeState<RocksDbStorage>,
    tx_ids: Vec<TxId>,
}

fn node(byte: u8) -> NodeUuid {
    NodeUuid::from_bytes([byte; 16])
}

fn row(idx: u8) -> RowUuid {
    RowUuid::from_bytes([idx; 16])
}

fn schema() -> JazzSchema {
    JazzSchema::new([TableSchema::new(
        TABLE,
        [
            ColumnSchema::new("title", ColumnType::String),
            ColumnSchema::new("owner", ColumnType::Uuid),
        ],
    )
    .with_read_policy(Policy::owner_only("todos", "owner"))])
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

fn cells(title: impl Into<String>, owner: AuthorId) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("title".to_owned(), Value::String(title.into())),
        ("owner".to_owned(), Value::Uuid(owner.0)),
    ])
}

fn peer_summary(peer: &PeerState) -> LinkSummary {
    LinkSummary {
        metrics: peer.metrics.clone(),
        shipped_complete_tx_payloads: peer.shipped_complete_tx_payloads().len(),
    }
}

fn send_view(node: &mut NodeState<RocksDbStorage>, peer: &mut PeerState, tx: &Sender<Wire>) {
    let update = peer.current_rows_update(node, TABLE).unwrap();
    send_sync(tx, update);
}

fn relay_ingest(node: &mut NodeState<RocksDbStorage>, message: &SyncMessage) {
    let SyncMessage::CommitUnit { tx, versions } = message else {
        panic!("expected commit unit");
    };
    node.ingest_relay_commit_unit(tx.clone(), versions.clone())
        .unwrap();
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
            node.apply_sync_message(*sync).unwrap();
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

fn core_thread(
    mut core: NodeState<RocksDbStorage>,
    from_edge: Receiver<Wire>,
    to_edge: Sender<Wire>,
) -> ThreadResult {
    let mut peer = PeerState::new();
    let mut ingests = 0_usize;
    loop {
        match from_edge.recv().unwrap() {
            message @ (Wire::Sync(_) | Wire::Frame(_)) => {
                let sync = message.into_sync().unwrap();
                let SyncMessage::CommitUnit { tx, versions } = *sync else {
                    panic!("core expected commit unit");
                };
                let updates = core
                    .ingest_commit_unit(tx, versions, u64::MAX - SKEW_TOLERANCE_MS)
                    .unwrap();
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
    ThreadResult {
        node: core,
        downstream_peer: Some(peer_summary(&peer)),
    }
}

fn relay_thread(
    mut node: NodeState<RocksDbStorage>,
    upstream_rx: Receiver<Wire>,
    upstream_tx: Sender<Wire>,
    downstream_rx: Receiver<Wire>,
    downstream_tx: Sender<Wire>,
    mut downstream_peer: PeerState,
) -> ThreadResult {
    let mut forwarded = 0_usize;
    let mut upstream_stopped = false;
    loop {
        while let Ok(message) = downstream_rx.try_recv() {
            if process_downstream(&mut node, message, &downstream_tx, &mut downstream_peer) {
                return ThreadResult {
                    node,
                    downstream_peer: Some(peer_summary(&downstream_peer)),
                };
            }
        }

        if upstream_stopped {
            let message = downstream_rx.recv().unwrap();
            if process_downstream(&mut node, message, &downstream_tx, &mut downstream_peer) {
                return ThreadResult {
                    node,
                    downstream_peer: Some(peer_summary(&downstream_peer)),
                };
            }
            continue;
        }

        match upstream_rx.recv_timeout(Duration::from_millis(1)) {
            Ok(message @ (Wire::Sync(_) | Wire::Frame(_))) => {
                let sync = message.into_sync().unwrap();
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
                node.apply_sync_message(*sync).unwrap();
            }
            Wire::Stop => panic!("ui received early stop"),
        }
    }
}

fn ui_thread(
    mut ui: NodeState<RocksDbStorage>,
    to_worker: Sender<Wire>,
    from_worker: Receiver<Wire>,
    ui_author: AuthorId,
    ui_owner: AuthorId,
) -> UiResult {
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
        let (tx_id, unit) = ui
            .commit_mergeable_unit(commit.cells(cells(title, ui_owner)))
            .unwrap();
        parents.insert(row_uuid, tx_id);
        tx_ids.push(tx_id);
        send_sync(&to_worker, unit);

        if idx == 20 || idx == 70 {
            let deleted = row(30 + (idx / 50) as u8);
            let (base_tx, base_unit) = ui
                .commit_mergeable_unit(
                    MergeableCommit::new(TABLE, deleted, 500 + idx)
                        .made_by(ui_author)
                        .cells(cells(format!("delete-base-{idx}"), ui_owner)),
                )
                .unwrap();
            parents.insert(deleted, base_tx);
            tx_ids.push(base_tx);
            send_sync(&to_worker, base_unit);

            let (delete_tx, delete_unit) = ui
                .commit_mergeable_unit(
                    MergeableCommit::new(TABLE, deleted, 501 + idx)
                        .made_by(ui_author)
                        .deletion(DeletionEvent::Deleted),
                )
                .unwrap();
            tx_ids.push(delete_tx);
            send_sync(&to_worker, delete_unit);

            let (restore_tx, restore_unit) = ui
                .commit_mergeable_unit(
                    MergeableCommit::new(TABLE, deleted, 502 + idx)
                        .made_by(ui_author)
                        .deletion(DeletionEvent::Restored),
                )
                .unwrap();
            tx_ids.push(restore_tx);
            send_sync(&to_worker, restore_unit);
        }

        if idx % 18 == 12 {
            drain_ui_downstream(&mut ui, &from_worker);
            let row_uuid = row(40 + ((idx / 18) % 8) as u8);
            let tx_id = ui.open_exclusive().unwrap();
            let _ = ui.tx_read(tx_id, TABLE, row_uuid).unwrap();
            let title = format!("exclusive-{idx}");
            ui.tx_write(tx_id, TABLE, row_uuid, cells(title, ui_owner), None)
                .unwrap();
            let (tx_id, unit) = ui.commit_exclusive(tx_id, ui_author, 1_000 + idx).unwrap();
            tx_ids.push(tx_id);
            send_sync(&to_worker, unit);
        }
    }

    to_worker.send(Wire::Stop).unwrap();
    while let Some(sync) = from_worker.recv().unwrap().into_sync() {
        ui.apply_sync_message(*sync).unwrap();
    }

    UiResult { node: ui, tx_ids }
}

fn global_rows(node: &mut NodeState<RocksDbStorage>) -> BTreeMap<RowUuid, BTreeMap<String, Value>> {
    let schema = schema();
    let table = &schema.tables[0];
    node.current_rows(TABLE, DurabilityTier::Global)
        .unwrap()
        .into_iter()
        .map(|row| (row.row_uuid(), row_cells(&row, table)))
        .collect()
}

fn local_rows(node: &mut NodeState<RocksDbStorage>) -> BTreeMap<RowUuid, BTreeMap<String, Value>> {
    let schema = schema();
    let table = &schema.tables[0];
    node.current_rows(TABLE, DurabilityTier::Local)
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
    node.subscription_current_rows(TABLE, DurabilityTier::Global)
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
            row.cell(table, &column.name)
                .map(|value| (column.name.clone(), value))
        })
        .collect()
}

fn assert_link_dedup(summary: LinkSummary) {
    // Maintained table-filtered current-row deltas may intentionally re-bundle
    // a complete tx payload that is already known to the peer when that tx is
    // not in the previous filtered result set. Keep those safety re-bundles
    // visible in the metric instead of treating every complete bundle as newly
    // shipped.
    assert_eq!(
        summary.metrics.version_bundles_out,
        summary.shipped_complete_tx_payloads as u64 + summary.metrics.duplicate_version_bundles_out
    );
    assert!(summary.metrics.view_updates_out > 0);
}

#[test]
fn threaded_four_tier_converges_with_fifo_links() {
    let schema = schema();
    let ui_author = AuthorId::from_bytes([7; 16]);
    let ui_owner = ui_author;

    let (_ui_dir, ui) = open_node(node(1), schema.clone());
    let (_worker_dir, worker) = open_node(node(2), schema.clone());
    let (_edge_dir, edge) = open_node(node(3), schema.clone());
    let (_core_dir, core) = open_node(node(4), schema);

    let (ui_to_worker_tx, ui_to_worker_rx) = mpsc::channel::<Wire>();
    let (worker_to_ui_tx, worker_to_ui_rx) = mpsc::channel::<Wire>();
    let (worker_to_edge_tx, worker_to_edge_rx) = mpsc::channel::<Wire>();
    let (edge_to_worker_tx, edge_to_worker_rx) = mpsc::channel::<Wire>();
    let (edge_to_core_tx, edge_to_core_rx) = mpsc::channel::<Wire>();
    let (core_to_edge_tx, core_to_edge_rx) = mpsc::channel::<Wire>();

    let core_handle = thread::spawn(move || core_thread(core, edge_to_core_rx, core_to_edge_tx));
    let edge_handle = thread::spawn(move || {
        relay_thread(
            edge,
            worker_to_edge_rx,
            edge_to_core_tx,
            core_to_edge_rx,
            edge_to_worker_tx,
            PeerState::new(),
        )
    });
    let worker_handle = thread::spawn(move || {
        relay_thread(
            worker,
            ui_to_worker_rx,
            worker_to_edge_tx,
            edge_to_worker_rx,
            worker_to_ui_tx,
            PeerState::for_author(ui_author),
        )
    });
    let ui_handle =
        thread::spawn(move || ui_thread(ui, ui_to_worker_tx, worker_to_ui_rx, ui_author, ui_owner));

    let mut ui_result = ui_handle.join().unwrap();
    let mut worker_result = worker_handle.join().unwrap();
    let mut edge_result = edge_handle.join().unwrap();
    let mut core_result = core_handle.join().unwrap();

    let core_global = global_rows(&mut core_result.node);
    let edge_global = global_rows(&mut edge_result.node);
    let worker_global = global_rows(&mut worker_result.node);
    let ui_global = global_rows(&mut ui_result.node);
    assert_eq!(edge_global, core_global);
    assert_eq!(worker_global, core_global);
    assert_eq!(ui_global, core_global);

    assert_eq!(local_rows(&mut core_result.node), core_global);
    assert_eq!(local_rows(&mut edge_result.node), core_global);
    assert_eq!(local_rows(&mut worker_result.node), core_global);
    assert_eq!(local_rows(&mut ui_result.node), core_global);

    let ui_policy_rows = subscription_rows(&mut ui_result.node);
    assert!(!ui_policy_rows.is_empty());
    assert!(
        ui_policy_rows
            .values()
            .all(|cells| cells.get("owner") == Some(&Value::Uuid(ui_author.0)))
    );

    for tx_id in ui_result.tx_ids {
        let core_fact = core_result.node.transaction_state(tx_id).unwrap();
        assert_eq!(
            edge_result.node.transaction_state(tx_id).unwrap(),
            core_fact
        );
        assert_eq!(
            worker_result.node.transaction_state(tx_id).unwrap(),
            core_fact
        );
        assert_eq!(ui_result.node.transaction_state(tx_id).unwrap(), core_fact);
        assert!(!matches!(core_fact.0, Fate::Pending));
    }

    assert_link_dedup(core_result.downstream_peer.unwrap());
    assert_link_dedup(edge_result.downstream_peer.unwrap());
    assert_link_dedup(worker_result.downstream_peer.unwrap());

    assert_eq!(
        edge_result.node.sync_metrics().parked_orphans,
        edge_result.node.sync_metrics().parked_orphans_resolved
    );
    assert_eq!(
        worker_result.node.sync_metrics().parked_orphans,
        worker_result.node.sync_metrics().parked_orphans_resolved
    );
}
