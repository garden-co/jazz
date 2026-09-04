use std::collections::BTreeMap;
use std::time::Instant;

mod schema_fixture;
mod support;

use jazz::block_on;
use jazz::groove::ivm::TickMetrics;
use jazz::groove::records::Value;
use jazz::ids::{NodeUuid, RowUuid};
use jazz::node::{MergeableCommit, NodeState, SKEW_TOLERANCE_MS};
use jazz::peer::PeerState;
use jazz::protocol::SyncMessage;
use jazz::protocol::expand_version_carriers;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
use jazz::tx::{DurabilityTier, Fate};
use jazz_storage_rocksdb::{Durability, RocksDbStorage};
use support::{
    csv_usizes, emit_json_line, insert_durability_tier, insert_node_metrics, phase_fields,
    reset_phase_counters,
};

const TABLE: &str = "todos";

fn main() {
    jazz_benchmark_guard::refuse_contaminated_measurement();
    for depth in depths() {
        for ahead in pending_sizes() {
            let mut bench = ColdSubscriptionBench::new();
            bench.seed_history(depth);
            bench.seed_pending(ahead);

            let global = bench.current_rows_update_elapsed(DurabilityTier::Global);
            let local = bench.current_rows_update_elapsed(DurabilityTier::Local);
            bench.emit_result(depth, ahead, DurabilityTier::Global, global);
            bench.emit_result(depth, ahead, DurabilityTier::Local, local);
        }
    }
}

#[allow(dead_code)]
pub(crate) fn correctness_smoke() {
    let mut bench = ColdSubscriptionBench::new();
    bench.seed_history(2);
    bench.seed_pending(1);
    let _ = bench.current_rows_update_elapsed(DurabilityTier::Global);
    let _ = bench.current_rows_update_elapsed(DurabilityTier::Local);
    current_state_history_depth_contract();
}

/// An ordinary current-state subscriber must receive and materialize only the
/// current winner, regardless of how many superseded versions of that one row
/// the authority retains.  This is deliberately an internal receipt because
/// storage read buckets and query-engine counters have no public application
/// API, while the update itself is a real core-to-peer protocol message.
/// It intentionally excludes explicit historical/as-of (`current_rows_at`),
/// branch-base, and direct version-fetch operations: those name history and
/// therefore do not have this current-state complexity contract.
///
/// ```text
/// writer ──1/10/1000 successive updates──► core history + current index
/// client peer ──subscribe current table──► one winner payload, bounded reads
/// ```
fn current_state_history_depth_contract() {
    let mut baseline = None;
    for depth in [1, 10, 1_000, 1_025] {
        let mut bench = ColdSubscriptionBench::new();
        let winner = bench.seed_history(depth);
        // Drop all node/query/runtime state and reopen the same RocksDB data
        // before measuring. This makes the receipt a cold-state contract, not
        // an accidental hit in the seeding runtime's caches.
        bench.reopen_core();

        // Local reads must be served entirely by the compact current sources.
        reset_phase_counters(&mut [bench.core_mut()]);
        let local_rows = block_on(bench.core_mut().current_rows(TABLE, DurabilityTier::Local))
            .expect("local current read");
        let local_metrics = bench.core().storage_read_metrics();
        let local_query_metrics = bench.core().query_engine_read_metrics().clone();
        let local_tick_metrics = normalized_tick_metrics(bench.core().last_tick_metrics());
        assert_eq!(local_rows.len(), 1, "depth {depth} local visible rows");
        assert_eq!(
            local_metrics.history_rows.reads, 0,
            "depth {depth}: local current read must not decode history rows"
        );
        assert_eq!(
            local_metrics.history_rows.ranges, 0,
            "depth {depth}: local current read must not range-scan history rows"
        );
        assert_eq!(
            local_metrics.history_indexes.reads, 0,
            "depth {depth}: local current read must not probe history indexes"
        );
        assert_eq!(
            local_metrics.history_indexes.ranges, 0,
            "depth {depth}: local current read must not range-scan history indexes"
        );

        // A fresh peer needs the one immutable winner for wire fidelity, but
        // no superseded payload. The bounded history read is the exact winner
        // lookup behind the current-index witness, not a history scan.
        reset_phase_counters(&mut [bench.core_mut()]);
        let mut peer = PeerState::new();
        let schema = bench.schema.clone();
        let update =
            support::table_subscription_update(bench.core_mut(), &mut peer, &schema, TABLE);
        let metrics = bench.core().storage_read_metrics();
        let query_metrics = bench.core().query_engine_read_metrics().clone();
        let tick_metrics = normalized_tick_metrics(bench.core().last_tick_metrics());
        let SyncMessage::ViewUpdate(payload) = &update else {
            panic!("current-row subscription must produce a view update");
        };
        let bundles = expand_version_carriers(&payload.version_carriers)
            .expect("current-row version carriers must expand");
        assert_eq!(bundles.len(), 1, "depth {depth}: one current winner bundle");
        assert_eq!(
            bundles[0].tx.tx_id, winner,
            "depth {depth}: delivery must materialize the last current version"
        );
        // Stale-winner plant: selecting the initial/superseded version instead
        // of the argmax current witness changes this identity and fails here.
        assert_eq!(
            bundles[0].versions.len(),
            1,
            "depth {depth}: no superseded row versions may cross the wire"
        );
        // The ViewUpdate envelope carries the advancing accepted-global cursor,
        // so its total bytes legitimately differ after 1 versus 1k commits.
        // The transported immutable winner body is the history-independent
        // payload: all measured winners have an equally-sized value and one
        // parent, and this encoding must therefore be exactly identical.
        let winner_wire_bytes = postcard::to_allocvec(&bundles[0].versions[0])
            .expect("encode delivered winner body")
            .len();
        // Authorities send exact inputs, not a second terminal result. The
        // receiver's ordinary IVM query must materialize the one current row.
        assert!(payload.result_member_adds.is_empty());
        let (_receiver_dir, mut receiver) = open_node(node(3), schema.clone());
        support::register_table_receiver(&mut receiver, &schema, TABLE, peer.identity());
        support::apply_and_settle(&mut receiver, update.clone());
        let (shape, binding, _) = support::table_subscription(&schema, TABLE, peer.identity());
        let received = block_on(receiver.query_rows(&shape, &binding, DurabilityTier::Global))
            .expect("receiver materializes current row");
        assert_eq!(received.len(), 1, "depth {depth}: one receiver result");
        assert_eq!(
            received[0].row_uuid(),
            row(),
            "depth {depth}: exact logical row"
        );
        assert!(
            metrics.history_rows.reads <= 2
                && metrics.history_rows.ranges <= 2
                && metrics.history_indexes.reads <= 2
                && metrics.history_indexes.ranges <= 2,
            "depth {depth}: serving the winner may do bounded exact history lookup, not a scan: {metrics:?}"
        );

        if let Some((
            ref baseline_local_metrics,
            ref baseline_local_query_metrics,
            ref baseline_local_tick_metrics,
            ref baseline_metrics,
            ref baseline_query_metrics,
            ref baseline_tick_metrics,
            baseline_bytes,
        )) = baseline
        {
            assert_eq!(
                &local_metrics, baseline_local_metrics,
                "depth {depth}: local current read storage work regressed with history depth"
            );
            assert_eq!(
                &local_query_metrics, baseline_local_query_metrics,
                "depth {depth}: local current query lowering work regressed with history depth"
            );
            assert_eq!(
                &local_tick_metrics, baseline_local_tick_metrics,
                "depth {depth}: local current IVM tick work regressed with history depth"
            );
            assert_eq!(
                &metrics, baseline_metrics,
                "depth {depth}: peer current delivery storage work regressed with history depth"
            );
            assert_eq!(
                &query_metrics, baseline_query_metrics,
                "depth {depth}: peer current query lowering work regressed with history depth"
            );
            assert_eq!(
                &tick_metrics, baseline_tick_metrics,
                "depth {depth}: peer current IVM tick work regressed with history depth"
            );
            assert_eq!(
                winner_wire_bytes, baseline_bytes,
                "depth {depth}: identical-size current winner body grew with retained history"
            );
        } else {
            baseline = Some((
                local_metrics,
                local_query_metrics,
                local_tick_metrics,
                metrics,
                query_metrics,
                tick_metrics,
                winner_wire_bytes,
            ));
        }
    }
}

/// Tick sequence numbers and internal memo node ids are runtime-instance
/// identities, not work. Every remaining TickMetrics/RuntimeStats counter is
/// compared exactly across depths.
fn normalized_tick_metrics(metrics: Option<&TickMetrics>) -> Option<TickMetrics> {
    metrics.cloned().map(|mut metrics| {
        metrics.tick = 0;
        metrics.hydration_memo_computed_nodes.clear();
        metrics
    })
}

struct ColdSubscriptionBench {
    writer: NodeState<RocksDbStorage>,
    core: Option<NodeState<RocksDbStorage>>,
    schema: JazzSchema,
    _writer_dir: tempfile::TempDir,
    core_dir: tempfile::TempDir,
}

impl ColdSubscriptionBench {
    fn new() -> Self {
        let schema = schema();
        let (writer_dir, writer) = open_node(node(1), schema.clone());
        let (core_dir, core) = open_node(node(2), schema.clone());
        Self {
            writer,
            core: Some(core),
            schema,
            _writer_dir: writer_dir,
            core_dir,
        }
    }

    fn core(&self) -> &NodeState<RocksDbStorage> {
        self.core.as_ref().expect("core must be open")
    }

    fn core_mut(&mut self) -> &mut NodeState<RocksDbStorage> {
        self.core.as_mut().expect("core must be open")
    }

    fn reopen_core(&mut self) {
        drop(self.core.take());
        let cfs = self.schema.column_families();
        let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
        let storage = RocksDbStorage::open_with_durability(
            self.core_dir.path(),
            &refs,
            Durability::WalNoSync,
        )
        .expect("reopen core rocksdb");
        self.core = Some(
            block_on(NodeState::new(node(2), self.schema.clone(), storage))
                .expect("reopen core node"),
        );
    }

    fn seed_history(&mut self, depth: usize) -> jazz::tx::TxId {
        let row_uuid = row();
        let mut parent = None;
        // Every measured winner has one parent: `depth` counts ordinary
        // last-seen-style updates after the initial create, rather than making
        // depth one a structurally different parentless version.
        for idx in 0..=depth {
            let mut commit =
                MergeableCommit::new(TABLE, row_uuid, 1_000 + idx as u64).cells(cells(idx));
            if let Some(parent_tx_id) = parent {
                commit = commit.parents(vec![parent_tx_id]);
            }
            let (publication, unit) =
                block_on(self.writer.commit_mergeable_unit(commit)).expect("mergeable commit");
            let tx_id = publication.tx_id();
            block_on(self.writer.persist_and_settle_transaction(publication))
                .expect("persist mergeable commit");
            let fate = core_ingest(self.core_mut(), &unit, u64::MAX - SKEW_TOLERANCE_MS);
            assert!(matches!(
                fate,
                SyncMessage::FateUpdate {
                    fate: Fate::Accepted,
                    ..
                }
            ));
            parent = Some(tx_id);
        }

        let rows = block_on(self.core_mut().current_rows(TABLE, DurabilityTier::Global))
            .expect("current rows");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].row_uuid(), row_uuid);
        parent.expect("depth is non-zero")
    }

    fn seed_pending(&mut self, ahead: usize) {
        for idx in 0..ahead {
            let publication = block_on(
                self.core_mut().commit_mergeable(
                    MergeableCommit::new(TABLE, pending_row(idx), 10_000_000 + idx as u64)
                        .cells(cells(idx)),
                ),
            )
            .expect("pending commit");
            block_on(self.core_mut().persist_and_settle_transaction(publication))
                .expect("persist pending commit");
        }

        let rows = block_on(self.core_mut().current_rows(TABLE, DurabilityTier::Local))
            .expect("current rows");
        assert_eq!(rows.len(), ahead + 1);
    }

    fn current_rows_update_elapsed(&mut self, tier: DurabilityTier) -> std::time::Duration {
        reset_phase_counters(&mut [self.core_mut()]);
        let mut peer = PeerState::new();
        let start = Instant::now();
        match tier {
            DurabilityTier::Global => {
                let schema = self.schema.clone();
                let _ =
                    support::table_subscription_update(self.core_mut(), &mut peer, &schema, TABLE);
            }
            DurabilityTier::Local => {
                let _ = block_on(self.core_mut().current_rows(TABLE, DurabilityTier::Local))
                    .expect("cold local current rows");
            }
            DurabilityTier::None | DurabilityTier::Edge => {
                unreachable!("bench only uses local/global")
            }
        }
        start.elapsed()
    }

    fn emit_result(
        &self,
        depth: usize,
        ahead: usize,
        tier: DurabilityTier,
        elapsed: std::time::Duration,
    ) {
        let phase = match tier {
            DurabilityTier::Global => "global_current_rows_update",
            DurabilityTier::Local => "local_current_rows",
            DurabilityTier::None | DurabilityTier::Edge => {
                unreachable!("bench only uses local/global")
            }
        };
        let mut fields = phase_fields(phase, elapsed.as_micros());
        fields.insert("depth".to_owned(), serde_json::json!(depth));
        fields.insert("pending_ahead".to_owned(), serde_json::json!(ahead));
        insert_durability_tier(&mut fields, tier);
        insert_node_metrics(&mut fields, "core", self.core());
        emit_json_line("cold_subscription", fields);
    }
}

fn core_ingest(
    core: &mut NodeState<RocksDbStorage>,
    message: &SyncMessage,
    now_ms: u64,
) -> SyncMessage {
    let SyncMessage::CommitUnit { tx, versions } = message else {
        panic!("expected commit unit");
    };
    let outcome = block_on(core.ingest_commit_unit(tx.clone(), versions.clone(), now_ms))
        .expect("core ingest");
    let [fate] = block_on(core.persist_and_settle_outcome(outcome))
        .expect("persist core ingest")
        .try_into()
        .expect("one fate update");
    fate
}

fn depths() -> Vec<usize> {
    csv_usizes("JAZZ_DEPTHS", "1000,5000,10000")
}

fn pending_sizes() -> Vec<usize> {
    csv_usizes("JAZZ_PENDING_SIZES", "0,10,100")
}

fn schema() -> JazzSchema {
    schema_fixture::compile(
        SchemaBuilder::new()
            .table(TableSchemaBuilder::new(TABLE).column("title", ColumnType::Text)),
    )
}

fn open_node(
    node_uuid: NodeUuid,
    schema: JazzSchema,
) -> (tempfile::TempDir, NodeState<RocksDbStorage>) {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage =
        RocksDbStorage::open_with_durability(temp_dir.path(), &refs, Durability::WalNoSync)
            .expect("open rocksdb");
    let node = block_on(NodeState::new(node_uuid, schema, storage)).expect("single node");
    (temp_dir, node)
}

fn cells(idx: usize) -> BTreeMap<String, Value> {
    BTreeMap::from([("title".to_owned(), Value::String(format!("title-{idx:08}")))])
}

fn node(byte: u8) -> NodeUuid {
    NodeUuid::from_bytes([byte; 16])
}

fn row() -> RowUuid {
    RowUuid::from_bytes([7; 16])
}

fn pending_row(idx: usize) -> RowUuid {
    let mut bytes = [8; 16];
    bytes[0..8].copy_from_slice(&(idx as u64).to_le_bytes());
    RowUuid::from_bytes(bytes)
}
