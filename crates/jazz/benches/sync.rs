use std::collections::BTreeMap;
use std::env;
use std::time::{Duration, Instant};

use hdrhistogram::Histogram;
use jazz::groove::records::Value;
use jazz::groove::schema::{ColumnSchema, ColumnType};
use jazz::groove::storage::{Durability, RocksDbStorage};
use jazz::ids::{AuthorId, NodeUuid, RowUuid};
use jazz::node::{MergeableCommit, NodeState, SKEW_TOLERANCE_MS};
use jazz::peer::PeerState;
use jazz::protocol::SyncMessage;
use jazz::schema::{JazzSchema, Policy, TableSchema};
use jazz::tx::{DeletionEvent, DurabilityTier, Fate, RejectionReason, TxId};

const TABLE: &str = "todos";

fn main() {
    let config = Config::from_env();
    let mut bench = SyncBench::new(config);
    bench.run();
    bench.print_json();
}

#[derive(Clone, Copy)]
struct Config {
    commits: usize,
    view_every: usize,
    seed: u64,
}

impl Config {
    fn from_env() -> Self {
        Self {
            commits: env_usize("GROOVE_COMMITS", 400),
            view_every: env_usize("GROOVE_VIEW_EVERY", 10).max(1),
            seed: env_u64("GROOVE_SEED", 0x510c_4eed),
        }
    }
}

struct SyncBench {
    config: Config,
    ui: NodeState<RocksDbStorage>,
    worker: NodeState<RocksDbStorage>,
    edge: NodeState<RocksDbStorage>,
    core: NodeState<RocksDbStorage>,
    _dirs: Vec<tempfile::TempDir>,
    core_to_edge: PeerState,
    edge_to_worker: PeerState,
    worker_to_ui: PeerState,
    ui_author: AuthorId,
    ui_owner: AuthorId,
    other_owner: AuthorId,
    rng: Rng,
    parents: BTreeMap<RowUuid, TxId>,
    metrics: Metrics,
}

impl SyncBench {
    fn new(config: Config) -> Self {
        let schema = schema();
        let mut dirs = Vec::new();
        let (dir, ui) = open_node(node(1), schema.clone());
        dirs.push(dir);
        let (dir, worker) = open_node(node(2), schema.clone());
        dirs.push(dir);
        let (dir, edge) = open_node(node(3), schema.clone());
        dirs.push(dir);
        let (dir, core) = open_node(node(4), schema);
        dirs.push(dir);
        let ui_author = AuthorId::from_bytes([7; 16]);
        Self {
            config,
            ui,
            worker,
            edge,
            core,
            _dirs: dirs,
            core_to_edge: PeerState::new(),
            edge_to_worker: PeerState::new(),
            worker_to_ui: PeerState::for_author(ui_author),
            ui_author,
            ui_owner: ui_author,
            other_owner: AuthorId::from_bytes([8; 16]),
            rng: Rng::new(config.seed),
            parents: BTreeMap::new(),
            metrics: Metrics::default(),
        }
    }

    fn run(&mut self) {
        self.seed_policy_hidden_row();
        for step in 0..self.config.commits {
            let (tx_id, unit, now_ms) = self.next_unit(step);

            let start = Instant::now();
            relay_ingest(&mut self.worker, &unit);
            relay_ingest(&mut self.edge, &unit);
            let fate = core_ingest(&mut self.core, &unit, now_ms);
            self.edge
                .apply_sync_message(fate.clone())
                .expect("edge fate");
            self.worker
                .apply_sync_message(fate.clone())
                .expect("worker fate");
            self.ui.apply_sync_message(fate.clone()).expect("ui fate");
            self.metrics.record_fate_rtt(start.elapsed());

            match &fate {
                SyncMessage::FateUpdate {
                    fate: Fate::Accepted,
                    ..
                } => {
                    self.metrics.accepted += 1;
                    if let Some(row_uuid) = content_unit_row(&unit) {
                        self.parents.insert(row_uuid, tx_id);
                    }
                }
                SyncMessage::FateUpdate {
                    fate: Fate::Rejected(reason),
                    ..
                } => self.metrics.rejected.record(reason),
                _ => panic!("expected fate update"),
            }

            if (step + 1).is_multiple_of(self.config.view_every) {
                self.refresh_views(true);
            }
        }
        self.refresh_views(true);
        self.assert_converged();
    }

    fn seed_policy_hidden_row(&mut self) {
        let (tx_id, unit) = self
            .ui
            .commit_mergeable_unit(
                MergeableCommit::new(TABLE, row(250), 1)
                    .made_by(self.ui_author)
                    .cells(cells("other-owner", self.other_owner)),
            )
            .expect("hidden row");
        let fate = self.pipeline_without_timing(&unit, u64::MAX - SKEW_TOLERANCE_MS);
        assert!(matches!(
            fate,
            SyncMessage::FateUpdate {
                fate: Fate::Accepted,
                ..
            }
        ));
        self.parents.insert(row(250), tx_id);
    }

    fn pipeline_without_timing(&mut self, unit: &SyncMessage, now_ms: u64) -> SyncMessage {
        relay_ingest(&mut self.worker, unit);
        relay_ingest(&mut self.edge, unit);
        let fate = core_ingest(&mut self.core, unit, now_ms);
        self.edge
            .apply_sync_message(fate.clone())
            .expect("edge fate");
        self.worker
            .apply_sync_message(fate.clone())
            .expect("worker fate");
        self.ui.apply_sync_message(fate.clone()).expect("ui fate");
        fate
    }

    fn next_unit(&mut self, step: usize) -> (TxId, SyncMessage, u64) {
        if step % 17 == 9 {
            return self.next_exclusive(step);
        }
        if step % 101 == 77 {
            return self.next_skewed_mergeable(step);
        }
        if matches!(step, 40 | 120) {
            return self.next_deletion_content(step);
        }
        if matches!(step, 41 | 121) {
            return self.next_deletion_event(step, DeletionEvent::Deleted);
        }
        if matches!(step, 42 | 122) {
            return self.next_deletion_event(step, DeletionEvent::Restored);
        }
        self.next_mergeable(step)
    }

    fn next_mergeable(&mut self, step: usize) -> (TxId, SyncMessage, u64) {
        let row_uuid = row((self.rng.usize(48) + 1) as u8);
        let mut commit =
            MergeableCommit::new(TABLE, row_uuid, 10 + step as u64).made_by(self.ui_author);
        if let Some(parent) = self.parents.get(&row_uuid).copied() {
            commit = commit.parents(vec![parent]);
        }
        let (tx_id, unit) = self
            .ui
            .commit_mergeable_unit(commit.cells(cells(format!("merge-{step}"), self.ui_owner)))
            .expect("mergeable commit");
        (tx_id, unit, u64::MAX - SKEW_TOLERANCE_MS)
    }

    fn next_skewed_mergeable(&mut self, step: usize) -> (TxId, SyncMessage, u64) {
        let row_uuid = row(90 + (step % 8) as u8);
        let (tx_id, unit) = self
            .ui
            .commit_mergeable_unit(
                MergeableCommit::new(TABLE, row_uuid, 1_000_000 + step as u64)
                    .made_by(self.ui_author)
                    .cells(cells(format!("skew-{step}"), self.ui_owner)),
            )
            .expect("skewed commit");
        (tx_id, unit, 0)
    }

    fn next_deletion_content(&mut self, step: usize) -> (TxId, SyncMessage, u64) {
        let row_uuid = row(70 + (step / 80) as u8);
        let (tx_id, unit) = self
            .ui
            .commit_mergeable_unit(
                MergeableCommit::new(TABLE, row_uuid, 2_000 + step as u64)
                    .made_by(self.ui_author)
                    .cells(cells(format!("delete-base-{step}"), self.ui_owner)),
            )
            .expect("delete base");
        (tx_id, unit, u64::MAX - SKEW_TOLERANCE_MS)
    }

    fn next_deletion_event(
        &mut self,
        step: usize,
        event: DeletionEvent,
    ) -> (TxId, SyncMessage, u64) {
        let row_uuid = row(70 + ((step - 1) / 80) as u8);
        let (tx_id, unit) = self
            .ui
            .commit_mergeable_unit(
                MergeableCommit::new(TABLE, row_uuid, 2_000 + step as u64)
                    .made_by(self.ui_author)
                    .deletion(event),
            )
            .expect("deletion event");
        (tx_id, unit, u64::MAX - SKEW_TOLERANCE_MS)
    }

    fn next_exclusive(&mut self, step: usize) -> (TxId, SyncMessage, u64) {
        let row_uuid = row(120 + (step % 12) as u8);
        let tx_id = self.ui.open_exclusive().expect("open exclusive");
        let _ = self.ui.tx_read(tx_id, TABLE, row_uuid).expect("read");
        self.ui
            .tx_write(
                tx_id,
                TABLE,
                row_uuid,
                cells(format!("exclusive-{step}"), self.ui_owner),
                None,
            )
            .expect("write");
        let (tx_id, unit) = self
            .ui
            .commit_exclusive(tx_id, self.ui_author, 3_000 + step as u64)
            .expect("exclusive");
        (tx_id, unit, u64::MAX - SKEW_TOLERANCE_MS)
    }

    fn refresh_views(&mut self, record: bool) {
        let start = Instant::now();
        refresh(&mut self.core, &mut self.edge, &mut self.core_to_edge);
        refresh(&mut self.edge, &mut self.worker, &mut self.edge_to_worker);
        refresh(&mut self.worker, &mut self.ui, &mut self.worker_to_ui);
        if record {
            self.metrics.record_view_refresh(start.elapsed());
        }
    }

    fn assert_converged(&mut self) {
        let core_rows = current_rows(&mut self.core);
        assert_eq!(current_rows(&mut self.edge), core_rows);
        assert_eq!(current_rows(&mut self.worker), core_rows);
        assert_eq!(current_rows(&mut self.ui), core_rows);
        let ui_rows = self
            .ui
            .subscription_current_rows(TABLE, DurabilityTier::Global)
            .expect("ui subscription");
        let schema = schema();
        let table = &schema.tables[0];
        assert!(
            ui_rows
                .iter()
                .all(|row| row.cell(table, "owner") == Some(Value::Uuid(self.ui_owner.0)))
        );
        assert_eq!(
            self.worker.sync_metrics().parked_orphans,
            self.worker.sync_metrics().parked_orphans_resolved
        );
        assert_eq!(
            self.edge.sync_metrics().parked_orphans,
            self.edge.sync_metrics().parked_orphans_resolved
        );
    }

    fn print_json(&self) {
        println!(
            "{{\"scenario\":\"four_tier_sync\",\
             \"seed\":{},\"commits\":{},\"view_every\":{},\
             \"fate_rtt_p50_us\":{},\"fate_rtt_p95_us\":{},\"fate_rtt_p99_us\":{},\
             \"view_refresh_p50_us\":{},\"view_refresh_p95_us\":{},\
             \"core_edge_version_bundles_out\":{},\"core_edge_complete_tx_payload_refs_out\":{},\
             \"edge_worker_version_bundles_out\":{},\"edge_worker_complete_tx_payload_refs_out\":{},\
             \"worker_ui_version_bundles_out\":{},\"worker_ui_complete_tx_payload_refs_out\":{},\
             \"accept_count\":{},\"reject_count\":{},\
             \"reject_client_clock_too_far_ahead\":{},\"reject_authorization_denied\":{},\
             \"reject_exclusive_conflict\":{},\"reject_cascade\":{},\"reject_malformed\":{},\"reject_causality_violation\":{},\
             \"worker_parked_orphans\":{},\"worker_parked_orphans_resolved\":{},\
             \"edge_parked_orphans\":{},\"edge_parked_orphans_resolved\":{}}}",
            self.config.seed,
            self.config.commits,
            self.config.view_every,
            self.metrics.fate_rtt.value_at_quantile(0.50),
            self.metrics.fate_rtt.value_at_quantile(0.95),
            self.metrics.fate_rtt.value_at_quantile(0.99),
            self.metrics.view_refresh.value_at_quantile(0.50),
            self.metrics.view_refresh.value_at_quantile(0.95),
            self.core_to_edge.metrics.version_bundles_out,
            self.core_to_edge.metrics.complete_tx_payload_refs_out,
            self.edge_to_worker.metrics.version_bundles_out,
            self.edge_to_worker.metrics.complete_tx_payload_refs_out,
            self.worker_to_ui.metrics.version_bundles_out,
            self.worker_to_ui.metrics.complete_tx_payload_refs_out,
            self.metrics.accepted,
            self.metrics.rejected.total(),
            self.metrics.rejected.client_clock_too_far_ahead,
            self.metrics.rejected.authorization_denied,
            self.metrics.rejected.exclusive_conflict,
            self.metrics.rejected.cascade,
            self.metrics.rejected.malformed,
            self.metrics.rejected.causality_violation,
            self.worker.sync_metrics().parked_orphans,
            self.worker.sync_metrics().parked_orphans_resolved,
            self.edge.sync_metrics().parked_orphans,
            self.edge.sync_metrics().parked_orphans_resolved,
        );
    }
}

fn relay_ingest(node: &mut NodeState<RocksDbStorage>, message: &SyncMessage) {
    let SyncMessage::CommitUnit { tx, versions } = message else {
        panic!("expected commit unit");
    };
    node.ingest_relay_commit_unit(tx.clone(), versions.clone())
        .expect("relay ingest");
}

fn core_ingest(
    core: &mut NodeState<RocksDbStorage>,
    message: &SyncMessage,
    now_ms: u64,
) -> SyncMessage {
    let SyncMessage::CommitUnit { tx, versions } = message else {
        panic!("expected commit unit");
    };
    let [fate] = core
        .ingest_commit_unit(tx.clone(), versions.clone(), now_ms)
        .expect("core ingest")
        .try_into()
        .expect("one fate update");
    fate
}

fn refresh(
    upstream: &mut NodeState<RocksDbStorage>,
    downstream: &mut NodeState<RocksDbStorage>,
    peer: &mut PeerState,
) {
    let update = peer
        .current_rows_update(upstream, TABLE)
        .expect("view update");
    downstream.apply_sync_message(update).expect("apply view");
}

fn content_unit_row(unit: &SyncMessage) -> Option<RowUuid> {
    let SyncMessage::CommitUnit { versions, .. } = unit else {
        panic!("expected commit unit");
    };
    versions
        .first()
        .filter(|version| version.deletion().is_none() && version.cell_at(0).is_some())
        .map(|version| version.row_uuid())
}

fn current_rows(
    node: &mut NodeState<RocksDbStorage>,
) -> BTreeMap<RowUuid, BTreeMap<String, Value>> {
    let schema = schema();
    let table = &schema.tables[0];
    node.current_rows(TABLE, DurabilityTier::Global)
        .expect("current rows")
        .into_iter()
        .map(|row| {
            let cells = table
                .columns
                .iter()
                .filter_map(|column| {
                    row.cell(table, &column.name)
                        .map(|value| (column.name.clone(), value))
                })
                .collect();
            (row.row_uuid(), cells)
        })
        .collect()
}

fn schema() -> JazzSchema {
    JazzSchema::new([TableSchema::new(
        TABLE,
        [
            ColumnSchema::new("title", ColumnType::String),
            ColumnSchema::new("owner", ColumnType::Uuid),
        ],
    )
    .with_read_policy(Policy::owner_only(TABLE, "owner"))])
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
    let node = NodeState::new(node_uuid, schema, storage).expect("single node");
    (temp_dir, node)
}

fn cells(title: impl Into<String>, owner: AuthorId) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("title".to_owned(), Value::String(title.into())),
        ("owner".to_owned(), Value::Uuid(owner.0)),
    ])
}

fn node(byte: u8) -> NodeUuid {
    NodeUuid::from_bytes([byte; 16])
}

fn row(idx: u8) -> RowUuid {
    RowUuid::from_bytes([idx; 16])
}

struct Metrics {
    fate_rtt: Histogram<u64>,
    view_refresh: Histogram<u64>,
    accepted: u64,
    rejected: RejectCounts,
}

impl Metrics {
    fn record_fate_rtt(&mut self, elapsed: Duration) {
        self.fate_rtt
            .record(elapsed.as_micros().min(u64::MAX as u128) as u64)
            .expect("record fate rtt");
    }

    fn record_view_refresh(&mut self, elapsed: Duration) {
        self.view_refresh
            .record(elapsed.as_micros().min(u64::MAX as u128) as u64)
            .expect("record view refresh");
    }
}

impl Default for Metrics {
    fn default() -> Self {
        Self {
            fate_rtt: Histogram::new(3).expect("fate histogram"),
            view_refresh: Histogram::new(3).expect("view histogram"),
            accepted: 0,
            rejected: RejectCounts::default(),
        }
    }
}

#[derive(Default)]
struct RejectCounts {
    client_clock_too_far_ahead: u64,
    authorization_denied: u64,
    exclusive_conflict: u64,
    cascade: u64,
    malformed: u64,
    causality_violation: u64,
}

impl RejectCounts {
    fn record(&mut self, reason: &RejectionReason) {
        match reason {
            RejectionReason::ClientClockTooFarAhead => self.client_clock_too_far_ahead += 1,
            RejectionReason::AuthorizationDenied => self.authorization_denied += 1,
            RejectionReason::ExclusiveConflict => self.exclusive_conflict += 1,
            RejectionReason::Cascade { .. } => self.cascade += 1,
            RejectionReason::MalformedCommit(_) => self.malformed += 1,
            RejectionReason::CausalityViolation => self.causality_violation += 1,
        }
    }

    fn total(&self) -> u64 {
        self.client_clock_too_far_ahead
            + self.authorization_denied
            + self.exclusive_conflict
            + self.cascade
            + self.malformed
            + self.causality_violation
    }
}

struct Rng {
    state: u64,
}

impl Rng {
    fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next(&mut self) -> u64 {
        self.state = self
            .state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        self.state
    }

    fn usize(&mut self, upper: usize) -> usize {
        (self.next() % upper as u64) as usize
    }
}

fn env_usize(name: &str, default: usize) -> usize {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_u64(name: &str, default: u64) -> u64 {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}
