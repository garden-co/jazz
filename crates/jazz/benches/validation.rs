use std::collections::{BTreeMap, BTreeSet};
use std::env;
use std::time::{Duration, Instant};

mod support;

use hdrhistogram::Histogram;
use jazz::groove::records::Value;
use jazz::groove::schema::{ColumnSchema, ColumnType};
use jazz::groove::storage::{Durability, RocksDbStorage};
use jazz::ids::{AuthorId, NodeUuid, RowUuid};
use jazz::node::{MergeableCommit, NodeState, SKEW_TOLERANCE_MS};
use jazz::peer::PeerState;
use jazz::protocol::{SyncMessage, VersionRecord};
use jazz::schema::{JazzSchema, Policy, TableSchema};
use jazz::time::GlobalSeq;
use jazz::tx::{DurabilityTier, Fate, RejectionReason, Transaction, TxId};
use support::{emit_json_line, insert_node_metrics, phase_fields, reset_phase_counters};

const TABLE: &str = "items";

fn main() {
    let config = Config::from_env();
    let mut bench = ValidationBench::new(config);
    bench.seed();
    let elapsed = bench.run();
    bench.print_json(elapsed);
}

#[derive(Clone, Copy)]
struct Config {
    clients: usize,
    rows: usize,
    commits: usize,
    hot_row_pct: u32,
    seed: u64,
}

impl Config {
    fn from_env() -> Self {
        Self {
            clients: env_usize("GROOVE_CLIENTS", 50).max(1),
            rows: env_usize("GROOVE_ROWS", 200).max(1),
            commits: env_usize("GROOVE_COMMITS", 500),
            hot_row_pct: env_u32("GROOVE_HOT_ROW_PCT", 20).min(100),
            seed: env_u64("GROOVE_SEED", 0x5eed_1234),
        }
    }
}

struct ValidationBench {
    config: Config,
    core: NodeState<RocksDbStorage>,
    clients: Vec<NodeState<RocksDbStorage>>,
    _core_dir: tempfile::TempDir,
    _client_dirs: Vec<tempfile::TempDir>,
    rng: Rng,
    rows: Vec<RowState>,
    hot_rows: Vec<usize>,
    model: BaselineModel,
    metrics: Metrics,
}

impl ValidationBench {
    fn new(config: Config) -> Self {
        let schema = schema();
        let (core_dir, core) = open_node(node(250), schema.clone());
        let mut client_dirs = Vec::with_capacity(config.clients);
        let mut clients = Vec::with_capacity(config.clients);
        for idx in 0..config.clients {
            let (dir, client) = open_node(node(idx as u8 + 1), schema.clone());
            client_dirs.push(dir);
            clients.push(client);
        }
        let rows = (0..config.rows)
            .map(|idx| RowState {
                row_uuid: row(idx),
                owner_idx: idx % config.clients,
            })
            .collect::<Vec<_>>();
        let hot_len = (config.rows / 20).clamp(1, config.rows);
        let hot_rows = (0..hot_len).collect();
        Self {
            config,
            core,
            clients,
            _core_dir: core_dir,
            _client_dirs: client_dirs,
            rng: Rng::new(config.seed),
            rows,
            hot_rows,
            model: BaselineModel::default(),
            metrics: Metrics::default(),
        }
    }

    fn seed(&mut self) {
        for row_idx in 0..self.rows.len() {
            let owner_idx = self.rows[row_idx].owner_idx;
            let made_by = author(owner_idx);
            let title = format!("seed-{row_idx}");
            let (tx_id, unit) = self.clients[owner_idx]
                .commit_mergeable_unit(
                    MergeableCommit::new(TABLE, self.rows[row_idx].row_uuid, 10)
                        .made_by(made_by)
                        .cells(cells(&title, made_by)),
                )
                .expect("seed commit");
            let fate = core_ingest(&mut self.core, &unit);
            apply_fate(&mut self.clients[owner_idx], &fate);
            let global_seq = accepted_global_seq(&fate);
            self.model.apply(
                tx_id,
                global_seq,
                unit_versions(&unit),
                DurabilityTier::Global,
            );
        }

        for client in &mut self.clients {
            let mut peer = PeerState::new();
            let update = peer
                .current_rows_update(&mut self.core, TABLE)
                .expect("seed view update");
            client.apply_sync_message(update).expect("apply seed view");
        }
    }

    fn run(&mut self) -> Duration {
        reset_phase_counters(&mut [&mut self.core]);
        let run_start = Instant::now();
        for step in 0..self.config.commits {
            let client_idx = self.rng.usize(self.config.clients);
            let tx_id = self.clients[client_idx]
                .open_exclusive()
                .expect("open exclusive");

            let read_count = 1 + self.rng.usize(3);
            for _ in 0..read_count {
                let row_idx = self.pick_row_for_operation(client_idx);
                let _ = self.clients[client_idx]
                    .tx_read(tx_id, TABLE, self.rows[row_idx].row_uuid)
                    .expect("tx read");
            }
            if self.rng.chance(1, 5) {
                self.metrics.predicate_reads += 1;
                let _ = self.clients[client_idx]
                    .tx_current_rows(tx_id, TABLE)
                    .expect("tx current rows");
            }

            let write_count = 1 + self.rng.usize(2);
            let mut write_rows = BTreeSet::new();
            for write_idx in 0..write_count {
                let mut row_idx = None;
                let mut candidate = self.pick_owned_row(client_idx);
                for _ in 0..self.rows.len() {
                    if write_rows.insert(candidate) {
                        row_idx = Some(candidate);
                        break;
                    }
                    candidate = self.pick_owned_row(client_idx);
                }
                let Some(row_idx) = row_idx else {
                    continue;
                };
                let title = format!("c{client_idx}-s{step}-w{write_idx}");
                self.clients[client_idx]
                    .tx_write(
                        tx_id,
                        TABLE,
                        self.rows[row_idx].row_uuid,
                        cells(&title, author(client_idx)),
                        None,
                    )
                    .expect("tx write");
            }

            let (_tx_id, unit) = self.clients[client_idx]
                .commit_exclusive(tx_id, author(client_idx), 1_000 + step as u64)
                .expect("commit exclusive");
            let (tx, versions) = commit_parts(&unit);

            let baseline_start = Instant::now();
            let baseline_accepts = self.model.validate(&tx, &versions);
            self.metrics.record_baseline(baseline_start.elapsed());

            let validation_start = Instant::now();
            let fate = core_ingest(&mut self.core, &unit);
            let validation_elapsed = validation_start.elapsed();
            self.metrics.record_validation(validation_elapsed);
            apply_fate(&mut self.clients[client_idx], &fate);

            match fate {
                SyncMessage::FateUpdate {
                    tx_id,
                    fate: Fate::Accepted,
                    global_seq: Some(global_seq),
                    ..
                } => {
                    assert!(baseline_accepts, "baseline/core decision mismatch");
                    self.metrics.accepted += 1;
                    self.model
                        .apply(tx_id, global_seq, versions, DurabilityTier::Global);
                }
                SyncMessage::FateUpdate {
                    fate: Fate::Rejected(reason),
                    ..
                } => {
                    assert!(!baseline_accepts, "baseline/core decision mismatch");
                    self.metrics.rejected.record(reason);
                }
                other => panic!("unexpected fate update: {other:?}"),
            }
        }
        run_start.elapsed()
    }

    fn pick_row_for_operation(&mut self, client_idx: usize) -> usize {
        if self.rng.percent(self.config.hot_row_pct) {
            let hot_idx = self.rng.usize(self.hot_rows.len());
            self.hot_rows[hot_idx]
        } else if self.rng.chance(3, 4) {
            self.pick_owned_row(client_idx)
        } else {
            self.rng.usize(self.rows.len())
        }
    }

    fn pick_owned_row(&mut self, client_idx: usize) -> usize {
        let owned_count = self.rows.len().div_ceil(self.config.clients);
        for _ in 0..owned_count.max(1) * 2 {
            let candidate = self.rng.usize(self.rows.len());
            if self.rows[candidate].owner_idx == client_idx {
                return candidate;
            }
        }
        self.rows
            .iter()
            .position(|row| row.owner_idx == client_idx)
            .unwrap_or_else(|| self.rng.usize(self.rows.len()))
    }

    fn print_json(&self, elapsed: Duration) {
        let mut fields = phase_fields("exclusive_validation_throughput", elapsed.as_micros());
        fields.insert("seed".to_owned(), serde_json::json!(self.config.seed));
        fields.insert("clients".to_owned(), serde_json::json!(self.config.clients));
        fields.insert("rows".to_owned(), serde_json::json!(self.config.rows));
        fields.insert("commits".to_owned(), serde_json::json!(self.config.commits));
        fields.insert(
            "hot_row_pct".to_owned(),
            serde_json::json!(self.config.hot_row_pct),
        );
        fields.insert(
            "core_ingest_p50_us".to_owned(),
            serde_json::json!(self.metrics.validation.value_at_quantile(0.50)),
        );
        fields.insert(
            "core_ingest_p95_us".to_owned(),
            serde_json::json!(self.metrics.validation.value_at_quantile(0.95)),
        );
        fields.insert(
            "core_ingest_p99_us".to_owned(),
            serde_json::json!(self.metrics.validation.value_at_quantile(0.99)),
        );
        fields.insert(
            "core_ingest_max_us".to_owned(),
            serde_json::json!(self.metrics.validation.max()),
        );
        fields.insert(
            "model_decision_only_p50_us".to_owned(),
            serde_json::json!(self.metrics.baseline.value_at_quantile(0.50)),
        );
        fields.insert(
            "accept_count".to_owned(),
            serde_json::json!(self.metrics.accepted),
        );
        fields.insert(
            "reject_count".to_owned(),
            serde_json::json!(self.metrics.rejected.total()),
        );
        fields.insert(
            "reject_client_clock_too_far_ahead".to_owned(),
            serde_json::json!(self.metrics.rejected.client_clock_too_far_ahead),
        );
        fields.insert(
            "reject_authorization_denied".to_owned(),
            serde_json::json!(self.metrics.rejected.authorization_denied),
        );
        fields.insert(
            "reject_exclusive_conflict".to_owned(),
            serde_json::json!(self.metrics.rejected.exclusive_conflict),
        );
        fields.insert(
            "reject_cascade".to_owned(),
            serde_json::json!(self.metrics.rejected.cascade),
        );
        fields.insert(
            "reject_malformed".to_owned(),
            serde_json::json!(self.metrics.rejected.malformed),
        );
        fields.insert(
            "reject_causality_violation".to_owned(),
            serde_json::json!(self.metrics.rejected.causality_violation),
        );
        fields.insert(
            "predicate_read_count".to_owned(),
            serde_json::json!(self.metrics.predicate_reads),
        );
        insert_node_metrics(&mut fields, "core", &self.core);
        emit_json_line("validation", fields);
    }
}

#[derive(Clone)]
struct RowState {
    row_uuid: RowUuid,
    owner_idx: usize,
}

#[derive(Default)]
struct BaselineModel {
    history: BTreeMap<RowUuid, Vec<ModelVersion>>,
}

impl BaselineModel {
    fn validate(&self, tx: &Transaction, versions: &[VersionRecord]) -> bool {
        let Some(snapshot) = tx.base_snapshot.as_ref() else {
            return false;
        };
        for read in tx.row_read_set.as_deref().unwrap_or(&[]) {
            if self.visible_now(read.row_uuid) != Some(read.version) {
                return false;
            }
        }
        for absent in tx.absent_read_set.as_deref().unwrap_or(&[]) {
            if self.visible_now(absent.row_uuid).is_some() {
                return false;
            }
        }
        if tx
            .predicate_read_set
            .as_deref()
            .unwrap_or(&[])
            .iter()
            .any(|predicate| predicate.table == TABLE)
        {
            let at_snapshot = self.visible_content_set_at(snapshot.global_base);
            let now = self.visible_content_set_now();
            if at_snapshot != now {
                return false;
            }
        }
        for version in versions {
            let parents = version.parents();
            let parent = match parents.as_slice() {
                [] => None,
                [parent] => Some(*parent),
                _ => return false,
            };
            if self.visible_now(version.row_uuid()) != parent {
                return false;
            }
        }
        true
    }

    fn apply(
        &mut self,
        tx_id: TxId,
        global_seq: GlobalSeq,
        versions: Vec<VersionRecord>,
        _durability: DurabilityTier,
    ) {
        for version in versions {
            self.history
                .entry(version.row_uuid())
                .or_default()
                .push(ModelVersion { tx_id, global_seq });
        }
    }

    fn visible_at(&self, row_uuid: RowUuid, global_base: GlobalSeq) -> Option<TxId> {
        self.history.get(&row_uuid).and_then(|versions| {
            versions
                .iter()
                .filter(|version| version.global_seq <= global_base)
                .max_by_key(|version| version.global_seq)
                .map(|version| version.tx_id)
        })
    }

    fn visible_now(&self, row_uuid: RowUuid) -> Option<TxId> {
        self.history.get(&row_uuid).and_then(|versions| {
            versions
                .iter()
                .max_by_key(|version| version.global_seq)
                .map(|version| version.tx_id)
        })
    }

    fn visible_content_set_at(&self, global_base: GlobalSeq) -> BTreeSet<TxId> {
        self.history
            .keys()
            .filter_map(|row_uuid| self.visible_at(*row_uuid, global_base))
            .collect()
    }

    fn visible_content_set_now(&self) -> BTreeSet<TxId> {
        self.history
            .keys()
            .filter_map(|row_uuid| self.visible_now(*row_uuid))
            .collect()
    }
}

struct ModelVersion {
    tx_id: TxId,
    global_seq: GlobalSeq,
}

struct Metrics {
    validation: Histogram<u64>,
    baseline: Histogram<u64>,
    accepted: u64,
    rejected: RejectCounts,
    predicate_reads: u64,
}

impl Default for Metrics {
    fn default() -> Self {
        Self {
            validation: Histogram::new(3).expect("validation histogram"),
            baseline: Histogram::new(3).expect("baseline histogram"),
            accepted: 0,
            rejected: RejectCounts::default(),
            predicate_reads: 0,
        }
    }
}

impl Metrics {
    fn record_validation(&mut self, elapsed: Duration) {
        self.validation
            .record(elapsed.as_micros().min(u64::MAX as u128) as u64)
            .expect("record validation latency");
    }

    fn record_baseline(&mut self, elapsed: Duration) {
        self.baseline
            .record(elapsed.as_micros().min(u64::MAX as u128) as u64)
            .expect("record baseline latency");
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
    fn record(&mut self, reason: RejectionReason) {
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

    fn chance(&mut self, numerator: u32, denominator: u32) -> bool {
        (self.next() % denominator as u64) < numerator as u64
    }

    fn percent(&mut self, pct: u32) -> bool {
        (self.next() % 100) < pct as u64
    }
}

fn schema() -> JazzSchema {
    JazzSchema::new([TableSchema::new(
        TABLE,
        [
            ColumnSchema::new("title", ColumnType::String),
            ColumnSchema::new("owner", ColumnType::Uuid),
        ],
    )
    .with_write_policy(Policy::owner_only(TABLE, "owner"))])
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

fn core_ingest(core: &mut NodeState<RocksDbStorage>, unit: &SyncMessage) -> SyncMessage {
    let SyncMessage::CommitUnit { tx, versions } = unit else {
        panic!("expected commit unit");
    };
    let [fate] = core
        .ingest_commit_unit(tx.clone(), versions.clone(), u64::MAX - SKEW_TOLERANCE_MS)
        .expect("core ingest")
        .try_into()
        .expect("one fate update");
    fate
}

fn apply_fate(node: &mut NodeState<RocksDbStorage>, fate: &SyncMessage) {
    node.apply_sync_message(fate.clone()).expect("apply fate");
}

fn accepted_global_seq(fate: &SyncMessage) -> GlobalSeq {
    let SyncMessage::FateUpdate {
        fate: Fate::Accepted,
        global_seq: Some(global_seq),
        ..
    } = fate
    else {
        panic!("expected accepted fate");
    };
    *global_seq
}

fn unit_versions(unit: &SyncMessage) -> Vec<VersionRecord> {
    let SyncMessage::CommitUnit { versions, .. } = unit else {
        panic!("expected commit unit");
    };
    versions.clone()
}

fn commit_parts(unit: &SyncMessage) -> (Transaction, Vec<VersionRecord>) {
    let SyncMessage::CommitUnit { tx, versions } = unit else {
        panic!("expected commit unit");
    };
    (tx.clone(), versions.clone())
}

fn cells(title: &str, owner: AuthorId) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("title".to_owned(), Value::String(title.to_owned())),
        ("owner".to_owned(), Value::Uuid(owner.0)),
    ])
}

fn author(idx: usize) -> AuthorId {
    let mut bytes = [0_u8; 16];
    bytes[0..8].copy_from_slice(&(idx as u64 + 1).to_be_bytes());
    AuthorId::from_bytes(bytes)
}

fn node(byte: u8) -> NodeUuid {
    NodeUuid::from_bytes([byte; 16])
}

fn row(idx: usize) -> RowUuid {
    let mut bytes = [0_u8; 16];
    bytes[0..8].copy_from_slice(&(idx as u64 + 1).to_be_bytes());
    RowUuid::from_bytes(bytes)
}

fn env_usize(name: &str, default: usize) -> usize {
    env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn env_u32(name: &str, default: u32) -> u32 {
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
