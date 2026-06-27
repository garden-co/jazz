use std::collections::BTreeMap;
use std::env;
use std::time::{Duration, Instant};

use hdrhistogram::Histogram;
use jazz::groove::records::Value;
use jazz::groove::schema::{ColumnSchema, ColumnType};
use jazz::groove::storage::{Durability, RocksDbStorage};
use jazz::ids::{AuthorId, NodeUuid, RowUuid};
use jazz::node::{MergeableCommit, NodeState, SKEW_TOLERANCE_MS};
use jazz::protocol::{SyncMessage, VersionRecord};
use jazz::schema::{JazzSchema, TableSchema};
use jazz::time::TxTime;
use jazz::tx::{DeletionEvent, DurabilityTier, Fate, Transaction};
use jazz_sim::{emit_json_line, metadata_fields};
use serde_json::{Map, Value as JsonValue, json};

const TABLE: &str = "items";

fn main() {
    let config = Config::from_env();
    run_node_open(&config);
    run_hlc(&config);
    run_domination(&config);
    run_deletion_register(&config);
    run_ingest_rate(&config);
    run_commit_unit(&config);
    run_read_set_capture(&config);
    run_validation_entries(&config);
}

#[derive(Clone, Copy)]
struct Config {
    seed: u64,
    iterations: usize,
}

impl Config {
    fn from_env() -> Self {
        Self {
            seed: env_u64("JAZZ_SEED", 0x006d_6963_726f),
            iterations: env_usize("JAZZ_MICRO_ITERS", 100).max(1),
        }
    }
}

fn run_node_open(config: &Config) {
    for versions in [1_000_usize, 10_000, 100_000] {
        let schema = schema();
        let temp_dir = tempfile::tempdir().expect("tempdir");
        {
            let mut node_ = open_node_at(temp_dir.path(), node(220), schema.clone());
            seed_local_rows(&mut node_, versions);
        }

        let mut hist = NsHist::new();
        for _ in 0..config.iterations {
            let start = Instant::now();
            let node_ = open_node_at(temp_dir.path(), node(220), schema.clone());
            hist.record(start.elapsed());
            black_box(node_);
        }
        emit_hist(
            config,
            "node_open",
            hist,
            [("versions", json!(versions as u64))],
        );
    }
}

fn run_hlc(config: &Config) {
    let mut register = TxTime::default();
    let mut mint = NsHist::new();
    for idx in 0..config.iterations {
        let start = Instant::now();
        register = TxTime::tick(register, 1_000 + idx as u64 / 128);
        black_box(register);
        mint.record(start.elapsed());
    }
    emit_hist(config, "hlc_mint", mint, []);

    let mut receive = NsHist::new();
    let mut merged = register;
    for idx in 0..config.iterations {
        let remote = TxTime::new(1_500 + idx as u64 / 64, (idx % 7) as u32);
        let start = Instant::now();
        merged = merged.max(remote);
        black_box(merged);
        receive.record(start.elapsed());
    }
    emit_hist(config, "hlc_receive_max", receive, []);

    let mut compare = NsHist::new();
    let a = TxTime::new(2_000, 1);
    let b = TxTime::new(2_000, 2);
    for _ in 0..config.iterations {
        let start = Instant::now();
        black_box(a < b);
        compare.record(start.elapsed());
    }
    emit_hist(config, "hlc_compare", compare, []);
}

fn run_domination(config: &Config) {
    for heads in [1_usize, 2, 8, 64] {
        let (dir, mut core) = open_node(node(210), schema());
        let _dir = dir;
        for idx in 0..heads {
            let unit = commit_unit(node((idx + 1) as u8), row(1), idx as u64, Vec::new());
            let fate = core_ingest(&mut core, &unit);
            assert_accepted(&fate);
        }

        let mut hist = NsHist::new();
        for _ in 0..config.iterations {
            let start = Instant::now();
            let rows = core
                .current_rows(TABLE, DurabilityTier::Local)
                .expect("current rows");
            black_box(rows.len());
            hist.record(start.elapsed());
        }
        emit_hist(
            config,
            "domination_winner_probe",
            hist,
            [
                ("heads", json!(heads)),
                (
                    "notes",
                    json!(
                        "public current_rows probe over heads constructed through normal ingest; exact private clock-condition helper is not exposed"
                    ),
                ),
            ],
        );
    }
}

fn run_deletion_register(config: &Config) {
    let (dir, mut core) = open_node(node(211), schema());
    let _dir = dir;
    let row_uuid = row(2);
    for idx in 0..32 {
        let deletion = if idx % 2 == 0 {
            DeletionEvent::Deleted
        } else {
            DeletionEvent::Restored
        };
        let unit = commit_deletion_unit(node((idx + 1) as u8), row_uuid, idx as u64, deletion);
        let fate = core_ingest(&mut core, &unit);
        assert_accepted(&fate);
    }
    let mut hist = NsHist::new();
    for _ in 0..config.iterations {
        let start = Instant::now();
        let rows = core
            .current_rows(TABLE, DurabilityTier::Local)
            .expect("current rows");
        black_box(rows.len());
        hist.record(start.elapsed());
    }
    emit_hist(
        config,
        "deletion_register_resolution",
        hist,
        [("events", json!(32_u64))],
    );
}

fn run_ingest_rate(config: &Config) {
    let (dir, mut core) = open_node(node(212), schema());
    let _dir = dir;
    let mut hist = NsHist::new();
    let mut bytes = 0_u64;
    for idx in 0..config.iterations {
        let unit = commit_unit(node(10), row(10_000 + idx), idx as u64, Vec::new());
        bytes += commit_unit_bytes(&unit);
        let start = Instant::now();
        let fate = core_ingest(&mut core, &unit);
        hist.record(start.elapsed());
        assert_accepted(&fate);
    }
    let p50 = hist.value_at_quantile(0.50).max(1);
    emit_hist(
        config,
        "version_ingest_rate",
        hist,
        [
            ("versions", json!(config.iterations)),
            ("bytes", json!(bytes)),
            ("versions_per_sec_p50", json!(1_000_000_000_u64 / p50)),
        ],
    );
}

fn run_commit_unit(config: &Config) {
    for rows_per_unit in [1_usize, 10, 100] {
        let table = table_schema();
        let schema_version = schema().version_id();
        let mut encode = NsHist::new();
        let mut decode = NsHist::new();
        let (dir, mut core) = open_node(node(213), schema());
        let _dir = dir;
        let mut bytes = 0_u64;
        for iter in 0..config.iterations {
            let start = Instant::now();
            let mut versions = Vec::with_capacity(rows_per_unit);
            for idx in 0..rows_per_unit {
                versions.push(
                    VersionRecord::from_cells(
                        &table,
                        schema_version,
                        row(20_000 + iter * rows_per_unit + idx),
                        Vec::new(),
                        &cells(&format!("cu-{iter}-{idx}")),
                        None,
                    )
                    .expect("encode version"),
                );
            }
            let tx = Transaction {
                tx_id: jazz::tx::TxId::new(
                    TxTime::new(50_000 + iter as u64, 0),
                    node((rows_per_unit % 200) as u8 + 1),
                ),
                kind: jazz::tx::TxKind::Mergeable,
                source_branch: None,
                n_total_writes: rows_per_unit.try_into().expect("rows per unit fits u32"),
                made_by: AuthorId::SYSTEM,
                base_snapshot: None,
                row_read_set: None,
                absent_read_set: None,
                predicate_read_set: None,
                user_metadata_json: None,
            };
            let unit = SyncMessage::CommitUnit {
                tx: tx.clone(),
                versions: versions.clone(),
            };
            encode.record(start.elapsed());
            bytes += commit_unit_bytes(&unit);

            let start = Instant::now();
            let [fate] = core
                .ingest_commit_unit(tx, versions, 100_000 + iter as u64)
                .expect("ingest commit unit")
                .try_into()
                .expect("one fate");
            decode.record(start.elapsed());
            assert_accepted(&fate);
        }
        emit_hist(
            config,
            "commit_unit_encode",
            encode,
            [
                ("rows_per_unit", json!(rows_per_unit)),
                ("bytes", json!(bytes)),
            ],
        );
        emit_hist(
            config,
            "commit_unit_decode_ingest",
            decode,
            [
                ("rows_per_unit", json!(rows_per_unit)),
                ("bytes", json!(bytes)),
            ],
        );
    }
}

fn run_read_set_capture(config: &Config) {
    let (dir, mut node_) = open_node(node(214), schema());
    let _dir = dir;
    seed_local_rows(&mut node_, 64);

    let mut row_hist = NsHist::new();
    let tx = node_.open_exclusive().expect("open tx");
    for idx in 0..config.iterations {
        let start = Instant::now();
        let _ = node_
            .tx_read(tx, TABLE, row(idx % 64))
            .expect("tx read capture");
        row_hist.record(start.elapsed());
    }
    node_.abandon_tx(tx).expect("abandon");
    emit_hist(config, "read_set_capture_row", row_hist, []);

    let mut predicate_hist = NsHist::new();
    for _ in 0..config.iterations {
        let tx = node_.open_exclusive().expect("open tx");
        let start = Instant::now();
        let rows = node_
            .tx_current_rows(tx, TABLE)
            .expect("tx current rows capture");
        black_box(rows.len());
        predicate_hist.record(start.elapsed());
        node_.abandon_tx(tx).expect("abandon");
    }
    emit_hist(config, "read_set_capture_predicate", predicate_hist, []);
}

fn run_validation_entries(config: &Config) {
    let (core_dir, mut core) = open_node(node(215), schema());
    let (client_dir, mut client) = open_node(node(216), schema());
    let (_core_dir, _client_dir) = (core_dir, client_dir);
    for idx in 0..64 {
        let unit = client
            .commit_mergeable_unit(
                MergeableCommit::new(TABLE, row(idx), 1_000 + idx as u64)
                    .made_by(AuthorId::SYSTEM)
                    .cells(cells(&format!("seed-{idx}"))),
            )
            .expect("seed unit")
            .1;
        let fate = core_ingest(&mut core, &unit);
        client.apply_sync_message(fate).expect("apply seed fate");
    }

    let mut row_hist = NsHist::new();
    for idx in 0..config.iterations {
        let tx = client.open_exclusive().expect("open tx");
        let _ = client.tx_read(tx, TABLE, row(idx % 64)).expect("tx read");
        client
            .tx_write(
                tx,
                TABLE,
                row(40_000 + idx),
                cells(&format!("rowval-{idx}")),
                None,
            )
            .expect("tx write");
        let (_tx_id, unit) = client
            .commit_exclusive(tx, AuthorId::SYSTEM, 10_000 + idx as u64)
            .expect("commit exclusive");
        let (tx, versions) = commit_parts(&unit);
        let start = Instant::now();
        let [fate] = core
            .ingest_commit_unit(tx, versions, 20_000 + idx as u64)
            .expect("core ingest")
            .try_into()
            .expect("one fate");
        row_hist.record(start.elapsed());
        client.apply_sync_message(fate.clone()).expect("apply fate");
        assert_accepted(&fate);
    }
    emit_hist(
        config,
        "validation_row_entry",
        row_hist,
        [("read_set_entries", json!(1_u64))],
    );

    let mut predicate_hist = NsHist::new();
    for idx in 0..config.iterations {
        let tx = client.open_exclusive().expect("open tx");
        let _ = client.tx_current_rows(tx, TABLE).expect("tx current rows");
        client
            .tx_write(
                tx,
                TABLE,
                row(80_000 + idx),
                cells(&format!("predval-{idx}")),
                None,
            )
            .expect("tx write");
        let (_tx_id, unit) = client
            .commit_exclusive(tx, AuthorId::SYSTEM, 100_000 + idx as u64)
            .expect("commit exclusive");
        let (tx, versions) = commit_parts(&unit);
        let start = Instant::now();
        let fates = core
            .ingest_commit_unit(tx, versions, 200_000 + idx as u64)
            .expect("core ingest");
        predicate_hist.record(start.elapsed());
        for fate in fates {
            client.apply_sync_message(fate.clone()).expect("apply fate");
            assert_accepted(&fate);
        }
    }
    emit_hist(
        config,
        "validation_predicate_entry",
        predicate_hist,
        [("predicate_entries", json!(1_u64))],
    );
}

fn seed_local_rows(node_: &mut NodeState<RocksDbStorage>, rows: usize) {
    for idx in 0..rows {
        let _ = node_
            .commit_mergeable_unit(
                MergeableCommit::new(TABLE, row(idx), 1_000 + idx as u64)
                    .made_by(AuthorId::SYSTEM)
                    .cells(cells(&format!("seed-{idx}"))),
            )
            .expect("seed local");
    }
}

fn commit_unit(
    node_uuid: NodeUuid,
    row_uuid: RowUuid,
    idx: u64,
    parents: Vec<jazz::tx::TxId>,
) -> SyncMessage {
    let (_dir, mut node_) = open_node(node_uuid, schema());
    node_
        .commit_mergeable_unit(
            MergeableCommit::new(TABLE, row_uuid, 1_000 + idx)
                .made_by(AuthorId::SYSTEM)
                .parents(parents)
                .cells(cells(&format!("v-{idx}"))),
        )
        .expect("commit unit")
        .1
}

fn commit_deletion_unit(
    node_uuid: NodeUuid,
    row_uuid: RowUuid,
    idx: u64,
    deletion: DeletionEvent,
) -> SyncMessage {
    let (_dir, mut node_) = open_node(node_uuid, schema());
    node_
        .commit_mergeable_unit(
            MergeableCommit::new(TABLE, row_uuid, 2_000 + idx)
                .made_by(AuthorId::SYSTEM)
                .deletion(deletion),
        )
        .expect("deletion unit")
        .1
}

fn core_ingest(core: &mut NodeState<RocksDbStorage>, unit: &SyncMessage) -> SyncMessage {
    let SyncMessage::CommitUnit { tx, versions } = unit else {
        panic!("expected commit unit");
    };
    let [fate] = core
        .ingest_commit_unit(
            tx.clone(),
            versions.clone(),
            tx.tx_id.physical_ms() + SKEW_TOLERANCE_MS,
        )
        .expect("core ingest")
        .try_into()
        .expect("one fate update");
    fate
}

fn assert_accepted(fate: &SyncMessage) {
    assert!(
        matches!(
            fate,
            SyncMessage::FateUpdate {
                fate: Fate::Accepted,
                global_seq: Some(_),
                ..
            }
        ),
        "expected accepted fate: {fate:?}"
    );
}

fn commit_parts(unit: &SyncMessage) -> (Transaction, Vec<VersionRecord>) {
    let SyncMessage::CommitUnit { tx, versions } = unit else {
        panic!("expected commit unit");
    };
    (tx.clone(), versions.clone())
}

fn commit_unit_bytes(unit: &SyncMessage) -> u64 {
    let SyncMessage::CommitUnit { versions, .. } = unit else {
        return 0;
    };
    versions
        .iter()
        .map(|version| version.record().raw().len() as u64)
        .sum()
}

fn schema() -> JazzSchema {
    JazzSchema::new([table_schema()])
}

fn table_schema() -> TableSchema {
    TableSchema::new(
        TABLE,
        [
            ColumnSchema::new("title", ColumnType::String),
            ColumnSchema::new("owner", ColumnType::Uuid),
        ],
    )
}

fn open_node(
    node_uuid: NodeUuid,
    schema: JazzSchema,
) -> (tempfile::TempDir, NodeState<RocksDbStorage>) {
    let temp_dir = tempfile::tempdir().expect("tempdir");
    let node = open_node_at(temp_dir.path(), node_uuid, schema);
    (temp_dir, node)
}

fn open_node_at(
    path: &std::path::Path,
    node_uuid: NodeUuid,
    schema: JazzSchema,
) -> NodeState<RocksDbStorage> {
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let storage = RocksDbStorage::open_with_durability(path, &refs, Durability::WalNoSync)
        .expect("open rocksdb");
    NodeState::new(node_uuid, schema, storage).expect("node")
}

fn cells(title: &str) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("title".to_owned(), Value::String(title.to_owned())),
        ("owner".to_owned(), Value::Uuid(AuthorId::SYSTEM.0)),
    ])
}

fn node(byte: u8) -> NodeUuid {
    NodeUuid::from_bytes([byte; 16])
}

fn row(idx: usize) -> RowUuid {
    let mut bytes = [0_u8; 16];
    bytes[0..8].copy_from_slice(&(idx as u64 + 1).to_be_bytes());
    RowUuid::from_bytes(bytes)
}

struct NsHist {
    hist: Histogram<u64>,
}

impl NsHist {
    fn new() -> Self {
        Self {
            hist: Histogram::new(3).expect("histogram"),
        }
    }

    fn record(&mut self, elapsed: Duration) {
        self.hist
            .record(elapsed.as_nanos().min(u64::MAX as u128) as u64)
            .expect("record sample");
    }

    fn value_at_quantile(&self, quantile: f64) -> u64 {
        self.hist.value_at_quantile(quantile)
    }
}

fn emit_hist<const N: usize>(
    config: &Config,
    primitive: &str,
    hist: NsHist,
    extra: [(&str, JsonValue); N],
) {
    let mut fields = metadata_fields("micro", "micro", config.seed, "micro");
    fields.insert("primitive".to_owned(), json!(primitive));
    fields.insert("samples".to_owned(), json!(config.iterations));
    fields.insert(
        format!("{primitive}_p50_ns"),
        json!(hist.hist.value_at_quantile(0.50)),
    );
    fields.insert(
        format!("{primitive}_p95_ns"),
        json!(hist.hist.value_at_quantile(0.95)),
    );
    fields.insert(
        format!("{primitive}_p99_ns"),
        json!(hist.hist.value_at_quantile(0.99)),
    );
    fields.insert(format!("{primitive}_max_ns"), json!(hist.hist.max()));
    for (key, value) in extra {
        fields.insert(key.to_owned(), value);
    }
    emit_object(fields);
}

fn emit_object(fields: Map<String, JsonValue>) {
    let line = serde_json::to_string(&JsonValue::Object(fields)).expect("json line");
    emit_json_line("micro", &line);
}

#[inline(never)]
fn black_box<T>(value: T) -> T {
    std::hint::black_box(value)
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
