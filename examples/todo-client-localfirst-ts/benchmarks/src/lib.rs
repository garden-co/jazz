//! Native worker/foreground decomposition of the synthetic browser batch receipt.
//! Direct nodes intentionally expose phase boundaries that the public Db owner
//! loop combines. This excludes JS, IndexedDB, scheduling and auth bootstrap.
use std::{collections::BTreeMap, time::Instant};
mod perf_control;
mod support;
use jazz::{
    block_on,
    groove::{
        records::Value,
        storage::{MemoryStorage, OrderedKvStorage, ReopenableStorage},
    },
    ids::{NodeUuid, RowUuid},
    node::{MergeableCommit, NodeState},
    peer::PeerState,
    protocol::{RegisterShapeOptions, ShapeAst, Subscribe, SyncMessage},
    schema::JazzSchema,
    tools::{ColumnType, SchemaBuilder, TableSchemaBuilder},
    tx::DurabilityTier,
};
use jazz_storage_rocksdb::{Durability, RocksDbStorage};
use serde_json::json;
thread_local! { static REPORT: std::cell::Cell<bool> = const { std::cell::Cell::new(false) }; }

#[cfg(unix)]
fn clock_ns() -> u64 {
    let mut time = libc::timespec {
        tv_sec: 0,
        tv_nsec: 0,
    };
    assert_eq!(
        unsafe { libc::clock_gettime(libc::CLOCK_MONOTONIC, &mut time) },
        0
    );
    time.tv_sec as u64 * 1_000_000_000 + time.tv_nsec as u64
}
#[cfg(not(unix))]
fn clock_ns() -> u64 {
    static START: std::sync::OnceLock<Instant> = std::sync::OnceLock::new();
    START.get_or_init(Instant::now).elapsed().as_nanos() as u64
}
#[inline(never)]
fn phase<T>(backend: &str, count: usize, name: &str, f: impl FnOnce() -> T) -> T {
    if !REPORT.get() {
        return f();
    }
    let profile = perf_control::PerfControl::selected(backend, name);
    let cpu_profiled = profile.is_some();
    let start_ns = clock_ns();
    let start = Instant::now();
    let result = f();
    let elapsed = start.elapsed();
    let end_ns = clock_ns();
    drop(profile);
    println!(
        "{}",
        json!({"rust_allocator":jazz_benchmark_guard::ALLOCATOR_NAME,"allocator_preload":std::env::var_os("LD_PRELOAD").is_some(),"backend":backend,"rows":count,"phase":name,"wall_us":elapsed.as_micros(),"start_ns":start_ns,"end_ns":end_ns,"cpu_profiled":cpu_profiled})
    );
    result
}
fn schema() -> JazzSchema {
    let schema = SchemaBuilder::new()
        .table(
            TableSchemaBuilder::new("tasks")
                .column("title", ColumnType::Text)
                .column("done", ColumnType::Boolean),
        )
        .build();
    JazzSchema::new(&schema).unwrap()
}
fn row(i: usize) -> RowUuid {
    RowUuid::from_bytes((i as u128 + 1).to_be_bytes())
}
fn cells(i: usize, done: bool) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("title".into(), Value::String(format!("Task {i}"))),
        ("done".into(), Value::Bool(done)),
    ])
}
fn open<S: OrderedKvStorage + ReopenableStorage + 'static>(
    schema: &JazzSchema,
    storage: S,
    id: u8,
) -> NodeState<S> {
    block_on(NodeState::new_with_shared_test_catalogue(
        NodeUuid::from_bytes([id; 16]),
        schema.clone(),
        storage,
    ))
    .unwrap()
}
fn publish<S: OrderedKvStorage>(
    worker: &mut NodeState<S>,
    peer: &mut PeerState,
    schema: &JazzSchema,
    initial: bool,
) -> SyncMessage {
    let (shape, binding, mut key) = support::table_subscription(schema, "tasks", peer.identity());
    let mut opts = RegisterShapeOptions::default();
    opts.tier = DurabilityTier::Local;
    key.read_view = opts.read_view_key();
    peer.set_subscription_policy_binding(key, (peer.identity(), BTreeMap::new()));
    if initial {
        block_on(
            peer.rehydrate_query_for_subscription_with_opts(worker, key, &shape, &binding, opts),
        )
        .unwrap()
        .expect("initial update")
    } else {
        block_on(peer.query_update_for_subscription_with_opts(worker, key, &shape, &binding, opts))
            .unwrap()
            .expect("updated scope")
    }
}
fn receiver(schema: &JazzSchema, peer: &PeerState) -> NodeState<MemoryStorage> {
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let mut node = open(schema, MemoryStorage::new(&refs).unwrap(), 2);
    let (shape, _, mut key) = support::table_subscription(schema, "tasks", peer.identity());
    let mut opts = RegisterShapeOptions::default();
    opts.tier = DurabilityTier::Local;
    key.read_view = opts.read_view_key();
    support::apply_and_settle(
        &mut node,
        SyncMessage::RegisterShape {
            shape_id: shape.shape_id(),
            ast: ShapeAst::from_validated(&shape),
            opts,
        },
    );
    support::apply_and_settle(
        &mut node,
        SyncMessage::Subscribe(Subscribe {
            shape_id: shape.shape_id(),
            subscription: key,
            values: vec![],
            known_state: None,
            delegated_session: Some(jazz::protocol::DelegatedSessionBinding {
                identity: peer.identity(),
                claims: BTreeMap::new(),
            }),
        }),
    );
    node
}
#[allow(clippy::too_many_arguments)]
fn deliver(
    backend: &str,
    count: usize,
    name: &str,
    message: SyncMessage,
    node: &mut NodeState<MemoryStorage>,
    schema: &JazzSchema,
    peer: &PeerState,
    changed: usize,
) {
    let bytes = phase(backend, count, &format!("{name}_encode"), || {
        jazz::wire::encode_sync_message(&message).unwrap()
    });
    if REPORT.get() {
        println!(
            "{}",
            json!({"phase":format!("{name}_payload"),"backend":backend,"rows":count,"bytes":bytes.len()})
        );
    }
    let message = phase(backend, count, &format!("{name}_decode"), || {
        jazz::wire::decode_sync_message_trusted(&bytes).unwrap()
    });
    node.reset_storage_read_metrics();
    phase(backend, count, &format!("{name}_receiver_ingest"), || {
        support::apply_and_settle(node, message)
    });
    emit_node_metrics(node, backend, count, &format!("{name}_receiver_metrics"));
    if REPORT.get() {
        verify_rows(backend, count, node, schema, peer, changed);
    }
}
fn verify_rows(
    backend: &str,
    count: usize,
    node: &mut NodeState<MemoryStorage>,
    schema: &JazzSchema,
    peer: &PeerState,
    changed: usize,
) {
    let (shape, binding, _) = support::table_subscription(schema, "tasks", peer.identity());
    let rows = phase(backend, count, "receiver_query", || {
        block_on(node.query_rows(&shape, &binding, DurabilityTier::Local)).unwrap()
    });
    assert_eq!(rows.len(), count);
    let actual = rows
        .iter()
        .map(|r| r.row_uuid())
        .collect::<std::collections::BTreeSet<_>>();
    assert_eq!(actual, (0..count).map(row).collect());
    let done_shape = jazz::query::Query::from("tasks")
        .filter(jazz::query::eq(
            jazz::query::col("done"),
            jazz::query::lit(true),
        ))
        .validate(schema)
        .unwrap();
    let done_binding = done_shape.bind(BTreeMap::new()).unwrap();
    let done_rows =
        block_on(node.query_rows(&done_shape, &done_binding, DurabilityTier::Local)).unwrap();
    let expected = changed;
    assert_eq!(
        done_rows
            .iter()
            .map(|r| r.row_uuid())
            .collect::<std::collections::BTreeSet<_>>(),
        (0..expected).map(row).collect()
    );
}
/// Prepared native worker/foreground workload. Receiver/body delivery and
/// worker WAL completion are included; JS facade staging and fsync are not.
pub struct Fixture<S: OrderedKvStorage + ReopenableStorage + 'static> {
    backend: &'static str,
    count: usize,
    schema: JazzSchema,
    seed: jazz::tx::TxId,
    worker: Option<NodeState<S>>,
    foreground: Option<NodeState<MemoryStorage>>,
    peer: PeerState,
    changed: usize,
    read_result: Vec<jazz::node::CurrentRow>,
    storage: Box<dyn FnMut() -> S>,
}
impl<S: OrderedKvStorage + ReopenableStorage + 'static> Fixture<S> {
    fn seeded(
        backend: &'static str,
        count: usize,
        mut storage: impl FnMut() -> S + 'static,
    ) -> Self {
        assert!((2..=4096).contains(&count));
        let schema = schema();
        let mut worker = open(&schema, storage(), 1);
        let commits = (0..count)
            .map(|i| MergeableCommit::new("tasks", row(i), 1000).cells(cells(i, false)))
            .collect();
        let publication = phase(backend, count, "seed_author", || {
            block_on(worker.commit_mergeable_many(commits)).unwrap()
        });
        let seed = publication.tx_id();
        phase(backend, count, "seed_persist", || {
            support::settle_transaction(&mut worker, publication)
        });
        drop(worker);
        Self {
            backend,
            count,
            schema,
            storage: Box::new(storage),
            seed,
            worker: None,
            foreground: None,
            peer: PeerState::new(),
            changed: 0,
            read_result: Vec::new(),
        }
    }
    #[inline(never)]
    pub fn reopen(&mut self) {
        self.foreground = None;
        self.worker = None;
        self.peer = PeerState::new();
        let mut worker = phase(self.backend, self.count, "reopen", || {
            open(&self.schema, (self.storage)(), 1)
        });
        let mut foreground = receiver(&self.schema, &self.peer);
        let message = phase(self.backend, self.count, "publish", || {
            publish(&mut worker, &mut self.peer, &self.schema, true)
        });
        deliver(
            self.backend,
            self.count,
            if self.changed == 0 { "initial" } else { "post" },
            message,
            &mut foreground,
            &self.schema,
            &self.peer,
            self.changed,
        );
        self.worker = Some(worker);
        self.foreground = Some(foreground);
        if !REPORT.get() {
            self.read_all();
        }
    }
    #[inline(never)]
    pub fn batch_update(&mut self, changed: usize) {
        assert!(self.changed == 0 && changed <= self.count);
        self.update_range(0..changed);
        self.changed = changed;
        if !REPORT.get() {
            self.read_all();
        }
    }
    #[inline(never)]
    pub fn sequential_update(&mut self, changed: usize) {
        assert!(self.changed == 0 && changed <= self.count);
        for i in 0..changed {
            self.update_range(i..i + 1);
        }
        self.changed = changed;
        if !REPORT.get() {
            self.read_all();
        }
    }
    fn read_all(&mut self) {
        let (shape, binding, _) =
            support::table_subscription(&self.schema, "tasks", self.peer.identity());
        self.read_result = block_on(self.foreground.as_mut().expect("loaded").query_rows(
            &shape,
            &binding,
            DurabilityTier::Local,
        ))
        .unwrap();
    }
    pub fn verify(&mut self) {
        if REPORT.get() {
            verify_rows(
                self.backend,
                self.count,
                self.foreground.as_mut().expect("loaded"),
                &self.schema,
                &self.peer,
                self.changed,
            );
            return;
        }
        assert_eq!(self.read_result.len(), self.count);
        let table = self
            .schema
            .tables
            .iter()
            .find(|table| table.name == "tasks")
            .unwrap();
        let actual = self
            .read_result
            .iter()
            .map(|value| (value.row_uuid(), value))
            .collect::<BTreeMap<_, _>>();
        assert_eq!(actual.len(), self.count);
        for i in 0..self.count {
            let value = actual
                .get(&row(i))
                .expect("expected task present in timed read");
            assert_eq!(
                value.cell(table, "title"),
                Some(Value::String(format!("Task {i}")))
            );
            assert_eq!(
                value.cell(table, "done"),
                Some(Value::Bool(i < self.changed))
            );
        }
    }
    fn update_range(&mut self, range: std::ops::Range<usize>) {
        let backend = self.backend;
        let count = self.count;
        let schema = &self.schema;
        let seed = self.seed;
        let worker = self.worker.as_mut().expect("reopen before updates");
        let foreground = self.foreground.as_mut().expect("reopen before updates");
        let peer = &mut self.peer;
        foreground.reset_storage_read_metrics();
        let commits = range
            .clone()
            .map(|i| {
                MergeableCommit::new("tasks", row(i), 2000)
                    .parents(vec![seed])
                    .cells(cells(i, true))
            })
            .collect();
        let publication = phase(backend, count, "batch_author", || {
            block_on(foreground.commit_mergeable_many(commits)).unwrap()
        });
        let update_tx = publication.tx_id();
        phase(backend, count, "batch_persist", || {
            support::settle_transaction(foreground, publication)
        });
        emit_node_metrics(foreground, backend, count, "batch_author_metrics");
        let unit = phase(backend, count, "batch_upload_build", || {
            block_on(foreground.commit_unit_for(update_tx)).unwrap()
        });
        let bytes = phase(backend, count, "batch_upload_encode", || {
            jazz::wire::encode_sync_message(&unit).unwrap()
        });
        let unit = phase(backend, count, "batch_upload_decode", || {
            jazz::wire::decode_sync_message(&bytes).unwrap()
        });
        let SyncMessage::CommitUnit { tx, versions } = unit else {
            panic!("commit unit")
        };
        worker.reset_storage_read_metrics();
        phase(backend, count, "batch_worker_ingest", || {
            block_on(worker.ingest_relay_commit_unit(tx, versions)).unwrap()
        });
        emit_node_metrics(worker, backend, count, "batch_worker_ingest_metrics");
        worker.reset_storage_read_metrics();
        let message = phase(backend, count, "batch_publish", || {
            publish(worker, peer, schema, false)
        });
        emit_node_metrics(worker, backend, count, "batch_publish_metrics");
        deliver(
            backend, count, "batch", message, foreground, schema, peer, range.end,
        );
    }
}
impl Fixture<RocksDbStorage> {
    pub fn rocksdb(count: usize) -> Self {
        jazz_benchmark_guard::refuse_contaminated_measurement();
        let dir = tempfile::tempdir().unwrap();
        let cfs = schema().column_families();
        Self::seeded("rocksdb_wal", count, move || {
            let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
            RocksDbStorage::open_with_durability(dir.path(), &refs, Durability::WalNoSync).unwrap()
        })
    }
    pub fn loaded(count: usize) -> Self {
        let mut fixture = Self::rocksdb(count);
        fixture.reopen();
        fixture
    }
}
fn run<S: OrderedKvStorage + ReopenableStorage + 'static>(
    backend: &'static str,
    count: usize,
    percent: usize,
    storage: impl FnMut() -> S + 'static,
) {
    let mut fixture = Fixture::seeded(backend, count, storage);
    fixture.reopen();
    fixture.batch_update(count * percent / 100);
    fixture.reopen();
    fixture.verify();
}
fn emit_node_metrics<S: OrderedKvStorage>(
    node: &NodeState<S>,
    backend: &str,
    count: usize,
    name: &str,
) {
    if !REPORT.get() {
        return;
    }
    let mut fields = support::phase_fields(name, 0);
    fields.insert("backend".into(), json!(backend));
    fields.insert("rows".into(), json!(count));
    support::insert_node_metrics(&mut fields, "node", node);
    support::emit_json_line("local_batch", fields);
}
fn raw_storage<S: OrderedKvStorage>(backend: &str, count: usize, storage: S) {
    use jazz::groove::storage::OwnedWriteOperation;
    let ops = (0..count)
        .map(|i| OwnedWriteOperation::Set {
            cf: "raw".into(),
            key: (i as u64).to_be_bytes().to_vec(),
            value: vec![42; 128],
        })
        .collect::<Vec<_>>();
    phase(backend, count, "raw_batch_write", || {
        block_on(storage.write_many(ops)).unwrap()
    });
    phase(backend, count, "raw_flush", || {
        block_on(storage.flush_write_boundary()).unwrap()
    });
    let rows = phase(backend, count, "raw_scan", || {
        block_on(storage.prefix("raw".into(), vec![])).unwrap()
    });
    assert_eq!(rows.len(), count);
    assert!(rows.iter().all(|(_, v)| v == &vec![42; 128]));
}
fn run_fixture(count: usize, percent: usize) {
    assert!(
        (2..=4096).contains(&count),
        "fixture uses one wire transaction: choose 2..=4096 rows"
    );
    raw_storage("memory", count, MemoryStorage::new(&["raw"]).unwrap());
    let raw_dir = tempfile::tempdir().unwrap();
    raw_storage(
        "rocksdb_wal",
        count,
        RocksDbStorage::open_with_durability(raw_dir.path(), &["raw"], Durability::WalNoSync)
            .unwrap(),
    );
    let schema = schema();
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let memory = MemoryStorage::new(&refs).unwrap();
    run("memory", count, percent, move || memory.clone());
    let dir = tempfile::tempdir().unwrap();
    run("rocksdb_wal", count, percent, move || {
        let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
        RocksDbStorage::open_with_durability(dir.path(), &refs, Durability::WalNoSync).unwrap()
    });
}

/// Internal protocol fixture: phase counters and direct publication are not
/// exposed by the public Db API. Assertions still check exact visible rows.
pub fn correctness_smoke() {
    run_fixture(10, 50);
}

pub fn profile_main() {
    REPORT.set(true);
    if std::env::var_os("JAZZ_PERF_PHASE").is_some() {
        eprintln!("scoped CPU attribution run: timings are not clean latency receipts");
    } else {
        jazz_benchmark_guard::refuse_contaminated_measurement();
    }
    let percent = support::env_usize("JAZZ_BATCH_UPDATE_PERCENT", 90);
    for count in support::csv_usizes("JAZZ_BATCH_ROWS", "1500") {
        run_fixture(count, percent);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn batch_reopen_preserves_exact_rows() {
        correctness_smoke();
    }
    #[test]
    fn updates_return_current_rows_before_reopen() {
        for sequential in [false, true] {
            let schema = schema();
            let cfs = schema.column_families();
            let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
            let memory = MemoryStorage::new(&refs).unwrap();
            let mut f = Fixture::seeded("memory", 10, move || memory.clone());
            f.reopen();
            f.verify();
            if sequential {
                f.sequential_update(9);
            } else {
                f.batch_update(9);
            }
            f.verify();
            f.reopen();
            f.verify();
        }
    }
    #[test]
    fn sequential_reopen_preserves_exact_rows() {
        let schema = schema();
        let cfs = schema.column_families();
        let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
        let memory = MemoryStorage::new(&refs).unwrap();
        let mut f = Fixture::seeded("memory", 10, move || memory.clone());
        f.reopen();
        f.sequential_update(9);
        f.reopen();
        f.verify();
    }
}
