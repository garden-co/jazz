//! Native worker/foreground decomposition of the synthetic browser batch receipt.
//! Direct nodes intentionally expose phase boundaries that the public Db owner
//! loop combines. This excludes JS, IndexedDB, scheduling and auth bootstrap.
use std::{collections::BTreeMap, time::Instant};
#[path = "support/perf_control.rs"]
mod perf_control;
mod schema_fixture;
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
    schema_fixture::compile(
        SchemaBuilder::new().table(
            TableSchemaBuilder::new("tasks")
                .column("title", ColumnType::Text)
                .column("done", ColumnType::Boolean),
        ),
    )
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
    println!(
        "{}",
        json!({"phase":format!("{name}_payload"),"backend":backend,"rows":count,"bytes":bytes.len()})
    );
    let message = phase(backend, count, &format!("{name}_decode"), || {
        jazz::wire::decode_sync_message_trusted(&bytes).unwrap()
    });
    node.reset_storage_read_metrics();
    phase(backend, count, &format!("{name}_receiver_ingest"), || {
        support::apply_and_settle(node, message)
    });
    emit_node_metrics(node, backend, count, &format!("{name}_receiver_metrics"));
    let (shape, binding, _) = support::table_subscription(schema, "tasks", peer.identity());
    let rows = phase(backend, count, &format!("{name}_receiver_query"), || {
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
    let expected = if name == "initial" { 0 } else { changed };
    assert_eq!(
        done_rows
            .iter()
            .map(|r| r.row_uuid())
            .collect::<std::collections::BTreeSet<_>>(),
        (0..expected).map(row).collect()
    );
}
fn run<S: OrderedKvStorage + ReopenableStorage + 'static>(
    backend: &str,
    count: usize,
    percent: usize,
    mut storage: impl FnMut() -> S,
) {
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
    let mut worker = phase(backend, count, "reopen", || open(&schema, storage(), 1));
    let mut peer = PeerState::new();
    let mut foreground = receiver(&schema, &peer);
    worker.reset_storage_read_metrics();
    let message = phase(backend, count, "initial_publish", || {
        publish(&mut worker, &mut peer, &schema, true)
    });
    emit_node_metrics(&worker, backend, count, "initial_publish_metrics");
    deliver(
        backend,
        count,
        "initial",
        message,
        &mut foreground,
        &schema,
        &peer,
        count,
    );
    assert!((1..=100).contains(&percent));
    let changed = count * percent / 100;
    foreground.reset_storage_read_metrics();
    let commits = (0..changed)
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
        support::settle_transaction(&mut foreground, publication)
    });
    emit_node_metrics(&foreground, backend, count, "batch_author_metrics");
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
    emit_node_metrics(&worker, backend, count, "batch_worker_ingest_metrics");
    worker.reset_storage_read_metrics();
    let message = phase(backend, count, "batch_publish", || {
        publish(&mut worker, &mut peer, &schema, false)
    });
    emit_node_metrics(&worker, backend, count, "batch_publish_metrics");
    deliver(
        backend,
        count,
        "batch",
        message,
        &mut foreground,
        &schema,
        &peer,
        changed,
    );
    drop(foreground);
    drop(worker);
    let mut worker = phase(backend, count, "post_reopen", || {
        open(&schema, storage(), 1)
    });
    let mut peer = PeerState::new();
    let mut foreground = receiver(&schema, &peer);
    let message = phase(backend, count, "post_publish", || {
        publish(&mut worker, &mut peer, &schema, true)
    });
    deliver(
        backend,
        count,
        "post",
        message,
        &mut foreground,
        &schema,
        &peer,
        changed,
    );
}
fn emit_node_metrics<S: OrderedKvStorage>(
    node: &NodeState<S>,
    backend: &str,
    count: usize,
    name: &str,
) {
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
    run("memory", count, percent, || memory.clone());
    let dir = tempfile::tempdir().unwrap();
    run("rocksdb_wal", count, percent, || {
        RocksDbStorage::open_with_durability(dir.path(), &refs, Durability::WalNoSync).unwrap()
    });
}

/// Internal protocol fixture: phase counters and direct publication are not
/// exposed by the public Db API. Assertions still check exact visible rows.
pub(crate) fn correctness_smoke() {
    run_fixture(10, 50);
}

pub(crate) fn main() {
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
