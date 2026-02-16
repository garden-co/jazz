use std::hint::black_box;
use std::path::Path;
use std::str::FromStr;
use std::time::Duration;

use bf_tree::{BfTree, Config as BfConfig, LeafInsertResult, LeafReadResult, ScanReturnField};
use criterion::{
    BatchSize, BenchmarkId, Criterion, Throughput, criterion_group, criterion_main,
    measurement::WallTime,
};
use fjall::{
    Database as FjallDatabase, Keyspace as FjallKeyspace, KeyspaceCreateOptions, PersistMode,
};
use opfs_btree::{
    BTreeOptions as OpfsBTreeOptions, OpfsBTree as OpfsBTreeDb, StdFile as OpfsStdFile,
};
use rocksdb::{Direction, IteratorMode, Options as RocksOptions, WriteOptions};
use surrealkv::{
    Durability as SurrealDurability, LSMIterator, Mode as SurrealMode,
    Transaction as SurrealTransaction, Tree as SurrealTree, TreeBuilder as SurrealTreeBuilder,
};
use tempfile::TempDir;
use tokio::runtime::{Builder as TokioRuntimeBuilder, Runtime as TokioRuntime};

const DEFAULT_VALUE_SIZES: [usize; 3] = [32, 256, 4096];
const DEFAULT_KEY_COUNT: usize = 5_000;
const DEFAULT_RANGE_QUERY_COUNT: usize = 2_000;
const RANGE_WINDOW_KEYS: usize = 128;
const RANGE_RESULT_LIMIT: usize = 64;
const BF_TREE_CACHE_BYTES: usize = 32 * 1024 * 1024;
const BF_TREE_MAX_VALUE_SIZE: usize = 30 * 1024;
const DEFAULT_MIXED_BASE_SEED: u64 = 0xA5A5_A5A5_0123_4567;

#[derive(Clone, Copy)]
struct MixedScenario {
    name: &'static str,
    read_pct: u8,
    write_pct: u8,
    update_pct: u8,
}

const MIXED_SCENARIOS: [MixedScenario; 3] = [
    MixedScenario {
        name: "mixed_random_70r_30w",
        read_pct: 70,
        write_pct: 30,
        update_pct: 80,
    },
    MixedScenario {
        name: "mixed_random_50r_50w_with_updates",
        read_pct: 50,
        write_pct: 50,
        update_pct: 90,
    },
    MixedScenario {
        name: "mixed_random_60r_20w_20d",
        read_pct: 60,
        write_pct: 20,
        update_pct: 80,
    },
];

#[derive(Clone, Copy)]
enum OpChoice {
    Read,
    Write,
    Delete,
}

#[derive(Clone, Copy)]
struct DeterministicRng {
    state: u64,
}

impl DeterministicRng {
    fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    fn next_u64(&mut self) -> u64 {
        self.state = self.state.wrapping_mul(6364136223846793005).wrapping_add(1);
        self.state
    }

    fn next_u8(&mut self) -> u8 {
        (self.next_u64() >> 56) as u8
    }

    fn next_usize(&mut self, upper: usize) -> usize {
        if upper == 0 {
            return 0;
        }
        (self.next_u64() as usize) % upper
    }
}

fn quick_mode() -> bool {
    std::env::var("JAZZ_COMPARE_BENCH_QUICK")
        .map(|v| {
            let v = v.trim();
            v == "1" || v.eq_ignore_ascii_case("true") || v.eq_ignore_ascii_case("yes")
        })
        .unwrap_or(false)
}

fn configure_group(group: &mut criterion::BenchmarkGroup<'_, WallTime>) {
    if quick_mode() {
        group.sample_size(10);
        group.warm_up_time(Duration::from_millis(200));
        group.measurement_time(Duration::from_millis(800));
    }
}

fn key_count() -> usize {
    std::env::var("JAZZ_COMPARE_BENCH_KEY_COUNT")
        .ok()
        .and_then(|v| usize::from_str(&v).ok())
        .filter(|&n| n > 0)
        .unwrap_or(DEFAULT_KEY_COUNT)
}

fn range_query_count() -> usize {
    std::env::var("JAZZ_COMPARE_RANGE_QUERY_COUNT")
        .ok()
        .and_then(|v| usize::from_str(&v).ok())
        .filter(|&n| n > 0)
        .unwrap_or(DEFAULT_RANGE_QUERY_COUNT)
}

fn value_sizes() -> Vec<usize> {
    std::env::var("JAZZ_COMPARE_BENCH_VALUE_SIZES")
        .ok()
        .map(|raw| {
            raw.split(',')
                .filter_map(|x| usize::from_str(x.trim()).ok())
                .filter(|&n| n > 0)
                .collect::<Vec<_>>()
        })
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| DEFAULT_VALUE_SIZES.to_vec())
}

fn derive_seed(label: &str, value_size: usize) -> u64 {
    let mut h = 0xcbf2_9ce4_8422_2325u64 ^ DEFAULT_MIXED_BASE_SEED ^ (value_size as u64);
    for &b in label.as_bytes() {
        h ^= b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    h ^ ((value_size as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15))
}

fn choose_operation(scenario: MixedScenario, roll: u8) -> OpChoice {
    if roll < scenario.read_pct {
        return OpChoice::Read;
    }
    if roll < scenario.read_pct.saturating_add(scenario.write_pct) {
        return OpChoice::Write;
    }
    OpChoice::Delete
}

fn engine_enabled(name: &str) -> bool {
    let Ok(raw) = std::env::var("JAZZ_COMPARE_ENGINES") else {
        return true;
    };
    raw.split(',').any(|entry| entry.trim() == name)
}

trait Engine {
    fn put(&mut self, key: &[u8], value: &[u8]);
    fn delete(&mut self, key: &[u8]);
    fn get_opt(&mut self, key: &[u8]) -> Option<Vec<u8>>;
    fn get(&mut self, key: &[u8]) -> Vec<u8>;
    fn range_checksum(&mut self, start: &[u8], end: &[u8], limit: usize) -> u64;
    fn finish_writes(&mut self);
}

struct OpfsBTreeEngine {
    db: OpfsBTreeDb<OpfsStdFile>,
}

impl OpfsBTreeEngine {
    fn open(path: &Path) -> Self {
        let file = OpfsStdFile::open(path.join("opfs-btree.data")).expect("open opfs-btree file");
        let mut options = OpfsBTreeOptions::default();
        options.cache_bytes = 32 * 1024 * 1024;
        let db = OpfsBTreeDb::open(file, options).expect("open opfs-btree");
        Self { db }
    }
}

impl Engine for OpfsBTreeEngine {
    fn put(&mut self, key: &[u8], value: &[u8]) {
        self.db.put(key, value).expect("opfs-btree put");
    }

    fn delete(&mut self, key: &[u8]) {
        self.db.delete(key).expect("opfs-btree delete");
    }

    fn get_opt(&mut self, key: &[u8]) -> Option<Vec<u8>> {
        self.db.get(key).expect("opfs-btree get")
    }

    fn get(&mut self, key: &[u8]) -> Vec<u8> {
        self.get_opt(key).expect("opfs-btree key present")
    }

    fn range_checksum(&mut self, start: &[u8], end: &[u8], limit: usize) -> u64 {
        let rows = self
            .db
            .range(start, end, limit)
            .expect("opfs-btree range scan");
        let mut checksum = rows.len() as u64;
        for (_, value) in rows {
            checksum = checksum.wrapping_add(value.first().copied().unwrap_or(0) as u64);
        }
        checksum
    }

    fn finish_writes(&mut self) {
        self.db.checkpoint().expect("opfs-btree checkpoint");
    }
}

struct BfTreeEngine {
    tree: BfTree,
    read_buffer: Vec<u8>,
    scan_buffer: Vec<u8>,
}

impl BfTreeEngine {
    fn open(path: &Path, max_value_size: usize) -> Self {
        let mut config = BfConfig::new(path.join("bftree.index"), BF_TREE_CACHE_BYTES);
        config.cb_min_record_size(4);
        // Keep cb_max_record_size within bf-tree leaf constraints:
        // cb_max_record_size + key_len must fit in leaf payload.
        let target_record = max_value_size + 64;
        let mut leaf_page_size = 16 * 1024;
        while leaf_page_size < target_record * 2 {
            leaf_page_size *= 2;
        }
        let max_record_size = target_record.min((leaf_page_size / 2).saturating_sub(128));
        config.leaf_page_size(leaf_page_size);
        config.cb_max_record_size(max_record_size);

        let tree = BfTree::new_from_snapshot(config, None).expect("open bf-tree");
        Self {
            tree,
            read_buffer: vec![0u8; max_value_size.saturating_add(1024)],
            scan_buffer: vec![0u8; max_value_size.saturating_add(1024)],
        }
    }
}

impl Engine for BfTreeEngine {
    fn put(&mut self, key: &[u8], value: &[u8]) {
        let result = self.tree.insert(key, value);
        assert!(
            matches!(result, LeafInsertResult::Success),
            "bf-tree insert failed: {:?}",
            result
        );
    }

    fn delete(&mut self, key: &[u8]) {
        self.tree.delete(key);
    }

    fn get_opt(&mut self, key: &[u8]) -> Option<Vec<u8>> {
        match self.tree.read(key, &mut self.read_buffer) {
            LeafReadResult::Found(len) => Some(self.read_buffer[..(len as usize)].to_vec()),
            LeafReadResult::Deleted | LeafReadResult::NotFound => None,
            LeafReadResult::InvalidKey => panic!("bf-tree invalid key"),
        }
    }

    fn get(&mut self, key: &[u8]) -> Vec<u8> {
        self.get_opt(key).expect("bf-tree key present")
    }

    fn range_checksum(&mut self, start: &[u8], end: &[u8], limit: usize) -> u64 {
        let mut iter = self
            .tree
            .scan_with_end_key(start, end, ScanReturnField::Value)
            .expect("bf-tree range scan");
        let mut seen = 0usize;
        let mut checksum = 0u64;

        while seen < limit {
            match iter.next(&mut self.scan_buffer) {
                Some((_key_len, value_len)) => {
                    if value_len > 0 {
                        checksum = checksum.wrapping_add(self.scan_buffer[0] as u64);
                    }
                    checksum = checksum.wrapping_add(value_len as u64);
                    seen += 1;
                }
                None => break,
            }
        }

        checksum.wrapping_add(seen as u64)
    }

    fn finish_writes(&mut self) {
        self.tree.snapshot();
    }
}

struct RocksDbEngine {
    db: rocksdb::DB,
    write_options: WriteOptions,
}

impl RocksDbEngine {
    fn open(path: &Path) -> Self {
        let mut options = RocksOptions::default();
        options.create_if_missing(true);
        options.set_use_fsync(false);

        let mut write_options = WriteOptions::default();
        write_options.set_sync(false);
        write_options.disable_wal(true);

        let db_path = path.join("rocksdb");
        let db = rocksdb::DB::open(&options, db_path).expect("open rocksdb");
        Self { db, write_options }
    }
}

impl Engine for RocksDbEngine {
    fn put(&mut self, key: &[u8], value: &[u8]) {
        self.db
            .put_opt(key, value, &self.write_options)
            .expect("rocksdb put");
    }

    fn delete(&mut self, key: &[u8]) {
        self.db
            .delete_opt(key, &self.write_options)
            .expect("rocksdb delete");
    }

    fn get_opt(&mut self, key: &[u8]) -> Option<Vec<u8>> {
        self.db
            .get_pinned(key)
            .expect("rocksdb get")
            .map(|v| v.as_ref().to_vec())
    }

    fn get(&mut self, key: &[u8]) -> Vec<u8> {
        self.get_opt(key).expect("rocksdb key present")
    }

    fn range_checksum(&mut self, start: &[u8], end: &[u8], limit: usize) -> u64 {
        let mut seen = 0usize;
        let mut checksum = 0u64;
        let mode = IteratorMode::From(start, Direction::Forward);

        for item in self.db.iterator(mode) {
            let (key, value) = item.expect("rocksdb range item");
            if key.as_ref() >= end {
                break;
            }
            checksum = checksum.wrapping_add(value.first().copied().unwrap_or(0) as u64);
            seen += 1;
            if seen == limit {
                break;
            }
        }

        checksum.wrapping_add(seen as u64)
    }

    fn finish_writes(&mut self) {
        self.db.flush().expect("rocksdb flush");
    }
}

struct FjallEngine {
    database: FjallDatabase,
    keyspace: FjallKeyspace,
}

impl FjallEngine {
    fn open(path: &Path) -> Self {
        let database = FjallDatabase::builder(path.join("fjall"))
            .open()
            .expect("open fjall database");
        let keyspace = database
            .keyspace("bench", KeyspaceCreateOptions::default)
            .expect("open fjall keyspace");
        Self { database, keyspace }
    }
}

impl Engine for FjallEngine {
    fn put(&mut self, key: &[u8], value: &[u8]) {
        self.keyspace.insert(key, value).expect("fjall insert");
    }

    fn delete(&mut self, key: &[u8]) {
        self.keyspace.remove(key).expect("fjall remove");
    }

    fn get_opt(&mut self, key: &[u8]) -> Option<Vec<u8>> {
        self.keyspace
            .get(key)
            .expect("fjall get")
            .map(|v| v.to_vec())
    }

    fn get(&mut self, key: &[u8]) -> Vec<u8> {
        self.get_opt(key).expect("fjall key present")
    }

    fn range_checksum(&mut self, start: &[u8], end: &[u8], limit: usize) -> u64 {
        let mut seen = 0usize;
        let mut checksum = 0u64;

        for item in self.keyspace.range(start.to_vec()..end.to_vec()) {
            let value = item.value().expect("fjall range value");
            checksum = checksum.wrapping_add(value.first().copied().unwrap_or(0) as u64);
            seen += 1;
            if seen == limit {
                break;
            }
        }

        checksum.wrapping_add(seen as u64)
    }

    fn finish_writes(&mut self) {
        self.database
            .persist(PersistMode::SyncData)
            .expect("fjall persist");
    }
}

struct SurrealKvEngine {
    tree: SurrealTree,
    runtime: TokioRuntime,
    write_txn: Option<SurrealTransaction>,
    read_txn: Option<SurrealTransaction>,
}

impl SurrealKvEngine {
    fn open(path: &Path) -> Self {
        let runtime = TokioRuntimeBuilder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("build surrealkv tokio runtime");
        let tree = {
            let _guard = runtime.enter();
            SurrealTreeBuilder::new()
                .with_path(path.join("surrealkv"))
                .with_level_count(4)
                .with_max_memtable_size(256 * 1024 * 1024)
                .without_compression()
                .build()
                .expect("open surrealkv")
        };
        Self {
            tree,
            runtime,
            write_txn: None,
            read_txn: None,
        }
    }

    fn ensure_write_txn(&mut self) -> &mut SurrealTransaction {
        if self.write_txn.is_none() {
            let txn = {
                let _guard = self.runtime.enter();
                self.tree
                    .begin()
                    .expect("begin surrealkv write txn")
                    .with_durability(SurrealDurability::Eventual)
            };
            self.write_txn = Some(txn);
        }
        self.write_txn.as_mut().expect("surrealkv write txn")
    }

    fn ensure_read_txn(&mut self) -> &mut SurrealTransaction {
        if self.read_txn.is_none() {
            let txn = {
                let _guard = self.runtime.enter();
                self.tree
                    .begin_with_mode(SurrealMode::ReadOnly)
                    .expect("begin surrealkv read txn")
            };
            self.read_txn = Some(txn);
        }
        self.read_txn.as_mut().expect("surrealkv read txn")
    }
}

impl Engine for SurrealKvEngine {
    fn put(&mut self, key: &[u8], value: &[u8]) {
        self.read_txn = None;
        self.ensure_write_txn()
            .set(key, value)
            .expect("surrealkv set");
    }

    fn delete(&mut self, key: &[u8]) {
        self.read_txn = None;
        self.ensure_write_txn()
            .delete(key)
            .expect("surrealkv delete");
    }

    fn get_opt(&mut self, key: &[u8]) -> Option<Vec<u8>> {
        if self.write_txn.is_some() {
            self.finish_writes();
        }
        self.ensure_read_txn().get(key).expect("surrealkv get")
    }

    fn get(&mut self, key: &[u8]) -> Vec<u8> {
        self.get_opt(key).expect("surrealkv key present")
    }

    fn range_checksum(&mut self, start: &[u8], end: &[u8], limit: usize) -> u64 {
        if self.write_txn.is_some() {
            self.finish_writes();
        }

        let txn = self.ensure_read_txn();
        let mut iter = txn.range(start, end).expect("surrealkv range");
        let mut has_more = iter.seek_first().expect("surrealkv seek_first");

        let mut seen = 0usize;
        let mut checksum = 0u64;
        while has_more && iter.valid() && seen < limit {
            let value = iter.value().expect("surrealkv iter value");
            checksum = checksum.wrapping_add(value.first().copied().unwrap_or(0) as u64);
            seen += 1;
            has_more = iter.next().expect("surrealkv iter next");
        }

        checksum.wrapping_add(seen as u64)
    }

    fn finish_writes(&mut self) {
        self.read_txn = None;
        if let Some(mut txn) = self.write_txn.take() {
            self.runtime
                .block_on(async { txn.commit().await })
                .expect("surrealkv commit");
        }
    }
}

fn key(i: usize) -> Vec<u8> {
    format!("k{i:08}").into_bytes()
}

fn value(size: usize, seed: u8) -> Vec<u8> {
    let mut out = vec![0u8; size];
    for (i, byte) in out.iter_mut().enumerate() {
        *byte = seed.wrapping_add((i % 251) as u8);
    }
    out
}

fn shuffled_indices(n: usize) -> Vec<usize> {
    let mut out: Vec<usize> = (0..n).collect();
    let mut state: u64 = 0x9E3779B97F4A7C15;
    for i in (1..n).rev() {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
        let j = (state as usize) % (i + 1);
        out.swap(i, j);
    }
    out
}

fn random_starts(n: usize, max_start: usize) -> Vec<usize> {
    let mut out = Vec::with_capacity(n);
    let mut state: u64 = 0xD1B54A32D192ED03;
    for _ in 0..n {
        state = state.wrapping_mul(6364136223846793005).wrapping_add(1);
        out.push((state as usize) % max_start.max(1));
    }
    out
}

fn engine_factories(
    max_value_size: usize,
) -> Vec<(&'static str, Box<dyn Fn(&Path) -> Box<dyn Engine>>)> {
    let mut out: Vec<(&'static str, Box<dyn Fn(&Path) -> Box<dyn Engine>>)> = Vec::new();
    if engine_enabled("opfs_btree") {
        out.push((
            "opfs_btree",
            Box::new(|path| Box::new(OpfsBTreeEngine::open(path))),
        ));
    }
    if engine_enabled("bf_tree") && max_value_size <= BF_TREE_MAX_VALUE_SIZE {
        out.push((
            "bf_tree",
            Box::new(move |path| Box::new(BfTreeEngine::open(path, max_value_size))),
        ));
    }
    if engine_enabled("rocksdb") {
        out.push((
            "rocksdb",
            Box::new(|path| Box::new(RocksDbEngine::open(path))),
        ));
    }
    if engine_enabled("surrealkv") {
        out.push((
            "surrealkv",
            Box::new(|path| Box::new(SurrealKvEngine::open(path))),
        ));
    }
    if engine_enabled("fjall") {
        out.push(("fjall", Box::new(|path| Box::new(FjallEngine::open(path)))));
    }
    out
}

fn cold_read_engine_factories(
    max_value_size: usize,
) -> Vec<(&'static str, Box<dyn Fn(&Path) -> Box<dyn Engine>>)> {
    let mut out: Vec<(&'static str, Box<dyn Fn(&Path) -> Box<dyn Engine>>)> = Vec::new();
    if engine_enabled("opfs_btree") {
        out.push((
            "opfs_btree",
            Box::new(|path| Box::new(OpfsBTreeEngine::open(path))),
        ));
    }
    if engine_enabled("bf_tree") && max_value_size <= BF_TREE_MAX_VALUE_SIZE {
        out.push((
            "bf_tree",
            Box::new(move |path| Box::new(BfTreeEngine::open(path, max_value_size))),
        ));
    }
    if engine_enabled("rocksdb") {
        out.push((
            "rocksdb",
            Box::new(|path| Box::new(RocksDbEngine::open(path))),
        ));
    }
    if engine_enabled("surrealkv") {
        out.push((
            "surrealkv",
            Box::new(|path| Box::new(SurrealKvEngine::open(path))),
        ));
    }
    if engine_enabled("fjall") {
        out.push(("fjall", Box::new(|path| Box::new(FjallEngine::open(path)))));
    }
    out
}

fn bench_seq_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("compare_native_seq_write");
    configure_group(&mut group);
    let key_count = key_count();
    let value_sizes = value_sizes();

    for value_size in value_sizes {
        let factories = engine_factories(value_size);
        group.throughput(Throughput::Elements(key_count as u64));
        for (engine_name, factory) in &factories {
            group.bench_with_input(
                BenchmarkId::new(*engine_name, format!("value_{value_size}")),
                &value_size,
                |b, &value_size| {
                    b.iter_batched(
                        || {
                            let dir = tempfile::tempdir().expect("tempdir");
                            let engine = factory(dir.path());
                            (dir, engine)
                        },
                        |(_dir, mut engine): (TempDir, Box<dyn Engine>)| {
                            for i in 0..key_count {
                                let k = key(i);
                                let v = value(value_size, (i % 251) as u8);
                                engine.put(&k, &v);
                            }
                            engine.finish_writes();
                        },
                        BatchSize::LargeInput,
                    )
                },
            );
        }
    }

    group.finish();
}

fn bench_random_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("compare_native_random_write");
    configure_group(&mut group);
    let key_count = key_count();
    let value_sizes = value_sizes();
    let order = shuffled_indices(key_count);

    for value_size in value_sizes {
        let factories = engine_factories(value_size);
        group.throughput(Throughput::Elements(key_count as u64));
        for (engine_name, factory) in &factories {
            group.bench_with_input(
                BenchmarkId::new(*engine_name, format!("value_{value_size}")),
                &value_size,
                |b, &value_size| {
                    b.iter_batched(
                        || {
                            let dir = tempfile::tempdir().expect("tempdir");
                            let engine = factory(dir.path());
                            (dir, engine)
                        },
                        |(_dir, mut engine): (TempDir, Box<dyn Engine>)| {
                            for &i in &order {
                                let k = key(i);
                                let v = value(value_size, (i % 251) as u8);
                                engine.put(&k, &v);
                            }
                            engine.finish_writes();
                        },
                        BatchSize::LargeInput,
                    )
                },
            );
        }
    }

    group.finish();
}

fn bench_seq_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("compare_native_seq_read");
    configure_group(&mut group);
    let key_count = key_count();
    let value_sizes = value_sizes();

    for value_size in value_sizes {
        let factories = engine_factories(value_size);
        group.throughput(Throughput::Elements(key_count as u64));
        for (engine_name, factory) in &factories {
            group.bench_with_input(
                BenchmarkId::new(*engine_name, format!("value_{value_size}")),
                &value_size,
                |b, &value_size| {
                    b.iter_batched(
                        || {
                            let dir = tempfile::tempdir().expect("tempdir");
                            let mut engine = factory(dir.path());
                            for i in 0..key_count {
                                let k = key(i);
                                let v = value(value_size, (i % 251) as u8);
                                engine.put(&k, &v);
                            }
                            engine.finish_writes();
                            (dir, engine)
                        },
                        |(_dir, mut engine): (TempDir, Box<dyn Engine>)| {
                            let mut checksum: u64 = 0;
                            for i in 0..key_count {
                                let k = key(i);
                                let v = engine.get(&k);
                                checksum = checksum.wrapping_add(v[0] as u64);
                            }
                            black_box(checksum);
                        },
                        BatchSize::LargeInput,
                    )
                },
            );
        }
    }

    group.finish();
}

fn bench_random_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("compare_native_random_read");
    configure_group(&mut group);
    let key_count = key_count();
    let value_sizes = value_sizes();
    let order = shuffled_indices(key_count);

    for value_size in value_sizes {
        let factories = engine_factories(value_size);
        group.throughput(Throughput::Elements(key_count as u64));
        for (engine_name, factory) in &factories {
            group.bench_with_input(
                BenchmarkId::new(*engine_name, format!("value_{value_size}")),
                &value_size,
                |b, &value_size| {
                    b.iter_batched(
                        || {
                            let dir = tempfile::tempdir().expect("tempdir");
                            let mut engine = factory(dir.path());
                            for i in 0..key_count {
                                let k = key(i);
                                let v = value(value_size, (i % 251) as u8);
                                engine.put(&k, &v);
                            }
                            engine.finish_writes();
                            (dir, engine)
                        },
                        |(_dir, mut engine): (TempDir, Box<dyn Engine>)| {
                            let mut checksum: u64 = 0;
                            for &i in &order {
                                let k = key(i);
                                let v = engine.get(&k);
                                checksum = checksum.wrapping_add(v[0] as u64);
                            }
                            black_box(checksum);
                        },
                        BatchSize::LargeInput,
                    )
                },
            );
        }
    }

    group.finish();
}

fn bench_mixed_scenario(c: &mut Criterion, scenario: MixedScenario) {
    let mut group = c.benchmark_group(format!("compare_native_{}", scenario.name));
    configure_group(&mut group);
    let op_count = key_count();
    let value_sizes = value_sizes();

    for value_size in value_sizes {
        let factories = engine_factories(value_size);
        group.throughput(Throughput::Elements(op_count as u64));
        for (engine_name, factory) in &factories {
            group.bench_with_input(
                BenchmarkId::new(*engine_name, format!("value_{value_size}")),
                &value_size,
                |b, &value_size| {
                    b.iter_batched(
                        || {
                            let dir = tempfile::tempdir().expect("tempdir");
                            let mut engine = factory(dir.path());
                            let initial_key_space = op_count.max(1);
                            for i in 0..initial_key_space {
                                let k = key(i);
                                let v = value(value_size, (i % 251) as u8);
                                engine.put(&k, &v);
                            }
                            engine.finish_writes();
                            (dir, engine)
                        },
                        |(_dir, mut engine): (TempDir, Box<dyn Engine>)| {
                            let mut rng =
                                DeterministicRng::new(derive_seed(scenario.name, value_size));
                            let mut key_space = op_count.max(1);
                            let mut checksum = 0u64;

                            for step in 0..op_count {
                                let op = choose_operation(scenario, rng.next_u8() % 100);
                                match op {
                                    OpChoice::Read => {
                                        let idx = rng.next_usize(key_space.max(1));
                                        let k = key(idx);
                                        if let Some(v) = engine.get_opt(&k) {
                                            checksum = checksum.wrapping_add(v[0] as u64);
                                        } else {
                                            checksum = checksum.wrapping_add(1);
                                        }
                                    }
                                    OpChoice::Write => {
                                        let update = (rng.next_u8() % 100) < scenario.update_pct;
                                        let idx = if update || key_space == 0 {
                                            rng.next_usize(key_space.max(1))
                                        } else {
                                            let i = key_space;
                                            key_space += 1;
                                            i
                                        };
                                        let k = key(idx);
                                        let v = value(value_size, ((step + idx) % 251) as u8);
                                        checksum = checksum.wrapping_add(v[0] as u64);
                                        engine.put(&k, &v);
                                    }
                                    OpChoice::Delete => {
                                        let idx = rng.next_usize(key_space.max(1));
                                        let k = key(idx);
                                        engine.delete(&k);
                                        checksum = checksum.wrapping_add(idx as u64);
                                    }
                                }
                            }

                            engine.finish_writes();
                            black_box(checksum);
                        },
                        BatchSize::LargeInput,
                    )
                },
            );
        }
    }

    group.finish();
}

fn bench_mixed_random_70r_30w(c: &mut Criterion) {
    bench_mixed_scenario(c, MIXED_SCENARIOS[0]);
}

fn bench_mixed_random_50r_50w_with_updates(c: &mut Criterion) {
    bench_mixed_scenario(c, MIXED_SCENARIOS[1]);
}

fn bench_mixed_random_60r_20w_20d(c: &mut Criterion) {
    bench_mixed_scenario(c, MIXED_SCENARIOS[2]);
}

fn bench_cold_seq_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("compare_native_cold_seq_read");
    configure_group(&mut group);
    let key_count = key_count();
    let value_sizes = value_sizes();

    for value_size in value_sizes {
        let factories = cold_read_engine_factories(value_size);
        group.throughput(Throughput::Elements(key_count as u64));
        for (engine_name, factory) in &factories {
            group.bench_with_input(
                BenchmarkId::new(*engine_name, format!("value_{value_size}")),
                &value_size,
                |b, &value_size| {
                    b.iter_batched(
                        || {
                            let dir = tempfile::tempdir().expect("tempdir");
                            let mut engine = factory(dir.path());
                            for i in 0..key_count {
                                let k = key(i);
                                let v = value(value_size, (i % 251) as u8);
                                engine.put(&k, &v);
                            }
                            engine.finish_writes();
                            drop(engine);
                            dir
                        },
                        |dir: TempDir| {
                            let mut engine = factory(dir.path());
                            let mut checksum: u64 = 0;
                            for i in 0..key_count {
                                let k = key(i);
                                let v = engine.get(&k);
                                checksum = checksum.wrapping_add(v[0] as u64);
                            }
                            black_box(checksum);
                        },
                        BatchSize::LargeInput,
                    )
                },
            );
        }
    }

    group.finish();
}

fn bench_cold_random_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("compare_native_cold_random_read");
    configure_group(&mut group);
    let key_count = key_count();
    let value_sizes = value_sizes();
    let order = shuffled_indices(key_count);

    for value_size in value_sizes {
        let factories = cold_read_engine_factories(value_size);
        group.throughput(Throughput::Elements(key_count as u64));
        for (engine_name, factory) in &factories {
            group.bench_with_input(
                BenchmarkId::new(*engine_name, format!("value_{value_size}")),
                &value_size,
                |b, &value_size| {
                    b.iter_batched(
                        || {
                            let dir = tempfile::tempdir().expect("tempdir");
                            let mut engine = factory(dir.path());
                            for i in 0..key_count {
                                let k = key(i);
                                let v = value(value_size, (i % 251) as u8);
                                engine.put(&k, &v);
                            }
                            engine.finish_writes();
                            drop(engine);
                            dir
                        },
                        |dir: TempDir| {
                            let mut engine = factory(dir.path());
                            let mut checksum: u64 = 0;
                            for &i in &order {
                                let k = key(i);
                                let v = engine.get(&k);
                                checksum = checksum.wrapping_add(v[0] as u64);
                            }
                            black_box(checksum);
                        },
                        BatchSize::LargeInput,
                    )
                },
            );
        }
    }

    group.finish();
}

fn bench_range_seq_window(c: &mut Criterion) {
    let mut group = c.benchmark_group("compare_native_range_seq_window");
    configure_group(&mut group);
    let key_count = key_count();
    let value_sizes = value_sizes();
    let query_count = range_query_count();
    let max_start = key_count.saturating_sub(RANGE_WINDOW_KEYS + 1).max(1);

    for value_size in value_sizes {
        let factories = engine_factories(value_size);
        group.throughput(Throughput::Elements(query_count as u64));
        for (engine_name, factory) in &factories {
            group.bench_with_input(
                BenchmarkId::new(*engine_name, format!("value_{value_size}")),
                &value_size,
                |b, &value_size| {
                    b.iter_batched(
                        || {
                            let dir = tempfile::tempdir().expect("tempdir");
                            let mut engine = factory(dir.path());
                            for i in 0..key_count {
                                let k = key(i);
                                let v = value(value_size, (i % 251) as u8);
                                engine.put(&k, &v);
                            }
                            engine.finish_writes();
                            (dir, engine)
                        },
                        |(_dir, mut engine): (TempDir, Box<dyn Engine>)| {
                            let mut checksum = 0u64;
                            for i in 0..query_count {
                                let start_idx = (i.saturating_mul(7)) % max_start;
                                let end_idx = start_idx + RANGE_WINDOW_KEYS;
                                checksum = checksum.wrapping_add(engine.range_checksum(
                                    &key(start_idx),
                                    &key(end_idx),
                                    RANGE_RESULT_LIMIT,
                                ));
                            }
                            black_box(checksum);
                        },
                        BatchSize::LargeInput,
                    )
                },
            );
        }
    }

    group.finish();
}

fn bench_range_random_window(c: &mut Criterion) {
    let mut group = c.benchmark_group("compare_native_range_random_window");
    configure_group(&mut group);
    let key_count = key_count();
    let value_sizes = value_sizes();
    let query_count = range_query_count();
    let max_start = key_count.saturating_sub(RANGE_WINDOW_KEYS + 1).max(1);
    let starts = random_starts(query_count, max_start);

    for value_size in value_sizes {
        let factories = engine_factories(value_size);
        group.throughput(Throughput::Elements(query_count as u64));
        for (engine_name, factory) in &factories {
            group.bench_with_input(
                BenchmarkId::new(*engine_name, format!("value_{value_size}")),
                &value_size,
                |b, &value_size| {
                    b.iter_batched(
                        || {
                            let dir = tempfile::tempdir().expect("tempdir");
                            let mut engine = factory(dir.path());
                            for i in 0..key_count {
                                let k = key(i);
                                let v = value(value_size, (i % 251) as u8);
                                engine.put(&k, &v);
                            }
                            engine.finish_writes();
                            (dir, engine)
                        },
                        |(_dir, mut engine): (TempDir, Box<dyn Engine>)| {
                            let mut checksum = 0u64;
                            for start_idx in &starts {
                                let end_idx = *start_idx + RANGE_WINDOW_KEYS;
                                checksum = checksum.wrapping_add(engine.range_checksum(
                                    &key(*start_idx),
                                    &key(end_idx),
                                    RANGE_RESULT_LIMIT,
                                ));
                            }
                            black_box(checksum);
                        },
                        BatchSize::LargeInput,
                    )
                },
            );
        }
    }

    group.finish();
}

criterion_group!(
    compare_native,
    bench_seq_write,
    bench_random_write,
    bench_seq_read,
    bench_random_read,
    bench_mixed_random_70r_30w,
    bench_mixed_random_50r_50w_with_updates,
    bench_mixed_random_60r_20w_20d,
    bench_cold_seq_read,
    bench_cold_random_read,
    bench_range_seq_window,
    bench_range_random_window
);
criterion_main!(compare_native);
