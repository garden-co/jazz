use std::collections::HashMap;
use std::future::Future;
use std::path::{Path, PathBuf};
use std::pin::pin;
use std::task::{Context, Poll, RawWaker, RawWakerVTable, Waker};
use std::time::{Duration, Instant};

use bench_core::{
    BenchEngine, Benchmark, EngineError, KvDataset, PhaseKind, benchmarks, decode_kv,
    phases::replay,
};
use criterion::{
    BenchmarkGroup, Criterion, Throughput, black_box, criterion_group, criterion_main,
    measurement::WallTime,
};
use redb::{
    Database, Durability, ReadOnlyTable, ReadableDatabase, ReadableTable, TableDefinition,
    WriteTransaction,
};
use rocksdb::{
    BlockBasedOptions, Cache, DB, DBCompressionType, Direction, IteratorMode, Options, ReadOptions,
    WriteBatch,
};
use tempfile::TempDir;

const CACHE_BYTES: usize = 32 * 1024 * 1024;
const SQLITE_CACHE_KIB: i64 = -(CACHE_BYTES as i64 / 1024);
const DATA_DIR: &str = concat!(env!("CARGO_MANIFEST_DIR"), "/../data");
const REDB_TABLE: TableDefinition<&[u8], &[u8]> = TableDefinition::new("kv");

trait NativeEngine: BenchEngine + Sized {
    const NAME: &'static str;

    fn open_fresh() -> Result<Self, EngineError>;
}

struct SqliteEngine {
    path: PathBuf,
    conn: Option<rusqlite::Connection>,
    _dir: TempDir,
}

impl SqliteEngine {
    fn conn(&self) -> Result<&rusqlite::Connection, EngineError> {
        self.conn
            .as_ref()
            .ok_or_else(|| EngineError::new("sqlite connection is closed"))
    }
}

impl NativeEngine for SqliteEngine {
    const NAME: &'static str = "rusqlite";

    fn open_fresh() -> Result<Self, EngineError> {
        let dir = tempfile::tempdir().map_err(eng)?;
        let path = dir.path().join("bench.sqlite");
        let conn = open_sqlite_conn(&path)?;
        Ok(Self {
            path,
            conn: Some(conn),
            _dir: dir,
        })
    }
}

impl BenchEngine for SqliteEngine {
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), EngineError> {
        self.conn()?
            .prepare_cached("INSERT OR REPLACE INTO kv(k, v) VALUES(?1, ?2)")
            .map_err(eng)?
            .execute(rusqlite::params![key, value])
            .map_err(eng)?;
        Ok(())
    }

    fn get(&mut self, key: &[u8]) -> Result<Option<u8>, EngineError> {
        let mut stmt = self
            .conn()?
            .prepare_cached("SELECT v FROM kv WHERE k = ?1")
            .map_err(eng)?;
        match stmt.query_row(rusqlite::params![key], |row| row.get::<_, Vec<u8>>(0)) {
            Ok(value) => Ok(value.first().copied()),
            Err(rusqlite::Error::QueryReturnedNoRows) => Ok(None),
            Err(error) => Err(eng(error)),
        }
    }

    fn delete(&mut self, key: &[u8]) -> Result<(), EngineError> {
        self.conn()?
            .prepare_cached("DELETE FROM kv WHERE k = ?1")
            .map_err(eng)?
            .execute(rusqlite::params![key])
            .map_err(eng)?;
        Ok(())
    }

    fn range(&mut self, lo: &[u8], hi: &[u8], limit: usize) -> Result<usize, EngineError> {
        let mut stmt = self
            .conn()?
            .prepare_cached("SELECT 1 FROM kv WHERE k >= ?1 AND k < ?2 ORDER BY k LIMIT ?3")
            .map_err(eng)?;
        let rows = stmt
            .query_map(rusqlite::params![lo, hi, limit as i64], |_| Ok(()))
            .map_err(eng)?
            .count();
        Ok(rows)
    }

    fn begin_phase(&mut self, _kind: PhaseKind) -> Result<(), EngineError> {
        self.conn()?.execute_batch("BEGIN").map_err(eng)
    }

    fn end_phase(&mut self, _kind: PhaseKind) -> Result<(), EngineError> {
        self.conn()?.execute_batch("COMMIT").map_err(eng)
    }

    async fn reopen(&mut self) -> Result<(), EngineError> {
        self.conn = None;
        self.conn = Some(open_sqlite_conn(&self.path)?);
        Ok(())
    }
}

struct RocksDbEngine {
    path: PathBuf,
    db: Option<DB>,
    pending: Option<PendingBatch>,
    _dir: TempDir,
}

struct PendingBatch {
    batch: WriteBatch,
    overlay: HashMap<Vec<u8>, Option<Vec<u8>>>,
}

impl RocksDbEngine {
    fn db(&self) -> Result<&DB, EngineError> {
        self.db
            .as_ref()
            .ok_or_else(|| EngineError::new("rocksdb is closed"))
    }
}

impl NativeEngine for RocksDbEngine {
    const NAME: &'static str = "rocksdb";

    fn open_fresh() -> Result<Self, EngineError> {
        let dir = tempfile::tempdir().map_err(eng)?;
        let path = dir.path().join("bench.rocksdb");
        let db = open_rocksdb(&path)?;
        Ok(Self {
            path,
            db: Some(db),
            pending: None,
            _dir: dir,
        })
    }
}

impl BenchEngine for RocksDbEngine {
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), EngineError> {
        if let Some(pending) = self.pending.as_mut() {
            pending.batch.put(key, value);
            pending.overlay.insert(key.to_vec(), Some(value.to_vec()));
        } else {
            self.db()?.put(key, value).map_err(eng)?;
        }
        Ok(())
    }

    fn get(&mut self, key: &[u8]) -> Result<Option<u8>, EngineError> {
        if let Some(pending) = &self.pending {
            if let Some(value) = pending.overlay.get(key) {
                return Ok(value.as_ref().and_then(|bytes| bytes.first().copied()));
            }
        }
        Ok(self
            .db()?
            .get(key)
            .map_err(eng)?
            .and_then(|value| value.first().copied()))
    }

    fn delete(&mut self, key: &[u8]) -> Result<(), EngineError> {
        if let Some(pending) = self.pending.as_mut() {
            pending.batch.delete(key);
            pending.overlay.insert(key.to_vec(), None);
        } else {
            self.db()?.delete(key).map_err(eng)?;
        }
        Ok(())
    }

    fn range(&mut self, lo: &[u8], hi: &[u8], limit: usize) -> Result<usize, EngineError> {
        if limit == 0 {
            return Ok(0);
        }

        let mut read_options = ReadOptions::default();
        read_options.set_iterate_upper_bound(hi.to_vec());
        let iter = self
            .db()?
            .iterator_opt(IteratorMode::From(lo, Direction::Forward), read_options);

        let mut rows = 0usize;
        for item in iter {
            item.map_err(eng)?;
            rows += 1;
            if rows == limit {
                break;
            }
        }
        Ok(rows)
    }

    fn begin_phase(&mut self, kind: PhaseKind) -> Result<(), EngineError> {
        if kind.is_write() {
            if self.pending.is_some() {
                return Err(EngineError::new("rocksdb write batch is already open"));
            }
            self.pending = Some(PendingBatch {
                batch: WriteBatch::default(),
                overlay: HashMap::new(),
            });
        }
        Ok(())
    }

    fn end_phase(&mut self, kind: PhaseKind) -> Result<(), EngineError> {
        if kind.is_write() {
            let pending = self
                .pending
                .take()
                .ok_or_else(|| EngineError::new("rocksdb write batch is not open"))?;
            self.db()?.write(&pending.batch).map_err(eng)?;
        }
        Ok(())
    }

    async fn reopen(&mut self) -> Result<(), EngineError> {
        self.pending = None;
        self.db = None;
        self.db = Some(open_rocksdb(&self.path)?);
        Ok(())
    }
}

struct RedbEngine {
    path: PathBuf,
    db: Option<Database>,
    write_txn: Option<WriteTransaction>,
    read_table: Option<ReadOnlyTable<&'static [u8], &'static [u8]>>,
    _dir: TempDir,
}

impl RedbEngine {
    fn db(&self) -> Result<&Database, EngineError> {
        self.db
            .as_ref()
            .ok_or_else(|| EngineError::new("redb is closed"))
    }

    fn active_write_txn(&self) -> Result<&WriteTransaction, EngineError> {
        self.write_txn
            .as_ref()
            .ok_or_else(|| EngineError::new("redb write transaction is not open"))
    }
}

impl NativeEngine for RedbEngine {
    const NAME: &'static str = "redb";

    fn open_fresh() -> Result<Self, EngineError> {
        let dir = tempfile::tempdir().map_err(eng)?;
        let path = dir.path().join("bench.redb");
        let db = Database::create(&path).map_err(eng)?;
        Ok(Self {
            path,
            db: Some(db),
            write_txn: None,
            read_table: None,
            _dir: dir,
        })
    }
}

impl BenchEngine for RedbEngine {
    fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), EngineError> {
        let mut table = self
            .active_write_txn()?
            .open_table(REDB_TABLE)
            .map_err(eng)?;
        table.insert(key, value).map_err(eng)?;
        Ok(())
    }

    fn get(&mut self, key: &[u8]) -> Result<Option<u8>, EngineError> {
        if let Some(table) = &self.read_table {
            return Ok(table
                .get(key)
                .map_err(eng)?
                .and_then(|value| value.value().first().copied()));
        }

        let table = self
            .active_write_txn()?
            .open_table(REDB_TABLE)
            .map_err(eng)?;
        Ok(table
            .get(key)
            .map_err(eng)?
            .and_then(|value| value.value().first().copied()))
    }

    fn delete(&mut self, key: &[u8]) -> Result<(), EngineError> {
        let mut table = self
            .active_write_txn()?
            .open_table(REDB_TABLE)
            .map_err(eng)?;
        table.remove(key).map_err(eng)?;
        Ok(())
    }

    fn range(&mut self, lo: &[u8], hi: &[u8], limit: usize) -> Result<usize, EngineError> {
        if limit == 0 {
            return Ok(0);
        }

        let count_rows = |table: &ReadOnlyTable<&'static [u8], &'static [u8]>| {
            let mut rows = 0usize;
            for item in table.range(lo..hi).map_err(eng)? {
                item.map_err(eng)?;
                rows += 1;
                if rows == limit {
                    break;
                }
            }
            Ok(rows)
        };

        if let Some(table) = &self.read_table {
            return count_rows(table);
        }

        let table = self
            .active_write_txn()?
            .open_table(REDB_TABLE)
            .map_err(eng)?;
        let mut rows = 0usize;
        for item in table.range(lo..hi).map_err(eng)? {
            item.map_err(eng)?;
            rows += 1;
            if rows == limit {
                break;
            }
        }
        Ok(rows)
    }

    fn begin_phase(&mut self, kind: PhaseKind) -> Result<(), EngineError> {
        if kind.is_write() {
            if self.write_txn.is_some() {
                return Err(EngineError::new("redb write transaction is already open"));
            }
            let mut txn = self.db()?.begin_write().map_err(eng)?;
            txn.set_durability(Durability::None).map_err(eng)?;
            self.write_txn = Some(txn);
        } else {
            if self.read_table.is_some() {
                return Err(EngineError::new("redb read table is already open"));
            }
            let txn = self.db()?.begin_read().map_err(eng)?;
            self.read_table = Some(txn.open_table(REDB_TABLE).map_err(eng)?);
        }
        Ok(())
    }

    fn end_phase(&mut self, kind: PhaseKind) -> Result<(), EngineError> {
        if kind.is_write() {
            let txn = self
                .write_txn
                .take()
                .ok_or_else(|| EngineError::new("redb write transaction is not open"))?;
            txn.commit().map_err(eng)?;
        } else {
            self.read_table = None;
        }
        Ok(())
    }

    async fn reopen(&mut self) -> Result<(), EngineError> {
        self.write_txn = None;
        self.read_table = None;
        self.db = None;
        self.db = Some(Database::open(&self.path).map_err(eng)?);
        Ok(())
    }
}

struct DatasetView<'a> {
    keys: Vec<&'a [u8]>,
    values: Vec<&'a [u8]>,
    record_count: u32,
}

impl<'a> DatasetView<'a> {
    fn new(dataset: &'a KvDataset) -> Self {
        Self {
            keys: dataset
                .records
                .iter()
                .map(|(key, _)| key.as_slice())
                .collect(),
            values: dataset
                .records
                .iter()
                .map(|(_, value)| value.as_slice())
                .collect(),
            record_count: dataset.records.len() as u32,
        }
    }
}

fn bench_native_storage(c: &mut Criterion) {
    for benchmark in benchmarks() {
        let dataset = load_dataset(&benchmark);
        let view = DatasetView::new(&dataset);

        for (phase_index, phase) in benchmark.phases.iter().enumerate() {
            let phase_name = phase.name();
            let mut group = c.benchmark_group(format!(
                "native_storage/{}/{}",
                benchmark.profile, phase_name
            ));
            group.throughput(Throughput::Elements(
                phase.op_count(view.record_count) as u64
            ));

            bench_engine::<SqliteEngine>(&mut group, &benchmark, &view, phase_index);
            bench_engine::<RocksDbEngine>(&mut group, &benchmark, &view, phase_index);
            bench_engine::<RedbEngine>(&mut group, &benchmark, &view, phase_index);

            group.finish();
        }
    }
}

fn bench_engine<E: NativeEngine>(
    group: &mut BenchmarkGroup<'_, WallTime>,
    benchmark: &Benchmark,
    view: &DatasetView<'_>,
    phase_index: usize,
) {
    group.bench_function(E::NAME, |bencher| {
        bencher.iter_custom(|iters| {
            time_phase::<E>(benchmark, view, phase_index, iters)
                .unwrap_or_else(|error| panic!("{} benchmark failed: {error}", E::NAME))
        });
    });
}

fn time_phase<E: NativeEngine>(
    benchmark: &Benchmark,
    view: &DatasetView<'_>,
    phase_index: usize,
    iters: u64,
) -> Result<Duration, EngineError> {
    let args = benchmark.phase_args(phase_index, view.record_count);
    let repeat_same_state = can_repeat_on_same_state(benchmark.phases[phase_index].kind());
    let mut total = Duration::ZERO;
    let mut checksum = 0u64;

    if repeat_same_state {
        let mut engine = E::open_fresh()?;
        prepare_engine(&mut engine, benchmark, view, phase_index)?;
        for _ in 0..iters {
            let started = Instant::now();
            checksum = checksum.wrapping_add(run_phase_with_args(
                &mut engine,
                benchmark,
                view,
                phase_index,
                &args,
            )?);
            total += started.elapsed();
        }
    } else {
        for _ in 0..iters {
            let mut engine = E::open_fresh()?;
            prepare_engine(&mut engine, benchmark, view, phase_index)?;
            let started = Instant::now();
            checksum = checksum.wrapping_add(run_phase_with_args(
                &mut engine,
                benchmark,
                view,
                phase_index,
                &args,
            )?);
            total += started.elapsed();
        }
    }

    black_box(checksum);
    Ok(total)
}

fn prepare_engine<E: BenchEngine>(
    engine: &mut E,
    benchmark: &Benchmark,
    view: &DatasetView<'_>,
    phase_index: usize,
) -> Result<(), EngineError> {
    for prior_index in 0..phase_index {
        let args = benchmark.phase_args(prior_index, view.record_count);
        run_phase_with_args(engine, benchmark, view, prior_index, &args)?;
    }
    Ok(())
}

fn run_phase_with_args<E: BenchEngine>(
    engine: &mut E,
    benchmark: &Benchmark,
    view: &DatasetView<'_>,
    phase_index: usize,
    args: &[u32],
) -> Result<u64, EngineError> {
    let phase = &benchmark.phases[phase_index];
    let kind = phase.kind();
    if kind.is_cold() {
        block_on(engine.reopen())?;
    }

    engine.begin_phase(kind)?;
    let (_, checksum) = replay(engine, phase, args, &view.keys, &view.values)?;
    engine.end_phase(kind)?;
    Ok(checksum)
}

fn can_repeat_on_same_state(kind: PhaseKind) -> bool {
    !matches!(kind, PhaseKind::Load | PhaseKind::Mixed)
}

fn load_dataset(benchmark: &Benchmark) -> KvDataset {
    let bytes = std::fs::read(Path::new(DATA_DIR).join(&benchmark.kv_fixture))
        .unwrap_or_else(|error| panic!("read fixture {}: {error}", benchmark.kv_fixture));
    let dataset = decode_kv(&bytes)
        .unwrap_or_else(|error| panic!("decode fixture {}: {error}", benchmark.kv_fixture));
    assert_eq!(dataset.profile, benchmark.profile);
    dataset
}

fn open_sqlite_conn(path: &Path) -> Result<rusqlite::Connection, EngineError> {
    let conn = rusqlite::Connection::open(path).map_err(eng)?;
    conn.execute_batch(&format!(
        "PRAGMA page_size = 16384;
         PRAGMA journal_mode = WAL;
         PRAGMA synchronous = NORMAL;
         PRAGMA cache_size = {SQLITE_CACHE_KIB};
         PRAGMA temp_store = MEMORY;
         PRAGMA busy_timeout = 5000;
         CREATE TABLE IF NOT EXISTS kv(
             k BLOB PRIMARY KEY,
             v BLOB NOT NULL
         ) WITHOUT ROWID;",
    ))
    .map_err(eng)?;
    Ok(conn)
}

fn open_rocksdb(path: &Path) -> Result<DB, EngineError> {
    let mut block_options = BlockBasedOptions::default();
    block_options.set_bloom_filter(10.0, false);
    let cache = Cache::new_lru_cache(CACHE_BYTES);
    block_options.set_block_cache(&cache);

    let mut options = Options::default();
    options.create_if_missing(true);
    options.set_block_based_table_factory(&block_options);
    options.set_compression_type(DBCompressionType::Lz4);
    options.set_bottommost_compression_type(DBCompressionType::Zstd);

    DB::open(&options, path).map_err(eng)
}

fn eng<E: ToString>(error: E) -> EngineError {
    EngineError::new(error.to_string())
}

fn block_on<F: Future>(future: F) -> F::Output {
    fn raw_waker() -> RawWaker {
        fn no_op(_: *const ()) {}
        fn clone(_: *const ()) -> RawWaker {
            raw_waker()
        }
        RawWaker::new(
            std::ptr::null(),
            &RawWakerVTable::new(clone, no_op, no_op, no_op),
        )
    }

    let waker = unsafe { Waker::from_raw(raw_waker()) };
    let mut cx = Context::from_waker(&waker);
    let mut future = pin!(future);
    loop {
        if let Poll::Ready(value) = future.as_mut().poll(&mut cx) {
            return value;
        }
    }
}

criterion_group! {
    name = benches;
    config = Criterion::default()
        .sample_size(10)
        .warm_up_time(Duration::from_millis(500))
        .measurement_time(Duration::from_secs(3));
    targets = bench_native_storage
}
criterion_main!(benches);
