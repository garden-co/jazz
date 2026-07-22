//! End-to-end Jazz ingestion / cold-load storage benchmark.
//!
//! Ingests the USDA PLANTS checklist (`data/plantlst.txt`, ~93k rows) through
//! the **public `jazz::db::Db` API** — real schema, real transactions — over a
//! storage adapter you cherry-pick on the command line, and measures the three
//! things that matter for storage selection:
//!
//! 1. **Write time** — wall-clock to insert every record (batched transactions)
//!    plus the flush/close cost to make it durable.
//! 2. **Write amplification** — physical on-disk bytes divided by the logical
//!    bytes ingested (both the raw CSV payload and Jazz's own encoded size).
//! 3. **Cold-load query latency** — close the DB, reopen it from disk with cold
//!    caches, and time a fixed set of queries reading from cold storage.
//!
//! The storage adapter is a runtime choice (`--storage rocksdb|btree|slatedb|memory`),
//! so the same Jazz workload is compared across every backend the engine ships.
//!
//! ```text
//! cargo run --release -p jazz-ingest-bench -- --storage rocksdb
//! cargo run --release -p jazz-ingest-bench -- --storage rocksdb,btree,slatedb --limit 20000
//! ```

use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

use jazz::db::{Db, DbConfig, DbIdentity};
use jazz::groove::records::Value;
use jazz::groove::schema::{ColumnSchema, ColumnType};
use jazz::groove::storage::{
    Durability, MemoryStorage, NativeBtreeStorage, OrderedKvStorage, ReopenableStorage,
    RocksDbStorage, SlateDbStorage,
};
use jazz::ids::{AuthorId, NodeUuid};
use jazz::query::{OrderDirection, Query, col, contains, eq, gte, in_list, lit, lt, ne};
use jazz::schema::{JazzSchema, TableSchema};

const TABLE: &str = "plants";
const COLUMNS: [&str; 5] = [
    "symbol",
    "synonym_symbol",
    "scientific_name",
    "common_name",
    "family",
];

// Query parameters shared by the Jazz and native layers so both run the exact
// same workload.
const SCI_TOKEN: &str = "Carex"; // substring searched in scientific_name
const FAMILY_SET: [&str; 3] = ["Poaceae", "Asteraceae", "Cyperaceae"];
const TOP_N: usize = 100;
/// Extra repeats after the cold+warm reads; `hot` is the best (min) of these,
/// i.e. the fully-warmed steady-state latency.
const HOT_ITERS: usize = 10;

/// When set, the DB is reopened (dropping the in-process block cache) before
/// each query, so every query's `cold` number is a true cold-cache read rather
/// than a hit on a cache that an earlier query warmed. Run-wide; set in `main`.
static COLD_PER_QUERY: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
fn cold_per_query() -> bool {
    COLD_PER_QUERY.load(std::sync::atomic::Ordering::Relaxed)
}

// ---------------------------------------------------------------------------
// Dataset
// ---------------------------------------------------------------------------

/// One CSV record. Column order matches [`COLUMNS`].
struct Plant {
    fields: [String; 5],
}

impl Plant {
    /// The logical payload size of this row: the sum of its field byte lengths.
    fn raw_bytes(&self) -> usize {
        self.fields.iter().map(String::len).sum()
    }

    /// The row as Jazz cells for `Db::insert` / `tx.insert`.
    fn cells(&self) -> BTreeMap<String, Value> {
        COLUMNS
            .iter()
            .zip(self.fields.iter())
            .map(|(name, value)| ((*name).to_owned(), Value::String(value.clone())))
            .collect()
    }
}

/// Parse one RFC-4180-ish line: comma-separated, double-quoted fields, `""` is
/// a literal quote, commas inside quotes are data. The USDA file quotes every
/// field and keeps one record per line, so line-based parsing is sufficient.
fn parse_csv_line(line: &str) -> Vec<String> {
    let mut fields = Vec::new();
    let mut cur = String::new();
    let mut in_quotes = false;
    let mut chars = line.chars().peekable();
    while let Some(c) = chars.next() {
        if in_quotes {
            if c == '"' {
                if chars.peek() == Some(&'"') {
                    cur.push('"');
                    chars.next();
                } else {
                    in_quotes = false;
                }
            } else {
                cur.push(c);
            }
        } else {
            match c {
                '"' => in_quotes = true,
                ',' => fields.push(std::mem::take(&mut cur)),
                _ => cur.push(c),
            }
        }
    }
    fields.push(cur);
    fields
}

fn load_dataset(path: &Path, limit: Option<usize>) -> Vec<Plant> {
    let text =
        fs::read_to_string(path).unwrap_or_else(|e| panic!("read dataset {}: {e}", path.display()));
    let mut plants = Vec::new();
    for line in text.lines().skip(1) {
        if line.is_empty() {
            continue;
        }
        let mut cols = parse_csv_line(line);
        cols.resize(5, String::new());
        plants.push(Plant {
            fields: [
                std::mem::take(&mut cols[0]),
                std::mem::take(&mut cols[1]),
                std::mem::take(&mut cols[2]),
                std::mem::take(&mut cols[3]),
                std::mem::take(&mut cols[4]),
            ],
        });
        if let Some(limit) = limit
            && plants.len() >= limit
        {
            break;
        }
    }
    plants
}

// ---------------------------------------------------------------------------
// Jazz plumbing
// ---------------------------------------------------------------------------

fn schema() -> JazzSchema {
    JazzSchema::new([TableSchema::new(
        TABLE,
        COLUMNS
            .iter()
            .map(|name| ColumnSchema::new(*name, ColumnType::String)),
    )])
}

fn open_db<S>(schema: &JazzSchema, storage: S) -> Db<S>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    block_on(Db::open(DbConfig::new(
        schema.clone(),
        storage,
        DbIdentity {
            node: NodeUuid::from_bytes([1u8; 16]),
            author: AuthorId::SYSTEM,
        },
    )))
    .expect("open db")
}

/// The query set exercised against cold storage. Each returns a labelled,
/// prepared-then-read closure so it can be timed against any open `Db`.
fn queries(sample: &Plant) -> Vec<(String, Query)> {
    let symbol = sample.fields[0].clone();
    let synonym = sample.fields[1].clone();
    vec![
        // Point lookup: the (symbol, synonym_symbol) pair is unique in the data.
        (
            "point_by_key".to_owned(),
            Query::from(TABLE)
                .filter(eq(col("symbol"), lit(Value::String(symbol))))
                .filter(eq(col("synonym_symbol"), lit(Value::String(synonym)))),
        ),
        // Range/prefix scan: all symbols in [AB, AC).
        (
            "prefix_scan_AB".to_owned(),
            Query::from(TABLE)
                .filter(gte(col("symbol"), lit(Value::String("AB".to_owned()))))
                .filter(lt(col("symbol"), lit(Value::String("AC".to_owned())))),
        ),
        // Filter by a non-key column.
        (
            "filter_family_Malvaceae".to_owned(),
            Query::from(TABLE).filter(eq(
                col("family"),
                lit(Value::String("Malvaceae".to_owned())),
            )),
        ),
        // Full-table scan.
        ("full_scan".to_owned(), Query::from(TABLE)),
        // Substring match on scientific_name.
        (
            "contains_scientific_Carex".to_owned(),
            Query::from(TABLE).filter(contains(
                col("scientific_name"),
                lit(Value::String(SCI_TOKEN.to_owned())),
            )),
        ),
        // Rows that have a common name (non-empty column).
        (
            "common_name_present".to_owned(),
            Query::from(TABLE).filter(ne(col("common_name"), lit(Value::String(String::new())))),
        ),
        // Membership in a set of families.
        (
            "family_in_set".to_owned(),
            Query::from(TABLE).filter(in_list(
                col("family"),
                FAMILY_SET
                    .iter()
                    .map(|f| lit(Value::String((*f).to_owned()))),
            )),
        ),
        // First N rows ordered by symbol.
        (
            "top_100_by_symbol".to_owned(),
            Query::from(TABLE)
                .order_by("symbol", OrderDirection::Asc)
                .limit(TOP_N),
        ),
    ]
}

/// Returns (cold_ms, warm_ms, hot_ms, rows): the first read (cold), the second
/// (warm), and the best of `HOT_ITERS` further reads (hot / steady-state).
fn run_query<S>(db: &Db<S>, query: &Query) -> (f64, f64, f64, usize)
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    let prepared = db.prepare_query(query).expect("prepare query");
    let read = || {
        let t = Instant::now();
        let rows = db.read(&prepared).expect("read query").len();
        (t.elapsed().as_secs_f64() * 1e3, rows)
    };
    let (cold, rows) = read();
    let (warm, _) = read();
    let mut hot = warm;
    for _ in 0..HOT_ITERS {
        hot = hot.min(read().0);
    }
    (cold, warm, hot, rows)
}

// ---------------------------------------------------------------------------
// The benchmark, generic over the storage adapter
// ---------------------------------------------------------------------------

struct QueryReport {
    name: String,
    rows: usize,
    cold_ms: Option<f64>,
    warm_ms: f64,
    hot_ms: f64,
}

struct Report {
    adapter: String,
    rows: usize,
    batch_size: usize,
    raw_input_bytes: u64,
    // `None` when the backend does not report per-column-family byte accounting
    // (only RocksDB and the in-memory store implement `approximate_class_bytes`).
    encoded_logical_bytes: Option<u64>,
    physical_bytes: Option<u64>,
    write_time: Duration,
    flush_close_time: Duration,
    cold_open_time: Option<Duration>,
    queries: Vec<QueryReport>,
}

/// `open` reopens storage at a fixed location every call, so the same bytes are
/// seen on the cold reopen. `location` is the path to `du` for physical size
/// (a directory for rocksdb/slatedb, a single file for btree); `None` for the
/// in-memory adapter, which has no on-disk footprint and no cold path.
fn benchmark<S>(
    adapter: &str,
    plants: &[Plant],
    batch_size: usize,
    schema: &JazzSchema,
    location: Option<&Path>,
    open: impl Fn() -> S,
) -> Report
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    let raw_input_bytes = plants.iter().map(|p| p.raw_bytes() as u64).sum();

    // --- Write phase: every record, in batched transactions. ---
    let db = open_db(schema, open());
    let t_write = Instant::now();
    for chunk in plants.chunks(batch_size) {
        db.transaction(|tx| {
            for plant in chunk {
                tx.insert(TABLE, plant.cells())?;
            }
            Ok(())
        })
        .expect("commit batch");
    }
    let write_time = t_write.elapsed();

    // Backends without per-CF byte accounting report 0; treat that as "unknown".
    let encoded_logical_bytes = match db.encoded_storage_bytes_for_test() {
        Ok(0) | Err(_) => None,
        Ok(bytes) => Some(bytes),
    };

    // --- Query phase. ---
    let query_set = queries(&plants[0]);
    let mut queries_out = Vec::new();
    let (flush_close_time, physical_bytes, cold_open_time);

    if let Some(location) = location {
        // Persistent: flush + close, measure on-disk size, reopen cold.
        let t_close = Instant::now();
        if let Err(e) = db.close() {
            // The slatedb prototype rejects the clean-close marker write; keep
            // going so its write/size numbers are still reported.
            eprintln!("  warning: {adapter} close() failed: {e}");
        }
        flush_close_time = t_close.elapsed();
        // Drop the write DB so its storage releases the on-disk lock before we
        // reopen the same path cold (RocksDB/btree hold an exclusive handle).
        drop(db);
        physical_bytes = Some(dir_size(location));

        let t_open = Instant::now();
        let cold_db = open_db(schema, open());
        cold_open_time = Some(t_open.elapsed());

        if cold_per_query() {
            // Reopen a fresh DB (cold block cache) for each query so its `cold`
            // number is a true cold read, not a hit on a cache an earlier query
            // warmed. Drop the shared handle first to release the storage lock;
            // each per-query handle is opened and dropped sequentially.
            if let Err(e) = cold_db.close() {
                eprintln!("  warning: {adapter} cold close() failed: {e}");
            }
            drop(cold_db);
            for (name, query) in &query_set {
                let db = open_db(schema, open());
                let (cold, warm, hot, rows) = run_query(&db, query);
                let _ = db.close();
                queries_out.push(QueryReport {
                    name: name.clone(),
                    rows,
                    cold_ms: Some(cold),
                    warm_ms: warm,
                    hot_ms: hot,
                });
            }
        } else {
            for (name, query) in &query_set {
                let (cold, warm, hot, rows) = run_query(&cold_db, query);
                queries_out.push(QueryReport {
                    name: name.clone(),
                    rows,
                    cold_ms: Some(cold),
                    warm_ms: warm,
                    hot_ms: hot,
                });
            }
            if let Err(e) = cold_db.close() {
                eprintln!("  warning: {adapter} cold close() failed: {e}");
            }
        }
    } else {
        // In-memory: no disk, no cold path — query the warm DB, then close.
        physical_bytes = None;
        cold_open_time = None;
        for (name, query) in &query_set {
            let (_, warm, hot, rows) = run_query(&db, query);
            queries_out.push(QueryReport {
                name: name.clone(),
                rows,
                cold_ms: None,
                warm_ms: warm,
                hot_ms: hot,
            });
        }
        let t_close = Instant::now();
        if let Err(e) = db.close() {
            eprintln!("  warning: {adapter} close() failed: {e}");
        }
        flush_close_time = t_close.elapsed();
    }

    Report {
        adapter: adapter.to_owned(),
        rows: plants.len(),
        batch_size,
        raw_input_bytes,
        encoded_logical_bytes,
        physical_bytes,
        write_time,
        flush_close_time,
        cold_open_time,
        queries: queries_out,
    }
}

/// Recursive on-disk size of a path (works for a directory or a single file).
fn dir_size(path: &Path) -> u64 {
    let Ok(md) = fs::symlink_metadata(path) else {
        return 0;
    };
    if md.is_dir() {
        let mut total = 0;
        if let Ok(entries) = fs::read_dir(path) {
            for entry in entries.flatten() {
                total += dir_size(&entry.path());
            }
        }
        total
    } else {
        md.len()
    }
}

fn column_family_refs(schema: &JazzSchema) -> Vec<String> {
    schema.column_families()
}

fn dispatch(adapter: &str, plants: &[Plant], batch_size: usize) -> Report {
    let schema = schema();
    let cfs = column_family_refs(&schema);
    let refs: Vec<&str> = cfs.iter().map(String::as_str).collect();
    let dir = tempfile::tempdir().expect("tempdir");

    match adapter {
        "memory" => {
            let refs = refs.clone();
            benchmark(adapter, plants, batch_size, &schema, None, move || {
                MemoryStorage::new(&refs)
            })
        }
        "rocksdb" => {
            let path = dir.path().to_path_buf();
            let refs = refs.clone();
            benchmark(
                adapter,
                plants,
                batch_size,
                &schema,
                Some(dir.path()),
                move || {
                    RocksDbStorage::open_with_durability(&path, &refs, Durability::WalNoSync)
                        .expect("open rocksdb")
                },
            )
        }
        "btree" => {
            let file: PathBuf = dir.path().join("btree.store");
            let file_for_size = file.clone();
            let refs = refs.clone();
            benchmark(
                adapter,
                plants,
                batch_size,
                &schema,
                Some(&file_for_size),
                move || NativeBtreeStorage::open(&file, &refs).expect("open btree"),
            )
        }
        "slatedb" => {
            let path = dir.path().to_path_buf();
            let refs = refs.clone();
            benchmark(
                adapter,
                plants,
                batch_size,
                &schema,
                Some(dir.path()),
                move || SlateDbStorage::open_bridged(path.clone(), &refs).expect("open slatedb"),
            )
        }
        other => {
            panic!("unknown storage adapter '{other}' (expected memory|rocksdb|btree|slatedb)")
        }
    }
}

// ---------------------------------------------------------------------------
// Direct storage-engine layer (`--raw`): no Jazz, no groove seam. Each engine
// is driven through its own native crate API, tuned for bulk ingest.
//
// A plant becomes one KV pair: the key is `symbol \0 synonym_symbol` (unique and
// ordered by symbol, so prefix/range scans work), the value is all five fields
// joined by 0x1F so the family filter can decode without a secondary index —
// the same "no index, filter is a scan" shape as the Jazz layer.
// ---------------------------------------------------------------------------

const FIELD_SEP: u8 = 0x1f;

fn plant_key(p: &Plant) -> Vec<u8> {
    let mut key = Vec::with_capacity(p.fields[0].len() + 1 + p.fields[1].len());
    key.extend_from_slice(p.fields[0].as_bytes());
    key.push(0);
    key.extend_from_slice(p.fields[1].as_bytes());
    key
}

fn plant_value(p: &Plant) -> Vec<u8> {
    p.fields
        .iter()
        .map(String::as_bytes)
        .collect::<Vec<_>>()
        .join(&[FIELD_SEP][..])
}

/// Field `idx` (0-based, matching [`COLUMNS`]) out of an encoded value.
fn value_field(value: &[u8], idx: usize) -> &[u8] {
    value.split(|b| *b == FIELD_SEP).nth(idx).unwrap_or(&[])
}

fn is_subslice(haystack: &[u8], needle: &[u8]) -> bool {
    !needle.is_empty() && haystack.windows(needle.len()).any(|w| w == needle)
}

// Native-layer predicates mirroring the Jazz-layer queries, applied to a row's
// encoded value. Kept as plain fns so both engines share them.
fn pred_family_malvaceae(value: &[u8]) -> bool {
    value_field(value, 4) == b"Malvaceae"
}
fn pred_contains_sci(value: &[u8]) -> bool {
    is_subslice(value_field(value, 2), SCI_TOKEN.as_bytes())
}
fn pred_common_present(value: &[u8]) -> bool {
    !value_field(value, 3).is_empty()
}
fn pred_family_in_set(value: &[u8]) -> bool {
    let family = value_field(value, 4);
    FAMILY_SET.iter().any(|f| f.as_bytes() == family)
}

fn timed(mut f: impl FnMut() -> usize) -> (f64, usize) {
    let t = Instant::now();
    let rows = f();
    (t.elapsed().as_secs_f64() * 1e3, rows)
}

// --- RocksDB, via the `rocksdb` crate directly. ---

fn rocks_options() -> rocksdb::Options {
    use rocksdb::{BlockBasedOptions, Cache, DBCompressionType, Options};
    let mut opts = Options::default();
    opts.create_if_missing(true);
    // Idiomatic bulk-ingest tuning: compression like groove's default profile,
    // a shared block cache, big write buffers, all cores for flush/compaction.
    opts.set_compression_type(DBCompressionType::Lz4);
    opts.set_bottommost_compression_type(DBCompressionType::Zstd);
    let cores = std::thread::available_parallelism().map_or(4, |n| n.get()) as i32;
    opts.increase_parallelism(cores);
    opts.set_write_buffer_size(64 << 20);
    opts.set_max_write_buffer_number(4);
    let mut block = BlockBasedOptions::default();
    block.set_block_cache(&Cache::new_lru_cache(128 << 20));
    opts.set_block_based_table_factory(&block);
    opts
}

fn run_rocksdb_raw(plants: &[Plant], batch_size: usize, dir: &Path) -> Report {
    use rocksdb::{DB, Direction, IteratorMode, ReadOptions, WriteBatch, WriteOptions};

    let raw_input_bytes = plants.iter().map(|p| p.raw_bytes() as u64).sum();
    let mut write_opts = WriteOptions::default();
    write_opts.set_sync(false); // WAL on, no per-commit fsync — matches Jazz's WalNoSync.

    let db = DB::open(&rocks_options(), dir).expect("open rocksdb");
    let t_write = Instant::now();
    for chunk in plants.chunks(batch_size) {
        let mut batch = WriteBatch::default();
        for p in chunk {
            batch.put(plant_key(p), plant_value(p));
        }
        db.write_opt(&batch, &write_opts).expect("write batch");
    }
    let write_time = t_write.elapsed();

    let t_close = Instant::now();
    db.flush().expect("flush rocksdb");
    let flush_close_time = t_close.elapsed();
    drop(db);
    let physical_bytes = Some(dir_size(dir));

    // Cold reopen.
    let t_open = Instant::now();
    let db = DB::open(&rocks_options(), dir).expect("reopen rocksdb");
    let cold_open_time = Some(t_open.elapsed());

    let key0 = plant_key(&plants[0]);
    // Each query as a fn over a `&DB`, so it can run against the shared handle
    // or a freshly-reopened (cold-cache) one.
    type RocksQuery = Box<dyn Fn(&DB) -> usize>;
    let specs: Vec<(&str, RocksQuery)> = vec![
        ("point_by_key", {
            let k = key0.clone();
            Box::new(move |db: &DB| db.get(&k).expect("get").is_some() as usize)
        }),
        (
            "prefix_scan_AB",
            Box::new(|db: &DB| {
                let mut ro = ReadOptions::default();
                ro.set_iterate_upper_bound(b"AC".to_vec());
                db.iterator_opt(IteratorMode::From(b"AB", Direction::Forward), ro)
                    .count()
            }),
        ),
        (
            "filter_family_Malvaceae",
            Box::new(|db: &DB| scan_pred_rocks(db, pred_family_malvaceae)),
        ),
        (
            "full_scan",
            Box::new(|db: &DB| db.iterator(IteratorMode::Start).count()),
        ),
        (
            "contains_scientific_Carex",
            Box::new(|db: &DB| scan_pred_rocks(db, pred_contains_sci)),
        ),
        (
            "common_name_present",
            Box::new(|db: &DB| scan_pred_rocks(db, pred_common_present)),
        ),
        (
            "family_in_set",
            Box::new(|db: &DB| scan_pred_rocks(db, pred_family_in_set)),
        ),
        (
            "top_100_by_symbol",
            Box::new(|db: &DB| db.iterator(IteratorMode::Start).take(TOP_N).count()),
        ),
    ];

    let queries: Vec<QueryReport> = if cold_per_query() {
        // Release the shared lock, then reopen a fresh cold DB per query.
        drop(db);
        specs
            .iter()
            .map(|(name, f)| {
                let qdb = DB::open(&rocks_options(), dir).expect("reopen rocksdb");
                let qr = measure_query(name, || f(&qdb));
                drop(qdb);
                qr
            })
            .collect()
    } else {
        let out = specs
            .iter()
            .map(|(name, f)| measure_query(name, || f(&db)))
            .collect();
        drop(db);
        out
    };

    Report {
        adapter: "raw:rocksdb".to_owned(),
        rows: plants.len(),
        batch_size,
        raw_input_bytes,
        encoded_logical_bytes: None,
        physical_bytes,
        write_time,
        flush_close_time,
        cold_open_time,
        queries,
    }
}

/// Time one query cold (first call), warm (second), then hot (best of
/// `HOT_ITERS` further calls) into a [`QueryReport`].
fn measure_query(name: &str, exec: impl Fn() -> usize) -> QueryReport {
    let (cold_ms, rows) = timed(&exec);
    let (warm_ms, _) = timed(&exec);
    let mut hot_ms = warm_ms;
    for _ in 0..HOT_ITERS {
        hot_ms = hot_ms.min(timed(&exec).0);
    }
    QueryReport {
        name: name.to_owned(),
        rows,
        cold_ms: Some(cold_ms),
        warm_ms,
        hot_ms,
    }
}

/// Full scan applying a value predicate — shared by the RocksDB filter queries.
fn scan_pred_rocks(db: &rocksdb::DB, pred: fn(&[u8]) -> bool) -> usize {
    db.iterator(rocksdb::IteratorMode::Start)
        .filter(|item| item.as_ref().map(|(_, v)| pred(v)).unwrap_or(false))
        .count()
}

// --- SlateDB, via the `slatedb` crate directly (async). ---

fn run_slatedb_raw(plants: &[Plant], batch_size: usize, dir: &Path, settle: Duration) -> Report {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    rt.block_on(slatedb_raw(plants, batch_size, dir, settle))
}

/// SlateDB settings tuned to match the RocksDB raw path's strengths:
/// (a) Zstd SST compression, and (b) an aggressive WAL garbage collector
/// (`min_age`/`interval` = 1s) so the duplicated WAL is reclaimed once a short
/// settle period has elapsed, instead of lingering for the default 60s.
fn slatedb_settings() -> slatedb::config::Settings {
    use slatedb::config::{
        CompressionCodec, GarbageCollectorDirectoryOptions, GarbageCollectorOptions, Settings,
    };
    Settings {
        compression_codec: Some(CompressionCodec::Zstd),
        garbage_collector_options: Some(GarbageCollectorOptions {
            wal_options: Some(GarbageCollectorDirectoryOptions {
                interval: Some(Duration::from_secs(1)),
                min_age: Duration::from_secs(1),
                dry_run: false,
            }),
            ..GarbageCollectorOptions::default()
        }),
        ..Default::default()
    }
}

async fn slatedb_raw(plants: &[Plant], batch_size: usize, dir: &Path, settle: Duration) -> Report {
    use slatedb::config::WriteOptions;
    use slatedb::object_store::{ObjectStore, local::LocalFileSystem};
    use std::sync::Arc;

    let raw_input_bytes = plants.iter().map(|p| p.raw_bytes() as u64).sum();
    let write_opts = WriteOptions {
        await_durable: false, // batches don't stall on WAL flush; matches groove.
        ..Default::default()
    };
    let store: Arc<dyn ObjectStore> =
        Arc::new(LocalFileSystem::new_with_prefix(dir).expect("local fs store"));
    // Run the garbage collector on the ambient runtime so the WAL GC actually
    // fires during the settle window.
    let db = slatedb::Db::builder("bench", store.clone())
        .with_settings(slatedb_settings())
        .with_gc_runtime(tokio::runtime::Handle::current())
        .build()
        .await
        .expect("open slatedb");

    let t_write = Instant::now();
    for chunk in plants.chunks(batch_size) {
        let mut batch = slatedb::WriteBatch::new();
        for p in chunk {
            batch.put(plant_key(p), plant_value(p));
        }
        db.write_with_options(batch, &write_opts)
            .await
            .expect("write batch");
    }
    let write_time = t_write.elapsed();

    let t_flush = Instant::now();
    db.flush().await.expect("flush slatedb");
    let mut flush_close_time = t_flush.elapsed();
    // (b) Keep the DB alive so the WAL GC can reclaim the duplicated WAL before
    // we measure the on-disk footprint.
    if !settle.is_zero() {
        tokio::time::sleep(settle).await;
    }
    let physical_bytes = Some(dir_size(dir));
    let t_close = Instant::now();
    db.close().await.expect("close slatedb");
    flush_close_time += t_close.elapsed();

    // Cold reopen.
    let t_open = Instant::now();
    let db = slatedb::Db::builder("bench", store.clone())
        .with_settings(slatedb_settings())
        .build()
        .await
        .expect("reopen slatedb");
    let cold_open_time = Some(t_open.elapsed());

    let key0 = plant_key(&plants[0]);

    async fn count_scan(db: &slatedb::Db, range: std::ops::Range<Vec<u8>>) -> usize {
        let mut iter = db.scan(range).await.expect("scan");
        let mut n = 0;
        while iter.next().await.expect("scan next").is_some() {
            n += 1;
        }
        n
    }
    async fn count_where(
        db: &slatedb::Db,
        range: std::ops::Range<Vec<u8>>,
        pred: fn(&[u8]) -> bool,
    ) -> usize {
        let mut iter = db.scan(range).await.expect("scan");
        let mut n = 0;
        while let Some(kv) = iter.next().await.expect("scan next") {
            if pred(&kv.value) {
                n += 1;
            }
        }
        n
    }
    async fn count_take_n(db: &slatedb::Db, range: std::ops::Range<Vec<u8>>, take: usize) -> usize {
        let mut iter = db.scan(range).await.expect("scan");
        let mut n = 0;
        while n < take && iter.next().await.expect("scan next").is_some() {
            n += 1;
        }
        n
    }

    // Query kinds, so each can run against the shared cold DB or a fresh
    // per-query one.
    enum Sq {
        Point,
        Prefix,
        FilterMalvaceae,
        Full,
        Contains,
        Common,
        FamilySet,
        TopN,
    }
    async fn exec_sq(db: &slatedb::Db, q: &Sq, key0: &[u8]) -> usize {
        let full = || vec![0u8]..vec![0xffu8];
        match q {
            Sq::Point => db.get(key0).await.expect("get").is_some() as usize,
            Sq::Prefix => count_scan(db, b"AB".to_vec()..b"AC".to_vec()).await,
            Sq::FilterMalvaceae => count_where(db, full(), pred_family_malvaceae).await,
            Sq::Full => count_scan(db, full()).await,
            Sq::Contains => count_where(db, full(), pred_contains_sci).await,
            Sq::Common => count_where(db, full(), pred_common_present).await,
            Sq::FamilySet => count_where(db, full(), pred_family_in_set).await,
            Sq::TopN => count_take_n(db, full(), TOP_N).await,
        }
    }
    async fn measure_sq(db: &slatedb::Db, q: &Sq, key0: &[u8]) -> (f64, f64, f64, usize) {
        let t = Instant::now();
        let rows = exec_sq(db, q, key0).await;
        let cold = t.elapsed().as_secs_f64() * 1e3;
        let t = Instant::now();
        let _ = exec_sq(db, q, key0).await;
        let warm = t.elapsed().as_secs_f64() * 1e3;
        let mut hot = warm;
        for _ in 0..HOT_ITERS {
            let t = Instant::now();
            let _ = exec_sq(db, q, key0).await;
            hot = hot.min(t.elapsed().as_secs_f64() * 1e3);
        }
        (cold, warm, hot, rows)
    }

    let specs = [
        ("point_by_key", Sq::Point),
        ("prefix_scan_AB", Sq::Prefix),
        ("filter_family_Malvaceae", Sq::FilterMalvaceae),
        ("full_scan", Sq::Full),
        ("contains_scientific_Carex", Sq::Contains),
        ("common_name_present", Sq::Common),
        ("family_in_set", Sq::FamilySet),
        ("top_100_by_symbol", Sq::TopN),
    ];
    let mut queries = Vec::new();
    if cold_per_query() {
        if let Err(e) = db.close().await {
            eprintln!("  warning: raw:slatedb cold close() failed: {e}");
        }
        drop(db);
        for (name, q) in &specs {
            let qdb = slatedb::Db::builder("bench", store.clone())
                .with_settings(slatedb_settings())
                .build()
                .await
                .expect("reopen slatedb");
            let (cold, warm, hot, rows) = measure_sq(&qdb, q, &key0).await;
            if let Err(e) = qdb.close().await {
                eprintln!("  warning: raw:slatedb close() failed: {e}");
            }
            queries.push(QueryReport {
                name: (*name).to_owned(),
                rows,
                cold_ms: Some(cold),
                warm_ms: warm,
                hot_ms: hot,
            });
        }
    } else {
        for (name, q) in &specs {
            let (cold, warm, hot, rows) = measure_sq(&db, q, &key0).await;
            queries.push(QueryReport {
                name: (*name).to_owned(),
                rows,
                cold_ms: Some(cold),
                warm_ms: warm,
                hot_ms: hot,
            });
        }
        if let Err(e) = db.close().await {
            eprintln!("  warning: raw:slatedb cold close() failed: {e}");
        }
    }

    Report {
        adapter: "raw:slatedb".to_owned(),
        rows: plants.len(),
        batch_size,
        raw_input_bytes,
        encoded_logical_bytes: None,
        physical_bytes,
        write_time,
        flush_close_time,
        cold_open_time,
        queries,
    }
}

fn dispatch_raw(engine: &str, plants: &[Plant], batch_size: usize, settle: Duration) -> Report {
    let dir = tempfile::tempdir().expect("tempdir");
    match engine {
        "rocksdb" => run_rocksdb_raw(plants, batch_size, dir.path()),
        "slatedb" => run_slatedb_raw(plants, batch_size, dir.path(), settle),
        other => panic!("unknown raw engine '{other}' (expected rocksdb|slatedb)"),
    }
}

// ---------------------------------------------------------------------------
// Reporting
// ---------------------------------------------------------------------------

fn print_report(report: &Report) {
    let rows = report.rows as f64;
    let write_s = report.write_time.as_secs_f64();
    let throughput = if write_s > 0.0 { rows / write_s } else { 0.0 };
    let amp = |n: u64, d: u64| if d > 0 { n as f64 / d as f64 } else { f64::NAN };

    println!("\n═══ {} ═══", report.adapter);
    println!("  rows                 {}", report.rows);
    println!("  batch size           {}", report.batch_size);
    println!(
        "  write time           {:.3} s   ({:.0} rows/s)",
        write_s, throughput
    );
    println!(
        "  flush + close        {:.3} s",
        report.flush_close_time.as_secs_f64()
    );
    if let Some(open) = report.cold_open_time {
        println!("  cold reopen (Db)     {:.3} s", open.as_secs_f64());
    }
    println!(
        "  raw input bytes      {:>12}  ({:.1} MiB)",
        report.raw_input_bytes,
        report.raw_input_bytes as f64 / (1024.0 * 1024.0)
    );
    match report.encoded_logical_bytes {
        Some(enc) => println!(
            "  encoded logical      {:>12}  ({:.1} MiB)",
            enc,
            enc as f64 / (1024.0 * 1024.0)
        ),
        None => println!("  encoded logical      n/a (backend has no byte accounting)"),
    }
    match report.physical_bytes {
        Some(phys) => {
            println!(
                "  physical on disk     {:>12}  ({:.1} MiB)",
                phys,
                phys as f64 / (1024.0 * 1024.0)
            );
            let vs_encoded = match report.encoded_logical_bytes {
                Some(enc) => format!("{:.2}× vs encoded", amp(phys, enc)),
                None => "n/a vs encoded".to_owned(),
            };
            println!(
                "  amplification        {:.2}× vs raw input   {}",
                amp(phys, report.raw_input_bytes),
                vs_encoded
            );
        }
        None => println!("  physical on disk     n/a (in-memory)"),
    }
    println!("  queries:");
    for q in &report.queries {
        match q.cold_ms {
            Some(cold) => println!(
                "    {:<28} {:>7} rows   cold {:>8.3} ms   warm {:>8.3} ms   hot {:>8.3} ms",
                q.name, q.rows, cold, q.warm_ms, q.hot_ms
            ),
            None => println!(
                "    {:<28} {:>7} rows   warm {:>8.3} ms   hot {:>8.3} ms",
                q.name, q.rows, q.warm_ms, q.hot_ms
            ),
        }
    }
}

fn print_json(report: &Report) {
    let amp = |n: u64, d: u64| if d > 0 { n as f64 / d as f64 } else { f64::NAN };
    let mut q_json = Vec::new();
    for q in &report.queries {
        let cold = match q.cold_ms {
            Some(c) => format!("{c:.3}"),
            None => "null".to_owned(),
        };
        q_json.push(format!(
            "{{\"name\":\"{}\",\"rows\":{},\"cold_ms\":{},\"warm_ms\":{:.3},\"hot_ms\":{:.3}}}",
            q.name, q.rows, cold, q.warm_ms, q.hot_ms
        ));
    }
    let (phys, amp_raw, amp_enc) = match report.physical_bytes {
        Some(p) => (
            p.to_string(),
            format!("{:.4}", amp(p, report.raw_input_bytes)),
            match report.encoded_logical_bytes {
                Some(enc) => format!("{:.4}", amp(p, enc)),
                None => "null".to_owned(),
            },
        ),
        None => ("null".to_owned(), "null".to_owned(), "null".to_owned()),
    };
    let encoded = match report.encoded_logical_bytes {
        Some(enc) => enc.to_string(),
        None => "null".to_owned(),
    };
    let cold_open = match report.cold_open_time {
        Some(d) => format!("{:.4}", d.as_secs_f64()),
        None => "null".to_owned(),
    };
    println!(
        "JSON {{\"adapter\":\"{}\",\"rows\":{},\"batch_size\":{},\"write_time_s\":{:.4},\"flush_close_s\":{:.4},\"cold_open_s\":{},\"raw_input_bytes\":{},\"encoded_logical_bytes\":{},\"physical_bytes\":{},\"amp_vs_raw\":{},\"amp_vs_encoded\":{},\"queries\":[{}]}}",
        report.adapter,
        report.rows,
        report.batch_size,
        report.write_time.as_secs_f64(),
        report.flush_close_time.as_secs_f64(),
        cold_open,
        report.raw_input_bytes,
        encoded,
        phys,
        amp_raw,
        amp_enc,
        q_json.join(","),
    );
}

// ---------------------------------------------------------------------------
// CLI
// ---------------------------------------------------------------------------

struct Args {
    storage: Vec<String>,
    raw: Vec<String>,
    input: PathBuf,
    batch_size: usize,
    limit: Option<usize>,
    json: bool,
    slatedb_settle_ms: u64,
    cold_per_query: bool,
}

fn parse_args() -> Args {
    let default_input = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("data/plantlst.txt");
    let mut args = Args {
        storage: Vec::new(),
        raw: Vec::new(),
        input: default_input,
        batch_size: 1000,
        limit: None,
        json: false,
        // (b) A WAL-GC settle is available, but once Zstd compression (a) is on
        // the WAL no longer dominates the footprint, so it is off by default.
        slatedb_settle_ms: 0,
        cold_per_query: false,
    };
    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        match arg.as_str() {
            "--storage" => {
                let v = it.next().expect("--storage needs a value");
                args.storage = v.split(',').map(|s| s.trim().to_owned()).collect();
            }
            "--raw" => {
                let v = it.next().expect("--raw needs a value");
                args.raw = v.split(',').map(|s| s.trim().to_owned()).collect();
            }
            "--input" => args.input = PathBuf::from(it.next().expect("--input needs a path")),
            "--batch-size" => {
                args.batch_size = it
                    .next()
                    .expect("--batch-size needs a number")
                    .parse()
                    .expect("--batch-size must be a number");
            }
            "--limit" => {
                args.limit = Some(
                    it.next()
                        .expect("--limit needs a number")
                        .parse()
                        .expect("--limit must be a number"),
                );
            }
            "--json" => args.json = true,
            "--cold-per-query" => args.cold_per_query = true,
            "--slatedb-settle-ms" => {
                args.slatedb_settle_ms = it
                    .next()
                    .expect("--slatedb-settle-ms needs a number")
                    .parse()
                    .expect("--slatedb-settle-ms must be a number");
            }
            "-h" | "--help" => {
                eprintln!(
                    "jazz-ingest-bench — storage ingestion/cold-load benchmark (USDA plants)\n\
                     \n\
                     Two layers on the same dataset and metrics:\n\
                     \x20 --storage <list>  through the Jazz Db API: memory,rocksdb,btree,slatedb\n\
                     \x20 --raw <list>      direct native engine API (no Jazz): rocksdb,slatedb\n\
                     (with neither flag, defaults to --raw rocksdb,slatedb)\n\
                     \n\
                     Options:\n\
                     \x20 --input <path>          CSV dataset (default bundled USDA plantlst.txt)\n\
                     \x20 --batch-size <n>        rows per transaction/write-batch (default 1000)\n\
                     \x20 --limit <n>             ingest only the first n rows\n\
                     \x20 --slatedb-settle-ms <n> raw:slatedb wait for WAL GC before sizing (default 0)\n\
                     \x20 --cold-per-query        reopen the DB before each query for a true cold-cache read\n\
                     \x20 --json                  also emit one machine-readable JSON line per run"
                );
                std::process::exit(0);
            }
            other => panic!("unknown argument '{other}' (try --help)"),
        }
    }
    args
}

fn main() {
    let mut args = parse_args();
    if args.storage.is_empty() && args.raw.is_empty() {
        args.raw = vec!["rocksdb".to_owned(), "slatedb".to_owned()];
    }
    COLD_PER_QUERY.store(args.cold_per_query, std::sync::atomic::Ordering::Relaxed);
    let plants = load_dataset(&args.input, args.limit);
    assert!(
        !plants.is_empty(),
        "dataset is empty: {}",
        args.input.display()
    );
    println!(
        "loaded {} records from {} ({} rows/batch)",
        plants.len(),
        args.input.display(),
        args.batch_size
    );

    // Jazz-layer adapters first, then direct engines. Each run is isolated so
    // one failure (e.g. the slatedb prototype) does not abort the comparison.
    let emit = |report: &Report| {
        print_report(report);
        if args.json {
            print_json(report);
        }
    };

    for adapter in &args.storage {
        let label = format!("jazz:{adapter}");
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let mut report = dispatch(adapter, &plants, args.batch_size);
            report.adapter = label.clone();
            report
        }));
        match result {
            Ok(report) => emit(&report),
            Err(_) => eprintln!("\n═══ {label} ═══\n  FAILED (see panic above)"),
        }
    }

    let settle = Duration::from_millis(args.slatedb_settle_ms);
    for engine in &args.raw {
        let label = format!("raw:{engine}");
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            dispatch_raw(engine, &plants, args.batch_size, settle)
        }));
        match result {
            Ok(report) => emit(&report),
            Err(_) => eprintln!("\n═══ {label} ═══\n  FAILED (see panic above)"),
        }
    }
}

/// Minimal executor: drive an async future to completion by busy-polling with a
/// no-op waker. Storage that actually suspends (SlateDB) does so on its own
/// bridge thread, so this never spins forever waiting on real I/O. Copied from
/// the jazz-sim benches (`customer_cold_start.rs`).
fn block_on<F: std::future::Future>(future: F) -> F::Output {
    let waker = std::task::Waker::noop();
    let mut cx = std::task::Context::from_waker(waker);
    let mut future = std::pin::pin!(future);
    loop {
        if let std::task::Poll::Ready(value) = future.as_mut().poll(&mut cx) {
            return value;
        }
    }
}
