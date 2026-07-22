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
//! The storage adapter is a runtime choice
//! (`--storage rocksdb|btree|slatedb|memory|sqlite|redb|postgres`), so the same Jazz
//! workload is compared across every backend selected for the run.
//!
//! ```text
//! cargo run --release -p jazz-ingest-bench -- --storage rocksdb
//! cargo run --release -p jazz-ingest-bench -- --storage rocksdb,btree,slatedb,sqlite,redb,postgres --limit 20000
//! ```

use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use jazz::db::{Db, DbConfig, DbIdentity};
use jazz::groove::records::Value;
use jazz::groove::schema::{ColumnSchema, ColumnType};
use jazz::groove::storage::{
    ColumnFamilyName, CurrentWinnerDelta, Durability, Error as StorageError, Key, KeyValue,
    MemoryStorage, NativeBtreeStorage, OrderedKvStorage, ReopenableStorage, RocksDbStorage,
    ScanVisitor, SlateDbStorage, StorageDelta, StorageDeltaKind, WriteOperation,
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

fn run_query<S>(db: &Db<S>, query: &Query) -> (Duration, usize)
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    let prepared = db.prepare_query(query).expect("prepare query");
    let t = Instant::now();
    let rows = db.read(&prepared).expect("read query");
    (t.elapsed(), rows.len())
}

// ---------------------------------------------------------------------------
// The benchmark, generic over the storage adapter
// ---------------------------------------------------------------------------

struct QueryReport {
    name: String,
    rows: usize,
    cold_ms: Option<f64>,
    warm_ms: f64,
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
/// seen on the cold reopen. Persistent backends provide their own physical-size
/// callback because local engines use paths while Postgres reports relation
/// bytes from the server.
fn benchmark<S>(
    adapter: &str,
    plants: &[Plant],
    batch_size: usize,
    schema: &JazzSchema,
    persistent: bool,
    physical_size: impl Fn() -> Option<u64>,
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

    if persistent {
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
        physical_bytes = physical_size();

        let t_open = Instant::now();
        let cold_db = open_db(schema, open());
        cold_open_time = Some(t_open.elapsed());

        for (name, query) in &query_set {
            let (cold, rows) = run_query(&cold_db, query); // first read = cold
            let (warm, _) = run_query(&cold_db, query); // second read = warm
            queries_out.push(QueryReport {
                name: name.clone(),
                rows,
                cold_ms: Some(cold.as_secs_f64() * 1e3),
                warm_ms: warm.as_secs_f64() * 1e3,
            });
        }
        if let Err(e) = cold_db.close() {
            eprintln!("  warning: {adapter} cold close() failed: {e}");
        }
    } else {
        // In-memory: no disk, no cold path — query the warm DB, then close.
        physical_bytes = None;
        cold_open_time = None;
        for (name, query) in &query_set {
            let (warm, rows) = run_query(&db, query);
            queries_out.push(QueryReport {
                name: name.clone(),
                rows,
                cold_ms: None,
                warm_ms: warm.as_secs_f64() * 1e3,
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

fn dispatch(
    adapter: &str,
    plants: &[Plant],
    batch_size: usize,
    ebs_jitter: SimulatedLatency,
    safekeeper_jitter: SafekeeperJitter,
) -> Report {
    let schema = schema();
    let cfs = column_family_refs(&schema);
    let refs: Vec<&str> = cfs.iter().map(String::as_str).collect();
    let dir = tempfile::tempdir().expect("tempdir");

    match adapter {
        "memory" => {
            let refs = refs.clone();
            benchmark(
                adapter,
                plants,
                batch_size,
                &schema,
                false,
                || None,
                move || MemoryStorage::new(&refs),
            )
        }
        "rocksdb" => {
            let path = dir.path().to_path_buf();
            let path_for_size = path.clone();
            let refs = refs.clone();
            benchmark(
                adapter,
                plants,
                batch_size,
                &schema,
                true,
                move || Some(dir_size(&path_for_size)),
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
                true,
                move || Some(dir_size(&file_for_size)),
                move || NativeBtreeStorage::open(&file, &refs).expect("open btree"),
            )
        }
        "slatedb" => {
            let path = dir.path().to_path_buf();
            let path_for_size = path.clone();
            let refs = refs.clone();
            benchmark(
                adapter,
                plants,
                batch_size,
                &schema,
                true,
                move || Some(dir_size(&path_for_size)),
                move || SlateDbStorage::open_bridged(path.clone(), &refs).expect("open slatedb"),
            )
        }
        "slatedb-localwal" | "slatedb-localwal-sync" => {
            let checkpoint_path = dir.path().join("slatedb-checkpoint");
            let wal_path = dir.path().join("local.wal");
            let root_for_size = dir.path().to_path_buf();
            let sync_on_commit = adapter == "slatedb-localwal-sync";
            let refs = refs.clone();
            benchmark(
                adapter,
                plants,
                batch_size,
                &schema,
                true,
                move || Some(dir_size(&root_for_size)),
                move || {
                    LocalWalSlateDbStorage::open(
                        checkpoint_path.clone(),
                        wal_path.clone(),
                        &refs,
                        sync_on_commit,
                        ebs_jitter,
                        safekeeper_jitter,
                    )
                    .expect("open local wal slatedb")
                },
            )
        }
        "sqlite" => {
            let file = dir.path().join("sqlite.db");
            let dir_for_size = dir.path().to_path_buf();
            let refs = refs.clone();
            benchmark(
                adapter,
                plants,
                batch_size,
                &schema,
                true,
                move || Some(dir_size(&dir_for_size)),
                move || SqliteStorage::open(&file, &refs).expect("open sqlite"),
            )
        }
        "redb" => {
            let file = dir.path().join("redb.store");
            let file_for_size = file.clone();
            let refs = refs.clone();
            benchmark(
                adapter,
                plants,
                batch_size,
                &schema,
                true,
                move || {
                    compact_redb_file(&file_for_size).expect("compact redb");
                    Some(dir_size(&file_for_size))
                },
                move || RedbStorage::open(&file, &refs).expect("open redb"),
            )
        }
        "postgres" => {
            let conn_str = postgres_url();
            let table = postgres_table_name("jazz_pg_kv");
            PostgresStorage::reset_table(&conn_str, &table).expect("reset postgres table");
            let conn_str_for_size = conn_str.clone();
            let table_for_size = table.clone();
            let refs = refs.clone();
            benchmark(
                adapter,
                plants,
                batch_size,
                &schema,
                true,
                move || {
                    Some(
                        PostgresStorage::physical_bytes(&conn_str_for_size, &table_for_size)
                            .expect("postgres physical size"),
                    )
                },
                move || {
                    PostgresStorage::open(conn_str.clone(), table.clone(), &refs)
                        .expect("open postgres")
                },
            )
        }
        other => {
            panic!(
                "unknown storage adapter '{other}' (expected memory|rocksdb|btree|slatedb|slatedb-localwal|slatedb-localwal-sync|sqlite|redb|postgres)"
            )
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

fn timed_query_report(name: &str, mut exec: impl FnMut() -> usize) -> QueryReport {
    let (cold_ms, rows) = timed(&mut exec);
    let (warm_ms, _) = timed(&mut exec);
    QueryReport {
        name: name.to_owned(),
        rows,
        cold_ms: Some(cold_ms),
        warm_ms,
    }
}

// --- Postgres, used both as a raw KV engine and as a Groove storage adapter. ---

const DEFAULT_POSTGRES_URL: &str =
    "host=localhost port=55432 user=postgres password=postgres dbname=postgres";

fn postgres_url() -> String {
    std::env::var("JAZZ_INGEST_POSTGRES_URL").unwrap_or_else(|_| DEFAULT_POSTGRES_URL.to_owned())
}

fn postgres_table_name(prefix: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .expect("system time before unix epoch")
        .as_nanos();
    format!("{prefix}_{}_{}", std::process::id(), nanos)
}

fn pg_ident(table: &str) -> String {
    assert!(
        table
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_'),
        "unsafe postgres table name: {table}"
    );
    format!("\"{table}\"")
}

fn quote_pg_string(value: &str) -> String {
    format!("'{}'", value.replace('\'', "''"))
}

fn pg_error(error: impl std::fmt::Display) -> StorageError {
    StorageError::InvalidStorageDelta(format!("postgres: {error}"))
}

fn apply_adapter_delta(
    existing: Option<&[u8]>,
    delta: &StorageDelta,
) -> Result<Vec<u8>, StorageError> {
    match delta.kind {
        StorageDeltaKind::CurrentWinnerV1 => {
            let candidate: CurrentWinnerDelta =
                postcard::from_bytes(&delta.payload).map_err(|error| {
                    StorageError::InvalidStorageDelta(format!("adapter delta decode: {error}"))
                })?;
            let Some(existing) = existing else {
                return Ok(candidate.record);
            };
            let existing_key = current_winner_key_for_adapter(
                existing,
                candidate.tx_time_offset as usize,
                candidate.tx_node_uuid_offset as usize,
            )?;
            let candidate_key = (candidate.tx_time, candidate.tx_node_uuid);
            if candidate.parents.contains(&existing_key) || candidate_key > existing_key {
                Ok(candidate.record)
            } else {
                Ok(existing.to_vec())
            }
        }
    }
}

fn current_winner_key_for_adapter(
    record: &[u8],
    tx_time_offset: usize,
    tx_node_uuid_offset: usize,
) -> Result<(u64, [u8; 16]), StorageError> {
    let time_bytes = record
        .get(tx_time_offset..tx_time_offset + 8)
        .ok_or_else(|| {
            StorageError::InvalidStorageDelta(
                "adapter current-winner tx_time offset out of bounds".to_owned(),
            )
        })?;
    let uuid_bytes = record
        .get(tx_node_uuid_offset..tx_node_uuid_offset + 16)
        .ok_or_else(|| {
            StorageError::InvalidStorageDelta(
                "adapter current-winner tx_node_uuid offset out of bounds".to_owned(),
            )
        })?;
    let mut uuid = [0; 16];
    uuid.copy_from_slice(uuid_bytes);
    Ok((
        u64::from_le_bytes(time_bytes.try_into().expect("slice length checked")),
        uuid,
    ))
}

fn prefix_upper_bound(prefix: &[u8]) -> Option<Vec<u8>> {
    let mut upper = prefix.to_vec();
    let index = upper.iter().rposition(|byte| *byte != 0xff)?;
    upper[index] += 1;
    upper.truncate(index + 1);
    Some(upper)
}

fn adapter_cf_prefix(cf: &ColumnFamilyName) -> Vec<u8> {
    let cf_bytes = cf.as_bytes();
    let len = u16::try_from(cf_bytes.len()).expect("column-family name too long for storage key");
    let mut key = Vec::with_capacity(2 + cf_bytes.len());
    key.extend_from_slice(&len.to_be_bytes());
    key.extend_from_slice(cf_bytes);
    key
}

fn adapter_storage_key(cf: &ColumnFamilyName, key: &Key) -> Vec<u8> {
    let mut storage_key = adapter_cf_prefix(cf);
    storage_key.extend_from_slice(key);
    storage_key
}

fn adapter_user_key(storage_key: &[u8]) -> &[u8] {
    let len = u16::from_be_bytes([storage_key[0], storage_key[1]]) as usize;
    &storage_key[2 + len..]
}

// --- Local WAL + SlateDB checkpoint, used to model cheap backing storage. ---

fn local_wal_error(error: impl std::fmt::Display) -> StorageError {
    StorageError::InvalidStorageDelta(format!("local wal: {error}"))
}

type SafekeeperJitter = SimulatedLatency;

#[derive(Clone, Copy, Default)]
struct SimulatedLatency {
    base_ms: u64,
    jitter_ms: u64,
}

impl SimulatedLatency {
    fn delay_for(self, seed: u64) -> Duration {
        let jitter = if self.jitter_ms == 0 {
            0
        } else {
            stable_jitter(seed) % (self.jitter_ms + 1)
        };
        Duration::from_millis(self.base_ms + jitter)
    }

    fn sleep_for(self, seed: u64) {
        let delay = self.delay_for(seed);
        if !delay.is_zero() {
            std::thread::sleep(delay);
        }
    }
}

fn stable_jitter(mut value: u64) -> u64 {
    value ^= value >> 12;
    value ^= value << 25;
    value ^= value >> 27;
    value.wrapping_mul(0x2545_f491_4f6c_dd1d)
}

pub struct LocalWalSlateDbStorage {
    hot: MemoryStorage,
    checkpoint: Mutex<jazz::groove::storage::SyncBridgeStorage>,
    column_families: BTreeSet<String>,
    wal_path: PathBuf,
    sync_on_commit: bool,
    ebs_jitter: SimulatedLatency,
    safekeeper_jitter: SafekeeperJitter,
    wal: Mutex<File>,
    sync_sequence: Mutex<u64>,
    pending_checkpoint: Mutex<Vec<jazz::groove::storage::OwnedWriteOperation>>,
}

impl LocalWalSlateDbStorage {
    fn open(
        checkpoint_path: PathBuf,
        wal_path: PathBuf,
        column_families: &[&str],
        sync_on_commit: bool,
        ebs_jitter: SimulatedLatency,
        safekeeper_jitter: SafekeeperJitter,
    ) -> Result<Self, StorageError> {
        let checkpoint = SlateDbStorage::open_bridged(checkpoint_path, column_families)?;
        let hot = MemoryStorage::new(column_families);
        for cf in column_families {
            checkpoint.scan_prefix(cf, b"", &mut |key, value| {
                hot.set(cf, key, value)?;
                Ok(())
            })?;
        }
        let wal = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&wal_path)
            .map_err(local_wal_error)?;
        Ok(Self {
            hot,
            checkpoint: Mutex::new(checkpoint),
            column_families: column_families.iter().map(|cf| (*cf).to_owned()).collect(),
            wal_path,
            sync_on_commit,
            ebs_jitter,
            safekeeper_jitter,
            wal: Mutex::new(wal),
            sync_sequence: Mutex::new(0),
            pending_checkpoint: Mutex::new(Vec::new()),
        })
    }

    fn append_wal(&self, operations: &[WriteOperation<'_>]) -> Result<(), StorageError> {
        let mut record = Vec::new();
        record.extend_from_slice(b"JZWAL001");
        record.extend_from_slice(&(operations.len() as u32).to_le_bytes());
        for operation in operations {
            match operation {
                WriteOperation::Set { cf, key, value } => {
                    record.push(1);
                    wal_bytes(&mut record, cf.as_bytes());
                    wal_bytes(&mut record, key);
                    wal_bytes(&mut record, value);
                }
                WriteOperation::Delete { cf, key } => {
                    record.push(2);
                    wal_bytes(&mut record, cf.as_bytes());
                    wal_bytes(&mut record, key);
                }
                WriteOperation::Delta { cf, key, delta } => {
                    record.push(3);
                    wal_bytes(&mut record, cf.as_bytes());
                    wal_bytes(&mut record, key);
                    record.push(match delta.kind {
                        StorageDeltaKind::CurrentWinnerV1 => 1,
                    });
                    wal_bytes(&mut record, &delta.payload);
                }
            }
        }

        {
            let mut wal = self.wal.lock().map_err(local_wal_error)?;
            wal.write_all(&(record.len() as u32).to_le_bytes())
                .map_err(local_wal_error)?;
            wal.write_all(&record).map_err(local_wal_error)?;
            if self.sync_on_commit {
                wal.sync_data().map_err(local_wal_error)?;
            }
        }

        if self.sync_on_commit {
            let sequence = {
                let mut sequence = self.sync_sequence.lock().map_err(local_wal_error)?;
                let current = *sequence;
                *sequence = sequence.wrapping_add(1);
                current
            };
            let seed = sequence ^ operations.len() as u64;
            self.ebs_jitter.sleep_for(seed);
            self.safekeeper_jitter
                .sleep_for(seed ^ 0x9e37_79b9_7f4a_7c15);
        }
        Ok(())
    }

    fn remember_checkpoint(&self, operations: &[WriteOperation<'_>]) -> Result<(), StorageError> {
        let mut pending = self.pending_checkpoint.lock().map_err(local_wal_error)?;
        pending.extend(operations.iter().map(owned_bench_write_operation));
        Ok(())
    }

    fn checkpoint_pending(&self) -> Result<(), StorageError> {
        {
            let wal = self.wal.lock().map_err(local_wal_error)?;
            wal.sync_data().map_err(local_wal_error)?;
        }

        let pending = {
            let mut pending = self.pending_checkpoint.lock().map_err(local_wal_error)?;
            std::mem::take(&mut *pending)
        };

        if !pending.is_empty() {
            let borrowed = pending
                .iter()
                .map(jazz::groove::storage::OwnedWriteOperation::as_write_operation)
                .collect::<Vec<_>>();
            self.checkpoint
                .lock()
                .map_err(local_wal_error)?
                .write_many(&borrowed)?;
        }

        self.checkpoint.lock().map_err(local_wal_error)?.close()?;

        let wal = self.wal.lock().map_err(local_wal_error)?;
        wal.set_len(0).map_err(local_wal_error)?;
        wal.sync_data().map_err(local_wal_error)
    }
}

fn wal_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
    out.extend_from_slice(bytes);
}

impl OrderedKvStorage for LocalWalSlateDbStorage {
    fn get(&self, cf: &ColumnFamilyName, key: &Key) -> Result<Option<Vec<u8>>, StorageError> {
        self.hot.get(cf, key)
    }

    fn set(&self, cf: &ColumnFamilyName, key: &Key, value: &[u8]) -> Result<(), StorageError> {
        let operation = WriteOperation::set(cf, key, value);
        self.append_wal(&[operation])?;
        self.remember_checkpoint(&[operation])?;
        self.hot.set(cf, key, value)
    }

    fn delete(&self, cf: &ColumnFamilyName, key: &Key) -> Result<(), StorageError> {
        let operation = WriteOperation::delete(cf, key);
        self.append_wal(&[operation])?;
        self.remember_checkpoint(&[operation])?;
        self.hot.delete(cf, key)
    }

    fn close(&self) -> Result<(), StorageError> {
        self.checkpoint_pending()
    }

    fn approximate_class_bytes(&self, cf: &ColumnFamilyName) -> Result<Option<u64>, StorageError> {
        self.hot.approximate_class_bytes(cf)
    }

    fn scan_range(
        &self,
        cf: &ColumnFamilyName,
        start: &Key,
        end: &Key,
        visit: &mut ScanVisitor<'_>,
    ) -> Result<(), StorageError> {
        self.hot.scan_range(cf, start, end, visit)
    }

    fn scan_prefix(
        &self,
        cf: &ColumnFamilyName,
        prefix: &Key,
        visit: &mut ScanVisitor<'_>,
    ) -> Result<(), StorageError> {
        self.hot.scan_prefix(cf, prefix, visit)
    }

    fn scan_prefix_reverse(
        &self,
        cf: &ColumnFamilyName,
        prefix: &Key,
        visit: &mut ScanVisitor<'_>,
    ) -> Result<(), StorageError> {
        self.hot.scan_prefix_reverse(cf, prefix, visit)
    }

    fn last_with_prefix(
        &self,
        cf: &ColumnFamilyName,
        prefix: &Key,
    ) -> Result<Option<KeyValue>, StorageError> {
        self.hot.last_with_prefix(cf, prefix)
    }

    fn last_with_prefix_before_or_at(
        &self,
        cf: &ColumnFamilyName,
        prefix: &Key,
        upper: &Key,
    ) -> Result<Option<KeyValue>, StorageError> {
        self.hot.last_with_prefix_before_or_at(cf, prefix, upper)
    }

    fn write_many(&self, operations: &[WriteOperation<'_>]) -> Result<(), StorageError> {
        self.append_wal(operations)?;
        self.remember_checkpoint(operations)?;
        self.hot.write_many(operations)
    }

    fn column_family_names(&self) -> Option<Vec<String>> {
        Some(self.column_families.iter().cloned().collect())
    }
}

impl ReopenableStorage for LocalWalSlateDbStorage {
    fn reopen(self, column_families: &[&str]) -> Result<Self, StorageError> {
        let Self {
            hot,
            checkpoint,
            column_families: mut known,
            wal_path,
            sync_on_commit,
            ebs_jitter,
            safekeeper_jitter,
            wal,
            sync_sequence,
            pending_checkpoint,
        } = self;
        for cf in column_families {
            known.insert((*cf).to_owned());
        }
        Ok(Self {
            hot: hot.reopen(column_families)?,
            checkpoint,
            column_families: known,
            wal_path,
            sync_on_commit,
            ebs_jitter,
            safekeeper_jitter,
            wal,
            sync_sequence,
            pending_checkpoint,
        })
    }
}

fn owned_bench_write_operation(
    operation: &WriteOperation<'_>,
) -> jazz::groove::storage::OwnedWriteOperation {
    match operation {
        WriteOperation::Set { cf, key, value } => jazz::groove::storage::OwnedWriteOperation::Set {
            cf: (*cf).to_owned(),
            key: key.to_vec(),
            value: value.to_vec(),
        },
        WriteOperation::Delete { cf, key } => jazz::groove::storage::OwnedWriteOperation::Delete {
            cf: (*cf).to_owned(),
            key: key.to_vec(),
        },
        WriteOperation::Delta { cf, key, delta } => {
            jazz::groove::storage::OwnedWriteOperation::Delta {
                cf: (*cf).to_owned(),
                key: key.to_vec(),
                delta: (*delta).clone(),
            }
        }
    }
}

pub struct PostgresStorage {
    conn_str: String,
    table: String,
    column_families: BTreeSet<String>,
    client: Mutex<postgres::Client>,
}

impl PostgresStorage {
    fn open(
        conn_str: String,
        table: String,
        column_families: &[&str],
    ) -> Result<Self, StorageError> {
        let mut client = postgres::Client::connect(&conn_str, postgres::NoTls).map_err(pg_error)?;
        let table_ident = pg_ident(&table);
        client
            .batch_execute(&format!(
                "CREATE TABLE IF NOT EXISTS {table_ident} (
                    cf TEXT NOT NULL,
                    key BYTEA NOT NULL,
                    value BYTEA NOT NULL,
                    PRIMARY KEY (cf, key)
                )"
            ))
            .map_err(pg_error)?;
        Ok(Self {
            conn_str,
            table,
            column_families: column_families.iter().map(|cf| (*cf).to_owned()).collect(),
            client: Mutex::new(client),
        })
    }

    fn reset_table(conn_str: &str, table: &str) -> Result<(), StorageError> {
        let mut client = postgres::Client::connect(conn_str, postgres::NoTls).map_err(pg_error)?;
        client
            .batch_execute(&format!("DROP TABLE IF EXISTS {}", pg_ident(table)))
            .map_err(pg_error)
    }

    fn physical_bytes(conn_str: &str, table: &str) -> Result<u64, StorageError> {
        let mut client = postgres::Client::connect(conn_str, postgres::NoTls).map_err(pg_error)?;
        let row = client
            .query_one(
                &format!(
                    "SELECT pg_total_relation_size({}::regclass)::bigint",
                    quote_pg_string(table)
                ),
                &[],
            )
            .map_err(pg_error)?;
        let bytes: i64 = row.get(0);
        Ok(bytes.max(0) as u64)
    }

    fn check_cf(&self, cf: &ColumnFamilyName) -> Result<(), StorageError> {
        if self.column_families.contains(cf) {
            Ok(())
        } else {
            Err(StorageError::ColumnFamilyNotFound(cf.to_owned()))
        }
    }

    fn table_ident(&self) -> String {
        pg_ident(&self.table)
    }
}

impl ReopenableStorage for PostgresStorage {
    fn reopen(mut self, column_families: &[&str]) -> Result<Self, StorageError> {
        for cf in column_families {
            self.column_families.insert((*cf).to_owned());
        }
        Self::open(self.conn_str, self.table, column_families)
    }
}

impl OrderedKvStorage for PostgresStorage {
    fn get(&self, cf: &ColumnFamilyName, key: &Key) -> Result<Option<Vec<u8>>, StorageError> {
        self.check_cf(cf)?;
        let mut client = self.client.lock().map_err(pg_error)?;
        let row = client
            .query_opt(
                &format!(
                    "SELECT value FROM {} WHERE cf = $1 AND key = $2",
                    self.table_ident()
                ),
                &[&cf, &key],
            )
            .map_err(pg_error)?;
        Ok(row.map(|row| row.get(0)))
    }

    fn set(&self, cf: &ColumnFamilyName, key: &Key, value: &[u8]) -> Result<(), StorageError> {
        self.write_many(&[WriteOperation::set(cf, key, value)])
    }

    fn delete(&self, cf: &ColumnFamilyName, key: &Key) -> Result<(), StorageError> {
        self.write_many(&[WriteOperation::delete(cf, key)])
    }

    fn close(&self) -> Result<(), StorageError> {
        let mut client = self.client.lock().map_err(pg_error)?;
        client.batch_execute("CHECKPOINT").map_err(pg_error)
    }

    fn approximate_class_bytes(&self, cf: &ColumnFamilyName) -> Result<Option<u64>, StorageError> {
        self.check_cf(cf)?;
        let mut client = self.client.lock().map_err(pg_error)?;
        let row = client
            .query_one(
                &format!(
                    "SELECT COALESCE(SUM(octet_length(key) + octet_length(value)), 0)::bigint \
                     FROM {} WHERE cf = $1",
                    self.table_ident()
                ),
                &[&cf],
            )
            .map_err(pg_error)?;
        let bytes: i64 = row.get(0);
        Ok(Some(bytes.max(0) as u64))
    }

    fn scan_range(
        &self,
        cf: &ColumnFamilyName,
        start: &Key,
        end: &Key,
        visit: &mut ScanVisitor<'_>,
    ) -> Result<(), StorageError> {
        self.check_cf(cf)?;
        let mut client = self.client.lock().map_err(pg_error)?;
        let rows = client
            .query(
                &format!(
                    "SELECT key, value FROM {} \
                     WHERE cf = $1 AND key >= $2 AND key < $3 ORDER BY key ASC",
                    self.table_ident()
                ),
                &[&cf, &start, &end],
            )
            .map_err(pg_error)?;
        for row in rows {
            let key: Vec<u8> = row.get(0);
            let value: Vec<u8> = row.get(1);
            visit(&key, &value)?;
        }
        Ok(())
    }

    fn scan_prefix(
        &self,
        cf: &ColumnFamilyName,
        prefix: &Key,
        visit: &mut ScanVisitor<'_>,
    ) -> Result<(), StorageError> {
        self.check_cf(cf)?;
        let mut client = self.client.lock().map_err(pg_error)?;
        let table_ident = self.table_ident();
        let rows = if let Some(upper) = prefix_upper_bound(prefix) {
            client
                .query(
                    &format!(
                        "SELECT key, value FROM {table_ident} \
                         WHERE cf = $1 AND key >= $2 AND key < $3 ORDER BY key ASC"
                    ),
                    &[&cf, &prefix, &upper.as_slice()],
                )
                .map_err(pg_error)?
        } else {
            client
                .query(
                    &format!(
                        "SELECT key, value FROM {table_ident} \
                         WHERE cf = $1 AND key >= $2 ORDER BY key ASC"
                    ),
                    &[&cf, &prefix],
                )
                .map_err(pg_error)?
        };
        for row in rows {
            let key: Vec<u8> = row.get(0);
            if !key.starts_with(prefix) {
                break;
            }
            let value: Vec<u8> = row.get(1);
            visit(&key, &value)?;
        }
        Ok(())
    }

    fn scan_prefix_reverse(
        &self,
        cf: &ColumnFamilyName,
        prefix: &Key,
        visit: &mut ScanVisitor<'_>,
    ) -> Result<(), StorageError> {
        self.check_cf(cf)?;
        let mut client = self.client.lock().map_err(pg_error)?;
        let table_ident = self.table_ident();
        let rows = if let Some(upper) = prefix_upper_bound(prefix) {
            client
                .query(
                    &format!(
                        "SELECT key, value FROM {table_ident} \
                         WHERE cf = $1 AND key >= $2 AND key < $3 ORDER BY key DESC"
                    ),
                    &[&cf, &prefix, &upper.as_slice()],
                )
                .map_err(pg_error)?
        } else {
            client
                .query(
                    &format!(
                        "SELECT key, value FROM {table_ident} \
                         WHERE cf = $1 AND key >= $2 ORDER BY key DESC"
                    ),
                    &[&cf, &prefix],
                )
                .map_err(pg_error)?
        };
        for row in rows {
            let key: Vec<u8> = row.get(0);
            if !key.starts_with(prefix) {
                continue;
            }
            let value: Vec<u8> = row.get(1);
            visit(&key, &value)?;
        }
        Ok(())
    }

    fn last_with_prefix(
        &self,
        cf: &ColumnFamilyName,
        prefix: &Key,
    ) -> Result<Option<KeyValue>, StorageError> {
        let mut last = None;
        self.scan_prefix_reverse(cf, prefix, &mut |key, value| {
            last = Some((key.to_vec(), value.to_vec()));
            Ok(())
        })?;
        Ok(last)
    }

    fn last_with_prefix_before_or_at(
        &self,
        cf: &ColumnFamilyName,
        prefix: &Key,
        upper: &Key,
    ) -> Result<Option<KeyValue>, StorageError> {
        self.check_cf(cf)?;
        let mut client = self.client.lock().map_err(pg_error)?;
        let table_ident = self.table_ident();
        let rows = if let Some(prefix_upper) = prefix_upper_bound(prefix) {
            client
                .query(
                    &format!(
                        "SELECT key, value FROM {table_ident} \
                         WHERE cf = $1 AND key >= $2 AND key <= $3 AND key < $4 \
                         ORDER BY key DESC LIMIT 1"
                    ),
                    &[&cf, &prefix, &upper, &prefix_upper.as_slice()],
                )
                .map_err(pg_error)?
        } else {
            client
                .query(
                    &format!(
                        "SELECT key, value FROM {table_ident} \
                         WHERE cf = $1 AND key >= $2 AND key <= $3 \
                         ORDER BY key DESC LIMIT 1"
                    ),
                    &[&cf, &prefix, &upper],
                )
                .map_err(pg_error)?
        };
        Ok(rows.first().and_then(|row| {
            let key: Vec<u8> = row.get(0);
            key.starts_with(prefix).then(|| (key, row.get(1)))
        }))
    }

    fn write_many(&self, operations: &[WriteOperation<'_>]) -> Result<(), StorageError> {
        for operation in operations {
            let cf = match operation {
                WriteOperation::Set { cf, .. }
                | WriteOperation::Delete { cf, .. }
                | WriteOperation::Delta { cf, .. } => *cf,
            };
            self.check_cf(cf)?;
        }

        let mut client = self.client.lock().map_err(pg_error)?;
        let mut tx = client.transaction().map_err(pg_error)?;
        let table_ident = self.table_ident();
        let get_stmt = tx
            .prepare(&format!(
                "SELECT value FROM {table_ident} WHERE cf = $1 AND key = $2"
            ))
            .map_err(pg_error)?;
        let set_stmt = tx
            .prepare(&format!(
                "INSERT INTO {table_ident} (cf, key, value) VALUES ($1, $2, $3) \
                 ON CONFLICT (cf, key) DO UPDATE SET value = EXCLUDED.value"
            ))
            .map_err(pg_error)?;
        let delete_stmt = tx
            .prepare(&format!(
                "DELETE FROM {table_ident} WHERE cf = $1 AND key = $2"
            ))
            .map_err(pg_error)?;

        for operation in operations {
            match operation {
                WriteOperation::Set { cf, key, value } => {
                    tx.execute(&set_stmt, &[cf, key, value]).map_err(pg_error)?;
                }
                WriteOperation::Delete { cf, key } => {
                    tx.execute(&delete_stmt, &[cf, key]).map_err(pg_error)?;
                }
                WriteOperation::Delta { cf, key, delta } => {
                    let existing = tx
                        .query_opt(&get_stmt, &[cf, key])
                        .map_err(pg_error)?
                        .map(|row| row.get::<_, Vec<u8>>(0));
                    let merged = apply_adapter_delta(existing.as_deref(), delta)?;
                    tx.execute(&set_stmt, &[cf, key, &merged.as_slice()])
                        .map_err(pg_error)?;
                }
            }
        }
        tx.commit().map_err(pg_error)
    }

    fn column_family_names(&self) -> Option<Vec<String>> {
        Some(self.column_families.iter().cloned().collect())
    }
}

// --- SQLite, used both as a raw KV engine and as a Groove storage adapter. ---

fn sqlite_error(error: impl std::fmt::Display) -> StorageError {
    StorageError::InvalidStorageDelta(format!("sqlite: {error}"))
}

pub struct SqliteStorage {
    path: PathBuf,
    column_families: BTreeSet<String>,
    conn: Mutex<rusqlite::Connection>,
}

impl SqliteStorage {
    fn open(path: impl AsRef<Path>, column_families: &[&str]) -> Result<Self, StorageError> {
        Self::open_with_names(
            path.as_ref().to_path_buf(),
            column_families.iter().map(|cf| (*cf).to_owned()).collect(),
        )
    }

    fn open_with_names(
        path: PathBuf,
        column_families: BTreeSet<String>,
    ) -> Result<Self, StorageError> {
        let conn = rusqlite::Connection::open(&path).map_err(sqlite_error)?;
        configure_sqlite(&conn)?;
        conn.execute_batch(
            "CREATE TABLE IF NOT EXISTS kv (
                key BLOB NOT NULL,
                value BLOB NOT NULL,
                PRIMARY KEY (key)
            ) WITHOUT ROWID;",
        )
        .map_err(sqlite_error)?;
        Ok(Self {
            path,
            column_families,
            conn: Mutex::new(conn),
        })
    }

    fn check_cf(&self, cf: &ColumnFamilyName) -> Result<(), StorageError> {
        if self.column_families.contains(cf) {
            Ok(())
        } else {
            Err(StorageError::ColumnFamilyNotFound(cf.to_owned()))
        }
    }
}

fn configure_sqlite(conn: &rusqlite::Connection) -> Result<(), StorageError> {
    conn.pragma_update(None, "journal_mode", "WAL")
        .map_err(sqlite_error)?;
    conn.pragma_update(None, "synchronous", "NORMAL")
        .map_err(sqlite_error)?;
    conn.pragma_update(None, "temp_store", "MEMORY")
        .map_err(sqlite_error)?;
    conn.pragma_update(None, "cache_size", -262_144)
        .map_err(sqlite_error)?;
    conn.pragma_update(None, "mmap_size", 256 * 1024 * 1024)
        .map_err(sqlite_error)?;
    conn.pragma_update(None, "locking_mode", "EXCLUSIVE")
        .map_err(sqlite_error)?;
    conn.pragma_update(None, "wal_autocheckpoint", 0)
        .map_err(sqlite_error)?;
    Ok(())
}

impl ReopenableStorage for SqliteStorage {
    fn reopen(self, column_families: &[&str]) -> Result<Self, StorageError> {
        let path = self.path.clone();
        let mut names = self.column_families.clone();
        for cf in column_families {
            names.insert((*cf).to_owned());
        }
        drop(self);
        Self::open_with_names(path, names)
    }
}

impl OrderedKvStorage for SqliteStorage {
    fn get(&self, cf: &ColumnFamilyName, key: &Key) -> Result<Option<Vec<u8>>, StorageError> {
        use rusqlite::{OptionalExtension, params};

        self.check_cf(cf)?;
        let storage_key = adapter_storage_key(cf, key);
        let conn = self.conn.lock().map_err(sqlite_error)?;
        conn.query_row(
            "SELECT value FROM kv WHERE key = ?1",
            params![storage_key],
            |row| row.get(0),
        )
        .optional()
        .map_err(sqlite_error)
    }

    fn set(&self, cf: &ColumnFamilyName, key: &Key, value: &[u8]) -> Result<(), StorageError> {
        self.write_many(&[WriteOperation::set(cf, key, value)])
    }

    fn delete(&self, cf: &ColumnFamilyName, key: &Key) -> Result<(), StorageError> {
        self.write_many(&[WriteOperation::delete(cf, key)])
    }

    fn close(&self) -> Result<(), StorageError> {
        let conn = self.conn.lock().map_err(sqlite_error)?;
        conn.pragma_update(None, "wal_checkpoint", "TRUNCATE")
            .map_err(sqlite_error)
    }

    fn approximate_class_bytes(&self, cf: &ColumnFamilyName) -> Result<Option<u64>, StorageError> {
        use rusqlite::params;

        self.check_cf(cf)?;
        let prefix = adapter_cf_prefix(cf);
        let end = prefix_upper_bound(&prefix).unwrap_or_else(|| vec![0xff]);
        let conn = self.conn.lock().map_err(sqlite_error)?;
        let bytes: i64 = conn
            .query_row(
                "SELECT COALESCE(SUM(length(key) + length(value)), 0)
                 FROM kv WHERE key >= ?1 AND key < ?2",
                params![prefix, end],
                |row| row.get(0),
            )
            .map_err(sqlite_error)?;
        Ok(Some(bytes.max(0) as u64))
    }

    fn scan_range(
        &self,
        cf: &ColumnFamilyName,
        start: &Key,
        end: &Key,
        visit: &mut ScanVisitor<'_>,
    ) -> Result<(), StorageError> {
        use rusqlite::params;

        self.check_cf(cf)?;
        let storage_start = adapter_storage_key(cf, start);
        let storage_end = adapter_storage_key(cf, end);
        let conn = self.conn.lock().map_err(sqlite_error)?;
        let mut stmt = conn
            .prepare(
                "SELECT key, value FROM kv
                 WHERE key >= ?1 AND key < ?2
                 ORDER BY key ASC",
            )
            .map_err(sqlite_error)?;
        let mut rows = stmt
            .query(params![storage_start, storage_end])
            .map_err(sqlite_error)?;
        while let Some(row) = rows.next().map_err(sqlite_error)? {
            let key: Vec<u8> = row.get(0).map_err(sqlite_error)?;
            let value: Vec<u8> = row.get(1).map_err(sqlite_error)?;
            visit(adapter_user_key(&key), &value)?;
        }
        Ok(())
    }

    fn scan_prefix(
        &self,
        cf: &ColumnFamilyName,
        prefix: &Key,
        visit: &mut ScanVisitor<'_>,
    ) -> Result<(), StorageError> {
        use rusqlite::params;

        self.check_cf(cf)?;
        let storage_prefix = adapter_storage_key(cf, prefix);
        let conn = self.conn.lock().map_err(sqlite_error)?;
        if let Some(upper) = prefix_upper_bound(&storage_prefix) {
            let mut stmt = conn
                .prepare(
                    "SELECT key, value FROM kv
                     WHERE key >= ?1 AND key < ?2
                     ORDER BY key ASC",
                )
                .map_err(sqlite_error)?;
            let mut rows = stmt
                .query(params![storage_prefix, upper])
                .map_err(sqlite_error)?;
            while let Some(row) = rows.next().map_err(sqlite_error)? {
                let key: Vec<u8> = row.get(0).map_err(sqlite_error)?;
                let value: Vec<u8> = row.get(1).map_err(sqlite_error)?;
                visit(adapter_user_key(&key), &value)?;
            }
            return Ok(());
        }

        let cf_prefix = adapter_cf_prefix(cf);
        let mut stmt = conn
            .prepare(
                "SELECT key, value FROM kv
                 WHERE key >= ?1
                 ORDER BY key ASC",
            )
            .map_err(sqlite_error)?;
        let mut rows = stmt.query(params![storage_prefix]).map_err(sqlite_error)?;
        while let Some(row) = rows.next().map_err(sqlite_error)? {
            let key: Vec<u8> = row.get(0).map_err(sqlite_error)?;
            if !key.starts_with(&cf_prefix) || !adapter_user_key(&key).starts_with(prefix) {
                break;
            }
            let value: Vec<u8> = row.get(1).map_err(sqlite_error)?;
            visit(adapter_user_key(&key), &value)?;
        }
        Ok(())
    }

    fn write_many(&self, operations: &[WriteOperation<'_>]) -> Result<(), StorageError> {
        use rusqlite::{OptionalExtension, params};

        for operation in operations {
            let cf = match operation {
                WriteOperation::Set { cf, .. }
                | WriteOperation::Delete { cf, .. }
                | WriteOperation::Delta { cf, .. } => *cf,
            };
            self.check_cf(cf)?;
        }

        let mut conn = self.conn.lock().map_err(sqlite_error)?;
        let tx = conn.transaction().map_err(sqlite_error)?;
        let mut get_stmt = tx
            .prepare_cached("SELECT value FROM kv WHERE key = ?1")
            .map_err(sqlite_error)?;
        let mut set_stmt = tx
            .prepare_cached(
                "INSERT INTO kv (key, value) VALUES (?1, ?2)
                 ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            )
            .map_err(sqlite_error)?;
        let mut delete_stmt = tx
            .prepare_cached("DELETE FROM kv WHERE key = ?1")
            .map_err(sqlite_error)?;
        for operation in operations {
            match operation {
                WriteOperation::Set { cf, key, value } => {
                    let storage_key = adapter_storage_key(cf, key);
                    set_stmt
                        .execute(params![storage_key, value])
                        .map_err(sqlite_error)?;
                }
                WriteOperation::Delete { cf, key } => {
                    let storage_key = adapter_storage_key(cf, key);
                    delete_stmt
                        .execute(params![storage_key])
                        .map_err(sqlite_error)?;
                }
                WriteOperation::Delta { cf, key, delta } => {
                    let storage_key = adapter_storage_key(cf, key);
                    let existing: Option<Vec<u8>> = get_stmt
                        .query_row(params![storage_key.as_slice()], |row| row.get(0))
                        .optional()
                        .map_err(sqlite_error)?;
                    let merged = apply_adapter_delta(existing.as_deref(), delta)?;
                    set_stmt
                        .execute(params![storage_key, merged])
                        .map_err(sqlite_error)?;
                }
            }
        }
        drop(delete_stmt);
        drop(set_stmt);
        drop(get_stmt);
        tx.commit().map_err(sqlite_error)
    }

    fn column_family_names(&self) -> Option<Vec<String>> {
        Some(self.column_families.iter().cloned().collect())
    }
}

// --- redb, used both as a raw KV engine and as a Groove storage adapter. ---

const REDB_KV: redb::TableDefinition<&[u8], &[u8]> = redb::TableDefinition::new("kv");

fn redb_error(error: impl std::fmt::Debug) -> StorageError {
    StorageError::InvalidStorageDelta(format!("redb: {error:?}"))
}

fn compact_redb_file(path: &Path) -> Result<(), StorageError> {
    let mut db = redb::Database::open(path).map_err(redb_error)?;
    while db.compact().map_err(redb_error)? {}
    Ok(())
}

pub struct RedbStorage {
    path: PathBuf,
    column_families: BTreeSet<String>,
    db: redb::Database,
}

impl RedbStorage {
    fn open(path: impl AsRef<Path>, column_families: &[&str]) -> Result<Self, StorageError> {
        Self::open_with_names(
            path.as_ref().to_path_buf(),
            column_families.iter().map(|cf| (*cf).to_owned()).collect(),
        )
    }

    fn open_with_names(
        path: PathBuf,
        column_families: BTreeSet<String>,
    ) -> Result<Self, StorageError> {
        let db = if path.exists() {
            redb::Database::open(&path).map_err(redb_error)?
        } else {
            redb::Database::create(&path).map_err(redb_error)?
        };
        {
            let mut write_txn = db.begin_write().map_err(redb_error)?;
            write_txn.set_durability(redb::Durability::Eventual);
            write_txn.open_table(REDB_KV).map_err(redb_error)?;
            write_txn.commit().map_err(redb_error)?;
        }
        Ok(Self {
            path,
            column_families,
            db,
        })
    }

    fn check_cf(&self, cf: &ColumnFamilyName) -> Result<(), StorageError> {
        if self.column_families.contains(cf) {
            Ok(())
        } else {
            Err(StorageError::ColumnFamilyNotFound(cf.to_owned()))
        }
    }
}

impl ReopenableStorage for RedbStorage {
    fn reopen(self, column_families: &[&str]) -> Result<Self, StorageError> {
        let path = self.path.clone();
        let mut names = self.column_families.clone();
        for cf in column_families {
            names.insert((*cf).to_owned());
        }
        drop(self);
        Self::open_with_names(path, names)
    }
}

impl OrderedKvStorage for RedbStorage {
    fn get(&self, cf: &ColumnFamilyName, key: &Key) -> Result<Option<Vec<u8>>, StorageError> {
        self.check_cf(cf)?;
        let storage_key = adapter_storage_key(cf, key);
        let read_txn = self.db.begin_read().map_err(redb_error)?;
        let table = read_txn.open_table(REDB_KV).map_err(redb_error)?;
        table
            .get(storage_key.as_slice())
            .map(|value| value.map(|value| value.value().to_vec()))
            .map_err(redb_error)
    }

    fn set(&self, cf: &ColumnFamilyName, key: &Key, value: &[u8]) -> Result<(), StorageError> {
        self.write_many(&[WriteOperation::set(cf, key, value)])
    }

    fn delete(&self, cf: &ColumnFamilyName, key: &Key) -> Result<(), StorageError> {
        self.write_many(&[WriteOperation::delete(cf, key)])
    }

    fn approximate_class_bytes(&self, cf: &ColumnFamilyName) -> Result<Option<u64>, StorageError> {
        self.check_cf(cf)?;
        let prefix = adapter_cf_prefix(cf);
        let end = prefix_upper_bound(&prefix).unwrap_or_else(|| vec![0xff]);
        let read_txn = self.db.begin_read().map_err(redb_error)?;
        let table = read_txn.open_table(REDB_KV).map_err(redb_error)?;
        let mut bytes = 0u64;
        for row in table
            .range(prefix.as_slice()..end.as_slice())
            .map_err(redb_error)?
        {
            let (key, value) = row.map_err(redb_error)?;
            bytes = bytes
                .saturating_add(adapter_user_key(key.value()).len() as u64)
                .saturating_add(value.value().len() as u64);
        }
        Ok(Some(bytes))
    }

    fn scan_range(
        &self,
        cf: &ColumnFamilyName,
        start: &Key,
        end: &Key,
        visit: &mut ScanVisitor<'_>,
    ) -> Result<(), StorageError> {
        self.check_cf(cf)?;
        let storage_start = adapter_storage_key(cf, start);
        let storage_end = adapter_storage_key(cf, end);
        let read_txn = self.db.begin_read().map_err(redb_error)?;
        let table = read_txn.open_table(REDB_KV).map_err(redb_error)?;
        for row in table
            .range(storage_start.as_slice()..storage_end.as_slice())
            .map_err(redb_error)?
        {
            let (key, value) = row.map_err(redb_error)?;
            visit(adapter_user_key(key.value()), value.value())?;
        }
        Ok(())
    }

    fn scan_prefix(
        &self,
        cf: &ColumnFamilyName,
        prefix: &Key,
        visit: &mut ScanVisitor<'_>,
    ) -> Result<(), StorageError> {
        self.check_cf(cf)?;
        let storage_start = adapter_storage_key(cf, prefix);
        let storage_end = prefix_upper_bound(&storage_start)
            .or_else(|| prefix_upper_bound(&adapter_cf_prefix(cf)))
            .unwrap_or_else(|| vec![0xff]);
        let read_txn = self.db.begin_read().map_err(redb_error)?;
        let table = read_txn.open_table(REDB_KV).map_err(redb_error)?;
        for row in table
            .range(storage_start.as_slice()..storage_end.as_slice())
            .map_err(redb_error)?
        {
            let (key, value) = row.map_err(redb_error)?;
            let user_key = adapter_user_key(key.value());
            if !user_key.starts_with(prefix) {
                break;
            }
            visit(user_key, value.value())?;
        }
        Ok(())
    }

    fn write_many(&self, operations: &[WriteOperation<'_>]) -> Result<(), StorageError> {
        for operation in operations {
            let cf = match operation {
                WriteOperation::Set { cf, .. }
                | WriteOperation::Delete { cf, .. }
                | WriteOperation::Delta { cf, .. } => *cf,
            };
            self.check_cf(cf)?;
        }

        use redb::ReadableTable;

        let mut write_txn = self.db.begin_write().map_err(redb_error)?;
        write_txn.set_durability(redb::Durability::Eventual);
        {
            let mut table = write_txn.open_table(REDB_KV).map_err(redb_error)?;
            for operation in operations {
                match operation {
                    WriteOperation::Set { cf, key, value } => {
                        let storage_key = adapter_storage_key(cf, key);
                        table
                            .insert(storage_key.as_slice(), *value)
                            .map_err(redb_error)?;
                    }
                    WriteOperation::Delete { cf, key } => {
                        let storage_key = adapter_storage_key(cf, key);
                        table.remove(storage_key.as_slice()).map_err(redb_error)?;
                    }
                    WriteOperation::Delta { cf, key, delta } => {
                        let storage_key = adapter_storage_key(cf, key);
                        let existing = table
                            .get(storage_key.as_slice())
                            .map_err(redb_error)?
                            .map(|value| value.value().to_vec());
                        let merged = apply_adapter_delta(existing.as_deref(), delta)?;
                        table
                            .insert(storage_key.as_slice(), merged.as_slice())
                            .map_err(redb_error)?;
                    }
                }
            }
        }
        write_txn.commit().map_err(redb_error)
    }

    fn column_family_names(&self) -> Option<Vec<String>> {
        Some(self.column_families.iter().cloned().collect())
    }
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
    // Full scan applying a value predicate — shared by the filter queries.
    let scan_pred = |pred: fn(&[u8]) -> bool| {
        db.iterator(IteratorMode::Start)
            .filter(|item| item.as_ref().map(|(_, v)| pred(v)).unwrap_or(false))
            .count()
    };
    let point = || db.get(&key0).expect("get").is_some() as usize;
    let prefix = || {
        let mut ro = ReadOptions::default();
        ro.set_iterate_upper_bound(b"AC".to_vec());
        db.iterator_opt(IteratorMode::From(b"AB", Direction::Forward), ro)
            .count()
    };
    let family = || scan_pred(pred_family_malvaceae);
    let full = || db.iterator(IteratorMode::Start).count();
    let contains_sci = || scan_pred(pred_contains_sci);
    let common = || scan_pred(pred_common_present);
    let family_set = || scan_pred(pred_family_in_set);
    // Ordered keys already sort by symbol, so a forward take(N) is the top-N.
    let top_n = || db.iterator(IteratorMode::Start).take(TOP_N).count();

    let queries = run_query_pair(&[
        ("point_by_key", &point as &dyn Fn() -> usize),
        ("prefix_scan_AB", &prefix),
        ("filter_family_Malvaceae", &family),
        ("full_scan", &full),
        ("contains_scientific_Carex", &contains_sci),
        ("common_name_present", &common),
        ("family_in_set", &family_set),
        ("top_100_by_symbol", &top_n),
    ]);
    drop(db);

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

/// Run each `(name, exec)` query cold (first call) then warm (second call).
fn run_query_pair(specs: &[(&str, &dyn Fn() -> usize)]) -> Vec<QueryReport> {
    specs
        .iter()
        .map(|(name, exec)| {
            let (cold_ms, rows) = timed(exec);
            let (warm_ms, _) = timed(exec);
            QueryReport {
                name: (*name).to_owned(),
                rows,
                cold_ms: Some(cold_ms),
                warm_ms,
            }
        })
        .collect()
}

// --- SlateDB, via the `slatedb` crate directly (async). ---

fn run_slatedb_raw(plants: &[Plant], batch_size: usize, dir: &Path) -> Report {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    rt.block_on(slatedb_raw(plants, batch_size, dir))
}

async fn slatedb_raw(plants: &[Plant], batch_size: usize, dir: &Path) -> Report {
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
    let db = slatedb::Db::builder("bench", store.clone())
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

    let t_close = Instant::now();
    db.flush().await.expect("flush slatedb");
    db.close().await.expect("close slatedb");
    let flush_close_time = t_close.elapsed();
    let physical_bytes = Some(dir_size(dir));

    // Cold reopen.
    let t_open = Instant::now();
    let db = slatedb::Db::builder("bench", store.clone())
        .build()
        .await
        .expect("reopen slatedb");
    let cold_open_time = Some(t_open.elapsed());

    let key0 = plant_key(&plants[0]);
    let full_range = vec![0u8]..vec![0xffu8];
    let prefix_range = b"AB".to_vec()..b"AC".to_vec();

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

    let mut queries = Vec::new();
    // Time an async query expression cold (first eval) then warm (second eval).
    macro_rules! timed_query {
        ($name:expr, $body:expr) => {{
            let t = Instant::now();
            let rows = $body;
            let cold = t.elapsed().as_secs_f64() * 1e3;
            let t = Instant::now();
            let _ = $body;
            let warm = t.elapsed().as_secs_f64() * 1e3;
            queries.push(QueryReport {
                name: $name.to_owned(),
                rows,
                cold_ms: Some(cold),
                warm_ms: warm,
            });
        }};
    }
    timed_query!(
        "point_by_key",
        db.get(&key0).await.expect("get").is_some() as usize
    );
    timed_query!(
        "prefix_scan_AB",
        count_scan(&db, prefix_range.clone()).await
    );
    timed_query!(
        "filter_family_Malvaceae",
        count_where(&db, full_range.clone(), pred_family_malvaceae).await
    );
    timed_query!("full_scan", count_scan(&db, full_range.clone()).await);
    timed_query!(
        "contains_scientific_Carex",
        count_where(&db, full_range.clone(), pred_contains_sci).await
    );
    timed_query!(
        "common_name_present",
        count_where(&db, full_range.clone(), pred_common_present).await
    );
    timed_query!(
        "family_in_set",
        count_where(&db, full_range.clone(), pred_family_in_set).await
    );
    timed_query!(
        "top_100_by_symbol",
        count_take_n(&db, full_range.clone(), TOP_N).await
    );

    db.close().await.expect("close cold slatedb");

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

fn run_sqlite_raw(plants: &[Plant], batch_size: usize, dir: &Path) -> Report {
    use rusqlite::{OptionalExtension, params};

    let path = dir.join("raw.sqlite");
    let mut conn = rusqlite::Connection::open(&path).expect("open raw sqlite");
    configure_sqlite(&conn).expect("configure raw sqlite");
    conn.execute_batch(
        "CREATE TABLE kv (
            key BLOB PRIMARY KEY,
            value BLOB NOT NULL
        ) WITHOUT ROWID;",
    )
    .expect("create raw sqlite table");

    let raw_input_bytes = plants.iter().map(|p| p.raw_bytes() as u64).sum();
    let t_write = Instant::now();
    for chunk in plants.chunks(batch_size) {
        let tx = conn.transaction().expect("raw sqlite transaction");
        {
            let mut stmt = tx
                .prepare("INSERT OR REPLACE INTO kv (key, value) VALUES (?1, ?2)")
                .expect("raw sqlite insert statement");
            for plant in chunk {
                stmt.execute(params![plant_key(plant), plant_value(plant)])
                    .expect("raw sqlite insert");
            }
        }
        tx.commit().expect("raw sqlite commit");
    }
    let write_time = t_write.elapsed();

    let t_close = Instant::now();
    conn.pragma_update(None, "wal_checkpoint", "TRUNCATE")
        .expect("raw sqlite checkpoint");
    let flush_close_time = t_close.elapsed();
    drop(conn);
    let physical_bytes = Some(dir_size(dir));

    let t_open = Instant::now();
    let conn = rusqlite::Connection::open(&path).expect("reopen raw sqlite");
    configure_sqlite(&conn).expect("configure reopened raw sqlite");
    let cold_open_time = Some(t_open.elapsed());
    let key0 = plant_key(&plants[0]);

    fn sqlite_count_scan_where(conn: &rusqlite::Connection, pred: fn(&[u8]) -> bool) -> usize {
        let mut stmt = conn
            .prepare("SELECT value FROM kv")
            .expect("raw sqlite scan statement");
        let rows = stmt
            .query_map([], |row| row.get::<_, Vec<u8>>(0))
            .expect("raw sqlite scan");
        rows.filter_map(Result::ok)
            .filter(|value| pred(value))
            .count()
    }

    let queries = vec![
        timed_query_report("point_by_key", || {
            conn.query_row(
                "SELECT value FROM kv WHERE key = ?1",
                params![key0.as_slice()],
                |row| row.get::<_, Vec<u8>>(0),
            )
            .optional()
            .expect("raw sqlite point")
            .is_some() as usize
        }),
        timed_query_report("prefix_scan_AB", || {
            conn.query_row(
                "SELECT count(*) FROM kv WHERE key >= ?1 AND key < ?2",
                params![b"AB".as_slice(), b"AC".as_slice()],
                |row| row.get::<_, i64>(0),
            )
            .expect("raw sqlite prefix") as usize
        }),
        timed_query_report("filter_family_Malvaceae", || {
            sqlite_count_scan_where(&conn, pred_family_malvaceae)
        }),
        timed_query_report("full_scan", || {
            conn.query_row("SELECT count(*) FROM kv", [], |row| row.get::<_, i64>(0))
                .expect("raw sqlite full scan") as usize
        }),
        timed_query_report("contains_scientific_Carex", || {
            sqlite_count_scan_where(&conn, pred_contains_sci)
        }),
        timed_query_report("common_name_present", || {
            sqlite_count_scan_where(&conn, pred_common_present)
        }),
        timed_query_report("family_in_set", || {
            sqlite_count_scan_where(&conn, pred_family_in_set)
        }),
        timed_query_report("top_100_by_symbol", || {
            conn.query_row(
                &format!("SELECT count(*) FROM (SELECT key FROM kv ORDER BY key LIMIT {TOP_N})"),
                [],
                |row| row.get::<_, i64>(0),
            )
            .expect("raw sqlite top n") as usize
        }),
    ];

    Report {
        adapter: "raw:sqlite".to_owned(),
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

fn run_redb_raw(plants: &[Plant], batch_size: usize, dir: &Path) -> Report {
    let path = dir.join("raw.redb");
    let db = redb::Database::create(&path).expect("create raw redb");
    {
        let mut write_txn = db.begin_write().expect("raw redb init write transaction");
        write_txn.set_durability(redb::Durability::Eventual);
        write_txn.open_table(REDB_KV).expect("raw redb open table");
        write_txn.commit().expect("raw redb init commit");
    }

    let raw_input_bytes = plants.iter().map(|p| p.raw_bytes() as u64).sum();
    let t_write = Instant::now();
    for chunk in plants.chunks(batch_size) {
        let mut write_txn = db.begin_write().expect("raw redb write transaction");
        write_txn.set_durability(redb::Durability::Eventual);
        {
            let mut table = write_txn.open_table(REDB_KV).expect("raw redb table");
            for plant in chunk {
                let key = plant_key(plant);
                let value = plant_value(plant);
                table
                    .insert(key.as_slice(), value.as_slice())
                    .expect("raw redb insert");
            }
        }
        write_txn.commit().expect("raw redb commit");
    }
    let write_time = t_write.elapsed();

    let t_close = Instant::now();
    drop(db);
    let flush_close_time = t_close.elapsed();
    compact_redb_file(&path).expect("compact raw redb");
    let physical_bytes = Some(dir_size(dir));

    let t_open = Instant::now();
    let db = redb::Database::open(&path).expect("reopen raw redb");
    let cold_open_time = Some(t_open.elapsed());
    let key0 = plant_key(&plants[0]);

    fn redb_count_scan_where(db: &redb::Database, pred: fn(&[u8]) -> bool) -> usize {
        let read_txn = db.begin_read().expect("raw redb scan transaction");
        let table = read_txn.open_table(REDB_KV).expect("raw redb scan table");
        table
            .range::<&[u8]>(..)
            .expect("raw redb scan")
            .filter_map(Result::ok)
            .filter(|(_, value)| pred(value.value()))
            .count()
    }

    fn redb_count_range(db: &redb::Database, start: &[u8], end: &[u8]) -> usize {
        let read_txn = db.begin_read().expect("raw redb range transaction");
        let table = read_txn.open_table(REDB_KV).expect("raw redb range table");
        table
            .range(start..end)
            .expect("raw redb range")
            .filter_map(Result::ok)
            .count()
    }

    let queries = vec![
        timed_query_report("point_by_key", || {
            let read_txn = db.begin_read().expect("raw redb point transaction");
            let table = read_txn.open_table(REDB_KV).expect("raw redb point table");
            table
                .get(key0.as_slice())
                .expect("raw redb point")
                .is_some() as usize
        }),
        timed_query_report("prefix_scan_AB", || redb_count_range(&db, b"AB", b"AC")),
        timed_query_report("filter_family_Malvaceae", || {
            redb_count_scan_where(&db, pred_family_malvaceae)
        }),
        timed_query_report("full_scan", || redb_count_scan_where(&db, |_| true)),
        timed_query_report("contains_scientific_Carex", || {
            redb_count_scan_where(&db, pred_contains_sci)
        }),
        timed_query_report("common_name_present", || {
            redb_count_scan_where(&db, pred_common_present)
        }),
        timed_query_report("family_in_set", || {
            redb_count_scan_where(&db, pred_family_in_set)
        }),
        timed_query_report("top_100_by_symbol", || {
            let read_txn = db.begin_read().expect("raw redb top transaction");
            let table = read_txn.open_table(REDB_KV).expect("raw redb top table");
            table
                .range::<&[u8]>(..)
                .expect("raw redb top")
                .take(TOP_N)
                .filter_map(Result::ok)
                .count()
        }),
    ];

    Report {
        adapter: "raw:redb".to_owned(),
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

fn run_postgres_raw(plants: &[Plant], batch_size: usize, conn_str: &str) -> Report {
    use postgres::binary_copy::BinaryCopyInWriter;
    use postgres::types::Type;

    let table = postgres_table_name("raw_pg_kv");
    let table_ident = pg_ident(&table);
    PostgresStorage::reset_table(conn_str, &table).expect("reset raw postgres table");
    let mut client =
        postgres::Client::connect(conn_str, postgres::NoTls).expect("connect postgres");
    client
        .batch_execute(&format!(
            "CREATE TABLE {table_ident} (
                key BYTEA PRIMARY KEY,
                value BYTEA NOT NULL
            )"
        ))
        .expect("create raw postgres table");

    let raw_input_bytes = plants.iter().map(|p| p.raw_bytes() as u64).sum();
    let t_write = Instant::now();
    {
        let sink = client
            .copy_in(&format!(
                "COPY {table_ident} (key, value) FROM STDIN BINARY"
            ))
            .expect("postgres copy in");
        let mut writer = BinaryCopyInWriter::new(sink, &[Type::BYTEA, Type::BYTEA]);
        for plant in plants {
            let key = plant_key(plant);
            let value = plant_value(plant);
            writer.write(&[&key, &value]).expect("postgres copy row");
        }
        writer.finish().expect("finish postgres copy");
    }
    let write_time = t_write.elapsed();

    let t_close = Instant::now();
    client
        .batch_execute("CHECKPOINT")
        .expect("postgres checkpoint");
    let flush_close_time = t_close.elapsed();
    drop(client);
    let physical_bytes = Some(
        PostgresStorage::physical_bytes(conn_str, &table).expect("raw postgres physical size"),
    );

    let t_open = Instant::now();
    let mut client =
        postgres::Client::connect(conn_str, postgres::NoTls).expect("reconnect postgres");
    let cold_open_time = Some(t_open.elapsed());
    let key0 = plant_key(&plants[0]);

    fn pg_count_scan_where(
        client: &mut postgres::Client,
        table_ident: &str,
        pred: fn(&[u8]) -> bool,
    ) -> usize {
        client
            .query(&format!("SELECT value FROM {table_ident}"), &[])
            .expect("postgres scan")
            .into_iter()
            .filter(|row| {
                let value: Vec<u8> = row.get(0);
                pred(&value)
            })
            .count()
    }

    let queries = vec![
        timed_query_report("point_by_key", || {
            client
                .query_opt(
                    &format!("SELECT value FROM {table_ident} WHERE key = $1"),
                    &[&key0.as_slice()],
                )
                .expect("postgres point")
                .is_some() as usize
        }),
        timed_query_report("prefix_scan_AB", || {
            let row = client
                .query_one(
                    &format!(
                        "SELECT count(*)::bigint FROM {table_ident} WHERE key >= $1 AND key < $2"
                    ),
                    &[&b"AB".as_slice(), &b"AC".as_slice()],
                )
                .expect("postgres prefix");
            let rows: i64 = row.get(0);
            rows as usize
        }),
        timed_query_report("filter_family_Malvaceae", || {
            pg_count_scan_where(&mut client, &table_ident, pred_family_malvaceae)
        }),
        timed_query_report("full_scan", || {
            let row = client
                .query_one(&format!("SELECT count(*)::bigint FROM {table_ident}"), &[])
                .expect("postgres full scan");
            let rows: i64 = row.get(0);
            rows as usize
        }),
        timed_query_report("contains_scientific_Carex", || {
            pg_count_scan_where(&mut client, &table_ident, pred_contains_sci)
        }),
        timed_query_report("common_name_present", || {
            pg_count_scan_where(&mut client, &table_ident, pred_common_present)
        }),
        timed_query_report("family_in_set", || {
            pg_count_scan_where(&mut client, &table_ident, pred_family_in_set)
        }),
        timed_query_report("top_100_by_symbol", || {
            let row = client
                .query_one(
                    &format!(
                        "SELECT count(*)::bigint FROM \
                         (SELECT key FROM {table_ident} ORDER BY key ASC LIMIT {TOP_N}) rows"
                    ),
                    &[],
                )
                .expect("postgres top n");
            let rows: i64 = row.get(0);
            rows as usize
        }),
    ];

    Report {
        adapter: "raw:postgres".to_owned(),
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

fn dispatch_raw(engine: &str, plants: &[Plant], batch_size: usize) -> Report {
    let dir = tempfile::tempdir().expect("tempdir");
    match engine {
        "rocksdb" => run_rocksdb_raw(plants, batch_size, dir.path()),
        "slatedb" => run_slatedb_raw(plants, batch_size, dir.path()),
        "sqlite" => run_sqlite_raw(plants, batch_size, dir.path()),
        "redb" => run_redb_raw(plants, batch_size, dir.path()),
        "postgres" => run_postgres_raw(plants, batch_size, &postgres_url()),
        other => {
            panic!("unknown raw engine '{other}' (expected rocksdb|slatedb|sqlite|redb|postgres)")
        }
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
                "    {:<28} {:>7} rows   cold {:>8.3} ms   warm {:>8.3} ms",
                q.name, q.rows, cold, q.warm_ms
            ),
            None => println!(
                "    {:<28} {:>7} rows   warm {:>8.3} ms",
                q.name, q.rows, q.warm_ms
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
            "{{\"name\":\"{}\",\"rows\":{},\"cold_ms\":{},\"warm_ms\":{:.3}}}",
            q.name, q.rows, cold, q.warm_ms
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
    ebs_jitter: SimulatedLatency,
    safekeeper_jitter: SafekeeperJitter,
    json: bool,
}

fn parse_args() -> Args {
    let default_input = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("data/plantlst.txt");
    let mut args = Args {
        storage: Vec::new(),
        raw: Vec::new(),
        input: default_input,
        batch_size: 1000,
        limit: None,
        ebs_jitter: SimulatedLatency::default(),
        safekeeper_jitter: SafekeeperJitter::default(),
        json: false,
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
            "--ebs-delay-ms" => {
                args.ebs_jitter.base_ms = it
                    .next()
                    .expect("--ebs-delay-ms needs a number")
                    .parse()
                    .expect("--ebs-delay-ms must be a number");
            }
            "--ebs-jitter-ms" => {
                args.ebs_jitter.jitter_ms = it
                    .next()
                    .expect("--ebs-jitter-ms needs a number")
                    .parse()
                    .expect("--ebs-jitter-ms must be a number");
            }
            "--safekeeper-delay-ms" => {
                args.safekeeper_jitter.base_ms = it
                    .next()
                    .expect("--safekeeper-delay-ms needs a number")
                    .parse()
                    .expect("--safekeeper-delay-ms must be a number");
            }
            "--safekeeper-jitter-ms" => {
                args.safekeeper_jitter.jitter_ms = it
                    .next()
                    .expect("--safekeeper-jitter-ms needs a number")
                    .parse()
                    .expect("--safekeeper-jitter-ms must be a number");
            }
            "--json" => args.json = true,
            "-h" | "--help" => {
                eprintln!(
                    "jazz-ingest-bench — storage ingestion/cold-load benchmark (USDA plants)\n\
                     \n\
                     Two layers on the same dataset and metrics:\n\
                     \x20 --storage <list>  through the Jazz Db API: memory,rocksdb,btree,slatedb,slatedb-localwal,slatedb-localwal-sync,sqlite,redb,postgres\n\
                     \x20 --raw <list>      direct native engine API (no Jazz): rocksdb,slatedb,sqlite,redb,postgres\n\
                     (with neither flag, defaults to --raw rocksdb,slatedb)\n\
                     \n\
                     Options:\n\
                     \x20 --input <path>    CSV dataset (default bundled USDA plantlst.txt)\n\
                     \x20 --batch-size <n>  rows per transaction/write-batch (default 1000)\n\
                     \x20 --limit <n>       ingest only the first n rows\n\
                     \x20 --ebs-delay-ms <n>\n\
                     \x20                   fixed local WAL fsync delay for slatedb-localwal-sync (default 0)\n\
                     \x20 --ebs-jitter-ms <n>\n\
                     \x20                   deterministic 0..n ms local WAL fsync jitter per sync batch (default 0)\n\
                     \x20 --safekeeper-delay-ms <n>\n\
                     \x20                   fixed remote ack delay for slatedb-localwal-sync (default 0)\n\
                     \x20 --safekeeper-jitter-ms <n>\n\
                     \x20                   deterministic 0..n ms remote ack jitter per sync batch (default 0)\n\
                     \x20 --json            also emit one machine-readable JSON line per run\n\
                     \n\
                     Postgres uses JAZZ_INGEST_POSTGRES_URL, defaulting to localhost:55432."
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
            let mut report = dispatch(
                adapter,
                &plants,
                args.batch_size,
                args.ebs_jitter,
                args.safekeeper_jitter,
            );
            report.adapter = label.clone();
            report
        }));
        match result {
            Ok(report) => emit(&report),
            Err(_) => eprintln!("\n═══ {label} ═══\n  FAILED (see panic above)"),
        }
    }

    for engine in &args.raw {
        let label = format!("raw:{engine}");
        let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            dispatch_raw(engine, &plants, args.batch_size)
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
