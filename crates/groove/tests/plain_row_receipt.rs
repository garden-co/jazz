//! Manual plain-row RocksDB receipt. Run with:
//! `cargo test -p groove --test plain_row_receipt -- --ignored --nocapture`.
//!
//! The output is intentionally raw: retain it with the command, git revision,
//! and host metadata (`uname -a`, CPU governor, and available memory).  The
//! RocksDB snapshot is backend attribution, not process allocation accounting;
//! its memtable number excludes Groove/Rust allocations and the shared cache.

use std::time::Instant;

use groove::db::Database;
use groove::records::{Value, VersionedRecord};
use groove::schema::{ColumnSchema, ColumnType, DatabaseSchema, IntegerKeyType, PrimaryKey, TableSchema};
use groove::storage::{ReopenableStorage, RocksDbStorage};

const ROWS: u64 = 1_000;
const UPDATES: u64 = 1_000;

fn schema() -> DatabaseSchema {
    DatabaseSchema::new([TableSchema::new("rows", [
        ColumnSchema::new("id", ColumnType::U64),
        ColumnSchema::new("body", ColumnType::String),
    ]).with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))])
}

fn row(id: u64, revision: u64) -> VersionedRecord {
    let descriptor = schema().table("rows").unwrap().record_schema_for_version(0).unwrap();
    VersionedRecord::create(0, descriptor, &[Value::U64(id), Value::String(format!("row-{id}-revision-{revision}"))]).unwrap()
}

#[test]
#[ignore = "manual storage receipt; noisy and host-specific"]
fn plain_row_write_point_history_scan_and_reopen_receipt() -> Result<(), Box<dyn std::error::Error>> {
    jazz_benchmark_guard::refuse_contaminated_measurement();
    let dir = tempfile::tempdir()?;
    let schema = schema();
    let storage = RocksDbStorage::open(dir.path(), &schema.column_families())?;
    let mut db = Database::new(schema.clone(), storage)?;
    let write_started = Instant::now();
    let mut checksum = 0u64;
    for revision in 0..2 {
        let count = if revision == 0 { ROWS } else { UPDATES };
        for id in 0..count {
            let mut batch = db.open_batch();
            if revision == 0 {
                batch.insert("rows", row(id, revision));
            } else {
                batch.update("rows", row(id, revision));
            }
            db.commit_batch(batch)?;
            checksum = checksum.wrapping_add(id ^ revision);
        }
    }
    let write = write_started.elapsed();
    db.reset_storage_read_metrics();
    let point_started = Instant::now();
    for id in 0..ROWS { assert!(db.primary_key_get("rows", &[Value::U64(id)])?.is_some()); checksum = checksum.wrapping_add(id); }
    let point = point_started.elapsed();
    let point_reads = db.take_storage_read_metrics();
    db.reset_storage_read_metrics();
    let scan_started = Instant::now();
    let scanned = db.primary_key_scan("rows", &[])?.len();
    let scan = scan_started.elapsed();
    let scan_reads = db.take_storage_read_metrics();
    assert_eq!(scanned as u64, ROWS, "plain-row scan must see every final row");
    let storage = db.into_storage();
    let before = storage.metrics()?;
    let reopen_started = Instant::now();
    let storage = storage.reopen(&schema.column_families())?;
    let reopen = reopen_started.elapsed();
    let after = storage.metrics()?;
    let disk_bytes = std::fs::read_dir(dir.path())?.filter_map(Result::ok).filter_map(|e| e.metadata().ok()).map(|m| m.len()).sum::<u64>();
    println!("plain_row_receipt rows={ROWS} updates={UPDATES} writes={} checksum={checksum} write_ms={} point_ms={} point_reads={:?} scan_ms={} scan_rows={scanned} scan_reads={:?} reopen_ms={} disk_bytes={} metrics_before={before:?} metrics_after={after:?}", ROWS + UPDATES, write.as_secs_f64()*1e3, point.as_secs_f64()*1e3, point_reads, scan.as_secs_f64()*1e3, scan_reads, reopen.as_secs_f64()*1e3, disk_bytes);
    Ok(())
}
