//! Manual plain-row RocksDB receipt. Its JSON line is intended to be captured
//! by `dev/benchmarks/run-receipt.mjs`.

use std::cell::Cell;
use std::fs;
use std::path::Path;
use std::time::Instant;

use groove::db::Database;
use groove::records::{Value, VersionedRecord};
use groove::schema::{
    ColumnSchema, ColumnType, DatabaseSchema, IntegerKeyType, PrimaryKey, TableSchema,
};
use groove::storage::{OrderedKvStorage, RocksDbStorage};
use serde_json::json;

const ROWS: u64 = 1_000;
const UPDATES: u64 = 1_000;

fn schema() -> DatabaseSchema {
    DatabaseSchema::new([TableSchema::new(
        "rows",
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("body", ColumnType::String),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))])
}

fn row(id: u64, revision: u64) -> VersionedRecord {
    let schema = schema();
    let descriptor = schema
        .table("rows")
        .unwrap()
        .record_schema_for_version(0)
        .unwrap();
    VersionedRecord::create(
        0,
        descriptor,
        &[
            Value::U64(id),
            Value::String(format!("row-{id}-revision-{revision}")),
        ],
    )
    .unwrap()
}

fn checksum_record(record: &VersionedRecord) -> Result<u64, groove::records::Error> {
    let mut hash = 0xcbf29ce484222325u64;
    for value in record.to_values()? {
        let bytes = match value {
            Value::U64(value) => value.to_le_bytes().to_vec(),
            Value::String(value) => value.into_bytes(),
            other => format!("{other:?}").into_bytes(),
        };
        for byte in bytes {
            hash ^= u64::from(byte);
            hash = hash.wrapping_mul(0x100000001b3);
        }
    }
    Ok(hash)
}

fn recursive_disk_bytes(path: &Path) -> std::io::Result<u64> {
    fs::read_dir(path)?.try_fold(0u64, |total, entry| {
        let entry = entry?;
        let metadata = entry.metadata()?;
        let bytes = if metadata.is_dir() {
            recursive_disk_bytes(&entry.path())?
        } else {
            metadata.len()
        };
        Ok(total.saturating_add(bytes))
    })
}

fn fresh_open_after_exclusive_drop(
    storage: RocksDbStorage,
    path: &Path,
    column_families: &[&str],
) -> Result<(RocksDbStorage, usize), Box<dyn std::error::Error>> {
    let fresh_open_attempts = Cell::new(0usize);
    let reopened = fresh_open_after_exclusive_drop_with(storage, path, column_families, || {
        fresh_open_attempts.set(fresh_open_attempts.get() + 1);
        RocksDbStorage::open(path, column_families)
    })?;
    Ok((reopened, fresh_open_attempts.get()))
}

fn fresh_open_after_exclusive_drop_with(
    storage: RocksDbStorage,
    path: &Path,
    column_families: &[&str],
    mut fresh_open: impl FnMut() -> Result<RocksDbStorage, groove::storage::Error>,
) -> Result<RocksDbStorage, Box<dyn std::error::Error>> {
    assert!(
        RocksDbStorage::open(path, column_families).is_err(),
        "RocksDB must reject a competing open while the original handle is alive"
    );
    storage.close()?;
    drop(storage);

    Ok(fresh_open()?)
}

#[test]
#[ignore = "manual storage receipt; noisy and host-specific"]
fn plain_row_write_point_scan_and_reopen_receipt() -> Result<(), Box<dyn std::error::Error>> {
    jazz_benchmark_guard::refuse_contaminated_measurement();
    let dir = tempfile::tempdir()?;
    let schema = schema();
    let column_families = schema.column_families();
    let storage = RocksDbStorage::open(dir.path(), &column_families)?;
    let mut db = Database::new(schema.clone(), storage)?;

    let write_started = Instant::now();
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
        }
    }
    let write = write_started.elapsed();

    db.reset_storage_read_metrics();
    let point_started = Instant::now();
    let mut checksum_before = 0u64;
    for id in 0..ROWS {
        let record = db
            .primary_key_get("rows", &[Value::U64(id)])?
            .expect("updated row exists");
        assert_eq!(
            record.to_values()?[1],
            Value::String(format!("row-{id}-revision-1"))
        );
        checksum_before = checksum_before.wrapping_add(checksum_record(&record)?);
    }
    let point = point_started.elapsed();
    let point_reads = db.take_storage_read_metrics();

    db.reset_storage_read_metrics();
    let scan_started = Instant::now();
    let scanned = db.primary_key_scan("rows", &[])?;
    let scan = scan_started.elapsed();
    let scan_reads = db.take_storage_read_metrics();
    assert_eq!(scanned.len() as u64, ROWS);
    let scan_checksum = scanned.iter().try_fold(0u64, |sum, record| {
        checksum_record(record).map(|hash| sum.wrapping_add(hash))
    })?;
    assert_eq!(scan_checksum, checksum_before);

    let storage = db.into_storage();
    let before = storage.metrics()?;

    let reopen_started = Instant::now();
    let (storage, fresh_open_attempts) =
        fresh_open_after_exclusive_drop(storage, dir.path(), &column_families)?;
    assert_eq!(fresh_open_attempts, 1);
    let reopened = Database::new(schema, storage)?;
    let reopen = reopen_started.elapsed();
    let disk_bytes = recursive_disk_bytes(dir.path())?;
    let mut checksum_after = 0u64;
    for id in 0..ROWS {
        let record = reopened
            .primary_key_get("rows", &[Value::U64(id)])?
            .expect("row survives fresh open");
        assert_eq!(
            record.to_values()?[1],
            Value::String(format!("row-{id}-revision-1"))
        );
        checksum_after = checksum_after.wrapping_add(checksum_record(&record)?);
    }
    assert_eq!(checksum_after, checksum_before);
    let after = reopened.into_storage().metrics()?;

    println!(
        "{}",
        json!({
            "scenario": "groove_plain_row",
            "phase": "plain_row_receipt",
            "config": {"rows": ROWS, "updates": UPDATES, "durability": "wal_no_sync", "timing_includes_metered_storage": true},
            "operation_counts": {"inserts": ROWS, "updates": UPDATES, "point_reads_before": ROWS, "scan_rows": scanned.len(), "point_reads_after": ROWS},
            "checksum_hex": format!("{checksum_after:016x}"),
            "write_us": write.as_micros(),
            "point_read_us": point.as_micros(),
            "scan_us": scan.as_micros(),
            "reopen_and_database_init_us": reopen.as_micros(),
            "fresh_open_attempts": fresh_open_attempts,
            "disk_bytes_recursive": disk_bytes,
            "logical_point_reads": format!("{point_reads:?}"),
            "logical_scan_reads": format!("{scan_reads:?}"),
            "rocksdb_before_close": before,
            "rocksdb_after_fresh_open": after,
            "memory_caveat": "memtable excludes shared block cache and Rust/process allocations"
        })
    );
    Ok(())
}

#[test]
fn recursive_disk_bytes_counts_nested_files() {
    let dir = tempfile::tempdir().unwrap();
    fs::create_dir(dir.path().join("nested")).unwrap();
    fs::write(dir.path().join("root"), [0; 3]).unwrap();
    fs::write(dir.path().join("nested/child"), [0; 5]).unwrap();
    assert_eq!(recursive_disk_bytes(dir.path()).unwrap(), 8);
}

#[test]
fn checksum_is_sensitive_to_returned_row_content() {
    assert_ne!(
        checksum_record(&row(7, 0)).unwrap(),
        checksum_record(&row(7, 1)).unwrap()
    );
}

#[test]
fn fresh_open_requires_dropping_the_original_exclusive_handle() {
    let dir = tempfile::tempdir().unwrap();
    let column_families = vec!["records"];
    let storage = RocksDbStorage::open(dir.path(), &["records"]).unwrap();
    storage.set("records", b"key", b"persisted").unwrap();

    let attempts = Cell::new(0usize);
    let reopened =
        fresh_open_after_exclusive_drop_with(storage, dir.path(), &column_families, || {
            attempts.set(attempts.get() + 1);
            RocksDbStorage::open(dir.path(), &column_families)
        })
        .unwrap();
    assert_eq!(
        attempts.get(),
        1,
        "fresh open callback must run exactly once"
    );
    assert_eq!(
        reopened.get("records", b"key").unwrap().as_deref(),
        Some(b"persisted".as_slice())
    );
}
