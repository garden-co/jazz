//! Manual plain-row RocksDB receipt. Its JSON line is intended to be captured
//! by the benchmark harness that produced it.

use std::cell::Cell;
use std::fs;
use std::path::Path;
use std::time::Instant;

use groove::db::Database;
use groove::records::{Value, VariantRecord};
use groove::schema::{
    ColumnSchema, ColumnType, DatabaseSchema, IntegerKeyType, PrimaryKey, TableSchema,
};
use groove::storage::{OrderedKvStorage, OwnedWriteOperation};
use jazz_storage_rocksdb::RocksDbStorage;
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

fn epoch_1_codec_fixture_schema() -> DatabaseSchema {
    // Declaration order is deliberately variable-before-fixed. The frozen
    // stored bytes below prove that every durable backend shares Groove's
    // physical fixed-first record layout without changing public row order.
    DatabaseSchema::new([TableSchema::new(
        "records",
        [
            ColumnSchema::new("label", ColumnType::String),
            ColumnSchema::new("id", ColumnType::U16),
            ColumnSchema::new("enabled", ColumnType::Bool),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U16))])
}

fn row(id: u64, revision: u64) -> VariantRecord {
    let schema = schema();
    let descriptor = schema.table("rows").unwrap().record_schema();
    VariantRecord::create(
        0,
        descriptor,
        &[
            Value::U64(id),
            Value::String(format!("row-{id}-revision-{revision}")),
        ],
    )
    .unwrap()
}

fn checksum_record(record: &VariantRecord) -> Result<u64, groove::records::Error> {
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

async fn fresh_open_after_exclusive_drop(
    storage: RocksDbStorage,
    path: &Path,
    column_families: &[&str],
) -> Result<(RocksDbStorage, usize), Box<dyn std::error::Error>> {
    let fresh_open_attempts = Cell::new(0usize);
    let reopened = fresh_open_after_exclusive_drop_with(storage, path, column_families, || {
        fresh_open_attempts.set(fresh_open_attempts.get() + 1);
        RocksDbStorage::open(path, column_families)
    })
    .await?;
    Ok((reopened, fresh_open_attempts.get()))
}

async fn fresh_open_after_exclusive_drop_with(
    storage: RocksDbStorage,
    path: &Path,
    column_families: &[&str],
    mut fresh_open: impl FnMut() -> Result<RocksDbStorage, groove::storage::Error>,
) -> Result<RocksDbStorage, Box<dyn std::error::Error>> {
    assert!(
        RocksDbStorage::open(path, column_families).is_err(),
        "RocksDB must reject a competing open while the original handle is alive"
    );
    storage.close().await?;
    drop(storage);

    Ok(fresh_open()?)
}

#[futures_test::test]
#[ignore = "#1787: manual storage receipt; noisy and host-specific"]
async fn plain_row_write_point_scan_and_reopen_receipt() -> Result<(), Box<dyn std::error::Error>> {
    jazz_benchmark_guard::refuse_contaminated_measurement();
    let dir = tempfile::tempdir()?;
    let schema = schema();
    let column_families = schema.column_families();
    let storage = RocksDbStorage::open(dir.path(), &column_families)?;
    let mut db = Database::new(schema.clone(), storage).await?;

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
            let applied = db.apply_batch(batch).await?;
            let persisted = applied.persist().await;
            db.finish_persistence(persisted)?;
        }
    }
    let write = write_started.elapsed();

    db.reset_storage_read_metrics();
    let point_started = Instant::now();
    let mut checksum_before = 0u64;
    for id in 0..ROWS {
        let record = db
            .primary_key_get("rows", &[Value::U64(id)])
            .await?
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
    let scanned = db.primary_key_scan("rows", &[]).await?;
    let scan = scan_started.elapsed();
    let scan_reads = db.take_storage_read_metrics();
    assert_eq!(scanned.len() as u64, ROWS);
    let scan_checksum = scanned.iter().try_fold(0u64, |sum, record| {
        checksum_record(record).map(|hash| sum.wrapping_add(hash))
    })?;
    assert_eq!(scan_checksum, checksum_before);

    db.close().await?;
    drop(db);
    let storage = RocksDbStorage::open(dir.path(), &column_families)?;
    let before = storage.metrics()?;

    let reopen_started = Instant::now();
    let (storage, fresh_open_attempts) =
        fresh_open_after_exclusive_drop(storage, dir.path(), &column_families).await?;
    assert_eq!(fresh_open_attempts, 1);
    let reopened = Database::new(schema.clone(), storage).await?;
    let reopen = reopen_started.elapsed();
    let disk_bytes = recursive_disk_bytes(dir.path())?;
    let mut checksum_after = 0u64;
    for id in 0..ROWS {
        let record = reopened
            .primary_key_get("rows", &[Value::U64(id)])
            .await?
            .expect("row survives fresh open");
        assert_eq!(
            record.to_values()?[1],
            Value::String(format!("row-{id}-revision-1"))
        );
        checksum_after = checksum_after.wrapping_add(checksum_record(&record)?);
    }
    assert_eq!(checksum_after, checksum_before);
    reopened.close().await?;
    drop(reopened);
    let storage = RocksDbStorage::open(dir.path(), &column_families)?;
    let after = storage.metrics()?;
    storage.close().await?;

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

#[futures_test::test]
async fn epoch_1_table_row_bytes_and_order_survive_a_fresh_rocksdb_open() {
    let directory = tempfile::tempdir().unwrap();
    let schema = epoch_1_codec_fixture_schema();
    let mut database = Database::new(
        schema.clone(),
        RocksDbStorage::open(directory.path(), &["records"]).unwrap(),
    )
    .await
    .unwrap();
    let mut batch = database.open_batch();
    batch.insert(
        "records",
        vec![
            Value::String("a".to_owned()),
            Value::U16(2),
            Value::Bool(false),
        ],
    );
    batch.insert(
        "records",
        vec![
            Value::String("hi".to_owned()),
            Value::U16(0x1234),
            Value::Bool(true),
        ],
    );
    let applied = database.apply_batch(batch).await.unwrap();
    let persisted = applied.persist().await;
    database.finish_persistence(persisted).unwrap();
    drop(applied);

    let storage = database.into_storage();
    assert_eq!(
        storage
            .prefix("records".to_owned(), Vec::new())
            .await
            .unwrap(),
        vec![
            (
                vec![0x01, 0x00, 0x02],
                vec![0x00, 0x02, 0x00, 0x00, 0x02, b'a'],
            ),
            (
                vec![0x01, 0x12, 0x34],
                vec![0x00, 0x34, 0x12, 0x01, 0x02, b'h', b'i'],
            ),
        ],
        "RocksDB must retain lexicographic U16 key order and canonical variant-tagged, fixed-first record bytes",
    );
    storage.close().await.unwrap();
    drop(storage);

    let reopened = Database::new(
        schema,
        RocksDbStorage::open(directory.path(), &["records"]).unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(
        reopened
            .primary_key_scan("records", &[])
            .await
            .unwrap()
            .into_iter()
            .map(|record| record.to_values().unwrap())
            .collect::<Vec<_>>(),
        vec![
            vec![
                Value::String("a".to_owned()),
                Value::U16(2),
                Value::Bool(false),
            ],
            vec![
                Value::String("hi".to_owned()),
                Value::U16(0x1234),
                Value::Bool(true),
            ],
        ],
        "a fresh RocksDB handle must decode the exact canonical bytes in declaration order",
    );
    reopened.close().await.unwrap();
}

#[futures_test::test]
async fn fresh_open_requires_dropping_the_original_exclusive_handle() {
    let dir = tempfile::tempdir().unwrap();
    let column_families = vec!["records"];
    let storage = RocksDbStorage::open(dir.path(), &["records"]).unwrap();
    storage
        .set("records".to_owned(), b"key".to_vec(), b"persisted".to_vec())
        .await
        .unwrap();

    let attempts = Cell::new(0usize);
    let reopened =
        fresh_open_after_exclusive_drop_with(storage, dir.path(), &column_families, || {
            attempts.set(attempts.get() + 1);
            RocksDbStorage::open(dir.path(), &column_families)
        })
        .await
        .unwrap();
    assert_eq!(
        attempts.get(),
        1,
        "fresh open callback must run exactly once"
    );
    assert_eq!(
        reopened
            .get("records".to_owned(), b"key".to_vec())
            .await
            .unwrap()
            .as_deref(),
        Some(b"persisted".as_slice())
    );
}

#[futures_test::test]
async fn durable_fresh_open_preserves_ordered_data_and_rejects_partial_batches() {
    let dir = tempfile::tempdir().unwrap();
    let column_families = vec!["records"];
    let storage = RocksDbStorage::open(dir.path(), &column_families).unwrap();
    storage
        .set("records".to_owned(), b"item:2".to_vec(), b"two".to_vec())
        .await
        .unwrap();
    storage
        .set("records".to_owned(), b"item:1".to_vec(), b"one".to_vec())
        .await
        .unwrap();
    let error = storage
        .write_many(vec![
            OwnedWriteOperation::Set {
                cf: "records".to_owned(),
                key: b"item:3".to_vec(),
                value: b"three".to_vec(),
            },
            OwnedWriteOperation::Set {
                cf: "missing".to_owned(),
                key: b"item:4".to_vec(),
                value: b"four".to_vec(),
            },
        ])
        .await
        .unwrap_err();
    assert!(matches!(
        error,
        groove::storage::Error::ColumnFamilyNotFound(_)
    ));
    assert_eq!(
        storage
            .get("records".to_owned(), b"item:3".to_vec())
            .await
            .unwrap(),
        None,
        "a rejected batch must be atomic before the fresh open"
    );

    let reopened = fresh_open_after_exclusive_drop(storage, dir.path(), &column_families)
        .await
        .unwrap()
        .0;
    assert_eq!(
        reopened
            .prefix("records".to_owned(), b"item:".to_vec())
            .await
            .unwrap(),
        vec![
            (b"item:1".to_vec(), b"one".to_vec()),
            (b"item:2".to_vec(), b"two".to_vec()),
        ],
        "a new RocksDB process handle must retain ordered committed data only"
    );
    reopened.close().await.unwrap();
}
