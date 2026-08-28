use base64::{Engine as _, engine::general_purpose::STANDARD};
use futures::executor::block_on;
use groove::storage::{Error, OrderedKvStorage, ScanRequest, collect_scan};
use jazz_storage_sqlite::SqliteStorage;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::ffi::OsString;
use std::path::Path;

const EPOCH_1_SQLITE_FIXTURE_BASE64: &str =
    include_str!("../fixtures/epoch-1-historical.sqlite.base64");
const EPOCH_1_SQLITE_FIXTURE_SHA256: &str =
    "a5aaa7fdcfab2759d10263e564419d18466d30e6f60ab911405f8e224b5de3d9";
const EPOCH_1_ORDERED_KV_PACK: &str = include_str!("../../groove/fixtures/epoch-1-ordered-kv.pack");
const EPOCH_1_ORDERED_KV_PACK_SHA256: &str =
    "5892ba4cb484da21f28316b90c260c6e07656ba7cfcc21e4c96944fc52baa2e7";

fn historical_epoch_1_sqlite_bytes() -> Vec<u8> {
    let bytes = STANDARD
        .decode(EPOCH_1_SQLITE_FIXTURE_BASE64.lines().collect::<String>())
        .expect("committed SQLite fixture is base64");
    assert!(historical_epoch_1_sqlite_checksum_matches(&bytes));
    bytes
}

fn historical_epoch_1_sqlite_checksum_matches(bytes: &[u8]) -> bool {
    format!("{:x}", Sha256::digest(bytes)) == EPOCH_1_SQLITE_FIXTURE_SHA256
}

fn write_historical_epoch_1_sqlite(path: &Path) {
    std::fs::write(path, historical_epoch_1_sqlite_bytes()).unwrap();
}

fn decode_hex(value: &str) -> Vec<u8> {
    assert_eq!(value.len() % 2, 0, "fixture hex is byte aligned");
    (0..value.len())
        .step_by(2)
        .map(|offset| u8::from_str_radix(&value[offset..offset + 2], 16).unwrap())
        .collect()
}

fn epoch_1_ordered_kv_pack() -> Vec<(String, Vec<u8>, Vec<u8>)> {
    assert_eq!(
        format!("{:x}", Sha256::digest(EPOCH_1_ORDERED_KV_PACK)),
        EPOCH_1_ORDERED_KV_PACK_SHA256,
        "the authoritative logical pack must match its checked-in checksum"
    );
    EPOCH_1_ORDERED_KV_PACK
        .lines()
        .skip(1)
        .map(|line| {
            let mut fields = line.split('\t');
            let family = fields.next().unwrap().to_owned();
            let key = decode_hex(fields.next().unwrap());
            let value = decode_hex(fields.next().unwrap());
            assert!(
                fields.next().is_none(),
                "fixture pack has exactly three fields"
            );
            (family, key, value)
        })
        .collect()
}

fn open(dir: &tempfile::TempDir) -> SqliteStorage {
    SqliteStorage::open(dir.path().join("jazz.sqlite"), &["records"]).unwrap()
}

#[test]
fn open_rejects_nul_column_family_before_creating_database() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("must-not-exist.sqlite");
    let too_long = "a".repeat(groove::storage::MAX_APPLICATION_STORAGE_NAME_BYTES + 1);
    for invalid in ["records\0evil", too_long.as_str()] {
        assert!(SqliteStorage::open(&path, &[invalid]).is_err());
    }
    assert!(!path.exists());
}

#[test]
fn historical_epoch_1_sqlite_fixture_is_checksum_guarded_before_materialization() {
    let mut corrupted = historical_epoch_1_sqlite_bytes();
    corrupted[0] ^= 1;
    assert!(
        !historical_epoch_1_sqlite_checksum_matches(&corrupted),
        "planted physical-fixture corruption must be rejected by the checksum gate"
    );
}

#[test]
fn historical_epoch_1_sqlite_fixture_read_only_snapshot_mixed_write_and_reopen() {
    block_on(async {
        let directory = tempfile::tempdir().unwrap();
        let historical_path = directory.path().join("epoch-1.sqlite");
        write_historical_epoch_1_sqlite(&historical_path);

        // The first open is deliberately raw and read-only: it proves the
        // committed physical file itself has the documented logical snapshot
        // before the current adapter can alter journal or metadata state.
        let read_only = rusqlite::Connection::open_with_flags(
            &historical_path,
            rusqlite::OpenFlags::SQLITE_OPEN_READ_ONLY,
        )
        .unwrap();
        let snapshot = read_only
            .prepare(
                "SELECT column_families.name, kv.k, kv.v \
                 FROM kv JOIN column_families ON kv.cf = column_families.id \
                 ORDER BY column_families.name, kv.k",
            )
            .unwrap()
            .query_map([], |row| {
                Ok((
                    row.get::<_, String>(0)?,
                    row.get::<_, Vec<u8>>(1)?,
                    row.get::<_, Vec<u8>>(2)?,
                ))
            })
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(snapshot, epoch_1_ordered_kv_pack());
        drop(read_only);

        let current = SqliteStorage::open(&historical_path, &["records", "indices"]).unwrap();
        assert_eq!(
            current
                .get("records".into(), b"user:2".to_vec())
                .await
                .unwrap(),
            Some(b"Grace".to_vec())
        );
        current
            .write_many(vec![
                OwnedWriteOperation::Set {
                    cf: "records".into(),
                    key: b"user:3".to_vec(),
                    value: b"Lin".to_vec(),
                },
                OwnedWriteOperation::Delete {
                    cf: "indices".into(),
                    key: b"name:Ada".to_vec(),
                },
                OwnedWriteOperation::Set {
                    cf: "indices".into(),
                    key: b"name:Lin".to_vec(),
                    value: b"3".to_vec(),
                },
            ])
            .await
            .unwrap();
        drop(current);

        let reopened = SqliteStorage::open(&historical_path, &["records", "indices"]).unwrap();
        assert_eq!(
            reopened
                .get("records".into(), b"user:3".to_vec())
                .await
                .unwrap(),
            Some(b"Lin".to_vec())
        );
        assert_eq!(
            reopened
                .get("indices".into(), b"name:Ada".to_vec())
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            reopened
                .get("indices".into(), b"name:Lin".to_vec())
                .await
                .unwrap(),
            Some(b"3".to_vec())
        );
    });
}

#[test]
fn scan_request_conforms_for_prefix_range_direction_and_limits() {
    block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let storage = open(&dir);
        for (key, value) in [
            (b"a:0".as_slice(), b"zero".as_slice()),
            (b"a:1".as_slice(), b"one".as_slice()),
            (b"a:2".as_slice(), b"two".as_slice()),
            (b"b:0".as_slice(), b"other".as_slice()),
            (&[0xff], b"ff".as_slice()),
            (&[0xff, 0x00], b"ff-zero".as_slice()),
        ] {
            storage
                .set("records".into(), key.to_vec(), value.to_vec())
                .await
                .unwrap();
        }

        let scan = |request| async {
            collect_scan(storage.scan(request).await.unwrap())
                .await
                .unwrap()
        };

        assert_eq!(
            scan(ScanRequest::prefix("records".into(), b"a:".to_vec()).with_max_items(2)).await,
            vec![
                (b"a:0".to_vec(), b"zero".to_vec()),
                (b"a:1".to_vec(), b"one".to_vec()),
            ]
        );
        assert_eq!(
            scan(
                ScanRequest::prefix("records".into(), b"a:".to_vec())
                    .reversed()
                    .with_max_items(2),
            )
            .await,
            vec![
                (b"a:2".to_vec(), b"two".to_vec()),
                (b"a:1".to_vec(), b"one".to_vec()),
            ]
        );
        assert_eq!(
            scan(
                ScanRequest::range("records".into(), b"a:1".to_vec(), b"b:0".to_vec())
                    .with_max_items(2),
            )
            .await,
            vec![
                (b"a:1".to_vec(), b"one".to_vec()),
                (b"a:2".to_vec(), b"two".to_vec()),
            ]
        );
        assert_eq!(
            scan(
                ScanRequest::range("records".into(), b"a:1".to_vec(), b"b:0".to_vec())
                    .reversed()
                    .with_max_items(1),
            )
            .await,
            vec![(b"a:2".to_vec(), b"two".to_vec())]
        );
        assert_eq!(
            scan(ScanRequest::prefix("records".into(), vec![0xff])).await,
            vec![
                (vec![0xff], b"ff".to_vec()),
                (vec![0xff, 0x00], b"ff-zero".to_vec()),
            ]
        );
        assert!(
            scan(ScanRequest::prefix("records".into(), Vec::new()).with_max_items(0))
                .await
                .is_empty()
        );
        assert!(matches!(
            storage
                .scan(ScanRequest::prefix("missing".into(), Vec::new()).with_max_items(0))
                .await,
            Err(Error::ColumnFamilyNotFound(name)) if name == "missing"
        ));
    });
}

#[test]
fn ordered_prefix_range_atomic_batch_and_reopen_contract() {
    block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let storage = open(&dir);
        groove::storage::conformance::persistence_order_and_batch_atomicity(&storage).await;
        groove::storage::conformance::atomic_conditionals_preserve_winners_and_reject_stale_deletes(&storage).await;
        groove::storage::conformance::invalid_batch_is_proven_uncommitted(&storage).await;
        groove::storage::conformance::reopen_preserves_data_and_adds_families(storage).await;
    });
}

#[test]
fn conditional_mutations_are_atomic_across_handles_and_aba_safe() {
    block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("jazz.sqlite");
        let first = SqliteStorage::open(&path, &["records"]).unwrap();
        let second = SqliteStorage::open(&path, &["records"]).unwrap();
        let key = b"locator".to_vec();
        assert_eq!(
            first
                .put_if_absent("records".into(), key.clone(), b"receipt-a".to_vec())
                .await
                .unwrap(),
            None
        );
        assert_eq!(
            second
                .put_if_absent("records".into(), key.clone(), b"receipt-b".to_vec())
                .await
                .unwrap(),
            Some(b"receipt-a".to_vec())
        );
        assert!(
            !second
                .compare_and_delete("records".into(), key.clone(), b"receipt-b".to_vec())
                .await
                .unwrap()
        );
        assert!(
            first
                .compare_and_delete("records".into(), key.clone(), b"receipt-a".to_vec())
                .await
                .unwrap()
        );
        assert_eq!(
            second
                .put_if_absent("records".into(), key.clone(), b"receipt-c".to_vec())
                .await
                .unwrap(),
            None
        );
        assert!(
            !first
                .compare_and_delete("records".into(), key.clone(), b"receipt-a".to_vec())
                .await
                .unwrap()
        );
    });
}

#[test]
fn rejects_wrong_format_and_closed_store() {
    block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("jazz.sqlite");
        let storage = SqliteStorage::open(&path, &["records"]).unwrap();
        storage.close().await.unwrap();
        assert!(matches!(
            storage.get("records".into(), vec![]).await,
            Err(Error::Backend { .. })
        ));
        let connection = rusqlite::Connection::open(&path).unwrap();
        connection
            .execute("UPDATE meta SET value = x'00' WHERE key = 'format'", [])
            .unwrap();
        drop(connection);
        assert!(matches!(
            SqliteStorage::open(&path, &["records"]),
            Err(Error::InvalidStorageLayout(_))
        ));
    });
}

#[test]
fn physical_header_ddl_and_jazz_blobs_are_pinned_across_reopen() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("jazz.sqlite");
    let storage = SqliteStorage::open(&path, &["records"]).unwrap();
    drop(storage);

    let connection = rusqlite::Connection::open(&path).unwrap();
    let application_id: i64 = connection
        .pragma_query_value(None, "application_id", |row| row.get(0))
        .unwrap();
    let user_version: i64 = connection
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .unwrap();
    assert_eq!(application_id, 0x4a41_5a5a, "SQLite header identifies Jazz");
    assert_eq!(user_version, 1, "SQLite header pins the v1 DDL");
    assert_eq!(
        connection
            .query_row("SELECT value FROM meta WHERE key = 'ddl_id'", [], |row| {
                row.get::<_, Vec<u8>>(0)
            })
            .unwrap(),
        b"jazz-groove-ordered-kv-ddl-v1"
    );
    assert_eq!(
        connection
            .query_row("SELECT value FROM meta WHERE key = 'format'", [], |row| {
                row.get::<_, Vec<u8>>(0)
            })
            .unwrap(),
        b"jazz-groove-ordered-kv"
    );
    assert!(
        connection
            .query_row(
                "SELECT value FROM meta WHERE key = 'epoch_manifest'",
                [],
                |row| row.get::<_, Vec<u8>>(0),
            )
            .unwrap()
            .starts_with(b"JSM1"),
        "the shared epoch manifest is stored as a raw, canonical metadata blob"
    );
    let ddl = connection
        .query_row(
            "SELECT sql FROM sqlite_master WHERE name = 'kv'",
            [],
            |row| row.get::<_, String>(0),
        )
        .unwrap();
    assert!(ddl.contains("PRIMARY KEY (cf, k)"));
    assert!(ddl.contains("WITHOUT ROWID"));
    drop(connection);

    SqliteStorage::open(&path, &["records"]).expect("exact physical v1 store reopens");
}

#[test]
fn missing_epoch_manifest_rejects_legacy_postcard_collision_before_reopen_mutates_data() {
    // This 24-byte payload is the former postcard `Vec<TxId>` spelling for
    // one transaction. A new fixed-width `Array<Tuple<U64, Uuid>>` can also
    // parse any 24-byte field, so testing the row codec alone would not prove
    // old persistent data is rejected. The epoch gate must run before this
    // ordinary value is visible to any Jazz/Groove decoder.
    let legacy_postcard_tx_ids = vec![
        0x01, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01, 0x10, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44,
        0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44, 0x44,
    ];
    assert_eq!(legacy_postcard_tx_ids.len(), 24);

    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("legacy-postcard-collision.sqlite");
    let storage = SqliteStorage::open(&path, &["jazz_merge_heads"]).unwrap();
    block_on(storage.set(
        "jazz_merge_heads".into(),
        b"physical-row-coordinate".to_vec(),
        legacy_postcard_tx_ids.clone(),
    ))
    .unwrap();
    drop(storage);

    let connection = rusqlite::Connection::open(&path).unwrap();
    connection
        .execute("DELETE FROM meta WHERE key = 'epoch_manifest'", [])
        .unwrap();
    drop(connection);

    assert!(matches!(
        SqliteStorage::open(&path, &["jazz_merge_heads", "must-not-be-created"]),
        Err(Error::InvalidStorageLayout(_))
    ));

    let connection = rusqlite::Connection::open(&path).unwrap();
    let preserved = connection
        .query_row(
            "SELECT kv.v FROM kv \
             JOIN column_families ON kv.cf = column_families.id \
             WHERE column_families.name = 'jazz_merge_heads' \
             AND kv.k = ?1",
            rusqlite::params![b"physical-row-coordinate".to_vec()],
            |row| row.get::<_, Vec<u8>>(0),
        )
        .unwrap();
    assert_eq!(preserved, legacy_postcard_tx_ids);
    let created: i64 = connection
        .query_row(
            "SELECT COUNT(*) FROM column_families WHERE name = 'must-not-be-created'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        created, 0,
        "manifest rejection must precede all reopen mutation"
    );
}

#[test]
fn rejects_wrong_sqlite_header_before_changing_foreign_store() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("foreign.sqlite");
    let connection = rusqlite::Connection::open(&path).unwrap();
    connection
        .execute_batch("CREATE TABLE foreign_data (value BLOB)")
        .unwrap();
    let before: String = connection
        .pragma_query_value(None, "journal_mode", |row| row.get(0))
        .unwrap();
    drop(connection);

    assert!(matches!(
        SqliteStorage::open(&path, &["records"]),
        Err(Error::InvalidStorageLayout(_))
    ));

    let connection = rusqlite::Connection::open(&path).unwrap();
    let after: String = connection
        .pragma_query_value(None, "journal_mode", |row| row.get(0))
        .unwrap();
    assert_eq!(after, before, "foreign store was rejected before WAL setup");
    assert_eq!(
        connection
            .query_row("SELECT COUNT(*) FROM foreign_data", [], |row| row
                .get::<_, i64>(0))
            .unwrap(),
        0
    );
}

fn directory_bytes(path: &Path) -> BTreeMap<OsString, Vec<u8>> {
    std::fs::read_dir(path)
        .unwrap()
        .map(|entry| {
            let entry = entry.unwrap();
            (entry.file_name(), std::fs::read(entry.path()).unwrap())
        })
        .collect()
}

fn assert_table_free_foreign_header_is_not_claimed(
    expected_application_id: i64,
    expected_user_version: i64,
) {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("foreign-empty.sqlite");
    let connection = rusqlite::Connection::open(&path).unwrap();
    connection
        .pragma_update(None, "application_id", expected_application_id)
        .unwrap();
    connection
        .pragma_update(None, "user_version", expected_user_version)
        .unwrap();
    let before_mode: String = connection
        .pragma_query_value(None, "journal_mode", |row| row.get(0))
        .unwrap();
    drop(connection);
    let before_bytes = std::fs::read(&path).unwrap();
    let before_directory = directory_bytes(directory.path());

    assert!(matches!(
        SqliteStorage::open(&path, &["records"]),
        Err(Error::InvalidStorageLayout(_))
    ));
    assert_eq!(std::fs::read(&path).unwrap(), before_bytes);
    assert_eq!(directory_bytes(directory.path()), before_directory);

    let connection = rusqlite::Connection::open(&path).unwrap();
    let application_id: i64 = connection
        .pragma_query_value(None, "application_id", |row| row.get(0))
        .unwrap();
    let user_version: i64 = connection
        .pragma_query_value(None, "user_version", |row| row.get(0))
        .unwrap();
    assert_eq!(application_id, expected_application_id);
    assert_eq!(user_version, expected_user_version);
    let after_mode: String = connection
        .pragma_query_value(None, "journal_mode", |row| row.get(0))
        .unwrap();
    assert_eq!(after_mode, before_mode, "rejection precedes WAL setup");
    assert_eq!(
        connection
            .query_row("SELECT COUNT(*) FROM sqlite_master", [], |row| row
                .get::<_, i64>(0))
            .unwrap(),
        0,
        "rejection creates no Jazz tables"
    );
}

#[test]
fn rejects_table_free_foreign_application_id_without_claiming_it() {
    assert_table_free_foreign_header_is_not_claimed(0x1122_3344, 0);
}

#[test]
fn rejects_table_free_foreign_user_version_without_claiming_it() {
    assert_table_free_foreign_header_is_not_claimed(0, 7);
}

#[test]
fn rejects_wrong_sqlite_user_version_and_ddl_identity() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("jazz.sqlite");
    let storage = SqliteStorage::open(&path, &["records"]).unwrap();
    drop(storage);

    let connection = rusqlite::Connection::open(&path).unwrap();
    connection.pragma_update(None, "user_version", 2).unwrap();
    drop(connection);
    assert!(matches!(
        SqliteStorage::open(&path, &["records"]),
        Err(Error::InvalidStorageLayout(_))
    ));

    let connection = rusqlite::Connection::open(&path).unwrap();
    connection.pragma_update(None, "user_version", 1).unwrap();
    connection
        .execute("UPDATE meta SET value = x'00' WHERE key = 'ddl_id'", [])
        .unwrap();
    drop(connection);
    assert!(matches!(
        SqliteStorage::open(&path, &["records"]),
        Err(Error::InvalidStorageLayout(_))
    ));

    let path = directory.path().join("bad-epoch-manifest.sqlite");
    let storage = SqliteStorage::open(&path, &["records"]).unwrap();
    drop(storage);
    let connection = rusqlite::Connection::open(&path).unwrap();
    connection
        .execute(
            "UPDATE meta SET value = x'4a534d310002' WHERE key = 'epoch_manifest'",
            [],
        )
        .unwrap();
    drop(connection);
    assert!(matches!(
        SqliteStorage::open(&path, &["records"]),
        Err(Error::InvalidStorageLayout(_))
    ));
}

#[test]
fn rejects_foreign_table_shape_before_adopting_data() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("jazz.sqlite");
    let storage = SqliteStorage::open(&path, &["records"]).unwrap();
    drop(storage);
    let connection = rusqlite::Connection::open(&path).unwrap();
    connection
        .execute_batch("DROP TABLE kv; CREATE TABLE kv (cf INTEGER, k BLOB, v BLOB)")
        .unwrap();
    drop(connection);
    assert!(matches!(
        SqliteStorage::open(&path, &["records"]),
        Err(Error::InvalidStorageLayout(_))
    ));
}
