use futures::executor::block_on;
use groove::storage::{
    Error, OrderedKvStorage, OwnedWriteOperation, ReopenableStorage, ScanRequest, collect_scan,
};
use jazz_storage_sqlite::SqliteStorage;

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
        storage
            .set("records".into(), b"user:2".to_vec(), b"two".to_vec())
            .await
            .unwrap();
        storage
            .set("records".into(), b"user:10".to_vec(), b"ten".to_vec())
            .await
            .unwrap();
        storage
            .set("records".into(), b"user:1".to_vec(), b"one".to_vec())
            .await
            .unwrap();
        assert_eq!(
            storage
                .prefix("records".into(), b"user:".to_vec())
                .await
                .unwrap(),
            vec![
                (b"user:1".to_vec(), b"one".to_vec()),
                (b"user:10".to_vec(), b"ten".to_vec()),
                (b"user:2".to_vec(), b"two".to_vec()),
            ]
        );
        let error = storage
            .write_many(vec![
                OwnedWriteOperation::Set {
                    cf: "records".into(),
                    key: b"user:3".to_vec(),
                    value: b"three".to_vec(),
                },
                OwnedWriteOperation::Set {
                    cf: "missing".into(),
                    key: b"user:4".to_vec(),
                    value: b"four".to_vec(),
                },
            ])
            .await
            .unwrap_err();
        assert!(matches!(error, Error::ColumnFamilyNotFound(name) if name == "missing"));
        assert_eq!(
            storage
                .get("records".into(), b"user:3".to_vec())
                .await
                .unwrap(),
            None
        );
        let storage = storage
            .reopen(vec!["records".into(), "indices".into()])
            .await
            .unwrap();
        storage
            .set("indices".into(), b"name:one".to_vec(), b"1".to_vec())
            .await
            .unwrap();
        assert_eq!(
            storage
                .get("records".into(), b"user:1".to_vec())
                .await
                .unwrap(),
            Some(b"one".to_vec())
        );
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
