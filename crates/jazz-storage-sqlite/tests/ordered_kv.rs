use futures::executor::block_on;
use groove::storage::{
    Error, OrderedKvStorage, OwnedWriteOperation, ReopenableStorage, ScanRequest, StorageDelta,
    collect_scan,
};
use jazz_storage_sqlite::SqliteStorage;

fn open(dir: &tempfile::TempDir) -> SqliteStorage {
    SqliteStorage::open(dir.path().join("jazz.sqlite"), &["records"]).unwrap()
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
fn conditional_delete_delta_matches_only_the_durable_value() {
    block_on(async {
        let dir = tempfile::tempdir().unwrap();
        let storage = open(&dir);
        let key = b"same-opaque-locator".to_vec();
        let old = b"old authenticated bytes".to_vec();
        let new = b"new authenticated bytes".to_vec();
        storage
            .set("records".into(), key.clone(), old.clone())
            .await
            .unwrap();

        storage
            .write_many(vec![OwnedWriteOperation::Delta {
                cf: "records".into(),
                key: key.clone(),
                delta: StorageDelta::delete_if_value_matches(b"different bytes".to_vec()),
            }])
            .await
            .unwrap();
        assert_eq!(
            storage.get("records".into(), key.clone()).await.unwrap(),
            Some(old.clone())
        );

        storage
            .write_many(vec![OwnedWriteOperation::Delta {
                cf: "records".into(),
                key: key.clone(),
                delta: StorageDelta::delete_if_value_matches(old),
            }])
            .await
            .unwrap();
        assert_eq!(
            storage.get("records".into(), key.clone()).await.unwrap(),
            None
        );

        storage
            .write_many(vec![OwnedWriteOperation::Delta {
                cf: "records".into(),
                key: key.clone(),
                delta: StorageDelta::set_if_absent(new.clone()),
            }])
            .await
            .unwrap();
        assert_eq!(storage.get("records".into(), key).await.unwrap(), Some(new));
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
