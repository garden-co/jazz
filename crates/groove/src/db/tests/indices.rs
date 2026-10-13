//! Durable schema indices, reads, ordering, uniqueness, and restart behavior.

use super::*;

#[futures_test::test]
async fn table_existence_is_bounded_and_observes_applied_resident_state() {
    for count in [1, 1024] {
        let storage = MemoryStorage::new(&["albums"]).unwrap();
        let mut database = Database::new(albums_schema(), storage).await.unwrap();
        assert!(!database.table_has_stored_rows("albums").await.unwrap());
        assert!(database.table_has_stored_rows("missing").await.is_err());
        let mut batch = database.open_batch();
        for id in 0..count {
            batch.insert(
                "albums",
                vec![Value::U64(id), Value::String("record".into())],
            );
        }
        assert!(
            !database.table_has_stored_rows("albums").await.unwrap(),
            "unapplied batch is not resident"
        );
        let applied = database.apply_batch(batch).await.unwrap();
        database.reset_storage_read_metrics();
        assert!(database.table_has_stored_rows("albums").await.unwrap());
        let reads = database.storage_read_metrics();
        assert_eq!(
            reads.total.reads, 1,
            "one candidate regardless of cardinality"
        );
        assert_eq!(reads.total.ranges, 1);
        let persisted = applied.persist().await;
        database.finish_persistence(persisted).unwrap();
        database.reset_storage_read_metrics();
        assert!(database.table_has_stored_rows("albums").await.unwrap());
        assert_eq!(database.storage_read_metrics().total.reads, 1);

        let mut batch = database.open_batch();
        for id in 0..count {
            batch.delete("albums", PrimaryKeyValue::U64(id));
        }
        assert!(database.table_has_stored_rows("albums").await.unwrap());
        let applied = database.apply_batch(batch).await.unwrap();
        assert!(
            !database.table_has_stored_rows("albums").await.unwrap(),
            "resident tombstones hide persisted rows"
        );
        let persisted = applied.persist().await;
        database.finish_persistence(persisted).unwrap();
        assert!(!database.table_has_stored_rows("albums").await.unwrap());
    }
}

#[futures_test::test]
async fn table_existence_rejects_poisoned_storage_after_failed_persistence() {
    let (storage, control) = TestStorage::controlled(&["albums"]);
    let mut database = Database::new(albums_schema(), storage).await.unwrap();
    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(1), Value::String("record".into())],
    );
    let applied = database.apply_batch(batch).await.unwrap();
    control.fail_next(TestStorageOperation::WriteMany);
    let persisted = applied.persist().await;
    assert!(database.finish_persistence(persisted).is_err());
    assert!(matches!(
        database.table_has_stored_rows("albums").await,
        Err(Error::DatabasePoisoned)
    ));
}

#[futures_test::test]
async fn database_creation_dedups_schema_indices_as_durable_nodes() {
    let storage =
        MemoryStorage::new(&["albums", "indices"]).expect("valid memory storage families");
    let database = Database::new(indexed_albums_schema(), storage)
        .await
        .unwrap();

    let durable_nodes = database
        .ivm_runtime
        .retained_node_ids()
        .into_iter()
        .filter(|node| {
            database
                .ivm_runtime
                .graph()
                .node(*node)
                .is_some_and(|node| node.is_durable())
        })
        .collect::<Vec<_>>();

    assert_eq!(durable_nodes.len(), 1);
}

#[futures_test::test]
async fn persist_maintains_schema_index_entries() {
    let storage =
        MemoryStorage::new(&["albums", "indices"]).expect("valid memory storage families");
    let mut database = Database::new(indexed_albums_schema(), storage)
        .await
        .unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(7), Value::String("Blue Train".to_owned())],
    );
    database.commit_batch(batch).await.unwrap();

    let prefix = index_prefix(&database, "albums", "albums_by_title");
    let entries = database
        .storage
        .prefix("indices".to_owned(), prefix.to_vec())
        .await
        .unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(persisted_index_value(&entries[0].1), Vec::<u8>::new());

    let mut batch = database.open_batch();
    batch.update(
        "albums",
        vec![Value::U64(7), Value::String("Giant Steps".to_owned())],
    );
    database.commit_batch(batch).await.unwrap();

    let entries = database
        .storage
        .prefix("indices".to_owned(), prefix.to_vec())
        .await
        .unwrap();
    assert_eq!(entries.len(), 1);
    assert!(
        entries[0]
            .0
            .windows("Giant Steps".len())
            .any(|window| window == b"Giant Steps")
    );
    assert_eq!(persisted_index_value(&entries[0].1), Vec::<u8>::new());

    let mut batch = database.open_batch();
    batch.delete("albums", PrimaryKeyValue::U64(7));
    database.commit_batch(batch).await.unwrap();

    assert!(
        database
            .storage
            .prefix("indices".to_owned(), prefix.to_vec())
            .await
            .unwrap()
            .is_empty()
    );
}

#[futures_test::test]
async fn persist_consolidates_same_tick_deltas_and_rejects_unique_conflicts() {
    let storage =
        MemoryStorage::new(&["albums", "indices"]).expect("valid memory storage families");
    let mut database = Database::new(unique_indexed_albums_schema(), storage)
        .await
        .unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(7), Value::String("Blue Train".to_owned())],
    );
    database.commit_batch(batch).await.unwrap();

    let mut batch = database.open_batch();
    batch.update(
        "albums",
        vec![Value::U64(7), Value::String("Blue Train".to_owned())],
    );
    database.commit_batch(batch).await.unwrap();
    assert_eq!(
        record_values(
            database
                .index_scan(
                    "albums",
                    "unique_albums_by_title",
                    &[Value::String("Blue Train".to_owned())],
                )
                .await
                .unwrap()
        ),
        [vec![Value::U64(7), Value::String("Blue Train".to_owned())]]
    );

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(8), Value::String("Blue Train".to_owned())],
    );
    assert!(matches!(
        database.commit_batch(batch).await.unwrap_err(),
        Error::IvmRuntime(IvmRuntimeError::UniqueIndexViolation { .. })
    ));
}

#[futures_test::test]
async fn public_database_facade_reads_secondary_indexes_with_memory_storage() {
    let schema = DatabaseSchema::new([TableSchema::new(
        "albums",
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("title", ColumnType::String),
            ColumnSchema::new("year", ColumnType::U64),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))
    .with_index(IndexSchema::new("albums_by_year", ["year"]))]);
    let storage =
        MemoryStorage::new(&["albums", "indices"]).expect("valid memory storage families");
    let mut database = Database::new(schema, storage).await.unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![
            Value::U64(1),
            Value::String("Blue Train".to_owned()),
            Value::U64(1957),
        ],
    );
    batch.insert(
        "albums",
        vec![
            Value::U64(2),
            Value::String("Kind of Blue".to_owned()),
            Value::U64(1959),
        ],
    );
    batch.insert(
        "albums",
        vec![
            Value::U64(3),
            Value::String("Mingus Ah Um".to_owned()),
            Value::U64(1959),
        ],
    );
    batch.insert(
        "albums",
        vec![
            Value::U64(4),
            Value::String("A Love Supreme".to_owned()),
            Value::U64(1965),
        ],
    );
    database.commit_batch(batch).await.unwrap();

    let albums_from_1959 = record_values(
        database
            .index_scan("albums", "albums_by_year", &[Value::U64(1959)])
            .await
            .unwrap(),
    );
    assert_eq!(
        albums_from_1959,
        vec![
            vec![
                Value::U64(2),
                Value::String("Kind of Blue".to_owned()),
                Value::U64(1959),
            ],
            vec![
                Value::U64(3),
                Value::String("Mingus Ah Um".to_owned()),
                Value::U64(1959),
            ],
        ]
    );

    let late_1950s_and_early_1960s = record_values(
        database
            .index_scan_range(
                "albums",
                "albums_by_year",
                &[Value::U64(1959)],
                &[Value::U64(1965)],
            )
            .await
            .unwrap(),
    );
    assert_eq!(late_1950s_and_early_1960s, albums_from_1959);
}

#[futures_test::test]
async fn index_reads_track_insert_update_delete_and_prefixes() {
    let storage =
        MemoryStorage::new(&["tracks", "indices"]).expect("valid memory storage families");
    let mut database = Database::new(indexed_tracks_schema(), storage)
        .await
        .unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "tracks",
        vec![
            Value::U64(1),
            Value::U64(7),
            Value::Nullable(None),
            Value::String("Intro".to_owned()),
        ],
    );
    batch.insert(
        "tracks",
        vec![
            Value::U64(2),
            Value::U64(7),
            Value::Nullable(Some(Box::new(Value::U64(2)))),
            Value::String("Part Two".to_owned()),
        ],
    );
    database.commit_batch(batch).await.unwrap();

    assert_eq!(
        record_values(
            database
                .index_get(
                    "tracks",
                    "tracks_by_album_disc",
                    &[Value::U64(7), Value::Nullable(None),]
                )
                .await
                .unwrap()
        ),
        vec![vec![
            Value::U64(1),
            Value::U64(7),
            Value::Nullable(None),
            Value::String("Intro".to_owned()),
        ]]
    );
    assert_eq!(
        record_values(
            database
                .index_scan("tracks", "tracks_by_album_disc", &[Value::U64(7)])
                .await
                .unwrap()
        )
        .len(),
        2
    );

    let mut batch = database.open_batch();
    batch.update(
        "tracks",
        vec![
            Value::U64(1),
            Value::U64(8),
            Value::Nullable(None),
            Value::String("Intro".to_owned()),
        ],
    );
    database.commit_batch(batch).await.unwrap();
    assert!(
        database
            .index_scan("tracks", "tracks_by_album_disc", &[Value::U64(7)])
            .await
            .unwrap()
            .len()
            == 1
    );

    let mut batch = database.open_batch();
    batch.delete("tracks", PrimaryKeyValue::U64(2));
    database.commit_batch(batch).await.unwrap();
    assert!(
        database
            .index_scan("tracks", "tracks_by_album_disc", &[Value::U64(7)])
            .await
            .unwrap()
            .is_empty()
    );
}

#[futures_test::test]
async fn persisted_index_update_retracts_old_key_when_indexed_value_changes_to_finite() {
    let storage =
        MemoryStorage::new(&["history", "indices"]).expect("valid memory storage families");
    let mut database = Database::new(interval_history_schema(), storage)
        .await
        .unwrap();
    let row = vec![7; 16];

    let mut batch = database.open_batch();
    batch.insert(
        "history",
        vec![
            Value::Bytes(row.clone()),
            Value::U64(1),
            Value::U64(1),
            Value::U64(u64::MAX),
            Value::String("open".to_owned()),
        ],
    );
    database.commit_batch(batch).await.unwrap();
    assert_eq!(
        database
            .index_scan("history", "history_by_until_row", &[Value::U64(u64::MAX)])
            .await
            .unwrap()
            .len(),
        1
    );

    let mut batch = database.open_batch();
    batch.update(
        "history",
        vec![
            Value::Bytes(row),
            Value::U64(1),
            Value::U64(1),
            Value::U64(2),
            Value::String("closed".to_owned()),
        ],
    );
    database.commit_batch(batch).await.unwrap();

    assert!(
        database
            .index_scan("history", "history_by_until_row", &[Value::U64(u64::MAX)])
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        record_values(
            database
                .index_scan("history", "history_by_until_row", &[Value::U64(2)])
                .await
                .unwrap()
        ),
        vec![vec![
            Value::Bytes(vec![7; 16]),
            Value::U64(1),
            Value::U64(1),
            Value::U64(2),
            Value::String("closed".to_owned()),
        ]]
    );
}

#[futures_test::test]
async fn persisted_index_update_preserves_entry_when_index_key_is_unchanged() {
    let storage =
        MemoryStorage::new(&["history", "indices"]).expect("valid memory storage families");
    let mut database = Database::new(interval_history_schema(), storage)
        .await
        .unwrap();
    let row = vec![7; 16];

    let mut batch = database.open_batch();
    batch.insert(
        "history",
        vec![
            Value::Bytes(row.clone()),
            Value::U64(1),
            Value::U64(1),
            Value::U64(u64::MAX),
            Value::String("before".to_owned()),
        ],
    );
    database.commit_batch(batch).await.unwrap();

    let mut batch = database.open_batch();
    batch.update(
        "history",
        vec![
            Value::Bytes(row),
            Value::U64(1),
            Value::U64(1),
            Value::U64(u64::MAX),
            Value::String("after".to_owned()),
        ],
    );
    database.commit_batch(batch).await.unwrap();

    assert_eq!(
        record_values(
            database
                .index_scan("history", "history_by_until_row", &[Value::U64(u64::MAX)])
                .await
                .unwrap()
        ),
        vec![vec![
            Value::Bytes(vec![7; 16]),
            Value::U64(1),
            Value::U64(1),
            Value::U64(u64::MAX),
            Value::String("after".to_owned()),
        ]]
    );
}

#[futures_test::test]
async fn uuid_primary_keys_nullable_index_keys_and_ordering_work() {
    let storage = MemoryStorage::new(&["docs", "indices"]).expect("valid memory storage families");
    let mut database = Database::new(uuid_docs_schema(), storage).await.unwrap();
    let low = uuid::Uuid::from_bytes([1; 16]);
    let mid = uuid::Uuid::from_bytes([2; 16]);
    let high = uuid::Uuid::from_bytes([3; 16]);
    let owner = uuid::Uuid::from_bytes([9; 16]);

    let mut batch = database.open_batch();
    batch.insert(
        "docs",
        vec![
            Value::Uuid(high),
            Value::Nullable(Some(Box::new(Value::Uuid(owner)))),
            Value::String("high".to_owned()),
        ],
    );
    batch.insert(
        "docs",
        vec![
            Value::Uuid(low),
            Value::Nullable(Some(Box::new(Value::Uuid(owner)))),
            Value::String("low".to_owned()),
        ],
    );
    batch.insert(
        "docs",
        vec![
            Value::Uuid(mid),
            Value::Nullable(None),
            Value::String("mid".to_owned()),
        ],
    );
    database.commit_batch(batch).await.unwrap();

    assert_eq!(
        record_values(
            database
                .index_scan(
                    "docs",
                    "docs_by_owner",
                    &[Value::Nullable(Some(Box::new(Value::Uuid(owner))))],
                )
                .await
                .unwrap(),
        ),
        vec![
            vec![
                Value::Uuid(low),
                Value::Nullable(Some(Box::new(Value::Uuid(owner)))),
                Value::String("low".to_owned()),
            ],
            vec![
                Value::Uuid(high),
                Value::Nullable(Some(Box::new(Value::Uuid(owner)))),
                Value::String("high".to_owned()),
            ],
        ]
    );
    assert_eq!(
        database
            .index_scan("docs", "docs_by_owner", &[Value::Nullable(None)])
            .await
            .unwrap()
            .len(),
        1
    );

    let mut batch = database.open_batch();
    batch.update(
        "docs",
        vec![
            Value::Uuid(mid),
            Value::Nullable(Some(Box::new(Value::Uuid(owner)))),
            Value::String("mid-owned".to_owned()),
        ],
    );
    database.commit_batch(batch).await.unwrap();

    assert_eq!(
        database
            .index_scan(
                "docs",
                "docs_by_owner",
                &[Value::Nullable(Some(Box::new(Value::Uuid(owner))))],
            )
            .await
            .unwrap()
            .len(),
        3
    );
}

#[futures_test::test]
async fn index_get_on_unique_index_returns_zero_or_one_record() {
    let storage =
        MemoryStorage::new(&["tracks", "indices"]).expect("valid memory storage families");
    let mut database = Database::new(indexed_tracks_schema(), storage)
        .await
        .unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "tracks",
        vec![
            Value::U64(1),
            Value::U64(7),
            Value::Nullable(None),
            Value::String("Intro".to_owned()),
        ],
    );
    database.commit_batch(batch).await.unwrap();

    assert_eq!(
        database
            .index_get(
                "tracks",
                "tracks_by_title_unique",
                &[Value::String("Intro".to_owned())],
            )
            .await
            .unwrap()
            .len(),
        1
    );
    assert!(
        database
            .index_get(
                "tracks",
                "tracks_by_title_unique",
                &[Value::String("Missing".to_owned())],
            )
            .await
            .unwrap()
            .is_empty()
    );
}

#[futures_test::test]
async fn tuple_columns_work_in_index_keys_and_nullable_columns() {
    let storage = MemoryStorage::new(&["edges", "indices"]).expect("valid memory storage families");
    let mut database = Database::new(tuple_edges_schema(), storage).await.unwrap();
    let node_a = uuid::Uuid::from_bytes([0x0a; 16]);
    let node_b = uuid::Uuid::from_bytes([0x0b; 16]);
    let parent_a = Value::Tuple(vec![Value::Uuid(node_a), Value::U64(1)]);
    let parent_b = Value::Tuple(vec![Value::Uuid(node_b), Value::U64(2)]);

    let mut batch = database.open_batch();
    batch.insert(
        "edges",
        vec![
            Value::U64(1),
            parent_b.clone(),
            Value::Nullable(Some(Box::new(parent_a.clone()))),
            Value::String("b".to_owned()),
        ],
    );
    batch.insert(
        "edges",
        vec![
            Value::U64(2),
            parent_a.clone(),
            Value::Nullable(None),
            Value::String("a".to_owned()),
        ],
    );
    database.commit_batch(batch).await.unwrap();

    let rows = database
        .index_get("edges", "edges_by_parent", std::slice::from_ref(&parent_a))
        .await
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].get("title").unwrap(), Value::String("a".to_owned()));

    let scanned = database
        .index_scan("edges", "edges_by_parent", &[])
        .await
        .unwrap()
        .into_iter()
        .map(|record| record.get("title").unwrap().clone())
        .collect::<Vec<_>>();
    assert_eq!(
        scanned,
        vec![Value::String("a".to_owned()), Value::String("b".to_owned())]
    );

    let rows = database
        .index_get("edges", "edges_by_parent", &[parent_b])
        .await
        .unwrap();
    assert_eq!(
        rows[0].get("maybe_parent").unwrap(),
        Value::Nullable(Some(Box::new(parent_a)))
    );
}

#[futures_test::test]
async fn raw_reads_return_encoded_base_records() {
    let storage =
        MemoryStorage::new(&["tracks", "indices"]).expect("valid memory storage families");
    let mut database = Database::new(indexed_tracks_schema(), storage)
        .await
        .unwrap();

    let mut batch = database.open_batch();
    batch.insert("tracks", track_values(1, 7, Some(1), "Intro"));
    batch.insert("tracks", track_values(2, 7, None, ""));
    database.commit_batch(batch).await.unwrap();

    let descriptor = database
        .ivm_runtime
        .schema()
        .table("tracks")
        .unwrap()
        .record_schema();
    let title_idx = descriptor.field_index("title").unwrap();
    let album_idx = descriptor.field_index("album_id").unwrap();

    let by_pk = database
        .primary_key_scan_raw("tracks", &[Value::U64(1)])
        .await
        .unwrap();
    assert_eq!(by_pk.len(), 1);
    assert_eq!(by_pk[0].record().get_str(title_idx).unwrap(), "Intro");

    let by_index = database
        .index_scan_raw("tracks", "tracks_by_album_disc", &[Value::U64(7)])
        .await
        .unwrap();
    assert_eq!(by_index.len(), 2);
    assert_eq!(by_index[0].record().get_u64(album_idx).unwrap(), 7);

    let exact = database
        .index_get_raw(
            "tracks",
            "tracks_by_album_disc",
            &[Value::U64(7), Value::Nullable(None)],
        )
        .await
        .unwrap();
    assert_eq!(exact.len(), 1);
    assert_eq!(exact[0].record().get_str(title_idx).unwrap(), "");

    let ranged = database
        .index_scan_range_raw(
            "tracks",
            "tracks_by_album_disc",
            &[Value::U64(7)],
            &[Value::U64(8)],
        )
        .await
        .unwrap();
    assert_eq!(ranged.len(), 2);
}

#[futures_test::test]
async fn persisted_index_scan_treats_missing_primary_key_record_as_invalid() {
    let storage =
        MemoryStorage::new(&["albums", "indices"]).expect("valid memory storage families");
    let mut database = Database::new(indexed_albums_schema(), storage)
        .await
        .unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(7), Value::String("Blue Train".to_owned())],
    );
    database.commit_batch(batch).await.unwrap();
    database
        .storage
        .delete("albums".to_owned(), PrimaryKeyValue::U64(7).into_bytes())
        .await
        .unwrap();

    assert!(matches!(
        database
            .index_scan("albums", "albums_by_title", &[Value::String("Blue Train".to_owned())]).await
            .unwrap_err(),
        Error::InvalidPersistedIndex(index) if index == "albums_by_title"
    ));
}

#[futures_test::test]
async fn primary_key_last_before_or_at_raw_returns_bounded_prefix_winner() {
    let storage =
        MemoryStorage::new(&["history", "indices"]).expect("valid memory storage families");
    let mut database = Database::new(history_schema(), storage).await.unwrap();

    let mut batch = database.open_batch();
    batch.insert("history", history_values(1, 10, 1, "older"));
    batch.insert("history", history_values(1, 20, 1, "winner"));
    batch.insert("history", history_values(1, 30, 1, "too-new"));
    batch.insert("history", history_values(2, 15, 1, "other-row"));
    database.commit_batch(batch).await.unwrap();

    let descriptor = database
        .ivm_runtime
        .schema()
        .table("history")
        .unwrap()
        .record_schema();
    let title_idx = descriptor.field_index("title").unwrap();
    let bounded = database
        .primary_key_last_before_or_at_raw(
            "history",
            &[Value::U64(1)],
            &[Value::U64(1), Value::U64(20), Value::U64(u64::MAX)],
        )
        .await
        .unwrap()
        .expect("bounded row");
    assert_eq!(bounded.record().get_str(title_idx).unwrap(), "winner");

    let before_first = database
        .primary_key_last_before_or_at_raw(
            "history",
            &[Value::U64(1)],
            &[Value::U64(1), Value::U64(5), Value::U64(u64::MAX)],
        )
        .await
        .unwrap();
    assert!(before_first.is_none());

    let ranged = database
        .primary_key_scan_range_raw(
            "history",
            &[Value::U64(1), Value::U64(10), Value::U64(0)],
            &[Value::U64(1), Value::U64(30), Value::U64(0)],
        )
        .await
        .unwrap();
    let titles = ranged
        .iter()
        .map(|raw| raw.record().get_str(title_idx).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(titles, vec!["older", "winner"]);
}

#[futures_test::test]
async fn reversed_range_apis_return_empty() {
    let schema = indexed_albums_schema().with_direct_record_store(DirectRecordStoreSchema::new(
        "streams",
        RecordDescriptor::new([("id", ValueType::U64)]),
        RecordDescriptor::new([("payload", ValueType::Bytes)]),
    ));
    let column_families = schema.column_families();
    let storage = MemoryStorage::new(&column_families).expect("valid memory storage families");
    let mut database = Database::new(schema, storage).await.unwrap();

    let mut batch = database.open_batch();
    batch.insert("albums", vec![Value::U64(1), Value::String("A".to_owned())]);
    batch.insert("albums", vec![Value::U64(2), Value::String("B".to_owned())]);
    database.commit_batch(batch).await.unwrap();

    assert!(
        database
            .primary_key_scan_range_raw("albums", &[Value::U64(3)], &[Value::U64(1)])
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        database
            .primary_key_scan_range_raw("albums", &[Value::U64(1)], &[Value::U64(1)])
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        database
            .primary_key_scan_range_raw("albums", &[Value::U64(1)], &[Value::U64(3)])
            .await
            .unwrap()
            .len(),
        2
    );

    assert!(
        database
            .index_scan_range_raw(
                "albums",
                "albums_by_title",
                &[Value::String("Z".to_owned())],
                &[Value::String("A".to_owned())],
            )
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        database
            .index_scan_range(
                "albums",
                "albums_by_title",
                &[Value::String("Z".to_owned())],
                &[Value::String("A".to_owned())],
            )
            .await
            .unwrap()
            .is_empty()
    );

    let store = database.direct_record_store("streams").unwrap();
    store
        .set(&[Value::U64(1)], &[Value::Bytes(b"one".to_vec())])
        .await
        .unwrap();
    store
        .set(&[Value::U64(2)], &[Value::Bytes(b"two".to_vec())])
        .await
        .unwrap();
    assert!(
        store
            .range(&[Value::U64(3)], &[Value::U64(1)])
            .await
            .unwrap()
            .is_empty()
    );
    assert!(
        store
            .range_entries(&[Value::U64(3)], &[Value::U64(1)])
            .await
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        store
            .range(&[Value::U64(1)], &[Value::U64(3)])
            .await
            .unwrap()
            .len(),
        2
    );
}

#[futures_test::test]
async fn randomized_index_reads_match_full_scan_oracle() {
    let storage =
        MemoryStorage::new(&["tracks", "indices"]).expect("valid memory storage families");
    let mut database = Database::new(indexed_tracks_schema(), storage)
        .await
        .unwrap();
    let mut rows = std::collections::BTreeMap::<u64, (u64, Option<u64>, String)>::new();
    let mut rng = 0x51eed_u64;

    for _ in 0..200 {
        rng = rng.wrapping_mul(6364136223846793005).wrapping_add(1);
        let id = (rng % 24) + 1;
        let album = ((rng >> 8) % 5) + 1;
        let disc = (!(rng >> 16).is_multiple_of(3)).then_some(((rng >> 24) % 3) + 1);
        let title = format!("t{id}-{album}-{}", disc.unwrap_or(0));
        let mut batch = database.open_batch();
        if rng & 1 == 0 || !rows.contains_key(&id) {
            rows.insert(id, (album, disc, title.clone()));
            batch.update("tracks", track_values(id, album, disc, &title));
        } else {
            rows.remove(&id);
            batch.delete("tracks", PrimaryKeyValue::U64(id));
        }
        database.commit_batch(batch).await.unwrap();

        let album_key = Value::U64(album);
        let mut expected = rows
            .iter()
            .filter(|(_, (row_album, _, _))| *row_album == album)
            .map(|(row_id, (row_album, row_disc, row_title))| {
                track_values(*row_id, *row_album, *row_disc, row_title)
            })
            .collect::<Vec<_>>();
        expected.sort_by_key(|values| format!("{values:?}"));
        let mut actual = record_values(
            database
                .index_scan("tracks", "tracks_by_album_disc", &[album_key])
                .await
                .unwrap(),
        );
        actual.sort_by_key(|values| format!("{values:?}"));
        assert_eq!(actual, expected);
    }
}

#[futures_test::test]
async fn persisted_index_keys_sort_by_index_value_then_primary_key() {
    let storage =
        MemoryStorage::new(&["albums", "indices"]).expect("valid memory storage families");
    let mut database = Database::new(indexed_albums_schema(), storage)
        .await
        .unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(256), Value::String("b".to_owned())],
    );
    batch.insert(
        "albums",
        vec![Value::U64(1), Value::String("aa".to_owned())],
    );
    database.commit_batch(batch).await.unwrap();

    let keys = database
        .storage
        .prefix(
            "indices".to_owned(),
            index_prefix(&database, "albums", "albums_by_title").to_vec(),
        )
        .await
        .unwrap()
        .into_iter()
        .map(|(key, _)| key)
        .collect::<Vec<_>>();

    assert_eq!(
        keys,
        [
            persisted_index_storage_key(
                &database,
                "albums_by_title",
                &encoded_title_index_key("aa", 1)
            ),
            persisted_index_storage_key(
                &database,
                "albums_by_title",
                &encoded_title_index_key("b", 256)
            ),
        ]
    );
}

#[futures_test::test]
async fn durable_non_unique_index_keys_append_separator_and_primary_key_suffix() {
    let storage =
        MemoryStorage::new(&["albums", "indices"]).expect("valid memory storage families");
    let mut database = Database::new(indexed_albums_schema(), storage)
        .await
        .unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(7), Value::String("Blue Train".to_owned())],
    );
    database.commit_batch(batch).await.unwrap();

    let entries = database
        .storage
        .prefix(
            "indices".to_owned(),
            index_prefix(&database, "albums", "albums_by_title").to_vec(),
        )
        .await
        .unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(
        entries[0].0,
        persisted_index_storage_key(
            &database,
            "albums_by_title",
            &encoded_title_index_key("Blue Train", 7)
        )
    );
    assert!(
        encoded_title_index_key("Blue Train", 7)
            .strip_prefix(encoded_title_key_part("Blue Train").as_slice())
            .is_some_and(|suffix| suffix.starts_with(&[0xff]))
    );
}

#[futures_test::test]
async fn unique_indices_use_only_index_columns_as_storage_keys() {
    let storage =
        MemoryStorage::new(&["albums", "indices"]).expect("valid memory storage families");
    let mut database = Database::new(unique_indexed_albums_schema(), storage)
        .await
        .unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(7), Value::String("Blue Train".to_owned())],
    );
    database.commit_batch(batch).await.unwrap();

    let prefix = index_prefix(&database, "albums", "unique_albums_by_title");
    let entries = database
        .storage
        .prefix("indices".to_owned(), prefix.to_vec())
        .await
        .unwrap();
    let expected_key = persisted_index_storage_key(
        &database,
        "unique_albums_by_title",
        &encoded_title_key_part("Blue Train"),
    );

    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].0, expected_key);
    assert_eq!(
        persisted_index_value(&entries[0].1),
        encoded_u64_index_part(7)
    );
}

#[futures_test::test]
async fn durable_unique_index_keys_omit_primary_key_suffix() {
    let storage =
        MemoryStorage::new(&["albums", "indices"]).expect("valid memory storage families");
    let mut database = Database::new(unique_indexed_albums_schema(), storage)
        .await
        .unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(7), Value::String("Blue Train".to_owned())],
    );
    database.commit_batch(batch).await.unwrap();

    let entries = database
        .storage
        .prefix(
            "indices".to_owned(),
            index_prefix(&database, "albums", "unique_albums_by_title").to_vec(),
        )
        .await
        .unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(
        entries[0].0,
        persisted_index_storage_key(
            &database,
            "unique_albums_by_title",
            &encoded_title_key_part("Blue Train"),
        )
    );
    assert!(!entries[0].0.ends_with(&encoded_u64_index_part(7)));
}

#[futures_test::test]
async fn primary_key_covering_indices_omit_redundant_suffix_and_recover_pk_from_key() {
    let schema = DatabaseSchema::new([TableSchema::new(
        "history",
        [
            ColumnSchema::new("row", ColumnType::U64),
            ColumnSchema::new("stamp", ColumnType::U64),
            ColumnSchema::new("node", ColumnType::U64),
            ColumnSchema::new("title", ColumnType::String),
        ],
    )
    .with_primary_key(PrimaryKey::composite([
        PrimaryKeyColumn::integer("row", IntegerKeyType::U64),
        PrimaryKeyColumn::integer("stamp", IntegerKeyType::U64),
        PrimaryKeyColumn::integer("node", IntegerKeyType::U64),
    ]))
    .with_index(IndexSchema::new("by_tx", ["stamp", "node", "row"]))]);
    let storage =
        MemoryStorage::new(&["history", "indices"]).expect("valid memory storage families");
    let mut database = Database::new(schema, storage).await.unwrap();

    let mut batch = database.open_batch();
    batch.insert("history", history_values(2, 10, 1, "older"));
    batch.insert("history", history_values(1, 20, 7, "newer"));
    database.commit_batch(batch).await.unwrap();

    let entries = database
        .storage
        .prefix(
            "indices".to_owned(),
            index_prefix(&database, "history", "by_tx").to_vec(),
        )
        .await
        .unwrap();
    assert_eq!(
        entries
            .iter()
            .map(|entry| entry.0.clone())
            .collect::<Vec<_>>(),
        [
            persisted_table_index_storage_key(
                &database,
                "history",
                "by_tx",
                &encoded_history_by_tx_key(10, 1, 2)
            ),
            persisted_table_index_storage_key(
                &database,
                "history",
                "by_tx",
                &encoded_history_by_tx_key(20, 7, 1)
            ),
        ]
    );
    assert!(
        entries
            .iter()
            .all(|(_, record)| persisted_index_value(record).is_empty())
    );

    let latest = database
        .index_last_raw("history", "by_tx", &[])
        .await
        .unwrap()
        .unwrap();
    assert_eq!(latest.key(), &history_key(1, 20, 7).into_bytes());
    assert_eq!(
        latest.record().get("title").unwrap(),
        Value::String("newer".to_owned())
    );

    let stamp_scan = database
        .index_scan("history", "by_tx", &[Value::U64(10)])
        .await
        .unwrap();
    assert_eq!(
        record_values(stamp_scan),
        [history_values(2, 10, 1, "older")]
    );
}

#[futures_test::test]
async fn unique_indices_reject_existing_conflicting_values() {
    let storage =
        MemoryStorage::new(&["albums", "indices"]).expect("valid memory storage families");
    let mut database = Database::new(unique_indexed_albums_schema(), storage)
        .await
        .unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(7), Value::String("Blue Train".to_owned())],
    );
    database.commit_batch(batch).await.unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(8), Value::String("Blue Train".to_owned())],
    );
    assert!(matches!(
        database.commit_batch(batch).await.unwrap_err(),
        Error::IvmRuntime(IvmRuntimeError::UniqueIndexViolation { .. })
    ));

    let prefix = index_prefix(&database, "albums", "unique_albums_by_title");
    let entries = database
        .storage
        .prefix("indices".to_owned(), prefix.to_vec())
        .await
        .unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(
        persisted_index_value(&entries[0].1),
        encoded_u64_index_part(7)
    );
}

// This stays at the Groove database seam because unique-index delta ordering
// is consolidated before any public Jazz query or mutation result exists.
/// A batch atomically transfers a unique title from `alice` (id 7) to `bob`
/// (id 8), regardless of whether its delete or insert is submitted first.
///
/// ```text
/// alice ──retract──┐
/// bob   ──insert───┴──► durable unique index ──► bob owns title
/// ```
#[futures_test::test]
async fn durable_unique_indices_allow_atomic_replacement_within_one_batch() {
    for insert_first in [false, true] {
        let storage =
            MemoryStorage::new(&["albums", "indices"]).expect("valid memory storage families");
        let mut database = Database::new(unique_indexed_albums_schema(), storage)
            .await
            .unwrap();

        let mut batch = database.open_batch();
        batch.insert(
            "albums",
            vec![Value::U64(7), Value::String("Blue Train".to_owned())],
        );
        database.commit_batch(batch).await.unwrap();

        let mut batch = database.open_batch();
        if insert_first {
            batch.insert(
                "albums",
                vec![Value::U64(8), Value::String("Blue Train".to_owned())],
            );
            batch.delete("albums", PrimaryKeyValue::U64(7));
        } else {
            batch.delete("albums", PrimaryKeyValue::U64(7));
            batch.insert(
                "albums",
                vec![Value::U64(8), Value::String("Blue Train".to_owned())],
            );
        }
        database.commit_batch(batch).await.unwrap();

        assert_eq!(
            record_values(
                database
                    .index_scan(
                        "albums",
                        "unique_albums_by_title",
                        &[Value::String("Blue Train".to_owned())],
                    )
                    .await
                    .unwrap()
            ),
            [vec![Value::U64(8), Value::String("Blue Train".to_owned())]]
        );
    }
}

/// `alice` owns a title before `bob` attempts a competing positive insertion;
/// the durable unique index rejects every attempt and retains `alice`.
#[futures_test::test]
async fn durable_unique_indices_reject_positive_delta_for_existing_different_record() {
    let storage =
        MemoryStorage::new(&["albums", "indices"]).expect("valid memory storage families");
    let mut database = Database::new(unique_indexed_albums_schema(), storage)
        .await
        .unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(7), Value::String("Blue Train".to_owned())],
    );
    database.commit_batch(batch).await.unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(8), Value::String("Blue Train".to_owned())],
    );

    assert!(matches!(
        database.commit_batch(batch).await.unwrap_err(),
        Error::IvmRuntime(IvmRuntimeError::UniqueIndexViolation { .. })
    ));
}

/// Competing same-batch positive owners (`alice` and `bob`) fail closed before
/// the durable unique index writes, in either insertion order.
#[futures_test::test]
async fn unique_indices_reject_conflicts_within_one_batch() {
    for ids in [[7_u64, 8], [8, 7]] {
        let storage =
            MemoryStorage::new(&["albums", "indices"]).expect("valid memory storage families");
        let mut database = Database::new(unique_indexed_albums_schema(), storage)
            .await
            .unwrap();

        let mut batch = database.open_batch();
        for id in ids {
            batch.insert(
                "albums",
                vec![Value::U64(id), Value::String("Blue Train".to_owned())],
            );
        }

        assert!(matches!(
            database.commit_batch(batch).await.unwrap_err(),
            Error::IvmRuntime(IvmRuntimeError::UniqueIndexViolation { .. })
        ));
        assert!(
            database
                .storage
                .prefix(
                    "indices".to_owned(),
                    index_prefix(&database, "albums", "unique_albums_by_title").to_vec()
                )
                .await
                .unwrap()
                .is_empty()
        );
    }
}

#[futures_test::test]
async fn table_and_index_state_survive_restart_for_resubscribed_graphs() {
    let table_graph = GraphBuilder::table("albums");
    let index_graph = GraphBuilder::index("albums", "albums_by_title");

    let storage = {
        let storage =
            MemoryStorage::new(&["albums", "indices"]).expect("valid memory storage families");
        let mut database = Database::new(indexed_albums_schema(), storage)
            .await
            .unwrap();
        database
            .subscribe_one_sink(table_graph.clone())
            .await
            .unwrap();
        database
            .subscribe_one_sink(index_graph.clone())
            .await
            .unwrap();

        let mut batch = database.open_batch();
        batch.insert(
            "albums",
            vec![Value::U64(7), Value::String("Blue Train".to_owned())],
        );
        database.commit_batch(batch).await.unwrap();
        database.into_storage()
    };

    {
        let mut database = Database::new(indexed_albums_schema(), storage)
            .await
            .unwrap();
        let table_subscription_id = database.subscribe_one_sink(table_graph).await.unwrap();
        let index_subscription_id = database.subscribe_one_sink(index_graph).await.unwrap();

        database.flush().await.unwrap();
        assert_eq!(
            expect_recv_vals(&table_subscription_id),
            [(vec![7_u64.into(), "Blue Train".into()], 1)]
        );
        assert_eq!(
            expect_recv_vals(&index_subscription_id),
            [(
                vec![
                    encoded_title_index_key("Blue Train", 7).into(),
                    Vec::<u8>::new().into(),
                ],
                1,
            )]
        );

        let mut batch = database.open_batch();
        batch.update(
            "albums",
            vec![Value::U64(7), Value::String("Giant Steps".to_owned())],
        );
        database.commit_batch(batch).await.unwrap();

        assert_eq!(
            expect_recv_vals(&table_subscription_id),
            [
                (vec![7_u64.into(), "Blue Train".into()], -1),
                (vec![7_u64.into(), "Giant Steps".into()], 1),
            ]
        );

        assert_eq!(
            expect_recv_vals(&index_subscription_id),
            [
                (
                    vec![
                        encoded_title_index_key("Blue Train", 7).into(),
                        Vec::<u8>::new().into(),
                    ],
                    -1,
                ),
                (
                    vec![
                        encoded_title_index_key("Giant Steps", 7).into(),
                        Vec::<u8>::new().into(),
                    ],
                    1,
                ),
            ]
        );
    }
}

#[futures_test::test]
async fn persisted_indices_can_be_deleted_after_restart() {
    let table_graph = GraphBuilder::table("albums");
    let index_graph = GraphBuilder::index("albums", "albums_by_title");

    let storage = {
        let storage =
            MemoryStorage::new(&["albums", "indices"]).expect("valid memory storage families");
        let mut database = Database::new(indexed_albums_schema(), storage)
            .await
            .unwrap();
        database
            .subscribe_one_sink(table_graph.clone())
            .await
            .unwrap();
        database
            .subscribe_one_sink(index_graph.clone())
            .await
            .unwrap();

        let mut batch = database.open_batch();
        batch.insert(
            "albums",
            vec![Value::U64(7), Value::String("Blue Train".to_owned())],
        );
        database.commit_batch(batch).await.unwrap();
        database.into_storage()
    };

    {
        let mut database = Database::new(indexed_albums_schema(), storage)
            .await
            .unwrap();
        let table_subscription_id = database.subscribe_one_sink(table_graph).await.unwrap();
        let index_subscription_id = database.subscribe_one_sink(index_graph).await.unwrap();

        database.flush().await.unwrap();
        assert_eq!(
            expect_recv_vals(&table_subscription_id),
            [(vec![7_u64.into(), "Blue Train".into()], 1)]
        );
        assert_eq!(
            expect_recv_vals(&index_subscription_id),
            [(
                vec![
                    encoded_title_index_key("Blue Train", 7).into(),
                    Vec::<u8>::new().into(),
                ],
                1,
            )]
        );

        let mut batch = database.open_batch();
        batch.delete("albums", PrimaryKeyValue::U64(7));
        database.commit_batch(batch).await.unwrap();

        assert_eq!(
            expect_recv_vals(&table_subscription_id),
            [(vec![7_u64.into(), "Blue Train".into()], -1)]
        );
        assert_eq!(
            expect_recv_vals(&index_subscription_id),
            [(
                vec![
                    encoded_title_index_key("Blue Train", 7).into(),
                    Vec::<u8>::new().into(),
                ],
                -1,
            )]
        );
    }
}

/// A public backfill with duplicate positive owners is rejected cleanly in
/// either order: `alice` and `bob` keep using the unmodified database.
#[futures_test::test]
async fn rejected_live_unique_backfill_preserves_runtime_storage_and_usability() {
    for records in [
        [(7_u64, "Blue Train"), (8_u64, "Blue Train")],
        [(8_u64, "Blue Train"), (7_u64, "Blue Train")],
    ] {
        let (storage, control) = TestStorage::controlled(&["albums", "indices"]);
        let mut database = Database::new(albums_schema(), storage).await.unwrap();
        let mut batch = database.open_batch();
        for (id, title) in records {
            batch.insert(
                "albums",
                vec![Value::U64(id), Value::String(title.to_owned())],
            );
        }
        database.commit_batch(batch).await.unwrap();

        let schema_before = database.ivm_runtime.schema().clone();
        let stats_before = database.runtime_stats();
        let nodes_before = database.ivm_runtime.retained_node_ids();
        let index_prefix = index_prefix(&database, "albums", "unique_albums_by_title");
        let index_bytes_before = database
            .storage
            .prefix("indices".to_owned(), index_prefix.to_vec())
            .await
            .unwrap();
        let rows_before = database.primary_key_scan("albums", &[]).await.unwrap();
        control.take_observed();

        let index = IndexSchema::new("unique_albums_by_title", ["title"]).unique();
        let error = database
            .register_table_index("albums", index.clone())
            .await
            .expect_err("duplicate positive backfill must reject");
        assert!(matches!(
            error,
            Error::IvmRuntime(IvmRuntimeError::UniqueIndexViolation { index: name })
                if name == "albums.unique_albums_by_title"
        ));
        assert!(
            !control
                .take_observed()
                .contains(&TestStorageOperation::WriteMany),
            "rejected backfill must not submit registration writes"
        );
        assert_eq!(database.ivm_runtime.schema(), &schema_before);
        assert_eq!(database.runtime_stats(), stats_before);
        assert_eq!(database.ivm_runtime.retained_node_ids(), nodes_before);
        assert_eq!(
            database
                .storage
                .prefix("indices".to_owned(), index_prefix.to_vec())
                .await
                .unwrap(),
            index_bytes_before
        );
        assert_eq!(
            database.primary_key_scan("albums", &[]).await.unwrap(),
            rows_before
        );
        assert!(database.ensure_usable().is_ok());
        assert!(matches!(
            database
                .index_get(
                    "albums",
                    "unique_albums_by_title",
                    &[Value::String("Blue Train".to_owned())],
                )
                .await,
            Err(Error::IndexNotFound { table, index })
                if table == "albums" && index == "unique_albums_by_title"
        ));
        let retry_error = database
            .register_table_index("albums", index.clone())
            .await
            .expect_err("the duplicate must continue rejecting before correction");
        assert!(matches!(
            retry_error,
            Error::IvmRuntime(IvmRuntimeError::UniqueIndexViolation { index: name })
                if name == "albums.unique_albums_by_title"
        ));

        let mut correction = database.open_batch();
        correction.delete("albums", PrimaryKeyValue::U64(8));
        correction.insert(
            "albums",
            vec![Value::U64(9), Value::String("Kind of Blue".to_owned())],
        );
        database.commit_batch(correction).await.unwrap();
        database
            .register_table_index("albums", index)
            .await
            .unwrap();
        assert_eq!(
            record_values(
                database
                    .index_get(
                        "albums",
                        "unique_albums_by_title",
                        &[Value::String("Blue Train".to_owned())],
                    )
                    .await
                    .unwrap()
            ),
            [vec![Value::U64(7), Value::String("Blue Train".to_owned())]]
        );
        assert_eq!(
            record_values(
                database
                    .index_get(
                        "albums",
                        "unique_albums_by_title",
                        &[Value::String("Kind of Blue".to_owned())],
                    )
                    .await
                    .unwrap()
            ),
            [vec![
                Value::U64(9),
                Value::String("Kind of Blue".to_owned())
            ]]
        );
    }
}

async fn assert_poisoned_registration_lifecycle(database: &mut Database) {
    assert!(matches!(
        database.primary_key_scan("albums", &[]).await,
        Err(Error::DatabasePoisoned)
    ));

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(9), Value::String("Kind of Blue".to_owned())],
    );
    assert!(matches!(
        database.commit_batch(batch).await,
        Err(Error::DatabasePoisoned)
    ));
    assert!(matches!(
        database
            .register_table_index("albums", IndexSchema::new("other", ["title"]))
            .await,
        Err(Error::DatabasePoisoned)
    ));
    database.close().await.unwrap();
}

/// A registration write failure after `alice`'s backfill starts poisons the
/// database so `bob` cannot observe or extend potentially partial state.
#[futures_test::test]
async fn live_index_backfill_write_failure_remains_poisoning() {
    let (storage, control) = TestStorage::controlled(&["albums", "indices"]);
    let mut database = Database::new(albums_schema(), storage).await.unwrap();
    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(7), Value::String("Blue Train".to_owned())],
    );
    database.commit_batch(batch).await.unwrap();
    let schema_before = database.ivm_runtime.schema().clone();
    let stats_before = database.runtime_stats();
    let nodes_before = database.ivm_runtime.retained_node_ids();
    let index_bytes_before = database
        .storage
        .prefix(
            "indices".to_owned(),
            index_prefix(&database, "albums", "unique_albums_by_title").to_vec(),
        )
        .await
        .unwrap();
    control.take_observed();
    control.fail_next(TestStorageOperation::WriteMany);

    let error = database
        .register_table_index(
            "albums",
            IndexSchema::new("unique_albums_by_title", ["title"]).unique(),
        )
        .await
        .expect_err("registration write failure must be returned");
    assert!(matches!(
        error,
        Error::IvmRuntime(IvmRuntimeError::Storage(crate::storage::Error::Backend {
            backend: "test",
            ..
        }))
    ));
    assert_eq!(
        control
            .take_observed()
            .into_iter()
            .filter(|operation| *operation == TestStorageOperation::WriteMany)
            .count(),
        1
    );
    assert_eq!(database.ivm_runtime.schema(), &schema_before);
    assert_eq!(database.runtime_stats(), stats_before);
    assert_eq!(database.ivm_runtime.retained_node_ids(), nodes_before);
    assert_eq!(
        database
            .storage
            .prefix(
                "indices".to_owned(),
                index_prefix(&database, "albums", "unique_albums_by_title").to_vec(),
            )
            .await
            .unwrap(),
        index_bytes_before
    );
    assert_poisoned_registration_lifecycle(&mut database).await;
}

/// A storage I/O failure while hydrating `alice`'s live index poisons the
/// database before `bob` can perform another operation.
#[futures_test::test]
async fn live_index_hydration_io_failure_remains_poisoning() {
    let (storage, control) = TestStorage::controlled(&["albums", "indices"]);
    let mut database = Database::new(albums_schema(), storage.clone())
        .await
        .unwrap();
    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(7), Value::String("Blue Train".to_owned())],
    );
    database.commit_batch(batch).await.unwrap();
    storage.evict_column_family("albums");
    let schema_before = database.ivm_runtime.schema().clone();
    let stats_before = database.runtime_stats();
    let nodes_before = database.ivm_runtime.retained_node_ids();
    let index_bytes_before = database
        .storage
        .prefix(
            "indices".to_owned(),
            index_prefix(&database, "albums", "unique_albums_by_title").to_vec(),
        )
        .await
        .unwrap();
    control.take_observed();
    control.fail_next(TestStorageOperation::ScanOpen);

    let error = database
        .register_table_index(
            "albums",
            IndexSchema::new("unique_albums_by_title", ["title"]).unique(),
        )
        .await
        .expect_err("hydration IO failure must be returned");
    assert!(matches!(
        error,
        Error::IvmRuntime(IvmRuntimeError::Storage(crate::storage::Error::Backend {
            backend: "test",
            ..
        }))
    ));
    let observed = control.take_observed();
    assert!(observed.contains(&TestStorageOperation::ScanOpen));
    assert!(!observed.contains(&TestStorageOperation::WriteMany));
    assert_eq!(database.ivm_runtime.schema(), &schema_before);
    assert_eq!(database.runtime_stats(), stats_before);
    assert_eq!(database.ivm_runtime.retained_node_ids(), nodes_before);
    assert_eq!(
        database
            .storage
            .prefix(
                "indices".to_owned(),
                index_prefix(&database, "albums", "unique_albums_by_title").to_vec(),
            )
            .await
            .unwrap(),
        index_bytes_before
    );
    assert_poisoned_registration_lifecycle(&mut database).await;
}

/// A runtime record-decoding failure during `alice`'s backfill poisons the
/// database before `bob` can use it again.
#[futures_test::test]
async fn live_index_non_unique_runtime_error_remains_poisoning() {
    let (storage, control) = TestStorage::controlled(&["albums", "indices"]);
    let mut database = Database::new(albums_schema(), storage).await.unwrap();
    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(7), Value::String("Blue Train".to_owned())],
    );
    database.commit_batch(batch).await.unwrap();

    database
        .storage
        .set(
            "albums".to_owned(),
            PrimaryKeyValue::U64(7).into_bytes(),
            vec![0xff],
        )
        .await
        .unwrap();
    let schema_before = database.ivm_runtime.schema().clone();
    let stats_before = database.runtime_stats();
    let nodes_before = database.ivm_runtime.retained_node_ids();
    let index_bytes_before = database
        .storage
        .prefix(
            "indices".to_owned(),
            index_prefix(&database, "albums", "unique_albums_by_title").to_vec(),
        )
        .await
        .unwrap();
    let table_bytes_before = database
        .storage
        .prefix("albums".to_owned(), Vec::new())
        .await
        .unwrap();
    control.take_observed();

    let error = database
        .register_table_index(
            "albums",
            IndexSchema::new("unique_albums_by_title", ["title"]).unique(),
        )
        .await
        .expect_err("record encoding failure must be returned");
    assert!(matches!(
        error,
        Error::IvmRuntime(IvmRuntimeError::RecordEncoding(_))
    ));
    assert!(
        !control
            .take_observed()
            .contains(&TestStorageOperation::WriteMany)
    );
    assert_eq!(database.ivm_runtime.schema(), &schema_before);
    assert_eq!(database.runtime_stats(), stats_before);
    assert_eq!(database.ivm_runtime.retained_node_ids(), nodes_before);
    assert_eq!(
        database
            .storage
            .prefix(
                "indices".to_owned(),
                index_prefix(&database, "albums", "unique_albums_by_title").to_vec(),
            )
            .await
            .unwrap(),
        index_bytes_before
    );
    assert_eq!(
        database
            .storage
            .prefix("albums".to_owned(), Vec::new())
            .await
            .unwrap(),
        table_bytes_before
    );
    assert_poisoned_registration_lifecycle(&mut database).await;
}

#[futures_test::test]
async fn live_index_registration_rejects_while_a_publication_is_resident() {
    let storage =
        MemoryStorage::new(&["albums", "indices"]).expect("valid memory storage families");
    let mut database = Database::new(albums_schema(), storage).await.unwrap();
    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(7), Value::String("Blue Train".to_owned())],
    );
    let applied = database.apply_batch(batch).await.unwrap();
    let index = IndexSchema::new("albums_by_title", ["title"]);

    let error = database
        .register_table_index("albums", index.clone())
        .await
        .expect_err("schema mutation must not race a resident publication");
    assert!(matches!(
        error,
        Error::TableIndexRegistrationWhilePublicationsResident { table, index }
            if table == "albums" && index == "albums_by_title"
    ));

    let persisted = applied.persist().await;
    database.finish_persistence(persisted).unwrap();
    database
        .register_table_index("albums", index)
        .await
        .unwrap();
    assert_eq!(
        record_values(
            database
                .index_scan(
                    "albums",
                    "albums_by_title",
                    &[Value::String("Blue Train".to_owned())],
                )
                .await
                .unwrap()
        ),
        [vec![Value::U64(7), Value::String("Blue Train".to_owned())]]
    );
}

// Internal storage corruption is necessary to reproduce old writer bugs and
// pin the durable repair marker; assertions use normal indexed/primary reads.
#[futures_test::test]
async fn declared_index_generation_repairs_missing_and_stale_entries_once() {
    let storage = MemoryStorage::new(&["albums", "indices"]).unwrap();
    let mut database = Database::new(indexed_albums_schema(), storage.clone())
        .await
        .unwrap();
    let mut batch = database.open_batch();
    for id in 0..1030 {
        batch.insert(
            "albums",
            vec![Value::U64(id), Value::String(format!("title-{id}"))],
        );
    }
    database.commit_batch(batch).await.unwrap();
    let primary_before = storage.prefix("albums".into(), Vec::new()).await.unwrap();
    let expected = storage
        .prefix(
            "indices".into(),
            index_prefix(&database, "albums", "albums_by_title").to_vec(),
        )
        .await
        .unwrap();
    assert_eq!(expected.len(), 1030);
    storage
        .delete("indices".into(), expected[0].0.clone())
        .await
        .unwrap();
    let stale_key = {
        let mut key = index_prefix(&database, "albums", "albums_by_title");
        key.extend(b"obsolete");
        key
    };
    storage
        .set(
            "indices".into(),
            stale_key.clone(),
            b"invalid obsolete value".to_vec(),
        )
        .await
        .unwrap();
    drop(database);
    let mut database = Database::new(indexed_albums_schema(), storage.clone())
        .await
        .unwrap();
    database.ensure_declared_index_generation(1).await.unwrap();
    assert_eq!(
        storage
            .prefix(
                "indices".into(),
                index_prefix(&database, "albums", "albums_by_title").to_vec()
            )
            .await
            .unwrap(),
        expected
    );
    assert_eq!(
        storage.prefix("albums".into(), Vec::new()).await.unwrap(),
        primary_before
    );
    assert_eq!(
        database
            .index_scan_raw("albums", "albums_by_title", &[])
            .await
            .unwrap()
            .len(),
        1030
    );
    // Byte fixture: one NUL followed by ASCII, then exactly BE u64.
    assert_eq!(
        storage
            .get(
                "indices".into(),
                b"\x00groove-declared-index-generation".to_vec()
            )
            .await
            .unwrap(),
        Some(vec![0, 0, 0, 0, 0, 0, 0, 1])
    );
    // A completed generation does no repair work on reopen. Injecting a stale
    // entry distinguishes that fast path from a silently repeated rebuild.
    storage
        .set("indices".into(), stale_key.clone(), b"sentinel".to_vec())
        .await
        .unwrap();
    drop(database);
    let mut database = Database::new(indexed_albums_schema(), storage.clone())
        .await
        .unwrap();
    database.ensure_declared_index_generation(1).await.unwrap();
    assert_eq!(
        storage
            .get("indices".into(), stale_key.clone())
            .await
            .unwrap(),
        Some(b"sentinel".to_vec())
    );
    database.ensure_declared_index_generation(2).await.unwrap();
    assert_eq!(
        storage.get("indices".into(), stale_key).await.unwrap(),
        None
    );
    assert_eq!(
        storage.prefix("albums".into(), Vec::new()).await.unwrap(),
        primary_before
    );
}

// Storage failpoints/cancellation cannot be driven through a public query.
#[futures_test::test]
async fn declared_index_generation_retries_cancelled_and_failed_repair() {
    for cancel in [false, true] {
        let (storage, control) = TestStorage::controlled(&["albums", "indices"]);
        let mut database = Database::new(indexed_albums_schema(), storage.clone())
            .await
            .unwrap();
        let mut batch = database.open_batch();
        batch.insert("albums", vec![Value::U64(7), Value::String("title".into())]);
        database.commit_batch(batch).await.unwrap();
        if cancel {
            control.pause_on(TestStorageOperation::WriteMany);
            control.release_one(); // Clear commits; suspend before replay.
            let mut repair = Box::pin(database.ensure_declared_index_generation(1));
            control.take_observed();
            for _ in 0..100 {
                assert!(futures::poll!(repair.as_mut()).is_pending());
                if control
                    .observed()
                    .iter()
                    .filter(|op| **op == TestStorageOperation::WriteMany)
                    .count()
                    == 2
                {
                    break;
                }
            }
            assert_eq!(
                control
                    .observed()
                    .iter()
                    .filter(|op| **op == TestStorageOperation::WriteMany)
                    .count(),
                2
            );
            drop(repair);
            control.resume();
        } else {
            control.fail_next(TestStorageOperation::FlushWriteBoundary);
            assert!(database.ensure_declared_index_generation(1).await.is_err());
        }
        assert!(matches!(
            database.ensure_usable(),
            Err(Error::DatabasePoisoned)
        ));
        assert_eq!(
            storage
                .get(
                    "indices".into(),
                    b"\0groove-declared-index-generation".to_vec()
                )
                .await
                .unwrap(),
            None
        );
        drop(database);
        let mut database = Database::new(indexed_albums_schema(), storage.clone())
            .await
            .unwrap();
        database.ensure_declared_index_generation(1).await.unwrap();
        assert_eq!(
            database
                .index_scan_raw("albums", "albums_by_title", &[])
                .await
                .unwrap()
                .len(),
            1
        );
        assert_eq!(
            database
                .primary_key_scan_raw("albums", &[])
                .await
                .unwrap()
                .len(),
            1
        );
    }
}

// Pin malformed/future durable marker handling before destructive repair.
#[futures_test::test]
async fn declared_index_generation_rejects_unknown_marker_without_writing() {
    for bytes in [vec![], vec![1], 2u64.to_be_bytes().to_vec()] {
        let (storage, control) = TestStorage::controlled(&["albums", "indices"]);
        storage
            .set(
                "indices".into(),
                b"\0groove-declared-index-generation".to_vec(),
                bytes,
            )
            .await
            .unwrap();
        let mut database = Database::new(indexed_albums_schema(), storage)
            .await
            .unwrap();
        control.take_observed();
        assert!(matches!(
            database.ensure_declared_index_generation(1).await,
            Err(Error::InvalidPersistedIndex(_))
        ));
        assert!(
            control
                .take_observed()
                .iter()
                .all(|op| *op == TestStorageOperation::Get)
        );
    }
}

// Primary-only installation emulates an older database with a broken unique
// index; duplicates across replay batches must fail closed without marking done.
#[futures_test::test]
async fn declared_index_generation_rejects_duplicate_unique_owners_across_batches() {
    let storage = MemoryStorage::new(&["albums", "indices"]).unwrap();
    let mut database = Database::new(albums_schema(), storage.clone())
        .await
        .unwrap();
    let mut batch = database.open_batch();
    for id in 0..1025 {
        let title = if id == 1024 {
            "title-0".into()
        } else {
            format!("title-{id}")
        };
        batch.insert("albums", vec![Value::U64(id), Value::String(title)]);
    }
    database.commit_batch(batch).await.unwrap();
    drop(database);
    let before = storage.prefix("albums".into(), Vec::new()).await.unwrap();
    let mut database = Database::new(unique_indexed_albums_schema(), storage.clone())
        .await
        .unwrap();
    assert!(matches!(
        database.ensure_declared_index_generation(1).await,
        Err(Error::IvmRuntime(
            IvmRuntimeError::UniqueIndexViolation { .. }
        ))
    ));
    assert_eq!(
        storage
            .get(
                "indices".into(),
                b"\0groove-declared-index-generation".to_vec()
            )
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        storage.prefix("albums".into(), Vec::new()).await.unwrap(),
        before
    );
}

// The final marker durability boundary is inaccessible via public queries.
#[futures_test::test]
async fn declared_index_generation_final_marker_flush_failure_and_cancel() {
    for cancel in [false, true] {
        let (storage, control) = TestStorage::controlled(&["albums", "indices"]);
        let mut database = Database::new(indexed_albums_schema(), storage.clone())
            .await
            .unwrap();
        let mut batch = database.open_batch();
        batch.insert("albums", vec![Value::U64(7), Value::String("title".into())]);
        database.commit_batch(batch).await.unwrap();
        let expected = storage
            .prefix(
                "indices".into(),
                index_prefix(&database, "albums", "albums_by_title").to_vec(),
            )
            .await
            .unwrap();
        control.take_observed();
        control.pause_on(TestStorageOperation::FlushWriteBoundary);
        control.release_one();
        let mut repair = Box::pin(database.ensure_declared_index_generation(1));
        for _ in 0..100 {
            assert!(futures::poll!(repair.as_mut()).is_pending());
            if control
                .observed()
                .iter()
                .filter(|op| **op == TestStorageOperation::FlushWriteBoundary)
                .count()
                == 2
            {
                break;
            }
        }
        let observed = control.observed();
        assert_eq!(
            observed
                .iter()
                .filter(|op| **op == TestStorageOperation::FlushWriteBoundary)
                .count(),
            2
        );
        let marker_write = observed
            .iter()
            .position(|op| *op == TestStorageOperation::Set)
            .unwrap();
        let first_flush = observed
            .iter()
            .position(|op| *op == TestStorageOperation::FlushWriteBoundary)
            .unwrap();
        assert!(first_flush < marker_write);
        if cancel {
            drop(repair);
        } else {
            control.fail_next(TestStorageOperation::FlushWriteBoundary);
            control.resume();
            assert!(repair.await.is_err());
        }
        control.resume();
        assert!(matches!(
            database.ensure_usable(),
            Err(Error::DatabasePoisoned)
        ));
        assert_eq!(
            storage
                .get(
                    "indices".into(),
                    b"\0groove-declared-index-generation".to_vec()
                )
                .await
                .unwrap(),
            Some(1u64.to_be_bytes().to_vec())
        );
        assert_eq!(
            storage
                .prefix(
                    "indices".into(),
                    index_prefix(&database, "albums", "albums_by_title").to_vec()
                )
                .await
                .unwrap(),
            expected
        );
        drop(database);
        let mut reopened = Database::new(indexed_albums_schema(), storage.clone())
            .await
            .unwrap();
        control.take_observed();
        let reads_before = control.point_read_count();
        reopened.ensure_declared_index_generation(1).await.unwrap();
        assert_eq!(control.point_read_count(), reads_before + 1);
        assert!(
            control
                .take_observed()
                .iter()
                .all(|op| *op == TestStorageOperation::Get)
        );
        assert_eq!(
            reopened
                .index_scan_raw("albums", "albums_by_title", &[])
                .await
                .unwrap()
                .len(),
            1
        );
    }
}

// Unknown physical variants must fail closed during primary replay. Only direct
// storage corruption can install a tag absent from the admitted schema.
#[futures_test::test]
async fn declared_index_generation_rejects_unknown_primary_variant() {
    let mut schema = indexed_albums_schema();
    schema.tables[0] = schema.tables[0].clone().with_variant(1, ["id", "title"]);
    let descriptor = schema.tables[0].record_schema_for_variant(1).unwrap();
    let storage = MemoryStorage::new(&["albums", "indices"]).unwrap();
    let mut database = Database::new(schema.clone(), storage.clone())
        .await
        .unwrap();
    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        crate::records::VariantRecord::create(
            1,
            descriptor,
            &[Value::U64(7), Value::String("title".into())],
        )
        .unwrap(),
    );
    database.commit_batch(batch).await.unwrap();
    drop(database);
    let rows = storage.prefix("albums".into(), Vec::new()).await.unwrap();
    let raw = descriptor
        .create(&[Value::U64(7), Value::String("title".into())])
        .unwrap();
    let invalid = crate::records::encode_variant_record(99, &raw);
    storage
        .set("albums".into(), rows[0].0.clone(), invalid.clone())
        .await
        .unwrap();
    let mut database = Database::new(schema, storage.clone()).await.unwrap();
    assert!(database.ensure_declared_index_generation(1).await.is_err());
    assert!(matches!(
        database.ensure_usable(),
        Err(Error::DatabasePoisoned)
    ));
    assert_eq!(
        storage
            .get(
                "indices".into(),
                b"\0groove-declared-index-generation".to_vec()
            )
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        storage
            .get("albums".into(), rows[0].0.clone())
            .await
            .unwrap(),
        Some(invalid)
    );
}

// ---------------------------------------------------------------------------
// Durable index layout v2: numeric ids, compact keys, empty values.
// ---------------------------------------------------------------------------

fn registered_index_id(database: &Database, table: &str, index: &str) -> u32 {
    database
        .ivm_runtime
        .index_ids()
        .borrow()
        .id(table, index)
        .unwrap_or_else(|| panic!("{table}.{index} has a durable id"))
}

fn albums_schema_with_indices(indices: Vec<IndexSchema>) -> DatabaseSchema {
    let mut table = TableSchema::new(
        "albums",
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("title", ColumnType::String),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64));
    for index in indices {
        table = table.with_index(index);
    }
    DatabaseSchema::new([table])
}

async fn indexed_titles(database: &Database, index: &str) -> Vec<Vec<u8>> {
    database
        .index_scan_raw("albums", index, &[])
        .await
        .unwrap()
        .into_iter()
        .map(|entry| entry.key().to_vec())
        .collect()
}

#[futures_test::test]
async fn durable_index_ids_survive_reopen_added_indices_and_reordering() {
    let storage = MemoryStorage::new(&["albums", "indices"]).unwrap();
    let by_title = IndexSchema::new("albums_by_title", ["title"]);
    let mut database = Database::new(
        albums_schema_with_indices(vec![by_title.clone()]),
        storage.clone(),
    )
    .await
    .unwrap();
    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(7), Value::String("Blue Train".into())],
    );
    batch.insert(
        "albums",
        vec![Value::U64(8), Value::String("Kind of Blue".into())],
    );
    database.commit_batch(batch).await.unwrap();
    let title_id = registered_index_id(&database, "albums", "albums_by_title");
    let before = indexed_titles(&database, "albums_by_title").await;
    assert_eq!(before.len(), 2);
    drop(database);

    // A newly declared index, listed first, gets the next id; the existing
    // one keeps its id and its entries stay readable.
    let reordered = albums_schema_with_indices(vec![
        IndexSchema::new("albums_by_title_and_id", ["title", "id"]),
        by_title.clone(),
    ]);
    let database = Database::new(reordered.clone(), storage.clone())
        .await
        .unwrap();
    assert_eq!(
        registered_index_id(&database, "albums", "albums_by_title"),
        title_id
    );
    let added_id = registered_index_id(&database, "albums", "albums_by_title_and_id");
    assert_eq!(added_id, title_id + 1);
    assert_eq!(indexed_titles(&database, "albums_by_title").await, before);
    drop(database);

    let mut database = Database::new(reordered, storage.clone()).await.unwrap();
    assert_eq!(
        registered_index_id(&database, "albums", "albums_by_title"),
        title_id
    );
    // The added id was only allocated in memory by the previous instance;
    // it becomes durable with this instance's first publication.
    database.commit_batch(database.open_batch()).await.unwrap();
    assert_eq!(
        registered_index_id(&database, "albums", "albums_by_title_and_id"),
        added_id
    );
    drop(database);
    let database = Database::new(albums_schema_with_indices(vec![by_title]), storage)
        .await
        .unwrap();
    assert_eq!(
        registered_index_id(&database, "albums", "albums_by_title"),
        title_id
    );
    assert_eq!(
        registered_index_id(&database, "albums", "albums_by_title_and_id"),
        added_id,
        "an undeclared index keeps its registration"
    );
}

#[futures_test::test]
async fn redefined_or_recreated_index_gets_a_fresh_id_and_never_reads_stale_entries() {
    let storage = MemoryStorage::new(&["albums", "indices"]).unwrap();
    let mut database = Database::new(indexed_albums_schema(), storage.clone())
        .await
        .unwrap();
    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(7), Value::String("Blue Train".into())],
    );
    batch.insert(
        "albums",
        vec![Value::U64(8), Value::String("Kind of Blue".into())],
    );
    database.commit_batch(batch).await.unwrap();
    let first_id = registered_index_id(&database, "albums", "albums_by_title");
    drop(database);

    // Same name, changed definition: a fresh id. Entries under the old id
    // are not read, even before the declared-index repair backfills.
    let redefined =
        albums_schema_with_indices(vec![IndexSchema::new("albums_by_title", ["title", "id"])]);
    let mut database = Database::new(redefined, storage.clone()).await.unwrap();
    let second_id = registered_index_id(&database, "albums", "albums_by_title");
    assert!(second_id > first_id);
    assert!(
        indexed_titles(&database, "albums_by_title")
            .await
            .is_empty()
    );
    database.ensure_declared_index_generation(1).await.unwrap();
    assert_eq!(indexed_titles(&database, "albums_by_title").await.len(), 2);
    drop(database);

    // Drop the index, change rows while it is absent, then recreate it with
    // the identical definition: the recreation backfills under a fresh id,
    // so neither the deleted row nor a missing entry leaks through.
    let mut database = Database::new(albums_schema(), storage.clone())
        .await
        .unwrap();
    let mut batch = database.open_batch();
    batch.delete("albums", PrimaryKeyValue::U64(7));
    batch.insert(
        "albums",
        vec![Value::U64(9), Value::String("Giant Steps".into())],
    );
    database.commit_batch(batch).await.unwrap();
    database
        .register_table_index(
            "albums",
            IndexSchema::new("albums_by_title", ["title", "id"]),
        )
        .await
        .unwrap();
    let third_id = registered_index_id(&database, "albums", "albums_by_title");
    assert!(third_id > second_id);
    let titles = database
        .index_scan("albums", "albums_by_title", &[])
        .await
        .unwrap()
        .into_iter()
        .map(|record| record.record().get("title").unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        titles,
        [
            Value::String("Giant Steps".into()),
            Value::String("Kind of Blue".into())
        ]
    );
    drop(database);

    // Retired ids are never reused, even after reopening.
    let database = Database::new(
        albums_schema_with_indices(vec![IndexSchema::new("albums_by_year", ["id"])]),
        storage,
    )
    .await
    .unwrap();
    assert_eq!(
        registered_index_id(&database, "albums", "albums_by_year"),
        third_id + 1
    );
}

#[futures_test::test]
async fn first_index_entries_carry_the_exact_id_registration_and_layout_marker() {
    let storage = MemoryStorage::new(&["albums", "indices"]).unwrap();
    let mut database = Database::new(indexed_albums_schema(), storage.clone())
        .await
        .unwrap();
    // Opening alone writes nothing: registrations are lazy.
    assert!(
        storage
            .prefix("indices".into(), Vec::new())
            .await
            .unwrap()
            .is_empty()
    );
    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(7), Value::String("Blue Train".into())],
    );
    database.commit_batch(batch).await.unwrap();

    // Byte fixture: `\0groove-index-id\0`, u16 BE table length, table,
    // index -> u32 BE id, unique flag, u16 BE column count, then u16 BE
    // length + name per column.
    let mut registry_key = b"\0groove-index-id\0\x00\x06albums".to_vec();
    registry_key.extend_from_slice(b"albums_by_title");
    assert_eq!(
        storage.prefix("indices".into(), Vec::new()).await.unwrap(),
        vec![
            (
                registry_key,
                b"\x00\x00\x00\x01\x00\x00\x01\x00\x05title".to_vec()
            ),
            (
                b"\0groove-index-layout".to_vec(),
                b"groove-durable-index-v2".to_vec()
            ),
            (
                [
                    &[0x01][..],
                    b"\x06Blue Train\x00\x00\xff\x03\x00\x00\x00\x00\x00\x00\x00\x07"
                ]
                .concat(),
                Vec::new()
            ),
        ]
    );
}

#[futures_test::test]
async fn durable_index_layout_refuses_earlier_or_unknown_index_layouts() {
    // An `indices` family written by the previous layout: name-prefixed
    // entries and no layout marker.
    let storage = MemoryStorage::new(&["albums", "indices"]).unwrap();
    storage
        .set(
            "indices".into(),
            b"\0groove-declared-index-generation".to_vec(),
            1_u64.to_be_bytes().to_vec(),
        )
        .await
        .unwrap();
    // Metadata alone (keys starting with 0x00) is not an index entry.
    Database::new(indexed_albums_schema(), storage.clone())
        .await
        .unwrap();
    storage
        .set(
            "indices".into(),
            b"albums\0albums_by_title\0\x07\x06Blue Train\x00\x00\x00\x00".to_vec(),
            b"legacy record".to_vec(),
        )
        .await
        .unwrap();
    for storage in [storage, {
        let unknown = MemoryStorage::new(&["albums", "indices"]).unwrap();
        unknown
            .set(
                "indices".into(),
                b"\0groove-index-layout".to_vec(),
                b"groove-durable-index-v3".to_vec(),
            )
            .await
            .unwrap();
        unknown
    }] {
        match Database::new(indexed_albums_schema(), storage).await {
            Err(Error::Storage(error)) => assert!(
                matches!(*error, crate::storage::Error::InvalidStorageLayout(_)),
                "expected InvalidStorageLayout, got {error:?}"
            ),
            Err(other) => panic!("expected a storage layout refusal, got {other:?}"),
            Ok(_) => panic!("an earlier or unknown index layout must be refused"),
        }
    }
}

#[futures_test::test]
async fn composite_primary_key_dedup_and_unique_entries_have_exact_compact_bytes() {
    // Jazz-shaped: `(branch_key, row_uuid)` primary key; the fk index leads
    // with the branch, so only `row_uuid` follows the separator.
    let schema = DatabaseSchema::new([TableSchema::new(
        "current",
        [
            ColumnSchema::new("branch_key", ColumnType::Bytes),
            ColumnSchema::new("row_uuid", ColumnType::Uuid),
            ColumnSchema::new("parent", ColumnType::Uuid.nullable()),
        ],
    )
    .with_primary_key(PrimaryKey::composite([
        PrimaryKeyColumn::bytes("branch_key"),
        PrimaryKeyColumn::uuid("row_uuid"),
    ]))
    .with_index(IndexSchema::new("by_parent", ["branch_key", "parent"]))
    .with_index(IndexSchema::new("unique_parent", ["parent"]).unique())
    .with_index(IndexSchema::new("by_row", ["row_uuid", "branch_key"]))]);
    let storage = MemoryStorage::new(&["current", "indices"]).unwrap();
    let mut database = Database::new(schema, storage.clone()).await.unwrap();
    let row = uuid::Uuid::from_bytes([0x11; 16]);
    let parent = uuid::Uuid::from_bytes([0x22; 16]);
    let mut batch = database.open_batch();
    batch.insert(
        "current",
        vec![
            Value::Bytes(vec![0x01, 0x00, 0x00, 0x00, 0x00]),
            Value::Uuid(row),
            Value::Nullable(Some(Box::new(Value::Uuid(parent)))),
        ],
    );
    database.commit_batch(batch).await.unwrap();

    let branch: &[u8] = b"\x07\x01\x00\xff\x00\xff\x00\xff\x00\xff\x00\x00";
    let parent_part = [&[0x09, 0x0a][..], parent.as_bytes()].concat();
    let row_part = [&[0x0a][..], row.as_bytes()].concat();
    let entries = |index: &str| {
        let prefix = index_prefix(&database, "current", index);
        let storage = storage.clone();
        async move { storage.prefix("indices".into(), prefix).await.unwrap() }
    };
    // [id][branch 12][parent 18][ff][row_uuid 17] -> empty value.
    let by_parent = entries("by_parent").await;
    assert_eq!(
        by_parent,
        vec![(
            [&[1][..], branch, &parent_part, &[0xff], &row_part].concat(),
            Vec::new()
        )]
    );
    assert_eq!(by_parent[0].0.len(), 49);
    // Unique: [id][parent 18] -> the primary-key columns the index lacks.
    assert_eq!(
        entries("unique_parent").await,
        vec![(
            [&[2][..], &parent_part].concat(),
            [branch, &row_part].concat()
        )]
    );
    // Covers the primary key: no separator, no suffix, empty value.
    assert_eq!(
        entries("by_row").await,
        vec![([&[3][..], &row_part, branch].concat(), Vec::new())]
    );
    // Every layout decodes back to the row through its index.
    for index in ["by_parent", "unique_parent", "by_row"] {
        let rows = database.index_scan("current", index, &[]).await.unwrap();
        assert_eq!(rows.len(), 1, "{index}");
        assert_eq!(
            rows[0].record().get("row_uuid").unwrap(),
            Value::Uuid(row),
            "{index}"
        );
    }
}

/// Deterministic splitmix64 stream for the key-encoding property test.
struct KeyRng(u64);

impl KeyRng {
    fn next(&mut self) -> u64 {
        self.0 = self.0.wrapping_add(0x9e37_79b9_7f4a_7c15);
        let mut z = self.0;
        z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
        z ^ (z >> 31)
    }

    fn below(&mut self, bound: u64) -> u64 {
        self.next() % bound
    }

    /// Variable-length bytes drawn mostly from boundary values.
    fn bytes(&mut self) -> Vec<u8> {
        const ALPHABET: [u8; 6] = [0x00, 0x01, 0x7f, 0xfe, 0xff, b'a'];
        (0..self.below(5))
            .map(|_| ALPHABET[self.below(ALPHABET.len() as u64) as usize])
            .collect()
    }

    fn value(&mut self, column_type: &ColumnType) -> Value {
        match column_type {
            ColumnType::U8 => Value::U8([0, 1, 0xfe, 0xff][self.below(4) as usize]),
            ColumnType::U64 => Value::U64([0, 1, 0xff, 1 << 56, u64::MAX][self.below(5) as usize]),
            ColumnType::I64 => Value::I64([i64::MIN, -1, 0, 1, i64::MAX][self.below(5) as usize]),
            ColumnType::Bool => Value::Bool(self.below(2) == 1),
            ColumnType::Bytes => Value::Bytes(self.bytes()),
            ColumnType::String => Value::String(
                ["", "\0", "\0\0", "a", "a\0", "a\0b", "\u{ff}", "ÿ\0", "b"]
                    [self.below(9) as usize]
                    .to_owned(),
            ),
            ColumnType::Uuid => {
                let byte = [0x00, 0x7f, 0xff][self.below(3) as usize];
                Value::Uuid(uuid::Uuid::from_bytes([byte; 16]))
            }
            ColumnType::Nullable(inner) => {
                if self.below(3) == 0 {
                    Value::Nullable(None)
                } else {
                    Value::Nullable(Some(Box::new(self.value(inner))))
                }
            }
            other => unreachable!("not generated: {other:?}"),
        }
    }
}

fn natural_order(left: &Value, right: &Value) -> std::cmp::Ordering {
    match (left, right) {
        (Value::U8(a), Value::U8(b)) => a.cmp(b),
        (Value::U64(a), Value::U64(b)) => a.cmp(b),
        (Value::I64(a), Value::I64(b)) => a.cmp(b),
        (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
        (Value::Bytes(a), Value::Bytes(b)) => a.cmp(b),
        (Value::String(a), Value::String(b)) => a.cmp(b),
        (Value::Uuid(a), Value::Uuid(b)) => a.cmp(b),
        (Value::Nullable(a), Value::Nullable(b)) => match (a, b) {
            (None, None) => std::cmp::Ordering::Equal,
            (None, Some(_)) => std::cmp::Ordering::Less,
            (Some(_), None) => std::cmp::Ordering::Greater,
            (Some(a), Some(b)) => natural_order(a, b),
        },
        _ => unreachable!("columns share one type"),
    }
}

/// Property: concatenated, single-escaped key parts of mixed column types
/// (with NUL/0xff bytes and variable-length strings) are prefix-free,
/// order-preserving and decode back exactly, also after a `0xff` + primary-key
/// suffix and behind a numeric index-id prefix.
#[test]
fn mixed_type_index_keys_are_prefix_free_order_preserving_and_decodable() {
    let types = [
        ColumnType::U8,
        ColumnType::U64,
        ColumnType::I64,
        ColumnType::Bool,
        ColumnType::Bytes,
        ColumnType::String,
        ColumnType::Uuid,
        ColumnType::Bytes.nullable(),
        ColumnType::U64.nullable(),
        ColumnType::String.nullable(),
    ];
    let mut rng = KeyRng(0x5eed_1dc5);
    let encode = |values: &[Value]| {
        let mut key = Vec::new();
        for value in values {
            crate::ivm::runtime::encode_key_part(&mut key, value).unwrap();
        }
        key
    };
    for _ in 0..20_000 {
        let columns = (0..1 + rng.below(4))
            .map(|_| types[rng.below(types.len() as u64) as usize].clone())
            .collect::<Vec<_>>();
        let left = columns.iter().map(|ty| rng.value(ty)).collect::<Vec<_>>();
        let right = columns.iter().map(|ty| rng.value(ty)).collect::<Vec<_>>();
        let (left_key, right_key) = (encode(&left), encode(&right));

        for (values, key) in [(&left, &left_key), (&right, &right_key)] {
            let mut remaining = key.as_slice();
            for (column, value) in columns.iter().zip(values.iter()) {
                let decoded =
                    crate::db::encoding::decode_index_key_part(&mut remaining, column, "prop")
                        .unwrap();
                assert_eq!(&decoded, value);
            }
            assert!(remaining.is_empty(), "exact consumption of {values:?}");
        }

        let expected = left
            .iter()
            .zip(right.iter())
            .map(|(a, b)| natural_order(a, b))
            .find(|ordering| ordering.is_ne())
            .unwrap_or(std::cmp::Ordering::Equal);
        assert_eq!(left_key.cmp(&right_key), expected, "{left:?} vs {right:?}");
        if expected.is_ne() {
            assert!(!right_key.starts_with(&left_key) && !left_key.starts_with(&right_key));
            // Any primary-key suffix keeps the index-column order.
            let mut left_entry = left_key.clone();
            left_entry.push(0xff);
            left_entry.extend(rng.bytes());
            let mut right_entry = right_key.clone();
            right_entry.push(0xff);
            right_entry.extend(rng.bytes());
            assert_eq!(left_entry.cmp(&right_entry), expected);
        }

        let id = [1, 127, 128, 16_383, 16_384, u32::MAX][rng.below(6) as usize];
        let mut stored = crate::ivm::runtime::durable_index_key_prefix(id);
        stored.extend_from_slice(&left_key);
        assert_eq!(
            crate::ivm::runtime::split_durable_index_key(&stored),
            Some((id, left_key.as_slice()))
        );
    }
}

/// Measurement harness: key/value bytes of one entry of each Jazz index
/// shape (column types as Jazz declares them), read raw from the physical
/// class-layout family. Run with `--nocapture` to print.
#[futures_test::test]
async fn jazz_shaped_index_entries_have_compact_sizes() {
    let branch = || Value::Bytes(vec![0x01, 0x00, 0x00, 0x00, 0x00]);
    let schema = DatabaseSchema::new([
        TableSchema::new(
            "jazz_physical_2_global_current",
            [
                ColumnSchema::new("branch_key", ColumnType::Bytes),
                ColumnSchema::new("row_uuid", ColumnType::Uuid),
                ColumnSchema::new("global_time", ColumnType::U64.nullable()),
                ColumnSchema::new("_app_project_id", ColumnType::Uuid.nullable()),
            ],
        )
        .with_primary_key(PrimaryKey::composite([
            PrimaryKeyColumn::bytes("branch_key"),
            PrimaryKeyColumn::uuid("row_uuid"),
        ]))
        .with_index(IndexSchema::new(
            "by_physical_app_v1_5",
            ["branch_key", "_app_project_id"],
        ))
        .with_index(IndexSchema::new(
            "by_seq",
            ["branch_key", "global_time", "row_uuid"],
        )),
        TableSchema::new(
            "jazz_transactions",
            [
                ColumnSchema::new("time", ColumnType::U64),
                ColumnSchema::new("node_id", ColumnType::U64),
                ColumnSchema::new("global_time", ColumnType::U64.nullable()),
            ],
        )
        .with_primary_key(PrimaryKey::composite([
            PrimaryKeyColumn::integer("time", IntegerKeyType::U64),
            PrimaryKeyColumn::integer("node_id", IntegerKeyType::U64),
        ]))
        .with_index(IndexSchema::new("by_global_time", ["global_time"])),
    ]);
    let layout = StorageLayout::jazz_class_v2();
    let families = layout.physical_column_families(schema.column_families());
    let refs = families.iter().map(String::as_str).collect::<Vec<_>>();
    let physical = MemoryStorage::new(&refs).unwrap();
    let mut database = Database::new_with_storage_layout(schema, physical.clone(), layout)
        .await
        .unwrap();
    // UUIDv7-shaped ids and realistic stamps: no zero padding to flatter
    // the escaping.
    let row = uuid::Uuid::from_u128(0x0192_1f3a_6b2c_7d4e_9f10_a2b3_c4d5_e6f7);
    let project = uuid::Uuid::from_u128(0x0192_1f3a_6b2c_7c11_8a22_b3c4_d5e6_f708);
    let (tx_time, node_id, global_time) = (1_727_000_000_123_456_u64, 0x3a5f_19c2_77e1_0b4d, 4_242);
    let mut batch = database.open_batch();
    batch.insert(
        "jazz_physical_2_global_current",
        vec![
            branch(),
            Value::Uuid(row),
            Value::Nullable(Some(Box::new(Value::U64(global_time)))),
            Value::Nullable(Some(Box::new(Value::Uuid(project)))),
        ],
    );
    batch.insert(
        "jazz_transactions",
        vec![
            Value::U64(tx_time),
            Value::U64(node_id),
            Value::Nullable(Some(Box::new(Value::U64(global_time)))),
        ],
    );
    database.commit_batch(batch).await.unwrap();

    let raw = physical
        .prefix("__groove_class_indices".into(), Vec::new())
        .await
        .unwrap();
    let registry = database.ivm_runtime.index_ids().borrow();
    let mut sizes = std::collections::BTreeMap::new();
    for (key, value) in &raw {
        let Some((id, _)) = crate::ivm::runtime::split_durable_index_key(key) else {
            continue;
        };
        let (table, index) = registry.names(id).unwrap();
        println!(
            "{table}.{index}: key {} B + value {} B = {} B",
            key.len(),
            value.len(),
            key.len() + value.len()
        );
        sizes.insert(index.to_owned(), (key.len(), value.len()));
    }
    // Before (name-prefixed layout, same shapes): fk 138+66, by_seq 99+45,
    // by_global_time 91+35 bytes. (`history.by_tx`, 110+53 before, no longer
    // exists.)
    assert_eq!(
        sizes,
        std::collections::BTreeMap::from([
            ("by_global_time".to_owned(), (30, 0)),
            ("by_physical_app_v1_5".to_owned(), (49, 0)),
            ("by_seq".to_owned(), (40, 0)),
        ])
    );
}
