//! Durable schema indices, reads, ordering, uniqueness, and restart behavior.

use super::*;

#[futures_test::test]
async fn database_creation_dedups_schema_indices_as_durable_nodes() {
    let storage = MemoryStorage::new(&["albums", "indices"]);
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
    let storage = MemoryStorage::new(&["albums", "indices"]);
    let mut database = Database::new(indexed_albums_schema(), storage)
        .await
        .unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(7), Value::String("Blue Train".to_owned())],
    );
    database.commit_batch(batch).await.unwrap();

    let prefix = b"albums\0albums_by_title\0";
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
    let storage = MemoryStorage::new(&["albums", "indices"]);
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
    let storage = MemoryStorage::new(&["albums", "indices"]);
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
    let storage = MemoryStorage::new(&["tracks", "indices"]);
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
    let storage = MemoryStorage::new(&["history", "indices"]);
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
    let storage = MemoryStorage::new(&["history", "indices"]);
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
    let storage = MemoryStorage::new(&["docs", "indices"]);
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
    let storage = MemoryStorage::new(&["tracks", "indices"]);
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
    let storage = MemoryStorage::new(&["edges", "indices"]);
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
    let storage = MemoryStorage::new(&["tracks", "indices"]);
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
    let storage = MemoryStorage::new(&["albums", "indices"]);
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
    let storage = MemoryStorage::new(&["history", "indices"]);
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
async fn randomized_index_reads_match_full_scan_oracle() {
    let storage = MemoryStorage::new(&["tracks", "indices"]);
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
    let storage = MemoryStorage::new(&["albums", "indices"]);
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
        .prefix("indices".to_owned(), b"albums\0albums_by_title\0".to_vec())
        .await
        .unwrap()
        .into_iter()
        .map(|(key, _)| key)
        .collect::<Vec<_>>();

    assert_eq!(
        keys,
        [
            persisted_index_storage_key("albums_by_title", &encoded_title_index_key("aa", 1)),
            persisted_index_storage_key("albums_by_title", &encoded_title_index_key("b", 256)),
        ]
    );
}

#[futures_test::test]
async fn durable_non_unique_index_keys_append_separator_and_primary_key_suffix() {
    let storage = MemoryStorage::new(&["albums", "indices"]);
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
        .prefix("indices".to_owned(), b"albums\0albums_by_title\0".to_vec())
        .await
        .unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(
        entries[0].0,
        persisted_index_storage_key("albums_by_title", &encoded_title_index_key("Blue Train", 7))
    );
    assert!(
        encoded_title_index_key("Blue Train", 7)
            .strip_prefix(encoded_title_key_part("Blue Train").as_slice())
            .is_some_and(|suffix| suffix.starts_with(&[0xff]))
    );
}

#[futures_test::test]
async fn unique_indices_use_only_index_columns_as_storage_keys() {
    let storage = MemoryStorage::new(&["albums", "indices"]);
    let mut database = Database::new(unique_indexed_albums_schema(), storage)
        .await
        .unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(7), Value::String("Blue Train".to_owned())],
    );
    database.commit_batch(batch).await.unwrap();

    let prefix = b"albums\0unique_albums_by_title\0";
    let entries = database
        .storage
        .prefix("indices".to_owned(), prefix.to_vec())
        .await
        .unwrap();
    let expected_key = persisted_index_storage_key(
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
    let storage = MemoryStorage::new(&["albums", "indices"]);
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
            b"albums\0unique_albums_by_title\0".to_vec(),
        )
        .await
        .unwrap();
    assert_eq!(entries.len(), 1);
    assert_eq!(
        entries[0].0,
        persisted_index_storage_key(
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
    let storage = MemoryStorage::new(&["history", "indices"]);
    let mut database = Database::new(schema, storage).await.unwrap();

    let mut batch = database.open_batch();
    batch.insert("history", history_values(2, 10, 1, "older"));
    batch.insert("history", history_values(1, 20, 7, "newer"));
    database.commit_batch(batch).await.unwrap();

    let entries = database
        .storage
        .prefix("indices".to_owned(), b"history\0by_tx\0".to_vec())
        .await
        .unwrap();
    assert_eq!(
        entries
            .iter()
            .map(|entry| entry.0.clone())
            .collect::<Vec<_>>(),
        [
            persisted_table_index_storage_key(
                "history",
                "by_tx",
                &encoded_history_by_tx_key(10, 1, 2)
            ),
            persisted_table_index_storage_key(
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
    let storage = MemoryStorage::new(&["albums", "indices"]);
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

    let prefix = b"albums\0unique_albums_by_title\0";
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

#[futures_test::test]
async fn durable_unique_indices_reject_positive_delta_for_existing_different_record() {
    let storage = MemoryStorage::new(&["albums", "indices"]);
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

#[futures_test::test]
async fn unique_indices_reject_conflicts_within_one_batch() {
    let storage = MemoryStorage::new(&["albums", "indices"]);
    let mut database = Database::new(unique_indexed_albums_schema(), storage)
        .await
        .unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(7), Value::String("Blue Train".to_owned())],
    );
    batch.insert(
        "albums",
        vec![Value::U64(8), Value::String("Blue Train".to_owned())],
    );

    assert!(matches!(
        database.commit_batch(batch).await.unwrap_err(),
        Error::IvmRuntime(IvmRuntimeError::UniqueIndexViolation { .. })
    ));
    assert!(
        database
            .storage
            .prefix(
                "indices".to_owned(),
                b"albums\0unique_albums_by_title\0".to_vec()
            )
            .await
            .unwrap()
            .is_empty()
    );
}

#[futures_test::test]
async fn table_and_index_state_survive_restart_for_resubscribed_graphs() {
    let table_graph = GraphBuilder::table("albums");
    let index_graph = GraphBuilder::index("albums", "albums_by_title");

    let storage = {
        let storage = MemoryStorage::new(&["albums", "indices"]);
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
        let storage = MemoryStorage::new(&["albums", "indices"]);
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

#[futures_test::test]
async fn live_index_registration_rejects_while_a_publication_is_resident() {
    let storage = MemoryStorage::new(&["albums", "indices"]);
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
