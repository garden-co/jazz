//! Atomic batches, staged overlays, direct records, commit metrics, and poisoning.

use super::*;

#[test]
fn commits_insert_update_and_delete_batches() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage =
        TestBtreeStorage::open(temp_dir.path().join("groove-test.btree"), &["albums"]).unwrap();
    let mut database = Database::new(albums_schema(), storage).unwrap();

    let mut batch = database.open_batch();
    assert!(batch.is_empty());
    batch.insert(
        "albums",
        vec![Value::U64(7), Value::String("Blue Train".to_owned())],
    );
    database.commit_batch(batch).unwrap();

    assert_eq!(
        database
            .storage
            .get("albums", &PrimaryKeyValue::U64(7).into_bytes())
            .unwrap(),
        Some(crate::records::encode_variant_record(
            0,
            &database
                .ivm_runtime
                .schema()
                .table("albums")
                .unwrap()
                .record_schema()
                .create(&[Value::U64(7), Value::String("Blue Train".to_owned())])
                .unwrap(),
        ))
    );

    let mut batch = database.open_batch();
    batch.update(
        "albums",
        vec![Value::U64(7), Value::String("Giant Steps".to_owned())],
    );
    database.commit_batch(batch).unwrap();
    let stored = database
        .storage
        .get("albums", &PrimaryKeyValue::U64(7).into_bytes())
        .unwrap()
        .unwrap();
    let descriptor = database
        .ivm_runtime
        .schema()
        .table("albums")
        .unwrap()
        .record_schema();
    let stored = version_zero_payload(&stored);
    assert_eq!(
        descriptor.get(stored, "title").unwrap(),
        Value::String("Giant Steps".to_owned())
    );

    let mut batch = database.open_batch();
    batch.delete("albums", PrimaryKeyValue::U64(7));
    database.commit_batch(batch).unwrap();
    assert_eq!(
        database
            .storage
            .get("albums", &PrimaryKeyValue::U64(7).into_bytes())
            .unwrap(),
        None
    );
}

#[test]
fn staged_batch_reads_observe_uncommitted_writes() {
    let mut database = Database::new(albums_schema(), MemoryStorage::new(&["albums"])).unwrap();

    let mut staged = database.open_staged_batch();
    staged.insert(
        "albums",
        vec![Value::U64(7), Value::String("Blue Train".to_owned())],
    );
    assert_eq!(
        staged
            .primary_key_scan("albums", &[Value::U64(7)])
            .unwrap()
            .into_iter()
            .map(|record| record.get("title").unwrap())
            .collect::<Vec<_>>(),
        vec![Value::String("Blue Train".to_owned())]
    );
    staged.update(
        "albums",
        vec![Value::U64(7), Value::String("Giant Steps".to_owned())],
    );
    assert_eq!(
        staged
            .primary_key_scan("albums", &[Value::U64(7)])
            .unwrap()
            .into_iter()
            .map(|record| record.get("title").unwrap())
            .collect::<Vec<_>>(),
        vec![Value::String("Giant Steps".to_owned())]
    );
    staged.delete("albums", PrimaryKeyValue::U64(7));
    assert!(
        staged
            .primary_key_scan("albums", &[Value::U64(7)])
            .unwrap()
            .is_empty()
    );
    staged.commit().unwrap();

    assert!(
        database
            .primary_key_scan("albums", &[Value::U64(7)])
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        database
            .last_commit_metrics()
            .unwrap()
            .tick
            .table_delta_records,
        0
    );
}

fn vec_derived_primary_key_scan_raw(
    database: &Database<MemoryStorage>,
    batch: &DatabaseBatch,
    table: &str,
    prefix: &[Value],
) -> Vec<(Vec<u8>, Vec<u8>)> {
    let mut key_prefix = Vec::new();
    for value in prefix {
        encode_primary_key_part(&mut key_prefix, value).unwrap();
    }
    let mut rows = database
        .primary_key_scan_raw(table, prefix)
        .unwrap()
        .into_iter()
        .map(EncodedKeyValue::into_parts)
        .collect::<std::collections::BTreeMap<_, _>>();
    for write in database
        .pending_writes_from_operations(&batch.operations)
        .unwrap()
    {
        if write.table() != table || !write.key().starts_with(&key_prefix) {
            continue;
        }
        match write {
            PendingTableWrite::Set { key, record, .. } => {
                rows.insert(key, record);
            }
            PendingTableWrite::Delete { key, .. } => {
                rows.remove(&key);
            }
        }
    }
    rows.into_iter().collect()
}

#[test]
fn staged_batch_storage_txn_handles_large_accumulated_batches() {
    let database = Database::new(albums_schema(), MemoryStorage::new(&["albums"])).unwrap();
    let mut batch = database.open_batch();
    for id in 0..10_000 {
        batch.insert(
            "albums",
            vec![Value::U64(id), Value::String(format!("album-{id}"))],
        );
    }

    let rows = database
        .primary_key_scan_raw_in_batch(&batch, "albums", &[Value::U64(9_999)])
        .unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(
        rows[0].record().get("title").unwrap(),
        Value::String("album-9999".to_owned())
    );
    assert_eq!(batch.txn_operations.borrow().len(), 10_000);
    assert_eq!(
        rows.iter()
            .cloned()
            .map(EncodedKeyValue::into_parts)
            .collect::<Vec<_>>(),
        vec_derived_primary_key_scan_raw(&database, &batch, "albums", &[Value::U64(9_999)])
    );

    let cached_rows = database
        .primary_key_scan_raw_in_batch(&batch, "albums", &[Value::U64(42)])
        .unwrap();
    assert_eq!(
        cached_rows[0].record().get("title").unwrap(),
        Value::String("album-42".to_owned())
    );

    batch.update(
        "albums",
        vec![Value::U64(42), Value::String("updated".to_owned())],
    );
    let updated = database
        .primary_key_scan_raw_in_batch(&batch, "albums", &[Value::U64(42)])
        .unwrap();
    assert_eq!(
        updated[0].record().get("title").unwrap(),
        Value::String("updated".to_owned())
    );
    assert_eq!(
        updated
            .iter()
            .cloned()
            .map(EncodedKeyValue::into_parts)
            .collect::<Vec<_>>(),
        vec_derived_primary_key_scan_raw(&database, &batch, "albums", &[Value::U64(42)])
    );

    batch.delete("albums", PrimaryKeyValue::U64(42));
    assert!(
        database
            .primary_key_scan_raw_in_batch(&batch, "albums", &[Value::U64(42)])
            .unwrap()
            .is_empty()
    );
    assert_eq!(
        database
            .primary_key_scan_raw_in_batch(&batch, "albums", &[])
            .unwrap()
            .len(),
        9_999
    );
    assert_eq!(batch.txn_indexed_operations.get(), batch.operations.len());
}

#[test]
fn primary_key_get_raw_observes_staged_overlay() {
    let mut database = Database::new(albums_schema(), MemoryStorage::new(&["albums"])).unwrap();
    let mut seed = database.open_batch();
    seed.insert(
        "albums",
        vec![Value::U64(1), Value::String("stored-one".to_owned())],
    );
    seed.insert(
        "albums",
        vec![Value::U64(2), Value::String("stored-two".to_owned())],
    );
    database.commit_batch(seed).unwrap();

    let mut batch = database.open_batch();
    batch.update(
        "albums",
        vec![Value::U64(1), Value::String("updated-one".to_owned())],
    );
    batch.delete("albums", PrimaryKeyValue::U64(2));
    batch.insert(
        "albums",
        vec![Value::U64(3), Value::String("inserted-three".to_owned())],
    );

    let updated = database
        .primary_key_get_raw_in_batch(&batch, "albums", &[Value::U64(1)])
        .unwrap()
        .unwrap();
    assert_eq!(
        updated.record().get("title").unwrap(),
        Value::String("updated-one".to_owned())
    );
    assert!(
        database
            .primary_key_get_raw_in_batch(&batch, "albums", &[Value::U64(2)])
            .unwrap()
            .is_none()
    );
    let inserted = database
        .primary_key_get_raw_in_batch(&batch, "albums", &[Value::U64(3)])
        .unwrap()
        .unwrap();
    assert_eq!(
        inserted.record().get("title").unwrap(),
        Value::String("inserted-three".to_owned())
    );
    assert_eq!(batch.txn_indexed_operations.get(), batch.operations.len());
}

#[test]
fn staged_batch_storage_txn_overlays_storage_for_prefix_scans() {
    let mut database = Database::new(albums_schema(), MemoryStorage::new(&["albums"])).unwrap();
    let mut seed = database.open_batch();
    seed.insert(
        "albums",
        vec![Value::U64(1), Value::String("stored-one".to_owned())],
    );
    seed.insert(
        "albums",
        vec![Value::U64(2), Value::String("stored-two".to_owned())],
    );
    database.commit_batch(seed).unwrap();

    let mut batch = database.open_batch();
    batch.update(
        "albums",
        vec![Value::U64(1), Value::String("staged-one".to_owned())],
    );
    batch.delete("albums", PrimaryKeyValue::U64(2));
    batch.insert(
        "albums",
        vec![Value::U64(3), Value::String("staged-three".to_owned())],
    );

    let rows = database
        .primary_key_scan_raw_in_batch(&batch, "albums", &[])
        .unwrap()
        .into_iter()
        .map(|row| row.record().get("title").unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        rows,
        vec![
            Value::String("staged-one".to_owned()),
            Value::String("staged-three".to_owned())
        ]
    );
    assert_eq!(
        database
            .primary_key_scan_raw_in_batch(&batch, "albums", &[])
            .unwrap()
            .into_iter()
            .map(EncodedKeyValue::into_parts)
            .collect::<Vec<_>>(),
        vec_derived_primary_key_scan_raw(&database, &batch, "albums", &[])
    );
}

#[test]
fn staged_batch_storage_txn_advances_only_new_operations() {
    let database = Database::new(albums_schema(), MemoryStorage::new(&["albums"])).unwrap();
    let mut batch = database.open_batch();
    for id in 0..10_000 {
        batch.insert(
            "albums",
            vec![Value::U64(id), Value::String(format!("album-{id}"))],
        );
    }
    database
        .primary_key_scan_raw_in_batch(&batch, "albums", &[Value::U64(9_999)])
        .unwrap();
    assert_eq!(batch.txn_indexed_operations.get(), 10_000);

    for id in 10_000..20_000 {
        batch.insert(
            "albums",
            vec![Value::U64(id), Value::String(format!("album-{id}"))],
        );
    }
    database
        .primary_key_scan_raw_in_batch(&batch, "albums", &[Value::U64(19_999)])
        .unwrap();
    assert_eq!(batch.txn_indexed_operations.get(), 20_000);
    assert_eq!(batch.txn_operations.borrow().len(), 20_000);

    batch.update(
        "albums",
        vec![Value::U64(19_999), Value::String("tail-updated".to_owned())],
    );
    database
        .primary_key_scan_raw_in_batch(&batch, "albums", &[Value::U64(19_999)])
        .unwrap();
    assert_eq!(batch.txn_indexed_operations.get(), 20_001);
    assert_eq!(batch.txn_operations.borrow().len(), 20_001);
}

#[test]
fn staged_batch_commit_ticks_once_for_multiple_writes() {
    let mut database = Database::new(albums_schema(), MemoryStorage::new(&["albums"])).unwrap();
    let subscription = database
        .subscribe_one_sink(GraphBuilder::table("albums"))
        .unwrap();
    assert!(subscription.recv().unwrap().is_empty());

    let mut staged = database.open_staged_batch();
    staged.insert(
        "albums",
        vec![Value::U64(1), Value::String("A Love Supreme".to_owned())],
    );
    staged.insert(
        "albums",
        vec![Value::U64(2), Value::String("Blue Train".to_owned())],
    );
    staged.commit().unwrap();

    let metrics = database.last_commit_metrics().unwrap();
    assert_eq!(metrics.tick.table_delta_records, 2);
    assert_eq!(metrics.tick.notifications_sent, 1);
    assert_eq!(metrics.tick.notification_records, 2);
    let mut observed = subscription.recv().unwrap().to_values().unwrap();
    observed.sort_by_key(|(values, _)| match values[0] {
        Value::U64(id) => id,
        _ => panic!("expected u64 id"),
    });
    assert_eq!(
        observed,
        vec![
            (
                vec![Value::U64(1), Value::String("A Love Supreme".to_owned())],
                1
            ),
            (
                vec![Value::U64(2), Value::String("Blue Train".to_owned())],
                1
            ),
        ]
    );
    assert!(matches!(subscription.try_recv(), Err(TryRecvError::Empty)));
}

#[test]
fn staged_batch_commit_matches_one_shot_wrapper() {
    let mut staged_db = Database::new(albums_schema(), MemoryStorage::new(&["albums"])).unwrap();
    let mut wrapper_db = Database::new(albums_schema(), MemoryStorage::new(&["albums"])).unwrap();

    let mut staged = staged_db.open_staged_batch();
    staged.insert(
        "albums",
        vec![Value::U64(1), Value::String("A Love Supreme".to_owned())],
    );
    staged.insert(
        "albums",
        vec![Value::U64(2), Value::String("Blue Train".to_owned())],
    );
    staged.delete("albums", PrimaryKeyValue::U64(1));
    staged.commit().unwrap();

    let mut wrapper = wrapper_db.open_batch();
    wrapper.insert(
        "albums",
        vec![Value::U64(1), Value::String("A Love Supreme".to_owned())],
    );
    wrapper.insert(
        "albums",
        vec![Value::U64(2), Value::String("Blue Train".to_owned())],
    );
    wrapper.delete("albums", PrimaryKeyValue::U64(1));
    wrapper_db.commit_batch(wrapper).unwrap();

    assert_eq!(
        staged_db
            .primary_key_scan("albums", &[])
            .unwrap()
            .into_iter()
            .map(|record| record.to_values())
            .collect::<Result<Vec<_>, _>>()
            .unwrap(),
        wrapper_db
            .primary_key_scan("albums", &[])
            .unwrap()
            .into_iter()
            .map(|record| record.to_values())
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
    );
    assert_eq!(
        staged_db
            .last_commit_metrics()
            .unwrap()
            .tick
            .table_delta_records,
        wrapper_db
            .last_commit_metrics()
            .unwrap()
            .tick
            .table_delta_records
    );
    assert_eq!(
        staged_db.last_commit_metrics().unwrap().storage_writes,
        wrapper_db.last_commit_metrics().unwrap().storage_writes
    );
}

#[test]
fn direct_record_store_stores_ordered_records_independent_of_tables() {
    let temp_dir = tempfile::tempdir().unwrap();
    let schema = albums_schema().with_direct_record_store(DirectRecordStoreSchema::new(
        "streams",
        RecordDescriptor::new([
            ("namespace", ColumnType::String.clone()),
            ("path", ColumnType::String.clone()),
        ]),
        RecordDescriptor::new([("bytes", ColumnType::Bytes.clone())]),
    ));
    let column_families = schema.column_families();
    let storage =
        TestBtreeStorage::open(temp_dir.path().join("groove-test.btree"), &column_families)
            .unwrap();
    let mut database = Database::new(schema.clone(), storage).unwrap();
    let subscription = database
        .subscribe_one_sink(GraphBuilder::table("albums"))
        .unwrap();
    assert!(subscription.recv().unwrap().is_empty());

    {
        let store = database.direct_record_store("streams").unwrap();
        store
            .set(
                &[
                    Value::String("content".to_owned()),
                    Value::String("content/02".to_owned()),
                ],
                &[Value::Bytes(b"two".to_vec())],
            )
            .unwrap();
        store
            .set(
                &[
                    Value::String("content".to_owned()),
                    Value::String("content/01".to_owned()),
                ],
                &[Value::Bytes(b"one".to_vec())],
            )
            .unwrap();
        store
            .set(
                &[
                    Value::String("content".to_owned()),
                    Value::String("content/03".to_owned()),
                ],
                &[Value::Bytes(b"three".to_vec())],
            )
            .unwrap();
        store
            .set(
                &[
                    Value::String("checkpoint".to_owned()),
                    Value::String("checkpoint".to_owned()),
                ],
                &[Value::Bytes(b"cp".to_vec())],
            )
            .unwrap();

        assert_eq!(
            store
                .get(&[
                    Value::String("content".to_owned()),
                    Value::String("content/02".to_owned()),
                ])
                .unwrap()
                .unwrap()
                .get("bytes")
                .unwrap(),
            Value::Bytes(b"two".to_vec())
        );
        assert_eq!(
            store
                .range(
                    &[
                        Value::String("content".to_owned()),
                        Value::String("content/01".to_owned()),
                    ],
                    &[
                        Value::String("content".to_owned()),
                        Value::String("content/04".to_owned()),
                    ]
                )
                .unwrap()
                .into_iter()
                .map(|record| record.get("bytes").unwrap())
                .collect::<Vec<_>>(),
            vec![
                Value::Bytes(b"one".to_vec()),
                Value::Bytes(b"two".to_vec()),
                Value::Bytes(b"three".to_vec()),
            ],
        );
        assert_eq!(
            store
                .prefix(&[Value::String("content".to_owned())])
                .unwrap()
                .into_iter()
                .map(|record| record.get("bytes").unwrap())
                .collect::<Vec<_>>(),
            vec![
                Value::Bytes(b"one".to_vec()),
                Value::Bytes(b"two".to_vec()),
                Value::Bytes(b"three".to_vec()),
            ],
        );

        let raw_value = database
            .storage
            .get(
                "streams",
                &PrimaryKeyValue::Composite(vec![
                    PrimaryKeyValue::String("content".to_owned()),
                    PrimaryKeyValue::String("content/01".to_owned()),
                ])
                .into_bytes(),
            )
            .unwrap()
            .unwrap();
        assert_eq!(raw_value, b"one");

        store
            .delete(&[
                Value::String("content".to_owned()),
                Value::String("content/02".to_owned()),
            ])
            .unwrap();
        assert!(
            store
                .get(&[
                    Value::String("content".to_owned()),
                    Value::String("content/02".to_owned()),
                ])
                .unwrap()
                .is_none()
        );
    }
    assert!(matches!(subscription.try_recv(), Err(TryRecvError::Empty)));
    assert!(database.primary_key_scan("albums", &[]).unwrap().is_empty());

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(7), Value::String("Blue Train".to_owned())],
    );
    database.commit_batch(batch).unwrap();
    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        vec![(
            vec![Value::U64(7), Value::String("Blue Train".to_owned())],
            1
        )]
    );
    assert_eq!(
        database
            .direct_record_store("streams")
            .unwrap()
            .get(&[
                Value::String("checkpoint".to_owned()),
                Value::String("checkpoint".to_owned()),
            ])
            .unwrap()
            .unwrap()
            .get("bytes")
            .unwrap(),
        Value::Bytes(b"cp".to_vec())
    );
    assert_eq!(database.storage.get("albums", b"content/01").unwrap(), None);

    drop(database);
    let column_families = schema.column_families();
    let storage =
        TestBtreeStorage::open(temp_dir.path().join("groove-test.btree"), &column_families)
            .unwrap();
    let reopened = Database::new(schema, storage).unwrap();
    let store = reopened.direct_record_store("streams").unwrap();
    assert_eq!(
        store
            .prefix(&[Value::String("content".to_owned())])
            .unwrap()
            .into_iter()
            .map(|record| record.get("bytes").unwrap())
            .collect::<Vec<_>>(),
        vec![
            Value::Bytes(b"one".to_vec()),
            Value::Bytes(b"three".to_vec()),
        ],
    );
    assert_eq!(
        reopened
            .primary_key_scan("albums", &[Value::U64(7)])
            .unwrap()
            .into_iter()
            .map(|record| record.get("title").unwrap())
            .collect::<Vec<_>>(),
        vec![Value::String("Blue Train".to_owned())]
    );
}

fn assert_direct_record_store_round_trips_array_of_record_values() {
    let child = RecordDescriptor::new([("id", ValueType::U64), ("title", ValueType::String)]);
    let schema = DatabaseSchema::new([]).with_direct_record_store(DirectRecordStoreSchema::new(
        "rendered_results",
        RecordDescriptor::new([("id", ValueType::U64)]),
        RecordDescriptor::new([(
            "results",
            ValueType::Array(Box::new(ValueType::Record(Box::new(child)))),
        )]),
    ));
    let storage = MemoryStorage::new(&schema.column_families());
    let database = Database::new(schema, storage).unwrap();
    let first = crate::records::OwnedRecord::new(
        child
            .create(&[Value::U64(1), Value::String("Kind of Blue".to_owned())])
            .unwrap(),
        child,
    );
    let second = crate::records::OwnedRecord::new(
        child
            .create(&[Value::U64(2), Value::String("A Love Supreme".to_owned())])
            .unwrap(),
        child,
    );
    let results = Value::Array(vec![Value::Record(first), Value::Record(second)]);
    let store = database.direct_record_store("rendered_results").unwrap();

    store
        .set(&[Value::U64(7)], std::slice::from_ref(&results))
        .unwrap();

    assert_eq!(
        store
            .get(&[Value::U64(7)])
            .unwrap()
            .unwrap()
            .get("results")
            .unwrap(),
        results
    );
}

fn assert_direct_record_store_rejects_noncanonical_record_value_bytes_at_admission() {
    let child = RecordDescriptor::new([("maybe_id", ValueType::Nullable(Box::new(ValueType::U8)))]);
    let schema = DatabaseSchema::new([]).with_direct_record_store(DirectRecordStoreSchema::new(
        "rendered_results",
        RecordDescriptor::new([("id", ValueType::U64)]),
        RecordDescriptor::new([(
            "results",
            ValueType::Array(Box::new(ValueType::Record(Box::new(child)))),
        )]),
    ));
    let storage = MemoryStorage::new(&schema.column_families());
    let database = Database::new(schema, storage).unwrap();
    let store = database.direct_record_store("rendered_results").unwrap();
    // A fixed-width null reserves a zero payload byte; this child has a
    // noncanonical nonzero payload and must not reach durable storage.
    let noncanonical = crate::records::OwnedRecord::new(vec![0, 7], child);

    assert!(matches!(
        store.set(
            &[Value::U64(7)],
            &[Value::Array(vec![Value::Record(noncanonical)])],
        ),
        Err(Error::RecordEncoding(crate::records::Error::InvalidOffset))
    ));
    assert!(store.get(&[Value::U64(7)]).unwrap().is_none());
}

#[test]
fn direct_record_store_rejects_record_containing_durable_keys_at_schema_admission() {
    assert_direct_record_store_round_trips_array_of_record_values();
    assert_direct_record_store_rejects_noncanonical_record_value_bytes_at_admission();

    let child = RecordDescriptor::new([("id", ValueType::U64)]);
    for (name, key_type) in [
        ("direct_record", ValueType::Record(Box::new(child))),
        (
            "array_record",
            ValueType::Array(Box::new(ValueType::Record(Box::new(child)))),
        ),
        (
            "nullable_array_record",
            ValueType::Nullable(Box::new(ValueType::Array(Box::new(ValueType::Record(
                Box::new(child),
            ))))),
        ),
    ] {
        let schema =
            DatabaseSchema::new([]).with_direct_record_store(DirectRecordStoreSchema::new(
                name,
                RecordDescriptor::new([("key", key_type)]),
                RecordDescriptor::new([("payload", ValueType::Bytes)]),
            ));
        let storage = MemoryStorage::new(&schema.column_families());

        assert!(matches!(
            Database::new(schema, storage),
            Err(Error::InvalidDirectRecordStoreKey(store)) if store == name
        ));
    }

    let scalar_schema =
        DatabaseSchema::new([]).with_direct_record_store(DirectRecordStoreSchema::new(
            "scalar_key",
            RecordDescriptor::new([("id", ValueType::U64)]),
            RecordDescriptor::new([("payload", ValueType::Bytes)]),
        ));
    let scalar_storage = MemoryStorage::new(&scalar_schema.column_families());
    let scalar_database = Database::new(scalar_schema, scalar_storage).unwrap();
    let scalar_store = scalar_database.direct_record_store("scalar_key").unwrap();

    scalar_store
        .set(&[Value::U64(7)], &[Value::Bytes(b"allowed".to_vec())])
        .unwrap();
    assert_eq!(
        scalar_store
            .get(&[Value::U64(7)])
            .unwrap()
            .unwrap()
            .get("payload")
            .unwrap(),
        Value::Bytes(b"allowed".to_vec())
    );
}

#[test]
fn commit_metrics_split_storage_and_tick_work() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage =
        TestBtreeStorage::open(temp_dir.path().join("groove-test.btree"), &["albums"]).unwrap();
    let mut database = Database::new(albums_schema(), storage).unwrap();
    database.set_tick_runtime_stats_enabled(true);
    let subscription = database
        .subscribe_one_sink(GraphBuilder::table("albums"))
        .unwrap();
    let _initial = subscription.recv().unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(7), Value::String("Blue Train".to_owned())],
    );
    database.commit_batch(batch).unwrap();

    let metrics = database.last_commit_metrics().unwrap();
    assert_eq!(metrics.storage_write_count, 1);
    assert!(metrics.storage_write_bytes > 0);
    assert_eq!(metrics.tick.table_delta_records, 1);
    assert_eq!(metrics.tick.notifications_sent, 1);
    assert_eq!(metrics.tick.notification_records, 1);
    assert!(metrics.tick.runtime_stats.graph_nodes > 0);
}

#[test]
fn commit_metrics_split_storage_writes_by_jazz_destination() {
    fn run(layout: StorageLayout) -> StorageWriteMetrics {
        let schema = DatabaseSchema::new([
            TableSchema::new(
                "jazz_docs_history",
                [
                    ColumnSchema::new("row_uuid", ColumnType::Uuid),
                    ColumnSchema::new("tx_time", ColumnType::U64),
                    ColumnSchema::new("tx_node_id", ColumnType::U64),
                    ColumnSchema::new("parent", ColumnType::Uuid),
                ],
            )
            .with_primary_key(PrimaryKey::composite([
                PrimaryKeyColumn::uuid("row_uuid"),
                PrimaryKeyColumn::integer("tx_time", IntegerKeyType::U64),
                PrimaryKeyColumn::integer("tx_node_id", IntegerKeyType::U64),
            ]))
            .with_index(IndexSchema::new(
                "by_tx",
                ["tx_time", "tx_node_id", "row_uuid"],
            )),
            TableSchema::new(
                "jazz_docs_global_current",
                [
                    ColumnSchema::new("row_uuid", ColumnType::Uuid),
                    ColumnSchema::new("tx_time", ColumnType::U64),
                    ColumnSchema::new("tx_node_id", ColumnType::U64),
                    ColumnSchema::new("user_parent", ColumnType::Uuid),
                ],
            )
            .with_primary_key(PrimaryKey::composite([PrimaryKeyColumn::uuid("row_uuid")]))
            .with_index(IndexSchema::new("by_user_parent", ["user_parent"])),
            TableSchema::new(
                "jazz_docs_register_global_current",
                [
                    ColumnSchema::new("row_uuid", ColumnType::Uuid),
                    ColumnSchema::new("tx_time", ColumnType::U64),
                ],
            )
            .with_primary_key(PrimaryKey::composite([PrimaryKeyColumn::uuid("row_uuid")])),
            TableSchema::new(
                "jazz_global_changes",
                [
                    ColumnSchema::new("table_name", ColumnType::Bytes),
                    ColumnSchema::new("row_uuid", ColumnType::Uuid),
                    ColumnSchema::new("layer", ColumnType::Bytes),
                    ColumnSchema::new("global_seq", ColumnType::U64),
                ],
            )
            .with_primary_key(PrimaryKey::composite([
                PrimaryKeyColumn::bytes("table_name"),
                PrimaryKeyColumn::uuid("row_uuid"),
                PrimaryKeyColumn::bytes("layer"),
                PrimaryKeyColumn::integer("global_seq", IntegerKeyType::U64),
            ]))
            .with_index(IndexSchema::new(
                "by_global_seq",
                ["global_seq", "table_name", "row_uuid", "layer"],
            )),
            TableSchema::new(
                "jazz_transactions",
                [
                    ColumnSchema::new("time", ColumnType::U64),
                    ColumnSchema::new("node_id", ColumnType::U64),
                    ColumnSchema::new("global_seq", ColumnType::U64),
                ],
            )
            .with_primary_key(PrimaryKey::composite([
                PrimaryKeyColumn::integer("time", IntegerKeyType::U64),
                PrimaryKeyColumn::integer("node_id", IntegerKeyType::U64),
            ]))
            .with_index(IndexSchema::new("by_global_seq", ["global_seq"])),
        ]);
        let column_families = layout.physical_column_families(schema.column_families());
        let refs = column_families
            .iter()
            .map(String::as_str)
            .collect::<Vec<_>>();
        let storage = MemoryStorage::new(&refs);
        let mut database = Database::new_with_storage_layout(schema, storage, layout).unwrap();
        let row_uuid = uuid(1);

        let mut batch = database.open_batch();
        batch.insert(
            "jazz_docs_history",
            vec![
                Value::Uuid(row_uuid),
                Value::U64(1),
                Value::U64(2),
                Value::Uuid(uuid(3)),
            ],
        );
        batch.insert(
            "jazz_docs_global_current",
            vec![
                Value::Uuid(row_uuid),
                Value::U64(1),
                Value::U64(2),
                Value::Uuid(uuid(3)),
            ],
        );
        batch.insert(
            "jazz_docs_register_global_current",
            vec![Value::Uuid(row_uuid), Value::U64(1)],
        );
        batch.insert(
            "jazz_global_changes",
            vec![
                Value::Bytes(b"docs".to_vec()),
                Value::Uuid(row_uuid),
                Value::Bytes(b"content".to_vec()),
                Value::U64(1),
            ],
        );
        batch.insert(
            "jazz_transactions",
            vec![Value::U64(1), Value::U64(2), Value::U64(1)],
        );
        database.commit_batch(batch).unwrap();

        database.last_commit_metrics().unwrap().storage_writes
    }

    let writes = run(StorageLayout::Identity);
    assert_eq!(writes.total.count, 9);
    assert_eq!(writes.history_rows.count, 1);
    assert_eq!(writes.history_indexes.count, 1);
    assert_eq!(writes.global_current_rows.count, 1);
    assert_eq!(writes.global_current_indexes.count, 1);
    assert_eq!(writes.register_global_current_rows.count, 1);
    assert_eq!(writes.global_changes_rows.count, 1);
    assert_eq!(writes.global_changes_indexes.count, 1);
    assert_eq!(writes.transactions_rows.count, 1);
    assert_eq!(writes.transactions_indexes.count, 1);
    assert_eq!(writes.other.count, 0);

    let class_writes = run(StorageLayout::jazz_class_v1());
    assert_eq!(class_writes, writes);
}

// Same-batch consolidation and conflict behavior.

#[test]
fn same_key_writes_in_one_batch_emit_deltas_against_earlier_batch_writes() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage =
        TestBtreeStorage::open(temp_dir.path().join("groove-test.btree"), &["albums"]).unwrap();
    let mut database = Database::new(albums_schema(), storage).unwrap();
    let subscription_id = database
        .subscribe_one_sink(GraphBuilder::table("albums"))
        .unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(7), Value::String("Blue Train".to_owned())],
    );
    batch.update(
        "albums",
        vec![Value::U64(7), Value::String("Giant Steps".to_owned())],
    );
    database.commit_batch(batch).unwrap();

    assert_eq!(
        expect_recv_vals(&subscription_id),
        [(vec![7_u64.into(), "Giant Steps".into()], 1)]
    );
    let stored = database
        .storage
        .get("albums", &PrimaryKeyValue::U64(7).into_bytes())
        .unwrap()
        .unwrap();
    assert_eq!(
        database
            .ivm_runtime
            .schema()
            .table("albums")
            .unwrap()
            .record_schema()
            .get(version_zero_payload(&stored), "title")
            .unwrap(),
        Value::String("Giant Steps".to_owned())
    );
}

#[test]
fn inserts_over_existing_primary_keys_are_rejected() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = TestBtreeStorage::open(
        temp_dir.path().join("groove-test.btree"),
        &["albums", "indices"],
    )
    .unwrap();
    let mut database = Database::new(indexed_albums_schema(), storage).unwrap();
    database
        .subscribe_one_sink(GraphBuilder::table("albums"))
        .unwrap();
    database
        .subscribe_one_sink(GraphBuilder::index("albums", "albums_by_title"))
        .unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(7), Value::String("Blue Train".to_owned())],
    );
    database.commit_batch(batch).unwrap();
    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(7), Value::String("Giant Steps".to_owned())],
    );
    let err = database.commit_batch(batch).unwrap_err();

    assert!(matches!(err, Error::DuplicatePrimaryKey { table, .. } if table == "albums"));
    let stored = database
        .storage
        .get("albums", &PrimaryKeyValue::U64(7).into_bytes())
        .unwrap()
        .unwrap();
    assert_eq!(
        database
            .ivm_runtime
            .schema()
            .table("albums")
            .unwrap()
            .record_schema()
            .get(version_zero_payload(&stored), "title")
            .unwrap(),
        Value::String("Blue Train".to_owned())
    );
}

#[test]
fn inserts_over_primary_keys_created_earlier_in_the_same_batch_are_rejected() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage = TestBtreeStorage::open(
        temp_dir.path().join("groove-test.btree"),
        &["albums", "indices"],
    )
    .unwrap();
    let mut database = Database::new(indexed_albums_schema(), storage).unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(7), Value::String("Blue Train".to_owned())],
    );
    batch.insert(
        "albums",
        vec![Value::U64(7), Value::String("Giant Steps".to_owned())],
    );
    let err = database.commit_batch(batch).unwrap_err();

    assert!(matches!(err, Error::DuplicatePrimaryKey { table, .. } if table == "albums"));
    assert!(
        database
            .storage
            .get("albums", &PrimaryKeyValue::U64(7).into_bytes())
            .unwrap()
            .is_none()
    );
}

#[test]
fn same_batch_same_key_operations_emit_only_the_consolidated_final_delta() {
    let temp_dir = tempfile::tempdir().unwrap();
    let storage =
        TestBtreeStorage::open(temp_dir.path().join("groove-test.btree"), &["albums"]).unwrap();
    let mut database = Database::new(albums_schema(), storage).unwrap();
    let subscription = database
        .subscribe_one_sink(GraphBuilder::table("albums"))
        .unwrap();
    let _initial = subscription.recv().unwrap();

    let mut batch = database.open_batch();
    batch.insert(
        "albums",
        vec![Value::U64(7), Value::String("Blue Train".to_owned())],
    );
    batch.update(
        "albums",
        vec![Value::U64(7), Value::String("Giant Steps".to_owned())],
    );
    database.commit_batch(batch).unwrap();

    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        [(
            vec![Value::U64(7), Value::String("Giant Steps".to_owned())],
            1
        )]
    );
}
