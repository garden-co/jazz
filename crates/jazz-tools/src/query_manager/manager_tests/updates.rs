use super::*;

#[test]
fn update_row() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    let handle = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();

    qm.update(
        &mut storage,
        handle.row_id,
        &[Value::Text("Alice Updated".into()), Value::Integer(150)],
    )
    .unwrap();

    let updated_query = qm
        .query("users")
        .filter_eq("name", Value::Text("Alice Updated".into()))
        .build();
    let updated_rows = execute_query(&mut qm, &mut storage, updated_query).unwrap();
    assert_eq!(updated_rows.len(), 1);
    assert_eq!(updated_rows[0].0, handle.row_id);
    assert_eq!(updated_rows[0].1[1], Value::Integer(150));

    let old_query = qm
        .query("users")
        .filter_eq("name", Value::Text("Alice".into()))
        .build();
    let old_rows = execute_query(&mut qm, &mut storage, old_query).unwrap();
    assert_eq!(
        old_rows.len(),
        0,
        "Old indexed value should no longer match"
    );
}

#[test]
fn local_update_updates_all_column_indices() {
    // Verifies that local update() correctly:
    // 1. Removes old values from column indices
    // 2. Adds new values to column indices
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert row with name="Alice", score=100
    let handle = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();

    // Query by name="Alice" → finds row
    let query = qm
        .query("users")
        .filter_eq("name", Value::Text("Alice".into()))
        .build();
    let results = execute_query(&mut qm, &mut storage, query).unwrap();
    assert_eq!(results.len(), 1);

    // Query by score=100 → finds row
    let query = qm
        .query("users")
        .filter_eq("score", Value::Integer(100))
        .build();
    let results = execute_query(&mut qm, &mut storage, query).unwrap();
    assert_eq!(results.len(), 1);

    // Update to name="Bob", score=200
    qm.update(
        &mut storage,
        handle.row_id,
        &[Value::Text("Bob".into()), Value::Integer(200)],
    )
    .unwrap();

    // Query by name="Alice" → empty (old value removed from index)
    let query = qm
        .query("users")
        .filter_eq("name", Value::Text("Alice".into()))
        .build();
    let results = execute_query(&mut qm, &mut storage, query).unwrap();
    assert_eq!(
        results.len(),
        0,
        "Old name value should be removed from index"
    );

    // Query by name="Bob" → finds row (new value in index)
    let query = qm
        .query("users")
        .filter_eq("name", Value::Text("Bob".into()))
        .build();
    let results = execute_query(&mut qm, &mut storage, query).unwrap();
    assert_eq!(results.len(), 1, "New name value should be in index");

    // Query by score=100 → empty (old value removed from index)
    let query = qm
        .query("users")
        .filter_eq("score", Value::Integer(100))
        .build();
    let results = execute_query(&mut qm, &mut storage, query).unwrap();
    assert_eq!(
        results.len(),
        0,
        "Old score value should be removed from index"
    );

    // Query by score=200 → finds row (new value in index)
    let query = qm
        .query("users")
        .filter_eq("score", Value::Integer(200))
        .build();
    let results = execute_query(&mut qm, &mut storage, query).unwrap();
    assert_eq!(results.len(), 1, "New score value should be in index");
}

#[test]
fn synced_update_updates_column_indices() {
    use crate::query_manager::encoding::encode_row;
    use std::collections::HashMap;

    // This test verifies that direct synced commits (receive_commit)
    // update indices through the row-native update lane.

    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);
    let branch = get_branch(&qm);

    // Simulate receiving a new object from sync
    let row_id = crate::object::ObjectId::new();
    let author = row_id;

    // Receive object with table metadata
    let mut metadata = HashMap::new();
    metadata.insert(MetadataKey::Table.to_string(), "users".to_string());
    put_test_row_metadata(&mut storage, row_id, metadata);

    // Encode the initial row data (name="Alice", score=100)
    let descriptor = RowDescriptor::new(vec![
        ColumnDescriptor::new("name", ColumnType::Text),
        ColumnDescriptor::new("score", ColumnType::Integer),
    ]);
    let initial_data = encode_row(
        &descriptor,
        &[Value::Text("Alice".into()), Value::Integer(100)],
    )
    .unwrap();

    // Receive the first commit (insert)
    let commit1 = stored_row_commit(smallvec![], initial_data.clone(), 1000, author.to_string());
    let commit1_id = receive_row_commit(&mut qm, &mut storage, row_id, &branch, commit1);

    // Process to handle the row-native update
    qm.process(&mut storage);

    // Query by name="Alice" → finds row
    let query = qm
        .query("users")
        .filter_eq("name", Value::Text("Alice".into()))
        .build();
    let results = execute_query(&mut qm, &mut storage, query).unwrap();
    assert_eq!(
        results.len(),
        1,
        "Should find row by name=Alice after sync insert"
    );

    // Query by score=100 → finds row
    let query = qm
        .query("users")
        .filter_eq("score", Value::Integer(100))
        .build();
    let results = execute_query(&mut qm, &mut storage, query).unwrap();
    assert_eq!(
        results.len(),
        1,
        "Should find row by score=100 after sync insert"
    );

    // Encode updated row data (name="Bob", score=200)
    let updated_data = encode_row(
        &descriptor,
        &[Value::Text("Bob".into()), Value::Integer(200)],
    )
    .unwrap();

    // Receive the second commit (update)
    let commit2 = stored_row_commit(
        smallvec![commit1_id],
        updated_data.clone(),
        2000,
        author.to_string(),
    );
    receive_row_commit(&mut qm, &mut storage, row_id, &branch, commit2);

    // Process to handle the row-native update
    qm.process(&mut storage);

    // Query by name="Alice" → empty (old value removed from index)
    let query = qm
        .query("users")
        .filter_eq("name", Value::Text("Alice".into()))
        .build();
    let results = execute_query(&mut qm, &mut storage, query).unwrap();
    assert_eq!(
        results.len(),
        0,
        "Old name value should be removed from index after sync update"
    );

    // Query by name="Bob" → finds row (new value in index)
    let query = qm
        .query("users")
        .filter_eq("name", Value::Text("Bob".into()))
        .build();
    let results = execute_query(&mut qm, &mut storage, query).unwrap();
    assert_eq!(
        results.len(),
        1,
        "New name value should be in index after sync update"
    );

    // Query by score=100 → empty (old value removed from index)
    let query = qm
        .query("users")
        .filter_eq("score", Value::Integer(100))
        .build();
    let results = execute_query(&mut qm, &mut storage, query).unwrap();
    assert_eq!(
        results.len(),
        0,
        "Old score value should be removed from index after sync update"
    );

    // Query by score=200 → finds row (new value in index)
    let query = qm
        .query("users")
        .filter_eq("score", Value::Integer(200))
        .build();
    let results = execute_query(&mut qm, &mut storage, query).unwrap();
    assert_eq!(
        results.len(),
        1,
        "New score value should be in index after sync update"
    );
}

#[test]
fn synced_update_is_visible_in_query() {
    use crate::query_manager::encoding::encode_row;
    use crate::sync_manager::{InboxEntry, ServerId, Source, SyncPayload};

    // Verify that synced updates (same row, new content) update indices correctly
    // and are visible in subsequent queries.
    // (Subscription delta behavior is covered by synced_update_emits_subscription_delta.)

    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);
    let branch = get_branch(&qm);

    // Insert a row locally first
    let insert_handle = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();
    let row_id = insert_handle.row_id;
    let first_commit_id = insert_handle.batch_id;

    // Process to settle the initial insert
    qm.process(&mut storage);
    let base_timestamp = load_visible_row(&storage, row_id, &branch).updated_at;

    // Verify initial data is queryable
    let query = qm
        .query("users")
        .filter_eq("name", Value::Text("Alice".into()))
        .build();
    let results = execute_query(&mut qm, &mut storage, query).unwrap();
    assert_eq!(results.len(), 1, "Should find initial row");
    assert_eq!(results[0].1[0], Value::Text("Alice".into()));
    assert_eq!(results[0].1[1], Value::Integer(100));

    // Now simulate a synced update to this row (e.g., from another peer)
    let descriptor = RowDescriptor::new(vec![
        ColumnDescriptor::new("name", ColumnType::Text),
        ColumnDescriptor::new("score", ColumnType::Integer),
    ]);
    let updated_data = encode_row(
        &descriptor,
        &[Value::Text("Alice Updated".into()), Value::Integer(200)],
    )
    .unwrap();

    let author = row_id; // Self-authored for simplicity
    let update_commit = stored_row_commit(
        smallvec![first_commit_id],
        updated_data,
        base_timestamp + 1,
        author.to_string(),
    );
    let row = update_commit.to_row(row_id, &branch, RowState::VisibleDirect);
    qm.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Server(ServerId::new()),
        payload: SyncPayload::RowBatchCreated {
            metadata: None,
            row,
        },
    });

    // Process to handle the synced update
    qm.process(&mut storage);

    // Old data should no longer be in index
    let query = qm
        .query("users")
        .filter_eq("name", Value::Text("Alice".into()))
        .build();
    let results = execute_query(&mut qm, &mut storage, query).unwrap();
    assert_eq!(results.len(), 0, "Old name should not be found");

    // New data should be queryable
    let query = qm
        .query("users")
        .filter_eq("name", Value::Text("Alice Updated".into()))
        .build();
    let results = execute_query(&mut qm, &mut storage, query).unwrap();
    assert_eq!(results.len(), 1, "Should find updated row by new name");
    assert_eq!(results[0].1[0], Value::Text("Alice Updated".into()));
    assert_eq!(results[0].1[1], Value::Integer(200));

    // Score index should also be updated
    let query = qm
        .query("users")
        .filter_eq("score", Value::Integer(200))
        .build();
    let results = execute_query(&mut qm, &mut storage, query).unwrap();
    assert_eq!(results.len(), 1, "Should find updated row by new score");
}

#[test]
fn update_reads_visible_region_after_legacy_commit_history_is_removed() {
    let schema = test_schema();
    let (mut writer_qm, mut storage) = create_query_manager(SyncManager::new(), schema.clone());
    let _branch = get_branch(&writer_qm);

    let handle = writer_qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();

    let mut reader_qm = QueryManager::new(SyncManager::new());
    reader_qm.set_current_schema(schema, "dev", "main");

    reader_qm
        .update(
            &mut storage,
            handle.row_id,
            &[Value::Text("Alice Updated".into()), Value::Integer(200)],
        )
        .expect(
            "update should succeed from visible-row state without legacy object-backed storage",
        );

    let query = reader_qm.query("users").build();
    let rows = execute_query(&mut reader_qm, &mut storage, query).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, handle.row_id);
    assert_eq!(
        rows[0].1,
        vec![Value::Text("Alice Updated".into()), Value::Integer(200)]
    );
}

#[test]
fn update_fails_filter_emits_removal() {
    // Verify: row passes filter, then update fails filter -> removal delta

    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert row with score=100
    let handle = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();

    // Subscribe to score > 50
    let query = qm
        .query("users")
        .filter_gt("score", Value::Integer(50))
        .build();
    let sub_id = qm.subscribe(query).unwrap();

    qm.process(&mut storage);
    let updates = qm.take_updates();
    assert_eq!(updates.len(), 1);
    assert_eq!(updates[0].subscription_id, sub_id);
    assert_eq!(
        updates[0].delta.added.len(),
        1,
        "Row should be added initially"
    );
    assert_eq!(updates[0].delta.added[0].id, handle.row_id);

    // Update score to 30 (fails filter)
    qm.update(
        &mut storage,
        handle.row_id,
        &[Value::Text("Alice".into()), Value::Integer(30)],
    )
    .unwrap();

    qm.process(&mut storage);

    let updates = qm.take_updates();
    assert_eq!(updates.len(), 1);
    assert_eq!(updates[0].subscription_id, sub_id);
    assert_eq!(
        updates[0].delta.removed.len(),
        1,
        "Row should be removed when it fails filter"
    );
    assert_eq!(updates[0].delta.removed[0].id, handle.row_id);
}

#[test]
fn update_passes_filter_emits_addition() {
    // Verify: row fails filter initially, then update passes filter -> addition delta

    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert row with score=30 (fails filter)
    let handle = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(30)],
        )
        .unwrap();

    // Subscribe to score > 50
    let query = qm
        .query("users")
        .filter_gt("score", Value::Integer(50))
        .build();
    let sub_id = qm.subscribe(query).unwrap();

    qm.process(&mut storage);
    let updates = qm.take_updates();
    // Row doesn't match filter, so no addition/removals, but we should still get an update for the subscription
    assert_eq!(updates.len(), 1);
    assert_eq!(updates[0].subscription_id, sub_id);
    assert!(updates[0].delta.added.is_empty());
    assert!(updates[0].delta.updated.is_empty());
    assert!(updates[0].delta.removed.is_empty());

    // Update score to 100 (passes filter)
    qm.update(
        &mut storage,
        handle.row_id,
        &[Value::Text("Alice".into()), Value::Integer(100)],
    )
    .unwrap();

    qm.process(&mut storage);

    let updates = qm.take_updates();
    assert_eq!(updates.len(), 1);
    assert_eq!(updates[0].subscription_id, sub_id);
    assert_eq!(
        updates[0].delta.added.len(),
        1,
        "Row should be added when it now passes filter"
    );
    assert_eq!(updates[0].delta.added[0].id, handle.row_id);
    assert!(updates[0].delta.updated.is_empty());
    assert!(updates[0].delta.removed.is_empty());
}

#[test]
fn update_still_passes_filter_emits_update() {
    // Verify: row passes filter, update still passes filter -> update delta

    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert row with score=100
    let handle = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();

    // Subscribe to score > 50
    let query = qm
        .query("users")
        .filter_gt("score", Value::Integer(50))
        .build();
    let sub_id = qm.subscribe(query).unwrap();

    qm.process(&mut storage);
    let _updates = qm.take_updates(); // Clear initial add

    // Update score to 200 (still passes filter)
    qm.update(
        &mut storage,
        handle.row_id,
        &[Value::Text("Alice Updated".into()), Value::Integer(200)],
    )
    .unwrap();

    qm.process(&mut storage);

    let updates = qm.take_updates();
    assert_eq!(updates.len(), 1);
    assert_eq!(updates[0].subscription_id, sub_id);
    assert_eq!(
        updates[0].delta.updated.len(),
        1,
        "Row should be updated when it still passes filter"
    );
    assert_eq!(updates[0].delta.updated[0].0.id, handle.row_id);
    assert_eq!(updates[0].delta.updated[0].1.id, handle.row_id);
}

#[test]
fn update_to_untracked_row_is_silent() {
    // Verify: row doesn't match filter, update still doesn't match -> no delta

    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert row with score=30 (fails filter)
    let handle = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(30)],
        )
        .unwrap();

    // Subscribe to score > 50
    let query = qm
        .query("users")
        .filter_gt("score", Value::Integer(50))
        .build();
    let _sub_id = qm.subscribe(query).unwrap();

    qm.process(&mut storage);
    let updates = qm.take_updates();
    assert_eq!(updates.len(), 1);
    assert!(updates[0].delta.added.is_empty());
    assert!(updates[0].delta.updated.is_empty());
    assert!(updates[0].delta.removed.is_empty());

    // Update score to 40 (still fails filter)
    qm.update(
        &mut storage,
        handle.row_id,
        &[Value::Text("Alice".into()), Value::Integer(40)],
    )
    .unwrap();

    qm.process(&mut storage);

    let updates = qm.take_updates();
    // No updates should be emitted since the row doesn't match the filter before or after the update
    assert!(
        updates.is_empty(),
        "No updates for row that doesn't match filter before or after update"
    );
}

#[test]
fn insert_then_update_same_cycle() {
    // Verify: insert + update before process() -> single added delta with final values

    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Subscribe first
    let query = qm.query("users").build();
    let sub_id = qm.subscribe(query).unwrap();

    // Insert
    let handle = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();

    // Update before process()
    qm.update(
        &mut storage,
        handle.row_id,
        &[Value::Text("Alice Updated".into()), Value::Integer(200)],
    )
    .unwrap();

    // Single process()
    qm.process(&mut storage);

    let updates = qm.take_updates();
    assert_eq!(updates.len(), 1);
    assert_eq!(updates[0].subscription_id, sub_id);

    // Should be added (not updated, since this is new to subscription)
    assert_eq!(updates[0].delta.added.len(), 1, "Row should be added");
    assert!(
        updates[0].delta.updated.is_empty(),
        "No spurious update delta"
    );

    // Verify final values
    let row = &updates[0].delta.added[0];
    let descriptor = RowDescriptor::new(vec![
        ColumnDescriptor::new("name", ColumnType::Text),
        ColumnDescriptor::new("score", ColumnType::Integer),
    ]);
    let values = crate::query_manager::encoding::decode_row(&descriptor, &row.data).unwrap();
    assert_eq!(values[0], Value::Text("Alice Updated".into()));
    assert_eq!(values[1], Value::Integer(200));
}
