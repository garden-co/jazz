use super::*;

#[test]
fn subscription_updates_after_insert_and_process() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Register subscription
    let query = qm.query("users").build();
    let sub_id = qm.subscribe(query).unwrap();

    // Insert a row
    let handle = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();

    // Process - should settle subscriptions
    qm.process(&mut storage);

    // Now we should have subscription updates
    let updates = qm.take_updates();
    assert_eq!(updates.len(), 1);
    assert_eq!(updates[0].subscription_id, sub_id);
    assert_eq!(updates[0].delta.added.len(), 1);
    assert_eq!(
        updates[0].delta.added[0].id, handle.row_id,
        "Delta should identify the inserted row"
    );
}

#[test]
fn settled_clean_subscription_does_not_request_visibility_recompute_on_idle_process() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    let query = qm.query("users").build();
    let sub_id = qm.subscribe(query).unwrap();

    qm.insert(
        &mut storage,
        "users",
        &[Value::Text("Alice".into()), Value::Integer(100)],
    )
    .unwrap();

    qm.process(&mut storage);
    let updates = qm.take_updates();
    assert_eq!(updates.len(), 1);

    let subscription = qm.subscriptions.get(&sub_id).unwrap();
    assert!(subscription.settled_once);
    assert!(!subscription.graph.has_dirty_nodes());
    assert!(!subscription.needs_visibility_recompute);

    qm.process(&mut storage);

    assert!(qm.take_updates().is_empty());
    assert!(!qm.subscriptions[&sub_id].needs_visibility_recompute);
}

#[test]
fn batch_fate_processing_does_not_scan_visible_regions_to_find_members() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let mut qm = QueryManager::new(sync_manager);
    qm.set_current_schema(schema, "dev", "main");
    let mut storage = CountingCatalogueUpsertsStorage::with_inner(seeded_memory_storage(
        &qm.schema_context().current_schema,
    ));

    let query = qm.query("users").build();
    qm.subscribe(query).unwrap();

    let handle = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();
    qm.process(&mut storage);
    qm.take_updates();
    storage.reset_visible_region_scans();

    qm.sync_manager_mut()
        .push_pending_batch_fate(crate::batch_fate::BatchFate::DurableDirect {
            batch_id: handle.batch_id,
            confirmed_tier: DurabilityTier::GlobalServer,
        });

    qm.process(&mut storage);

    assert_eq!(
        storage.visible_region_scans(),
        0,
        "batch fate processing should use batch records and subscription provenance, not scan/decode every visible row"
    );
}

#[test]
fn sorted_limited_subscription_reorders_when_new_top_row_arrives() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    let query = qm.query("users").order_by_desc("score").limit(2).build();
    let sub_id = qm.subscribe(query).unwrap();

    qm.insert(
        &mut storage,
        "users",
        &[Value::Text("Alice".into()), Value::Integer(100)],
    )
    .unwrap();
    qm.insert(
        &mut storage,
        "users",
        &[Value::Text("Bob".into()), Value::Integer(50)],
    )
    .unwrap();
    qm.insert(
        &mut storage,
        "users",
        &[Value::Text("Charlie".into()), Value::Integer(75)],
    )
    .unwrap();
    qm.process(&mut storage);
    let _initial_updates = qm.take_updates();

    qm.insert(
        &mut storage,
        "users",
        &[Value::Text("Diana".into()), Value::Integer(125)],
    )
    .unwrap();
    qm.process(&mut storage);

    let updates = qm.take_updates();
    let delta = updates
        .iter()
        .find(|u| u.subscription_id == sub_id)
        .map(|u| &u.delta)
        .expect("sorted/limited subscription should emit update for new top row");

    assert_eq!(delta.added.len(), 1);
    assert_eq!(delta.removed.len(), 1);

    let results = qm.get_subscription_results(sub_id);
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].1[0], Value::Text("Diana".into())); // 125
    assert_eq!(results[1].1[0], Value::Text("Alice".into())); // 100
}

#[test]
fn offset_limited_subscription_shifts_window_when_deleting_row_before_window() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    let query = qm
        .query("users")
        .order_by("score")
        .offset(1)
        .limit(2)
        .build();
    let sub_id = qm.subscribe(query).unwrap();

    let handle_a = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("A".into()), Value::Integer(1)],
        )
        .unwrap();
    let handle_b = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("B".into()), Value::Integer(2)],
        )
        .unwrap();
    let handle_c = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("C".into()), Value::Integer(3)],
        )
        .unwrap();
    let handle_d = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("D".into()), Value::Integer(4)],
        )
        .unwrap();

    qm.process(&mut storage);
    let _initial_updates = qm.take_updates();

    let initial_results = qm.get_subscription_results(sub_id);
    assert_eq!(
        initial_results
            .iter()
            .map(|(id, _)| *id)
            .collect::<Vec<_>>(),
        vec![handle_b.row_id, handle_c.row_id]
    );

    qm.delete(&mut storage, handle_a.row_id).unwrap();
    qm.process(&mut storage);

    let updates = qm.take_updates();
    let delta = updates
        .iter()
        .find(|u| u.subscription_id == sub_id)
        .map(|u| &u.delta)
        .expect("offset/limit subscription should emit update when leading row is deleted");

    assert_eq!(delta.removed.len(), 1);
    assert_eq!(delta.removed[0].id, handle_b.row_id);
    assert_eq!(delta.added.len(), 1);
    assert_eq!(delta.added[0].id, handle_d.row_id);

    let results = qm.get_subscription_results(sub_id);
    assert_eq!(
        results.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
        vec![handle_c.row_id, handle_d.row_id]
    );
}

#[test]
fn synced_insert_appears_in_subscription_delta() {
    use crate::query_manager::encoding::{decode_row, encode_row};
    use std::collections::HashMap;

    // Verify that a synced insert appears in subscription deltas

    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);
    let branch = get_branch(&qm);

    // Simulate receiving a new row from sync BEFORE subscribing
    // (similar to existing synced_update_updates_column_indices pattern)
    let row_id = crate::object::ObjectId::new();
    let author = row_id;

    // Receive object with table metadata
    let mut metadata = HashMap::new();
    metadata.insert(MetadataKey::Table.to_string(), "users".to_string());
    put_test_row_metadata(&mut storage, row_id, metadata);

    // Subscribe before the synced row arrives.
    let query = qm.query("users").build();
    let sub_id = qm.subscribe(query).unwrap();

    // Encode the row data (name="SyncedUser", score=42)
    let descriptor = RowDescriptor::new(vec![
        ColumnDescriptor::new("name", ColumnType::Text),
        ColumnDescriptor::new("score", ColumnType::Integer),
    ]);
    let row_data = encode_row(
        &descriptor,
        &[Value::Text("SyncedUser".into()), Value::Integer(42)],
    )
    .unwrap();

    // Receive the commit (insert)
    let commit = stored_row_commit(smallvec![], row_data, 1000, author.to_string());
    receive_row_commit(&mut qm, &mut storage, row_id, &branch, commit);

    // Process to handle the row-native update
    qm.process(&mut storage);

    // Verify subscription delta contains the added row
    let updates = qm.take_updates();
    assert_eq!(updates.len(), 1, "Should have one subscription update");
    assert_eq!(updates[0].subscription_id, sub_id);
    assert_eq!(
        updates[0].delta.added.len(),
        1,
        "Delta should contain one added row"
    );

    // Decode the row to verify contents
    let row = &updates[0].delta.added[0];
    let values = decode_row(&descriptor, &row.data).unwrap();
    assert_eq!(values[0], Value::Text("SyncedUser".into()));
    assert_eq!(values[1], Value::Integer(42));
}

#[test]
fn synced_row_visible_in_filtered_subscription() {
    use crate::query_manager::encoding::{decode_row, encode_row};
    use std::collections::HashMap;

    // Verify that synced rows are correctly filtered by subscription predicates.
    // Rows matching the filter appear in deltas; rows not matching are excluded.

    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);
    let branch = get_branch(&qm);

    // Subscribe to filtered query: users with score > 25
    let query = qm
        .query("users")
        .filter_gt("score", Value::Integer(25))
        .build();
    let sub_id = qm.subscribe(query).unwrap();

    // Row descriptor for encoding
    let descriptor = RowDescriptor::new(vec![
        ColumnDescriptor::new("name", ColumnType::Text),
        ColumnDescriptor::new("score", ColumnType::Integer),
    ]);

    // --- Test 1: Synced row that matches filter (score=30 > 25) ---

    let row_id_1 = crate::object::ObjectId::new();
    let author_1 = row_id_1;

    let mut metadata_1 = HashMap::new();
    metadata_1.insert(MetadataKey::Table.to_string(), "users".to_string());
    put_test_row_metadata(&mut storage, row_id_1, metadata_1);

    let data_1 = encode_row(
        &descriptor,
        &[Value::Text("HighScorer".into()), Value::Integer(30)],
    )
    .unwrap();

    let commit_1 = stored_row_commit(smallvec![], data_1, 1000, author_1.to_string());
    receive_row_commit(&mut qm, &mut storage, row_id_1, &branch, commit_1);

    qm.process(&mut storage);

    let updates = qm.take_updates();
    assert_eq!(
        updates.len(),
        1,
        "Should have subscription update for matching row"
    );
    assert_eq!(updates[0].subscription_id, sub_id);
    assert_eq!(
        updates[0].delta.added.len(),
        1,
        "Delta should contain the matching row"
    );

    // Verify the row data
    let row = &updates[0].delta.added[0];
    let values = decode_row(&descriptor, &row.data).unwrap();
    assert_eq!(values[0], Value::Text("HighScorer".into()));
    assert_eq!(values[1], Value::Integer(30));

    // --- Test 2: Synced row that does NOT match filter (score=20 < 25) ---

    let row_id_2 = crate::object::ObjectId::new();
    let author_2 = row_id_2;

    let mut metadata_2 = HashMap::new();
    metadata_2.insert(MetadataKey::Table.to_string(), "users".to_string());
    put_test_row_metadata(&mut storage, row_id_2, metadata_2);

    let data_2 = encode_row(
        &descriptor,
        &[Value::Text("LowScorer".into()), Value::Integer(20)],
    )
    .unwrap();

    let commit_2 = stored_row_commit(smallvec![], data_2, 2000, author_2.to_string());
    receive_row_commit(&mut qm, &mut storage, row_id_2, &branch, commit_2);

    qm.process(&mut storage);

    let updates = qm.take_updates();
    // Should have NO updates because the row doesn't match the filter
    assert_eq!(
        updates.len(),
        0,
        "Should have no subscription update for non-matching row"
    );

    // But verify it's in the index (just not in the filtered subscription)
    let query = qm
        .query("users")
        .filter_eq("name", Value::Text("LowScorer".into()))
        .build();
    let results = execute_query(&mut qm, &mut storage, query).unwrap();
    assert_eq!(
        results.len(),
        1,
        "Non-matching row should still be in index"
    );
}

#[test]
fn local_update_emits_subscription_delta() {
    // Verify that local qm.update() causes subscription to emit an update delta
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert row
    let handle = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();

    // Subscribe to all users
    let query = qm.query("users").build();
    let sub_id = qm.subscribe(query).unwrap();

    // Process to get the initial add
    qm.process(&mut storage);
    let updates = qm.take_updates();
    assert_eq!(updates.len(), 1);
    assert_eq!(updates[0].delta.added.len(), 1);

    // Update the row
    qm.update(
        &mut storage,
        handle.row_id,
        &[Value::Text("Alice Updated".into()), Value::Integer(200)],
    )
    .unwrap();

    // Process
    qm.process(&mut storage);

    // Should have an update delta
    let updates = qm.take_updates();
    assert_eq!(updates.len(), 1, "Should have one subscription update");
    assert_eq!(updates[0].subscription_id, sub_id);
    assert_eq!(
        updates[0].delta.updated.len(),
        1,
        "Delta should contain one updated row"
    );

    // Verify old and new values
    let (old_row, new_row) = &updates[0].delta.updated[0];
    let descriptor = RowDescriptor::new(vec![
        ColumnDescriptor::new("name", ColumnType::Text),
        ColumnDescriptor::new("score", ColumnType::Integer),
    ]);
    let old_values =
        crate::query_manager::encoding::decode_row(&descriptor, &old_row.data).unwrap();
    let new_values =
        crate::query_manager::encoding::decode_row(&descriptor, &new_row.data).unwrap();

    assert_eq!(old_values[0], Value::Text("Alice".into()));
    assert_eq!(old_values[1], Value::Integer(100));
    assert_eq!(new_values[0], Value::Text("Alice Updated".into()));
    assert_eq!(new_values[1], Value::Integer(200));
}

#[test]
fn synced_update_emits_subscription_delta() {
    use crate::query_manager::encoding::encode_row;
    use crate::sync_manager::{InboxEntry, ServerId, Source, SyncPayload};

    // Verify that synced updates (receive_commit) cause subscription to emit update delta

    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert a row locally first
    let handle = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();
    let row_id = handle.row_id;
    let first_commit_id = handle.batch_id;

    // Subscribe to all users
    let query = qm.query("users").build();
    let sub_id = qm.subscribe(query).unwrap();

    // Process to get the initial add
    qm.process(&mut storage);
    let _updates = qm.take_updates(); // Clear initial add
    let branch = get_branch(&qm);
    let base_timestamp = load_visible_row(&storage, row_id, &branch).updated_at;

    // Now simulate a synced update
    let descriptor = RowDescriptor::new(vec![
        ColumnDescriptor::new("name", ColumnType::Text),
        ColumnDescriptor::new("score", ColumnType::Integer),
    ]);
    let updated_data = encode_row(
        &descriptor,
        &[Value::Text("Alice Synced".into()), Value::Integer(300)],
    )
    .unwrap();

    let author = row_id;
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

    // Process
    qm.process(&mut storage);

    // Should have an update delta
    let updates = qm.take_updates();
    assert_eq!(updates.len(), 1, "Should have one subscription update");
    assert_eq!(updates[0].subscription_id, sub_id);
    assert_eq!(
        updates[0].delta.updated.len(),
        1,
        "Delta should contain one updated row"
    );

    // Verify new values
    let (_old_row, new_row) = &updates[0].delta.updated[0];
    let new_values =
        crate::query_manager::encoding::decode_row(&descriptor, &new_row.data).unwrap();
    assert_eq!(new_values[0], Value::Text("Alice Synced".into()));
    assert_eq!(new_values[1], Value::Integer(300));
}

#[test]
fn multiple_updates_same_row_single_delta() {
    // Verify that marking a row updated multiple times before process()
    // results in a single update delta reflecting final state

    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert and subscribe
    let handle = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();

    let query = qm.query("users").build();
    let _sub_id = qm.subscribe(query).unwrap();

    qm.process(&mut storage);
    let _updates = qm.take_updates(); // Clear initial add

    // Update twice before process()
    qm.update(
        &mut storage,
        handle.row_id,
        &[Value::Text("Alice V2".into()), Value::Integer(200)],
    )
    .unwrap();
    qm.update(
        &mut storage,
        handle.row_id,
        &[Value::Text("Alice V3".into()), Value::Integer(300)],
    )
    .unwrap();

    // Single process()
    qm.process(&mut storage);

    let updates = qm.take_updates();
    assert_eq!(updates.len(), 1, "Should have one subscription update");
    assert_eq!(
        updates[0].delta.updated.len(),
        1,
        "Should have single update delta, not two"
    );

    // Verify it reflects final state
    let (_old_row, new_row) = &updates[0].delta.updated[0];
    let descriptor = RowDescriptor::new(vec![
        ColumnDescriptor::new("name", ColumnType::Text),
        ColumnDescriptor::new("score", ColumnType::Integer),
    ]);
    let new_values =
        crate::query_manager::encoding::decode_row(&descriptor, &new_row.data).unwrap();
    assert_eq!(new_values[0], Value::Text("Alice V3".into()));
    assert_eq!(new_values[1], Value::Integer(300));
}

#[test]
fn synced_update_that_fails_filter_emits_removal_delta() {
    use crate::query_manager::encoding::encode_row;

    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);
    let branch = get_branch(&qm);

    let query = qm
        .query("users")
        .filter_ge("score", Value::Integer(75))
        .build();
    let sub_id = qm.subscribe(query).unwrap();

    let handle = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();
    let base_batch_id = handle.batch_id;

    qm.process(&mut storage);
    let updates = qm.take_updates();
    assert_eq!(updates.len(), 1);
    assert_eq!(updates[0].subscription_id, sub_id);
    assert_eq!(updates[0].delta.added.len(), 1);
    assert_eq!(updates[0].delta.added[0].id, handle.row_id);

    let base_timestamp = load_visible_row(&storage, handle.row_id, &branch).updated_at;
    let descriptor = RowDescriptor::new(vec![
        ColumnDescriptor::new("name", ColumnType::Text),
        ColumnDescriptor::new("score", ColumnType::Integer),
    ]);
    let updated_data = encode_row(
        &descriptor,
        &[Value::Text("Alice".into()), Value::Integer(30)],
    )
    .unwrap();
    let synced_commit = stored_row_commit(
        smallvec![base_batch_id],
        updated_data,
        base_timestamp + 1,
        handle.row_id.to_string(),
    );
    receive_row_commit(&mut qm, &mut storage, handle.row_id, &branch, synced_commit);

    qm.process(&mut storage);

    let updates = qm.take_updates();
    assert_eq!(updates.len(), 1);
    assert_eq!(updates[0].subscription_id, sub_id);
    assert_eq!(
        updates[0].delta.removed.len(),
        1,
        "synced update should remove the row when it no longer matches the filter"
    );
    assert_eq!(updates[0].delta.removed[0].id, handle.row_id);
    assert!(updates[0].delta.added.is_empty());
    assert!(updates[0].delta.updated.is_empty());
}

#[test]
fn synced_boolean_eq_update_that_fails_filter_emits_removal_delta() {
    use crate::query_manager::encoding::encode_row;

    let mut schema = Schema::new();
    schema.insert(
        TableName::new("todos"),
        RowDescriptor::new(vec![
            ColumnDescriptor::new("title", ColumnType::Text),
            ColumnDescriptor::new("done", ColumnType::Boolean),
            ColumnDescriptor::new("priority", ColumnType::Integer).nullable(),
            ColumnDescriptor::new("owner_id", ColumnType::Uuid).nullable(),
            ColumnDescriptor::new(
                "tags",
                ColumnType::Array {
                    element: Box::new(ColumnType::Text),
                },
            ),
            ColumnDescriptor::new("payload", ColumnType::Bytea).nullable(),
        ])
        .into(),
    );
    let (mut qm, mut storage) = create_query_manager(SyncManager::new(), schema);
    let branch = get_branch(&qm);

    let query = qm
        .query("todos")
        .filter_eq("done", Value::Boolean(false))
        .build();
    let sub_id = qm.subscribe(query).unwrap();

    let handle = qm
        .insert(
            &mut storage,
            "todos",
            &[
                Value::Text("watch-me".into()),
                Value::Boolean(false),
                Value::Null,
                Value::Null,
                Value::Array(vec![Value::Text("x".into())]),
                Value::Null,
            ],
        )
        .unwrap();
    let base_batch_id = handle.batch_id;

    qm.process(&mut storage);
    let updates = qm.take_updates();
    assert_eq!(updates.len(), 1);
    assert_eq!(updates[0].subscription_id, sub_id);
    assert_eq!(updates[0].delta.added.len(), 1);
    assert_eq!(updates[0].delta.added[0].id, handle.row_id);

    let base_timestamp = load_visible_row(&storage, handle.row_id, &branch).updated_at;
    let descriptor = RowDescriptor::new(vec![
        ColumnDescriptor::new("title", ColumnType::Text),
        ColumnDescriptor::new("done", ColumnType::Boolean),
        ColumnDescriptor::new("priority", ColumnType::Integer).nullable(),
        ColumnDescriptor::new("owner_id", ColumnType::Uuid).nullable(),
        ColumnDescriptor::new(
            "tags",
            ColumnType::Array {
                element: Box::new(ColumnType::Text),
            },
        ),
        ColumnDescriptor::new("payload", ColumnType::Bytea).nullable(),
    ]);
    let updated_data = encode_row(
        &descriptor,
        &[
            Value::Text("watch-me".into()),
            Value::Boolean(true),
            Value::Null,
            Value::Null,
            Value::Array(vec![Value::Text("x".into())]),
            Value::Null,
        ],
    )
    .unwrap();
    let synced_commit = stored_row_commit(
        smallvec![base_batch_id],
        updated_data,
        base_timestamp + 1,
        handle.row_id.to_string(),
    );
    receive_row_commit(&mut qm, &mut storage, handle.row_id, &branch, synced_commit);

    qm.process(&mut storage);

    let updates = qm.take_updates();
    assert_eq!(updates.len(), 1);
    assert_eq!(updates[0].subscription_id, sub_id);
    assert_eq!(
        updates[0].delta.removed.len(),
        1,
        "synced boolean update should remove the row when it no longer matches the filter"
    );
    assert_eq!(updates[0].delta.removed[0].id, handle.row_id);
    assert!(updates[0].delta.added.is_empty());
    assert!(updates[0].delta.updated.is_empty());
}

#[test]
fn sync_inbox_insert_flows_to_subscription_delta() {
    // End-to-end test: sync message → SyncManager inbox → QueryManager subscription
    // This tests the full path through push_inbox() → process_inbox() → process()
    use crate::query_manager::encoding::{decode_row, encode_row};
    use crate::sync_manager::{InboxEntry, ServerId, Source, SyncPayload};

    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);
    let branch = get_branch(&qm);

    // Add a "server" that we'll receive updates from
    let server_id = ServerId::new();
    connect_server(&mut qm, &storage, server_id);

    // Subscribe to users table
    let query = qm.query("users").build();
    let sub_id = qm.subscribe(query).unwrap();

    // Process to initialize - expect an initial empty update (subscription settled)
    qm.process(&mut storage);
    let updates = qm.take_updates();
    assert_eq!(updates.len(), 1, "Should have initial settlement update");
    assert!(
        updates[0].delta.added.is_empty(),
        "Initial delta should be empty"
    );

    // Construct the sync message payload
    let row_id = crate::object::ObjectId::new();
    let author = row_id;

    let descriptor = RowDescriptor::new(vec![
        ColumnDescriptor::new("name", ColumnType::Text),
        ColumnDescriptor::new("score", ColumnType::Integer),
    ]);
    let row_data = encode_row(
        &descriptor,
        &[Value::Text("SyncedUser".into()), Value::Integer(42)],
    )
    .unwrap();

    let commit = stored_row_commit(smallvec![], row_data, 1000, author.to_string());
    let row = commit.to_row(row_id, &branch, RowState::VisibleDirect);

    // Object metadata marking it as a "users" table row
    let mut obj_metadata = std::collections::HashMap::new();
    obj_metadata.insert(MetadataKey::Table.to_string(), "users".to_string());

    // Push the sync message through SyncManager's inbox
    qm.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Server(server_id),
        payload: SyncPayload::RowBatchCreated {
            metadata: Some(crate::sync_manager::RowMetadata {
                id: row_id,
                metadata: obj_metadata,
            }),
            row,
        },
    });

    // Process the inbox (SyncManager level)
    qm.sync_manager_mut().process_inbox(&mut storage);

    // Process (QueryManager level) - this should pick up the object update
    qm.process(&mut storage);

    // Verify subscription received the delta
    let updates = qm.take_updates();
    assert_eq!(updates.len(), 1, "Should have one subscription update");
    assert_eq!(updates[0].subscription_id, sub_id);
    assert_eq!(
        updates[0].delta.added.len(),
        1,
        "Delta should contain one added row"
    );

    // Verify the row contents
    let row = &updates[0].delta.added[0];
    let values = decode_row(&descriptor, &row.data).unwrap();
    assert_eq!(values[0], Value::Text("SyncedUser".into()));
    assert_eq!(values[1], Value::Integer(42));
}

#[test]
fn sync_inbox_update_flows_to_subscription_delta() {
    // End-to-end test: sync update message → subscription emits update delta
    use crate::query_manager::encoding::{decode_row, encode_row};
    use crate::sync_manager::{InboxEntry, ServerId, Source, SyncPayload};

    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);
    let branch = get_branch(&qm);

    // Add a "server"
    let server_id = ServerId::new();
    connect_server(&mut qm, &storage, server_id);
    // Insert a row locally first
    let handle = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();
    let row_id = handle.row_id;
    let first_commit_id = handle.batch_id;

    // Subscribe to users
    let query = qm.query("users").build();
    let sub_id = qm.subscribe(query).unwrap();

    // Process to get initial state
    qm.process(&mut storage);
    let _ = qm.take_updates(); // Clear initial delta
    let base_timestamp = load_visible_row(&storage, row_id, &branch).updated_at;

    // Now simulate receiving an update from sync (as if another peer modified the row)
    let descriptor = RowDescriptor::new(vec![
        ColumnDescriptor::new("name", ColumnType::Text),
        ColumnDescriptor::new("score", ColumnType::Integer),
    ]);
    let updated_data = encode_row(
        &descriptor,
        &[Value::Text("Alice Updated".into()), Value::Integer(999)],
    )
    .unwrap();

    let update_commit = stored_row_commit(
        smallvec![first_commit_id],
        updated_data,
        base_timestamp + 1,
        row_id.to_string(),
    );
    let row = update_commit.to_row(row_id, &branch, RowState::VisibleDirect);

    // Push the update through SyncManager inbox
    qm.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Server(server_id),
        payload: SyncPayload::RowBatchCreated {
            metadata: None, // No metadata needed for existing object
            row,
        },
    });

    // Process both layers
    qm.sync_manager_mut().process_inbox(&mut storage);
    qm.process(&mut storage);

    // Verify subscription received update delta
    let updates = qm.take_updates();
    assert_eq!(updates.len(), 1, "Should have one subscription update");
    assert_eq!(updates[0].subscription_id, sub_id);
    assert_eq!(
        updates[0].delta.updated.len(),
        1,
        "Delta should contain one updated row"
    );

    // Verify the new values
    let (_old_row, new_row) = &updates[0].delta.updated[0];
    let values = decode_row(&descriptor, &new_row.data).unwrap();
    assert_eq!(values[0], Value::Text("Alice Updated".into()));
    assert_eq!(values[1], Value::Integer(999));
}

#[test]
fn two_peer_sync_insert_reaches_subscription() {
    // Full two-peer test: Peer A inserts → (simulated sync) → Peer B subscription delta
    // This demonstrates the conceptual flow even though we construct the payload manually
    use crate::query_manager::encoding::decode_row;
    use crate::sync_manager::{InboxEntry, ServerId, Source, SyncPayload};

    // Create two peers
    let sync_manager_a = SyncManager::new();
    let sync_manager_b = SyncManager::new();
    let schema = test_schema();
    let (mut peer_a, mut storage_a) = create_query_manager(sync_manager_a, schema.clone());
    let (mut peer_b, mut storage_b) = create_query_manager(sync_manager_b, schema);

    // Peer B sets up query subscription
    let query = peer_b.query("users").build();
    let sub_id = peer_b.subscribe(query).unwrap();

    // Peer B adds a "server" (representing Peer A)
    let peer_a_as_server = ServerId::new();
    connect_server(&mut peer_b, &storage_b, peer_a_as_server);

    // Process both to initialize
    peer_a.process(&mut storage_a);
    peer_b.process(&mut storage_b);
    let _ = peer_b.take_updates();

    // Peer A inserts a row
    let handle = peer_a
        .insert(
            &mut storage_a,
            "users",
            &[Value::Text("FromPeerA".into()), Value::Integer(123)],
        )
        .unwrap();
    let row_id = handle.row_id;

    // Read the actual row metadata and visible row from storage.
    // This simulates "what would be sent over the wire"
    let branch_name = get_branch(&peer_a);
    let metadata = test_row_metadata(&storage_a, row_id);
    let row = load_visible_row(&storage_a, row_id, &branch_name);

    // Send to Peer B via SyncManager inbox
    peer_b.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Server(peer_a_as_server),
        payload: SyncPayload::RowBatchCreated {
            metadata: Some(crate::sync_manager::RowMetadata {
                id: row_id,
                metadata,
            }),
            row,
        },
    });

    // Peer B processes the sync message
    peer_b.sync_manager_mut().process_inbox(&mut storage_b);
    peer_b.process(&mut storage_b);

    // Verify Peer B's subscription received the row
    let updates = peer_b.take_updates();
    assert_eq!(
        updates.len(),
        1,
        "Peer B should have one subscription update"
    );
    assert_eq!(updates[0].subscription_id, sub_id);
    assert_eq!(
        updates[0].delta.added.len(),
        1,
        "Delta should contain one added row"
    );

    // Verify the row came from Peer A
    let row = &updates[0].delta.added[0];
    let descriptor = RowDescriptor::new(vec![
        ColumnDescriptor::new("name", ColumnType::Text),
        ColumnDescriptor::new("score", ColumnType::Integer),
    ]);
    let values = decode_row(&descriptor, &row.data).unwrap();
    assert_eq!(values[0], Value::Text("FromPeerA".into()));
    assert_eq!(values[1], Value::Integer(123));
}

#[test]
fn subscribe_with_sync_sends_to_servers() {
    use crate::sync_manager::{Destination, ServerId, SyncPayload};
    use uuid::Uuid;

    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut client_qm, _storage) = create_query_manager(sync_manager, schema);

    // Add a server
    let server_id = ServerId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    connect_server(&mut client_qm, &_storage, server_id);

    // Clear initial outbox (full sync to server)
    let _ = client_qm.sync_manager_mut().take_outbox();

    // Subscribe with sync
    let query = client_qm
        .query("users")
        .filter_gt("score", Value::Integer(50))
        .build();

    let _sub_id = client_qm.subscribe_with_sync(query, None, None).unwrap();

    // Check outbox for QuerySubscription
    let outbox = client_qm.sync_manager_mut().take_outbox();

    let query_subs: Vec<_> = outbox
        .iter()
        .filter(|e| matches!(e.destination, Destination::Server(id) if id == server_id))
        .filter(|e| matches!(e.payload, SyncPayload::QuerySubscription { .. }))
        .collect();

    assert_eq!(
        query_subs.len(),
        1,
        "Should send QuerySubscription to server"
    );
}

#[test]
fn subscribe_with_sync_local_only_sends_to_connected_tier() {
    use crate::sync_manager::QueryPropagation;
    use crate::sync_manager::{Destination, ServerId, SyncPayload};
    use uuid::Uuid;

    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut client_qm, _storage) = create_query_manager(sync_manager, schema);

    let server_id = ServerId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    connect_server(&mut client_qm, &_storage, server_id);
    let _ = client_qm.sync_manager_mut().take_outbox();

    let query = client_qm
        .query("users")
        .filter_gt("score", Value::Integer(50))
        .build();

    let _sub_id = client_qm
        .subscribe_with_sync_and_propagation(query, None, None, QueryPropagation::LocalOnly)
        .unwrap();

    let outbox = client_qm.sync_manager_mut().take_outbox();
    let query_subs: Vec<_> = outbox
        .iter()
        .filter(|e| matches!(e.destination, Destination::Server(id) if id == server_id))
        .filter(|e| matches!(e.payload, SyncPayload::QuerySubscription { .. }))
        .collect();

    assert_eq!(
        query_subs.len(),
        1,
        "local-only subscription should be sent to connected tier"
    );
}

#[test]
fn subscribe_with_sync_local_only_on_persistence_tier_does_not_send_upstream() {
    use crate::sync_manager::QueryPropagation;
    use crate::sync_manager::{Destination, DurabilityTier, ServerId, SyncPayload};
    use uuid::Uuid;

    let sync_manager = SyncManager::new().with_durability_tier(DurabilityTier::Local);
    let schema = test_schema();
    let (mut worker_qm, _storage) = create_query_manager(sync_manager, schema);

    let server_id = ServerId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    connect_server(&mut worker_qm, &_storage, server_id);
    let _ = worker_qm.sync_manager_mut().take_outbox();

    let query = worker_qm
        .query("users")
        .filter_gt("score", Value::Integer(50))
        .build();

    let _sub_id = worker_qm
        .subscribe_with_sync_and_propagation(query, None, None, QueryPropagation::LocalOnly)
        .unwrap();

    let outbox = worker_qm.sync_manager_mut().take_outbox();
    let query_subs: Vec<_> = outbox
        .iter()
        .filter(|e| matches!(e.destination, Destination::Server(id) if id == server_id))
        .filter(|e| matches!(e.payload, SyncPayload::QuerySubscription { .. }))
        .collect();

    assert_eq!(
        query_subs.len(),
        0,
        "local-tier local-only subscription should not be sent to upstream sync server"
    );
}

#[test]
fn add_server_replays_existing_local_query_subscriptions() {
    use crate::sync_manager::{Destination, QueryId, ServerId, SyncPayload};
    use uuid::Uuid;

    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut client_qm, _storage) = create_query_manager(sync_manager, schema);

    let query = client_qm
        .query("users")
        .filter_gt("score", Value::Integer(50))
        .build();
    let sub_id = client_qm.subscribe_with_sync(query, None, None).unwrap();

    // No server connected yet, so no outbound subscription forwarding.
    let outbox_before_add_server = client_qm.sync_manager_mut().take_outbox();
    assert!(
        outbox_before_add_server
            .iter()
            .all(|e| !matches!(e.payload, SyncPayload::QuerySubscription { .. })),
        "Should not forward QuerySubscription before any server is connected"
    );

    let server_id = ServerId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    connect_query_manager_upstream(&mut client_qm, &_storage, server_id);

    let outbox = client_qm.sync_manager_mut().take_outbox();
    let replayed: Vec<_> = outbox
        .iter()
        .filter(|e| matches!(e.destination, Destination::Server(id) if id == server_id))
        .filter_map(|e| {
            if let SyncPayload::QuerySubscription { query_id, .. } = &e.payload {
                Some(*query_id)
            } else {
                None
            }
        })
        .collect();

    assert_eq!(
        replayed,
        vec![QueryId(sub_id.0)],
        "add_server should replay active local subscriptions to the new server"
    );
}

#[test]
fn add_server_replays_local_only_query_subscriptions() {
    use crate::sync_manager::QueryPropagation;
    use crate::sync_manager::{Destination, ServerId, SyncPayload};
    use uuid::Uuid;

    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut client_qm, _storage) = create_query_manager(sync_manager, schema);

    let query = client_qm
        .query("users")
        .filter_gt("score", Value::Integer(50))
        .build();

    let _sub_id = client_qm
        .subscribe_with_sync_and_propagation(query, None, None, QueryPropagation::LocalOnly)
        .unwrap();

    let server_id = ServerId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    connect_query_manager_upstream(&mut client_qm, &_storage, server_id);
    let outbox = client_qm.sync_manager_mut().take_outbox();

    let replayed: Vec<_> = outbox
        .iter()
        .filter(|e| matches!(e.destination, Destination::Server(id) if id == server_id))
        .filter(|e| matches!(e.payload, SyncPayload::QuerySubscription { .. }))
        .collect();

    assert_eq!(
        replayed.len(),
        1,
        "add_server should replay local-only subscriptions"
    );
}

#[test]
fn unsubscribe_with_sync_sends_to_servers() {
    use crate::sync_manager::{Destination, ServerId, SyncPayload};
    use uuid::Uuid;

    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut client_qm, _storage) = create_query_manager(sync_manager, schema);

    // Add a server
    let server_id = ServerId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    connect_server(&mut client_qm, &_storage, server_id);

    // Subscribe with sync
    let query = client_qm
        .query("users")
        .filter_gt("score", Value::Integer(50))
        .build();

    let sub_id = client_qm.subscribe_with_sync(query, None, None).unwrap();

    // Clear outbox
    let _ = client_qm.sync_manager_mut().take_outbox();

    // Unsubscribe with sync
    client_qm.unsubscribe_with_sync(sub_id);

    // Check outbox for QueryUnsubscription
    let outbox = client_qm.sync_manager_mut().take_outbox();

    let query_unsubs: Vec<_> = outbox
        .iter()
        .filter(|e| matches!(e.destination, Destination::Server(id) if id == server_id))
        .filter(|e| matches!(e.payload, SyncPayload::QueryUnsubscription { .. }))
        .collect();

    assert_eq!(
        query_unsubs.len(),
        1,
        "Should send QueryUnsubscription to server"
    );
}

#[test]
fn mid_tier_forwards_query_subscription_upstream() {
    use crate::sync_manager::{ClientId, Destination, InboxEntry, ServerId, Source, SyncPayload};
    use uuid::Uuid;

    // Setup mid-tier server with schema
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut mid_tier, mut storage) = create_query_manager(sync_manager, schema);

    // Add upstream server
    let upstream_id = ServerId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    connect_server(&mut mid_tier, &storage, upstream_id);

    // Add downstream client
    let client_id = ClientId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    connect_client(&mut mid_tier, &storage, client_id);

    // Clear the outbox (add_server queues full sync)
    let _ = mid_tier.sync_manager_mut().take_outbox();

    // Simulate receiving QuerySubscription from client
    let query = mid_tier
        .query("users")
        .filter_gt("score", Value::Integer(50))
        .build();

    mid_tier.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Client(client_id),
        payload: SyncPayload::QuerySubscription {
            query_id: crate::sync_manager::QueryId(42),
            query: Box::new(query),
            session: None,
            required_tier: None,
            propagation: crate::sync_manager::QueryPropagation::Full,
            policy_context_tables: vec![],
        },
    });

    // Process the subscription
    mid_tier
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();
    mid_tier.process(&mut storage);

    // Check that QuerySubscription was forwarded to upstream server
    let outbox = mid_tier.sync_manager_mut().take_outbox();

    let forwarded: Vec<_> = outbox
        .iter()
        .filter(|e| matches!(e.destination, Destination::Server(id) if id == upstream_id))
        .filter(|e| matches!(e.payload, SyncPayload::QuerySubscription { .. }))
        .collect();

    assert_eq!(
        forwarded.len(),
        1,
        "Mid-tier should forward QuerySubscription to upstream server"
    );
}

#[test]
fn mid_tier_suppresses_repeated_local_query_settled_for_global_downstream_subscription() {
    use crate::sync_manager::{
        ClientId, Destination, DurabilityTier, InboxEntry, OutboxEntry, ServerId, Source,
        SyncPayload,
    };
    use uuid::Uuid;

    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut mid_tier, mut storage) = create_query_manager(sync_manager, schema);

    let upstream_id = ServerId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    connect_server(&mut mid_tier, &storage, upstream_id);

    let client_id = ClientId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    connect_client(&mut mid_tier, &storage, client_id);
    let _ = mid_tier.sync_manager_mut().take_outbox();

    let query = mid_tier.query("users").build();

    mid_tier.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Client(client_id),
        payload: SyncPayload::QuerySubscription {
            query_id: crate::sync_manager::QueryId(42),
            query: Box::new(query),
            session: None,
            required_tier: Some(DurabilityTier::GlobalServer),
            propagation: crate::sync_manager::QueryPropagation::Full,
            policy_context_tables: vec![],
        },
    });

    mid_tier.process(&mut storage);

    let outbox = mid_tier.sync_manager_mut().take_outbox();
    assert!(
        outbox.iter().any(|entry| matches!(
            entry,
            OutboxEntry {
                destination: Destination::Server(id),
                payload: SyncPayload::QuerySubscription { query_id, required_tier: None, .. },
            } if *id == upstream_id
                && *query_id == crate::sync_manager::QueryId(42)
        )),
        "mid-tier should forward the subscription upstream without imposing the downstream-local tier gate on the upstream server"
    );
    assert!(
        outbox.iter().any(|entry| matches!(
            entry,
            OutboxEntry {
                destination: Destination::Client(id),
                payload: SyncPayload::QuerySettled {
                    query_id,
                    tier: DurabilityTier::Local,
                    ..
                },
            } if *id == client_id && *query_id == crate::sync_manager::QueryId(42)
        )),
        "the first lower-tier settled signal is preserved as the initial remote scope snapshot"
    );

    mid_tier.process(&mut storage);
    let outbox = mid_tier.sync_manager_mut().take_outbox();
    assert!(
        outbox.iter().all(|entry| !matches!(
            entry,
            OutboxEntry {
                destination: Destination::Client(id),
                payload: SyncPayload::QuerySettled {
                    query_id,
                    tier: DurabilityTier::Local,
                    ..
                },
            } if *id == client_id && *query_id == crate::sync_manager::QueryId(42)
        )),
        "after the initial scope snapshot, below-required settled signals should be suppressed"
    );
}

#[test]
fn mid_tier_keeps_local_query_settled_for_legacy_downstream_subscription() {
    use crate::sync_manager::{
        ClientId, Destination, InboxEntry, OutboxEntry, ServerId, Source, SyncPayload,
    };
    use uuid::Uuid;

    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut mid_tier, mut storage) = create_query_manager(sync_manager, schema);

    let upstream_id = ServerId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    connect_server(&mut mid_tier, &storage, upstream_id);

    let client_id = ClientId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    connect_client(&mut mid_tier, &storage, client_id);
    let _ = mid_tier.sync_manager_mut().take_outbox();

    let query = mid_tier.query("users").build();

    mid_tier.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Client(client_id),
        payload: SyncPayload::QuerySubscription {
            query_id: crate::sync_manager::QueryId(42),
            query: Box::new(query),
            session: None,
            required_tier: None,
            propagation: crate::sync_manager::QueryPropagation::Full,
            policy_context_tables: vec![],
        },
    });

    mid_tier.process(&mut storage);

    let outbox = mid_tier.sync_manager_mut().take_outbox();
    assert!(
        outbox.iter().any(|entry| matches!(
            entry,
            OutboxEntry {
                destination: Destination::Client(id),
                payload: SyncPayload::QuerySettled {
                    query_id,
                    tier: crate::sync_manager::DurabilityTier::Local,
                    ..
                },
            } if *id == client_id && *query_id == crate::sync_manager::QueryId(42)
        )),
        "subscriptions without an explicit required tier should retain the legacy local settled signal"
    );
}

#[test]
fn mid_tier_does_not_forward_local_only_query_subscription_upstream() {
    use crate::sync_manager::{ClientId, Destination, InboxEntry, ServerId, Source, SyncPayload};
    use uuid::Uuid;

    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut mid_tier, mut storage) = create_query_manager(sync_manager, schema);

    let upstream_id = ServerId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    connect_server(&mut mid_tier, &storage, upstream_id);

    let client_id = ClientId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    connect_client(&mut mid_tier, &storage, client_id);
    let _ = mid_tier.sync_manager_mut().take_outbox();

    let query = mid_tier
        .query("users")
        .filter_gt("score", Value::Integer(50))
        .build();

    mid_tier.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Client(client_id),
        payload: SyncPayload::QuerySubscription {
            query_id: crate::sync_manager::QueryId(42),
            query: Box::new(query),
            session: None,
            required_tier: None,
            propagation: crate::sync_manager::QueryPropagation::LocalOnly,
            policy_context_tables: vec![],
        },
    });
    mid_tier.process(&mut storage);

    let outbox = mid_tier.sync_manager_mut().take_outbox();
    let forwarded: Vec<_> = outbox
        .iter()
        .filter(|e| matches!(e.destination, Destination::Server(id) if id == upstream_id))
        .filter(|e| matches!(e.payload, SyncPayload::QuerySubscription { .. }))
        .collect();

    assert_eq!(
        forwarded.len(),
        0,
        "Mid-tier should not forward local-only QuerySubscription upstream"
    );
}

#[test]
fn add_server_does_not_replay_downstream_local_only_query_subscription() {
    use crate::sync_manager::{ClientId, Destination, InboxEntry, ServerId, Source, SyncPayload};
    use uuid::Uuid;

    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut mid_tier, mut storage) = create_query_manager(sync_manager, schema);

    let downstream_client = ClientId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    connect_client(&mut mid_tier, &storage, downstream_client);

    let query = mid_tier
        .query("users")
        .filter_gt("score", Value::Integer(50))
        .build();

    mid_tier.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Client(downstream_client),
        payload: SyncPayload::QuerySubscription {
            query_id: crate::sync_manager::QueryId(77),
            query: Box::new(query),
            session: None,
            required_tier: None,
            propagation: crate::sync_manager::QueryPropagation::LocalOnly,
            policy_context_tables: vec![],
        },
    });
    mid_tier.process(&mut storage);
    let _ = mid_tier.sync_manager_mut().take_outbox();

    let upstream_id = ServerId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    connect_query_manager_upstream(&mut mid_tier, &storage, upstream_id);

    let outbox = mid_tier.sync_manager_mut().take_outbox();
    let replayed: Vec<_> = outbox
        .iter()
        .filter(|e| matches!(e.destination, Destination::Server(id) if id == upstream_id))
        .filter(|e| matches!(e.payload, SyncPayload::QuerySubscription { .. }))
        .collect();

    assert_eq!(
        replayed.len(),
        0,
        "add_server should not replay downstream local-only subscriptions upstream"
    );
}

#[test]
fn mid_tier_forwards_query_unsubscription_upstream() {
    use crate::sync_manager::{ClientId, Destination, InboxEntry, ServerId, Source, SyncPayload};
    use uuid::Uuid;

    // Setup mid-tier server
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut mid_tier, mut storage) = create_query_manager(sync_manager, schema);

    // Add upstream server and downstream client
    let upstream_id = ServerId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    let client_id = ClientId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    connect_server(&mut mid_tier, &storage, upstream_id);
    connect_client(&mut mid_tier, &storage, client_id);

    // First, establish subscription
    let query = mid_tier
        .query("users")
        .filter_gt("score", Value::Integer(50))
        .build();

    let query_id = crate::sync_manager::QueryId(42);

    mid_tier.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Client(client_id),
        payload: SyncPayload::QuerySubscription {
            query_id,
            query: Box::new(query),
            session: None,
            required_tier: None,
            propagation: crate::sync_manager::QueryPropagation::Full,
            policy_context_tables: vec![],
        },
    });
    mid_tier.process(&mut storage);

    // Clear outbox
    let _ = mid_tier.sync_manager_mut().take_outbox();

    // Now send unsubscription
    mid_tier.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Client(client_id),
        payload: SyncPayload::QueryUnsubscription { query_id },
    });
    mid_tier.process(&mut storage);

    // Check that QueryUnsubscription was forwarded upstream
    let outbox = mid_tier.sync_manager_mut().take_outbox();

    let forwarded: Vec<_> = outbox
        .iter()
        .filter(|e| matches!(e.destination, Destination::Server(id) if id == upstream_id))
        .filter(|e| matches!(e.payload, SyncPayload::QueryUnsubscription { .. }))
        .collect();

    assert_eq!(
        forwarded.len(),
        1,
        "Mid-tier should forward QueryUnsubscription to upstream server"
    );
}

#[test]
fn mid_tier_relays_objects_to_clients_with_matching_scope() {
    use crate::object::ObjectId;
    use crate::sync_manager::{
        ClientId, Destination, InboxEntry, RowMetadata, ServerId, Source, SyncPayload,
    };
    use uuid::Uuid;

    // Setup mid-tier server
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut mid_tier, mut storage) = create_query_manager(sync_manager, schema.clone());

    // Add upstream server and downstream client
    let upstream_id = ServerId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    let client_id = ClientId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    connect_server(&mut mid_tier, &storage, upstream_id);
    connect_client(&mut mid_tier, &storage, client_id);

    // Insert a matching row locally first (so it's in scope)
    let handle = mid_tier
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(75)],
        )
        .unwrap();
    mid_tier.process(&mut storage);

    // Get the schema branch
    let branch_str = get_branch(&mid_tier);

    // Establish client subscription
    let query = mid_tier
        .query("users")
        .filter_gt("score", Value::Integer(50))
        .build();

    mid_tier.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Client(client_id),
        payload: SyncPayload::QuerySubscription {
            query_id: crate::sync_manager::QueryId(42),
            query: Box::new(query),
            session: None,
            required_tier: None,
            propagation: crate::sync_manager::QueryPropagation::Full,
            policy_context_tables: vec![],
        },
    });
    mid_tier.process(&mut storage);

    // Clear outbox (initial sync messages)
    let _ = mid_tier.sync_manager_mut().take_outbox();

    // Now receive an update for the existing object from upstream
    // (simulating upstream sending fresh data)
    let table_schema = schema.get(&TableName::new("users")).unwrap();
    let row_data = encode_row(
        &table_schema.columns,
        &[Value::Text("Alice".into()), Value::Integer(80)],
    )
    .unwrap();

    let author = ObjectId::new();
    let base_timestamp = load_visible_row(&storage, handle.row_id, &branch_str).updated_at;
    let commit = stored_row_commit(
        smallvec![handle.batch_id],
        row_data,
        base_timestamp + 1,
        author.to_string(),
    );
    let row = commit.to_row(handle.row_id, &branch_str, RowState::VisibleDirect);

    mid_tier.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Server(upstream_id),
        payload: SyncPayload::RowBatchCreated {
            metadata: Some(RowMetadata {
                id: handle.row_id,
                metadata: [("table".to_string(), "users".to_string())]
                    .into_iter()
                    .collect(),
            }),
            row,
        },
    });
    mid_tier.process(&mut storage);

    // Check that the update was forwarded to the client
    let outbox = mid_tier.sync_manager_mut().take_outbox();

    let relayed: Vec<_> = outbox
        .iter()
        .filter(|e| matches!(e.destination, Destination::Client(id) if id == client_id))
        .filter(|e| match &e.payload {
            SyncPayload::RowBatchNeeded { row, .. } => row.row_id == handle.row_id,
            _ => false,
        })
        .collect();

    assert_eq!(
        relayed.len(),
        1,
        "Mid-tier should relay matching row batch entries downstream"
    );
}

#[test]
fn synced_subscription_filters_rows_removed_from_remote_scope() {
    use crate::query_manager::policy::Operation;
    use crate::sync_manager::{ClientId, ServerId};
    use uuid::Uuid;

    let mut schema = Schema::new();
    schema.insert(
        TableName::new("recursive_folders"),
        TableSchema::with_policies(
            RowDescriptor::new(vec![
                ColumnDescriptor::new("owner_id", ColumnType::Text),
                ColumnDescriptor::new("name", ColumnType::Text),
                ColumnDescriptor::new("parent_id", ColumnType::Uuid)
                    .nullable()
                    .references("recursive_folders"),
            ]),
            TablePolicies::new().with_select(PolicyExpr::or(vec![
                PolicyExpr::eq_session("owner_id", vec!["user_id".into()]),
                PolicyExpr::and(vec![
                    PolicyExpr::IsNotNull {
                        column: "parent_id".into(),
                    },
                    PolicyExpr::inherits(Operation::Select, "parent_id"),
                ]),
            ])),
        ),
    );

    let server_sync = SyncManager::new();
    let (mut server, mut server_io) = create_query_manager(server_sync, schema.clone());
    let client_sync = SyncManager::new();
    let (mut client, mut client_io) = create_query_manager(client_sync, schema.clone());

    let server_id = ServerId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    let client_id = ClientId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    connect_server(&mut client, &client_io, server_id);
    connect_client(&mut server, &server_io, client_id);
    let _ = client.sync_manager_mut().take_outbox();

    let root_id = server
        .insert(
            &mut server_io,
            "recursive_folders",
            &[
                Value::Text("alice".into()),
                Value::Text("Root".into()),
                Value::Null,
            ],
        )
        .unwrap()
        .row_id;
    let child_id = server
        .insert(
            &mut server_io,
            "recursive_folders",
            &[
                Value::Text("bob".into()),
                Value::Text("Child".into()),
                Value::Null,
            ],
        )
        .unwrap()
        .row_id;
    let _grand_id = server
        .insert(
            &mut server_io,
            "recursive_folders",
            &[
                Value::Text("carol".into()),
                Value::Text("Grand".into()),
                Value::Uuid(child_id),
            ],
        )
        .unwrap()
        .row_id;
    server.process(&mut server_io);

    let sub_id = client
        .subscribe_with_sync(
            client.query("recursive_folders").build(),
            Some(PolicySession::new("alice")),
            None,
        )
        .unwrap();
    pump_messages(
        &mut client,
        &mut server,
        &mut client_io,
        &mut server_io,
        client_id,
        server_id,
    );
    assert_eq!(client.get_subscription_results(sub_id).len(), 1);

    server
        .update(
            &mut server_io,
            child_id,
            &[
                Value::Text("bob".into()),
                Value::Text("Child".into()),
                Value::Uuid(root_id),
            ],
        )
        .unwrap();
    server.process(&mut server_io);
    pump_messages(
        &mut client,
        &mut server,
        &mut client_io,
        &mut server_io,
        client_id,
        server_id,
    );
    assert_eq!(client.get_subscription_results(sub_id).len(), 3);

    server
        .update(
            &mut server_io,
            child_id,
            &[
                Value::Text("bob".into()),
                Value::Text("Child".into()),
                Value::Null,
            ],
        )
        .unwrap();
    server.process(&mut server_io);
    pump_messages(
        &mut client,
        &mut server,
        &mut client_io,
        &mut server_io,
        client_id,
        server_id,
    );

    let remote_scope = client
        .sync_manager()
        .remote_query_scope(crate::sync_manager::QueryId(sub_id.0));
    assert_eq!(
        remote_scope,
        [(root_id, crate::object::BranchName::new(get_branch(&client)))]
            .into_iter()
            .collect()
    );

    let subscription = client
        .subscriptions
        .get(&sub_id)
        .expect("client subscription");
    assert_eq!(subscription.current_ordered_ids, vec![root_id]);
    assert_eq!(subscription.current_visible_rows.len(), 1);
    assert_eq!(
        decode_row(
            &subscription.graph.combined_descriptor,
            &subscription.current_visible_rows[&root_id].data
        )
        .unwrap(),
        vec![
            Value::Text("alice".into()),
            Value::Text("Root".into()),
            Value::Null
        ]
    );
}
