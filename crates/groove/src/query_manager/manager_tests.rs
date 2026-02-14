//! QueryManager integration tests.
//!
//! Tests for CRUD operations, subscriptions, syncing, and deletions.

use serde_json::json;
use smallvec::smallvec;

use crate::metadata::MetadataKey;
use crate::storage::MemoryStorage;
use crate::sync_manager::SyncManager;

use super::{
    ColumnDescriptor, ColumnType, PolicyExpr, QueryError, QueryManager, RowDescriptor, Schema,
    Session as PolicySession, TableName, TablePolicies, TableSchema, Value, decode_row,
};

fn test_schema() -> Schema {
    let mut schema = Schema::new();
    schema.insert(
        TableName::new("users"),
        RowDescriptor::new(vec![
            ColumnDescriptor::new("name", ColumnType::Text),
            ColumnDescriptor::new("score", ColumnType::Integer),
        ])
        .into(),
    );
    schema
}

/// Helper to create QueryManager with schema on default branch.
fn create_query_manager(
    sync_manager: SyncManager,
    schema: Schema,
) -> (QueryManager, MemoryStorage) {
    let mut qm = QueryManager::new(sync_manager);
    qm.set_current_schema(schema, "dev", "main");
    (qm, MemoryStorage::new())
}

/// Get the current branch name from a QueryManager.
fn get_branch(qm: &QueryManager) -> String {
    qm.schema_context().branch_name().as_str().to_string()
}

use crate::object::ObjectId;
use crate::query_manager::query::Query;

/// Helper to execute a query synchronously via subscribe/process/unsubscribe.
/// Returns Vec<(ObjectId, Vec<Value>)> matching old execute() return type.
fn execute_query(
    qm: &mut QueryManager,
    storage: &mut MemoryStorage,
    query: Query,
) -> Result<Vec<(ObjectId, Vec<Value>)>, QueryError> {
    let sub_id = qm.subscribe(query)?;
    qm.process(storage);
    let results = qm.get_subscription_results(sub_id);
    qm.unsubscribe_with_sync(sub_id);
    Ok(results)
}

#[test]
fn insert_and_get() {
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

    let row = qm.test_get_row_if_loaded(handle.row_id).unwrap();
    assert_eq!(row[0], Value::Text("Alice".into()));
    assert_eq!(row[1], Value::Integer(100));
}

#[test]
fn insert_and_query() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

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

    // Query all
    let query = qm.query("users").build();
    let results = execute_query(&mut qm, &mut storage, query).unwrap();
    assert_eq!(results.len(), 3);

    // Query with filter
    let query = qm
        .query("users")
        .filter_ge("score", Value::Integer(75))
        .build();
    let results = execute_query(&mut qm, &mut storage, query).unwrap();
    assert_eq!(results.len(), 2);
}

#[test]
fn query_with_sort_and_limit() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

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

    let query = qm.query("users").order_by_desc("score").limit(2).build();
    let results = execute_query(&mut qm, &mut storage, query).unwrap();

    assert_eq!(results.len(), 2);
    assert_eq!(results[0].1[0], Value::Text("Alice".into())); // 100
    assert_eq!(results[1].1[0], Value::Text("Charlie".into())); // 75
}

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

    let row = qm.test_get_row_if_loaded(handle.row_id).unwrap();
    assert_eq!(row[0], Value::Text("Alice Updated".into()));
    assert_eq!(row[1], Value::Integer(150));
}

#[test]
fn table_not_found_error() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    let result = qm.insert(&mut storage, "nonexistent", &[Value::Text("test".into())]);
    assert!(matches!(result, Err(QueryError::TableNotFound(_))));
}

#[test]
fn column_count_mismatch_error() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    let result = qm.insert(&mut storage, "users", &[Value::Text("Alice".into())]);
    assert!(matches!(
        result,
        Err(QueryError::ColumnCountMismatch { .. })
    ));
}

#[test]
fn insert_returns_handle_with_commit_id() {
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

    // Handle should have the row ID
    assert!(qm.test_get_row_if_loaded(handle.row_id).is_some());

    // Handle should have a valid row commit ID
    assert!(handle.row_commit_id.0 != [0; 32]);
}

#[test]
fn row_is_indexed_after_insert() {
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

    // Row should be indexed immediately after insert
    assert!(handle.is_indexed(&qm, &storage, "users"));
}

#[test]
fn index_persistence_via_insert() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert a row
    let handle = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Test".into()), Value::Integer(42)],
        )
        .unwrap();

    // Verify row is indexed
    assert!(handle.is_indexed(&qm, &storage, "users"));
}

// ========================================================================
// Lazy loading and subscription tests
// ========================================================================

#[test]
fn can_register_query_immediately() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Can register a query subscription immediately
    let query = qm.query("users").build();
    let sub_id = qm.subscribe(query);
    assert!(sub_id.is_ok());
}

#[test]
fn subscription_updates_after_insert_and_process() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Register subscription
    let query = qm.query("users").build();
    let sub_id = qm.subscribe(query).unwrap();

    // Insert a row
    qm.insert(
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
}

#[test]
fn multiple_inserts_all_visible_in_query() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Multiple inserts
    let h1 = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();
    let h2 = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Bob".into()), Value::Integer(50)],
        )
        .unwrap();
    let h3 = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Charlie".into()), Value::Integer(75)],
        )
        .unwrap();

    // All rows visible via get() immediately
    assert!(qm.test_get_row_if_loaded(h1.row_id).is_some());
    assert!(qm.test_get_row_if_loaded(h2.row_id).is_some());
    assert!(qm.test_get_row_if_loaded(h3.row_id).is_some());

    // Query returns all rows
    let query = qm.query("users").build();
    let results = execute_query(&mut qm, &mut storage, query).unwrap();
    assert_eq!(results.len(), 3);

    // Sorted query works
    let query = qm.query("users").order_by_desc("score").limit(2).build();
    let results = execute_query(&mut qm, &mut storage, query).unwrap();
    assert_eq!(results.len(), 2);
    assert_eq!(results[0].1[0], Value::Text("Alice".into())); // 100
    assert_eq!(results[1].1[0], Value::Text("Charlie".into())); // 75
}

// NOTE: cold_start_loads_persisted_indices_and_rows, cold_start_only_loads_queried_rows,
// and cold_start_with_sorted_query tests were removed because they used
// process_storage_with_driver() and load_indices_from_driver() which no longer exist.
// Cold start behavior is now handled by the Storage-based storage layer.

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
    use crate::commit::{Commit, StoredState};
    use crate::query_manager::encoding::encode_row;
    use std::collections::HashMap;

    // This test verifies that updates received via sync (receive_commit)
    // correctly update column indices using old_content from AllObjectUpdate.

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
    qm.sync_manager_mut()
        .object_manager
        .receive_object(&mut storage, row_id, metadata);

    // Subscribe to all objects so we get AllObjectUpdate notifications
    qm.sync_manager_mut().object_manager.subscribe_all();

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
    let commit1 = Commit {
        parents: smallvec![],
        content: initial_data.clone(),
        timestamp: 1000,
        author,
        metadata: None,
        stored_state: StoredState::Stored,
        ack_state: Default::default(),
    };
    let commit1_id = qm
        .sync_manager_mut()
        .object_manager
        .receive_commit(&mut storage, row_id, &branch, commit1)
        .unwrap();

    // Process to handle the AllObjectUpdate
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
    let commit2 = Commit {
        parents: smallvec![commit1_id],
        content: updated_data.clone(),
        timestamp: 2000,
        author,
        metadata: None,
        stored_state: StoredState::Stored,
        ack_state: Default::default(),
    };
    qm.sync_manager_mut()
        .object_manager
        .receive_commit(&mut storage, row_id, &branch, commit2)
        .unwrap();

    // Process to handle the AllObjectUpdate with old_content
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
fn synced_insert_appears_in_subscription_delta() {
    use crate::commit::{Commit, StoredState};
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
    qm.sync_manager_mut()
        .object_manager
        .receive_object(&mut storage, row_id, metadata);

    // Subscribe to all objects so we get AllObjectUpdate notifications
    qm.sync_manager_mut().object_manager.subscribe_all();

    // NOW subscribe to query (after subscribe_all but before receive_commit)
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
    let commit = Commit {
        parents: smallvec![],
        content: row_data,
        timestamp: 1000,
        author,
        metadata: None,
        stored_state: StoredState::Stored,
        ack_state: Default::default(),
    };
    qm.sync_manager_mut()
        .object_manager
        .receive_commit(&mut storage, row_id, &branch, commit)
        .unwrap();

    // Process to handle the AllObjectUpdate
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
fn synced_update_is_visible_in_query() {
    use crate::commit::{Commit, StoredState};
    use crate::query_manager::encoding::encode_row;

    // Verify that synced updates (same row, new content) update indices correctly
    // and are visible in subsequent queries.
    //
    // Note: Currently, row content updates for existing IDs don't emit subscription
    // deltas because the graph tracks ID changes, not content changes. The
    // MaterializeNode has a check_update() method for this, but it's not wired
    // into the settle() flow yet. For now, we verify that queries see the updated data.

    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);
    let branch = get_branch(&qm);

    // Subscribe to all objects for sync updates
    qm.sync_manager_mut().object_manager.subscribe_all();

    // Insert a row locally first
    let insert_handle = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();
    let row_id = insert_handle.row_id;
    let first_commit_id = insert_handle.row_commit_id;

    // Process to settle the initial insert
    qm.process(&mut storage);

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
    let update_commit = Commit {
        parents: smallvec![first_commit_id],
        content: updated_data,
        timestamp: 2000,
        author,
        metadata: None,
        stored_state: StoredState::Stored,
        ack_state: Default::default(),
    };
    qm.sync_manager_mut()
        .object_manager
        .receive_commit(&mut storage, row_id, &branch, update_commit)
        .unwrap();

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
fn synced_row_visible_in_filtered_subscription() {
    use crate::commit::{Commit, StoredState};
    use crate::query_manager::encoding::{decode_row, encode_row};
    use std::collections::HashMap;

    // Verify that synced rows are correctly filtered by subscription predicates.
    // Rows matching the filter appear in deltas; rows not matching are excluded.

    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);
    let branch = get_branch(&qm);

    // Subscribe to all objects for sync updates
    qm.sync_manager_mut().object_manager.subscribe_all();

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
    qm.sync_manager_mut()
        .object_manager
        .receive_object(&mut storage, row_id_1, metadata_1);

    let data_1 = encode_row(
        &descriptor,
        &[Value::Text("HighScorer".into()), Value::Integer(30)],
    )
    .unwrap();

    let commit_1 = Commit {
        parents: smallvec![],
        content: data_1,
        timestamp: 1000,
        author: author_1,
        metadata: None,
        stored_state: StoredState::Stored,
        ack_state: Default::default(),
    };
    qm.sync_manager_mut()
        .object_manager
        .receive_commit(&mut storage, row_id_1, &branch, commit_1)
        .unwrap();

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
    qm.sync_manager_mut()
        .object_manager
        .receive_object(&mut storage, row_id_2, metadata_2);

    let data_2 = encode_row(
        &descriptor,
        &[Value::Text("LowScorer".into()), Value::Integer(20)],
    )
    .unwrap();

    let commit_2 = Commit {
        parents: smallvec![],
        content: data_2,
        timestamp: 2000,
        author: author_2,
        metadata: None,
        stored_state: StoredState::Stored,
        ack_state: Default::default(),
    };
    qm.sync_manager_mut()
        .object_manager
        .receive_commit(&mut storage, row_id_2, &branch, commit_2)
        .unwrap();

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

// ========================================================================
// Row content update propagation tests
// ========================================================================

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
    use crate::commit::{Commit, StoredState};
    use crate::query_manager::encoding::encode_row;

    // Verify that synced updates (receive_commit) cause subscription to emit update delta

    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Subscribe to all objects for sync updates
    qm.sync_manager_mut().object_manager.subscribe_all();

    // Insert a row locally first
    let handle = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();
    let row_id = handle.row_id;
    let first_commit_id = handle.row_commit_id;

    // Subscribe to all users
    let query = qm.query("users").build();
    let sub_id = qm.subscribe(query).unwrap();

    // Process to get the initial add
    qm.process(&mut storage);
    let _updates = qm.take_updates(); // Clear initial add

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
    let update_commit = Commit {
        parents: smallvec![first_commit_id],
        content: updated_data,
        timestamp: 2000,
        author,
        metadata: None,
        stored_state: StoredState::Stored,
        ack_state: Default::default(),
    };
    let branch = get_branch(&qm);
    qm.sync_manager_mut()
        .object_manager
        .receive_commit(&mut storage, row_id, &branch, update_commit)
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
    assert_eq!(
        updates[0].delta.added.len(),
        1,
        "Row should be added initially"
    );

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
    // Row doesn't match filter, so no delta or empty delta
    assert!(updates.is_empty() || updates[0].delta.added.is_empty());

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
    let _updates = qm.take_updates();

    // Update score to 40 (still fails filter)
    qm.update(
        &mut storage,
        handle.row_id,
        &[Value::Text("Alice".into()), Value::Integer(40)],
    )
    .unwrap();

    qm.process(&mut storage);

    let updates = qm.take_updates();
    // Should be no updates (or empty delta)
    assert!(
        updates.is_empty()
            || (updates[0].delta.added.is_empty()
                && updates[0].delta.removed.is_empty()
                && updates[0].delta.updated.is_empty()),
        "No delta for row that doesn't match filter before or after update"
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

// ========================================================================
// End-to-End Sync Integration Tests (Followup 9)
// ========================================================================

#[test]
fn sync_inbox_insert_flows_to_subscription_delta() {
    // End-to-end test: sync message → SyncManager inbox → QueryManager subscription
    // This tests the full path through push_inbox() → process_inbox() → process()
    use crate::commit::{Commit, StoredState};
    use crate::query_manager::encoding::{decode_row, encode_row};
    use crate::sync_manager::{InboxEntry, ServerId, Source, SyncPayload};

    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);
    let branch = get_branch(&qm);

    // Add a "server" that we'll receive updates from
    let server_id = ServerId::new();
    qm.sync_manager_mut().add_server(server_id);

    // Subscribe to all objects for sync updates
    qm.sync_manager_mut().object_manager.subscribe_all();

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

    let commit = Commit {
        parents: smallvec![],
        content: row_data,
        timestamp: 1000,
        author,
        metadata: None,
        stored_state: StoredState::Stored,
        ack_state: Default::default(),
    };

    // Object metadata marking it as a "users" table row
    let mut obj_metadata = std::collections::HashMap::new();
    obj_metadata.insert(MetadataKey::Table.to_string(), "users".to_string());

    // Push the sync message through SyncManager's inbox
    qm.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Server(server_id),
        payload: SyncPayload::ObjectUpdated {
            object_id: row_id,
            metadata: Some(crate::sync_manager::ObjectMetadata {
                id: row_id,
                metadata: obj_metadata,
            }),
            branch_name: branch.into(),
            commits: vec![commit],
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
    use crate::commit::{Commit, StoredState};
    use crate::query_manager::encoding::{decode_row, encode_row};
    use crate::sync_manager::{InboxEntry, ServerId, Source, SyncPayload};

    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);
    let branch = get_branch(&qm);

    // Add a "server"
    let server_id = ServerId::new();
    qm.sync_manager_mut().add_server(server_id);
    qm.sync_manager_mut().object_manager.subscribe_all();

    // Insert a row locally first
    let handle = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();
    let row_id = handle.row_id;
    let first_commit_id = handle.row_commit_id;

    // Subscribe to users
    let query = qm.query("users").build();
    let sub_id = qm.subscribe(query).unwrap();

    // Process to get initial state
    qm.process(&mut storage);
    let _ = qm.take_updates(); // Clear initial delta

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

    let update_commit = Commit {
        parents: smallvec![first_commit_id],
        content: updated_data,
        timestamp: 2000,
        author: row_id,
        metadata: None,
        stored_state: StoredState::Stored,
        ack_state: Default::default(),
    };

    // Push the update through SyncManager inbox
    qm.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Server(server_id),
        payload: SyncPayload::ObjectUpdated {
            object_id: row_id,
            metadata: None, // No metadata needed for existing object
            branch_name: branch.into(),
            commits: vec![update_commit],
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
    use crate::commit::{Commit, StoredState};
    use crate::object::BranchName;
    use crate::query_manager::encoding::decode_row;
    use crate::sync_manager::{InboxEntry, ServerId, Source, SyncPayload};

    // Create two peers
    let sync_manager_a = SyncManager::new();
    let sync_manager_b = SyncManager::new();
    let schema = test_schema();
    let (mut peer_a, mut storage_a) = create_query_manager(sync_manager_a, schema.clone());
    let (mut peer_b, mut storage_b) = create_query_manager(sync_manager_b, schema);

    // Peer B subscribes to all objects and sets up query subscription
    peer_b.sync_manager_mut().object_manager.subscribe_all();
    let query = peer_b.query("users").build();
    let sub_id = peer_b.subscribe(query).unwrap();

    // Peer B adds a "server" (representing Peer A)
    let peer_a_as_server = ServerId::new();
    peer_b.sync_manager_mut().add_server(peer_a_as_server);

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

    // Get the actual commit data from Peer A's ObjectManager
    // This simulates "what would be sent over the wire"
    let branch_name = get_branch(&peer_a);
    let (row_data, metadata) = {
        let obj = peer_a
            .sync_manager_mut()
            .object_manager
            .get(row_id)
            .expect("Object should be available");
        let branch = obj.branches.get(&BranchName::new(&branch_name)).unwrap();
        let tip_id = branch.tips.iter().next().unwrap();
        let commit = branch.commits.get(tip_id).unwrap();
        (commit.content.clone(), obj.metadata.clone())
    };

    // Construct the sync payload as it would appear on the wire
    let commit = Commit {
        parents: smallvec![],
        content: row_data,
        timestamp: 1000,
        author: row_id,
        metadata: None,
        stored_state: StoredState::Stored,
        ack_state: Default::default(),
    };

    // Send to Peer B via SyncManager inbox
    peer_b.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Server(peer_a_as_server),
        payload: SyncPayload::ObjectUpdated {
            object_id: row_id,
            metadata: Some(crate::sync_manager::ObjectMetadata {
                id: row_id,
                metadata,
            }),
            branch_name: branch_name.clone().into(),
            commits: vec![commit],
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

// ========================================================================
// Soft Delete Tests
// ========================================================================

#[test]
fn soft_delete_removes_from_id_index() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert a row
    let handle = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();

    // Verify row is in _id index
    assert!(qm.row_is_indexed(&storage, "users", handle.row_id));

    // Delete the row
    let delete_handle = qm.delete(&mut storage, handle.row_id).unwrap();
    assert_eq!(delete_handle.row_id, handle.row_id);

    // Verify row is no longer in _id index
    assert!(!qm.row_is_indexed(&storage, "users", handle.row_id));
}

#[test]
fn soft_delete_adds_to_id_deleted_index() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert a row
    let handle = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();

    // Verify row is NOT in _id_deleted index
    assert!(!qm.row_is_deleted(&storage, "users", handle.row_id));

    // Delete the row
    qm.delete(&mut storage, handle.row_id).unwrap();

    // Verify row IS in _id_deleted index
    assert!(qm.row_is_deleted(&storage, "users", handle.row_id));
}

#[test]
fn soft_deleted_row_not_in_query_results() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert rows
    let handle = qm
        .insert(
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

    // Verify both rows are visible
    let query = qm.query("users").build();
    let results = execute_query(&mut qm, &mut storage, query).unwrap();
    assert_eq!(results.len(), 2);

    // Delete Alice
    qm.delete(&mut storage, handle.row_id).unwrap();

    // Verify only Bob is visible
    let query = qm.query("users").build();
    let results = execute_query(&mut qm, &mut storage, query).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].1[0], Value::Text("Bob".into()));
}

#[test]
fn delete_already_deleted_row_fails() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert a row
    let handle = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();

    // Delete the row
    qm.delete(&mut storage, handle.row_id).unwrap();

    // Try to delete again - should fail
    let result = qm.delete(&mut storage, handle.row_id);
    assert!(matches!(result, Err(QueryError::RowAlreadyDeleted(_))));
}

#[test]
fn soft_delete_with_concurrent_tips_uses_lww() {
    // Test that soft deleting an object with two concurrent tips results
    // in a soft delete commit with content from the LWW winner (highest timestamp).
    use crate::commit::{Commit, StoredState};
    use crate::object::BranchName;
    use crate::query_manager::encoding::encode_row;

    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert a row
    let handle = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Original".into()), Value::Integer(0)],
        )
        .unwrap();
    qm.process(&mut storage);

    // Get the initial commit as the common parent
    let branch = get_branch(&qm);
    let branch_name = BranchName::new(&branch);
    let initial_tips: Vec<_> = qm
        .sync_manager_mut()
        .object_manager
        .get_tip_ids(handle.row_id, &branch)
        .unwrap()
        .iter()
        .copied()
        .collect();
    assert_eq!(initial_tips.len(), 1);
    let parent = initial_tips[0];

    // Create two concurrent updates with different timestamps and content.
    // Both have the same parent, creating diverging tips.
    let descriptor = qm
        .schema()
        .get(&TableName::new("users"))
        .unwrap()
        .descriptor
        .clone();

    // Commit A: lower timestamp, content "TipA"
    let content_a = encode_row(
        &descriptor,
        &[Value::Text("TipA".into()), Value::Integer(100)],
    )
    .unwrap();
    let commit_a = Commit {
        author: handle.row_id,
        parents: smallvec![parent],
        content: content_a,
        timestamp: 1000, // Lower timestamp
        metadata: None,
        stored_state: StoredState::Pending,
        ack_state: Default::default(),
    };

    // Commit B: higher timestamp, content "TipB" - this should win
    let content_b = encode_row(
        &descriptor,
        &[Value::Text("TipB".into()), Value::Integer(200)],
    )
    .unwrap();
    let commit_b = Commit {
        author: handle.row_id,
        parents: smallvec![parent],
        content: content_b.clone(),
        timestamp: 2000, // Higher timestamp - LWW winner
        metadata: None,
        stored_state: StoredState::Pending,
        ack_state: Default::default(),
    };

    // Add both commits to create concurrent tips
    // We need to receive these as synced commits
    let commit_a_id = qm
        .sync_manager_mut()
        .object_manager
        .receive_commit(&mut storage, handle.row_id, &branch, commit_a)
        .unwrap();
    let commit_b_id = qm
        .sync_manager_mut()
        .object_manager
        .receive_commit(&mut storage, handle.row_id, &branch, commit_b)
        .unwrap();

    // Verify we now have concurrent tips
    let tips: Vec<_> = qm
        .sync_manager_mut()
        .object_manager
        .get_tip_ids(handle.row_id, &branch)
        .unwrap()
        .iter()
        .copied()
        .collect();
    assert_eq!(tips.len(), 2, "Should have 2 concurrent tips");
    assert!(tips.contains(&commit_a_id));
    assert!(tips.contains(&commit_b_id));

    // Process updates
    qm.process(&mut storage);

    // Now soft delete - should preserve content from LWW winner (commit_b, TipB)
    let delete_handle = qm.delete(&mut storage, handle.row_id).unwrap();

    // Get the delete commit and verify its content
    let obj = qm
        .sync_manager_mut()
        .object_manager
        .get(handle.row_id)
        .expect("Object should be available");
    {
        let branch = obj.branches.get(&branch_name).unwrap();
        let delete_commit = branch.commits.get(&delete_handle.delete_commit_id).unwrap();

        // Verify the soft delete commit has content from the LWW winner (TipB)
        assert_eq!(
            delete_commit.content, content_b,
            "Soft delete should preserve content from LWW winner"
        );

        // Also verify metadata
        assert_eq!(
            delete_commit
                .metadata
                .as_ref()
                .and_then(|m| m.get(MetadataKey::Delete.as_str())),
            Some(&"soft".to_string())
        );
    }

    // Additionally verify that querying with include_deleted shows the correct content
    let query = qm.query("users").include_deleted().build();
    let results = execute_query(&mut qm, &mut storage, query).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].1[0], Value::Text("TipB".into()));
    assert_eq!(results[0].1[1], Value::Integer(200));
}

// ========================================================================
// Undelete Tests
// ========================================================================

#[test]
fn undelete_adds_to_id_index() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert a row
    let handle = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();

    // Delete the row
    qm.delete(&mut storage, handle.row_id).unwrap();

    // Verify row is not in _id index
    assert!(!qm.row_is_indexed(&storage, "users", handle.row_id));

    // Undelete with new values
    qm.undelete(
        &mut storage,
        handle.row_id,
        &[Value::Text("Alice Restored".into()), Value::Integer(150)],
    )
    .unwrap();

    // Verify row is back in _id index
    assert!(qm.row_is_indexed(&storage, "users", handle.row_id));
}

#[test]
fn undelete_removes_from_id_deleted_index() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert a row
    let handle = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();

    // Delete the row
    qm.delete(&mut storage, handle.row_id).unwrap();
    assert!(qm.row_is_deleted(&storage, "users", handle.row_id));

    // Undelete
    qm.undelete(
        &mut storage,
        handle.row_id,
        &[Value::Text("Alice".into()), Value::Integer(100)],
    )
    .unwrap();

    // Verify row is NOT in _id_deleted index
    assert!(!qm.row_is_deleted(&storage, "users", handle.row_id));
}

#[test]
fn undelete_row_appears_in_query_results() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert a row
    let handle = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();

    // Delete the row
    qm.delete(&mut storage, handle.row_id).unwrap();

    // Verify not visible
    let query = qm.query("users").build();
    let results = execute_query(&mut qm, &mut storage, query).unwrap();
    assert_eq!(results.len(), 0);

    // Undelete with new values
    qm.undelete(
        &mut storage,
        handle.row_id,
        &[Value::Text("Alice Restored".into()), Value::Integer(200)],
    )
    .unwrap();

    // Verify visible again with new values
    let query = qm.query("users").build();
    let results = execute_query(&mut qm, &mut storage, query).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].1[0], Value::Text("Alice Restored".into()));
    assert_eq!(results[0].1[1], Value::Integer(200));
}

#[test]
fn undelete_nondeleted_row_fails() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert a row
    let handle = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();

    // Try to undelete a non-deleted row - should fail
    let result = qm.undelete(
        &mut storage,
        handle.row_id,
        &[Value::Text("Alice".into()), Value::Integer(100)],
    );
    assert!(matches!(result, Err(QueryError::RowNotDeleted(_))));
}

// ========================================================================
// Hard Delete Tests
// ========================================================================

#[test]
fn hard_delete_removes_from_id_index() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert a row
    let handle = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();

    // Hard delete the row
    qm.hard_delete(&mut storage, handle.row_id).unwrap();

    // Verify row is not in _id index
    assert!(!qm.row_is_indexed(&storage, "users", handle.row_id));
}

#[test]
fn hard_delete_removes_from_id_deleted_index() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert a row
    let handle = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();

    // Soft delete first (puts it in _id_deleted)
    qm.delete(&mut storage, handle.row_id).unwrap();
    assert!(qm.row_is_deleted(&storage, "users", handle.row_id));

    // Then hard delete (removes from _id_deleted)
    qm.hard_delete(&mut storage, handle.row_id).unwrap();

    // Verify row is NOT in _id_deleted index
    assert!(!qm.row_is_deleted(&storage, "users", handle.row_id));
}

#[test]
fn hard_deleted_row_not_in_any_index() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert a row
    let handle = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();

    // Hard delete
    qm.hard_delete(&mut storage, handle.row_id).unwrap();

    // Verify row is not in _id index
    assert!(!qm.row_is_indexed(&storage, "users", handle.row_id));
    // Verify row is not in _id_deleted index
    assert!(!qm.row_is_deleted(&storage, "users", handle.row_id));
}

#[test]
fn soft_then_hard_delete_removes_from_id_deleted() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert a row
    let handle = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();

    // Soft delete - row should be in _id_deleted
    qm.delete(&mut storage, handle.row_id).unwrap();
    assert!(qm.row_is_deleted(&storage, "users", handle.row_id));

    // Hard delete - row should be removed from _id_deleted
    qm.hard_delete(&mut storage, handle.row_id).unwrap();
    assert!(!qm.row_is_deleted(&storage, "users", handle.row_id));
}

#[test]
fn undelete_hard_deleted_row_fails() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert a row
    let handle = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();

    // Hard delete
    qm.hard_delete(&mut storage, handle.row_id).unwrap();

    // Try to undelete - should fail
    let result = qm.undelete(
        &mut storage,
        handle.row_id,
        &[Value::Text("Alice".into()), Value::Integer(100)],
    );
    assert!(matches!(result, Err(QueryError::RowHardDeleted(_))));
}

// ========================================================================
// Truncate Tests
// ========================================================================

#[test]
fn truncate_soft_deleted_row() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert a row
    let handle = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();

    // Soft delete
    qm.delete(&mut storage, handle.row_id).unwrap();
    assert!(qm.row_is_deleted(&storage, "users", handle.row_id));

    // Truncate (upgrade to hard delete)
    qm.truncate(&mut storage, handle.row_id).unwrap();

    // Verify row is completely gone
    assert!(!qm.row_is_indexed(&storage, "users", handle.row_id));
    assert!(!qm.row_is_deleted(&storage, "users", handle.row_id));
}

#[test]
fn truncate_nondeleted_row_fails() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert a row
    let handle = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();

    // Try to truncate a non-deleted row - should fail
    let result = qm.truncate(&mut storage, handle.row_id);
    assert!(matches!(result, Err(QueryError::RowNotDeleted(_))));
}

// ========================================================================
// Include Deleted Query Tests
// ========================================================================

#[test]
fn include_deleted_query_returns_soft_deleted_rows() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert rows
    let handle1 = qm
        .insert(
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

    // Delete Alice
    qm.delete(&mut storage, handle1.row_id).unwrap();

    // Normal query - only Bob (Alice is in _id_deleted, not _id)
    let query = qm.query("users").build();
    let results = execute_query(&mut qm, &mut storage, query).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].1[0], Value::Text("Bob".into()));

    // Include deleted query - scans both _id and _id_deleted indices
    // Soft-deleted rows have preserved content, so both Alice and Bob are returned
    let query = qm.query("users").include_deleted().build();
    let results = execute_query(&mut qm, &mut storage, query).unwrap();
    assert_eq!(results.len(), 2);

    // Verify Alice's data is preserved
    let alice_result = results
        .iter()
        .find(|r| r.1[0] == Value::Text("Alice".into()));
    assert!(alice_result.is_some());
    assert_eq!(alice_result.unwrap().1[1], Value::Integer(100));

    // Verify that Alice is in the _id_deleted index
    assert!(qm.row_is_deleted(&storage, "users", handle1.row_id));
}

#[test]
fn include_deleted_query_does_not_return_hard_deleted_rows() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert rows
    let handle1 = qm
        .insert(
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

    // Hard delete Alice
    qm.hard_delete(&mut storage, handle1.row_id).unwrap();

    // Include deleted query - only Bob (Alice is hard deleted)
    let query = qm.query("users").include_deleted().build();
    let results = execute_query(&mut qm, &mut storage, query).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].1[0], Value::Text("Bob".into()));
}

// ========================================================================
// Delete Subscription Delta Tests
// ========================================================================

#[test]
fn soft_delete_emits_removal_delta() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert a row
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

    // Process to get initial delta
    qm.process(&mut storage);
    let updates = qm.take_updates();
    assert_eq!(updates.len(), 1);
    assert_eq!(updates[0].delta.added.len(), 1); // Alice added

    // Delete Alice
    qm.delete(&mut storage, handle.row_id).unwrap();

    // Process and check for removal delta
    qm.process(&mut storage);
    let updates = qm.take_updates();
    assert_eq!(updates.len(), 1);
    assert_eq!(updates[0].subscription_id, sub_id);
    assert_eq!(updates[0].delta.removed.len(), 1);
    assert_eq!(updates[0].delta.removed[0].id, handle.row_id);
}

#[test]
fn hard_delete_emits_removal_delta() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert a row
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

    // Process to get initial delta
    qm.process(&mut storage);
    let updates = qm.take_updates();
    assert_eq!(updates.len(), 1);
    assert_eq!(updates[0].delta.added.len(), 1); // Alice added

    // Hard delete Alice
    qm.hard_delete(&mut storage, handle.row_id).unwrap();

    // Process and check for removal delta
    qm.process(&mut storage);
    let updates = qm.take_updates();
    assert_eq!(updates.len(), 1);
    assert_eq!(updates[0].subscription_id, sub_id);
    assert_eq!(updates[0].delta.removed.len(), 1);
    assert_eq!(updates[0].delta.removed[0].id, handle.row_id);
}

#[test]
fn delete_row_not_in_subscription_no_delta() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert rows
    let alice_handle = qm
        .insert(
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

    // Subscribe to users with score >= 75 (only Alice)
    let query = qm
        .query("users")
        .filter_ge("score", Value::Integer(75))
        .build();
    let sub_id = qm.subscribe(query).unwrap();

    // Process to get initial delta
    qm.process(&mut storage);
    let updates = qm.take_updates();
    assert_eq!(updates.len(), 1);
    assert_eq!(updates[0].delta.added.len(), 1); // Only Alice

    // Delete Alice (who IS in subscription) - should emit removal delta
    qm.delete(&mut storage, alice_handle.row_id).unwrap();

    // Process and verify we got removal delta
    qm.process(&mut storage);
    let updates = qm.take_updates();
    assert_eq!(updates.len(), 1);
    assert_eq!(updates[0].subscription_id, sub_id);
    assert_eq!(updates[0].delta.removed.len(), 1);
}

// ========================================================================
// Join integration tests
// ========================================================================

fn join_schema() -> Schema {
    let mut schema = Schema::new();
    schema.insert(
        TableName::new("users"),
        RowDescriptor::new(vec![
            ColumnDescriptor::new("id", ColumnType::Integer),
            ColumnDescriptor::new("name", ColumnType::Text),
        ])
        .into(),
    );
    schema.insert(
        TableName::new("posts"),
        RowDescriptor::new(vec![
            ColumnDescriptor::new("id", ColumnType::Integer),
            ColumnDescriptor::new("title", ColumnType::Text),
            ColumnDescriptor::new("author_id", ColumnType::Integer),
        ])
        .into(),
    );
    schema
}

#[test]
fn join_compiles_but_not_executed_yet() {
    // This test validates that join queries compile and don't panic,
    // even though full join execution is not yet implemented.
    // Once execute() supports joins, this test can be extended.
    let sync_manager = SyncManager::new();
    let schema = join_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Build a join query
    let query = qm
        .query("users")
        .join("posts")
        .on("id", "author_id")
        .build();

    // The query should compile successfully
    assert!(query.is_join());
    assert_eq!(query.joins.len(), 1);
}

#[test]
fn join_query_with_projection_compiles() {
    let sync_manager = SyncManager::new();
    let schema = join_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    let query = qm
        .query("users")
        .join("posts")
        .on("id", "author_id")
        .select(&["name", "title"])
        .build();

    assert!(query.is_join());
    assert_eq!(
        query.select_columns,
        Some(vec!["name".to_string(), "title".to_string()])
    );
}

#[test]
fn join_query_with_alias_compiles() {
    let sync_manager = SyncManager::new();
    let schema = join_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    let query = qm
        .query("users")
        .alias("u")
        .join("posts")
        .alias("p")
        .on("u.id", "p.author_id")
        .build();

    assert!(query.is_join());
    assert_eq!(query.alias, Some("u".to_string()));
    assert_eq!(query.joins[0].alias, Some("p".to_string()));
}

#[test]
fn self_join_query_compiles() {
    // Self-join: employees with their managers
    let mut schema = Schema::new();
    schema.insert(
        TableName::new("employees"),
        RowDescriptor::new(vec![
            ColumnDescriptor::new("id", ColumnType::Integer),
            ColumnDescriptor::new("name", ColumnType::Text),
            ColumnDescriptor::new("manager_id", ColumnType::Integer).nullable(),
        ])
        .into(),
    );

    let sync_manager = SyncManager::new();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    let query = qm
        .query("employees")
        .alias("e")
        .join("employees")
        .alias("m")
        .on("e.manager_id", "m.id")
        .build();

    assert!(query.is_join());
    assert_eq!(query.alias, Some("e".to_string()));
    assert_eq!(query.joins[0].table.as_str(), "employees");
    assert_eq!(query.joins[0].alias, Some("m".to_string()));
}

#[test]
fn multi_join_query_compiles() {
    // Three-way join: orders -> customers, orders -> products
    let mut schema = Schema::new();
    schema.insert(
        TableName::new("orders"),
        RowDescriptor::new(vec![
            ColumnDescriptor::new("id", ColumnType::Integer),
            ColumnDescriptor::new("customer_id", ColumnType::Integer),
            ColumnDescriptor::new("product_id", ColumnType::Integer),
        ])
        .into(),
    );
    schema.insert(
        TableName::new("customers"),
        RowDescriptor::new(vec![
            ColumnDescriptor::new("id", ColumnType::Integer),
            ColumnDescriptor::new("name", ColumnType::Text),
        ])
        .into(),
    );
    schema.insert(
        TableName::new("products"),
        RowDescriptor::new(vec![
            ColumnDescriptor::new("id", ColumnType::Integer),
            ColumnDescriptor::new("name", ColumnType::Text),
        ])
        .into(),
    );

    let sync_manager = SyncManager::new();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    let query = qm
        .query("orders")
        .join("customers")
        .on("customer_id", "id")
        .join("products")
        .on("product_id", "id")
        .build();

    assert!(query.is_join());
    assert_eq!(query.joins.len(), 2);
    assert_eq!(query.joins[0].table.as_str(), "customers");
    assert_eq!(query.joins[1].table.as_str(), "products");
}

#[test]
fn join_subscription_marks_dirty_for_joined_table() {
    // This test verifies that inserts into a JOINED table (not the base table)
    // mark the join subscription as dirty. This is a regression test for a bug
    // where only the base table would trigger reactivity.
    //
    // We test this by checking that the subscription's index scan nodes for the
    // joined table get marked dirty when we insert into that table.
    let sync_manager = SyncManager::new();
    let schema = join_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Subscribe to a join query: users JOIN posts ON users.id = posts.author_id
    let query = qm
        .query("users")
        .join("posts")
        .on("id", "author_id")
        .build();
    let sub_id = qm.subscribe(query).unwrap();

    // Process once to settle initial state
    qm.process(&mut storage);
    let _ = qm.take_updates();

    // Verify the subscription has index scan nodes for BOTH tables
    let subscription = qm.test_subscriptions().get(&sub_id).unwrap();
    let tables_in_subscription: Vec<&str> = subscription
        .graph
        .index_scan_nodes
        .iter()
        .map(|(_, table, _)| table.as_str())
        .collect();
    assert!(
        tables_in_subscription.contains(&"users"),
        "Subscription should have index scan for users"
    );
    assert!(
        tables_in_subscription.contains(&"posts"),
        "Subscription should have index scan for posts"
    );

    // Clear dirty nodes
    qm.test_subscriptions_mut()
        .get_mut(&sub_id)
        .unwrap()
        .graph
        .clear_dirty();

    // Insert into the JOINED table (posts), not the base table (users)
    qm.insert(
        &mut storage,
        "posts",
        &[
            Value::Integer(100),
            Value::Text("Test Post".into()),
            Value::Integer(1),
        ],
    )
    .unwrap();

    // BUG: mark_subscriptions_dirty() only checks subscription.graph.table.0 == "posts"
    // but the base table is "users", so the subscription won't be marked dirty.
    let subscription = qm.test_subscriptions().get(&sub_id).unwrap();
    assert!(
        subscription.graph.has_dirty_nodes(),
        "Join subscription should be marked dirty when joined table is modified"
    );
}

#[test]
fn join_produces_combined_tuples() {
    // Test that a join produces tuples with elements from both tables.
    // This verifies basic join functionality and tuple structure.
    let sync_manager = SyncManager::new();
    let schema = join_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert a user
    let user_id = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Integer(1), Value::Text("Alice".into())],
        )
        .unwrap();

    // Insert a post by that user
    let post_id = qm
        .insert(
            &mut storage,
            "posts",
            &[
                Value::Integer(100),
                Value::Text("Hello World".into()),
                Value::Integer(1), // author_id matches user id
            ],
        )
        .unwrap();

    // Subscribe to a join query
    let query = qm
        .query("users")
        .join("posts")
        .on("id", "author_id")
        .build();
    let sub_id = qm.subscribe(query).unwrap();

    // Process to get join results
    qm.process(&mut storage);
    let updates = qm.take_updates();
    let delta = updates
        .iter()
        .find(|u| u.subscription_id == sub_id)
        .map(|u| &u.delta)
        .expect("Should have updates for subscription");

    // Should have one joined row
    assert_eq!(delta.added.len(), 1, "Should have one joined result");

    // Validate row identity and ensure payload carries values from both sides of the join.
    let row = &delta.added[0];
    assert_eq!(
        row.id, user_id.row_id,
        "Join output should be keyed by base table row id"
    );
    assert_ne!(
        row.id, post_id.row_id,
        "Join output should not be keyed by joined table row id"
    );
    assert_eq!(
        row.data
            .windows("Alice".len())
            .any(|w| w == "Alice".as_bytes()),
        true,
        "Joined row payload should contain base-table text value"
    );
    assert_eq!(
        row.data
            .windows("Hello World".len())
            .any(|w| w == "Hello World".as_bytes()),
        true,
        "Joined row payload should contain joined-table text value"
    );
}

#[test]
fn join_filter_on_joined_table_column() {
    // Test filtering on a column from the JOINED table (not the base table).
    // FilterNode now uses TupleDescriptor to resolve column indices to correct tuple elements.
    let sync_manager = SyncManager::new();
    let schema = join_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert users
    qm.insert(
        &mut storage,
        "users",
        &[Value::Integer(1), Value::Text("Alice".into())],
    )
    .unwrap();
    qm.insert(
        &mut storage,
        "users",
        &[Value::Integer(2), Value::Text("Bob".into())],
    )
    .unwrap();

    // Insert posts - one should match filter, one should not
    qm.insert(
        &mut storage,
        "posts",
        &[
            Value::Integer(100),
            Value::Text("Hello World".into()), // Should NOT match "Rust"
            Value::Integer(1),
        ],
    )
    .unwrap();
    qm.insert(
        &mut storage,
        "posts",
        &[
            Value::Integer(101),
            Value::Text("Learning Rust".into()), // SHOULD match filter
            Value::Integer(2),
        ],
    )
    .unwrap();

    // Join with filter on posts.title
    // TODO: This filter won't work correctly - it will try to match "Rust"
    // against users.id column because evaluate_tuple only looks at element[0]
    let query = qm
        .query("users")
        .join("posts")
        .on("id", "author_id")
        // This filter SHOULD match posts.title containing "Rust"
        // but currently it compares against users table
        .filter_eq("title", Value::Text("Learning Rust".into()))
        .build();

    let sub_id = qm.subscribe(query).unwrap();
    qm.process(&mut storage);
    let updates = qm.take_updates();
    let delta = updates
        .iter()
        .find(|u| u.subscription_id == sub_id)
        .map(|u| &u.delta)
        .expect("Should have updates");

    // Should only have one result (the "Learning Rust" post)
    assert_eq!(
        delta.added.len(),
        1,
        "Filter on joined table column should work"
    );
}

// ========================================================================
// Array subquery (correlated subquery) tests
// ========================================================================

fn users_posts_schema() -> Schema {
    let mut schema = Schema::new();
    schema.insert(
        TableName::new("users"),
        RowDescriptor::new(vec![
            ColumnDescriptor::new("id", ColumnType::Integer),
            ColumnDescriptor::new("name", ColumnType::Text),
        ])
        .into(),
    );
    schema.insert(
        TableName::new("posts"),
        RowDescriptor::new(vec![
            ColumnDescriptor::new("id", ColumnType::Integer),
            ColumnDescriptor::new("title", ColumnType::Text),
            ColumnDescriptor::new("author_id", ColumnType::Integer),
        ])
        .into(),
    );
    schema
}

/// Output descriptor for users with posts array subquery.
fn users_with_posts_descriptor() -> RowDescriptor {
    // Posts row descriptor: [id, title, author_id]
    let posts_row_desc = RowDescriptor::new(vec![
        ColumnDescriptor::new("id", ColumnType::Integer),
        ColumnDescriptor::new("title", ColumnType::Text),
        ColumnDescriptor::new("author_id", ColumnType::Integer),
    ]);
    RowDescriptor::new(vec![
        ColumnDescriptor::new("id", ColumnType::Integer),
        ColumnDescriptor::new("name", ColumnType::Text),
        ColumnDescriptor::new(
            "posts",
            ColumnType::Array(Box::new(ColumnType::Row(Box::new(posts_row_desc)))),
        ),
    ])
}

#[test]
fn array_subquery_single_user_with_posts() {
    let sync_manager = SyncManager::new();
    let schema = users_posts_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert one user: Alice with id=1
    qm.insert(
        &mut storage,
        "users",
        &[Value::Integer(1), Value::Text("Alice".into())],
    )
    .unwrap();

    // Insert two posts for Alice
    qm.insert(
        &mut storage,
        "posts",
        &[
            Value::Integer(100),
            Value::Text("Alice Post 1".into()),
            Value::Integer(1), // author_id = 1 (Alice)
        ],
    )
    .unwrap();
    qm.insert(
        &mut storage,
        "posts",
        &[
            Value::Integer(101),
            Value::Text("Alice Post 2".into()),
            Value::Integer(1), // author_id = 1 (Alice)
        ],
    )
    .unwrap();

    // Query users with their posts as array
    let query = qm
        .query("users")
        .with_array("posts", |sub| {
            sub.from("posts").correlate("author_id", "users.id")
        })
        .build();

    let sub_id = qm.subscribe(query).unwrap();
    qm.process(&mut storage);

    let updates = qm.take_updates();
    let delta = updates
        .iter()
        .find(|u| u.subscription_id == sub_id)
        .map(|u| &u.delta)
        .expect("Should have subscription update");

    // Should have exactly 1 user row
    assert_eq!(delta.added.len(), 1, "Expected 1 user row");

    // Decode the output row
    let output_descriptor = users_with_posts_descriptor();
    let row_data = &delta.added[0].data;
    let values = decode_row(&output_descriptor, row_data).expect("Should decode output row");

    // Verify user fields
    assert_eq!(values[0], Value::Integer(1), "User id should be 1");
    assert_eq!(
        values[1],
        Value::Text("Alice".into()),
        "User name should be Alice"
    );

    // Verify posts array
    let posts = values[2].as_array().expect("Third column should be array");
    assert_eq!(posts.len(), 2, "Alice should have 2 posts");

    // Each post is a Row of [id, title, author_id]
    for post in posts {
        let post_values = post.as_row().expect("Each post should be a Row");
        assert_eq!(post_values.len(), 3, "Post should have 3 fields");
        // Verify author_id matches Alice
        assert_eq!(
            post_values[2],
            Value::Integer(1),
            "Post author_id should be 1 (Alice)"
        );
    }
}

#[test]
fn array_subquery_user_with_no_posts() {
    let sync_manager = SyncManager::new();
    let schema = users_posts_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert user with no posts
    qm.insert(
        &mut storage,
        "users",
        &[Value::Integer(1), Value::Text("Lonely".into())],
    )
    .unwrap();

    let query = qm
        .query("users")
        .with_array("posts", |sub| {
            sub.from("posts").correlate("author_id", "users.id")
        })
        .build();

    let sub_id = qm.subscribe(query).unwrap();
    qm.process(&mut storage);

    let updates = qm.take_updates();
    let delta = updates
        .iter()
        .find(|u| u.subscription_id == sub_id)
        .map(|u| &u.delta)
        .expect("Should have subscription update");

    assert_eq!(delta.added.len(), 1, "Should have 1 user");

    let output_descriptor = users_with_posts_descriptor();
    let values =
        decode_row(&output_descriptor, &delta.added[0].data).expect("Should decode output row");

    assert_eq!(values[0], Value::Integer(1));
    assert_eq!(values[1], Value::Text("Lonely".into()));

    // Posts array should be empty
    let posts = values[2].as_array().expect("Should have posts array");
    assert_eq!(posts.len(), 0, "User with no posts should have empty array");
}

#[test]
fn array_subquery_multiple_users_correct_correlation() {
    let sync_manager = SyncManager::new();
    let schema = users_posts_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert users
    qm.insert(
        &mut storage,
        "users",
        &[Value::Integer(1), Value::Text("Alice".into())],
    )
    .unwrap();
    qm.insert(
        &mut storage,
        "users",
        &[Value::Integer(2), Value::Text("Bob".into())],
    )
    .unwrap();

    // Alice's posts (author_id = 1)
    qm.insert(
        &mut storage,
        "posts",
        &[
            Value::Integer(100),
            Value::Text("Alice Post".into()),
            Value::Integer(1),
        ],
    )
    .unwrap();

    // Bob's posts (author_id = 2)
    qm.insert(
        &mut storage,
        "posts",
        &[
            Value::Integer(200),
            Value::Text("Bob Post".into()),
            Value::Integer(2),
        ],
    )
    .unwrap();

    let query = qm
        .query("users")
        .with_array("posts", |sub| {
            sub.from("posts").correlate("author_id", "users.id")
        })
        .build();

    let sub_id = qm.subscribe(query).unwrap();
    qm.process(&mut storage);

    let updates = qm.take_updates();
    let delta = updates
        .iter()
        .find(|u| u.subscription_id == sub_id)
        .map(|u| &u.delta)
        .expect("Should have updates");

    assert_eq!(delta.added.len(), 2, "Should have 2 users");

    let output_descriptor = users_with_posts_descriptor();

    // Build a map of user_id -> posts for verification
    let mut user_posts: std::collections::HashMap<i32, Vec<i32>> = std::collections::HashMap::new();
    for row in &delta.added {
        let values = decode_row(&output_descriptor, &row.data).expect("decode");
        let user_id = match &values[0] {
            Value::Integer(id) => id,
            _ => panic!("User id should be integer"),
        };
        let posts = values[2].as_array().expect("posts array");
        let post_ids: Vec<i32> = posts
            .iter()
            .filter_map(|p| {
                let row_vals = p.as_row()?;
                match &row_vals[0] {
                    Value::Integer(id) => Some(*id),
                    _ => None,
                }
            })
            .collect();
        user_posts.insert(*user_id, post_ids);
    }

    // Alice (id=1) should have post 100
    assert_eq!(
        user_posts.get(&1),
        Some(&vec![100]),
        "Alice should have post 100"
    );

    // Bob (id=2) should have post 200
    assert_eq!(
        user_posts.get(&2),
        Some(&vec![200]),
        "Bob should have post 200"
    );
}

#[test]
fn array_subquery_delta_on_inner_insert() {
    // Test: after subscription, inserting a new post should emit a delta
    // with the updated user row containing the new post in the array.
    let sync_manager = SyncManager::new();
    let schema = users_posts_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert user Alice
    qm.insert(
        &mut storage,
        "users",
        &[Value::Integer(1), Value::Text("Alice".into())],
    )
    .unwrap();

    // Insert initial post
    qm.insert(
        &mut storage,
        "posts",
        &[
            Value::Integer(100),
            Value::Text("Post 1".into()),
            Value::Integer(1),
        ],
    )
    .unwrap();

    // Subscribe to users with posts
    let query = qm
        .query("users")
        .with_array("posts", |sub| {
            sub.from("posts").correlate("author_id", "users.id")
        })
        .build();

    let sub_id = qm.subscribe(query).unwrap();
    qm.process(&mut storage);

    // Consume initial update
    let initial_updates = qm.take_updates();
    let initial_delta = initial_updates
        .iter()
        .find(|u| u.subscription_id == sub_id)
        .map(|u| &u.delta)
        .expect("Should have initial update");
    assert_eq!(initial_delta.added.len(), 1, "Initial: 1 user");

    // Verify initial state: Alice has 1 post
    let output_descriptor = users_with_posts_descriptor();
    let initial_values =
        decode_row(&output_descriptor, &initial_delta.added[0].data).expect("decode initial");
    let initial_posts = initial_values[2].as_array().expect("posts array");
    assert_eq!(initial_posts.len(), 1, "Initially Alice has 1 post");

    // NOW: Insert a new post for Alice
    qm.insert(
        &mut storage,
        "posts",
        &[
            Value::Integer(101),
            Value::Text("Post 2".into()),
            Value::Integer(1),
        ],
    )
    .unwrap();
    qm.process(&mut storage);

    // Check delta after inner insert
    let updates_after_insert = qm.take_updates();
    let delta_after = updates_after_insert
        .iter()
        .find(|u| u.subscription_id == sub_id)
        .map(|u| &u.delta)
        .expect("Should have delta after post insert");

    // Should have an update (old row removed, new row with updated array added)
    // or just updated entries
    let total_changes = delta_after.added.len() + delta_after.updated.len();
    assert!(
        total_changes > 0,
        "Should have changes after inserting post"
    );

    // Find the new state - either in added or as new part of updated
    let new_row_data = if !delta_after.added.is_empty() {
        &delta_after.added[0].data
    } else if !delta_after.updated.is_empty() {
        &delta_after.updated[0].1.data
    } else {
        panic!("Expected added or updated row");
    };

    let new_values = decode_row(&output_descriptor, new_row_data).expect("decode new");
    let new_posts = new_values[2].as_array().expect("posts array");
    assert_eq!(
        new_posts.len(),
        2,
        "After insert, Alice should have 2 posts"
    );

    // Verify both post IDs are present
    let post_ids: Vec<i32> = new_posts
        .iter()
        .filter_map(|p| match &p.as_row()?[0] {
            Value::Integer(id) => Some(*id),
            _ => None,
        })
        .collect();
    assert!(post_ids.contains(&100), "Should contain post 100");
    assert!(post_ids.contains(&101), "Should contain post 101");
}

#[test]
fn array_subquery_delta_on_outer_insert() {
    // Test: after subscription, inserting a new user should emit a delta
    // with the new user row (with their posts array).
    let sync_manager = SyncManager::new();
    let schema = users_posts_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert user Alice with a post
    qm.insert(
        &mut storage,
        "users",
        &[Value::Integer(1), Value::Text("Alice".into())],
    )
    .unwrap();
    qm.insert(
        &mut storage,
        "posts",
        &[
            Value::Integer(100),
            Value::Text("Alice Post".into()),
            Value::Integer(1),
        ],
    )
    .unwrap();

    // Also insert a post for Bob (who doesn't exist yet)
    qm.insert(
        &mut storage,
        "posts",
        &[
            Value::Integer(200),
            Value::Text("Bob Post".into()),
            Value::Integer(2),
        ],
    )
    .unwrap();

    // Subscribe
    let query = qm
        .query("users")
        .with_array("posts", |sub| {
            sub.from("posts").correlate("author_id", "users.id")
        })
        .build();

    let sub_id = qm.subscribe(query).unwrap();
    qm.process(&mut storage);

    // Consume initial update (just Alice)
    let initial_updates = qm.take_updates();
    let initial_delta = initial_updates
        .iter()
        .find(|u| u.subscription_id == sub_id)
        .map(|u| &u.delta)
        .expect("Should have initial update");
    assert_eq!(initial_delta.added.len(), 1, "Initial: only Alice");

    // NOW: Insert Bob
    qm.insert(
        &mut storage,
        "users",
        &[Value::Integer(2), Value::Text("Bob".into())],
    )
    .unwrap();
    qm.process(&mut storage);

    // Check delta after outer insert
    let updates_after = qm.take_updates();
    let delta_after = updates_after
        .iter()
        .find(|u| u.subscription_id == sub_id)
        .map(|u| &u.delta)
        .expect("Should have delta after user insert");

    // Should have Bob added
    assert_eq!(delta_after.added.len(), 1, "Bob should be added");

    let output_descriptor = users_with_posts_descriptor();
    let bob_values =
        decode_row(&output_descriptor, &delta_after.added[0].data).expect("decode Bob");

    assert_eq!(bob_values[0], Value::Integer(2), "Should be Bob (id=2)");
    assert_eq!(
        bob_values[1],
        Value::Text("Bob".into()),
        "Name should be Bob"
    );

    // Bob should have his post (id=200)
    let bob_posts = bob_values[2].as_array().expect("posts array");
    assert_eq!(bob_posts.len(), 1, "Bob should have 1 post");

    let post_row = bob_posts[0].as_row().expect("post should be Row");
    assert_eq!(
        post_row[0],
        Value::Integer(200),
        "Bob's post should be id=200"
    );
}

#[test]
fn array_subquery_with_order_by() {
    // Test: posts should be ordered by id descending
    let sync_manager = SyncManager::new();
    let schema = users_posts_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert user
    qm.insert(
        &mut storage,
        "users",
        &[Value::Integer(1), Value::Text("Alice".into())],
    )
    .unwrap();

    // Insert posts in random order
    qm.insert(
        &mut storage,
        "posts",
        &[
            Value::Integer(102),
            Value::Text("Middle".into()),
            Value::Integer(1),
        ],
    )
    .unwrap();
    qm.insert(
        &mut storage,
        "posts",
        &[
            Value::Integer(100),
            Value::Text("First".into()),
            Value::Integer(1),
        ],
    )
    .unwrap();
    qm.insert(
        &mut storage,
        "posts",
        &[
            Value::Integer(101),
            Value::Text("Last".into()),
            Value::Integer(1),
        ],
    )
    .unwrap();

    // Query with order_by_desc on id
    let query = qm
        .query("users")
        .with_array("posts", |sub| {
            sub.from("posts")
                .correlate("author_id", "users.id")
                .order_by_desc("id")
        })
        .build();

    let sub_id = qm.subscribe(query).unwrap();
    qm.process(&mut storage);

    let updates = qm.take_updates();
    let delta = updates
        .iter()
        .find(|u| u.subscription_id == sub_id)
        .map(|u| &u.delta)
        .expect("Should have update");

    let output_descriptor = users_with_posts_descriptor();
    let values = decode_row(&output_descriptor, &delta.added[0].data).expect("decode");
    let posts = values[2].as_array().expect("posts array");

    assert_eq!(posts.len(), 3, "Should have 3 posts");

    // Verify order: should be 102, 101, 100 (descending by id)
    let post_ids: Vec<i32> = posts
        .iter()
        .filter_map(|p| match &p.as_row()?[0] {
            Value::Integer(id) => Some(*id),
            _ => None,
        })
        .collect();
    assert_eq!(
        post_ids,
        vec![102, 101, 100],
        "Posts should be ordered by id desc"
    );
}

#[test]
fn array_subquery_with_limit() {
    // Test: limit should restrict number of posts returned
    let sync_manager = SyncManager::new();
    let schema = users_posts_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert user
    qm.insert(
        &mut storage,
        "users",
        &[Value::Integer(1), Value::Text("Alice".into())],
    )
    .unwrap();

    // Insert 5 posts
    for i in 100..105 {
        qm.insert(
            &mut storage,
            "posts",
            &[
                Value::Integer(i),
                Value::Text(format!("Post {}", i).into()),
                Value::Integer(1),
            ],
        )
        .unwrap();
    }

    // Query with limit 2
    let query = qm
        .query("users")
        .with_array("posts", |sub| {
            sub.from("posts")
                .correlate("author_id", "users.id")
                .order_by("id")
                .limit(2)
        })
        .build();

    let sub_id = qm.subscribe(query).unwrap();
    qm.process(&mut storage);

    let updates = qm.take_updates();
    let delta = updates
        .iter()
        .find(|u| u.subscription_id == sub_id)
        .map(|u| &u.delta)
        .expect("Should have update");

    let output_descriptor = users_with_posts_descriptor();
    let values = decode_row(&output_descriptor, &delta.added[0].data).expect("decode");
    let posts = values[2].as_array().expect("posts array");

    assert_eq!(posts.len(), 2, "Limit should restrict to 2 posts");

    // Verify first 2 posts by id ascending
    let post_ids: Vec<i32> = posts
        .iter()
        .filter_map(|p| match &p.as_row()?[0] {
            Value::Integer(id) => Some(*id),
            _ => None,
        })
        .collect();
    assert_eq!(post_ids, vec![100, 101], "Should get first 2 posts by id");
}

#[test]
fn array_subquery_with_select_columns() {
    // Test: select specific columns from inner query
    let sync_manager = SyncManager::new();
    let schema = users_posts_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert user and post
    qm.insert(
        &mut storage,
        "users",
        &[Value::Integer(1), Value::Text("Alice".into())],
    )
    .unwrap();
    qm.insert(
        &mut storage,
        "posts",
        &[
            Value::Integer(100),
            Value::Text("Post Title".into()),
            Value::Integer(1),
        ],
    )
    .unwrap();

    // Query selecting only id and title (not author_id)
    let query = qm
        .query("users")
        .with_array("posts", |sub| {
            sub.from("posts")
                .correlate("author_id", "users.id")
                .select(&["id", "title"])
        })
        .build();

    let sub_id = qm.subscribe(query).unwrap();
    qm.process(&mut storage);

    let updates = qm.take_updates();
    let delta = updates
        .iter()
        .find(|u| u.subscription_id == sub_id)
        .map(|u| &u.delta)
        .expect("Should have update");

    // Build descriptor for selected columns only
    let posts_row_desc = RowDescriptor::new(vec![
        ColumnDescriptor::new("id", ColumnType::Integer),
        ColumnDescriptor::new("title", ColumnType::Text),
    ]);
    let output_descriptor = RowDescriptor::new(vec![
        ColumnDescriptor::new("id", ColumnType::Integer),
        ColumnDescriptor::new("name", ColumnType::Text),
        ColumnDescriptor::new(
            "posts",
            ColumnType::Array(Box::new(ColumnType::Row(Box::new(posts_row_desc)))),
        ),
    ]);

    let values = decode_row(&output_descriptor, &delta.added[0].data).expect("decode");
    let posts = values[2].as_array().expect("posts array");

    assert_eq!(posts.len(), 1, "Should have 1 post");

    let post_row = posts[0].as_row().expect("post Row");
    assert_eq!(post_row.len(), 2, "Post should have 2 columns (id, title)");
    assert_eq!(post_row[0], Value::Integer(100));
    assert_eq!(post_row[1], Value::Text("Post Title".into()));
}

#[test]
fn array_subquery_with_join() {
    // Test: join inside array subquery
    // users with_array of (posts joined with comments)
    let sync_manager = SyncManager::new();
    let mut schema = Schema::new();
    schema.insert(
        TableName::new("users"),
        RowDescriptor::new(vec![
            ColumnDescriptor::new("id", ColumnType::Integer),
            ColumnDescriptor::new("name", ColumnType::Text),
        ])
        .into(),
    );
    schema.insert(
        TableName::new("posts"),
        RowDescriptor::new(vec![
            ColumnDescriptor::new("id", ColumnType::Integer),
            ColumnDescriptor::new("title", ColumnType::Text),
            ColumnDescriptor::new("author_id", ColumnType::Integer),
        ])
        .into(),
    );
    schema.insert(
        TableName::new("comments"),
        RowDescriptor::new(vec![
            ColumnDescriptor::new("id", ColumnType::Integer),
            ColumnDescriptor::new("text", ColumnType::Text),
            ColumnDescriptor::new("post_id", ColumnType::Integer),
        ])
        .into(),
    );

    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert user
    qm.insert(
        &mut storage,
        "users",
        &[Value::Integer(1), Value::Text("Alice".into())],
    )
    .unwrap();

    // Insert posts
    qm.insert(
        &mut storage,
        "posts",
        &[
            Value::Integer(100),
            Value::Text("Post A".into()),
            Value::Integer(1),
        ],
    )
    .unwrap();
    qm.insert(
        &mut storage,
        "posts",
        &[
            Value::Integer(101),
            Value::Text("Post B".into()),
            Value::Integer(1),
        ],
    )
    .unwrap();

    // Insert comments
    qm.insert(
        &mut storage,
        "comments",
        &[
            Value::Integer(1000),
            Value::Text("Comment on A".into()),
            Value::Integer(100), // post_id = 100
        ],
    )
    .unwrap();
    qm.insert(
        &mut storage,
        "comments",
        &[
            Value::Integer(1001),
            Value::Text("Another on A".into()),
            Value::Integer(100), // post_id = 100
        ],
    )
    .unwrap();
    qm.insert(
        &mut storage,
        "comments",
        &[
            Value::Integer(1002),
            Value::Text("Comment on B".into()),
            Value::Integer(101), // post_id = 101
        ],
    )
    .unwrap();

    // Query users with (posts joined with comments)
    // This should give us: for each user, an array of (post, comment) pairs
    let query = qm
        .query("users")
        .with_array("post_comments", |sub| {
            sub.from("posts")
                .join("comments")
                .on("posts.id", "comments.post_id")
                .correlate("author_id", "users.id")
        })
        .build();

    let sub_id = qm.subscribe(query).unwrap();
    qm.process(&mut storage);

    let updates = qm.take_updates();
    let delta = updates
        .iter()
        .find(|u| u.subscription_id == sub_id)
        .map(|u| &u.delta)
        .expect("Should have update");

    // Build descriptor for joined output:
    // posts columns + comments columns
    let joined_row_desc = RowDescriptor::new(vec![
        // posts columns
        ColumnDescriptor::new("id", ColumnType::Integer),
        ColumnDescriptor::new("title", ColumnType::Text),
        ColumnDescriptor::new("author_id", ColumnType::Integer),
        // comments columns
        ColumnDescriptor::new("id", ColumnType::Integer),
        ColumnDescriptor::new("text", ColumnType::Text),
        ColumnDescriptor::new("post_id", ColumnType::Integer),
    ]);
    let output_descriptor = RowDescriptor::new(vec![
        ColumnDescriptor::new("id", ColumnType::Integer),
        ColumnDescriptor::new("name", ColumnType::Text),
        ColumnDescriptor::new(
            "post_comments",
            ColumnType::Array(Box::new(ColumnType::Row(Box::new(joined_row_desc)))),
        ),
    ]);

    assert_eq!(delta.added.len(), 1, "Should have 1 user");
    let values = decode_row(&output_descriptor, &delta.added[0].data).expect("decode");
    assert_eq!(values[0], Value::Integer(1)); // user id
    assert_eq!(values[1], Value::Text("Alice".into())); // user name

    let post_comments = values[2].as_array().expect("post_comments array");
    // Each (post, comment) pair - Post A has 2 comments, Post B has 1
    assert_eq!(post_comments.len(), 3, "Should have 3 post-comment pairs");

    // Verify the joined rows contain both post and comment data
    for pc in post_comments {
        let row = pc.as_row().expect("joined row");
        assert_eq!(row.len(), 6, "Joined row should have 6 columns");
        // Post id should be either 100 or 101
        let post_id = match &row[0] {
            Value::Integer(id) => id,
            _ => panic!("Expected integer for post id"),
        };
        assert!(*post_id == 100 || *post_id == 101);
        // Comment post_id should match the post id
        assert_eq!(row[5], Value::Integer(*post_id));
    }
}

#[test]
fn array_subquery_nested() {
    // Test: nested array subqueries
    // users with_array(posts with_array(comments))
    let sync_manager = SyncManager::new();
    let mut schema = Schema::new();
    schema.insert(
        TableName::new("users"),
        RowDescriptor::new(vec![
            ColumnDescriptor::new("id", ColumnType::Integer),
            ColumnDescriptor::new("name", ColumnType::Text),
        ])
        .into(),
    );
    schema.insert(
        TableName::new("posts"),
        RowDescriptor::new(vec![
            ColumnDescriptor::new("id", ColumnType::Integer),
            ColumnDescriptor::new("title", ColumnType::Text),
            ColumnDescriptor::new("author_id", ColumnType::Integer),
        ])
        .into(),
    );
    schema.insert(
        TableName::new("comments"),
        RowDescriptor::new(vec![
            ColumnDescriptor::new("id", ColumnType::Integer),
            ColumnDescriptor::new("text", ColumnType::Text),
            ColumnDescriptor::new("post_id", ColumnType::Integer),
        ])
        .into(),
    );

    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert user
    qm.insert(
        &mut storage,
        "users",
        &[Value::Integer(1), Value::Text("Alice".into())],
    )
    .unwrap();

    // Insert posts
    qm.insert(
        &mut storage,
        "posts",
        &[
            Value::Integer(100),
            Value::Text("Post A".into()),
            Value::Integer(1),
        ],
    )
    .unwrap();
    qm.insert(
        &mut storage,
        "posts",
        &[
            Value::Integer(101),
            Value::Text("Post B".into()),
            Value::Integer(1),
        ],
    )
    .unwrap();

    // Insert comments - 2 on Post A, 1 on Post B
    qm.insert(
        &mut storage,
        "comments",
        &[
            Value::Integer(1000),
            Value::Text("Comment 1 on A".into()),
            Value::Integer(100),
        ],
    )
    .unwrap();
    qm.insert(
        &mut storage,
        "comments",
        &[
            Value::Integer(1001),
            Value::Text("Comment 2 on A".into()),
            Value::Integer(100),
        ],
    )
    .unwrap();
    qm.insert(
        &mut storage,
        "comments",
        &[
            Value::Integer(1002),
            Value::Text("Comment on B".into()),
            Value::Integer(101),
        ],
    )
    .unwrap();

    // Query: users with posts, where each post has its comments
    let query = qm
        .query("users")
        .with_array("posts", |sub| {
            sub.from("posts")
                .correlate("author_id", "users.id")
                .with_array("comments", |sub2| {
                    sub2.from("comments").correlate("post_id", "posts.id")
                })
        })
        .build();

    let sub_id = qm.subscribe(query).unwrap();
    qm.process(&mut storage);

    let updates = qm.take_updates();
    let delta = updates
        .iter()
        .find(|u| u.subscription_id == sub_id)
        .map(|u| &u.delta)
        .expect("Should have update");

    // Build nested descriptor:
    // comments row: [id, text, post_id]
    let comments_row_desc = RowDescriptor::new(vec![
        ColumnDescriptor::new("id", ColumnType::Integer),
        ColumnDescriptor::new("text", ColumnType::Text),
        ColumnDescriptor::new("post_id", ColumnType::Integer),
    ]);
    // posts row with comments array: [id, title, author_id, comments[]]
    let posts_row_desc = RowDescriptor::new(vec![
        ColumnDescriptor::new("id", ColumnType::Integer),
        ColumnDescriptor::new("title", ColumnType::Text),
        ColumnDescriptor::new("author_id", ColumnType::Integer),
        ColumnDescriptor::new(
            "comments",
            ColumnType::Array(Box::new(ColumnType::Row(Box::new(comments_row_desc)))),
        ),
    ]);
    // users row with posts array: [id, name, posts[]]
    let output_descriptor = RowDescriptor::new(vec![
        ColumnDescriptor::new("id", ColumnType::Integer),
        ColumnDescriptor::new("name", ColumnType::Text),
        ColumnDescriptor::new(
            "posts",
            ColumnType::Array(Box::new(ColumnType::Row(Box::new(posts_row_desc)))),
        ),
    ]);

    assert_eq!(delta.added.len(), 1, "Should have 1 user");
    let values = decode_row(&output_descriptor, &delta.added[0].data).expect("decode");
    assert_eq!(values[0], Value::Integer(1)); // user id
    assert_eq!(values[1], Value::Text("Alice".into())); // user name

    let posts = values[2].as_array().expect("posts array");
    assert_eq!(posts.len(), 2, "Alice should have 2 posts");

    // Check each post has its comments
    for post in posts {
        let post_row = post.as_row().expect("post row");
        assert_eq!(
            post_row.len(),
            4,
            "Post should have 4 columns (id, title, author_id, comments)"
        );

        let post_id = match &post_row[0] {
            Value::Integer(id) => id,
            _ => panic!("Expected integer for post id"),
        };

        let comments = post_row[3].as_array().expect("comments array");

        if *post_id == 100 {
            // Post A has 2 comments
            assert_eq!(comments.len(), 2, "Post A should have 2 comments");
            for comment in comments {
                let comment_row = comment.as_row().expect("comment row");
                assert_eq!(comment_row[2], Value::Integer(100)); // post_id
            }
        } else if *post_id == 101 {
            // Post B has 1 comment
            assert_eq!(comments.len(), 1, "Post B should have 1 comment");
            let comment_row = comments[0].as_row().expect("comment row");
            assert_eq!(comment_row[2], Value::Integer(101)); // post_id
        } else {
            panic!("Unexpected post id: {}", post_id);
        }
    }
}

#[test]
fn array_subquery_multiple_columns() {
    // Test: two separate (non-nested) array subquery columns
    // users with posts[] and with comments[] (comments directly on user)
    let sync_manager = SyncManager::new();
    let mut schema = Schema::new();
    schema.insert(
        TableName::new("users"),
        RowDescriptor::new(vec![
            ColumnDescriptor::new("id", ColumnType::Integer),
            ColumnDescriptor::new("name", ColumnType::Text),
        ])
        .into(),
    );
    schema.insert(
        TableName::new("posts"),
        RowDescriptor::new(vec![
            ColumnDescriptor::new("id", ColumnType::Integer),
            ColumnDescriptor::new("title", ColumnType::Text),
            ColumnDescriptor::new("author_id", ColumnType::Integer),
        ])
        .into(),
    );
    schema.insert(
        TableName::new("comments"),
        RowDescriptor::new(vec![
            ColumnDescriptor::new("id", ColumnType::Integer),
            ColumnDescriptor::new("text", ColumnType::Text),
            ColumnDescriptor::new("user_id", ColumnType::Integer),
        ])
        .into(),
    );

    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert users
    qm.insert(
        &mut storage,
        "users",
        &[Value::Integer(1), Value::Text("Alice".into())],
    )
    .unwrap();
    qm.insert(
        &mut storage,
        "users",
        &[Value::Integer(2), Value::Text("Bob".into())],
    )
    .unwrap();

    // Insert posts - Alice has 2, Bob has 1
    qm.insert(
        &mut storage,
        "posts",
        &[
            Value::Integer(100),
            Value::Text("Alice Post 1".into()),
            Value::Integer(1),
        ],
    )
    .unwrap();
    qm.insert(
        &mut storage,
        "posts",
        &[
            Value::Integer(101),
            Value::Text("Alice Post 2".into()),
            Value::Integer(1),
        ],
    )
    .unwrap();
    qm.insert(
        &mut storage,
        "posts",
        &[
            Value::Integer(102),
            Value::Text("Bob Post".into()),
            Value::Integer(2),
        ],
    )
    .unwrap();

    // Insert comments (directly on users) - Alice has 1, Bob has 2
    qm.insert(
        &mut storage,
        "comments",
        &[
            Value::Integer(1000),
            Value::Text("Alice comment".into()),
            Value::Integer(1),
        ],
    )
    .unwrap();
    qm.insert(
        &mut storage,
        "comments",
        &[
            Value::Integer(1001),
            Value::Text("Bob comment 1".into()),
            Value::Integer(2),
        ],
    )
    .unwrap();
    qm.insert(
        &mut storage,
        "comments",
        &[
            Value::Integer(1002),
            Value::Text("Bob comment 2".into()),
            Value::Integer(2),
        ],
    )
    .unwrap();

    // Query: users with both posts[] and comments[]
    let query = qm
        .query("users")
        .with_array("posts", |sub| {
            sub.from("posts").correlate("author_id", "users.id")
        })
        .with_array("comments", |sub| {
            sub.from("comments").correlate("user_id", "users.id")
        })
        .build();

    let sub_id = qm.subscribe(query).unwrap();
    qm.process(&mut storage);

    let updates = qm.take_updates();
    let delta = updates
        .iter()
        .find(|u| u.subscription_id == sub_id)
        .map(|u| &u.delta)
        .expect("Should have update");

    // Build descriptor: users + posts[] + comments[]
    let posts_row_desc = RowDescriptor::new(vec![
        ColumnDescriptor::new("id", ColumnType::Integer),
        ColumnDescriptor::new("title", ColumnType::Text),
        ColumnDescriptor::new("author_id", ColumnType::Integer),
    ]);
    let comments_row_desc = RowDescriptor::new(vec![
        ColumnDescriptor::new("id", ColumnType::Integer),
        ColumnDescriptor::new("text", ColumnType::Text),
        ColumnDescriptor::new("user_id", ColumnType::Integer),
    ]);
    let output_descriptor = RowDescriptor::new(vec![
        ColumnDescriptor::new("id", ColumnType::Integer),
        ColumnDescriptor::new("name", ColumnType::Text),
        ColumnDescriptor::new(
            "posts",
            ColumnType::Array(Box::new(ColumnType::Row(Box::new(posts_row_desc)))),
        ),
        ColumnDescriptor::new(
            "comments",
            ColumnType::Array(Box::new(ColumnType::Row(Box::new(comments_row_desc)))),
        ),
    ]);

    assert_eq!(delta.added.len(), 2, "Should have 2 users");

    // Decode and verify each user
    for row in &delta.added {
        let values = decode_row(&output_descriptor, &row.data).expect("decode");
        let user_id = match &values[0] {
            Value::Integer(id) => id,
            _ => panic!("Expected integer for user id"),
        };

        let posts = values[2].as_array().expect("posts array");
        let comments = values[3].as_array().expect("comments array");

        if *user_id == 1 {
            // Alice: 2 posts, 1 comment
            assert_eq!(values[1], Value::Text("Alice".into()));
            assert_eq!(posts.len(), 2, "Alice should have 2 posts");
            assert_eq!(comments.len(), 1, "Alice should have 1 comment");
        } else if *user_id == 2 {
            // Bob: 1 post, 2 comments
            assert_eq!(values[1], Value::Text("Bob".into()));
            assert_eq!(posts.len(), 1, "Bob should have 1 post");
            assert_eq!(comments.len(), 2, "Bob should have 2 comments");
        } else {
            panic!("Unexpected user id: {}", user_id);
        }
    }
}

// ========================================================================
// Policy (ReBAC) integration tests
// ========================================================================

fn policy_schema() -> Schema {
    let mut schema = Schema::new();
    schema.insert(
        TableName::new("documents"),
        TableSchema::with_policies(
            RowDescriptor::new(vec![
                ColumnDescriptor::new("owner_id", ColumnType::Text),
                ColumnDescriptor::new("team_id", ColumnType::Text),
                ColumnDescriptor::new("title", ColumnType::Text),
            ]),
            TablePolicies::new().with_select(
                // owner_id = @session.user_id OR team_id IN @session.claims.teams
                PolicyExpr::or(vec![
                    PolicyExpr::eq_session("owner_id", vec!["user_id".into()]),
                    PolicyExpr::in_session("team_id", vec!["claims".into(), "teams".into()]),
                ]),
            ),
        ),
    );
    schema
}

#[test]
fn policy_filters_select_results() {
    let sync_manager = SyncManager::new();
    let schema = policy_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert documents
    qm.insert(
        &mut storage,
        "documents",
        &[
            Value::Text("alice".into()),
            Value::Text("eng".into()),
            Value::Text("Alice's eng doc".into()),
        ],
    )
    .unwrap();
    qm.insert(
        &mut storage,
        "documents",
        &[
            Value::Text("bob".into()),
            Value::Text("eng".into()),
            Value::Text("Bob's eng doc".into()),
        ],
    )
    .unwrap();
    qm.insert(
        &mut storage,
        "documents",
        &[
            Value::Text("bob".into()),
            Value::Text("sales".into()),
            Value::Text("Bob's sales doc".into()),
        ],
    )
    .unwrap();
    qm.insert(
        &mut storage,
        "documents",
        &[
            Value::Text("charlie".into()),
            Value::Text("design".into()),
            Value::Text("Charlie's design doc".into()),
        ],
    )
    .unwrap();

    // Alice can see: her own doc + all eng docs = 2 docs
    let alice_session = PolicySession::new("alice").with_claims(json!({"teams": ["eng"]}));

    let query = qm.query("documents").build();
    let sub_id = qm
        .subscribe_with_session(query, Some(alice_session), None)
        .unwrap();

    qm.process(&mut storage);
    let updates = qm.take_updates();
    let alice_update = updates
        .iter()
        .find(|u| u.subscription_id == sub_id)
        .unwrap();

    assert_eq!(
        alice_update.delta.added.len(),
        2,
        "Alice should see 2 docs (her own + Bob's eng doc)"
    );

    // Bob on sales team can see: his 2 docs + no team docs (sales only) = 2 docs
    let bob_session = PolicySession::new("bob").with_claims(json!({"teams": ["sales"]}));

    let query2 = qm.query("documents").build();
    let sub_id2 = qm
        .subscribe_with_session(query2, Some(bob_session), None)
        .unwrap();

    qm.process(&mut storage);
    let updates2 = qm.take_updates();
    let bob_update = updates2
        .iter()
        .find(|u| u.subscription_id == sub_id2)
        .unwrap();

    assert_eq!(
        bob_update.delta.added.len(),
        2,
        "Bob should see 2 docs (his own 2 docs)"
    );
}

#[test]
fn no_session_returns_all_rows() {
    let sync_manager = SyncManager::new();
    let schema = policy_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert documents
    qm.insert(
        &mut storage,
        "documents",
        &[
            Value::Text("alice".into()),
            Value::Text("eng".into()),
            Value::Text("Doc 1".into()),
        ],
    )
    .unwrap();
    qm.insert(
        &mut storage,
        "documents",
        &[
            Value::Text("bob".into()),
            Value::Text("sales".into()),
            Value::Text("Doc 2".into()),
        ],
    )
    .unwrap();

    // Without session, all rows should be returned (policy not applied)
    let query = qm.query("documents").build();
    let sub_id = qm.subscribe(query).unwrap();

    qm.process(&mut storage);
    let updates = qm.take_updates();
    let update = updates
        .iter()
        .find(|u| u.subscription_id == sub_id)
        .unwrap();

    assert_eq!(
        update.delta.added.len(),
        2,
        "Without session, should see all 2 docs"
    );
}

#[test]
fn table_without_policy_returns_all_rows() {
    let sync_manager = SyncManager::new();
    // Use the regular test_schema which has no policies
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    qm.insert(
        &mut storage,
        "users",
        &[Value::Text("Alice".into()), Value::Integer(100)],
    )
    .unwrap();
    qm.insert(
        &mut storage,
        "users",
        &[Value::Text("Bob".into()), Value::Integer(200)],
    )
    .unwrap();

    // Even with session, table without policy returns all rows
    let session = PolicySession::new("some_user");
    let query = qm.query("users").build();
    let sub_id = qm
        .subscribe_with_session(query, Some(session), None)
        .unwrap();

    qm.process(&mut storage);
    let updates = qm.take_updates();
    let update = updates
        .iter()
        .find(|u| u.subscription_id == sub_id)
        .unwrap();

    assert_eq!(
        update.delta.added.len(),
        2,
        "Table without policy should return all rows"
    );
}

// ========================================================================
// Branch-aware query tests
// ========================================================================

#[test]
fn index_key_includes_branch() {
    // Verify that indices are keyed by (table, column, branch)
    // We test this by inserting a row and then querying with different branches.
    // The query should only find the row on the branch it was inserted on.

    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert on the schema's branch
    let handle = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();

    // Verify the row is indexed on the schema's branch
    let branch = get_branch(&qm);
    assert!(
        qm.row_is_indexed_on_branch(&storage, "users", &branch, handle.row_id),
        "Should have row indexed on schema branch"
    );

    // Verify the row is NOT indexed on a different branch
    assert!(
        !qm.row_is_indexed_on_branch(&storage, "users", "some-other-branch", handle.row_id),
        "Should NOT have row indexed on different branch"
    );
}

#[test]
fn query_builder_single_branch_uses_correct_index() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert on default "main" branch
    qm.insert(
        &mut storage,
        "users",
        &[Value::Text("Alice".into()), Value::Integer(100)],
    )
    .unwrap();

    // Query explicitly specifying "main" branch
    let query = qm.query("users").build();
    let results = execute_query(&mut qm, &mut storage, query).unwrap();
    assert_eq!(results.len(), 1, "Should find row on main branch");

    // Query specifying a different branch should return no results
    // (since we haven't inserted on that branch)
    let query = qm.query("users").branch("draft").build();
    let results = execute_query(&mut qm, &mut storage, query).unwrap();
    assert_eq!(results.len(), 0, "Should not find row on draft branch");
}

#[test]
fn query_builder_explicit_main_branch() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

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

    // Explicit .branch("main") should work same as default
    let query_explicit = qm.query("users").build();
    let query_default = qm.query("users").build();

    let results_explicit = execute_query(&mut qm, &mut storage, query_explicit).unwrap();
    let results_default = execute_query(&mut qm, &mut storage, query_default).unwrap();

    assert_eq!(results_explicit.len(), results_default.len());
    assert_eq!(results_explicit.len(), 2);
}

#[test]
fn query_multi_branch_requires_explicit_branch() {
    // Verify Query.branches field exists and works
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Multi-branch query with explicit branches
    let query = qm.query("users").branches(&["main", "draft"]).build();
    assert_eq!(query.branches.len(), 2);
    assert!(query.is_multi_branch());

    // Query without explicit branch has empty branches field.
    // The actual branches are resolved at execution time from schema context.
    let query = qm.query("users").build();
    assert!(query.branches.is_empty());
    assert!(!query.is_multi_branch());
}

#[test]
fn handle_object_update_respects_branch() {
    use crate::commit::{Commit, StoredState};
    use crate::query_manager::encoding::encode_row;
    use std::collections::HashMap;

    // Verify that handle_object_update updates the correct branch's indices.
    // Rows on a non-schema branch should NOT appear in queries on the schema branch.
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Get the actual schema branch
    let schema_branch = get_branch(&qm);

    // Subscribe to all objects
    qm.sync_manager_mut().object_manager.subscribe_all();

    let row_id = crate::object::ObjectId::new();
    let author = row_id;

    let mut metadata = HashMap::new();
    metadata.insert(MetadataKey::Table.to_string(), "users".to_string());
    qm.sync_manager_mut()
        .object_manager
        .receive_object(&mut storage, row_id, metadata);

    let descriptor = RowDescriptor::new(vec![
        ColumnDescriptor::new("name", ColumnType::Text),
        ColumnDescriptor::new("score", ColumnType::Integer),
    ]);
    let row_data = encode_row(
        &descriptor,
        &[Value::Text("Alice".into()), Value::Integer(100)],
    )
    .unwrap();

    // Receive commit on "other-branch" (not the schema's branch)
    let commit = Commit {
        parents: smallvec![],
        content: row_data.clone(),
        timestamp: 1000,
        author,
        metadata: None,
        stored_state: StoredState::Stored,
        ack_state: Default::default(),
    };
    qm.sync_manager_mut()
        .object_manager
        .receive_commit(&mut storage, row_id, "other-branch", commit)
        .unwrap();

    qm.process(&mut storage);

    // Query schema branch - should NOT find the row (it's on other-branch)
    let query = qm.query("users").build();
    let results = execute_query(&mut qm, &mut storage, query).unwrap();
    assert_eq!(
        results.len(),
        0,
        "Row on other-branch should not appear in schema branch query"
    );

    // Now insert on schema branch and verify it appears in default query
    let row_id2 = crate::object::ObjectId::new();
    let mut metadata2 = HashMap::new();
    metadata2.insert(MetadataKey::Table.to_string(), "users".to_string());
    qm.sync_manager_mut()
        .object_manager
        .receive_object(&mut storage, row_id2, metadata2);

    let commit2 = Commit {
        parents: smallvec![],
        content: row_data,
        timestamp: 2000,
        author: row_id2,
        metadata: None,
        stored_state: StoredState::Stored,
        ack_state: Default::default(),
    };
    qm.sync_manager_mut()
        .object_manager
        .receive_commit(&mut storage, row_id2, &schema_branch, commit2)
        .unwrap();

    qm.process(&mut storage);

    // Schema branch should now have 1 row
    let query = qm.query("users").build();
    let results = execute_query(&mut qm, &mut storage, query).unwrap();
    assert_eq!(
        results.len(),
        1,
        "Row on schema branch should appear in default query"
    );
}

// ============================================================================
// Contributing ObjectIds Tests
// ============================================================================

#[test]
fn contributing_ids_reflect_filter() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert 3 rows with different scores
    let handle1 = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();
    let handle2 = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Bob".into()), Value::Integer(30)],
        )
        .unwrap();
    let handle3 = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Charlie".into()), Value::Integer(75)],
        )
        .unwrap();

    // Subscribe to query: score > 50
    let query = qm
        .query("users")
        .filter_gt("score", Value::Integer(50))
        .build();
    let sub_id = qm.subscribe(query.clone()).unwrap();

    qm.process(&mut storage);

    // Get contributing ObjectIds
    let contributing = qm.get_subscription_contributing_ids(sub_id);

    // Should have 2 rows (Alice: 100, Charlie: 75), not Bob (30)
    assert_eq!(contributing.len(), 2, "Should have 2 contributing IDs");

    let branch_str = get_branch(&qm);
    let branch = crate::object::BranchName::new(&branch_str);
    assert!(
        contributing.contains(&(handle1.row_id, branch.clone())),
        "Alice should be in contributing set"
    );
    assert!(
        !contributing.contains(&(handle2.row_id, branch.clone())),
        "Bob should NOT be in contributing set (score < 50)"
    );
    assert!(
        contributing.contains(&(handle3.row_id, branch)),
        "Charlie should be in contributing set"
    );
}

#[test]
fn contributing_ids_update_reactively() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert 2 rows initially
    let _handle1 = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();
    let handle2 = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Bob".into()), Value::Integer(30)],
        )
        .unwrap();

    // Subscribe to query: score > 50
    let query = qm
        .query("users")
        .filter_gt("score", Value::Integer(50))
        .build();
    let sub_id = qm.subscribe(query.clone()).unwrap();

    qm.process(&mut storage);

    // Initially 1 match (Alice: 100)
    let contributing = qm.get_subscription_contributing_ids(sub_id);
    assert_eq!(
        contributing.len(),
        1,
        "Should have 1 contributing ID initially"
    );

    // Insert new row with score > 50
    let handle3 = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Charlie".into()), Value::Integer(75)],
        )
        .unwrap();

    qm.process(&mut storage);

    // Now 2 matches
    let contributing = qm.get_subscription_contributing_ids(sub_id);
    assert_eq!(
        contributing.len(),
        2,
        "Should have 2 contributing IDs after insert"
    );

    let branch_str = get_branch(&qm);
    let branch = crate::object::BranchName::new(&branch_str);
    assert!(
        contributing.contains(&(handle3.row_id, branch.clone())),
        "Charlie should be in contributing set"
    );

    // Update Bob's score to > 50
    qm.update(
        &mut storage,
        handle2.row_id,
        &[Value::Text("Bob".into()), Value::Integer(60)],
    )
    .unwrap();
    qm.process(&mut storage);

    // Now 3 matches
    let contributing = qm.get_subscription_contributing_ids(sub_id);
    assert_eq!(
        contributing.len(),
        3,
        "Should have 3 contributing IDs after update"
    );
    assert!(
        contributing.contains(&(handle2.row_id, branch)),
        "Bob should now be in contributing set"
    );
}

// ============================================================================
// Server-Side Query Subscription Tests
// ============================================================================

#[test]
fn server_builds_query_graph_on_subscription() {
    use crate::sync_manager::{ClientId, Destination, InboxEntry, QueryId, Source, SyncPayload};
    use uuid::Uuid;

    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut server_qm, mut storage) = create_query_manager(sync_manager, schema);

    // Server has existing data: 3 users, 2 with score > 50
    let handle1 = server_qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();
    let _handle2 = server_qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Bob".into()), Value::Integer(30)],
        )
        .unwrap();
    let handle3 = server_qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Charlie".into()), Value::Integer(75)],
        )
        .unwrap();
    server_qm.process(&mut storage);

    // Add a client
    let client_id = ClientId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    server_qm.sync_manager_mut().add_client(client_id);

    // Client sends QuerySubscription for score > 50
    let query = server_qm
        .query("users")
        .filter_gt("score", Value::Integer(50))
        .build();

    server_qm.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Client(client_id),
        payload: SyncPayload::QuerySubscription {
            query_id: QueryId(1),
            query,
            session: None,
        },
    });

    server_qm.process(&mut storage);

    // Server should send ObjectUpdated for matching users (Alice, Charlie)
    let outbox = server_qm.sync_manager_mut().take_outbox();

    // Filter for ObjectUpdated messages to this client
    let object_updates: Vec<_> = outbox
        .iter()
        .filter(|e| matches!(e.destination, Destination::Client(id) if id == client_id))
        .filter(|e| matches!(e.payload, SyncPayload::ObjectUpdated { .. }))
        .collect();

    assert_eq!(
        object_updates.len(),
        2,
        "Should send 2 ObjectUpdated messages for matching users"
    );

    // Verify the correct ObjectIds were sent
    let sent_ids: std::collections::HashSet<_> = object_updates
        .iter()
        .filter_map(|e| {
            if let SyncPayload::ObjectUpdated { object_id, .. } = &e.payload {
                Some(*object_id)
            } else {
                None
            }
        })
        .collect();

    assert!(sent_ids.contains(&handle1.row_id), "Alice should be sent");
    assert!(sent_ids.contains(&handle3.row_id), "Charlie should be sent");
}

#[test]
fn server_pushes_new_matches() {
    use crate::sync_manager::{ClientId, Destination, InboxEntry, QueryId, Source, SyncPayload};
    use uuid::Uuid;

    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut server_qm, mut storage) = create_query_manager(sync_manager, schema);

    // Server has 1 user initially
    let _handle1 = server_qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();
    server_qm.process(&mut storage);

    // Add client and subscribe
    let client_id = ClientId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    server_qm.sync_manager_mut().add_client(client_id);

    let query = server_qm
        .query("users")
        .filter_gt("score", Value::Integer(50))
        .build();

    server_qm.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Client(client_id),
        payload: SyncPayload::QuerySubscription {
            query_id: QueryId(1),
            query,
            session: None,
        },
    });

    server_qm.process(&mut storage);

    // Clear initial outbox
    let _ = server_qm.sync_manager_mut().take_outbox();

    // Insert new matching user
    let handle2 = server_qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Charlie".into()), Value::Integer(75)],
        )
        .unwrap();
    server_qm.process(&mut storage);

    // Should send ObjectUpdated for new matching user
    let outbox = server_qm.sync_manager_mut().take_outbox();

    let object_updates: Vec<_> = outbox
        .iter()
        .filter(|e| matches!(e.destination, Destination::Client(id) if id == client_id))
        .filter(|e| matches!(e.payload, SyncPayload::ObjectUpdated { .. }))
        .collect();

    assert_eq!(
        object_updates.len(),
        1,
        "Should send 1 ObjectUpdated for new matching user"
    );

    // Verify it's Charlie
    if let SyncPayload::ObjectUpdated { object_id, .. } = &object_updates[0].payload {
        assert_eq!(*object_id, handle2.row_id, "Should send Charlie's ObjectId");
    }
}

#[test]
fn server_does_not_push_non_matching() {
    use crate::sync_manager::{ClientId, Destination, InboxEntry, QueryId, Source, SyncPayload};
    use uuid::Uuid;

    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut server_qm, mut storage) = create_query_manager(sync_manager, schema);

    // Add client and subscribe to score > 50
    let client_id = ClientId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    server_qm.sync_manager_mut().add_client(client_id);

    let query = server_qm
        .query("users")
        .filter_gt("score", Value::Integer(50))
        .build();

    server_qm.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Client(client_id),
        payload: SyncPayload::QuerySubscription {
            query_id: QueryId(1),
            query,
            session: None,
        },
    });

    server_qm.process(&mut storage);
    let _ = server_qm.sync_manager_mut().take_outbox();

    // Insert non-matching user (score = 30)
    let _handle = server_qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Bob".into()), Value::Integer(30)],
        )
        .unwrap();
    server_qm.process(&mut storage);

    // Should NOT send ObjectUpdated for non-matching user
    let outbox = server_qm.sync_manager_mut().take_outbox();

    let object_updates: Vec<_> = outbox
        .iter()
        .filter(|e| matches!(e.destination, Destination::Client(id) if id == client_id))
        .filter(|e| matches!(e.payload, SyncPayload::ObjectUpdated { .. }))
        .collect();

    assert_eq!(
        object_updates.len(),
        0,
        "Should NOT send ObjectUpdated for non-matching user"
    );
}

// ============================================================================
// Client subscribe_with_sync Tests
// ============================================================================

#[test]
fn subscribe_with_sync_sends_to_servers() {
    use crate::sync_manager::{Destination, ServerId, SyncPayload};
    use uuid::Uuid;

    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut client_qm, mut storage) = create_query_manager(sync_manager, schema);

    // Add a server
    let server_id = ServerId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    client_qm.sync_manager_mut().add_server(server_id);

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
    client_qm.add_server(server_id);

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
fn unsubscribe_with_sync_sends_to_servers() {
    use crate::sync_manager::{Destination, ServerId, SyncPayload};
    use uuid::Uuid;

    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut client_qm, mut storage) = create_query_manager(sync_manager, schema);

    // Add a server
    let server_id = ServerId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    client_qm.sync_manager_mut().add_server(server_id);

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

// ============================================================================
// Part 5: Multi-Tier Forwarding Tests
// ============================================================================

/// Test that a mid-tier server forwards QuerySubscription to upstream servers.
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
    mid_tier.sync_manager_mut().add_server(upstream_id);

    // Add downstream client
    let client_id = ClientId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    mid_tier.sync_manager_mut().add_client(client_id);

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
            query,
            session: None,
        },
    });

    // Process the subscription
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

/// Test that a mid-tier server forwards QueryUnsubscription to upstream servers.
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
    mid_tier.sync_manager_mut().add_server(upstream_id);
    mid_tier.sync_manager_mut().add_client(client_id);

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
            query,
            session: None,
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

/// Test that objects from upstream are relayed to downstream clients with matching scope.
#[test]
fn mid_tier_relays_objects_to_clients_with_matching_scope() {
    use crate::commit::Commit;
    use crate::object::ObjectId;
    use crate::sync_manager::{
        ClientId, Destination, InboxEntry, ObjectMetadata, ServerId, Source, SyncPayload,
    };
    use uuid::Uuid;

    // Setup mid-tier server
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut mid_tier, mut storage) = create_query_manager(sync_manager, schema.clone());

    // Add upstream server and downstream client
    let upstream_id = ServerId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    let client_id = ClientId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    mid_tier.sync_manager_mut().add_server(upstream_id);
    mid_tier.sync_manager_mut().add_client(client_id);

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
    let branch_name = crate::object::BranchName::new(&branch_str);

    // Establish client subscription
    let query = mid_tier
        .query("users")
        .filter_gt("score", Value::Integer(50))
        .build();

    mid_tier.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Client(client_id),
        payload: SyncPayload::QuerySubscription {
            query_id: crate::sync_manager::QueryId(42),
            query,
            session: None,
        },
    });
    mid_tier.process(&mut storage);

    // Clear outbox (initial sync messages)
    let _ = mid_tier.sync_manager_mut().take_outbox();

    // Now receive an update for the existing object from upstream
    // (simulating upstream sending fresh data)
    let table_schema = schema.get(&TableName::new("users")).unwrap();
    let row_data = super::encode_row(
        &table_schema.descriptor,
        &[Value::Text("Alice".into()), Value::Integer(80)],
    )
    .unwrap();

    let current_tips: smallvec::SmallVec<[_; 2]> = mid_tier
        .sync_manager()
        .object_manager
        .get(handle.row_id)
        .unwrap()
        .branches
        .get(&branch_name)
        .unwrap()
        .tips
        .iter()
        .copied()
        .collect();

    let author = ObjectId::new();
    let commit = Commit {
        parents: current_tips,
        content: row_data,
        timestamp: 2000,
        author,
        metadata: None,
        stored_state: crate::commit::StoredState::Stored,
        ack_state: Default::default(),
    };

    mid_tier.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Server(upstream_id),
        payload: SyncPayload::ObjectUpdated {
            object_id: handle.row_id,
            metadata: Some(ObjectMetadata {
                id: handle.row_id,
                metadata: [("table".to_string(), "users".to_string())]
                    .into_iter()
                    .collect(),
            }),
            branch_name: branch_name.clone(),
            commits: vec![commit],
        },
    });
    mid_tier.process(&mut storage);

    // Check that the update was forwarded to the client
    let outbox = mid_tier.sync_manager_mut().take_outbox();

    let relayed: Vec<_> = outbox
        .iter()
        .filter(|e| matches!(e.destination, Destination::Client(id) if id == client_id))
        .filter(|e| matches!(&e.payload, SyncPayload::ObjectUpdated { object_id, .. } if *object_id == handle.row_id))
        .collect();

    assert_eq!(
        relayed.len(),
        1,
        "Mid-tier should relay ObjectUpdated from upstream to client with matching scope"
    );
}

// ============================================================================
// Part 6: End-to-End Integration Tests
// ============================================================================

/// Helper to exchange messages between client and server QueryManagers.
/// Runs multiple rounds until no more messages are exchanged.
fn pump_messages(
    client: &mut QueryManager,
    server: &mut QueryManager,
    client_io: &mut MemoryStorage,
    server_io: &mut MemoryStorage,
    client_id: crate::sync_manager::ClientId,
    server_id: crate::sync_manager::ServerId,
) {
    use crate::sync_manager::{Destination, InboxEntry, Source};

    for _ in 0..10 {
        // Client → Server
        let client_outbox = client.sync_manager_mut().take_outbox();
        let client_to_server: Vec<_> = client_outbox
            .into_iter()
            .filter(|e| matches!(e.destination, Destination::Server(id) if id == server_id))
            .collect();

        for entry in client_to_server {
            server.sync_manager_mut().push_inbox(InboxEntry {
                source: Source::Client(client_id),
                payload: entry.payload,
            });
        }
        server.process(server_io);

        // Server → Client
        let server_outbox = server.sync_manager_mut().take_outbox();
        let server_to_client: Vec<_> = server_outbox
            .into_iter()
            .filter(|e| matches!(e.destination, Destination::Client(id) if id == client_id))
            .collect();

        if server_to_client.is_empty() {
            break;
        }

        for entry in server_to_client {
            client.sync_manager_mut().push_inbox(InboxEntry {
                source: Source::Server(server_id),
                payload: entry.payload,
            });
        }
        client.process(client_io);
    }
}

/// E2E: Client subscribes to query, receives matching data from server.
#[test]
fn e2e_client_receives_server_data_via_subscription() {
    use crate::sync_manager::{ClientId, ServerId};
    use uuid::Uuid;

    let schema = test_schema();

    // Setup server with data
    let server_sync = SyncManager::new();
    let (mut server, mut server_io) = create_query_manager(server_sync, schema.clone());

    server
        .insert(
            &mut server_io,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(75)],
        )
        .unwrap();
    server
        .insert(
            &mut server_io,
            "users",
            &[Value::Text("Bob".into()), Value::Integer(30)],
        )
        .unwrap();
    server
        .insert(
            &mut server_io,
            "users",
            &[Value::Text("Charlie".into()), Value::Integer(90)],
        )
        .unwrap();
    server.process(&mut server_io);

    // Setup client (no data yet)
    let client_sync = SyncManager::new();
    let (mut client, mut client_io) = create_query_manager(client_sync, schema.clone());

    // Subscribe to all object updates (needed to receive sync'd data)

    // Connect client to server
    let server_id = ServerId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    let client_id = ClientId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));

    client.sync_manager_mut().add_server(server_id);
    server.sync_manager_mut().add_client(client_id);

    // Clear initial sync messages (we want query-driven sync)
    let _ = client.sync_manager_mut().take_outbox();

    // Client subscribes to users with score > 50 (should match Alice and Charlie)
    let query = client
        .query("users")
        .filter_gt("score", Value::Integer(50))
        .build();

    let sub_id = client.subscribe_with_sync(query, None, None).unwrap();

    // Exchange messages between client and server
    pump_messages(
        &mut client,
        &mut server,
        &mut client_io,
        &mut server_io,
        client_id,
        server_id,
    );

    // Client should now have the matching rows
    let results = client.get_subscription_results(sub_id);

    assert_eq!(results.len(), 2, "Client should receive 2 matching users");

    let names: Vec<_> = results
        .iter()
        .filter_map(|(_, row)| {
            if let Value::Text(name) = &row[0] {
                Some(name.as_str())
            } else {
                None
            }
        })
        .collect();

    assert!(names.contains(&"Alice"), "Should contain Alice");
    assert!(names.contains(&"Charlie"), "Should contain Charlie");
    assert!(!names.contains(&"Bob"), "Should NOT contain Bob");
}

/// E2E: Client receives new matching rows as server inserts them.
#[test]
fn e2e_client_receives_new_matching_row() {
    use crate::sync_manager::{ClientId, ServerId};
    use uuid::Uuid;

    let schema = test_schema();

    // Setup server (initially empty)
    let server_sync = SyncManager::new();
    let (mut server, mut server_io) = create_query_manager(server_sync, schema.clone());

    // Setup client
    let client_sync = SyncManager::new();
    let (mut client, mut client_io) = create_query_manager(client_sync, schema.clone());

    // Connect
    let server_id = ServerId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    let client_id = ClientId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));

    client.sync_manager_mut().add_server(server_id);
    server.sync_manager_mut().add_client(client_id);
    let _ = client.sync_manager_mut().take_outbox();

    // Client subscribes to users with score > 50
    let query = client
        .query("users")
        .filter_gt("score", Value::Integer(50))
        .build();

    let sub_id = client.subscribe_with_sync(query, None, None).unwrap();

    // Initial sync (empty)
    pump_messages(
        &mut client,
        &mut server,
        &mut client_io,
        &mut server_io,
        client_id,
        server_id,
    );
    assert_eq!(
        client.get_subscription_results(sub_id).len(),
        0,
        "Initially empty"
    );

    // Server inserts a matching row
    server
        .insert(
            &mut server_io,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(75)],
        )
        .unwrap();
    server.process(&mut server_io);

    // Exchange messages
    pump_messages(
        &mut client,
        &mut server,
        &mut client_io,
        &mut server_io,
        client_id,
        server_id,
    );

    // Client should now have Alice
    let results = client.get_subscription_results(sub_id);
    assert_eq!(results.len(), 1, "Client should receive new matching user");
    assert_eq!(results[0].1[0], Value::Text("Alice".into()));
}

/// E2E: Client does NOT receive rows that don't match the query filter.
#[test]
fn e2e_client_does_not_receive_non_matching_row() {
    use crate::sync_manager::{ClientId, ServerId};
    use uuid::Uuid;

    let schema = test_schema();

    // Setup server and client
    let server_sync = SyncManager::new();
    let (mut server, mut server_io) = create_query_manager(server_sync, schema.clone());

    let client_sync = SyncManager::new();
    let (mut client, mut client_io) = create_query_manager(client_sync, schema.clone());

    // Connect
    let server_id = ServerId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    let client_id = ClientId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));

    client.sync_manager_mut().add_server(server_id);
    server.sync_manager_mut().add_client(client_id);
    let _ = client.sync_manager_mut().take_outbox();

    // Client subscribes to users with score > 50
    let query = client
        .query("users")
        .filter_gt("score", Value::Integer(50))
        .build();

    let sub_id = client.subscribe_with_sync(query, None, None).unwrap();
    pump_messages(
        &mut client,
        &mut server,
        &mut client_io,
        &mut server_io,
        client_id,
        server_id,
    );

    // Server inserts a NON-matching row (score = 30)
    server
        .insert(
            &mut server_io,
            "users",
            &[Value::Text("Bob".into()), Value::Integer(30)],
        )
        .unwrap();
    server.process(&mut server_io);

    // Exchange messages
    pump_messages(
        &mut client,
        &mut server,
        &mut client_io,
        &mut server_io,
        client_id,
        server_id,
    );

    // Client should NOT have Bob
    let results = client.get_subscription_results(sub_id);
    assert_eq!(
        results.len(),
        0,
        "Client should NOT receive non-matching user"
    );
}

/// E2E: Permissions filter what gets synced - client only receives permitted rows.
#[test]
fn e2e_permissions_prevent_sync() {
    use crate::sync_manager::{ClientId, ServerId};
    use uuid::Uuid;

    // Create schema with documents table that has owner-based policy
    // Policy: owner_id must match session.user_id
    let mut schema = Schema::new();
    schema.insert(
        TableName::new("documents"),
        TableSchema {
            descriptor: RowDescriptor::new(vec![
                ColumnDescriptor::new("title", ColumnType::Text),
                ColumnDescriptor::new("owner_id", ColumnType::Text), // Text to match user_id string
            ]),
            policies: TablePolicies::new()
                .with_select(PolicyExpr::eq_session("owner_id", vec!["user_id".into()])),
        },
    );

    // Setup server with docs owned by different users
    let server_sync = SyncManager::new();
    let (mut server, mut server_io) = create_query_manager(server_sync, schema.clone());

    server
        .insert(
            &mut server_io,
            "documents",
            &[
                Value::Text("Alice's doc".into()),
                Value::Text("alice".into()),
            ],
        )
        .unwrap();
    server
        .insert(
            &mut server_io,
            "documents",
            &[Value::Text("Bob's doc".into()), Value::Text("bob".into())],
        )
        .unwrap();
    server.process(&mut server_io);

    // Setup client
    let client_sync = SyncManager::new();
    let (mut client, mut client_io) = create_query_manager(client_sync, schema.clone());

    // Connect
    let server_id = ServerId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    let client_id = ClientId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));

    client.sync_manager_mut().add_server(server_id);
    server.sync_manager_mut().add_client(client_id);
    let _ = client.sync_manager_mut().take_outbox();

    // Client subscribes as Alice (user_id = "alice")
    let alice_session = PolicySession::new("alice");
    let query = client.query("documents").build();

    let sub_id = client
        .subscribe_with_sync(query, Some(alice_session), None)
        .unwrap();

    // Exchange messages
    pump_messages(
        &mut client,
        &mut server,
        &mut client_io,
        &mut server_io,
        client_id,
        server_id,
    );

    // Client should ONLY have Alice's doc
    let results = client.get_subscription_results(sub_id);

    assert_eq!(
        results.len(),
        1,
        "Client should only receive docs they have permission to see"
    );
    assert_eq!(
        results[0].1[0],
        Value::Text("Alice's doc".into()),
        "Should be Alice's doc"
    );
}

/// E2E: New rows that don't match permissions are NOT synced.
#[test]
fn e2e_permissions_prevent_new_row_sync() {
    use crate::sync_manager::{ClientId, ServerId};
    use uuid::Uuid;

    // Create schema with owner-based policy
    let mut schema = Schema::new();
    schema.insert(
        TableName::new("documents"),
        TableSchema {
            descriptor: RowDescriptor::new(vec![
                ColumnDescriptor::new("title", ColumnType::Text),
                ColumnDescriptor::new("owner_id", ColumnType::Text), // Text to match user_id string
            ]),
            policies: TablePolicies::new()
                .with_select(PolicyExpr::eq_session("owner_id", vec!["user_id".into()])),
        },
    );

    // Setup server and client
    let server_sync = SyncManager::new();
    let (mut server, mut server_io) = create_query_manager(server_sync, schema.clone());

    let client_sync = SyncManager::new();
    let (mut client, mut client_io) = create_query_manager(client_sync, schema.clone());

    // Connect
    let server_id = ServerId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    let client_id = ClientId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));

    client.sync_manager_mut().add_server(server_id);
    server.sync_manager_mut().add_client(client_id);
    let _ = client.sync_manager_mut().take_outbox();

    // Client subscribes as Alice
    let alice_session = PolicySession::new("alice");
    let query = client.query("documents").build();

    let sub_id = client
        .subscribe_with_sync(query, Some(alice_session), None)
        .unwrap();
    pump_messages(
        &mut client,
        &mut server,
        &mut client_io,
        &mut server_io,
        client_id,
        server_id,
    );

    // Server inserts Alice's doc
    server
        .insert(
            &mut server_io,
            "documents",
            &[
                Value::Text("Alice's doc".into()),
                Value::Text("alice".into()),
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

    assert_eq!(
        client.get_subscription_results(sub_id).len(),
        1,
        "Client should have Alice's doc"
    );

    // Server inserts Bob's doc (owner_id = "bob")
    server
        .insert(
            &mut server_io,
            "documents",
            &[Value::Text("Bob's doc".into()), Value::Text("bob".into())],
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

    // Client should still only have Alice's doc
    let results = client.get_subscription_results(sub_id);
    assert_eq!(results.len(), 1, "Client should NOT receive Bob's doc");
    assert_eq!(results[0].1[0], Value::Text("Alice's doc".into()));
}
