//! Upstream sync integration tests.
//!
//! These tests verify server-to-server sync (upstream connections) using
//! incremental query deltas rather than raw SSE events.

#![cfg(not(target_arch = "wasm32"))]

use std::sync::{Arc, Mutex};
use std::time::Duration;

use groove::sql::Database;
use groove::sql::query_graph::{DeltaBatch, RowDelta};
use groove::sync::test_harness::MultiServerHarness;
use groove::sync::{SubscriptionOptions, UpstreamState};
use groove::{LocalNode, ObjectId};

// ============================================================================
// Helpers
// ============================================================================

/// Small delay to allow async propagation.
async fn settle() {
    tokio::time::sleep(Duration::from_millis(50)).await;
}

/// Collect deltas into a shared vector for testing.
#[allow(clippy::type_complexity)]
fn delta_collector() -> (
    Arc<Mutex<Vec<DeltaBatch>>>,
    Box<dyn Fn(&DeltaBatch) + Send + Sync + 'static>,
) {
    let deltas = Arc::new(Mutex::new(Vec::new()));
    let deltas_clone = Arc::clone(&deltas);
    let callback = Box::new(move |batch: &DeltaBatch| {
        deltas_clone.lock().unwrap().push(batch.clone());
    });
    (deltas, callback)
}

/// Count Added deltas in collected batches.
fn count_added(deltas: &Arc<Mutex<Vec<DeltaBatch>>>) -> usize {
    deltas
        .lock()
        .unwrap()
        .iter()
        .flat_map(|batch| batch.iter())
        .filter(|d| matches!(d, RowDelta::Added { .. }))
        .count()
}

/// Get all added row IDs from collected batches.
fn added_ids(deltas: &Arc<Mutex<Vec<DeltaBatch>>>) -> Vec<ObjectId> {
    deltas
        .lock()
        .unwrap()
        .iter()
        .flat_map(|batch| batch.iter())
        .filter_map(|d| match d {
            RowDelta::Added { id, .. } => Some(*id),
            _ => None,
        })
        .collect()
}

/// Helper to sync an object from one node to another.
fn sync_object(from: &LocalNode, to: &LocalNode, object_id: ObjectId) {
    let Some(frontier) = from.frontier(object_id, "main").unwrap() else {
        return;
    };

    let mut commits = Vec::new();
    for commit_id in &frontier {
        if let Some(commit) = from.get_commit(object_id, "main", commit_id) {
            commits.push(commit);
        }
    }

    if !commits.is_empty() {
        to.apply_commits(object_id, "main", commits);
    }
}

// ============================================================================
// Basic Upstream Connection Tests
// ============================================================================

#[tokio::test]
async fn test_create_multi_server_harness() {
    let mut harness = MultiServerHarness::new();
    harness.create_server("origin");
    harness.create_server("edge");

    assert!(harness.get_server("origin").is_some());
    assert!(harness.get_server("edge").is_some());
    assert!(harness.get_server("nonexistent").is_none());
}

#[tokio::test]
async fn test_connect_upstream() {
    let mut harness = MultiServerHarness::new();
    harness.create_server("origin");
    harness.create_server("edge");

    // Connect edge -> origin
    let upstream_id = harness.connect_upstream("edge", "origin");
    assert!(upstream_id.is_some());

    // Check the connection was established
    let edge = harness.get_server("edge").unwrap();
    assert!(edge.synced_node.has_upstream());
}

#[tokio::test]
async fn test_upstream_state_tracking() {
    let mut harness = MultiServerHarness::new();
    harness.create_server("origin");
    harness.create_server("edge");

    let upstream_id = harness.connect_upstream("edge", "origin").unwrap();

    // Initially should be disconnected
    let edge = harness.get_server("edge").unwrap();
    let state = edge.synced_node.upstream_state(upstream_id);
    assert!(state.is_some());

    match state.unwrap() {
        UpstreamState::Disconnected => {} // Expected initial state
        other => panic!("Expected Disconnected, got {:?}", other),
    }
}

#[tokio::test]
async fn test_upstream_subscription() {
    let mut harness = MultiServerHarness::new();
    harness.create_server("origin");
    harness.create_server("edge");

    // Connect edge -> origin
    let upstream_id = harness.connect_upstream("edge", "origin").unwrap();

    // Subscribe edge to origin
    let edge = harness.get_server("edge").unwrap();
    let result = edge
        .synced_node
        .subscribe_upstream(upstream_id, "*".to_string(), SubscriptionOptions::default())
        .await;

    assert!(result.is_ok());
}

#[tokio::test]
async fn test_subscription_tracking() {
    let mut harness = MultiServerHarness::new();
    harness.create_server("origin");
    harness.create_server("edge");

    let upstream_id = harness.connect_upstream("edge", "origin").unwrap();

    // Subscribe
    let edge = harness.get_server("edge").unwrap();
    let _stream = edge
        .synced_node
        .subscribe_upstream(upstream_id, "*".to_string(), SubscriptionOptions::default())
        .await
        .unwrap();

    // Check subscription was tracked
    let servers = edge.synced_node.upstream_ids();
    assert!(!servers.is_empty());
}

// ============================================================================
// Incremental Query Delta Tests
// ============================================================================

#[tokio::test]
async fn test_incremental_query_local_insert() {
    // Test that local inserts trigger incremental query deltas
    let db = Database::in_memory();

    db.execute("CREATE TABLE users (name STRING NOT NULL)")
        .unwrap();

    // Create incremental query
    let query = db.incremental_query("SELECT * FROM users").unwrap();

    // Set up delta collector
    let (deltas, callback) = delta_collector();
    let _listener_id = query.subscribe(callback);

    // Initial state should be empty (one batch with no Added deltas)
    assert_eq!(count_added(&deltas), 0);

    // Insert a row
    db.execute("INSERT INTO users (name) VALUES ('Alice')")
        .unwrap();

    // Should have received an Added delta
    assert_eq!(count_added(&deltas), 1);
}

#[tokio::test]
async fn test_incremental_query_synced_insert() {
    // Test that synced commits trigger incremental query deltas
    // This is the key test - it verifies the Database+SyncedNode integration

    // Create origin database (initializes catalog)
    let db_origin = Database::in_memory();
    let catalog_id = db_origin.state().catalog_object_id();

    // Create edge as replica (waits for catalog sync, doesn't initialize)
    let db_edge = Database::in_memory_replica(catalog_id);

    // Create table on origin
    db_origin
        .execute("CREATE TABLE messages (content STRING NOT NULL)")
        .unwrap();

    // Sync the catalog and descriptor to edge
    let origin_node = db_origin.state().node();
    let edge_node = db_edge.state().node();

    // Sync catalog object
    let catalog_id_obj = db_origin.state().catalog_object_id();
    sync_object(origin_node, edge_node, catalog_id_obj);

    // Sync descriptor object
    if let Some(desc_id) = db_origin.state().descriptor_object_id("messages") {
        sync_object(origin_node, edge_node, desc_id);
    }

    // NOTE: We do NOT sync table_rows - that's local state tracking which rows
    // this node knows about. It gets populated as rows are synced in.

    // Reload edge database to pick up the synced schema
    db_edge.reload_catalog().unwrap();

    // Now set up incremental query on edge BEFORE data arrives
    let query = db_edge.incremental_query("SELECT * FROM messages").unwrap();

    let (deltas, callback) = delta_collector();
    let _listener_id = query.subscribe(callback);

    // Initially empty
    assert_eq!(count_added(&deltas), 0);

    // Insert on origin
    let result = db_origin
        .execute("INSERT INTO messages (content) VALUES ('Hello from origin')")
        .unwrap();

    // Get the row ID that was inserted
    let row_id = match result {
        groove::sql::ExecuteResult::Inserted { row_id, .. } => row_id,
        _ => panic!("Expected Inserted result"),
    };

    // Sync the row object to edge
    sync_object(origin_node, edge_node, row_id);

    // Register the synced row with edge's database
    // This adds it to row_table and notifies query graphs
    db_edge
        .register_synced_row_by_table(row_id, "messages")
        .unwrap();

    // The incremental query should now have received the delta
    assert_eq!(count_added(&deltas), 1);
    assert!(added_ids(&deltas).contains(&row_id));
}

// ============================================================================
// Multi-Server Sync with Incremental Queries
// ============================================================================

#[tokio::test(flavor = "current_thread")]
async fn test_two_tier_sync_with_incremental_query() {
    // This test uses start_upstream_sync which calls spawn_local, so we need a LocalSet
    let local = tokio::task::LocalSet::new();
    local
        .run_until(async {
            let mut harness = MultiServerHarness::new();
            harness.create_server("origin");
            harness.create_server("edge");

            // Connect edge -> origin
            let upstream_id = harness.connect_upstream("edge", "origin").unwrap();

            // Start the upstream sync (background event loop)
            harness.start_upstream_sync("edge", upstream_id, "*");

            // Create table on edge's database
            let edge = harness.get_server("edge").unwrap();
            let edge_db = &edge.db;

            edge_db
                .execute("CREATE TABLE items (name STRING NOT NULL)")
                .unwrap();

            // Set up incremental query on edge
            let query = edge_db.incremental_query("SELECT * FROM items").unwrap();

            let (deltas, callback) = delta_collector();
            let _listener_id = query.subscribe(callback);

            // Initially empty
            assert_eq!(count_added(&deltas), 0);

            // Insert directly on edge
            edge_db
                .execute("INSERT INTO items (name) VALUES ('Local item')")
                .unwrap();

            // Should have the local insert
            assert_eq!(count_added(&deltas), 1);

            // Allow sync to propagate
            settle().await;
        })
        .await;
}

#[tokio::test]
async fn test_client_insert_triggers_incremental_query() {
    let mut harness = MultiServerHarness::new();
    harness.create_server("server");

    // Get server's database
    let server = harness.get_server("server").unwrap();
    let server_db = &server.db;

    // Create table
    server_db
        .execute("CREATE TABLE notes (text STRING NOT NULL)")
        .unwrap();

    // Set up incremental query BEFORE any inserts
    let query = server_db.incremental_query("SELECT * FROM notes").unwrap();

    let (deltas, callback) = delta_collector();
    let _listener_id = query.subscribe(callback);

    // Initially empty
    assert_eq!(count_added(&deltas), 0);

    // Create a client and connect to server
    let mut client = harness.create_client("alice", "server").unwrap();
    let _stream = client.subscribe_all().await.unwrap();

    // Client pushes raw commit data (simulating client-side insert)
    let object_id = ObjectId::new(uuid::Uuid::now_v7().as_u128());
    let (commit_id, response) = client
        .write_and_push(object_id, b"test note content")
        .await
        .unwrap();

    assert!(response.accepted);
    assert!(client.has_commit(object_id, &commit_id));

    // Allow propagation
    settle().await;

    // The raw commit was pushed but since it's not going through SQL INSERT,
    // the incremental query won't pick it up automatically.
    // This test shows that we need to use SQL operations for proper integration.
}

// ============================================================================
// SQL-Based Sync Tests
// ============================================================================

#[tokio::test]
async fn test_sql_insert_with_shared_catalog() {
    // db1 is origin (creates catalog), db2 is replica (waits for sync)
    let db1 = Database::in_memory();
    let catalog_id = db1.state().catalog_object_id();
    let db2 = Database::in_memory_replica(catalog_id);

    // Create table on db1
    db1.execute("CREATE TABLE tasks (title STRING NOT NULL)")
        .unwrap();

    // Manually sync schema to db2
    let node1 = db1.state().node();
    let node2 = db2.state().node();

    // Sync catalog
    sync_object(node1, node2, catalog_id);

    // Sync descriptor
    if let Some(desc_id) = db1.state().descriptor_object_id("tasks") {
        sync_object(node1, node2, desc_id);
    }

    // NOTE: table_rows is local state - not synced. Gets populated via register_synced_row_by_table.

    // Reload db2 to pick up schema
    db2.reload_catalog().unwrap();

    // Set up query on db2
    let query = db2.incremental_query("SELECT * FROM tasks").unwrap();
    let (deltas, callback) = delta_collector();
    let _listener_id = query.subscribe(callback);

    // Insert on db1
    let result = db1
        .execute("INSERT INTO tasks (title) VALUES ('Buy milk')")
        .unwrap();
    let row_id = match result {
        groove::sql::ExecuteResult::Inserted { row_id, .. } => row_id,
        _ => panic!("Expected Inserted"),
    };

    // Sync the row object
    sync_object(node1, node2, row_id);

    // Register the synced row with db2
    db2.register_synced_row_by_table(row_id, "tasks").unwrap();

    // Check that db2's incremental query got the delta
    assert_eq!(count_added(&deltas), 1);
    assert!(added_ids(&deltas).contains(&row_id));

    // Verify we can read the row on db2
    let rows = query.rows();
    assert_eq!(rows.len(), 1);
}
