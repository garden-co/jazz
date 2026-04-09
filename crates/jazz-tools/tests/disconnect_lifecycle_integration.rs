#![cfg(feature = "test")]

mod support;

use std::collections::HashMap;
use std::time::Duration;

use jazz_tools::server::TestingServer;
use jazz_tools::{ColumnType, DurabilityTier, QueryBuilder, SchemaBuilder, TableSchema, Value};
use support::{TestingClient, wait_for_query};

const READY_TIMEOUT: Duration = Duration::from_secs(30);
const QUERY_TIMEOUT: Duration = Duration::from_secs(25);

fn test_schema() -> jazz_tools::Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("todos")
                .column("title", ColumnType::Text)
                .column("completed", ColumnType::Boolean)
                .policies(support::allow_all_policies()),
        )
        .build()
}

/// Full HTTP-layer integration test for the disconnect → TTL expiry → reap → reconnect flow.
///
/// ```text
/// alice ──SSE connect──▶ server ──creates todo──▶ edge-settled
///                             │
///                     alice disconnects (shutdown)
///                             │
///                     server sets TTL=1ms, runs sweep → alice reaped
///                             │
///                     alice reconnects with new SSE ──▶ queries work
///                             │
///                     bob connects ──▶ sees alice's todo (data persisted)
/// ```
#[tokio::test]
async fn client_reconnects_after_server_reaps_stale_state() {
    let server = TestingServer::start().await;
    let schema = test_schema();

    // Phase 1: Alice connects and creates a todo
    let alice = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("alice-lifecycle")
        .ready_on("todos", READY_TIMEOUT)
        .connect()
        .await;

    let (todo_id, _) = alice
        .create(
            "todos",
            HashMap::from([
                ("title".to_string(), Value::Text("survive-reap".to_string())),
                ("completed".to_string(), Value::Boolean(false)),
            ]),
        )
        .await
        .expect("create todo");

    // Wait for edge-settlement so the data is persisted server-side
    wait_for_query(
        &alice,
        QueryBuilder::new("todos").build(),
        Some(DurabilityTier::EdgeServer),
        QUERY_TIMEOUT,
        "alice's todo edge-settled",
        |rows| {
            if rows.len() == 1 { Some(()) } else { None }
        },
    )
    .await;

    // Phase 2: Alice disconnects
    alice.shutdown().await.expect("alice shutdown");

    // Phase 3: Server reaps alice's client state
    // Wait for the SSE stream cleanup to register the disconnect candidate.
    // The cleanup runs asynchronously when the stream is dropped.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while server.disconnect_candidate_count().await == 0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for disconnect candidate"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Set TTL to 1ms so alice is immediately eligible for reaping
    server.set_client_ttl(Duration::from_millis(1)).await;
    // Small sleep to ensure the disconnect timestamp is past the TTL
    tokio::time::sleep(Duration::from_millis(10)).await;
    let reaped = server.run_sweep_once().await;
    assert!(
        !reaped.is_empty(),
        "server should have reaped at least one client"
    );

    // Phase 4: Alice reconnects with a fresh SSE connection
    let alice_reconnected = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("alice-lifecycle")
        .ready_on("todos", READY_TIMEOUT)
        .connect()
        .await;

    // Alice can still query and see her data (persisted in server storage)
    let rows = wait_for_query(
        &alice_reconnected,
        QueryBuilder::new("todos").build(),
        Some(DurabilityTier::EdgeServer),
        QUERY_TIMEOUT,
        "alice sees todo after reconnect",
        |rows| {
            if rows.len() == 1 {
                Some(rows.to_vec())
            } else {
                None
            }
        },
    )
    .await;
    assert_eq!(rows[0].0, todo_id, "same todo should be visible");

    // Phase 5: Alice can still mutate after reconnection
    alice_reconnected
        .update(
            todo_id,
            vec![(
                "title".to_string(),
                Value::Text("updated-after-reap".to_string()),
            )],
        )
        .await
        .expect("update after reconnect");

    wait_for_query(
        &alice_reconnected,
        QueryBuilder::new("todos").build(),
        Some(DurabilityTier::EdgeServer),
        QUERY_TIMEOUT,
        "alice's update settles after reconnect",
        |rows| {
            if rows.len() == 1 {
                let title = &rows[0].1[0];
                if *title == Value::Text("updated-after-reap".to_string()) {
                    Some(())
                } else {
                    None
                }
            } else {
                None
            }
        },
    )
    .await;

    // Phase 6: Bob connects and sees alice's data (proves server storage intact)
    let bob = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("bob-lifecycle")
        .ready_on("todos", READY_TIMEOUT)
        .connect()
        .await;

    let bob_rows = wait_for_query(
        &bob,
        QueryBuilder::new("todos").build(),
        Some(DurabilityTier::EdgeServer),
        QUERY_TIMEOUT,
        "bob sees alice's todo",
        |rows| {
            if rows.len() == 1 {
                Some(rows.to_vec())
            } else {
                None
            }
        },
    )
    .await;
    assert_eq!(bob_rows[0].0, todo_id);
    assert_eq!(
        bob_rows[0].1[0],
        Value::Text("updated-after-reap".to_string()),
        "bob should see the post-reap update"
    );

    alice_reconnected
        .shutdown()
        .await
        .expect("alice_reconnected shutdown");
    bob.shutdown().await.expect("bob shutdown");
    server.shutdown().await;
}

/// Verifies that multiple clients can be reaped independently and the surviving
/// client's data and queries remain unaffected.
///
/// ```text
/// alice ──creates todo──▶ server ◀──creates todo── bob
///                             │
///                     alice disconnects, bob stays
///                             │
///                     sweep reaps alice only
///                             │
///                     bob queries ──▶ sees both todos (server data intact)
/// ```
#[tokio::test]
async fn sweep_reaps_disconnected_client_without_affecting_connected_client() {
    let server = TestingServer::start().await;
    let schema = test_schema();

    let alice = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("alice-selective")
        .ready_on("todos", READY_TIMEOUT)
        .connect()
        .await;

    let bob = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("bob-selective")
        .ready_on("todos", READY_TIMEOUT)
        .connect()
        .await;

    // Both create a todo
    alice
        .create(
            "todos",
            HashMap::from([
                ("title".to_string(), Value::Text("alice-todo".to_string())),
                ("completed".to_string(), Value::Boolean(false)),
            ]),
        )
        .await
        .expect("alice create");

    bob.create(
        "todos",
        HashMap::from([
            ("title".to_string(), Value::Text("bob-todo".to_string())),
            ("completed".to_string(), Value::Boolean(false)),
        ]),
    )
    .await
    .expect("bob create");

    // Wait for both to be edge-settled
    wait_for_query(
        &bob,
        QueryBuilder::new("todos").build(),
        Some(DurabilityTier::EdgeServer),
        QUERY_TIMEOUT,
        "both todos settled",
        |rows| if rows.len() == 2 { Some(()) } else { None },
    )
    .await;

    // Alice disconnects
    alice.shutdown().await.expect("alice shutdown");

    // Wait for disconnect candidate to appear
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while server.disconnect_candidate_count().await == 0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for disconnect candidate"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Reap alice
    server.set_client_ttl(Duration::from_millis(1)).await;
    tokio::time::sleep(Duration::from_millis(10)).await;
    let reaped = server.run_sweep_once().await;
    assert!(!reaped.is_empty(), "alice should be reaped");

    // Bob is unaffected — can still query and see both todos
    let bob_rows = wait_for_query(
        &bob,
        QueryBuilder::new("todos").build(),
        Some(DurabilityTier::EdgeServer),
        QUERY_TIMEOUT,
        "bob still sees both todos after alice reaped",
        |rows| {
            if rows.len() == 2 {
                Some(rows.to_vec())
            } else {
                None
            }
        },
    )
    .await;
    assert_eq!(bob_rows.len(), 2);

    // Bob can still create
    bob.create(
        "todos",
        HashMap::from([
            ("title".to_string(), Value::Text("bob-todo-2".to_string())),
            ("completed".to_string(), Value::Boolean(false)),
        ]),
    )
    .await
    .expect("bob create after reap");

    wait_for_query(
        &bob,
        QueryBuilder::new("todos").build(),
        Some(DurabilityTier::EdgeServer),
        QUERY_TIMEOUT,
        "bob's new todo settles",
        |rows| if rows.len() == 3 { Some(()) } else { None },
    )
    .await;

    bob.shutdown().await.expect("bob shutdown");
    server.shutdown().await;
}

/// Verifies that the background sweep task (spawned in ServerBuilder) fires
/// automatically and reaps expired candidates without manual run_sweep_once.
///
/// ```text
/// alice ──SSE connect──▶ server ──disconnects──▶ candidate registered
///                             │
///                     TTL set to 1ms, background sweep fires (30s interval)
///                             │
///                     alice reaped by background sweep
/// ```
#[tokio::test]
async fn background_sweep_task_reaps_expired_candidates() {
    let server = TestingServer::start().await;
    let schema = test_schema();

    let alice = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema)
        .with_user_id("alice-bg-sweep")
        .ready_on("todos", READY_TIMEOUT)
        .connect()
        .await;

    alice.shutdown().await.expect("alice shutdown");

    // Wait for disconnect candidate
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while server.disconnect_candidate_count().await == 0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for disconnect candidate"
        );
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // Set TTL to 1ms so the background sweep will reap alice on its next tick
    server.set_client_ttl(Duration::from_millis(1)).await;

    // Wait for the background sweep task to fire and reap alice.
    // The sweep runs every 30s, but we wait up to 35s to account for timing.
    let deadline = tokio::time::Instant::now() + Duration::from_secs(35);
    while server.disconnect_candidate_count().await > 0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for background sweep to reap candidate"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    // Candidate was reaped by the background task (not by manual run_sweep_once)
    assert_eq!(
        server.disconnect_candidate_count().await,
        0,
        "background sweep should have reaped alice"
    );

    server.shutdown().await;
}
