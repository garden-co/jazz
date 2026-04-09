#![cfg(feature = "test")]

mod support;

use std::collections::HashMap;
use std::time::Duration;

use jazz_tools::server::TestingServer;
use jazz_tools::sync_tracer::SyncTracer;
use jazz_tools::{ColumnType, DurabilityTier, QueryBuilder, SchemaBuilder, TableSchema, Value};
use support::{TestingClient, wait_for_query};

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

/// Alice creates a todo, bob sees it. The tracer captures the full flow.
///
/// ```text
/// alice ──ObjectUpdated──► server ──ObjectUpdated──► bob
///       ◄──PersistenceAck──
/// ```
#[tokio::test]
async fn alice_write_bob_read() {
    let tracer = SyncTracer::new();

    let server = TestingServer::builder()
        .with_tracer(tracer.clone())
        .start()
        .await;

    let alice = TestingClient::builder()
        .with_server(&server)
        .with_schema(test_schema())
        .with_user_id("alice")
        .with_tracer(&tracer, "alice")
        .ready_on("todos", Duration::from_secs(30))
        .connect()
        .await;

    let bob = TestingClient::builder()
        .with_server(&server)
        .with_schema(test_schema())
        .with_user_id("bob")
        .with_tracer(&tracer, "bob")
        .ready_on("todos", Duration::from_secs(30))
        .connect()
        .await;

    // Clear catalogue/schema setup noise
    tracer.clear();

    alice
        .create(
            "todos",
            HashMap::from([
                ("title".to_string(), Value::Text("traced-todo".to_string())),
                ("completed".to_string(), Value::Boolean(false)),
            ]),
        )
        .await
        .expect("alice creates todo");

    wait_for_query(
        &bob,
        QueryBuilder::new("todos").build(),
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(25),
        "bob sees traced-todo",
        |rows| (rows.len() == 1).then_some(rows),
    )
    .await;

    insta::assert_snapshot!(tracer.tally(), @"
    alice    -> server  : ObjectUpdated (1)
    alice    => server  : ObjectUpdated (1)
    bob      -> server  : QuerySubscription (1), QueryUnsubscription (1)
    bob      => server  : QuerySubscription (1), QueryUnsubscription (1)
    server   -> alice   : PersistenceAck (2)
    server   -> bob     : ObjectUpdated (1), QuerySettled (2)
    server   => alice   : PersistenceAck (2)
    server   => bob     : ObjectUpdated (1), QuerySettled (2)
    ");

    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    server.shutdown().await;
}

/// Bob updates alice's todo. The round-trip is visible in the trace.
///
/// ```text
/// alice ──create──► server ──► bob
/// bob   ──update──► server ──► alice
/// ```
#[tokio::test]
async fn bob_updates_alice_todo() {
    let tracer = SyncTracer::new();

    let server = TestingServer::builder()
        .with_tracer(tracer.clone())
        .start()
        .await;

    let alice = TestingClient::builder()
        .with_server(&server)
        .with_schema(test_schema())
        .with_user_id("alice")
        .with_tracer(&tracer, "alice")
        .ready_on("todos", Duration::from_secs(30))
        .connect()
        .await;

    let bob = TestingClient::builder()
        .with_server(&server)
        .with_schema(test_schema())
        .with_user_id("bob")
        .with_tracer(&tracer, "bob")
        .ready_on("todos", Duration::from_secs(30))
        .connect()
        .await;

    tracer.clear();

    // Alice creates
    let (todo_id, _) = alice
        .create(
            "todos",
            HashMap::from([
                ("title".to_string(), Value::Text("collab-todo".to_string())),
                ("completed".to_string(), Value::Boolean(false)),
            ]),
        )
        .await
        .expect("alice creates todo");

    // Bob sees it
    wait_for_query(
        &bob,
        QueryBuilder::new("todos").build(),
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(25),
        "bob sees todo",
        |rows| (rows.len() == 1).then_some(rows),
    )
    .await;

    tracer.clear();

    // Bob updates
    bob.update(
        todo_id,
        vec![("completed".to_string(), Value::Boolean(true))],
    )
    .await
    .expect("bob updates todo");

    // Alice sees the update
    wait_for_query(
        &alice,
        QueryBuilder::new("todos").build(),
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(25),
        "alice sees completed=true",
        |rows| {
            rows.iter()
                .any(|(_, vals)| vals.iter().any(|v| matches!(v, Value::Boolean(true))))
                .then_some(rows)
        },
    )
    .await;

    tracer.wait_until_settled(Duration::from_secs(10)).await;

    // Assert message flow shape (types present, not exact counts — the server
    // may batch ObjectUpdated messages non-deterministically).
    tracer.expect_contains(
        "
        alice    -> server   QuerySubscription
        server   -> alice    ObjectUpdated
        server   -> alice    QuerySettled
    ",
    );
    tracer.expect_contains(
        "
        bob      -> server   ObjectUpdated
        server   -> bob      PersistenceAck
    ",
    );

    // Bob sent an ObjectUpdated to server
    let bob_sent = tracer.from("bob");
    assert!(
        bob_sent.iter().any(|m| m.is_object_updated()),
        "bob should have sent ObjectUpdated"
    );

    // Alice received at least one ObjectUpdated from server
    let alice_recv = tracer.to("alice");
    assert!(
        alice_recv.iter().any(|m| m.is_object_updated()),
        "alice should have received ObjectUpdated from server"
    );

    // Bob received PersistenceAck from server
    let bob_recv = tracer.to("bob");
    assert!(
        bob_recv.iter().any(|m| m.is_persistence_ack()),
        "bob should have received PersistenceAck"
    );

    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    server.shutdown().await;
}

/// Single-writer flow: alice writes, server acks.
#[tokio::test]
async fn single_writer_flow() {
    let tracer = SyncTracer::new();

    let server = TestingServer::builder()
        .with_tracer(tracer.clone())
        .start()
        .await;

    let alice = TestingClient::builder()
        .with_server(&server)
        .with_schema(test_schema())
        .with_user_id("alice")
        .with_tracer(&tracer, "alice")
        .ready_on("todos", Duration::from_secs(30))
        .connect()
        .await;

    tracer.clear();

    alice
        .create(
            "todos",
            HashMap::from([
                ("title".to_string(), Value::Text("solo-todo".to_string())),
                ("completed".to_string(), Value::Boolean(false)),
            ]),
        )
        .await
        .expect("create todo");

    tracer.wait_until_settled(Duration::from_secs(10)).await;

    insta::assert_snapshot!(tracer.tally(), @"
    alice    -> server  : ObjectUpdated (1), QueryUnsubscription (1)
    alice    => server  : ObjectUpdated (1)
    server   -> alice   : PersistenceAck (2)
    server   => alice   : PersistenceAck (2)
    ");

    alice.shutdown().await.expect("shutdown alice");
    server.shutdown().await;
}

/// Named objects: trace shows "my-todo" instead of hex IDs.
///
/// Single-writer so message order is deterministic — safe for `trace()` snapshots.
#[tokio::test]
async fn named_object_trace() {
    let tracer = SyncTracer::new();

    let server = TestingServer::builder()
        .with_tracer(tracer.clone())
        .start()
        .await;

    let alice = TestingClient::builder()
        .with_server(&server)
        .with_schema(test_schema())
        .with_user_id("alice")
        .with_tracer(&tracer, "alice")
        .ready_on("todos", Duration::from_secs(30))
        .connect()
        .await;

    tracer.clear();

    let (todo_id, _) = alice
        .create(
            "todos",
            HashMap::from([
                ("title".to_string(), Value::Text("buy milk".to_string())),
                ("completed".to_string(), Value::Boolean(false)),
            ]),
        )
        .await
        .expect("create todo");

    tracer.register_object(todo_id, "my-todo");

    tracer.wait_until_settled(Duration::from_secs(10)).await;

    insta::assert_snapshot!(tracer.trace_normalized(), @"
    # => sent, -> received
    alice    => server    ObjectUpdated        obj:my-todo branch:main commits:[C1]
    alice    -> server    ObjectUpdated        obj:my-todo branch:main commits:[C1]
    alice    -> server    QueryUnsubscription  query:0
    server   => alice     PersistenceAck       obj:my-todo branch:main confirmed:[C1] tier:EdgeServer
    server   -> alice     PersistenceAck       obj:my-todo branch:main confirmed:[C1] tier:EdgeServer
    server   => alice     PersistenceAck       obj:my-todo branch:main confirmed:[C1] tier:GlobalServer
    server   -> alice     PersistenceAck       obj:my-todo branch:main confirmed:[C1] tier:GlobalServer
    ");

    alice.shutdown().await.expect("shutdown alice");
    server.shutdown().await;
}
