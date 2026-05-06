#![cfg(feature = "test")]

mod support;

use std::collections::HashMap;
use std::time::Duration;

use jazz_tools::server::TestingServer;
use jazz_tools::sync_manager::SyncPayload;
use jazz_tools::sync_tracer::SyncTracer;
use jazz_tools::{ColumnType, DurabilityTier, QueryBuilder, SchemaBuilder, TableSchema, Value};
use support::{TestingClient, wait_for_query};

fn test_schema() -> jazz_tools::Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("todos")
                .column("title", ColumnType::Text)
                .column("completed", ColumnType::Boolean),
        )
        .build()
}

/// Alice creates a todo, bob sees it. The tracer captures the full flow.
///
/// ```text
/// alice ──RowBatchCreated────► server ──RowBatchNeeded────► bob
///       ◄──BatchFate──────
/// ```
#[tokio::test]
async fn alice_write_bob_read() {
    let tracer = SyncTracer::new();
    let schema = test_schema();

    let server = TestingServer::builder()
        .with_schema(schema.clone())
        .with_tracer(tracer.clone())
        .start()
        .await;

    let alice = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("alice")
        .with_tracer(&tracer, "alice")
        .ready_on("todos", Duration::from_secs(30))
        .connect()
        .await;

    let bob = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema)
        .with_user_id("bob")
        .with_tracer(&tracer, "bob")
        .ready_on("todos", Duration::from_secs(30))
        .connect()
        .await;

    // Clear catalogue/schema setup noise
    tracer.clear();

    alice
        .create_persisted(
            "todos",
            HashMap::from([
                ("title".to_string(), Value::Text("traced-todo".to_string())),
                ("completed".to_string(), Value::Boolean(false)),
            ]),
            DurabilityTier::EdgeServer,
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

    let alice_sent = tracer.from("alice");
    assert!(
        alice_sent.iter().any(|message| message.is_object_updated()),
        "alice should send a row batch to the server"
    );

    let bob_sent = tracer.from("bob");
    assert!(
        bob_sent
            .iter()
            .any(|message| message.payload.variant_name() == "QuerySubscription"),
        "bob should subscribe before receiving the row"
    );

    let alice_received = tracer.to("alice");
    assert!(
        alice_received
            .iter()
            .any(|message| message.payload.variant_name() == "BatchFate"),
        "alice should receive a settlement for the created row batch"
    );
    let bob_received = tracer
        .between("server", "bob")
        .into_iter()
        .filter(|message| {
            message.side == jazz_tools::sync_tracer::Side::Recv
                && message.from.name() == "server"
                && message.to.name() == "bob"
        })
        .collect::<Vec<_>>();
    let bob_sent_from_server = tracer
        .between("server", "bob")
        .into_iter()
        .filter(|message| {
            message.side == jazz_tools::sync_tracer::Side::Send
                && message.from.name() == "server"
                && message.to.name() == "bob"
        })
        .collect::<Vec<_>>();

    for messages in [&bob_received, &bob_sent_from_server] {
        assert!(
            messages
                .iter()
                .any(|message| message.payload.variant_name() == "BatchFate"),
            "bob should see a batch settlement from the server"
        );
        assert!(
            messages.iter().any(|message| matches!(
                &message.payload,
                SyncPayload::QuerySettled { scope, .. } if !scope.is_empty()
            )),
            "bob should see query settlement carrying the server scope"
        );
        assert!(
            messages.iter().any(|message| message.is_object_updated()),
            "bob should receive the created row batch from the server"
        );
    }

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
    let schema = test_schema();

    let server = TestingServer::builder()
        .with_schema(schema.clone())
        .with_tracer(tracer.clone())
        .start()
        .await;

    let alice = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("alice")
        .with_tracer(&tracer, "alice")
        .ready_on("todos", Duration::from_secs(30))
        .connect()
        .await;

    let bob = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema)
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
    bob.update_persisted(
        todo_id,
        vec![("completed".to_string(), Value::Boolean(true))],
        DurabilityTier::EdgeServer,
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

    // Assert message flow shape (types present, not exact counts).
    tracer.expect_contains(
        "
        alice    -> server   QuerySubscription
        server   -> alice    RowBatchNeeded
        server   -> alice    QuerySettled
    ",
    );
    tracer.expect_contains(
        "
        bob      -> server   RowBatchCreated
        server   -> bob      BatchFate
    ",
    );

    // Bob sent a row update to the server.
    let bob_sent = tracer.from("bob");
    assert!(
        bob_sent.iter().any(|m| m.is_object_updated()),
        "bob should have sent a row update"
    );

    // Alice received at least one row update from server.
    let alice_recv = tracer.to("alice");
    assert!(
        alice_recv.iter().any(|m| m.is_object_updated()),
        "alice should have received a row update from server"
    );

    // Bob received a batch-level durability confirmation from server.
    let bob_recv = tracer.to("bob");
    assert!(
        bob_recv
            .iter()
            .any(|m| m.payload.variant_name() == "BatchFate"),
        "bob should have received a batch settlement"
    );

    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    server.shutdown().await;
}

/// Single-writer flow: alice writes, server confirms durability.
#[tokio::test]
async fn single_writer_flow() {
    let tracer = SyncTracer::new();
    let schema = test_schema();

    let server = TestingServer::builder()
        .with_schema(schema.clone())
        .with_tracer(tracer.clone())
        .start()
        .await;

    let alice = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema)
        .with_user_id("alice")
        .with_tracer(&tracer, "alice")
        .ready_on("todos", Duration::from_secs(30))
        .connect()
        .await;

    tracer.clear();

    alice
        .create_persisted(
            "todos",
            HashMap::from([
                ("title".to_string(), Value::Text("solo-todo".to_string())),
                ("completed".to_string(), Value::Boolean(false)),
            ]),
            DurabilityTier::EdgeServer,
        )
        .await
        .expect("create todo");

    tracer.wait_until_settled(Duration::from_secs(10)).await;

    insta::assert_snapshot!(tracer.tally(), @"
    alice    -> server  : QueryUnsubscription (1), RowBatchCreated (1), SealBatch (1)
    alice    => server  : RowBatchCreated (1), SealBatch (1)
    server   -> alice   : BatchFate (1)
    server   => alice   : BatchFate (1)
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
    let schema = test_schema();

    let server = TestingServer::builder()
        .with_schema(schema.clone())
        .with_tracer(tracer.clone())
        .start()
        .await;

    let alice = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema)
        .with_user_id("alice")
        .with_tracer(&tracer, "alice")
        .ready_on("todos", Duration::from_secs(30))
        .connect()
        .await;

    tracer.clear();

    let (todo_id, _) = alice
        .create_persisted(
            "todos",
            HashMap::from([
                ("title".to_string(), Value::Text("buy milk".to_string())),
                ("completed".to_string(), Value::Boolean(false)),
            ]),
            DurabilityTier::EdgeServer,
        )
        .await
        .expect("create todo");

    tracer.register_object(todo_id, "my-todo");

    tracer.wait_until_settled(Duration::from_secs(10)).await;

    insta::assert_snapshot!(tracer.trace_normalized(), @"
    # => sent, -> received
    alice    -> server    QueryUnsubscription  query:0
    alice    => server    RowBatchCreated      created row:my-todo branch:main batch:B1
    alice    -> server    RowBatchCreated      created row:my-todo branch:main batch:B1
    alice    => server    SealBatch            seal batch:B1 target:main members:[row:my-todo] frontier:0
    alice    -> server    SealBatch            seal batch:B1 target:main members:[row:my-todo] frontier:0
    server   => alice     BatchFate            durable_direct batch:B1 tier:GlobalServer
    server   -> alice     BatchFate            durable_direct batch:B1 tier:GlobalServer
    ");

    alice.shutdown().await.expect("shutdown alice");
    server.shutdown().await;
}
