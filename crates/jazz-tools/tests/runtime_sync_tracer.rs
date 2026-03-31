#![cfg(feature = "test")]

//! Tests for the RuntimeCore-level sync tracer.
//!
//! These tests use `JazzClient::enable_sync_tracer()` — the same code path
//! that WASM (`runtime.enableSyncTracer()`) and NAPI (`runtime.enableSyncTracer()`)
//! expose to JavaScript. They verify the tracer captures messages flowing
//! through RuntimeCore, not the external hooks in client.rs/routes.rs.

mod support;

use std::collections::HashMap;
use std::time::Duration;

use jazz_tools::server::TestingServer;
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

/// The RuntimeCore tracer captures outgoing ObjectUpdated and incoming PersistenceAck.
///
/// This is the code path WASM/NAPI users call:
///   runtime.enableSyncTracer();
///   // ... operations ...
///   runtime.syncTracerTally(); // or dump, summary, etc.
#[tokio::test]
async fn runtime_tracer_captures_write_and_ack() {
    let server = TestingServer::start().await;

    let alice = TestingClient::builder()
        .with_server(&server)
        .with_schema(test_schema())
        .with_user_id("alice")
        .ready_on("todos", Duration::from_secs(30))
        .connect()
        .await;

    // Enable tracer on the runtime — same as WASM/NAPI would do
    alice.enable_sync_tracer();

    alice
        .create(
            "todos",
            HashMap::from([
                ("title".to_string(), Value::Text("traced".to_string())),
                ("completed".to_string(), Value::Boolean(false)),
            ]),
        )
        .await
        .expect("create todo");

    // Wait for server round-trip
    tokio::time::sleep(Duration::from_millis(500)).await;

    let snapshot = alice.sync_tracer().expect("tracer should be enabled");

    // Should have outgoing ObjectUpdateds and incoming PersistenceAcks
    assert!(snapshot.count > 0, "tracer should have recorded messages");
    assert!(
        snapshot.tally.contains("ObjectUpdated"),
        "tally should contain ObjectUpdated: {}",
        snapshot.tally,
    );
    assert!(
        snapshot.tally.contains("PersistenceAck"),
        "tally should contain PersistenceAck: {}",
        snapshot.tally,
    );

    // The tally uses tier_label as participant name.
    // Client tier_label is "unknown" by default (set in RuntimeCore::new).
    println!("=== Tally ===\n{}", snapshot.tally);
    println!("=== Summary ===\n{}", snapshot.summary);

    alice.shutdown().await.expect("shutdown");
    server.shutdown().await;
}

/// Two clients: alice writes, bob reads. Both have tracers enabled.
/// Alice's tracer sees outgoing ObjectUpdated. Bob's tracer sees incoming ObjectUpdated.
#[tokio::test]
async fn runtime_tracer_two_clients() {
    let server = TestingServer::start().await;

    let alice = TestingClient::builder()
        .with_server(&server)
        .with_schema(test_schema())
        .with_user_id("alice")
        .ready_on("todos", Duration::from_secs(30))
        .connect()
        .await;

    let bob = TestingClient::builder()
        .with_server(&server)
        .with_schema(test_schema())
        .with_user_id("bob")
        .ready_on("todos", Duration::from_secs(30))
        .connect()
        .await;

    // Enable tracers
    alice.enable_sync_tracer();
    bob.enable_sync_tracer();

    alice
        .create(
            "todos",
            HashMap::from([
                ("title".to_string(), Value::Text("for-bob".to_string())),
                ("completed".to_string(), Value::Boolean(false)),
            ]),
        )
        .await
        .expect("create todo");

    wait_for_query(
        &bob,
        QueryBuilder::new("todos").build(),
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(25),
        "bob sees todo",
        |rows| (rows.len() == 1).then_some(rows),
    )
    .await;

    let alice_snap = alice.sync_tracer().expect("alice tracer enabled");
    let bob_snap = bob.sync_tracer().expect("bob tracer enabled");

    println!("=== Alice tally ===\n{}", alice_snap.tally);
    println!("=== Bob tally ===\n{}", bob_snap.tally);

    // Alice sent ObjectUpdated (outgoing)
    assert!(
        alice_snap.tally.contains("ObjectUpdated"),
        "alice should have sent ObjectUpdated",
    );

    // Bob received ObjectUpdated (incoming from server)
    assert!(
        bob_snap.tally.contains("ObjectUpdated"),
        "bob should have received ObjectUpdated",
    );

    // Bob received QuerySettled (incoming from server)
    assert!(
        bob_snap.tally.contains("QuerySettled"),
        "bob should have received QuerySettled",
    );

    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    server.shutdown().await;
}

/// Tracer is off by default — sync_tracer() returns None.
#[tokio::test]
async fn runtime_tracer_disabled_by_default() {
    let server = TestingServer::start().await;

    let alice = TestingClient::builder()
        .with_server(&server)
        .with_schema(test_schema())
        .with_user_id("alice")
        .ready_on("todos", Duration::from_secs(30))
        .connect()
        .await;

    // Don't enable tracer
    assert!(
        alice.sync_tracer().is_none(),
        "tracer should be None when not enabled",
    );

    alice.shutdown().await.expect("shutdown");
    server.shutdown().await;
}

/// trace_normalized() output is stable and uses => for send, -> for recv.
#[tokio::test]
async fn runtime_tracer_normalized_output() {
    let server = TestingServer::start().await;

    let alice = TestingClient::builder()
        .with_server(&server)
        .with_schema(test_schema())
        .with_user_id("alice")
        .ready_on("todos", Duration::from_secs(30))
        .connect()
        .await;

    alice.enable_sync_tracer();

    alice
        .create(
            "todos",
            HashMap::from([
                ("title".to_string(), Value::Text("normalized".to_string())),
                ("completed".to_string(), Value::Boolean(false)),
            ]),
        )
        .await
        .expect("create todo");

    tokio::time::sleep(Duration::from_millis(500)).await;

    let snapshot = alice.sync_tracer().expect("tracer enabled");

    println!("=== trace_normalized ===\n{}", snapshot.trace_normalized);

    // Header line
    assert!(
        snapshot
            .trace_normalized
            .starts_with("# => sent, -> received"),
        "should start with arrow legend",
    );

    // Should contain => (outgoing from runtime)
    assert!(
        snapshot.trace_normalized.contains("=>"),
        "should have => for outgoing messages",
    );

    // Should contain -> (incoming to runtime)
    assert!(
        snapshot.trace_normalized.contains("->"),
        "should have -> for incoming messages",
    );

    // Should use auto-named commits (C1, C2, etc.)
    assert!(
        snapshot.trace_normalized.contains("commits:[C"),
        "should use auto-named commits: {}",
        snapshot.trace_normalized,
    );

    alice.shutdown().await.expect("shutdown");
    server.shutdown().await;
}
