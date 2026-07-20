#![cfg(feature = "test")]

mod support;

use std::sync::LazyLock;
use std::time::Duration;

use jazz_tools::server::JazzServer;
use jazz_tools::{
    ColumnDescriptor, ColumnMergeStrategy, ColumnType, DurabilityTier, JazzClient, QueryBuilder,
    RowDescriptor, Schema, TableName, TableSchema, Value,
};
use support::{TestingClient, wait_for_query};

const READY_TIMEOUT: Duration = Duration::from_secs(30);
const QUERY_TIMEOUT: Duration = Duration::from_secs(25);
static COUNTER_SUITE_LOCK: LazyLock<tokio::sync::Mutex<()>> =
    LazyLock::new(|| tokio::sync::Mutex::new(()));

fn counter_schema() -> Schema {
    let counters = TableSchema::new(RowDescriptor::new(vec![
        ColumnDescriptor::new("value", ColumnType::Integer)
            .merge_strategy(ColumnMergeStrategy::Counter),
    ]));
    Schema::from([(TableName::new("counters"), counters)])
}

fn counter_value(row: &(jazz_tools::ObjectId, Vec<Value>)) -> Option<i32> {
    match row.1.first() {
        Some(Value::Integer(value)) => Some(*value),
        _ => None,
    }
}

#[tokio::test]
/// Keeps concurrent counter deltas exact across persistent restart and replay.
///
/// ```text
/// bob ── offline +3 ──► server ◄── +5 ── alice
/// charlie ◄──────────── merged 8 ─────────── server
/// ```
async fn counter_merge_does_not_recount_a_writer_after_restart_and_reconnect() {
    let _suite_guard = COUNTER_SUITE_LOCK.lock().await;
    let schema = counter_schema();
    let server = JazzServer::start_with_schema(schema.clone()).await;

    let alice = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("alice-counter-reconnect")
        .ready_on("counters", READY_TIMEOUT)
        .connect()
        .await;
    let (mut bob_context, bob) = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("bob-counter-reconnect")
        .with_persistent_storage()
        .ready_on("counters", READY_TIMEOUT)
        .connect_with_context()
        .await;

    let (counter_id, _, _) = alice
        .insert("counters", jazz_tools::row_input!("value" => 0))
        .expect("alice creates counter");
    let query = QueryBuilder::new("counters").build();
    wait_for_query(
        &bob,
        query.clone(),
        Some(DurabilityTier::EdgeServer),
        QUERY_TIMEOUT,
        "bob sees the counter base",
        |rows| {
            (rows.len() == 1 && rows[0].0 == counter_id && counter_value(&rows[0]) == Some(0))
                .then_some(())
        },
    )
    .await;

    // Bob keeps the shared base locally, then goes offline before writing.
    // Alice's +5 and Bob's offline +3 are concurrent absolute snapshots, so
    // the server must merge their MRCA-relative deltas to the canonical 8.
    bob.shutdown()
        .await
        .expect("bob shuts down before offline write");
    bob_context.server_url = String::new();
    let bob_offline = JazzClient::connect(bob_context.clone())
        .await
        .expect("bob opens his persistent storage offline");
    bob_offline
        .update(counter_id, vec![("value".to_string(), Value::Integer(3))])
        .expect("bob writes +3");
    bob_offline
        .shutdown()
        .await
        .expect("bob shuts down after offline write");
    bob_context.server_url = server.base_url();

    let alice_batch = alice
        .update(counter_id, vec![("value".to_string(), Value::Integer(5))])
        .expect("alice writes +5");
    alice
        .wait_for_batch(alice_batch, DurabilityTier::EdgeServer)
        .await
        .expect("alice's write settles at the server");

    let observer = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("charlie-counter-observer")
        .ready_on("counters", READY_TIMEOUT)
        .connect()
        .await;

    let bob_restarted = JazzClient::connect(bob_context)
        .await
        .expect("bob reconnects with persistent storage");
    wait_for_query(
        &alice,
        query.clone(),
        Some(DurabilityTier::EdgeServer),
        QUERY_TIMEOUT,
        "live Alice sees the canonical counter",
        |rows| {
            (rows.len() == 1 && rows[0].0 == counter_id && counter_value(&rows[0]) == Some(8))
                .then_some(())
        },
    )
    .await;
    wait_for_query(
        &observer,
        query.clone(),
        Some(DurabilityTier::EdgeServer),
        QUERY_TIMEOUT,
        "fresh observer sees the canonical counter",
        |rows| {
            (rows.len() == 1 && rows[0].0 == counter_id && counter_value(&rows[0]) == Some(8))
                .then_some(())
        },
    )
    .await;
    wait_for_query(
        &bob_restarted,
        query,
        Some(DurabilityTier::EdgeServer),
        QUERY_TIMEOUT,
        "restarted Bob converges to the canonical counter",
        |rows| {
            (rows.len() == 1 && rows[0].0 == counter_id && counter_value(&rows[0]) == Some(8))
                .then_some(())
        },
    )
    .await;

    bob_restarted
        .shutdown()
        .await
        .expect("shutdown restarted Bob");
    observer.shutdown().await.expect("shutdown observer");
    alice.shutdown().await.expect("shutdown Alice");
    server.shutdown().await;
}

#[tokio::test]
/// Keeps causal counter snapshots from being counted as independent roots.
///
/// ```text
/// alice ── 0 ──► bob
/// alice ── 2 ──► server ──► bob
/// alice ── 3 (parent 2) ──► server ──► bob
/// charlie ───────────── fresh observer ───────────► 3
/// ```
async fn counter_merge_does_not_recount_causal_updates_for_live_clients() {
    let _suite_guard = COUNTER_SUITE_LOCK.lock().await;
    let schema = counter_schema();
    let server = JazzServer::start_with_schema(schema.clone()).await;

    let alice = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("alice-counter-causal")
        .ready_on("counters", READY_TIMEOUT)
        .connect()
        .await;
    let bob = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("bob-counter-causal")
        .ready_on("counters", READY_TIMEOUT)
        .connect()
        .await;

    let (counter_id, _, _) = alice
        .insert("counters", jazz_tools::row_input!("value" => 0))
        .expect("alice creates counter");
    let query = QueryBuilder::new("counters").build();
    wait_for_query(
        &bob,
        query.clone(),
        Some(DurabilityTier::EdgeServer),
        QUERY_TIMEOUT,
        "bob sees the causal counter base",
        |rows| {
            (rows.len() == 1 && rows[0].0 == counter_id && counter_value(&rows[0]) == Some(0))
                .then_some(())
        },
    )
    .await;

    let first_batch = alice
        .update(counter_id, vec![("value".to_string(), Value::Integer(2))])
        .expect("alice writes the first causal snapshot");
    alice
        .wait_for_batch(first_batch, DurabilityTier::EdgeServer)
        .await
        .expect("first causal snapshot settles");
    let second_batch = alice
        .update(counter_id, vec![("value".to_string(), Value::Integer(3))])
        .expect("alice writes the second causal snapshot");
    alice
        .wait_for_batch(second_batch, DurabilityTier::EdgeServer)
        .await
        .expect("second causal snapshot settles");

    for (name, client) in [("Alice", &alice), ("Bob", &bob)] {
        wait_for_query(
            client,
            query.clone(),
            Some(DurabilityTier::EdgeServer),
            QUERY_TIMEOUT,
            &format!("{name} sees the causal counter result"),
            |rows| {
                (rows.len() == 1 && rows[0].0 == counter_id && counter_value(&rows[0]) == Some(3))
                    .then_some(())
            },
        )
        .await;
    }

    let observer = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema)
        .with_user_id("charlie-counter-causal")
        .ready_on("counters", READY_TIMEOUT)
        .connect()
        .await;
    wait_for_query(
        &observer,
        query,
        Some(DurabilityTier::EdgeServer),
        QUERY_TIMEOUT,
        "fresh observer sees the causal counter result",
        |rows| {
            (rows.len() == 1 && rows[0].0 == counter_id && counter_value(&rows[0]) == Some(3))
                .then_some(())
        },
    )
    .await;

    observer.shutdown().await.expect("shutdown causal observer");
    bob.shutdown().await.expect("shutdown causal Bob");
    alice.shutdown().await.expect("shutdown causal Alice");
    server.shutdown().await;
}
