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
use uuid::Uuid;

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

    let bob_restarted = JazzClient::connect(bob_context.clone())
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
    bob_context.server_url = String::new();
    let bob_offline_after_merge = JazzClient::connect(bob_context)
        .await
        .expect("bob reopens his merged state offline");
    bob_offline_after_merge
        .update(counter_id, vec![("value".to_string(), Value::Integer(9))])
        .expect("bob writes the next counter value offline");
    let local_rows = bob_offline_after_merge
        .query(QueryBuilder::new("counters").build(), None)
        .await
        .expect("query Bob's offline counter");
    assert_eq!(
        local_rows
            .iter()
            .find(|row| row.0 == counter_id)
            .and_then(counter_value),
        Some(9),
        "a local write after a scoped merge must build on the merged visible value"
    );
    bob_offline_after_merge
        .shutdown()
        .await
        .expect("shutdown Bob after the offline follow-up");
    observer.shutdown().await.expect("shutdown observer");
    alice.shutdown().await.expect("shutdown Alice");
    server.shutdown().await;
}

#[tokio::test]
/// Preserves a merged counter projection through the receiving writer's next update.
///
/// ```text
/// bob ── base ── offline +3 ────────────────► server
/// alice ── +5 ──► server ── merged 8 ───────► bob
/// bob ── offline follow-up 9 ───────────────► server
/// charlie ◄────────────── merged 9 ────────── server
/// ```
async fn counter_merge_preserves_projection_when_scoped_snapshot_replays_local_batch() {
    let _suite_guard = COUNTER_SUITE_LOCK.lock().await;
    let schema = counter_schema();
    let server = JazzServer::start_with_schema(schema.clone()).await;

    let alice = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("alice-counter-projection")
        .ready_on("counters", READY_TIMEOUT)
        .connect()
        .await;
    let (mut bob_context, bob) = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("bob-counter-projection")
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

    bob.shutdown()
        .await
        .expect("bob shuts down before offline write");
    bob_context.server_url = String::new();
    let bob_offline = JazzClient::connect(bob_context.clone())
        .await
        .expect("bob opens his persistent storage offline");
    bob_offline
        .update(counter_id, vec![("value".to_string(), Value::Integer(3))])
        .expect("bob writes +3 offline");
    bob_offline
        .shutdown()
        .await
        .expect("bob shuts down after offline write");

    let alice_batch = alice
        .update(counter_id, vec![("value".to_string(), Value::Integer(5))])
        .expect("alice writes +5 first");
    alice
        .wait_for_batch(alice_batch, DurabilityTier::EdgeServer)
        .await
        .expect("alice's write settles at the server");

    bob_context.server_url = server.base_url();
    let observer = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("charlie-counter-projection")
        .ready_on("counters", READY_TIMEOUT)
        .connect()
        .await;
    let bob_restarted = JazzClient::connect(bob_context.clone())
        .await
        .expect("bob reconnects with persistent storage");

    wait_for_query(
        &bob_restarted,
        query.clone(),
        Some(DurabilityTier::EdgeServer),
        QUERY_TIMEOUT,
        "bob sees the merged counter",
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
        "fresh observer sees the merged counter",
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
    bob_context.server_url = String::new();
    let bob_follow_up = JazzClient::connect(bob_context.clone())
        .await
        .expect("bob reopens his merged state offline");
    bob_follow_up
        .update(counter_id, vec![("value".to_string(), Value::Integer(9))])
        .expect("bob writes the next counter value offline");
    let local_rows = bob_follow_up
        .query(QueryBuilder::new("counters").build(), None)
        .await
        .expect("query Bob's offline counter");
    assert_eq!(
        local_rows
            .iter()
            .find(|row| row.0 == counter_id)
            .and_then(counter_value),
        Some(9),
        "a local write after a scoped merge must build on the merged visible value"
    );

    bob_follow_up
        .shutdown()
        .await
        .expect("shutdown Bob after the offline follow-up");

    bob_context.server_url = server.base_url();
    let bob_after_follow_up = JazzClient::connect(bob_context)
        .await
        .expect("bob reconnects with the offline follow-up");
    wait_for_query(
        &observer,
        query,
        Some(DurabilityTier::EdgeServer),
        QUERY_TIMEOUT,
        "observer sees exactly the merged counter plus Bob's follow-up",
        |rows| {
            (rows.len() == 1 && rows[0].0 == counter_id && counter_value(&rows[0]) == Some(9))
                .then_some(())
        },
    )
    .await;

    bob_after_follow_up
        .shutdown()
        .await
        .expect("shutdown Bob after reconnecting the follow-up");
    observer.shutdown().await.expect("shutdown observer");
    alice.shutdown().await.expect("shutdown Alice");
    server.shutdown().await;
}

#[tokio::test]
/// Rebases Bob's staged counter intent over Alice's incoming scoped projection.
///
/// Ordinary reads exclude Bob's open transaction and therefore show Alice's
/// server value. Bob's transaction-scoped read must retain his pending `+1`.
///
/// ```text
/// bob ── stage +1 (open transaction) ───────────────► local only
/// alice ── +5 ──► server ── scoped 5 ──────────────► bob
/// bob ordinary read: 5    bob transaction read: 6
/// bob ── commit +1 ──► server ── merged 6 ─────────► charlie
/// ```
async fn scoped_projection_rebases_an_open_counter_transaction() {
    let _suite_guard = COUNTER_SUITE_LOCK.lock().await;
    let schema = counter_schema();
    let server = JazzServer::start_with_schema(schema.clone()).await;

    let alice = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("alice-counter-pending")
        .ready_on("counters", READY_TIMEOUT)
        .connect()
        .await;
    let bob = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("bob-counter-pending")
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
        "bob sees the counter base",
        |rows| {
            (rows.len() == 1 && rows[0].0 == counter_id && counter_value(&rows[0]) == Some(0))
                .then_some(())
        },
    )
    .await;

    let bob_tx = bob
        .begin_transaction()
        .expect("bob begins a counter transaction");
    let bob_batch = bob_tx
        .update(counter_id, vec![("value".to_string(), Value::Integer(1))])
        .expect("bob stages +1");
    assert_eq!(bob_batch, bob_tx.batch_id());
    wait_for_query(
        bob_tx.client(),
        query.clone(),
        None,
        QUERY_TIMEOUT,
        "bob's transaction sees its staged +1",
        |rows| {
            (rows.len() == 1 && rows[0].0 == counter_id && counter_value(&rows[0]) == Some(1))
                .then_some(())
        },
    )
    .await;

    let alice_batch = alice
        .update(counter_id, vec![("value".to_string(), Value::Integer(5))])
        .expect("alice writes +5 while Bob's transaction is open");
    alice
        .wait_for_batch(alice_batch, DurabilityTier::EdgeServer)
        .await
        .expect("alice's +5 settles");
    wait_for_query(
        &bob,
        query.clone(),
        Some(DurabilityTier::EdgeServer),
        QUERY_TIMEOUT,
        "bob's ordinary read receives Alice's scoped projection",
        |rows| {
            (rows.len() == 1 && rows[0].0 == counter_id && counter_value(&rows[0]) == Some(5))
                .then_some(())
        },
    )
    .await;
    wait_for_query(
        bob_tx.client(),
        query.clone(),
        None,
        QUERY_TIMEOUT,
        "bob's transaction rebases its pending +1 over the scoped 5",
        |rows| {
            (rows.len() == 1 && rows[0].0 == counter_id && counter_value(&rows[0]) == Some(6))
                .then_some(())
        },
    )
    .await;
    let rebased_query = QueryBuilder::new("counters")
        .filter_eq("value", Value::Integer(6))
        .build();
    wait_for_query(
        bob_tx.client(),
        rebased_query,
        None,
        QUERY_TIMEOUT,
        "bob's transaction filters against the rebased counter projection",
        |rows| {
            (rows.len() == 1 && rows[0].0 == counter_id && counter_value(&rows[0]) == Some(6))
                .then_some(())
        },
    )
    .await;

    assert_eq!(bob_tx.commit().expect("bob commits +1"), bob_batch);
    let rejection = bob
        .wait_for_batch(bob_batch, DurabilityTier::EdgeServer)
        .await
        .expect_err("bob's stale transaction is rejected")
        .to_string();
    assert!(
        rejection.contains("transaction_conflict"),
        "unexpected rejection: {rejection}"
    );

    let observer = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema)
        .with_user_id("charlie-counter-pending")
        .ready_on("counters", READY_TIMEOUT)
        .connect()
        .await;
    wait_for_query(
        &observer,
        query,
        Some(DurabilityTier::EdgeServer),
        QUERY_TIMEOUT,
        "fresh observer sees Alice's durable +5",
        |rows| {
            (rows.len() == 1 && rows[0].0 == counter_id && counter_value(&rows[0]) == Some(5))
                .then_some(())
        },
    )
    .await;

    observer.shutdown().await.expect("shutdown observer");
    bob.shutdown().await.expect("shutdown Bob");
    alice.shutdown().await.expect("shutdown Alice");
    server.shutdown().await;
}

#[tokio::test]
/// Authors a transaction's counter delta from the scoped projection Bob observed.
///
/// ```text
/// alice ── +5 ──► server ── scoped 5 ──────────────► bob
/// bob ── begin transaction ── set 6 ──► local 6
/// bob ── commit +1 ──► server ── merged 6 ─────────► charlie
/// ```
async fn counter_transaction_staged_after_scoped_projection_commits_observed_value() {
    let _suite_guard = COUNTER_SUITE_LOCK.lock().await;
    let schema = counter_schema();
    let server = JazzServer::start_with_schema(schema.clone()).await;

    let alice = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("alice-counter-transaction-basis")
        .ready_on("counters", READY_TIMEOUT)
        .connect()
        .await;
    let bob = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("bob-counter-transaction-basis")
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
        "bob sees the counter base",
        |rows| {
            (rows.len() == 1 && rows[0].0 == counter_id && counter_value(&rows[0]) == Some(0))
                .then_some(())
        },
    )
    .await;

    let alice_batch = alice
        .update(counter_id, vec![("value".to_string(), Value::Integer(5))])
        .expect("alice writes +5");
    alice
        .wait_for_batch(alice_batch, DurabilityTier::EdgeServer)
        .await
        .expect("alice's +5 settles");
    wait_for_query(
        &bob,
        query.clone(),
        Some(DurabilityTier::EdgeServer),
        QUERY_TIMEOUT,
        "bob receives Alice's scoped projection",
        |rows| {
            (rows.len() == 1 && rows[0].0 == counter_id && counter_value(&rows[0]) == Some(5))
                .then_some(())
        },
    )
    .await;

    let bob_tx = bob
        .begin_transaction()
        .expect("bob begins a counter transaction");
    let bob_batch = bob_tx
        .update(counter_id, vec![("value".to_string(), Value::Integer(6))])
        .expect("bob stages +1 from the observed 5");
    assert_eq!(bob_batch, bob_tx.batch_id());
    wait_for_query(
        bob_tx.client(),
        query.clone(),
        None,
        QUERY_TIMEOUT,
        "bob's transaction sees the requested 6",
        |rows| {
            (rows.len() == 1 && rows[0].0 == counter_id && counter_value(&rows[0]) == Some(6))
                .then_some(())
        },
    )
    .await;
    let repeated_batch = bob_tx
        .update(counter_id, vec![("value".to_string(), Value::Integer(6))])
        .expect("bob repeats the staged visible value");
    assert_eq!(repeated_batch, bob_batch);
    let filtered_query = QueryBuilder::new("counters")
        .filter_eq("value", Value::Integer(6))
        .build();
    wait_for_query(
        bob_tx.client(),
        filtered_query,
        None,
        QUERY_TIMEOUT,
        "bob's transaction filters against the requested 6",
        |rows| {
            (rows.len() == 1 && rows[0].0 == counter_id && counter_value(&rows[0]) == Some(6))
                .then_some(())
        },
    )
    .await;

    assert_eq!(bob_tx.commit().expect("bob commits +1"), bob_batch);
    bob.wait_for_batch(bob_batch, DurabilityTier::EdgeServer)
        .await
        .expect("bob's transaction settles");

    let observer = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema)
        .with_user_id("charlie-counter-transaction-basis")
        .ready_on("counters", READY_TIMEOUT)
        .connect()
        .await;
    wait_for_query(
        &observer,
        query,
        Some(DurabilityTier::EdgeServer),
        QUERY_TIMEOUT,
        "fresh observer sees exactly 6",
        |rows| {
            (rows.len() == 1 && rows[0].0 == counter_id && counter_value(&rows[0]) == Some(6))
                .then_some(())
        },
    )
    .await;

    observer.shutdown().await.expect("shutdown observer");
    bob.shutdown().await.expect("shutdown Bob");
    alice.shutdown().await.expect("shutdown Alice");
    server.shutdown().await;
}

#[tokio::test]
/// Keeps caller-supplied IDs from turning concurrent inserts into causal updates.
///
/// Alice and bob intentionally create the same deterministic ID while offline;
/// this is not a random UUID collision.
///
/// ```text
/// alice ── offline insert id=shared, value=2 ──► server
/// bob   ── offline insert id=shared, value=3 ──► server
/// observer ◄────────── concurrent-root merge 5 ────────── server
/// ```
async fn parentless_client_rows_with_the_same_id_remain_concurrent() {
    let _suite_guard = COUNTER_SUITE_LOCK.lock().await;
    let schema = counter_schema();
    let server = JazzServer::start_with_schema(schema.clone()).await;

    let (mut alice_context, alice) = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("alice-counter-shared-id")
        .with_persistent_storage()
        .ready_on("counters", READY_TIMEOUT)
        .connect_with_context()
        .await;
    let (mut bob_context, bob) = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("bob-counter-shared-id")
        .with_persistent_storage()
        .ready_on("counters", READY_TIMEOUT)
        .connect_with_context()
        .await;

    alice
        .shutdown()
        .await
        .expect("alice shuts down before the offline insert");
    bob.shutdown()
        .await
        .expect("bob shuts down before the offline insert");
    alice_context.server_url = String::new();
    bob_context.server_url = String::new();

    let shared_id =
        Uuid::parse_str("550e8400-e29b-41d4-a716-446655440010").expect("parse shared UUID");
    let alice_offline = JazzClient::connect(alice_context.clone())
        .await
        .expect("alice opens her persistent storage offline");
    let (_, _, _) = alice_offline
        .insert_with_id("counters", shared_id, jazz_tools::row_input!("value" => 2))
        .expect("alice inserts the shared ID offline");
    alice_offline
        .shutdown()
        .await
        .expect("alice shuts down after the offline insert");

    let bob_offline = JazzClient::connect(bob_context.clone())
        .await
        .expect("bob opens his persistent storage offline");
    let (counter_id, _, _) = bob_offline
        .insert_with_id("counters", shared_id, jazz_tools::row_input!("value" => 3))
        .expect("bob inserts the shared ID offline");
    bob_offline
        .shutdown()
        .await
        .expect("bob shuts down after the offline insert");

    alice_context.server_url = server.base_url();
    let alice_restarted = JazzClient::connect(alice_context)
        .await
        .expect("alice reconnects with her offline insert");
    wait_for_query(
        &alice_restarted,
        QueryBuilder::new("counters").build(),
        Some(DurabilityTier::EdgeServer),
        QUERY_TIMEOUT,
        "alice's root reaches the server first",
        |rows| {
            (rows.len() == 1 && rows[0].0 == counter_id && counter_value(&rows[0]) == Some(2))
                .then_some(())
        },
    )
    .await;

    bob_context.server_url = server.base_url();
    let bob_restarted = JazzClient::connect(bob_context)
        .await
        .expect("bob reconnects with his concurrent root");

    let observer = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema)
        .with_user_id("charlie-counter-shared-id")
        .ready_on("counters", READY_TIMEOUT)
        .connect()
        .await;
    wait_for_query(
        &observer,
        QueryBuilder::new("counters").build(),
        Some(DurabilityTier::EdgeServer),
        QUERY_TIMEOUT,
        "observer sees both parentless inserts merged as concurrent roots",
        |rows| {
            (rows.len() == 1 && rows[0].0 == counter_id && counter_value(&rows[0]) == Some(5))
                .then_some(())
        },
    )
    .await;

    observer.shutdown().await.expect("shutdown observer");
    bob_restarted.shutdown().await.expect("shutdown bob");
    alice_restarted.shutdown().await.expect("shutdown alice");
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
