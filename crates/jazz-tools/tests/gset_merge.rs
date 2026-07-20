#![cfg(feature = "test")]

mod support;

use std::collections::HashMap;
use std::sync::LazyLock;
use std::time::Duration;

use jazz_tools::server::JazzServer;
use jazz_tools::sync_manager::SyncPayload;
use jazz_tools::{
    ColumnDescriptor, ColumnMergeStrategy, ColumnType, DurabilityTier, JazzClient, ObjectId, Query,
    QueryBuilder, RowDescriptor, Schema, TableName, TableSchema, Value,
};
use support::{TestingClient, wait_for, wait_for_query};

const READY_TIMEOUT: Duration = Duration::from_secs(30);
const QUERY_TIMEOUT: Duration = Duration::from_secs(25);
static GSET_SUITE_LOCK: LazyLock<tokio::sync::Mutex<()>> =
    LazyLock::new(|| tokio::sync::Mutex::new(()));

async fn lock_gset_suite() -> tokio::sync::MutexGuard<'static, ()> {
    GSET_SUITE_LOCK.lock().await
}

/// `docs` table with a `tags` array column that merges as a grow-only set.
fn gset_schema() -> Schema {
    let tags = ColumnDescriptor::new(
        "tags",
        ColumnType::Array {
            element: Box::new(ColumnType::Text),
        },
    )
    .merge_strategy(ColumnMergeStrategy::GSet);
    let docs = TableSchema::new(RowDescriptor::new(vec![
        ColumnDescriptor::new("name", ColumnType::Text),
        tags,
    ]));
    Schema::from([(TableName::new("docs"), docs)])
}

fn doc_values(name: &str, tags: &[&str]) -> HashMap<String, Value> {
    HashMap::from([
        ("name".to_string(), Value::Text(name.to_string())),
        ("tags".to_string(), tags_value(tags)),
    ])
}

fn tags_value(tags: &[&str]) -> Value {
    Value::Array(tags.iter().map(|t| Value::Text(t.to_string())).collect())
}

/// Extract the `tags` column (index 1) of a queried row as a `Vec<String>`.
fn tags_of(row: &(ObjectId, Vec<Value>)) -> Option<Vec<String>> {
    match &row.1[1] {
        Value::Array(elements) => elements
            .iter()
            .map(|value| match value {
                Value::Text(s) => Some(s.clone()),
                _ => None,
            })
            .collect(),
        _ => None,
    }
}

/// Drives a deterministic concurrent merge instead of racing two writes.
///
/// `second` is blocked from receiving server traffic for the duration, so it
/// writes without ever observing `first`'s update — a genuine concurrent write
/// sharing the same ancestor — while the server is forced to process `first`'s
/// write strictly before `second`'s. Flipping which client is `first` lets a
/// test assert convergence is identical regardless of propagation order, with
/// no dependence on the async scheduler.
async fn merge_concurrently(
    server: &JazzServer,
    doc_id: ObjectId,
    column: &str,
    first: &JazzClient,
    first_value: Value,
    second: &JazzClient,
    second_value: Value,
) {
    let blocked = server.block_messages_to(second.client_id().expect("second client id"));

    let first_batch = first
        .update(doc_id, vec![(column.to_string(), first_value)])
        .expect("first replica writes");
    first
        .wait_for_batch(first_batch, DurabilityTier::EdgeServer)
        .await
        .expect("first write settles at the server before the second is sent");

    let second_batch = second
        .update(doc_id, vec![(column.to_string(), second_value)])
        .expect("second replica writes");
    blocked
        .wait_until_buffered(
            |payload| {
                matches!(
                    payload,
                    SyncPayload::BatchFate { fate } if fate.batch_id() == second_batch
                )
            },
            QUERY_TIMEOUT,
        )
        .await
        .expect("server merges the second write while that replica is blocked");
    blocked.unblock();
    second
        .wait_for_batch(second_batch, DurabilityTier::EdgeServer)
        .await
        .expect("second write settles after unblocking");
}

/// Wait until both replicas report `doc_id`'s column as `expected`.
async fn assert_converges<T>(
    a: &JazzClient,
    b: &JazzClient,
    query: &Query,
    doc_id: ObjectId,
    extract: fn(&(ObjectId, Vec<Value>)) -> Option<Vec<T>>,
    expected: Vec<T>,
    description: &str,
) where
    T: PartialEq,
{
    wait_for(QUERY_TIMEOUT, description, || async {
        let a_rows = a
            .query(query.clone(), Some(DurabilityTier::EdgeServer))
            .await
            .ok()?;
        let b_rows = b
            .query(query.clone(), Some(DurabilityTier::EdgeServer))
            .await
            .ok()?;
        let a_val = a_rows
            .iter()
            .find(|row| row.0 == doc_id)
            .and_then(extract)?;
        let b_val = b_rows
            .iter()
            .find(|row| row.0 == doc_id)
            .and_then(extract)?;
        (a_val == expected && b_val == expected).then_some(())
    })
    .await;
}

#[tokio::test]
async fn concurrent_writes_converge_to_sorted_union() {
    let _suite_guard = lock_gset_suite().await;
    let schema = gset_schema();
    let server = JazzServer::start_with_schema(schema.clone()).await;

    let alice = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("alice-gset")
        .ready_on("docs", READY_TIMEOUT)
        .connect()
        .await;
    let bob = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema)
        .with_user_id("bob-gset")
        .ready_on("docs", READY_TIMEOUT)
        .connect()
        .await;

    // One doc per propagation order, each starting from the same `[seed]` base.
    let (doc_alice_first, _, _) = alice
        .insert("docs", doc_values("a", &["seed"]))
        .expect("alice creates doc a");
    let (doc_bob_first, _, _) = alice
        .insert("docs", doc_values("b", &["seed"]))
        .expect("alice creates doc b");

    let query = QueryBuilder::new("docs").build();
    wait_for_query(
        &bob,
        query.clone(),
        Some(DurabilityTier::EdgeServer),
        QUERY_TIMEOUT,
        "bob sees both docs",
        |rows| (rows.len() == 2).then_some(()),
    )
    .await;

    let expected = vec!["alice".to_string(), "bob".to_string(), "seed".to_string()];

    // Order A: the server merges alice's write before bob's.
    merge_concurrently(
        &server,
        doc_alice_first,
        "tags",
        &alice,
        tags_value(&["seed", "alice"]),
        &bob,
        tags_value(&["seed", "bob"]),
    )
    .await;
    assert_converges(
        &alice,
        &bob,
        &query,
        doc_alice_first,
        tags_of,
        expected.clone(),
        "alice-first order converges to the sorted union",
    )
    .await;

    // Order B: reversed — bob's write reaches the server first, same result.
    merge_concurrently(
        &server,
        doc_bob_first,
        "tags",
        &bob,
        tags_value(&["seed", "bob"]),
        &alice,
        tags_value(&["seed", "alice"]),
    )
    .await;
    assert_converges(
        &alice,
        &bob,
        &query,
        doc_bob_first,
        tags_of,
        expected,
        "bob-first order converges to the same sorted union",
    )
    .await;

    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    server.shutdown().await;
}

#[tokio::test]
async fn concurrent_writes_never_remove_a_shared_element() {
    let _suite_guard = lock_gset_suite().await;
    let schema = gset_schema();
    let server = JazzServer::start_with_schema(schema.clone()).await;

    let alice = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("alice-grow-only")
        .ready_on("docs", READY_TIMEOUT)
        .connect()
        .await;
    let bob = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema)
        .with_user_id("bob-grow-only")
        .ready_on("docs", READY_TIMEOUT)
        .connect()
        .await;

    // Both replicas start synced to a doc that already contains "keep".
    let (doc_alice_first, _, _) = alice
        .insert("docs", doc_values("a", &["base", "keep"]))
        .expect("alice creates doc a");
    let (doc_bob_first, _, _) = alice
        .insert("docs", doc_values("b", &["base", "keep"]))
        .expect("alice creates doc b");

    let query = QueryBuilder::new("docs").build();
    wait_for_query(
        &bob,
        query.clone(),
        Some(DurabilityTier::EdgeServer),
        QUERY_TIMEOUT,
        "bob sees both docs",
        |rows| (rows.len() == 2).then_some(()),
    )
    .await;

    // Each concurrent write drops "keep" and adds its own tag. Grow-only keeps
    // "keep" alive through the shared ancestor under either propagation order —
    // neither contender carries it, so only ancestor union can preserve it.
    let expected = vec![
        "alice".to_string(),
        "base".to_string(),
        "bob".to_string(),
        "keep".to_string(),
    ];

    merge_concurrently(
        &server,
        doc_alice_first,
        "tags",
        &alice,
        tags_value(&["base", "alice"]),
        &bob,
        tags_value(&["base", "bob"]),
    )
    .await;
    assert_converges(
        &alice,
        &bob,
        &query,
        doc_alice_first,
        tags_of,
        expected.clone(),
        "alice-first order keeps the omitted shared element",
    )
    .await;

    merge_concurrently(
        &server,
        doc_bob_first,
        "tags",
        &bob,
        tags_value(&["base", "bob"]),
        &alice,
        tags_value(&["base", "alice"]),
    )
    .await;
    assert_converges(
        &alice,
        &bob,
        &query,
        doc_bob_first,
        tags_of,
        expected,
        "bob-first order keeps the omitted shared element",
    )
    .await;

    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    server.shutdown().await;
}

#[tokio::test]
/// Keeps a scoped GSet replay's merged elements when bob writes another column offline.
///
/// ```text
/// bob ── offline +bob ──► server ◄── +alice ── alice
/// bob ◄──────────── scoped merged snapshot ─────────── server
/// bob ── offline name update ──► merged tags remain visible
/// ```
async fn scoped_replay_preserves_merged_elements_for_a_local_write() {
    let _suite_guard = lock_gset_suite().await;
    let schema = gset_schema();
    let server = JazzServer::start_with_schema(schema.clone()).await;

    let alice = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("alice-gset-scoped-replay")
        .ready_on("docs", READY_TIMEOUT)
        .connect()
        .await;
    let (mut bob_context, bob) = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("bob-gset-scoped-replay")
        .with_persistent_storage()
        .connect_with_context()
        .await;

    let (doc_id, _, _) = alice
        .insert("docs", doc_values("shared", &["seed"]))
        .expect("alice creates the document");
    let query = QueryBuilder::new("docs").build();
    wait_for_query(
        &bob,
        query.clone(),
        Some(DurabilityTier::EdgeServer),
        QUERY_TIMEOUT,
        "bob sees the shared document base",
        |rows| (rows.len() == 1 && rows[0].0 == doc_id).then_some(()),
    )
    .await;

    bob.shutdown()
        .await
        .expect("bob shuts down before the offline write");
    bob_context.server_url = String::new();
    let bob_offline = JazzClient::connect(bob_context.clone())
        .await
        .expect("bob opens his persistent storage offline");
    bob_offline
        .update(
            doc_id,
            vec![("tags".to_string(), tags_value(&["seed", "bob"]))],
        )
        .expect("bob adds his tag offline");
    bob_offline
        .shutdown()
        .await
        .expect("bob shuts down after the offline write");
    bob_context.server_url = server.base_url();

    let alice_batch = alice
        .update(
            doc_id,
            vec![("tags".to_string(), tags_value(&["seed", "alice"]))],
        )
        .expect("alice adds her tag");
    alice
        .wait_for_batch(alice_batch, DurabilityTier::EdgeServer)
        .await
        .expect("alice's write settles at the server");

    let expected_tags = vec!["alice".to_string(), "bob".to_string(), "seed".to_string()];
    let bob_restarted = JazzClient::connect(bob_context.clone())
        .await
        .expect("bob reconnects with persistent storage");
    wait_for_query(
        &bob_restarted,
        query.clone(),
        Some(DurabilityTier::EdgeServer),
        QUERY_TIMEOUT,
        "restarted bob sees the merged tags",
        |rows| {
            (rows.len() == 1
                && rows[0].0 == doc_id
                && tags_of(&rows[0]) == Some(expected_tags.clone()))
            .then_some(())
        },
    )
    .await;

    let observer = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("charlie-gset-scoped-replay")
        .ready_on("docs", READY_TIMEOUT)
        .connect()
        .await;
    wait_for_query(
        &observer,
        query.clone(),
        Some(DurabilityTier::EdgeServer),
        QUERY_TIMEOUT,
        "fresh observer sees the merged tags",
        |rows| {
            (rows.len() == 1
                && rows[0].0 == doc_id
                && tags_of(&rows[0]) == Some(expected_tags.clone()))
            .then_some(())
        },
    )
    .await;

    bob_restarted
        .shutdown()
        .await
        .expect("bob shuts down after receiving the scoped merge");

    bob_context.server_url = String::new();
    let bob_offline_after_merge = JazzClient::connect(bob_context)
        .await
        .expect("bob reopens the merged document offline");
    bob_offline_after_merge
        .update(
            doc_id,
            vec![(
                "name".to_string(),
                Value::Text("edited while offline".to_string()),
            )],
        )
        .expect("bob updates the document name offline");
    let local_rows = bob_offline_after_merge
        .query(query, None)
        .await
        .expect("query bob's offline document");
    let local_row = local_rows
        .iter()
        .find(|row| row.0 == doc_id)
        .expect("offline document remains visible");
    assert_eq!(tags_of(local_row), Some(expected_tags));
    bob_offline_after_merge
        .shutdown()
        .await
        .expect("shutdown bob after the offline follow-up");

    observer.shutdown().await.expect("shutdown observer");
    alice.shutdown().await.expect("shutdown alice");
    server.shutdown().await;
}

/// `docs` table with a `scores` float-array column merging as a grow-only set.
fn gset_float_schema() -> Schema {
    let scores = ColumnDescriptor::new(
        "scores",
        ColumnType::Array {
            element: Box::new(ColumnType::Double),
        },
    )
    .merge_strategy(ColumnMergeStrategy::GSet);
    let docs = TableSchema::new(RowDescriptor::new(vec![
        ColumnDescriptor::new("name", ColumnType::Text),
        scores,
    ]));
    Schema::from([(TableName::new("docs"), docs)])
}

fn score_doc_values(name: &str, scores: &[f64]) -> HashMap<String, Value> {
    HashMap::from([
        ("name".to_string(), Value::Text(name.to_string())),
        ("scores".to_string(), scores_value(scores)),
    ])
}

fn scores_value(scores: &[f64]) -> Value {
    Value::Array(scores.iter().map(|s| Value::Double(*s)).collect())
}

/// Extract the `scores` column (index 1) as raw bit patterns, so `-0.0` and
/// `+0.0` (which compare equal under IEEE) are distinguishable.
fn scores_bits(row: &(ObjectId, Vec<Value>)) -> Option<Vec<u64>> {
    match &row.1[1] {
        Value::Array(elements) => elements
            .iter()
            .map(|value| match value {
                Value::Double(f) => Some(f.to_bits()),
                _ => None,
            })
            .collect(),
        _ => None,
    }
}

/// `-0.0` and `+0.0` are the same number but distinct bit patterns. The merge
/// keys on the raw encoding, which never normalises them, so both survive and
/// replicas writing them in opposite orders converge byte-identically under
/// either propagation order.
#[tokio::test]
async fn distinct_float_representations_converge_deterministically() {
    let _suite_guard = lock_gset_suite().await;
    let schema = gset_float_schema();
    let server = JazzServer::start_with_schema(schema.clone()).await;

    let alice = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("alice-float")
        .ready_on("docs", READY_TIMEOUT)
        .connect()
        .await;
    let bob = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema)
        .with_user_id("bob-float")
        .ready_on("docs", READY_TIMEOUT)
        .connect()
        .await;

    let (doc_alice_first, _, _) = alice
        .insert("docs", score_doc_values("a", &[]))
        .expect("alice creates doc a");
    let (doc_bob_first, _, _) = alice
        .insert("docs", score_doc_values("b", &[]))
        .expect("alice creates doc b");

    let query = QueryBuilder::new("docs").build();
    wait_for_query(
        &bob,
        query.clone(),
        Some(DurabilityTier::EdgeServer),
        QUERY_TIMEOUT,
        "bob sees both docs",
        |rows| (rows.len() == 2).then_some(()),
    )
    .await;

    // Both zeros retained (no normalising collision), in one canonical order.
    let expected = vec![0.0_f64.to_bits(), (-0.0_f64).to_bits()];

    merge_concurrently(
        &server,
        doc_alice_first,
        "scores",
        &alice,
        scores_value(&[0.0, -0.0]),
        &bob,
        scores_value(&[-0.0, 0.0]),
    )
    .await;
    assert_converges(
        &alice,
        &bob,
        &query,
        doc_alice_first,
        scores_bits,
        expected.clone(),
        "alice-first order keeps both zero representations",
    )
    .await;

    merge_concurrently(
        &server,
        doc_bob_first,
        "scores",
        &bob,
        scores_value(&[-0.0, 0.0]),
        &alice,
        scores_value(&[0.0, -0.0]),
    )
    .await;
    assert_converges(
        &alice,
        &bob,
        &query,
        doc_bob_first,
        scores_bits,
        expected,
        "bob-first order converges to the same bit patterns",
    )
    .await;

    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    server.shutdown().await;
}
