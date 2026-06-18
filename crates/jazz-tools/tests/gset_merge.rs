#![cfg(feature = "test")]

mod support;

use std::collections::HashMap;
use std::sync::{Arc, LazyLock};
use std::time::Duration;

use jazz_tools::server::TestingServer;
use jazz_tools::{
    ColumnDescriptor, ColumnMergeStrategy, ColumnType, DurabilityTier, ObjectId, QueryBuilder,
    RowDescriptor, Schema, TableName, TableSchema, Value,
};
use support::{TestingClient, wait_for_query};

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

/// Two replicas write disjoint tag sets concurrently; both converge to the
/// union, in identical (sorted-by-encoded-value) order.
///
/// ```text
/// alice ──tags=[seed,alice]──► server ◄──tags=[seed,bob]── bob
///                       both query → [alice, bob, seed]
/// ```
#[tokio::test]
async fn concurrent_writes_converge_to_sorted_union() {
    let _suite_guard = lock_gset_suite().await;
    let schema = gset_schema();
    let server = TestingServer::start_with_schema(schema.clone()).await;

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

    let (doc_id, _, _) = alice
        .insert("docs", doc_values("d", &["seed"]))
        .expect("alice creates doc");

    let query = QueryBuilder::new("docs").build();
    wait_for_query(
        &bob,
        query.clone(),
        Some(DurabilityTier::EdgeServer),
        QUERY_TIMEOUT,
        "bob sees alice's doc",
        |rows| (rows.len() == 1 && rows[0].0 == doc_id).then_some(()),
    )
    .await;

    let alice = Arc::new(alice);
    let bob = Arc::new(bob);
    let alice2 = Arc::clone(&alice);
    let bob2 = Arc::clone(&bob);

    let alice_handle = tokio::spawn(async move {
        alice2
            .update(
                doc_id,
                vec![("tags".to_string(), tags_value(&["seed", "alice"]))],
            )
            .expect("alice writes tags");
    });
    let bob_handle = tokio::spawn(async move {
        bob2.update(
            doc_id,
            vec![("tags".to_string(), tags_value(&["seed", "bob"]))],
        )
        .expect("bob writes tags");
    });
    let (alice_res, bob_res) = tokio::join!(alice_handle, bob_handle);
    alice_res.expect("alice task panicked");
    bob_res.expect("bob task panicked");

    let expected = vec!["alice".to_string(), "bob".to_string(), "seed".to_string()];
    support::wait_for(QUERY_TIMEOUT, "both converge to the sorted union", || {
        let alice = Arc::clone(&alice);
        let bob = Arc::clone(&bob);
        let query = query.clone();
        let expected = expected.clone();
        async move {
            let alice_rows = alice
                .query(query.clone(), Some(DurabilityTier::EdgeServer))
                .await
                .ok()?;
            let bob_rows = bob
                .query(query, Some(DurabilityTier::EdgeServer))
                .await
                .ok()?;
            if alice_rows.len() == 1 && bob_rows.len() == 1 {
                let alice_tags = tags_of(&alice_rows[0])?;
                let bob_tags = tags_of(&bob_rows[0])?;
                // Byte-identical ordering across replicas, equal to the canonical union.
                if alice_tags == expected && bob_tags == expected {
                    return Some(());
                }
            }
            None
        }
    })
    .await;

    Arc::try_unwrap(alice)
        .unwrap_or_else(|_| panic!("alice still shared"))
        .shutdown()
        .await
        .expect("shutdown alice");
    Arc::try_unwrap(bob)
        .unwrap_or_else(|_| panic!("bob still shared"))
        .shutdown()
        .await
        .expect("shutdown bob");
    server.shutdown().await;
}

/// Grow-only monotonicity: an element written by one replica is never dropped
/// by a concurrent write from another that lacks it.
#[tokio::test]
async fn concurrent_write_never_drops_a_peers_element() {
    let _suite_guard = lock_gset_suite().await;
    let schema = gset_schema();
    let server = TestingServer::start_with_schema(schema.clone()).await;

    let alice = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("alice-mono")
        .ready_on("docs", READY_TIMEOUT)
        .connect()
        .await;
    let bob = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema)
        .with_user_id("bob-mono")
        .ready_on("docs", READY_TIMEOUT)
        .connect()
        .await;

    let (doc_id, _, _) = alice
        .insert("docs", doc_values("d", &["base"]))
        .expect("alice creates doc");

    let query = QueryBuilder::new("docs").build();
    wait_for_query(
        &bob,
        query.clone(),
        Some(DurabilityTier::EdgeServer),
        QUERY_TIMEOUT,
        "bob sees alice's doc",
        |rows| (rows.len() == 1 && rows[0].0 == doc_id).then_some(()),
    )
    .await;

    let alice = Arc::new(alice);
    let bob = Arc::new(bob);
    let alice2 = Arc::clone(&alice);
    let bob2 = Arc::clone(&bob);

    // Alice adds "keep"; Bob concurrently writes a set that lacks "keep".
    let alice_handle = tokio::spawn(async move {
        alice2
            .update(
                doc_id,
                vec![("tags".to_string(), tags_value(&["base", "keep"]))],
            )
            .expect("alice writes tags");
    });
    let bob_handle = tokio::spawn(async move {
        bob2.update(
            doc_id,
            vec![("tags".to_string(), tags_value(&["base", "other"]))],
        )
        .expect("bob writes tags");
    });
    let (alice_res, bob_res) = tokio::join!(alice_handle, bob_handle);
    alice_res.expect("alice task panicked");
    bob_res.expect("bob task panicked");

    let expected = vec!["base".to_string(), "keep".to_string(), "other".to_string()];
    support::wait_for(
        QUERY_TIMEOUT,
        "peer's element survives the concurrent write",
        || {
            let alice = Arc::clone(&alice);
            let bob = Arc::clone(&bob);
            let query = query.clone();
            let expected = expected.clone();
            async move {
                let alice_rows = alice
                    .query(query.clone(), Some(DurabilityTier::EdgeServer))
                    .await
                    .ok()?;
                let bob_rows = bob
                    .query(query, Some(DurabilityTier::EdgeServer))
                    .await
                    .ok()?;
                if alice_rows.len() == 1 && bob_rows.len() == 1 {
                    let alice_tags = tags_of(&alice_rows[0])?;
                    let bob_tags = tags_of(&bob_rows[0])?;
                    if alice_tags == expected && bob_tags == expected {
                        return Some(());
                    }
                }
                None
            }
        },
    )
    .await;

    Arc::try_unwrap(alice)
        .unwrap_or_else(|_| panic!("alice still shared"))
        .shutdown()
        .await
        .expect("shutdown alice");
    Arc::try_unwrap(bob)
        .unwrap_or_else(|_| panic!("bob still shared"))
        .shutdown()
        .await
        .expect("shutdown bob");
    server.shutdown().await;
}
