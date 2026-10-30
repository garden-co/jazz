//! Subscriptions must deliver rows whose values spill out of line (#3349).
//!
//! Values above `INLINE_VALUE_MAX_BYTES` (64 KiB) are stored as chunked large
//! values. A client that receives such a row from the server has to fetch its
//! chunks before its local result can include the row.
use std::collections::BTreeMap;
use std::time::Duration;

use jazz::query::Query;
use jazz::row_input;
use jazz::tools::{
    ColumnType, JazzClient, ObjectId, ReadTier, SchemaBuilder, SubscriptionStream,
    SubscriptionStreamItem, TableSchema, Value, permissions,
};
use jazz_testkit::{connect_ready_client, wait_for_global_txs};

const READY: Duration = Duration::from_secs(30);
const DELIVERY: Duration = Duration::from_secs(20);

fn schema() -> jazz::tools::Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("docs")
                .column("label", ColumnType::Text)
                .column("body", ColumnType::Text)
                .policies(permissions(|p| {
                    p.allow_read().always();
                    p.allow_insert().always();
                    p.allow_update().always();
                    p.allow_delete().always();
                })),
        )
        .build()
}

fn body(size: usize, label: &str) -> String {
    let mut value = format!("{label}:");
    value.push_str(&"x".repeat(size - value.len()));
    value
}

async fn insert_global(client: &JazzClient, label: &str, body: &str) -> ObjectId {
    let (id, _, tx) = client
        .insert("docs", row_input!("label" => label, "body" => body))
        .unwrap();
    wait_for_global_txs(client, &[tx.unwrap()]).await;
    id
}

fn bodies(rows: &[(ObjectId, Vec<Value>)]) -> BTreeMap<ObjectId, usize> {
    rows.iter()
        .map(|(id, values)| {
            let body = values
                .iter()
                .find_map(|value| match value {
                    Value::Text(text) if text.len() > 64 => Some(text.len()),
                    _ => None,
                })
                .unwrap_or(0);
            (*id, body)
        })
        .collect()
}

/// Wait on subscription events until the client's local view of `docs`
/// contains exactly `expected` (row id -> body length).
async fn wait_for_bodies(
    client: &JazzClient,
    stream: &mut SubscriptionStream,
    expected: &BTreeMap<ObjectId, usize>,
    description: &str,
) {
    let deadline = tokio::time::Instant::now() + DELIVERY;
    let mut events = 0;
    loop {
        let rows = client
            .query(Query::from("docs"), ReadTier::LocalFirst)
            .await
            .map(jazz::tools::test_support::ordinary_rows)
            .unwrap();
        if events > 0 && &bodies(&rows) == expected {
            return;
        }
        let now = tokio::time::Instant::now();
        let item = tokio::time::timeout(deadline.saturating_duration_since(now), stream.next())
            .await
            .unwrap_or_else(|_| {
                panic!(
                    "{description}: timed out after {events} subscription events; \
                     local rows {:?}, expected {expected:?}",
                    bodies(&rows)
                )
            })
            .unwrap_or_else(|| panic!("{description}: subscription stream closed"));
        if let SubscriptionStreamItem::Rejected { reason } = item {
            panic!("{description}: subscription rejected: {reason:?}");
        }
        events += 1;
    }
}

/// A client that subscribes after the table already holds spilled rows gets
/// its first result, and a subscription that was open before a spilled row
/// was written receives that row too.
///
/// ```text
/// writer ── 60 KB, 70 KB, 800 KB rows ──► server
///                                           │
///             fresh reader subscribes ──────┤ first result has all three
///                                           │
/// writer ── another 70 KB row ─────────────►│ open subscription receives it
/// ```
#[tokio::test(flavor = "current_thread")]
async fn subscriptions_deliver_rows_with_spilled_values() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let schema = schema();
            let server = jazz_server::JazzServer::start_with_schema(schema.clone())
                .await
                .unwrap();
            let writer = connect_ready_client(&server, &schema, "writer", "docs", READY).await;

            let mut expected = BTreeMap::new();
            for (label, size) in [("inline", 60_000), ("spilled", 70_000), ("big", 800_000)] {
                let id = insert_global(&writer, label, &body(size, label)).await;
                expected.insert(id, size);
            }

            let reader = connect_ready_client(&server, &schema, "reader", "docs", READY).await;
            let mut stream = reader
                .subscribe_with_read_tier(Query::from("docs"), ReadTier::Remote)
                .await
                .unwrap();
            wait_for_bodies(&reader, &mut stream, &expected, "fresh subscriber").await;

            let id = insert_global(&writer, "later", &body(70_000, "later")).await;
            expected.insert(id, 70_000);
            wait_for_bodies(&reader, &mut stream, &expected, "open subscriber").await;
        })
        .await;
}
