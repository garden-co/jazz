use std::collections::BTreeMap;
use std::time::Duration;

use jazz::query::{ArraySubquery, Query};
use jazz::row_input;
use jazz::tools::test_support::AllowAll;
use jazz::tools::{
    ColumnType, JazzClient, ObjectId, OrderedRowDelta, ReadTier, Row, SchemaBuilder, TableSchema,
    Value,
};
use jazz_server::JazzServer;
use jazz_testkit::{
    TestingClient, collect_stream_deltas, wait_for_global_txs, wait_for_subscription_update,
};

struct Fixture {
    server: JazzServer,
    alice: JazzClient,
    transport: jazz_testkit::TransportControl,
    schema: jazz::tools::Schema,
}

impl Fixture {
    async fn start() -> Self {
        let schema = SchemaBuilder::new()
            .table(TableSchema::builder("assets").column("status", ColumnType::Text))
            .table(TableSchema::builder("messages").column("text", ColumnType::Text))
            .table(
                TableSchema::builder("attachments")
                    .fk_column("message_id", "messages")
                    .fk_column("asset_id", "assets"),
            )
            .allow_all()
            .build();
        let server = JazzServer::start_with_schema(schema.clone()).await.unwrap();
        let transport = jazz_testkit::TransportControl::default();
        let alice = TestingClient::builder()
            .with_server(&server)
            .with_schema(schema.clone())
            .with_user_id("alice")
            .with_transport_control(transport.clone())
            .ready_on("messages", Duration::from_secs(10))
            .connect()
            .await;
        Self {
            server,
            alice,
            transport,
            schema,
        }
    }

    async fn shutdown(self) {
        self.alice.shutdown().await.unwrap();
        self.server.shutdown().await;
    }
}

fn query() -> Query {
    Query::from("messages").array_subquery(
        ArraySubquery::new("attachments", "attachments", "message_id", "id")
            .nested(ArraySubquery::new("asset", "assets", "id", "asset_id")),
    )
}

fn has_asset(attachments: &Value, asset: ObjectId) -> bool {
    attachments.as_array().unwrap().iter().any(|attachment| {
        attachment
            .as_row()
            .unwrap()
            .last()
            .unwrap()
            .as_array()
            .unwrap()
            .iter()
            .any(|row| row.row_id() == Some(asset))
    })
}

fn row_has_asset(row: &Row, asset: ObjectId) -> bool {
    has_asset(row.get("attachments").unwrap(), asset)
}

// This query has one root, so only membership and row contents need reducing.
// Include removals: selecting the last nonempty update could hide the bug.
fn current_rows(log: &[OrderedRowDelta]) -> Vec<Row> {
    let mut rows = BTreeMap::new();
    for delta in log {
        for removed in &delta.removed {
            rows.remove(&removed.id);
        }
        for added in &delta.added {
            rows.insert(added.id.clone(), added.row.clone());
        }
        for updated in &delta.updated {
            if let Some(row) = &updated.row {
                rows.insert(updated.id.clone(), row.clone());
            }
        }
    }
    rows.into_values().collect()
}

fn assert_asset_stays_visible(log: &[OrderedRowDelta], asset: ObjectId) {
    assert!(!log.last().expect("subscription delivered changes").pending);
    let mut appeared = false;
    for end in 1..=log.len() {
        let rows = current_rows(&log[..end]);
        let visible = rows.iter().any(|row| row_has_asset(row, asset));
        assert!(
            !appeared || visible,
            "nested asset disappeared from the live view at delivery {end}"
        );
        appeared |= visible;
    }
    assert!(appeared, "asset must appear in the live view");
    assert_eq!(current_rows(log).len(), 1);
}

async fn assert_fresh_read_has_asset(alice: &JazzClient, asset: ObjectId) {
    let rows = tokio::time::timeout(
        Duration::from_secs(10),
        alice.query(query(), ReadTier::Remote),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(rows.len(), 1);
    assert!(
        has_asset(&rows[0].fields.last().unwrap().value, asset),
        "fresh read retains the asset"
    );
}

/// Alice retains her nested asset after server responses arrive together.
/// This exercises replacement ordering across multiple updates in one drain.
/// alice subscribes -> inserts three related rows -> queued replies released -> asset remains
#[tokio::test]
async fn writer_keeps_nested_asset_after_batched_settlement() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let f = Fixture::start().await;
            let mut stream = f
                .alice
                .subscribe_with_read_tier(query(), ReadTier::Remote)
                .await
                .unwrap();
            let mut log = vec![];
            wait_for_subscription_update(
                &mut stream,
                &mut log,
                Duration::from_secs(10),
                "initial empty subscription",
                |log| log.iter().any(|delta| !delta.pending),
            )
            .await;
            log.clear();
            f.transport.block_inbound();

            let (asset, _, asset_tx) = f
                .alice
                .insert("assets", row_input!("status" => "pending"))
                .unwrap();
            let (message, _, message_tx) = f
                .alice
                .insert("messages", row_input!("text" => "photo"))
                .unwrap();
            let (_, _, attachment_tx) = f
                .alice
                .insert(
                    "attachments",
                    row_input!("message_id" => message, "asset_id" => asset),
                )
                .unwrap();

            // Bob observes the complete server state while Alice's responses
            // remain blocked. Releasing her link then delivers the queued updates.
            let bob = TestingClient::builder()
                .with_server(&f.server)
                .with_schema(f.schema.clone())
                .with_user_id("bob")
                .connect()
                .await;
            assert_fresh_read_has_asset(&bob, asset).await;
            f.transport.unblock().unwrap();
            bob.shutdown().await.unwrap();

            wait_for_global_txs(
                &f.alice,
                &[
                    asset_tx.unwrap(),
                    message_tx.unwrap(),
                    attachment_tx.unwrap(),
                ],
            )
            .await;
            collect_stream_deltas(&mut stream, &mut log, Duration::from_millis(500)).await;
            assert_fresh_read_has_asset(&f.alice, asset).await;
            assert!(!log.last().expect("subscription delivered changes").pending);
            let rows = current_rows(&log);
            assert_eq!(rows.len(), 1);
            assert!(
                row_has_asset(&rows[0], asset),
                "settled subscription retains the nested asset"
            );
            f.shutdown().await;
        })
        .await;
}

/// Alice's optimistic nested asset must survive acknowledgement of all three writes.
/// alice subscribes -> inserts asset, message, attachment -> server settles -> asset remains
#[tokio::test]
async fn writer_keeps_nested_asset_through_settlement() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let f = Fixture::start().await;
            let mut stream = f
                .alice
                .subscribe_with_read_tier(query(), ReadTier::Remote)
                .await
                .unwrap();
            let mut log = vec![];
            wait_for_subscription_update(
                &mut stream,
                &mut log,
                Duration::from_secs(10),
                "initial empty subscription",
                |log| log.iter().any(|delta| !delta.pending),
            )
            .await;
            log.clear();

            let (asset, _, asset_tx) = f
                .alice
                .insert("assets", row_input!("status" => "pending"))
                .unwrap();
            let (message, _, message_tx) = f
                .alice
                .insert("messages", row_input!("text" => "photo"))
                .unwrap();
            let (_, _, attachment_tx) = f
                .alice
                .insert(
                    "attachments",
                    row_input!("message_id" => message, "asset_id" => asset),
                )
                .unwrap();

            wait_for_global_txs(
                &f.alice,
                &[
                    asset_tx.unwrap(),
                    message_tx.unwrap(),
                    attachment_tx.unwrap(),
                ],
            )
            .await;
            collect_stream_deltas(&mut stream, &mut log, Duration::from_millis(500)).await;
            assert_fresh_read_has_asset(&f.alice, asset).await;
            assert_asset_stays_visible(&log, asset);
            f.shutdown().await;
        })
        .await;
}

/// Alice waits for each insert to settle before inserting the next related row.
#[tokio::test]
async fn writer_keeps_nested_asset_when_each_insert_settles() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let f = Fixture::start().await;
            let mut stream = f
                .alice
                .subscribe_with_read_tier(query(), ReadTier::Remote)
                .await
                .unwrap();
            let mut log = vec![];
            wait_for_subscription_update(
                &mut stream,
                &mut log,
                Duration::from_secs(10),
                "initial empty subscription",
                |log| log.iter().any(|delta| !delta.pending),
            )
            .await;
            log.clear();

            let (asset, _, tx) = f
                .alice
                .insert("assets", row_input!("status" => "pending"))
                .unwrap();
            wait_for_global_txs(&f.alice, &[tx.unwrap()]).await;
            let (message, _, tx) = f
                .alice
                .insert("messages", row_input!("text" => "photo"))
                .unwrap();
            wait_for_global_txs(&f.alice, &[tx.unwrap()]).await;
            let (_, _, tx) = f
                .alice
                .insert(
                    "attachments",
                    row_input!("message_id" => message, "asset_id" => asset),
                )
                .unwrap();
            wait_for_global_txs(&f.alice, &[tx.unwrap()]).await;

            collect_stream_deltas(&mut stream, &mut log, Duration::from_millis(500)).await;
            assert_fresh_read_has_asset(&f.alice, asset).await;
            assert_asset_stays_visible(&log, asset);
            f.shutdown().await;
        })
        .await;
}

/// Alice's local-first subscription retains her asset while the writes settle remotely.
#[tokio::test]
async fn writer_keeps_nested_asset_in_local_first_subscription() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let f = Fixture::start().await;
            let mut stream = f
                .alice
                .subscribe_with_read_tier(query(), ReadTier::LocalFirst)
                .await
                .unwrap();
            let mut log = vec![];
            wait_for_subscription_update(
                &mut stream,
                &mut log,
                Duration::from_secs(10),
                "initial empty subscription",
                |log| log.iter().any(|delta| !delta.pending),
            )
            .await;
            log.clear();

            let (asset, _, asset_tx) = f
                .alice
                .insert("assets", row_input!("status" => "pending"))
                .unwrap();
            let (message, _, message_tx) = f
                .alice
                .insert("messages", row_input!("text" => "photo"))
                .unwrap();
            let (_, _, attachment_tx) = f
                .alice
                .insert(
                    "attachments",
                    row_input!("message_id" => message, "asset_id" => asset),
                )
                .unwrap();
            wait_for_global_txs(
                &f.alice,
                &[
                    asset_tx.unwrap(),
                    message_tx.unwrap(),
                    attachment_tx.unwrap(),
                ],
            )
            .await;

            collect_stream_deltas(&mut stream, &mut log, Duration::from_millis(500)).await;
            assert_fresh_read_has_asset(&f.alice, asset).await;
            assert_asset_stays_visible(&log, asset);
            f.shutdown().await;
        })
        .await;
}

/// A remote read is answered by the server after Alice's earlier writes reach
/// it, so it sees them without waiting for their acknowledgements.
#[tokio::test]
async fn writer_reads_own_nested_writes_remotely_right_after_inserting() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let f = Fixture::start().await;
            let (asset, _, _) = f
                .alice
                .insert("assets", row_input!("status" => "pending"))
                .unwrap();
            let (message, _, _) = f
                .alice
                .insert("messages", row_input!("text" => "photo"))
                .unwrap();
            f.alice
                .insert(
                    "attachments",
                    row_input!("message_id" => message, "asset_id" => asset),
                )
                .unwrap();

            assert_fresh_read_has_asset(&f.alice, asset).await;
            f.shutdown().await;
        })
        .await;
}
