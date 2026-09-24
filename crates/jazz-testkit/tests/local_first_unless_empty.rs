//! `ReadTier::LocalFirstUnlessEmpty` through the public native client.
//!
//! A non-empty local result opens immediately, exactly like `LocalFirst`. An
//! empty local opening waits for the first remote view only while an upstream
//! link is live, and never waits without one.

use std::collections::BTreeSet;
use std::time::Duration;

use jazz::query::{Query, col, eq, lit};
use jazz::row_input;
use jazz::tools::test_support::{disconnect_client, ordinary_rows};
use jazz::tools::{
    ColumnType, JazzClient, ObjectId, OrderedRowDelta, ReadTier, ResultKey, Schema, SchemaBuilder,
    SubscriptionStream, SubscriptionStreamItem, TableSchema,
};
use jazz_server::JazzServer;
use jazz_testkit as support;

/// Bound for deliveries that must not wait on a remote view.
const IMMEDIATE: Duration = Duration::from_secs(2);
/// Bound for deliveries that wait on the first remote view.
const REMOTE: Duration = Duration::from_secs(10);

fn items_schema() -> Schema {
    SchemaBuilder::new()
        .table(TableSchema::builder("items").column("label", ColumnType::Text))
        .build()
}

async fn connect(server: &JazzServer, schema: &Schema, user: &str) -> JazzClient {
    support::connect(server.make_client_context_for_user(schema.clone(), user))
        .await
        .expect("connect client")
}

/// Writes the rows through another client and waits until the server holds
/// them, so a fresh reader's local store starts empty for this query.
async fn seed_remote(writer: &JazzClient, labels: &[&str]) -> BTreeSet<ObjectId> {
    let mut ids = BTreeSet::new();
    let mut txs = Vec::new();
    for label in labels {
        let (id, _, tx) = writer
            .insert("items", row_input!("label" => *label))
            .expect("insert seed row");
        ids.insert(id);
        txs.push(tx.expect("ordinary mutation commits immediately"));
    }
    support::wait_for_global_txs(writer, &txs).await;
    ids
}

async fn first_delta(stream: &mut SubscriptionStream, within: Duration) -> OrderedRowDelta {
    let item = tokio::time::timeout(within, stream.next())
        .await
        .expect("opening delivery arrives in time")
        .expect("subscription stream stays open");
    match item {
        SubscriptionStreamItem::Delta(delta) => delta,
        SubscriptionStreamItem::Rejected { reason } => panic!("subscription rejected: {reason:?}"),
    }
}

fn added_ids(delta: &OrderedRowDelta) -> BTreeSet<ResultKey> {
    delta.added.iter().map(|added| added.id.clone()).collect()
}

fn keys(ids: &BTreeSet<ObjectId>) -> BTreeSet<ResultKey> {
    ids.iter().copied().map(ResultKey::from).collect()
}

/// Local knowledge opens immediately even while the remote view (which also
/// holds the other writer's rows) has not arrived.
#[tokio::test(flavor = "current_thread")]
async fn non_empty_local_result_opens_without_waiting_for_the_remote_view() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let schema = items_schema();
            let server = JazzServer::start_with_schema(schema.clone())
                .await
                .expect("start test server");
            let writer = connect(&server, &schema, "aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaa501").await;
            let reader = connect(&server, &schema, "aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaa502").await;
            seed_remote(&writer, &["remote one", "remote two"]).await;
            let (local_id, _, _) = reader
                .insert("items", row_input!("label" => "local"))
                .expect("insert local row");
            let local_only = keys(&BTreeSet::from([local_id]));

            let rows = tokio::time::timeout(
                IMMEDIATE,
                reader.query(Query::from("items"), ReadTier::LocalFirstUnlessEmpty),
            )
            .await
            .expect("non-empty local one-shot does not wait")
            .expect("one-shot succeeds");
            assert_eq!(
                ordinary_rows(rows)
                    .into_iter()
                    .map(|(id, _)| ResultKey::from(id))
                    .collect::<BTreeSet<_>>(),
                local_only,
                "a non-empty local one-shot returns local knowledge"
            );

            let mut stream = reader
                .subscribe_with_read_tier(Query::from("items"), ReadTier::LocalFirstUnlessEmpty)
                .await
                .expect("subscribe");
            let opening = first_delta(&mut stream, IMMEDIATE).await;
            assert_eq!(
                added_ids(&opening),
                local_only,
                "the opening is the local result: {opening:?}"
            );
            assert!(
                opening.pending,
                "the opening is delivered before the remote view settles: {opening:?}"
            );
        })
        .await;
}

async fn assert_empty_local_waits_for_remote_rows(tier: ReadTier, users: [&str; 3]) {
    let schema = items_schema();
    let server = JazzServer::start_with_schema(schema.clone())
        .await
        .expect("start test server");
    let writer = connect(&server, &schema, users[0]).await;
    let expected = keys(&seed_remote(&writer, &["first", "second"]).await);

    let reader = connect(&server, &schema, users[1]).await;
    let mut stream = reader
        .subscribe_with_read_tier(Query::from("items"), tier)
        .await
        .expect("subscribe");
    let opening = first_delta(&mut stream, REMOTE).await;
    assert_eq!(
        added_ids(&opening),
        expected,
        "an empty local opening is withheld until the remote rows arrive: {opening:?}"
    );
    assert!(opening.removed.is_empty() && opening.updated.is_empty());

    let one_shot_reader = connect(&server, &schema, users[2]).await;
    let rows = tokio::time::timeout(REMOTE, one_shot_reader.query(Query::from("items"), tier))
        .await
        .expect("remote one-shot settles")
        .expect("one-shot succeeds");
    assert_eq!(
        ordinary_rows(rows)
            .into_iter()
            .map(|(id, _)| ResultKey::from(id))
            .collect::<BTreeSet<_>>(),
        expected,
        "an empty local one-shot returns the remote rows"
    );
}

#[tokio::test(flavor = "current_thread")]
async fn empty_local_result_waits_for_the_first_remote_view_while_connected() {
    tokio::task::LocalSet::new()
        .run_until(assert_empty_local_waits_for_remote_rows(
            ReadTier::LocalFirstUnlessEmpty,
            [
                "aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaa511",
                "aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaa512",
                "aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaa513",
            ],
        ))
        .await;
}

#[tokio::test(flavor = "current_thread")]
#[allow(deprecated)]
async fn deprecated_remote_if_possible_alias_reads_like_local_first_unless_empty() {
    assert_eq!(ReadTier::RemoteIfPossible, ReadTier::LocalFirstUnlessEmpty);
    tokio::task::LocalSet::new()
        .run_until(assert_empty_local_waits_for_remote_rows(
            ReadTier::RemoteIfPossible,
            [
                "aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaa521",
                "aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaa522",
                "aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaa523",
            ],
        ))
        .await;
}

/// An empty remote view still releases the opening once it settles.
#[tokio::test(flavor = "current_thread")]
async fn empty_local_and_empty_remote_view_opens_once_the_remote_view_settles() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let schema = items_schema();
            let server = JazzServer::start_with_schema(schema.clone())
                .await
                .expect("start test server");
            let reader = connect(&server, &schema, "aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaa531").await;
            let query = Query::from("items").filter(eq(col("label"), lit("absent")));
            let mut stream = reader
                .subscribe_with_read_tier(query.clone(), ReadTier::LocalFirstUnlessEmpty)
                .await
                .expect("subscribe");
            let opening = first_delta(&mut stream, REMOTE).await;
            assert!(opening.is_empty(), "{opening:?}");
            assert!(
                !opening.pending,
                "the empty opening carries the settled remote view: {opening:?}"
            );
            let rows =
                tokio::time::timeout(REMOTE, reader.query(query, ReadTier::LocalFirstUnlessEmpty))
                    .await
                    .expect("one-shot settles")
                    .expect("one-shot succeeds");
            assert!(rows.is_empty());
        })
        .await;
}

/// Without a configured server nothing can supply a remote view, so the read
/// is plain local-first. (A serverless native client publishes a subscription
/// only from local changes, for `LocalFirst` too, so the subscription is
/// observed through its first local write.)
#[tokio::test(flavor = "current_thread")]
async fn empty_local_result_without_a_server_opens_immediately() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let client = JazzClient::test_client(items_schema()).await;
            let rows = tokio::time::timeout(
                IMMEDIATE,
                client.query(Query::from("items"), ReadTier::LocalFirstUnlessEmpty),
            )
            .await
            .expect("offline one-shot does not wait")
            .expect("one-shot succeeds");
            assert!(rows.is_empty());

            let mut stream = client
                .subscribe_with_read_tier(Query::from("items"), ReadTier::LocalFirstUnlessEmpty)
                .await
                .expect("subscribe");
            let (id, _, _) = client
                .insert("items", row_input!("label" => "offline"))
                .expect("insert local row");
            let delta = first_delta(&mut stream, IMMEDIATE).await;
            assert_eq!(added_ids(&delta), keys(&BTreeSet::from([id])), "{delta:?}");
        })
        .await;
}

/// A configured server whose link is down cannot supply the remote view, so
/// the empty local result opens immediately even though the server holds
/// matching rows.
#[tokio::test(flavor = "current_thread")]
async fn empty_local_result_with_a_disconnected_server_opens_immediately() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let schema = items_schema();
            let server = JazzServer::start_with_schema(schema.clone())
                .await
                .expect("start test server");
            let writer = connect(&server, &schema, "aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaa541").await;
            seed_remote(&writer, &["unreachable"]).await;
            let reader = connect(&server, &schema, "aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaa542").await;
            assert!(disconnect_client(&reader), "detach the live transport");
            assert!(!reader.is_connected());

            let rows = tokio::time::timeout(
                IMMEDIATE,
                reader.query(Query::from("items"), ReadTier::LocalFirstUnlessEmpty),
            )
            .await
            .expect("disconnected one-shot does not wait")
            .expect("one-shot succeeds");
            assert!(rows.is_empty());

            let mut stream = reader
                .subscribe_with_read_tier(Query::from("items"), ReadTier::LocalFirstUnlessEmpty)
                .await
                .expect("subscribe");
            let opening = first_delta(&mut stream, IMMEDIATE).await;
            assert!(opening.is_empty(), "{opening:?}");
            assert!(opening.pending, "{opening:?}");
        })
        .await;
}
