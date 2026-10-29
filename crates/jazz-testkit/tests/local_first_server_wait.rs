//! Local first with a server-wait timeout through the public native client
//! (`JazzClient::query_local_first`, `JazzClient::subscribe_local_first`).
//!
//! While the server could answer, the initial load waits up to the timeout for
//! the server's answer, even when local knowledge is non-empty. Without a
//! live link it never waits.

use std::collections::BTreeSet;
use std::time::Duration;

use jazz::query::{Query, col, eq, lit};
use jazz::row_input;
use jazz::tools::test_support::{disconnect_client, ordinary_rows};
use jazz::tools::{
    ColumnType, JazzClient, ObjectId, OrderedRowDelta, ResultKey, Schema, SchemaBuilder,
    SubscriptionStream, SubscriptionStreamItem, TableSchema,
};
use jazz_server::JazzServer;
use jazz_testkit as support;

/// Bound for deliveries that must not wait on a remote view.
const IMMEDIATE: Duration = Duration::from_secs(2);
/// Bound for deliveries that wait on the server's answer.
const REMOTE: Duration = Duration::from_secs(10);
/// The server wait the reads ask for: longer than any bound above, so a
/// delivery within them is never the timeout's.
const WAIT: Duration = Duration::from_secs(60);

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
/// them, so a fresh reader's local store does not hold them yet.
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

/// A reader whose local store holds only its own write still opens on the
/// server's answer, which adds the other writer's rows to its own.
#[tokio::test(flavor = "current_thread")]
async fn non_empty_local_result_waits_for_the_servers_answer() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let schema = items_schema();
            let server = JazzServer::start_with_schema(schema.clone())
                .await
                .expect("start test server");
            let writer = connect(&server, &schema, "aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaa701").await;
            let reader = connect(&server, &schema, "aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaa702").await;
            let mut expected = seed_remote(&writer, &["remote one", "remote two"]).await;
            let (local_id, _, _) = reader
                .insert("items", row_input!("label" => "local"))
                .expect("insert local row");
            expected.insert(local_id);
            let expected = keys(&expected);

            let rows =
                tokio::time::timeout(REMOTE, reader.query_local_first(Query::from("items"), WAIT))
                    .await
                    .expect("the server answers before the bound")
                    .expect("one-shot succeeds");
            assert_eq!(
                ordinary_rows(rows)
                    .into_iter()
                    .map(|(id, _)| ResultKey::from(id))
                    .collect::<BTreeSet<_>>(),
                expected,
                "the one-shot returns the server's rows and the local write"
            );

            let mut stream = reader
                .subscribe_local_first(Query::from("items"), WAIT)
                .await
                .expect("subscribe");
            let opening = first_delta(&mut stream, REMOTE).await;
            assert_eq!(
                added_ids(&opening),
                expected,
                "the opening is the server's answer: {opening:?}"
            );
        })
        .await;
}

/// A disconnected reader cannot hear from the server, so neither read waits.
#[tokio::test(flavor = "current_thread")]
async fn a_disconnected_reader_does_not_wait() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let schema = items_schema();
            let server = JazzServer::start_with_schema(schema.clone())
                .await
                .expect("start test server");
            let writer = connect(&server, &schema, "aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaa703").await;
            let reader = connect(&server, &schema, "aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaa704").await;
            seed_remote(&writer, &["remote"]).await;
            assert!(disconnect_client(&reader), "detach the live transport");
            assert!(!reader.is_connected());
            let (local_id, _, _) = reader
                .insert("items", row_input!("label" => "local"))
                .expect("insert local row");
            let local_only = keys(&BTreeSet::from([local_id]));

            let rows = tokio::time::timeout(
                IMMEDIATE,
                reader.query_local_first(Query::from("items"), WAIT),
            )
            .await
            .expect("an offline one-shot does not wait")
            .expect("one-shot succeeds");
            assert_eq!(
                ordinary_rows(rows)
                    .into_iter()
                    .map(|(id, _)| ResultKey::from(id))
                    .collect::<BTreeSet<_>>(),
                local_only
            );

            let mut stream = reader
                .subscribe_local_first(Query::from("items"), WAIT)
                .await
                .expect("subscribe");
            let opening = first_delta(&mut stream, IMMEDIATE).await;
            assert_eq!(added_ids(&opening), local_only, "{opening:?}");
        })
        .await;
}

/// A reader whose local store holds nothing for the query opens on the
/// server's rows rather than its empty local result, for the subscription and
/// the one-shot alike.
///
/// ```text
/// writer ──insert a, b──► server
/// alice (empty store): subscribe (wait 60 s) ─► opening a, b
/// bob   (empty store): one-shot  (wait 60 s) ─► a, b
/// ```
#[tokio::test(flavor = "current_thread")]
async fn empty_local_result_waits_for_the_servers_rows() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let schema = items_schema();
            let server = JazzServer::start_with_schema(schema.clone())
                .await
                .expect("start test server");
            let writer = connect(&server, &schema, "aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaa711").await;
            let expected = keys(&seed_remote(&writer, &["first", "second"]).await);

            let alice = connect(&server, &schema, "aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaa712").await;
            let mut stream = alice
                .subscribe_local_first(Query::from("items"), WAIT)
                .await
                .expect("subscribe");
            let opening = first_delta(&mut stream, REMOTE).await;
            assert_eq!(
                added_ids(&opening),
                expected,
                "the empty local opening is withheld until the server's rows arrive: {opening:?}"
            );
            assert!(opening.removed.is_empty() && opening.updated.is_empty());

            let bob = connect(&server, &schema, "aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaa713").await;
            let rows =
                tokio::time::timeout(REMOTE, bob.query_local_first(Query::from("items"), WAIT))
                    .await
                    .expect("the server answers before the bound")
                    .expect("one-shot succeeds");
            assert_eq!(
                ordinary_rows(rows)
                    .into_iter()
                    .map(|(id, _)| ResultKey::from(id))
                    .collect::<BTreeSet<_>>(),
                expected,
                "the one-shot returns the server's rows"
            );
        })
        .await;
}

/// The server answering "nothing matches" releases the opening as a settled
/// empty result; neither read waits out its timeout for rows that will not
/// come.
///
/// ```text
/// alice: subscribe label = "absent" (wait 60 s) ─ server answers [] ─► settled empty opening
/// alice: one-shot  label = "absent" (wait 60 s) ─ server answers [] ─► []
/// ```
#[tokio::test(flavor = "current_thread")]
async fn an_empty_server_answer_releases_the_opening() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let schema = items_schema();
            let server = JazzServer::start_with_schema(schema.clone())
                .await
                .expect("start test server");
            let alice = connect(&server, &schema, "aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaa721").await;
            let query = Query::from("items").filter(eq(col("label"), lit("absent")));
            let mut stream = alice
                .subscribe_local_first(query.clone(), WAIT)
                .await
                .expect("subscribe");
            let opening = first_delta(&mut stream, REMOTE).await;
            assert!(opening.is_empty(), "{opening:?}");
            assert!(
                !opening.pending,
                "the empty opening carries the server's settled answer: {opening:?}"
            );
            let rows = tokio::time::timeout(REMOTE, alice.query_local_first(query, WAIT))
                .await
                .expect("the server answers before the bound")
                .expect("one-shot succeeds");
            assert!(rows.is_empty());
        })
        .await;
}

/// Without a configured server nothing can answer, so both reads are plain
/// local-first at once: the empty local opening is published immediately,
/// still pending (nothing has settled it, exactly as with a disconnected
/// server), and the first local write follows as an ordinary change.
///
/// ```text
/// alice (no server): one-shot (wait 60 s) ─► [] at once
/// alice: subscribe (wait 60 s) ─► empty pending opening at once ─ insert x ─► delta x
/// ```
#[tokio::test(flavor = "current_thread")]
async fn a_reader_without_a_server_does_not_wait() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let alice = JazzClient::test_client(items_schema()).await;
            let rows = tokio::time::timeout(
                IMMEDIATE,
                alice.query_local_first(Query::from("items"), WAIT),
            )
            .await
            .expect("a serverless one-shot does not wait")
            .expect("one-shot succeeds");
            assert!(rows.is_empty());

            let mut stream = alice
                .subscribe_local_first(Query::from("items"), WAIT)
                .await
                .expect("subscribe");
            let opening = first_delta(&mut stream, IMMEDIATE).await;
            assert!(opening.is_empty(), "{opening:?}");
            assert!(opening.pending, "{opening:?}");

            let (id, _, _) = alice
                .insert("items", row_input!("label" => "offline"))
                .expect("insert local row");
            let delta = first_delta(&mut stream, IMMEDIATE).await;
            assert_eq!(added_ids(&delta), keys(&BTreeSet::from([id])), "{delta:?}");
        })
        .await;
}
