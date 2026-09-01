use jazz_testkit as support;

use std::collections::BTreeSet;
use std::time::Duration;

use jazz::query::{Query, col, eq, lit, table};
use jazz::row_input;
use jazz::tools::{
    ColumnType, DurabilityTier, QueryResult, ReadTier, ResultKey, Schema, SchemaBuilder,
    SubscriptionStreamItem, TableSchema, Value,
};
use jazz_server::JazzServer;
use support::TestingClient;

fn todos_schema() -> Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("todos")
                .column("title", ColumnType::Text)
                .column("bucket", ColumnType::Text)
                .column("done", ColumnType::Boolean),
        )
        .build()
}

async fn next_delta(stream: &mut jazz::tools::SubscriptionStream) -> jazz::tools::OrderedRowDelta {
    let item = tokio::time::timeout(Duration::from_secs(10), stream.next())
        .await
        .expect("subscription delta arrives")
        .expect("subscription remains open");
    let SubscriptionStreamItem::Delta(delta) = item else {
        panic!("subscription was rejected")
    };
    delta
}

async fn next_delta_with_added(
    stream: &mut jazz::tools::SubscriptionStream,
) -> jazz::tools::OrderedRowDelta {
    for _ in 0..8 {
        let delta = next_delta(stream).await;
        if !delta.added.is_empty() {
            return delta;
        }
    }
    panic!("subscription did not emit an added output occurrence")
}

async fn next_delta_with_updated(
    stream: &mut jazz::tools::SubscriptionStream,
) -> jazz::tools::OrderedRowDelta {
    for _ in 0..8 {
        let delta = next_delta(stream).await;
        if !delta.updated.is_empty() {
            return delta;
        }
    }
    panic!("subscription did not emit an updated output occurrence")
}

async fn next_delta_with_removed(
    stream: &mut jazz::tools::SubscriptionStream,
) -> jazz::tools::OrderedRowDelta {
    for _ in 0..8 {
        let delta = next_delta(stream).await;
        if !delta.removed.is_empty() {
            return delta;
        }
    }
    panic!("subscription did not emit a removed output occurrence")
}

fn key_for_joined_title(results: &[QueryResult], title: &str) -> ResultKey {
    results
        .iter()
        .find(|result| result.get("joined.title") == Some(&Value::Text(title.to_owned())))
        .unwrap_or_else(|| panic!("missing joined result with title {title}: {results:?}"))
        .key
        .clone()
}

fn joined_todos(sources: &[(&str, &str, &str)]) -> Query {
    sources.iter().fold(
        Query::from(table("todos").alias("root")).filter(eq(col("root.done"), lit(false))),
        |query, (alias, left, right)| query.flat_join(table("todos").alias(*alias), *left, *right),
    )
}

#[tokio::test(flavor = "current_thread")]
async fn forwarded_flat_join_reset_keeps_contributor_facts_visible_to_one_shot_reads() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let schema = todos_schema();
            let server = JazzServer::start_with_schema(schema.clone()).await;
            let client = TestingClient::builder()
                .with_server(&server)
                .with_schema(schema)
                .with_user_id("00000000-0000-4000-8000-000000000026")
                .ready_on("todos", Duration::from_secs(30))
                .connect()
                .await;
            let (_root, _, tx) = client
                .insert(
                    "todos",
                    row_input!("title" => "root", "bucket" => "shared", "done" => false),
                )
                .expect("insert root");
            support::wait_for_edge_txs(
                &client,
                &[tx.expect("ordinary mutation commits immediately")],
            )
            .await;
            let (_first_joined, _, tx) = client
                .insert(
                    "todos",
                    row_input!("title" => "first", "bucket" => "shared", "done" => true),
                )
                .expect("insert first joined source");
            support::wait_for_edge_txs(
                &client,
                &[tx.expect("ordinary mutation commits immediately")],
            )
            .await;

            // Both tuple positions deliberately use the same physical table.
            // The forwarded reset still needs a distinct contributor role for
            // each source position, even when both positions select one row.
            let query = joined_todos(&[
                ("joined", "root.bucket", "joined.bucket"),
                ("third", "joined.bucket", "third.bucket"),
            ]);
            let mut stream = client.subscribe(query.clone()).await.expect("subscribe");
            let reset = next_delta(&mut stream).await;
            let root_occurrence = reset.added[0].id.clone();

            let (_joined, _, tx) = client
                .insert(
                    "todos",
                    row_input!("title" => "second", "bucket" => "shared", "done" => true),
                )
                .expect("insert joined source");
            client
                .wait_for_transaction(
                    tx.expect("ordinary mutation commits immediately"),
                    DurabilityTier::Local,
                )
                .await
                .expect("joined source settles locally");
            let joined_occurrence = key_for_joined_title(
                &client
                    .query_results_with_read_tier(query, ReadTier::LocalFirst)
                    .await
                    .expect("one-shot flat join remains complete"),
                "second",
            );
            for _ in 0..8 {
                let delta = next_delta(&mut stream).await;
                assert!(
                    delta.removed.iter().all(|row| row.id != root_occurrence),
                    "one authority generation must not transiently retract its existing occurrence"
                );
                assert!(
                    delta.added.iter().all(|row| row.id != root_occurrence),
                    "one authority generation must not re-add its existing occurrence"
                );
                if delta.added.iter().any(|row| row.id == joined_occurrence) {
                    client.shutdown().await.expect("shutdown client");
                    server.shutdown().await;
                    return;
                }
            }
            panic!("joined occurrence was not published");
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn forwarded_flat_join_reconciles_joined_source_deletion() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let schema = todos_schema();
            let server = JazzServer::start_with_schema(schema.clone()).await;
            let client = TestingClient::builder()
                .with_server(&server)
                .with_schema(schema)
                .with_user_id("00000000-0000-4000-8000-000000000027")
                .ready_on("todos", Duration::from_secs(30))
                .connect()
                .await;
            let (_root, _, tx) = client
                .insert(
                    "todos",
                    row_input!("title" => "root", "bucket" => "shared", "done" => false),
                )
                .expect("insert root");
            support::wait_for_edge_txs(
                &client,
                &[tx.expect("ordinary mutation commits immediately")],
            )
            .await;
            let (joined, _, tx) = client
                .insert(
                    "todos",
                    row_input!("title" => "joined", "bucket" => "shared", "done" => true),
                )
                .expect("insert joined source");
            support::wait_for_edge_txs(
                &client,
                &[tx.expect("ordinary mutation commits immediately")],
            )
            .await;

            let query = joined_todos(&[("joined", "root.bucket", "joined.bucket")]);
            let initial = client
                .query_results_with_read_tier(query.clone(), ReadTier::LocalFirst)
                .await
                .expect("query initial joined results");
            let joined_occurrence = key_for_joined_title(&initial, "joined");
            let mut stream = client.subscribe(query.clone()).await.expect("subscribe");
            let reset = next_delta(&mut stream).await;
            assert!(
                reset.added.iter().any(|row| row.id == joined_occurrence),
                "initial reset includes joined-source occurrence"
            );

            let tx = client.delete(joined).expect("delete joined source");
            support::wait_for_edge_txs(
                &client,
                &[tx.expect("ordinary mutation commits immediately")],
            )
            .await;
            let removal = next_delta_with_removed(&mut stream).await;
            assert!(
                removal
                    .removed
                    .iter()
                    .any(|row| row.id == joined_occurrence),
                "deleting a joined source retracts its forwarded occurrence"
            );
            assert!(
                client
                    .query_results_with_read_tier(query, ReadTier::LocalFirst)
                    .await
                    .expect("query after joined-source deletion")
                    .iter()
                    .all(|row| row.key != joined_occurrence),
                "one-shot and maintained membership agree after deletion"
            );

            client.shutdown().await.expect("shutdown client");
            server.shutdown().await;
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn flat_join_output_occurrence_identity_addresses_additions_removals_and_replacements() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let schema = todos_schema();
            let server = JazzServer::start_with_schema(schema.clone()).await;
            let client = TestingClient::builder()
                .with_server(&server)
                .with_schema(schema)
                .with_user_id("00000000-0000-4000-8000-000000000023")
                .ready_on("todos", Duration::from_secs(30))
                .connect()
                .await;
            let (root, _, tx) = client
                .insert(
                    "todos",
                    row_input!("title" => "draft", "bucket" => "shared", "done" => false),
                )
                .expect("insert todo");
            support::wait_for_edge_txs(&client, &[tx.expect("ordinary mutation commits immediately")]).await;

            let joined_query = joined_todos(&[("joined", "root.bucket", "joined.bucket")]);
            let mut joined_stream = client
                .subscribe(joined_query.clone())
                .await
                .expect("joined maintained output is supported");
            let joined_reset = next_delta(&mut joined_stream).await;
            let initial_results = client
                .query_results(joined_query.clone(), Some(DurabilityTier::Local))
                .await
                .expect("one-shot joined output is supported");
            let self_key = key_for_joined_title(&initial_results, "draft");
            assert!(
                joined_reset
                    .added
                    .iter()
                    .any(|row| row.id == self_key),
                "the root is the only initial join contributor"
            );
            let encoded = serde_json::to_vec(&self_key).expect("serialize result key");
            let wire: Vec<u8> = serde_json::from_slice(&encoded).expect("inspect opaque key bytes");
            assert_eq!(wire.first(), Some(&1), "result key wire format is versioned");
            assert_eq!(
                serde_json::from_slice::<ResultKey>(&encoded).expect("deserialize result key"),
                self_key,
                "result keys retain their complete opaque identity through serialization"
            );
            let mut unsupported = wire;
            unsupported[0] = 3;
            assert!(
                serde_json::from_value::<ResultKey>(serde_json::json!(unsupported)).is_err(),
                "unknown ResultKey wire versions fail closed"
            );
            let tx = client.begin_transaction().expect("begin joined read-your-writes tx");
            tx.insert(
                "todos",
                row_input!("title" => "staged", "bucket" => "shared", "done" => true),
            )
            .expect("stage joined-side insert");
            let staged_results = tx
                .query_results(joined_query.clone(), Some(DurabilityTier::Local))
                .await
                .expect("joined query reads its staged write");
            assert_eq!(
                key_for_joined_title(&staged_results, "staged")
                    .row_id(),
                None,
                "a joined transaction result cannot collapse to a source row id"
            );
            tx.rollback().expect("roll back staged joined-side insert");

            let (first, _, tx) = client
                .insert(
                    "todos",
                    row_input!("title" => "first", "bucket" => "shared", "done" => true),
                )
                .expect("insert first matching joined row");
            client
                .wait_for_transaction(
                    tx.expect("ordinary mutation commits immediately"),
                    DurabilityTier::Local,
                )
                .await
                .expect("first joined row settles locally");
            let first_added = next_delta_with_added(&mut joined_stream).await;
            let first_key = key_for_joined_title(
                &client
                    .query_results(joined_query.clone(), Some(DurabilityTier::Local))
                    .await
                    .expect("query joined results after first fan-out"),
                "first",
            );
            assert!(
                first_added
                    .added
                    .iter()
                    .any(|row| row.id == first_key),
                "the first joined row is addressed beneath its root: {first_added:?}"
            );

            let (second, _, tx) = client
                .insert(
                    "todos",
                    row_input!("title" => "second", "bucket" => "shared", "done" => true),
                )
                .expect("insert second matching join row");
            support::wait_for_edge_txs(&client, &[tx.expect("ordinary mutation commits immediately")]).await;
            let fan_out = next_delta_with_added(&mut joined_stream).await;
            let current_results = client
                .query_results(joined_query.clone(), Some(DurabilityTier::Local))
                .await
                .expect("query joined results after second fan-out");
            let second_key = key_for_joined_title(&current_results, "second");
            assert_ne!(first_key, second_key, "fan-out results have distinct keys");
            assert!(
                fan_out
                    .added
                    .iter()
                    .any(|row| row.id == second_key),
                "a second matching source produces a distinct occurrence under the same root: {fan_out:?}"
            );
            let two_hop_query = joined_todos(&[
                ("first_hop", "root.bucket", "first_hop.bucket"),
                ("second_hop", "first_hop.bucket", "second_hop.bucket"),
            ]);
            let two_hop_results = client
                .query_results(two_hop_query.clone(), Some(DurabilityTier::Local))
                .await
                .expect("two-hop one-shot join");
            let ordered = two_hop_results
                .iter()
                .find(|result| {
                    result.get("first_hop.title") == Some(&Value::Text("first".to_owned()))
                        && result.get("second_hop.title")
                            == Some(&Value::Text("second".to_owned()))
                })
                .expect("ordered two-hop result")
                .key
                .clone();
            let reversed = two_hop_results
                .iter()
                .find(|result| {
                    result.get("first_hop.title") == Some(&Value::Text("second".to_owned()))
                        && result.get("second_hop.title")
                            == Some(&Value::Text("first".to_owned()))
                })
                .expect("reversed two-hop result")
                .key
                .clone();
            assert_ne!(ordered, reversed, "join position is part of ResultKey");
            let encoded = serde_json::to_vec(&ordered).expect("serialize three-part key");
            assert_eq!(
                serde_json::from_slice::<ResultKey>(&encoded).expect("deserialize three-part key"),
                ordered
            );
            let mut two_hop_stream = client
                .subscribe(two_hop_query)
                .await
                .expect("subscribe to two-hop flat join");
            let two_hop_reset = next_delta_with_added(&mut two_hop_stream).await;
            assert_eq!(
                two_hop_reset
                    .added
                    .iter()
                    .map(|change| change.id.clone())
                    .collect::<BTreeSet<_>>(),
                two_hop_results
                    .iter()
                    .map(|result| result.key.clone())
                    .collect(),
                "one-shot and maintained two-hop keys are identical"
            );
            let tx = client
                .update(
                    root,
                    vec![("title".to_owned(), Value::Text("revised".to_owned()))],
                )
                .expect("replace root source content");
            support::wait_for_edge_txs(&client, &[tx.expect("ordinary mutation commits immediately")]).await;
            let replacement = next_delta_with_updated(&mut joined_stream).await;
            let two_hop_root_replacement = next_delta_with_updated(&mut two_hop_stream).await;
            assert!(
                replacement
                    .updated
                    .iter()
                    .any(|row| row.id == second_key),
                "a root-source replacement is addressed by its composite occurrence id"
            );
            assert!(
                two_hop_root_replacement
                    .updated
                    .iter()
                    .any(|change| change.id == ordered),
                "root replacement retains the three-component result key"
            );

            let tx = client
                .update(
                    second,
                    vec![("title".to_owned(), Value::Text("second revised".to_owned()))],
                )
                .expect("replace joined source content");
            support::wait_for_edge_txs(&client, &[tx.expect("ordinary mutation commits immediately")]).await;
            let joined_replacement = next_delta_with_updated(&mut joined_stream).await;
            let two_hop_joined_replacement = next_delta_with_updated(&mut two_hop_stream).await;
            assert!(
                joined_replacement
                    .updated
                    .iter()
                    .any(|row| row.id == second_key),
                "a joined-source replacement retains its result key"
            );
            assert!(
                two_hop_joined_replacement
                    .updated
                    .iter()
                    .any(|change| change.id == ordered),
                "second-hop replacement retains the three-component result key"
            );
            assert_eq!(
                client
                    .query_results(joined_query.clone(), Some(DurabilityTier::Local))
                    .await
                    .expect("query after joined-side replacement")
                    .into_iter()
                    .find(|result| result.key == second_key)
                    .and_then(|result| result.get("joined.title").cloned()),
                Some(Value::Text("second revised".to_owned()))
            );

            let tx = client.delete(first).expect("remove first joined row");
            support::wait_for_edge_txs(&client, &[tx.expect("ordinary mutation commits immediately")]).await;
            let removal = next_delta_with_removed(&mut joined_stream).await;
            let two_hop_removal = next_delta_with_removed(&mut two_hop_stream).await;
            assert!(
                removal
                    .removed
                    .iter()
                    .any(|row| row.id == first_key),
                "a joined-source removal is addressed by its composite occurrence id"
            );
            assert!(
                two_hop_removal
                    .removed
                    .iter()
                    .any(|change| change.id == ordered),
                "first-hop removal retracts the exact three-component key"
            );
            assert!(
                two_hop_removal
                    .removed
                    .iter()
                    .any(|change| change.id == reversed),
                "second-hop removal retracts the exact three-component key"
            );

            drop(joined_stream);
            let rehydrated = TestingClient::builder()
                .with_server(&server)
                .with_schema(todos_schema())
                .with_user_id("00000000-0000-4000-8000-000000000024")
                .ready_on("todos", Duration::from_secs(30))
                .connect()
                .await;
            let mut rehydrated_stream = rehydrated
                .subscribe(
                    joined_todos(&[("joined", "root.bucket", "joined.bucket")]),
                )
                .await
                .expect("rehydrated joined maintained output is supported");
            let rehydrated_reset = next_delta_with_added(&mut rehydrated_stream).await;
            assert!(
                rehydrated_reset
                    .added
                    .iter()
                    .any(|row| row.id == second_key),
                "reset/rehydrate preserves the remaining composite occurrence id: {rehydrated_reset:?}"
            );

            rehydrated.shutdown().await.expect("shutdown rehydrated client");
            client.shutdown().await.expect("shutdown test client");
            server.shutdown().await;
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn flat_join_payload_netting_drops_add_then_remove_in_one_transaction() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let schema = todos_schema();
            let server = JazzServer::start_with_schema(schema.clone()).await;
            let client = TestingClient::builder()
                .with_server(&server)
                .with_schema(schema)
                .with_user_id("00000000-0000-4000-8000-000000000025")
                .ready_on("todos", Duration::from_secs(30))
                .connect()
                .await;
            let (_root, _, tx) = client
                .insert(
                    "todos",
                    row_input!("title" => "root", "bucket" => "shared", "done" => false),
                )
                .expect("insert root");
            support::wait_for_edge_txs(
                &client,
                &[tx.expect("ordinary mutation commits immediately")],
            )
            .await;

            let joined_query = joined_todos(&[("joined", "root.bucket", "joined.bucket")]);
            let mut stream = client
                .subscribe(joined_query)
                .await
                .expect("subscribe to flat joined output");
            let _reset = next_delta(&mut stream).await;

            let tx = client.begin_transaction().expect("begin transaction");
            let (transient, _, _) = tx
                .insert(
                    "todos",
                    row_input!("title" => "transient", "bucket" => "shared", "done" => true),
                )
                .expect("stage matching joined row");
            tx.delete(transient)
                .expect("stage removal of that same joined occurrence");
            let net_tx = tx.commit().expect("commit add-then-remove tx");

            let (_durable, _, durable_tx) = client
                .insert(
                    "todos",
                    row_input!("title" => "durable", "bucket" => "shared", "done" => true),
                )
                .expect("insert durable matching joined row");
            support::wait_for_edge_txs(
                &client,
                &[
                    net_tx,
                    durable_tx.expect("ordinary mutation commits immediately"),
                ],
            )
            .await;
            let delta = next_delta_with_added(&mut stream).await;
            let results = client
                .query_results(
                    joined_todos(&[("joined", "root.bucket", "joined.bucket")]),
                    Some(DurabilityTier::Local),
                )
                .await
                .expect("query joined results after netting");
            let durable_key = key_for_joined_title(&results, "durable");
            assert!(
                delta.added.iter().any(|row| row.id == durable_key),
                "the final payload is the durable occurrence: {delta:?}"
            );
            assert!(
                !results.iter().any(|result| {
                    result.get("joined.title") == Some(&Value::Text("transient".to_owned()))
                }),
                "the add-then-remove occurrence was netted out: {delta:?}"
            );

            client.shutdown().await.expect("shutdown test client");
            server.shutdown().await;
        })
        .await;
}
