#![cfg(feature = "test")]

mod support;

use std::collections::BTreeMap;
use std::time::Duration;

use jazz::row_input;
use jazz::tools::server::JazzServer;
use jazz::tools::{
    ColumnType, DurabilityTier, ObjectId, OutputOccurrenceId, QueryBuilder, Schema, SchemaBuilder,
    SubscriptionStreamItem, TableSchema, Value,
};
use support::TestingClient;
use uuid::Uuid;

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
            let (root, _, batch) = client
                .insert(
                    "todos",
                    row_input!("title" => "draft", "bucket" => "shared", "done" => false),
                )
                .expect("insert todo");
            client
                .wait_for_batch(batch, DurabilityTier::EdgeServer)
                .await
                .expect("todo settles locally");

            let first_join = ObjectId::from_uuid(Uuid::from_bytes([0x11; 16]));
            let second_join = ObjectId::from_uuid(Uuid::from_bytes([0x22; 16]));
            let ordered = OutputOccurrenceId::new(root, [first_join, second_join]);
            let reversed = OutputOccurrenceId::new(root, [second_join, first_join]);
            assert_ne!(
                ordered, reversed,
                "declared join order is part of the identity"
            );
            assert_eq!(
                ordered.contributing_rows().collect::<Vec<_>>(),
                vec![root, first_join, second_join]
            );
            assert_ne!(ordered.canonical_bytes(), reversed.canonical_bytes());
            let by_occurrence =
                BTreeMap::from([(ordered.clone(), "ordered"), (reversed, "reversed")]);
            assert_eq!(by_occurrence.get(&ordered), Some(&"ordered"));

            let joined_query = QueryBuilder::new("todos")
                .alias("root")
                .filter_eq("done", Value::Boolean(false))
                .join("todos")
                .alias("joined")
                .on("root.bucket", "joined.bucket")
                .build();
            let mut joined_stream = client
                .subscribe(joined_query.clone())
                .await
                .expect("joined maintained output is supported");
            let joined_reset = next_delta(&mut joined_stream).await;
            assert!(
                joined_reset
                    .added
                    .iter()
                    .any(|row| row.id == OutputOccurrenceId::new(root, [root])),
                "the root is the only initial join contributor"
            );

            let (first, _, batch) = client
                .insert(
                    "todos",
                    row_input!("title" => "first", "bucket" => "shared", "done" => true),
                )
                .expect("insert first matching joined row");
            client
                .wait_for_batch(batch, DurabilityTier::Local)
                .await
                .expect("first joined row settles locally");
            let first_added = next_delta_with_added(&mut joined_stream).await;
            assert!(
                first_added
                    .added
                    .iter()
                    .any(|row| row.id == OutputOccurrenceId::new(root, [first])),
                "the first joined row is addressed beneath its root: {first_added:?}"
            );

            let (second, _, batch) = client
                .insert(
                    "todos",
                    row_input!("title" => "second", "bucket" => "shared", "done" => true),
                )
                .expect("insert second matching join row");
            client
                .wait_for_batch(batch, DurabilityTier::EdgeServer)
                .await
                .expect("second todo settles");
            let fan_out = next_delta_with_added(&mut joined_stream).await;
            assert!(
                fan_out
                    .added
                    .iter()
                    .any(|row| row.id == OutputOccurrenceId::new(root, [second])),
                "a second matching source produces a distinct occurrence under the same root: {fan_out:?}"
            );

            let batch = client
                .update(
                    root,
                    vec![("title".to_owned(), Value::Text("revised".to_owned()))],
                )
                .expect("replace root source content");
            client
                .wait_for_batch(batch, DurabilityTier::EdgeServer)
                .await
                .expect("replacement settles");
            let replacement = next_delta_with_updated(&mut joined_stream).await;
            assert!(
                replacement
                    .updated
                    .iter()
                    .any(|row| row.id == OutputOccurrenceId::new(root, [second])),
                "a root-source replacement is addressed by its composite occurrence id"
            );

            let batch = client.delete(first).expect("remove first joined row");
            client
                .wait_for_batch(batch, DurabilityTier::EdgeServer)
                .await
                .expect("joined-row removal settles");
            let removal = next_delta_with_removed(&mut joined_stream).await;
            assert!(
                removal
                    .removed
                    .iter()
                    .any(|row| row.id == OutputOccurrenceId::new(root, [first])),
                "a joined-source removal is addressed by its composite occurrence id"
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
                    QueryBuilder::new("todos")
                        .alias("root")
                        .filter_eq("done", Value::Boolean(false))
                        .join("todos")
                        .alias("joined")
                        .on("root.bucket", "joined.bucket")
                        .build(),
                )
                .await
                .expect("rehydrated joined maintained output is supported");
            let rehydrated_reset = next_delta_with_added(&mut rehydrated_stream).await;
            assert!(
                rehydrated_reset
                    .added
                    .iter()
                    .any(|row| row.id == OutputOccurrenceId::new(root, [second])),
                "reset/rehydrate preserves the remaining composite occurrence id: {rehydrated_reset:?}"
            );

            rehydrated.shutdown().await.expect("shutdown rehydrated client");
            client.shutdown().await.expect("shutdown test client");
            server.shutdown().await;
        })
        .await;
}
