#![cfg(feature = "test")]

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use jazz::row_input;
use jazz::tools::server::JazzServer;
use jazz::tools::{
    ColumnType, DurabilityTier, JazzClient, QueryBuilder, ResultKey, Schema, SchemaBuilder,
    SubscriptionStreamItem, TableSchema,
};

fn hydration_schema() -> Schema {
    SchemaBuilder::new()
        .table(TableSchema::builder("items").column("label", ColumnType::Text))
        .build()
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "known red; tracked in TEST_BURNDOWN.md"]
async fn fresh_subscription_first_delivery_reduces_from_empty_to_initial_view() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let schema = hydration_schema();
            let server = JazzServer::start_with_schema(schema.clone()).await;
            let writer = JazzClient::connect(server.make_client_context_for_user(
                schema.clone(),
                "aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaa401",
            ))
            .await
            .expect("connect writer");
            let client = JazzClient::connect(
                server.make_client_context_for_user(schema, "aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaa402"),
            )
            .await
            .expect("connect subscriber");
            let (first_id, _, first_batch) = writer
                .insert("items", row_input!("label" => "first"))
                .expect("insert first initial item");
            let (second_id, _, second_batch) = writer
                .insert("items", row_input!("label" => "second"))
                .expect("insert second initial item");
            writer
                .wait_for_batch(
                    first_batch.expect("ordinary mutation commits immediately"),
                    DurabilityTier::EdgeServer,
                )
                .await
                .expect("first initial item settles");
            writer
                .wait_for_batch(
                    second_batch.expect("ordinary mutation commits immediately"),
                    DurabilityTier::EdgeServer,
                )
                .await
                .expect("second initial item settles");

            let query = QueryBuilder::new("items").build();
            let expected_ids = BTreeSet::from([first_id, second_id]);
            let rows = client
                .query(query.clone(), Some(DurabilityTier::EdgeServer))
                .await
                .expect("subscriber reaches the initial edge view");
            assert_eq!(
                rows.into_iter().map(|(id, _)| id).collect::<BTreeSet<_>>(),
                expected_ids,
                "subscriber must see the complete initial edge view before attaching"
            );
            let mut stream = client
                .subscribe(query)
                .await
                .expect("subscribe after initial rows exist");
            let item = tokio::time::timeout(Duration::from_secs(5), stream.next())
                .await
                .expect("initial subscription delivery arrives")
                .expect("subscription stream stays open");
            let delta = match item {
                SubscriptionStreamItem::Delta(delta) => delta,
                SubscriptionStreamItem::Rejected { reason } => {
                    panic!("initial subscription rejected: {reason:?}")
                }
            };

            // §16.1.1 requires a fresh subscription to reduce its first reset
            // delta from an empty result set to the complete initial view.
            assert!(
                delta.removed.is_empty(),
                "initial reset must not remove: {delta:?}"
            );
            assert!(
                delta.updated.is_empty(),
                "initial reset must not update: {delta:?}"
            );
            let mut reduced = BTreeMap::new();
            for added in delta.added {
                assert!(
                    reduced.insert(added.id, added.row.data).is_none(),
                    "initial reset must not add an occurrence twice"
                );
            }
            let expected = BTreeSet::from([ResultKey::from(first_id), ResultKey::from(second_id)]);
            assert_eq!(
                reduced.keys().cloned().collect::<BTreeSet<_>>(),
                expected,
                "reducing the first delivery from empty must yield the initial view"
            );
        })
        .await;
}
