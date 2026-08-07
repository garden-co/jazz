#![cfg(feature = "test")]

use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use jazz::row_input;
use jazz::tools::{
    ColumnType, DurabilityTier, JazzClient, OutputOccurrenceId, QueryBuilder, Schema,
    SchemaBuilder, SubscriptionStreamItem, TableSchema,
};

fn hydration_schema() -> Schema {
    SchemaBuilder::new()
        .table(TableSchema::builder("items").column("label", ColumnType::Text))
        .build()
}

#[tokio::test(flavor = "current_thread")]
async fn fresh_subscription_first_delivery_reduces_from_empty_to_initial_view() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let client = JazzClient::test_client(hydration_schema()).await;
            let (first_id, _, first_batch) = client
                .insert("items", row_input!("label" => "first"))
                .expect("insert first initial item");
            let (second_id, _, second_batch) = client
                .insert("items", row_input!("label" => "second"))
                .expect("insert second initial item");
            client
                .wait_for_batch(first_batch, DurabilityTier::Local)
                .await
                .expect("first initial item settles");
            client
                .wait_for_batch(second_batch, DurabilityTier::Local)
                .await
                .expect("second initial item settles");

            let mut stream = client
                .subscribe(QueryBuilder::new("items").build())
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
            let expected = BTreeSet::from([
                OutputOccurrenceId::from(first_id),
                OutputOccurrenceId::from(second_id),
            ]);
            assert_eq!(
                reduced.keys().cloned().collect::<BTreeSet<_>>(),
                expected,
                "reducing the first delivery from empty must yield the initial view"
            );
        })
        .await;
}
