#![cfg(feature = "test")]

mod support;

use std::time::Duration;

use jazz_tools::row_input;
use jazz_tools::server::JazzServer;
use jazz_tools::{
    ColumnType, DurabilityTier, QueryBuilder, Schema, SchemaBuilder, TableSchema, Value,
};
use support::{
    TestingClient, has_added, wait_for_edge_query_ready, wait_for_query,
    wait_for_subscription_update,
};

const QUERY_TIMEOUT: Duration = Duration::from_secs(25);
const BIG_SAFE_PLUS_ONE: i64 = 9_007_199_254_740_993;

fn bigint_schema() -> Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("metrics")
                .column("label", ColumnType::Text)
                .column("amount", ColumnType::BigInt),
        )
        .build()
}

#[tokio::test(flavor = "current_thread")]
async fn bigint_insert_query_order_predicate_and_subscribe_are_lossless() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let schema = bigint_schema();
            let server = JazzServer::start_with_schema(schema.clone()).await;
            let client = TestingClient::builder()
                .with_server(&server)
                .with_schema(schema)
                .with_user_id("00000000-0000-4000-8000-000000000064")
                .ready_on("metrics", QUERY_TIMEOUT)
                .connect()
                .await;

            let rows = [
                ("negative", -BIG_SAFE_PLUS_ONE),
                ("small", 42),
                ("huge", BIG_SAFE_PLUS_ONE),
            ];
            for (label, amount) in rows {
                let (_, _, batch_id) = client
                    .insert("metrics", row_input!("label" => label, "amount" => amount))
                    .expect("insert bigint row");
                client
                    .wait_for_batch(batch_id, DurabilityTier::EdgeServer)
                    .await
                    .expect("bigint row settles");
            }

            let ordered_query = QueryBuilder::new("metrics")
                .select(&["label", "amount"])
                .order_by("amount")
                .build();
            wait_for_query(
                &client,
                ordered_query,
                Some(DurabilityTier::EdgeServer),
                QUERY_TIMEOUT,
                "BIGINT rows order by signed amount",
                |rows| {
                    let values = rows
                        .iter()
                        .map(|(_, values)| values.clone())
                        .collect::<Vec<_>>();
                    (values
                        == vec![
                            vec![
                                Value::Text("negative".to_owned()),
                                Value::BigInt(-BIG_SAFE_PLUS_ONE),
                            ],
                            vec![Value::Text("small".to_owned()), Value::BigInt(42)],
                            vec![
                                Value::Text("huge".to_owned()),
                                Value::BigInt(BIG_SAFE_PLUS_ONE),
                            ],
                        ])
                    .then_some(())
                },
            )
            .await;

            let filtered_query = QueryBuilder::new("metrics")
                .select(&["label", "amount"])
                .filter_gt("amount", Value::Integer(9))
                .filter_lt("amount", Value::BigInt(BIG_SAFE_PLUS_ONE + 1))
                .order_by("amount")
                .build();
            wait_for_query(
                &client,
                filtered_query,
                Some(DurabilityTier::EdgeServer),
                QUERY_TIMEOUT,
                "BIGINT predicates coerce integer literals and preserve i64 literals",
                |rows| {
                    let values = rows
                        .iter()
                        .map(|(_, values)| values.clone())
                        .collect::<Vec<_>>();
                    (values
                        == vec![
                            vec![Value::Text("small".to_owned()), Value::BigInt(42)],
                            vec![
                                Value::Text("huge".to_owned()),
                                Value::BigInt(BIG_SAFE_PLUS_ONE),
                            ],
                        ])
                    .then_some(())
                },
            )
            .await;

            let subscription_query = QueryBuilder::new("metrics")
                .filter_gt("amount", Value::BigInt(BIG_SAFE_PLUS_ONE))
                .build();
            let mut stream = client
                .subscribe(subscription_query)
                .await
                .expect("subscribe to BIGINT predicate");
            let mut log = Vec::new();

            let (late_id, _, batch_id) = client
                .insert(
                    "metrics",
                    row_input!("label" => "later", "amount" => BIG_SAFE_PLUS_ONE + 1),
                )
                .expect("insert subscribed bigint row");
            client
                .wait_for_batch(batch_id, DurabilityTier::EdgeServer)
                .await
                .expect("subscribed bigint row settles");

            wait_for_subscription_update(
                &mut stream,
                &mut log,
                QUERY_TIMEOUT,
                "BIGINT subscription predicate receives inserted row",
                |log| has_added(log, late_id),
            )
            .await;

            wait_for_edge_query_ready(&client, "metrics", QUERY_TIMEOUT).await;
            server.shutdown().await;
        })
        .await;
}
