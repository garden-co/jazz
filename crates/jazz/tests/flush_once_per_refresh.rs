#![cfg(feature = "test")]

use std::time::{Duration, Instant};

use jazz::row_input;
use jazz::tools::{
    ColumnType, DurabilityTier, JazzClient, QueryBuilder, Schema, SchemaBuilder,
    SubscriptionStream, TableSchema, Value,
};

fn route_schema() -> Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("items")
                .column("route", ColumnType::Integer)
                .column("label", ColumnType::Text),
        )
        .build()
}

async fn measure_unrelated_route_refresh(route_count: usize) -> Duration {
    let client = JazzClient::test_client(route_schema()).await;
    let mut subscriptions: Vec<SubscriptionStream> = Vec::with_capacity(route_count);
    for route in 0..route_count {
        subscriptions.push(
            client
                .subscribe(
                    QueryBuilder::new("items")
                        .filter_eq("route", Value::Integer(route as i32))
                        .build(),
                )
                .await
                .unwrap_or_else(|error| panic!("subscribe route {route}: {error}")),
        );
    }

    let start = Instant::now();
    let (_, _, batch) = client
        .insert(
            "items",
            row_input!(
                "route" => Value::Integer(1_000_000),
                "label" => "changed",
            ),
        )
        .expect("write unrelated row");
    client
        .wait_for_batch(batch, DurabilityTier::Local)
        .await
        .expect("settle unrelated row");
    let elapsed = start.elapsed();

    assert_eq!(subscriptions.len(), route_count);
    elapsed
}

#[tokio::test(flavor = "current_thread")]
async fn unrelated_subscription_refresh_is_linear_in_routes_not_flushes() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let small = measure_unrelated_route_refresh(100).await;
            let large = measure_unrelated_route_refresh(1_000).await;
            let route_ratio = large.as_secs_f64() / small.as_secs_f64().max(0.000_001);

            eprintln!(
                "flush-once refresh small={small:?} large={large:?} route_ratio={route_ratio:.2}"
            );
            assert!(
                route_ratio <= 15.0,
                "one-row refresh grew superlinearly with unrelated subscriptions: \
                 small={small:?}, large={large:?}, route_ratio={route_ratio:.2}"
            );
        })
        .await;
}
