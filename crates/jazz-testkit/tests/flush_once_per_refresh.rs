use std::time::{Duration, Instant};

use jazz::query::{col, eq, lit};
use jazz::row_input;
use jazz::tools::{
    ColumnType, DurabilityTier, JazzClient, Schema, SchemaBuilder, SubscriptionStream, TableSchema,
    Value,
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

struct RouteRefreshFixture {
    client: JazzClient,
    _subscriptions: Vec<SubscriptionStream>,
}

impl RouteRefreshFixture {
    async fn new(route_count: usize) -> Self {
        let client = JazzClient::test_client(route_schema()).await;
        let mut subscriptions: Vec<SubscriptionStream> = Vec::with_capacity(route_count);
        for route in 0..route_count {
            subscriptions.push(
                client
                    .subscribe(
                        jazz::query::Query::from("items")
                            .filter(eq(col("route"), lit(route as i32))),
                    )
                    .await
                    .unwrap_or_else(|error| panic!("subscribe route {route}: {error}")),
            );
        }

        assert_eq!(subscriptions.len(), route_count);
        Self {
            client,
            _subscriptions: subscriptions,
        }
    }

    async fn measure_unrelated_route_refresh(&self) -> Duration {
        let start = Instant::now();
        let (_, _, batch) = self
            .client
            .insert(
                "items",
                row_input!(
                    "route" => Value::Integer(1_000_000),
                    "label" => "changed",
                ),
            )
            .expect("write unrelated row");
        self.client
            .wait_for_transaction(
                batch.expect("ordinary mutation commits immediately"),
                DurabilityTier::Local,
            )
            .await
            .expect("settle unrelated row");
        start.elapsed()
    }
}

#[tokio::test(flavor = "current_thread")]
async fn unrelated_subscription_refresh_is_linear_in_routes_not_flushes() {
    tokio::task::LocalSet::new()
        .run_until(async {
            const PAIRS: usize = 5;
            let small_fixture = RouteRefreshFixture::new(100).await;
            let large_fixture = RouteRefreshFixture::new(1_000).await;

            // Run each already-prepared topology back-to-back, alternating the
            // order. This prevents a one-off scheduler pause or cache effect
            // from making the 100-route control implausibly cheap. Taking the
            // median preserves the original 15x ceiling while discarding the
            // two noisiest paired observations.
            let mut samples = Vec::with_capacity(PAIRS);
            for pair in 0..PAIRS {
                if pair % 2 == 0 {
                    let small = small_fixture.measure_unrelated_route_refresh().await;
                    let large = large_fixture.measure_unrelated_route_refresh().await;
                    samples.push((small, large));
                } else {
                    let large = large_fixture.measure_unrelated_route_refresh().await;
                    let small = small_fixture.measure_unrelated_route_refresh().await;
                    samples.push((small, large));
                }
            }
            let mut ratios = samples.iter().map(|(small, large)| {
                large.as_secs_f64() / small.as_secs_f64().max(0.000_001)
            }).collect::<Vec<_>>();
            ratios.sort_by(f64::total_cmp);
            let route_ratio = ratios[PAIRS / 2];

            eprintln!(
                "flush-once refresh pairs={samples:?} sorted_ratios={ratios:?} median_ratio={route_ratio:.2}"
            );
            assert!(
                route_ratio <= 15.0,
                "one-row refresh grew superlinearly with unrelated subscriptions: \
                 pairs={samples:?}, sorted_ratios={ratios:?}, median_ratio={route_ratio:.2}"
            );
        })
        .await;
}
