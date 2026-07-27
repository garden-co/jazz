#![cfg(feature = "test-utils")]

mod support;

use std::time::{Duration, Instant};

use jazz::row_input;
use jazz::tools::public_schema::AggregateFunction;
use jazz::tools::server::JazzServer;
use jazz::tools::{
    ColumnMergeStrategy, ColumnType, DurabilityTier, JazzClient, PolicyExpr, QueryBuilder,
    RowDescriptor, Schema, SchemaBuilder, TableName, TablePolicies, TableSchema, Value,
};
use support::{TestingClient, wait_for_query};
use uuid::Uuid;

const QUERY_TIMEOUT: Duration = Duration::from_secs(25);

fn metrics_schema() -> Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("metrics")
                .column("bucket", ColumnType::Text)
                .column("score", ColumnType::Integer),
        )
        .build()
}

fn nullable_metrics_schema() -> Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("metrics")
                .column("bucket", ColumnType::Text)
                .nullable_column("score", ColumnType::Integer),
        )
        .build()
}

fn bigint_metrics_schema() -> Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("metrics")
                .column("bucket", ColumnType::Text)
                .column("score", ColumnType::BigInt),
        )
        .build()
}

fn counter_schema() -> Schema {
    let mut schema = SchemaBuilder::new()
        .table(
            TableSchema::builder("counters")
                .column("name", ColumnType::Text)
                .column("count", ColumnType::Integer),
        )
        .build();
    let table = schema
        .get_mut(&TableName::new("counters"))
        .expect("counters table exists");
    table.columns = RowDescriptor::new(
        table
            .columns
            .columns
            .iter()
            .map(|column| {
                if column.name.as_str() == "count" {
                    column.clone().merge_strategy(ColumnMergeStrategy::Counter)
                } else {
                    column.clone()
                }
            })
            .collect(),
    );
    schema
}

fn test_user_id(subject: &str) -> String {
    Uuid::new_v5(&Uuid::NAMESPACE_URL, subject.as_bytes()).to_string()
}

fn policy_metrics_schema() -> Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("metrics")
                .column("owner_id", ColumnType::Text)
                .column("score", ColumnType::Integer)
                .policies(
                    TablePolicies::new()
                        .with_insert(PolicyExpr::True)
                        .with_select(PolicyExpr::eq_session("owner_id", vec!["user_id".into()]))
                        .with_delete(PolicyExpr::True),
                ),
        )
        .build()
}

async fn wait_for_values(
    client: &JazzClient,
    query: jazz::tools::Query,
    expected: Vec<Vec<Value>>,
    label: &str,
) {
    let deadline = Instant::now() + Duration::from_secs(5);
    let last_actual;
    loop {
        let mut actual = client
            .query(query.clone(), None)
            .await
            .unwrap_or_else(|err| panic!("{label}: query failed: {err}"))
            .into_iter()
            .map(|(_, values)| values)
            .collect::<Vec<_>>();
        actual.sort_by(|left, right| format!("{left:?}").cmp(&format!("{right:?}")));
        if actual == expected {
            return;
        }
        if Instant::now() >= deadline {
            last_actual = actual;
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert_eq!(last_actual, expected, "{label}");
}

async fn wait_for_subscription_driven_values(
    client: &JazzClient,
    stream: &mut jazz_tools::SubscriptionStream,
    query: jazz_tools::Query,
    expected: Vec<Vec<Value>>,
    label: &str,
) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    let last_actual;
    loop {
        tokio::time::timeout_at(deadline, stream.next())
            .await
            .unwrap_or_else(|_| panic!("{label}: timed out waiting for subscription delta"))
            .unwrap_or_else(|| panic!("{label}: subscription ended"));
        let mut actual = client
            .query(query.clone(), None)
            .await
            .unwrap_or_else(|err| panic!("{label}: query after subscription event failed: {err}"))
            .into_iter()
            .map(|(_, values)| values)
            .collect::<Vec<_>>();
        actual.sort_by(|left, right| format!("{left:?}").cmp(&format!("{right:?}")));
        if actual == expected {
            return;
        }
        if tokio::time::Instant::now() >= deadline {
            last_actual = actual;
            break;
        }
    }
    assert_eq!(last_actual, expected, "{label}");
}

async fn insert_metric(client: &JazzClient, bucket: &str, score: i32) {
    let (_, _, batch) = client
        .insert("metrics", row_input!("bucket" => bucket, "score" => score))
        .expect("insert integer metric");
    client
        .wait_for_batch(batch, DurabilityTier::Local)
        .await
        .expect("integer metric settles");
}

async fn insert_metric_at_tier(
    client: &JazzClient,
    bucket: &str,
    score: i32,
    tier: DurabilityTier,
) {
    let (_, _, batch) = client
        .insert("metrics", row_input!("bucket" => bucket, "score" => score))
        .expect("insert integer metric");
    client
        .wait_for_batch(batch, tier)
        .await
        .expect("integer metric settles");
}

async fn insert_bigint_metric(client: &JazzClient, bucket: &str, score: i64) {
    let (_, _, batch) = client
        .insert(
            "metrics",
            row_input!("bucket" => bucket, "score" => Value::BigInt(score)),
        )
        .expect("insert bigint metric");
    client
        .wait_for_batch(batch, DurabilityTier::Local)
        .await
        .expect("bigint metric settles");
}

fn aggregate_query(
    outputs: impl IntoIterator<Item = (AggregateFunction, &'static str)>,
) -> jazz::tools::Query {
    let mut builder = QueryBuilder::new("metrics");
    for (function, column) in outputs {
        builder = match function {
            AggregateFunction::Count => builder.count(),
            AggregateFunction::Sum => builder.sum(column),
            AggregateFunction::Avg => builder.avg(column),
            AggregateFunction::Min => builder.min(column),
            AggregateFunction::Max => builder.max(column),
        };
    }
    builder.group_by("bucket").build()
}

#[tokio::test(flavor = "current_thread")]
async fn aggregate_subscription_count_and_grouped_sum_track_full_state() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let schema = metrics_schema();
            let server = JazzServer::start_with_schema(schema.clone()).await;
            let client = JazzClient::connect(
                server.make_client_context_for_user(schema, "aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaaaa1"),
            )
            .await
            .expect("connect client");
            let count_query = QueryBuilder::new("metrics").count().build();
            let grouped_sum_query = QueryBuilder::new("metrics")
                .sum("score")
                .group_by("bucket")
                .build();
            let _count_stream = client
                .subscribe(count_query.clone())
                .await
                .expect("subscribe count aggregate");
            let _sum_stream = client
                .subscribe(grouped_sum_query.clone())
                .await
                .expect("subscribe grouped sum aggregate");

            wait_for_values(
                &client,
                count_query.clone(),
                vec![vec![Value::Timestamp(0)]],
                "initial empty count",
            )
            .await;

            let (a1, _, batch) = client
                .insert("metrics", row_input!("bucket" => "a", "score" => 10))
                .expect("insert a1");
            client
                .wait_for_batch(batch, DurabilityTier::Local)
                .await
                .expect("a1 settles");
            wait_for_values(
                &client,
                count_query.clone(),
                vec![vec![Value::Timestamp(1)]],
                "count after a1",
            )
            .await;
            wait_for_values(
                &client,
                grouped_sum_query.clone(),
                vec![vec![Value::Text("a".to_owned()), Value::Integer(10)]],
                "sum after a1",
            )
            .await;

            let (b1, _, batch) = client
                .insert("metrics", row_input!("bucket" => "b", "score" => 7))
                .expect("insert b1");
            client
                .wait_for_batch(batch, DurabilityTier::Local)
                .await
                .expect("b1 settles");
            wait_for_values(
                &client,
                count_query.clone(),
                vec![vec![Value::Timestamp(2)]],
                "count after b1",
            )
            .await;
            wait_for_values(
                &client,
                grouped_sum_query.clone(),
                vec![
                    vec![Value::Text("a".to_owned()), Value::Integer(10)],
                    vec![Value::Text("b".to_owned()), Value::Integer(7)],
                ],
                "sum after b1",
            )
            .await;

            let batch = client.delete(b1).expect("delete b1 and empty b");
            client
                .wait_for_batch(batch, DurabilityTier::Local)
                .await
                .expect("delete b1 settles");
            let (_b2, _, batch) = client
                .insert("metrics", row_input!("bucket" => "b", "score" => 5))
                .expect("repopulate b");
            client
                .wait_for_batch(batch, DurabilityTier::Local)
                .await
                .expect("repopulate b settles");
            wait_for_values(
                &client,
                grouped_sum_query.clone(),
                vec![
                    vec![Value::Text("a".to_owned()), Value::Integer(10)],
                    vec![Value::Text("b".to_owned()), Value::Integer(5)],
                ],
                "sum after repopulating b",
            )
            .await;

            let batch = client.delete(a1).expect("delete a1");
            client
                .wait_for_batch(batch, DurabilityTier::Local)
                .await
                .expect("delete settles");
            wait_for_values(
                &client,
                count_query.clone(),
                vec![vec![Value::Timestamp(1)]],
                "count after delete a1",
            )
            .await;
            wait_for_values(
                &client,
                grouped_sum_query.clone(),
                vec![vec![Value::Text("b".to_owned()), Value::Integer(5)]],
                "sum after delete a1",
            )
            .await;
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn aggregate_sum_public_boundary_preserves_nullable_results() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let schema = nullable_metrics_schema();
            let server = JazzServer::start_with_schema(schema.clone()).await;
            let client = JazzClient::connect(
                server.make_client_context_for_user(schema, "aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaaaa3"),
            )
            .await
            .expect("connect client");
            let sum_query = QueryBuilder::new("metrics").sum("score").build();
            let mut stream = client
                .subscribe(sum_query.clone())
                .await
                .expect("subscribe sum aggregate");

            wait_for_values(
                &client,
                sum_query.clone(),
                vec![vec![Value::Null]],
                "one-shot empty sum is public null",
            )
            .await;
            wait_for_subscription_driven_values(
                &client,
                &mut stream,
                sum_query.clone(),
                vec![vec![Value::Null]],
                "subscription empty sum is public null",
            )
            .await;

            let (_null_row, _, batch) = client
                .insert(
                    "metrics",
                    row_input!("bucket" => "a", "score" => Value::Null),
                )
                .expect("insert null score");
            client
                .wait_for_batch(batch, DurabilityTier::Local)
                .await
                .expect("null score settles");
            wait_for_values(
                &client,
                sum_query.clone(),
                vec![vec![Value::Null]],
                "one-shot all-null sum is public null",
            )
            .await;
            wait_for_subscription_driven_values(
                &client,
                &mut stream,
                sum_query,
                vec![vec![Value::Null]],
                "subscription all-null sum is public null",
            )
            .await;

            // The mixed null/non-null case is not covered here: writing a
            // non-null value into a nullable column through the public client
            // currently fails because the public write path does not wrap the
            // value using the schema's nullable type. That is independent of
            // aggregate semantics; empty and all-NULL cases are the boundary
            // this test owns.
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn maintained_integer_sum_accumulates_multiple_deltas_and_retracts_empty_group() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let schema = metrics_schema();
            let server = JazzServer::start_with_schema(schema.clone()).await;
            let client = JazzClient::connect(
                server.make_client_context_for_user(schema, "aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaaaa6"),
            )
            .await
            .expect("connect client");
            let grouped_sum_query = QueryBuilder::new("metrics")
                .sum("score")
                .group_by("bucket")
                .build();
            let _sum_stream = client
                .subscribe(grouped_sum_query.clone())
                .await
                .expect("subscribe grouped sum aggregate");

            let (first, _, batch) = client
                .insert("metrics", row_input!("bucket" => "same", "score" => 10))
                .expect("insert first metric");
            client
                .wait_for_batch(batch, DurabilityTier::Local)
                .await
                .expect("first metric settles");
            wait_for_values(
                &client,
                grouped_sum_query.clone(),
                vec![vec![Value::Text("same".to_owned()), Value::Integer(10)]],
                "sum after first same-group delta",
            )
            .await;

            let (second, _, batch) = client
                .insert("metrics", row_input!("bucket" => "same", "score" => 7))
                .expect("insert second metric");
            client
                .wait_for_batch(batch, DurabilityTier::Local)
                .await
                .expect("second metric settles");
            wait_for_values(
                &client,
                grouped_sum_query.clone(),
                vec![vec![Value::Text("same".to_owned()), Value::Integer(17)]],
                "sum accumulates a second same-group delta",
            )
            .await;

            let batch = client.delete(first).expect("delete first metric");
            client
                .wait_for_batch(batch, DurabilityTier::Local)
                .await
                .expect("first delete settles");
            wait_for_values(
                &client,
                grouped_sum_query.clone(),
                vec![vec![Value::Text("same".to_owned()), Value::Integer(7)]],
                "sum subtracts a signed deletion delta",
            )
            .await;

            let batch = client.delete(second).expect("delete second metric");
            client
                .wait_for_batch(batch, DurabilityTier::Local)
                .await
                .expect("second delete settles");
            wait_for_values(
                &client,
                grouped_sum_query,
                Vec::new(),
                "empty signed aggregate group is retracted",
            )
            .await;
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn integer_sum_uses_public_signed_values_for_multi_row_groups() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let schema = metrics_schema();
            let server = JazzServer::start_with_schema(schema.clone()).await;
            let client = JazzClient::connect(
                server.make_client_context_for_user(schema, "aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaaaa2"),
            )
            .await
            .expect("connect client");

            insert_metric_at_tier(&client, "positive", 10, DurabilityTier::EdgeServer).await;
            insert_metric_at_tier(&client, "positive", 7, DurabilityTier::EdgeServer).await;
            insert_metric_at_tier(&client, "negative", -4, DurabilityTier::EdgeServer).await;
            insert_metric_at_tier(&client, "negative", -6, DurabilityTier::EdgeServer).await;
            insert_metric_at_tier(&client, "mixed", -5, DurabilityTier::EdgeServer).await;
            insert_metric_at_tier(&client, "mixed", 8, DurabilityTier::EdgeServer).await;

            wait_for_values(
                &client,
                QueryBuilder::new("metrics")
                    .sum("score")
                    .group_by("bucket")
                    .build(),
                vec![
                    vec![Value::Text("mixed".to_owned()), Value::Integer(3)],
                    vec![Value::Text("negative".to_owned()), Value::Integer(-10)],
                    vec![Value::Text("positive".to_owned()), Value::Integer(17)],
                ],
                "integer grouped sum uses public signed values",
            )
            .await;
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn integer_avg_uses_public_signed_values_for_multi_row_groups() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let schema = metrics_schema();
            let server = JazzServer::start_with_schema(schema.clone()).await;
            let client = JazzClient::connect(
                server.make_client_context_for_user(schema, "aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaaaa3"),
            )
            .await
            .expect("connect client");

            insert_metric(&client, "positive", 10).await;
            insert_metric(&client, "positive", 7).await;
            insert_metric(&client, "negative", -4).await;
            insert_metric(&client, "negative", -6).await;
            insert_metric(&client, "mixed", -5).await;
            insert_metric(&client, "mixed", 8).await;

            wait_for_values(
                &client,
                aggregate_query([(AggregateFunction::Avg, "score")]),
                vec![
                    vec![Value::Text("mixed".to_owned()), Value::Double(1.5)],
                    vec![Value::Text("negative".to_owned()), Value::Double(-5.0)],
                    vec![Value::Text("positive".to_owned()), Value::Double(8.5)],
                ],
                "integer grouped avg uses public signed values",
            )
            .await;
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn integer_min_max_and_order_by_remain_signed() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let schema = metrics_schema();
            let server = JazzServer::start_with_schema(schema.clone()).await;
            let client = TestingClient::builder()
                .with_server(&server)
                .with_schema(schema)
                .with_user_id("aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaaaa4")
                .ready_on("metrics", QUERY_TIMEOUT)
                .connect()
                .await;

            insert_metric(&client, "positive", 10).await;
            insert_metric(&client, "positive", 7).await;
            insert_metric(&client, "negative", -4).await;
            insert_metric(&client, "negative", -6).await;
            insert_metric(&client, "mixed", -5).await;
            insert_metric(&client, "mixed", 8).await;

            wait_for_values(
                &client,
                aggregate_query([
                    (AggregateFunction::Min, "score"),
                    (AggregateFunction::Max, "score"),
                ]),
                vec![
                    vec![
                        Value::Text("mixed".to_owned()),
                        Value::Integer(-5),
                        Value::Integer(8),
                    ],
                    vec![
                        Value::Text("negative".to_owned()),
                        Value::Integer(-6),
                        Value::Integer(-4),
                    ],
                    vec![
                        Value::Text("positive".to_owned()),
                        Value::Integer(7),
                        Value::Integer(10),
                    ],
                ],
                "integer min/max stay signed",
            )
            .await;

            wait_for_query(
                &client,
                QueryBuilder::new("metrics")
                    .select(&["bucket", "score"])
                    .order_by("score")
                    .build(),
                Some(DurabilityTier::EdgeServer),
                QUERY_TIMEOUT,
                "integer order_by stays signed",
                |rows| {
                    let values = rows
                        .iter()
                        .map(|(_, values)| values.clone())
                        .collect::<Vec<_>>();
                    (values
                        == vec![
                            vec![Value::Text("negative".to_owned()), Value::Integer(-6)],
                            vec![Value::Text("mixed".to_owned()), Value::Integer(-5)],
                            vec![Value::Text("negative".to_owned()), Value::Integer(-4)],
                            vec![Value::Text("positive".to_owned()), Value::Integer(7)],
                            vec![Value::Text("mixed".to_owned()), Value::Integer(8)],
                            vec![Value::Text("positive".to_owned()), Value::Integer(10)],
                        ])
                    .then_some(())
                },
            )
            .await;
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn bigint_aggregates_keep_signed_value_semantics() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let schema = bigint_metrics_schema();
            let server = JazzServer::start_with_schema(schema.clone()).await;
            let client = JazzClient::connect(
                server.make_client_context_for_user(schema, "aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaaaa5"),
            )
            .await
            .expect("connect client");

            insert_bigint_metric(&client, "positive", 10).await;
            insert_bigint_metric(&client, "positive", 7).await;
            insert_bigint_metric(&client, "negative", -4).await;
            insert_bigint_metric(&client, "negative", -6).await;
            insert_bigint_metric(&client, "mixed", -5).await;
            insert_bigint_metric(&client, "mixed", 8).await;

            wait_for_values(
                &client,
                aggregate_query([
                    (AggregateFunction::Sum, "score"),
                    (AggregateFunction::Avg, "score"),
                    (AggregateFunction::Min, "score"),
                    (AggregateFunction::Max, "score"),
                ]),
                vec![
                    vec![
                        Value::Text("mixed".to_owned()),
                        Value::BigInt(3),
                        Value::Double(1.5),
                        Value::BigInt(-5),
                        Value::BigInt(8),
                    ],
                    vec![
                        Value::Text("negative".to_owned()),
                        Value::BigInt(-10),
                        Value::Double(-5.0),
                        Value::BigInt(-6),
                        Value::BigInt(-4),
                    ],
                    vec![
                        Value::Text("positive".to_owned()),
                        Value::BigInt(17),
                        Value::Double(8.5),
                        Value::BigInt(7),
                        Value::BigInt(10),
                    ],
                ],
                "bigint aggregates keep signed semantics",
            )
            .await;
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn aggregate_sum_bigint_survives_public_client_boundary() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let schema = bigint_metrics_schema();
            let server = JazzServer::start_with_schema(schema.clone()).await;
            let client = JazzClient::connect(
                server.make_client_context_for_user(schema, "aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaaaa4"),
            )
            .await
            .expect("connect client");
            let sum_query = QueryBuilder::new("metrics").sum("score").build();

            wait_for_values(
                &client,
                sum_query.clone(),
                vec![vec![Value::Null]],
                "empty bigint sum is public null",
            )
            .await;

            let (_negative_row, _, batch) = client
                .insert(
                    "metrics",
                    row_input!("bucket" => "a", "score" => Value::BigInt(-3)),
                )
                .expect("insert negative bigint score");
            client
                .wait_for_batch(batch, DurabilityTier::Local)
                .await
                .expect("negative bigint score settles");
            let (_positive_row, _, batch) = client
                .insert(
                    "metrics",
                    row_input!("bucket" => "a", "score" => Value::BigInt(5)),
                )
                .expect("insert positive bigint score");
            client
                .wait_for_batch(batch, DurabilityTier::Local)
                .await
                .expect("positive bigint score settles");

            wait_for_values(
                &client,
                sum_query,
                vec![vec![Value::BigInt(2)]],
                "bigint sum decodes exact signed public value",
            )
            .await;
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn integer_counter_columns_merge_signed_public_values() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let schema = counter_schema();
            let server = JazzServer::start_with_schema(schema.clone()).await;
            let alice = JazzClient::connect(server.make_client_context_for_user(
                schema.clone(),
                "aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaaaa7",
            ))
            .await
            .expect("connect alice");
            let bob = JazzClient::connect(
                server.make_client_context_for_user(schema, "aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaaaa8"),
            )
            .await
            .expect("connect bob");
            let query = QueryBuilder::new("counters")
                .select(&["name", "count"])
                .build();

            let (counter_id, _, batch) = alice
                .insert("counters", row_input!("name" => "shared", "count" => 0))
                .expect("insert counter");
            alice
                .wait_for_batch(batch, DurabilityTier::EdgeServer)
                .await
                .expect("counter insert settles at edge");
            wait_for_query(
                &bob,
                query.clone(),
                Some(DurabilityTier::EdgeServer),
                QUERY_TIMEOUT,
                "bob sees counter base",
                |rows| {
                    rows.iter()
                        .any(|(id, values)| {
                            *id == counter_id
                                && values
                                    == &vec![Value::Text("shared".to_owned()), Value::Integer(0)]
                        })
                        .then_some(())
                },
            )
            .await;

            let alice_batch = alice
                .update(counter_id, vec![("count".to_owned(), Value::Integer(3))])
                .expect("alice updates counter");
            let bob_batch = bob
                .update(counter_id, vec![("count".to_owned(), Value::Integer(5))])
                .expect("bob updates counter");
            alice
                .wait_for_batch(alice_batch, DurabilityTier::EdgeServer)
                .await
                .expect("alice counter update reaches edge");
            bob.wait_for_batch(bob_batch, DurabilityTier::EdgeServer)
                .await
                .expect("bob counter update reaches edge");

            wait_for_query(
                &alice,
                query,
                Some(DurabilityTier::EdgeServer),
                QUERY_TIMEOUT,
                "signed integer counter deltas merge",
                |rows| {
                    rows.iter()
                        .any(|(id, values)| {
                            *id == counter_id
                                && values
                                    == &vec![Value::Text("shared".to_owned()), Value::Integer(8)]
                        })
                        .then_some(())
                },
            )
            .await;
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn aggregate_subscription_spy_stays_at_policy_visible_truth() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let schema = policy_metrics_schema();
            let server = JazzServer::start_with_schema(schema.clone()).await;
            let admin_id = test_user_id("aggregate-admin");
            let spy_id = test_user_id("aggregate-spy");
            let admin = JazzClient::connect(
                server.make_client_context_for_user(schema.clone(), admin_id.clone()),
            )
            .await
            .expect("connect admin");
            let spy = JazzClient::connect(
                server.make_client_context_for_user(schema.clone(), spy_id.clone()),
            )
            .await
            .expect("connect spy");
            let count_query = QueryBuilder::new("metrics").count().build();
            let _spy_stream = spy
                .subscribe(count_query.clone())
                .await
                .expect("subscribe spy aggregate");

            wait_for_values(
                &spy,
                count_query.clone(),
                vec![vec![Value::Timestamp(0)]],
                "spy initial count",
            )
            .await;

            let (admin_row, _, batch) = admin
                .insert(
                    "metrics",
                    row_input!("owner_id" => admin_id.clone(), "score" => 10),
                )
                .expect("insert admin row");
            admin
                .wait_for_batch(batch, DurabilityTier::Local)
                .await
                .expect("admin row settles");
            wait_for_values(
                &spy,
                count_query.clone(),
                vec![vec![Value::Timestamp(0)]],
                "spy count ignores admin row",
            )
            .await;

            let batch = admin.delete(admin_row).expect("delete admin row");
            admin
                .wait_for_batch(batch, DurabilityTier::Local)
                .await
                .expect("admin delete settles");
            wait_for_values(
                &spy,
                count_query,
                vec![vec![Value::Timestamp(0)]],
                "spy count remains zero after invisible delete",
            )
            .await;
        })
        .await;
}
