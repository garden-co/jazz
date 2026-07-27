#![cfg(feature = "test-utils")]

use std::time::{Duration, Instant};

use jazz_tools::server::JazzServer;
use jazz_tools::{
    ColumnType, DurabilityTier, JazzClient, PolicyExpr, QueryBuilder, Schema, SchemaBuilder,
    TablePolicies, TableSchema, Value, row_input,
};
use uuid::Uuid;
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

#[derive(Clone, Copy)]
enum AggregateCase {
    Avg,
    Min,
    Max,
}

impl AggregateCase {
    fn name(self) -> &'static str {
        match self {
            Self::Avg => "avg",
            Self::Min => "min",
            Self::Max => "max",
        }
    }

    fn query(self) -> jazz_tools::Query {
        match self {
            Self::Avg => QueryBuilder::new("metrics").avg("score").build(),
            Self::Min => QueryBuilder::new("metrics").min("score").build(),
            Self::Max => QueryBuilder::new("metrics").max("score").build(),
        }
    }

    fn grouped_query(self) -> jazz_tools::Query {
        match self {
            Self::Avg => QueryBuilder::new("metrics")
                .avg("score")
                .group_by("bucket")
                .build(),
            Self::Min => QueryBuilder::new("metrics")
                .min("score")
                .group_by("bucket")
                .build(),
            Self::Max => QueryBuilder::new("metrics")
                .max("score")
                .group_by("bucket")
                .build(),
        }
    }

    fn populated_value(self) -> Value {
        match self {
            Self::Avg => Value::Double(15.0),
            Self::Min => Value::Integer(5),
            Self::Max => Value::Integer(30),
        }
    }

    fn grouped_values(self) -> Vec<Vec<Value>> {
        match self {
            Self::Avg => vec![
                vec![Value::Text("a".to_owned()), Value::Double(20.0)],
                vec![Value::Text("b".to_owned()), Value::Double(5.0)],
            ],
            Self::Min => vec![
                vec![Value::Text("a".to_owned()), Value::Integer(10)],
                vec![Value::Text("b".to_owned()), Value::Integer(5)],
            ],
            Self::Max => vec![
                vec![Value::Text("a".to_owned()), Value::Integer(30)],
                vec![Value::Text("b".to_owned()), Value::Integer(5)],
            ],
        }
    }

    fn populated_schema(self) -> Schema {
        match self {
            Self::Avg => bigint_metrics_schema(),
            Self::Min | Self::Max => metrics_schema(),
        }
    }

    fn score_input(self, score: i64) -> Value {
        match self {
            Self::Avg => Value::BigInt(score),
            Self::Min | Self::Max => Value::Integer(score.try_into().expect("score fits i32")),
        }
    }
}

async fn wait_for_values(
    client: &JazzClient,
    query: jazz_tools::Query,
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

async fn assert_nullable_empty_and_all_null(case: AggregateCase, subject: &str) {
    let schema = nullable_metrics_schema();
    let server = JazzServer::start_with_schema(schema.clone()).await;
    let client =
        JazzClient::connect(server.make_client_context_for_user(schema, test_user_id(subject)))
            .await
            .expect("connect client");
    let query = case.query();
    let mut stream = client
        .subscribe(query.clone())
        .await
        .unwrap_or_else(|err| panic!("subscribe {} aggregate: {err}", case.name()));

    wait_for_values(
        &client,
        query.clone(),
        vec![vec![Value::Null]],
        &format!("one-shot empty {} is public null", case.name()),
    )
    .await;
    wait_for_subscription_driven_values(
        &client,
        &mut stream,
        query.clone(),
        vec![vec![Value::Null]],
        &format!("subscription empty {} is public null", case.name()),
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
        query.clone(),
        vec![vec![Value::Null]],
        &format!("one-shot all-null {} is public null", case.name()),
    )
    .await;
    wait_for_subscription_driven_values(
        &client,
        &mut stream,
        query,
        vec![vec![Value::Null]],
        &format!("subscription all-null {} is public null", case.name()),
    )
    .await;
}

async fn assert_populated_and_grouped(case: AggregateCase, subject: &str) {
    let schema = case.populated_schema();
    let server = JazzServer::start_with_schema(schema.clone()).await;
    let client =
        JazzClient::connect(server.make_client_context_for_user(schema, test_user_id(subject)))
            .await
            .expect("connect client");
    let query = case.query();
    let grouped_query = case.grouped_query();
    let mut stream = client
        .subscribe(query.clone())
        .await
        .unwrap_or_else(|err| panic!("subscribe {} aggregate: {err}", case.name()));
    let mut grouped_stream = client
        .subscribe(grouped_query.clone())
        .await
        .unwrap_or_else(|err| panic!("subscribe grouped {} aggregate: {err}", case.name()));

    for (bucket, score) in [("a", 10), ("a", 30), ("b", 5)] {
        let (_row, _, batch) = client
            .insert(
                "metrics",
                row_input!("bucket" => bucket, "score" => case.score_input(score)),
            )
            .expect("insert score");
        client
            .wait_for_batch(batch, DurabilityTier::Local)
            .await
            .expect("score settles");
    }

    wait_for_values(
        &client,
        query.clone(),
        vec![vec![case.populated_value()]],
        &format!("one-shot populated {}", case.name()),
    )
    .await;
    wait_for_subscription_driven_values(
        &client,
        &mut stream,
        query,
        vec![vec![case.populated_value()]],
        &format!("subscription populated {}", case.name()),
    )
    .await;

    wait_for_values(
        &client,
        grouped_query.clone(),
        case.grouped_values(),
        &format!("one-shot grouped {}", case.name()),
    )
    .await;
    wait_for_subscription_driven_values(
        &client,
        &mut grouped_stream,
        grouped_query,
        case.grouped_values(),
        &format!("subscription grouped {}", case.name()),
    )
    .await;
}

#[tokio::test(flavor = "current_thread")]
async fn aggregate_avg_public_boundary_and_subscription_results() {
    tokio::task::LocalSet::new()
        .run_until(async {
            assert_nullable_empty_and_all_null(AggregateCase::Avg, "aggregate-avg-null").await;
            assert_populated_and_grouped(AggregateCase::Avg, "aggregate-avg-populated").await;
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn aggregate_min_public_boundary_and_subscription_results() {
    tokio::task::LocalSet::new()
        .run_until(async {
            assert_nullable_empty_and_all_null(AggregateCase::Min, "aggregate-min-null").await;
            assert_populated_and_grouped(AggregateCase::Min, "aggregate-min-populated").await;
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn aggregate_max_public_boundary_and_subscription_results() {
    tokio::task::LocalSet::new()
        .run_until(async {
            assert_nullable_empty_and_all_null(AggregateCase::Max, "aggregate-max-null").await;
            assert_populated_and_grouped(AggregateCase::Max, "aggregate-max-populated").await;
        })
        .await;
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
                sum_query.clone(),
                vec![vec![Value::Null]],
                "subscription all-null sum is public null",
            )
            .await;

            // The mixed null/non-null case is NOT covered here: writing a
            // non-null value into a nullable column through the public client
            // currently fails with `value does not match type Nullable(U32)`,
            // because public_to_core_value maps Value::Integer to a bare
            // CoreValue::U32 with no schema-aware wrapping. That is a gap in the
            // public WRITE path, independent of aggregate semantics, so it is
            // recorded rather than worked around here. The empty-input and
            // all-NULL cases above are the ones this PR's semantics own.
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
