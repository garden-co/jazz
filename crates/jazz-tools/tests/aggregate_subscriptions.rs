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
