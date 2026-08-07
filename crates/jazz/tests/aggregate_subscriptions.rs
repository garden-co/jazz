#![cfg(feature = "test-utils")]

mod support;

use std::time::{Duration, Instant};

use jazz::groove::records::{BorrowedRecord, RecordDescriptor, Value as GrooveValue, ValueType};
use jazz::row_input;
use jazz::tools::public_schema::AggregateFunction;
use jazz::tools::server::JazzServer;
use jazz::tools::{
    ColumnMergeStrategy, ColumnType, DurabilityTier, JazzClient, PolicyExpr, QueryBuilder, Row,
    RowDescriptor, Schema, SchemaBuilder, SubscriptionStream, SubscriptionStreamItem, TableName,
    TablePolicies, TableSchema, Value,
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

fn bigint_metrics_schema() -> Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("metrics")
                .column("bucket", ColumnType::Text)
                .column("score", ColumnType::BigInt),
        )
        .build()
}

fn double_metrics_schema() -> Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("metrics")
                .column("bucket", ColumnType::Text)
                .column("score", ColumnType::Double),
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

/// A materialized view of values delivered by the public subscription stream.
///
/// This deliberately decodes only `SubscriptionStreamItem::Delta` rows. It
/// never asks `JazzClient` to re-run the query, so it observes maintained
/// delivery rather than one-shot evaluation.
struct ObservedSubscription {
    stream: SubscriptionStream,
    descriptor: RecordDescriptor,
    rows: Vec<Row>,
    observed_initial_delta: bool,
    delivered_deltas: usize,
}

impl ObservedSubscription {
    fn new(stream: SubscriptionStream, descriptor: RecordDescriptor) -> Self {
        Self {
            stream,
            descriptor,
            rows: Vec::new(),
            observed_initial_delta: false,
            delivered_deltas: 0,
        }
    }

    fn apply_delta(&mut self, delta: jazz::tools::OrderedRowDelta) {
        for removed in delta.removed {
            self.rows.retain(|row| row.id != removed.id);
        }
        for updated in delta.updated {
            let Some(row) = updated.row else {
                continue;
            };
            let position = self
                .rows
                .iter()
                .position(|current| current.id == updated.id)
                .unwrap_or_else(|| panic!("subscription updated unknown row {:?}", updated.id));
            self.rows[position] = row;
        }
        for added in delta.added {
            if let Some(position) = self.rows.iter().position(|current| current.id == added.id) {
                self.rows[position] = added.row;
            } else {
                self.rows.push(added.row);
            }
        }
    }

    fn values(&self) -> Vec<Vec<Value>> {
        // Public aggregate subscription rows use a synthetic row id plus the
        // query columns. The observer removes only that id envelope, then
        // decodes the user-visible aggregate columns from delivered bytes.
        let descriptor = RecordDescriptor::new(
            std::iter::once(("row_uuid".to_owned(), ValueType::Uuid)).chain(
                self.descriptor.fields().iter().map(|field| {
                    (
                        field.name.clone().expect("named aggregate field"),
                        ValueType::Nullable(Box::new(field.value_type.clone())),
                    )
                }),
            ),
        );
        let mut values = self
            .rows
            .iter()
            .map(|row| {
                BorrowedRecord::new(row.data.as_ref(), &descriptor)
                    .to_values()
                    .unwrap_or_else(|err| panic!("decode delivered subscription row: {err}"))
                    .into_iter()
                    .skip(1)
                    .take(self.descriptor.fields().len())
                    .map(|value| match value {
                        GrooveValue::Nullable(Some(value)) => match *value {
                            GrooveValue::String(value) => Value::Text(value),
                            GrooveValue::I32(value) => Value::Integer(value),
                            GrooveValue::I64(value) => Value::BigInt(value),
                            GrooveValue::U64(value) => Value::Timestamp(value),
                            GrooveValue::F64(value) => Value::Double(value),
                            other => panic!("unsupported delivered aggregate value: {other:?}"),
                        },
                        GrooveValue::String(value) => Value::Text(value),
                        GrooveValue::I32(value) => Value::Integer(value),
                        GrooveValue::I64(value) => Value::BigInt(value),
                        GrooveValue::U64(value) => Value::Timestamp(value),
                        GrooveValue::F64(value) => Value::Double(value),
                        other => panic!("unsupported delivered aggregate value: {other:?}"),
                    })
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        values.sort_by(|left, right| format!("{left:?}").cmp(&format!("{right:?}")));
        values
    }

    async fn wait_for_values(&mut self, expected: Vec<Vec<Value>>, label: &str) {
        self.wait_for_values_since(self.delivered_deltas, expected, label)
            .await;
    }

    async fn wait_for_values_since(
        &mut self,
        minimum_deliveries: usize,
        expected: Vec<Vec<Value>>,
        label: &str,
    ) {
        let deadline = Instant::now() + Duration::from_secs(5);
        loop {
            if self.delivered_deltas >= minimum_deliveries
                && self.observed_initial_delta
                && self.values() == expected
            {
                return;
            }
            let now = Instant::now();
            let item =
                tokio::time::timeout(deadline.saturating_duration_since(now), self.stream.next())
                    .await
                    .unwrap_or_else(|_| {
                        panic!(
                            "{label}: timed out; delivered values were {:?}",
                            self.values()
                        )
                    })
                    .unwrap_or_else(|| panic!("{label}: subscription stream closed"));
            match item {
                SubscriptionStreamItem::Delta(delta) => {
                    self.apply_delta(delta);
                    self.observed_initial_delta = true;
                    self.delivered_deltas += 1;
                }
                SubscriptionStreamItem::Rejected { reason } => {
                    panic!("{label}: subscription rejected: {reason:?}")
                }
            }
        }
    }

    async fn assert_values_remain(
        &mut self,
        expected: Vec<Vec<Value>>,
        duration: Duration,
        label: &str,
    ) {
        let deadline = Instant::now() + duration;
        loop {
            if self.values() != expected {
                panic!("{label}: delivered values were {:?}", self.values());
            }
            let now = Instant::now();
            if now >= deadline {
                return;
            }
            match tokio::time::timeout(deadline - now, self.stream.next()).await {
                Err(_) => return,
                Ok(None) => panic!("{label}: subscription stream closed"),
                Ok(Some(SubscriptionStreamItem::Rejected { reason })) => {
                    panic!("{label}: subscription rejected: {reason:?}")
                }
                Ok(Some(SubscriptionStreamItem::Delta(delta))) => {
                    self.apply_delta(delta);
                    self.observed_initial_delta = true;
                    self.delivered_deltas += 1;
                }
            }
        }
    }
}

fn aggregate_descriptor(
    fields: impl IntoIterator<Item = (&'static str, ValueType)>,
) -> RecordDescriptor {
    RecordDescriptor::new(fields)
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
    insert_bigint_metric_at_tier(client, bucket, score, DurabilityTier::Local).await;
}

async fn insert_bigint_metric_at_tier(
    client: &JazzClient,
    bucket: &str,
    score: i64,
    tier: DurabilityTier,
) {
    let (_, _, batch) = client
        .insert(
            "metrics",
            row_input!("bucket" => bucket, "score" => Value::BigInt(score)),
        )
        .expect("insert bigint metric");
    client
        .wait_for_batch(batch, tier)
        .await
        .expect("bigint metric settles");
}

async fn insert_double_metric_at_tier(
    client: &JazzClient,
    bucket: &str,
    score: f64,
    tier: DurabilityTier,
) {
    let (_, _, batch) = client
        .insert(
            "metrics",
            row_input!("bucket" => bucket, "score" => Value::Double(score)),
        )
        .expect("insert double metric");
    client
        .wait_for_batch(batch, tier)
        .await
        .expect("double metric settles");
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
            let writer = JazzClient::connect(server.make_client_context_for_user(
                schema.clone(),
                "aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaaaa1",
            ))
            .await
            .expect("connect writer");
            let client = JazzClient::connect(
                server.make_client_context_for_user(schema, "aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaaa11"),
            )
            .await
            .expect("connect client");
            let count_query = QueryBuilder::new("metrics").count().build();
            let grouped_sum_query = QueryBuilder::new("metrics")
                .sum("score")
                .group_by("bucket")
                .build();
            let mut count_stream = ObservedSubscription::new(
                client
                    .subscribe(count_query.clone())
                    .await
                    .expect("subscribe count aggregate"),
                aggregate_descriptor([("count", ValueType::U64)]),
            );
            let mut sum_stream = ObservedSubscription::new(
                client
                    .subscribe(grouped_sum_query.clone())
                    .await
                    .expect("subscribe grouped sum aggregate"),
                aggregate_descriptor([
                    ("bucket", ValueType::String),
                    ("sum_score", ValueType::I32),
                ]),
            );

            count_stream
                .wait_for_values(Vec::new(), "initial empty count")
                .await;

            let (a1, _, batch) = writer
                .insert("metrics", row_input!("bucket" => "a", "score" => 10))
                .expect("insert a1");
            writer
                .wait_for_batch(batch, DurabilityTier::EdgeServer)
                .await
                .expect("a1 settles");
            count_stream
                .wait_for_values(vec![vec![Value::Timestamp(1)]], "count after a1")
                .await;
            sum_stream
                .wait_for_values(
                    vec![vec![Value::Text("a".to_owned()), Value::Integer(10)]],
                    "sum after a1",
                )
                .await;

            let (b1, _, batch) = writer
                .insert("metrics", row_input!("bucket" => "b", "score" => 7))
                .expect("insert b1");
            writer
                .wait_for_batch(batch, DurabilityTier::EdgeServer)
                .await
                .expect("b1 settles");
            count_stream
                .wait_for_values(vec![vec![Value::Timestamp(2)]], "count after b1")
                .await;
            sum_stream
                .wait_for_values(
                    vec![
                        vec![Value::Text("a".to_owned()), Value::Integer(10)],
                        vec![Value::Text("b".to_owned()), Value::Integer(7)],
                    ],
                    "sum after b1",
                )
                .await;

            let batch = writer.delete(b1).expect("delete b1 and empty b");
            writer
                .wait_for_batch(batch, DurabilityTier::EdgeServer)
                .await
                .expect("delete b1 settles");
            let (_b2, _, batch) = writer
                .insert("metrics", row_input!("bucket" => "b", "score" => 5))
                .expect("repopulate b");
            writer
                .wait_for_batch(batch, DurabilityTier::EdgeServer)
                .await
                .expect("repopulate b settles");
            sum_stream
                .wait_for_values(
                    vec![
                        vec![Value::Text("a".to_owned()), Value::Integer(10)],
                        vec![Value::Text("b".to_owned()), Value::Integer(5)],
                    ],
                    "sum after repopulating b",
                )
                .await;

            let batch = writer.delete(a1).expect("delete a1");
            writer
                .wait_for_batch(batch, DurabilityTier::EdgeServer)
                .await
                .expect("delete settles");
            count_stream
                .wait_for_values(vec![vec![Value::Timestamp(1)]], "count after delete a1")
                .await;
            sum_stream
                .wait_for_values(
                    vec![vec![Value::Text("b".to_owned()), Value::Integer(5)]],
                    "sum after delete a1",
                )
                .await;
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn maintained_integer_sum_accumulates_multiple_deltas_and_retracts_empty_group() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let schema = metrics_schema();
            let server = JazzServer::start_with_schema(schema.clone()).await;
            let writer = JazzClient::connect(server.make_client_context_for_user(
                schema.clone(),
                "aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaaaa6",
            ))
            .await
            .expect("connect writer");
            let client = JazzClient::connect(
                server.make_client_context_for_user(schema, "aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaaa12"),
            )
            .await
            .expect("connect client");
            let grouped_sum_query = QueryBuilder::new("metrics")
                .sum("score")
                .group_by("bucket")
                .build();
            let mut sum_stream = ObservedSubscription::new(
                client
                    .subscribe(grouped_sum_query.clone())
                    .await
                    .expect("subscribe grouped sum aggregate"),
                aggregate_descriptor([
                    ("bucket", ValueType::String),
                    ("sum_score", ValueType::I32),
                ]),
            );

            let (first, _, batch) = writer
                .insert("metrics", row_input!("bucket" => "same", "score" => 10))
                .expect("insert first metric");
            writer
                .wait_for_batch(batch, DurabilityTier::EdgeServer)
                .await
                .expect("first metric settles");
            sum_stream
                .wait_for_values(
                    vec![vec![Value::Text("same".to_owned()), Value::Integer(10)]],
                    "sum after first same-group delta",
                )
                .await;

            let (second, _, batch) = writer
                .insert("metrics", row_input!("bucket" => "same", "score" => 7))
                .expect("insert second metric");
            writer
                .wait_for_batch(batch, DurabilityTier::EdgeServer)
                .await
                .expect("second metric settles");
            sum_stream
                .wait_for_values(
                    vec![vec![Value::Text("same".to_owned()), Value::Integer(17)]],
                    "sum accumulates a second same-group delta",
                )
                .await;

            let batch = writer.delete(first).expect("delete first metric");
            writer
                .wait_for_batch(batch, DurabilityTier::EdgeServer)
                .await
                .expect("first delete settles");
            sum_stream
                .wait_for_values(
                    vec![vec![Value::Text("same".to_owned()), Value::Integer(7)]],
                    "sum subtracts a signed deletion delta",
                )
                .await;

            let batch = writer.delete(second).expect("delete second metric");
            writer
                .wait_for_batch(batch, DurabilityTier::EdgeServer)
                .await
                .expect("second delete settles");
            sum_stream
                .wait_for_values(Vec::new(), "empty signed aggregate group is retracted")
                .await;
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn maintained_bigint_sum_replaces_a_multi_row_group_after_insert() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let schema = bigint_metrics_schema();
            let server = JazzServer::start_with_schema(schema.clone()).await;
            let writer = JazzClient::connect(server.make_client_context_for_user(
                schema.clone(),
                "aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaaaa9",
            ))
            .await
            .expect("connect writer");
            let client = JazzClient::connect(
                server.make_client_context_for_user(schema, "aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaaa13"),
            )
            .await
            .expect("connect client");
            let query = QueryBuilder::new("metrics")
                .sum("score")
                .group_by("bucket")
                .build();

            insert_bigint_metric_at_tier(&writer, "same", -11, DurabilityTier::EdgeServer).await;
            insert_bigint_metric_at_tier(&writer, "same", 7, DurabilityTier::EdgeServer).await;
            let mut stream = ObservedSubscription::new(
                client
                    .subscribe(query.clone())
                    .await
                    .expect("subscribe grouped bigint sum"),
                aggregate_descriptor([
                    ("bucket", ValueType::String),
                    ("sum_score", ValueType::I64),
                ]),
            );
            stream
                .wait_for_values(
                    vec![vec![Value::Text("same".to_owned()), Value::BigInt(-4)]],
                    "initial multi-row bigint sum",
                )
                .await;

            insert_bigint_metric_at_tier(&writer, "same", 3, DurabilityTier::EdgeServer).await;
            stream
                .wait_for_values(
                    vec![vec![Value::Text("same".to_owned()), Value::BigInt(-1)]],
                    "bigint sum replaces the prior group result after insert",
                )
                .await;
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn maintained_double_sum_and_avg_replace_a_multi_row_group_after_insert() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let schema = double_metrics_schema();
            let server = JazzServer::start_with_schema(schema.clone()).await;
            let writer = JazzClient::connect(server.make_client_context_for_user(
                schema.clone(),
                "aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaaa10",
            ))
            .await
            .expect("connect writer");
            let client = JazzClient::connect(
                server.make_client_context_for_user(schema, "aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaaa14"),
            )
            .await
            .expect("connect client");
            let sum_query = QueryBuilder::new("metrics")
                .sum("score")
                .group_by("bucket")
                .build();
            let avg_query = QueryBuilder::new("metrics")
                .avg("score")
                .group_by("bucket")
                .build();

            insert_double_metric_at_tier(&writer, "same", 1.5, DurabilityTier::EdgeServer).await;
            insert_double_metric_at_tier(&writer, "same", -0.25, DurabilityTier::EdgeServer).await;
            let mut sum_stream = ObservedSubscription::new(
                client
                    .subscribe(sum_query.clone())
                    .await
                    .expect("subscribe grouped double sum"),
                aggregate_descriptor([
                    ("bucket", ValueType::String),
                    ("sum_score", ValueType::F64),
                ]),
            );
            let mut avg_stream = ObservedSubscription::new(
                client
                    .subscribe(avg_query.clone())
                    .await
                    .expect("subscribe grouped double avg"),
                aggregate_descriptor([
                    ("bucket", ValueType::String),
                    ("avg_score", ValueType::F64),
                ]),
            );

            sum_stream
                .wait_for_values(
                    vec![vec![Value::Text("same".to_owned()), Value::Double(1.25)]],
                    "initial multi-row double sum",
                )
                .await;
            avg_stream
                .wait_for_values(
                    vec![vec![Value::Text("same".to_owned()), Value::Double(0.625)]],
                    "initial multi-row double avg",
                )
                .await;

            insert_double_metric_at_tier(&writer, "same", 0.5, DurabilityTier::EdgeServer).await;
            sum_stream
                .wait_for_values(
                    vec![vec![Value::Text("same".to_owned()), Value::Double(1.75)]],
                    "double sum replaces the prior group result after insert",
                )
                .await;
            avg_stream
                .wait_for_values(
                    vec![vec![
                        Value::Text("same".to_owned()),
                        Value::Double(1.75 / 3.0),
                    ]],
                    "double avg replaces the prior group result after insert",
                )
                .await;
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn maintained_min_and_max_replace_multi_row_groups() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let schema = metrics_schema();
            let server = JazzServer::start_with_schema(schema.clone()).await;
            let writer = JazzClient::connect(server.make_client_context_for_user(
                schema.clone(),
                "aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaaa15",
            ))
            .await
            .expect("connect writer");
            let client = JazzClient::connect(
                server.make_client_context_for_user(schema, "aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaaa16"),
            )
            .await
            .expect("connect client");
            insert_metric_at_tier(&writer, "same", 10, DurabilityTier::EdgeServer).await;
            insert_metric_at_tier(&writer, "same", 4, DurabilityTier::EdgeServer).await;
            let mut min_stream = ObservedSubscription::new(
                client
                    .subscribe(
                        QueryBuilder::new("metrics")
                            .min("score")
                            .group_by("bucket")
                            .build(),
                    )
                    .await
                    .expect("subscribe grouped min"),
                aggregate_descriptor([
                    ("bucket", ValueType::String),
                    ("min_score", ValueType::I32),
                ]),
            );
            let mut max_stream = ObservedSubscription::new(
                client
                    .subscribe(
                        QueryBuilder::new("metrics")
                            .max("score")
                            .group_by("bucket")
                            .build(),
                    )
                    .await
                    .expect("subscribe grouped max"),
                aggregate_descriptor([
                    ("bucket", ValueType::String),
                    ("max_score", ValueType::I32),
                ]),
            );
            min_stream
                .wait_for_values(
                    vec![vec![Value::Text("same".to_owned()), Value::Integer(4)]],
                    "initial multi-row min",
                )
                .await;
            max_stream
                .wait_for_values(
                    vec![vec![Value::Text("same".to_owned()), Value::Integer(10)]],
                    "initial multi-row max",
                )
                .await;
            insert_metric_at_tier(&writer, "same", 1, DurabilityTier::EdgeServer).await;
            min_stream
                .wait_for_values(
                    vec![vec![Value::Text("same".to_owned()), Value::Integer(1)]],
                    "min replacement",
                )
                .await;
            max_stream
                .wait_for_values(
                    vec![vec![Value::Text("same".to_owned()), Value::Integer(10)]],
                    "max remains stable",
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
            let mut spy_stream = ObservedSubscription::new(
                spy.subscribe(count_query.clone())
                    .await
                    .expect("subscribe spy aggregate"),
                aggregate_descriptor([("count", ValueType::U64)]),
            );

            spy_stream
                .wait_for_values(Vec::new(), "spy initial count")
                .await;

            let (admin_row, _, batch) = admin
                .insert(
                    "metrics",
                    row_input!("owner_id" => admin_id.clone(), "score" => 10),
                )
                .expect("insert admin row");
            admin
                .wait_for_batch(batch, DurabilityTier::EdgeServer)
                .await
                .expect("admin row settles");
            spy_stream
                .assert_values_remain(
                    Vec::new(),
                    Duration::from_millis(250),
                    "spy count ignores admin row",
                )
                .await;

            let batch = admin.delete(admin_row).expect("delete admin row");
            admin
                .wait_for_batch(batch, DurabilityTier::EdgeServer)
                .await
                .expect("admin delete settles");
            spy_stream
                .assert_values_remain(
                    Vec::new(),
                    Duration::from_millis(250),
                    "spy count remains empty after invisible delete",
                )
                .await;
        })
        .await;
}
