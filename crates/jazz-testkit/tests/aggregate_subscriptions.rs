use jazz_testkit as support;

use std::time::{Duration, Instant};

use jazz::groove::records::{BorrowedRecord, RecordDescriptor, Value as GrooveValue, ValueType};
use jazz::query::{Aggregate, AggregateFunction, OrderDirection};
use jazz::row_input;
use jazz::tools::{
    ColumnMergeStrategy, ColumnType, DurabilityTier, JazzClient, PolicyExpr, Row, RowDescriptor,
    Schema, SchemaBuilder, SubscriptionStream, SubscriptionStreamItem, TableName, TablePolicies,
    TableSchema, Value,
};
use jazz_server::JazzServer;
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

fn count_named_metrics_schema() -> Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("metrics")
                .column("count", ColumnType::Text)
                .column("score", ColumnType::Integer),
        )
        .build()
}

fn mixed_metrics_schema() -> Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("metrics")
                .column("bucket", ColumnType::Text)
                .column("score", ColumnType::Integer)
                .column("high", ColumnType::BigInt),
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

fn aggregate_alias_collision_schema() -> Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("metrics")
                // This is a valid public column name which happens to match
                // the public label for SUM(score).
                .column("sum_score", ColumnType::Text)
                .nullable_column("score", ColumnType::BigInt),
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
                        .with_select(PolicyExpr::eq_session("owner_id", vec!["user".to_owned()]))
                        .with_delete(PolicyExpr::True),
                ),
        )
        .build()
}

async fn wait_for_values(
    client: &JazzClient,
    query: jazz::query::Query,
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
    group_field_count: usize,
    output_functions: Vec<AggregateFunction>,
    rows: Vec<Row>,
    observed_initial_delta: bool,
    delivered_deltas: usize,
}

impl ObservedSubscription {
    fn new(
        stream: SubscriptionStream,
        query: &jazz::query::Query,
        descriptor: RecordDescriptor,
    ) -> Self {
        let aggregate = query
            .aggregate
            .as_ref()
            .expect("aggregate subscription query");
        // Decode with the same key that core query normalization uses, rather
        // than reconstructing a function ordering in this observer. The key
        // includes function, input column, and generated alias.
        let mut outputs = aggregate.aggregates.clone();
        outputs.sort_by(|left, right| {
            let rank = |function| match function {
                AggregateFunction::Avg => b'a',
                AggregateFunction::Count => b'c',
                AggregateFunction::Min => b'n',
                AggregateFunction::Sum => b's',
                AggregateFunction::Max => b'x',
            };
            rank(left.function)
                .cmp(&rank(right.function))
                .then_with(|| left.column.cmp(&right.column))
                .then_with(|| left.alias.cmp(&right.alias))
        });
        let output_functions = outputs
            .into_iter()
            .map(|aggregate| aggregate.function)
            .collect();
        Self {
            stream,
            descriptor,
            group_field_count: usize::from(aggregate.group_by.is_some()),
            output_functions,
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
        // Maintained aggregate delivery preserves the compiler record rather
        // than reserializing a public row. Its fixed prefix carries the
        // synthetic membership identity; the query fields follow it under
        // their physical names. Decode that real wire shape, then expose only
        // the public query columns to assertions below.
        let query_fields = self.descriptor.fields();
        let output_wire_type = |field: &jazz::groove::records::DescriptorField,
                                function: AggregateFunction| {
            if function == AggregateFunction::Count {
                field.value_type.clone()
            } else {
                ValueType::Nullable(Box::new(field.value_type.clone()))
            }
        };
        let first_output = query_fields
            .get(self.group_field_count)
            .expect("aggregate query has an output field");
        let first_function = *self
            .output_functions
            .first()
            .expect("aggregate query has an output function");
        let descriptor = RecordDescriptor::new(
            [
                ("row_uuid".to_owned(), ValueType::Uuid),
                ("table_name".to_owned(), ValueType::String),
                ("synthetic_row".to_owned(), ValueType::String),
                (
                    "synthetic_replacement".to_owned(),
                    output_wire_type(first_output, first_function),
                ),
            ]
            .into_iter()
            .chain(query_fields.iter().enumerate().map(|(index, field)| {
                let name = field.name.as_deref().expect("named aggregate field");
                let is_group_field = index < self.group_field_count;
                let physical_name = if is_group_field {
                    format!("user_{name}")
                } else {
                    format!("__jazz_aggregate_{name}")
                };
                (
                    physical_name,
                    if is_group_field {
                        field.value_type.clone()
                    } else {
                        output_wire_type(
                            field,
                            self.output_functions[index - self.group_field_count],
                        )
                    },
                )
            })),
        );
        let mut values = self
            .rows
            .iter()
            .map(|row| {
                BorrowedRecord::new(row.data.as_ref(), &descriptor)
                    .to_values()
                    .unwrap_or_else(|err| panic!("decode delivered subscription row: {err}"))
                    .into_iter()
                    .skip(4)
                    .take(self.descriptor.fields().len())
                    .map(|value| match value {
                        GrooveValue::Nullable(None) => Value::Null,
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

async fn wait_for_one_shot_values(
    client: &JazzClient,
    query: jazz::query::Query,
    expected: Vec<Vec<Value>>,
    label: &str,
) {
    wait_for_query(
        client,
        query,
        Some(DurabilityTier::EdgeServer),
        QUERY_TIMEOUT,
        label,
        |rows| {
            let mut actual = rows
                .iter()
                .map(|(_, values)| values.clone())
                .collect::<Vec<_>>();
            actual.sort_by(|left, right| format!("{left:?}").cmp(&format!("{right:?}")));
            (actual == expected).then_some(())
        },
    )
    .await;
}

async fn wait_for_subscription_driven_values(
    client: &JazzClient,
    stream: &mut jazz::tools::SubscriptionStream,
    query: jazz::query::Query,
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
    let (_, _, tx) = client
        .insert("metrics", row_input!("bucket" => bucket, "score" => score))
        .expect("insert integer metric");
    client
        .wait_for_transaction(
            tx.expect("ordinary mutation commits immediately"),
            DurabilityTier::Local,
        )
        .await
        .expect("integer metric settles");
}

async fn insert_metric_at_tier(
    client: &JazzClient,
    bucket: &str,
    score: i32,
    tier: DurabilityTier,
) {
    let (_, _, tx) = client
        .insert("metrics", row_input!("bucket" => bucket, "score" => score))
        .expect("insert integer metric");
    client
        .wait_for_transaction(tx.expect("ordinary mutation commits immediately"), tier)
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
    let (_, _, tx) = client
        .insert(
            "metrics",
            row_input!("bucket" => bucket, "score" => Value::BigInt(score)),
        )
        .expect("insert bigint metric");
    client
        .wait_for_transaction(tx.expect("ordinary mutation commits immediately"), tier)
        .await
        .expect("bigint metric settles");
}

async fn insert_double_metric_at_tier(
    client: &JazzClient,
    bucket: &str,
    score: f64,
    tier: DurabilityTier,
) {
    let (_, _, tx) = client
        .insert(
            "metrics",
            row_input!("bucket" => bucket, "score" => Value::Double(score)),
        )
        .expect("insert double metric");
    client
        .wait_for_transaction(tx.expect("ordinary mutation commits immediately"), tier)
        .await
        .expect("double metric settles");
}

fn aggregate_query(
    outputs: impl IntoIterator<Item = (AggregateFunction, &'static str)>,
) -> jazz::query::Query {
    jazz::query::Query::from("metrics")
        .aggregate(
            outputs
                .into_iter()
                .map(|(function, column)| match function {
                    AggregateFunction::Count => Aggregate::count(),
                    AggregateFunction::Sum => Aggregate::sum(column),
                    AggregateFunction::Avg => Aggregate::avg(column),
                    AggregateFunction::Min => Aggregate::min(column),
                    AggregateFunction::Max => Aggregate::max(column),
                }),
        )
        .group_by("bucket")
}

#[tokio::test(flavor = "current_thread")]
async fn aggregate_subscription_count_and_grouped_sum_track_full_state() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let schema = metrics_schema();
            let server = JazzServer::start_with_schema(schema.clone()).await;
            let writer = jazz_testkit::connect(server.make_client_context_for_user(
                schema.clone(),
                "aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaaaa1",
            ))
            .await
            .expect("connect writer");
            let client = jazz_testkit::connect(
                server.make_client_context_for_user(schema, "aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaaa11"),
            )
            .await
            .expect("connect client");
            let count_query = jazz::query::Query::from("metrics").count();
            let grouped_sum_query = jazz::query::Query::from("metrics")
                .sum("score")
                .group_by("bucket");
            let mut count_stream = ObservedSubscription::new(
                client
                    .subscribe(count_query.clone())
                    .await
                    .expect("subscribe count aggregate"),
                &count_query,
                aggregate_descriptor([("count", ValueType::U64)]),
            );
            let mut sum_stream = ObservedSubscription::new(
                client
                    .subscribe(grouped_sum_query.clone())
                    .await
                    .expect("subscribe grouped sum aggregate"),
                &grouped_sum_query,
                aggregate_descriptor([
                    ("bucket", ValueType::String),
                    ("sum_score", ValueType::I32),
                ]),
            );

            count_stream
                .wait_for_values(Vec::new(), "initial empty count")
                .await;
            wait_for_one_shot_values(
                &client,
                count_query.clone(),
                vec![vec![Value::Timestamp(0)]],
                "one-shot initial empty count",
            )
            .await;

            let (a1, _, tx) = writer
                .insert("metrics", row_input!("bucket" => "a", "score" => 10))
                .expect("insert a1");
            support::wait_for_edge_txs(
                &writer,
                &[tx.expect("ordinary mutation commits immediately")],
            )
            .await;
            count_stream
                .wait_for_values(vec![vec![Value::Timestamp(1)]], "count after a1")
                .await;
            sum_stream
                .wait_for_values(
                    vec![vec![Value::Text("a".to_owned()), Value::Integer(10)]],
                    "sum after a1",
                )
                .await;

            let (b1, _, tx) = writer
                .insert("metrics", row_input!("bucket" => "b", "score" => 7))
                .expect("insert b1");
            support::wait_for_edge_txs(
                &writer,
                &[tx.expect("ordinary mutation commits immediately")],
            )
            .await;
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

            let delete_tx = writer.delete(b1).expect("delete b1 and empty b");
            support::wait_for_edge_txs(
                &writer,
                &[delete_tx.expect("ordinary mutation commits immediately")],
            )
            .await;
            let (_b2, _, insert_tx) = writer
                .insert("metrics", row_input!("bucket" => "b", "score" => 5))
                .expect("repopulate b");
            support::wait_for_edge_txs(
                &writer,
                &[insert_tx.expect("ordinary mutation commits immediately")],
            )
            .await;
            sum_stream
                .wait_for_values(
                    vec![
                        vec![Value::Text("a".to_owned()), Value::Integer(10)],
                        vec![Value::Text("b".to_owned()), Value::Integer(5)],
                    ],
                    "sum after repopulating b",
                )
                .await;

            let tx = writer.delete(a1).expect("delete a1");
            support::wait_for_edge_txs(
                &writer,
                &[tx.expect("ordinary mutation commits immediately")],
            )
            .await;
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

/// A group column named `count` remains distinct from a SUM output on the
/// maintained subscription stream.
///
/// writer ──insert──► server ──aggregate delivery──► subscriber
#[tokio::test(flavor = "current_thread")]
async fn aggregate_subscription_group_field_named_count_uses_structural_wire_slots() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let schema = count_named_metrics_schema();
            let server = JazzServer::start_with_schema(schema.clone()).await;
            let writer = jazz_testkit::connect(server.make_client_context_for_user(
                schema.clone(),
                "aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaaa21",
            ))
            .await
            .expect("connect writer");
            let subscriber = jazz_testkit::connect(
                server.make_client_context_for_user(schema, "aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaaa22"),
            )
            .await
            .expect("connect subscriber");
            let query = jazz::query::Query::from("metrics")
                .sum("score")
                .group_by("count");
            let mut stream = ObservedSubscription::new(
                subscriber
                    .subscribe(query.clone())
                    .await
                    .expect("subscribe grouped sum"),
                &query,
                aggregate_descriptor([("count", ValueType::String), ("sum_score", ValueType::I32)]),
            );
            stream
                .wait_for_values(Vec::new(), "initial grouped sum is empty")
                .await;

            let (_, _, tx) = writer
                .insert("metrics", row_input!("count" => "group", "score" => 1))
                .expect("insert grouped sum metric");
            support::wait_for_edge_txs(
                &writer,
                &[tx.expect("ordinary mutation commits immediately")],
            )
            .await;
            stream
                .wait_for_values(
                    vec![vec![Value::Text("group".to_owned()), Value::Integer(1)]],
                    "group field count and aggregate sum remain distinct",
                )
                .await;
        })
        .await;
}

/// Maintained delivery orders mixed and repeated aggregate functions with the
/// same canonical key as core query normalization.
///
/// writer ──insert──► server ──aggregate replacement──► subscriber
#[tokio::test(flavor = "current_thread")]
async fn aggregate_subscription_uses_core_canonical_order_for_mixed_outputs() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let schema = mixed_metrics_schema();
            let server = JazzServer::start_with_schema(schema.clone()).await;
            let writer = jazz_testkit::connect(server.make_client_context_for_user(
                schema.clone(),
                "aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaaa23",
            ))
            .await
            .expect("connect writer");
            let subscriber = jazz_testkit::connect(
                server.make_client_context_for_user(schema, "aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaaa24"),
            )
            .await
            .expect("connect subscriber");
            let query = jazz::query::Query::from("metrics")
                .aggregate([
                    Aggregate::count(),
                    Aggregate::sum("high"),
                    Aggregate::sum("score"),
                    Aggregate::avg("score"),
                ])
                .group_by("bucket");
            let mut stream = ObservedSubscription::new(
                subscriber
                    .subscribe(query.clone())
                    .await
                    .expect("subscribe mixed aggregate outputs"),
                &query,
                // The core key sorts AVG before SUM and then orders repeated
                // SUM outputs by their input column and generated alias.
                aggregate_descriptor([
                    ("bucket", ValueType::String),
                    ("avg_score", ValueType::F64),
                    ("count", ValueType::U64),
                    ("sum_high", ValueType::I64),
                    ("sum_score", ValueType::I32),
                ]),
            );
            stream
                .wait_for_values(Vec::new(), "initial mixed aggregate is empty")
                .await;

            let mut txs = Vec::new();
            for (score, high) in [(3, 10_i64), (1, 5_i64)] {
                let (_, _, tx) = writer
                    .insert(
                        "metrics",
                        row_input!(
                            "bucket" => "group",
                            "score" => score,
                            "high" => Value::BigInt(high),
                        ),
                    )
                    .expect("insert mixed aggregate metric");
                txs.push(tx.expect("ordinary mutation commits immediately"));
            }
            support::wait_for_edge_txs(&writer, &txs).await;
            stream
                .wait_for_values(
                    vec![vec![
                        Value::Text("group".to_owned()),
                        Value::Double(2.0),
                        Value::Timestamp(2),
                        Value::BigInt(15),
                        Value::Integer(4),
                    ]],
                    "mixed aggregate replacement keeps canonical output order",
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
            let client = jazz_testkit::connect(
                server.make_client_context_for_user(schema, "aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaaaa3"),
            )
            .await
            .expect("connect client");
            let sum_query = jazz::query::Query::from("metrics").sum("score");
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

            let (_null_row, _, tx) = client
                .insert(
                    "metrics",
                    row_input!("bucket" => "a", "score" => Value::Null),
                )
                .expect("insert null score");
            client
                .wait_for_transaction(
                    tx.expect("ordinary mutation commits immediately"),
                    DurabilityTier::Local,
                )
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
async fn grouped_null_aggregate_membership_survives_absence_and_replacement() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let schema = nullable_metrics_schema();
            let server = JazzServer::start_with_schema(schema.clone()).await;
            let writer = jazz_testkit::connect(server.make_client_context_for_user(
                schema.clone(),
                "aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaaaa7",
            ))
            .await
            .expect("connect writer");
            let client = jazz_testkit::connect(
                server.make_client_context_for_user(schema, "aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaaa17"),
            )
            .await
            .expect("connect subscriber");
            let query = jazz::query::Query::from("metrics")
                .aggregate([Aggregate::sum("score"), Aggregate::count()])
                .group_by("bucket");
            let mut stream = ObservedSubscription::new(
                client
                    .subscribe(query.clone())
                    .await
                    .expect("subscribe grouped nullable aggregate"),
                &query,
                aggregate_descriptor([
                    ("bucket", ValueType::String),
                    ("count", ValueType::U64),
                    ("sum_score", ValueType::I32),
                ]),
            );
            stream
                .wait_for_values(Vec::new(), "initial grouped aggregate is empty")
                .await;

            let mut rows = Vec::new();
            let mut txs = Vec::new();
            for bucket in ["null", "null", "gone", "changed"] {
                let (row, _, tx) = writer
                    .insert(
                        "metrics",
                        row_input!("bucket" => bucket, "score" => Value::Null),
                    )
                    .expect("insert nullable metric");
                rows.push(row);
                txs.push(tx.expect("ordinary mutation commits immediately"));
            }
            support::wait_for_edge_txs(&writer, &txs).await;
            stream
                .wait_for_values(
                    vec![
                        vec![
                            Value::Text("changed".to_owned()),
                            Value::Timestamp(1),
                            Value::Null,
                        ],
                        vec![
                            Value::Text("gone".to_owned()),
                            Value::Timestamp(1),
                            Value::Null,
                        ],
                        vec![
                            Value::Text("null".to_owned()),
                            Value::Timestamp(2),
                            Value::Null,
                        ],
                    ],
                    "all-null groups remain delivered",
                )
                .await;

            let tx = writer.delete(rows[2]).expect("delete gone group");
            support::wait_for_edge_txs(
                &writer,
                &[tx.expect("ordinary mutation commits immediately")],
            )
            .await;
            stream
                .wait_for_values(
                    vec![
                        vec![
                            Value::Text("changed".to_owned()),
                            Value::Timestamp(1),
                            Value::Null,
                        ],
                        vec![
                            Value::Text("null".to_owned()),
                            Value::Timestamp(2),
                            Value::Null,
                        ],
                    ],
                    "absent group is retracted without losing all-null group",
                )
                .await;

            let (_, _, tx) = writer
                .insert(
                    "metrics",
                    row_input!("bucket" => "changed", "score" => Value::Null),
                )
                .expect("replace changed aggregate group");
            support::wait_for_edge_txs(
                &writer,
                &[tx.expect("ordinary mutation commits immediately")],
            )
            .await;
            stream
                .wait_for_values(
                    vec![
                        vec![
                            Value::Text("changed".to_owned()),
                            Value::Timestamp(2),
                            Value::Null,
                        ],
                        vec![
                            Value::Text("null".to_owned()),
                            Value::Timestamp(2),
                            Value::Null,
                        ],
                    ],
                    "present-null group can change after another group disappears",
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
            let writer = jazz_testkit::connect(server.make_client_context_for_user(
                schema.clone(),
                "aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaaaa6",
            ))
            .await
            .expect("connect writer");
            let client = jazz_testkit::connect(
                server.make_client_context_for_user(schema, "aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaaa12"),
            )
            .await
            .expect("connect client");
            let grouped_sum_query = jazz::query::Query::from("metrics")
                .sum("score")
                .group_by("bucket");
            let mut sum_stream = ObservedSubscription::new(
                client
                    .subscribe(grouped_sum_query.clone())
                    .await
                    .expect("subscribe grouped sum aggregate"),
                &grouped_sum_query,
                aggregate_descriptor([
                    ("bucket", ValueType::String),
                    ("sum_score", ValueType::I32),
                ]),
            );

            let (first, _, tx) = writer
                .insert("metrics", row_input!("bucket" => "same", "score" => 10))
                .expect("insert first metric");
            support::wait_for_edge_txs(
                &writer,
                &[tx.expect("ordinary mutation commits immediately")],
            )
            .await;
            sum_stream
                .wait_for_values(
                    vec![vec![Value::Text("same".to_owned()), Value::Integer(10)]],
                    "sum after first same-group delta",
                )
                .await;

            let (second, _, tx) = writer
                .insert("metrics", row_input!("bucket" => "same", "score" => 7))
                .expect("insert second metric");
            support::wait_for_edge_txs(
                &writer,
                &[tx.expect("ordinary mutation commits immediately")],
            )
            .await;
            sum_stream
                .wait_for_values(
                    vec![vec![Value::Text("same".to_owned()), Value::Integer(17)]],
                    "sum accumulates a second same-group delta",
                )
                .await;

            let tx = writer.delete(first).expect("delete first metric");
            support::wait_for_edge_txs(
                &writer,
                &[tx.expect("ordinary mutation commits immediately")],
            )
            .await;
            sum_stream
                .wait_for_values(
                    vec![vec![Value::Text("same".to_owned()), Value::Integer(7)]],
                    "sum subtracts a signed deletion delta",
                )
                .await;

            let tx = writer.delete(second).expect("delete second metric");
            support::wait_for_edge_txs(
                &writer,
                &[tx.expect("ordinary mutation commits immediately")],
            )
            .await;
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
            let writer = jazz_testkit::connect(server.make_client_context_for_user(
                schema.clone(),
                "aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaaaa9",
            ))
            .await
            .expect("connect writer");
            let client = jazz_testkit::connect(
                server.make_client_context_for_user(schema, "aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaaa13"),
            )
            .await
            .expect("connect client");
            let query = jazz::query::Query::from("metrics")
                .sum("score")
                .group_by("bucket");

            insert_bigint_metric_at_tier(&writer, "same", -11, DurabilityTier::EdgeServer).await;
            insert_bigint_metric_at_tier(&writer, "same", 7, DurabilityTier::EdgeServer).await;
            let mut stream = ObservedSubscription::new(
                client
                    .subscribe(query.clone())
                    .await
                    .expect("subscribe grouped bigint sum"),
                &query,
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
            let writer = jazz_testkit::connect(server.make_client_context_for_user(
                schema.clone(),
                "aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaaa10",
            ))
            .await
            .expect("connect writer");
            let client = jazz_testkit::connect(
                server.make_client_context_for_user(schema, "aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaaa14"),
            )
            .await
            .expect("connect client");
            let sum_query = jazz::query::Query::from("metrics")
                .sum("score")
                .group_by("bucket");
            let avg_query = jazz::query::Query::from("metrics")
                .avg("score")
                .group_by("bucket");

            insert_double_metric_at_tier(&writer, "same", 1.5, DurabilityTier::EdgeServer).await;
            insert_double_metric_at_tier(&writer, "same", -0.25, DurabilityTier::EdgeServer).await;
            let mut sum_stream = ObservedSubscription::new(
                client
                    .subscribe(sum_query.clone())
                    .await
                    .expect("subscribe grouped double sum"),
                &sum_query,
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
                &avg_query,
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
            let writer = jazz_testkit::connect(server.make_client_context_for_user(
                schema.clone(),
                "aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaaa15",
            ))
            .await
            .expect("connect writer");
            let client = jazz_testkit::connect(
                server.make_client_context_for_user(schema, "aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaaa16"),
            )
            .await
            .expect("connect client");
            insert_metric_at_tier(&writer, "same", 10, DurabilityTier::EdgeServer).await;
            insert_metric_at_tier(&writer, "same", 4, DurabilityTier::EdgeServer).await;
            let min_query = jazz::query::Query::from("metrics")
                .min("score")
                .group_by("bucket");
            let max_query = jazz::query::Query::from("metrics")
                .max("score")
                .group_by("bucket");
            let mut min_stream = ObservedSubscription::new(
                client
                    .subscribe(min_query.clone())
                    .await
                    .expect("subscribe grouped min"),
                &min_query,
                aggregate_descriptor([
                    ("bucket", ValueType::String),
                    ("min_score", ValueType::I32),
                ]),
            );
            let mut max_stream = ObservedSubscription::new(
                client
                    .subscribe(max_query.clone())
                    .await
                    .expect("subscribe grouped max"),
                &max_query,
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
            let client = jazz_testkit::connect(
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
                jazz::query::Query::from("metrics")
                    .sum("score")
                    .group_by("bucket"),
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
            let client = jazz_testkit::connect(
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
                jazz::query::Query::from("metrics")
                    .select(["bucket", "score"])
                    .order_by("score", OrderDirection::Asc),
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
            let client = jazz_testkit::connect(
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
            let client = jazz_testkit::connect(
                server.make_client_context_for_user(schema, "aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaaaa4"),
            )
            .await
            .expect("connect client");
            let sum_query = jazz::query::Query::from("metrics").sum("score");

            wait_for_values(
                &client,
                sum_query.clone(),
                vec![vec![Value::Null]],
                "empty bigint sum is public null",
            )
            .await;

            let (_negative_row, _, tx) = client
                .insert(
                    "metrics",
                    row_input!("bucket" => "a", "score" => Value::BigInt(-3)),
                )
                .expect("insert negative bigint score");
            client
                .wait_for_transaction(
                    tx.expect("ordinary mutation commits immediately"),
                    DurabilityTier::Local,
                )
                .await
                .expect("negative bigint score settles");
            let (_positive_row, _, tx) = client
                .insert(
                    "metrics",
                    row_input!("bucket" => "a", "score" => Value::BigInt(5)),
                )
                .expect("insert positive bigint score");
            client
                .wait_for_transaction(
                    tx.expect("ordinary mutation commits immediately"),
                    DurabilityTier::Local,
                )
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
async fn aggregate_outputs_do_not_collide_with_grouped_public_column_names() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let schema = aggregate_alias_collision_schema();
            let server = JazzServer::start_with_schema(schema.clone()).await;
            let client = jazz_testkit::connect(
                server.make_client_context_for_user(schema, "aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaaaa6"),
            )
            .await
            .expect("connect client");

            let (_, _, tx) = client
                .insert(
                    "metrics",
                    row_input!(
                        "sum_score" => "negative",
                        "score" => Value::BigInt(-3)
                    ),
                )
                .expect("insert signed aggregate value");
            client
                .wait_for_transaction(
                    tx.expect("ordinary mutation commits immediately"),
                    DurabilityTier::Local,
                )
                .await
                .expect("signed aggregate value settles");
            let (_, _, tx) = client
                .insert(
                    "metrics",
                    row_input!(
                        "sum_score" => "negative",
                        "score" => Value::Null
                    ),
                )
                .expect("insert nullable aggregate value");
            client
                .wait_for_transaction(
                    tx.expect("ordinary mutation commits immediately"),
                    DurabilityTier::Local,
                )
                .await
                .expect("nullable aggregate value settles");

            let grouped = jazz::query::Query::from("metrics")
                .sum("score")
                .group_by("sum_score");
            wait_for_values(
                &client,
                grouped,
                vec![vec![Value::Text("negative".to_owned()), Value::BigInt(-3)]],
                "grouped public field and aggregate output stay distinct",
            )
            .await;
            wait_for_values(
                &client,
                jazz::query::Query::from("metrics").sum("score"),
                vec![vec![Value::BigInt(-3)]],
                "non-grouped signed nullable aggregate remains public",
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
            let alice = jazz_testkit::connect(server.make_client_context_for_user(
                schema.clone(),
                "aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaaaa7",
            ))
            .await
            .expect("connect alice");
            let bob = jazz_testkit::connect(
                server.make_client_context_for_user(schema, "aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaaaa8"),
            )
            .await
            .expect("connect bob");
            let query = jazz::query::Query::from("counters").select(["name", "count"]);

            let (counter_id, _, tx) = alice
                .insert("counters", row_input!("name" => "shared", "count" => 0))
                .expect("insert counter");
            support::wait_for_edge_txs(
                &alice,
                &[tx.expect("ordinary mutation commits immediately")],
            )
            .await;
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

            let alice_tx = alice
                .update(counter_id, vec![("count".to_owned(), Value::Integer(3))])
                .expect("alice updates counter");
            let bob_tx = bob
                .update(counter_id, vec![("count".to_owned(), Value::Integer(5))])
                .expect("bob updates counter");
            support::wait_for_edge_txs(
                &alice,
                &[alice_tx.expect("ordinary mutation commits immediately")],
            )
            .await;
            support::wait_for_edge_txs(
                &bob,
                &[bob_tx.expect("ordinary mutation commits immediately")],
            )
            .await;

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
            let admin = jazz_testkit::connect(
                server.make_client_context_for_user(schema.clone(), admin_id.clone()),
            )
            .await
            .expect("connect admin");
            let spy = jazz_testkit::connect(
                server.make_client_context_for_user(schema.clone(), spy_id.clone()),
            )
            .await
            .expect("connect spy");
            let count_query = jazz::query::Query::from("metrics").count();
            let mut spy_stream = ObservedSubscription::new(
                spy.subscribe(count_query.clone())
                    .await
                    .expect("subscribe spy aggregate"),
                &count_query,
                aggregate_descriptor([("count", ValueType::U64)]),
            );

            spy_stream
                .wait_for_values(Vec::new(), "spy initial count")
                .await;
            wait_for_one_shot_values(
                &spy,
                count_query.clone(),
                vec![vec![Value::Timestamp(0)]],
                "one-shot spy initial count",
            )
            .await;

            let (admin_row, _, tx) = admin
                .insert(
                    "metrics",
                    row_input!("owner_id" => admin_id.clone(), "score" => 10),
                )
                .expect("insert admin row");
            support::wait_for_edge_txs(
                &admin,
                &[tx.expect("ordinary mutation commits immediately")],
            )
            .await;
            spy_stream
                .assert_values_remain(
                    Vec::new(),
                    Duration::from_millis(250),
                    "spy count ignores admin row",
                )
                .await;
            wait_for_one_shot_values(
                &spy,
                count_query.clone(),
                vec![vec![Value::Timestamp(0)]],
                "one-shot spy count ignores admin row",
            )
            .await;

            let tx = admin.delete(admin_row).expect("delete admin row");
            support::wait_for_edge_txs(
                &admin,
                &[tx.expect("ordinary mutation commits immediately")],
            )
            .await;
            spy_stream
                .assert_values_remain(
                    Vec::new(),
                    Duration::from_millis(250),
                    "spy count remains empty after invisible delete",
                )
                .await;
            wait_for_one_shot_values(
                &spy,
                count_query,
                vec![vec![Value::Timestamp(0)]],
                "one-shot spy count remains zero after invisible delete",
            )
            .await;
        })
        .await;
}
