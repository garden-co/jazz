//! Black-box coverage for Groove's SQL aggregate semantics.
//!
//! These tests pin the following invariants through the public `Database` API:
//! 1. `SUM`, `AVG`, `MIN`, and `MAX` always return nullable columns.
//! 2. Grouped aggregates over zero rows return no rows.
//! 3. Ungrouped aggregates always return exactly one row.
//! 4. An all-null input produces null for `SUM`, `AVG`, `MIN`, and `MAX`;
//!    `COUNT(column)` produces zero, while `COUNT(*)` still counts rows.
//! 5. Integer `AVG` has the fixed output type `Nullable(F64)`; maintained view
//!    output types never change with their contents.
//! 6. Signed `I64` inputs are supported by `SUM`, `AVG`, `MIN`, and `MAX`.

use groove::db::{Database, Error as DatabaseError, GraphBuilder, IvmRuntimeError};
use groove::ivm::{AggregateExpr, AggregateFunction, PlanExpr};
use groove::records::{Value, ValueType};
use groove::schema::{
    ColumnSchema, ColumnType, DatabaseSchema, IntegerKeyType, PrimaryKey, TableSchema,
};
use groove::storage::MemoryStorage;

fn metric_schema(score_type: ColumnType) -> DatabaseSchema {
    DatabaseSchema::new([TableSchema::new(
        "metrics",
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("bucket", ColumnType::U64),
            ColumnSchema::new("score", score_type),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))])
}

fn aggregate(
    function: AggregateFunction,
    column: Option<&str>,
    output_name: &str,
) -> AggregateExpr {
    AggregateExpr {
        function,
        expression: column.map(|column| PlanExpr::Field(column.to_owned())),
        distinct: false,
        output_name: Some(output_name.to_owned()),
        output_identity: None,
    }
}

fn metric_aggregates(group_cols: impl IntoIterator<Item = &'static str>) -> GraphBuilder {
    GraphBuilder::aggregate(
        GraphBuilder::table("metrics"),
        group_cols,
        [
            aggregate(AggregateFunction::Count, None, "row_count"),
            aggregate(AggregateFunction::Count, Some("score"), "score_count"),
            aggregate(AggregateFunction::Sum, Some("score"), "sum_score"),
            aggregate(AggregateFunction::Avg, Some("score"), "avg_score"),
            aggregate(AggregateFunction::Min, Some("score"), "min_score"),
            aggregate(AggregateFunction::Max, Some("score"), "max_score"),
        ],
    )
}

fn null() -> Value {
    Value::Nullable(None)
}

fn some(value: Value) -> Value {
    Value::Nullable(Some(Box::new(value)))
}

#[futures_test::test]
async fn non_count_aggregate_outputs_are_always_nullable() {
    let storage = MemoryStorage::new(&["metrics"]).expect("valid memory storage families");
    let mut database = Database::new(metric_schema(ColumnType::U64), storage)
        .await
        .unwrap();

    let result = database.query_graph(metric_aggregates([])).await.unwrap();
    let output_types = result
        .descriptor
        .fields()
        .iter()
        .map(|field| field.value_type.clone())
        .collect::<Vec<_>>();

    assert_eq!(
        output_types,
        vec![
            ValueType::U64,
            ValueType::U64,
            ValueType::Nullable(Box::new(ValueType::U64)),
            ValueType::Nullable(Box::new(ValueType::F64)),
            ValueType::Nullable(Box::new(ValueType::U64)),
            ValueType::Nullable(Box::new(ValueType::U64)),
        ]
    );
}

#[futures_test::test]
async fn grouped_aggregate_over_zero_rows_returns_no_rows() {
    let storage = MemoryStorage::new(&["metrics"]).expect("valid memory storage families");
    let mut database = Database::new(metric_schema(ColumnType::U64), storage)
        .await
        .unwrap();

    assert!(
        database
            .query_graph(metric_aggregates(["bucket"]))
            .await
            .unwrap()
            .is_empty()
    );
}

#[futures_test::test]
async fn ungrouped_aggregate_over_zero_rows_returns_one_row() {
    let storage = MemoryStorage::new(&["metrics"]).expect("valid memory storage families");
    let mut database = Database::new(metric_schema(ColumnType::U64), storage)
        .await
        .unwrap();

    assert_eq!(
        database
            .query_graph(metric_aggregates([]))
            .await
            .unwrap()
            .to_values()
            .unwrap(),
        [(
            vec![Value::U64(0), Value::U64(0), null(), null(), null(), null(),],
            1,
        )]
    );
}

#[futures_test::test]
async fn all_null_inputs_return_null_except_for_counts() {
    let storage = MemoryStorage::new(&["metrics"]).expect("valid memory storage families");
    let mut database = Database::new(metric_schema(ColumnType::U64.nullable()), storage)
        .await
        .unwrap();
    let mut batch = database.open_batch();
    batch.insert("metrics", vec![Value::U64(1), Value::U64(10), null()]);
    let applied = database.apply_batch(batch).await.unwrap();
    let persisted = applied.persist().await;
    database.finish_persistence(persisted).unwrap();

    assert_eq!(
        database
            .query_graph(metric_aggregates(["bucket"]))
            .await
            .unwrap()
            .to_values()
            .unwrap(),
        [(
            vec![
                Value::U64(10),
                Value::U64(1),
                Value::U64(0),
                null(),
                null(),
                null(),
                null(),
            ],
            1,
        )]
    );
}

#[futures_test::test]
async fn nullable_aggregate_outputs_wrap_non_null_results() {
    let storage = MemoryStorage::new(&["metrics"]).expect("valid memory storage families");
    let mut database = Database::new(metric_schema(ColumnType::U64.nullable()), storage)
        .await
        .unwrap();
    let mut batch = database.open_batch();
    batch.insert(
        "metrics",
        vec![Value::U64(1), Value::U64(10), some(Value::U64(5))],
    );
    let applied = database.apply_batch(batch).await.unwrap();
    let persisted = applied.persist().await;
    database.finish_persistence(persisted).unwrap();

    assert_eq!(
        database
            .query_graph(metric_aggregates(["bucket"]))
            .await
            .unwrap()
            .to_values()
            .unwrap(),
        [(
            vec![
                Value::U64(10),
                Value::U64(1),
                Value::U64(1),
                some(Value::U64(5)),
                some(Value::F64(5.0)),
                some(Value::U64(5)),
                some(Value::U64(5)),
            ],
            1,
        )]
    );
}

#[futures_test::test]
async fn signed_i64_inputs_are_supported() {
    let storage = MemoryStorage::new(&["metrics"]).expect("valid memory storage families");
    let mut database = Database::new(metric_schema(ColumnType::I64), storage)
        .await
        .unwrap();
    let mut batch = database.open_batch();
    batch.insert(
        "metrics",
        vec![Value::U64(1), Value::U64(10), Value::I64(-3)],
    );
    batch.insert(
        "metrics",
        vec![Value::U64(2), Value::U64(10), Value::I64(2)],
    );
    let applied = database.apply_batch(batch).await.unwrap();
    let persisted = applied.persist().await;
    database.finish_persistence(persisted).unwrap();

    assert_eq!(
        database
            .query_graph(metric_aggregates(["bucket"]))
            .await
            .unwrap()
            .to_values()
            .unwrap(),
        [(
            vec![
                Value::U64(10),
                Value::U64(2),
                Value::U64(2),
                some(Value::I64(-1)),
                some(Value::F64(-0.5)),
                some(Value::I64(-3)),
                some(Value::I64(2)),
            ],
            1,
        )]
    );
}

#[futures_test::test]
async fn sum_overflow_fails_with_a_named_error_at_the_declared_width() {
    let storage = MemoryStorage::new(&["metrics"]).expect("valid memory storage families");
    let mut database = Database::new(metric_schema(ColumnType::U8), storage)
        .await
        .unwrap();
    let mut batch = database.open_batch();
    batch.insert(
        "metrics",
        vec![Value::U64(1), Value::U64(10), Value::U8(250)],
    );
    batch.insert(
        "metrics",
        vec![Value::U64(2), Value::U64(10), Value::U8(10)],
    );
    let applied = database.apply_batch(batch).await.unwrap();
    let persisted = applied.persist().await;
    database.finish_persistence(persisted).unwrap();

    assert!(matches!(
        database.query_graph(metric_aggregates(["bucket"])).await,
        Err(DatabaseError::IvmRuntime(
            IvmRuntimeError::AggregateOverflow
        ))
    ));
}
