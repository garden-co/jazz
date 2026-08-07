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
//! 7. `SUM` widens narrow unsigned inputs to `U64` and signed inputs to `I64`.
//! 8. One-shot and maintained `SUM` both report named overflow errors.

use groove::db::{Database, GraphBuilder};
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

fn sum_graph() -> GraphBuilder {
    GraphBuilder::aggregate(
        GraphBuilder::table("metrics"),
        ["bucket"],
        [aggregate(
            AggregateFunction::Sum,
            Some("score"),
            "sum_score",
        )],
    )
}

fn insert_metric(database: &mut Database<MemoryStorage>, id: u64, score: Value) {
    let mut batch = database.open_batch();
    batch.insert("metrics", vec![Value::U64(id), Value::U64(10), score]);
    database.commit_batch(batch).unwrap();
}

fn null() -> Value {
    Value::Nullable(None)
}

fn some(value: Value) -> Value {
    Value::Nullable(Some(Box::new(value)))
}

#[test]
fn non_count_aggregate_outputs_are_always_nullable() {
    let storage = MemoryStorage::new(&["metrics"]);
    let mut database = Database::new(metric_schema(ColumnType::U64), storage).unwrap();

    let result = database.query_graph(metric_aggregates([])).unwrap();
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

#[test]
fn grouped_aggregate_over_zero_rows_returns_no_rows() {
    let storage = MemoryStorage::new(&["metrics"]);
    let mut database = Database::new(metric_schema(ColumnType::U64), storage).unwrap();

    assert!(
        database
            .query_graph(metric_aggregates(["bucket"]))
            .unwrap()
            .is_empty()
    );
}

#[test]
fn ungrouped_aggregate_over_zero_rows_returns_one_row() {
    let storage = MemoryStorage::new(&["metrics"]);
    let mut database = Database::new(metric_schema(ColumnType::U64), storage).unwrap();

    assert_eq!(
        database
            .query_graph(metric_aggregates([]))
            .unwrap()
            .to_values()
            .unwrap(),
        [(
            vec![Value::U64(0), Value::U64(0), null(), null(), null(), null(),],
            1,
        )]
    );
}

#[test]
fn all_null_inputs_return_null_except_for_counts() {
    let storage = MemoryStorage::new(&["metrics"]);
    let mut database = Database::new(metric_schema(ColumnType::U64.nullable()), storage).unwrap();
    let mut batch = database.open_batch();
    batch.insert("metrics", vec![Value::U64(1), Value::U64(10), null()]);
    database.commit_batch(batch).unwrap();

    assert_eq!(
        database
            .query_graph(metric_aggregates(["bucket"]))
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

#[test]
fn nullable_aggregate_outputs_wrap_non_null_results() {
    let storage = MemoryStorage::new(&["metrics"]);
    let mut database = Database::new(metric_schema(ColumnType::U64.nullable()), storage).unwrap();
    let mut batch = database.open_batch();
    batch.insert(
        "metrics",
        vec![Value::U64(1), Value::U64(10), some(Value::U64(5))],
    );
    database.commit_batch(batch).unwrap();

    assert_eq!(
        database
            .query_graph(metric_aggregates(["bucket"]))
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

#[test]
fn signed_i64_inputs_are_supported() {
    let storage = MemoryStorage::new(&["metrics"]);
    let mut database = Database::new(metric_schema(ColumnType::I64), storage).unwrap();
    let mut batch = database.open_batch();
    batch.insert(
        "metrics",
        vec![Value::U64(1), Value::U64(10), Value::I64(-3)],
    );
    batch.insert(
        "metrics",
        vec![Value::U64(2), Value::U64(10), Value::I64(2)],
    );
    database.commit_batch(batch).unwrap();

    assert_eq!(
        database
            .query_graph(metric_aggregates(["bucket"]))
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

#[test]
fn sum_widens_u8_u16_and_u32_without_narrowing_the_result() {
    for (score_type, scores, expected) in [
        (
            ColumnType::U8,
            vec![Value::U8(200), Value::U8(100)],
            Value::U64(300),
        ),
        (
            ColumnType::U16,
            vec![Value::U16(60_000), Value::U16(10_000)],
            Value::U64(70_000),
        ),
        (
            ColumnType::U32,
            vec![Value::U32(4_000_000_000), Value::U32(500_000_000)],
            Value::U64(4_500_000_000),
        ),
    ] {
        let storage = MemoryStorage::new(&["metrics"]);
        let mut database = Database::new(metric_schema(score_type), storage).unwrap();
        for (id, score) in scores.into_iter().enumerate() {
            insert_metric(&mut database, id as u64 + 1, score);
        }

        let result = database.query_graph(sum_graph()).unwrap();
        assert_eq!(
            result.descriptor.fields()[1].value_type,
            ValueType::Nullable(Box::new(ValueType::U64))
        );
        assert_eq!(
            result.to_values().unwrap(),
            [(vec![Value::U64(10), some(expected.clone())], 1)]
        );
        let subscription = database.subscribe_one_sink(sum_graph()).unwrap();
        assert_eq!(
            subscription.recv().unwrap().to_values().unwrap(),
            [(vec![Value::U64(10), some(expected)], 1)]
        );
    }
}

#[test]
fn sum_widens_i32_and_preserves_signed_negative_results() {
    let storage = MemoryStorage::new(&["metrics"]);
    let mut database = Database::new(metric_schema(ColumnType::I32), storage).unwrap();
    let mut batch = database.open_batch();
    batch.insert(
        "metrics",
        vec![Value::U64(1), Value::U64(10), Value::I32(i32::MAX)],
    );
    batch.insert(
        "metrics",
        vec![Value::U64(2), Value::U64(10), Value::I32(1)],
    );
    batch.insert(
        "metrics",
        vec![Value::U64(3), Value::U64(20), Value::I32(-10)],
    );
    batch.insert(
        "metrics",
        vec![Value::U64(4), Value::U64(20), Value::I32(3)],
    );
    database.commit_batch(batch).unwrap();

    let result = database.query_graph(sum_graph()).unwrap();
    assert_eq!(
        result.descriptor.fields()[1].value_type,
        ValueType::Nullable(Box::new(ValueType::I64))
    );
    assert_eq!(
        result.to_values().unwrap(),
        [
            (
                vec![Value::U64(10), some(Value::I64(i64::from(i32::MAX) + 1))],
                1,
            ),
            (vec![Value::U64(20), some(Value::I64(-7))], 1),
        ]
    );
}

#[test]
fn maintained_sum_retractions_reach_zero_and_signed_negative_values() {
    let storage = MemoryStorage::new(&["metrics"]);
    let mut database = Database::new(metric_schema(ColumnType::I32), storage).unwrap();
    let subscription = database.subscribe_one_sink(sum_graph()).unwrap();
    assert!(subscription.recv().unwrap().is_empty());

    insert_metric(&mut database, 1, Value::I32(7));
    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        [(vec![Value::U64(10), some(Value::I64(7))], 1)]
    );

    insert_metric(&mut database, 2, Value::I32(-7));
    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        [
            (vec![Value::U64(10), some(Value::I64(0))], 1),
            (vec![Value::U64(10), some(Value::I64(7))], -1),
        ]
    );

    insert_metric(&mut database, 3, Value::I32(-3));
    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        [
            (vec![Value::U64(10), some(Value::I64(0))], -1),
            (vec![Value::U64(10), some(Value::I64(-3))], 1),
        ]
    );

    let mut batch = database.open_batch();
    batch.delete("metrics", groove::db::PrimaryKeyValue::U64(3));
    database.commit_batch(batch).unwrap();
    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        [
            (vec![Value::U64(10), some(Value::I64(0))], 1),
            (vec![Value::U64(10), some(Value::I64(-3))], -1),
        ]
    );
}

#[test]
fn maintained_unsigned_sum_retraction_reaches_zero_without_underflow() {
    let storage = MemoryStorage::new(&["metrics"]);
    let mut database = Database::new(metric_schema(ColumnType::U8), storage).unwrap();
    let subscription = database.subscribe_one_sink(sum_graph()).unwrap();
    assert!(subscription.recv().unwrap().is_empty());

    let mut batch = database.open_batch();
    batch.insert("metrics", vec![Value::U64(1), Value::U64(10), Value::U8(5)]);
    batch.insert("metrics", vec![Value::U64(2), Value::U64(10), Value::U8(0)]);
    database.commit_batch(batch).unwrap();
    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        [(vec![Value::U64(10), some(Value::U64(5))], 1)]
    );
    let mut batch = database.open_batch();
    batch.delete("metrics", groove::db::PrimaryKeyValue::U64(1));
    database.commit_batch(batch).unwrap();

    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        [
            (vec![Value::U64(10), some(Value::U64(0))], 1),
            (vec![Value::U64(10), some(Value::U64(5))], -1),
        ]
    );
}

#[test]
fn nullable_sum_skips_null_inputs_while_count_and_all_null_groups_keep_sql_semantics() {
    let storage = MemoryStorage::new(&["metrics"]);
    let mut database = Database::new(metric_schema(ColumnType::U8.nullable()), storage).unwrap();
    let mut batch = database.open_batch();
    batch.insert("metrics", vec![Value::U64(1), Value::U64(10), null()]);
    batch.insert(
        "metrics",
        vec![Value::U64(2), Value::U64(10), some(Value::U8(7))],
    );
    batch.insert("metrics", vec![Value::U64(3), Value::U64(20), null()]);
    database.commit_batch(batch).unwrap();

    let result = database.query_graph(metric_aggregates(["bucket"])).unwrap();
    assert_eq!(
        result.descriptor.fields()[3].value_type,
        ValueType::Nullable(Box::new(ValueType::U64))
    );
    assert_eq!(
        result.to_values().unwrap(),
        [
            (
                vec![
                    Value::U64(10),
                    Value::U64(2),
                    Value::U64(1),
                    some(Value::U64(7)),
                    some(Value::F64(7.0)),
                    some(Value::U8(7)),
                    some(Value::U8(7)),
                ],
                1,
            ),
            (
                vec![
                    Value::U64(20),
                    Value::U64(1),
                    Value::U64(0),
                    null(),
                    null(),
                    null(),
                    null(),
                ],
                1,
            ),
        ]
    );
}

#[test]
fn one_shot_and_maintained_sum_agree_on_named_u64_and_i64_overflow() {
    for (score_type, first, second) in [
        (ColumnType::U64, Value::U64(u64::MAX), Value::U64(1)),
        (ColumnType::I64, Value::I64(i64::MAX), Value::I64(1)),
    ] {
        let storage = MemoryStorage::new(&["metrics"]);
        let mut one_shot = Database::new(metric_schema(score_type.clone()), storage).unwrap();
        let mut batch = one_shot.open_batch();
        batch.insert(
            "metrics",
            vec![Value::U64(1), Value::U64(10), first.clone()],
        );
        batch.insert(
            "metrics",
            vec![Value::U64(2), Value::U64(10), second.clone()],
        );
        one_shot.commit_batch(batch).unwrap();
        assert!(matches!(
            one_shot.query_graph(sum_graph()),
            Err(groove::db::Error::IvmRuntime(
                groove::db::IvmRuntimeError::AggregateSumOverflow
            ))
        ));

        let storage = MemoryStorage::new(&["metrics"]);
        let mut maintained = Database::new(metric_schema(score_type), storage).unwrap();
        let subscription = maintained.subscribe_one_sink(sum_graph()).unwrap();
        assert!(subscription.recv().unwrap().is_empty());
        let mut batch = maintained.open_batch();
        batch.insert("metrics", vec![Value::U64(1), Value::U64(10), first]);
        batch.insert("metrics", vec![Value::U64(2), Value::U64(10), second]);
        assert!(matches!(
            maintained.commit_batch(batch),
            Err(groove::db::Error::IvmRuntime(
                groove::db::IvmRuntimeError::AggregateSumOverflow
            ))
        ));
    }
}
