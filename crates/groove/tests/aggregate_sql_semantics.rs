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
//! 7. A failed aggregate update leaves maintained state unchanged, so a later
//!    valid update emits a delta from the last successfully committed state.
//! 8. A one-row aggregate update has allocation cost independent of the number
//!    of rows already maintained by the aggregate.

use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;

use groove::db::{Database, Error, GraphBuilder};
use groove::ivm::{AggregateExpr, AggregateFunction, IvmRuntimeError, PlanExpr};
use groove::records::{Value, ValueType};
use groove::schema::{
    ColumnSchema, ColumnType, DatabaseSchema, IntegerKeyType, PrimaryKey, TableSchema,
};
use groove::storage::MemoryStorage;

struct CountingAllocator;

thread_local! {
    static ALLOCATION_COUNTING_ACTIVE: Cell<bool> = const { Cell::new(false) };
    static ALLOCATED_BYTES: Cell<u64> = const { Cell::new(0) };
}

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let _ = ALLOCATION_COUNTING_ACTIVE.try_with(|active| {
            if active.get() {
                let _ = ALLOCATED_BYTES.try_with(|bytes| {
                    bytes.set(bytes.get() + layout.size() as u64);
                });
            }
        });
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static GLOBAL: CountingAllocator = CountingAllocator;

fn start_counting_allocations() {
    ALLOCATED_BYTES.with(|bytes| bytes.set(0));
    ALLOCATION_COUNTING_ACTIVE.with(|active| active.set(true));
}

fn stop_counting_allocations() -> u64 {
    ALLOCATION_COUNTING_ACTIVE.with(|active| active.set(false));
    ALLOCATED_BYTES.with(Cell::get)
}

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

// Integer SUM overflow is currently a legitimate error. Accepted trade-off:
// SUM preserves the input column's integer type, so the result cannot exceed
// that type's representable range.
#[test]
fn aggregate_subscription_recovers_after_sum_overflow() {
    let storage = MemoryStorage::new(&["metrics"]);
    let mut database = Database::new(metric_schema(ColumnType::U8), storage).unwrap();
    let graph = GraphBuilder::aggregate(
        GraphBuilder::table("metrics"),
        ["bucket"],
        [aggregate(
            AggregateFunction::Sum,
            Some("score"),
            "sum_score",
        )],
    );
    let subscription = database.subscribe_one_sink(graph).unwrap();

    assert!(subscription.recv().unwrap().is_empty());

    let mut batch = database.open_batch();
    batch.insert(
        "metrics",
        vec![Value::U64(1), Value::U64(10), Value::U8(250)],
    );
    database.commit_batch(batch).unwrap();

    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        [(vec![Value::U64(10), some(Value::U8(250))], 1,)]
    );

    let mut batch = database.open_batch();
    batch.insert(
        "metrics",
        vec![Value::U64(2), Value::U64(10), Value::U8(10)],
    );

    assert!(matches!(
        database.commit_batch(batch),
        Err(Error::IvmRuntime(IvmRuntimeError::UnsupportedOperator))
    ));

    let mut batch = database.open_batch();
    batch.insert("metrics", vec![Value::U64(3), Value::U64(10), Value::U8(5)]);
    database.commit_batch(batch).unwrap();

    assert_eq!(
        subscription.recv().unwrap().to_values().unwrap(),
        [
            (vec![Value::U64(10), some(Value::U8(250))], -1,),
            (vec![Value::U64(10), some(Value::U8(255))], 1,),
        ]
    );
}

fn measure_single_row_aggregate_update(existing_rows: usize) -> u64 {
    let storage = MemoryStorage::new(&["metrics"]);
    let mut database = Database::new(metric_schema(ColumnType::U64), storage).unwrap();
    let mut batch = database.open_batch();
    for id in 0..existing_rows {
        batch.insert(
            "metrics",
            vec![Value::U64(id as u64), Value::U64(id as u64), Value::U64(1)],
        );
    }
    database.commit_batch(batch).unwrap();

    let graph = GraphBuilder::aggregate(
        GraphBuilder::table("metrics"),
        ["bucket"],
        [aggregate(
            AggregateFunction::Sum,
            Some("score"),
            "sum_score",
        )],
    );
    let subscription = database.subscribe_one_sink(graph).unwrap();
    subscription.recv().unwrap();

    start_counting_allocations();
    let mut batch = database.open_batch();
    batch.insert(
        "metrics",
        vec![
            Value::U64(existing_rows as u64),
            Value::U64(0),
            Value::U64(1),
        ],
    );
    database.commit_batch(batch).unwrap();
    subscription.recv().unwrap();
    stop_counting_allocations()
}

#[test]
fn aggregate_single_row_update_allocations_are_scale_independent() {
    let small = measure_single_row_aggregate_update(1_000);
    let large = measure_single_row_aggregate_update(20_000);
    let ratio = large as f64 / small.max(1) as f64;

    // Keep the same 3x noise allowance as the incremental-delivery canaries:
    // it tolerates allocator/runtime noise while catching full-state rebuilds.
    assert!(
        ratio <= 3.0,
        "one-row aggregate update allocation scaled with maintained state: \
         small={small}, large={large}, ratio={ratio:.2}"
    );
}
