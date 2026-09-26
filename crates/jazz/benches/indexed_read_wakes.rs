//! Native receipt for indexed hydration under unrelated executor wakes.
//! This phase benchmark does not model browser I/O latency or app startup.
use futures::{executor::block_on, task::noop_waker};
use groove::{
    db::{Database, GraphBuilder},
    ivm::{LiteralValue, ProjectField, StaticScanSpec},
    records::{RecordDescriptor, Value, ValueType, VariantRecord},
    schema::{
        ColumnSchema, ColumnType, DatabaseSchema, IndexSchema, IntegerKeyType, PrimaryKey,
        TableSchema,
    },
    storage::{TestStorage, TestStorageOperation},
};
use std::{future::Future, task::Context, time::Instant};

fn run(rows: u64, pending: bool, pass: usize) {
    let setup_started = Instant::now();
    let schema = DatabaseSchema::new([TableSchema::new(
        "items",
        [
            ColumnSchema::new("id", ColumnType::U64),
            ColumnSchema::new("group", ColumnType::String),
        ],
    )
    .with_primary_key(PrimaryKey::new("id", IntegerKeyType::U64))
    .with_index(IndexSchema::new("items_by_group", ["group"]))
    .with_variant(1, ["group", "id"])]);
    let (storage, control) = TestStorage::controlled(&["items", "indices"]);
    let mut database = block_on(Database::new(schema, storage.clone())).unwrap();
    let physical = RecordDescriptor::new([("group", ValueType::String), ("id", ValueType::U64)]);
    database
        .define_variant_projection(
            "items",
            "logical-item",
            RecordDescriptor::new([("id", ValueType::U64), ("group", ValueType::String)]),
        )
        .unwrap();
    database
        .register_variant_projection_case(
            "items",
            "logical-item",
            1,
            [ProjectField::named("id"), ProjectField::named("group")],
        )
        .unwrap();
    let mut batch = database.open_batch();
    for id in 0..rows {
        batch.insert(
            "items",
            VariantRecord::create(
                1,
                physical,
                &[Value::String("shared".into()), Value::U64(id)],
            )
            .unwrap(),
        );
    }
    block_on(database.commit_batch(batch)).unwrap();
    storage.evict_all();
    control.take_observed();
    if pending {
        control.pause_on(TestStorageOperation::Get);
    }
    let setup_ms = setup_started.elapsed().as_secs_f64() * 1000.;
    let initial_started = Instant::now();
    let before = control.poll_count(TestStorageOperation::Get);
    let subscription = block_on(
        database.subscribe_one_sink(GraphBuilder::variant_index_scan(
            "items",
            "items_by_group",
            "logical-item",
            StaticScanSpec::Prefix(vec![LiteralValue::String("shared".into())]),
        )),
    )
    .unwrap();
    let mut progress = Box::pin(database.drive_progress());
    let waker = noop_waker();
    let mut cx = Context::from_waker(&waker);
    let mut unrelated_ms = 0.;
    let mut extra_polls = 0;
    if pending {
        for _ in 0..(rows * 2 + 32) {
            assert!(progress.as_mut().poll(&mut cx).is_pending());
            if control.poll_count(TestStorageOperation::Get) >= before + 2 * rows as usize {
                break;
            }
        }
        assert_eq!(
            control
                .observed()
                .iter()
                .filter(|operation| **operation == TestStorageOperation::Get)
                .count(),
            rows as usize,
            "all reads must be parked before measurement"
        );
        for _ in 0..2 {
            assert!(progress.as_mut().poll(&mut cx).is_pending());
        }
        let parked = control.poll_count(TestStorageOperation::Get);
        let noise_started = Instant::now();
        for _ in 0..rows {
            assert!(progress.as_mut().poll(&mut cx).is_pending());
        }
        unrelated_ms = noise_started.elapsed().as_secs_f64() * 1000.;
        extra_polls = control.poll_count(TestStorageOperation::Get) - parked;
        control.resume_operation(TestStorageOperation::Get);
    }
    block_on(progress).unwrap();
    let complete_ms = initial_started.elapsed().as_secs_f64() * 1000.;
    let returned = subscription.recv().unwrap().deltas.len();
    println!(
        "{{\"benchmark\":\"indexed_read_wakes\",\"rows\":{rows},\"pending\":{pending},\"pass\":{pass},\"setup_ms\":{setup_ms:.3},\"complete_ms\":{complete_ms:.3},\"unrelated_poll_ms\":{unrelated_ms:.3},\"extra_point_polls\":{extra_polls},\"returned_rows\":{returned}}}"
    );
    assert_eq!(returned, rows as usize);
}

fn main() {
    jazz_benchmark_guard::refuse_contaminated_measurement();
    for rows in [100, 1000] {
        for pending in [false, true] {
            for pass in 0..3 {
                run(rows, pending, pass);
            }
        }
    }
}
