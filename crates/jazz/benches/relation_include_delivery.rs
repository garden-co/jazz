use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::collections::BTreeMap;
use std::time::Instant;

mod schema_fixture;
mod support;

use jazz::block_on;
use jazz::db::{Db, DbConfig, DbIdentity, ReadOpts, SeededRowIdSource, SubscriptionEvent};
use jazz::groove::records::Value;
use jazz::groove::storage::MemoryStorage;
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{ArraySubquery, Query};
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
use serde_json::json;
use support::{csv_usizes, emit_json_line, env_usize, phase_fields};

struct CountingAllocator;

// The receipt runs each sample on the main thread. Keeping these counters
// thread-local prevents unrelated allocator activity from becoming part of a
// sample if the harness evolves to do setup concurrently.
thread_local! {
    static ACTIVE: Cell<bool> = const { Cell::new(false) };
    static ALLOCS: Cell<u64> = const { Cell::new(0) };
    static BYTES: Cell<u64> = const { Cell::new(0) };
}

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let _ = ACTIVE.try_with(|active| {
            if active.get() {
                let _ = ALLOCS.try_with(|allocs| allocs.set(allocs.get() + 1));
                let _ = BYTES.try_with(|bytes| bytes.set(bytes.get() + layout.size() as u64));
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

const DEFAULT_SCALES: &str = "1000,2500,5000,10000,20000";
const DEFAULT_SAMPLES: usize = 3;
// 2026-08-20 three-sample receipt: 1.001013x allocations and 1.026953x bytes.
// A 1.035x limit preserves about 0.8 percentage points above the larger
// observed drift, rather than inheriting the canary's deliberately loose 3x
// band. The 10k-rung byte bump is identical on the integration base.
const DEFAULT_MAX_RATIO: f64 = 1.035;

fn main() {
    jazz_benchmark_guard::refuse_contaminated_measurement();
    let scales = csv_usizes("JAZZ_INC_DELIVERY_SCALES", DEFAULT_SCALES);
    assert!(
        scales.len() >= 3,
        "JAZZ_INC_DELIVERY_SCALES needs at least three rungs to establish a curve"
    );
    let samples = env_usize("JAZZ_INC_DELIVERY_SAMPLES", DEFAULT_SAMPLES).max(1);
    let max_ratio = std::env::var("JAZZ_INC_DELIVERY_MAX_RATIO")
        .ok()
        .and_then(|value| value.parse::<f64>().ok())
        .unwrap_or(DEFAULT_MAX_RATIO);

    let mut rungs = Vec::with_capacity(scales.len());
    for scale in scales {
        let measurements = (0..samples)
            .map(|sample| measure_single_child_insert(scale, sample))
            .collect::<Vec<_>>();
        let summary = summarize_rung(&measurements);
        emit_rung(scale, samples, summary, &measurements);
        rungs.push((scale, summary));
    }

    emit_slope(samples, max_ratio, &rungs);
}

#[allow(dead_code)]
pub(crate) fn correctness_smoke() {
    // Exercise the delivery-shape assertions without treating an allocator
    // ratio measured on this host as a correctness signal.
    let _ = measure_single_child_insert(3, 0);
}

#[derive(Clone, Copy, Debug)]
struct Measurement {
    allocs: u64,
    bytes: u64,
    wall_us: u128,
    delivery: DeliveryShape,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct DeliveryShape {
    added: usize,
    updated: usize,
    removed: usize,
    terminal_operations: usize,
}

#[derive(Clone, Copy, Debug)]
struct RungSummary {
    median_allocs: u64,
    median_alloc_bytes: u64,
    median_wall_us: u128,
    delivery: DeliveryShape,
}

fn summarize_rung(samples: &[Measurement]) -> RungSummary {
    let delivery = samples.first().expect("sample").delivery;
    assert!(
        samples
            .iter()
            .all(|measurement| measurement.delivery == delivery),
        "one-row insert delivered different delta shapes across samples"
    );

    RungSummary {
        median_allocs: median(samples.iter().map(|measurement| measurement.allocs)),
        median_alloc_bytes: median(samples.iter().map(|measurement| measurement.bytes)),
        median_wall_us: median(samples.iter().map(|measurement| measurement.wall_us)),
        delivery,
    }
}

fn median<T: Ord>(values: impl Iterator<Item = T>) -> T {
    let mut values = values.collect::<Vec<_>>();
    values.sort_unstable();
    let median_index = values.len() / 2;
    values
        .into_iter()
        .nth(median_index)
        .expect("at least one sample")
}

fn emit_rung(scale: usize, samples: usize, summary: RungSummary, all: &[Measurement]) {
    let mut fields = phase_fields("rung", summary.median_wall_us);
    fields.insert("accumulated_children".to_owned(), json!(scale));
    fields.insert("change_rows".to_owned(), json!(1));
    fields.insert("samples".to_owned(), json!(samples));
    fields.insert("median_allocs".to_owned(), json!(summary.median_allocs));
    fields.insert(
        "median_alloc_bytes".to_owned(),
        json!(summary.median_alloc_bytes),
    );
    fields.insert("median_wall_us".to_owned(), json!(summary.median_wall_us));
    fields.insert(
        "allocs_min".to_owned(),
        json!(all.iter().map(|m| m.allocs).min()),
    );
    fields.insert(
        "allocs_max".to_owned(),
        json!(all.iter().map(|m| m.allocs).max()),
    );
    fields.insert(
        "alloc_bytes_min".to_owned(),
        json!(all.iter().map(|m| m.bytes).min()),
    );
    fields.insert(
        "alloc_bytes_max".to_owned(),
        json!(all.iter().map(|m| m.bytes).max()),
    );
    fields.insert("delivered_added".to_owned(), json!(summary.delivery.added));
    fields.insert(
        "delivered_updated".to_owned(),
        json!(summary.delivery.updated),
    );
    fields.insert(
        "delivered_removed".to_owned(),
        json!(summary.delivery.removed),
    );
    fields.insert(
        "delivered_terminal_operations".to_owned(),
        json!(summary.delivery.terminal_operations),
    );
    emit_json_line("relation_include_delivery", fields);
}

fn emit_slope(samples: usize, max_ratio: f64, rungs: &[(usize, RungSummary)]) {
    let allocs = rungs
        .iter()
        .map(|(_, summary)| summary.median_allocs)
        .collect::<Vec<_>>();
    let bytes = rungs
        .iter()
        .map(|(_, summary)| summary.median_alloc_bytes)
        .collect::<Vec<_>>();
    let alloc_ratio = ratio(&allocs);
    let byte_ratio = ratio(&bytes);
    let flat = alloc_ratio <= max_ratio && byte_ratio <= max_ratio;

    let mut fields = phase_fields("slope", 0);
    fields.insert("rungs".to_owned(), json!(rungs.len()));
    fields.insert("samples_per_rung".to_owned(), json!(samples));
    fields.insert("change_rows".to_owned(), json!(1));
    fields.insert("alloc_ratio_max_to_min".to_owned(), json!(alloc_ratio));
    fields.insert("alloc_bytes_ratio_max_to_min".to_owned(), json!(byte_ratio));
    fields.insert(
        "allocs_per_accumulated_child_slope".to_owned(),
        json!(least_squares_slope(rungs, |summary| summary.median_allocs as f64)),
    );
    fields.insert(
        "alloc_bytes_per_accumulated_child_slope".to_owned(),
        json!(least_squares_slope(rungs, |summary| {
            summary.median_alloc_bytes as f64
        })),
    );
    fields.insert("max_ratio_rule".to_owned(), json!(max_ratio));
    fields.insert(
        "max_ratio_rule_source".to_owned(),
        json!("2026-08-20 base/head three-sample byte ratio 1.026953 + 0.008047 margin"),
    );
    fields.insert("flat_by_ratio_rule".to_owned(), json!(flat));
    fields.insert(
        "flat_rule".to_owned(),
        json!("max(per-metric median work) / min(per-metric median work) <= max_ratio_rule for both allocations and allocation bytes"),
    );
    emit_json_line("relation_include_delivery", fields);

    assert!(
        flat,
        "INV-INC-1 receipt failed: allocation ratio {alloc_ratio:.3}, byte ratio {byte_ratio:.3}, maximum {max_ratio:.3}"
    );
}

fn ratio(values: &[u64]) -> f64 {
    let min = *values.iter().min().expect("at least one rung");
    let max = *values.iter().max().expect("at least one rung");
    max as f64 / min.max(1) as f64
}

fn least_squares_slope(rungs: &[(usize, RungSummary)], value: impl Fn(RungSummary) -> f64) -> f64 {
    let count = rungs.len() as f64;
    let mean_x = rungs.iter().map(|(scale, _)| *scale as f64).sum::<f64>() / count;
    let mean_y = rungs
        .iter()
        .map(|(_, measurement)| value(*measurement))
        .sum::<f64>()
        / count;
    let numerator = rungs
        .iter()
        .map(|(scale, measurement)| (*scale as f64 - mean_x) * (value(*measurement) - mean_y))
        .sum::<f64>();
    let denominator = rungs
        .iter()
        .map(|(scale, _)| (*scale as f64 - mean_x).powi(2))
        .sum::<f64>();
    numerator / denominator
}

fn reset_alloc_counter() {
    ALLOCS.with(|allocs| allocs.set(0));
    BYTES.with(|bytes| bytes.set(0));
    ACTIVE.with(|active| active.set(true));
}

fn stop_alloc_counter() -> (u64, u64) {
    ACTIVE.with(|active| active.set(false));
    (ALLOCS.with(Cell::get), BYTES.with(Cell::get))
}

fn relation_schema() -> JazzSchema {
    schema_fixture::compile(
        SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("parents")
                    .column("label", ColumnType::Text)
                    .column("ordinal", ColumnType::Integer),
            )
            .table(
                TableSchemaBuilder::new("children")
                    .fk_column("parent_id", "parents")
                    .column("label", ColumnType::Text)
                    .column("ordinal", ColumnType::Integer),
            ),
    )
}

fn open_db(scale: usize, sample: usize) -> Db<MemoryStorage> {
    let schema = relation_schema();
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    block_on(Db::open(
        DbConfig::new(
            schema,
            MemoryStorage::new(&refs),
            DbIdentity {
                node: NodeUuid::from_bytes([(scale as u8).wrapping_add(sample as u8); 16]),
                author: AuthorSubject::for_test_bytes([0xa1; 16]),
            },
        )
        .with_id_source(SeededRowIdSource::new((scale + sample) as u64 + 1)),
    ))
    .expect("open receipt database")
}

fn relation_query() -> Query {
    Query::from("parents").array_subquery(
        ArraySubquery::new("children", "children", "parent_id", "id").select(["label", "ordinal"]),
    )
}

fn measure_single_child_insert(scale: usize, sample: usize) -> Measurement {
    let db = open_db(scale, sample);
    let parent = seed_relation_fixture(&db, scale);
    let prepared = db.prepare_query(&relation_query()).expect("prepare query");
    let mut stream = block_on(db.subscribe(&prepared, ReadOpts::default())).expect("subscribe");
    expect_initial_snapshot(
        block_on(stream.next_event()).expect("initial relation hydration"),
        parent,
    );

    reset_alloc_counter();
    let start = Instant::now();
    block_on(db.insert(
        "children",
        BTreeMap::from([
            ("parent_id".to_owned(), Value::Uuid(parent.0)),
            (
                "label".to_owned(),
                Value::String("measured-child".to_owned()),
            ),
            ("ordinal".to_owned(), Value::I32(1)),
        ]),
        jazz::db::InsertOptions {
            row_id: Some(row(10_000_000)),
            ..Default::default()
        },
    ))
    .expect("insert exactly one measured child");
    let event = block_on(stream.next_event()).expect("measured relation update");
    let wall_us = start.elapsed().as_micros();
    let (allocs, bytes) = stop_alloc_counter();
    let counts = expect_single_child_delta(event, parent);

    Measurement {
        allocs,
        bytes,
        wall_us,
        delivery: counts,
    }
}

fn seed_relation_fixture(db: &Db<MemoryStorage>, child_rows: usize) -> RowUuid {
    let parent = row(1);
    block_on(db.insert(
        "parents",
        BTreeMap::from([
            (
                "label".to_owned(),
                Value::String("receipt-parent".to_owned()),
            ),
            ("ordinal".to_owned(), Value::I32(0)),
        ]),
        jazz::db::InsertOptions {
            row_id: Some(parent),
            ..Default::default()
        },
    ))
    .expect("insert parent");
    for index in 0..child_rows {
        block_on(db.insert(
            "children",
            BTreeMap::from([
                ("parent_id".to_owned(), Value::Uuid(parent.0)),
                ("label".to_owned(), Value::String(format!("child-{index}"))),
                ("ordinal".to_owned(), Value::I32(index as i32)),
            ]),
            jazz::db::InsertOptions {
                row_id: Some(row(1_000 + index as u64)),
                ..Default::default()
            },
        ))
        .unwrap_or_else(|error| panic!("seed child {index}: {error}"));
    }
    parent
}

fn expect_initial_snapshot(event: SubscriptionEvent, parent: RowUuid) {
    match event {
        SubscriptionEvent::Delta { reset, added, .. } => assert!(
            reset && added.iter().any(|row| row.row_uuid() == parent),
            "initial terminal hydration did not contain the parent"
        ),
        other => panic!("expected initial relation delta, got {other:?}"),
    }
}

fn expect_single_child_delta(event: SubscriptionEvent, parent: RowUuid) -> DeliveryShape {
    match event {
        SubscriptionEvent::Delta {
            reset,
            added,
            updated,
            removed,
            terminal_operations,
            ..
        } => {
            assert!(!reset, "structured child changes must remain incremental");
            assert!(added.is_empty(), "an existing terminal root is not added");
            assert!(
                updated.is_empty(),
                "a child patch must not rebuild its terminal root"
            );
            assert!(
                removed.is_empty(),
                "an existing terminal root is not removed"
            );
            assert_eq!(
                terminal_operations.len(),
                1,
                "exactly one terminal root is patched"
            );
            let expected_root_key = [10]
                .into_iter()
                .chain(parent.0.as_bytes().iter().copied())
                .collect::<Vec<_>>();
            assert!(
                terminal_operations
                    .iter()
                    .all(|operation| operation.root_key == expected_root_key),
                "one child insert did not patch the expected terminal root: {terminal_operations:?}"
            );
            DeliveryShape {
                added: added.len(),
                updated: updated.len(),
                removed: removed.len(),
                terminal_operations: terminal_operations.len(),
            }
        }
        other => panic!("expected measured relation delta, got {other:?}"),
    }
}

fn row(seed: u64) -> RowUuid {
    let mut bytes = [0_u8; 16];
    bytes[..8].copy_from_slice(&0x019e_0000_0000_7000_u64.to_be_bytes());
    bytes[8..].copy_from_slice(&seed.to_be_bytes());
    RowUuid::from_bytes(bytes)
}
