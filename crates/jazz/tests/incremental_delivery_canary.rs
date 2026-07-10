use std::alloc::{GlobalAlloc, Layout, System};
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use jazz::block_on;
use jazz::db::{Db, DbConfig, DbIdentity, ReadOpts, SeededRowIdSource, SubscriptionEvent};
use jazz::groove::records::Value;
use jazz::groove::schema::{ColumnSchema, ColumnType};
use jazz::groove::storage::{MemoryStorage, RocksDbStorage};
use jazz::ids::{AuthorId, NodeUuid, RowUuid};
use jazz::query::{ArraySubquery, Query};
use jazz::schema::{JazzSchema, Policy, TableSchema};

struct CountingAllocator;

static ACTIVE: AtomicBool = AtomicBool::new(false);
static ALLOCS: AtomicU64 = AtomicU64::new(0);
static BYTES: AtomicU64 = AtomicU64::new(0);

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if ACTIVE.load(Ordering::Relaxed) {
            ALLOCS.fetch_add(1, Ordering::Relaxed);
            BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        }
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        unsafe { System.dealloc(ptr, layout) }
    }
}

#[global_allocator]
static GLOBAL: CountingAllocator = CountingAllocator;

#[derive(Debug, Clone, Copy)]
struct AllocSnapshot {
    allocs: u64,
    bytes: u64,
}

fn reset_alloc_counter() {
    ALLOCS.store(0, Ordering::Relaxed);
    BYTES.store(0, Ordering::Relaxed);
    ACTIVE.store(true, Ordering::Relaxed);
}

fn stop_alloc_counter() -> AllocSnapshot {
    ACTIVE.store(false, Ordering::Relaxed);
    AllocSnapshot {
        allocs: ALLOCS.load(Ordering::Relaxed),
        bytes: BYTES.load(Ordering::Relaxed),
    }
}

fn relation_schema() -> JazzSchema {
    JazzSchema::new([
        TableSchema::new(
            "parents",
            [
                ColumnSchema::new("label", ColumnType::String),
                ColumnSchema::new("ordinal", ColumnType::U32),
            ],
        )
        .with_read_policy(Policy::public())
        .with_write_policy(Policy::public()),
        TableSchema::new(
            "children",
            [
                ColumnSchema::new("parent_id", ColumnType::Uuid),
                ColumnSchema::new("label", ColumnType::String),
                ColumnSchema::new("ordinal", ColumnType::U32),
            ],
        )
        .with_reference("parent_id", "parents")
        .with_read_policy(Policy::public())
        .with_write_policy(Policy::public()),
    ])
}

fn write_schema() -> JazzSchema {
    JazzSchema::new([
        TableSchema::new(
            "parents",
            [
                ColumnSchema::new("label", ColumnType::String),
                ColumnSchema::new("ordinal", ColumnType::U32),
            ],
        )
        .with_read_policy(Policy::public())
        .with_write_policy(Policy::public()),
        TableSchema::new(
            "children",
            [
                ColumnSchema::new("parent_id", ColumnType::Uuid),
                ColumnSchema::new("label", ColumnType::String),
                ColumnSchema::new("ordinal", ColumnType::U32),
            ],
        )
        .with_reference("parent_id", "parents")
        .with_read_policy(Policy::public())
        .with_write_policy(Policy::public()),
    ])
}

fn open_db(scale: usize) -> Db<MemoryStorage> {
    open_db_with_schema(scale, relation_schema())
}

fn open_db_with_schema(scale: usize, schema: JazzSchema) -> Db<MemoryStorage> {
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    block_on(Db::open(
        DbConfig::new(
            schema,
            MemoryStorage::new(&refs),
            DbIdentity {
                node: NodeUuid::from_bytes([scale as u8; 16]),
                author: AuthorId::from_bytes([0xa1; 16]),
            },
        )
        .with_id_source(SeededRowIdSource::new(scale as u64 + 1)),
    ))
    .expect("open canary db")
}

fn open_rocks_db_with_schema(
    scale: usize,
    schema: JazzSchema,
) -> (tempfile::TempDir, Db<RocksDbStorage>) {
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    let dir = tempfile::tempdir().expect("temp rocks dir");
    let storage = RocksDbStorage::open(dir.path(), &refs).expect("open rocks canary storage");
    let db = block_on(Db::open(
        DbConfig::new(
            schema,
            storage,
            DbIdentity {
                node: NodeUuid::from_bytes([scale as u8; 16]),
                author: AuthorId::from_bytes([0xa1; 16]),
            },
        )
        .with_id_source(SeededRowIdSource::new(scale as u64 + 1)),
    ))
    .expect("open rocks canary db");
    (dir, db)
}

fn row(seed: u64) -> RowUuid {
    let mut bytes = [0u8; 16];
    bytes[..8].copy_from_slice(&0x019e_0000_0000_7000u64.to_be_bytes());
    bytes[8..].copy_from_slice(&seed.to_be_bytes());
    RowUuid::from_bytes(bytes)
}

fn relation_query() -> Query {
    Query::from("parents").array_subquery(
        ArraySubquery::new("children", "children", "parent_id", "id").select(["label", "ordinal"]),
    )
}

fn seed_relation_fixture(db: &Db<MemoryStorage>, child_rows: usize) -> RowUuid {
    let parent = row(1);
    db.insert_with_id(
        "parents",
        parent,
        BTreeMap::from([
            (
                "label".to_owned(),
                Value::String("canary-parent".to_owned()),
            ),
            ("ordinal".to_owned(), Value::U32(0)),
        ]),
    )
    .expect("insert parent");

    for index in 0..child_rows {
        db.insert_with_id(
            "children",
            row(1_000 + index as u64),
            BTreeMap::from([
                ("parent_id".to_owned(), Value::Uuid(parent.0)),
                ("label".to_owned(), Value::String(format!("child-{index}"))),
                ("ordinal".to_owned(), Value::U32(index as u32)),
            ]),
        )
        .unwrap_or_else(|err| panic!("insert child {index}: {err}"));
    }

    parent
}

fn expect_parent_snapshot(event: SubscriptionEvent, parent: RowUuid, label: &str) {
    match event {
        SubscriptionEvent::Delta {
            added,
            added_related,
            added_edges,
            reset,
            ..
        } => {
            assert!(
                added.iter().any(|row| row.row_uuid() == parent)
                    || added_related.iter().any(|row| row.row_uuid() == parent)
                    || (!reset && !added_edges.is_empty()),
                "{label}: relation delta did not include parent state or edge additions"
            );
        }
        other => panic!("{label}: expected relation event, got {other:?}"),
    }
}

fn measure_single_child_insert(scale: usize) -> AllocSnapshot {
    let db = open_db(scale);
    let parent = seed_relation_fixture(&db, scale);
    let prepared = db
        .prepare_query(&relation_query())
        .expect("prepare relation query");
    let mut stream = block_on(db.subscribe(&prepared, ReadOpts::default()))
        .expect("subscribe relation include query");

    expect_parent_snapshot(
        block_on(stream.next_event()).expect("initial relation hydration"),
        parent,
        "initial hydration",
    );

    reset_alloc_counter();
    db.insert_with_id(
        "children",
        row(10_000_000 + scale as u64),
        BTreeMap::from([
            ("parent_id".to_owned(), Value::Uuid(parent.0)),
            (
                "label".to_owned(),
                Value::String(format!("measured-child-{scale}")),
            ),
            ("ordinal".to_owned(), Value::U32((scale + 1) as u32)),
        ]),
    )
    .expect("insert measured child");
    expect_parent_snapshot(
        block_on(stream.next_event()).expect("measured relation update"),
        parent,
        "measured update",
    );
    stop_alloc_counter()
}

#[test]
fn maintained_relation_include_single_row_changes_are_scale_independent() {
    let small = measure_single_child_insert(1_000);
    let large = measure_single_child_insert(20_000);

    // This canary is intentionally about mechanism, not observable correctness.
    // A 20x larger accumulated include relation receiving the same one-row
    // child insert should stay in the same constant band. The 3x factor allows
    // allocator/runtime noise while still catching full-state rebuild+diff work.
    let alloc_ratio = large.allocs as f64 / small.allocs.max(1) as f64;
    let byte_ratio = large.bytes as f64 / small.bytes.max(1) as f64;
    assert!(
        alloc_ratio <= 3.0 && byte_ratio <= 3.0,
        "INV-INC-1 violation: per-change relation/include allocation scaled with accumulated state: \
         small={small:?}, large={large:?}, alloc_ratio={alloc_ratio:.2}, byte_ratio={byte_ratio:.2}"
    );
}

#[derive(Debug, Clone, Copy)]
struct TxMeasurement {
    elapsed: Duration,
    allocs: u64,
    bytes: u64,
}

fn write_cells(parent: RowUuid, index: usize) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("parent_id".to_owned(), Value::Uuid(parent.0)),
        (
            "label".to_owned(),
            Value::String(format!("write-child-{index}")),
        ),
        ("ordinal".to_owned(), Value::U32(index as u32)),
    ])
}

fn seed_rocks_write_fixture(db: &Db<RocksDbStorage>, child_rows: usize) -> RowUuid {
    let parent = row(50_000_000);
    db.insert_with_id(
        "parents",
        parent,
        BTreeMap::from([
            ("label".to_owned(), Value::String("write-parent".to_owned())),
            ("ordinal".to_owned(), Value::U32(0)),
        ]),
    )
    .expect("insert write parent");

    let mut next = 0usize;
    while next < child_rows {
        let start = next;
        let end = (start + 200).min(child_rows);
        db.transaction(|tx| {
            for index in start..end {
                tx.insert_with_id(
                    "children",
                    row(60_000_000 + index as u64),
                    write_cells(parent, index),
                )?;
            }
            Ok(())
        })
        .unwrap_or_else(|err| panic!("seed rocks write tx {start}..{end}: {err}"));
        next = end;
    }
    parent
}

fn measure_rocks_write_transaction(existing_rows: usize) -> TxMeasurement {
    let (_dir, db) = open_rocks_db_with_schema(existing_rows + 10, write_schema());
    let parent = seed_rocks_write_fixture(&db, existing_rows);
    let start_index = 70_000_000 + existing_rows;

    reset_alloc_counter();
    let started = Instant::now();
    db.transaction(|tx| {
        for offset in 0..200 {
            let index = start_index + offset;
            tx.update("children", row(index as u64), write_cells(parent, index))?;
        }
        Ok(())
    })
    .expect("measured rocks write transaction");
    let elapsed = started.elapsed();
    let allocs = stop_alloc_counter();
    TxMeasurement {
        elapsed,
        allocs: allocs.allocs,
        bytes: allocs.bytes,
    }
}

#[test]
fn mergeable_transaction_write_cost_is_scale_independent() {
    let small = measure_rocks_write_transaction(1_000);
    let large = measure_rocks_write_transaction(20_000);
    let time_ratio = large.elapsed.as_secs_f64() / small.elapsed.as_secs_f64().max(0.000_001);
    let alloc_ratio = large.allocs as f64 / small.allocs.max(1) as f64;
    let byte_ratio = large.bytes as f64 / small.bytes.max(1) as f64;
    eprintln!(
        "write canary small={small:?} large={large:?} time_ratio={time_ratio:.2} alloc_ratio={alloc_ratio:.2} byte_ratio={byte_ratio:.2}"
    );
    assert!(
        time_ratio <= 3.0 && alloc_ratio <= 3.0 && byte_ratio <= 3.0,
        "write-path ingest cost scaled with accumulated rows: \
         small={small:?}, large={large:?}, time_ratio={time_ratio:.2}, \
         alloc_ratio={alloc_ratio:.2}, byte_ratio={byte_ratio:.2}"
    );
}
