use std::alloc::{GlobalAlloc, Layout, System};
use std::cell::Cell;
use std::collections::BTreeMap;
use std::time::{Duration, Instant};

mod common;

use jazz_testkit::duplex_transport;

use jazz::block_on;
use jazz::db::{
    Db, DbConfig, DbIdentity, LocalUpdates, MergeableTxOps, Propagation, ReadOpts,
    SeededRowIdSource, SubscriptionEvent,
};
use jazz::groove::records::Value;
use jazz::groove::storage::TestStorage;
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{ArraySubquery, Query};
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
use jazz::tx::DurabilityTier;
use jazz_storage_rocksdb::RocksDbStorage;

use common::{allow_all_policies, compile_schema};

struct CountingAllocator;

// Counters are thread-local so concurrently running tests in this binary
// cannot pollute each other's measurement windows; the test harness runs
// each test on its own thread. try_with guards against TLS teardown.
thread_local! {
    static T_ACTIVE: Cell<bool> = const { Cell::new(false) };
    static T_ALLOCS: Cell<u64> = const { Cell::new(0) };
    static T_BYTES: Cell<u64> = const { Cell::new(0) };
}

unsafe impl GlobalAlloc for CountingAllocator {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        let _ = T_ACTIVE.try_with(|active| {
            if active.get() {
                let _ = T_ALLOCS.try_with(|allocs| allocs.set(allocs.get() + 1));
                let _ = T_BYTES.try_with(|bytes| bytes.set(bytes.get() + layout.size() as u64));
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

#[derive(Debug, Clone, Copy)]
struct AllocSnapshot {
    allocs: u64,
    bytes: u64,
}

fn reset_alloc_counter() {
    T_ALLOCS.with(|allocs| allocs.set(0));
    T_BYTES.with(|bytes| bytes.set(0));
    T_ACTIVE.with(|active| active.set(true));
}

fn stop_alloc_counter() -> AllocSnapshot {
    T_ACTIVE.with(|active| active.set(false));
    AllocSnapshot {
        allocs: T_ALLOCS.with(Cell::get),
        bytes: T_BYTES.with(Cell::get),
    }
}

fn relation_schema() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("parents")
                    .column("label", ColumnType::Text)
                    .column("ordinal", ColumnType::Integer)
                    .policies(allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new("children")
                    .fk_column("parent_id", "parents")
                    .column("label", ColumnType::Text)
                    .column("ordinal", ColumnType::Integer)
                    .policies(allow_all_policies()),
            )
            .build(),
    )
}

fn write_schema() -> JazzSchema {
    relation_schema()
}

fn reset_batch_schema() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("items")
                    .column("label", ColumnType::Text)
                    .column("ordinal", ColumnType::Integer)
                    .policies(allow_all_policies()),
            )
            .build(),
    )
}

fn global_read_opts() -> ReadOpts {
    ReadOpts {
        tier: DurabilityTier::Global,
        local_updates: LocalUpdates::Deferred,
        propagation: Propagation::Full,
        include_deleted: false,
        ..ReadOpts::default()
    }
}

use duplex_transport::duplex;

fn open_db(scale: usize) -> Db<TestStorage> {
    open_db_with_schema(scale, relation_schema())
}

fn open_db_with_schema(scale: usize, schema: JazzSchema) -> Db<TestStorage> {
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    block_on(Db::open(
        DbConfig::new(
            schema,
            TestStorage::new(&refs),
            DbIdentity {
                node: NodeUuid::from_bytes([scale as u8; 16]),
                author: AuthorSubject::for_test_bytes([0xa1; 16]),
            },
        )
        .with_id_source(SeededRowIdSource::new(scale as u64 + 1)),
    ))
    .expect("open canary db")
}

fn open_history_complete_db_with_schema(scale: usize, schema: JazzSchema) -> Db<TestStorage> {
    let cfs = schema.column_families();
    let refs = cfs.iter().map(String::as_str).collect::<Vec<_>>();
    block_on(Db::open_history_complete(
        DbConfig::new(
            schema,
            TestStorage::new(&refs),
            DbIdentity {
                node: NodeUuid::from_bytes([(scale as u8).wrapping_add(0x40); 16]),
                author: AuthorSubject::SYSTEM,
            },
        )
        .with_id_source(SeededRowIdSource::new(scale as u64 + 10_000)),
    ))
    .expect("open history-complete canary db")
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
                author: AuthorSubject::for_test_bytes([0xa1; 16]),
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

fn seed_relation_fixture(db: &Db<TestStorage>, child_rows: usize) -> RowUuid {
    let parent = row(1);
    block_on(db.insert(
        "parents",
        BTreeMap::from([
            (
                "label".to_owned(),
                Value::String("canary-parent".to_owned()),
            ),
            ("ordinal".to_owned(), Value::I32(0)),
        ]),
        jazz::db::InsertOptions {
            row_id: Some(parent),
            ..Default::default()
        },
    ))
    .expect("insert parent");

    // The canary measures a change against a large *current relation*, not
    // admission work for 20,000 independent historical transactions. Seed in
    // bounded ordinary mergeable transactions so fixture construction does not
    // consume the CI watchdog before the maintained-view path is exercised.
    let mut next = 0usize;
    while next < child_rows {
        let start = next;
        let end = (start + 500).min(child_rows);
        block_on(db.transaction(async |tx| {
            for index in start..end {
                tx.insert(
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
                )
                .await?;
            }
            Ok(())
        }))
        .unwrap_or_else(|err| panic!("insert children {start}..{end}: {err}"));
        next = end;
    }

    parent
}

fn seed_reset_batch_fixture(db: &Db<TestStorage>, rows: usize) {
    for index in 0..rows {
        db.seed_settled_mergeable_for_bootstrap(
            "items",
            row(30_000_000 + index as u64),
            AuthorSubject::SYSTEM,
            BTreeMap::from([
                ("label".to_owned(), Value::String(format!("item-{index}"))),
                ("ordinal".to_owned(), Value::I32(index as i32)),
            ]),
        )
        .unwrap_or_else(|err| panic!("seed reset-batch item {index}: {err}"));
    }
}

fn drive_until_covered(
    server: &Db<TestStorage>,
    client: &Db<TestStorage>,
    attachment: &jazz::db::QueryAttachment,
) {
    for _ in 0..100 {
        block_on(client.tick()).expect("tick client");
        block_on(server.tick()).expect("tick server");
        block_on(client.tick()).expect("tick client after server");
        if client.query_attachment_is_covered(attachment) {
            return;
        }
    }
    panic!("timed out waiting for query coverage");
}

fn drain_until_idle(server: &Db<TestStorage>, client: &Db<TestStorage>) {
    for _ in 0..1_000 {
        let client_before = block_on(client.tick_stats()).expect("drain client");
        let server_stats = block_on(server.tick_stats()).expect("drain server");
        let client_after = block_on(client.tick_stats()).expect("drain client after server");
        if client_before.remote_sync_applied == 0
            && server_stats.remote_sync_applied == 0
            && client_after.remote_sync_applied == 0
        {
            return;
        }
    }
    panic!("timed out draining reset-batch sync work");
}

fn expect_parent_snapshot(event: SubscriptionEvent, parent: RowUuid, label: &str) {
    match event {
        SubscriptionEvent::Delta {
            reset,
            added,
            updated,
            terminal_operations,
            ..
        } => {
            if label == "measured update" {
                assert!(!reset, "{label}: structured update must remain incremental");
            }
            let patched_parent = terminal_operations.iter().any(|operation| {
                operation.root_key.as_slice()
                    == [10]
                        .into_iter()
                        .chain(parent.0.as_bytes().iter().copied())
                        .collect::<Vec<_>>()
            });
            assert!(
                added.iter().any(|row| row.row_uuid() == parent)
                    || updated.iter().any(|row| row.row_uuid() == parent)
                    || patched_parent,
                "{label}: terminal delta did not address parent state: {terminal_operations:?}"
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
    block_on(db.insert(
        "children",
        BTreeMap::from([
            ("parent_id".to_owned(), Value::Uuid(parent.0)),
            (
                "label".to_owned(),
                Value::String(format!("measured-child-{scale}")),
            ),
            ("ordinal".to_owned(), Value::I32((scale + 1) as i32)),
        ]),
        jazz::db::InsertOptions {
            row_id: Some(row(10_000_000 + scale as u64)),
            ..Default::default()
        },
    ))
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
    // Preserve the 20x scale gap that exposes accumulated-state work while
    // keeping initial hydration below the suite watchdog. The only measured
    // operation is the one-row update below, so a larger fixture merely made
    // the canary's unmeasured setup compete with unrelated CI work.
    let small = measure_single_child_insert(250);
    let large = measure_single_child_insert(5_000);

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

fn measure_post_reset_single_insert(existing_rows: usize) -> AllocSnapshot {
    let schema = reset_batch_schema();
    let server = open_history_complete_db_with_schema(existing_rows, schema.clone());
    let client = open_db_with_schema(existing_rows + 1, schema);
    seed_reset_batch_fixture(&server, existing_rows);

    let (client_transport, server_transport) = duplex();
    let _upstream = jazz::db::block_on(client.connect_upstream(client_transport));
    let _subscriber = server.accept_subscriber(server_transport, AuthorSubject::SYSTEM);

    let prepared = client
        .prepare_query(&Query::from("items"))
        .expect("prepare reset-batch query");
    let attachment = client
        .attach_query_with_opts(&prepared, global_read_opts())
        .expect("attach reset-batch query");
    drive_until_covered(&server, &client, &attachment);
    let reset_rows = block_on(client.all(&prepared, global_read_opts()))
        .expect("read reset-batch rows after reset");
    assert_eq!(reset_rows.len(), existing_rows);
    drain_until_idle(&server, &client);

    server
        .seed_settled_mergeable_for_bootstrap(
            "items",
            row(90_000_000 + existing_rows as u64),
            AuthorSubject::SYSTEM,
            BTreeMap::from([
                (
                    "label".to_owned(),
                    Value::String(format!("post-reset-{existing_rows}")),
                ),
                ("ordinal".to_owned(), Value::I32(existing_rows as i32)),
            ]),
        )
        .expect("seed post-reset item");
    block_on(server.tick()).expect("queue post-reset update");

    reset_alloc_counter();
    block_on(client.tick()).expect("apply post-reset update");
    let snapshot = stop_alloc_counter();
    let updated_rows = block_on(client.all(&prepared, global_read_opts()))
        .expect("read reset-batch rows after post-reset update");
    assert_eq!(updated_rows.len(), existing_rows + 1);
    snapshot
}

#[test]
fn reset_batch_post_reset_single_row_changes_are_scale_independent() {
    let small = measure_post_reset_single_insert(500);
    let large = measure_post_reset_single_insert(2_000);

    let alloc_ratio = large.allocs as f64 / small.allocs.max(1) as f64;
    let byte_ratio = large.bytes as f64 / small.bytes.max(1) as f64;
    assert!(
        alloc_ratio <= 3.0 && byte_ratio <= 3.0,
        "INV-INC-1 reset-batch violation: one-row post-reset update allocation scaled with applied reset size: \
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
        ("ordinal".to_owned(), Value::I32(index as i32)),
    ])
}

fn seed_rocks_write_fixture(db: &Db<RocksDbStorage>, child_rows: usize) -> RowUuid {
    let parent = row(50_000_000);
    block_on(db.insert(
        "parents",
        BTreeMap::from([
            ("label".to_owned(), Value::String("write-parent".to_owned())),
            ("ordinal".to_owned(), Value::I32(0)),
        ]),
        jazz::db::InsertOptions {
            row_id: Some(parent),
            ..Default::default()
        },
    ))
    .expect("insert write parent");

    let mut next = 0usize;
    while next < child_rows {
        let start = next;
        let end = (start + 200).min(child_rows);
        block_on(db.transaction(async |tx| {
            for index in start..end {
                tx.insert(
                    "children",
                    write_cells(parent, index),
                    jazz::db::InsertOptions {
                        row_id: Some(row(60_000_000 + index as u64)),
                        ..Default::default()
                    },
                )
                .await?;
            }
            Ok(())
        }))
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
    block_on(db.transaction(async |tx| {
        for offset in 0..200 {
            let index = start_index + offset;
            tx.update(
                "children",
                row(index as u64),
                write_cells(parent, index),
                Default::default(),
            )
            .await?;
        }
        Ok(())
    }))
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
