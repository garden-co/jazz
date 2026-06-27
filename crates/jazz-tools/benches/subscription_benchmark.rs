//! core subscription latency benchmarks.
//!
//! These benchmarks intentionally exercise `jazz::db::Db` directly, bypassing
//! the legacy RuntimeCore/SchemaManager/SyncManager stack while that path is
//! being replaced.
//!
//! Measures:
//! - Single subscription: time from insert to update appearing
//! - Fan-out: time to notify 100 subscriptions
//! - Cold start: time to receive initial result set
//! - Filtered subscription: time to notify a subscribed filtered query

use std::collections::BTreeMap;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use jazz::db::{
    Db, DbConfig, DbIdentity, ReadOpts, SeededRowIdSource, SubscriptionEvent, block_on,
};
use jazz::groove::records::Value;
use jazz::groove::schema::{ColumnSchema, ColumnType};
use jazz::groove::storage::MemoryStorage;
use jazz::ids::{AuthorId, NodeUuid, RowUuid};
use jazz::query::{Query, all_of, col, eq, lit};
use jazz::schema::{JazzSchema, Policy, TableSchema};
use jazz::tx::DurabilityTier;

type CoreDb = Db<MemoryStorage>;

const AUTHOR: AuthorId = AuthorId(uuid::uuid!("00000000-0000-0000-0000-0000000000a1"));
const OTHER_AUTHOR: AuthorId = AuthorId(uuid::uuid!("00000000-0000-0000-0000-0000000000b2"));
const FANOUT_SUBSCRIPTIONS: usize = 100;

fn schema() -> JazzSchema {
    JazzSchema::new([TableSchema::new(
        "documents",
        [
            ColumnSchema::new("folder", ColumnType::Uuid),
            ColumnSchema::new("title", ColumnType::String),
            ColumnSchema::new("content", ColumnType::String),
            ColumnSchema::new("author", ColumnType::Uuid),
            ColumnSchema::new("created_at", ColumnType::U64),
            ColumnSchema::new("done", ColumnType::Bool),
        ],
    )
    .with_read_policy(Policy::public())
    .with_write_policy(Policy::public())])
}

fn open_db(seed: u64) -> CoreDb {
    let schema = schema();
    let column_families = schema.column_families();
    let refs = column_families
        .iter()
        .map(String::as_str)
        .collect::<Vec<_>>();
    block_on(Db::open(
        DbConfig::new(
            schema,
            MemoryStorage::new(&refs),
            DbIdentity {
                node: NodeUuid::from_bytes([seed as u8; 16]),
                author: AUTHOR,
            },
        )
        .with_id_source(SeededRowIdSource::new(seed)),
    ))
    .expect("open core subscription benchmark db")
}

fn row_uuid(index: usize) -> RowUuid {
    RowUuid::from_bytes([(index % 251 + 1) as u8; 16])
}

fn cells(index: usize) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("folder".to_owned(), Value::Uuid(row_uuid(index % 32).0)),
        (
            "title".to_owned(),
            Value::String(format!("Document {index}")),
        ),
        (
            "content".to_owned(),
            Value::String(format!("Content body for document {index}")),
        ),
        ("author".to_owned(), Value::Uuid(AUTHOR.0)),
        ("created_at".to_owned(), Value::U64(index as u64)),
        ("done".to_owned(), Value::Bool(index % 2 == 0)),
    ])
}

fn filtered_cells(index: usize) -> BTreeMap<String, Value> {
    let mut cells = cells(index);
    let author = if index % 2 == 0 { AUTHOR } else { OTHER_AUTHOR };
    cells.insert("author".to_owned(), Value::Uuid(author.0));
    cells.insert("folder".to_owned(), Value::Uuid(row_uuid(index % 2).0));
    cells
}

fn seed_documents(db: &CoreDb, count: usize) {
    for index in 0..count {
        let write = db
            .insert("documents", cells(index))
            .expect("seed core benchmark row");
        block_on(write.wait(DurabilityTier::Local)).expect("seed row should be local");
    }
}

fn seed_filtered_documents(db: &CoreDb, count: usize) {
    for index in 0..count {
        let write = db
            .insert("documents", filtered_cells(index))
            .expect("seed core benchmark row");
        block_on(write.wait(DurabilityTier::Local)).expect("seed row should be local");
    }
}

fn all_documents_query(db: &CoreDb) -> jazz::db::PreparedQuery {
    db.prepare_query(&Query::from("documents"))
        .expect("prepare documents query")
}

fn author_filter_query(db: &CoreDb) -> jazz::db::PreparedQuery {
    db.prepare_query(&Query::from("documents").filter(eq(col("author"), lit(AUTHOR.0))))
        .expect("prepare author-filtered documents query")
}

fn narrow_filter_query(db: &CoreDb) -> jazz::db::PreparedQuery {
    db.prepare_query(&Query::from("documents").filter(all_of([
        eq(col("author"), lit(AUTHOR.0)),
        eq(col("folder"), lit(row_uuid(0).0)),
        eq(col("done"), lit(true)),
    ])))
    .expect("prepare narrow-filtered documents query")
}

fn read_opened_len(event: Option<SubscriptionEvent>) -> usize {
    match event {
        Some(SubscriptionEvent::Opened { current, .. }) => current.len(),
        other => panic!("expected opened subscription event, got {other:?}"),
    }
}

fn read_added_len(event: Option<SubscriptionEvent>) -> usize {
    match event {
        Some(SubscriptionEvent::Delta { added, .. }) => added.len(),
        other => panic!("expected subscription delta event, got {other:?}"),
    }
}

/// Measure latency from insert to subscription update.
fn single_subscription_latency(c: &mut Criterion) {
    let mut group = c.benchmark_group("subscription/single_latency");

    for scale in [1_000usize] {
        group.throughput(Throughput::Elements(1));
        group.bench_with_input(BenchmarkId::new("documents", scale), &scale, |b, &scale| {
            let db = open_db(1);
            seed_documents(&db, scale);
            let query = all_documents_query(&db);
            let mut subscription =
                block_on(db.subscribe(&query, ReadOpts::default())).expect("subscribe");
            assert_eq!(read_opened_len(block_on(subscription.next_event())), scale);
            let mut next = scale;

            b.iter(|| {
                next += 1;
                db.insert("documents", cells(next))
                    .expect("core subscribed insert should succeed");
                assert_eq!(read_added_len(block_on(subscription.next_event())), 1);
            });
        });
    }

    group.finish();
}

/// Measure fan-out latency: time to notify multiple subscriptions.
fn fanout_latency(c: &mut Criterion) {
    let mut group = c.benchmark_group("subscription/fanout");

    for scale in [1_000usize] {
        group.throughput(Throughput::Elements(FANOUT_SUBSCRIPTIONS as u64));
        group.bench_with_input(
            BenchmarkId::new("subscriptions_x100", scale),
            &scale,
            |b, &scale| {
                let db = open_db(2);
                seed_documents(&db, scale);
                let query = all_documents_query(&db);
                let mut subscriptions = (0..FANOUT_SUBSCRIPTIONS)
                    .map(|_| {
                        let mut subscription =
                            block_on(db.subscribe(&query, ReadOpts::default())).expect("subscribe");
                        assert_eq!(read_opened_len(block_on(subscription.next_event())), scale);
                        subscription
                    })
                    .collect::<Vec<_>>();
                let mut next = scale;

                b.iter(|| {
                    next += 1;
                    db.insert("documents", cells(next))
                        .expect("core fanout insert should succeed");

                    let notified = subscriptions
                        .iter_mut()
                        .map(|subscription| read_added_len(block_on(subscription.next_event())))
                        .sum::<usize>();
                    assert_eq!(notified, FANOUT_SUBSCRIPTIONS);
                });
            },
        );
    }

    group.finish();
}

/// Measure cold start: time to get initial result set.
fn cold_start_latency(c: &mut Criterion) {
    let mut group = c.benchmark_group("subscription/cold_start");

    for scale in [1_000usize] {
        group.bench_with_input(
            BenchmarkId::new("initial_load", scale),
            &scale,
            |b, &scale| {
                let db = open_db(3);
                seed_documents(&db, scale);
                let query = all_documents_query(&db);

                b.iter(|| {
                    let mut subscription =
                        block_on(db.subscribe(&query, ReadOpts::default())).expect("subscribe");
                    assert_eq!(read_opened_len(block_on(subscription.next_event())), scale);
                });
            },
        );
    }

    group.finish();
}

/// Measure filtered subscription: only see matching documents.
fn filtered_subscription_latency(c: &mut Criterion) {
    let mut group = c.benchmark_group("subscription/filtered");

    for scale in [1_000usize] {
        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::new("author_filter", scale),
            &scale,
            |b, &scale| {
                let db = open_db(4);
                seed_filtered_documents(&db, scale);
                let query = author_filter_query(&db);
                let mut subscription =
                    block_on(db.subscribe(&query, ReadOpts::default())).expect("subscribe");
                assert_eq!(
                    read_opened_len(block_on(subscription.next_event())),
                    scale / 2
                );
                let mut next = scale + (scale % 2);

                b.iter(|| {
                    next += 2;
                    db.insert("documents", filtered_cells(next))
                        .expect("core filtered insert should succeed");
                    assert_eq!(read_added_len(block_on(subscription.next_event())), 1);
                });
            },
        );
    }

    group.finish();
}

/// Measure batch insert latency with subscription (exercises delta fast path).
fn batch_insert_subscription_latency(c: &mut Criterion) {
    let mut group = c.benchmark_group("subscription/batch_insert");

    for scale in [1_000usize, 10_000usize] {
        let batch_size = 100usize;
        group.throughput(Throughput::Elements(batch_size as u64));
        group.bench_with_input(
            BenchmarkId::new("documents_x100", scale),
            &scale,
            |b, &scale| {
                let db = open_db(5);
                seed_filtered_documents(&db, scale);
                let query = narrow_filter_query(&db);
                let mut subscription =
                    block_on(db.subscribe(&query, ReadOpts::default())).expect("subscribe");
                let initial_len = read_opened_len(block_on(subscription.next_event()));
                let mut next = scale + (scale % 2);

                b.iter(|| {
                    let mut tx = db.mergeable_tx();
                    for _ in 0..batch_size {
                        next += 2;
                        tx.insert("documents", filtered_cells(next))
                            .expect("core batch insert should stage");
                    }
                    tx.commit().expect("core batch insert should commit");
                    assert_eq!(
                        read_added_len(block_on(subscription.next_event())),
                        batch_size
                    );
                    assert!(subscription.try_next_event().is_none());
                });

                assert!(initial_len <= scale);
            },
        );
    }

    group.finish();
}

criterion_group!(
    benches,
    single_subscription_latency,
    fanout_latency,
    cold_start_latency,
    filtered_subscription_latency,
    batch_insert_subscription_latency
);
criterion_main!(benches);
