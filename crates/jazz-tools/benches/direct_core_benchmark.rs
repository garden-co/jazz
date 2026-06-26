//! Direct jazz_core facade benchmarks.
//!
//! These benchmarks intentionally exercise the replacement core path directly
//! instead of the legacy jazz-tools RuntimeCore stack.

use std::collections::BTreeMap;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use jazz::db::{
    Db, DbConfig, DbIdentity, ReadOpts, SeededRowIdSource, SubscriptionEvent, block_on,
};
use jazz::groove::records::Value;
use jazz::groove::schema::{ColumnSchema, ColumnType};
use jazz::groove::storage::MemoryStorage;
use jazz::ids::{AuthorId, NodeUuid, RowUuid};
use jazz::query::Query;
use jazz::schema::{JazzSchema, Policy, TableSchema};
use jazz::tx::DurabilityTier;

type DirectDb = Db<MemoryStorage>;

const AUTHOR: AuthorId = AuthorId(uuid::uuid!("00000000-0000-0000-0000-0000000000a1"));

fn schema() -> JazzSchema {
    JazzSchema::new([TableSchema::new(
        "documents",
        [
            ColumnSchema::new("folder", ColumnType::Uuid),
            ColumnSchema::new("title", ColumnType::String),
            ColumnSchema::new("content", ColumnType::String),
            ColumnSchema::new("author", ColumnType::Uuid),
            ColumnSchema::new("created_at", ColumnType::U64),
        ],
    )
    .with_read_policy(Policy::public())
    .with_write_policy(Policy::public())])
}

fn open_db(seed: u64) -> DirectDb {
    let schema = schema();
    let column_families = schema.column_families();
    let refs = column_families.iter().map(String::as_str).collect::<Vec<_>>();
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
    .expect("open direct benchmark db")
}

fn row_uuid(index: usize) -> RowUuid {
    RowUuid::from_bytes([(index % 251 + 1) as u8; 16])
}

fn cells(index: usize) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("folder".to_owned(), Value::Uuid(row_uuid(index % 32).0)),
        ("title".to_owned(), Value::String(format!("Document {index}"))),
        (
            "content".to_owned(),
            Value::String(format!("Content body for document {index}")),
        ),
        ("author".to_owned(), Value::Uuid(AUTHOR.0)),
        ("created_at".to_owned(), Value::U64(index as u64)),
    ])
}

fn seed_documents(db: &DirectDb, count: usize) -> Vec<RowUuid> {
    (0..count)
        .map(|index| {
            let write = db
                .insert("documents", cells(index))
                .expect("seed direct benchmark row");
            block_on(write.wait(DurabilityTier::Local)).expect("seed row should be local");
            write.row_uuid()
        })
        .collect()
}

fn all_documents_query(db: &DirectDb) -> jazz::db::PreparedQuery {
    db.prepare_query(&Query::from("documents"))
        .expect("prepare documents query")
}

fn direct_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("direct_core/insert");

    for initial_rows in [1_000usize] {
        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::new("documents", initial_rows),
            &initial_rows,
            |b, &initial_rows| {
                let db = open_db(1);
                seed_documents(&db, initial_rows);
                let mut next = initial_rows;

                b.iter(|| {
                    next += 1;
                    let write = db
                        .insert("documents", cells(next))
                        .expect("direct insert should succeed");
                    block_on(write.wait(DurabilityTier::Local)).expect("insert should be local");
                    write.row_uuid()
                });
            },
        );
    }

    group.finish();
}

fn direct_update_and_read(c: &mut Criterion) {
    let mut group = c.benchmark_group("direct_core/update_read");

    for row_count in [1_000usize] {
        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::new("documents", row_count),
            &row_count,
            |b, &row_count| {
                let db = open_db(2);
                let rows = seed_documents(&db, row_count);
                let query = all_documents_query(&db);
                let mut index = 0usize;

                b.iter(|| {
                    let row = rows[index % rows.len()];
                    index += 1;
                    db.update(
                        "documents",
                        row,
                        BTreeMap::from([(
                            "content".to_owned(),
                            Value::String(format!("Updated content {index}")),
                        )]),
                    )
                    .expect("direct update should succeed");
                    db.read(&query).expect("direct read should succeed").len()
                });
            },
        );
    }

    group.finish();
}

fn direct_subscribed_write(c: &mut Criterion) {
    let mut group = c.benchmark_group("direct_core/subscribed_write");

    for row_count in [1_000usize] {
        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::new("documents", row_count),
            &row_count,
            |b, &row_count| {
                let db = open_db(3);
                seed_documents(&db, row_count);
                let query = all_documents_query(&db);
                let mut subscription =
                    block_on(db.subscribe(&query, ReadOpts::default())).expect("subscribe");
                match block_on(subscription.next_event()) {
                    Some(SubscriptionEvent::Opened { current, .. }) => {
                        assert_eq!(current.len(), row_count);
                    }
                    other => panic!("expected opened subscription event, got {other:?}"),
                }
                let mut next = row_count;

                b.iter(|| {
                    next += 1;
                    db.insert("documents", cells(next))
                        .expect("direct subscribed insert should succeed");
                    match block_on(subscription.next_event()) {
                        Some(SubscriptionEvent::Delta { added, .. }) => added.len(),
                        other => panic!("expected subscription delta event, got {other:?}"),
                    }
                });
            },
        );
    }

    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = direct_insert, direct_update_and_read, direct_subscribed_write
}
criterion_main!(benches);
