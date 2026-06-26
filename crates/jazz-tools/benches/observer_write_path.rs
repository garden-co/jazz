//! Focused direct-core write-path benchmark for plain vs observed mutations.
//!
//! The reproduction case is a content-only update on a fixed-size table. That
//! keeps result cardinality stable so the benchmark isolates the overhead of
//! maintaining a live query, rather than measuring table growth.

#![allow(clippy::single_element_loop)]

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

type BenchDb = Db<MemoryStorage>;

const AUTHOR: AuthorId = AuthorId(uuid::uuid!("00000000-0000-0000-0000-0000000000a1"));

fn schema() -> JazzSchema {
    JazzSchema::new([TableSchema::new(
        "documents",
        [
            ColumnSchema::new("title", ColumnType::String),
            ColumnSchema::new("content", ColumnType::String),
            ColumnSchema::new("created_at", ColumnType::U64),
        ],
    )
    .with_read_policy(Policy::public())
    .with_write_policy(Policy::public())])
}

fn open_db(seed: u64) -> BenchDb {
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
    .expect("open direct observer benchmark db")
}

fn document_cells(index: usize) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("title".to_owned(), Value::String(format!("Document {index}"))),
        (
            "content".to_owned(),
            Value::String(format!("Content body for document {index}")),
        ),
        ("created_at".to_owned(), Value::U64(index as u64)),
    ])
}

fn seed_documents(db: &BenchDb, count: usize) -> Vec<RowUuid> {
    (0..count)
        .map(|index| {
            db.insert("documents", document_cells(index))
                .expect("seed direct observer benchmark row")
                .row_uuid()
        })
        .collect()
}

fn content_update(index: usize) -> BTreeMap<String, Value> {
    BTreeMap::from([
        (
            "content".to_owned(),
            Value::String(format!("Updated content {index}")),
        ),
        ("created_at".to_owned(), Value::U64(index as u64)),
    ])
}

fn all_documents_query(db: &BenchDb) -> jazz::db::PreparedQuery {
    db.prepare_query(&Query::from("documents"))
        .expect("prepare documents query")
}

fn update_write_path_with_and_without_observer(c: &mut Criterion) {
    let mut group = c.benchmark_group("observer_write_path/update_content");

    for scale in [1_000usize] {
        group.throughput(Throughput::Elements(1));

        group.bench_with_input(
            BenchmarkId::new("no_observer", scale),
            &scale,
            |b, &scale| {
                let db = open_db(1);
                let rows = seed_documents(&db, scale);
                let mut row_index = 0usize;
                let mut update_index = 0usize;

                b.iter(|| {
                    update_index += 1;
                    let row = rows[row_index % rows.len()];
                    row_index += 1;

                    db.update("documents", row, content_update(update_index))
                        .expect("direct update without observer should succeed")
                });
            },
        );

        group.bench_with_input(
            BenchmarkId::new("observe_all", scale),
            &scale,
            |b, &scale| {
                let db = open_db(2);
                let rows = seed_documents(&db, scale);
                let query = all_documents_query(&db);
                let mut subscription =
                    block_on(db.subscribe(&query, ReadOpts::default())).expect("subscribe");
                match block_on(subscription.next_event()) {
                    Some(SubscriptionEvent::Opened { current, .. }) => {
                        assert_eq!(current.len(), scale);
                    }
                    other => panic!("expected opened subscription event, got {other:?}"),
                }

                let mut row_index = 0usize;
                let mut update_index = 0usize;

                b.iter(|| {
                    update_index += 1;
                    let row = rows[row_index % rows.len()];
                    row_index += 1;

                    db.update("documents", row, content_update(update_index))
                        .expect("direct update with observer should succeed");
                    match block_on(subscription.next_event()) {
                        Some(SubscriptionEvent::Delta { updated, .. }) => updated.len(),
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
    targets = update_write_path_with_and_without_observer
}
criterion_main!(benches);
