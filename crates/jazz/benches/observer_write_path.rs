//! Focused core write-path benchmark for plain vs observed mutations.
//!
//! The reproduction case is a content-only update on a fixed-size table. That
//! keeps result cardinality stable so the benchmark isolates the overhead of
//! maintaining a live query, rather than measuring table growth.

#![allow(clippy::single_element_loop)]

use std::collections::BTreeMap;

mod schema_fixture;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use jazz::db::{
    Db, DbConfig, DbIdentity, ReadOpts, SeededRowIdSource, SubscriptionEvent, block_on,
};
use jazz::groove::records::Value;
use jazz::groove::storage::MemoryStorage;
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{OrderDirection, Query};
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};

type BenchDb = Db<MemoryStorage>;

const AUTHOR_UUID: uuid::Uuid = uuid::uuid!("00000000-0000-0000-0000-0000000000a1");

fn schema() -> JazzSchema {
    schema_fixture::compile(
        SchemaBuilder::new().table(
            TableSchemaBuilder::new("documents")
                .column("title", ColumnType::Text)
                .column("content", ColumnType::Text)
                .column("created_at", ColumnType::Timestamp),
        ),
    )
}

fn open_db(seed: u64) -> BenchDb {
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
                author: AuthorSubject::for_test_uuid(AUTHOR_UUID),
            },
        )
        .with_id_source(SeededRowIdSource::new(seed)),
    ))
    .expect("open core observer benchmark db")
}

fn document_cells(index: usize) -> BTreeMap<String, Value> {
    BTreeMap::from([
        (
            "title".to_owned(),
            Value::String(format!("Document {index}")),
        ),
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
            db.insert("documents", document_cells(index), Default::default())
                .expect("seed core observer benchmark row")
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

fn payload_only_update(index: usize) -> BTreeMap<String, Value> {
    BTreeMap::from([(
        "content".to_owned(),
        Value::String(format!("Updated payload {index}")),
    )])
}

fn all_documents_query(db: &BenchDb) -> jazz::db::PreparedQuery {
    db.prepare_query(&Query::from("documents"))
        .expect("prepare documents query")
}

fn documents_by_created_at_query(db: &BenchDb) -> jazz::db::PreparedQuery {
    db.prepare_query(&Query::from("documents").order_by("created_at", OrderDirection::Asc))
        .expect("prepare ordered documents query")
}

fn first_documents_by_created_at_query(db: &BenchDb) -> jazz::db::PreparedQuery {
    db.prepare_query(
        &Query::from("documents")
            .order_by("created_at", OrderDirection::Asc)
            .limit(50),
    )
    .expect("prepare limited ordered documents query")
}

fn update_write_path_with_and_without_observer(c: &mut Criterion) {
    let mut group = c.benchmark_group("observer_write_path/update_content");

    for scale in [100usize, 1_000, 10_000] {
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

                    db.update(
                        "documents",
                        row,
                        content_update(update_index),
                        Default::default(),
                    )
                    .expect("core update without observer should succeed")
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
                    Some(SubscriptionEvent::Delta {
                        reset: true, added, ..
                    }) => {
                        assert_eq!(added.len(), scale);
                    }
                    other => panic!("expected reset subscription event, got {other:?}"),
                }

                let mut row_index = 0usize;
                let mut update_index = 0usize;

                b.iter(|| {
                    update_index += 1;
                    let row = rows[row_index % rows.len()];
                    row_index += 1;

                    db.update(
                        "documents",
                        row,
                        content_update(update_index),
                        Default::default(),
                    )
                    .expect("core update with observer should succeed");
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

fn update_payload_with_ordered_observer(c: &mut Criterion) {
    let mut group = c.benchmark_group("observer_write_path/update_ordered_payload");

    for scale in [100usize, 1_000, 10_000] {
        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::new("stable_sort_key", scale),
            &scale,
            |b, &scale| {
                let db = open_db(3);
                let rows = seed_documents(&db, scale);
                let query = documents_by_created_at_query(&db);
                let mut subscription =
                    block_on(db.subscribe(&query, ReadOpts::default())).expect("subscribe");
                match block_on(subscription.next_event()) {
                    Some(SubscriptionEvent::Delta {
                        reset: true, added, ..
                    }) => assert_eq!(added.len(), scale),
                    other => panic!("expected ordered reset event, got {other:?}"),
                }

                let mut row_index = 0usize;
                let mut update_index = 0usize;
                b.iter(|| {
                    update_index += 1;
                    let row = rows[row_index % rows.len()];
                    row_index += 1;
                    db.update(
                        "documents",
                        row,
                        payload_only_update(update_index),
                        Default::default(),
                    )
                    .expect("ordered payload update should succeed");
                    match block_on(subscription.next_event()) {
                        Some(SubscriptionEvent::Delta { updated, .. }) => updated.len(),
                        other => panic!("expected ordered subscription delta, got {other:?}"),
                    }
                });
            },
        );
    }

    group.finish();
}

fn update_payload_inside_finite_ordered_window(c: &mut Criterion) {
    let mut group = c.benchmark_group("observer_write_path/update_finite_ordered_payload");

    for scale in [100usize, 1_000, 10_000] {
        group.throughput(Throughput::Elements(1));
        group.bench_with_input(
            BenchmarkId::new("limit_50_stable_sort_key", scale),
            &scale,
            |b, &scale| {
                let db = open_db(4);
                let rows = seed_documents(&db, scale);
                let query = first_documents_by_created_at_query(&db);
                let mut subscription =
                    block_on(db.subscribe(&query, ReadOpts::default())).expect("subscribe");
                match block_on(subscription.next_event()) {
                    Some(SubscriptionEvent::Delta {
                        reset: true, added, ..
                    }) => assert_eq!(added.len(), 50),
                    other => panic!("expected limited ordered reset event, got {other:?}"),
                }

                let mut update_index = 0usize;
                b.iter(|| {
                    update_index += 1;
                    db.update(
                        "documents",
                        rows[0],
                        payload_only_update(update_index),
                        Default::default(),
                    )
                    .expect("finite ordered payload update should succeed");
                    match block_on(subscription.next_event()) {
                        Some(SubscriptionEvent::Delta { updated, .. }) => updated.len(),
                        other => {
                            panic!("expected limited ordered subscription delta, got {other:?}")
                        }
                    }
                });
            },
        );
    }

    group.finish();
}

fn guarded_benches(c: &mut Criterion) {
    jazz_benchmark_guard::refuse_contaminated_measurement();
    update_write_path_with_and_without_observer(c);
    update_payload_with_ordered_observer(c);
    update_payload_inside_finite_ordered_window(c);
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = guarded_benches
}
criterion_main!(benches);
mod support;

use support::BenchFutureExt as _;
