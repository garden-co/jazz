//! Direct jazz_core update throughput benchmark for permissioned operations.
//!
//! Measures updates/second through `jazz::db::Db<MemoryStorage>` so this
//! exercises the direct core replacement path instead of the legacy
//! RuntimeCore/SchemaManager/SyncManager layers. The single-row case cycles
//! through owned documents; the batch case applies 100 direct updates per
//! iteration.

#![allow(clippy::single_element_loop)]

use std::collections::BTreeMap;

use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};
use jazz::db::{Db, DbConfig, DbIdentity, SeededRowIdSource, block_on};
use jazz::groove::records::Value;
use jazz::groove::schema::{ColumnSchema, ColumnType};
use jazz::groove::storage::MemoryStorage;
use jazz::ids::{AuthorId, NodeUuid, RowUuid};
use jazz::schema::{JazzSchema, Policy, TableSchema};
use jazz::tx::DurabilityTier;

type DirectCoreDb = Db<MemoryStorage>;

const AUTHOR: AuthorId = AuthorId(uuid::uuid!("00000000-0000-0000-0000-0000000000a1"));

fn schema() -> JazzSchema {
    JazzSchema::new([
        TableSchema::new("folders", [ColumnSchema::new("name", ColumnType::String)])
            .with_read_policy(Policy::public())
            .with_write_policy(Policy::public()),
        TableSchema::new(
            "documents",
            [
                ColumnSchema::new("folder", ColumnType::Uuid),
                ColumnSchema::new("title", ColumnType::String),
                ColumnSchema::new("content", ColumnType::String),
                ColumnSchema::new("author", ColumnType::Uuid),
                ColumnSchema::new("created_at", ColumnType::U64),
            ],
        )
        .with_reference("folder", "folders")
        .with_read_policy(Policy::public())
        .with_write_policy(Policy::owner_only("documents", "author")),
    ])
}

fn open_direct_core_db(seed: u64) -> DirectCoreDb {
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
    .expect("open direct update benchmark db")
}

fn document_cells(index: usize, folder: RowUuid) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("folder".to_owned(), Value::Uuid(folder.0)),
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
    ])
}

fn update_cells(
    index: u64,
    folder: RowUuid,
    title: String,
    content: &str,
) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("folder".to_owned(), Value::Uuid(folder.0)),
        ("title".to_owned(), Value::String(title)),
        ("content".to_owned(), Value::String(content.to_owned())),
        ("author".to_owned(), Value::Uuid(AUTHOR.0)),
        ("created_at".to_owned(), Value::U64(index)),
    ])
}

struct Fixture {
    owned_documents: Vec<RowUuid>,
    owned_folders: Vec<RowUuid>,
}

fn seed_fixture(db: &DirectCoreDb, count: usize) -> Fixture {
    let folder_count = 32usize.min(count.max(1));
    let owned_folders = (0..folder_count)
        .map(|index| {
            let write = db
                .insert(
                    "folders",
                    BTreeMap::from([("name".to_owned(), Value::String(format!("Folder {index}")))]),
                )
                .expect("seed folder");
            block_on(write.wait(DurabilityTier::Local)).expect("folder seed should be local");
            write.row_uuid()
        })
        .collect::<Vec<_>>();

    let owned_documents = (0..count)
        .map(|index| {
            let folder = owned_folders[index % owned_folders.len()];
            let write = db
                .insert("documents", document_cells(index, folder))
                .expect("seed owned document");
            block_on(write.wait(DurabilityTier::Local)).expect("document seed should be local");
            write.row_uuid()
        })
        .collect();

    Fixture {
        owned_documents,
        owned_folders,
    }
}

fn update_own_documents(c: &mut Criterion) {
    let mut group = c.benchmark_group("direct_core/update/own_documents");

    for scale in [1_000usize] {
        group.throughput(Throughput::Elements(1));
        group.bench_with_input(BenchmarkId::new("documents", scale), &scale, |b, &scale| {
            let db = open_direct_core_db(1);
            let data = seed_fixture(&db, scale);
            let mut doc_idx = 0usize;
            let mut update_counter = 0u64;

            b.iter(|| {
                update_counter += 1;
                let doc_id = data.owned_documents[doc_idx % data.owned_documents.len()];
                let folder_id = data.owned_folders[doc_idx % data.owned_folders.len()];
                doc_idx += 1;

                let write = db
                    .update(
                        "documents",
                        doc_id,
                        update_cells(
                            update_counter,
                            folder_id,
                            format!("Updated Title {update_counter}"),
                            "Updated content",
                        ),
                    )
                    .expect("update own document should succeed");
                block_on(write.wait(DurabilityTier::Local)).expect("update should be local");
            });
        });
    }

    group.finish();
}

fn update_batch(c: &mut Criterion) {
    let mut group = c.benchmark_group("direct_core/update/batch");

    for scale in [1_000usize] {
        let batch_size = 100;
        group.throughput(Throughput::Elements(batch_size as u64));
        group.bench_with_input(
            BenchmarkId::new("documents_x100", scale),
            &scale,
            |b, &scale| {
                let db = open_direct_core_db(2);
                let data = seed_fixture(&db, scale);
                let doc_ids = data
                    .owned_documents
                    .iter()
                    .cycle()
                    .take(batch_size)
                    .copied()
                    .collect::<Vec<_>>();
                let folder_id = data.owned_folders[0];
                let mut batch_counter = 0u64;

                b.iter(|| {
                    batch_counter += 1;

                    for (i, &doc_id) in doc_ids.iter().enumerate() {
                        let timestamp = batch_counter * batch_size as u64 + i as u64;
                        let write = db
                            .update(
                                "documents",
                                doc_id,
                                update_cells(
                                    timestamp,
                                    folder_id,
                                    format!("Batch {batch_counter} Update {i}"),
                                    "Batch updated content",
                                ),
                            )
                            .expect("batch update should succeed");
                        block_on(write.wait(DurabilityTier::Local))
                            .expect("batch update should be local");
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
    targets = update_own_documents, update_batch
}
criterion_main!(benches);
