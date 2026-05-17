//! Write amplification benchmark for document-table index profiles.
//!
//! The benchmark ids include expected logical index-entry mutations for the
//! operation. That is the storage-independent amplification before RocksDB's
//! own WAL/memtable/compaction amplification.

use std::collections::HashMap;

use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use jazz_tools::object::ObjectId;
use jazz_tools::query_manager::types::{
    ColumnType, CompositeIndexColumn, Schema, TableSchema, Value,
};
use jazz_tools::runtime_core::{NoopScheduler, RuntimeCore};
use jazz_tools::schema_manager::{AppId, SchemaManager};
use jazz_tools::storage::MemoryStorage;
use jazz_tools::sync_manager::SyncManager;

type BenchRuntime = RuntimeCore<MemoryStorage, NoopScheduler>;

const SEED_ROWS: usize = 1_000;

#[derive(Debug, Clone, Copy)]
struct IndexProfile {
    label: &'static str,
    indexed_columns: Option<&'static [&'static str]>,
    owner_updated_index: bool,
    org_updated_index: bool,
    insert_index_entries: usize,
    title_update_index_mutations: usize,
    updated_at_update_index_mutations: usize,
}

const INDEX_PROFILES: &[IndexProfile] = &[
    IndexProfile {
        label: "default_all_columns",
        indexed_columns: None,
        owner_updated_index: false,
        org_updated_index: false,
        insert_index_entries: 11,
        title_update_index_mutations: 4,
        updated_at_update_index_mutations: 4,
    },
    IndexProfile {
        label: "minimal_single_columns",
        indexed_columns: Some(&["owner_id", "org_id", "updated_at"]),
        owner_updated_index: false,
        org_updated_index: false,
        insert_index_entries: 6,
        title_update_index_mutations: 2,
        updated_at_update_index_mutations: 4,
    },
    IndexProfile {
        label: "minimal_plus_owner_updated",
        indexed_columns: Some(&["owner_id", "org_id", "updated_at"]),
        owner_updated_index: true,
        org_updated_index: false,
        insert_index_entries: 7,
        title_update_index_mutations: 2,
        updated_at_update_index_mutations: 6,
    },
    IndexProfile {
        label: "minimal_plus_owner_and_org_updated",
        indexed_columns: Some(&["owner_id", "org_id", "updated_at"]),
        owner_updated_index: true,
        org_updated_index: true,
        insert_index_entries: 8,
        title_update_index_mutations: 2,
        updated_at_update_index_mutations: 8,
    },
];

fn row<const N: usize>(pairs: [(&str, Value); N]) -> HashMap<String, Value> {
    pairs
        .into_iter()
        .map(|(key, value)| (key.to_string(), value))
        .collect()
}

fn document_values(index: u64) -> HashMap<String, Value> {
    row([
        ("owner_id", Value::Text(format!("user-{}", index % 10_000))),
        ("org_id", Value::Text(format!("org-{}", index % 1_000))),
        ("updated_at", Value::Timestamp(index)),
        ("title", Value::Text(format!("Document {index}"))),
        ("content", Value::Text(format!("Body {index}"))),
        ("status", Value::Text("open".to_string())),
        ("tag", Value::Text(format!("tag-{}", index % 20))),
        ("priority", Value::Integer((index % 5) as i32)),
    ])
}

fn document_schema(profile: IndexProfile) -> Schema {
    let builder = TableSchema::builder("documents")
        .column("owner_id", ColumnType::Text)
        .column("org_id", ColumnType::Text)
        .column("updated_at", ColumnType::Timestamp)
        .column("title", ColumnType::Text)
        .column("content", ColumnType::Text)
        .column("status", ColumnType::Text)
        .column("tag", ColumnType::Text)
        .column("priority", ColumnType::Integer);

    let builder = if let Some(indexed_columns) = profile.indexed_columns {
        builder.index_only(indexed_columns.iter().copied())
    } else {
        builder
    };

    let builder = if profile.owner_updated_index {
        builder.composite_index(vec![
            CompositeIndexColumn::asc("owner_id"),
            CompositeIndexColumn::desc("updated_at"),
        ])
    } else {
        builder
    };

    let builder = if profile.org_updated_index {
        builder.composite_index(vec![
            CompositeIndexColumn::asc("org_id"),
            CompositeIndexColumn::desc("updated_at"),
        ])
    } else {
        builder
    };

    let mut schema = Schema::new();
    let (name, table) = builder.build_named();
    schema.insert(name, table);
    schema
}

fn create_runtime(profile: IndexProfile) -> BenchRuntime {
    let app_id_name = format!("write-amplification-{}", profile.label);
    let schema_manager = SchemaManager::new(
        SyncManager::new(),
        document_schema(profile),
        AppId::from_name(&app_id_name),
        "dev",
        "main",
    )
    .expect("schema manager");
    RuntimeCore::new(schema_manager, MemoryStorage::new(), NoopScheduler)
}

fn seed_runtime(profile: IndexProfile) -> (BenchRuntime, Vec<ObjectId>) {
    let mut runtime = create_runtime(profile);
    let mut ids = Vec::with_capacity(SEED_ROWS);
    for index in 0..SEED_ROWS as u64 {
        let ((row_id, _), _) = runtime
            .insert("documents", document_values(index), None)
            .expect("seed document");
        ids.push(row_id);
    }
    (runtime, ids)
}

fn bench_insert(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_amplification/insert_document");
    group.throughput(Throughput::Elements(1));

    for profile in INDEX_PROFILES {
        let mut runtime = create_runtime(*profile);
        let mut counter = 0_u64;
        let id = BenchmarkId::new(
            profile.label,
            format!("logical_index_entries={}", profile.insert_index_entries),
        );

        group.bench_function(id, |b| {
            b.iter(|| {
                counter += 1;
                let result = runtime
                    .insert("documents", document_values(counter), None)
                    .expect("insert document");
                black_box(result);
            });
        });
    }

    group.finish();
}

fn bench_update_title_only(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_amplification/update_title_only");
    group.throughput(Throughput::Elements(1));

    for profile in INDEX_PROFILES {
        let (mut runtime, ids) = seed_runtime(*profile);
        let mut cursor = 0_usize;
        let mut counter = 0_u64;
        let id = BenchmarkId::new(
            profile.label,
            format!(
                "logical_index_mutations={}",
                profile.title_update_index_mutations
            ),
        );

        group.bench_function(id, |b| {
            b.iter(|| {
                counter += 1;
                let row_id = ids[cursor % ids.len()];
                cursor += 1;
                let result = runtime.update(
                    row_id,
                    vec![(
                        "title".to_string(),
                        Value::Text(format!("Retitled {counter}")),
                    )],
                    None,
                );
                black_box(result.expect("update title"));
            });
        });
    }

    group.finish();
}

fn bench_update_updated_at(c: &mut Criterion) {
    let mut group = c.benchmark_group("write_amplification/update_updated_at");
    group.throughput(Throughput::Elements(1));

    for profile in INDEX_PROFILES {
        let (mut runtime, ids) = seed_runtime(*profile);
        let mut cursor = 0_usize;
        let mut counter = 1_000_000_u64;
        let id = BenchmarkId::new(
            profile.label,
            format!(
                "logical_index_mutations={}",
                profile.updated_at_update_index_mutations
            ),
        );

        group.bench_function(id, |b| {
            b.iter(|| {
                counter += 1;
                let row_id = ids[cursor % ids.len()];
                cursor += 1;
                let result = runtime.update(
                    row_id,
                    vec![("updated_at".to_string(), Value::Timestamp(counter))],
                    None,
                );
                black_box(result.expect("update updated_at"));
            });
        });
    }

    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = bench_insert, bench_update_title_only, bench_update_updated_at
}
criterion_main!(benches);
