//! Scoped document-list pagination benchmark.
//!
//! Models the hot path:
//! `documents where owner_id = current_user order by updated_at desc limit 50`.

use std::collections::HashMap;
use std::time::Instant;

use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use futures::executor::block_on;
use jazz_tools::object::ObjectId;
use jazz_tools::query_manager::query::QueryBuilder;
use jazz_tools::query_manager::types::{
    ColumnType, CompositeIndexColumn, Schema, TableSchema, Value,
};
use jazz_tools::runtime_core::{NoopScheduler, RuntimeCore};
use jazz_tools::schema_manager::{AppId, SchemaManager};
#[cfg(feature = "rocksdb")]
use jazz_tools::storage::RocksDBStorage;
use jazz_tools::storage::{MemoryStorage, Storage};
use jazz_tools::sync_manager::SyncManager;

type BenchRuntime<S = MemoryStorage> = RuntimeCore<S, NoopScheduler>;

const TARGET_OWNER: &str = "alice";
const PAGE_SIZE: usize = 50;

fn row<const N: usize>(pairs: [(&str, Value); N]) -> HashMap<String, Value> {
    pairs
        .into_iter()
        .map(|(key, value)| (key.to_string(), value))
        .collect()
}

fn document_schema(with_composite_index: bool) -> Schema {
    let builder = TableSchema::builder("documents")
        .column("owner_id", ColumnType::Text)
        .column("org_id", ColumnType::Text)
        .column("updated_at", ColumnType::Timestamp)
        .column("title", ColumnType::Text)
        .index_only(["owner_id", "org_id", "updated_at"]);

    let builder = if with_composite_index {
        builder.composite_index(vec![
            CompositeIndexColumn::asc("owner_id"),
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

fn create_runtime<S: Storage>(storage: S, with_composite_index: bool) -> BenchRuntime<S> {
    let schema_manager = SchemaManager::new(
        SyncManager::new(),
        document_schema(with_composite_index),
        AppId::from_name("scoped-pagination-bench"),
        "dev",
        "main",
    )
    .expect("schema manager");
    RuntimeCore::new(schema_manager, storage, NoopScheduler)
}

fn seed_documents(
    runtime: &mut BenchRuntime<impl Storage>,
    total_rows: usize,
    target_owner_rows: usize,
) -> Vec<ObjectId> {
    let mut target_ids = Vec::with_capacity(target_owner_rows);
    let target_stride = (total_rows / target_owner_rows).max(1);

    for index in 0..total_rows {
        let is_target = index % target_stride == 0 && target_ids.len() < target_owner_rows;
        let owner_id = if is_target {
            TARGET_OWNER.to_string()
        } else {
            format!("user-{}", index % 10_000)
        };
        let ((row_id, _), _) = runtime
            .insert(
                "documents",
                row([
                    ("owner_id", Value::Text(owner_id)),
                    ("org_id", Value::Text(format!("org-{}", index % 1_000))),
                    ("updated_at", Value::Timestamp(index as u64)),
                    ("title", Value::Text(format!("Document {}", index))),
                ]),
                None,
            )
            .expect("insert document");
        if is_target {
            target_ids.push(row_id);
        }
    }

    target_ids
}

fn query_owner_page_with_storage(runtime: &mut BenchRuntime<impl Storage>) -> usize {
    let query = QueryBuilder::new("documents")
        .filter_eq("owner_id", Value::Text(TARGET_OWNER.to_string()))
        .order_by_desc("updated_at")
        .limit(PAGE_SIZE)
        .build();
    block_on(runtime.query(query, None))
        .expect("owner page query")
        .len()
}

fn prepare_case<S: Storage>(
    storage: S,
    storage_label: &str,
    total_rows: usize,
    target_owner_rows: usize,
    with_composite_index: bool,
) -> BenchRuntime<S> {
    let seed_start = Instant::now();
    let mut runtime = create_runtime(storage, with_composite_index);
    let target_ids = seed_documents(&mut runtime, total_rows, target_owner_rows);
    eprintln!(
        "seeded {storage_label}/{total_rows}_rows_{target_owner_rows}_owned/composite={with_composite_index} in {:?}",
        seed_start.elapsed()
    );
    assert_eq!(target_ids.len(), target_owner_rows);
    assert_eq!(query_owner_page_with_storage(&mut runtime), PAGE_SIZE);
    runtime
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn scoped_owner_page(c: &mut Criterion) {
    let mut group = c.benchmark_group("scoped_pagination/owner_updated_desc_limit_50");
    group.sample_size(10);

    let total_rows = env_usize("SCOPED_PAGINATION_TOTAL_ROWS", 100_000);
    let target_owner_rows = env_usize("SCOPED_PAGINATION_TARGET_OWNER_ROWS", 10_000);
    let storage_filter = std::env::var("SCOPED_PAGINATION_STORAGE").unwrap_or_default();
    let case_filter = std::env::var("SCOPED_PAGINATION_CASE").unwrap_or_default();

    for (total_rows, target_owner_rows) in [(total_rows, target_owner_rows)] {
        group.throughput(Throughput::Elements(PAGE_SIZE as u64));

        for (label, with_composite_index) in [
            ("baseline_single_column", false),
            ("composite_owner_updated", true),
        ] {
            if !case_filter.is_empty() && case_filter != label {
                continue;
            }

            let id = BenchmarkId::new(
                label,
                format!("{total_rows}_rows_{target_owner_rows}_owned"),
            );

            if storage_filter.is_empty() || storage_filter == "memory" {
                let mut runtime = prepare_case(
                    MemoryStorage::new(),
                    "memory",
                    total_rows,
                    target_owner_rows,
                    with_composite_index,
                );
                group.bench_function(id.clone(), move |b| {
                    b.iter(|| {
                        let rows = query_owner_page_with_storage(&mut runtime);
                        black_box(rows);
                    });
                });
            }

            #[cfg(feature = "rocksdb")]
            if storage_filter == "rocksdb" {
                let temp_dir = tempfile::tempdir().expect("rocksdb tempdir");
                let storage =
                    RocksDBStorage::open(temp_dir.path(), 512 * 1024 * 1024).expect("open rocksdb");
                let mut runtime = prepare_case(
                    storage,
                    "rocksdb",
                    total_rows,
                    target_owner_rows,
                    with_composite_index,
                );
                group.bench_function(id.clone(), move |b| {
                    let _keep_temp_dir_alive = &temp_dir;
                    b.iter(|| {
                        let rows = query_owner_page_with_storage(&mut runtime);
                        black_box(rows);
                    });
                });
            }

            #[cfg(not(feature = "rocksdb"))]
            if storage_filter == "rocksdb" {
                panic!("SCOPED_PAGINATION_STORAGE=rocksdb requires --features rocksdb");
            }
        }
    }

    group.finish();
}

criterion_group!(benches, scoped_owner_page);
criterion_main!(benches);
