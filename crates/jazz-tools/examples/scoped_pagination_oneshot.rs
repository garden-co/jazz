use std::collections::HashMap;
use std::time::{Duration, Instant};

use futures::executor::block_on;
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

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
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
        AppId::from_name("scoped-pagination-oneshot"),
        "dev",
        "main",
    )
    .expect("schema manager");
    RuntimeCore::new(schema_manager, storage, NoopScheduler)
}

fn seed_documents(runtime: &mut BenchRuntime<impl Storage>, total_rows: usize, owned_rows: usize) {
    let target_stride = (total_rows / owned_rows).max(1);
    let mut inserted_owned = 0usize;
    let progress_every = env_usize("SCOPED_PAGINATION_PROGRESS_EVERY", 100_000);
    let started = Instant::now();
    let mut last_progress = started;

    for index in 0..total_rows {
        let is_target = index % target_stride == 0 && inserted_owned < owned_rows;
        let owner_id = if is_target {
            inserted_owned += 1;
            TARGET_OWNER.to_string()
        } else {
            format!("user-{}", index % 10_000)
        };

        runtime
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

        let inserted = index + 1;
        if progress_every > 0 && inserted % progress_every == 0 {
            let now = Instant::now();
            let elapsed = now.duration_since(started);
            let interval = now.duration_since(last_progress);
            last_progress = now;
            eprintln!(
                "seeded {inserted}/{total_rows} rows ({inserted_owned} owned), elapsed {:?}, recent {:.0} rows/s",
                elapsed,
                progress_every as f64 / interval.as_secs_f64()
            );
        }
    }

    assert_eq!(inserted_owned, owned_rows);
}

fn query_owner_page(runtime: &mut BenchRuntime<impl Storage>) -> usize {
    let query = QueryBuilder::new("documents")
        .filter_eq("owner_id", Value::Text(TARGET_OWNER.to_string()))
        .order_by_desc("updated_at")
        .limit(PAGE_SIZE)
        .build();
    block_on(runtime.query(query, None))
        .expect("owner page query")
        .len()
}

fn measure_queries(runtime: &mut BenchRuntime<impl Storage>, iterations: usize) -> Vec<Duration> {
    let mut timings = Vec::with_capacity(iterations);
    for _ in 0..iterations {
        let started = Instant::now();
        let rows = query_owner_page(runtime);
        let elapsed = started.elapsed();
        assert_eq!(rows, PAGE_SIZE);
        timings.push(elapsed);
    }
    timings
}

fn print_timings(timings: &[Duration]) {
    let mut millis: Vec<f64> = timings
        .iter()
        .map(|duration| duration.as_secs_f64() * 1000.0)
        .collect();
    millis.sort_by(|left, right| left.partial_cmp(right).unwrap());
    let avg = millis.iter().sum::<f64>() / millis.len() as f64;
    let p50 = millis[millis.len() / 2];
    let p95 = millis[((millis.len() as f64 * 0.95).ceil() as usize).saturating_sub(1)];
    println!(
        "query timings: avg={avg:.3}ms p50={p50:.3}ms p95={p95:.3}ms min={:.3}ms max={:.3}ms",
        millis[0],
        millis[millis.len() - 1]
    );
}

fn run<S: Storage>(storage: S, storage_label: &str) {
    let total_rows = env_usize("SCOPED_PAGINATION_TOTAL_ROWS", 10_000_000);
    let owned_rows = env_usize("SCOPED_PAGINATION_TARGET_OWNER_ROWS", 10_000);
    let iterations = env_usize("SCOPED_PAGINATION_QUERY_ITERATIONS", 20);
    let case = std::env::var("SCOPED_PAGINATION_CASE")
        .unwrap_or_else(|_| "composite_owner_updated".to_string());
    let with_composite_index = match case.as_str() {
        "baseline_single_column" => false,
        "composite_owner_updated" => true,
        other => panic!("unknown SCOPED_PAGINATION_CASE={other}"),
    };

    println!(
        "running storage={storage_label} case={case} total_rows={total_rows} owned_rows={owned_rows}"
    );
    let mut runtime = create_runtime(storage, with_composite_index);

    let seed_start = Instant::now();
    seed_documents(&mut runtime, total_rows, owned_rows);
    println!("seed complete in {:?}", seed_start.elapsed());

    let timings = measure_queries(&mut runtime, iterations);
    print_timings(&timings);
}

fn main() {
    let storage = std::env::var("SCOPED_PAGINATION_STORAGE").unwrap_or_else(|_| "memory".into());
    match storage.as_str() {
        "memory" => run(MemoryStorage::new(), "memory"),
        "rocksdb" => {
            #[cfg(feature = "rocksdb")]
            {
                let temp_dir = tempfile::tempdir().expect("rocksdb tempdir");
                let storage =
                    RocksDBStorage::open(temp_dir.path(), 512 * 1024 * 1024).expect("open rocksdb");
                run(storage, "rocksdb");
            }
            #[cfg(not(feature = "rocksdb"))]
            panic!("SCOPED_PAGINATION_STORAGE=rocksdb requires --features rocksdb");
        }
        other => panic!("unknown SCOPED_PAGINATION_STORAGE={other}"),
    }
}
