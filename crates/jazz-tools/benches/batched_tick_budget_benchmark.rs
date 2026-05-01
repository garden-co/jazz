use criterion::{BenchmarkId, Criterion, black_box, criterion_group, criterion_main};
use jazz_tools::query_manager::types::{ColumnType, Schema, SchemaBuilder, TableSchema};
use jazz_tools::runtime_core::{NoopScheduler, RuntimeCore};
use jazz_tools::schema_manager::{AppId, SchemaManager};
use jazz_tools::storage::MemoryStorage;
use jazz_tools::sync_manager::{
    DurabilityTier, InboxEntry, QueryId, ServerId, Source, SyncManager, SyncPayload,
};
use std::time::Duration;

fn bench_schema() -> Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("items")
                .column("id", ColumnType::Uuid)
                .column("title", ColumnType::Text),
        )
        .build()
}

fn runtime_with_parked_query_settled(count: u64) -> RuntimeCore<MemoryStorage, NoopScheduler> {
    let schema_manager = SchemaManager::new(
        SyncManager::new(),
        bench_schema(),
        AppId::from_name("batched-tick-budget-bench"),
        "dev",
        "main",
    )
    .expect("bench schema should initialize");
    let mut core = RuntimeCore::new(schema_manager, MemoryStorage::new(), NoopScheduler);
    let server_id = ServerId::new();

    for through_seq in 1..=count {
        core.park_sync_message(InboxEntry {
            source: Source::Server(server_id),
            payload: SyncPayload::QuerySettled {
                query_id: QueryId(1),
                tier: DurabilityTier::EdgeServer,
                through_seq,
            },
        });
    }

    core
}

fn parked_sync_backlog_single_tick(c: &mut Criterion) {
    let mut group = c.benchmark_group("batched_tick/parked_sync_backlog_single_tick");

    for count in [1_000_u64, 10_000, 100_000] {
        group.bench_with_input(BenchmarkId::from_parameter(count), &count, |b, &count| {
            b.iter_custom(|iters| {
                let mut total = Duration::ZERO;
                for _ in 0..iters {
                    let mut core = runtime_with_parked_query_settled(count);
                    let start = std::time::Instant::now();
                    core.batched_tick();
                    total += start.elapsed();
                    black_box(core);
                }
                total
            });
        });
    }

    group.finish();
}

criterion_group!(benches, parked_sync_backlog_single_tick);
criterion_main!(benches);
