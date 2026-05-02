//! Server subscription authorization-scope benchmarks.
//!
//! Models a downstream client subscribing to a large `useAll(...).limit(n)`
//! query with row-level select policies. The initial server scope computation
//! used to re-run visibility checks for output tuples and sync-scope tuples
//! separately, which shows up as main-thread WASM work in browser profiles.

use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use jazz_tools::query_manager::manager::QueryManager;
use jazz_tools::query_manager::policy::PolicyExpr;
use jazz_tools::query_manager::query::QueryBuilder;
use jazz_tools::query_manager::session::Session;
use jazz_tools::query_manager::types::{
    ColumnDescriptor, ColumnType, RowDescriptor, Schema, TableName, TablePolicies, TableSchema,
    Value,
};
use jazz_tools::schema_manager::AppId;
use jazz_tools::storage::MemoryStorage;
use jazz_tools::sync_manager::{
    ClientId, InboxEntry, QueryId, QueryPropagation, Source, SyncManager, SyncPayload,
};

const USER_ID: &str = "benchmark_user";

fn schema() -> Schema {
    let mut schema = Schema::new();
    let descriptor = RowDescriptor::new(vec![
        ColumnDescriptor::new("owner_id", ColumnType::Text),
        ColumnDescriptor::new("name", ColumnType::Text),
        ColumnDescriptor::new("score", ColumnType::Integer),
    ]);
    let policies = TablePolicies::new()
        .with_select(PolicyExpr::eq_session("owner_id", vec!["user_id".into()]));
    schema.insert(
        TableName::new("items"),
        TableSchema::with_policies(descriptor, policies),
    );
    schema
}

fn setup(row_count: usize) -> (QueryManager, MemoryStorage) {
    let mut manager = QueryManager::new(SyncManager::new());
    manager.set_catalogue_app_id(AppId::from_name("authorization-scope-bench").to_string());
    manager.set_current_schema(schema(), "dev", "main");
    let mut storage = MemoryStorage::new();

    for index in 0..row_count {
        manager
            .insert(
                &mut storage,
                "items",
                &[
                    Value::Text(USER_ID.to_string()),
                    Value::Text(format!("Item {index}")),
                    Value::Integer(index as i32),
                ],
            )
            .expect("insert benchmark item");
    }
    manager.process(&mut storage);
    let _ = manager.sync_manager_mut().take_outbox();

    (manager, storage)
}

fn subscribe_limit(manager: &mut QueryManager, storage: &mut MemoryStorage, limit: usize) {
    let client_id = ClientId::new();
    manager
        .sync_manager_mut()
        .add_client_with_storage(storage, client_id);
    let _ = manager.sync_manager_mut().take_outbox();

    let query = QueryBuilder::new("items").limit(limit).build();
    manager.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Client(client_id),
        payload: SyncPayload::QuerySubscription {
            query_id: QueryId(1),
            query: Box::new(query),
            session: Some(Session::new(USER_ID)),
            propagation: QueryPropagation::Full,
            policy_context_tables: vec![],
        },
    });

    manager.process(storage);
    black_box(manager.sync_manager_mut().take_outbox());
}

fn initial_authorized_scope(c: &mut Criterion) {
    let mut group = c.benchmark_group("server_authorization_scope/initial_limit");

    for row_count in [1_000usize, 2_000, 10_000] {
        group.throughput(Throughput::Elements(row_count as u64));
        group.bench_with_input(
            BenchmarkId::new("select_policy", row_count),
            &row_count,
            |b, &row_count| {
                b.iter_batched(
                    || setup(row_count),
                    |(mut manager, mut storage)| {
                        subscribe_limit(&mut manager, &mut storage, row_count);
                    },
                    criterion::BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = initial_authorized_scope
}
criterion_main!(benches);
