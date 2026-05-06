//! Server subscription authorization-scope benchmarks.
//!
//! Models a downstream client subscribing to a large `useAll(...).limit(n)`
//! query with row-level select policies. The initial server scope computation
//! used to re-run visibility checks for output tuples and sync-scope tuples
//! separately, which shows up as main-thread WASM work in browser profiles.

use std::collections::HashMap;
use std::sync::Arc;

use criterion::{BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main};
use jazz_tools::query_manager::manager::QueryManager;
use jazz_tools::query_manager::policy::PolicyExpr;
use jazz_tools::query_manager::query::QueryBuilder;
use jazz_tools::query_manager::session::Session;
use jazz_tools::query_manager::types::{
    ColumnDescriptor, ColumnType, RowDescriptor, Schema, SchemaHash, TableName, TablePolicies,
    TableSchema, Value,
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

fn wide_schema(schema_index: usize, extra_columns: usize) -> Schema {
    let mut schema = Schema::new();
    let mut columns = vec![
        ColumnDescriptor::new("owner_id", ColumnType::Text),
        ColumnDescriptor::new("name", ColumnType::Text),
        ColumnDescriptor::new("score", ColumnType::Integer),
    ];

    for column_index in 0..extra_columns {
        columns.push(ColumnDescriptor::new(
            format!("metadata_{schema_index}_{column_index}"),
            ColumnType::Text,
        ));
    }

    let descriptor = RowDescriptor::new(columns);
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

fn setup_with_wide_schema_catalogue(
    row_count: usize,
    known_schema_count: usize,
    extra_columns: usize,
) -> (QueryManager, MemoryStorage) {
    let current_schema = wide_schema(0, extra_columns);
    let mut manager = QueryManager::new(SyncManager::new());
    manager
        .set_catalogue_app_id(AppId::from_name("authorization-scope-wide-catalogue").to_string());
    manager.set_current_schema(current_schema.clone(), "dev", "main");

    let mut known_schemas = HashMap::new();
    known_schemas.insert(SchemaHash::compute(&current_schema), current_schema);
    for schema_index in 1..known_schema_count {
        let schema = wide_schema(schema_index, extra_columns);
        known_schemas.insert(SchemaHash::compute(&schema), schema);
    }
    manager.set_known_schemas(Arc::new(known_schemas));

    let mut storage = MemoryStorage::new();
    for index in 0..row_count {
        let mut values = vec![
            Value::Text(USER_ID.to_string()),
            Value::Text(format!("Item {index}")),
            Value::Integer(index as i32),
        ];
        values.extend(
            (0..extra_columns)
                .map(|column_index| Value::Text(format!("item-{index}-metadata-{column_index}"))),
        );
        manager
            .insert(&mut storage, "items", &values)
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

fn subscribe_many(
    manager: &mut QueryManager,
    storage: &mut MemoryStorage,
    subscription_count: usize,
    limit: usize,
) {
    for subscription_index in 0..subscription_count {
        let client_id = ClientId::new();
        manager
            .sync_manager_mut()
            .add_client_with_storage(storage, client_id);
        let _ = manager.sync_manager_mut().take_outbox();

        let query = QueryBuilder::new("items").limit(limit).build();
        manager.sync_manager_mut().push_inbox(InboxEntry {
            source: Source::Client(client_id),
            payload: SyncPayload::QuerySubscription {
                query_id: QueryId(subscription_index as u64 + 1),
                query: Box::new(query),
                session: Some(Session::new(USER_ID)),
                propagation: QueryPropagation::Full,
                policy_context_tables: vec![],
            },
        });
    }

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

fn initial_authorized_scope_with_wide_schema_catalogue(c: &mut Criterion) {
    let mut group = c.benchmark_group("server_authorization_scope/wide_schema_catalogue");

    for (row_count, known_schema_count, extra_columns) in [(2_000usize, 500usize, 256usize)] {
        group.throughput(Throughput::Elements(row_count as u64));
        group.bench_with_input(
            BenchmarkId::new(
                format!("{known_schema_count}_schemas_{extra_columns}_extra_columns"),
                row_count,
            ),
            &(row_count, known_schema_count, extra_columns),
            |b, &(row_count, known_schema_count, extra_columns)| {
                b.iter_batched(
                    || {
                        setup_with_wide_schema_catalogue(
                            row_count,
                            known_schema_count,
                            extra_columns,
                        )
                    },
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

fn many_initial_authorized_scopes_share_schema_context(c: &mut Criterion) {
    let mut group = c.benchmark_group("server_authorization_scope/many_subscriptions");

    for (row_count, known_schema_count, extra_columns, subscription_count) in
        [(1_000usize, 500usize, 256usize, 25usize)]
    {
        group.throughput(Throughput::Elements(
            (row_count * subscription_count) as u64,
        ));
        group.bench_with_input(
            BenchmarkId::new(
                format!(
                    "{subscription_count}_subscriptions_{known_schema_count}_schemas_{extra_columns}_extra_columns"
                ),
                row_count,
            ),
            &(row_count, known_schema_count, extra_columns, subscription_count),
            |b, &(row_count, known_schema_count, extra_columns, subscription_count)| {
                b.iter_batched(
                    || {
                        setup_with_wide_schema_catalogue(
                            row_count,
                            known_schema_count,
                            extra_columns,
                        )
                    },
                    |(mut manager, mut storage)| {
                        subscribe_many(&mut manager, &mut storage, subscription_count, row_count);
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
    targets = initial_authorized_scope, initial_authorized_scope_with_wide_schema_catalogue, many_initial_authorized_scopes_share_schema_context
}
criterion_main!(benches);
