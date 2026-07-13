//! Criterion benchmarks for the server-side row-history lookup used by writes.
//!
//! The benchmark drives the same public `QueryManager::process` entry point
//! that a server uses for an incoming client row batch. It compares fresh
//! inserts and same-schema updates with migrated deletes, varying history
//! depth and the number of visible schema branches. Setup is outside the
//! measured section so the result focuses on processing one write.

use std::collections::HashMap;
use std::sync::Arc;

use criterion::{
    BatchSize, BenchmarkId, Criterion, Throughput, black_box, criterion_group, criterion_main,
};
use jazz_tools::metadata::{DeleteKind, MetadataKey, RowProvenance, row_provenance_metadata};
use jazz_tools::object::{BranchName, ObjectId};
use jazz_tools::query_manager::encoding::encode_row;
use jazz_tools::query_manager::manager::QueryManager;
use jazz_tools::query_manager::policy::PolicyExpr;
use jazz_tools::query_manager::session::Session;
use jazz_tools::query_manager::types::{
    ColumnType, ComposedBranchName, RowDescriptor, Schema, SchemaBuilder, SchemaHash,
    TablePolicies, TableSchema, Value,
};
use jazz_tools::row_histories::{RowState, StoredRowBatch};
use jazz_tools::schema_manager::generate_lens;
use jazz_tools::storage::{MemoryStorage, RowLocator, Storage};
use jazz_tools::sync_manager::{ClientId, InboxEntry, Source, SyncManager, SyncPayload};
use jazz_tools::test_support::{apply_test_row_batch, persist_test_schema};

const TABLE: &str = "users";
const ENV: &str = "dev";
const USER_BRANCH: &str = "main";
const USER_ID: &str = "benchmark-user";

struct PendingWrite {
    manager: QueryManager,
    storage: MemoryStorage,
}

fn allow_all_policies() -> TablePolicies {
    TablePolicies::new()
        .with_select(PolicyExpr::True)
        .with_insert(PolicyExpr::True)
        .with_update(Some(PolicyExpr::True), PolicyExpr::True)
        .with_delete(PolicyExpr::True)
}

fn schema_v1() -> Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder(TABLE)
                .column("id", ColumnType::Uuid)
                .column("name", ColumnType::Text),
        )
        .build()
}

fn schema_v2() -> Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder(TABLE)
                .column("id", ColumnType::Uuid)
                .column("name", ColumnType::Text)
                .policies(allow_all_policies()),
        )
        .build()
}

fn schema_hash(schema: &Schema) -> SchemaHash {
    SchemaHash::compute(schema)
}

fn branch_name(schema_hash: SchemaHash, user_branch: &str) -> BranchName {
    ComposedBranchName::new(ENV, schema_hash, user_branch).to_branch_name()
}

fn row_locator_metadata(schema_hash: SchemaHash) -> HashMap<String, String> {
    HashMap::from([
        (MetadataKey::Table.to_string(), TABLE.to_string()),
        (
            MetadataKey::OriginSchemaHash.to_string(),
            schema_hash.to_string(),
        ),
    ])
}

fn row_data(descriptor: &RowDescriptor, row_id: ObjectId, name: &str) -> Vec<u8> {
    encode_row(
        descriptor,
        &[Value::Uuid(row_id), Value::Text(name.to_string())],
    )
    .expect("benchmark row should encode")
}

fn row_metadata(
    provenance: &RowProvenance,
    delete_kind: Option<DeleteKind>,
) -> HashMap<String, String> {
    row_provenance_metadata(provenance, delete_kind)
        .into_iter()
        .collect()
}

fn seed_history(
    storage: &mut MemoryStorage,
    schema: &Schema,
    origin_hash: SchemaHash,
    row_id: ObjectId,
    depth: usize,
    fanout: usize,
) -> StoredRowBatch {
    let descriptor = &schema[&TABLE.into()].columns;
    let locator = RowLocator {
        table: TABLE.to_string().into(),
        origin_schema_hash: Some(origin_hash),
    };
    storage
        .put_row_locator(row_id, Some(&locator))
        .expect("benchmark row locator should persist");

    let mut latest: Option<StoredRowBatch> = None;
    for branch_index in 0..fanout {
        let branch = branch_name(origin_hash, &format!("history-{branch_index}"));
        let mut parents = Vec::new();
        for version in 0..depth {
            let provenance = match latest.as_ref() {
                Some(previous) => RowProvenance::for_update(
                    &previous.row_provenance(),
                    USER_ID,
                    version as u64 + 2,
                ),
                None => RowProvenance::for_insert(USER_ID, 1),
            };
            let row = StoredRowBatch::new(
                row_id,
                branch.as_str(),
                parents.iter().copied(),
                row_data(descriptor, row_id, &format!("version-{version}")),
                provenance.clone(),
                row_metadata(&provenance, None),
                RowState::VisibleDirect,
                None,
            );
            let batch_id = row.batch_id();
            apply_test_row_batch(storage, row_id, branch.as_str(), row.clone())
                .expect("benchmark history row should apply");
            parents = vec![batch_id];
            latest = Some(row);
        }
    }

    latest.expect("benchmark history must contain at least one row")
}

fn setup_manager(
    origin_schema: &Schema,
    current_schema: &Schema,
) -> (QueryManager, MemoryStorage, ClientId) {
    let mut storage = MemoryStorage::new();
    let lens = generate_lens(origin_schema, current_schema);
    let origin_hash = schema_hash(origin_schema);
    let current_hash = schema_hash(current_schema);
    persist_test_schema(&mut storage, origin_schema);
    persist_test_schema(&mut storage, current_schema);

    let mut manager = QueryManager::new(SyncManager::new());
    manager.set_current_schema(current_schema.clone(), ENV, USER_BRANCH);
    manager.add_live_schema(origin_schema.clone());
    manager.register_lens(lens);
    manager.set_known_schemas(Arc::new(HashMap::from([
        (origin_hash, origin_schema.clone()),
        (current_hash, current_schema.clone()),
    ])));

    let client_id = ClientId::new();
    manager.sync_manager_mut().add_client(client_id);
    manager
        .sync_manager_mut()
        .set_client_session(client_id, Session::new(USER_ID));

    manager.process(&mut storage);
    let _ = manager.sync_manager_mut().take_outbox();
    (manager, storage, client_id)
}

fn pending_write(
    mut manager: QueryManager,
    storage: MemoryStorage,
    client_id: ClientId,
    payload: SyncPayload,
) -> PendingWrite {
    manager.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Client(client_id),
        payload,
    });
    PendingWrite { manager, storage }
}

fn fresh_insert() -> PendingWrite {
    let origin = schema_v1();
    let current = schema_v2();
    let (manager, storage, client_id) = setup_manager(&origin, &current);
    let current_hash = schema_hash(&current);
    let row_id = ObjectId::new();
    let provenance = RowProvenance::for_insert(USER_ID, 1);
    let row = StoredRowBatch::new(
        row_id,
        branch_name(current_hash, USER_BRANCH).as_str(),
        [],
        row_data(&current[&TABLE.into()].columns, row_id, "fresh"),
        provenance.clone(),
        row_metadata(&provenance, None),
        RowState::VisibleDirect,
        None,
    );
    pending_write(
        manager,
        storage,
        client_id,
        SyncPayload::RowBatchCreated {
            metadata: Some(jazz_tools::sync_manager::RowMetadata {
                id: row_id,
                metadata: row_locator_metadata(current_hash),
            }),
            row,
        },
    )
}

fn same_schema_update(depth: usize) -> PendingWrite {
    let origin = schema_v1();
    let current = schema_v2();
    let (manager, mut storage, client_id) = setup_manager(&origin, &current);
    let current_hash = schema_hash(&current);
    let row_id = ObjectId::new();
    let previous = seed_history(&mut storage, &current, current_hash, row_id, depth, 1);
    let provenance = RowProvenance::for_update(&previous.row_provenance(), USER_ID, 1000);
    let row = StoredRowBatch::new(
        row_id,
        branch_name(current_hash, "history-0").as_str(),
        [previous.batch_id()],
        row_data(
            &current[&TABLE.into()].columns,
            row_id,
            "same-schema-update",
        ),
        provenance.clone(),
        row_metadata(&provenance, None),
        RowState::VisibleDirect,
        None,
    );
    pending_write(
        manager,
        storage,
        client_id,
        SyncPayload::RowBatchCreated {
            metadata: Some(jazz_tools::sync_manager::RowMetadata {
                id: row_id,
                metadata: row_locator_metadata(current_hash),
            }),
            row,
        },
    )
}

fn migrated_delete(depth: usize, fanout: usize) -> PendingWrite {
    let origin = schema_v1();
    let current = schema_v2();
    let (manager, mut storage, client_id) = setup_manager(&origin, &current);
    let origin_hash = schema_hash(&origin);
    let current_hash = schema_hash(&current);
    let row_id = ObjectId::new();
    let previous = seed_history(&mut storage, &origin, origin_hash, row_id, depth, fanout);
    let provenance = RowProvenance::for_update(&previous.row_provenance(), USER_ID, 1000);
    let row = StoredRowBatch::new(
        row_id,
        branch_name(current_hash, USER_BRANCH).as_str(),
        [],
        // The schemas intentionally have the same row layout. This isolates
        // branch/history work from lens transformation cost.
        row_data(&origin[&TABLE.into()].columns, row_id, "migrated-delete"),
        provenance.clone(),
        row_metadata(&provenance, Some(DeleteKind::Soft)),
        RowState::VisibleDirect,
        None,
    );
    pending_write(
        manager,
        storage,
        client_id,
        SyncPayload::RowBatchCreated {
            metadata: Some(jazz_tools::sync_manager::RowMetadata {
                id: row_id,
                metadata: row_locator_metadata(origin_hash),
            }),
            row,
        },
    )
}

fn process_pending(mut pending: PendingWrite) {
    pending.manager.process(&mut pending.storage);
    black_box(pending.manager.sync_manager_mut().take_outbox());
}

fn server_row_history_paths(c: &mut Criterion) {
    let mut group = c.benchmark_group("server_write_path/row_history");
    group.throughput(Throughput::Elements(1));

    group.bench_function("fresh_insert", |b| {
        b.iter_batched(fresh_insert, process_pending, BatchSize::SmallInput);
    });

    for depth in [1usize, 10, 100, 1_000] {
        group.bench_with_input(
            BenchmarkId::new("same_schema_update/history_depth", depth),
            &depth,
            |b, &depth| {
                b.iter_batched(
                    || same_schema_update(depth),
                    process_pending,
                    BatchSize::SmallInput,
                );
            },
        );
        group.bench_with_input(
            BenchmarkId::new("migrated_delete/history_depth", depth),
            &depth,
            |b, &depth| {
                b.iter_batched(
                    || migrated_delete(depth, 1),
                    process_pending,
                    BatchSize::SmallInput,
                );
            },
        );
    }

    for fanout in [1usize, 4, 16, 64] {
        group.bench_with_input(
            BenchmarkId::new("migrated_delete/branch_fanout", fanout),
            &fanout,
            |b, &fanout| {
                b.iter_batched(
                    || migrated_delete(1, fanout),
                    process_pending,
                    BatchSize::SmallInput,
                );
            },
        );
    }

    group.finish();
}

criterion_group! {
    name = benches;
    config = Criterion::default().sample_size(10);
    targets = server_row_history_paths
}
criterion_main!(benches);
