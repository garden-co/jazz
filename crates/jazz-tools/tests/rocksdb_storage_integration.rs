#![cfg(all(feature = "test", feature = "rocksdb"))]

mod support;

use std::collections::{BTreeSet, HashMap};
use std::time::Duration;

use jazz_tools::batch_fate::{
    BatchFate, BatchMode, CapturedFrontierMember, SealedBatchMember, SealedBatchSubmission,
};
use jazz_tools::catalogue::CatalogueEntry;
use jazz_tools::metadata::{MetadataKey, ObjectType, RowProvenance};
use jazz_tools::object::{BranchName, ObjectId};
use jazz_tools::query_manager::encoding::encode_row;
use jazz_tools::query_manager::types::{SchemaHash, TableName};
use jazz_tools::row_histories::{BatchId, RowState, StoredRowBatch, VisibleRowEntry};
use jazz_tools::schema_manager::encoding::encode_schema;
use jazz_tools::server::{TestingJwksServer, TestingServer};
use jazz_tools::storage::{RocksDBStorage, Storage};
use jazz_tools::sync_manager::DurabilityTier;
use jazz_tools::{
    AppContext, ClientStorage, ColumnType, JazzClient, QueryBuilder, SchemaBuilder, TableSchema,
    Value,
};
use support::{
    TestingClient, publish_allow_all_permissions, push_catalogue_in_memory, wait_for_query,
};
use tempfile::TempDir;

const READY_TIMEOUT: Duration = Duration::from_secs(30);

fn todos_schema() -> jazz_tools::Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("todos")
                .column("title", ColumnType::Text)
                .column("completed", ColumnType::Boolean),
        )
        .build()
}

fn multi_table_schema() -> jazz_tools::Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("todos")
                .column("title", ColumnType::Text)
                .column("completed", ColumnType::Boolean),
        )
        .table(
            TableSchema::builder("notes")
                .column("body", ColumnType::Text)
                .column("priority", ColumnType::Integer),
        )
        .build()
}

fn indexed_schema() -> jazz_tools::Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("products")
                .column("name", ColumnType::Text)
                .column("price", ColumnType::Double)
                .column("category", ColumnType::Text),
        )
        .build()
}

fn persist_schema<H: Storage>(storage: &mut H, schema: &jazz_tools::Schema) -> SchemaHash {
    let schema_hash = SchemaHash::compute(schema);
    storage
        .upsert_catalogue_entry(&CatalogueEntry {
            object_id: schema_hash.to_object_id(),
            metadata: HashMap::from([(
                MetadataKey::Type.to_string(),
                ObjectType::CatalogueSchema.to_string(),
            )]),
            content: encode_schema(schema),
        })
        .expect("persist schema catalogue entry");
    schema_hash
}

fn encode_todo_row(schema: &jazz_tools::Schema, title: &str, completed: bool) -> Vec<u8> {
    encode_row(
        &schema[&TableName::new("todos")].columns,
        &[Value::Text(title.to_string()), Value::Boolean(completed)],
    )
    .expect("encode todo row")
}

fn seed_rocksdb_sealed_batch_acceptance(
    storage: &mut RocksDBStorage,
    schema: &jazz_tools::Schema,
) -> (BatchId, ObjectId) {
    let schema_hash = persist_schema(storage, schema);
    let batch_id = BatchId::new();
    let row_id = ObjectId::new();
    let staged_row = StoredRowBatch::new_with_batch_id(
        batch_id,
        row_id,
        "main",
        Vec::<BatchId>::new(),
        encode_todo_row(schema, "recovered-transaction", false),
        RowProvenance::for_insert("alice".to_string(), 1_000),
        HashMap::new(),
        RowState::StagingPending,
        None,
    );

    storage
        .put_row_locator(
            row_id,
            Some(&jazz_tools::storage::RowLocator {
                table: "todos".into(),
                origin_schema_hash: Some(schema_hash),
            }),
        )
        .expect("persist row locator");
    storage
        .append_history_region_rows("todos", std::slice::from_ref(&staged_row))
        .expect("persist staged row");
    storage
        .upsert_sealed_batch_submission(&SealedBatchSubmission::new(
            batch_id,
            BatchMode::Transactional,
            BranchName::new("main"),
            vec![SealedBatchMember {
                object_id: row_id,
                row_digest: staged_row.content_digest(),
            }],
            Vec::new(),
        ))
        .expect("persist sealed submission");

    (batch_id, row_id)
}

fn seed_rocksdb_sealed_batch_frontier_conflict(
    storage: &mut RocksDBStorage,
    schema: &jazz_tools::Schema,
) -> (BatchId, String, ObjectId, ObjectId) {
    let schema_hash = persist_schema(storage, schema);
    let batch_id = BatchId::new();
    let target_branch = "dev-aaaaaaaaaaaa-main".to_string();
    let sibling_branch = "dev-bbbbbbbbbbbb-main".to_string();
    let existing_row_id = ObjectId::new();
    let conflicting_row_id = ObjectId::new();
    let staged_row_id = ObjectId::new();

    let existing_row = StoredRowBatch::new(
        existing_row_id,
        target_branch.as_str(),
        Vec::<BatchId>::new(),
        encode_todo_row(schema, "target-existing", false),
        RowProvenance::for_insert("alice".to_string(), 900),
        HashMap::new(),
        RowState::VisibleDirect,
        None,
    );
    let conflicting_row = StoredRowBatch::new(
        conflicting_row_id,
        sibling_branch.as_str(),
        Vec::<BatchId>::new(),
        encode_todo_row(schema, "sibling-existing", false),
        RowProvenance::for_insert("bob".to_string(), 950),
        HashMap::new(),
        RowState::VisibleDirect,
        None,
    );
    let staged_row = StoredRowBatch::new_with_batch_id(
        batch_id,
        staged_row_id,
        target_branch.as_str(),
        Vec::<BatchId>::new(),
        encode_todo_row(schema, "staged-conflict", false),
        RowProvenance::for_insert("alice".to_string(), 1_000),
        HashMap::new(),
        RowState::StagingPending,
        None,
    );

    for row_id in [existing_row_id, conflicting_row_id, staged_row_id] {
        storage
            .put_row_locator(
                row_id,
                Some(&jazz_tools::storage::RowLocator {
                    table: "todos".into(),
                    origin_schema_hash: Some(schema_hash),
                }),
            )
            .expect("persist row locator");
    }
    storage
        .append_history_region_rows(
            "todos",
            &[
                existing_row.clone(),
                conflicting_row.clone(),
                staged_row.clone(),
            ],
        )
        .expect("persist history rows");
    storage
        .upsert_visible_region_rows(
            "todos",
            &[
                VisibleRowEntry::rebuild(existing_row.clone(), std::slice::from_ref(&existing_row)),
                VisibleRowEntry::rebuild(
                    conflicting_row.clone(),
                    std::slice::from_ref(&conflicting_row),
                ),
            ],
        )
        .expect("persist visible frontier");
    storage
        .upsert_sealed_batch_submission(&SealedBatchSubmission::new(
            batch_id,
            BatchMode::Transactional,
            BranchName::new(target_branch.clone()),
            vec![SealedBatchMember {
                object_id: staged_row_id,
                row_digest: staged_row.content_digest(),
            }],
            vec![CapturedFrontierMember {
                object_id: existing_row_id,
                branch_name: BranchName::new(target_branch.clone()),
                batch_id: existing_row.batch_id(),
            }],
        ))
        .expect("persist conflicting sealed submission");

    (batch_id, target_branch, staged_row_id, existing_row_id)
}

async fn make_client(
    server: &TestingServer,
    schema: jazz_tools::Schema,
    user_id: &str,
    ready_table: &str,
) -> JazzClient {
    push_catalogue_in_memory(
        server.server_state(),
        server.app_id(),
        "dev",
        "main",
        std::slice::from_ref(&schema),
        &[],
    )
    .await
    .expect("push schema catalogue");

    let client = TestingClient::builder()
        .with_server(server)
        .with_schema(schema.clone())
        .with_user_id(user_id)
        .connect()
        .await;

    publish_allow_all_permissions(
        &server.base_url(),
        server.app_id(),
        server.admin_secret(),
        &schema,
    )
    .await;
    wait_for_query(
        &client,
        QueryBuilder::new(ready_table).build(),
        Some(DurabilityTier::EdgeServer),
        READY_TIMEOUT,
        format!("EdgeServer query readiness for {ready_table}"),
        |_| Some(()),
    )
    .await;

    client
}

/// Connects a client to a server that uses an external JWKS URL (where the
/// built-in `make_client_context_for_user` helper is unavailable).
async fn make_client_external_jwks(
    server: &TestingServer,
    schema: jazz_tools::Schema,
    user_id: &str,
    ready_table: &str,
) -> JazzClient {
    push_catalogue_in_memory(
        server.server_state(),
        server.app_id(),
        "dev",
        "main",
        std::slice::from_ref(&schema),
        &[],
    )
    .await
    .expect("push schema catalogue");

    let context = AppContext {
        app_id: server.app_id(),
        client_id: None,
        schema: schema.clone(),
        server_url: server.base_url(),
        data_dir: tempfile::TempDir::new().expect("temp client dir").keep(),
        storage: ClientStorage::Memory,
        jwt_token: Some(TestingServer::jwt_for_user(user_id)),
        backend_secret: None,
        admin_secret: None,
        sync_tracer: None,
    };

    let client = JazzClient::connect(context).await.expect("connect client");

    publish_allow_all_permissions(
        &server.base_url(),
        server.app_id(),
        server.admin_secret(),
        &schema,
    )
    .await;
    wait_for_query(
        &client,
        QueryBuilder::new(ready_table).build(),
        Some(DurabilityTier::EdgeServer),
        READY_TIMEOUT,
        format!("EdgeServer query readiness for {ready_table}"),
        |_| Some(()),
    )
    .await;

    client
}

/// Single entry point — all subtests run sequentially so only one RocksDB
/// server instance exists at a time (avoids file-descriptor exhaustion).
#[tokio::test]
async fn rocksdb_server_storage() {
    // --- shared-server subtests ---
    let server = TestingServer::builder()
        .with_rocksdb_storage()
        .start()
        .await;

    large_dataset_correctness(&server).await;
    update_and_delete(&server).await;
    deep_update_history(&server).await;
    multi_table_isolation(&server).await;
    index_queries(&server).await;

    server.shutdown().await;

    // --- restart subtests (need their own server lifecycle) ---
    restart_preserves_data().await;
    catalogue_entries_survive_restart().await;
    sealed_batch_acceptance_recovers_after_restart().await;
    sealed_batch_frontier_conflict_rejects_after_restart().await;
}

/// Alice creates 200 todos. Bob connects fresh and must see all 200 with
/// correct, unique titles.
///
/// ```text
/// alice ──create 200 todos──► server (rocksdb)
///                                 │
///                  bob connects and queries
///                                 │
///                                 └──► all 200 rows, correct titles
/// ```
async fn large_dataset_correctness(server: &TestingServer) {
    const ROW_COUNT: usize = 200;

    let schema = todos_schema();
    let alice = make_client(server, schema.clone(), "alice-bulk", "todos").await;

    let mut expected_titles: BTreeSet<String> = BTreeSet::new();
    for i in 0..(ROW_COUNT - 1) {
        let title = format!("todo-{i:03}");
        expected_titles.insert(title.clone());
        alice
            .create(
                "todos",
                HashMap::from([
                    ("title".to_string(), Value::Text(title)),
                    ("completed".to_string(), Value::Boolean(false)),
                ]),
            )
            .await
            .expect("create todo");
    }

    let final_title = format!("todo-{:03}", ROW_COUNT - 1);
    expected_titles.insert(final_title.clone());
    alice
        .create_persisted(
            "todos",
            HashMap::from([
                ("title".to_string(), Value::Text(final_title)),
                ("completed".to_string(), Value::Boolean(false)),
            ]),
            DurabilityTier::EdgeServer,
        )
        .await
        .expect("create final persisted todo");

    wait_for_query(
        &alice,
        QueryBuilder::new("todos").build(),
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(30),
        format!("alice sees {ROW_COUNT} todos"),
        |rows| (rows.len() == ROW_COUNT).then_some(()),
    )
    .await;

    let bob = make_client(server, schema, "bob-bulk", "todos").await;

    let bob_rows = wait_for_query(
        &bob,
        QueryBuilder::new("todos").build(),
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(30),
        format!("bob sees {ROW_COUNT} todos"),
        |rows| (rows.len() == ROW_COUNT).then_some(rows),
    )
    .await;

    let actual_titles: BTreeSet<String> = bob_rows
        .iter()
        .filter_map(|(_, cols)| match cols.first() {
            Some(Value::Text(t)) => Some(t.clone()),
            _ => None,
        })
        .collect();

    assert_eq!(
        actual_titles, expected_titles,
        "every title must be present"
    );

    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
}

/// Create, update, then delete rows. A fresh client must see only the
/// surviving, updated state.
///
/// ```text
/// alice ──create 5──► update 3 titles──► delete 2──► server (rocksdb)
///                                                        │
///                                         bob connects and queries
///                                                        │
///                                         3 rows with updated titles
/// ```
async fn update_and_delete(server: &TestingServer) {
    let schema = todos_schema();
    let alice = make_client(server, schema.clone(), "alice-crud", "todos").await;

    let mut ids = Vec::new();
    for i in 0..5u32 {
        let (id, _) = alice
            .create(
                "todos",
                HashMap::from([
                    ("title".to_string(), Value::Text(format!("original-{i}"))),
                    ("completed".to_string(), Value::Boolean(false)),
                ]),
            )
            .await
            .expect("create todo");
        ids.push(id);
    }

    wait_for_query(
        &alice,
        QueryBuilder::new("todos").build(),
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(30),
        "alice sees new todos after bulk",
        |rows| {
            ids.iter()
                .all(|id| rows.iter().any(|(rid, _)| rid == id))
                .then_some(())
        },
    )
    .await;

    // Update first 3.
    for (i, id) in ids.iter().take(3).enumerate() {
        alice
            .update(
                *id,
                vec![("title".to_string(), Value::Text(format!("updated-{i}")))],
            )
            .await
            .expect("update todo");
    }

    // Delete last 2.
    for id in ids.iter().skip(3) {
        alice.delete(*id).await.expect("delete todo");
    }

    // Wait for alice to see the deletes reflected.
    wait_for_query(
        &alice,
        QueryBuilder::new("todos").build(),
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(30),
        "alice sees deletes applied",
        |rows| (!rows.iter().any(|(id, _)| ids[3..].contains(id))).then_some(()),
    )
    .await;

    // Bob connects fresh.
    let bob = make_client(server, schema, "bob-crud", "todos").await;

    let bob_rows = wait_for_query(
        &bob,
        QueryBuilder::new("todos").build(),
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(30),
        "bob sees updated state",
        |rows| {
            let has_all_updated = ids[..3]
                .iter()
                .all(|id| rows.iter().any(|(rid, _)| rid == id));
            let has_no_deleted = !rows.iter().any(|(id, _)| ids[3..].contains(id));
            (has_all_updated && has_no_deleted).then_some(rows)
        },
    )
    .await;

    let titles: BTreeSet<String> = bob_rows
        .iter()
        .filter(|(id, _)| ids[..3].contains(id))
        .filter_map(|(_, cols)| match cols.first() {
            Some(Value::Text(t)) => Some(t.clone()),
            _ => None,
        })
        .collect();
    let expected: BTreeSet<String> = (0..3).map(|i| format!("updated-{i}")).collect();
    assert_eq!(titles, expected, "bob should see updated titles");

    let bob_ids: BTreeSet<_> = bob_rows.iter().map(|(id, _)| *id).collect();
    for id in ids.iter().skip(3) {
        assert!(!bob_ids.contains(id), "deleted row {id:?} should be absent");
    }

    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
}

/// Many updates to the same object. A fresh client must resolve only the
/// latest value.
///
/// ```text
/// alice ──create + update ×200──► server (rocksdb)
///                                     │
///                      bob connects and queries
///                                     │
///                                     └──► latest title only
/// ```
async fn deep_update_history(server: &TestingServer) {
    const UPDATE_COUNT: usize = 200;

    let schema = todos_schema();
    let alice = make_client(server, schema.clone(), "alice-deep", "todos").await;

    let (todo_id, _) = alice
        .create_persisted(
            "todos",
            HashMap::from([
                ("title".to_string(), Value::Text("revision-000".to_string())),
                ("completed".to_string(), Value::Boolean(false)),
            ]),
            DurabilityTier::EdgeServer,
        )
        .await
        .expect("create persisted todo");

    // This test is about replaying a deep server history for a fresh client,
    // not about transport reordering. Make each revision edge-durable before
    // sending the next so Bob observes one causal history.
    for rev in 1..=UPDATE_COUNT {
        alice
            .update_persisted(
                todo_id,
                vec![(
                    "title".to_string(),
                    Value::Text(format!("revision-{rev:03}")),
                )],
                DurabilityTier::EdgeServer,
            )
            .await
            .expect("persist todo update");
    }

    let final_title = format!("revision-{UPDATE_COUNT:03}");

    wait_for_query(
        &alice,
        QueryBuilder::new("todos").build(),
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(30),
        format!("alice sees final title {final_title}"),
        |rows| {
            (rows.iter().any(|(id, cols)| {
                *id == todo_id && cols.first() == Some(&Value::Text(final_title.clone()))
            }))
            .then_some(())
        },
    )
    .await;

    let bob = make_client(server, schema, "bob-deep", "todos").await;

    let bob_row = wait_for_query(
        &bob,
        QueryBuilder::new("todos").build(),
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(30),
        format!("bob sees final title {final_title}"),
        |rows| {
            rows.iter()
                .find(|(id, cols)| {
                    *id == todo_id && cols.first() == Some(&Value::Text(final_title.clone()))
                })
                .cloned()
        },
    )
    .await;

    assert_eq!(bob_row.0, todo_id);
    assert_eq!(bob_row.1[0], Value::Text(final_title));

    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
}

/// Rows in different tables are isolated: creating in "todos" does not leak
/// into "notes" and vice versa.
///
/// ```text
/// alice ──create 5 todos + 3 notes──► server (rocksdb)
///                                         │
///                          bob queries each table separately
///                                         │
///                          todos: 5   notes: 3
/// ```
async fn multi_table_isolation(server: &TestingServer) {
    let schema = multi_table_schema();
    let alice = make_client(server, schema.clone(), "alice-multi", "todos").await;

    let mut todo_ids = Vec::new();
    for i in 0..5 {
        let (id, _) = alice
            .create(
                "todos",
                HashMap::from([
                    ("title".to_string(), Value::Text(format!("mt-todo-{i}"))),
                    ("completed".to_string(), Value::Boolean(false)),
                ]),
            )
            .await
            .expect("create todo");
        todo_ids.push(id);
    }

    let mut note_ids = Vec::new();
    for i in 0..3 {
        let (id, _) = alice
            .create(
                "notes",
                HashMap::from([
                    ("body".to_string(), Value::Text(format!("mt-note-{i}"))),
                    ("priority".to_string(), Value::Integer(i as i32)),
                ]),
            )
            .await
            .expect("create note");
        note_ids.push(id);
    }

    wait_for_query(
        &alice,
        QueryBuilder::new("notes").build(),
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(30),
        "alice sees 3 notes",
        |rows| {
            note_ids
                .iter()
                .all(|id| rows.iter().any(|(rid, _)| rid == id))
                .then_some(())
        },
    )
    .await;

    let bob = make_client(server, schema, "bob-multi", "todos").await;

    let todos = wait_for_query(
        &bob,
        QueryBuilder::new("todos").build(),
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(30),
        "bob sees multi-table todos",
        |rows| {
            todo_ids
                .iter()
                .all(|id| rows.iter().any(|(rid, _)| rid == id))
                .then_some(rows)
        },
    )
    .await;

    let notes = wait_for_query(
        &bob,
        QueryBuilder::new("notes").build(),
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(30),
        "bob sees multi-table notes",
        |rows| {
            note_ids
                .iter()
                .all(|id| rows.iter().any(|(rid, _)| rid == id))
                .then_some(rows)
        },
    )
    .await;

    // Verify correct content in each table.
    for (id, cols) in &todos {
        if todo_ids.contains(id) {
            if let Some(Value::Text(t)) = cols.first() {
                assert!(t.starts_with("mt-todo-"), "unexpected title in todos: {t}");
            }
        }
    }
    for (id, cols) in &notes {
        if note_ids.contains(id) {
            if let Some(Value::Text(t)) = cols.first() {
                assert!(t.starts_with("mt-note-"), "unexpected body in notes: {t}");
            }
        }
    }

    // Notes should not contain todo IDs and vice versa.
    let note_row_ids: BTreeSet<_> = notes.iter().map(|(id, _)| *id).collect();
    for id in &todo_ids {
        assert!(
            !note_row_ids.contains(id),
            "todo id {id:?} should not appear in notes"
        );
    }
    let todo_row_ids: BTreeSet<_> = todos.iter().map(|(id, _)| *id).collect();
    for id in &note_ids {
        assert!(
            !todo_row_ids.contains(id),
            "note id {id:?} should not appear in todos"
        );
    }

    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
}

/// Creates products with varying prices and categories, then verifies that
/// filter_eq and filter_gt return correct results through the server.
///
/// ```text
/// alice ──create 20 products──► server (rocksdb)
///                                   │
///                    bob queries with filters
///                                   │
///              filter_eq(category, "electronics") → 10 rows
///              filter_gt(price, 150.0) → 4 rows
/// ```
async fn index_queries(server: &TestingServer) {
    let schema = indexed_schema();
    let alice = make_client(server, schema.clone(), "alice-index", "products").await;

    let mut product_ids = Vec::new();
    for i in 0..20u32 {
        let category = if i % 2 == 0 { "electronics" } else { "books" };
        let (id, _) = alice
            .create(
                "products",
                HashMap::from([
                    ("name".to_string(), Value::Text(format!("product-{i:02}"))),
                    ("price".to_string(), Value::Double(i as f64 * 10.0)),
                    ("category".to_string(), Value::Text(category.to_string())),
                ]),
            )
            .await
            .expect("create product");
        product_ids.push(id);
    }

    wait_for_query(
        &alice,
        QueryBuilder::new("products").build(),
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(30),
        "alice sees 20 products",
        |rows| {
            product_ids
                .iter()
                .all(|id| rows.iter().any(|(rid, _)| rid == id))
                .then_some(())
        },
    )
    .await;

    let bob = make_client(server, schema, "bob-index", "products").await;

    // Wait for bob to see all products before filtering.
    wait_for_query(
        &bob,
        QueryBuilder::new("products").build(),
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(30),
        "bob sees 20 products",
        |rows| {
            product_ids
                .iter()
                .all(|id| rows.iter().any(|(rid, _)| rid == id))
                .then_some(())
        },
    )
    .await;

    // Exact match on category.
    let electronics = wait_for_query(
        &bob,
        QueryBuilder::new("products")
            .filter_eq("category", Value::Text("electronics".to_string()))
            .build(),
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(30),
        "bob sees 10 electronics",
        |rows| (rows.len() == 10).then_some(rows),
    )
    .await;
    for (_, cols) in &electronics {
        if let Some(Value::Text(cat)) = cols.get(2) {
            assert_eq!(
                cat, "electronics",
                "filter_eq should only return electronics"
            );
        }
    }

    // Greater-than filter: products with price > 150.0.
    // Prices are i*10 for i in 0..20 (0, 10, ..., 190).
    // price > 150: 160, 170, 180, 190 → 4 products.
    let expensive = wait_for_query(
        &bob,
        QueryBuilder::new("products")
            .filter_gt("price", Value::Double(150.0))
            .build(),
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(30),
        "bob sees 4 expensive products",
        |rows| (rows.len() == 4).then_some(rows),
    )
    .await;
    for (_, cols) in &expensive {
        if let Some(Value::Double(p)) = cols.get(1) {
            assert!(*p > 150.0, "price {p} should be > 150.0");
        }
    }

    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
}

// ---------------------------------------------------------------------------
// Restart tests — need their own server lifecycle
// ---------------------------------------------------------------------------

/// Alice creates rows, the server shuts down and restarts from the same
/// data_dir. Bob connects to the restarted server and must see pre-restart
/// data. Alice then creates more rows and Bob sees the combined set.
///
/// ```text
/// alice ──create 10──► server₁ (rocksdb, data_dir)
///                          │
///                      server₁ stops
///                          │
///                server₂ starts (same data_dir)
///                          │
///          bob connects ──► sees 10 pre-restart rows
///          alice creates 5 more ──► bob sees all 15
/// ```
async fn restart_preserves_data() {
    const BEFORE_COUNT: usize = 10;
    const AFTER_COUNT: usize = 5;

    let data_dir = TempDir::new().expect("temp data dir");
    let jwks = TestingJwksServer::start().await;
    let schema = todos_schema();

    // --- server₁ ---
    let server1 = TestingServer::builder()
        .with_rocksdb_storage()
        .with_data_dir(data_dir.path())
        .with_jwks_url(jwks.endpoint())
        .start()
        .await;

    let alice = make_client_external_jwks(&server1, schema.clone(), "alice-restart", "todos").await;

    let mut before_ids = Vec::new();
    for i in 0..BEFORE_COUNT {
        let (id, _) = alice
            .create(
                "todos",
                HashMap::from([
                    (
                        "title".to_string(),
                        Value::Text(format!("before-restart-{i:02}")),
                    ),
                    ("completed".to_string(), Value::Boolean(false)),
                ]),
            )
            .await
            .expect("create before restart");
        before_ids.push(id);
    }

    wait_for_query(
        &alice,
        QueryBuilder::new("todos").build(),
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(30),
        format!("alice sees {BEFORE_COUNT} todos"),
        |rows| {
            before_ids
                .iter()
                .all(|id| rows.iter().any(|(rid, _)| rid == id))
                .then_some(())
        },
    )
    .await;

    alice
        .shutdown()
        .await
        .expect("shutdown alice before restart");
    server1.shutdown().await;

    // --- server₂ (same data_dir) ---
    let server2 = TestingServer::builder()
        .with_rocksdb_storage()
        .with_data_dir(data_dir.path())
        .with_jwks_url(jwks.endpoint())
        .start()
        .await;

    let bob = make_client_external_jwks(&server2, schema.clone(), "bob-restart", "todos").await;

    let pre_restart_rows = wait_for_query(
        &bob,
        QueryBuilder::new("todos").build(),
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(30),
        format!("bob sees {BEFORE_COUNT} pre-restart todos"),
        |rows| {
            before_ids
                .iter()
                .all(|id| rows.iter().any(|(rid, _)| rid == id))
                .then_some(rows)
        },
    )
    .await;

    let pre_titles: BTreeSet<String> = pre_restart_rows
        .iter()
        .filter(|(id, _)| before_ids.contains(id))
        .filter_map(|(_, cols)| match cols.first() {
            Some(Value::Text(t)) => Some(t.clone()),
            _ => None,
        })
        .collect();
    for i in 0..BEFORE_COUNT {
        assert!(
            pre_titles.contains(&format!("before-restart-{i:02}")),
            "missing pre-restart title {i}"
        );
    }

    // Alice reconnects and creates more.
    let alice = make_client_external_jwks(&server2, schema, "alice-restart", "todos").await;
    for i in 0..AFTER_COUNT {
        alice
            .create(
                "todos",
                HashMap::from([
                    (
                        "title".to_string(),
                        Value::Text(format!("after-restart-{i:02}")),
                    ),
                    ("completed".to_string(), Value::Boolean(false)),
                ]),
            )
            .await
            .expect("create after restart");
    }

    wait_for_query(
        &bob,
        QueryBuilder::new("todos").build(),
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(30),
        "bob sees post-restart todos",
        |rows| {
            let has_after = (0..AFTER_COUNT).all(|i| {
                let title = format!("after-restart-{i:02}");
                rows.iter()
                    .any(|(_, cols)| cols.first() == Some(&Value::Text(title.clone())))
            });
            has_after.then_some(())
        },
    )
    .await;

    alice
        .shutdown()
        .await
        .expect("shutdown alice after restart");
    bob.shutdown().await.expect("shutdown bob");
    server2.shutdown().await;
}

/// Verifies that schema metadata (catalogue entries) persists across a
/// server restart. After restart, a fresh client can query without the
/// server needing to re-discover the schema.
///
/// ```text
/// alice ──create + query──► server₁ (rocksdb, data_dir)
///                               │
///                           server₁ stops
///                               │
///                     server₂ starts (same data_dir)
///                               │
///                  bob connects and queries immediately
///                               │
///                               └──► rows available (schema was persisted)
/// ```
async fn catalogue_entries_survive_restart() {
    let data_dir = TempDir::new().expect("temp data dir");
    let jwks = TestingJwksServer::start().await;
    let schema = todos_schema();

    let server1 = TestingServer::builder()
        .with_rocksdb_storage()
        .with_data_dir(data_dir.path())
        .with_jwks_url(jwks.endpoint())
        .start()
        .await;

    let alice =
        make_client_external_jwks(&server1, schema.clone(), "alice-catalogue", "todos").await;

    let (todo_id, _) = alice
        .create(
            "todos",
            HashMap::from([
                (
                    "title".to_string(),
                    Value::Text("catalogue-test".to_string()),
                ),
                ("completed".to_string(), Value::Boolean(true)),
            ]),
        )
        .await
        .expect("create todo");

    wait_for_query(
        &alice,
        QueryBuilder::new("todos").build(),
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(30),
        "alice sees 1 todo",
        |rows| rows.iter().any(|(id, _)| *id == todo_id).then_some(()),
    )
    .await;

    alice.shutdown().await.expect("shutdown alice");
    server1.shutdown().await;

    // Restart with same data_dir — catalogue entries should be rehydrated.
    let server2 = TestingServer::builder()
        .with_rocksdb_storage()
        .with_data_dir(data_dir.path())
        .with_jwks_url(jwks.endpoint())
        .start()
        .await;

    let bob = make_client_external_jwks(&server2, schema, "bob-catalogue", "todos").await;

    let bob_row = wait_for_query(
        &bob,
        QueryBuilder::new("todos").build(),
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(30),
        "bob sees todo after restart",
        |rows| rows.iter().find(|(id, _)| *id == todo_id).cloned(),
    )
    .await;

    assert_eq!(bob_row.0, todo_id);
    assert_eq!(bob_row.1[0], Value::Text("catalogue-test".to_string()));

    bob.shutdown().await.expect("shutdown bob");
    server2.shutdown().await;
}

async fn sealed_batch_acceptance_recovers_after_restart() {
    let data_dir = TempDir::new().expect("temp data dir");
    let db_path = data_dir.path().join("jazz.rocksdb");
    let schema = todos_schema();
    let (batch_id, row_id) = {
        let mut storage =
            RocksDBStorage::open(&db_path, 8 * 1024 * 1024).expect("open rocksdb storage");
        let seeded = seed_rocksdb_sealed_batch_acceptance(&mut storage, &schema);
        storage.flush();
        storage.close().expect("close seeded rocksdb storage");
        seeded
    };

    let jwks = TestingJwksServer::start().await;
    let server = TestingServer::builder()
        .with_rocksdb_storage()
        .with_data_dir(data_dir.path())
        .with_jwks_url(jwks.endpoint())
        .start()
        .await;

    let bob =
        make_client_external_jwks(&server, schema.clone(), "bob-sealed-accept", "todos").await;

    bob.shutdown().await.expect("shutdown bob");
    server.shutdown().await;

    let reopened = RocksDBStorage::open(&db_path, 8 * 1024 * 1024).expect("reopen rocksdb storage");
    assert_eq!(
        reopened
            .load_authoritative_batch_fate(batch_id)
            .expect("load authoritative settlement"),
        Some(BatchFate::AcceptedTransaction {
            batch_id,
            confirmed_tier: DurabilityTier::GlobalServer,
        })
    );
    assert_eq!(
        reopened
            .load_sealed_batch_submission(batch_id)
            .expect("load sealed submission after recovery"),
        None
    );
    let visible = reopened
        .load_visible_region_row("todos", "main", row_id)
        .expect("load visible row after recovery")
        .expect("accepted row should remain visible");
    assert_eq!(visible.state, RowState::VisibleTransactional);
    reopened.close().expect("close reopened rocksdb storage");
}

async fn sealed_batch_frontier_conflict_rejects_after_restart() {
    let data_dir = TempDir::new().expect("temp data dir");
    let db_path = data_dir.path().join("jazz.rocksdb");
    let schema = todos_schema();
    let (batch_id, target_branch, staged_row_id, _existing_row_id) = {
        let mut storage =
            RocksDBStorage::open(&db_path, 8 * 1024 * 1024).expect("open rocksdb storage");
        let seeded = seed_rocksdb_sealed_batch_frontier_conflict(&mut storage, &schema);
        storage.flush();
        storage.close().expect("close seeded rocksdb storage");
        seeded
    };

    let jwks = TestingJwksServer::start().await;
    let server = TestingServer::builder()
        .with_rocksdb_storage()
        .with_data_dir(data_dir.path())
        .with_jwks_url(jwks.endpoint())
        .start()
        .await;

    let bob = make_client_external_jwks(
        &server,
        schema.clone(),
        "bob-sealed-frontier-conflict",
        "todos",
    )
    .await;

    bob.shutdown().await.expect("shutdown bob");
    server.shutdown().await;

    let reopened = RocksDBStorage::open(&db_path, 8 * 1024 * 1024).expect("reopen rocksdb storage");
    assert_eq!(
        reopened
            .load_authoritative_batch_fate(batch_id)
            .expect("load rejected settlement"),
        Some(BatchFate::Rejected {
            batch_id,
            code: "transaction_conflict".to_string(),
            reason: "family-visible frontier changed since batch was sealed".to_string(),
        })
    );
    assert_eq!(
        reopened
            .load_sealed_batch_submission(batch_id)
            .expect("load sealed submission after rejection"),
        None
    );
    assert_eq!(
        reopened
            .load_visible_region_row("todos", target_branch.as_str(), staged_row_id)
            .expect("load staged row visibility"),
        None
    );
    reopened.close().expect("close reopened rocksdb storage");
}
