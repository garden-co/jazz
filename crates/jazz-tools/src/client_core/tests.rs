use super::*;
use crate::object::ObjectId;
use crate::query_manager::session::WriteContext;
use crate::query_manager::types::Value;
use crate::query_manager::types::{ColumnType, Schema, SchemaBuilder, TableName, TableSchema};
use crate::row_histories::BatchId;
use crate::runtime_core::{NoopScheduler, RuntimeCore};
use crate::schema_manager::{AppId, SchemaManager};
use crate::storage::MemoryStorage;
use crate::sync_manager::{DurabilityTier, SyncManager};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

fn users_schema() -> Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("name", ColumnType::Text),
        )
        .build()
}

fn test_runtime(schema: Schema) -> RuntimeCore<MemoryStorage, NoopScheduler> {
    let app_id = AppId::from_name("client-core-test");
    let schema_manager =
        SchemaManager::new(SyncManager::new(), schema, app_id, "dev", "main").unwrap();
    let mut runtime = RuntimeCore::new(schema_manager, MemoryStorage::new(), NoopScheduler);
    runtime.immediate_tick();
    runtime
}

fn user_insert_values(id: ObjectId, name: &str) -> HashMap<String, Value> {
    HashMap::from([
        ("id".to_string(), Value::Uuid(id)),
        ("name".to_string(), Value::Text(name.to_string())),
    ])
}

#[test]
fn client_core_wraps_runtime_and_exposes_schema() {
    let schema = users_schema();
    let client = JazzClientCore::from_runtime_parts(
        ClientConfig::memory_for_test("client-core-test", schema.clone()),
        test_runtime(schema),
    )
    .expect("client core should be constructed");

    assert!(
        client
            .current_schema()
            .contains_key(&TableName::new("users"))
    );
}

#[test]
fn browser_main_thread_defaults_reads_to_local() {
    let mut config = ClientConfig::memory_for_test("browser-default-test", users_schema());
    config.runtime_flavor = ClientRuntimeFlavor::BrowserMainThread;
    config.server_url = Some("https://example.test".to_string());

    assert_eq!(
        config.resolved_default_durability_tier(),
        DurabilityTier::Local
    );
}

#[test]
fn non_browser_server_clients_default_reads_to_edge() {
    let mut config = ClientConfig::memory_for_test("node-default-test", users_schema());
    config.runtime_flavor = ClientRuntimeFlavor::Node;
    config.server_url = Some("https://example.test".to_string());

    assert_eq!(
        config.resolved_default_durability_tier(),
        DurabilityTier::EdgeServer
    );
}

#[test]
fn explicit_default_durability_tier_wins() {
    let mut config = ClientConfig::memory_for_test("explicit-default-test", users_schema());
    config.runtime_flavor = ClientRuntimeFlavor::BrowserMainThread;
    config.server_url = Some("https://example.test".to_string());
    config.default_durability_tier = Some(DurabilityTier::GlobalServer);

    assert_eq!(
        config.resolved_default_durability_tier(),
        DurabilityTier::GlobalServer
    );
}

#[test]
fn client_error_preserves_stable_code_and_context() {
    let error = ClientError::new(ClientErrorCode::BatchRejected, "permission denied")
        .with_batch_id("abc123")
        .with_table("todos")
        .with_object_id("row1");

    assert_eq!(error.code, ClientErrorCode::BatchRejected);
    assert_eq!(error.batch_id.as_deref(), Some("abc123"));
    assert_eq!(error.table.as_deref(), Some("todos"));
    assert_eq!(error.object_id.as_deref(), Some("row1"));
}

#[test]
fn client_core_insert_seals_standalone_direct_write() {
    let schema = users_schema();
    let mut client = JazzClientCore::from_runtime_parts(
        ClientConfig::memory_for_test("standalone-insert-test", schema.clone()),
        test_runtime(schema),
    )
    .unwrap();

    let user_id = ObjectId::new();
    let result = client
        .insert(
            "users",
            user_insert_values(user_id, "Alice"),
            Some(WriteOptions {
                object_id: Some(user_id),
                ..Default::default()
            }),
        )
        .expect("insert should succeed");

    let record = client
        .local_batch_record(result.handle.batch_id)
        .expect("record load should succeed")
        .expect("standalone write should retain a local batch record");

    assert_eq!(result.row.id, user_id);
    assert!(
        record.sealed,
        "standalone direct writes should seal in Rust"
    );
}

#[test]
fn client_core_update_and_delete_seal_standalone_direct_writes() {
    let schema = users_schema();
    let mut client = JazzClientCore::from_runtime_parts(
        ClientConfig::memory_for_test("standalone-mutations-test", schema.clone()),
        test_runtime(schema),
    )
    .unwrap();

    let user_id = ObjectId::new();
    client
        .insert(
            "users",
            user_insert_values(user_id, "Alice"),
            Some(WriteOptions {
                object_id: Some(user_id),
                ..Default::default()
            }),
        )
        .expect("insert should create the row");

    let update = client
        .update(
            user_id,
            vec![("name".to_string(), Value::Text("Alicia".to_string()))],
            None,
        )
        .expect("update should succeed");
    let delete = client.delete(user_id, None).expect("delete should succeed");

    let update_record = client
        .local_batch_record(update.batch_id)
        .unwrap()
        .expect("update batch record should exist");
    let delete_record = client
        .local_batch_record(delete.batch_id)
        .unwrap()
        .expect("delete batch record should exist");

    assert!(update_record.sealed);
    assert!(delete_record.sealed);
}

#[test]
fn client_core_unsealed_writes_leave_batches_open_for_runtime_compatibility() {
    let schema = users_schema();
    let mut client = JazzClientCore::from_runtime_parts(
        ClientConfig::memory_for_test("unsealed-runtime-write-test", schema.clone()),
        test_runtime(schema),
    )
    .unwrap();

    let user_id = ObjectId::new();
    let insert = client
        .insert_unsealed(
            "users",
            user_insert_values(user_id, "Alice"),
            Some(WriteOptions {
                object_id: Some(user_id),
                ..Default::default()
            }),
        )
        .expect("unsealed insert should succeed");

    let insert_record = client
        .local_batch_record(insert.handle.batch_id)
        .expect("insert record load should succeed");
    assert!(
        insert_record.is_none(),
        "unsealed runtime writes should not publish a sealed local record yet"
    );

    client
        .seal_batch(insert.handle.batch_id)
        .expect("explicit seal should succeed");

    let update = client
        .update_unsealed(
            user_id,
            vec![("name".to_string(), Value::Text("Alicia".to_string()))],
            None,
        )
        .expect("unsealed update should succeed");
    let delete = client
        .delete_unsealed(user_id, None)
        .expect("unsealed delete should succeed");

    let update_record = client
        .local_batch_record(update.batch_id)
        .expect("update record load should succeed");
    let delete_record = client
        .local_batch_record(delete.batch_id)
        .expect("delete record load should succeed");

    assert!(update_record.is_none());
    assert!(delete_record.is_none());
}

#[test]
fn client_core_raw_write_context_preserves_legacy_batch_context() {
    let schema = users_schema();
    let mut client = JazzClientCore::from_runtime_parts(
        ClientConfig::memory_for_test("legacy-write-context-test", schema.clone()),
        test_runtime(schema),
    )
    .unwrap();

    let user_id = ObjectId::new();
    let legacy_batch_id = BatchId::new();
    let write_context = WriteContext::default()
        .with_batch_mode(crate::batch_fate::BatchMode::Transactional)
        .with_batch_id(legacy_batch_id);

    let result = client
        .insert_unsealed(
            "users",
            user_insert_values(user_id, "Alice"),
            Some(WriteOptions {
                object_id: Some(user_id),
                write_context: Some(write_context),
                ..Default::default()
            }),
        )
        .expect("legacy-context insert should succeed");

    client
        .seal_batch(result.handle.batch_id)
        .expect("legacy batch should seal through the core wrapper");

    let record = client
        .local_batch_record(result.handle.batch_id)
        .unwrap()
        .expect("legacy batch record should exist");

    assert_eq!(result.handle.batch_id, legacy_batch_id);
    assert_eq!(record.mode, crate::batch_fate::BatchMode::Transactional);
    assert!(record.sealed);
}

#[test]
fn client_core_local_batch_helpers_wrap_runtime_state() {
    let schema = users_schema();
    let mut client = JazzClientCore::from_runtime_parts(
        ClientConfig::memory_for_test("local-batch-helper-test", schema.clone()),
        test_runtime(schema),
    )
    .unwrap();

    let result = client
        .insert("users", user_insert_values(ObjectId::new(), "Alice"), None)
        .expect("insert should succeed");

    let records = client
        .local_batch_records()
        .expect("record list should load");
    assert!(
        records
            .iter()
            .any(|record| record.batch_id == result.handle.batch_id)
    );

    assert_eq!(client.drain_rejected_batch_ids(), Vec::<BatchId>::new());
    assert!(
        !client
            .acknowledge_rejected_batch(BatchId::new())
            .expect("ack should return false for an unknown batch")
    );

    client
        .seal_batch(result.handle.batch_id)
        .expect("seal helper should work");
    let record = client
        .local_batch_record(result.handle.batch_id)
        .unwrap()
        .expect("record should still exist after seal");
    assert!(record.sealed);
}

#[test]
fn direct_batch_context_is_owned_for_binding_adapters() {
    let schema = users_schema();
    let mut client = JazzClientCore::from_runtime_parts(
        ClientConfig::memory_for_test("owned-direct-batch-test", schema.clone()),
        test_runtime(schema),
    )
    .unwrap();

    let batch = client.begin_direct_batch_context();
    assert!(
        client
            .current_schema()
            .contains_key(&TableName::new("users"))
    );

    let alice = client
        .insert_in_batch(
            &batch,
            "users",
            user_insert_values(ObjectId::new(), "Alice"),
            None,
        )
        .expect("first insert should succeed");
    let bob = client
        .insert_in_batch(
            &batch,
            "users",
            user_insert_values(ObjectId::new(), "Bob"),
            None,
        )
        .expect("second insert should succeed");
    let handle = client
        .commit_batch_context(batch)
        .expect("batch commit should seal");

    assert_eq!(alice.handle.batch_id, bob.handle.batch_id);
    assert_eq!(alice.handle.batch_id, handle.batch_id);
}

#[test]
fn direct_batch_context_supports_update_and_delete_for_binding_adapters() {
    let schema = users_schema();
    let mut client = JazzClientCore::from_runtime_parts(
        ClientConfig::memory_for_test("owned-direct-batch-mutations-test", schema.clone()),
        test_runtime(schema),
    )
    .unwrap();

    let user_id = ObjectId::new();
    let batch = client.begin_direct_batch_context();
    let inserted = client
        .insert_in_batch(
            &batch,
            "users",
            user_insert_values(user_id, "Alice"),
            Some(WriteOptions {
                object_id: Some(user_id),
                ..Default::default()
            }),
        )
        .expect("insert should succeed");
    let updated = client
        .update_in_batch(
            &batch,
            user_id,
            vec![("name".to_string(), Value::Text("Alicia".to_string()))],
            None,
        )
        .expect("update should succeed");
    let deleted = client
        .delete_in_batch(&batch, user_id, None)
        .expect("delete should succeed");
    let handle = client
        .commit_batch_context(batch)
        .expect("batch commit should seal");

    assert_eq!(inserted.handle.batch_id, updated.batch_id);
    assert_eq!(inserted.handle.batch_id, deleted.batch_id);
    assert_eq!(inserted.handle.batch_id, handle.batch_id);
}

#[test]
fn local_wait_check_succeeds_after_direct_batch_commit() {
    let schema = users_schema();
    let mut client = JazzClientCore::from_runtime_parts(
        ClientConfig::memory_for_test("local-wait-test", schema.clone()),
        test_runtime(schema),
    )
    .unwrap();

    let result = client
        .insert("users", user_insert_values(ObjectId::new(), "Alice"), None)
        .unwrap();

    assert_eq!(
        client.check_batch_wait(result.handle.batch_id, DurabilityTier::Local),
        BatchWaitOutcome::Satisfied
    );
}

#[test]
fn client_core_can_wrap_a_shared_runtime_handle() {
    let schema = users_schema();
    let runtime = Arc::new(Mutex::new(test_runtime(schema.clone())));
    let mut client = JazzClientCore::from_runtime_host(
        ClientConfig::memory_for_test("shared-runtime-test", schema),
        SharedRuntimeHost::new(runtime),
    )
    .expect("shared host should construct");

    let result = client
        .insert("users", user_insert_values(ObjectId::new(), "Alice"), None)
        .expect("insert should work through shared runtime host");

    assert_eq!(
        client.check_batch_wait(result.handle.batch_id, DurabilityTier::Local),
        BatchWaitOutcome::Satisfied
    );
}
