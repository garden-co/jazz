use super::*;
use crate::batch_fate::BatchMode;
use crate::client_core::write::write_context;
use crate::object::ObjectId;
use crate::query_manager::session::{Session, WriteContext};
use crate::query_manager::types::{ColumnType, Schema, SchemaBuilder, TableSchema, Value};
use crate::row_histories::BatchId;
use crate::runtime_core::{NoopScheduler, RuntimeCore};
use crate::schema_manager::{AppId, SchemaManager};
use crate::storage::MemoryStorage;
use crate::sync_manager::SyncManager;
use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;
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

fn local_test_client() -> JazzClientCore<LocalRuntimeHost<MemoryStorage, NoopScheduler>> {
    JazzClientCore::from_runtime_host(
        ClientConfig::new("dev", "main"),
        LocalRuntimeHost::new(Rc::new(RefCell::new(test_runtime(users_schema())))),
    )
}

fn user_insert_values(id: ObjectId, name: &str) -> HashMap<String, Value> {
    HashMap::from([
        ("id".to_string(), Value::Uuid(id)),
        ("name".to_string(), Value::Text(name.to_string())),
    ])
}

fn local_batch_record(
    client: &JazzClientCore<LocalRuntimeHost<MemoryStorage, NoopScheduler>>,
    batch_id: BatchId,
) -> crate::batch_fate::LocalBatchRecord {
    client
        .with_runtime(|runtime| runtime.local_batch_record(batch_id))
        .expect("record load should succeed")
        .expect("local batch record should exist")
}

#[test]
fn sealed_writes_seal_direct_batches() {
    let mut client = local_test_client();
    let user_id = ObjectId::new();
    let ((inserted_id, _values), insert_batch_id) = client
        .insert(
            "users",
            user_insert_values(user_id, "Alice"),
            Some(WriteOptions {
                object_id: Some(user_id),
                ..Default::default()
            }),
        )
        .expect("insert should succeed");

    let update_batch_id = client
        .update(
            user_id,
            vec![("name".to_string(), Value::Text("Alicia".to_string()))],
            None,
        )
        .expect("update should succeed");
    let delete_batch_id = client.delete(user_id, None).expect("delete should succeed");

    assert_eq!(inserted_id, user_id);
    assert!(local_batch_record(&client, insert_batch_id).sealed);
    assert!(local_batch_record(&client, update_batch_id).sealed);
    assert!(local_batch_record(&client, delete_batch_id).sealed);
}

#[test]
fn unsealed_writes_keep_batches_open_for_binding_compatibility() {
    let mut client = local_test_client();
    let user_id = ObjectId::new();
    let ((_inserted_id, _values), insert_batch_id) = client
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
        .with_runtime(|runtime| runtime.local_batch_record(insert_batch_id))
        .expect("insert record load should succeed");
    assert!(insert_record.is_none());

    client
        .seal_batch(insert_batch_id)
        .expect("explicit seal should succeed");
}

#[test]
fn caller_supplied_write_context_batch_is_preserved() {
    let mut client = local_test_client();
    let user_id = ObjectId::new();
    let legacy_batch_id = BatchId::new();
    let write_context = WriteContext::default()
        .with_batch_mode(BatchMode::Transactional)
        .with_batch_id(legacy_batch_id);

    let ((_inserted_id, _values), batch_id) = client
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
        .seal_batch(batch_id)
        .expect("legacy batch should seal through the core wrapper");
    let record = local_batch_record(&client, batch_id);

    assert_eq!(batch_id, legacy_batch_id);
    assert_eq!(record.mode, BatchMode::Transactional);
    assert!(record.sealed);
}

#[test]
fn batch_context_groups_writes_until_explicit_seal() {
    let mut client = local_test_client();
    let user_id = ObjectId::new();
    let batch = client.begin_write_batch_context(BatchMode::Direct);
    let batch_context = write_context(&WriteOptions::default(), Some(&batch))
        .expect("batch write context should be created");

    assert_eq!(batch.mode(), BatchMode::Direct);
    assert!(batch.target_branch_name().starts_with("dev-"));
    assert!(batch.target_branch_name().ends_with("-main"));

    let ((_inserted_id, _values), insert_batch_id) = client
        .insert_unsealed(
            "users",
            user_insert_values(user_id, "Alice"),
            Some(WriteOptions {
                object_id: Some(user_id),
                write_context: Some(batch_context.clone()),
                ..Default::default()
            }),
        )
        .expect("insert should succeed");
    let update_batch_id = client
        .update_unsealed(
            user_id,
            vec![("name".to_string(), Value::Text("Alicia".to_string()))],
            Some(WriteOptions {
                write_context: Some(batch_context.clone()),
                ..Default::default()
            }),
        )
        .expect("update should succeed");
    let delete_batch_id = client
        .delete_unsealed(
            user_id,
            Some(WriteOptions {
                write_context: Some(batch_context.clone()),
                ..Default::default()
            }),
        )
        .expect("delete should succeed");
    client
        .seal_batch(batch.batch_id())
        .expect("batch should seal");

    assert_eq!(insert_batch_id, update_batch_id);
    assert_eq!(insert_batch_id, delete_batch_id);
    assert_eq!(insert_batch_id, batch.batch_id());
    assert!(local_batch_record(&client, batch.batch_id()).sealed);
}

#[test]
fn write_options_merge_session_and_batch_context() {
    let client = local_test_client();
    let batch = client.begin_write_batch_context(BatchMode::Transactional);
    let session = Session::new("alice");

    let context = write_context(
        &WriteOptions {
            session: Some(session.clone()),
            attribution: Some("alice-device".to_string()),
            updated_at: Some(123),
            ..Default::default()
        },
        Some(&batch),
    )
    .expect("write context");

    assert_eq!(context.session, Some(session));
    assert_eq!(context.attribution.as_deref(), Some("alice-device"));
    assert_eq!(context.updated_at, Some(123));
    assert_eq!(context.batch_mode(), BatchMode::Transactional);
    assert_eq!(context.batch_id(), Some(batch.batch_id()));
    assert_eq!(
        context.target_branch_name(),
        Some(batch.target_branch_name())
    );
}

#[test]
fn shared_runtime_host_uses_existing_runtime_handle() {
    let schema = users_schema();
    let runtime = Arc::new(Mutex::new(test_runtime(schema)));
    let mut client = JazzClientCore::from_runtime_host(
        ClientConfig::new("dev", "main"),
        SharedRuntimeHost::new(Arc::clone(&runtime)),
    );

    let ((_inserted_id, _values), batch_id) = client
        .insert("users", user_insert_values(ObjectId::new(), "Alice"), None)
        .expect("insert should work through shared runtime host");

    let record = runtime
        .lock()
        .expect("runtime lock")
        .local_batch_record(batch_id)
        .expect("load record")
        .expect("record exists");
    assert!(record.sealed);
}
