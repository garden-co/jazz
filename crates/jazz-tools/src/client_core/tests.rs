use super::*;
use crate::query_manager::types::{ColumnType, Schema, SchemaBuilder, TableName, TableSchema};
use crate::runtime_core::{NoopScheduler, RuntimeCore};
use crate::schema_manager::{AppId, SchemaManager};
use crate::storage::MemoryStorage;
use crate::sync_manager::{DurabilityTier, SyncManager};

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
