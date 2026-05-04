use super::*;
use crate::query_manager::types::{ColumnType, Schema, SchemaBuilder, TableName, TableSchema};
use crate::runtime_core::{NoopScheduler, RuntimeCore};
use crate::schema_manager::{AppId, SchemaManager};
use crate::storage::MemoryStorage;
use crate::sync_manager::SyncManager;

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
