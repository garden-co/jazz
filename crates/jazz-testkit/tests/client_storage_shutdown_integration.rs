//! Public-API coverage that a connected persistent client releases its local
//! storage when it shuts down.

use jazz::tools::server::JazzServer;
use jazz::tools::{ClientStorage, ColumnType, JazzClient, SchemaBuilder, TableSchema};
use tempfile::TempDir;

fn test_schema() -> jazz::tools::Schema {
    SchemaBuilder::new()
        .table(TableSchema::builder("todos").column("title", ColumnType::Text))
        .build()
}

#[tokio::test]
async fn shutdown_releases_persistent_storage_for_reopen() {
    tokio::task::LocalSet::new()
        .run_until(shutdown_releases_persistent_storage_for_reopen_impl())
        .await
}

async fn shutdown_releases_persistent_storage_for_reopen_impl() {
    let server = JazzServer::start_with_schema(test_schema()).await;
    let data_dir = TempDir::new().expect("create persistent client directory");
    let mut context = server.make_client_context_for_user(test_schema(), "storage-release-user");
    context.storage = ClientStorage::Persistent;
    context.storage_factory = Some(std::sync::Arc::new(
        jazz_storage_rocksdb::RocksDbStorageFactory,
    ));
    context.data_dir = data_dir.path().to_path_buf();

    JazzClient::connect(context.clone())
        .await
        .expect("connect persistent client")
        .shutdown()
        .await
        .expect("shutdown persistent client");

    JazzClient::connect(context)
        .await
        .expect("reopen the same persistent client directory")
        .shutdown()
        .await
        .expect("shutdown reopened persistent client");

    server.shutdown().await;
}
