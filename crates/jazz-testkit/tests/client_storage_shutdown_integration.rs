//! Public-API coverage that a connected persistent client releases its local
//! storage when it shuts down.

use jazz::{
    query::Query,
    tools::{ClientStorage, ColumnType, ReadTier, SchemaBuilder, TableSchema},
};
use jazz_server::JazzServer;
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

    let client = jazz_testkit::connect(context.clone())
        .await
        .expect("connect persistent client");
    let retained_clone = client.clone();
    client.shutdown().await.expect("shutdown persistent client");

    let error = retained_clone
        .query_with_read_tier(Query::from("todos"), ReadTier::LocalFirst)
        .await
        .expect_err("a retained JazzClient clone must not revive a shut-down context");
    assert!(
        error.to_string().contains("client is shut down"),
        "retained clone reported an unexpected shutdown error: {error}"
    );

    jazz_testkit::connect(context)
        .await
        .expect("reopen the same persistent client directory")
        .shutdown()
        .await
        .expect("shutdown reopened persistent client");

    server.shutdown().await;
}
