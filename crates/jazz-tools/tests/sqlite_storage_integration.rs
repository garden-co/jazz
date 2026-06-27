#![cfg(all(feature = "test", feature = "sqlite"))]

use jazz_tools::server::{ServerBuilder, StorageBackend};
use jazz_tools::{AppId, SchemaBuilder, TableSchema};
use tempfile::TempDir;

fn todos_schema() -> jazz_tools::Schema {
    SchemaBuilder::new()
        .table(TableSchema::builder("todos").column("title", jazz_tools::ColumnType::Text))
        .build()
}

#[tokio::test]
async fn sqlite_is_not_a_supported_server_storage_backend() {
    let data_dir = TempDir::new().expect("temp data dir");

    let err = ServerBuilder::new(AppId::random())
        .with_schema(todos_schema())
        .with_storage(StorageBackend::Sqlite {
            path: data_dir.path().to_path_buf(),
        })
        .build()
        .await
        .expect_err("sqlite must not be accepted as core server storage");

    assert!(
        err.contains("core server catalogue storage does not support sqlite")
            || err.contains("core server storage does not support sqlite"),
        "unexpected sqlite server storage error: {err}"
    );
}
