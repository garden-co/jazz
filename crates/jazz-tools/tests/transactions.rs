#![cfg(feature = "test-utils")]

use jazz_tools::row_input;
use jazz_tools::{
    ColumnType, JazzClient, ObjectId, QueryBuilder, Schema, SchemaBuilder, TableSchema, Value,
};

fn todo_schema() -> Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("todos")
                .column("title", ColumnType::Text)
                .column("completed", ColumnType::Boolean),
        )
        .build()
}

async fn all_todos(client: &JazzClient) -> Vec<(ObjectId, Vec<Value>)> {
    client
        .query(
            QueryBuilder::new("todos")
                .select(&["title", "completed"])
                .build(),
            None,
        )
        .await
        .expect("query todos")
}

#[tokio::test]
async fn transaction_stages_writes_and_can_commit() {
    let client = JazzClient::test_client(todo_schema()).await;
    let tx = client
        .begin_transaction()
        .expect("begin transaction through client API");
    let batch_id = tx.batch_id();

    let (todo_id, inserted_values, write_batch_id) = tx
        .insert(
            "todos",
            row_input!("title" => "ship transactions", "completed" => false),
        )
        .expect("insert in transaction");

    assert_eq!(write_batch_id, batch_id);
    assert!(
        all_todos(&client).await.is_empty(),
        "ordinary client reads should ignore an open transaction"
    );
    assert_eq!(
        all_todos(tx.client()).await,
        vec![(todo_id, inserted_values)],
        "transaction-scoped reads should include staged rows"
    );

    assert_eq!(tx.commit().expect("commit transaction"), batch_id);
    assert!(
        client.commit_transaction(batch_id).is_err(),
        "committed transaction should reject a second commit"
    );
}

#[tokio::test]
async fn transaction_can_be_rolled_back() {
    let client = JazzClient::test_client(todo_schema()).await;
    let tx = client
        .begin_transaction()
        .expect("begin transaction through client API");
    let batch_id = tx.batch_id();

    let (todo_id, inserted_values, _) = tx
        .insert(
            "todos",
            row_input!("title" => "discard me", "completed" => false),
        )
        .expect("insert in transaction");
    assert_eq!(
        all_todos(tx.client()).await,
        vec![(todo_id, inserted_values)]
    );

    client
        .rollback_transaction(batch_id)
        .expect("roll back transaction by id");
    assert!(
        all_todos(&client).await.is_empty(),
        "rolled back transaction should not make staged rows visible"
    );
    assert!(
        client.commit_transaction(batch_id).is_err(),
        "rolled back transaction should reject commit"
    );
}
