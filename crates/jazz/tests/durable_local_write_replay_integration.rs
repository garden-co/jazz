#![cfg(feature = "test")]

//! Durable local writes are an upload backlog, not merely local visibility.

mod support;

use jazz::row_input;
use jazz::tools::server::JazzServer;
use jazz::tools::{
    ClientId, ClientStorage, ColumnType, DurabilityTier, JazzClient, QueryBuilder, Schema,
    SchemaBuilder, TableSchema,
};

use support::{has_row, wait_for_rows};

fn test_schema() -> Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("todos")
                .column("title", ColumnType::Text)
                .column("completed", ColumnType::Boolean),
        )
        .build()
}

#[tokio::test]
async fn persistent_restart_replays_pending_write_with_valid_token() {
    tokio::task::LocalSet::new()
        .run_until(persistent_restart_replays_pending_write_with_valid_token_impl())
        .await
}

async fn persistent_restart_replays_pending_write_with_valid_token_impl() {
    let server = JazzServer::start_with_schema(test_schema()).await;
    let mut context = server.make_client_context_for_user(test_schema(), "durable-replay-user");
    context.backend_secret = None;
    context.admin_secret = None;
    context.client_id = Some(ClientId::new());
    context.storage = ClientStorage::Persistent;

    // Keep the initial write offline so only the reopened client can deliver
    // it. The context still carries the helper's ordinary long-lived JWT.
    let mut offline_context = context.clone();
    offline_context.server_url.clear();
    let offline = JazzClient::connect(offline_context)
        .await
        .expect("open persistent client offline");
    let (todo_id, expected_values, batch_id) = offline
        .insert(
            "todos",
            row_input!("title" => "deliver after restart", "completed" => false),
        )
        .expect("create offline todo");
    offline
        .wait_for_batch(batch_id, DurabilityTier::Local)
        .await
        .expect("offline todo reaches local durability");
    offline.shutdown().await.expect("shutdown offline client");

    let reopened = JazzClient::connect(context)
        .await
        .expect("reopen persistent client with valid token");
    wait_for_rows(
        &reopened,
        QueryBuilder::new("todos").build(),
        "reopened client uploads its durable local write",
        |rows| has_row(&rows, todo_id, &expected_values).then_some(()),
    )
    .await;

    reopened.shutdown().await.expect("shutdown reopened client");
    server.shutdown().await;
}
