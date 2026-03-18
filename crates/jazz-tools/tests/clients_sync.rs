#![cfg(feature = "test-utils")]

mod support;

use std::collections::BTreeSet;
use std::time::Duration;

use jazz_tools::server::TestingServer;
use jazz_tools::{
    ColumnType, DurabilityTier, JazzClient, QueryBuilder, SchemaBuilder, TableSchema, Value,
};
use support::wait_for_query;

fn test_schema() -> jazz_tools::Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("todos")
                .column("title", ColumnType::Text)
                .column("completed", ColumnType::Boolean),
        )
        .build()
}

async fn wait_for_edge_query_ready(client: &JazzClient, timeout: Duration) {
    let query = QueryBuilder::new("todos").build();
    wait_for_query(
        client,
        query,
        Some(DurabilityTier::EdgeServer),
        timeout,
        "EdgeServer query readiness",
        |_| Some(()),
    )
    .await;
}

#[tokio::test]
async fn jazz_tools_cli_two_clients_sync_values() {
    let server = TestingServer::start().await;
    let client_a = JazzClient::connect(server.make_client_context(test_schema()))
        .await
        .expect("connect client a");
    let client_b = JazzClient::connect(server.make_client_context(test_schema()))
        .await
        .expect("connect client b");

    wait_for_edge_query_ready(&client_a, Duration::from_secs(30)).await;
    wait_for_edge_query_ready(&client_b, Duration::from_secs(30)).await;

    client_a
        .create(
            "todos",
            vec![
                Value::Text("shared-through-server".to_string()),
                Value::Boolean(false),
            ],
        )
        .await
        .expect("create from client a");

    let rows_on_b = wait_for_query(
        &client_b,
        QueryBuilder::new("todos").build(),
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(25),
        "todos count 1",
        |rows| (rows.len() == 1).then_some(rows),
    )
    .await;
    let todo_id = rows_on_b[0].0;

    client_b
        .update(
            todo_id,
            vec![("completed".to_string(), Value::Boolean(true))],
        )
        .await
        .expect("update from client b");

    let rows_on_a = wait_for_query(
        &client_a,
        QueryBuilder::new("todos").build(),
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(25),
        "todo completed=true",
        |rows| {
            let has_expected_completed = rows.iter().any(|(_, values)| {
                values
                    .iter()
                    .any(|value| matches!(value, Value::Boolean(flag) if *flag))
            });

            (!rows.is_empty() && has_expected_completed).then_some(rows)
        },
    )
    .await;
    assert!(
        rows_on_a[0]
            .1
            .iter()
            .any(|value| matches!(value, Value::Boolean(true))),
        "client a should observe client b's update through the server"
    );

    client_a.shutdown().await.expect("shutdown client a");
    client_b.shutdown().await.expect("shutdown client b");
    server.shutdown().await;
}

#[tokio::test]
async fn jazz_tools_cli_two_different_users_sync_values() {
    let server = TestingServer::start().await;
    let client_alice =
        JazzClient::connect(server.make_client_context_for_user(test_schema(), "alice-sync-user"))
            .await
            .expect("connect alice client");
    let client_bob =
        JazzClient::connect(server.make_client_context_for_user(test_schema(), "bob-sync-user"))
            .await
            .expect("connect bob client");

    wait_for_edge_query_ready(&client_alice, Duration::from_secs(30)).await;
    wait_for_edge_query_ready(&client_bob, Duration::from_secs(30)).await;

    client_alice
        .create(
            "todos",
            vec![
                Value::Text("shared-across-users".to_string()),
                Value::Boolean(false),
            ],
        )
        .await
        .expect("alice creates todo");

    let rows_on_bob = wait_for_query(
        &client_bob,
        QueryBuilder::new("todos").build(),
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(25),
        "todos count 1",
        |rows| (rows.len() == 1).then_some(rows),
    )
    .await;
    let shared_todo_id = rows_on_bob[0].0;

    client_bob
        .update(
            shared_todo_id,
            vec![("completed".to_string(), Value::Boolean(true))],
        )
        .await
        .expect("bob updates alice todo");

    let _ = wait_for_query(
        &client_alice,
        QueryBuilder::new("todos").build(),
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(25),
        "todo completed=true",
        |rows| {
            let has_expected_completed = rows.iter().any(|(_, values)| {
                values
                    .iter()
                    .any(|value| matches!(value, Value::Boolean(flag) if *flag))
            });

            (!rows.is_empty() && has_expected_completed).then_some(rows)
        },
    )
    .await;

    client_bob
        .create(
            "todos",
            vec![Value::Text("from-bob".to_string()), Value::Boolean(false)],
        )
        .await
        .expect("bob creates todo");

    let expected_titles = ["shared-across-users", "from-bob"]
        .into_iter()
        .map(str::to_string)
        .collect::<BTreeSet<_>>();
    let rows_on_alice = wait_for_query(
        &client_alice,
        QueryBuilder::new("todos").build(),
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(25),
        format!("titles {:?}", expected_titles),
        |rows| {
            let titles = rows
                .iter()
                .flat_map(|(_, values)| values.iter())
                .filter_map(|value| match value {
                    Value::Text(text) => Some(text.clone()),
                    _ => None,
                })
                .collect::<BTreeSet<_>>();

            (titles == expected_titles).then_some(rows)
        },
    )
    .await;
    assert_eq!(
        rows_on_alice.len(),
        2,
        "alice should observe both shared rows across users"
    );

    client_alice
        .shutdown()
        .await
        .expect("shutdown alice client");
    client_bob.shutdown().await.expect("shutdown bob client");
    server.shutdown().await;
}
