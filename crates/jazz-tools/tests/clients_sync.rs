#![cfg(feature = "test")]

mod support;

use jazz_tools::AppContext;
use std::collections::BTreeSet;
use std::collections::HashMap;
use std::time::Duration;

use jazz_tools::server::TestingServer;
use jazz_tools::{
    ColumnType, DurabilityTier, JazzClient, QueryBuilder, SchemaBuilder, TableSchema, Value,
};
use support::{publish_schema_and_permissions, wait_for_query};

fn test_schema() -> jazz_tools::Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("todos")
                .column("title", ColumnType::Text)
                .column("completed", ColumnType::Boolean)
                .policies(support::allow_all_policies()),
        )
        .build()
}

fn make_user_context(
    server: &TestingServer,
    schema: jazz_tools::Schema,
    user_id: &str,
) -> AppContext {
    let mut context = server.make_client_context_for_user(schema, user_id);
    context.backend_secret = None;
    context.admin_secret = None;
    context
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

/// Verifies that a fresh client resolves the latest state for a single object
/// after a long chain of overwrites has already compacted through the server.
///
/// Alice creates one todo, updates its `title` 100 times, and waits until the
/// final title is Edge-settled. Bob then connects from a fresh local state and
/// queries the table. Bob must observe the final title rather than an earlier
/// revision from the object's history.
///
/// ```text
/// alice ──create + title update ×100──► server
///                                          │
///                          bob connects fresh and queries
///                                          │
///                                          └──► latest title only
/// ```
#[tokio::test]
async fn fresh_client_resolves_object_with_deep_update_history() {
    const DEEP_HISTORY_UPDATES: usize = 100;

    let server = TestingServer::start().await;
    let schema = test_schema();
    publish_schema_and_permissions(&server.base_url(), server.admin_secret(), &schema)
        .await
        .expect("publish test schema and permissions");
    let writer = JazzClient::connect(make_user_context(&server, schema.clone(), "alice-history"))
        .await
        .expect("connect history writer");

    wait_for_edge_query_ready(&writer, Duration::from_secs(30)).await;

    let (todo_id, _) = writer
        .create(
            "todos",
            HashMap::from([
                ("title".to_string(), Value::Text("revision-000".to_string())),
                ("completed".to_string(), Value::Boolean(false)),
            ]),
        )
        .await
        .expect("create deep-history todo");

    let final_title = format!("revision-{DEEP_HISTORY_UPDATES:03}");
    for revision in 1..=DEEP_HISTORY_UPDATES {
        writer
            .update(
                todo_id,
                vec![(
                    "title".to_string(),
                    Value::Text(format!("revision-{revision:03}")),
                )],
            )
            .await
            .expect("update deep-history todo");
    }

    let writer_rows = wait_for_query(
        &writer,
        QueryBuilder::new("todos").build(),
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(25),
        format!("writer sees final title {final_title}"),
        |rows| {
            (rows.len() == 1
                && rows[0].0 == todo_id
                && rows[0].1.first() == Some(&Value::Text(final_title.clone())))
            .then_some(rows)
        },
    )
    .await;
    assert_eq!(writer_rows.len(), 1);
    assert_eq!(writer_rows[0].1[0], Value::Text(final_title.clone()));

    let fresh_client = JazzClient::connect(make_user_context(&server, schema, "bob-fresh-history"))
        .await
        .expect("connect fresh history reader");
    wait_for_edge_query_ready(&fresh_client, Duration::from_secs(30)).await;

    let fresh_rows = wait_for_query(
        &fresh_client,
        QueryBuilder::new("todos").build(),
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(25),
        format!("fresh client sees final title {final_title}"),
        |rows| {
            (rows.len() == 1
                && rows[0].0 == todo_id
                && rows[0].1.first() == Some(&Value::Text(final_title.clone())))
            .then_some(rows)
        },
    )
    .await;
    assert_eq!(fresh_rows.len(), 1);
    assert_eq!(fresh_rows[0].0, todo_id);
    assert_eq!(fresh_rows[0].1[0], Value::Text(final_title));

    writer.shutdown().await.expect("shutdown history writer");
    fresh_client
        .shutdown()
        .await
        .expect("shutdown fresh history reader");
    server.shutdown().await;
}

#[tokio::test]
async fn jazz_tools_cli_two_clients_sync_values() {
    let server = TestingServer::start().await;
    let schema = test_schema();
    publish_schema_and_permissions(&server.base_url(), server.admin_secret(), &schema)
        .await
        .expect("publish test schema and permissions");
    let client_a = JazzClient::connect(make_user_context(&server, schema.clone(), "cli-sync-user"))
        .await
        .expect("connect client a");
    let client_b = JazzClient::connect(make_user_context(&server, schema, "cli-sync-user"))
        .await
        .expect("connect client b");

    wait_for_edge_query_ready(&client_a, Duration::from_secs(30)).await;
    wait_for_edge_query_ready(&client_b, Duration::from_secs(30)).await;

    client_a
        .create(
            "todos",
            HashMap::from([
                (
                    "title".to_string(),
                    Value::Text("shared-through-server".to_string()),
                ),
                ("completed".to_string(), Value::Boolean(false)),
            ]),
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
    let schema = test_schema();
    publish_schema_and_permissions(&server.base_url(), server.admin_secret(), &schema)
        .await
        .expect("publish test schema and permissions");
    let client_alice = JazzClient::connect(make_user_context(
        &server,
        schema.clone(),
        "alice-sync-user",
    ))
    .await
    .expect("connect alice client");
    let client_bob = JazzClient::connect(make_user_context(&server, schema, "bob-sync-user"))
        .await
        .expect("connect bob client");

    wait_for_edge_query_ready(&client_alice, Duration::from_secs(30)).await;
    wait_for_edge_query_ready(&client_bob, Duration::from_secs(30)).await;

    client_alice
        .create(
            "todos",
            HashMap::from([
                (
                    "title".to_string(),
                    Value::Text("shared-across-users".to_string()),
                ),
                ("completed".to_string(), Value::Boolean(false)),
            ]),
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
            HashMap::from([
                ("title".to_string(), Value::Text("from-bob".to_string())),
                ("completed".to_string(), Value::Boolean(false)),
            ]),
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
