#![cfg(feature = "test")]

mod support;

use std::collections::BTreeSet;
use std::collections::HashMap;
use std::time::Duration;

use jazz_tools::row_input;
use jazz_tools::server::TestingServer;
use jazz_tools::sync_manager::SyncPayload;
use jazz_tools::{
    ColumnType, DurabilityTier, JazzClient, QueryBuilder, SchemaBuilder, TableSchema, Value,
};
use support::{publish_allow_all_permissions, wait_for_query};
use uuid::Uuid;

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
async fn wait_for_batch_waits_until_expected_tier_confirmation_reaches_client() {
    let schema = test_schema();
    let server = TestingServer::start_with_schema(schema.clone()).await;
    let alice =
        JazzClient::connect(server.make_client_context_for_user(schema, "alice-wait-for-batch"))
            .await
            .expect("connect alice");
    let alice_client_id = alice.client_id().expect("alice transport client id");

    wait_for_edge_query_ready(&alice, Duration::from_secs(30)).await;

    let blocked = server.block_messages_to(alice_client_id);
    let (_, _, batch_id) = alice
        .insert(
            "todos",
            row_input!("title" => "blocked confirmation", "completed" => false),
        )
        .expect("insert todo");

    blocked
        .wait_until_buffered(
            |payload| {
                matches!(
                    payload,
                    SyncPayload::BatchFate { fate }
                        if fate.batch_id() == batch_id
                            && fate
                                .confirmed_tier()
                                .is_some_and(|tier| tier >= DurabilityTier::EdgeServer)
                )
            },
            Duration::from_secs(5),
        )
        .await
        .expect("server should produce the edge confirmation while messages are blocked");
    assert!(
        blocked.buffered_count() > 0,
        "blocked client should have buffered server messages"
    );

    {
        let wait_for_batch = alice.wait_for_batch(batch_id, DurabilityTier::EdgeServer);
        tokio::pin!(wait_for_batch);

        assert!(
            tokio::time::timeout(Duration::from_millis(200), &mut wait_for_batch)
                .await
                .is_err(),
            "wait_for_batch should not resolve before the confirmation is delivered"
        );

        blocked.unblock();
        wait_for_batch
            .await
            .expect("wait_for_batch should resolve after unblocking the confirmation");
    }

    alice.shutdown().await.expect("shutdown alice");
    server.shutdown().await;
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

    let schema = test_schema();
    let server = TestingServer::start_with_schema(schema.clone()).await;
    let writer =
        JazzClient::connect(server.make_client_context_for_user(schema.clone(), "alice-history"))
            .await
            .expect("connect history writer");

    wait_for_edge_query_ready(&writer, Duration::from_secs(30)).await;

    let (todo_id, _, _) = writer
        .insert(
            "todos",
            HashMap::from([
                ("title".to_string(), Value::Text("revision-000".to_string())),
                ("completed".to_string(), Value::Boolean(false)),
            ]),
        )
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

    let fresh_client =
        JazzClient::connect(server.make_client_context_for_user(schema, "bob-fresh-history"))
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
    let schema = test_schema();
    let server = TestingServer::start_with_schema(schema.clone()).await;
    let client_a = JazzClient::connect(server.make_client_context(schema.clone()))
        .await
        .expect("connect client a");
    let client_b = JazzClient::connect(server.make_client_context(schema))
        .await
        .expect("connect client b");

    wait_for_edge_query_ready(&client_a, Duration::from_secs(30)).await;
    wait_for_edge_query_ready(&client_b, Duration::from_secs(30)).await;

    client_a
        .insert(
            "todos",
            HashMap::from([
                (
                    "title".to_string(),
                    Value::Text("shared-through-server".to_string()),
                ),
                ("completed".to_string(), Value::Boolean(false)),
            ]),
        )
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
async fn update_through_one_client_waits_for_ack_and_updates_peer_query_results() {
    let schema = test_schema();
    let server = TestingServer::start_with_schema(schema.clone()).await;
    let client_a = JazzClient::connect(server.make_client_context(schema.clone()))
        .await
        .expect("connect client a");
    let client_b = JazzClient::connect(server.make_client_context(schema))
        .await
        .expect("connect client b");

    wait_for_edge_query_ready(&client_a, Duration::from_secs(30)).await;
    wait_for_edge_query_ready(&client_b, Duration::from_secs(30)).await;

    let (todo_id, _, _) = client_a
        .insert(
            "todos",
            row_input!("title" => "update-through-server", "completed" => false),
        )
        .expect("create todo from client a");

    let query = QueryBuilder::new("todos").build();
    wait_for_query(
        &client_b,
        query.clone(),
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(25),
        "client b sees inserted todo before update",
        |rows| rows.iter().any(|(id, _)| *id == todo_id).then_some(()),
    )
    .await;

    let batch_id = client_a
        .update(
            todo_id,
            vec![("completed".to_string(), Value::Boolean(true))],
        )
        .expect("update todo from client a");
    client_a
        .wait_for_batch(batch_id, DurabilityTier::EdgeServer)
        .await
        .expect("update reaches edge");

    let expected_values = vec![
        Value::Text("update-through-server".to_string()),
        Value::Boolean(true),
    ];
    let rows_after_update = wait_for_query(
        &client_b,
        query,
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(25),
        "client b sees updated todo",
        |rows| {
            rows.iter()
                .find(|(id, values)| *id == todo_id && values == &expected_values)
                .cloned()
        },
    )
    .await;
    assert_eq!(rows_after_update.0, todo_id);
    assert_eq!(rows_after_update.1, expected_values);

    client_a.shutdown().await.expect("shutdown client a");
    client_b.shutdown().await.expect("shutdown client b");
    server.shutdown().await;
}

#[tokio::test]
async fn delete_through_one_client_removes_row_from_peer_query_results() {
    let schema = test_schema();
    let server = TestingServer::start_with_schema(schema.clone()).await;
    let client_a = JazzClient::connect(server.make_client_context(schema.clone()))
        .await
        .expect("connect client a");
    let client_b = JazzClient::connect(server.make_client_context(schema))
        .await
        .expect("connect client b");

    wait_for_edge_query_ready(&client_a, Duration::from_secs(30)).await;
    wait_for_edge_query_ready(&client_b, Duration::from_secs(30)).await;

    let (todo_id, _, _) = client_a
        .insert(
            "todos",
            row_input!("title" => "delete-through-server", "completed" => false),
        )
        .expect("create todo from client a");

    let query = QueryBuilder::new("todos").build();
    wait_for_query(
        &client_b,
        query.clone(),
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(25),
        "client b sees inserted todo before delete",
        |rows| rows.iter().any(|(id, _)| *id == todo_id).then_some(()),
    )
    .await;

    let batch_id = client_a.delete(todo_id).expect("delete todo from client a");
    client_a
        .wait_for_batch(batch_id, DurabilityTier::EdgeServer)
        .await
        .expect("delete reaches edge");

    let rows_after_delete = wait_for_query(
        &client_b,
        query,
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(25),
        "client b no longer sees deleted todo",
        |rows| rows.iter().all(|(id, _)| *id != todo_id).then_some(rows),
    )
    .await;
    assert!(
        rows_after_delete.iter().all(|(id, _)| *id != todo_id),
        "deleted row should not remain visible to peer: {rows_after_delete:?}"
    );

    client_a.shutdown().await.expect("shutdown client a");
    client_b.shutdown().await.expect("shutdown client b");
    server.shutdown().await;
}

#[tokio::test]
async fn caller_supplied_uuid_is_used_for_created_row() {
    let schema = test_schema();
    let server = TestingServer::start_with_schema(schema.clone()).await;
    publish_allow_all_permissions(
        &server.base_url(),
        server.app_id(),
        server.admin_secret(),
        &schema,
    )
    .await;
    let client = JazzClient::connect(server.make_client_context(schema.clone()))
        .await
        .expect("connect writer");

    wait_for_edge_query_ready(&client, Duration::from_secs(30)).await;

    let external_id =
        Uuid::parse_str("550e8400-e29b-41d4-a716-446655440000").expect("parse external uuid");

    let (todo_id, expected_values, _) = client
        .insert_with_id(
            "todos",
            external_id,
            HashMap::from([
                (
                    "title".to_string(),
                    Value::Text("external-id-created".to_string()),
                ),
                ("completed".to_string(), Value::Boolean(false)),
            ]),
        )
        .expect("create row with external id");

    assert_eq!(todo_id.uuid(), &external_id);

    let rows = wait_for_query(
        &client,
        QueryBuilder::new("todos").build(),
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(25),
        "query returns row created with external id",
        |rows| {
            (rows.len() == 1 && rows[0].0 == todo_id && rows[0].1 == expected_values)
                .then_some(rows)
        },
    )
    .await;

    assert_eq!(rows[0].0.uuid(), &external_id);

    client.shutdown().await.expect("shutdown writer");
    server.shutdown().await;
}

#[tokio::test]
async fn caller_supplied_uuid_keeps_created_at_as_explicit_metadata() {
    let schema = test_schema();
    let server = TestingServer::start_with_schema(schema.clone()).await;
    publish_allow_all_permissions(
        &server.base_url(),
        server.app_id(),
        server.admin_secret(),
        &schema,
    )
    .await;
    let client = JazzClient::connect(server.make_client_context(schema.clone()))
        .await
        .expect("connect writer");

    wait_for_edge_query_ready(&client, Duration::from_secs(30)).await;

    let external_id =
        Uuid::parse_str("550e8400-e29b-41d4-a716-446655440002").expect("parse external uuid");

    client
        .upsert(
            "todos",
            external_id,
            HashMap::from([
                ("title".to_string(), Value::Text("first-title".to_string())),
                ("completed".to_string(), Value::Boolean(false)),
            ]),
        )
        .expect("insert row through upsert");

    let provenance_query = QueryBuilder::new("todos")
        .select(&["$createdAt", "$updatedAt"])
        .build();

    client
        .upsert(
            "todos",
            external_id,
            HashMap::from([(
                "title".to_string(),
                Value::Text("updated-title".to_string()),
            )]),
        )
        .expect("upsert row with external id");

    let updated_rows = wait_for_query(
        &client,
        provenance_query,
        Some(DurabilityTier::Local),
        Duration::from_secs(25),
        "updated provenance query returns row",
        |rows| (rows.len() == 1 && rows[0].0.uuid() == &external_id).then_some(rows),
    )
    .await;

    let Value::Timestamp(updated_created_at) = updated_rows[0].1[0] else {
        panic!("updated $createdAt should decode as timestamp")
    };
    let Value::Timestamp(updated_updated_at) = updated_rows[0].1[1] else {
        panic!("updated $updatedAt should decode as timestamp")
    };

    assert_eq!(updated_rows[0].0.uuid(), &external_id);
    assert!(
        updated_created_at < updated_updated_at,
        "created_at should remain the original timestamp after an update"
    );

    client.shutdown().await.expect("shutdown writer");
    server.shutdown().await;
}

#[tokio::test]
async fn upsert_uses_external_uuid_for_insert_and_updates_existing_row() {
    let schema = test_schema();
    let server = TestingServer::start_with_schema(schema.clone()).await;
    publish_allow_all_permissions(
        &server.base_url(),
        server.app_id(),
        server.admin_secret(),
        &schema,
    )
    .await;
    let client = JazzClient::connect(server.make_client_context(schema.clone()))
        .await
        .expect("connect writer");

    wait_for_edge_query_ready(&client, Duration::from_secs(30)).await;

    let external_id =
        Uuid::parse_str("550e8400-e29b-41d4-a716-446655440001").expect("parse external uuid");

    client
        .upsert(
            "todos",
            external_id,
            HashMap::from([
                ("title".to_string(), Value::Text("first-title".to_string())),
                ("completed".to_string(), Value::Boolean(false)),
            ]),
        )
        .expect("insert row through upsert");

    client
        .upsert(
            "todos",
            external_id,
            HashMap::from([(
                "title".to_string(),
                Value::Text("updated-title".to_string()),
            )]),
        )
        .expect("update existing row through upsert");

    let rows = wait_for_query(
        &client,
        QueryBuilder::new("todos").build(),
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(25),
        "query returns updated row from upsert",
        |rows| {
            (rows.len() == 1
                && rows[0].0.uuid() == &external_id
                && rows[0].1
                    == vec![
                        Value::Text("updated-title".to_string()),
                        Value::Boolean(false),
                    ])
            .then_some(rows)
        },
    )
    .await;

    assert_eq!(rows[0].0.uuid(), &external_id);
    assert_eq!(
        rows[0].1,
        vec![
            Value::Text("updated-title".to_string()),
            Value::Boolean(false)
        ]
    );

    client.shutdown().await.expect("shutdown writer");
    server.shutdown().await;
}

#[tokio::test]
async fn jazz_tools_cli_two_different_users_sync_values() {
    let schema = test_schema();
    let server = TestingServer::start_with_schema(schema.clone()).await;
    let client_alice =
        JazzClient::connect(server.make_client_context_for_user(schema.clone(), "alice-sync-user"))
            .await
            .expect("connect alice client");
    let client_bob =
        JazzClient::connect(server.make_client_context_for_user(schema, "bob-sync-user"))
            .await
            .expect("connect bob client");

    wait_for_edge_query_ready(&client_alice, Duration::from_secs(30)).await;
    wait_for_edge_query_ready(&client_bob, Duration::from_secs(30)).await;

    client_alice
        .insert(
            "todos",
            HashMap::from([
                (
                    "title".to_string(),
                    Value::Text("shared-across-users".to_string()),
                ),
                ("completed".to_string(), Value::Boolean(false)),
            ]),
        )
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
        .insert(
            "todos",
            HashMap::from([
                ("title".to_string(), Value::Text("from-bob".to_string())),
                ("completed".to_string(), Value::Boolean(false)),
            ]),
        )
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
