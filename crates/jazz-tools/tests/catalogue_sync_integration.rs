#![cfg(feature = "test")]

//! E2E catalogue sync integration test.
//!
//! Verifies that schema+lens catalogue objects propagate through the full
//! SyncManager pipeline (not via direct `process_catalogue_update()` calls).

mod support;

use std::collections::HashMap;
use std::time::Duration;

use jazz_tools::schema_manager::{Lens, generate_lens};
use jazz_tools::server::TestingServer;
use jazz_tools::{
    ColumnType, DurabilityTier, JazzClient, QueryBuilder, SchemaBuilder, TableSchema, Value,
};
use support::{
    TestingClient, deny_all_select_permissions, has_added, has_removed,
    publish_allow_all_permissions, publish_permissions, push_catalogue_in_memory,
    wait_for_edge_query_ready, wait_for_query, wait_for_subscription_update,
};

fn user_values_v1(id: jazz_tools::ObjectId, name: &str) -> HashMap<String, Value> {
    HashMap::from([
        ("id".to_string(), Value::Uuid(id)),
        ("name".to_string(), Value::Text(name.to_string())),
    ])
}

fn user_values_v2(id: jazz_tools::ObjectId, name: &str, email: &str) -> HashMap<String, Value> {
    HashMap::from([
        ("id".to_string(), Value::Uuid(id)),
        ("name".to_string(), Value::Text(name.to_string())),
        ("email".to_string(), Value::Text(email.to_string())),
    ])
}

fn schema_v1() -> jazz_tools::Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("name", ColumnType::Text),
        )
        .build()
}

fn schema_v2() -> jazz_tools::Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("name", ColumnType::Text)
                .nullable_column("email", ColumnType::Text),
        )
        .build()
}

fn v1_to_v2_lens() -> Lens {
    generate_lens(&schema_v1(), &schema_v2())
}

async fn seed_schema_catalogue(server: &TestingServer, schema: &jazz_tools::Schema) {
    push_catalogue_in_memory(
        server.server_state(),
        server.app_id(),
        "dev",
        "main",
        std::slice::from_ref(schema),
        &[],
    )
    .await
    .expect("push schema catalogue");
}

/// A dynamic server should fail closed before any permissions head is
/// published, then expose rows once an explicit head is installed.
#[tokio::test]
async fn dynamic_server_denies_reads_until_permissions_head_is_published() {
    let server = TestingServer::start().await;
    let schema = schema_v1();
    seed_schema_catalogue(&server, &schema).await;

    let mut reader_context = server.make_client_context_for_user(schema.clone(), "reader-dynamic");
    reader_context.backend_secret = None;
    reader_context.admin_secret = None;
    let reader = JazzClient::connect(reader_context)
        .await
        .expect("connect reader");

    assert!(
        tokio::time::timeout(
            Duration::from_secs(3),
            reader.query(
                QueryBuilder::new("users").build(),
                Some(DurabilityTier::EdgeServer),
            ),
        )
        .await
        .is_err(),
        "dynamic server should not settle reads before any permissions head is published"
    );

    publish_allow_all_permissions(
        &server.base_url(),
        server.app_id(),
        server.admin_secret(),
        &schema,
    )
    .await;

    wait_for_edge_query_ready(&reader, "users", Duration::from_secs(30)).await;

    let admin =
        JazzClient::connect(server.make_client_context_for_user(schema.clone(), "admin-dynamic"))
            .await
            .expect("connect admin");
    wait_for_edge_query_ready(&admin, "users", Duration::from_secs(30)).await;

    let user_id_value = jazz_tools::ObjectId::new();
    let (user_obj_id, _) = admin
        .create_persisted(
            "users",
            user_values_v1(user_id_value, "visible after permissions"),
            DurabilityTier::EdgeServer,
        )
        .await
        .expect("admin creates user after permissions publish");

    let rows_after_permissions = wait_for_query(
        &reader,
        QueryBuilder::new("users").build(),
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(25),
        "reader sees row after permissions head publish",
        |rows| (rows.len() == 1 && rows[0].0 == user_obj_id).then_some(rows),
    )
    .await;
    assert_eq!(
        rows_after_permissions[0].1,
        vec![
            Value::Uuid(user_id_value),
            Value::Text("visible after permissions".to_string()),
        ]
    );

    admin.shutdown().await.expect("shutdown admin");
    reader.shutdown().await.expect("shutdown reader");
    server.shutdown().await;
}

#[tokio::test]
async fn dynamic_server_keeps_pre_permissions_user_write_hidden_after_publish() {
    let server = TestingServer::start().await;
    let schema = schema_v1();
    seed_schema_catalogue(&server, &schema).await;
    let query = QueryBuilder::new("users").build();
    let observer = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("observer-queued-write")
        .connect()
        .await;
    let writer = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("writer-queued-write")
        .as_user()
        .connect()
        .await;

    let queued_user_id = jazz_tools::ObjectId::new();
    let queued_row_id = jazz_tools::ObjectId::new();
    let queued_write_error = writer
        .create_persisted_with_id(
            "users",
            *queued_row_id.uuid(),
            user_values_v1(queued_user_id, "queued before permissions"),
            DurabilityTier::EdgeServer,
        )
        .await
        .expect_err("pre-permissions persisted create should be rejected");
    let queued_write_error = queued_write_error.to_string();
    assert!(
        queued_write_error.contains("permissions_head_missing"),
        "expected permissions-head rejection, got: {queued_write_error}"
    );
    assert!(
        queued_write_error.contains("no published permissions head"),
        "expected missing permissions-head reason, got: {queued_write_error}"
    );

    assert!(
        tokio::time::timeout(
            Duration::from_secs(3),
            observer.query(query.clone(), Some(DurabilityTier::EdgeServer)),
        )
        .await
        .is_err(),
        "server should not settle observer queries before permissions arrive"
    );

    publish_allow_all_permissions(
        &server.base_url(),
        server.app_id(),
        server.admin_secret(),
        &schema,
    )
    .await;
    wait_for_edge_query_ready(&observer, "users", Duration::from_secs(30)).await;
    wait_for_edge_query_ready(&writer, "users", Duration::from_secs(30)).await;

    let rows_after_publish = wait_for_query(
        &observer,
        query.clone(),
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(25),
        "pre-permissions user write stays hidden after permissions publish",
        |rows| rows.is_empty().then_some(rows),
    )
    .await;
    assert!(rows_after_publish.is_empty());

    let accepted_user_id = jazz_tools::ObjectId::new();
    let (accepted_row_id, _) = writer
        .create_persisted(
            "users",
            user_values_v1(accepted_user_id, "accepted after permissions"),
            DurabilityTier::EdgeServer,
        )
        .await
        .expect("post-publish create should succeed");

    let rows_after_create = wait_for_query(
        &observer,
        query.clone(),
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(25),
        "observer sees accepted row after permissions publish",
        |rows| {
            (rows.len() == 1
                && rows[0].0 == accepted_row_id
                && rows[0].1
                    == vec![
                        Value::Uuid(accepted_user_id),
                        Value::Text("accepted after permissions".to_string()),
                    ])
            .then_some(rows)
        },
    )
    .await;
    assert_eq!(rows_after_create.len(), 1);
    assert_ne!(
        rows_after_create[0].0, queued_row_id,
        "pre-permissions row should stay hidden after permissions arrive"
    );

    writer
        .update_persisted(
            accepted_row_id,
            vec![(
                "name".to_string(),
                Value::Text("updated after permissions".to_string()),
            )],
            DurabilityTier::EdgeServer,
        )
        .await
        .expect("update should succeed once permissions exist");

    let rows_after_update = wait_for_query(
        &observer,
        query.clone(),
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(25),
        "observer sees update after permissions publish",
        |rows| {
            (rows.len() == 1
                && rows[0].0 == accepted_row_id
                && rows[0].1
                    == vec![
                        Value::Uuid(accepted_user_id),
                        Value::Text("updated after permissions".to_string()),
                    ])
            .then_some(rows)
        },
    )
    .await;
    assert_eq!(rows_after_update.len(), 1);

    writer
        .delete_persisted(accepted_row_id, DurabilityTier::EdgeServer)
        .await
        .expect("delete should succeed once permissions exist");

    let rows_after_delete = wait_for_query(
        &observer,
        query,
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(25),
        "observer sees delete after permissions publish",
        |rows| rows.is_empty().then_some(rows),
    )
    .await;
    assert!(rows_after_delete.is_empty());

    observer.shutdown().await.expect("shutdown observer");
    writer.shutdown().await.expect("shutdown writer");
    server.shutdown().await;
}

#[tokio::test]
async fn dynamic_server_rejects_user_write_after_permissions_timeout() {
    let server = TestingServer::start().await;
    let schema = schema_v1();
    seed_schema_catalogue(&server, &schema).await;
    let query = QueryBuilder::new("users").build();
    let observer = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("observer-timeout-write")
        .connect()
        .await;
    let writer = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("writer-timeout-write")
        .as_user()
        .connect()
        .await;

    let denied_user_id = jazz_tools::ObjectId::new();
    let (denied_row_id, _) = writer
        .create(
            "users",
            user_values_v1(denied_user_id, "timed out before permissions"),
        )
        .await
        .expect("optimistic local create before timeout");

    tokio::time::sleep(Duration::from_secs(12)).await;
    assert!(
        tokio::time::timeout(
            Duration::from_secs(3),
            observer.query(query.clone(), Some(DurabilityTier::EdgeServer)),
        )
        .await
        .is_err(),
        "observer query should remain unsettled before permissions are published"
    );
    publish_allow_all_permissions(
        &server.base_url(),
        server.app_id(),
        server.admin_secret(),
        &schema,
    )
    .await;
    wait_for_edge_query_ready(&observer, "users", Duration::from_secs(30)).await;
    wait_for_edge_query_ready(&writer, "users", Duration::from_secs(30)).await;

    let allowed_user_id = jazz_tools::ObjectId::new();
    let (allowed_row_id, _) = writer
        .create_persisted(
            "users",
            user_values_v1(allowed_user_id, "accepted after timeout window"),
            DurabilityTier::EdgeServer,
        )
        .await
        .expect("create should succeed after permissions publish");

    let observer_rows = wait_for_query(
        &observer,
        query,
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(25),
        "observer sees only post-timeout allowed row",
        |rows| (rows.len() == 1 && rows[0].0 == allowed_row_id).then_some(rows),
    )
    .await;
    assert_eq!(observer_rows.len(), 1);
    assert_eq!(observer_rows[0].0, allowed_row_id);
    assert_ne!(
        observer_rows[0].0, denied_row_id,
        "timed-out row should stay rejected even after permissions arrive"
    );
    assert_eq!(
        observer_rows[0].1,
        vec![
            Value::Uuid(allowed_user_id),
            Value::Text("accepted after timeout window".to_string()),
        ]
    );

    observer.shutdown().await.expect("shutdown observer");
    writer.shutdown().await.expect("shutdown writer");
    server.shutdown().await;
}

#[tokio::test]
async fn dynamic_server_live_subscription_replays_on_first_permissions_head_and_retightening() {
    let server = TestingServer::start().await;
    let schema = schema_v1();
    seed_schema_catalogue(&server, &schema).await;
    let query = QueryBuilder::new("users").build();

    let reader = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("reader-subscribe")
        .as_user()
        .connect()
        .await;
    let mut stream = reader
        .subscribe(query.clone())
        .await
        .expect("subscribe reader before permissions");
    let mut log = Vec::new();

    wait_for_subscription_update(
        &mut stream,
        &mut log,
        Duration::from_secs(10),
        "initial empty local subscription snapshot before permissions",
        |updates| !updates.is_empty(),
    )
    .await;
    assert!(
        log[0].is_empty(),
        "plain local subscription should fail closed as an empty local delta before permissions"
    );

    let allow_head = publish_allow_all_permissions(
        &server.base_url(),
        server.app_id(),
        server.admin_secret(),
        &schema,
    )
    .await;

    let admin =
        JazzClient::connect(server.make_client_context_for_user(schema.clone(), "admin-subscribe"))
            .await
            .expect("connect admin");
    wait_for_edge_query_ready(&admin, "users", Duration::from_secs(30)).await;

    let user_id_value = jazz_tools::ObjectId::new();
    let (user_obj_id, _) = admin
        .create_persisted(
            "users",
            user_values_v1(user_id_value, "subscription target"),
            DurabilityTier::EdgeServer,
        )
        .await
        .expect("admin creates user after permissions publish");

    wait_for_subscription_update(
        &mut stream,
        &mut log,
        Duration::from_secs(25),
        "subscription add after first permissions head",
        |updates| has_added(updates, user_obj_id),
    )
    .await;

    publish_permissions(
        &server.base_url(),
        server.app_id(),
        server.admin_secret(),
        &schema,
        deny_all_select_permissions(&schema),
        Some(allow_head.bundle_object_id),
    )
    .await;
    wait_for_subscription_update(
        &mut stream,
        &mut log,
        Duration::from_secs(25),
        "subscription remove after tighter permissions head",
        |updates| has_removed(updates, user_obj_id),
    )
    .await;

    let rows_after_retighten = wait_for_query(
        &reader,
        query,
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(25),
        "reader query after tighter permissions head",
        Some,
    )
    .await;
    assert!(
        rows_after_retighten.is_empty(),
        "reader should lose visibility after permissions are tightened"
    );

    admin.shutdown().await.expect("shutdown admin");
    reader.shutdown().await.expect("shutdown reader");
    server.shutdown().await;
}

/// Alice writes under schema v1. The v2 schema and v1→v2 lens are pushed
/// to the server via the real catalogue sync pipeline. Bob connects with
/// schema v2 and sees Alice's data transformed through the lens.
///
/// ```text
/// alice (v1) ──create user──► server
///                                │
///              push v2 schema + lens via WS sync
///                                │
///                  bob (v2) connects and queries
///                                │
///                                └──► user row with email: null
/// ```
#[tokio::test]
async fn catalogue_sync_e2e_schema_evolution_through_sync_manager() {
    let server = TestingServer::start().await;
    let target_schema = schema_v2();

    // === Push v2 schema + lens to server through the real sync pipeline ===
    push_catalogue_in_memory(
        server.server_state(),
        server.app_id(),
        "dev",
        "main",
        &[schema_v1(), schema_v2()],
        &[v1_to_v2_lens()],
    )
    .await
    .expect("push catalogue");
    publish_allow_all_permissions(
        &server.base_url(),
        server.app_id(),
        server.admin_secret(),
        &target_schema,
    )
    .await;

    // === Alice connects with v1, creates a user after permissions publish ===
    let alice =
        JazzClient::connect(server.make_client_context_for_user(schema_v1(), "alice-catalogue"))
            .await
            .expect("connect alice");

    wait_for_edge_query_ready(&alice, "users", Duration::from_secs(30)).await;

    let user_id_value = jazz_tools::ObjectId::new();
    let (user_obj_id, _) = alice
        .create_persisted(
            "users",
            user_values_v1(user_id_value, "Alice Smith"),
            DurabilityTier::EdgeServer,
        )
        .await
        .expect("alice creates user after permissions publish");

    // === Bob connects with v2, queries — should see Alice's row with email: null ===
    let bob =
        JazzClient::connect(server.make_client_context_for_user(target_schema, "bob-catalogue"))
            .await
            .expect("connect bob");

    wait_for_edge_query_ready(&bob, "users", Duration::from_secs(30)).await;

    let bob_rows = wait_for_query(
        &bob,
        QueryBuilder::new("users").build(),
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(25),
        "bob sees alice's user with email column",
        |rows| (rows.len() == 1 && rows[0].0 == user_obj_id).then_some(rows),
    )
    .await;

    assert_eq!(bob_rows.len(), 1, "bob should see exactly one user");
    assert_eq!(bob_rows[0].0, user_obj_id);

    let values = &bob_rows[0].1;
    assert_eq!(
        values[0],
        Value::Uuid(user_id_value),
        "id should match alice's user"
    );
    assert_eq!(
        values[1],
        Value::Text("Alice Smith".to_string()),
        "name should match alice's user"
    );
    assert_eq!(
        values[2],
        Value::Null,
        "email should be null (default from lens transform)"
    );

    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    server.shutdown().await;
}

/// Bob writes under schema v2 after the server has received the v1/v2
/// catalogue. Alice connects with schema v1 and sees Bob's data transformed
/// through the backward lens.
///
/// ```text
/// push v1 schema + v2 schema + lens ──► server
///                                        │
/// bob (v2) ──create user with email──► server
///                                        │
///                  alice (v1) connects and queries
///                                        │
///                                        └──► user row without email column
/// ```
#[tokio::test]
async fn catalogue_sync_e2e_backward_data_migration_through_sync_manager() {
    let server = TestingServer::start().await;
    let target_schema = schema_v2();

    // Seed the server with both schemas and the v1<->v2 lens before clients connect.
    push_catalogue_in_memory(
        server.server_state(),
        server.app_id(),
        "dev",
        "main",
        &[schema_v1(), schema_v2()],
        &[v1_to_v2_lens()],
    )
    .await
    .expect("push catalogue");
    publish_allow_all_permissions(
        &server.base_url(),
        server.app_id(),
        server.admin_secret(),
        &target_schema,
    )
    .await;

    // === Bob connects with v2, creates a user with the new email column ===
    let bob = JazzClient::connect(server.make_client_context_for_user(schema_v2(), "bob-backward"))
        .await
        .expect("connect bob");

    wait_for_edge_query_ready(&bob, "users", Duration::from_secs(30)).await;

    let user_id_value = jazz_tools::ObjectId::new();
    let user_email = "bob@example.com";
    let (user_obj_id, _) = bob
        .create(
            "users",
            user_values_v2(user_id_value, "Bob Backward", user_email),
        )
        .await
        .expect("bob creates user");

    wait_for_query(
        &bob,
        QueryBuilder::new("users").build(),
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(25),
        "bob's v2 user settled at edge",
        |rows| (rows.len() == 1 && rows[0].0 == user_obj_id).then_some(rows),
    )
    .await;

    // === Alice connects with v1, queries — should see Bob's row without email ===
    let alice =
        JazzClient::connect(server.make_client_context_for_user(schema_v1(), "alice-backward"))
            .await
            .expect("connect alice");

    wait_for_edge_query_ready(&alice, "users", Duration::from_secs(30)).await;

    let alice_rows = wait_for_query(
        &alice,
        QueryBuilder::new("users").build(),
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(25),
        "alice sees bob's user without email column",
        |rows| (rows.len() == 1 && rows[0].0 == user_obj_id).then_some(rows),
    )
    .await;

    assert_eq!(alice_rows.len(), 1, "alice should see exactly one user");
    assert_eq!(alice_rows[0].0, user_obj_id);

    let values = &alice_rows[0].1;
    assert_eq!(
        values.len(),
        2,
        "v1 view should not include the email column"
    );
    assert_eq!(
        values[0],
        Value::Uuid(user_id_value),
        "id should match bob's user"
    );
    assert_eq!(
        values[1],
        Value::Text("Bob Backward".to_string()),
        "name should match bob's user"
    );

    bob.shutdown().await.expect("shutdown bob");
    alice.shutdown().await.expect("shutdown alice");
    server.shutdown().await;
}

#[tokio::test]
async fn catalogue_sync_e2e_schema_evolution_keeps_authorization_through_v1_head() {
    let server = TestingServer::start().await;
    let query = QueryBuilder::new("users").build();
    let v1_schema = schema_v1();
    push_catalogue_in_memory(
        server.server_state(),
        server.app_id(),
        "dev",
        "main",
        std::slice::from_ref(&v1_schema),
        &[],
    )
    .await
    .expect("push v1 catalogue before publishing v1 permissions");
    publish_allow_all_permissions(
        &server.base_url(),
        server.app_id(),
        server.admin_secret(),
        &v1_schema,
    )
    .await;

    let alice =
        JazzClient::connect(server.make_client_context_for_user(schema_v1(), "alice-v1-head"))
            .await
            .expect("connect alice");

    wait_for_edge_query_ready(&alice, "users", Duration::from_secs(30)).await;

    let user_id_value = jazz_tools::ObjectId::new();
    let (user_obj_id, _) = alice
        .create_persisted(
            "users",
            user_values_v1(user_id_value, "Alice Through Lens"),
            DurabilityTier::EdgeServer,
        )
        .await
        .expect("alice creates user after v1 permissions publish");

    wait_for_query(
        &alice,
        query.clone(),
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(25),
        "alice row settled before v1 permissions publish",
        |rows| (rows.len() == 1 && rows[0].0 == user_obj_id).then_some(rows),
    )
    .await;

    let v1_schema = schema_v1();
    publish_allow_all_permissions(
        &server.base_url(),
        server.app_id(),
        server.admin_secret(),
        &v1_schema,
    )
    .await;
    push_catalogue_in_memory(
        server.server_state(),
        server.app_id(),
        "dev",
        "main",
        &[v1_schema, schema_v2()],
        &[v1_to_v2_lens()],
    )
    .await
    .expect("push catalogue after v1 permissions head");

    let bob = JazzClient::connect(server.make_client_context_for_user(schema_v2(), "bob-v2-head"))
        .await
        .expect("connect bob");
    wait_for_edge_query_ready(&bob, "users", Duration::from_secs(30)).await;

    let bob_rows = wait_for_query(
        &bob,
        query,
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(25),
        "bob sees alice row through v1 authorization schema",
        |rows| (rows.len() == 1 && rows[0].0 == user_obj_id).then_some(rows),
    )
    .await;
    assert_eq!(
        bob_rows[0].1,
        vec![
            Value::Uuid(user_id_value),
            Value::Text("Alice Through Lens".to_string()),
            Value::Null,
        ]
    );

    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    server.shutdown().await;
}
