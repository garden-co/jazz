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
    AppContext, ColumnType, DurabilityTier, JazzClient, QueryBuilder, SchemaBuilder, TableSchema,
    Value,
};
use support::{
    publish_schema_and_permissions, push_catalogue_in_memory, wait_for_edge_query_ready,
    wait_for_query,
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
                .column("name", ColumnType::Text)
                .policies(support::allow_all_policies()),
        )
        .build()
}

fn schema_v2() -> jazz_tools::Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("users")
                .column("id", ColumnType::Uuid)
                .column("name", ColumnType::Text)
                .nullable_column("email", ColumnType::Text)
                .policies(support::allow_all_policies()),
        )
        .build()
}

fn v1_to_v2_lens() -> Lens {
    generate_lens(&schema_v1(), &schema_v2())
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

/// Alice writes under schema v1. The v2 schema and v1→v2 lens are pushed
/// to the server via the real catalogue sync pipeline. Bob connects with
/// schema v2 and sees Alice's data transformed through the lens.
///
/// ```text
/// alice (v1) ──create user──► server
///                                │
///              push v2 schema + lens via HTTP /sync
///                                │
///                  bob (v2) connects and queries
///                                │
///                                └──► user row with email: null
/// ```
#[tokio::test]
async fn catalogue_sync_e2e_schema_evolution_through_sync_manager() {
    let server = TestingServer::start().await;
    publish_schema_and_permissions(&server.base_url(), server.admin_secret(), &schema_v1())
        .await
        .expect("publish v1 schema and permissions");

    // === Alice connects with v1, creates a user ===
    let alice = JazzClient::connect(make_user_context(&server, schema_v1(), "alice-catalogue"))
        .await
        .expect("connect alice");

    wait_for_edge_query_ready(&alice, "users", Duration::from_secs(30)).await;

    let user_id_value = jazz_tools::ObjectId::new();
    let (user_obj_id, _) = alice
        .create("users", user_values_v1(user_id_value, "Alice Smith"))
        .await
        .expect("alice creates user");

    // Wait for Alice's row to settle at EdgeServer
    wait_for_query(
        &alice,
        QueryBuilder::new("users").build(),
        Some(DurabilityTier::EdgeServer),
        Duration::from_secs(25),
        "alice's user settled at edge",
        |rows| (rows.len() == 1 && rows[0].0 == user_obj_id).then_some(rows),
    )
    .await;

    // === Push v2 schema + lens to server through the real sync pipeline ===
    push_catalogue_in_memory(
        &server.base_url(),
        server.app_id(),
        "dev",
        "main",
        server.admin_secret(),
        &[schema_v1(), schema_v2()],
        &[v1_to_v2_lens()],
    )
    .await
    .expect("push catalogue");

    // === Bob connects with v2, queries — should see Alice's row with email: null ===
    let bob = JazzClient::connect(make_user_context(&server, schema_v2(), "bob-catalogue"))
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

    // Seed the server with both schemas and the v1<->v2 lens before clients connect.
    push_catalogue_in_memory(
        &server.base_url(),
        server.app_id(),
        "dev",
        "main",
        server.admin_secret(),
        &[schema_v1(), schema_v2()],
        &[v1_to_v2_lens()],
    )
    .await
    .expect("push catalogue");

    // === Bob connects with v2, creates a user with the new email column ===
    let bob = JazzClient::connect(make_user_context(&server, schema_v2(), "bob-backward"))
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
    let alice = JazzClient::connect(make_user_context(&server, schema_v1(), "alice-backward"))
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
