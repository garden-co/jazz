#![cfg(feature = "test")]

//! E2E catalogue sync integration test.
//!
//! Verifies that schema+lens catalogue objects propagate through the full
//! SyncManager pipeline (not via direct `process_catalogue_update()` calls).

mod support;

use std::collections::HashMap;
use std::time::Duration;

use jazz_tools::query_manager::types::SchemaHash;
use jazz_tools::schema_catalogue;
use jazz_tools::schema_manager::{Lens, LensOp, LensTransform};
use jazz_tools::server::TestingServer;
use jazz_tools::{
    ColumnType, DurabilityTier, JazzClient, QueryBuilder, SchemaBuilder, TableSchema, Value,
};
use support::{wait_for_edge_query_ready, wait_for_query};

fn user_values(id: jazz_tools::ObjectId, name: &str) -> HashMap<String, Value> {
    HashMap::from([
        ("id".to_string(), Value::Uuid(id)),
        ("name".to_string(), Value::Text(name.to_string())),
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
    let v1_hash = SchemaHash::compute(&schema_v1());
    let v2_hash = SchemaHash::compute(&schema_v2());
    let forward = LensTransform::with_ops(vec![LensOp::AddColumn {
        table: "users".to_string(),
        column: "email".to_string(),
        column_type: ColumnType::Text,
        default: Value::Null,
    }]);
    Lens::new(v1_hash, v2_hash, forward)
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

    // === Alice connects with v1, creates a user ===
    let alice =
        JazzClient::connect(server.make_client_context_for_user(schema_v1(), "alice-catalogue"))
            .await
            .expect("connect alice");

    wait_for_edge_query_ready(&alice, "users", Duration::from_secs(30)).await;

    let user_id_value = jazz_tools::ObjectId::new();
    let (user_obj_id, _) = alice
        .create("users", user_values(user_id_value, "Alice Smith"))
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
    schema_catalogue::push_in_memory(
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
    let bob =
        JazzClient::connect(server.make_client_context_for_user(schema_v2(), "bob-catalogue"))
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
