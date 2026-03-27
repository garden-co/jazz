#![cfg(feature = "test")]

//! Integration test for ephemeral payload claims merging into the server session.
//!
//! Verifies the fix from 094a5626: when a JWT-authenticated client supplies
//! per-subscription claims (e.g. join_code for invite flows), those claims are
//! merged into the server-established session so that policy conditions like
//! `@session.claims.join_code` evaluate correctly — without allowing user_id
//! spoofing.
//!
//! Scenario:
//!
//!   ┌─────────┐         ┌──────────┐         ┌─────────┐
//!   │  admin   │──create─▶  server  ◀──query──│  alice   │
//!   │ (insert) │         │ (rooms)  │         │ (claims) │
//!   └─────────┘         └──────────┘         └─────────┘
//!
//!   1. Admin creates a room with join_code = "secret-123"
//!   2. Alice connects with JWT claims { join_code: "secret-123" }
//!      → she can see the room (claims merged into session)
//!   3. Bob connects with JWT claims { join_code: "wrong-code" }
//!      → he cannot see the room
//!   4. Carol connects with no claims at all
//!      → she cannot see the room

mod support;

use std::collections::HashMap;
use std::time::Duration;

use jazz_tools::query_manager::policy::PolicyExpr;
use jazz_tools::query_manager::types::TablePolicies;
use jazz_tools::server::TestingServer;
use jazz_tools::{
    ColumnType, DurabilityTier, QueryBuilder, Schema, SchemaBuilder, TableSchema, Value,
};
use serde_json::json;
use support::{TestingClient, wait_for_query};

const READY_TIMEOUT: Duration = Duration::from_secs(30);
const QUERY_TIMEOUT: Duration = Duration::from_secs(25);

/// Schema: a `rooms` table where SELECT requires
/// `join_code = @session.claims.join_code`.
fn claims_gated_schema() -> Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("rooms")
                .column("name", ColumnType::Text)
                .column("join_code", ColumnType::Text)
                .policies(
                    TablePolicies::new()
                        .with_select(PolicyExpr::eq_session(
                            "join_code",
                            vec!["claims".into(), "join_code".into()],
                        ))
                        .with_insert(PolicyExpr::True),
                ),
        )
        .build()
}

#[tokio::test]
async fn ephemeral_claims_merged_into_session() {
    let server = TestingServer::start_with_schema(claims_gated_schema()).await;
    let schema = claims_gated_schema();

    // Admin creates a room with join_code = "secret-123"
    let admin = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("admin")
        .ready_on("rooms", READY_TIMEOUT)
        .connect()
        .await;

    let (room_id, _) = admin
        .create(
            "rooms",
            HashMap::from([
                ("name".to_string(), Value::Text("Party Room".to_string())),
                (
                    "join_code".to_string(),
                    Value::Text("secret-123".to_string()),
                ),
            ]),
        )
        .await
        .expect("admin creates room");

    let query = QueryBuilder::new("rooms").build();

    // Alice: correct join_code claim → should see the room
    let alice = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("alice")
        .with_claims(json!({"join_code": "secret-123"}))
        .ready_on("rooms", READY_TIMEOUT)
        .connect()
        .await;

    wait_for_query(
        &alice,
        query.clone(),
        Some(DurabilityTier::EdgeServer),
        QUERY_TIMEOUT,
        "alice sees room with matching join_code",
        |rows| rows.iter().any(|(id, _)| *id == room_id).then_some(()),
    )
    .await;

    // Bob: wrong join_code claim → should NOT see the room
    let bob = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("bob")
        .with_claims(json!({"join_code": "wrong-code"}))
        .ready_on("rooms", READY_TIMEOUT)
        .connect()
        .await;

    let bob_rows = bob
        .query(query.clone(), Some(DurabilityTier::EdgeServer))
        .await
        .expect("bob queries rooms");

    assert!(
        !bob_rows.iter().any(|(id, _)| *id == room_id),
        "bob should NOT see the room (wrong join_code)"
    );

    // Carol: no claims at all → should NOT see the room
    let carol = TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id("carol")
        .as_user()
        .ready_on("rooms", READY_TIMEOUT)
        .connect()
        .await;

    let carol_rows = carol
        .query(query.clone(), Some(DurabilityTier::EdgeServer))
        .await
        .expect("carol queries rooms");

    assert!(
        !carol_rows.iter().any(|(id, _)| *id == room_id),
        "carol should NOT see the room (no claims)"
    );
}
