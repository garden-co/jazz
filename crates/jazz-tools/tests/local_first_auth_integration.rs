#![cfg(feature = "test")]

//! End-to-end integration coverage for local-first (Ed25519 seed) auth.
//!
//! These tests prove that a `JazzClient` carrying an Ed25519-minted Bearer
//! token syncs correctly, that seeds map deterministically to principals
//! across devices, and that state persists across reconnects.
//!
//! Narrow middleware-level assertions (wrong audience, expired token,
//! disabled flag) live in `local_first_auth.rs` and `src/middleware/auth.rs`.

mod support;

use std::collections::HashMap;

use jazz_tools::server::TestingServer;
use jazz_tools::{
    AppContext, ClientStorage, ColumnType, JazzClient, QueryBuilder, Schema, SchemaBuilder,
    TableSchema, Value, identity,
};

use support::{has_row, wait_for_rows};

const TOKEN_TTL_SECS: u64 = 3600;

fn alice_seed() -> [u8; 32] {
    let mut seed = [0u8; 32];
    seed[0] = 0xAA;
    seed[31] = 0x01;
    seed
}

fn bob_seed() -> [u8; 32] {
    let mut seed = [0u8; 32];
    seed[0] = 0xBB;
    seed[31] = 0x02;
    seed
}

fn test_schema() -> Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("todos")
                .column("title", ColumnType::Text)
                .column("completed", ColumnType::Boolean),
        )
        .build()
}

/// Build an `AppContext` that authenticates with a local-first Ed25519 token
/// derived from `seed`. Drops the backend/admin secrets so the server must
/// accept (or reject) the Bearer token on its own merits.
fn local_first_context(
    server: &TestingServer,
    schema: Schema,
    seed: &[u8; 32],
    storage: ClientStorage,
) -> AppContext {
    let user_id = identity::derive_user_id(seed).to_string();
    let mut ctx = server.make_client_context_for_user(schema, &user_id);
    let audience = server.app_id().to_string();
    let token = identity::mint_local_first_token(seed, &audience, TOKEN_TTL_SECS)
        .expect("mint local-first token");
    ctx.jwt_token = Some(token);
    ctx.backend_secret = None;
    ctx.admin_secret = None;
    ctx.storage = storage;
    ctx
}

fn todo_values(title: &str, completed: bool) -> HashMap<String, Value> {
    HashMap::from([
        ("title".to_string(), Value::Text(title.to_string())),
        ("completed".to_string(), Value::Boolean(completed)),
    ])
}

/// Same seed on two independent clients produces the same principal, and a
/// row written on device A syncs to device B purely through server-side
/// principal recognition.
#[tokio::test]
async fn same_seed_syncs_across_devices() {
    let server = TestingServer::start_with_schema(test_schema()).await;

    let alice_device_a = JazzClient::connect(local_first_context(
        &server,
        test_schema(),
        &alice_seed(),
        ClientStorage::Memory,
    ))
    .await
    .expect("connect alice device A");

    let (todo_id, expected_values) = alice_device_a
        .create("todos", todo_values("buy milk", false))
        .await
        .expect("alice device A creates todo");

    let alice_device_b = JazzClient::connect(local_first_context(
        &server,
        test_schema(),
        &alice_seed(),
        ClientStorage::Memory,
    ))
    .await
    .expect("connect alice device B");

    wait_for_rows(
        &alice_device_b,
        QueryBuilder::new("todos").build(),
        "alice device B sees todo written by device A",
        |rows| has_row(&rows, todo_id, &expected_values).then_some(()),
    )
    .await;

    alice_device_a.shutdown().await.expect("shutdown device A");
    alice_device_b.shutdown().await.expect("shutdown device B");
    server.shutdown().await;
}

/// Alice's and Bob's seeds derive distinct principal IDs, and each client
/// sees the other's row once the server propagates it. This is the baseline
/// for any future per-principal permission scoping.
#[tokio::test]
async fn different_seeds_produce_distinct_principals() {
    let server = TestingServer::start_with_schema(test_schema()).await;

    let alice_user_id = identity::derive_user_id(&alice_seed()).to_string();
    let bob_user_id = identity::derive_user_id(&bob_seed()).to_string();
    assert_ne!(
        alice_user_id, bob_user_id,
        "distinct seeds must derive distinct principal IDs"
    );

    let alice = JazzClient::connect(local_first_context(
        &server,
        test_schema(),
        &alice_seed(),
        ClientStorage::Memory,
    ))
    .await
    .expect("connect alice");

    let bob = JazzClient::connect(local_first_context(
        &server,
        test_schema(),
        &bob_seed(),
        ClientStorage::Memory,
    ))
    .await
    .expect("connect bob");

    let (alice_todo_id, alice_values) = alice
        .create("todos", todo_values("alice's task", false))
        .await
        .expect("alice creates todo");
    let (bob_todo_id, bob_values) = bob
        .create("todos", todo_values("bob's task", true))
        .await
        .expect("bob creates todo");

    wait_for_rows(
        &alice,
        QueryBuilder::new("todos").build(),
        "alice converges on both todos",
        |rows| {
            (has_row(&rows, alice_todo_id, &alice_values)
                && has_row(&rows, bob_todo_id, &bob_values))
            .then_some(())
        },
    )
    .await;

    wait_for_rows(
        &bob,
        QueryBuilder::new("todos").build(),
        "bob converges on both todos",
        |rows| {
            (has_row(&rows, alice_todo_id, &alice_values)
                && has_row(&rows, bob_todo_id, &bob_values))
            .then_some(())
        },
    )
    .await;

    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    server.shutdown().await;
}

/// A client that saves its seed and data_dir should reconnect as the same
/// principal and still see its own rows locally.
#[tokio::test]
async fn persistent_seed_reconnects_as_same_principal() {
    let server = TestingServer::start_with_schema(test_schema()).await;

    let context = local_first_context(
        &server,
        test_schema(),
        &alice_seed(),
        ClientStorage::Persistent,
    );
    let expected_user_id = identity::derive_user_id(&alice_seed()).to_string();

    let first = JazzClient::connect(context.clone())
        .await
        .expect("first connect");
    let (todo_id, expected_values) = first
        .create("todos", todo_values("remember this", false))
        .await
        .expect("create todo");

    // Let the row settle at EdgeServer so we can verify the server recognized
    // our principal on the first connect.
    wait_for_rows(
        &first,
        QueryBuilder::new("todos").build(),
        "todo settles at server under alice's principal",
        |rows| has_row(&rows, todo_id, &expected_values).then_some(()),
    )
    .await;

    first.shutdown().await.expect("shutdown first");

    let reconnected = JazzClient::connect(context.clone())
        .await
        .expect("reconnect with same seed and data_dir");

    // Local state survived: row is visible immediately from local storage.
    let local_rows = reconnected
        .query(QueryBuilder::new("todos").build(), None)
        .await
        .expect("local query after reconnect");
    assert!(
        has_row(&local_rows, todo_id, &expected_values),
        "reconnected client should see its own persisted row locally"
    );

    // Server still recognizes the same principal: an EdgeServer query
    // (which requires successful server auth) succeeds and returns the row.
    wait_for_rows(
        &reconnected,
        QueryBuilder::new("todos").build(),
        "reconnected client re-authenticates with same principal and reads from edge",
        |rows| has_row(&rows, todo_id, &expected_values).then_some(()),
    )
    .await;

    assert_eq!(
        identity::derive_user_id(&alice_seed()).to_string(),
        expected_user_id,
        "derived user_id must stay stable across reconnects"
    );

    reconnected.shutdown().await.expect("shutdown reconnected");
    server.shutdown().await;
}
