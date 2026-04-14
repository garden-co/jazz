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
use std::time::Duration;

use jazz_tools::server::TestingServer;
use jazz_tools::{
    AppContext, ClientStorage, ColumnType, JazzClient, QueryBuilder, Schema, SchemaBuilder,
    Session, TableSchema, Value, identity,
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

/// A write committed by a local-first client records `$createdBy` as the
/// principal derived from the seed. This wires the `$createdBy` magic column
/// up to the Ed25519 identity path end-to-end.
#[tokio::test]
async fn local_first_writes_carry_derived_principal_as_created_by() {
    let server = TestingServer::start_with_schema(test_schema()).await;

    let alice = JazzClient::connect(local_first_context(
        &server,
        test_schema(),
        &alice_seed(),
        ClientStorage::Memory,
    ))
    .await
    .expect("connect alice");

    let alice_user_id = identity::derive_user_id(&alice_seed()).to_string();

    let (todo_id, _) = alice
        .for_session(Session::new(&alice_user_id))
        .create("todos", todo_values("provenance check", false))
        .await
        .expect("alice creates todo");

    let expected = vec![
        Value::Text("provenance check".to_string()),
        Value::Boolean(false),
        Value::Text(alice_user_id),
    ];

    wait_for_rows(
        &alice,
        QueryBuilder::new("todos")
            .select(&["title", "completed", "$createdBy"])
            .build(),
        "todo records alice's derived principal in $createdBy",
        |rows| has_row(&rows, todo_id, &expected).then_some(()),
    )
    .await;

    alice.shutdown().await.expect("shutdown alice");
    server.shutdown().await;
}

/// A local-first client and a default HS256-JWT client share the same server
/// and converge on each other's rows, each attributed to the matching
/// principal. Guards the mixed-auth path some apps will run during migration.
#[tokio::test]
async fn local_first_and_jwt_clients_coexist() {
    let server = TestingServer::start_with_schema(test_schema()).await;

    let alice = JazzClient::connect(local_first_context(
        &server,
        test_schema(),
        &alice_seed(),
        ClientStorage::Memory,
    ))
    .await
    .expect("connect alice (local-first)");
    let alice_user_id = identity::derive_user_id(&alice_seed()).to_string();

    // Bob authenticates via the default HS256 JWT minted by TestingServer.
    let mut bob_ctx = server.make_client_context_for_user(test_schema(), "bob");
    bob_ctx.backend_secret = None;
    bob_ctx.admin_secret = None;
    let bob = JazzClient::connect(bob_ctx)
        .await
        .expect("connect bob (jwt)");

    let (alice_id, _) = alice
        .for_session(Session::new(&alice_user_id))
        .create("todos", todo_values("alice via ed25519", false))
        .await
        .expect("alice creates todo");
    let (bob_id, _) = bob
        .for_session(Session::new("bob"))
        .create("todos", todo_values("bob via jwt", true))
        .await
        .expect("bob creates todo");

    let alice_row = vec![
        Value::Text("alice via ed25519".to_string()),
        Value::Boolean(false),
        Value::Text(alice_user_id.clone()),
    ];
    let bob_row = vec![
        Value::Text("bob via jwt".to_string()),
        Value::Boolean(true),
        Value::Text("bob".to_string()),
    ];

    wait_for_rows(
        &alice,
        QueryBuilder::new("todos")
            .select(&["title", "completed", "$createdBy"])
            .build(),
        "alice converges with bob's jwt-attributed row",
        |rows| {
            (has_row(&rows, alice_id, &alice_row) && has_row(&rows, bob_id, &bob_row)).then_some(())
        },
    )
    .await;

    wait_for_rows(
        &bob,
        QueryBuilder::new("todos")
            .select(&["title", "completed", "$createdBy"])
            .build(),
        "bob converges with alice's ed25519-attributed row",
        |rows| {
            (has_row(&rows, alice_id, &alice_row) && has_row(&rows, bob_id, &bob_row)).then_some(())
        },
    )
    .await;

    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    server.shutdown().await;
}

/// Writes committed while the bearer token is expired stay queued in local
/// storage; once the client reconnects with a freshly-minted token, the
/// queued write flushes to the server.
#[tokio::test]
async fn expired_token_reconnect_flushes_queued_writes() {
    let server = TestingServer::start_with_schema(test_schema()).await;

    let audience = server.app_id().to_string();
    let seed = alice_seed();

    // Base context with persistent storage; swap in a 1-second TTL token so we
    // can drive it past expiry in the test timeline.
    let mut ctx = local_first_context(&server, test_schema(), &seed, ClientStorage::Persistent);
    ctx.jwt_token = Some(
        identity::mint_local_first_token(&seed, &audience, 1).expect("mint short-lived token"),
    );

    let client = JazzClient::connect(ctx.clone())
        .await
        .expect("connect with short-lived token");

    // Pre-expiry write: confirm the session is healthy and reaches the edge.
    let (pre_id, pre_values) = client
        .create("todos", todo_values("pre-expiry", false))
        .await
        .expect("pre-expiry create");
    wait_for_rows(
        &client,
        QueryBuilder::new("todos").build(),
        "pre-expiry todo settles at edge server",
        |rows| has_row(&rows, pre_id, &pre_values).then_some(()),
    )
    .await;

    // Let the token lapse. 1s TTL + 1500ms yields a definitively-expired token.
    tokio::time::sleep(Duration::from_millis(1500)).await;

    // Post-expiry write: commit succeeds locally even though the server would
    // now reject the bearer token on sync.
    let (queued_id, queued_values) = client
        .create("todos", todo_values("post-expiry", true))
        .await
        .expect("post-expiry create");

    client.shutdown().await.expect("shutdown expired client");

    // Reconnect with a fresh token; the data_dir is reused so the queued
    // write replays.
    let mut fresh_ctx = ctx.clone();
    fresh_ctx.jwt_token = Some(
        identity::mint_local_first_token(&seed, &audience, TOKEN_TTL_SECS)
            .expect("mint refreshed token"),
    );

    let reconnected = JazzClient::connect(fresh_ctx)
        .await
        .expect("reconnect with fresh token");

    wait_for_rows(
        &reconnected,
        QueryBuilder::new("todos").build(),
        "queued post-expiry write flushes after token refresh",
        |rows| {
            (has_row(&rows, pre_id, &pre_values) && has_row(&rows, queued_id, &queued_values))
                .then_some(())
        },
    )
    .await;

    reconnected.shutdown().await.expect("shutdown reconnected");
    server.shutdown().await;
}
