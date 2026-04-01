use std::collections::HashMap;
use std::time::Duration;

use super::support::{connect_ready_user, wait_for_rows};
use jazz_tools::query_manager::policy::PolicyExpr;
use jazz_tools::query_manager::session::Session;
use jazz_tools::query_manager::types::{TablePolicies, TableSchemaBuilder};
use jazz_tools::server::TestingServer;
use jazz_tools::{
    ColumnType, JazzClient, ObjectId, QueryBuilder, Schema, SchemaBuilder, TableSchema, Value,
};

const READY_TIMEOUT: Duration = Duration::from_secs(30);

fn make_notes_schema(table_name: &str, policies: TablePolicies) -> TableSchemaBuilder {
    TableSchema::builder(table_name)
        .column("title", ColumnType::Text)
        .policies(policies)
}

fn note_input(title: &str) -> HashMap<String, Value> {
    HashMap::from([("title".to_string(), Value::Text(title.to_string()))])
}

fn provenance_values(title: &str, created_by: &str, updated_by: &str) -> Vec<Value> {
    vec![
        Value::Text(title.to_string()),
        Value::Text(created_by.to_string()),
        Value::Text(updated_by.to_string()),
    ]
}

async fn create_note_as(client: &JazzClient, user_id: &str, title: &str) -> ObjectId {
    client
        .for_session(Session::new(user_id))
        .create("notes", note_input(title))
        .await
        .expect("create note with session-authored provenance")
        .0
}

async fn start_alice_and_bob_server(schema: Schema) -> (TestingServer, JazzClient, JazzClient) {
    let server = TestingServer::builder()
        .with_schema(schema.clone())
        .start()
        .await;

    let ready_table = schema
        .keys()
        .next()
        .map(|table| table.as_str().to_string())
        .expect("schema must contain at least one table");

    let alice = connect_ready_user(&server, &schema, "alice", &ready_table, READY_TIMEOUT).await;
    let bob = connect_ready_user(&server, &schema, "bob", &ready_table, READY_TIMEOUT).await;

    (server, alice, bob)
}

/// Verifies that `$createdBy` policies scope read/update/delete access to the
/// creator when every mutation comes from an ordinary session client.
///
/// Actors: `alice` creates one note, `bob` creates another and then tries to
/// mutate Alice's row.
///
/// ```text
/// alice client ──create──────────────► server ──query──► alice sees alice row
/// bob client ───create───────────────► server ──query──► bob sees bob row
/// bob client ───update/delete alice row───────► server ──policy check──► ✗
/// ```
#[tokio::test]
async fn created_by_policies_scope_crud_to_creators() {
    let created_by_policy = PolicyExpr::eq_session("$createdBy", vec!["user_id".into()]);
    let schema = SchemaBuilder::new()
        .table(make_notes_schema(
            "notes",
            TablePolicies::new()
                .with_select(created_by_policy.clone())
                .with_insert(PolicyExpr::True)
                .with_update(Some(created_by_policy.clone()), created_by_policy.clone())
                .with_delete(created_by_policy),
        ))
        .build();
    let (server, alice, bob) = start_alice_and_bob_server(schema.clone()).await;
    let alice_note = create_note_as(&alice, "alice", "alice note").await;
    let bob_note = create_note_as(&bob, "bob", "bob note").await;

    let query = QueryBuilder::new("notes")
        .select(&["title", "$createdBy", "$updatedBy"])
        .order_by("title")
        .build();

    let alice_rows = wait_for_rows(
        &alice,
        query.clone(),
        "alice sees only creator-owned row",
        |rows| (rows.len() == 1 && rows[0].0 == alice_note).then_some(rows),
    )
    .await;
    assert_eq!(
        alice_rows[0].1,
        provenance_values("alice note", "alice", "alice")
    );

    let bob_rows = wait_for_rows(
        &bob,
        query.clone(),
        "bob sees only creator-owned row",
        |rows| (rows.len() == 1 && rows[0].0 == bob_note).then_some(rows),
    )
    .await;
    assert_eq!(bob_rows[0].1, provenance_values("bob note", "bob", "bob"));

    let denied_update = bob
        .for_session(Session::new("bob"))
        .update(
            alice_note,
            vec![("title".to_string(), Value::Text("bob edit".into()))],
        )
        .await;
    assert!(
        denied_update.is_err(),
        "bob should not be able to update alice's row under $createdBy policy"
    );
    let denied_delete = bob
        .for_session(Session::new("bob"))
        .delete(alice_note)
        .await;
    assert!(
        denied_delete.is_err(),
        "bob should not be able to delete alice's row under $createdBy policy"
    );

    let alice_rows = wait_for_rows(
        &alice,
        query.clone(),
        "alice row survives bob's rejected mutations",
        |rows| {
            (rows.len() == 1
                && rows[0].0 == alice_note
                && rows[0].1 == provenance_values("alice note", "alice", "alice"))
            .then_some(rows)
        },
    )
    .await;
    assert_eq!(alice_rows.len(), 1);

    let bob_rows = wait_for_rows(
        &bob,
        query.clone(),
        "bob still cannot see alice's row",
        |rows| {
            (rows.len() == 1
                && rows[0].0 == bob_note
                && rows[0].1 == provenance_values("bob note", "bob", "bob"))
            .then_some(rows)
        },
    )
    .await;
    assert_eq!(bob_rows.len(), 1);

    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    server.shutdown().await;
}

/// Verifies that a `$updatedBy` select policy moves visibility to the latest
/// editor and preserves creator timestamps across edits using only session
/// clients.
///
/// Actors: `alice` creates the row, `bob` performs the later update.
///
/// ```text
/// alice client ──create(shared=true)──► server ──query──► alice and bob see row
/// bob client ───update(shared=false)─► server ──$updatedBy = bob
///                                       ├── alice query──► row hidden
///                                       └── bob query────► row visible
/// ```
#[tokio::test]
async fn updated_by_select_policy_moves_visibility_to_last_editor() {
    let updated_by_policy = PolicyExpr::eq_session("$updatedBy", vec!["user_id".into()]);
    let shared_policy = PolicyExpr::eq_literal("shared", Value::Boolean(true));
    let schema = SchemaBuilder::new()
        .table(
            TableSchema::builder("notes")
                .column("title", ColumnType::Text)
                .column("shared", ColumnType::Boolean)
                .policies(
                    TablePolicies::new()
                        .with_select(PolicyExpr::or(vec![shared_policy, updated_by_policy]))
                        .with_insert(PolicyExpr::True)
                        .with_update(Some(PolicyExpr::True), PolicyExpr::True),
                ),
        )
        .build();
    let (server, alice, bob) = start_alice_and_bob_server(schema.clone()).await;
    let query = QueryBuilder::new("notes")
        .select(&[
            "title",
            "shared",
            "$createdBy",
            "$updatedBy",
            "$createdAt",
            "$updatedAt",
        ])
        .build();
    // The shared flag bootstraps the row into Bob's local state before the
    // `$updatedBy` handoff on the later update.
    let note_id = alice
        .for_session(Session::new("alice"))
        .create(
            "notes",
            HashMap::from([
                ("title".to_string(), Value::Text("draft".to_string())),
                ("shared".to_string(), Value::Boolean(true)),
            ]),
        )
        .await
        .expect("alice creates shared draft")
        .0;

    let initial_rows = wait_for_rows(
        &alice,
        query.clone(),
        "alice sees initial provenance",
        |rows| (rows.len() == 1 && rows[0].0 == note_id).then_some(rows),
    )
    .await;
    assert_eq!(initial_rows[0].1[0], Value::Text("draft".into()));
    assert_eq!(initial_rows[0].1[1], Value::Boolean(true));
    assert_eq!(initial_rows[0].1[2], Value::Text("alice".into()));
    assert_eq!(initial_rows[0].1[3], Value::Text("alice".into()));
    let Value::Timestamp(initial_created_at) = initial_rows[0].1[4] else {
        panic!("$createdAt should decode as timestamp")
    };
    let Value::Timestamp(initial_updated_at) = initial_rows[0].1[5] else {
        panic!("$updatedAt should decode as timestamp")
    };

    let bob_rows = wait_for_rows(
        &bob,
        query.clone(),
        "bob sees the shared draft before takeover",
        |rows| (rows.len() == 1 && rows[0].0 == note_id).then_some(rows),
    )
    .await;
    assert_eq!(bob_rows[0].1[0], Value::Text("draft".into()));
    assert_eq!(bob_rows[0].1[1], Value::Boolean(true));
    assert_eq!(bob_rows[0].1[2], Value::Text("alice".into()));
    assert_eq!(bob_rows[0].1[3], Value::Text("alice".into()));

    bob.for_session(Session::new("bob"))
        .update(
            note_id,
            vec![
                ("title".to_string(), Value::Text("revised by bob".into())),
                ("shared".to_string(), Value::Boolean(false)),
            ],
        )
        .await
        .expect("bob becomes latest updater");

    let alice_rows = wait_for_rows(
        &alice,
        query.clone(),
        "alice no longer sees bob-updated row",
        |rows| rows.is_empty().then_some(rows),
    )
    .await;
    assert!(alice_rows.is_empty());

    let bob_rows = wait_for_rows(
        &bob,
        query.clone(),
        "bob sees row after becoming latest updater",
        |rows| (rows.len() == 1 && rows[0].0 == note_id).then_some(rows),
    )
    .await;
    assert_eq!(bob_rows[0].1[0], Value::Text("revised by bob".into()));
    assert_eq!(bob_rows[0].1[1], Value::Boolean(false));
    assert_eq!(bob_rows[0].1[2], Value::Text("alice".into()));
    assert_eq!(bob_rows[0].1[3], Value::Text("bob".into()));
    let Value::Timestamp(updated_created_at) = bob_rows[0].1[4] else {
        panic!("updated $createdAt should decode as timestamp")
    };
    let Value::Timestamp(updated_updated_at) = bob_rows[0].1[5] else {
        panic!("updated $updatedAt should decode as timestamp")
    };
    assert_eq!(updated_created_at, initial_created_at);
    assert!(updated_updated_at >= initial_updated_at);

    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    server.shutdown().await;
}

/// Verifies that provenance magic columns expose user-authored principals and
/// insert timestamps in ordinary end-to-end queries.
///
/// Actors: `alice` and `bob`, each writing one note through their own session
/// client and reading through an unrestricted query.
///
/// ```text
/// alice client ──create────► server ──► unrestricted query
/// bob client ───create────► server ──► unrestricted query
/// ```
#[tokio::test]
async fn provenance_magic_columns_expose_user_principals_and_insert_timestamps() {
    let schema = SchemaBuilder::new()
        .table(make_notes_schema(
            "notes",
            TablePolicies::new()
                .with_select(PolicyExpr::True)
                .with_insert(PolicyExpr::True)
                .with_update(Some(PolicyExpr::True), PolicyExpr::True)
                .with_delete(PolicyExpr::True),
        ))
        .build();
    let (server, alice, bob) = start_alice_and_bob_server(schema.clone()).await;

    let alice_note = create_note_as(&alice, "alice", "alice note").await;
    let bob_note = create_note_as(&bob, "bob", "bob note").await;

    let query = QueryBuilder::new("notes")
        .select(&[
            "title",
            "$createdBy",
            "$updatedBy",
            "$createdAt",
            "$updatedAt",
        ])
        .order_by("title")
        .build();

    let rows = wait_for_rows(
        &alice,
        query,
        "alice sees provenance columns for both user rows",
        |rows| (rows.len() == 2).then_some(rows),
    )
    .await;
    let alice_row = rows
        .iter()
        .find(|(id, _)| *id == alice_note)
        .expect("alice-authored row should be present");
    assert_eq!(alice_row.1[0], Value::Text("alice note".into()));
    assert_eq!(alice_row.1[1], Value::Text("alice".into()));
    assert_eq!(alice_row.1[2], Value::Text("alice".into()));
    let Value::Timestamp(alice_created_at) = alice_row.1[3] else {
        panic!("alice $createdAt should decode as timestamp")
    };
    let Value::Timestamp(alice_updated_at) = alice_row.1[4] else {
        panic!("alice $updatedAt should decode as timestamp")
    };
    assert_eq!(alice_created_at, alice_updated_at);

    let bob_row = rows
        .iter()
        .find(|(id, _)| *id == bob_note)
        .expect("bob-authored row should be present");
    assert_eq!(bob_row.1[0], Value::Text("bob note".into()));
    assert_eq!(bob_row.1[1], Value::Text("bob".into()));
    assert_eq!(bob_row.1[2], Value::Text("bob".into()));
    let Value::Timestamp(bob_created_at) = bob_row.1[3] else {
        panic!("bob $createdAt should decode as timestamp")
    };
    let Value::Timestamp(bob_updated_at) = bob_row.1[4] else {
        panic!("bob $updatedAt should decode as timestamp")
    };
    assert_eq!(bob_created_at, bob_updated_at);

    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    server.shutdown().await;
}
