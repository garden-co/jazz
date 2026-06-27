use std::collections::HashMap;
use std::time::Duration;

use super::support::{connect_ready_client, connect_ready_user, wait_for_rows};
use super::{pe, permissions};
use jazz_tools::schema_api::{Session, WriteContext};
use jazz_tools::schema_api::{TablePolicies, TableSchemaBuilder};
use jazz_tools::row_input;
use jazz_tools::server::JazzServer;
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
    row_input!("title" => title)
}

fn provenance_values(title: &str, created_by: &str, updated_by: &str) -> Vec<Value> {
    vec![title.into(), created_by.into(), updated_by.into()]
}

async fn create_note_as(client: &JazzClient, user_id: &str, title: &str) -> ObjectId {
    client
        .for_session(Session::new(user_id))
        .insert("notes", note_input(title))
        .expect("create note with session-authored provenance")
        .0
}

async fn create_note_without_session(client: &JazzClient, title: &str) -> ObjectId {
    client
        .insert("notes", note_input(title))
        .expect("create note without attribution")
        .0
}

async fn create_note_with_backend_attribution(
    server: &JazzServer,
    schema: &Schema,
    attributed_user_id: &str,
    title: &str,
) -> ObjectId {
    let write_context = WriteContext {
        session: None,
        attribution: Some(attributed_user_id.to_string()),
        updated_at: None,
        batch_mode: None,
        batch_id: None,
        target_branch_name: None,
    };
    let backend = connect_ready_client(server, schema, "backend", "notes", READY_TIMEOUT).await;
    let (note_id, _row_values, _batch_id) = backend
        .with_write_context(write_context)
        .insert("notes", note_input(title))
        .expect("create note with backend attribution");
    backend.shutdown().await.expect("shutdown backend");

    note_id
}

async fn start_alice_and_bob_server(schema: Schema) -> (JazzServer, JazzClient, JazzClient) {
    let server = JazzServer::builder()
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
    let created_by_policy = pe::eq("$createdBy", pe::session("user_id"));
    let schema = SchemaBuilder::new()
        .table(make_notes_schema(
            "notes",
            permissions(|p| {
                p.allow_read().where_(created_by_policy.clone());
                p.allow_insert().always();
                p.allow_update()
                    .where_old(created_by_policy.clone())
                    .where_new(created_by_policy.clone());
                p.allow_delete().where_(created_by_policy);
            }),
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
        .update(alice_note, vec![("title".to_string(), "bob edit".into())]);
    assert!(
        denied_update.is_err(),
        "bob should not be able to update alice's row under $createdBy policy"
    );
    let denied_delete = bob.for_session(Session::new("bob")).delete(alice_note);
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

/// Verifies that backend/server writes with no attribution stamp
/// `jazz:system`, so `$createdBy` policies fail closed for ordinary users.
///
/// Actors: a backend client writes one derived row without a session, then
/// `alice` writes her own note through a normal user session.
///
/// ```text
/// backend client ─create(no session)──► server ──$createdBy = jazz:system
/// alice client ──create(as alice)─────► server ──$createdBy = alice
/// alice query ────────────────────────► sees only alice row
/// bob query ──────────────────────────► sees nothing
/// ```
#[tokio::test]
async fn created_by_policies_hide_server_generated_rows_without_attribution() {
    let created_by_policy = pe::eq("$createdBy", pe::session("user_id"));
    let schema = SchemaBuilder::new()
        .table(make_notes_schema(
            "notes",
            permissions(|p| {
                p.allow_read().where_(created_by_policy.clone());
                p.allow_insert().always();
                p.allow_update()
                    .where_old(created_by_policy.clone())
                    .where_new(created_by_policy.clone());
                p.allow_delete().where_(created_by_policy);
            }),
        ))
        .build();
    let (server, alice, bob) = start_alice_and_bob_server(schema.clone()).await;
    let backend = connect_ready_client(&server, &schema, "backend", "notes", READY_TIMEOUT).await;

    let system_note = create_note_without_session(&backend, "server-generated").await;
    let alice_note = create_note_as(&alice, "alice", "alice note").await;
    let query = QueryBuilder::new("notes")
        .select(&["title", "$createdBy"])
        .order_by("title")
        .build();

    let alice_rows = wait_for_rows(
        &alice,
        query.clone(),
        "alice sees only explicitly attributed user-owned rows",
        |rows| (rows.len() == 1 && rows[0].0 == alice_note).then_some(rows),
    )
    .await;
    assert_eq!(
        alice_rows[0].1,
        vec![Value::from("alice note"), "alice".into()]
    );
    assert!(
        alice_rows.iter().all(|(id, _)| *id != system_note),
        "server-generated row should stay hidden from alice under $createdBy policy"
    );

    let bob_rows = wait_for_rows(
        &bob,
        query,
        "bob does not see the server-generated system row by default",
        |rows| rows.is_empty().then_some(rows),
    )
    .await;
    assert!(bob_rows.is_empty());

    assert_ne!(system_note, alice_note);

    backend.shutdown().await.expect("shutdown backend");
    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    server.shutdown().await;
}

/// Verifies that `$createdBy = "jazz:system"` can be used as an explicit
/// allowlist branch when ordinary users should read server-generated rows.
///
/// Actors: a backend client writes one system-authored row without a session,
/// and `alice` writes one user-authored row through her session.
///
/// ```text
/// backend client ─create(no session)──► server ──$createdBy = jazz:system
/// alice client ──create(as alice)─────► server ──$createdBy = alice
/// alice query ────────────────────────► sees system row + alice row
/// bob query ──────────────────────────► sees only system row
/// ```
#[tokio::test]
async fn created_by_policies_can_allow_reads_from_system_author() {
    let created_by_policy = pe::eq("$createdBy", pe::session("user_id"));
    let system_author_policy = pe::eq("$createdBy", "jazz:system");
    let schema = SchemaBuilder::new()
        .table(make_notes_schema(
            "notes",
            permissions(|p| {
                p.allow_read().where_(pe::any_of([
                    created_by_policy.clone(),
                    system_author_policy,
                ]));
                p.allow_insert().always();
                p.allow_update()
                    .where_old(created_by_policy.clone())
                    .where_new(created_by_policy.clone());
                p.allow_delete().where_(created_by_policy);
            }),
        ))
        .build();
    let (server, alice, bob) = start_alice_and_bob_server(schema.clone()).await;
    let backend = connect_ready_client(&server, &schema, "backend", "notes", READY_TIMEOUT).await;

    let system_note = create_note_without_session(&backend, "server-generated").await;
    let alice_note = create_note_as(&alice, "alice", "alice note").await;
    let query = QueryBuilder::new("notes")
        .select(&["title", "$createdBy"])
        .order_by("title")
        .build();

    let alice_rows = wait_for_rows(
        &alice,
        query.clone(),
        "alice sees both her own row and the allowed system-authored row",
        |rows| {
            (rows.len() == 2
                && rows.iter().any(|(id, _)| *id == alice_note)
                && rows.iter().any(|(id, _)| *id == system_note))
            .then_some(rows)
        },
    )
    .await;
    let alice_owned = alice_rows
        .iter()
        .find(|(id, _)| *id == alice_note)
        .expect("alice-owned row should be visible");
    assert_eq!(
        alice_owned.1,
        vec![Value::from("alice note"), "alice".into()]
    );
    let system_owned = alice_rows
        .iter()
        .find(|(id, _)| *id == system_note)
        .expect("system-authored row should be visible");
    assert_eq!(
        system_owned.1,
        vec![Value::from("server-generated"), "jazz:system".into()]
    );

    let bob_rows = wait_for_rows(
        &bob,
        query,
        "bob sees only the allowed system-authored row",
        |rows| (rows.len() == 1 && rows[0].0 == system_note).then_some(rows),
    )
    .await;
    assert_eq!(
        bob_rows[0].1,
        vec![Value::from("server-generated"), "jazz:system".into()]
    );

    backend.shutdown().await.expect("shutdown backend");
    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    server.shutdown().await;
}

/// Verifies that backend writes can keep backend permissions while stamping
/// row authorship as `alice`, so `$createdBy` policies treat the row as hers.
///
/// Actors: a backend runtime creates one row with `alice` attribution and both
/// users query under a creator-only policy.
///
/// ```text
/// backend runtime ─create(attribution=alice)──► server ──$createdBy = alice
/// alice query ────────────────────────────────► sees attributed row
/// bob query ──────────────────────────────────► sees nothing
/// ```
#[tokio::test]
async fn created_by_policies_allow_backend_attribution_to_specific_user() {
    let created_by_policy = pe::eq("$createdBy", pe::session("user_id"));
    let schema = SchemaBuilder::new()
        .table(make_notes_schema(
            "notes",
            permissions(|p| {
                p.allow_read().where_(created_by_policy.clone());
                p.allow_insert().never();
                p.allow_update()
                    .where_old(created_by_policy.clone())
                    .where_new(created_by_policy.clone());
                p.allow_delete().where_(created_by_policy);
            }),
        ))
        .build();
    let (server, alice, bob) = start_alice_and_bob_server(schema.clone()).await;

    let attributed_note =
        create_note_with_backend_attribution(&server, &schema, "alice", "backend for alice").await;
    let query = QueryBuilder::new("notes")
        .select(&["title", "$createdBy", "$updatedBy"])
        .build();

    let alice_rows = wait_for_rows(
        &alice,
        query.clone(),
        "alice sees the backend-attributed row as her own",
        |rows| (rows.len() == 1 && rows[0].0 == attributed_note).then_some(rows),
    )
    .await;
    assert_eq!(
        alice_rows[0].1,
        provenance_values("backend for alice", "alice", "alice")
    );

    let bob_rows = wait_for_rows(
        &bob,
        query,
        "bob cannot see alice-attributed backend row",
        |rows| rows.is_empty().then_some(rows),
    )
    .await;
    assert!(bob_rows.is_empty());

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
    let updated_by_policy = pe::eq("$updatedBy", pe::session("user_id"));
    let shared_policy = pe::eq("shared", true);
    let schema = SchemaBuilder::new()
        .table(
            TableSchema::builder("notes")
                .column("title", ColumnType::Text)
                .column("shared", ColumnType::Boolean)
                .policies(permissions(|p| {
                    p.allow_read()
                        .where_(pe::any_of([shared_policy, updated_by_policy]));
                    p.allow_insert().always();
                    p.allow_update().always();
                })),
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
        .insert("notes", row_input!("title" => "draft", "shared" => true))
        .expect("alice creates shared draft")
        .0;

    let initial_rows = wait_for_rows(
        &alice,
        query.clone(),
        "alice sees initial provenance",
        |rows| (rows.len() == 1 && rows[0].0 == note_id).then_some(rows),
    )
    .await;
    assert_eq!(initial_rows[0].1[0], Value::from("draft"));
    assert_eq!(initial_rows[0].1[1], Value::from(true));
    assert_eq!(initial_rows[0].1[2], Value::from("alice"));
    assert_eq!(initial_rows[0].1[3], Value::from("alice"));
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
    assert_eq!(bob_rows[0].1[0], Value::from("draft"));
    assert_eq!(bob_rows[0].1[1], Value::from(true));
    assert_eq!(bob_rows[0].1[2], Value::from("alice"));
    assert_eq!(bob_rows[0].1[3], Value::from("alice"));

    bob.for_session(Session::new("bob"))
        .update(
            note_id,
            vec![
                ("title".to_string(), "revised by bob".into()),
                ("shared".to_string(), false.into()),
            ],
        )
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
    assert_eq!(bob_rows[0].1[0], Value::from("revised by bob"));
    assert_eq!(bob_rows[0].1[1], Value::from(false));
    assert_eq!(bob_rows[0].1[2], Value::from("alice"));
    assert_eq!(bob_rows[0].1[3], Value::from("bob"));
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
            permissions(|p| {
                p.allow_read().always();
                p.allow_insert().always();
                p.allow_update().always();
                p.allow_delete().always();
            }),
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
    assert_eq!(alice_row.1[0], Value::from("alice note"));
    assert_eq!(alice_row.1[1], Value::from("alice"));
    assert_eq!(alice_row.1[2], Value::from("alice"));
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
    assert_eq!(bob_row.1[0], Value::from("bob note"));
    assert_eq!(bob_row.1[1], Value::from("bob"));
    assert_eq!(bob_row.1[2], Value::from("bob"));
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
