use std::collections::HashMap;
use std::time::Duration;

use jazz::query::Query;

use super::support::wait_for_edge_txs;
use super::support::{connect_ready_client, connect_ready_user, wait_for_rows};
use super::{pe, permissions};
use jazz::tools::{
    ColumnType, JazzClient, ObjectId, SchemaBuilder, TablePolicies, TableSchema,
    TableSchemaBuilder, Value,
};
use jazz::tools::{Session, WriteContext};
use jazz_server::JazzServer;

const READY_TIMEOUT: Duration = Duration::from_secs(30);

fn make_notes_schema(table_name: &str, policies: TablePolicies) -> TableSchemaBuilder {
    TableSchema::builder(table_name)
        .column("title", ColumnType::Text)
        .policies(policies)
}

fn note_input(title: &str) -> HashMap<String, Value> {
    jazz::row_input!("title" => title)
}

fn provenance_values(title: &str, created_by: &str, updated_by: &str) -> Vec<Value> {
    vec![
        title.into(),
        canonical_user_principal(created_by),
        canonical_user_principal(updated_by),
    ]
}

fn canonical_user_principal(user_id: &str) -> Value {
    Value::from(
        Session::new("urn:jazz:test", user_id)
            .author_subject()
            .expect("test session has a canonical author subject")
            .canonical()
            .to_owned(),
    )
}

async fn create_note_as(client: &JazzClient, user_id: &str, title: &str) -> ObjectId {
    client
        .for_session(Session::new("urn:jazz:test", user_id))
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

/// A backend connection normally has system authority. Its explicit
/// `for_session` context must survive `begin_transaction`, so the staged write
/// uses both the provider UUID claim for the policy and the canonical logical
/// author for provenance.
#[tokio::test]
async fn backend_session_transaction_preserves_raw_claims_and_logical_author() {
    tokio::task::LocalSet::new()
        .run_until(backend_session_transaction_preserves_raw_claims_and_logical_author_inner())
        .await;
}

async fn backend_session_transaction_preserves_raw_claims_and_logical_author_inner() {
    let session_policy = pe::all_of([
        pe::eq("owner", pe::session(vec!["claims", "sub"])),
        pe::eq("$createdBy", pe::session("user")),
    ]);
    let schema = SchemaBuilder::new()
        .table(
            TableSchema::builder("notes")
                .column("title", ColumnType::Text)
                .column("owner", ColumnType::Uuid)
                .policies(permissions(|p| {
                    p.allow_read().always();
                    p.allow_insert().where_(session_policy);
                })),
        )
        .build();
    let server = JazzServer::builder()
        .with_schema(schema.clone())
        .start()
        .await;
    let backend = connect_ready_client(&server, &schema, "backend", "notes", READY_TIMEOUT).await;
    let session = Session::new("urn:jazz:test", super::ALICE_ID);
    let transaction = backend
        .for_session(session.clone())
        .begin_transaction()
        .expect("begin backend session transaction");
    let owner = ObjectId::from_uuid(uuid::Uuid::parse_str(super::ALICE_ID).unwrap());
    let (note_id, _, staged) = transaction
        .insert(
            "notes",
            jazz::row_input!("title" => "session transaction", "owner" => Value::Uuid(owner)),
        )
        .expect("raw UUID user_id and logical author policy allow staged insert");
    assert_eq!(staged, None);
    let staged_rows = transaction
        .query(
            Query::from("notes").select(["title", "$createdBy", "$updatedBy"]),
            None,
        )
        .await
        .expect("transaction reads retain the explicit session author");
    assert_eq!(
        staged_rows[0].1,
        provenance_values("session transaction", super::ALICE_ID, super::ALICE_ID),
        "staged provenance must not use the backend SYSTEM author"
    );
    let transaction_id = transaction.commit().expect("commit session transaction");
    wait_for_edge_txs(&backend, &[transaction_id]).await;

    let rows = wait_for_rows(
        &backend,
        Query::from("notes").select(["title", "$createdBy", "$updatedBy"]),
        "backend observes canonical session provenance",
        |rows| (rows.len() == 1 && rows[0].0 == note_id).then_some(rows),
    )
    .await;
    assert_eq!(
        rows[0].1,
        provenance_values("session transaction", super::ALICE_ID, super::ALICE_ID),
        "backend SYSTEM identity must not replace the explicit session author"
    );

    backend.shutdown().await.expect("shutdown backend");
    server.shutdown().await;
}

async fn create_note_with_backend_attribution(
    backend: &JazzClient,
    attributed_user_id: &str,
    title: &str,
) -> ObjectId {
    let write_context = WriteContext {
        attribution: Some(attributed_user_id.to_string()),
        ..Default::default()
    };
    let (note_id, _, transaction_id) = backend
        .with_write_context(write_context)
        .insert("notes", note_input(title))
        .expect("create note with backend attribution");
    wait_for_edge_txs(
        backend,
        &[transaction_id.expect("backend attributed insert should commit immediately")],
    )
    .await;

    note_id
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
    tokio::task::LocalSet::new()
        .run_until(created_by_policies_scope_crud_to_creators_inner())
        .await;
}

async fn created_by_policies_scope_crud_to_creators_inner() {
    let created_by_policy = pe::eq("$createdBy", pe::session("user"));
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
    let server = JazzServer::builder()
        .with_schema(schema.clone())
        .start()
        .await;
    let alice = connect_ready_user(&server, &schema, super::ALICE_ID, "notes", READY_TIMEOUT).await;
    let bob = connect_ready_user(&server, &schema, super::BOB_ID, "notes", READY_TIMEOUT).await;
    let alice_note = create_note_as(&alice, super::ALICE_ID, "alice note").await;
    let bob_note = create_note_as(&bob, super::BOB_ID, "bob note").await;

    let query = Query::from("notes")
        .select(["title", "$createdBy", "$updatedBy"])
        .order_by("title", jazz::query::OrderDirection::Asc);

    let alice_rows = wait_for_rows(
        &alice,
        query.clone(),
        "alice sees only creator-owned row",
        |rows| (rows.len() == 1 && rows[0].0 == alice_note).then_some(rows),
    )
    .await;
    assert_eq!(
        alice_rows[0].1,
        provenance_values("alice note", super::ALICE_ID, super::ALICE_ID)
    );

    let bob_rows = wait_for_rows(
        &bob,
        query.clone(),
        "bob sees only creator-owned row",
        |rows| (rows.len() == 1 && rows[0].0 == bob_note).then_some(rows),
    )
    .await;
    assert_eq!(
        bob_rows[0].1,
        provenance_values("bob note", super::BOB_ID, super::BOB_ID)
    );

    let denied_update = bob
        .for_session(Session::new("urn:jazz:test", super::BOB_ID))
        .update(alice_note, vec![("title".to_string(), "bob edit".into())]);
    assert!(
        denied_update.is_err(),
        "bob should not be able to update alice's row under $createdBy policy"
    );
    let denied_delete = bob
        .for_session(Session::new("urn:jazz:test", super::BOB_ID))
        .delete(alice_note);
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
                && rows[0].1 == provenance_values("alice note", super::ALICE_ID, super::ALICE_ID))
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
                && rows[0].1 == provenance_values("bob note", super::BOB_ID, super::BOB_ID))
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
    tokio::task::LocalSet::new()
        .run_until(created_by_policies_hide_server_generated_rows_without_attribution_inner())
        .await;
}

async fn created_by_policies_hide_server_generated_rows_without_attribution_inner() {
    let created_by_policy = pe::eq("$createdBy", pe::session("user"));
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
    let server = JazzServer::builder()
        .with_schema(schema.clone())
        .start()
        .await;
    let alice = connect_ready_user(&server, &schema, super::ALICE_ID, "notes", READY_TIMEOUT).await;
    let bob = connect_ready_user(&server, &schema, super::BOB_ID, "notes", READY_TIMEOUT).await;
    let backend = connect_ready_client(&server, &schema, "backend", "notes", READY_TIMEOUT).await;

    let system_note = create_note_without_session(&backend, "server-generated").await;
    let alice_note = create_note_as(&alice, super::ALICE_ID, "alice note").await;
    let query = Query::from("notes")
        .select(["title", "$createdBy"])
        .order_by("title", jazz::query::OrderDirection::Asc);

    let alice_rows = wait_for_rows(
        &alice,
        query.clone(),
        "alice sees only explicitly attributed user-owned rows",
        |rows| (rows.len() == 1 && rows[0].0 == alice_note).then_some(rows),
    )
    .await;
    assert_eq!(
        alice_rows[0].1,
        vec![
            Value::from("alice note"),
            canonical_user_principal(super::ALICE_ID),
        ]
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
#[ignore = "#1758: server schema conversion rejects `$createdBy = \"jazz:system\"` with OperandTypeMismatch"]
async fn created_by_policies_can_allow_reads_from_system_author() {
    tokio::task::LocalSet::new()
        .run_until(created_by_policies_can_allow_reads_from_system_author_inner())
        .await;
}

async fn created_by_policies_can_allow_reads_from_system_author_inner() {
    let created_by_policy = pe::eq("$createdBy", pe::session("user"));
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
    let server = JazzServer::builder()
        .with_schema(schema.clone())
        .start()
        .await;
    let alice = connect_ready_user(&server, &schema, super::ALICE_ID, "notes", READY_TIMEOUT).await;
    let bob = connect_ready_user(&server, &schema, super::BOB_ID, "notes", READY_TIMEOUT).await;
    let backend = connect_ready_client(&server, &schema, "backend", "notes", READY_TIMEOUT).await;

    let system_note = create_note_without_session(&backend, "server-generated").await;
    let alice_note = create_note_as(&alice, super::ALICE_ID, "alice note").await;
    let query = Query::from("notes")
        .select(["title", "$createdBy"])
        .order_by("title", jazz::query::OrderDirection::Asc);

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
        vec![Value::from("alice note"), super::ALICE_ID.into()]
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
#[ignore = "#1758: trusted backend attribution is ignored by the Rust client, so an INSERT policy of never is rejected with authorization_denied"]
async fn created_by_policies_allow_backend_attribution_to_specific_user() {
    tokio::task::LocalSet::new()
        .run_until(created_by_policies_allow_backend_attribution_to_specific_user_inner())
        .await;
}

async fn created_by_policies_allow_backend_attribution_to_specific_user_inner() {
    let created_by_policy = pe::eq("$createdBy", pe::session("user"));
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
    let server = JazzServer::builder()
        .with_schema(schema.clone())
        .start()
        .await;
    let alice = connect_ready_user(&server, &schema, super::ALICE_ID, "notes", READY_TIMEOUT).await;
    let bob = connect_ready_user(&server, &schema, super::BOB_ID, "notes", READY_TIMEOUT).await;
    let backend = connect_ready_client(&server, &schema, "backend", "notes", READY_TIMEOUT).await;

    let attributed_note =
        create_note_with_backend_attribution(&backend, super::ALICE_ID, "backend for alice").await;
    let query = Query::from("notes").select(["title", "$createdBy", "$updatedBy"]);

    let alice_rows = wait_for_rows(
        &alice,
        query.clone(),
        "alice sees the backend-attributed row as her own",
        |rows| (rows.len() == 1 && rows[0].0 == attributed_note).then_some(rows),
    )
    .await;
    assert_eq!(
        alice_rows[0].1,
        provenance_values("backend for alice", super::ALICE_ID, super::ALICE_ID)
    );

    let bob_rows = wait_for_rows(
        &bob,
        query,
        "bob cannot see alice-attributed backend row",
        |rows| rows.is_empty().then_some(rows),
    )
    .await;
    assert!(bob_rows.is_empty());

    backend.shutdown().await.expect("shutdown backend");
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
    tokio::task::LocalSet::new()
        .run_until(updated_by_select_policy_moves_visibility_to_last_editor_inner())
        .await;
}

async fn updated_by_select_policy_moves_visibility_to_last_editor_inner() {
    let updated_by_policy = pe::eq("$updatedBy", pe::session("user"));
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
    let server = JazzServer::builder()
        .with_schema(schema.clone())
        .start()
        .await;
    let alice = connect_ready_user(&server, &schema, super::ALICE_ID, "notes", READY_TIMEOUT).await;
    let bob = connect_ready_user(&server, &schema, super::BOB_ID, "notes", READY_TIMEOUT).await;
    let query = Query::from("notes").select([
        "title",
        "shared",
        "$createdBy",
        "$updatedBy",
        "$createdAt",
        "$updatedAt",
    ]);
    // The shared flag bootstraps the row into Bob's local state before the
    // `$updatedBy` handoff on the later update.
    let note_id = alice
        .for_session(Session::new("urn:jazz:test", super::ALICE_ID))
        .insert(
            "notes",
            jazz::row_input!("title" => "draft", "shared" => true),
        )
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
    assert_eq!(
        initial_rows[0].1[2],
        canonical_user_principal(super::ALICE_ID)
    );
    assert_eq!(
        initial_rows[0].1[3],
        canonical_user_principal(super::ALICE_ID)
    );
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
    assert_eq!(bob_rows[0].1[2], canonical_user_principal(super::ALICE_ID));
    assert_eq!(bob_rows[0].1[3], canonical_user_principal(super::ALICE_ID));

    let bob_update = bob
        .for_session(Session::new("urn:jazz:test", super::BOB_ID))
        .update(
            note_id,
            vec![
                ("title".to_string(), "revised by bob".into()),
                ("shared".to_string(), false.into()),
            ],
        )
        .expect("bob becomes latest updater")
        .expect("ordinary Bob update commits immediately");
    wait_for_edge_txs(&bob, &[bob_update]).await;

    let alice_rows = tokio::time::timeout(
        READY_TIMEOUT,
        wait_for_rows(
            &alice,
            query.clone(),
            "alice no longer sees bob-updated row",
            |rows| rows.is_empty().then_some(rows),
        ),
    )
    .await
    .expect("Alice's removal query must not stall after Bob's update reaches edge");
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
    assert_eq!(bob_rows[0].1[2], canonical_user_principal(super::ALICE_ID));
    assert_eq!(bob_rows[0].1[3], canonical_user_principal(super::BOB_ID));
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
async fn provenance_columns_expose_user_principals_and_insert_timestamps() {
    tokio::task::LocalSet::new()
        .run_until(provenance_columns_expose_user_principals_and_insert_timestamps_inner())
        .await;
}

async fn provenance_columns_expose_user_principals_and_insert_timestamps_inner() {
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
    let server = JazzServer::builder()
        .with_schema(schema.clone())
        .start()
        .await;
    let alice = connect_ready_user(&server, &schema, super::ALICE_ID, "notes", READY_TIMEOUT).await;
    let bob = connect_ready_user(&server, &schema, super::BOB_ID, "notes", READY_TIMEOUT).await;

    let alice_note = create_note_as(&alice, super::ALICE_ID, "alice note").await;
    let bob_note = create_note_as(&bob, super::BOB_ID, "bob note").await;

    let query = Query::from("notes")
        .select([
            "title",
            "$createdBy",
            "$updatedBy",
            "$createdAt",
            "$updatedAt",
        ])
        .order_by("title", jazz::query::OrderDirection::Asc);

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
    assert_eq!(alice_row.1[1], canonical_user_principal(super::ALICE_ID));
    assert_eq!(alice_row.1[2], canonical_user_principal(super::ALICE_ID));
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
    assert_eq!(bob_row.1[1], canonical_user_principal(super::BOB_ID));
    assert_eq!(bob_row.1[2], canonical_user_principal(super::BOB_ID));
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
