use std::collections::HashMap;
use std::time::Duration;

use jazz::query::Query;

use super::support::{
    connect_ready_user, wait_for_edge_tx_rejection, wait_for_edge_txs, wait_for_rows,
};
use super::{pe, permissions};
use jazz::tools::Session;
use jazz::tools::{
    ColumnType, JazzClient, ObjectId, SchemaBuilder, TablePolicies, TableSchema,
    TableSchemaBuilder, Value,
};
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
    vec![title.into(), created_by.into(), updated_by.into()]
}

async fn create_note_as(client: &JazzClient, user_id: &str, title: &str) -> ObjectId {
    client
        .for_session(Session::new(user_id))
        .insert("notes", note_input(title))
        .expect("create note with session-authored provenance")
        .0
}

/// Verifies that `$createdBy` SELECT policies scope rows to their creators.
///
/// Actors: `alice` and `bob` each create one note and query the edge server.
///
/// ```text
/// alice client ──create──────────────► server ──query──► alice sees alice row
/// bob client ───create───────────────► server ──query──► bob sees bob row
/// ```
#[tokio::test]
async fn created_by_policies_scope_reads_to_creators() {
    tokio::task::LocalSet::new()
        .run_until(created_by_policies_scope_reads_to_creators_inner())
        .await;
}

async fn created_by_policies_scope_reads_to_creators_inner() {
    let created_by_policy = pe::eq("$createdBy", pe::session("user_id"));
    let schema = SchemaBuilder::new()
        .table(make_notes_schema(
            "notes",
            permissions(|p| {
                p.allow_read().where_(created_by_policy);
                p.allow_insert().always();
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
        "alice sees only her creator-owned row",
        |rows| (rows.len() == 1 && rows[0].0 == alice_note).then_some(rows),
    )
    .await;
    assert_eq!(
        alice_rows[0].1,
        provenance_values("alice note", super::ALICE_ID, super::ALICE_ID)
    );

    let bob_rows = wait_for_rows(&bob, query, "bob sees only his creator-owned row", |rows| {
        (rows.len() == 1 && rows[0].0 == bob_note).then_some(rows)
    })
    .await;
    assert_eq!(
        bob_rows[0].1,
        provenance_values("bob note", super::BOB_ID, super::BOB_ID)
    );

    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    server.shutdown().await;
}

/// Verifies that a `$createdBy` UPDATE policy is enforced by the edge server:
/// creators can update their rows, while other users' optimistic updates are
/// rejected and leave authoritative state unchanged.
///
/// All rows are readable so Bob can observe Alice's note and submit the update
/// through the public client API; only UPDATE authorization uses `$createdBy`.
///
/// ```text
/// bob client ────update alice row────────► edge server ──reject──► title unchanged
/// alice client ──update alice row────────► edge server ──accept──► title changes
/// ```
#[tokio::test]
async fn created_by_update_policy_allows_creator_and_rejects_other_users_at_edge() {
    tokio::task::LocalSet::new()
        .run_until(created_by_update_policy_allows_creator_and_rejects_other_users_at_edge_inner())
        .await;
}

async fn created_by_update_policy_allows_creator_and_rejects_other_users_at_edge_inner() {
    let created_by_is_session = pe::eq("$createdBy", pe::session("user_id"));
    let schema = SchemaBuilder::new()
        .table(make_notes_schema(
            "notes",
            permissions(|p| {
                p.allow_read().always();
                p.allow_insert().always();
                p.allow_update()
                    .where_old(created_by_is_session.clone())
                    .where_new(created_by_is_session);
            }),
        ))
        .build();
    let server = JazzServer::start_with_schema(schema.clone()).await;
    let alice = connect_ready_user(&server, &schema, super::ALICE_ID, "notes", READY_TIMEOUT).await;
    let bob = connect_ready_user(&server, &schema, super::BOB_ID, "notes", READY_TIMEOUT).await;

    let (alice_note, _, insert_tx) = alice
        .insert("notes", note_input("alice note"))
        .expect("alice should create her note");
    wait_for_edge_txs(
        &alice,
        &[insert_tx.expect("alice insert should commit immediately")],
    )
    .await;

    let query = Query::from("notes").select(["title", "$createdBy", "$updatedBy"]);
    wait_for_rows(&bob, query.clone(), "bob observes alice's note", |rows| {
        rows.iter()
            .any(|(id, values)| {
                *id == alice_note
                    && *values == provenance_values("alice note", super::ALICE_ID, super::ALICE_ID)
            })
            .then_some(())
    })
    .await;

    let bob_update_tx = bob
        .update(alice_note, vec![("title".to_string(), "bob edit".into())])
        .expect("non-creator update should be accepted optimistically")
        .expect("non-creator update should commit locally");
    wait_for_edge_tx_rejection(&bob, bob_update_tx).await;

    wait_for_rows(
        &alice,
        query.clone(),
        "bob's rejected update leaves alice's authoritative row unchanged",
        |rows| {
            rows.iter()
                .any(|(id, values)| {
                    *id == alice_note
                        && *values
                            == provenance_values("alice note", super::ALICE_ID, super::ALICE_ID)
                })
                .then_some(())
        },
    )
    .await;

    let alice_update_tx = alice
        .update(alice_note, vec![("title".to_string(), "alice edit".into())])
        .expect("creator update should be accepted locally")
        .expect("creator update should commit immediately");
    wait_for_edge_txs(&alice, &[alice_update_tx]).await;

    wait_for_rows(
        &bob,
        query,
        "bob observes alice's accepted update",
        |rows| {
            rows.iter()
                .any(|(id, values)| {
                    *id == alice_note
                        && *values
                            == provenance_values("alice edit", super::ALICE_ID, super::ALICE_ID)
                })
                .then_some(())
        },
    )
    .await;

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
    let (note_id, _, insert_tx) = alice
        .for_session(Session::new(super::ALICE_ID))
        .insert(
            "notes",
            jazz::row_input!("title" => "draft", "shared" => true),
        )
        .expect("alice creates shared draft");
    wait_for_edge_txs(
        &alice,
        &[insert_tx.expect("alice insert should commit immediately")],
    )
    .await;

    let initial_rows = wait_for_rows(
        &alice,
        query.clone(),
        "alice sees initial provenance",
        |rows| (rows.len() == 1 && rows[0].0 == note_id).then_some(rows),
    )
    .await;
    assert_eq!(initial_rows[0].1[0], Value::from("draft"));
    assert_eq!(initial_rows[0].1[1], Value::from(true));
    assert_eq!(initial_rows[0].1[2], Value::from(super::ALICE_ID));
    assert_eq!(initial_rows[0].1[3], Value::from(super::ALICE_ID));
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
    assert_eq!(bob_rows[0].1[2], Value::from(super::ALICE_ID));
    assert_eq!(bob_rows[0].1[3], Value::from(super::ALICE_ID));

    let bob_update_tx = bob
        .for_session(Session::new(super::BOB_ID))
        .update(
            note_id,
            vec![
                ("title".to_string(), "revised by bob".into()),
                ("shared".to_string(), false.into()),
            ],
        )
        .expect("bob becomes latest updater")
        .expect("bob update should commit immediately");
    wait_for_edge_txs(&bob, &[bob_update_tx]).await;

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
    assert_eq!(bob_rows[0].1[2], Value::from(super::ALICE_ID));
    assert_eq!(bob_rows[0].1[3], Value::from(super::BOB_ID));
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
    assert_eq!(alice_row.1[1], Value::from(super::ALICE_ID));
    assert_eq!(alice_row.1[2], Value::from(super::ALICE_ID));
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
    assert_eq!(bob_row.1[1], Value::from(super::BOB_ID));
    assert_eq!(bob_row.1[2], Value::from(super::BOB_ID));
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
