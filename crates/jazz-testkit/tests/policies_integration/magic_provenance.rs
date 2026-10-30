use jazz_server::JazzServer;
use jazz_testkit::{connect_ready_client, connect_ready_user, wait_for_global_txs};

use super::*;

const READY_TIMEOUT: Duration = Duration::from_secs(30);

fn attributed_to(principal: &str) -> WriteContext {
    WriteContext {
        attribution: Some(principal.into()),
        ..Default::default()
    }
}

async fn connect_provenance_author(
    server: &JazzServer,
    schema: &Schema,
    user_id: &str,
) -> (JazzClient, Session) {
    let (context, client) = jazz_testkit::TestingClient::builder()
        .with_server(server)
        .with_schema(schema.clone())
        .with_user_id(user_id)
        .as_user()
        .ready_on("notes", READY_TIMEOUT)
        .connect_with_context()
        .await;
    let mut session = Session::new("urn:jazz:test", user_id);
    session.account_id = Some(context.account_id.expect("enrolled author account"));
    (client, session)
}

fn author_record(account: jazz::account_registry::AccountId, issuer: &str, subject: &str) -> Value {
    Value::Row {
        id: None,
        values: vec![
            Value::Uuid(ObjectId::from_uuid(account.0)),
            Value::Row {
                id: None,
                values: vec![Value::Text(issuer.into()), Value::Text(subject.into())],
            },
        ],
    }
}

/// Verifies provenance magic columns for normal session writes, backend
/// attribution, timestamps, query filters, and system-authored writes.
#[tokio::test]
async fn provenance_magic_columns_capture_insert_update_and_system_authors() {
    tokio::task::LocalSet::new()
        .run_until(provenance_magic_columns_capture_insert_update_and_system_authors_inner())
        .await;
}

async fn provenance_magic_columns_capture_insert_update_and_system_authors_inner() {
    let schema = provenance_notes_schema();
    let server = JazzServer::start_with_schema(schema.clone())
        .await
        .expect("start test server");
    let backend_node = uuid::Uuid::new_v4();
    let client = connect_ready_client(
        &server,
        &schema,
        &backend_node.to_string(),
        "notes",
        READY_TIMEOUT,
    )
    .await;
    let (alice, alice_session) = connect_provenance_author(&server, &schema, super::ALICE_ID).await;
    let (bob, bob_session) = connect_provenance_author(&server, &schema, super::BOB_ID).await;
    let alice_author = author_record(
        alice_session.account_id.unwrap(),
        "urn:jazz:test",
        super::ALICE_ID,
    );
    let bob_account = bob_session.account_id.unwrap();
    let bob_author = author_record(bob_account, "urn:jazz:test", super::BOB_ID);
    let system_author = author_record(
        jazz::account_registry::SYSTEM_ACCOUNT_ID,
        "urn:jazz:system",
        &backend_node.to_string(),
    );

    let (note, _, note_tx) = alice
        .insert("notes", crate::row_input!("title" => "draft"))
        .expect("alice-authored note should insert");
    wait_for_global_txs(
        &alice,
        &[note_tx.expect("alice note should commit immediately")],
    )
    .await;

    let initial = client
        .query(
            Query::from("notes")
                .filter(eq(col("title"), lit("draft")))
                .select([
                    "title",
                    "$createdBy",
                    "$updatedBy",
                    "$createdAt",
                    "$updatedAt",
                ]),
            jazz::tools::ReadTier::Remote,
        )
        .await
        .map(jazz::tools::test_support::ordinary_rows)
        .expect("query initial note");
    assert_eq!(initial.len(), 1, "draft note should be queryable");
    assert_eq!(
        initial[0].1[0],
        Value::Text("draft".into()),
        "projected title should decode"
    );
    assert_eq!(initial[0].1[1], alice_author.clone());
    assert_eq!(initial[0].1[2], alice_author.clone());
    let Value::Timestamp(initial_created_at) = initial[0].1[3] else {
        panic!("$createdAt should decode as a timestamp")
    };
    let Value::Timestamp(initial_updated_at) = initial[0].1[4] else {
        panic!("$updatedAt should decode as a timestamp")
    };
    assert_eq!(
        initial_created_at, initial_updated_at,
        "fresh inserts should initialize created/updated timestamps together"
    );

    let update_tx = client
        .with_write_context(attributed_to(
            bob_session
                .author_subject()
                .expect("Bob author")
                .canonical(),
        ))
        .update(
            "notes",
            note,
            vec![("title".into(), Value::Text("revised".into()))],
        )
        .expect("attributed update should succeed without a session")
        .expect("attributed update should commit immediately");
    wait_for_global_txs(&client, &[update_tx]).await;

    let updated = client
        .query(
            Query::from("notes")
                .filter(eq(col("title"), lit("revised")))
                .select([
                    "title",
                    "$createdBy",
                    "$updatedBy",
                    "$createdAt",
                    "$updatedAt",
                ]),
            jazz::tools::ReadTier::Remote,
        )
        .await
        .map(jazz::tools::test_support::ordinary_rows)
        .expect("query updated note");
    assert_eq!(updated.len(), 1, "updated note should remain queryable");
    assert_eq!(updated[0].1[0], Value::Text("revised".into()));
    assert_eq!(updated[0].1[1], alice_author.clone());
    assert_eq!(updated[0].1[2], bob_author.clone());
    let Value::Timestamp(updated_created_at) = updated[0].1[3] else {
        panic!("updated $createdAt should decode as a timestamp")
    };
    let Value::Timestamp(updated_updated_at) = updated[0].1[4] else {
        panic!("updated $updatedAt should decode as a timestamp")
    };
    assert_eq!(
        updated_created_at, initial_created_at,
        "created_at should be preserved across updates"
    );
    assert!(
        updated_updated_at >= initial_updated_at,
        "updated_at should move forward on update"
    );

    let updated_by_bob = client
        .query(
            Query::from("notes")
                .filter(eq(col("$updatedBy.account"), lit(bob_account.0)))
                .select(["title", "$updatedBy"]),
            jazz::tools::ReadTier::Remote,
        )
        .await
        .map(jazz::tools::test_support::ordinary_rows)
        .expect("query notes updated by bob");
    assert_eq!(updated_by_bob.len(), 1);
    assert_eq!(
        updated_by_bob[0].1,
        vec![Value::Text("revised".into()), bob_author.clone()]
    );

    let system_tx = client
        .insert("notes", crate::row_input!("title" => "system note"))
        .expect("system-authored note should insert without a session")
        .2
        .expect("system note should commit immediately");
    wait_for_global_txs(&client, &[system_tx]).await;
    let system = client
        .query(
            Query::from("notes")
                .filter(eq(col("title"), lit("system note")))
                .select(["title", "$createdBy", "$updatedBy"]),
            jazz::tools::ReadTier::Remote,
        )
        .await
        .map(jazz::tools::test_support::ordinary_rows)
        .expect("query system-authored note");
    assert_eq!(system.len(), 1);
    assert_eq!(
        system[0].1,
        vec![
            Value::Text("system note".into()),
            system_author.clone(),
            system_author.clone(),
        ]
    );

    client.shutdown().await.expect("shutdown admin");
    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    server.shutdown().await;
}

/// Verifies that write contexts can explicitly override `$updatedAt` while
/// preserving the original creator and creation timestamp.
#[tokio::test]
async fn provenance_magic_columns_allow_explicit_updated_at_override() {
    tokio::task::LocalSet::new()
        .run_until(provenance_magic_columns_allow_explicit_updated_at_override_inner())
        .await;
}

async fn provenance_magic_columns_allow_explicit_updated_at_override_inner() {
    let schema = provenance_notes_schema();
    let server = JazzServer::start_with_schema(schema.clone())
        .await
        .expect("start test server");
    let client =
        connect_ready_client(&server, &schema, "provenance-admin", "notes", READY_TIMEOUT).await;
    let (alice, alice_session) = connect_provenance_author(&server, &schema, super::ALICE_ID).await;
    let (bob, bob_session) = connect_provenance_author(&server, &schema, super::BOB_ID).await;
    let alice_author = author_record(
        alice_session.account_id.unwrap(),
        "urn:jazz:test",
        super::ALICE_ID,
    );
    let bob_author = author_record(
        bob_session.account_id.unwrap(),
        "urn:jazz:test",
        super::BOB_ID,
    );

    let (note, _, note_tx) = alice
        .insert("notes", crate::row_input!("title" => "draft"))
        .expect("alice-authored note should insert");
    wait_for_global_txs(
        &alice,
        &[note_tx.expect("alice note should commit immediately")],
    )
    .await;

    let initial = client
        .query(
            Query::from("notes")
                .filter(eq(col("title"), lit("draft")))
                .select(["$createdAt", "$updatedAt"]),
            jazz::tools::ReadTier::Remote,
        )
        .await
        .map(jazz::tools::test_support::ordinary_rows)
        .expect("query initial note timestamps");
    assert_eq!(initial.len(), 1, "draft note should be queryable");
    let Value::Timestamp(initial_created_at) = initial[0].1[0] else {
        panic!("$createdAt should decode as a timestamp")
    };

    let custom_updated_at = initial_created_at + 10_000;
    let bob_backfill = WriteContext {
        updated_at: Some(custom_updated_at),
        ..attributed_to(
            bob_session
                .author_subject()
                .expect("Bob author")
                .canonical(),
        )
    };

    let update_tx = client
        .with_write_context(bob_backfill)
        .update(
            "notes",
            note,
            vec![("title".into(), Value::Text("backfilled".into()))],
        )
        .expect("explicit updated_at override should succeed")
        .expect("backfill update should commit immediately");
    wait_for_global_txs(&client, &[update_tx]).await;

    let updated = client
        .query(
            Query::from("notes")
                .filter(eq(col("title"), lit("backfilled")))
                .select([
                    "title",
                    "$createdBy",
                    "$updatedBy",
                    "$createdAt",
                    "$updatedAt",
                ]),
            jazz::tools::ReadTier::Remote,
        )
        .await
        .map(jazz::tools::test_support::ordinary_rows)
        .expect("query backfilled note");
    assert_eq!(updated.len(), 1, "backfilled note should remain queryable");
    assert_eq!(updated[0].1[0], Value::Text("backfilled".into()));
    assert_eq!(updated[0].1[1], alice_author);
    assert_eq!(updated[0].1[2], bob_author);
    let Value::Timestamp(updated_created_at) = updated[0].1[3] else {
        panic!("updated $createdAt should decode as a timestamp")
    };
    let Value::Timestamp(updated_updated_at) = updated[0].1[4] else {
        panic!("updated $updatedAt should decode as a timestamp")
    };
    assert_eq!(updated_created_at, initial_created_at);
    assert_eq!(updated_updated_at, custom_updated_at);

    client.shutdown().await.expect("shutdown admin");
    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    server.shutdown().await;
}

/// Verifies `$createdBy`-based row policies: creators can read/update/delete
/// their rows, backend-attributed rows behave as creator-owned, and system rows stay hidden.
#[tokio::test]
async fn created_by_permissions_allow_creators_and_hide_system_rows() {
    tokio::task::LocalSet::new()
        .run_until(created_by_permissions_allow_creators_and_hide_system_rows_inner())
        .await;
}

async fn created_by_permissions_allow_creators_and_hide_system_rows_inner() {
    let schema = authorship_permissions_schema();
    let server = JazzServer::start_with_schema(schema.clone())
        .await
        .expect("start test server");
    let client =
        connect_ready_client(&server, &schema, "provenance-admin", "notes", READY_TIMEOUT).await;
    let (alice, alice_session) = connect_provenance_author(&server, &schema, super::ALICE_ID).await;
    let alice_author = author_record(
        alice_session.account_id.unwrap(),
        "urn:jazz:test",
        super::ALICE_ID,
    );
    let bob = connect_ready_user(&server, &schema, super::BOB_ID, "notes", READY_TIMEOUT).await;

    let (alice_owned, _, alice_owned_tx) = alice
        .insert("notes", crate::row_input!("title" => "alice-owned"))
        .expect("creator-based insert policy should allow alice");
    let (alice_attributed, _, attributed_tx) = client
        .with_write_context(attributed_to(
            alice_session
                .author_subject()
                .expect("Alice author")
                .canonical(),
        ))
        .insert("notes", crate::row_input!("title" => "alice-attributed"))
        .expect("backend-attributed note should stamp alice as creator");
    let system_tx = client
        .insert("notes", crate::row_input!("title" => "system-owned"))
        .expect("system note should insert")
        .2
        .expect("system note should commit immediately");
    wait_for_global_txs(
        &alice,
        &[alice_owned_tx.expect("alice note should commit immediately")],
    )
    .await;
    wait_for_global_txs(
        &client,
        &[
            attributed_tx.expect("attributed note should commit immediately"),
            system_tx,
        ],
    )
    .await;

    let alice_visible = alice
        .query(
            Query::from("notes")
                .select(["title", "$createdBy"])
                .order_by("title", jazz::query::OrderDirection::Asc),
            jazz::tools::ReadTier::Remote,
        )
        .await
        .map(jazz::tools::test_support::ordinary_rows)
        .expect("query notes as alice");
    assert_eq!(
        alice_visible
            .iter()
            .map(|(_, values)| values.clone())
            .collect::<Vec<_>>(),
        vec![
            vec![Value::Text("alice-attributed".into()), alice_author.clone(),],
            vec![Value::Text("alice-owned".into()), alice_author.clone()],
        ],
        "alice should only see notes authored as alice"
    );

    let bob_visible = bob
        .query(
            Query::from("notes").select(["title"]),
            jazz::tools::ReadTier::Remote,
        )
        .await
        .map(jazz::tools::test_support::ordinary_rows)
        .expect("query notes as bob");
    assert!(
        bob_visible.is_empty(),
        "bob should not see alice/system notes"
    );

    let alice_update_tx = alice
        .update(
            "notes",
            alice_attributed,
            vec![(
                "title".into(),
                Value::Text("alice-attributed-updated".into()),
            )],
        )
        .expect("creator should be able to update attributed rows")
        .expect("creator update should commit immediately");
    let alice_delete_tx = alice
        .delete("notes", alice_owned)
        .expect("creator should be able to delete her own row")
        .expect("creator delete should commit immediately");
    wait_for_global_txs(&alice, &[alice_update_tx, alice_delete_tx]).await;

    let alice_after_mutations = alice
        .query(
            Query::from("notes")
                .select(["title"])
                .order_by("title", jazz::query::OrderDirection::Asc),
            jazz::tools::ReadTier::Remote,
        )
        .await
        .map(jazz::tools::test_support::ordinary_rows)
        .expect("query notes as alice after mutations");
    assert_eq!(
        alice_after_mutations
            .iter()
            .map(|(_, values)| values[0].clone())
            .collect::<Vec<_>>(),
        vec![Value::Text("alice-attributed-updated".into())],
        "alice should retain access to the surviving creator-owned row"
    );

    client.shutdown().await.expect("shutdown admin");
    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    server.shutdown().await;
}
