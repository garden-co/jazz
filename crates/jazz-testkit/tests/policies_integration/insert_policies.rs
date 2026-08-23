use jazz::tools::DurabilityTier;
use jazz_server::JazzServer;
use jazz_testkit::{connect_ready_user, wait_for_edge_txs};

use super::*;

const READY_TIMEOUT: Duration = Duration::from_secs(30);

/// Verifies the happy path for a simple INSERT policy where the inserted row's
/// owner matches the session user.
#[tokio::test]
async fn rebac_insert_allowed_by_simple_policy() {
    tokio::task::LocalSet::new()
        .run_until(rebac_insert_allowed_by_simple_policy_inner())
        .await;
}

async fn rebac_insert_allowed_by_simple_policy_inner() {
    let schema = rebac_test_schema();
    let server = JazzServer::start_with_schema(schema.clone()).await;
    let alice = connect_ready_user(
        &server,
        &schema,
        super::ALICE_ID,
        "documents",
        READY_TIMEOUT,
    )
    .await;

    let transaction_id = alice
        .insert(
            "documents",
            crate::row_input!(
                "owner_id" => super::ALICE_ID,
                "title" => "My Doc",
                "folder_id" => Value::Null,
            ),
        )
        .expect("insert should be allowed when owner_id matches the session user")
        .2
        .expect("allowed insert should commit immediately");
    wait_for_edge_txs(&alice, &[transaction_id]).await;

    alice.shutdown().await.expect("shutdown alice");
    server.shutdown().await;
}

/// Verifies local INSERT denial when a simple owner policy does not match the
/// session user.
#[tokio::test]
async fn rebac_insert_denied_by_simple_policy() {
    tokio::task::LocalSet::new()
        .run_until(rebac_insert_denied_by_simple_policy_inner())
        .await;
}

async fn rebac_insert_denied_by_simple_policy_inner() {
    let schema = rebac_test_schema();
    let server = JazzServer::start_with_schema(schema.clone()).await;
    let alice = connect_ready_user(
        &server,
        &schema,
        super::ALICE_ID,
        "documents",
        READY_TIMEOUT,
    )
    .await;

    let transaction_id = alice
        .insert(
            "documents",
            crate::row_input!(
                "owner_id" => super::BOB_ID,
                "title" => "Stolen Doc",
                "folder_id" => Value::Null,
            ),
        )
        .expect("the current client accepts the optimistic insert")
        .2
        .expect("denied insert should commit locally");
    let rejected = alice
        .wait_for_transaction(transaction_id, DurabilityTier::EdgeServer)
        .await;
    assert!(
        rejected.is_err(),
        "insert should be denied when owner_id does not match the session user"
    );

    alice.shutdown().await.expect("shutdown alice");
    server.shutdown().await;
}

/// Verifies that permissive local runtimes allow direct writes to tables with
/// no loaded permission bundle or explicit row policies.
#[tokio::test]
async fn permissive_local_runtime_without_loaded_policies_allows_sync_pending_write_without_policy()
{
    tokio::task::LocalSet::new().run_until(permissive_local_runtime_without_loaded_policies_allows_sync_pending_write_without_policy_inner()).await;
}

async fn permissive_local_runtime_without_loaded_policies_allows_sync_pending_write_without_policy_inner()
 {
    let notes_table = TableSchema::builder("notes").column("content", ColumnType::Text);
    let schema = SchemaBuilder::new().table(notes_table).build();
    let server = JazzServer::start_with_schema(schema.clone()).await;
    let client =
        connect_ready_user(&server, &schema, super::ALICE_ID, "notes", READY_TIMEOUT).await;

    let (note_id, _, _) = client
        .insert("notes", crate::row_input!("content" => "A note"))
        .expect("table without explicit policies should allow local writes");
    let rows = client
        .query(
            Query::from("notes")
                .filter(eq(col("id"), lit(*note_id.uuid())))
                .select(["content"]),
            None,
        )
        .await
        .expect("query inserted note");
    assert_eq!(
        rows,
        vec![(note_id, vec![Value::Text("A note".into())])],
        "table without explicit policies should expose the inserted row"
    );

    client.shutdown().await.expect("shutdown client");
    server.shutdown().await;
}

/// Verifies that an enforcing local runtime with an empty loaded permissions
/// bundle denies writes that lack an explicit INSERT policy.
#[tokio::test]
#[ignore = "the server currently allows INSERT when a table has no explicit policy bundle"]
async fn loaded_empty_permissions_bundle_denies_sync_pending_write_without_explicit_policy() {
    tokio::task::LocalSet::new()
        .run_until(
            loaded_empty_permissions_bundle_denies_sync_pending_write_without_explicit_policy_inner(
            ),
        )
        .await;
}

async fn loaded_empty_permissions_bundle_denies_sync_pending_write_without_explicit_policy_inner() {
    let notes_table = TableSchema::builder("notes").column("content", ColumnType::Text);
    let schema = SchemaBuilder::new().table(notes_table).build();
    let server = JazzServer::start_with_schema(schema.clone()).await;
    let client =
        connect_ready_user(&server, &schema, super::ALICE_ID, "notes", READY_TIMEOUT).await;

    let transaction_id = client
        .insert("notes", crate::row_input!("content" => "A note"))
        .expect("the current client accepts the optimistic insert")
        .2
        .expect("denied insert should commit locally");
    let rejected = client
        .wait_for_transaction(transaction_id, DurabilityTier::EdgeServer)
        .await;
    assert!(
        rejected.is_err(),
        "server should deny writes without an explicit insert policy"
    );

    client.shutdown().await.expect("shutdown client");
    server.shutdown().await;
}

/// Verifies that one local client can evaluate the same schema under different
/// sessions, showing each user only their own inserted rows.
#[tokio::test]
async fn rebac_two_clients_different_sessions() {
    tokio::task::LocalSet::new()
        .run_until(rebac_two_clients_different_sessions_inner())
        .await;
}

async fn rebac_two_clients_different_sessions_inner() {
    let schema = rebac_test_schema();
    let server = JazzServer::start_with_schema(schema.clone()).await;
    let alice = connect_ready_user(
        &server,
        &schema,
        super::ALICE_ID,
        "documents",
        READY_TIMEOUT,
    )
    .await;
    let bob = connect_ready_user(&server, &schema, super::BOB_ID, "documents", READY_TIMEOUT).await;

    let (alice_doc, _, alice_tx) = alice
        .insert(
            "documents",
            crate::row_input!(
                "owner_id" => super::ALICE_ID,
                "title" => "Alice's Doc",
                "folder_id" => Value::Null,
            ),
        )
        .expect("alice should be able to insert alice-owned document");
    let (bob_doc, _, bob_tx) = bob
        .insert(
            "documents",
            crate::row_input!(
                "owner_id" => super::BOB_ID,
                "title" => "Bob's Doc",
                "folder_id" => Value::Null,
            ),
        )
        .expect("bob should be able to insert bob-owned document");
    wait_for_edge_txs(
        &alice,
        &[alice_tx.expect("alice insert should commit immediately")],
    )
    .await;
    wait_for_edge_txs(
        &bob,
        &[bob_tx.expect("bob insert should commit immediately")],
    )
    .await;

    let alice_visible_docs: HashSet<_> = alice
        .query(
            Query::from("documents").select(["title"]),
            Some(DurabilityTier::EdgeServer),
        )
        .await
        .expect("query documents as alice")
        .into_iter()
        .map(|(id, _)| id)
        .collect();
    assert!(
        alice_visible_docs.contains(&alice_doc),
        "alice should see alice-owned document"
    );
    assert!(
        !alice_visible_docs.contains(&bob_doc),
        "alice should not see bob-owned document"
    );

    let bob_visible_docs: HashSet<_> = bob
        .query(
            Query::from("documents").select(["title"]),
            Some(DurabilityTier::EdgeServer),
        )
        .await
        .expect("query documents as bob")
        .into_iter()
        .map(|(id, _)| id)
        .collect();
    assert!(
        bob_visible_docs.contains(&bob_doc),
        "bob should see bob-owned document"
    );
    assert!(
        !bob_visible_docs.contains(&alice_doc),
        "bob should not see alice-owned document"
    );

    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    server.shutdown().await;
}

/// Verifies that INSERT policies using a NULL literal distinguish explicit NULL
/// values from non-null values.
#[tokio::test]
async fn local_insert_policy_with_null_literal_allows_null_rows_and_denies_non_null_rows() {
    tokio::task::LocalSet::new()
        .run_until(
            local_insert_policy_with_null_literal_allows_null_rows_and_denies_non_null_rows_inner(),
        )
        .await;
}

async fn local_insert_policy_with_null_literal_allows_null_rows_and_denies_non_null_rows_inner() {
    let tasks_policies = permissions(|p| {
        p.allow_insert().where_(pe::eq("deleted_at", pe::null()));
    });
    let schema = SchemaBuilder::new()
        .table(
            TableSchema::builder("tasks")
                .column("title", ColumnType::Text)
                .nullable_column("deleted_at", ColumnType::Text)
                .policies(tasks_policies),
        )
        .build();

    let server = JazzServer::start_with_schema(schema.clone()).await;
    let client =
        connect_ready_user(&server, &schema, super::ALICE_ID, "tasks", READY_TIMEOUT).await;

    let allowed_tx = client
        .insert(
            "tasks",
            crate::row_input!("title" => "draft", "deleted_at" => Value::Null),
        )
        .expect("null row should satisfy deleted_at = NULL policy")
        .2
        .expect("allowed insert should commit immediately");
    wait_for_edge_txs(&client, &[allowed_tx]).await;

    let archived_tx = client
        .insert(
            "tasks",
            crate::row_input!("title" => "archived", "deleted_at" => "2026-03-30T12:00:00Z"),
        )
        .expect("the current client accepts the optimistic insert")
        .2
        .expect("denied insert should commit locally");
    let rejected = client
        .wait_for_transaction(archived_tx, DurabilityTier::EdgeServer)
        .await;
    assert!(
        rejected.is_err(),
        "non-null row should fail deleted_at = NULL policy"
    );

    client.shutdown().await.expect("shutdown client");
    server.shutdown().await;
}
