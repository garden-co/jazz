#[cfg(feature = "test-utils")]
use crate::JazzClient;

use super::*;

#[cfg(feature = "test-utils")]
async fn enforcing_test_client(schema: Schema) -> JazzClient {
    JazzClient::connect_with_row_policy_mode(
        crate::AppContext::test(schema),
        crate::query_manager::types::RowPolicyMode::Enforcing,
    )
    .await
    .expect("connect enforcing local JazzClient")
}

/// Verifies the happy path for a simple INSERT policy where the inserted row's
/// owner matches the session user.
#[cfg(feature = "test-utils")]
#[tokio::test]
async fn rebac_insert_allowed_by_simple_policy() {
    let client = JazzClient::test_client(rebac_test_schema()).await;

    client
        .for_session(Session::new("alice"))
        .insert(
            "documents",
            crate::row_input!(
                "owner_id" => "alice",
                "title" => "My Doc",
                "folder_id" => Value::Null,
            ),
        )
        .expect("insert should be allowed when owner_id matches the session user");
}

/// Verifies local INSERT denial when a simple owner policy does not match the
/// session user.
#[cfg(feature = "test-utils")]
#[tokio::test]
async fn rebac_insert_denied_by_simple_policy() {
    let client = JazzClient::test_client(rebac_test_schema()).await;

    let err = client
        .for_session(Session::new("alice"))
        .insert(
            "documents",
            crate::row_input!(
                "owner_id" => "bob",
                "title" => "Stolen Doc",
                "folder_id" => Value::Null,
            ),
        )
        .expect_err("insert should be denied when owner_id does not match the session user");
    assert_client_policy_denied(err, "documents", Operation::Insert);
}

/// Verifies that permissive local runtimes allow direct writes to tables with
/// no loaded permission bundle or explicit row policies.
#[cfg(feature = "test-utils")]
#[tokio::test]
async fn permissive_local_runtime_without_loaded_policies_allows_sync_pending_write_without_policy()
{
    let notes_table = TableSchema::builder("notes").column("content", ColumnType::Text);
    let schema = SchemaBuilder::new().table(notes_table).build();
    let client = JazzClient::test_client(schema).await;

    let (note_id, _, _) = client
        .insert("notes", crate::row_input!("content" => "A note"))
        .expect("table without explicit policies should allow local writes");
    let rows = client
        .query(
            QueryBuilder::new("notes")
                .filter_eq("id", Value::Uuid(note_id))
                .select(&["content"])
                .build(),
            None,
        )
        .await
        .expect("query inserted note");
    assert_eq!(
        rows,
        vec![(note_id, vec![Value::Text("A note".into())])],
        "table without explicit policies should expose the inserted row"
    );
}

/// Verifies that an enforcing local runtime with an empty loaded permissions
/// bundle denies writes that lack an explicit INSERT policy.
#[cfg(feature = "test-utils")]
#[tokio::test]
async fn loaded_empty_permissions_bundle_denies_sync_pending_write_without_explicit_policy() {
    let notes_table = TableSchema::builder("notes").column("content", ColumnType::Text);
    let schema = SchemaBuilder::new().table(notes_table).build();
    let client = enforcing_test_client(schema).await;

    let err = client
        .for_session(Session::new("alice"))
        .insert("notes", crate::row_input!("content" => "A note"))
        .expect_err("enforcing client should deny writes without an explicit insert policy");
    assert_client_policy_denied(err, "notes", Operation::Insert);
}

/// Verifies that one local client can evaluate the same schema under different
/// sessions, showing each user only their own inserted rows.
#[cfg(feature = "test-utils")]
#[tokio::test]
async fn rebac_two_clients_different_sessions() {
    let client = JazzClient::test_client(rebac_test_schema()).await;

    let (alice_doc, _, _) = client
        .for_session(Session::new("alice"))
        .insert(
            "documents",
            crate::row_input!(
                "owner_id" => "alice",
                "title" => "Alice's Doc",
                "folder_id" => Value::Null,
            ),
        )
        .expect("alice should be able to insert alice-owned document");
    let (bob_doc, _, _) = client
        .for_session(Session::new("bob"))
        .insert(
            "documents",
            crate::row_input!(
                "owner_id" => "bob",
                "title" => "Bob's Doc",
                "folder_id" => Value::Null,
            ),
        )
        .expect("bob should be able to insert bob-owned document");

    let alice_visible_docs: HashSet<_> = client
        .for_session(Session::new("alice"))
        .query(
            QueryBuilder::new("documents").select(&["title"]).build(),
            None,
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

    let bob_visible_docs: HashSet<_> = client
        .for_session(Session::new("bob"))
        .query(
            QueryBuilder::new("documents").select(&["title"]).build(),
            None,
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
}

/// Verifies that INSERT policies using a NULL literal distinguish explicit NULL
/// values from non-null values.
#[cfg(feature = "test-utils")]
#[tokio::test]
async fn local_insert_policy_with_null_literal_allows_null_rows_and_denies_non_null_rows() {
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

    let client = JazzClient::test_client(schema).await;

    client
        .for_session(Session::new("alice"))
        .insert(
            "tasks",
            crate::row_input!("title" => "draft", "deleted_at" => Value::Null),
        )
        .expect("null row should satisfy deleted_at = NULL policy");

    let archived_err = client
        .for_session(Session::new("alice"))
        .insert(
            "tasks",
            crate::row_input!("title" => "archived", "deleted_at" => "2026-03-30T12:00:00Z"),
        )
        .expect_err("non-null row should fail deleted_at = NULL policy");
    assert_client_policy_denied(archived_err, "tasks", Operation::Insert);
}
