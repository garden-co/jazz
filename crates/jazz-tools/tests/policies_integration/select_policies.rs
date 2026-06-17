use crate::JazzClient;

use super::*;

async fn query_documents_as_alice(client: &crate::JazzClient) -> HashSet<ObjectId> {
    client
        .for_session(Session::new("alice"))
        .query(QueryBuilder::new("documents").build(), None)
        .await
        .expect("query documents as alice")
        .into_iter()
        .map(|(id, _)| id)
        .collect()
}

/// Verifies that SELECT policies comparing a nullable column to a NULL literal
/// include NULL rows and filter out non-null rows.
#[tokio::test]
async fn rebac_select_policy_with_null_literal_filters_query_results() {
    let documents_policies = permissions(|p| {
        p.allow_read().where_(pe::eq("deleted_at", pe::null()));
    });
    let schema = SchemaBuilder::new()
        .table(
            TableSchema::builder("documents")
                .column("title", ColumnType::Text)
                .nullable_column("deleted_at", ColumnType::Text)
                .policies(documents_policies),
        )
        .build();

    let client = JazzClient::test_client(schema).await;
    let (visible_id, _, _) = client
        .insert(
            "documents",
            crate::row_input!("title" => "draft", "deleted_at" => Value::Null),
        )
        .expect("seed visible document");
    let (hidden_id, _, _) = client
        .insert(
            "documents",
            crate::row_input!(
                "title" => "soft-deleted",
                "deleted_at" => "2026-03-30T12:00:00Z",
            ),
        )
        .expect("seed soft-deleted document");

    let visible_ids = query_documents_as_alice(&client).await;
    assert!(
        visible_ids.contains(&visible_id),
        "rows with deleted_at = NULL should remain visible"
    );
    assert!(
        !visible_ids.contains(&hidden_id),
        "rows with non-null deleted_at should be filtered out"
    );
}

/// Verifies that SELECT policies using IS NULL behave the same way for nullable
/// columns, including NULL rows and filtering non-null rows.
#[tokio::test]
async fn rebac_select_policy_with_is_null_filters_query_results() {
    let documents_policies = permissions(|p| {
        p.allow_read().where_(pe::is_null("deleted_at"));
    });
    let schema = SchemaBuilder::new()
        .table(
            TableSchema::builder("documents")
                .column("title", ColumnType::Text)
                .nullable_column("deleted_at", ColumnType::Text)
                .policies(documents_policies),
        )
        .build();

    let client = JazzClient::test_client(schema).await;
    let (visible_id, _, _) = client
        .insert(
            "documents",
            crate::row_input!("title" => "draft", "deleted_at" => Value::Null),
        )
        .expect("seed visible document");
    let (hidden_id, _, _) = client
        .insert(
            "documents",
            crate::row_input!(
                "title" => "soft-deleted",
                "deleted_at" => "2026-03-30T12:00:00Z",
            ),
        )
        .expect("seed soft-deleted document");

    let visible_ids = query_documents_as_alice(&client).await;
    assert!(
        visible_ids.contains(&visible_id),
        "rows with deleted_at IS NULL should remain visible"
    );
    assert!(
        !visible_ids.contains(&hidden_id),
        "rows with non-null deleted_at should be filtered out by IS NULL"
    );
}
