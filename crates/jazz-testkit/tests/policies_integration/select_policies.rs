use std::collections::HashSet;
use std::time::Duration;

use jazz::query::Query;
use jazz::row_input;
use jazz::tools::{
    ColumnType, JazzClient, ObjectId, SchemaBuilder, TableSchema, Value, permissions,
    policy_expr as pe,
};
use jazz_server::JazzServer;
use jazz_testkit::{TestingClient, wait_for_edge_txs};

async fn query_documents_as_alice(client: &JazzClient) -> HashSet<ObjectId> {
    client
        .query(Query::from("documents"), None)
        .await
        .expect("query documents as alice")
        .into_iter()
        .map(|(id, _)| id)
        .collect()
}

/// Verifies that SELECT policies comparing a nullable column to a NULL literal
/// include NULL rows and filter out non-null rows.
#[tokio::test(flavor = "current_thread")]
async fn rebac_select_policy_with_null_literal_filters_query_results() {
    tokio::task::LocalSet::new()
        .run_until(async {
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

            let server = JazzServer::start_with_schema(schema.clone()).await;
            let admin = TestingClient::builder()
                .with_server(&server)
                .with_schema(schema.clone())
                .with_user_id("select-policy-admin")
                .as_admin()
                .ready_on("documents", Duration::from_secs(30))
                .connect()
                .await;
            let (visible_id, _, visible_tx) = admin
                .insert(
                    "documents",
                    row_input!("title" => "draft", "deleted_at" => Value::Null),
                )
                .expect("seed visible document");
            let visible_tx = visible_tx.expect("ordinary mutation commits immediately");
            let (hidden_id, _, hidden_tx) = admin
                .insert(
                    "documents",
                    row_input!(
                        "title" => "soft-deleted",
                        "deleted_at" => "2026-03-30T12:00:00Z",
                    ),
                )
                .expect("seed soft-deleted document");
            let hidden_tx = hidden_tx.expect("ordinary mutation commits immediately");
            wait_for_edge_txs(&admin, &[visible_tx, hidden_tx]).await;

            let alice = TestingClient::builder()
                .with_server(&server)
                .with_schema(schema)
                .with_user_id("alice")
                .ready_on("documents", Duration::from_secs(30))
                .connect()
                .await;
            let visible_ids = query_documents_as_alice(&alice).await;
            assert!(
                visible_ids.contains(&visible_id),
                "rows with deleted_at = NULL should remain visible"
            );
            assert!(
                !visible_ids.contains(&hidden_id),
                "rows with non-null deleted_at should be filtered out"
            );

            alice.shutdown().await.expect("shutdown alice");
            admin.shutdown().await.expect("shutdown admin");
            server.shutdown().await;
        })
        .await;
}

/// Verifies that SELECT policies using IS NULL behave the same way for nullable
/// columns, including NULL rows and filtering non-null rows.
#[tokio::test(flavor = "current_thread")]
async fn rebac_select_policy_with_is_null_filters_query_results() {
    tokio::task::LocalSet::new()
        .run_until(async {
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

            let server = JazzServer::start_with_schema(schema.clone()).await;
            let admin = TestingClient::builder()
                .with_server(&server)
                .with_schema(schema.clone())
                .with_user_id("select-policy-admin")
                .as_admin()
                .ready_on("documents", Duration::from_secs(30))
                .connect()
                .await;
            let (visible_id, _, visible_tx) = admin
                .insert(
                    "documents",
                    row_input!("title" => "draft", "deleted_at" => Value::Null),
                )
                .expect("seed visible document");
            let visible_tx = visible_tx.expect("ordinary mutation commits immediately");
            let (hidden_id, _, hidden_tx) = admin
                .insert(
                    "documents",
                    row_input!(
                        "title" => "soft-deleted",
                        "deleted_at" => "2026-03-30T12:00:00Z",
                    ),
                )
                .expect("seed soft-deleted document");
            let hidden_tx = hidden_tx.expect("ordinary mutation commits immediately");
            wait_for_edge_txs(&admin, &[visible_tx, hidden_tx]).await;

            let alice = TestingClient::builder()
                .with_server(&server)
                .with_schema(schema)
                .with_user_id("alice")
                .ready_on("documents", Duration::from_secs(30))
                .connect()
                .await;
            let visible_ids = query_documents_as_alice(&alice).await;
            assert!(
                visible_ids.contains(&visible_id),
                "rows with deleted_at IS NULL should remain visible"
            );
            assert!(
                !visible_ids.contains(&hidden_id),
                "rows with non-null deleted_at should be filtered out by IS NULL"
            );

            alice.shutdown().await.expect("shutdown alice");
            admin.shutdown().await.expect("shutdown admin");
            server.shutdown().await;
        })
        .await;
}
