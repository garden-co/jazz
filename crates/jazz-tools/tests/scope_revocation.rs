#![cfg(feature = "test")]

mod support;

use std::collections::HashMap;
use std::time::Duration;

use jazz_tools::public_schema::{PolicyExpr, TablePolicies};
use jazz_tools::row_input;
use jazz_tools::server::JazzServer;
use jazz_tools::{
    ColumnDescriptor, ColumnType, DurabilityTier, JazzClient, ObjectId, QueryBuilder,
    RowDescriptor, Session, TableName, TableSchema, Value,
};
use support::{
    publish_permissions, push_catalogue_in_memory, wait_for_edge_query_ready, wait_for_query,
};
use uuid::Uuid;

const READY_TIMEOUT: Duration = Duration::from_secs(45);
const QUERY_TIMEOUT: Duration = Duration::from_secs(45);

fn owned_docs_schema() -> jazz_tools::Schema {
    HashMap::from([(
        TableName::new("docs"),
        TableSchema::with_policies(
            RowDescriptor::new(vec![
                ColumnDescriptor::new("owner_id", ColumnType::Uuid),
                ColumnDescriptor::new("title", ColumnType::Text),
            ]),
            TablePolicies::new()
                .with_insert(PolicyExpr::True)
                .with_select(PolicyExpr::eq_session("owner_id", vec!["user_id".into()]))
                .with_update(Some(PolicyExpr::True), PolicyExpr::True)
                .with_delete(PolicyExpr::True),
        ),
    )])
}

fn user_client_context(
    server: &JazzServer,
    schema: jazz_tools::Schema,
    user_id: &str,
) -> jazz_tools::AppContext {
    let mut context = server.make_client_context_for_user(schema, user_id);
    context.backend_secret = None;
    context.admin_secret = None;
    context
}

/// Revocation is forward-looking sync narrowing, not post-delivery redaction.
///
/// Bob first receives a row whose owner matches his authenticated `user_id`.
/// A trusted writer then transfers ownership away. Bob's next Edge-settled
/// one-shot query must remove the row from the settled result set, but Bob's
/// purely local read may still see the already-delivered copy.
#[tokio::test(flavor = "current_thread")]
async fn scope_revocation_removes_edge_results_without_redacting_local_copy() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let server = JazzServer::start().await;
            let schema = owned_docs_schema();

            push_catalogue_in_memory(
                server.server_state(),
                server.app_id(),
                "dev",
                "main",
                &[schema.clone()],
                &[],
            )
            .await
            .expect("push owned-docs catalogue");

            publish_permissions(
                &server.base_url(),
                server.app_id(),
                server.admin_secret(),
                &schema,
                schema
                    .iter()
                    .map(|(table_name, table_schema)| (*table_name, table_schema.policies.clone()))
                    .collect::<Vec<_>>(),
                None,
            )
            .await;

            let bob_owner_id = ObjectId::from_uuid(Uuid::new_v4());
            let bob_user_id = bob_owner_id.uuid().to_string();
            let alice_owner_id = ObjectId::from_uuid(Uuid::new_v4());
            let writer_user_id = Uuid::new_v4().to_string();

            let writer = JazzClient::connect(
                server.make_client_context_for_user(schema.clone(), &writer_user_id),
            )
            .await
            .expect("connect trusted writer");
            wait_for_edge_query_ready(&writer, "docs", READY_TIMEOUT).await;

            let bob =
                JazzClient::connect(user_client_context(&server, schema.clone(), &bob_user_id))
                    .await
                    .expect("connect bob");
            wait_for_edge_query_ready(&bob, "docs", READY_TIMEOUT).await;

            let (doc_id, _, create_batch) = writer
                .for_session(Session::new(writer_user_id.clone()))
                .insert(
                    "docs",
                    row_input!("owner_id" => bob_owner_id, "title" => "visible-before-revoke"),
                )
                .expect("trusted writer creates bob-visible doc");
            writer
                .wait_for_batch(create_batch, DurabilityTier::EdgeServer)
                .await
                .expect("create reaches edge");

            let query = QueryBuilder::new("docs").build();
            wait_for_query(
                &bob,
                query.clone(),
                Some(DurabilityTier::EdgeServer),
                QUERY_TIMEOUT,
                "bob sees doc before revocation",
                |rows| rows.iter().any(|(id, _)| *id == doc_id).then_some(()),
            )
            .await;

            let revoke_batch = writer
                .for_session(Session::new(writer_user_id))
                .update(doc_id, vec![("owner_id".to_owned(), Value::Uuid(alice_owner_id))])
                .expect("trusted writer transfers ownership away from bob");
            writer
                .wait_for_batch(revoke_batch, DurabilityTier::EdgeServer)
                .await
                .expect("ownership transfer reaches edge");

            let edge_rows_after_revoke = wait_for_query(
                &bob,
                query.clone(),
                Some(DurabilityTier::EdgeServer),
                QUERY_TIMEOUT,
                "bob EdgeServer query excludes doc after revocation",
                |rows| rows.iter().all(|(id, _)| *id != doc_id).then_some(rows),
            )
            .await;
            assert!(
                edge_rows_after_revoke.iter().all(|(id, _)| *id != doc_id),
                "revoked row must not remain in Bob's settled EdgeServer result: {edge_rows_after_revoke:?}"
            );

            let local_rows_after_revoke = bob
                .query(query, None)
                .await
                .expect("bob local query after revocation");
            assert!(
                local_rows_after_revoke.iter().any(|(id, _)| *id == doc_id),
                "local reads are not a redaction boundary; already-delivered row should remain locally readable: {local_rows_after_revoke:?}"
            );

            writer.shutdown().await.expect("shutdown writer");
            bob.shutdown().await.expect("shutdown bob");
            server.shutdown().await;
        })
        .await;
}
