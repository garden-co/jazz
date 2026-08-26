use jazz_testkit as support;

use std::time::Duration;

use jazz::row_input;
use jazz::tools::public_schema::{PolicyExpr, TablePolicies};
use jazz::tools::{
    ColumnDescriptor, ColumnType, DurabilityTier, ObjectId, RowDescriptor, Session, TableName,
    TableSchema, Value,
};
use jazz_server::JazzServer;
use support::{
    publish_permissions, push_catalogue_in_memory, wait_for_edge_query_ready, wait_for_query,
};
use uuid::Uuid;

const READY_TIMEOUT: Duration = Duration::from_secs(45);
const QUERY_TIMEOUT: Duration = Duration::from_secs(45);

fn owned_docs_schema() -> jazz::tools::Schema {
    jazz::tools::Schema::from([(
        TableName::new("docs"),
        TableSchema::with_policies(
            RowDescriptor::new(vec![
                ColumnDescriptor::new("owner_id", ColumnType::Uuid),
                ColumnDescriptor::new("transfer_writer_id", ColumnType::Uuid),
                ColumnDescriptor::new("title", ColumnType::Text),
            ]),
            TablePolicies::new()
                .with_insert(PolicyExpr::True)
                .with_select(PolicyExpr::or(vec![
                    PolicyExpr::eq_session("owner_id", vec!["user".to_owned()]),
                    PolicyExpr::eq_session("transfer_writer_id", vec!["user".to_owned()]),
                ]))
                .with_update(Some(PolicyExpr::True), PolicyExpr::True)
                .with_delete(PolicyExpr::True),
        ),
    )])
}

fn user_client_context(
    server: &JazzServer,
    schema: jazz::tools::Schema,
    user_id: &str,
) -> jazz::tools::AppContext {
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
                std::slice::from_ref(&schema),
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
            let writer_reader_id = ObjectId::from_uuid(Uuid::new_v4());
            let writer_user_id = writer_reader_id.uuid().to_string();

            let writer = jazz_testkit::connect(
                server.make_client_context_for_user(schema.clone(), &writer_user_id),
            )
            .await
            .expect("connect trusted writer");
            wait_for_edge_query_ready(&writer, "docs", READY_TIMEOUT).await;

            let bob =
                jazz_testkit::connect(user_client_context(&server, schema.clone(), &bob_user_id))
                    .await
                    .expect("connect bob");
            wait_for_edge_query_ready(&bob, "docs", READY_TIMEOUT).await;

            let (doc_id, _, create_tx) = writer
                .for_session(Session::new("urn:jazz:test", writer_user_id.clone()))
                .insert(
                    "docs",
                    row_input!(
                        "owner_id" => bob_owner_id,
                        "transfer_writer_id" => writer_reader_id,
                        "title" => "visible-before-revoke",
                    ),
                )
                .expect("trusted writer creates bob-visible doc");
            support::wait_for_edge_txs(&writer, &[create_tx.expect("ordinary mutation commits immediately")]).await;

            let query = jazz::query::Query::from("docs");
            wait_for_query(
                &bob,
                query.clone(),
                Some(DurabilityTier::EdgeServer),
                QUERY_TIMEOUT,
                "bob sees doc before revocation",
                |rows| rows.iter().any(|(id, _)| *id == doc_id).then_some(()),
            )
            .await;

            // An UPSERT needs read access to its target row. This writer has
            // that access only through this row's transfer_writer_id, keeping
            // Bob's owner-scoped revocation behavior intact.
            let revoke_tx = writer
                .for_session(Session::new("urn:jazz:test", writer_user_id))
                .update(doc_id, vec![("owner_id".to_owned(), Value::Uuid(alice_owner_id))])
                .expect("narrowly authorized writer transfers ownership away from bob");
            support::wait_for_edge_txs(&writer, &[revoke_tx.expect("ordinary mutation commits immediately")]).await;

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
