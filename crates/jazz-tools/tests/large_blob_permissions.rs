#![cfg(feature = "test")]

mod support;

use std::collections::HashMap;
use std::time::Duration;

use jazz_tools::AppContext;
use jazz_tools::public_schema::{LargeValueKind, PolicyExpr, TablePolicies};
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

fn test_author_id(subject: &str) -> ObjectId {
    let uuid = Uuid::parse_str(subject)
        .unwrap_or_else(|_| Uuid::new_v5(&Uuid::NAMESPACE_URL, subject.as_bytes()));
    ObjectId::from_uuid(uuid)
}

fn test_user_id(subject: &str) -> String {
    test_author_id(subject).uuid().to_string()
}

fn large_blob_assets_schema() -> jazz_tools::Schema {
    HashMap::from([(
        TableName::new("assets"),
        TableSchema::with_policies(
            RowDescriptor::new(vec![
                ColumnDescriptor::new("owner_id", ColumnType::Uuid),
                ColumnDescriptor::new("name", ColumnType::Text),
                ColumnDescriptor::new("data", ColumnType::Bytea).large_value(LargeValueKind::Blob),
            ]),
            TablePolicies::new()
                .with_insert(PolicyExpr::eq_session("owner_id", vec!["user_id".into()]))
                .with_select(PolicyExpr::eq_session("owner_id", vec!["user_id".into()]))
                .with_update(
                    Some(PolicyExpr::eq_session("owner_id", vec!["user_id".into()])),
                    PolicyExpr::eq_session("owner_id", vec!["user_id".into()]),
                )
                .with_delete(PolicyExpr::eq_session("owner_id", vec!["user_id".into()])),
        ),
    )])
}

fn asset_values(owner_id: ObjectId, name: &str, data: Vec<u8>) -> HashMap<String, Value> {
    row_input!("owner_id" => owner_id, "name" => name, "data" => data)
}

fn user_client_context(
    server: &JazzServer,
    schema: jazz_tools::Schema,
    user_id: &str,
) -> AppContext {
    let mut context = server.make_client_context_for_user(schema, user_id);
    context.backend_secret = None;
    context
}

/// Exercises that file-like large blob values use ordinary row/value
/// permissions on their table.
///
/// Alice can insert and read her blob row, Bob's query is filtered by the same
/// table SELECT policy, and Mallory's spoofed owner insert is rejected by the
/// same table INSERT check.
///
/// ```text
/// alice   --insert blob asset--> server --row policy--> alice sees Bytea
/// bob     --query assets-------> server --row policy--x empty
/// mallory --spoof owner-------> server --row policy--x rejected
/// ```
#[tokio::test(flavor = "current_thread")]
async fn large_blob_values_follow_ordinary_row_permissions() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let server = JazzServer::start().await;
            let schema = large_blob_assets_schema();

            push_catalogue_in_memory(
                server.server_state(),
                server.app_id(),
                "dev",
                "main",
                &[schema.clone()],
                &[],
            )
            .await
            .expect("push large blob catalogue");

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

            let alice_owner_id = test_author_id("alice");
            let alice_user_id = test_user_id("alice");
            let bob_user_id = test_user_id("bob");
            let mallory_user_id = test_user_id("mallory");

            let alice =
                JazzClient::connect(user_client_context(&server, schema.clone(), &alice_user_id))
                    .await
                    .expect("connect alice");
            wait_for_edge_query_ready(&alice, "assets", Duration::from_secs(30)).await;

            let bob =
                JazzClient::connect(user_client_context(&server, schema.clone(), &bob_user_id))
                    .await
                    .expect("connect bob");
            wait_for_edge_query_ready(&bob, "assets", Duration::from_secs(30)).await;

            let mallory = JazzClient::connect(user_client_context(
                &server,
                schema.clone(),
                &mallory_user_id,
            ))
            .await
            .expect("connect mallory");
            wait_for_edge_query_ready(&mallory, "assets", Duration::from_secs(30)).await;

            let blob = b"file-like payload stored as an ordinary row value".repeat(64);
            let (asset_row_id, _, alice_batch_id) = alice
                .for_session(Session::new(alice_user_id))
                .insert(
                    "assets",
                    asset_values(alice_owner_id, "alice.bin", blob.clone()),
                )
                .expect("alice creates blob asset");
            alice
                .wait_for_batch(alice_batch_id, DurabilityTier::EdgeServer)
                .await
                .expect("alice blob asset reaches edge");

            let alice_rows = wait_for_query(
                &alice,
                QueryBuilder::new("assets").build(),
                Some(DurabilityTier::EdgeServer),
                Duration::from_secs(25),
                "alice sees her blob asset through ordinary row permissions",
                |rows| (rows.len() == 1 && rows[0].0 == asset_row_id).then_some(rows),
            )
            .await;
            assert_eq!(
                alice_rows[0].1,
                vec![
                    Value::Uuid(alice_owner_id),
                    Value::Text("alice.bin".to_string()),
                    Value::Bytea(blob),
                ]
            );

            let bob_rows = wait_for_query(
                &bob,
                QueryBuilder::new("assets").build(),
                Some(DurabilityTier::EdgeServer),
                Duration::from_secs(3),
                "bob does not see alice blob asset through ordinary row permissions",
                Some,
            )
            .await;
            assert!(
                bob_rows.is_empty(),
                "Bob should not see Alice's blob asset without row SELECT permission"
            );

            mallory
                .for_session(Session::new(mallory_user_id))
                .insert(
                    "assets",
                    asset_values(alice_owner_id, "spoofed.bin", b"spoofed".to_vec()),
                )
                .expect_err(
                    "mallory spoofed blob asset should be rejected immediately by row INSERT permission",
                );

            alice.shutdown().await.expect("shutdown alice");
            bob.shutdown().await.expect("shutdown bob");
            mallory.shutdown().await.expect("shutdown mallory");
            server.shutdown().await;
        })
        .await;
}
