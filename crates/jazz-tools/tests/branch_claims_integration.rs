#![cfg(feature = "test")]

mod support;

use std::time::Duration;

use jazz_tools::public_schema::{PolicyExpr, SchemaHash, TablePolicies};
use jazz_tools::server::JazzServer;
use jazz_tools::{
    ColumnType, DurabilityTier, QueryBuilder, Schema, SchemaBuilder, TableSchema, row_input,
};
use serde_json::json;
use support::{TestingClient, wait_for_query};

const READY_TIMEOUT: Duration = Duration::from_secs(30);
const QUERY_TIMEOUT: Duration = Duration::from_secs(25);

fn branch_claims_gated_schema() -> Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("rooms")
                .column("name", ColumnType::Text)
                .column("join_code", ColumnType::Text)
                .policies(
                    TablePolicies::new()
                        .with_insert(PolicyExpr::True)
                        .with_select(PolicyExpr::eq_session(
                            "join_code",
                            vec!["claims".into(), "join_code".into()],
                        )),
                ),
        )
        .build()
}

#[tokio::test(flavor = "current_thread")]
async fn explicit_branch_query_applies_claims_select_policy() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let schema = branch_claims_gated_schema();
            let branch = format!("client-{}-main", SchemaHash::compute(&schema).short());
            let server = JazzServer::start_with_schema(schema.clone()).await;

            let admin = TestingClient::builder()
                .with_server(&server)
                .with_schema(schema.clone())
                .with_user_id("aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaaaa1")
                .as_admin()
                .ready_on("rooms", READY_TIMEOUT)
                .connect()
                .await;

            let (room_id, _, batch_id) = admin
                .insert(
                    "rooms",
                    row_input!("name" => "Party Room", "join_code" => "secret-123"),
                )
                .expect("admin creates claims-gated room");
            admin
                .wait_for_batch(batch_id, DurabilityTier::EdgeServer)
                .await
                .expect("room reaches edge");

            let explicit_branch_query = QueryBuilder::new("rooms").branch(branch).build();

            let alice = TestingClient::builder()
                .with_server(&server)
                .with_schema(schema.clone())
                .with_user_id("aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaaaa2")
                .with_claims(json!({"join_code": "secret-123"}))
                .ready_on("rooms", READY_TIMEOUT)
                .connect()
                .await;

            wait_for_query(
                &alice,
                explicit_branch_query.clone(),
                Some(DurabilityTier::EdgeServer),
                QUERY_TIMEOUT,
                "matching claim sees explicit-branch row",
                |rows| rows.iter().any(|(id, _)| *id == room_id).then_some(()),
            )
            .await;

            let bob = TestingClient::builder()
                .with_server(&server)
                .with_schema(schema.clone())
                .with_user_id("aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaaaa3")
                .with_claims(json!({"join_code": "wrong-code"}))
                .ready_on("rooms", READY_TIMEOUT)
                .connect()
                .await;

            let bob_rows = bob
                .query(
                    explicit_branch_query.clone(),
                    Some(DurabilityTier::EdgeServer),
                )
                .await
                .expect("bob queries explicit branch");
            assert!(
                bob_rows.iter().all(|(id, _)| *id != room_id),
                "wrong claim should not see explicit-branch row: {bob_rows:?}"
            );

            let carol = TestingClient::builder()
                .with_server(&server)
                .with_schema(schema)
                .with_user_id("aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaaaa4")
                .as_user()
                .ready_on("rooms", READY_TIMEOUT)
                .connect()
                .await;

            let carol_rows = carol
                .query(explicit_branch_query, Some(DurabilityTier::EdgeServer))
                .await
                .expect("carol queries explicit branch");
            assert!(
                carol_rows.iter().all(|(id, _)| *id != room_id),
                "missing claim should not see explicit-branch row: {carol_rows:?}"
            );

            admin.shutdown().await.expect("shutdown admin");
            alice.shutdown().await.expect("shutdown alice");
            bob.shutdown().await.expect("shutdown bob");
            carol.shutdown().await.expect("shutdown carol");
            server.shutdown().await;
        })
        .await;
}
