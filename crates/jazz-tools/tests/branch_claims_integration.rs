#![cfg(feature = "test")]

mod support;

use std::time::Duration;

use jazz_tools::public_schema::{PolicyExpr, TablePolicies};
use jazz_tools::server::JazzServer;
use jazz_tools::{
    ColumnType, DurabilityTier, QueryBuilder, Schema, SchemaBuilder, TableSchema, row_input,
};
use serde_json::json;
use support::{TestingClient, has_added, wait_for_query, wait_for_subscription_update};

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
async fn query_applies_claims_select_policy() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let schema = branch_claims_gated_schema();
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

            let query = QueryBuilder::new("rooms").build();

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
                query.clone(),
                Some(DurabilityTier::EdgeServer),
                QUERY_TIMEOUT,
                "matching claim sees row",
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
                .query(query.clone(), Some(DurabilityTier::EdgeServer))
                .await
                .expect("bob queries rooms");
            assert!(
                bob_rows.iter().all(|(id, _)| *id != room_id),
                "wrong claim should not see row: {bob_rows:?}"
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
                .query(query, Some(DurabilityTier::EdgeServer))
                .await
                .expect("carol queries rooms");
            assert!(
                carol_rows.iter().all(|(id, _)| *id != room_id),
                "missing claim should not see row: {carol_rows:?}"
            );

            admin.shutdown().await.expect("shutdown admin");
            alice.shutdown().await.expect("shutdown alice");
            bob.shutdown().await.expect("shutdown bob");
            carol.shutdown().await.expect("shutdown carol");
            server.shutdown().await;
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
async fn subscription_matches_claims_select_query() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let schema = branch_claims_gated_schema();
            let server = JazzServer::start_with_schema(schema.clone()).await;

            let admin = TestingClient::builder()
                .with_server(&server)
                .with_schema(schema.clone())
                .with_user_id("bbbbbbbb-bbbb-4bbb-bbbb-bbbbbbbbbbb1")
                .as_admin()
                .ready_on("rooms", READY_TIMEOUT)
                .connect()
                .await;

            let (room_id, _, batch_id) = admin
                .insert(
                    "rooms",
                    row_input!("name" => "Subscription Room", "join_code" => "secret-123"),
                )
                .expect("admin creates claims-gated room");
            admin
                .wait_for_batch(batch_id, DurabilityTier::EdgeServer)
                .await
                .expect("room reaches edge");

            let query = QueryBuilder::new("rooms").build();

            let alice = TestingClient::builder()
                .with_server(&server)
                .with_schema(schema.clone())
                .with_user_id("bbbbbbbb-bbbb-4bbb-bbbb-bbbbbbbbbbb2")
                .with_claims(json!({"join_code": "secret-123"}))
                .ready_on("rooms", READY_TIMEOUT)
                .connect()
                .await;
            let mut alice_stream = alice
                .subscribe(query.clone())
                .await
                .expect("alice subscribes to rooms");
            let mut alice_log = Vec::new();

            wait_for_subscription_update(
                &mut alice_stream,
                &mut alice_log,
                QUERY_TIMEOUT,
                "matching claim subscription sees row",
                |updates| has_added(updates, room_id),
            )
            .await;
            wait_for_query(
                &alice,
                query.clone(),
                Some(DurabilityTier::EdgeServer),
                QUERY_TIMEOUT,
                "matching claim one-shot sees row",
                |rows| rows.iter().any(|(id, _)| *id == room_id).then_some(()),
            )
            .await;

            let bob = TestingClient::builder()
                .with_server(&server)
                .with_schema(schema.clone())
                .with_user_id("bbbbbbbb-bbbb-4bbb-bbbb-bbbbbbbbbbb3")
                .with_claims(json!({"join_code": "wrong-code"}))
                .ready_on("rooms", READY_TIMEOUT)
                .connect()
                .await;
            let mut bob_stream = bob
                .subscribe(query.clone())
                .await
                .expect("bob subscribes to rooms");
            let mut bob_log = Vec::new();
            wait_for_subscription_update(
                &mut bob_stream,
                &mut bob_log,
                QUERY_TIMEOUT,
                "wrong claim subscription receives initial snapshot",
                |updates| !updates.is_empty(),
            )
            .await;
            assert!(
                !has_added(&bob_log, room_id),
                "wrong claim subscription should not see row: {bob_log:?}"
            );
            let bob_rows = bob
                .query(query.clone(), Some(DurabilityTier::EdgeServer))
                .await
                .expect("bob queries rooms");
            assert!(
                bob_rows.iter().all(|(id, _)| *id != room_id),
                "wrong claim one-shot should not see row: {bob_rows:?}"
            );

            let carol = TestingClient::builder()
                .with_server(&server)
                .with_schema(schema)
                .with_user_id("bbbbbbbb-bbbb-4bbb-bbbb-bbbbbbbbbbb4")
                .as_user()
                .ready_on("rooms", READY_TIMEOUT)
                .connect()
                .await;
            let mut carol_stream = carol
                .subscribe(query.clone())
                .await
                .expect("carol subscribes to rooms");
            let mut carol_log = Vec::new();
            wait_for_subscription_update(
                &mut carol_stream,
                &mut carol_log,
                QUERY_TIMEOUT,
                "missing claim subscription receives initial snapshot",
                |updates| !updates.is_empty(),
            )
            .await;
            assert!(
                !has_added(&carol_log, room_id),
                "missing claim subscription should not see row: {carol_log:?}"
            );
            let carol_rows = carol
                .query(query, Some(DurabilityTier::EdgeServer))
                .await
                .expect("carol queries rooms");
            assert!(
                carol_rows.iter().all(|(id, _)| *id != room_id),
                "missing claim one-shot should not see row: {carol_rows:?}"
            );

            admin.shutdown().await.expect("shutdown admin");
            alice.shutdown().await.expect("shutdown alice");
            bob.shutdown().await.expect("shutdown bob");
            carol.shutdown().await.expect("shutdown carol");
            server.shutdown().await;
        })
        .await;
}

#[tokio::test(flavor = "current_thread")]
#[ignore = "tracking: public QueryBuilder::branch is not wired into JazzClient read/subscribe opts yet"]
async fn explicit_branch_subscription_should_match_claims_select_query() {
    panic!(
        "tracking: wire QueryBuilder::branch into the core read view for both one-shot reads and subscriptions"
    );
}
