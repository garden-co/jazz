#![cfg(feature = "test")]

mod support;

use std::time::Duration;

use jazz_tools::public_schema::{PolicyExpr, TablePolicies};
use jazz_tools::server::JazzServer;
use jazz_tools::{
    ColumnType, DurabilityTier, QueryBuilder, Schema, SchemaBuilder, TableSchema, policy_expr,
    row_input,
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

fn role_claims_gated_schema() -> Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("role_in_list_rooms")
                .column("name", ColumnType::Text)
                .policies(
                    TablePolicies::new()
                        .with_insert(PolicyExpr::True)
                        .with_select(PolicyExpr::SessionInList {
                            path: vec!["claims".into(), "role".into()],
                            values: vec!["admin".into(), "member".into()],
                        }),
                ),
        )
        .table(
            TableSchema::builder("role_or_rooms")
                .column("name", ColumnType::Text)
                .policies(
                    TablePolicies::new()
                        .with_insert(PolicyExpr::True)
                        .with_select(PolicyExpr::Or(vec![
                            policy_expr::session_where("claims.role", "admin"),
                            policy_expr::session_where("claims.role", "member"),
                        ])),
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
async fn session_role_in_list_matches_equivalent_or_policy() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let schema = role_claims_gated_schema();
            let server = JazzServer::start_with_schema(schema.clone()).await;

            let admin = TestingClient::builder()
                .with_server(&server)
                .with_schema(schema.clone())
                .with_user_id("dddddddd-dddd-4ddd-dddd-ddddddddddd1")
                .as_admin()
                .ready_on("role_in_list_rooms", READY_TIMEOUT)
                .connect()
                .await;

            let (in_list_row_id, _, in_list_batch_id) = admin
                .insert("role_in_list_rooms", row_input!("name" => "in-list room"))
                .expect("admin creates in-list room");
            admin
                .wait_for_batch(in_list_batch_id, DurabilityTier::EdgeServer)
                .await
                .expect("in-list room reaches edge");
            let (or_row_id, _, or_batch_id) = admin
                .insert("role_or_rooms", row_input!("name" => "or room"))
                .expect("admin creates or room");
            admin
                .wait_for_batch(or_batch_id, DurabilityTier::EdgeServer)
                .await
                .expect("or room reaches edge");

            let in_list_query = QueryBuilder::new("role_in_list_rooms").build();
            let or_query = QueryBuilder::new("role_or_rooms").build();

            for (role, user_id) in [
                ("admin", "dddddddd-dddd-4ddd-dddd-ddddddddddd2"),
                ("member", "dddddddd-dddd-4ddd-dddd-ddddddddddd3"),
            ] {
                let client = TestingClient::builder()
                    .with_server(&server)
                    .with_schema(schema.clone())
                    .with_user_id(user_id)
                    .with_claims(json!({"role": role}))
                    .ready_on("role_in_list_rooms", READY_TIMEOUT)
                    .connect()
                    .await;

                wait_for_query(
                    &client,
                    in_list_query.clone(),
                    Some(DurabilityTier::EdgeServer),
                    QUERY_TIMEOUT,
                    "matching role sees SessionInList row",
                    |rows| {
                        rows.iter()
                            .any(|(id, _)| *id == in_list_row_id)
                            .then_some(())
                    },
                )
                .await;
                wait_for_query(
                    &client,
                    or_query.clone(),
                    Some(DurabilityTier::EdgeServer),
                    QUERY_TIMEOUT,
                    "matching role sees Or-of-equals row",
                    |rows| rows.iter().any(|(id, _)| *id == or_row_id).then_some(()),
                )
                .await;

                client
                    .shutdown()
                    .await
                    .unwrap_or_else(|error| panic!("shutdown {role}: {error}"));
            }

            for (label, user_id, claims) in [
                (
                    "non-matching role",
                    "dddddddd-dddd-4ddd-dddd-ddddddddddd4",
                    json!({"role": "viewer"}),
                ),
                (
                    "missing role",
                    "dddddddd-dddd-4ddd-dddd-ddddddddddd5",
                    json!({}),
                ),
            ] {
                let client = TestingClient::builder()
                    .with_server(&server)
                    .with_schema(schema.clone())
                    .with_user_id(user_id)
                    .with_claims(claims)
                    .ready_on("role_in_list_rooms", READY_TIMEOUT)
                    .connect()
                    .await;

                let in_list_rows = client
                    .query(in_list_query.clone(), Some(DurabilityTier::EdgeServer))
                    .await
                    .unwrap_or_else(|error| panic!("{label} queries SessionInList rooms: {error}"));
                let or_rows = client
                    .query(or_query.clone(), Some(DurabilityTier::EdgeServer))
                    .await
                    .unwrap_or_else(|error| panic!("{label} queries Or-of-equals rooms: {error}"));

                assert!(
                    in_list_rows.iter().all(|(id, _)| *id != in_list_row_id),
                    "{label} should not see SessionInList row: {in_list_rows:?}"
                );
                assert!(
                    or_rows.iter().all(|(id, _)| *id != or_row_id),
                    "{label} should not see Or-of-equals row: {or_rows:?}"
                );

                client
                    .shutdown()
                    .await
                    .unwrap_or_else(|error| panic!("shutdown {label}: {error}"));
            }

            admin.shutdown().await.expect("shutdown admin");
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
async fn same_shape_subscriptions_route_claims_per_identity() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let schema = branch_claims_gated_schema();
            let server = JazzServer::start_with_schema(schema.clone()).await;

            let admin = TestingClient::builder()
                .with_server(&server)
                .with_schema(schema.clone())
                .with_user_id("cccccccc-cccc-4ccc-cccc-ccccccccccc1")
                .as_admin()
                .ready_on("rooms", READY_TIMEOUT)
                .connect()
                .await;

            let (alpha_id, _, alpha_batch) = admin
                .insert(
                    "rooms",
                    row_input!("name" => "Alpha Room", "join_code" => "alpha"),
                )
                .expect("admin creates alpha room");
            admin
                .wait_for_batch(alpha_batch, DurabilityTier::EdgeServer)
                .await
                .expect("alpha room reaches edge");
            let (beta_id, _, beta_batch) = admin
                .insert(
                    "rooms",
                    row_input!("name" => "Beta Room", "join_code" => "beta"),
                )
                .expect("admin creates beta room");
            admin
                .wait_for_batch(beta_batch, DurabilityTier::EdgeServer)
                .await
                .expect("beta room reaches edge");

            let query = QueryBuilder::new("rooms").build();

            let simple = TestingClient::builder()
                .with_server(&server)
                .with_schema(schema.clone())
                .with_user_id("cccccccc-cccc-4ccc-cccc-ccccccccccc2")
                .with_claims(json!({"join_code": "alpha"}))
                .ready_on("rooms", READY_TIMEOUT)
                .connect()
                .await;
            let mut simple_stream = simple
                .subscribe(query.clone())
                .await
                .expect("simple subscribes to rooms");
            let mut simple_log = Vec::new();
            wait_for_subscription_update(
                &mut simple_stream,
                &mut simple_log,
                QUERY_TIMEOUT,
                "simple subscription sees only alpha",
                |updates| has_added(updates, alpha_id),
            )
            .await;
            assert!(
                !has_added(&simple_log, beta_id),
                "simple claim route must not receive beta row: {simple_log:?}"
            );
            let simple_rows = simple
                .query(query.clone(), Some(DurabilityTier::EdgeServer))
                .await
                .expect("simple queries rooms");
            assert_eq!(
                simple_rows.iter().filter(|(id, _)| *id == alpha_id).count(),
                1,
                "simple one-shot should see alpha row: {simple_rows:?}"
            );
            assert!(
                simple_rows.iter().all(|(id, _)| *id != beta_id),
                "simple one-shot must not see beta row: {simple_rows:?}"
            );

            let admin_reader = TestingClient::builder()
                .with_server(&server)
                .with_schema(schema.clone())
                .with_user_id("cccccccc-cccc-4ccc-cccc-ccccccccccc3")
                .as_admin()
                .ready_on("rooms", READY_TIMEOUT)
                .connect()
                .await;
            let mut admin_stream = admin_reader
                .subscribe(query.clone())
                .await
                .expect("admin subscribes to rooms");
            let mut admin_log = Vec::new();
            wait_for_subscription_update(
                &mut admin_stream,
                &mut admin_log,
                QUERY_TIMEOUT,
                "admin subscription sees all rooms",
                |updates| has_added(updates, alpha_id) && has_added(updates, beta_id),
            )
            .await;
            let admin_rows = admin_reader
                .query(query.clone(), Some(DurabilityTier::EdgeServer))
                .await
                .expect("admin queries rooms");
            assert!(
                admin_rows.iter().any(|(id, _)| *id == alpha_id)
                    && admin_rows.iter().any(|(id, _)| *id == beta_id),
                "admin one-shot should see both rows: {admin_rows:?}"
            );

            let spy = TestingClient::builder()
                .with_server(&server)
                .with_schema(schema.clone())
                .with_user_id("cccccccc-cccc-4ccc-cccc-ccccccccccc4")
                .with_claims(json!({"join_code": "spy"}))
                .ready_on("rooms", READY_TIMEOUT)
                .connect()
                .await;
            let mut spy_stream = spy
                .subscribe(query.clone())
                .await
                .expect("spy subscribes to rooms");
            let mut spy_log = Vec::new();
            wait_for_subscription_update(
                &mut spy_stream,
                &mut spy_log,
                QUERY_TIMEOUT,
                "spy subscription receives an initial empty snapshot",
                |updates| !updates.is_empty(),
            )
            .await;
            assert!(
                !has_added(&spy_log, alpha_id) && !has_added(&spy_log, beta_id),
                "spy subscription must not receive rows: {spy_log:?}"
            );
            let spy_rows = spy
                .query(query, Some(DurabilityTier::EdgeServer))
                .await
                .expect("spy queries rooms");
            assert!(
                spy_rows
                    .iter()
                    .all(|(id, _)| *id != alpha_id && *id != beta_id),
                "spy one-shot must see no rooms: {spy_rows:?}"
            );

            admin.shutdown().await.expect("shutdown writer admin");
            simple.shutdown().await.expect("shutdown simple");
            admin_reader
                .shutdown()
                .await
                .expect("shutdown reader admin");
            spy.shutdown().await.expect("shutdown spy");
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
