use jazz_testkit as support;

use std::time::Duration;

use jazz::row_input;
use jazz::tools::public_schema::{PolicyExpr, TablePolicies};
use jazz::tools::{
    ColumnType, DurabilityTier, Schema, SchemaBuilder, TableSchema, Value, policy_expr,
};
use jazz_server::JazzServer;
use serde_json::json;
use support::{
    TestingClient, has_added_id, has_removed, wait_for_query, wait_for_subscription_update,
};

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

fn admin_claims_gated_schema() -> Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("admin_rooms")
                .column("name", ColumnType::Text)
                .policies(
                    TablePolicies::new()
                        .with_insert(PolicyExpr::True)
                        .with_select(policy_expr::session_where("claims.admin", true)),
                ),
        )
        .build()
}

fn numeric_claims_write_gated_schema() -> Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("integer_claim_rows")
                .column("access_level", ColumnType::Integer)
                .policies(TablePolicies::new().with_insert(PolicyExpr::eq_session(
                    "access_level",
                    vec!["claims".into(), "access_level".into()],
                ))),
        )
        .table(
            TableSchema::builder("bigint_claim_rows")
                .column("access_level", ColumnType::BigInt)
                .policies(TablePolicies::new().with_insert(PolicyExpr::eq_session(
                    "access_level",
                    vec!["claims".into(), "access_level".into()],
                ))),
        )
        .build()
}

fn numeric_claims_gated_schema() -> Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("integer_claim_rows")
                .column("access_level", ColumnType::Integer)
                .policies(
                    TablePolicies::new()
                        .with_insert(PolicyExpr::True)
                        .with_select(PolicyExpr::eq_session(
                            "access_level",
                            vec!["claims".into(), "access_level".into()],
                        )),
                ),
        )
        .table(
            TableSchema::builder("bigint_claim_rows")
                .column("access_level", ColumnType::BigInt)
                .policies(
                    TablePolicies::new()
                        .with_insert(PolicyExpr::True)
                        .with_select(PolicyExpr::eq_session(
                            "access_level",
                            vec!["claims".into(), "access_level".into()],
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

            let (room_id, _, transaction_id) = admin
                .insert(
                    "rooms",
                    row_input!("name" => "Party Room", "join_code" => "secret-123"),
                )
                .expect("admin creates claims-gated room");
            support::wait_for_edge_txs(
                &admin,
                &[transaction_id.expect("ordinary mutation commits immediately")],
            )
            .await;

            let query = jazz::query::Query::from("rooms");

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
async fn numeric_claims_match_integer_columns_across_core_widths() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let schema = numeric_claims_gated_schema();
            let server = JazzServer::start_with_schema(schema.clone()).await;

            let admin = TestingClient::builder()
                .with_server(&server)
                .with_schema(schema.clone())
                .with_user_id("bbbbbbbb-bbbb-4bbb-bbbb-bbbbbbbbbbb1")
                .as_admin()
                .ready_on("integer_claim_rows", READY_TIMEOUT)
                .connect()
                .await;

            let (integer_row_id, _, integer_tx) = admin
                .insert(
                    "integer_claim_rows",
                    row_input!("access_level" => Value::Integer(-7)),
                )
                .expect("admin creates integer claims row");
            let (bigint_row_id, _, bigint_tx) = admin
                .insert(
                    "bigint_claim_rows",
                    row_input!("access_level" => Value::BigInt(7)),
                )
                .expect("admin creates bigint claims row");
            support::wait_for_edge_txs(
                &admin,
                &[
                    integer_tx.expect("ordinary mutation commits immediately"),
                    bigint_tx.expect("ordinary mutation commits immediately"),
                ],
            )
            .await;

            let bigint_claim_user = TestingClient::builder()
                .with_server(&server)
                .with_schema(schema.clone())
                .with_user_id("bbbbbbbb-bbbb-4bbb-bbbb-bbbbbbbbbbb2")
                // Negative JWT integers are carried by the core as I64.
                .with_claims(json!({"access_level": -7}))
                .ready_on("integer_claim_rows", READY_TIMEOUT)
                .connect()
                .await;
            wait_for_query(
                &bigint_claim_user,
                jazz::query::Query::from("integer_claim_rows"),
                Some(DurabilityTier::EdgeServer),
                QUERY_TIMEOUT,
                "I64 claim matches I32 column",
                |rows| {
                    rows.iter()
                        .any(|(id, _)| *id == integer_row_id)
                        .then_some(())
                },
            )
            .await;

            let integer_claim_user = TestingClient::builder()
                .with_server(&server)
                .with_schema(schema)
                .with_user_id("bbbbbbbb-bbbb-4bbb-bbbb-bbbbbbbbbbb3")
                // Small positive JWT integers are carried by the core as U32.
                .with_claims(json!({"access_level": 7}))
                .ready_on("bigint_claim_rows", READY_TIMEOUT)
                .connect()
                .await;
            wait_for_query(
                &integer_claim_user,
                jazz::query::Query::from("bigint_claim_rows"),
                Some(DurabilityTier::EdgeServer),
                QUERY_TIMEOUT,
                "U32 claim matches I64 column",
                |rows| {
                    rows.iter()
                        .any(|(id, _)| *id == bigint_row_id)
                        .then_some(())
                },
            )
            .await;

            admin.shutdown().await.expect("shutdown admin");
            bigint_claim_user
                .shutdown()
                .await
                .expect("shutdown bigint claim user");
            integer_claim_user
                .shutdown()
                .await
                .expect("shutdown integer claim user");
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

            let (in_list_row_id, _, in_list_tx_id) = admin
                .insert("role_in_list_rooms", row_input!("name" => "in-list room"))
                .expect("admin creates in-list room");
            let (or_row_id, _, or_tx_id) = admin
                .insert("role_or_rooms", row_input!("name" => "or room"))
                .expect("admin creates or room");
            support::wait_for_edge_txs(
                &admin,
                &[
                    in_list_tx_id.expect("ordinary mutation commits immediately"),
                    or_tx_id.expect("ordinary mutation commits immediately"),
                ],
            )
            .await;

            let in_list_query = jazz::query::Query::from("role_in_list_rooms");
            let or_query = jazz::query::Query::from("role_or_rooms");

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

            let (room_id, _, transaction_id) = admin
                .insert(
                    "rooms",
                    row_input!("name" => "Subscription Room", "join_code" => "secret-123"),
                )
                .expect("admin creates claims-gated room");
            support::wait_for_edge_txs(
                &admin,
                &[transaction_id.expect("ordinary mutation commits immediately")],
            )
            .await;

            let query = jazz::query::Query::from("rooms");

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
                |updates| has_added_id(updates, room_id),
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
                !has_added_id(&bob_log, room_id),
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
                !has_added_id(&carol_log, room_id),
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
/// Keeps alice's authorized subscription and receipt-derived view isolated
/// from mallory's second JWT for the same authenticated author.
///
/// ```text
/// writer ──insert──► server ──authorized view──► alice
///                              └──denied view────► mallory
/// ```
async fn same_identity_sessions_keep_claims_isolated() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let schema = admin_claims_gated_schema();
            let server = JazzServer::start_with_schema(schema.clone()).await;

            let writer = TestingClient::builder()
                .with_server(&server)
                .with_schema(schema.clone())
                .with_user_id("bbbbbbbb-bbbb-4bbb-bbbb-bbbbbbbbbbc1")
                .as_admin()
                .ready_on("admin_rooms", READY_TIMEOUT)
                .connect()
                .await;
            let identity = "bbbbbbbb-bbbb-4bbb-bbbb-bbbbbbbbbbc2";
            let authorized = TestingClient::builder()
                .with_server(&server)
                .with_schema(schema.clone())
                .with_user_id(identity)
                .with_claims(json!({"admin": true}))
                .ready_on("admin_rooms", READY_TIMEOUT)
                .connect()
                .await;
            let query = jazz::query::Query::from("admin_rooms");

            let (initial_id, _, initial_tx) = writer
                .insert(
                    "admin_rooms",
                    row_input!("name" => "visible before revocation"),
                )
                .expect("writer inserts initially visible room");
            support::wait_for_edge_txs(&writer, &[initial_tx.expect("ordinary mutation commits immediately")]).await;

            let mut stream = authorized
                .subscribe(query.clone())
                .await
                .expect("authorized identity subscribes");
            let mut stream_log = Vec::new();
            wait_for_subscription_update(
                &mut stream,
                &mut stream_log,
                QUERY_TIMEOUT,
                "authorized subscription receives its initial room",
                |updates| has_added_id(updates, initial_id),
            )
            .await;

            let non_admin = TestingClient::builder()
                .with_server(&server)
                .with_schema(schema.clone())
                .with_user_id(identity)
                .with_claims(json!({"admin": false}))
                .ready_on("admin_rooms", READY_TIMEOUT)
                .connect()
                .await;
            let non_admin_rows = non_admin
                .query(query.clone(), Some(DurabilityTier::EdgeServer))
                .await
                .expect("non-admin sibling performs one-shot read");
            assert!(
                non_admin_rows.is_empty(),
                "the non-admin sibling must fail closed: {non_admin_rows:?}"
            );

            // A second JWT is a distinct authenticated session, even when it
            // has the same user id. It must not implicitly revoke or widen
            // the existing session's authorization; global revocation requires
            // a distinct authenticated control signal.
            let (future_id, _, future_tx) = writer
                .insert("admin_rooms", row_input!("name" => "visible to original session"))
                .expect("writer inserts a later room");
            support::wait_for_edge_txs(&writer, &[future_tx.expect("ordinary mutation commits immediately")]).await;
            wait_for_subscription_update(
                &mut stream,
                &mut stream_log,
                QUERY_TIMEOUT,
                "the original authorized session receives later authorized rows",
                |updates| has_added_id(updates, future_id),
            )
            .await;
            assert!(
                !has_removed(&stream_log, initial_id),
                "a second same-identity JWT must not retract the original session's rows: {stream_log:#?}"
            );

            writer.shutdown().await.expect("shutdown writer");
            authorized
                .shutdown()
                .await
                .expect("shutdown authorized client");
            non_admin
                .shutdown()
                .await
                .expect("shutdown non-admin client");
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

            let (alpha_id, _, alpha_tx) = admin
                .insert(
                    "rooms",
                    row_input!("name" => "Alpha Room", "join_code" => "alpha"),
                )
                .expect("admin creates alpha room");
            let (beta_id, _, beta_tx) = admin
                .insert(
                    "rooms",
                    row_input!("name" => "Beta Room", "join_code" => "beta"),
                )
                .expect("admin creates beta room");
            support::wait_for_edge_txs(
                &admin,
                &[
                    alpha_tx.expect("ordinary mutation commits immediately"),
                    beta_tx.expect("ordinary mutation commits immediately"),
                ],
            )
            .await;

            let query = jazz::query::Query::from("rooms");

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
                |updates| has_added_id(updates, alpha_id),
            )
            .await;
            assert!(
                !has_added_id(&simple_log, beta_id),
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
                |updates| has_added_id(updates, alpha_id) && has_added_id(updates, beta_id),
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
                !has_added_id(&spy_log, alpha_id) && !has_added_id(&spy_log, beta_id),
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
async fn numeric_claims_authorize_writes_across_core_widths() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let schema = numeric_claims_write_gated_schema();
            let server = JazzServer::start_with_schema(schema.clone()).await;

            let bigint_claim_user = TestingClient::builder()
                .with_server(&server)
                .with_schema(schema.clone())
                .with_user_id("bbbbbbbb-bbbb-4bbb-bbbb-bbbbbbbbbbb2")
                // Negative JWT integers are carried by the core as I64.
                .with_claims(json!({"access_level": -7}))
                .ready_on("integer_claim_rows", READY_TIMEOUT)
                .connect()
                .await;
            let (_, _, integer_tx) = bigint_claim_user
                .insert(
                    "integer_claim_rows",
                    row_input!("access_level" => Value::Integer(-7)),
                )
                .expect("I64 claim creates I32 row");
            support::wait_for_edge_txs(
                &bigint_claim_user,
                &[integer_tx.expect("ordinary mutation commits immediately")],
            )
            .await;

            let integer_claim_user = TestingClient::builder()
                .with_server(&server)
                .with_schema(schema)
                .with_user_id("bbbbbbbb-bbbb-4bbb-bbbb-bbbbbbbbbbb3")
                // Small positive JWT integers are carried by the core as U32.
                .with_claims(json!({"access_level": 7}))
                .ready_on("bigint_claim_rows", READY_TIMEOUT)
                .connect()
                .await;
            let (_, _, bigint_tx) = integer_claim_user
                .insert(
                    "bigint_claim_rows",
                    row_input!("access_level" => Value::BigInt(7)),
                )
                .expect("U32 claim creates I64 row");
            support::wait_for_edge_txs(
                &integer_claim_user,
                &[bigint_tx.expect("ordinary mutation commits immediately")],
            )
            .await;

            bigint_claim_user
                .shutdown()
                .await
                .expect("shutdown bigint claim user");
            integer_claim_user
                .shutdown()
                .await
                .expect("shutdown integer claim user");
            server.shutdown().await;
        })
        .await;
}
