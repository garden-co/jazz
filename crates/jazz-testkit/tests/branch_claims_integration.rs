use jazz_testkit as support;

use std::collections::BTreeMap;
use std::time::Duration;

use jazz::db::ReadOpts;
use jazz::groove::records::Value as CoreValue;
use jazz::ids::{BranchId, RowUuid};
use jazz::protocol::{ReadViewSourceSpec, ReadViewSpec};
use jazz::row_input;
use jazz::tools::public_schema::{PolicyExpr, TablePolicies};
use jazz::tools::{
    ColumnType, DurabilityTier, Schema, SchemaBuilder, TableSchema, Value, policy_expr,
};
use jazz_server::JazzServer;
use serde_json::json;
use support::{
    TestingClient, has_added, has_removed, wait_for_query, wait_for_subscription_update,
};

const READY_TIMEOUT: Duration = Duration::from_secs(30);
const QUERY_TIMEOUT: Duration = Duration::from_secs(25);

fn branch_read_opts(branch: BranchId) -> ReadOpts {
    ReadOpts {
        tier: jazz::tx::DurabilityTier::Global,
        read_view: ReadViewSpec {
            source: ReadViewSourceSpec::Branch { branch: branch.0 },
            ..ReadViewSpec::default()
        },
        ..ReadOpts::default()
    }
}

fn root_read_opts() -> ReadOpts {
    ReadOpts {
        tier: jazz::tx::DurabilityTier::Global,
        ..ReadOpts::default()
    }
}

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
            admin
                .wait_for_transaction(
                    transaction_id.expect("ordinary mutation commits immediately"),
                    DurabilityTier::EdgeServer,
                )
                .await
                .expect("room reaches edge");

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

            let (integer_row_id, _, integer_batch) = admin
                .insert(
                    "integer_claim_rows",
                    row_input!("access_level" => Value::Integer(-7)),
                )
                .expect("admin creates integer claims row");
            admin
                .wait_for_transaction(
                    integer_batch.expect("ordinary mutation commits immediately"),
                    DurabilityTier::EdgeServer,
                )
                .await
                .expect("integer row reaches edge");
            let (bigint_row_id, _, bigint_batch) = admin
                .insert(
                    "bigint_claim_rows",
                    row_input!("access_level" => Value::BigInt(7)),
                )
                .expect("admin creates bigint claims row");
            admin
                .wait_for_transaction(
                    bigint_batch.expect("ordinary mutation commits immediately"),
                    DurabilityTier::EdgeServer,
                )
                .await
                .expect("bigint row reaches edge");

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

            let (in_list_row_id, _, in_list_batch_id) = admin
                .insert("role_in_list_rooms", row_input!("name" => "in-list room"))
                .expect("admin creates in-list room");
            admin
                .wait_for_transaction(
                    in_list_batch_id.expect("ordinary mutation commits immediately"),
                    DurabilityTier::EdgeServer,
                )
                .await
                .expect("in-list room reaches edge");
            let (or_row_id, _, or_batch_id) = admin
                .insert("role_or_rooms", row_input!("name" => "or room"))
                .expect("admin creates or room");
            admin
                .wait_for_transaction(
                    or_batch_id.expect("ordinary mutation commits immediately"),
                    DurabilityTier::EdgeServer,
                )
                .await
                .expect("or room reaches edge");

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
            admin
                .wait_for_transaction(
                    transaction_id.expect("ordinary mutation commits immediately"),
                    DurabilityTier::EdgeServer,
                )
                .await
                .expect("room reaches edge");

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

            let (initial_id, _, initial_batch) = writer
                .insert(
                    "admin_rooms",
                    row_input!("name" => "visible before revocation"),
                )
                .expect("writer inserts initially visible room");
            writer
                .wait_for_transaction(initial_batch.expect("ordinary mutation commits immediately"), DurabilityTier::EdgeServer)
                .await
                .expect("initial room reaches edge");

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
                |updates| has_added(updates, initial_id),
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
            let (future_id, _, future_batch) = writer
                .insert("admin_rooms", row_input!("name" => "visible to original session"))
                .expect("writer inserts a later room");
            writer
                .wait_for_transaction(future_batch.expect("ordinary mutation commits immediately"), DurabilityTier::EdgeServer)
                .await
                .expect("later room reaches edge");
            wait_for_subscription_update(
                &mut stream,
                &mut stream_log,
                QUERY_TIMEOUT,
                "the original authorized session receives later authorized rows",
                |updates| has_added(updates, future_id),
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

            let (alpha_id, _, alpha_batch) = admin
                .insert(
                    "rooms",
                    row_input!("name" => "Alpha Room", "join_code" => "alpha"),
                )
                .expect("admin creates alpha room");
            admin
                .wait_for_transaction(
                    alpha_batch.expect("ordinary mutation commits immediately"),
                    DurabilityTier::EdgeServer,
                )
                .await
                .expect("alpha room reaches edge");
            let (beta_id, _, beta_batch) = admin
                .insert(
                    "rooms",
                    row_input!("name" => "Beta Room", "join_code" => "beta"),
                )
                .expect("admin creates beta room");
            admin
                .wait_for_transaction(
                    beta_batch.expect("ordinary mutation commits immediately"),
                    DurabilityTier::EdgeServer,
                )
                .await
                .expect("beta room reaches edge");

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
async fn explicit_branch_subscription_should_match_claims_select_query() {
    tokio::task::LocalSet::new()
        .run_until(async {
            let schema = branch_claims_gated_schema();
            let server = JazzServer::start_with_schema(schema.clone()).await;
            let branch = BranchId(uuid::Uuid::from_bytes([0x42; 16]));
            let branch_row = RowUuid(uuid::Uuid::from_bytes([0x43; 16]));
            let sibling = BranchId(uuid::Uuid::from_bytes([0x44; 16]));
            let sibling_row = RowUuid(uuid::Uuid::from_bytes([0x45; 16]));
            let empty_branch = BranchId(uuid::Uuid::from_bytes([0x46; 16]));
            let empty_row = RowUuid(uuid::Uuid::from_bytes([0x47; 16]));
            server.create_branch_for_test(empty_branch).await;
            let empty_query = jazz::query::Query::from("rooms");
            let empty_opts = branch_read_opts(empty_branch);
            let partition_probe = TestingClient::builder()
                .with_server(&server)
                .with_schema(schema.clone())
                .with_user_id("aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaaa44")
                .with_claims(json!({"join_code": "wrong"}))
                .ready_on("rooms", READY_TIMEOUT)
                .connect()
                .await;
            let absent_partition_rows = partition_probe
                .query_with_opts(empty_query.clone(), empty_opts.clone())
                .await
                .expect("denied probe queries empty branch");
            assert!(
                absent_partition_rows.is_empty(),
                "a denied query must not observe an absent sparse partition: {absent_partition_rows:?}"
            );
            server
                .seed_branch_row_for_test(
                    branch,
                    "rooms",
                    branch_row,
                    BTreeMap::from([
                        (
                            "name".to_owned(),
                            CoreValue::String("branch room".to_owned()),
                        ),
                        (
                            "join_code".to_owned(),
                            CoreValue::String("branch-secret".to_owned()),
                        ),
                    ]),
                )
                .await;
            server
                .seed_branch_row_for_test(
                    empty_branch,
                    "rooms",
                    empty_row,
                    BTreeMap::from([
                        (
                            "name".to_owned(),
                            CoreValue::String("lazy partition room".to_owned()),
                        ),
                        (
                            "join_code".to_owned(),
                            CoreValue::String("branch-secret".to_owned()),
                        ),
                    ]),
                )
                .await;
            let populated_partition_rows = partition_probe
                .query_with_opts(empty_query.clone(), empty_opts.clone())
                .await
                .expect("denied probe queries lazily populated branch");
            assert!(
                populated_partition_rows.is_empty(),
                "a denied query must not distinguish a lazily created partition: {populated_partition_rows:?}"
            );
            server
                .seed_branch_row_for_test(
                    sibling,
                    "rooms",
                    sibling_row,
                    BTreeMap::from([
                        (
                            "name".to_owned(),
                            CoreValue::String("sibling branch room".to_owned()),
                        ),
                        (
                            "join_code".to_owned(),
                            CoreValue::String("branch-secret".to_owned()),
                        ),
                    ]),
                )
                .await;

            let branch_query = jazz::query::Query::from("rooms");
            let sibling_query = jazz::query::Query::from("rooms");
            let root_query = jazz::query::Query::from("rooms");
            let branch_opts = branch_read_opts(branch);
            let sibling_opts = branch_read_opts(sibling);
            let matching = TestingClient::builder()
                .with_server(&server)
                .with_schema(schema.clone())
                .with_user_id("aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaaa42")
                .with_claims(json!({"join_code": "branch-secret"}))
                .ready_on("rooms", READY_TIMEOUT)
                .connect()
                .await;

            let branch_rows = matching
                .query_with_opts(branch_query.clone(), branch_opts.clone())
                .await
                .expect("matching claim queries branch");
            assert!(
                branch_rows
                    .iter()
                    .any(|(id, _)| *id == jazz::tools::ObjectId::from_uuid(branch_row.0)),
                "branch query must traverse the selected branch view: {branch_rows:?}"
            );
            assert!(
                branch_rows
                    .iter()
                    .all(|(id, _)| *id != jazz::tools::ObjectId::from_uuid(sibling_row.0)),
                "one branch must not read its sibling's overlay: {branch_rows:?}"
            );
            let sibling_rows = matching
                .query_with_opts(sibling_query, sibling_opts)
                .await
                .expect("matching claim queries sibling branch");
            assert!(
                sibling_rows
                    .iter()
                    .any(|(id, _)| *id == jazz::tools::ObjectId::from_uuid(sibling_row.0)),
                "sibling branch query must see its own overlay: {sibling_rows:?}"
            );
            assert!(
                sibling_rows
                    .iter()
                    .all(|(id, _)| *id != jazz::tools::ObjectId::from_uuid(branch_row.0)),
                "sibling query must not reuse the first branch's cached result: {sibling_rows:?}"
            );
            let root_rows = matching
                .query_with_opts(root_query, root_read_opts())
                .await
                .expect("matching claim queries root");
            assert!(
                root_rows
                    .iter()
                    .all(|(id, _)| *id != jazz::tools::ObjectId::from_uuid(branch_row.0)),
                "root view must not contain branch-local data: {root_rows:?}"
            );

            let mut stream = matching
                .subscribe_with_opts(branch_query.clone(), branch_opts.clone())
                .await
                .expect("matching claim subscribes to branch");
            let mut updates = Vec::new();
            wait_for_subscription_update(
                &mut stream,
                &mut updates,
                QUERY_TIMEOUT,
                "branch subscription sees branch-local row",
                |updates| has_added(updates, jazz::tools::ObjectId::from_uuid(branch_row.0)),
            )
            .await;

            let denied = TestingClient::builder()
                .with_server(&server)
                .with_schema(schema)
                .with_user_id("aaaaaaaa-aaaa-4aaa-aaaa-aaaaaaaaaa43")
                .with_claims(json!({"join_code": "wrong"}))
                .ready_on("rooms", READY_TIMEOUT)
                .connect()
                .await;
            let denied_rows = denied
                .query_with_opts(branch_query.clone(), branch_opts.clone())
                .await
                .expect("nonmatching claim queries branch");
            assert!(
                denied_rows
                    .iter()
                    .all(|(id, _)| *id != jazz::tools::ObjectId::from_uuid(branch_row.0)),
                "branch query must retain ordinary select policy: {denied_rows:?}"
            );
            let mut denied_stream = denied
                .subscribe_with_opts(branch_query, branch_opts)
                .await
                .expect("nonmatching claim subscribes to branch");
            let mut denied_log = Vec::new();
            wait_for_subscription_update(
                &mut denied_stream,
                &mut denied_log,
                QUERY_TIMEOUT,
                "nonmatching branch subscription receives its empty snapshot",
                |updates| !updates.is_empty(),
            )
            .await;
            assert!(
                !has_added(&denied_log, jazz::tools::ObjectId::from_uuid(branch_row.0)),
                "branch subscription must retain ordinary select policy: {denied_log:?}"
            );
            let mut lazy_denied_stream = partition_probe
                .subscribe_with_opts(empty_query, empty_opts)
                .await
                .expect("denied probe subscribes after lazy partition creation");
            let mut lazy_denied_log = Vec::new();
            wait_for_subscription_update(
                &mut lazy_denied_stream,
                &mut lazy_denied_log,
                QUERY_TIMEOUT,
                "denied probe receives its empty lazy-partition snapshot",
                |updates| !updates.is_empty(),
            )
            .await;
            assert!(
                !has_added(&lazy_denied_log, jazz::tools::ObjectId::from_uuid(empty_row.0)),
                "a denied subscription must not reveal lazily partitioned branch rows: {lazy_denied_log:?}"
            );

            matching.shutdown().await.expect("shutdown matching client");
            denied.shutdown().await.expect("shutdown denied client");
            partition_probe
                .shutdown()
                .await
                .expect("shutdown partition probe");
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
            let (_, _, integer_batch) = bigint_claim_user
                .insert(
                    "integer_claim_rows",
                    row_input!("access_level" => Value::Integer(-7)),
                )
                .expect("I64 claim creates I32 row");
            bigint_claim_user
                .wait_for_transaction(
                    integer_batch.expect("ordinary mutation commits immediately"),
                    DurabilityTier::EdgeServer,
                )
                .await
                .expect("I64 claim matches I32 write policy");

            let integer_claim_user = TestingClient::builder()
                .with_server(&server)
                .with_schema(schema)
                .with_user_id("bbbbbbbb-bbbb-4bbb-bbbb-bbbbbbbbbbb3")
                // Small positive JWT integers are carried by the core as U32.
                .with_claims(json!({"access_level": 7}))
                .ready_on("bigint_claim_rows", READY_TIMEOUT)
                .connect()
                .await;
            let (_, _, bigint_batch) = integer_claim_user
                .insert(
                    "bigint_claim_rows",
                    row_input!("access_level" => Value::BigInt(7)),
                )
                .expect("U32 claim creates I64 row");
            integer_claim_user
                .wait_for_transaction(
                    bigint_batch.expect("ordinary mutation commits immediately"),
                    DurabilityTier::EdgeServer,
                )
                .await
                .expect("U32 claim matches I64 write policy");

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
