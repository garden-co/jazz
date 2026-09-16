use std::time::Duration;

use crate::JazzClient;
use jazz::tools::DurabilityTier;
use jazz_server::JazzServer;
use jazz_testkit::{connect_ready_user, wait_for_query};

use super::*;

const WAIT_TIMEOUT: Duration = Duration::from_secs(5);

fn session(subject: &str, account: u128) -> Session {
    let mut session = Session::new("urn:jazz:test", subject);
    session.account_id = Some(jazz::account_registry::AccountId(uuid::Uuid::from_u128(
        account,
    )));
    session
}

async fn wait_for_protected_row(
    client: &JazzClient,
    protected_id: ObjectId,
    expected_data: &str,
    description: &str,
) {
    wait_for_query(
        client,
        Query::from("protected")
            .filter(eq(col("id"), lit(*protected_id.uuid())))
            .select(["data"]),
        jazz::tools::ReadTier::Remote,
        WAIT_TIMEOUT,
        description,
        |rows| (rows == [(protected_id, vec![Value::Text(expected_data.into())])]).then_some(()),
    )
    .await;
}

async fn wait_for_protected_row_absent(
    client: &JazzClient,
    protected_id: ObjectId,
    description: &str,
) {
    wait_for_query(
        client,
        Query::from("protected")
            .filter(eq(col("id"), lit(*protected_id.uuid())))
            .select(["data"]),
        jazz::tools::ReadTier::Remote,
        WAIT_TIMEOUT,
        description,
        |rows| rows.is_empty().then_some(()),
    )
    .await;
}

async fn wait_for_admin_row(client: &JazzClient, admin_id: ObjectId, user_id: &str) {
    wait_for_query(
        client,
        Query::from("admins")
            .filter(eq(col("id"), lit(*admin_id.uuid())))
            .select(["user_id"]),
        jazz::tools::ReadTier::Remote,
        WAIT_TIMEOUT,
        format!("{user_id} admin row becomes visible"),
        |rows| (rows == [(admin_id, vec![Value::Text(user_id.into())])]).then_some(()),
    )
    .await;
}

/// Verifies that a permissive local insert which fails a server-side EXISTS
/// INSERT policy is rejected on sync and does not become visible to peers.
#[tokio::test]
async fn rebac_exists_clause_denies_non_matching_insert() {
    tokio::task::LocalSet::new()
        .run_until(rebac_exists_clause_denies_non_matching_insert_inner())
        .await;
}

async fn rebac_exists_clause_denies_non_matching_insert_inner() {
    let protected_policies = permissions(|p| {
        p.allow_read().always();
        p.allow_insert().where_(pe::exists(
            pe::table("admins").where_(pe::eq("user_id", pe::session(vec!["claims", "sub"]))),
        ));
    });
    let schema = SchemaBuilder::new()
        .table(
            TableSchema::builder("admins")
                .column("user_id", ColumnType::Text)
                .policies(permissions(|p| p.allow_read().always())),
        )
        .table(
            TableSchema::builder("protected")
                .column("data", ColumnType::Text)
                .policies(protected_policies),
        )
        .build();

    let server = JazzServer::start_with_schema(schema.clone())
        .await
        .expect("start test server");
    let bob = connect_ready_user(
        &server,
        &schema,
        super::BOB_ID,
        "protected",
        Duration::from_secs(30),
    )
    .await;
    let alice = connect_ready_user(
        &server,
        &schema,
        super::ALICE_ID,
        "protected",
        Duration::from_secs(30),
    )
    .await;

    let (protected_id, _, transaction_id) = bob
        .insert("protected", crate::row_input!("data" => "secret data"))
        .expect("permissive non-admin insert should succeed locally");
    let error = bob
        .wait_for_transaction(
            transaction_id.expect("permissive insert should commit immediately"),
            DurabilityTier::EdgeServer,
        )
        .await
        .expect_err("the server must reject a non-admin insert under EXISTS");
    assert!(
        error.to_string().ends_with("authorization_denied"),
        "expected an authority policy rejection, got {error}"
    );
    wait_for_protected_row_absent(
        &alice,
        protected_id,
        "alice never sees bob's rejected protected insert",
    )
    .await;

    bob.shutdown().await.expect("shutdown bob");
    alice.shutdown().await.expect("shutdown alice");
    server.shutdown().await;
}

/// Verifies that UPDATE USING policies with EXISTS are enforced on sync, and
/// that a rejected optimistic update rolls back to server-authoritative state.
#[tokio::test]
async fn rebac_update_denied_by_using_exists_policy() {
    tokio::task::LocalSet::new()
        .run_until(rebac_update_denied_by_using_exists_policy_inner())
        .await;
}

async fn rebac_update_denied_by_using_exists_policy_inner() {
    let protected_policies = permissions(|p| {
        p.allow_read().always();
        p.allow_insert().always();
        p.allow_update()
            .where_old(pe::exists(
                pe::table("admins").where_(pe::eq("user_id", pe::session(vec!["claims", "sub"]))),
            ))
            .where_new(pe::always());
    });
    let schema = SchemaBuilder::new()
        .table(
            TableSchema::builder("admins")
                .column("user_id", ColumnType::Text)
                .policies(permissions(|p| {
                    p.allow_read().always();
                    p.allow_insert()
                        .where_(pe::eq("user_id", pe::session(vec!["claims", "sub"])));
                })),
        )
        .table(
            TableSchema::builder("protected")
                .column("data", ColumnType::Text)
                .policies(protected_policies),
        )
        .build();

    let server = JazzServer::start_with_schema(schema.clone())
        .await
        .expect("start test server");
    let alice =
        jazz_testkit::connect(server.make_client_context_for_user(schema.clone(), super::ALICE_ID))
            .await
            .expect("connect alice");
    // Bob authors optimistic local writes as an ordinary user. Backend
    // credentials would bypass the server policy this test exercises.
    let bob = jazz_testkit::TestingClient::builder()
        .with_server(&server)
        .with_schema(schema.clone())
        .with_user_id(super::BOB_ID)
        .as_user()
        .ready_on("protected", Duration::from_secs(30))
        .connect()
        .await;

    let (admin_id, _, _) = alice
        .insert("admins", crate::row_input!("user_id" => super::ALICE_ID))
        .expect("seed alice admin row");
    let (protected_id, _, _) = alice
        .insert("protected", crate::row_input!("data" => "original data"))
        .expect("seed protected row");

    wait_for_admin_row(&bob, admin_id, super::ALICE_ID).await;
    wait_for_protected_row(
        &bob,
        protected_id,
        "original data",
        "bob sees the protected row before attempting the permissive update",
    )
    .await;

    let bob_transaction_id = bob
        .update(
            "protected",
            protected_id,
            vec![("data".into(), Value::Text("hacked by bob".into()))],
        )
        .expect("permissive non-admin update should succeed locally");
    let rejected = bob
        .wait_for_transaction(
            bob_transaction_id.expect("permissive update should commit immediately"),
            DurabilityTier::EdgeServer,
        )
        .await
        .expect_err("bob's update should be rejected by EXISTS in USING policy on sync");
    assert_transaction_policy_denied(rejected);

    wait_for_protected_row(
        &alice,
        protected_id,
        "original data",
        "alice still sees original data after bob's rejected update",
    )
    .await;
    wait_for_protected_row(
        &bob,
        protected_id,
        "original data",
        "bob sees original data again after his rejected update",
    )
    .await;

    alice
        .update(
            "protected",
            protected_id,
            vec![("data".into(), Value::Text("updated by admin alice".into()))],
        )
        .expect("admin update should be allowed locally");
    wait_for_protected_row(
        &bob,
        protected_id,
        "updated by admin alice",
        "bob sees alice's accepted admin update",
    )
    .await;

    server.shutdown().await;
}

/// Verifies server settlement of explicit-session UPDATEs under an EXISTS
/// policy: Bob's optimistic write is rejected and Alice's admin write is accepted.
#[tokio::test]
async fn explicit_session_update_using_exists_policy_allows_admin_and_denies_non_admin() {
    tokio::task::LocalSet::new()
        .run_until(
            explicit_session_update_using_exists_policy_allows_admin_and_denies_non_admin_inner(),
        )
        .await;
}

async fn explicit_session_update_using_exists_policy_allows_admin_and_denies_non_admin_inner() {
    let protected_policies = permissions(|p| {
        p.allow_read().always();
        p.allow_update()
            .where_old(pe::exists(
                pe::table("admins").where_(pe::eq("user_id", pe::session(vec!["claims", "sub"]))),
            ))
            .where_new(pe::always());
    });
    let schema = SchemaBuilder::new()
        .table(
            TableSchema::builder("admins")
                .column("user_id", ColumnType::Text)
                .policies(permissions(|p| p.allow_read().always())),
        )
        .table(
            TableSchema::builder("protected")
                .column("data", ColumnType::Text)
                .policies(protected_policies),
        )
        .build();

    let server = JazzServer::start_with_schema(schema.clone())
        .await
        .expect("start test server");
    let client = super::support::connect_ready_client(
        &server,
        &schema,
        "exists-admin",
        "protected",
        Duration::from_secs(30),
    )
    .await;

    client
        .insert("admins", crate::row_input!("user_id" => super::ALICE_ID))
        .expect("seed admin row");
    let protected = client
        .insert("protected", crate::row_input!("data" => "initial"))
        .expect("seed protected row")
        .0;

    wait_for_protected_row(&client, protected, "initial", "seed row settled").await;
    let bob_transaction = client
        .for_session(session(super::BOB_ID, 2))
        .update(
            "protected",
            protected,
            vec![("data".into(), Value::Text("bob update".into()))],
        )
        .expect("stage optimistic non-admin update")
        .expect("update commits immediately");
    let rejection = client
        .wait_for_transaction(bob_transaction, DurabilityTier::EdgeServer)
        .await
        .expect_err("server must reject Bob's update");
    assert_transaction_policy_denied(rejection);
    wait_for_protected_row(&client, protected, "initial", "Bob's update is rolled back").await;

    let alice_transaction = client
        .for_session(session(super::ALICE_ID, 1))
        .update(
            "protected",
            protected,
            vec![("data".into(), Value::Text("alice update".into()))],
        )
        .expect("stage admin update")
        .expect("update commits immediately");
    client
        .wait_for_transaction(alice_transaction, DurabilityTier::EdgeServer)
        .await
        .expect("server accepts Alice's update");
    wait_for_protected_row(
        &client,
        protected,
        "alice update",
        "Alice's update is visible",
    )
    .await;

    client.shutdown().await.expect("shutdown client");
    server.shutdown().await;
}
