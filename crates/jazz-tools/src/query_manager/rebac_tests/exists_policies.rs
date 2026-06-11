#[cfg(feature = "client")]
use std::time::Duration;

#[cfg(feature = "client")]
use crate::JazzClient;
#[cfg(feature = "test-utils")]
use crate::server::TestingServer;
#[cfg(feature = "client")]
use crate::sync_manager::DurabilityTier;
#[cfg(feature = "client")]
use crate::test_support::wait_for_query;

use super::*;

#[cfg(feature = "client")]
const WAIT_TIMEOUT: Duration = Duration::from_secs(5);

#[cfg(feature = "client")]
async fn wait_for_protected_row(
    client: &JazzClient,
    protected_id: ObjectId,
    expected_data: &str,
    description: &str,
) {
    wait_for_query(
        client,
        QueryBuilder::new("protected")
            .filter_eq("id", Value::Uuid(protected_id))
            .select(&["data"])
            .build(),
        Some(DurabilityTier::EdgeServer),
        WAIT_TIMEOUT,
        description,
        |rows| (rows == [(protected_id, vec![Value::Text(expected_data.into())])]).then_some(()),
    )
    .await;
}

#[cfg(feature = "client")]
async fn wait_for_protected_row_absent(
    client: &JazzClient,
    protected_id: ObjectId,
    description: &str,
) {
    wait_for_query(
        client,
        QueryBuilder::new("protected")
            .filter_eq("id", Value::Uuid(protected_id))
            .select(&["data"])
            .build(),
        Some(DurabilityTier::EdgeServer),
        WAIT_TIMEOUT,
        description,
        |rows| rows.is_empty().then_some(()),
    )
    .await;
}

#[cfg(feature = "client")]
async fn wait_for_admin_row(client: &JazzClient, admin_id: ObjectId, user_id: &str) {
    wait_for_query(
        client,
        QueryBuilder::new("admins")
            .filter_eq("id", Value::Uuid(admin_id))
            .select(&["user_id"])
            .build(),
        Some(DurabilityTier::EdgeServer),
        WAIT_TIMEOUT,
        format!("{user_id} admin row becomes visible"),
        |rows| (rows == [(admin_id, vec![Value::Text(user_id.into())])]).then_some(()),
    )
    .await;
}

#[cfg(feature = "test-utils")]
#[tokio::test]
async fn rebac_exists_clause_denies_non_matching_insert() {
    let protected_policies = permissions(|p| {
        p.allow_read().always();
        p.allow_insert().where_(pe::exists(
            pe::table("admins").where_(pe::eq("user_id", pe::session("user_id"))),
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

    let server = TestingServer::start_with_schema(schema.clone()).await;
    let bob = JazzClient::connect_with_row_policy_mode(
        server.make_client_context_for_user(schema.clone(), "bob"),
        crate::query_manager::types::RowPolicyMode::PermissiveLocal,
    )
    .await
    .expect("connect bob");
    let alice = JazzClient::connect(server.make_client_context_for_user(schema, "alice"))
        .await
        .expect("connect alice");

    let (protected_id, _, batch_id) = bob
        .insert(
            "protected",
            crate::row_input!("data" => "secret data"),
            None,
        )
        .expect("permissive non-admin insert should succeed locally");
    let rejected = bob
        .wait_for_batch(batch_id, DurabilityTier::EdgeServer)
        .await;
    assert!(
        rejected.is_err(),
        "non-admin insert should be rejected by EXISTS policy on sync"
    );
    wait_for_protected_row_absent(
        &alice,
        protected_id,
        "alice never sees bob's rejected protected insert",
    )
    .await;

    server.shutdown().await;
}

#[cfg(feature = "test-utils")]
#[tokio::test]
async fn rebac_update_denied_by_using_exists_policy() {
    let protected_policies = permissions(|p| {
        p.allow_read().always();
        p.allow_insert().always();
        p.allow_update()
            .where_old(pe::exists(
                pe::table("admins").where_(pe::eq("user_id", pe::session("user_id"))),
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
                        .where_(pe::eq("user_id", pe::session("user_id")));
                })),
        )
        .table(
            TableSchema::builder("protected")
                .column("data", ColumnType::Text)
                .policies(protected_policies),
        )
        .build();

    let server = TestingServer::start_with_schema(schema.clone()).await;
    let alice = JazzClient::connect(server.make_client_context_for_user(schema.clone(), "alice"))
        .await
        .expect("connect alice");
    let bob = JazzClient::connect_with_row_policy_mode(
        server.make_client_context_for_user(schema.clone(), "bob"),
        crate::query_manager::types::RowPolicyMode::PermissiveLocal,
    )
    .await
    .expect("connect permissive bob");

    let (admin_id, _, _) = alice
        .insert("admins", crate::row_input!("user_id" => "alice"), None)
        .expect("seed alice admin row");
    let (protected_id, _, _) = alice
        .insert(
            "protected",
            crate::row_input!("data" => "original data"),
            None,
        )
        .expect("seed protected row");

    wait_for_admin_row(&bob, admin_id, "alice").await;
    wait_for_protected_row(
        &bob,
        protected_id,
        "original data",
        "bob sees the protected row before attempting the permissive update",
    )
    .await;

    let bob_batch_id = bob
        .update(
            protected_id,
            vec![("data".into(), Value::Text("hacked by bob".into()))],
            None,
        )
        .expect("permissive non-admin update should succeed locally");
    let rejected = bob
        .wait_for_batch(bob_batch_id, DurabilityTier::EdgeServer)
        .await;
    assert!(
        rejected.is_err(),
        "bob's update should be rejected by EXISTS in USING policy on sync"
    );

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
            protected_id,
            vec![("data".into(), Value::Text("updated by admin alice".into()))],
            None,
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

#[cfg(feature = "client")]
#[tokio::test]
async fn local_update_using_exists_policy_allows_admin_and_denies_non_admin() {
    let protected_policies = permissions(|p| {
        p.allow_update()
            .where_old(pe::exists(
                pe::table("admins").where_(pe::eq("user_id", pe::session("user_id"))),
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

    let client = JazzClient::test_client(schema).await;

    client
        .insert("admins", crate::row_input!("user_id" => "alice"), None)
        .expect("seed admin row");
    let protected = client
        .insert("protected", crate::row_input!("data" => "initial"), None)
        .expect("seed protected row")
        .0;

    let bob_err = client
        .for_session(Session::new("bob"))
        .update(
            protected,
            vec![("data".into(), Value::Text("bob update".into()))],
        )
        .expect_err("non-admin update should be denied");
    assert_client_policy_denied(bob_err, "protected", Operation::Update);

    client
        .for_session(Session::new("alice"))
        .update(
            protected,
            vec![("data".into(), Value::Text("alice update".into()))],
        )
        .expect("admin update should be allowed");
}
