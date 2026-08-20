use jazz_server::JazzServer;
use jazz_testkit::{
    connect_ready_client, connect_ready_user, wait_for_edge_tx_rejection, wait_for_edge_txs,
};

use super::*;

/// Verifies server INSERT enforcement for an EXISTS_REL admin policy: sessions
/// without a matching admin row are denied and admins are allowed.
#[tokio::test]
async fn insert_with_exists_rel_policy_denies_non_admin() {
    tokio::task::LocalSet::new()
        .run_until(insert_with_exists_rel_policy_denies_non_admin_inner())
        .await;
}

async fn insert_with_exists_rel_policy_denies_non_admin_inner() {
    let projects_policies = permissions(|p| {
        p.allow_insert().where_(pe::exists(
            pe::table("admins").where_(pe::rel::eq_session("user_id", "user_id")),
        ));
    });
    let schema = SchemaBuilder::new()
        .table(
            TableSchema::builder("admins")
                .column("user_id", ColumnType::Text)
                .policies(permissions(|p| p.allow_read().always())),
        )
        .table(
            TableSchema::builder("projects")
                .column("name", ColumnType::Text)
                .policies(projects_policies),
        )
        .build();
    let server = JazzServer::start_with_schema(schema.clone()).await;
    let admin = connect_ready_client(
        &server,
        &schema,
        "exists-rel-admin",
        "projects",
        Duration::from_secs(30),
    )
    .await;
    let alice = connect_ready_user(
        &server,
        &schema,
        super::ALICE_ID,
        "projects",
        Duration::from_secs(30),
    )
    .await;
    let bob = connect_ready_user(
        &server,
        &schema,
        super::BOB_ID,
        "projects",
        Duration::from_secs(30),
    )
    .await;

    let admin_tx = admin
        .insert("admins", crate::row_input!("user_id" => super::ALICE_ID))
        .expect("seed admin row")
        .2
        .expect("admin seed should commit immediately");
    wait_for_edge_txs(&admin, &[admin_tx]).await;

    let bob_tx = bob
        .insert("projects", crate::row_input!("name" => "bob project"))
        .expect("bob's client should accept the insert optimistically")
        .2
        .expect("bob's insert should commit locally");
    wait_for_edge_tx_rejection(&bob, bob_tx).await;

    let alice_tx = alice
        .insert("projects", crate::row_input!("name" => "alice project"))
        .expect("admin insert should be accepted locally")
        .2
        .expect("alice's insert should commit locally");
    wait_for_edge_txs(&alice, &[alice_tx]).await;

    admin.shutdown().await.expect("shutdown admin");
    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    server.shutdown().await;
}

/// Verifies that relation predicates compare NULL literals correctly inside
/// EXISTS_REL, allowing active rows and denying revoked rows.
#[tokio::test]
#[ignore = "server schema conversion rejects ExistsRel equality against NULL with OperandTypeMismatch"]
async fn insert_with_exists_rel_null_literal_predicate_matches_null_rows() {
    tokio::task::LocalSet::new()
        .run_until(insert_with_exists_rel_null_literal_predicate_matches_null_rows_inner())
        .await;
}

async fn insert_with_exists_rel_null_literal_predicate_matches_null_rows_inner() {
    let projects_policies = permissions(|p| {
        p.allow_insert()
            .where_(pe::exists(pe::table("admins").where_(pe::rel::all_of([
                pe::rel::eq_session("user_id", "user_id"),
                pe::rel::eq_literal("revoked_at", Value::Null),
            ]))));
    });
    let schema = SchemaBuilder::new()
        .table(
            TableSchema::builder("admins")
                .column("user_id", ColumnType::Text)
                .nullable_column("revoked_at", ColumnType::Text)
                .policies(permissions(|p| p.allow_read().always())),
        )
        .table(
            TableSchema::builder("projects")
                .column("name", ColumnType::Text)
                .policies(projects_policies),
        )
        .build();
    let server = JazzServer::start_with_schema(schema.clone()).await;
    let admin = connect_ready_client(
        &server,
        &schema,
        "exists-rel-admin",
        "projects",
        Duration::from_secs(30),
    )
    .await;
    let alice = connect_ready_user(
        &server,
        &schema,
        super::ALICE_ID,
        "projects",
        Duration::from_secs(30),
    )
    .await;
    let carol = connect_ready_user(
        &server,
        &schema,
        super::CAROL_ID,
        "projects",
        Duration::from_secs(30),
    )
    .await;

    let active_admin_tx = admin
        .insert(
            "admins",
            crate::row_input!("user_id" => super::ALICE_ID, "revoked_at" => Value::Null),
        )
        .expect("seed active admin row")
        .2
        .expect("active admin seed should commit immediately");
    let revoked_admin_tx = admin
        .insert(
            "admins",
            crate::row_input!("user_id" => super::CAROL_ID, "revoked_at" => "2026-03-30T12:00:00Z"),
        )
        .expect("seed revoked admin row")
        .2
        .expect("revoked admin seed should commit immediately");
    wait_for_edge_txs(&admin, &[active_admin_tx, revoked_admin_tx]).await;

    let alice_tx = alice
        .insert("projects", crate::row_input!("name" => "alice project"))
        .expect("active admin insert should be accepted locally")
        .2
        .expect("active admin insert should commit locally");
    wait_for_edge_txs(&alice, &[alice_tx]).await;

    let carol_tx = carol
        .insert("projects", crate::row_input!("name" => "carol project"))
        .expect("revoked admin insert should be accepted optimistically")
        .2
        .expect("revoked admin insert should commit locally");
    wait_for_edge_tx_rejection(&carol, carol_tx).await;

    admin.shutdown().await.expect("shutdown admin");
    alice.shutdown().await.expect("shutdown alice");
    carol.shutdown().await.expect("shutdown carol");
    server.shutdown().await;
}
