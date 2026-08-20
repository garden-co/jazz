use std::time::Duration;

use crate::JazzClient;
use jazz::tools::DurabilityTier;
use jazz_server::JazzServer;
use jazz_testkit::{
    connect_ready_user, wait_for_edge_tx_rejection, wait_for_edge_txs, wait_for_query,
};

use super::*;

const WAIT_TIMEOUT: Duration = Duration::from_secs(5);

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
        Some(DurabilityTier::EdgeServer),
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
        Some(DurabilityTier::EdgeServer),
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
        Some(DurabilityTier::EdgeServer),
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

    let server = JazzServer::start_with_schema(schema.clone()).await;
    let bob =
        jazz_testkit::connect(server.make_client_context_for_user(schema.clone(), super::BOB_ID))
            .await
            .expect("connect bob");
    let alice = jazz_testkit::connect(server.make_client_context_for_user(schema, super::ALICE_ID))
        .await
        .expect("connect alice");

    let (protected_id, _, transaction_id) = bob
        .insert("protected", crate::row_input!("data" => "secret data"))
        .expect("permissive non-admin insert should succeed locally");
    wait_for_edge_tx_rejection(
        &bob,
        transaction_id.expect("permissive insert should commit immediately"),
    )
    .await;
    wait_for_protected_row_absent(
        &alice,
        protected_id,
        "alice never sees bob's rejected protected insert",
    )
    .await;

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

    let server = JazzServer::start_with_schema(schema.clone()).await;
    let alice = connect_ready_user(&server, &schema, super::ALICE_ID, "admins", WAIT_TIMEOUT).await;
    let bob = connect_ready_user(&server, &schema, super::BOB_ID, "admins", WAIT_TIMEOUT).await;

    let (admin_id, _, admin_tx) = alice
        .insert("admins", crate::row_input!("user_id" => super::ALICE_ID))
        .expect("seed alice admin row");
    let (protected_id, _, protected_tx) = alice
        .insert("protected", crate::row_input!("data" => "original data"))
        .expect("seed protected row");
    wait_for_edge_txs(
        &alice,
        &[
            admin_tx.expect("admin seed should commit immediately"),
            protected_tx.expect("protected seed should commit immediately"),
        ],
    )
    .await;

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
            protected_id,
            vec![("data".into(), Value::Text("hacked by bob".into()))],
        )
        .expect("permissive non-admin update should succeed locally");
    wait_for_edge_tx_rejection(
        &bob,
        bob_transaction_id.expect("permissive update should commit immediately"),
    )
    .await;

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

    let alice_transaction_id = alice
        .update(
            protected_id,
            vec![("data".into(), Value::Text("updated by admin alice".into()))],
        )
        .expect("admin update should be allowed locally")
        .expect("admin update should commit immediately");
    wait_for_edge_txs(&alice, &[alice_transaction_id]).await;
    wait_for_protected_row(
        &bob,
        protected_id,
        "updated by admin alice",
        "bob sees alice's accepted admin update",
    )
    .await;

    server.shutdown().await;
}
