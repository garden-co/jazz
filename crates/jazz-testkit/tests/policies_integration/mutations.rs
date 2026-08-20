use crate::JazzClient;
use jazz_server::JazzServer;
use jazz_testkit::{
    connect_ready_user, wait_for_edge_tx_rejection, wait_for_edge_txs, wait_for_query,
};

use super::*;

async fn wait_for_protected_rows(
    client: &JazzClient,
    protected_id: ObjectId,
    description: &str,
    mut predicate: impl FnMut(&[(ObjectId, Vec<Value>)]) -> bool,
) -> Vec<(ObjectId, Vec<Value>)> {
    let query = Query::from("protected")
        .filter(eq(col("id"), lit(*protected_id.uuid())))
        .select(["data"]);

    wait_for_query(
        client,
        query,
        Some(jazz::tools::DurabilityTier::EdgeServer),
        Duration::from_secs(5),
        description,
        |rows| predicate(&rows).then_some(rows),
    )
    .await
}

/// Verifies that synced soft deletes are authorized by DELETE policies, and
/// that a rejected optimistic delete restores the row for the originating peer.
#[tokio::test]
async fn synced_soft_delete_should_use_delete_policy() {
    tokio::task::LocalSet::new()
        .run_until(synced_soft_delete_should_use_delete_policy_inner())
        .await;
}

async fn synced_soft_delete_should_use_delete_policy_inner() {
    let only_admins_can_delete = permissions(|p| {
        p.allow_read().always();
        p.allow_insert().always();
        p.allow_delete().where_(pe::exists(
            pe::table("admins").where_(pe::rel::eq_session("user_id", "user_id")),
        ));
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
                .policies(only_admins_can_delete),
        )
        .build();

    let server = JazzServer::start_with_schema(schema.clone()).await;
    let alice = connect_ready_user(
        &server,
        &schema,
        super::ALICE_ID,
        "admins",
        Duration::from_secs(5),
    )
    .await;
    let bob = connect_ready_user(
        &server,
        &schema,
        super::BOB_ID,
        "admins",
        Duration::from_secs(5),
    )
    .await;

    let (admin_id, _, admin_tx) = alice
        .insert("admins", crate::row_input!("user_id" => super::ALICE_ID))
        .expect("seed alice admin row");
    let (protected_id, _, protected_tx) = alice
        .insert("protected", crate::row_input!("data" => "initial"))
        .expect("seed protected row");
    wait_for_edge_txs(
        &alice,
        &[
            admin_tx.expect("admin seed should commit immediately"),
            protected_tx.expect("protected seed should commit immediately"),
        ],
    )
    .await;

    wait_for_query(
        &bob,
        Query::from("admins")
            .filter(eq(col("id"), lit(*admin_id.uuid())))
            .select(["user_id"]),
        Some(jazz::tools::DurabilityTier::EdgeServer),
        Duration::from_secs(5),
        "bob sees alice's admin row",
        |rows| (rows == [(admin_id, vec![Value::Text(super::ALICE_ID.into())])]).then_some(rows),
    )
    .await;
    wait_for_protected_rows(
        &bob,
        protected_id,
        "bob syncs the protected row before attempting the delete",
        |rows| rows == [(protected_id, vec![Value::Text("initial".into())])],
    )
    .await;

    let bob_delete_transaction = bob
        .delete(protected_id)
        .expect("bob should accept the delete locally");
    wait_for_edge_tx_rejection(
        &bob,
        bob_delete_transaction.expect("permissive delete should commit immediately"),
    )
    .await;

    wait_for_protected_rows(
        &alice,
        protected_id,
        "alice still sees the protected row after bob's rejected delete",
        |rows| rows == [(protected_id, vec![Value::Text("initial".into())])],
    )
    .await;
    wait_for_protected_rows(
        &bob,
        protected_id,
        "bob sees the protected row again after his rejected delete",
        |rows| rows == [(protected_id, vec![Value::Text("initial".into())])],
    )
    .await;

    let alice_delete_transaction = alice
        .delete(protected_id)
        .expect("admin soft delete should be accepted locally")
        .expect("admin delete should commit immediately");
    wait_for_edge_txs(&alice, &[alice_delete_transaction]).await;

    wait_for_protected_rows(
        &bob,
        protected_id,
        "bob no longer sees the protected row after alice's accepted delete",
        |rows| rows.is_empty(),
    )
    .await;

    server.shutdown().await;
}
