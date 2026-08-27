use crate::JazzClient;
use jazz_server::JazzServer;
use jazz_testkit::{connect_ready_client, connect_ready_user, wait_for_edge_txs, wait_for_query};

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

/// Verifies that UPDATE evaluates the USING/old-row policy, not only the
/// WITH CHECK/new-row policy, so invisible rows cannot be edited.
#[tokio::test]
#[ignore = "#1762: the public client refuses updates to policy-hidden rows as unobserved before UPDATE USING can be exercised"]
async fn rebac_update_denied_by_using_policy() {
    tokio::task::LocalSet::new()
        .run_until(rebac_update_denied_by_using_policy_inner())
        .await;
}

async fn rebac_update_denied_by_using_policy_inner() {
    // Schema with both USING and WITH CHECK for updates
    let docs_table = TableSchema::builder("documents")
        .column("owner_id", ColumnType::Text)
        .column("content", ColumnType::Text);

    // UPDATE policy: USING (owner_id = @user_id) WITH CHECK (owner_id = @user_id)
    // This means: you can only update rows you own, and the result must still be owned by you
    let owner_is_session = pe::eq("owner_id", pe::session(vec!["claims", "sub"]));
    let docs_policies = permissions(|p| {
        p.allow_read().where_(owner_is_session.clone());
        p.allow_update()
            .where_old(owner_is_session.clone()) // USING
            .where_new(owner_is_session); // WITH CHECK
    });

    let schema = SchemaBuilder::new()
        .table(docs_table.policies(docs_policies))
        .build();
    let server = JazzServer::start_with_schema(schema.clone()).await;
    let admin = connect_ready_client(
        &server,
        &schema,
        "mutations-admin",
        "documents",
        Duration::from_secs(30),
    )
    .await;
    let alice = connect_ready_user(
        &server,
        &schema,
        super::ALICE_ID,
        "documents",
        Duration::from_secs(30),
    )
    .await;
    let bob = connect_ready_user(
        &server,
        &schema,
        super::BOB_ID,
        "documents",
        Duration::from_secs(30),
    )
    .await;

    let (obj_id, _, transaction_id) = admin
        .insert(
            "documents",
            crate::row_input!("owner_id" => super::ALICE_ID, "content" => "Alice's secret"),
        )
        .expect("seed alice document");
    wait_for_edge_txs(
        &admin,
        &[transaction_id.expect("seed insert should commit immediately")],
    )
    .await;

    // Bob tries to update Alice's document (keeping owner as alice to pass WITH CHECK,
    // but USING should still deny because Bob can't see Alice's row).
    let err = bob
        .update(
            obj_id,
            vec![
                ("owner_id".into(), Value::Text(super::ALICE_ID.into())),
                ("content".into(), Value::Text("Hacked by Bob".into())),
            ],
        )
        .expect_err("Bob's update of Alice's document should be denied by USING policy");
    assert_client_policy_denied(err, "documents", Operation::Update);

    let alice_rows = alice
        .query(
            Query::from("documents")
                .filter(eq(col("id"), lit(*obj_id.uuid())))
                .select(["content"]),
            Some(jazz::tools::DurabilityTier::EdgeServer),
        )
        .await
        .expect("query alice document");
    assert_eq!(
        alice_rows,
        vec![(obj_id, vec![Value::Text("Alice's secret".into())])],
        "Bob's denied update should not change Alice's document"
    );

    admin.shutdown().await.expect("shutdown admin");
    alice.shutdown().await.expect("shutdown alice");
    bob.shutdown().await.expect("shutdown bob");
    server.shutdown().await;
}

/// Verifies that synced soft deletes are authorized by DELETE policies, and
/// that a rejected optimistic delete restores the row for the originating peer.
#[tokio::test]
#[ignore = "#1759: server schema conversion requires the DELETE ExistsRel policy to include an outer-row equality"]
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
            pe::table("admins").where_(pe::rel::eq_session("user_id", vec!["claims", "sub"])),
        ));
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
                .policies(only_admins_can_delete),
        )
        .build();

    let server = JazzServer::start_with_schema(schema.clone()).await;
    let alice =
        jazz_testkit::connect(server.make_client_context_for_user(schema.clone(), super::ALICE_ID))
            .await
            .expect("connect alice");
    let bob =
        jazz_testkit::connect(server.make_client_context_for_user(schema.clone(), super::BOB_ID))
            .await
            .expect("connect bob");

    let (admin_id, _, _) = alice
        .insert("admins", crate::row_input!("user_id" => super::ALICE_ID))
        .expect("seed alice admin row");
    let (protected_id, _, _) = alice
        .insert("protected", crate::row_input!("data" => "initial"))
        .expect("seed protected row");

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
    let bob_delete = bob
        .wait_for_transaction(
            bob_delete_transaction.expect("permissive delete should commit immediately"),
            jazz::tools::DurabilityTier::EdgeServer,
        )
        .await;
    assert!(
        bob_delete.is_err(),
        "non-admin soft delete should be rejected by the server delete policy"
    );

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

    alice
        .delete(protected_id)
        .expect("admin soft delete should be accepted locally");

    wait_for_protected_rows(
        &bob,
        protected_id,
        "bob no longer sees the protected row after alice's accepted delete",
        |rows| rows.is_empty(),
    )
    .await;

    server.shutdown().await;
}
