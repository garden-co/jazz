#[cfg(feature = "client")]
use crate::JazzClient;
#[cfg(feature = "test-utils")]
use crate::server::TestingServer;
#[cfg(feature = "test-utils")]
use crate::test_support::wait_for_query;

use super::*;

#[cfg(feature = "test-utils")]
async fn wait_for_protected_rows(
    client: &JazzClient,
    protected_id: ObjectId,
    description: &str,
    mut predicate: impl FnMut(&[(ObjectId, Vec<Value>)]) -> bool,
) -> Vec<(ObjectId, Vec<Value>)> {
    let query = QueryBuilder::new("protected")
        .filter_eq("id", Value::Uuid(protected_id))
        .select(&["data"])
        .build();

    wait_for_query(
        client,
        query,
        Some(crate::sync_manager::DurabilityTier::EdgeServer),
        Duration::from_secs(5),
        description,
        |rows| predicate(&rows).then_some(rows),
    )
    .await
}

#[cfg(feature = "client")]
#[tokio::test]
async fn rebac_update_denied_by_using_policy() {
    // Schema with both USING and WITH CHECK for updates
    let docs_table = TableSchema::builder("documents")
        .column("owner_id", ColumnType::Text)
        .column("content", ColumnType::Text);

    // UPDATE policy: USING (owner_id = @user_id) WITH CHECK (owner_id = @user_id)
    // This means: you can only update rows you own, and the result must still be owned by you
    let owner_is_session = pe::eq("owner_id", pe::session("user_id"));
    let docs_policies = permissions(|p| {
        p.allow_read().where_(owner_is_session.clone());
        p.allow_update()
            .where_old(owner_is_session.clone()) // USING
            .where_new(owner_is_session); // WITH CHECK
    });

    let schema = SchemaBuilder::new()
        .table(docs_table.policies(docs_policies))
        .build();
    let client = JazzClient::test_client(schema).await;

    let (obj_id, _, _) = client
        .insert(
            "documents",
            crate::row_input!("owner_id" => "alice", "content" => "Alice's secret"),
            None,
        )
        .expect("seed alice document");

    // Bob tries to update Alice's document (keeping owner as alice to pass WITH CHECK,
    // but USING should still deny because Bob can't see Alice's row).
    let err = client
        .for_session(Session::new("bob"))
        .update(
            obj_id,
            vec![
                ("owner_id".into(), Value::Text("alice".into())),
                ("content".into(), Value::Text("Hacked by Bob".into())),
            ],
        )
        .expect_err("Bob's update of Alice's document should be denied by USING policy");
    assert_client_policy_denied(err, "documents", Operation::Update);

    let alice_rows = client
        .for_session(Session::new("alice"))
        .query(
            QueryBuilder::new("documents")
                .filter_eq("id", Value::Uuid(obj_id))
                .select(&["content"])
                .build(),
            None,
        )
        .await
        .expect("query alice document");
    assert_eq!(
        alice_rows,
        vec![(obj_id, vec![Value::Text("Alice's secret".into())])],
        "Bob's denied update should not change Alice's document"
    );
}

#[cfg(feature = "test-utils")]
#[tokio::test]
async fn synced_soft_delete_should_use_delete_policy() {
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

    let server = TestingServer::start_with_schema(schema.clone()).await;
    let alice = JazzClient::connect(server.make_client_context_for_user(schema.clone(), "alice"))
        .await
        .expect("connect alice");
    let bob = JazzClient::connect_with_row_policy_mode(
        server.make_client_context_for_user(schema.clone(), "bob"),
        crate::query_manager::types::RowPolicyMode::PermissiveLocal,
    )
    .await
    .expect("connect bob");

    let (admin_id, _, _) = alice
        .insert("admins", crate::row_input!("user_id" => "alice"), None)
        .expect("seed alice admin row");
    let (protected_id, _, _) = alice
        .insert("protected", crate::row_input!("data" => "initial"), None)
        .expect("seed protected row");

    wait_for_query(
        &bob,
        QueryBuilder::new("admins")
            .filter_eq("id", Value::Uuid(admin_id))
            .select(&["user_id"])
            .build(),
        Some(crate::sync_manager::DurabilityTier::EdgeServer),
        Duration::from_secs(5),
        "bob sees alice's admin row",
        |rows| (rows == [(admin_id, vec![Value::Text("alice".into())])]).then_some(rows),
    )
    .await;
    wait_for_protected_rows(
        &bob,
        protected_id,
        "bob syncs the protected row before attempting the delete",
        |rows| rows == [(protected_id, vec![Value::Text("initial".into())])],
    )
    .await;

    let bob_delete_batch = bob
        .delete(protected_id, None)
        .expect("bob should accept the delete locally");
    let bob_delete = bob
        .wait_for_batch(
            bob_delete_batch,
            crate::sync_manager::DurabilityTier::EdgeServer,
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
        .delete(protected_id, None)
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
