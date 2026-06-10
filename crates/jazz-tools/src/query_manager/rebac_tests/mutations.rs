#[cfg(feature = "client")]
use crate::JazzClient;

use super::*;

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

#[test]
fn synced_soft_delete_should_use_delete_policy() {
    let protected_table = TableSchema::builder("protected").column("data", ColumnType::Text);
    let protected_descriptor = protected_table.clone().build().columns;
    let protected_policies = permissions(|p| {
        p.allow_delete().where_(pe::exists(
            pe::table("admins").where_(pe::rel::eq_session("user_id", "user_id")),
        ));
    });
    let schema = SchemaBuilder::new()
        .table(
            TableSchema::builder("admins")
                .column("user_id", ColumnType::Text)
                .policies(permissions(|p| p.allow_read().always())),
        )
        .table(protected_table.policies(protected_policies))
        .build();

    let sync_manager = SyncManager::new();
    let mut qm = create_query_manager(sync_manager, schema);
    let mut storage = seeded_memory_storage(&qm.schema_context().current_schema);

    qm.insert(&mut storage, "admins", &[Value::Text("alice".into())])
        .expect("seed admin row");
    let protected = qm
        .insert(&mut storage, "protected", &[Value::Text("initial".into())])
        .expect("seed protected row");
    let branch = get_branch(&qm);

    let protected_metadata =
        test_row_metadata(&storage, protected.row_id).expect("protected row metadata");

    let bob_client = ClientId::new();
    connect_client(&mut qm, &storage, bob_client);
    qm.sync_manager_mut()
        .set_client_session(bob_client, Session::new("bob"));

    let mut bob_scope = HashSet::new();
    bob_scope.insert((protected.row_id, branch.clone().into()));
    set_client_query_scope(&mut qm, &storage, bob_client, QueryId(1), bob_scope, None);
    qm.sync_manager_mut().take_outbox();

    let delete_content =
        encode_row(&protected_descriptor, &[Value::Text("initial".into())]).unwrap();
    let delete_commit = stored_row_commit(
        smallvec![protected.batch_id],
        delete_content,
        2000,
        ObjectId::new().to_string(),
        Some(DeleteKind::Soft),
    );

    qm.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Client(bob_client),
        payload: row_batch_created_payload(
            protected.row_id,
            &branch,
            Some(RowMetadata {
                id: protected.row_id,
                metadata: protected_metadata,
            }),
            &delete_commit,
        ),
    });

    for _ in 0..10 {
        qm.process(&mut storage);
    }

    let outbox = qm.sync_manager_mut().take_outbox();
    let denied = client_write_was_rejected(
        &outbox,
        bob_client,
        row_batch_id_for_commit(protected.row_id, &branch, &delete_commit),
    );
    assert!(
        denied,
        "soft deletes replicated over sync should be checked against DELETE policy"
    );

    let tips = test_row_tip_ids(&storage, protected.row_id, &branch).unwrap();
    assert!(
        !tips.contains(&row_batch_id_for_commit(
            protected.row_id,
            &branch,
            &delete_commit
        )),
        "denied synced soft delete should not be applied"
    );
    assert!(
        !qm.row_is_deleted(&storage, "protected", protected.row_id),
        "denied synced soft delete should leave the row visible"
    );
}
