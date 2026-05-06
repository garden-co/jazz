use super::*;

#[test]
fn rebac_update_denied_by_using_policy() {
    // Schema with both USING and WITH CHECK for updates
    let mut schema = Schema::new();

    let docs_descriptor = RowDescriptor::new(vec![
        ColumnDescriptor::new("owner_id", ColumnType::Text),
        ColumnDescriptor::new("content", ColumnType::Text),
    ]);

    // UPDATE policy: USING (owner_id = @user_id) WITH CHECK (owner_id = @user_id)
    // This means: you can only update rows you own, and the result must still be owned by you
    let docs_policies = TablePolicies::new()
        .with_select(PolicyExpr::eq_session("owner_id", vec!["user_id".into()]))
        .with_update(
            Some(PolicyExpr::eq_session("owner_id", vec!["user_id".into()])), // USING
            PolicyExpr::eq_session("owner_id", vec!["user_id".into()]),       // WITH CHECK
        );

    schema.insert(
        TableName::new("documents"),
        TableSchema::with_policies(docs_descriptor.clone(), docs_policies),
    );

    let sync_manager = SyncManager::new();
    let mut qm = create_query_manager(sync_manager, schema);
    let mut storage = seeded_memory_storage(&qm.schema_context().current_schema);

    // Create Alice's document first (as server/no session)
    let mut metadata = std::collections::HashMap::new();
    metadata.insert(MetadataKey::Table.to_string(), "documents".to_string());
    let obj_id = create_test_row(&mut storage, Some(metadata.clone()));

    let alice_content = encode_row(
        &docs_descriptor,
        &[
            Value::Text("alice".into()),
            Value::Text("Alice's secret".into()),
        ],
    )
    .unwrap();
    let author = ObjectId::new();
    let initial_commit = add_row_commit(
        &mut storage,
        obj_id,
        "main",
        vec![],
        alice_content,
        1000,
        author.to_string(),
    );

    // Now Bob connects and tries to update Alice's document
    let bob_client = ClientId::new();
    connect_client(&mut qm, &storage, bob_client);
    qm.sync_manager_mut()
        .set_client_session(bob_client, Session::new("bob"));

    // Register query scope for Bob
    let mut scope = HashSet::new();
    scope.insert((obj_id, "main".into()));
    set_client_query_scope(&mut qm, &storage, bob_client, QueryId(1), scope, None);
    qm.sync_manager_mut().take_outbox();

    // Bob tries to update Alice's document (keeping owner as alice to pass WITH CHECK,
    // but USING should still deny because Bob can't see Alice's row)
    let bob_update_content = encode_row(
        &docs_descriptor,
        &[
            Value::Text("alice".into()),
            Value::Text("Hacked by Bob".into()),
        ],
    )
    .unwrap();

    let update_commit = stored_row_commit(
        smallvec![initial_commit],
        bob_update_content,
        2000,
        ObjectId::new().to_string(),
        None,
    );

    qm.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Client(bob_client),
        payload: row_batch_created_payload(
            obj_id,
            "main",
            Some(RowMetadata {
                id: obj_id,
                metadata,
            }),
            &update_commit,
        ),
    });

    // Process
    qm.process(&mut storage);

    // Should get permission denied (Bob cannot see Alice's row via USING)
    let outbox = qm.sync_manager_mut().take_outbox();
    assert!(
        client_write_was_rejected(
            &outbox,
            bob_client,
            row_batch_id_for_commit(obj_id, "main", &update_commit),
        ),
        "Bob's update of Alice's document should be denied by USING policy"
    );

    // Update should NOT be applied
    let tips = test_row_tip_ids(&storage, obj_id, "main").unwrap();
    assert!(
        !tips.contains(&row_batch_id_for_commit(obj_id, "main", &update_commit,)),
        "Bob's update should be denied - he cannot see Alice's document"
    );
}

#[test]
fn synced_soft_delete_should_use_delete_policy() {
    let mut schema = Schema::new();
    let admins_descriptor =
        RowDescriptor::new(vec![ColumnDescriptor::new("user_id", ColumnType::Text)]);
    schema.insert(
        TableName::new("admins"),
        TableSchema::with_policies(
            admins_descriptor.clone(),
            TablePolicies::new().with_select(PolicyExpr::True),
        ),
    );

    let protected_descriptor =
        RowDescriptor::new(vec![ColumnDescriptor::new("data", ColumnType::Text)]);
    let protected_policies = TablePolicies::new().with_delete(PolicyExpr::ExistsRel {
        rel: RelExpr::Filter {
            input: Box::new(RelExpr::TableScan {
                table: TableName::new("admins"),
            }),
            predicate: PredicateExpr::Cmp {
                left: ColumnRef::unscoped("user_id"),
                op: PredicateCmpOp::Eq,
                right: ValueRef::SessionRef(vec!["user_id".into()]),
            },
        },
    });
    schema.insert(
        TableName::new("protected"),
        TableSchema::with_policies(protected_descriptor.clone(), protected_policies),
    );

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
