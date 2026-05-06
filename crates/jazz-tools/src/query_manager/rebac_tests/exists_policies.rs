use super::*;

#[test]
fn rebac_exists_clause_denies_non_matching_insert() {
    // Schema with EXISTS policy: only admins can insert
    let mut schema = Schema::new();

    // Admins table
    let admins_descriptor =
        RowDescriptor::new(vec![ColumnDescriptor::new("user_id", ColumnType::Text)]);
    schema.insert(
        TableName::new("admins"),
        TableSchema::with_policies(
            admins_descriptor,
            TablePolicies::new().with_select(PolicyExpr::True),
        ),
    );

    // Protected table: only admins can insert
    let protected_descriptor =
        RowDescriptor::new(vec![ColumnDescriptor::new("data", ColumnType::Text)]);
    let protected_policies = TablePolicies::new().with_insert(PolicyExpr::Exists {
        table: "admins".into(),
        condition: Box::new(PolicyExpr::eq_session("user_id", vec!["user_id".into()])),
    });
    schema.insert(
        TableName::new("protected"),
        TableSchema::with_policies(protected_descriptor, protected_policies),
    );

    let sync_manager = SyncManager::new();
    let mut qm = create_query_manager(sync_manager, schema);
    let mut storage = seeded_memory_storage(&qm.schema_context().current_schema);

    // Add a client with session for non-admin user
    let client_id = ClientId::new();
    connect_client(&mut qm, &storage, client_id);
    qm.sync_manager_mut()
        .set_client_session(client_id, Session::new("regular_user"));

    // Note: We do NOT add "regular_user" to admins table

    // Create object for protected row
    let mut metadata = std::collections::HashMap::new();
    metadata.insert(MetadataKey::Table.to_string(), "protected".to_string());
    let obj_id = create_test_row(&mut storage, Some(metadata.clone()));

    // Register query scope
    let mut scope = HashSet::new();
    scope.insert((obj_id, "main".into()));
    set_client_query_scope(&mut qm, &storage, client_id, QueryId(1), scope, None);
    qm.sync_manager_mut().take_outbox();

    // Encode row content
    let protected_desc = RowDescriptor::new(vec![ColumnDescriptor::new("data", ColumnType::Text)]);
    let content = encode_row(&protected_desc, &[Value::Text("secret data".into())]).unwrap();

    // Non-admin tries to insert
    let commit = stored_row_commit(
        smallvec![],
        content,
        1000,
        ObjectId::new().to_string(),
        None,
    );

    qm.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Client(client_id),
        payload: row_batch_created_payload(
            obj_id,
            "main",
            Some(RowMetadata {
                id: obj_id,
                metadata,
            }),
            &commit,
        ),
    });

    // Process
    qm.process(&mut storage);

    // Should get permission denied (non-admin cannot insert)
    let outbox = qm.sync_manager_mut().take_outbox();
    assert!(
        client_write_was_rejected(
            &outbox,
            client_id,
            row_batch_id_for_commit(obj_id, "main", &commit),
        ),
        "Non-admin insert should be denied by EXISTS policy"
    );

    // Commit should NOT be applied to the branch.
    assert!(
        test_row_metadata(&storage, obj_id).is_some(),
        "Object should still exist after denied insert"
    );
    let tips = test_row_tip_ids(&storage, obj_id, "main");
    assert!(
        tips.is_err(),
        "Denied insert should not create tips on branch main"
    );
}

#[test]
fn rebac_update_denied_by_using_exists_policy() {
    // Schema with EXISTS policy: only admins can update
    let mut schema = Schema::new();

    // Admins table
    let admins_descriptor =
        RowDescriptor::new(vec![ColumnDescriptor::new("user_id", ColumnType::Text)]);
    schema.insert(
        TableName::new("admins"),
        TableSchema::with_policies(
            admins_descriptor.clone(),
            TablePolicies::new().with_select(PolicyExpr::True),
        ),
    );

    // Protected table: only admins can update (via EXISTS in USING)
    let protected_descriptor =
        RowDescriptor::new(vec![ColumnDescriptor::new("data", ColumnType::Text)]);
    let protected_policies = TablePolicies::new().with_update(
        // USING: EXISTS (SELECT FROM admins WHERE user_id = @session.user_id)
        Some(PolicyExpr::Exists {
            table: "admins".into(),
            condition: Box::new(PolicyExpr::eq_session("user_id", vec!["user_id".into()])),
        }),
        // WITH CHECK: no restriction on new row
        PolicyExpr::True,
    );
    schema.insert(
        TableName::new("protected"),
        TableSchema::with_policies(protected_descriptor.clone(), protected_policies),
    );

    let sync_manager = SyncManager::new();
    let mut qm = create_query_manager(sync_manager, schema);
    let mut storage = seeded_memory_storage(&qm.schema_context().current_schema);

    // Add Alice as admin (using insert to properly index the row)
    let _alice_admin = qm
        .insert(&mut storage, "admins", &[Value::Text("alice".into())])
        .unwrap();

    // Create a protected row (as server, no session) - also using insert for proper indexing
    let protected_handle = qm
        .insert(
            &mut storage,
            "protected",
            &[Value::Text("original data".into())],
        )
        .unwrap();
    let protected_obj = protected_handle.row_id;
    let initial_commit = protected_handle.batch_id;

    // Get object metadata for later use in update payloads
    let protected_metadata = test_row_metadata(&storage, protected_obj).unwrap_or_default();

    // ---- Bob (non-admin) tries to update ----
    let branch = get_branch(&qm);
    let bob_client = ClientId::new();
    connect_client(&mut qm, &storage, bob_client);
    qm.sync_manager_mut()
        .set_client_session(bob_client, Session::new("bob"));

    // Register query scope for Bob
    let mut bob_scope = HashSet::new();
    bob_scope.insert((protected_obj, branch.clone().into()));
    set_client_query_scope(&mut qm, &storage, bob_client, QueryId(1), bob_scope, None);
    qm.sync_manager_mut().take_outbox();

    // Bob tries to update the protected row
    let bob_update_content = encode_row(
        &protected_descriptor,
        &[Value::Text("hacked by bob".into())],
    )
    .unwrap();
    let bob_commit = stored_row_commit(
        smallvec![initial_commit],
        bob_update_content,
        2000,
        ObjectId::new().to_string(),
        None,
    );

    qm.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Client(bob_client),
        payload: row_batch_created_payload(
            protected_obj,
            &branch,
            Some(RowMetadata {
                id: protected_obj,
                metadata: protected_metadata.clone(),
            }),
            &bob_commit,
        ),
    });

    // Process - may need multiple iterations for EXISTS to settle
    for _ in 0..10 {
        qm.process(&mut storage);
    }

    // Bob should get permission denied
    let outbox = qm.sync_manager_mut().take_outbox();
    assert!(
        client_write_was_rejected(
            &outbox,
            bob_client,
            row_batch_id_for_commit(protected_obj, &branch, &bob_commit),
        ),
        "Bob's update should be denied by EXISTS in USING policy"
    );

    // Bob's update should NOT be applied
    let tips = test_row_tip_ids(&storage, protected_obj, &branch).unwrap();
    assert!(
        !tips.contains(&row_batch_id_for_commit(
            protected_obj,
            &branch,
            &bob_commit,
        )),
        "Bob's update should not be applied - he is not an admin"
    );

    // ---- Alice (admin) tries to update ----
    let alice_client = ClientId::new();
    connect_client(&mut qm, &storage, alice_client);
    qm.sync_manager_mut()
        .set_client_session(alice_client, Session::new("alice"));

    // Register query scope for Alice
    let mut alice_scope = HashSet::new();
    alice_scope.insert((protected_obj, branch.clone().into()));
    set_client_query_scope(
        &mut qm,
        &storage,
        alice_client,
        QueryId(2),
        alice_scope,
        None,
    );
    qm.sync_manager_mut().take_outbox();

    // Alice tries to update the protected row
    let alice_update_content = encode_row(
        &protected_descriptor,
        &[Value::Text("updated by admin alice".into())],
    )
    .unwrap();
    let alice_commit = stored_row_commit(
        smallvec![initial_commit],
        alice_update_content,
        3000,
        ObjectId::new().to_string(),
        None,
    );

    qm.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Client(alice_client),
        payload: row_batch_created_payload(
            protected_obj,
            &branch,
            Some(RowMetadata {
                id: protected_obj,
                metadata: protected_metadata.clone(),
            }),
            &alice_commit,
        ),
    });

    // Process - may need multiple iterations for EXISTS to settle
    for _ in 0..10 {
        qm.process(&mut storage);
    }

    // Alice should NOT get permission denied
    let outbox = qm.sync_manager_mut().take_outbox();
    assert!(
        !client_write_was_rejected(
            &outbox,
            alice_client,
            row_batch_id_for_commit(protected_obj, &branch, &alice_commit),
        ),
        "Alice's update should be allowed by EXISTS in USING policy (she is an admin)"
    );

    // Alice's update SHOULD be applied
    let tips = test_row_tip_ids(&storage, protected_obj, &branch).unwrap();
    assert!(
        tips.contains(&row_batch_id_for_commit(
            protected_obj,
            &branch,
            &alice_commit,
        )),
        "Alice's update should be applied - she is an admin"
    );
}

#[test]
fn local_update_using_exists_policy_allows_admin_and_denies_non_admin() {
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
    let protected_policies = TablePolicies::new().with_update(
        Some(PolicyExpr::Exists {
            table: "admins".into(),
            condition: Box::new(PolicyExpr::eq_session("user_id", vec!["user_id".into()])),
        }),
        PolicyExpr::True,
    );
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

    let bob_err = qm
        .update_with_session(
            &mut storage,
            protected.row_id,
            &[Value::Text("bob update".into())],
            Some(&Session::new("bob")),
        )
        .expect_err("non-admin update should be denied");
    assert!(matches!(
        bob_err,
        QueryError::PolicyDenied {
            table,
            operation: Operation::Update
        } if table == TableName::new("protected")
    ));

    qm.update_with_session(
        &mut storage,
        protected.row_id,
        &[Value::Text("alice update".into())],
        Some(&Session::new("alice")),
    )
    .expect("admin update should be allowed");
}
