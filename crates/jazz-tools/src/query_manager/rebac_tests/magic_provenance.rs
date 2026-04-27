use super::*;

#[test]
fn magic_columns_reactively_track_update_and_delete_permissions() {
    let schema = magic_introspection_schema();
    let sync_manager = SyncManager::new();
    let mut qm = create_query_manager(sync_manager, schema);
    let mut storage = seeded_memory_storage(&qm.schema_context().current_schema);

    let protected = qm
        .insert(&mut storage, "protected", &[Value::Text("initial".into())])
        .expect("seed protected row");

    let query = qm
        .query("protected")
        .select(&["data", "$canRead", "$canEdit", "$canDelete"])
        .build();
    let sub_id = qm
        .subscribe_with_session(query, Some(Session::new("alice")), None)
        .expect("subscribe with session");

    qm.process(&mut storage);
    let initial_update = qm
        .take_updates()
        .into_iter()
        .find(|update| update.subscription_id == sub_id)
        .expect("initial magic column update");
    let initial_row = initial_update
        .delta
        .added
        .first()
        .expect("initial protected row");
    let initial_values = decode_row(&initial_update.descriptor, &initial_row.data).unwrap();
    assert_eq!(
        initial_values,
        vec![
            Value::Text("initial".into()),
            Value::Boolean(true),
            Value::Boolean(false),
            Value::Boolean(false),
        ]
    );

    qm.insert(&mut storage, "admins", &[Value::Text("alice".into())])
        .expect("grant alice admin");

    qm.process(&mut storage);
    let dependency_update = qm
        .take_updates()
        .into_iter()
        .find(|update| update.subscription_id == sub_id)
        .expect("magic column dependency update");
    let (_old_row, new_row) = dependency_update
        .delta
        .updated
        .first()
        .expect("magic columns should re-evaluate existing row");
    let updated_values = decode_row(&dependency_update.descriptor, &new_row.data).unwrap();
    assert_eq!(
        updated_values,
        vec![
            Value::Text("initial".into()),
            Value::Boolean(true),
            Value::Boolean(true),
            Value::Boolean(true),
        ]
    );

    qm.update_with_session(
        &mut storage,
        protected.row_id,
        &[Value::Text("updated".into())],
        Some(&Session::new("alice")),
    )
    .expect("magic $canEdit should match actual update permission");
    qm.delete_with_session(&mut storage, protected.row_id, Some(&Session::new("alice")))
        .expect("magic $canDelete should match actual delete permission");
}

#[test]
fn magic_columns_return_null_without_session_and_do_not_change_default_output_shape() {
    let schema = magic_introspection_schema();
    let sync_manager = SyncManager::new();
    let mut qm = create_query_manager(sync_manager, schema);
    let mut storage = seeded_memory_storage(&qm.schema_context().current_schema);

    qm.insert(&mut storage, "protected", &[Value::Text("initial".into())])
        .expect("seed protected row");
    qm.insert(&mut storage, "admins", &[Value::Text("alice".into())])
        .expect("grant alice admin");

    let projected_query = qm
        .query("protected")
        .select(&["data", "$canRead", "$canEdit", "$canDelete"])
        .build();
    let projected_sub = qm
        .subscribe_with_session(projected_query, None, None)
        .expect("subscribe without session");

    qm.process(&mut storage);
    let projected_update = qm
        .take_updates()
        .into_iter()
        .find(|update| update.subscription_id == projected_sub)
        .expect("initial projected update");
    let projected_row = projected_update
        .delta
        .added
        .first()
        .expect("projected protected row");
    let projected_values = decode_row(&projected_update.descriptor, &projected_row.data).unwrap();
    assert_eq!(
        projected_values,
        vec![
            Value::Text("initial".into()),
            Value::Null,
            Value::Null,
            Value::Null
        ]
    );

    let filtered_query = qm
        .query("protected")
        .filter_eq("$canDelete", Value::Boolean(true))
        .build();
    let filtered_sub = qm
        .subscribe_with_session(filtered_query, Some(Session::new("alice")), None)
        .expect("subscribe filtered query");

    qm.process(&mut storage);
    let filtered_update = qm
        .take_updates()
        .into_iter()
        .find(|update| update.subscription_id == filtered_sub)
        .expect("filtered update");
    assert_eq!(filtered_update.descriptor.columns.len(), 1);
    assert_eq!(filtered_update.descriptor.columns[0].name.as_str(), "data");

    let filtered_row = filtered_update
        .delta
        .added
        .first()
        .expect("filtered protected row");
    let filtered_values = decode_row(&filtered_update.descriptor, &filtered_row.data).unwrap();
    assert_eq!(filtered_values, vec![Value::Text("initial".into())]);
}

#[test]
fn provenance_magic_columns_capture_insert_update_and_system_authors() {
    let sync_manager = SyncManager::new();
    let schema = provenance_notes_schema();
    let mut qm = create_query_manager(sync_manager, schema);
    let mut storage = seeded_memory_storage(&qm.schema_context().current_schema);

    let alice_session = Session::new("alice");
    let bob_attribution = WriteContext {
        session: None,
        attribution: Some("bob".into()),
        updated_at: None,
        batch_mode: None,
        batch_id: None,
        target_branch_name: None,
    };

    let note = qm
        .insert_with_session(
            &mut storage,
            "notes",
            &[Value::Text("draft".into())],
            Some(&alice_session),
        )
        .expect("alice-authored note should insert");

    let initial = query_rows(
        &mut qm,
        &mut storage,
        QueryBuilder::new("notes")
            .filter_eq("title", Value::Text("draft".into()))
            .select(&[
                "title",
                "$createdBy",
                "$updatedBy",
                "$createdAt",
                "$updatedAt",
            ])
            .build(),
        None,
    );
    assert_eq!(initial.len(), 1, "draft note should be queryable");
    assert_eq!(
        initial[0].1[0],
        Value::Text("draft".into()),
        "projected title should decode"
    );
    assert_eq!(initial[0].1[1], Value::Text("alice".into()));
    assert_eq!(initial[0].1[2], Value::Text("alice".into()));
    let Value::Timestamp(initial_created_at) = initial[0].1[3] else {
        panic!("$createdAt should decode as a timestamp")
    };
    let Value::Timestamp(initial_updated_at) = initial[0].1[4] else {
        panic!("$updatedAt should decode as a timestamp")
    };
    assert_eq!(
        initial_created_at, initial_updated_at,
        "fresh inserts should initialize created/updated timestamps together"
    );

    qm.update_with_write_context(
        &mut storage,
        note.row_id,
        &[Value::Text("revised".into())],
        Some(&bob_attribution),
    )
    .expect("attributed update should succeed without a session");

    let updated = query_rows(
        &mut qm,
        &mut storage,
        QueryBuilder::new("notes")
            .filter_eq("title", Value::Text("revised".into()))
            .select(&[
                "title",
                "$createdBy",
                "$updatedBy",
                "$createdAt",
                "$updatedAt",
            ])
            .build(),
        None,
    );
    assert_eq!(updated.len(), 1, "updated note should remain queryable");
    assert_eq!(updated[0].1[0], Value::Text("revised".into()));
    assert_eq!(updated[0].1[1], Value::Text("alice".into()));
    assert_eq!(updated[0].1[2], Value::Text("bob".into()));
    let Value::Timestamp(updated_created_at) = updated[0].1[3] else {
        panic!("updated $createdAt should decode as a timestamp")
    };
    let Value::Timestamp(updated_updated_at) = updated[0].1[4] else {
        panic!("updated $updatedAt should decode as a timestamp")
    };
    assert_eq!(
        updated_created_at, initial_created_at,
        "created_at should be preserved across updates"
    );
    assert!(
        updated_updated_at >= initial_updated_at,
        "updated_at should move forward on update"
    );

    let updated_by_bob = query_rows(
        &mut qm,
        &mut storage,
        QueryBuilder::new("notes")
            .filter_eq("$updatedBy", Value::Text("bob".into()))
            .select(&["title", "$updatedBy"])
            .build(),
        None,
    );
    assert_eq!(updated_by_bob.len(), 1);
    assert_eq!(
        updated_by_bob[0].1,
        vec![Value::Text("revised".into()), Value::Text("bob".into())]
    );

    qm.insert(&mut storage, "notes", &[Value::Text("system note".into())])
        .expect("system-authored note should insert without a session");
    let system = query_rows(
        &mut qm,
        &mut storage,
        QueryBuilder::new("notes")
            .filter_eq("title", Value::Text("system note".into()))
            .select(&["title", "$createdBy", "$updatedBy"])
            .build(),
        None,
    );
    assert_eq!(system.len(), 1);
    assert_eq!(
        system[0].1,
        vec![
            Value::Text("system note".into()),
            Value::Text(SYSTEM_PRINCIPAL_ID.into()),
            Value::Text(SYSTEM_PRINCIPAL_ID.into()),
        ]
    );
}

#[test]
fn provenance_magic_columns_allow_explicit_updated_at_override() {
    let sync_manager = SyncManager::new();
    let schema = provenance_notes_schema();
    let mut qm = create_query_manager(sync_manager, schema);
    let mut storage = seeded_memory_storage(&qm.schema_context().current_schema);

    let alice_session = Session::new("alice");
    let note = qm
        .insert_with_session(
            &mut storage,
            "notes",
            &[Value::Text("draft".into())],
            Some(&alice_session),
        )
        .expect("alice-authored note should insert");

    let initial = query_rows(
        &mut qm,
        &mut storage,
        QueryBuilder::new("notes")
            .filter_eq("title", Value::Text("draft".into()))
            .select(&["$createdAt", "$updatedAt"])
            .build(),
        None,
    );
    assert_eq!(initial.len(), 1, "draft note should be queryable");
    let Value::Timestamp(initial_created_at) = initial[0].1[0] else {
        panic!("$createdAt should decode as a timestamp")
    };

    let custom_updated_at = initial_created_at + 10_000;
    let bob_backfill = WriteContext {
        session: None,
        attribution: Some("bob".into()),
        updated_at: Some(custom_updated_at),
        batch_mode: None,
        batch_id: None,
        target_branch_name: None,
    };

    qm.update_with_write_context(
        &mut storage,
        note.row_id,
        &[Value::Text("backfilled".into())],
        Some(&bob_backfill),
    )
    .expect("explicit updated_at override should succeed");

    let updated = query_rows(
        &mut qm,
        &mut storage,
        QueryBuilder::new("notes")
            .filter_eq("title", Value::Text("backfilled".into()))
            .select(&[
                "title",
                "$createdBy",
                "$updatedBy",
                "$createdAt",
                "$updatedAt",
            ])
            .build(),
        None,
    );
    assert_eq!(updated.len(), 1, "backfilled note should remain queryable");
    assert_eq!(updated[0].1[0], Value::Text("backfilled".into()));
    assert_eq!(updated[0].1[1], Value::Text("alice".into()));
    assert_eq!(updated[0].1[2], Value::Text("bob".into()));
    let Value::Timestamp(updated_created_at) = updated[0].1[3] else {
        panic!("updated $createdAt should decode as a timestamp")
    };
    let Value::Timestamp(updated_updated_at) = updated[0].1[4] else {
        panic!("updated $updatedAt should decode as a timestamp")
    };
    assert_eq!(updated_created_at, initial_created_at);
    assert_eq!(updated_updated_at, custom_updated_at);
}

#[test]
fn created_by_permissions_allow_creators_and_hide_system_rows() {
    let sync_manager = SyncManager::new();
    let schema = authorship_permissions_schema();
    let mut qm = create_query_manager(sync_manager, schema);
    let mut storage = seeded_memory_storage(&qm.schema_context().current_schema);

    let alice_session = Session::new("alice");
    let bob_session = Session::new("bob");
    let alice_attribution = WriteContext {
        session: None,
        attribution: Some("alice".into()),
        updated_at: None,
        batch_mode: None,
        batch_id: None,
        target_branch_name: None,
    };

    let alice_owned = qm
        .insert_with_session(
            &mut storage,
            "notes",
            &[Value::Text("alice-owned".into())],
            Some(&alice_session),
        )
        .expect("creator-based insert policy should allow alice");
    let alice_attributed = qm
        .insert_with_write_context(
            &mut storage,
            "notes",
            &[Value::Text("alice-attributed".into())],
            Some(&alice_attribution),
        )
        .expect("backend-attributed note should stamp alice as creator");
    qm.insert(&mut storage, "notes", &[Value::Text("system-owned".into())])
        .expect("system note should insert");

    let alice_visible = query_rows(
        &mut qm,
        &mut storage,
        QueryBuilder::new("notes")
            .select(&["title", "$createdBy"])
            .order_by("title")
            .build(),
        Some(alice_session.clone()),
    );
    assert_eq!(
        alice_visible
            .iter()
            .map(|(_, values)| values.clone())
            .collect::<Vec<_>>(),
        vec![
            vec![
                Value::Text("alice-attributed".into()),
                Value::Text("alice".into()),
            ],
            vec![
                Value::Text("alice-owned".into()),
                Value::Text("alice".into())
            ],
        ],
        "alice should only see notes authored as alice"
    );

    let bob_visible = query_rows(
        &mut qm,
        &mut storage,
        QueryBuilder::new("notes").select(&["title"]).build(),
        Some(bob_session.clone()),
    );
    assert!(
        bob_visible.is_empty(),
        "bob should not see alice/system notes"
    );

    let bob_update_err = qm
        .update_with_session(
            &mut storage,
            alice_owned.row_id,
            &[Value::Text("bob edit".into())],
            Some(&bob_session),
        )
        .expect_err("non-creator update should be denied");
    assert!(matches!(
        bob_update_err,
        QueryError::PolicyDenied {
            table,
            operation: Operation::Update
        } if table == TableName::new("notes")
    ));

    let bob_delete_err = qm
        .delete_with_session(&mut storage, alice_owned.row_id, Some(&bob_session))
        .expect_err("non-creator delete should be denied");
    assert!(matches!(
        bob_delete_err,
        QueryError::PolicyDenied {
            table,
            operation: Operation::Delete
        } if table == TableName::new("notes")
    ));

    qm.update_with_session(
        &mut storage,
        alice_attributed.row_id,
        &[Value::Text("alice-attributed-updated".into())],
        Some(&alice_session),
    )
    .expect("creator should be able to update attributed rows");
    qm.delete_with_session(&mut storage, alice_owned.row_id, Some(&alice_session))
        .expect("creator should be able to delete her own row");

    let alice_after_mutations = query_rows(
        &mut qm,
        &mut storage,
        QueryBuilder::new("notes")
            .select(&["title"])
            .order_by("title")
            .build(),
        Some(alice_session),
    );
    assert_eq!(
        alice_after_mutations
            .iter()
            .map(|(_, values)| values[0].clone())
            .collect::<Vec<_>>(),
        vec![Value::Text("alice-attributed-updated".into())],
        "alice should retain access to the surviving creator-owned row"
    );
}
