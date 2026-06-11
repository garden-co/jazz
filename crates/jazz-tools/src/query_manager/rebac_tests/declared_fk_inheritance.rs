use super::*;

#[test]
fn rebac_declared_fk_inheritance_grants_select_access() {
    use crate::query_manager::query::QueryBuilder;

    let schema = declared_file_inheritance_schema(false);
    let sync_manager = SyncManager::new();
    let mut qm = create_query_manager(sync_manager, schema);
    let mut storage = seeded_memory_storage(&qm.schema_context().current_schema);

    let file_id = qm
        .insert(
            &mut storage,
            "files",
            &[Value::Text("bob".into()), Value::Text("bob-file".into())],
        )
        .unwrap()
        .row_id;
    let _todo_id = qm
        .insert(
            &mut storage,
            "todos",
            &[
                Value::Text("alice".into()),
                Value::Text("todo".into()),
                Value::Uuid(file_id),
            ],
        )
        .unwrap()
        .row_id;

    let sub_id = qm
        .subscribe_with_session(
            QueryBuilder::new("files").build(),
            Some(Session::new("alice")),
            None,
        )
        .unwrap();

    for _ in 0..10 {
        qm.process(&mut storage);
    }

    let visible_ids: HashSet<_> = qm
        .get_subscription_results(sub_id)
        .into_iter()
        .map(|(id, _)| id)
        .collect();
    assert!(
        visible_ids.contains(&file_id),
        "alice should see file via allowedTo.readReferencing(policy.todos, \"image\")"
    );
}

#[test]
fn rebac_declared_fk_inheritance_grants_update_access() {
    let schema = declared_file_inheritance_schema(false);
    let sync_manager = SyncManager::new();
    let mut qm = create_query_manager(sync_manager, schema);
    let mut storage = seeded_memory_storage(&qm.schema_context().current_schema);

    let file_id = qm
        .insert(
            &mut storage,
            "files",
            &[Value::Text("bob".into()), Value::Text("bob-file".into())],
        )
        .unwrap()
        .row_id;
    let _todo_id = qm
        .insert(
            &mut storage,
            "todos",
            &[
                Value::Text("alice".into()),
                Value::Text("todo".into()),
                Value::Uuid(file_id),
            ],
        )
        .unwrap()
        .row_id;

    let update = qm.update_with_session(
        &mut storage,
        file_id,
        &[
            Value::Text("bob".into()),
            Value::Text("updated by alice".into()),
        ],
        Some(&Session::new("alice")),
    );
    assert!(
        update.is_ok(),
        "alice should update file via declared inherited access from todos row"
    );
}

#[test]
fn rebac_declared_fk_inheritance_array_membership_grants_access() {
    use crate::query_manager::query::QueryBuilder;

    let schema = declared_file_inheritance_schema(true);
    let sync_manager = SyncManager::new();
    let mut qm = create_query_manager(sync_manager, schema);
    let mut storage = seeded_memory_storage(&qm.schema_context().current_schema);

    let file_id = qm
        .insert(
            &mut storage,
            "files",
            &[Value::Text("bob".into()), Value::Text("array-file".into())],
        )
        .unwrap()
        .row_id;
    let _todo_id = qm
        .insert(
            &mut storage,
            "todos",
            &[
                Value::Text("alice".into()),
                Value::Text("todo".into()),
                Value::Array(vec![Value::Uuid(file_id), Value::Uuid(file_id)]),
            ],
        )
        .unwrap()
        .row_id;

    let sub_id = qm
        .subscribe_with_session(
            QueryBuilder::new("files").build(),
            Some(Session::new("alice")),
            None,
        )
        .unwrap();

    for _ in 0..10 {
        qm.process(&mut storage);
    }

    let visible_ids: HashSet<_> = qm
        .get_subscription_results(sub_id)
        .into_iter()
        .map(|(id, _)| id)
        .collect();
    assert!(
        visible_ids.contains(&file_id),
        "array FK membership should grant inherited access when target id is present"
    );
}

#[test]
fn rebac_declared_fk_inheritance_cycle_fails_closed() {
    use crate::query_manager::query::QueryBuilder;

    let a_policies = permissions(|p| {
        p.allow_read().where_(pe::any_of([
            pe::eq("owner_id", pe::session("user_id")),
            pe::allowed_to_read_referencing("table_b", "a_id"),
        ]));
    });
    let b_policies = permissions(|p| {
        p.allow_read().where_(pe::any_of([
            pe::eq("owner_id", pe::session("user_id")),
            pe::allowed_to_read_referencing("table_a", "b_id"),
        ]));
    });
    let schema = SchemaBuilder::new()
        .table(
            TableSchema::builder("table_a")
                .column("owner_id", ColumnType::Text)
                .nullable_fk_column("b_id", "table_b")
                .policies(a_policies),
        )
        .table(
            TableSchema::builder("table_b")
                .column("owner_id", ColumnType::Text)
                .nullable_fk_column("a_id", "table_a")
                .policies(b_policies),
        )
        .build();

    let sync_manager = SyncManager::new();
    let mut qm = create_query_manager(sync_manager, schema);
    let mut storage = seeded_memory_storage(&qm.schema_context().current_schema);

    let a_id = qm
        .insert(
            &mut storage,
            "table_a",
            &[Value::Text("bob".into()), Value::Null],
        )
        .unwrap()
        .row_id;
    let b_id = qm
        .insert(
            &mut storage,
            "table_b",
            &[Value::Text("carol".into()), Value::Uuid(a_id)],
        )
        .unwrap()
        .row_id;

    qm.update(
        &mut storage,
        a_id,
        &[Value::Text("bob".into()), Value::Uuid(b_id)],
    )
    .unwrap();

    let sub_id = qm
        .subscribe_with_session(
            QueryBuilder::new("table_a").build(),
            Some(Session::new("alice")),
            None,
        )
        .unwrap();

    for _ in 0..10 {
        qm.process(&mut storage);
    }

    let visible_ids: HashSet<_> = qm
        .get_subscription_results(sub_id)
        .into_iter()
        .map(|(id, _)| id)
        .collect();
    assert!(
        visible_ids.is_empty(),
        "cycle path should fail closed and not grant access"
    );
}

#[test]
fn rebac_declared_fk_inheritance_reacts_to_fk_updates() {
    use crate::query_manager::query::QueryBuilder;

    let schema = declared_file_inheritance_schema(false);
    let sync_manager = SyncManager::new();
    let mut qm = create_query_manager(sync_manager, schema);
    let mut storage = seeded_memory_storage(&qm.schema_context().current_schema);

    let file_id = qm
        .insert(
            &mut storage,
            "files",
            &[
                Value::Text("bob".into()),
                Value::Text("delayed-link".into()),
            ],
        )
        .unwrap()
        .row_id;
    let todo_id = qm
        .insert(
            &mut storage,
            "todos",
            &[
                Value::Text("alice".into()),
                Value::Text("todo".into()),
                Value::Null,
            ],
        )
        .unwrap()
        .row_id;

    let sub_id = qm
        .subscribe_with_session(
            QueryBuilder::new("files").build(),
            Some(Session::new("alice")),
            None,
        )
        .unwrap();

    for _ in 0..10 {
        qm.process(&mut storage);
    }
    let initially_visible: HashSet<_> = qm
        .get_subscription_results(sub_id)
        .into_iter()
        .map(|(id, _)| id)
        .collect();
    assert!(
        !initially_visible.contains(&file_id),
        "file should be hidden before an inheriting reference exists"
    );

    qm.update(
        &mut storage,
        todo_id,
        &[
            Value::Text("alice".into()),
            Value::Text("todo".into()),
            Value::Uuid(file_id),
        ],
    )
    .unwrap();

    for _ in 0..10 {
        qm.process(&mut storage);
    }
    let visible_after_link: HashSet<_> = qm
        .get_subscription_results(sub_id)
        .into_iter()
        .map(|(id, _)| id)
        .collect();
    assert!(
        visible_after_link.contains(&file_id),
        "updating referencing FK should re-evaluate and grant access to linked target row"
    );
}
