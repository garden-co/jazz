use super::*;

#[test]
fn local_insert_with_exists_policy_propagates_enforcing_mode_to_nested_exists_rel() {
    let projects_policies = permissions(|p| {
        p.allow_insert()
            .where_(pe::exists(pe::table("admins").where_(pe::all_of([
                pe::eq("user_id", pe::session("user_id")),
                pe::exists(pe::table("team_memberships").where_(pe::rel::all_of([
                    pe::rel::eq_outer("team_id", "team_id"),
                    pe::rel::eq_session("user_id", "user_id"),
                ]))),
            ]))));
    });
    let schema = SchemaBuilder::new()
        .table(
            TableSchema::builder("admins")
                .column("user_id", ColumnType::Text)
                .column("team_id", ColumnType::Text),
        )
        .table(
            TableSchema::builder("team_memberships")
                .column("team_id", ColumnType::Text)
                .column("user_id", ColumnType::Text),
        )
        .table(
            TableSchema::builder("projects")
                .column("name", ColumnType::Text)
                .policies(projects_policies),
        )
        .build();

    let sync_manager = SyncManager::new();
    let mut qm = create_query_manager(sync_manager, schema);
    let mut storage = seeded_memory_storage(&qm.schema_context().current_schema);

    qm.insert(
        &mut storage,
        "admins",
        &[Value::Text("alice".into()), Value::Text("team-a".into())],
    )
    .expect("seed admin row");
    qm.insert(
        &mut storage,
        "team_memberships",
        &[Value::Text("team-a".into()), Value::Text("alice".into())],
    )
    .expect("seed membership row");

    let err = qm
        .insert_with_session(
            &mut storage,
            "projects",
            &[Value::Text("alice project".into())],
            Some(&Session::new("alice")),
        )
        .expect_err(
            "enforcing mode should deny nested EXISTS_REL checks when the probed table lacks an explicit SELECT policy",
        );
    assert!(matches!(
        err,
        QueryError::PolicyDenied {
            table,
            operation: Operation::Insert
        } if table == TableName::new("projects")
    ));
}

#[test]
fn local_insert_with_exists_rel_policy_denies_non_admin() {
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

    let sync_manager = SyncManager::new();
    let mut qm = create_query_manager(sync_manager, schema);
    let mut storage = seeded_memory_storage(&qm.schema_context().current_schema);

    qm.insert(&mut storage, "admins", &[Value::Text("alice".into())])
        .expect("seed admin row");

    let bob_err = qm
        .insert_with_session(
            &mut storage,
            "projects",
            &[Value::Text("bob project".into())],
            Some(&Session::new("bob")),
        )
        .expect_err("non-admin insert should be denied");
    assert!(matches!(
        bob_err,
        QueryError::PolicyDenied {
            table,
            operation: Operation::Insert
        } if table == TableName::new("projects")
    ));

    qm.insert_with_session(
        &mut storage,
        "projects",
        &[Value::Text("alice project".into())],
        Some(&Session::new("alice")),
    )
    .expect("admin insert should be allowed");
}

#[test]
fn local_insert_with_exists_rel_policy_requires_explicit_select_on_scanned_table() {
    let projects_policies = permissions(|p| {
        p.allow_insert().where_(pe::exists(
            pe::table("admins").where_(pe::rel::eq_session("user_id", "user_id")),
        ));
    });
    let schema = SchemaBuilder::new()
        .table(TableSchema::builder("admins").column("user_id", ColumnType::Text))
        .table(
            TableSchema::builder("projects")
                .column("name", ColumnType::Text)
                .policies(projects_policies),
        )
        .build();

    let sync_manager = SyncManager::new();
    let mut qm = create_query_manager(sync_manager, schema);
    let mut storage = seeded_memory_storage(&qm.schema_context().current_schema);

    qm.insert(&mut storage, "admins", &[Value::Text("alice".into())])
        .expect("seed admin row");

    let err = qm
        .insert_with_session(
            &mut storage,
            "projects",
            &[Value::Text("alice project".into())],
            Some(&Session::new("alice")),
        )
        .expect_err(
            "enforcing mode should deny EXISTS_REL scans when the scanned table lacks an explicit SELECT policy",
        );
    assert!(matches!(
        err,
        QueryError::PolicyDenied {
            table,
            operation: Operation::Insert
        } if table == TableName::new("projects")
    ));
}

#[test]
fn local_insert_with_exists_rel_null_literal_predicate_matches_null_rows() {
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

    let sync_manager = SyncManager::new();
    let mut qm = create_query_manager(sync_manager, schema);
    let mut storage = seeded_memory_storage(&qm.schema_context().current_schema);

    qm.insert(
        &mut storage,
        "admins",
        &[Value::Text("alice".into()), Value::Null],
    )
    .expect("seed active admin row");
    qm.insert(
        &mut storage,
        "admins",
        &[
            Value::Text("carol".into()),
            Value::Text("2026-03-30T12:00:00Z".into()),
        ],
    )
    .expect("seed revoked admin row");

    qm.insert_with_session(
        &mut storage,
        "projects",
        &[Value::Text("alice project".into())],
        Some(&Session::new("alice")),
    )
    .expect("active admin row should satisfy revoked_at = NULL predicate");

    let carol_err = qm
        .insert_with_session(
            &mut storage,
            "projects",
            &[Value::Text("carol project".into())],
            Some(&Session::new("carol")),
        )
        .expect_err("revoked admin row should fail revoked_at = NULL predicate");
    assert!(matches!(
        carol_err,
        QueryError::PolicyDenied {
            table,
            operation: Operation::Insert
        } if table == TableName::new("projects")
    ));
}

#[test]
fn local_delete_with_exists_rel_policy_allows_admin_and_denies_non_admin() {
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
        .table(
            TableSchema::builder("protected")
                .column("data", ColumnType::Text)
                .policies(protected_policies),
        )
        .build();

    let sync_manager = SyncManager::new();
    let mut qm = create_query_manager(sync_manager, schema);
    let mut storage = seeded_memory_storage(&qm.schema_context().current_schema);

    qm.insert(&mut storage, "admins", &[Value::Text("alice".into())])
        .expect("seed admin row");
    let protected = qm
        .insert(&mut storage, "protected", &[Value::Text("initial".into())])
        .expect("seed protected row");

    let bob_err = qm
        .delete_with_session(&mut storage, protected.row_id, Some(&Session::new("bob")))
        .expect_err("non-admin delete should be denied");
    assert!(matches!(
        bob_err,
        QueryError::PolicyDenied {
            table,
            operation: Operation::Delete
        } if table == TableName::new("protected")
    ));

    qm.delete_with_session(&mut storage, protected.row_id, Some(&Session::new("alice")))
        .expect("admin delete should be allowed");
    assert!(qm.row_is_deleted(&storage, "protected", protected.row_id));
}
