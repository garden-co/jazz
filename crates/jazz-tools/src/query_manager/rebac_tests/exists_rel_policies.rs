use super::*;

#[test]
fn local_insert_with_exists_policy_propagates_enforcing_mode_to_nested_exists_rel() {
    let mut schema = Schema::new();
    schema.insert(
        TableName::new("admins"),
        TableSchema::new(RowDescriptor::new(vec![
            ColumnDescriptor::new("user_id", ColumnType::Text),
            ColumnDescriptor::new("team_id", ColumnType::Text),
        ])),
    );
    schema.insert(
        TableName::new("team_memberships"),
        TableSchema::new(RowDescriptor::new(vec![
            ColumnDescriptor::new("team_id", ColumnType::Text),
            ColumnDescriptor::new("user_id", ColumnType::Text),
        ])),
    );

    let projects_descriptor =
        RowDescriptor::new(vec![ColumnDescriptor::new("name", ColumnType::Text)]);
    let projects_policies = TablePolicies::new().with_insert(PolicyExpr::Exists {
        table: "admins".into(),
        condition: Box::new(PolicyExpr::And(vec![
            PolicyExpr::eq_session("user_id", vec!["user_id".into()]),
            PolicyExpr::ExistsRel {
                rel: RelExpr::Filter {
                    input: Box::new(RelExpr::TableScan {
                        table: TableName::new("team_memberships"),
                    }),
                    predicate: PredicateExpr::And(vec![
                        PredicateExpr::Cmp {
                            left: ColumnRef::unscoped("team_id"),
                            op: PredicateCmpOp::Eq,
                            right: ValueRef::OuterColumn(ColumnRef::unscoped("team_id")),
                        },
                        PredicateExpr::Cmp {
                            left: ColumnRef::unscoped("user_id"),
                            op: PredicateCmpOp::Eq,
                            right: ValueRef::SessionRef(vec!["user_id".into()]),
                        },
                    ]),
                },
            },
        ])),
    });
    schema.insert(
        TableName::new("projects"),
        TableSchema::with_policies(projects_descriptor, projects_policies),
    );

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

    let projects_descriptor =
        RowDescriptor::new(vec![ColumnDescriptor::new("name", ColumnType::Text)]);
    let projects_policies = TablePolicies::new().with_insert(PolicyExpr::ExistsRel {
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
        TableName::new("projects"),
        TableSchema::with_policies(projects_descriptor, projects_policies),
    );

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
    let mut schema = Schema::new();
    schema.insert(
        TableName::new("admins"),
        TableSchema::new(RowDescriptor::new(vec![ColumnDescriptor::new(
            "user_id",
            ColumnType::Text,
        )])),
    );

    let projects_descriptor =
        RowDescriptor::new(vec![ColumnDescriptor::new("name", ColumnType::Text)]);
    let projects_policies = TablePolicies::new().with_insert(PolicyExpr::ExistsRel {
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
        TableName::new("projects"),
        TableSchema::with_policies(projects_descriptor, projects_policies),
    );

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
    let mut schema = Schema::new();
    let admins_descriptor = RowDescriptor::new(vec![
        ColumnDescriptor::new("user_id", ColumnType::Text),
        ColumnDescriptor::new("revoked_at", ColumnType::Text).nullable(),
    ]);
    schema.insert(
        TableName::new("admins"),
        TableSchema::with_policies(
            admins_descriptor.clone(),
            TablePolicies::new().with_select(PolicyExpr::True),
        ),
    );

    let projects_descriptor =
        RowDescriptor::new(vec![ColumnDescriptor::new("name", ColumnType::Text)]);
    let projects_policies = TablePolicies::new().with_insert(PolicyExpr::ExistsRel {
        rel: RelExpr::Filter {
            input: Box::new(RelExpr::TableScan {
                table: TableName::new("admins"),
            }),
            predicate: PredicateExpr::And(vec![
                PredicateExpr::Cmp {
                    left: ColumnRef::unscoped("user_id"),
                    op: PredicateCmpOp::Eq,
                    right: ValueRef::SessionRef(vec!["user_id".into()]),
                },
                PredicateExpr::Cmp {
                    left: ColumnRef::unscoped("revoked_at"),
                    op: PredicateCmpOp::Eq,
                    right: ValueRef::Literal(Value::Null),
                },
            ]),
        },
    });
    schema.insert(
        TableName::new("projects"),
        TableSchema::with_policies(projects_descriptor, projects_policies),
    );

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
