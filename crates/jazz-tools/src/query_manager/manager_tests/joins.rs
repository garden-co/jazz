use super::*;

#[test]
fn join_compiles_but_not_executed_yet() {
    // This test validates that join queries compile and don't panic,
    // even though full join execution is not yet implemented.
    // Once execute() supports joins, this test can be extended.
    let sync_manager = SyncManager::new();
    let schema = join_schema();
    let (qm, _storage) = create_query_manager(sync_manager, schema);

    // Build a join query
    let query = qm
        .query("users")
        .join("posts")
        .on("id", "author_id")
        .build();

    // The query should compile successfully
    assert!(query.is_join());
    assert_eq!(query.joins.len(), 1);
}

#[test]
fn join_query_with_projection_compiles() {
    let sync_manager = SyncManager::new();
    let schema = join_schema();
    let (qm, _storage) = create_query_manager(sync_manager, schema);

    let query = qm
        .query("users")
        .join("posts")
        .on("id", "author_id")
        .select(&["name", "title"])
        .build();

    assert!(query.is_join());
    assert_eq!(
        query.select_columns,
        Some(vec!["name".to_string(), "title".to_string()])
    );
}

#[test]
fn join_query_with_alias_compiles() {
    let sync_manager = SyncManager::new();
    let schema = join_schema();
    let (qm, _storage) = create_query_manager(sync_manager, schema);

    let query = qm
        .query("users")
        .alias("u")
        .join("posts")
        .alias("p")
        .on("u.id", "p.author_id")
        .build();

    assert!(query.is_join());
    assert_eq!(query.alias, Some("u".to_string()));
    assert_eq!(query.joins[0].alias, Some("p".to_string()));
}

#[test]
fn self_join_query_compiles() {
    // Self-join: employees with their managers
    let mut schema = Schema::new();
    schema.insert(
        TableName::new("employees"),
        RowDescriptor::new(vec![
            ColumnDescriptor::new("id", ColumnType::Integer),
            ColumnDescriptor::new("name", ColumnType::Text),
            ColumnDescriptor::new("manager_id", ColumnType::Integer).nullable(),
        ])
        .into(),
    );

    let sync_manager = SyncManager::new();
    let (qm, _storage) = create_query_manager(sync_manager, schema);

    let query = qm
        .query("employees")
        .alias("e")
        .join("employees")
        .alias("m")
        .on("e.manager_id", "m.id")
        .build();

    assert!(query.is_join());
    assert_eq!(query.alias, Some("e".to_string()));
    assert_eq!(query.joins[0].table.as_str(), "employees");
    assert_eq!(query.joins[0].alias, Some("m".to_string()));
}

#[test]
fn multi_join_query_compiles() {
    // Three-way join: orders -> customers, orders -> products
    let mut schema = Schema::new();
    schema.insert(
        TableName::new("orders"),
        RowDescriptor::new(vec![
            ColumnDescriptor::new("id", ColumnType::Integer),
            ColumnDescriptor::new("customer_id", ColumnType::Integer),
            ColumnDescriptor::new("product_id", ColumnType::Integer),
        ])
        .into(),
    );
    schema.insert(
        TableName::new("customers"),
        RowDescriptor::new(vec![
            ColumnDescriptor::new("id", ColumnType::Integer),
            ColumnDescriptor::new("name", ColumnType::Text),
        ])
        .into(),
    );
    schema.insert(
        TableName::new("products"),
        RowDescriptor::new(vec![
            ColumnDescriptor::new("id", ColumnType::Integer),
            ColumnDescriptor::new("name", ColumnType::Text),
        ])
        .into(),
    );

    let sync_manager = SyncManager::new();
    let (qm, _storage) = create_query_manager(sync_manager, schema);

    let query = qm
        .query("orders")
        .join("customers")
        .on("customer_id", "id")
        .join("products")
        .on("product_id", "id")
        .build();

    assert!(query.is_join());
    assert_eq!(query.joins.len(), 2);
    assert_eq!(query.joins[0].table.as_str(), "customers");
    assert_eq!(query.joins[1].table.as_str(), "products");
}

#[test]
fn join_without_on_clause_fails_query_build() {
    let sync_manager = SyncManager::new();
    let schema = join_schema();
    let (qm, _storage) = create_query_manager(sync_manager, schema);

    let result = qm.query("users").join("posts").try_build();
    assert!(
        result.is_err(),
        "Join queries without ON should fail at build time"
    );
}

#[test]
fn join_subscription_fails_for_invalid_join_column() {
    let sync_manager = SyncManager::new();
    let schema = join_schema();
    let (mut qm, _storage) = create_query_manager(sync_manager, schema);

    let query = qm
        .query("users")
        .join("posts")
        .on("missing_column", "author_id")
        .build();
    let result = qm.subscribe(query);

    match result {
        Err(QueryError::QueryCompilationError(msg)) => {
            assert_eq!(
                msg,
                "invalid relation plan: unsupported relation_ir shape for schema-context query compilation"
            );
        }
        other => panic!(
            "Join queries with invalid join columns should fail with QueryCompilationError, got {other:?}"
        ),
    }
}

#[test]
fn join_subscription_fails_for_circular_join_chain() {
    let sync_manager = SyncManager::new();
    let schema = join_schema();
    let (mut qm, _storage) = create_query_manager(sync_manager, schema);

    let query = qm
        .query("users")
        .join("posts")
        .on("id", "author_id")
        .join("users")
        .on("author_id", "id")
        .build();
    let result = qm.subscribe(query);

    match result {
        Err(QueryError::QueryCompilationError(msg)) => {
            assert_eq!(
                msg,
                "invalid relation plan: unsupported relation_ir shape for schema-context query compilation"
            );
        }
        other => panic!(
            "Circular/self join chains should fail with QueryCompilationError, got {other:?}"
        ),
    }
}

#[test]
fn join_subscription_marks_dirty_for_joined_table() {
    // Inserts into a JOINED table should trigger observable updates for join subscriptions.
    let sync_manager = SyncManager::new();
    let schema = join_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Seed base table row so a later post insert can produce a join match.
    let user_handle = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Integer(1), Value::Text("Alice".into())],
        )
        .unwrap();

    // Subscribe to a join query: users JOIN posts ON users.id = posts.author_id.
    let query = qm
        .query("users")
        .join("posts")
        .on("id", "author_id")
        .build();
    let sub_id = qm.subscribe(query).unwrap();

    // Process once to settle initial state and clear bootstrap updates.
    qm.process(&mut storage);
    let _ = qm.take_updates();

    // Insert into the JOINED table (posts), not the base table (users).
    qm.insert(
        &mut storage,
        "posts",
        &[
            Value::Integer(100),
            Value::Text("Test Post".into()),
            Value::Integer(1),
        ],
    )
    .unwrap();

    qm.process(&mut storage);
    let updates = qm.take_updates();
    let delta = updates
        .iter()
        .find(|u| u.subscription_id == sub_id)
        .map(|u| &u.delta)
        .expect("Join subscription should emit an update after joined-table insert");
    assert_eq!(
        delta.added.len(),
        1,
        "Joined insert should add one joined row"
    );

    let row = &delta.added[0];
    assert_eq!(
        row.id, user_handle.row_id,
        "Join output should still be keyed by base-table row id"
    );
    assert!(
        row.data
            .windows("Alice".len())
            .any(|window| window == "Alice".as_bytes()),
        "Joined row payload should contain base-table value"
    );
    assert!(
        row.data
            .windows("Test Post".len())
            .any(|window| window == "Test Post".as_bytes()),
        "Joined row payload should contain joined-table value"
    );
}

#[test]
fn join_produces_combined_tuples() {
    // Test that a join produces tuples with elements from both tables.
    // This verifies basic join functionality and tuple structure.
    let sync_manager = SyncManager::new();
    let schema = join_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert a user
    let user_id = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Integer(1), Value::Text("Alice".into())],
        )
        .unwrap();

    // Insert a post by that user
    let post_id = qm
        .insert(
            &mut storage,
            "posts",
            &[
                Value::Integer(100),
                Value::Text("Hello World".into()),
                Value::Integer(1), // author_id matches user id
            ],
        )
        .unwrap();

    // Subscribe to a join query
    let query = qm
        .query("users")
        .join("posts")
        .on("id", "author_id")
        .build();
    let sub_id = qm.subscribe(query).unwrap();

    // Process to get join results
    qm.process(&mut storage);
    let updates = qm.take_updates();
    let delta = updates
        .iter()
        .find(|u| u.subscription_id == sub_id)
        .map(|u| &u.delta)
        .expect("Should have updates for subscription");

    // Should have one joined row
    assert_eq!(delta.added.len(), 1, "Should have one joined result");

    // Validate row identity and ensure payload carries values from both sides of the join.
    let row = &delta.added[0];
    assert_eq!(
        row.id, user_id.row_id,
        "Join output should be keyed by base table row id"
    );
    assert_ne!(
        row.id, post_id.row_id,
        "Join output should not be keyed by joined table row id"
    );
    assert!(
        row.data
            .windows("Alice".len())
            .any(|w| w == "Alice".as_bytes()),
        "Joined row payload should contain base-table text value"
    );
    assert!(
        row.data
            .windows("Hello World".len())
            .any(|w| w == "Hello World".as_bytes()),
        "Joined row payload should contain joined-table text value"
    );
}

#[test]
fn join_filter_on_joined_table_column() {
    // Test filtering on a column from the JOINED table (not the base table).
    let sync_manager = SyncManager::new();
    let schema = join_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert users
    qm.insert(
        &mut storage,
        "users",
        &[Value::Integer(1), Value::Text("Alice".into())],
    )
    .unwrap();
    let bob_handle = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Integer(2), Value::Text("Bob".into())],
        )
        .unwrap();
    qm.insert(
        &mut storage,
        "posts",
        &[
            Value::Integer(100),
            Value::Text("Hello World".into()), // Should NOT match "Rust"
            Value::Integer(1),
        ],
    )
    .unwrap();
    qm.insert(
        &mut storage,
        "posts",
        &[
            Value::Integer(101),
            Value::Text("Learning Rust".into()), // SHOULD match filter
            Value::Integer(2),
        ],
    )
    .unwrap();

    // Join with filter on posts.title
    let query = qm
        .query("users")
        .join("posts")
        .on("id", "author_id")
        // This filter should match only the joined post title.
        .filter_eq("title", Value::Text("Learning Rust".into()))
        .build();

    let sub_id = qm.subscribe(query).unwrap();
    qm.process(&mut storage);
    let updates = qm.take_updates();
    let delta = updates
        .iter()
        .find(|u| u.subscription_id == sub_id)
        .map(|u| &u.delta)
        .expect("Should have updates");

    // Should only have one result (the "Learning Rust" post)
    assert_eq!(
        delta.added.len(),
        1,
        "Filter on joined table column should work"
    );
    let row = &delta.added[0];
    assert_eq!(
        row.id, bob_handle.row_id,
        "Only Bob's joined row should match Learning Rust"
    );
    assert!(
        row.data
            .windows("Bob".len())
            .any(|window| window == "Bob".as_bytes()),
        "Joined row should include Bob's name"
    );
    assert!(
        row.data
            .windows("Learning Rust".len())
            .any(|window| window == "Learning Rust".as_bytes()),
        "Joined row should include the matching post title"
    );
}

#[test]
fn join_filter_on_scoped_alias_columns() {
    let sync_manager = SyncManager::new();
    let schema = join_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    qm.insert(
        &mut storage,
        "users",
        &[Value::Integer(1), Value::Text("Alice".into())],
    )
    .unwrap();
    let bob_handle = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Integer(2), Value::Text("Bob".into())],
        )
        .unwrap();
    qm.insert(
        &mut storage,
        "posts",
        &[
            Value::Integer(100),
            Value::Text("Hello World".into()),
            Value::Integer(1),
        ],
    )
    .unwrap();
    qm.insert(
        &mut storage,
        "posts",
        &[
            Value::Integer(101),
            Value::Text("Learning Rust".into()),
            Value::Integer(2),
        ],
    )
    .unwrap();

    let query = qm
        .query("users")
        .alias("u")
        .join("posts")
        .alias("p")
        .on("u.id", "p.author_id")
        .filter_eq("u.name", Value::Text("Bob".into()))
        .filter_eq("p.title", Value::Text("Learning Rust".into()))
        .build();

    let sub_id = qm.subscribe(query).unwrap();
    qm.process(&mut storage);
    let updates = qm.take_updates();
    let delta = updates
        .iter()
        .find(|u| u.subscription_id == sub_id)
        .map(|u| &u.delta)
        .expect("Should have updates");

    assert_eq!(
        delta.added.len(),
        1,
        "Scoped alias filters should resolve against the correct joined columns"
    );
    let row = &delta.added[0];
    assert_eq!(row.id, bob_handle.row_id);
    assert!(
        row.data
            .windows("Bob".len())
            .any(|window| window == "Bob".as_bytes()),
        "Joined row should include the filtered base alias value"
    );
    assert!(
        row.data
            .windows("Learning Rust".len())
            .any(|window| window == "Learning Rust".as_bytes()),
        "Joined row should include the filtered joined alias value"
    );
}

#[test]
fn join_subscription_can_project_joined_element_output() {
    let sync_manager = SyncManager::new();
    let schema = join_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    qm.insert(
        &mut storage,
        "users",
        &[Value::Integer(1), Value::Text("Alice".into())],
    )
    .unwrap();
    let post = qm
        .insert(
            &mut storage,
            "posts",
            &[
                Value::Integer(100),
                Value::Text("Hello World".into()),
                Value::Integer(1),
            ],
        )
        .unwrap();

    let query = qm
        .query("users")
        .join("posts")
        .on("id", "author_id")
        .result_element_index(1)
        .build();

    let sub_id = qm.subscribe(query).unwrap();
    qm.process(&mut storage);
    let updates = qm.take_updates();
    let delta = updates
        .iter()
        .find(|u| u.subscription_id == sub_id)
        .map(|u| &u.delta)
        .expect("Expected projected join update");

    assert_eq!(delta.added.len(), 1);
    let row = &delta.added[0];
    assert_eq!(
        row.id, post.row_id,
        "Projected join output should be keyed by joined row id"
    );
    assert!(
        row.data
            .windows("Hello World".len())
            .any(|window| window == "Hello World".as_bytes()),
        "Projected join output should contain joined-table payload"
    );
}

#[test]
fn join_subscription_can_execute_precise_relation_ir_projection() {
    use crate::query_manager::relation_ir::{
        ColumnRef, JoinCondition, JoinKind, ProjectColumn, ProjectExpr, RelExpr,
    };

    let sync_manager = SyncManager::new();
    let schema = join_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    let user = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Integer(1), Value::Text("Alice".into())],
        )
        .unwrap();
    qm.insert(
        &mut storage,
        "posts",
        &[
            Value::Integer(100),
            Value::Text("Hello World".into()),
            Value::Integer(1),
        ],
    )
    .unwrap();

    let mut query = qm
        .query("users")
        .join("posts")
        .on("users.id", "posts.author_id")
        .build();
    query.relation_ir = RelExpr::Project {
        input: Box::new(RelExpr::Join {
            left: Box::new(RelExpr::TableScan {
                table: TableName::new("users"),
            }),
            right: Box::new(RelExpr::TableScan {
                table: TableName::new("posts"),
            }),
            on: vec![JoinCondition {
                left: ColumnRef::scoped("users", "id"),
                right: ColumnRef::scoped("posts", "author_id"),
            }],
            join_kind: JoinKind::Inner,
        }),
        columns: vec![
            ProjectColumn {
                alias: "author_name".into(),
                expr: ProjectExpr::Column(ColumnRef::scoped("users", "name")),
            },
            ProjectColumn {
                alias: "post_title".into(),
                expr: ProjectExpr::Column(ColumnRef::scoped("posts", "title")),
            },
        ],
    };
    query.select_columns = None;

    let sub_id = qm.subscribe(query).unwrap();
    qm.process(&mut storage);
    let updates = qm.take_updates();
    let update = updates
        .iter()
        .find(|u| u.subscription_id == sub_id)
        .expect("Expected precise projection update");

    assert_eq!(update.delta.added.len(), 1);
    assert_eq!(update.descriptor.columns.len(), 2);
    assert_eq!(update.descriptor.columns[0].name, "author_name");
    assert_eq!(update.descriptor.columns[1].name, "post_title");

    let row = &update.delta.added[0];
    assert_eq!(row.id, user.row_id);
    let values =
        decode_row(&update.descriptor, &row.data).expect("should decode precise projection");
    assert_eq!(
        values,
        vec![
            Value::Text("Alice".into()),
            Value::Text("Hello World".into())
        ]
    );
}

#[test]
fn join_subscription_precise_relation_ir_full_joined_element_preserves_implicit_id_row_shape() {
    use crate::query_manager::relation_ir::{
        ColumnRef, JoinCondition, JoinKind, ProjectColumn, ProjectExpr, RelExpr,
    };

    let sync_manager = SyncManager::new();
    let schema = join_schema_with_implicit_base_id();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    let alice = qm
        .insert(&mut storage, "users", &[Value::Text("Alice".into())])
        .unwrap();
    let post = qm
        .insert(
            &mut storage,
            "posts",
            &[Value::Text("Hello World".into()), Value::Uuid(alice.row_id)],
        )
        .unwrap();

    let mut query = qm
        .query("users")
        .join("posts")
        .on("users.id", "posts.author_id")
        .build();
    query.relation_ir = RelExpr::Project {
        input: Box::new(RelExpr::Join {
            left: Box::new(RelExpr::TableScan {
                table: TableName::new("users"),
            }),
            right: Box::new(RelExpr::TableScan {
                table: TableName::new("posts"),
            }),
            on: vec![JoinCondition {
                left: ColumnRef::scoped("users", "id"),
                right: ColumnRef::scoped("__hop_0", "author_id"),
            }],
            join_kind: JoinKind::Inner,
        }),
        columns: vec![
            ProjectColumn {
                alias: "id".into(),
                expr: ProjectExpr::Column(ColumnRef::scoped("__hop_0", "id")),
            },
            ProjectColumn {
                alias: "title".into(),
                expr: ProjectExpr::Column(ColumnRef::scoped("__hop_0", "title")),
            },
            ProjectColumn {
                alias: "author_id".into(),
                expr: ProjectExpr::Column(ColumnRef::scoped("__hop_0", "author_id")),
            },
        ],
    };
    query.select_columns = None;

    let sub_id = qm.subscribe(query).unwrap();
    qm.process(&mut storage);
    let updates = qm.take_updates();
    let update = updates
        .iter()
        .find(|u| u.subscription_id == sub_id)
        .expect("Expected precise implicit-id projection update");

    assert_eq!(update.delta.added.len(), 1);
    assert_eq!(
        update.descriptor.columns.len(),
        2,
        "descriptor should only contain declared data columns",
    );
    assert_eq!(update.descriptor.columns[0].name, "title");
    assert_eq!(update.descriptor.columns[1].name, "author_id");

    let row = &update.delta.added[0];
    assert_eq!(row.id, post.row_id);
    let values = decode_row(&update.descriptor, &row.data)
        .expect("should decode full joined element projection");
    assert_eq!(
        values,
        vec![Value::Text("Hello World".into()), Value::Uuid(alice.row_id)]
    );
}

#[test]
fn join_subscription_can_filter_and_project_magic_columns() {
    use crate::query_manager::relation_ir::{
        ColumnRef, JoinCondition, JoinKind, PredicateCmpOp, PredicateExpr, ProjectColumn,
        ProjectExpr, RelExpr, ValueRef,
    };

    let sync_manager = SyncManager::new();
    let schema = join_schema_with_magic_permissions();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    qm.insert(
        &mut storage,
        "users",
        &[Value::Integer(1), Value::Text("Alice".into())],
    )
    .unwrap();
    qm.insert(
        &mut storage,
        "users",
        &[Value::Integer(2), Value::Text("Bob".into())],
    )
    .unwrap();
    qm.insert(
        &mut storage,
        "posts",
        &[
            Value::Text("Alice Post".into()),
            Value::Integer(1),
            Value::Text("alice".into()),
        ],
    )
    .unwrap();
    qm.insert(
        &mut storage,
        "posts",
        &[
            Value::Text("Bob Post".into()),
            Value::Integer(2),
            Value::Text("bob".into()),
        ],
    )
    .unwrap();

    let mut query = qm
        .query("users")
        .join("posts")
        .on("users.id", "posts.author_id")
        .build();
    query.relation_ir = RelExpr::Project {
        input: Box::new(RelExpr::Filter {
            input: Box::new(RelExpr::Join {
                left: Box::new(RelExpr::TableScan {
                    table: TableName::new("users"),
                }),
                right: Box::new(RelExpr::TableScan {
                    table: TableName::new("posts"),
                }),
                on: vec![JoinCondition {
                    left: ColumnRef::scoped("users", "id"),
                    right: ColumnRef::scoped("posts", "author_id"),
                }],
                join_kind: JoinKind::Inner,
            }),
            predicate: PredicateExpr::Cmp {
                left: ColumnRef::scoped("posts", "$canDelete"),
                op: PredicateCmpOp::Eq,
                right: ValueRef::Literal(Value::Boolean(true)),
            },
        }),
        columns: vec![
            ProjectColumn {
                alias: "user_name".into(),
                expr: ProjectExpr::Column(ColumnRef::scoped("users", "name")),
            },
            ProjectColumn {
                alias: "post_title".into(),
                expr: ProjectExpr::Column(ColumnRef::scoped("posts", "title")),
            },
            ProjectColumn {
                alias: "can_edit".into(),
                expr: ProjectExpr::Column(ColumnRef::scoped("posts", "$canEdit")),
            },
            ProjectColumn {
                alias: "can_delete".into(),
                expr: ProjectExpr::Column(ColumnRef::scoped("posts", "$canDelete")),
            },
        ],
    };
    query.select_columns = None;

    let sub_id = qm
        .subscribe_with_session(query, Some(PolicySession::new("alice")), None)
        .unwrap();
    qm.process(&mut storage);
    let update = qm
        .take_updates()
        .into_iter()
        .find(|u| u.subscription_id == sub_id)
        .expect("joined magic projection update");

    assert_eq!(update.delta.added.len(), 1);
    assert_eq!(update.descriptor.columns.len(), 4);
    let values = decode_row(&update.descriptor, &update.delta.added[0].data).unwrap();
    assert_eq!(
        values,
        vec![
            Value::Text("Alice".into()),
            Value::Text("Alice Post".into()),
            Value::Boolean(true),
            Value::Boolean(true),
        ]
    );
}

#[test]
fn join_subscription_supports_implicit_base_id_keys() {
    let sync_manager = SyncManager::new();
    let schema = join_schema_with_implicit_base_id();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    let alice = qm
        .insert(&mut storage, "users", &[Value::Text("Alice".into())])
        .unwrap();
    qm.insert(
        &mut storage,
        "posts",
        &[
            Value::Text("Hello".into()),
            Value::Uuid(alice.row_id), // FK to implicit users.id
        ],
    )
    .unwrap();

    let query = qm
        .query("users")
        .join("posts")
        .on("users.id", "posts.author_id")
        .build();

    let sub_id = qm.subscribe(query).unwrap();
    qm.process(&mut storage);
    let updates = qm.take_updates();
    let delta = updates
        .iter()
        .find(|u| u.subscription_id == sub_id)
        .map(|u| &u.delta)
        .expect("Should have join subscription update");

    assert_eq!(delta.added.len(), 1, "Expected implicit-id join match");
    let row = &delta.added[0];
    assert_eq!(
        row.id, alice.row_id,
        "Join result should remain keyed by base row identity"
    );
    assert!(
        row.data
            .windows("Alice".len())
            .any(|window| window == "Alice".as_bytes()),
        "Joined row should include base-table value"
    );
    assert!(
        row.data
            .windows("Hello".len())
            .any(|window| window == "Hello".as_bytes()),
        "Joined row should include joined-table value"
    );
}

#[test]
fn deleting_parent_row_does_not_cascade_to_joined_rows() {
    // Deleting a parent row should not implicitly delete rows in other tables.
    // Child-table data should remain.
    let sync_manager = SyncManager::new();
    let schema = join_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    let user = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Integer(1), Value::Text("Alice".into())],
        )
        .unwrap();
    qm.insert(
        &mut storage,
        "posts",
        &[
            Value::Integer(100),
            Value::Text("Hello World".into()),
            Value::Integer(1),
        ],
    )
    .unwrap();

    let users_before_query = qm.query("users").build();
    let users_before = execute_query(&mut qm, &mut storage, users_before_query).unwrap();
    assert_eq!(
        users_before.len(),
        1,
        "Precondition: parent row should exist"
    );
    let posts_before_query = qm.query("posts").build();
    let posts_before = execute_query(&mut qm, &mut storage, posts_before_query).unwrap();
    assert_eq!(
        posts_before.len(),
        1,
        "Precondition: child row should exist"
    );

    qm.delete(&mut storage, user.row_id).unwrap();
    qm.process(&mut storage);

    let users_after_query = qm.query("users").build();
    let users_after = execute_query(&mut qm, &mut storage, users_after_query).unwrap();
    assert_eq!(
        users_after.len(),
        0,
        "Parent row should be deleted from its table"
    );

    let posts_query = qm.query("posts").build();
    let posts = execute_query(&mut qm, &mut storage, posts_query).unwrap();
    assert_eq!(
        posts.len(),
        1,
        "Child table row should remain after parent delete"
    );
    assert_eq!(posts[0].1[1], Value::Text("Hello World".into()));
}

#[test]
fn array_subquery_with_join() {
    // Test: join inside array subquery
    // users with_array of (posts joined with comments)
    let sync_manager = SyncManager::new();
    let mut schema = Schema::new();
    schema.insert(
        TableName::new("users"),
        RowDescriptor::new(vec![
            ColumnDescriptor::new("id", ColumnType::Integer),
            ColumnDescriptor::new("name", ColumnType::Text),
        ])
        .into(),
    );
    schema.insert(
        TableName::new("posts"),
        RowDescriptor::new(vec![
            ColumnDescriptor::new("id", ColumnType::Integer),
            ColumnDescriptor::new("title", ColumnType::Text),
            ColumnDescriptor::new("author_id", ColumnType::Integer),
        ])
        .into(),
    );
    schema.insert(
        TableName::new("comments"),
        RowDescriptor::new(vec![
            ColumnDescriptor::new("id", ColumnType::Integer),
            ColumnDescriptor::new("text", ColumnType::Text),
            ColumnDescriptor::new("post_id", ColumnType::Integer),
        ])
        .into(),
    );

    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert user
    qm.insert(
        &mut storage,
        "users",
        &[Value::Integer(1), Value::Text("Alice".into())],
    )
    .unwrap();

    // Insert posts
    qm.insert(
        &mut storage,
        "posts",
        &[
            Value::Integer(100),
            Value::Text("Post A".into()),
            Value::Integer(1),
        ],
    )
    .unwrap();
    qm.insert(
        &mut storage,
        "posts",
        &[
            Value::Integer(101),
            Value::Text("Post B".into()),
            Value::Integer(1),
        ],
    )
    .unwrap();

    // Insert comments
    qm.insert(
        &mut storage,
        "comments",
        &[
            Value::Integer(1000),
            Value::Text("Comment on A".into()),
            Value::Integer(100), // post_id = 100
        ],
    )
    .unwrap();
    qm.insert(
        &mut storage,
        "comments",
        &[
            Value::Integer(1001),
            Value::Text("Another on A".into()),
            Value::Integer(100), // post_id = 100
        ],
    )
    .unwrap();
    qm.insert(
        &mut storage,
        "comments",
        &[
            Value::Integer(1002),
            Value::Text("Comment on B".into()),
            Value::Integer(101), // post_id = 101
        ],
    )
    .unwrap();

    // Query users with (posts joined with comments)
    // This should give us: for each user, an array of (post, comment) pairs
    let query = qm
        .query("users")
        .with_array("post_comments", |sub| {
            sub.from("posts")
                .join("comments")
                .on("posts.id", "comments.post_id")
                .correlate("author_id", "users.id")
        })
        .build();

    let sub_id = qm.subscribe(query).unwrap();
    qm.process(&mut storage);

    let updates = qm.take_updates();
    let delta = updates
        .iter()
        .find(|u| u.subscription_id == sub_id)
        .map(|u| &u.delta)
        .expect("Should have update");

    // Build descriptor for joined output:
    // posts columns + comments columns (row id in Value::Row { id, .. })
    let joined_row_desc = RowDescriptor::new(vec![
        // posts columns
        ColumnDescriptor::new("post_id", ColumnType::Integer),
        ColumnDescriptor::new("title", ColumnType::Text),
        ColumnDescriptor::new("author_id", ColumnType::Integer),
        // comments columns
        ColumnDescriptor::new("comment_id", ColumnType::Integer),
        ColumnDescriptor::new("text", ColumnType::Text),
        ColumnDescriptor::new("post_id", ColumnType::Integer),
    ]);
    let output_descriptor = RowDescriptor::new(vec![
        ColumnDescriptor::new("id", ColumnType::Integer),
        ColumnDescriptor::new("name", ColumnType::Text),
        ColumnDescriptor::new(
            "post_comments",
            ColumnType::Array {
                element: Box::new(ColumnType::Row {
                    columns: Box::new(joined_row_desc),
                }),
            },
        ),
    ]);

    assert_eq!(delta.added.len(), 1, "Should have 1 user");
    let values = decode_row(&output_descriptor, &delta.added[0].data).expect("decode");
    assert_eq!(values[0], Value::Integer(1)); // user id
    assert_eq!(values[1], Value::Text("Alice".into())); // user name

    let post_comments = values[2].as_array().expect("post_comments array");
    // Each (post, comment) pair - Post A has 2 comments, Post B has 1
    assert_eq!(post_comments.len(), 3, "Should have 3 post-comment pairs");

    // Verify the joined rows contain both post and comment data
    for pc in post_comments {
        let row = pc.as_row().expect("joined row");
        assert_eq!(row.len(), 6, "Joined row should have 6 columns");
        assert!(pc.row_id().is_some(), "joined Row should have an id");
        // Post id should be either 100 or 101
        let post_id = match &row[0] {
            Value::Integer(id) => id,
            _ => panic!("Expected integer for post id"),
        };
        assert!(*post_id == 100 || *post_id == 101);
        // Comment post_id should match the post id
        assert_eq!(row[5], Value::Integer(*post_id));
    }
}

#[test]
fn join_query_applies_policy_filter_on_joined_table() {
    let sync_manager = SyncManager::new();
    let schema = join_policy_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    qm.insert(&mut storage, "users", &[Value::Text("alice".into())])
        .unwrap();
    qm.insert(&mut storage, "users", &[Value::Text("bob".into())])
        .unwrap();

    qm.insert(
        &mut storage,
        "posts",
        &[
            Value::Text("alice".into()),
            Value::Text("Alice post".into()),
        ],
    )
    .unwrap();
    qm.insert(
        &mut storage,
        "posts",
        &[Value::Text("bob".into()), Value::Text("Bob post".into())],
    )
    .unwrap();

    let query = qm
        .query("users")
        .join("posts")
        .on("users.name", "posts.owner_name")
        .build();
    let sub_id = qm
        .subscribe_with_session(query, Some(PolicySession::new("alice")), None)
        .unwrap();

    qm.process(&mut storage);
    let updates = qm.take_updates();
    let update = updates
        .iter()
        .find(|u| u.subscription_id == sub_id)
        .expect("join subscription should emit initial delta");
    assert_eq!(
        update.delta.added.len(),
        1,
        "join query should apply policy filter on joined table rows"
    );
}

#[test]
fn local_join_query_uses_current_permissions_for_joined_provenance_after_lens_transform() {
    let sync_manager = SyncManager::new();
    let (mut qm, mut storage) = create_query_manager(sync_manager, legacy_join_provenance_schema());
    configure_legacy_join_client_with_current_permissions(&mut qm);

    qm.insert(&mut storage, "users", &[Value::Text("bob".into())])
        .unwrap();
    qm.insert(
        &mut storage,
        "posts",
        &[
            Value::Text("bob".into()),
            Value::Text("Bob private post".into()),
        ],
    )
    .unwrap();

    let query = QueryBuilder::new("users")
        .join("posts")
        .on("users.name", "posts.owner_name")
        .build();
    let alice_sub = qm
        .subscribe_with_session(query.clone(), Some(PolicySession::new("alice")), None)
        .unwrap();
    let bob_sub = qm
        .subscribe_with_session(query, Some(PolicySession::new("bob")), None)
        .unwrap();

    qm.process(&mut storage);

    assert!(
        qm.get_subscription_results(alice_sub).is_empty(),
        "Joined provenance rows should be filtered when the transformed current permissions deny them"
    );

    let bob_results = qm.get_subscription_results(bob_sub);
    assert_eq!(
        bob_results.len(),
        1,
        "Bob should see the joined row after provenance rows are transformed into the current auth schema"
    );
    assert!(
        bob_results[0]
            .1
            .iter()
            .any(|value| matches!(value, Value::Text(text) if text == "Bob private post")),
        "Joined result should retain the post payload after current-permissions filtering"
    );
}

#[test]
fn server_join_query_uses_current_permissions_for_joined_provenance() {
    use std::collections::HashMap;
    use std::sync::Arc;

    use crate::query_manager::types::{ComposedBranchName, SchemaHash};
    use crate::sync_manager::{ClientId, Destination, InboxEntry, QueryId, Source, SyncPayload};

    let authorization_schema = join_policy_schema();
    let structural_schema: Schema = authorization_schema
        .iter()
        .map(|(table_name, table_schema)| {
            let mut structural = table_schema.clone();
            structural.policies = TablePolicies::default();
            (*table_name, structural)
        })
        .collect();
    let schema_hash = SchemaHash::compute(&structural_schema);
    let branch = ComposedBranchName::new("dev", schema_hash, "main")
        .to_branch_name()
        .as_str()
        .to_string();

    let sync_manager = SyncManager::new();
    let mut server_qm = QueryManager::new(sync_manager);
    let mut known_schemas = HashMap::new();
    known_schemas.insert(schema_hash, structural_schema);
    server_qm.set_known_schemas(Arc::new(known_schemas));
    let storage_schema = authorization_schema.clone();
    server_qm.set_authorization_schema(authorization_schema);

    let mut storage = seeded_memory_storage(&storage_schema);
    let author = ObjectId::new();

    let mut user_metadata = HashMap::new();
    user_metadata.insert(MetadataKey::Table.to_string(), "users".to_string());
    let user_id = create_test_row(&mut storage, Some(user_metadata));
    add_row_commit(
        &mut storage,
        user_id,
        &branch,
        vec![],
        encode_row(
            &RowDescriptor::new(vec![ColumnDescriptor::new("name", ColumnType::Text)]),
            &[Value::Text("bob".into())],
        )
        .unwrap(),
        1000,
        author.to_string(),
    );

    let mut post_metadata = HashMap::new();
    post_metadata.insert(MetadataKey::Table.to_string(), "posts".to_string());
    let post_id = create_test_row(&mut storage, Some(post_metadata));
    add_row_commit(
        &mut storage,
        post_id,
        &branch,
        vec![],
        encode_row(
            &RowDescriptor::new(vec![
                ColumnDescriptor::new("owner_name", ColumnType::Text),
                ColumnDescriptor::new("title", ColumnType::Text),
            ]),
            &[
                Value::Text("bob".into()),
                Value::Text("Bob private post".into()),
            ],
        )
        .unwrap(),
        1000,
        author.to_string(),
    );

    let client_id = ClientId::new();
    connect_client(&mut server_qm, &storage, client_id);
    let session = PolicySession::new("alice");
    server_qm
        .sync_manager_mut()
        .set_client_session(client_id, session.clone());

    let query = QueryBuilder::new("users")
        .branch(&branch)
        .join("posts")
        .on("users.name", "posts.owner_name")
        .build();

    server_qm.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Client(client_id),
        payload: SyncPayload::QuerySubscription {
            query_id: QueryId(1),
            query: Box::new(query),
            session: Some(session),
            required_tier: None,
            propagation: crate::sync_manager::QueryPropagation::Full,
            policy_context_tables: vec![],
        },
    });

    server_qm.process(&mut storage);

    let outbox = server_qm.sync_manager_mut().take_outbox();
    let row_updates: Vec<_> = outbox
        .iter()
        .filter(|entry| matches!(entry.destination, Destination::Client(id) if id == client_id))
        .filter(|entry| matches!(entry.payload, SyncPayload::RowBatchNeeded { .. }))
        .collect();

    assert!(
        row_updates.is_empty(),
        "Joined rows should be filtered when current permissions deny any contributing provenance row"
    );
}

#[test]
fn join_query_with_multiple_branches_reads_all_branches() {
    let sync_manager = SyncManager::new();
    let schema = join_policy_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    let main_branch = get_branch(&qm);
    let draft_branch = get_branch_for_user_branch(&qm, "draft");

    qm.insert(&mut storage, "users", &[Value::Text("alice".into())])
        .unwrap();
    qm.insert(
        &mut storage,
        "posts",
        &[Value::Text("alice".into()), Value::Text("main post".into())],
    )
    .unwrap();

    qm.insert_on_branch(
        &mut storage,
        "users",
        &draft_branch,
        &[Value::Text("dora".into())],
    )
    .unwrap();
    qm.insert_on_branch(
        &mut storage,
        "posts",
        &draft_branch,
        &[Value::Text("dora".into()), Value::Text("draft post".into())],
    )
    .unwrap();

    let query = qm
        .query("users")
        .branches(&[main_branch.as_str(), draft_branch.as_str()])
        .join("posts")
        .on("users.name", "posts.owner_name")
        .build();
    let sub_id = qm.subscribe(query).unwrap();
    qm.process(&mut storage);
    let updates = qm.take_updates();
    let update = updates
        .iter()
        .find(|u| u.subscription_id == sub_id)
        .expect("join subscription should emit initial delta");
    assert_eq!(
        update.delta.added.len(),
        2,
        "join query across branches should include rows from each branch"
    );
}

#[test]
fn sync_backed_joined_exists_rel_session_subscription_keeps_local_rows_when_server_scope_is_empty()
{
    use crate::query_manager::relation_ir::{
        ColumnRef, JoinCondition, JoinKind, PredicateCmpOp, PredicateExpr, RelExpr, RowIdRef,
        ValueRef,
    };
    use crate::sync_manager::{ClientId, ServerId};
    use uuid::Uuid;

    let mut schema = Schema::new();
    schema.insert(
        TableName::new("teams"),
        TableSchema {
            columns: RowDescriptor::new(vec![ColumnDescriptor::new("name", ColumnType::Text)]),
            indexed_columns: None,
            policies: TablePolicies::new().with_select(PolicyExpr::ExistsRel {
                rel: RelExpr::Filter {
                    input: Box::new(RelExpr::Join {
                        left: Box::new(RelExpr::TableScan {
                            table: TableName::new("user_team_edges"),
                        }),
                        right: Box::new(RelExpr::TableScan {
                            table: TableName::new("teams"),
                        }),
                        on: vec![JoinCondition {
                            left: ColumnRef::scoped("user_team_edges", "team_id"),
                            right: ColumnRef::scoped("__join_0", "id"),
                        }],
                        join_kind: JoinKind::Inner,
                    }),
                    predicate: PredicateExpr::And(vec![
                        PredicateExpr::Cmp {
                            left: ColumnRef::scoped("user_team_edges", "user_id"),
                            op: PredicateCmpOp::Eq,
                            right: ValueRef::SessionRef(vec!["user_id".into()]),
                        },
                        PredicateExpr::Cmp {
                            left: ColumnRef::scoped("__join_0", "id"),
                            op: PredicateCmpOp::Eq,
                            right: ValueRef::RowId(RowIdRef::Outer),
                        },
                    ]),
                },
            }),
        },
    );
    schema.insert(
        TableName::new("user_team_edges"),
        TableSchema::new(RowDescriptor::new(vec![
            ColumnDescriptor::new("user_id", ColumnType::Text),
            ColumnDescriptor::new("team_id", ColumnType::Uuid),
        ])),
    );

    let server_sync = SyncManager::new();
    let (mut server, mut server_io) = create_query_manager(server_sync, schema.clone());
    let client_sync = SyncManager::new();
    let (mut client, mut client_io) = create_query_manager(client_sync, schema);

    let team_row = client
        .insert(&mut client_io, "teams", &[Value::Text("Alice".into())])
        .unwrap();
    client
        .insert(
            &mut client_io,
            "user_team_edges",
            &[Value::Text("alice".into()), Value::Uuid(team_row.row_id)],
        )
        .unwrap();
    client.process(&mut client_io);

    let local_sub_id = client
        .subscribe_with_session(
            client.query("teams").build(),
            Some(PolicySession::new("alice")),
            None,
        )
        .unwrap();
    client.process(&mut client_io);
    assert_eq!(
        client.get_subscription_results(local_sub_id).len(),
        1,
        "local session subscriptions should already see the joined EXISTS_REL grant"
    );

    let server_id = ServerId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    let client_id = ClientId(Uuid::new_v7(uuid::Timestamp::now(uuid::NoContext)));
    connect_server(&mut client, &client_io, server_id);
    connect_client(&mut server, &server_io, client_id);
    let _ = client.sync_manager_mut().take_outbox();

    let sub_id = client
        .subscribe_with_sync(
            client.query("teams").build(),
            Some(PolicySession::new("alice")),
            Some(crate::sync_manager::DurabilityTier::EdgeServer),
        )
        .unwrap();

    pump_messages(
        &mut client,
        &mut server,
        &mut client_io,
        &mut server_io,
        client_id,
        server_id,
    );

    let results = client.get_subscription_results(sub_id);
    assert_eq!(
        results.len(),
        1,
        "sync-backed joined EXISTS_REL policies should keep local rows even when the server scope is empty"
    );
    assert_eq!(results[0].1, vec![Value::Text("Alice".into())]);
}
