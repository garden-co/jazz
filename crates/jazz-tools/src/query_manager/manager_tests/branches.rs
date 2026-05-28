use super::*;
use crate::query_manager::relation_ir::{ColumnRef, OrderByExpr, OrderDirection, RelExpr};

#[test]
fn index_key_includes_branch() {
    // Verify branch isolation through observable query results.
    // A row inserted on the current branch should not appear on another branch.

    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert on the schema's branch.
    qm.insert(
        &mut storage,
        "users",
        &[Value::Text("Alice".into()), Value::Integer(100)],
    )
    .unwrap();

    let branch = get_branch(&qm);
    let current_branch_query = qm.query("users").branch(&branch).build();
    let current_branch_results =
        execute_query(&mut qm, &mut storage, current_branch_query).unwrap();
    assert_eq!(
        current_branch_results.len(),
        1,
        "Should find row on current branch"
    );
    assert_eq!(current_branch_results[0].1[0], Value::Text("Alice".into()));

    // Verify the row is NOT visible on a different branch.
    let other_branch = get_branch_for_user_branch(&qm, "some-other-branch");
    let other_branch_query = qm.query("users").branch(&other_branch).build();
    let other_branch_results = execute_query(&mut qm, &mut storage, other_branch_query).unwrap();
    assert!(
        other_branch_results.is_empty(),
        "Should NOT find row on a different branch"
    );
}

#[test]
fn query_builder_single_branch_uses_correct_index() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Insert on default "main" branch
    qm.insert(
        &mut storage,
        "users",
        &[Value::Text("Alice".into()), Value::Integer(100)],
    )
    .unwrap();

    // Query explicitly specifying "main" branch
    let query = qm.query("users").build();
    let results = execute_query(&mut qm, &mut storage, query).unwrap();
    assert_eq!(results.len(), 1, "Should find row on main branch");

    // Query specifying a different branch should return no results
    // (since we haven't inserted on that branch)
    let draft_branch = get_branch_for_user_branch(&qm, "draft");
    let query = qm.query("users").branch(&draft_branch).build();
    let results = execute_query(&mut qm, &mut storage, query).unwrap();
    assert_eq!(results.len(), 0, "Should not find row on draft branch");
}

#[test]
fn query_builder_explicit_main_branch() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    qm.insert(
        &mut storage,
        "users",
        &[Value::Text("Alice".into()), Value::Integer(100)],
    )
    .unwrap();
    qm.insert(
        &mut storage,
        "users",
        &[Value::Text("Bob".into()), Value::Integer(50)],
    )
    .unwrap();

    // Explicit current/main branch should work same as default.
    let main_branch = get_branch(&qm);
    let query_explicit = qm.query("users").branch(&main_branch).build();
    let query_default = qm.query("users").build();

    let results_explicit = execute_query(&mut qm, &mut storage, query_explicit).unwrap();
    let results_default = execute_query(&mut qm, &mut storage, query_default).unwrap();

    assert_eq!(results_explicit.len(), results_default.len());
    assert_eq!(results_explicit.len(), 2);
}

#[test]
fn query_on_composed_noncurrent_branch_reads_rows() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);
    let draft_branch = get_branch_for_user_branch(&qm, "draft");

    let inserted = qm
        .insert_on_branch(
            &mut storage,
            "users",
            &draft_branch,
            &[Value::Text("Dora".into()), Value::Integer(42)],
            None,
        )
        .unwrap();

    assert_eq!(
        storage.index_lookup("users", "_id", &draft_branch, &Value::Uuid(inserted.row_id)),
        vec![inserted.row_id]
    );

    let query = qm.query("users").branch(&draft_branch).build();
    let results = execute_query(&mut qm, &mut storage, query).unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].1[0], Value::Text("Dora".into()));
}

#[test]
fn union_query_with_branch_arms_reads_rows_from_each_branch() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);
    let main_branch = get_branch(&qm);
    let draft_branch = get_branch_for_user_branch(&qm, "draft");

    qm.insert(
        &mut storage,
        "users",
        &[Value::Text("Main Alice".into()), Value::Integer(100)],
    )
    .unwrap();
    qm.insert_on_branch(
        &mut storage,
        "users",
        &draft_branch,
        &[Value::Text("Draft Dora".into()), Value::Integer(42)],
        None,
    )
    .unwrap();

    let mut query = qm.query("users").build();
    query.relation_ir = RelExpr::Union {
        inputs: vec![
            RelExpr::Branch {
                input: Box::new(RelExpr::TableScan {
                    table: TableName::new("users"),
                }),
                branches: vec![main_branch],
            },
            RelExpr::Branch {
                input: Box::new(RelExpr::TableScan {
                    table: TableName::new("users"),
                }),
                branches: vec![draft_branch],
            },
        ],
    };

    let results = execute_query(&mut qm, &mut storage, query).unwrap();
    let mut names: Vec<_> = results
        .iter()
        .map(|(_, values)| match &values[0] {
            Value::Text(value) => value.clone(),
            other => panic!("expected text name, got {other:?}"),
        })
        .collect();
    names.sort();
    assert_eq!(
        names,
        vec!["Draft Dora".to_string(), "Main Alice".to_string()]
    );
}

#[test]
fn branch_query_applies_order_by_and_limit_on_selected_branch() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);
    let draft_branch = get_branch_for_user_branch(&qm, "draft");

    qm.insert(
        &mut storage,
        "users",
        &[Value::Text("Main Alice".into()), Value::Integer(999)],
    )
    .unwrap();
    qm.insert_on_branch(
        &mut storage,
        "users",
        &draft_branch,
        &[Value::Text("Draft Low".into()), Value::Integer(10)],
        None,
    )
    .unwrap();
    qm.insert_on_branch(
        &mut storage,
        "users",
        &draft_branch,
        &[Value::Text("Draft High".into()), Value::Integer(30)],
        None,
    )
    .unwrap();
    qm.insert_on_branch(
        &mut storage,
        "users",
        &draft_branch,
        &[Value::Text("Draft Mid".into()), Value::Integer(20)],
        None,
    )
    .unwrap();

    let query = qm
        .query("users")
        .branch(&draft_branch)
        .order_by_desc("score")
        .limit(2)
        .build();
    let results = execute_query(&mut qm, &mut storage, query).unwrap();

    assert_eq!(
        results
            .iter()
            .map(|(_, values)| values[0].clone())
            .collect::<Vec<_>>(),
        vec![
            Value::Text("Draft High".into()),
            Value::Text("Draft Mid".into())
        ]
    );
}

#[test]
fn union_query_with_branch_arms_applies_order_by_and_limit() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);
    let main_branch = get_branch(&qm);
    let draft_branch = get_branch_for_user_branch(&qm, "draft");

    qm.insert(
        &mut storage,
        "users",
        &[Value::Text("Main Low".into()), Value::Integer(10)],
    )
    .unwrap();
    qm.insert(
        &mut storage,
        "users",
        &[Value::Text("Main High".into()), Value::Integer(40)],
    )
    .unwrap();
    qm.insert_on_branch(
        &mut storage,
        "users",
        &draft_branch,
        &[Value::Text("Draft Mid".into()), Value::Integer(30)],
        None,
    )
    .unwrap();
    qm.insert_on_branch(
        &mut storage,
        "users",
        &draft_branch,
        &[Value::Text("Draft Lower".into()), Value::Integer(20)],
        None,
    )
    .unwrap();

    let branch_union = RelExpr::Union {
        inputs: vec![
            RelExpr::Branch {
                input: Box::new(RelExpr::TableScan {
                    table: TableName::new("users"),
                }),
                branches: vec![main_branch],
            },
            RelExpr::Branch {
                input: Box::new(RelExpr::TableScan {
                    table: TableName::new("users"),
                }),
                branches: vec![draft_branch],
            },
        ],
    };
    let mut query = qm.query("users").build();
    query.relation_ir = RelExpr::Limit {
        input: Box::new(RelExpr::OrderBy {
            input: Box::new(branch_union),
            terms: vec![OrderByExpr {
                column: ColumnRef::unscoped("score"),
                direction: OrderDirection::Desc,
            }],
        }),
        limit: 2,
    };

    let results = execute_query(&mut qm, &mut storage, query).unwrap();

    assert_eq!(
        results
            .iter()
            .map(|(_, values)| values[0].clone())
            .collect::<Vec<_>>(),
        vec![
            Value::Text("Main High".into()),
            Value::Text("Draft Mid".into())
        ]
    );
}

#[test]
fn internal_resolved_branch_carrier_requires_explicit_branches() {
    // Query.branches is an internal runtime/schema-manager carrier, not public query API.
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (qm, _storage) = create_query_manager(sync_manager, schema);
    let main_branch = get_branch(&qm);
    let draft_branch = get_branch_for_user_branch(&qm, "draft");

    // Internal carrier query with explicit resolved branches.
    let query = qm
        .query("users")
        .branches(&[main_branch.as_str(), draft_branch.as_str()])
        .build();
    assert_eq!(query.branches.len(), 2);
    assert!(query.branches.len() > 1);

    // Query without explicit branch has empty branches field.
    // The actual branches are resolved at execution time from schema context.
    let query = qm.query("users").build();
    assert!(query.branches.is_empty());
    assert!(query.branches.len() <= 1);
}

#[test]
fn handle_object_update_respects_branch() {
    use crate::query_manager::encoding::encode_row;
    use std::collections::HashMap;

    // Verify that handle_object_update updates the correct branch's indices.
    // Rows on a non-schema branch should NOT appear in queries on the schema branch.
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    // Get the actual schema branch
    let schema_branch = get_branch(&qm);

    let row_id = crate::object::ObjectId::new();
    let author = row_id;

    let mut metadata = HashMap::new();
    metadata.insert(MetadataKey::Table.to_string(), "users".to_string());
    put_test_row_metadata(&mut storage, row_id, metadata);

    let descriptor = RowDescriptor::new(vec![
        ColumnDescriptor::new("name", ColumnType::Text),
        ColumnDescriptor::new("score", ColumnType::Integer),
    ]);
    let row_data = encode_row(
        &descriptor,
        &[Value::Text("Alice".into()), Value::Integer(100)],
    )
    .unwrap();

    // Receive commit on "other-branch" (not the schema's branch)
    let commit = stored_row_commit(smallvec![], row_data.clone(), 1000, author.to_string());
    receive_row_commit(&mut qm, &mut storage, row_id, "other-branch", commit);

    qm.process(&mut storage);

    // Query schema branch - should NOT find the row (it's on other-branch)
    let query = qm.query("users").build();
    let results = execute_query(&mut qm, &mut storage, query).unwrap();
    assert_eq!(
        results.len(),
        0,
        "Row on other-branch should not appear in schema branch query"
    );

    // Now insert on schema branch and verify it appears in default query
    let row_id2 = crate::object::ObjectId::new();
    let mut metadata2 = HashMap::new();
    metadata2.insert(MetadataKey::Table.to_string(), "users".to_string());
    put_test_row_metadata(&mut storage, row_id2, metadata2);

    let commit2 = stored_row_commit(smallvec![], row_data, 2000, row_id2.to_string());
    receive_row_commit(&mut qm, &mut storage, row_id2, &schema_branch, commit2);

    qm.process(&mut storage);

    // Schema branch should now have 1 row
    let query = qm.query("users").build();
    let results = execute_query(&mut qm, &mut storage, query).unwrap();
    assert_eq!(
        results.len(),
        1,
        "Row on schema branch should appear in default query"
    );
}
