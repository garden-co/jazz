use super::*;

#[test]
fn index_key_includes_branch() {
    // Verify storage indexes stay branch-local even though app-branch reads
    // fall back to main rows.

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

    let other_branch = get_branch_for_user_branch(&qm, "some-other-branch");
    assert_eq!(
        storage.index_lookup(
            "users",
            "_id",
            &other_branch,
            &Value::Uuid(current_branch_results[0].0)
        ),
        Vec::new(),
        "Row should not be indexed on the other branch"
    );

    let other_branch_query = qm.query("users").branch(&other_branch).build();
    let other_branch_results = execute_query(&mut qm, &mut storage, other_branch_query).unwrap();
    assert_eq!(
        other_branch_results.len(),
        1,
        "Branch read falls back to main"
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

    // Query specifying a different app branch falls back to main.
    let draft_branch = get_branch_for_user_branch(&qm, "draft");
    let query = qm.query("users").branch(&draft_branch).build();
    let results = execute_query(&mut qm, &mut storage, query).unwrap();
    assert_eq!(results.len(), 1, "Should find fallback row on draft branch");
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

    // Explicit .branch("main") should work same as default
    let query_explicit = qm.query("users").build();
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
fn branch_query_reads_main_rows_as_overlay_fallback() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);
    let draft_branch = get_branch_for_user_branch(&qm, "draft");

    qm.insert(
        &mut storage,
        "users",
        &[Value::Text("Alice".into()), Value::Integer(100)],
    )
    .unwrap();

    let query = qm.query("users").branch(&draft_branch).build();
    let results = execute_query(&mut qm, &mut storage, query).unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].1[0], Value::Text("Alice".into()));
}

#[test]
fn branch_query_prefers_branch_row_over_newer_main_row() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);
    let draft_branch = get_branch_for_user_branch(&qm, "draft");

    let inserted = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();

    let descriptor = test_schema()
        .get(&TableName::new("users"))
        .unwrap()
        .columns
        .clone();
    let branch_data = encode_row(
        &descriptor,
        &[Value::Text("Alice draft".into()), Value::Integer(50)],
    )
    .unwrap();
    receive_row_commit(
        &mut qm,
        &mut storage,
        inserted.row_id,
        &draft_branch,
        stored_row_commit(smallvec![], branch_data, 2_000, "alice"),
    );
    qm.process(&mut storage);

    qm.update(
        &mut storage,
        inserted.row_id,
        &[Value::Text("Alice main".into()), Value::Integer(200)],
    )
    .unwrap();

    let query = qm.query("users").branch(&draft_branch).build();
    let results = execute_query(&mut qm, &mut storage, query).unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].1[0], Value::Text("Alice draft".into()));
    assert_eq!(results[0].1[1], Value::Integer(50));
}

#[test]
fn branch_query_filters_against_overlay_row() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);
    let draft_branch = get_branch_for_user_branch(&qm, "draft");

    let inserted = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();

    let descriptor = test_schema()
        .get(&TableName::new("users"))
        .unwrap()
        .columns
        .clone();
    let branch_data = encode_row(
        &descriptor,
        &[Value::Text("Alice draft".into()), Value::Integer(50)],
    )
    .unwrap();
    receive_row_commit(
        &mut qm,
        &mut storage,
        inserted.row_id,
        &draft_branch,
        stored_row_commit(smallvec![], branch_data, 2_000, "alice"),
    );
    qm.process(&mut storage);

    let query = qm
        .query("users")
        .branch(&draft_branch)
        .filter_eq("score", Value::Integer(100))
        .build();
    let results = execute_query(&mut qm, &mut storage, query).unwrap();

    assert!(
        results.is_empty(),
        "main row matches score=100, but branch overlay row has score=50"
    );
}

#[test]
fn branch_diff_magic_column_reports_changed_columns() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema.clone());
    let draft_branch = get_branch_for_user_branch(&qm, "draft");

    let inserted = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();

    let descriptor = schema
        .get(&TableName::new("users"))
        .unwrap()
        .columns
        .clone();
    let branch_data = encode_row(
        &descriptor,
        &[Value::Text("Alice draft".into()), Value::Integer(50)],
    )
    .unwrap();
    receive_row_commit(
        &mut qm,
        &mut storage,
        inserted.row_id,
        &draft_branch,
        stored_row_commit(smallvec![], branch_data, 2_000, "alice"),
    );
    qm.process(&mut storage);

    let mut query = qm
        .query("users")
        .branch(&draft_branch)
        .select(&["name", "score", "$diff"])
        .build();
    query.diff = true;
    let results = execute_query(&mut qm, &mut storage, query).unwrap();

    assert_eq!(results.len(), 1);
    assert_eq!(results[0].1[0], Value::Text("Alice draft".into()));
    let Value::Text(diff_json) = &results[0].1[2] else {
        panic!("$diff should be encoded as JSON text");
    };
    let diff: serde_json::Value = serde_json::from_str(diff_json).unwrap();
    assert_eq!(diff["kind"], "update");
    assert_eq!(diff["changed"], serde_json::json!(["name", "score"]));
    assert_eq!(diff["conflicts"], serde_json::json!([]));
}

#[test]
fn query_multi_branch_requires_explicit_branch() {
    // Verify Query.branches field exists and works
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (qm, _storage) = create_query_manager(sync_manager, schema);
    let main_branch = get_branch(&qm);
    let draft_branch = get_branch_for_user_branch(&qm, "draft");

    // Multi-branch query with explicit branches
    let query = qm
        .query("users")
        .branches(&[main_branch.as_str(), draft_branch.as_str()])
        .build();
    assert_eq!(query.branches.len(), 2);
    assert!(query.is_multi_branch());

    // Query without explicit branch has empty branches field.
    // The actual branches are resolved at execution time from schema context.
    let query = qm.query("users").build();
    assert!(query.branches.is_empty());
    assert!(!query.is_multi_branch());
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
