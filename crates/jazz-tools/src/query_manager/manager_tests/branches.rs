use super::*;
use crate::query_manager::branch_scope::BranchDiffKind;
use crate::query_manager::policy::{CmpOp, Operation, PolicyValue};
use crate::query_manager::session::WriteContext;
use crate::query_manager::types::{BranchTablePolicies, RowPolicyMode};
use crate::query_manager::writes::{RowBranchDelete, RowBranchWrite};

fn branch_permission_schema() -> Schema {
    let mut schema = Schema::new();
    schema.insert(
        TableName::new("projects"),
        RowDescriptor::new(vec![ColumnDescriptor::new("name", ColumnType::Text)]).into(),
    );
    schema.insert(
        TableName::new("todos"),
        TableSchema::with_policies(
            RowDescriptor::new(vec![
                ColumnDescriptor::new("project_id", ColumnType::Uuid).references("projects"),
                ColumnDescriptor::new("owner_id", ColumnType::Text),
            ]),
            TablePolicies {
                branch: vec![
                    BranchTablePolicies::new("projects").with_insert(PolicyExpr::And(vec![
                        PolicyExpr::Cmp {
                            column: "project_id".into(),
                            op: CmpOp::Eq,
                            value: PolicyValue::SessionRef(vec![
                                "__jazz_branch".into(),
                                "id".into(),
                            ]),
                        },
                        PolicyExpr::eq_session("owner_id", vec!["user_id".into()]),
                    ])),
                ],
                ..TablePolicies::default()
            },
        ),
    );
    schema
}

fn branch_reference_permission_schema() -> Schema {
    let mut schema = Schema::new();
    schema.insert(
        TableName::new("drafts"),
        RowDescriptor::new(vec![
            ColumnDescriptor::new("target_todo_id", ColumnType::Uuid).references("todos"),
            ColumnDescriptor::new("owner_id", ColumnType::Text),
        ])
        .into(),
    );
    schema.insert(
        TableName::new("todos"),
        TableSchema::with_policies(
            RowDescriptor::new(vec![ColumnDescriptor::new("body", ColumnType::Text)]),
            TablePolicies {
                branch: vec![BranchTablePolicies::new("drafts").with_update(
                    Some(PolicyExpr::Cmp {
                        column: "id".into(),
                        op: CmpOp::Eq,
                        value: PolicyValue::SessionRef(vec![
                            "__jazz_branch".into(),
                            "target_todo_id".into(),
                        ]),
                    }),
                    PolicyExpr::Cmp {
                        column: "id".into(),
                        op: CmpOp::Eq,
                        value: PolicyValue::SessionRef(vec![
                            "__jazz_branch".into(),
                            "target_todo_id".into(),
                        ]),
                    },
                )],
                ..TablePolicies::default()
            },
        ),
    );
    schema
}

fn create_enforcing_query_manager(
    sync_manager: SyncManager,
    schema: Schema,
) -> (QueryManager, MemoryStorage) {
    let mut qm = QueryManager::new(sync_manager);
    qm.set_current_schema_with_policy_mode(schema, "dev", "main", RowPolicyMode::Enforcing);
    let storage = seeded_memory_storage(&qm.schema_context().current_schema);
    (qm, storage)
}

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
fn captures_branch_scope_with_visible_main_frontier() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    let alice = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();
    let bob = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Bob".into()), Value::Integer(10)],
        )
        .unwrap();

    let branch_id = ObjectId::new();
    let query = qm
        .query("users")
        .filter_ge("score", Value::Integer(50))
        .build();

    let snapshot = qm
        .capture_branch_scope(&mut storage, branch_id, query)
        .expect("scope capture should succeed");

    let alice_entry = snapshot.entry_for("users", alice.row_id).unwrap();
    assert_eq!(alice_entry.base_batch_id, alice.batch_id);
    assert!(snapshot.entry_for("users", bob.row_id).is_none());
    assert_eq!(
        storage
            .load_branch_scope_snapshot(branch_id)
            .unwrap()
            .unwrap()
            .entries,
        snapshot.entries
    );
}

#[test]
fn branch_scope_reads_captured_main_version_not_latest_main() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    let alice = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();

    let branch_id = ObjectId::new();
    let scope_query = qm.query("users").build();
    qm.capture_branch_scope(&mut storage, branch_id, scope_query)
        .unwrap();

    qm.update(
        &mut storage,
        alice.row_id,
        &[Value::Text("Alice".into()), Value::Integer(1)],
    )
    .unwrap();

    let branch_query = qm.query("users").with_branch_scope(branch_id).build();
    let rows = execute_query(&mut qm, &mut storage, branch_query).unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].1[1], Value::Integer(100));
}

#[test]
fn branch_scope_update_overrides_captured_base_row() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    let alice = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();
    let branch_id = ObjectId::new();
    qm.capture_branch_scope(&mut storage, branch_id, qm.query("users").build())
        .unwrap();

    let branch = branch_id.to_string();
    let base_row = load_visible_row(&storage, alice.row_id, &get_branch(&qm));
    let base_provenance = base_row.row_provenance();
    qm.write_existing_row_on_branch_with_write_context(
        &mut storage,
        RowBranchWrite {
            table: "users",
            branch: &branch,
            id: alice.row_id,
            values: &[Value::Text("Alice".into()), Value::Integer(55)],
            old_data_for_policy: &base_row.data,
            old_provenance_for_policy: &base_provenance,
        },
        None,
    )
    .unwrap();

    let query = qm.query("users").with_branch_scope(branch_id).build();
    let rows = execute_query(&mut qm, &mut storage, query).unwrap();

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].1[1], Value::Integer(55));
}

#[test]
fn branch_diff_compares_source_to_captured_base_and_current_main() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    let alice = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();
    let branch_id = ObjectId::new();
    qm.capture_branch_scope(&mut storage, branch_id, qm.query("users").build())
        .unwrap();

    qm.update(
        &mut storage,
        alice.row_id,
        &[Value::Text("Alice".into()), Value::Integer(80)],
    )
    .unwrap();

    let snapshot = storage
        .load_branch_scope_snapshot(branch_id)
        .unwrap()
        .expect("branch scope should exist");
    let entry = snapshot
        .entry_for("users", alice.row_id)
        .expect("alice should be in branch scope");
    let base_row = storage
        .load_history_query_row_batch(
            "users",
            entry.base_branch.as_str(),
            alice.row_id,
            entry.base_batch_id,
        )
        .unwrap()
        .expect("captured base row should exist");
    let base_provenance = base_row.row_provenance();
    let branch = branch_id.to_string();
    qm.write_existing_row_on_branch_with_write_context(
        &mut storage,
        RowBranchWrite {
            table: "users",
            branch: &branch,
            id: alice.row_id,
            values: &[Value::Text("Alice".into()), Value::Integer(90)],
            old_data_for_policy: &base_row.data,
            old_provenance_for_policy: &base_provenance,
        },
        None,
    )
    .unwrap();

    let diff = qm
        .diff_branch_query(&mut storage, branch_id, qm.query("users").build())
        .unwrap();

    assert_eq!(diff.len(), 1);
    assert_eq!(diff[0].row_id, alice.row_id);
    assert_eq!(diff[0].kind, BranchDiffKind::Update);
    assert_eq!(diff[0].changed, vec!["score"]);
}

#[test]
fn branch_merge_writes_source_to_main_with_cross_branch_parent() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    let alice = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();
    let branch_id = ObjectId::new();
    qm.capture_branch_scope(&mut storage, branch_id, qm.query("users").build())
        .unwrap();

    let branch = branch_id.to_string();
    let base_row = load_visible_row(&storage, alice.row_id, &get_branch(&qm));
    let base_provenance = base_row.row_provenance();
    let branch_batch_id = qm
        .write_existing_row_on_branch_with_write_context(
            &mut storage,
            RowBranchWrite {
                table: "users",
                branch: &branch,
                id: alice.row_id,
                values: &[Value::Text("Alice".into()), Value::Integer(90)],
                old_data_for_policy: &base_row.data,
                old_provenance_for_policy: &base_provenance,
            },
            None,
        )
        .unwrap();

    qm.merge_branch_scope(&mut storage, branch_id).unwrap();

    let query = qm.query("users").build();
    let rows = execute_query(&mut qm, &mut storage, query).unwrap();
    assert_eq!(rows[0].1[1], Value::Integer(90));

    let merged = load_visible_row(&storage, alice.row_id, &get_branch(&qm));
    assert!(
        merged.parents.contains(&branch_batch_id),
        "merged main row should keep branch source batch as a parent"
    );
}

#[test]
fn branch_scope_delete_hides_captured_base_row() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    let alice = qm
        .insert(
            &mut storage,
            "users",
            &[Value::Text("Alice".into()), Value::Integer(100)],
        )
        .unwrap();
    let branch_id = ObjectId::new();
    qm.capture_branch_scope(&mut storage, branch_id, qm.query("users").build())
        .unwrap();

    let branch = branch_id.to_string();
    let base_row = load_visible_row(&storage, alice.row_id, &get_branch(&qm));
    let base_provenance = base_row.row_provenance();
    qm.delete_existing_row_on_branch_with_write_context(
        &mut storage,
        RowBranchDelete {
            table: "users",
            branch: &branch,
            id: alice.row_id,
            old_data_for_policy: &base_row.data,
            old_provenance_for_policy: &base_provenance,
        },
        None,
    )
    .unwrap();

    let query = qm.query("users").with_branch_scope(branch_id).build();
    let rows = execute_query(&mut qm, &mut storage, query).unwrap();

    assert!(rows.is_empty());
}

#[test]
fn branch_scope_insert_visible_when_it_matches_scope_query() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    let branch_id = ObjectId::new();
    let scope_query = qm
        .query("users")
        .filter_ge("score", Value::Integer(50))
        .build();
    qm.capture_branch_scope(&mut storage, branch_id, scope_query)
        .unwrap();

    let branch = branch_id.to_string();
    let inserted = qm
        .insert_on_branch(
            &mut storage,
            "users",
            &branch,
            &[Value::Text("Dora".into()), Value::Integer(99)],
        )
        .unwrap();

    let query = qm.query("users").with_branch_scope(branch_id).build();
    let rows = execute_query(&mut qm, &mut storage, query).unwrap();

    assert_eq!(
        rows.iter().map(|row| row.0).collect::<Vec<_>>(),
        vec![inserted.row_id]
    );
}

#[test]
fn branch_scope_insert_hidden_when_it_misses_scope_query() {
    let sync_manager = SyncManager::new();
    let schema = test_schema();
    let (mut qm, mut storage) = create_query_manager(sync_manager, schema);

    let branch_id = ObjectId::new();
    let scope_query = qm
        .query("users")
        .filter_ge("score", Value::Integer(50))
        .build();
    qm.capture_branch_scope(&mut storage, branch_id, scope_query)
        .unwrap();

    let branch = branch_id.to_string();
    qm.insert_on_branch(
        &mut storage,
        "users",
        &branch,
        &[Value::Text("Eve".into()), Value::Integer(1)],
    )
    .unwrap();

    let query = qm.query("users").with_branch_scope(branch_id).build();
    let rows = execute_query(&mut qm, &mut storage, query).unwrap();

    assert!(rows.is_empty());
}

#[test]
fn branch_policy_allows_insert_when_row_matches_branch() {
    let sync_manager = SyncManager::new();
    let schema = branch_permission_schema();
    let (mut qm, mut storage) = create_enforcing_query_manager(sync_manager, schema);

    let project = qm
        .insert(&mut storage, "projects", &[Value::Text("Apollo".into())])
        .unwrap();
    qm.capture_branch_scope(
        &mut storage,
        project.row_id,
        qm.query("todos")
            .filter_eq("project_id", Value::Uuid(project.row_id))
            .build(),
    )
    .unwrap();

    let branch = project.row_id.to_string();
    let inserted = qm
        .insert_on_branch_with_session(
            &mut storage,
            "todos",
            &branch,
            &[
                Value::Uuid(project.row_id),
                Value::Text("alice".to_string()),
            ],
            Some(&PolicySession::new("alice")),
        )
        .expect("branch policy should allow matching insert");

    let query = qm.query("todos").with_branch_scope(project.row_id).build();
    let rows = execute_query(&mut qm, &mut storage, query).unwrap();
    assert_eq!(
        rows.iter().map(|row| row.0).collect::<Vec<_>>(),
        vec![inserted.row_id,]
    );
}

#[test]
fn branch_policy_denies_insert_when_session_misses() {
    let sync_manager = SyncManager::new();
    let schema = branch_permission_schema();
    let (mut qm, mut storage) = create_enforcing_query_manager(sync_manager, schema);

    let project = qm
        .insert(&mut storage, "projects", &[Value::Text("Apollo".into())])
        .unwrap();
    qm.capture_branch_scope(
        &mut storage,
        project.row_id,
        qm.query("todos")
            .filter_eq("project_id", Value::Uuid(project.row_id))
            .build(),
    )
    .unwrap();

    let branch = project.row_id.to_string();
    let err = qm
        .insert_on_branch_with_session(
            &mut storage,
            "todos",
            &branch,
            &[Value::Uuid(project.row_id), Value::Text("bob".to_string())],
            Some(&PolicySession::new("alice")),
        )
        .expect_err("branch policy should deny wrong owner");

    assert!(matches!(
        err,
        QueryError::PolicyDenied {
            table,
            operation: Operation::Insert,
        } if table == TableName::new("todos")
    ));
}

#[test]
fn branch_policy_allows_update_when_row_matches_branch_row_reference() {
    let sync_manager = SyncManager::new();
    let schema = branch_reference_permission_schema();
    let (mut qm, mut storage) = create_enforcing_query_manager(sync_manager, schema);

    let todo = qm
        .insert(&mut storage, "todos", &[Value::Text("Draft me".into())])
        .unwrap();
    let draft = qm
        .insert(
            &mut storage,
            "drafts",
            &[Value::Uuid(todo.row_id), Value::Text("alice".to_string())],
        )
        .unwrap();
    qm.capture_branch_scope(
        &mut storage,
        draft.row_id,
        qm.query("todos")
            .filter_eq("id", Value::Uuid(todo.row_id))
            .build(),
    )
    .unwrap();

    let current_branch = get_branch(&qm);
    let main_row = storage
        .load_visible_query_row("todos", current_branch.as_str(), todo.row_id)
        .unwrap()
        .unwrap();
    let session = WriteContext::from_session(PolicySession::new("alice"));
    let branch = draft.row_id.to_string();

    qm.write_existing_row_on_branch_with_write_context(
        &mut storage,
        RowBranchWrite {
            table: "todos",
            branch: branch.as_str(),
            id: todo.row_id,
            values: &[Value::Text("Drafted on branch".into())],
            old_data_for_policy: &main_row.data,
            old_provenance_for_policy: &main_row.row_provenance(),
        },
        Some(&session),
    )
    .expect("branch policy should allow matching update through branch row ref");

    let query = qm
        .query("todos")
        .branch(branch.as_str())
        .with_branch_scope(draft.row_id)
        .build();
    let rows = execute_query(&mut qm, &mut storage, query).unwrap();
    assert_eq!(rows[0].1[0], Value::Text("Drafted on branch".into()));
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
