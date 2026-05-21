use std::collections::HashMap;

use crate::object::ObjectId;
use crate::query_manager::policy::{CmpOp, PolicyValue};
use crate::query_manager::query::QueryBuilder;
use crate::query_manager::types::SchemaBuilder;
use crate::query_manager::writes::RowBranchWrite;

use super::*;

fn project_matches_branch_policy() -> PolicyExpr {
    PolicyExpr::Cmp {
        column: "projectId".into(),
        op: CmpOp::Eq,
        value: PolicyValue::BranchRef("projectId".into()),
    }
}

fn branch_schema_with_todo_policies(todo_policies: TablePolicies) -> Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("branches")
                .column("projectId", ColumnType::Uuid)
                .column("ownerId", ColumnType::Text)
                .policies(TablePolicies::new().with_select(PolicyExpr::Cmp {
                    column: "ownerId".into(),
                    op: CmpOp::Eq,
                    value: PolicyValue::SessionRef(vec!["user_id".into()]),
                })),
        )
        .table(
            TableSchema::builder("todos")
                .column("projectId", ColumnType::Uuid)
                .column("title", ColumnType::Text)
                .policies(todo_policies),
        )
        .build()
}

fn branch_schema_with_backing_and_todo_policies(
    backing_policies: TablePolicies,
    todo_policies: TablePolicies,
) -> Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("branches")
                .column("projectId", ColumnType::Uuid)
                .column("ownerId", ColumnType::Text)
                .policies(backing_policies),
        )
        .table(
            TableSchema::builder("todos")
                .column("projectId", ColumnType::Uuid)
                .column("title", ColumnType::Text)
                .policies(todo_policies),
        )
        .build()
}

fn branch_schema_without_backing_policy(todo_policies: TablePolicies) -> Schema {
    branch_schema_with_backing_and_todo_policies(TablePolicies::new(), todo_policies)
}

fn branch_schema_with_approvals(todo_policies: TablePolicies) -> Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("branches")
                .column("projectId", ColumnType::Uuid)
                .column("ownerId", ColumnType::Text)
                .policies(TablePolicies::new().with_select(PolicyExpr::Cmp {
                    column: "ownerId".into(),
                    op: CmpOp::Eq,
                    value: PolicyValue::SessionRef(vec!["user_id".into()]),
                })),
        )
        .table(
            TableSchema::builder("todos")
                .column("projectId", ColumnType::Uuid)
                .column("title", ColumnType::Text)
                .policies(todo_policies),
        )
        .table(
            TableSchema::builder("approvals")
                .column("projectId", ColumnType::Uuid)
                .policies(TablePolicies::default()),
        )
        .build()
}

fn branch_schema(include_todo_branch_policy: bool) -> Schema {
    let mut todo_policies = TablePolicies::default();
    if include_todo_branch_policy {
        todo_policies.for_branch = HashMap::from([(
            TableName::new("branches"),
            TablePolicies::new()
                .with_select(project_matches_branch_policy())
                .with_insert(project_matches_branch_policy()),
        )]);
    }

    branch_schema_with_todo_policies(todo_policies)
}

fn todos_metadata() -> HashMap<String, String> {
    HashMap::from([(MetadataKey::Table.to_string(), "todos".to_string())])
}

fn manager_with_branch_schema(
    include_todo_branch_policy: bool,
) -> (QueryManager, MemoryStorage, Schema) {
    let schema = branch_schema(include_todo_branch_policy);
    let sync_manager = SyncManager::new();
    let qm = create_query_manager_with_policy_mode(
        sync_manager,
        schema.clone(),
        RowPolicyMode::Enforcing,
    );
    let storage = seeded_memory_storage(&qm.schema_context().current_schema);
    (qm, storage, schema)
}

fn branch_name_for(schema: &Schema, branch_id: ObjectId) -> String {
    ComposedBranchName::new("dev", SchemaHash::compute(schema), &branch_id.to_string())
        .to_branch_name()
        .as_str()
        .to_string()
}

fn seed_branch_and_todo(
    qm: &mut QueryManager,
    storage: &mut MemoryStorage,
    schema: &Schema,
    project_id: ObjectId,
    owner: &str,
) -> ObjectId {
    let branch_id = qm
        .insert(
            storage,
            "branches",
            &[Value::Uuid(project_id), Value::Text(owner.into())],
        )
        .expect("insert branch row")
        .row_id;

    let branch_name = branch_name_for(schema, branch_id);
    qm.insert_on_branch(
        storage,
        "todos",
        &branch_name,
        &[
            Value::Uuid(project_id),
            Value::Text("Write API docs".into()),
        ],
        None,
    )
    .expect("insert branch-visible todo");

    branch_id
}

#[test]
fn branch_read_requires_readable_backing_row_and_matching_branch_policy() {
    let (mut qm, mut storage, schema) = manager_with_branch_schema(true);
    let project_id = ObjectId::new();
    let branch_id = seed_branch_and_todo(&mut qm, &mut storage, &schema, project_id, "alice");
    let branch_name = branch_name_for(&schema, branch_id);

    let rows = query_rows(
        &mut qm,
        &mut storage,
        QueryBuilder::new("todos").branch(branch_name).build(),
        Some(Session::new("alice")),
    );

    assert_eq!(rows.len(), 1);
}

#[test]
fn branch_read_denies_when_backing_row_is_not_readable() {
    let (mut qm, mut storage, schema) = manager_with_branch_schema(true);
    let project_id = ObjectId::new();
    let branch_id = seed_branch_and_todo(&mut qm, &mut storage, &schema, project_id, "alice");
    let branch_name = branch_name_for(&schema, branch_id);

    let rows = query_rows(
        &mut qm,
        &mut storage,
        QueryBuilder::new("todos").branch(branch_name).build(),
        Some(Session::new("bob")),
    );

    assert!(rows.is_empty());
}

#[test]
fn branch_read_denies_when_for_branch_block_is_missing() {
    let (mut qm, mut storage, schema) = manager_with_branch_schema(false);
    let project_id = ObjectId::new();
    let branch_id = seed_branch_and_todo(&mut qm, &mut storage, &schema, project_id, "alice");
    let branch_name = branch_name_for(&schema, branch_id);

    let rows = query_rows(
        &mut qm,
        &mut storage,
        QueryBuilder::new("todos").branch(branch_name).build(),
        Some(Session::new("alice")),
    );

    assert!(rows.is_empty());
}

#[test]
fn branch_read_does_not_fall_back_to_normal_policy_for_non_row_id_branch() {
    let mut todo_policies = TablePolicies::new()
        .with_select(PolicyExpr::True)
        .with_insert(PolicyExpr::True);
    todo_policies.for_branch = HashMap::from([(
        TableName::new("branches"),
        TablePolicies::new()
            .with_select(PolicyExpr::False)
            .with_insert(PolicyExpr::True),
    )]);
    let schema = branch_schema_with_todo_policies(todo_policies);
    let sync_manager = SyncManager::new();
    let mut qm = create_query_manager_with_policy_mode(
        sync_manager,
        schema.clone(),
        RowPolicyMode::Enforcing,
    );
    let mut storage = seeded_memory_storage(&qm.schema_context().current_schema);
    let branch_name = ComposedBranchName::new("dev", SchemaHash::compute(&schema), "alice-draft")
        .to_branch_name()
        .as_str()
        .to_string();

    qm.insert_on_branch(
        &mut storage,
        "todos",
        &branch_name,
        &[Value::Uuid(ObjectId::new()), Value::Text("Draft".into())],
        None,
    )
    .expect("seed non-row-id branch todo");

    let rows = query_rows(
        &mut qm,
        &mut storage,
        QueryBuilder::new("todos").branch(branch_name).build(),
        Some(Session::new("alice")),
    );

    assert!(rows.is_empty());
}

#[test]
fn for_branch_only_schema_infers_enforcing_policy_mode() {
    let mut todo_policies = TablePolicies::default();
    todo_policies.for_branch = HashMap::from([(
        TableName::new("branches"),
        TablePolicies::new().with_insert(PolicyExpr::True),
    )]);
    let schema = branch_schema_without_backing_policy(todo_policies);

    let mut writer = create_query_manager_with_policy_mode(
        SyncManager::new(),
        schema.clone(),
        RowPolicyMode::PermissiveLocal,
    );
    let mut storage = seeded_memory_storage(&writer.schema_context().current_schema);
    let project_id = ObjectId::new();
    let branch_id = seed_branch_and_todo(&mut writer, &mut storage, &schema, project_id, "alice");
    let branch_name = branch_name_for(&schema, branch_id);

    let mut reader = create_query_manager(SyncManager::new(), schema);
    let rows = query_rows(
        &mut reader,
        &mut storage,
        QueryBuilder::new("todos").branch(branch_name).build(),
        Some(Session::new("alice")),
    );

    assert!(rows.is_empty());
}

#[test]
fn branch_select_true_still_requires_readable_backing_row() {
    let mut todo_policies = TablePolicies::default();
    todo_policies.for_branch = HashMap::from([(
        TableName::new("branches"),
        TablePolicies::new()
            .with_select(PolicyExpr::True)
            .with_insert(PolicyExpr::True),
    )]);
    let schema = branch_schema_with_todo_policies(todo_policies);
    let mut qm = create_query_manager_with_policy_mode(
        SyncManager::new(),
        schema.clone(),
        RowPolicyMode::Enforcing,
    );
    let mut storage = seeded_memory_storage(&qm.schema_context().current_schema);
    let branch_id = seed_branch_and_todo(&mut qm, &mut storage, &schema, ObjectId::new(), "alice");
    let branch_name = branch_name_for(&schema, branch_id);

    let rows = query_rows(
        &mut qm,
        &mut storage,
        QueryBuilder::new("todos").branch(branch_name).build(),
        Some(Session::new("bob")),
    );

    assert!(rows.is_empty());
}

#[test]
fn branch_select_reacts_when_backing_row_becomes_unreadable() {
    let backing_policies = TablePolicies::new()
        .with_select(PolicyExpr::Cmp {
            column: "ownerId".into(),
            op: CmpOp::Eq,
            value: PolicyValue::SessionRef(vec!["user_id".into()]),
        })
        .with_update(Some(PolicyExpr::True), PolicyExpr::True);
    let mut todo_policies = TablePolicies::default();
    todo_policies.for_branch = HashMap::from([(
        TableName::new("branches"),
        TablePolicies::new()
            .with_select(PolicyExpr::True)
            .with_insert(PolicyExpr::True),
    )]);
    let schema = branch_schema_with_backing_and_todo_policies(backing_policies, todo_policies);
    let mut qm = create_query_manager_with_policy_mode(
        SyncManager::new(),
        schema.clone(),
        RowPolicyMode::Enforcing,
    );
    let mut storage = seeded_memory_storage(&qm.schema_context().current_schema);
    let project_id = ObjectId::new();
    let branch_id = seed_branch_and_todo(&mut qm, &mut storage, &schema, project_id, "alice");
    let branch_name = branch_name_for(&schema, branch_id);
    let sub_id = qm
        .subscribe_with_session(
            QueryBuilder::new("todos").branch(branch_name).build(),
            Some(Session::new("alice")),
            None,
        )
        .expect("subscribe branch query");

    qm.process(&mut storage);
    assert_eq!(qm.get_subscription_results(sub_id).len(), 1);

    qm.update(
        &mut storage,
        branch_id,
        &[Value::Uuid(project_id), Value::Text("bob".into())],
    )
    .expect("update backing branch owner");
    qm.process(&mut storage);

    assert!(qm.get_subscription_results(sub_id).is_empty());
    qm.unsubscribe_with_sync(sub_id);
}

#[test]
fn branch_read_with_exists_branch_policy_can_match_branch_ref() {
    let mut todo_policies = TablePolicies::default();
    todo_policies.for_branch = HashMap::from([(
        TableName::new("branches"),
        TablePolicies::new().with_select(PolicyExpr::Exists {
            table: "approvals".into(),
            condition: Box::new(project_matches_branch_policy()),
        }),
    )]);
    let schema = branch_schema_with_approvals(todo_policies);
    let sync_manager = SyncManager::new();
    let mut qm = create_query_manager_with_policy_mode(
        sync_manager,
        schema.clone(),
        RowPolicyMode::Enforcing,
    );
    let mut storage = seeded_memory_storage(&qm.schema_context().current_schema);
    let project_id = ObjectId::new();
    let branch_id = seed_branch_and_todo(&mut qm, &mut storage, &schema, project_id, "alice");
    let branch_name = branch_name_for(&schema, branch_id);
    qm.insert_on_branch(
        &mut storage,
        "approvals",
        &branch_name,
        &[Value::Uuid(project_id)],
        None,
    )
    .expect("insert branch approval");

    let rows = query_rows(
        &mut qm,
        &mut storage,
        QueryBuilder::new("todos").branch(branch_name).build(),
        Some(Session::new("alice")),
    );

    assert_eq!(rows.len(), 1);
}

#[test]
fn branch_insert_uses_matching_branch_policy() {
    let (mut qm, mut storage, schema) = manager_with_branch_schema(true);
    let project_id = ObjectId::new();
    let branch_id = qm
        .insert(
            &mut storage,
            "branches",
            &[Value::Uuid(project_id), Value::Text("alice".into())],
        )
        .expect("insert branch row")
        .row_id;
    let branch_name = branch_name_for(&schema, branch_id);
    let write_context = WriteContext::from_session(Session::new("alice"));

    let inserted = qm
        .insert_on_branch(
            &mut storage,
            "todos",
            &branch_name,
            &[
                Value::Uuid(project_id),
                Value::Text("Write API docs".into()),
            ],
            Some(&write_context),
        )
        .expect("branch insert should use branch-scoped policy");

    assert_ne!(inserted.row_id, branch_id);
}

#[test]
fn branch_update_allows_missing_using_when_with_check_matches() {
    let mut todo_policies = TablePolicies::default();
    todo_policies.for_branch = HashMap::from([(
        TableName::new("branches"),
        TablePolicies::new().with_update(None, project_matches_branch_policy()),
    )]);
    let schema = branch_schema_with_todo_policies(todo_policies);
    let sync_manager = SyncManager::new();
    let mut qm = create_query_manager_with_policy_mode(
        sync_manager,
        schema.clone(),
        RowPolicyMode::Enforcing,
    );
    let mut storage = seeded_memory_storage(&qm.schema_context().current_schema);
    let project_id = ObjectId::new();
    let branch_id = qm
        .insert(
            &mut storage,
            "branches",
            &[Value::Uuid(project_id), Value::Text("alice".into())],
        )
        .expect("insert branch row")
        .row_id;
    let branch_name = branch_name_for(&schema, branch_id);
    let todo = qm
        .insert_on_branch(
            &mut storage,
            "todos",
            &branch_name,
            &[
                Value::Uuid(project_id),
                Value::Text("Write API docs".into()),
            ],
            None,
        )
        .expect("seed branch-visible todo");
    let row = storage
        .load_visible_region_row("todos", &branch_name, todo.row_id)
        .expect("load visible branch row")
        .expect("branch row should exist");
    let old_data = row.data.clone();
    let old_provenance = row.row_provenance();
    let write_context = WriteContext::from_session(Session::new("alice"));

    qm.write_existing_row_on_branch_with_schema_and_write_context(
        &mut storage,
        RowBranchWrite {
            table: "todos",
            branch: &branch_name,
            id: todo.row_id,
            values: &[
                Value::Uuid(project_id),
                Value::Text("Update API docs".into()),
            ],
            old_data_for_policy: &old_data,
            old_provenance_for_policy: &old_provenance,
        },
        &schema,
        Some(&write_context),
        true,
    )
    .expect("branch update should not require both update clauses");
}

#[test]
fn synced_branch_insert_uses_branch_policy_without_normal_insert_policy() {
    let (mut qm, mut storage, schema) = manager_with_branch_schema(true);
    let project_id = ObjectId::new();
    let branch_id = qm
        .insert(
            &mut storage,
            "branches",
            &[Value::Uuid(project_id), Value::Text("alice".into())],
        )
        .expect("insert branch row")
        .row_id;
    let branch_name = branch_name_for(&schema, branch_id);
    let todos_descriptor = schema
        .get(&TableName::new("todos"))
        .unwrap()
        .columns
        .clone();
    let todo_id = create_test_row(&mut storage, Some(todos_metadata()));
    let client_id = ClientId::new();
    connect_client(&mut qm, &storage, client_id);
    qm.sync_manager_mut()
        .set_client_session(client_id, Session::new("alice"));
    set_client_query_scope(
        &mut qm,
        &storage,
        client_id,
        QueryId(1),
        HashSet::from([(todo_id, BranchName::new(&branch_name))]),
        None,
    );
    qm.sync_manager_mut().take_outbox();

    let content = encode_row(
        &todos_descriptor,
        &[
            Value::Uuid(project_id),
            Value::Text("Write branch sync docs".into()),
        ],
    )
    .unwrap();
    let commit = stored_row_commit(
        smallvec![],
        content,
        1_000,
        ObjectId::new().to_string(),
        None,
    );
    qm.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Client(client_id),
        payload: row_batch_created_payload(
            todo_id,
            &branch_name,
            Some(RowMetadata {
                id: todo_id,
                metadata: todos_metadata(),
            }),
            &commit,
        ),
    });

    for _ in 0..10 {
        qm.process(&mut storage);
    }

    let batch_id = row_batch_id_for_commit(todo_id, &branch_name, &commit);
    let outbox = qm.sync_manager_mut().take_outbox();
    assert!(!client_write_was_rejected(&outbox, client_id, batch_id));
    assert!(
        test_row_tip_ids(&storage, todo_id, &branch_name)
            .unwrap()
            .contains(&batch_id)
    );
}

#[test]
fn synced_branch_delete_uses_branch_policy_without_normal_delete_policy() {
    let mut todo_policies = TablePolicies::default();
    todo_policies.for_branch = HashMap::from([(
        TableName::new("branches"),
        TablePolicies::new()
            .with_insert(project_matches_branch_policy())
            .with_delete(project_matches_branch_policy()),
    )]);
    let schema = branch_schema_with_todo_policies(todo_policies);
    let mut qm = create_query_manager_with_policy_mode(
        SyncManager::new(),
        schema.clone(),
        RowPolicyMode::Enforcing,
    );
    let mut storage = seeded_memory_storage(&qm.schema_context().current_schema);
    let project_id = ObjectId::new();
    let branch_id = qm
        .insert(
            &mut storage,
            "branches",
            &[Value::Uuid(project_id), Value::Text("alice".into())],
        )
        .expect("insert branch row")
        .row_id;
    let branch_name = branch_name_for(&schema, branch_id);
    let todo = qm
        .insert_on_branch(
            &mut storage,
            "todos",
            &branch_name,
            &[
                Value::Uuid(project_id),
                Value::Text("Write branch sync docs".into()),
            ],
            None,
        )
        .expect("seed branch-visible todo");
    let todos_descriptor = schema
        .get(&TableName::new("todos"))
        .unwrap()
        .columns
        .clone();
    let client_id = ClientId::new();
    connect_client(&mut qm, &storage, client_id);
    qm.sync_manager_mut()
        .set_client_session(client_id, Session::new("alice"));
    set_client_query_scope(
        &mut qm,
        &storage,
        client_id,
        QueryId(1),
        HashSet::from([(todo.row_id, BranchName::new(&branch_name))]),
        None,
    );
    qm.sync_manager_mut().take_outbox();

    let content = encode_row(
        &todos_descriptor,
        &[
            Value::Uuid(project_id),
            Value::Text("Write branch sync docs".into()),
        ],
    )
    .unwrap();
    let commit = stored_row_commit(
        smallvec![todo.batch_id],
        content,
        2_000,
        ObjectId::new().to_string(),
        Some(DeleteKind::Soft),
    );
    qm.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Client(client_id),
        payload: row_batch_created_payload(
            todo.row_id,
            &branch_name,
            Some(RowMetadata {
                id: todo.row_id,
                metadata: todos_metadata(),
            }),
            &commit,
        ),
    });

    for _ in 0..10 {
        qm.process(&mut storage);
    }

    let batch_id = row_batch_id_for_commit(todo.row_id, &branch_name, &commit);
    let outbox = qm.sync_manager_mut().take_outbox();
    assert!(!client_write_was_rejected(&outbox, client_id, batch_id));
    assert!(
        test_row_tip_ids(&storage, todo.row_id, &branch_name)
            .unwrap()
            .contains(&batch_id)
    );
}

#[test]
fn synced_branch_update_on_uuid_branch_without_for_branch_denies_normal_policy_bypass() {
    let schema = SchemaBuilder::new()
        .table(
            TableSchema::builder("branches")
                .column("projectId", ColumnType::Uuid)
                .column("ownerId", ColumnType::Text)
                .policies(
                    TablePolicies::new()
                        .with_select(PolicyExpr::True)
                        .with_insert(PolicyExpr::True),
                ),
        )
        .table(
            TableSchema::builder("todos")
                .column("projectId", ColumnType::Uuid)
                .column("title", ColumnType::Text)
                .policies(
                    TablePolicies::new().with_update(Some(PolicyExpr::True), PolicyExpr::True),
                ),
        )
        .build();
    let mut qm = create_query_manager_with_policy_mode(
        SyncManager::new(),
        schema.clone(),
        RowPolicyMode::Enforcing,
    );
    let mut storage = seeded_memory_storage(&qm.schema_context().current_schema);
    let project_id = ObjectId::new();
    let branch_id = qm
        .insert(
            &mut storage,
            "branches",
            &[Value::Uuid(project_id), Value::Text("alice".into())],
        )
        .expect("insert branch row")
        .row_id;
    let branch_name = branch_name_for(&schema, branch_id);
    let todos_descriptor = schema
        .get(&TableName::new("todos"))
        .unwrap()
        .columns
        .clone();
    let todo_id = create_test_row(&mut storage, Some(todos_metadata()));
    let seed_content = encode_row(
        &todos_descriptor,
        &[
            Value::Uuid(project_id),
            Value::Text("Original branch title".into()),
        ],
    )
    .unwrap();
    let seed_commit = stored_row_commit(
        smallvec![],
        seed_content,
        1_000,
        ObjectId::new().to_string(),
        None,
    );
    let seed_batch_id = row_batch_id_for_commit(todo_id, &branch_name, &seed_commit);
    apply_test_row_batch(
        &mut storage,
        todo_id,
        &branch_name,
        seed_commit.to_row(todo_id, &branch_name, RowState::VisibleDirect),
    )
    .expect("seed branch row");
    let client_id = ClientId::new();
    connect_client(&mut qm, &storage, client_id);
    qm.sync_manager_mut()
        .set_client_session(client_id, Session::new("alice"));
    set_client_query_scope(
        &mut qm,
        &storage,
        client_id,
        QueryId(1),
        HashSet::from([(todo_id, BranchName::new(&branch_name))]),
        None,
    );
    qm.sync_manager_mut().take_outbox();

    let update_content = encode_row(
        &todos_descriptor,
        &[
            Value::Uuid(project_id),
            Value::Text("Unauthorized branch title".into()),
        ],
    )
    .unwrap();
    let update_commit = stored_row_commit(
        smallvec![seed_batch_id],
        update_content,
        2_000,
        ObjectId::new().to_string(),
        None,
    );
    qm.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Client(client_id),
        payload: row_batch_created_payload(
            todo_id,
            &branch_name,
            Some(RowMetadata {
                id: todo_id,
                metadata: todos_metadata(),
            }),
            &update_commit,
        ),
    });

    for _ in 0..10 {
        qm.process(&mut storage);
    }

    let update_batch_id = row_batch_id_for_commit(todo_id, &branch_name, &update_commit);
    let outbox = qm.sync_manager_mut().take_outbox();
    assert!(client_write_was_rejected(
        &outbox,
        client_id,
        update_batch_id
    ));
    assert!(
        !test_row_tip_ids(&storage, todo_id, &branch_name)
            .unwrap()
            .contains(&update_batch_id)
    );
}

#[test]
fn local_branch_insert_uses_current_permissions_after_lens_transform() {
    let legacy = branch_schema_without_backing_policy(TablePolicies::new());
    let mut auth_todo_policies = TablePolicies::default();
    auth_todo_policies.for_branch = HashMap::from([(
        TableName::new("branches"),
        TablePolicies::new().with_insert(PolicyExpr::Cmp {
            column: "ownerId".into(),
            op: CmpOp::Eq,
            value: PolicyValue::SessionRef(vec!["user_id".into()]),
        }),
    )]);
    let auth = SchemaBuilder::new()
        .table(
            TableSchema::builder("branches")
                .column("projectId", ColumnType::Uuid)
                .column("ownerId", ColumnType::Text)
                .policies(TablePolicies::new().with_select(PolicyExpr::Cmp {
                    column: "ownerId".into(),
                    op: CmpOp::Eq,
                    value: PolicyValue::SessionRef(vec!["user_id".into()]),
                })),
        )
        .table(
            TableSchema::builder("todos")
                .column("projectId", ColumnType::Uuid)
                .column("title", ColumnType::Text)
                .column("ownerId", ColumnType::Text)
                .policies(auth_todo_policies),
        )
        .build();
    let legacy_hash = SchemaHash::compute(&legacy);
    let auth_hash = SchemaHash::compute(&auth);
    let mut transform = LensTransform::new();
    transform.push(
        LensOp::AddColumn {
            table: "todos".to_string(),
            column: "ownerId".to_string(),
            column_type: ColumnType::Text,
            default: Value::Text("alice".to_string()),
        },
        false,
    );
    let lens = Lens::new(legacy_hash, auth_hash, transform);
    let mut qm = create_query_manager_with_policy_mode(
        SyncManager::new(),
        legacy.clone(),
        RowPolicyMode::Enforcing,
    );
    qm.set_known_schemas(Arc::new(HashMap::from([
        (legacy_hash, legacy.clone()),
        (auth_hash, auth.clone()),
    ])));
    qm.register_lens(lens);
    qm.set_authorization_schema(auth);
    let mut storage = seeded_memory_storage(&qm.schema_context().current_schema);
    let project_id = ObjectId::new();
    let branch_id = qm
        .insert(
            &mut storage,
            "branches",
            &[Value::Uuid(project_id), Value::Text("alice".into())],
        )
        .expect("insert branch row")
        .row_id;
    let branch_name = branch_name_for(&legacy, branch_id);

    qm.insert_on_branch(
        &mut storage,
        "todos",
        &branch_name,
        &[
            Value::Uuid(project_id),
            Value::Text("Write lens-aware branch docs".into()),
        ],
        Some(&WriteContext::from_session(Session::new("alice"))),
    )
    .expect("branch policy should evaluate against transformed authorization row");

    let err = qm
        .insert_on_branch(
            &mut storage,
            "todos",
            &branch_name,
            &[
                Value::Uuid(project_id),
                Value::Text("Bob should not write this".into()),
            ],
            Some(&WriteContext::from_session(Session::new("bob"))),
        )
        .expect_err("bob should not satisfy transformed owner policy");
    assert!(matches!(
        err,
        QueryError::PolicyDenied {
            table,
            operation: Operation::Insert
        } if table == TableName::new("todos")
    ));
}

fn branch_ref_lens_schemas() -> (Schema, Schema, Lens, SchemaHash, SchemaHash) {
    let legacy = SchemaBuilder::new()
        .table(TableSchema::builder("branches").column("projectId", ColumnType::Uuid))
        .table(
            TableSchema::builder("todos")
                .column("projectId", ColumnType::Uuid)
                .column("title", ColumnType::Text),
        )
        .build();
    let mut auth_todo_policies = TablePolicies::default();
    auth_todo_policies.for_branch = HashMap::from([(
        TableName::new("branches"),
        TablePolicies::new().with_insert(PolicyExpr::Cmp {
            column: "createdBy".into(),
            op: CmpOp::Eq,
            value: PolicyValue::BranchRef("ownerId".into()),
        }),
    )]);
    let auth = SchemaBuilder::new()
        .table(
            TableSchema::builder("branches")
                .column("projectId", ColumnType::Uuid)
                .column("ownerId", ColumnType::Text)
                .policies(
                    TablePolicies::new()
                        .with_select(PolicyExpr::True)
                        .with_insert(PolicyExpr::True),
                ),
        )
        .table(
            TableSchema::builder("todos")
                .column("projectId", ColumnType::Uuid)
                .column("title", ColumnType::Text)
                .column("createdBy", ColumnType::Text)
                .policies(auth_todo_policies),
        )
        .build();
    let legacy_hash = SchemaHash::compute(&legacy);
    let auth_hash = SchemaHash::compute(&auth);
    let mut transform = LensTransform::new();
    transform.push(
        LensOp::AddColumn {
            table: "branches".to_string(),
            column: "ownerId".to_string(),
            column_type: ColumnType::Text,
            default: Value::Text("alice".to_string()),
        },
        false,
    );
    transform.push(
        LensOp::AddColumn {
            table: "todos".to_string(),
            column: "createdBy".to_string(),
            column_type: ColumnType::Text,
            default: Value::Text("alice".to_string()),
        },
        false,
    );
    let lens = Lens::new(legacy_hash, auth_hash, transform);
    (legacy, auth, lens, legacy_hash, auth_hash)
}

#[test]
fn local_branch_insert_transforms_backing_row_before_branch_ref_policy() {
    let (legacy, auth, lens, legacy_hash, auth_hash) = branch_ref_lens_schemas();
    let mut qm = create_query_manager_with_policy_mode(
        SyncManager::new(),
        legacy.clone(),
        RowPolicyMode::PermissiveLocal,
    );
    qm.set_known_schemas(Arc::new(HashMap::from([
        (legacy_hash, legacy.clone()),
        (auth_hash, auth.clone()),
    ])));
    qm.register_lens(lens);
    let mut storage = seeded_memory_storage(&qm.schema_context().current_schema);
    let project_id = ObjectId::new();
    let branch_id = qm
        .insert(&mut storage, "branches", &[Value::Uuid(project_id)])
        .expect("insert legacy branch row")
        .row_id;
    let branch_name = branch_name_for(&legacy, branch_id);
    qm.set_authorization_schema(auth);

    qm.insert_on_branch(
        &mut storage,
        "todos",
        &branch_name,
        &[
            Value::Uuid(project_id),
            Value::Text("Write branch-ref lens docs".into()),
        ],
        Some(&WriteContext::from_session(Session::new("alice"))),
    )
    .expect("branch ref should use transformed backing row");
}

#[test]
fn synced_branch_insert_transforms_backing_row_before_branch_ref_policy() {
    let (legacy, auth, lens, legacy_hash, auth_hash) = branch_ref_lens_schemas();
    let mut qm = create_query_manager_with_policy_mode(
        SyncManager::new(),
        legacy.clone(),
        RowPolicyMode::PermissiveLocal,
    );
    qm.set_known_schemas(Arc::new(HashMap::from([
        (legacy_hash, legacy.clone()),
        (auth_hash, auth.clone()),
    ])));
    qm.register_lens(lens);
    let mut storage = seeded_memory_storage(&qm.schema_context().current_schema);
    let project_id = ObjectId::new();
    let branch_id = qm
        .insert(&mut storage, "branches", &[Value::Uuid(project_id)])
        .expect("insert legacy branch row")
        .row_id;
    let branch_name = branch_name_for(&legacy, branch_id);
    qm.set_authorization_schema(auth);

    let todos_descriptor = legacy
        .get(&TableName::new("todos"))
        .unwrap()
        .columns
        .clone();
    let todo_id = create_test_row(&mut storage, Some(todos_metadata()));
    let client_id = ClientId::new();
    connect_client(&mut qm, &storage, client_id);
    qm.sync_manager_mut()
        .set_client_session(client_id, Session::new("alice"));
    set_client_query_scope(
        &mut qm,
        &storage,
        client_id,
        QueryId(1),
        HashSet::from([(todo_id, BranchName::new(&branch_name))]),
        None,
    );
    qm.sync_manager_mut().take_outbox();

    let content = encode_row(
        &todos_descriptor,
        &[
            Value::Uuid(project_id),
            Value::Text("Write branch-ref sync docs".into()),
        ],
    )
    .unwrap();
    let commit = stored_row_commit(
        smallvec![],
        content,
        1_000,
        ObjectId::new().to_string(),
        None,
    );
    qm.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Client(client_id),
        payload: row_batch_created_payload(
            todo_id,
            &branch_name,
            Some(RowMetadata {
                id: todo_id,
                metadata: todos_metadata(),
            }),
            &commit,
        ),
    });

    for _ in 0..10 {
        qm.process(&mut storage);
    }

    let batch_id = row_batch_id_for_commit(todo_id, &branch_name, &commit);
    let outbox = qm.sync_manager_mut().take_outbox();
    assert!(!client_write_was_rejected(&outbox, client_id, batch_id));
    assert!(
        test_row_tip_ids(&storage, todo_id, &branch_name)
            .unwrap()
            .contains(&batch_id)
    );
}
