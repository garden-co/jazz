use super::*;
use crate::query_manager::query::Query;

fn branch_permissions_schema() -> Schema {
    let project_policies = permissions(|p| {
        p.allow_read().always();
        p.allow_insert().always();
    });
    let branch_policies = permissions(|p| {
        p.allow_read()
            .where_(pe::eq("ownerId", pe::session("user_id")));
        p.allow_insert().always();
    });
    let todo_policies = permissions(|p| {
        p.allow_read().always();
        p.allow_insert().always();
    });

    SchemaBuilder::new()
        .table(
            TableSchema::builder("projects")
                .column("name", ColumnType::Text)
                .column("ownerId", ColumnType::Text)
                .policies(project_policies),
        )
        .table(
            TableSchema::builder("branches")
                .fk_column("projectId", "projects")
                .column("ownerId", ColumnType::Text)
                .policies(branch_policies),
        )
        .table(
            TableSchema::builder("todos")
                .fk_column("projectId", "projects")
                .column("title", ColumnType::Text)
                .column("ownerId", ColumnType::Text)
                .policies(todo_policies),
        )
        .build()
}

fn branch_policy_map() -> crate::query_manager::types::BranchPolicies {
    HashMap::from([(
        TableName::new("branches"),
        HashMap::from([(
            TableName::new("todos"),
            permissions(|p| {
                p.allow_read()
                    .where_(pe::eq("projectId", pe::branch("projectId")));
                p.allow_insert().where_(pe::all_of([
                    pe::eq("projectId", pe::branch("projectId")),
                    pe::eq("ownerId", pe::session("user_id")),
                ]));
                p.allow_update()
                    .where_(pe::eq("projectId", pe::branch("projectId")));
                p.allow_delete()
                    .where_(pe::eq("projectId", pe::branch("projectId")));
            }),
        )]),
    )])
}

fn get_branch_for_user_branch(qm: &QueryManager, user_branch: &str) -> String {
    ComposedBranchName::new(
        &qm.schema_context().env,
        qm.schema_context().current_hash,
        user_branch,
    )
    .to_branch_name()
    .as_str()
    .to_string()
}

fn execute_query_with_session<H: Storage>(
    qm: &mut QueryManager,
    storage: &mut H,
    query: Query,
    session: Session,
) -> Result<Vec<(ObjectId, Vec<Value>)>, QueryError> {
    let sub_id = qm.subscribe_with_session(query, Some(session), None)?;
    qm.process(storage);
    let results = qm.get_subscription_results(sub_id);
    qm.unsubscribe_with_sync(sub_id);
    Ok(results)
}

fn setup_branch_permissions() -> (QueryManager, MemoryStorage, ObjectId, ObjectId, String) {
    let schema = branch_permissions_schema();
    let mut qm = create_query_manager(SyncManager::new(), schema.clone());
    qm.set_authorization_schema_with_branch_policies(schema, branch_policy_map());
    let mut storage = MemoryStorage::new();

    let project = qm
        .insert(
            &mut storage,
            "projects",
            &[Value::Text("Project".into()), Value::Text("alice".into())],
        )
        .expect("insert project");
    let branch_row = qm
        .insert(
            &mut storage,
            "branches",
            &[Value::Uuid(project.row_id), Value::Text("alice".into())],
        )
        .expect("insert branch row");
    let branch = get_branch_for_user_branch(&qm, &branch_row.row_id.to_string());

    (qm, storage, project.row_id, branch_row.row_id, branch)
}

fn visible_todos_on_branch(
    qm: &mut QueryManager,
    storage: &mut MemoryStorage,
    branch: &str,
    session: &str,
) -> Vec<(ObjectId, Vec<Value>)> {
    let query = qm.query("todos").branch(branch).build();
    execute_query_with_session(qm, storage, query, Session::new(session)).expect("query todos")
}

fn insert_todo_on_branch_as(
    qm: &mut QueryManager,
    storage: &mut MemoryStorage,
    branch: &str,
    project_id: ObjectId,
    title: &str,
    owner_id: &str,
    session: &str,
) -> Result<crate::query_manager::manager::InsertResult, QueryError> {
    let write_context = WriteContext::from_session(Session::new(session));
    qm.insert_on_branch(
        storage,
        "todos",
        branch,
        &[
            Value::Uuid(project_id),
            Value::Text(title.into()),
            Value::Text(owner_id.into()),
        ],
        Some(&write_context),
    )
}

fn update_todo_on_branch_as(
    qm: &mut QueryManager,
    storage: &mut MemoryStorage,
    branch: &str,
    row_id: ObjectId,
    project_id: ObjectId,
    title: &str,
    owner_id: &str,
    session: &str,
) -> Result<BatchId, QueryError> {
    let current_row = storage
        .load_visible_region_row("todos", branch, row_id)
        .expect("load row")
        .expect("row exists");
    let write_context = WriteContext::from_session(Session::new(session));
    qm.write_existing_row_on_branch_with_schema_and_write_context(
        storage,
        crate::query_manager::writes::RowBranchWrite {
            table: "todos",
            branch,
            id: row_id,
            values: &[
                Value::Uuid(project_id),
                Value::Text(title.into()),
                Value::Text(owner_id.into()),
            ],
            old_data_for_policy: &current_row.data,
            old_provenance_for_policy: &current_row.row_provenance(),
        },
        qm.schema.clone().as_ref(),
        Some(&write_context),
        true,
    )
}

fn delete_todo_on_branch_as(
    qm: &mut QueryManager,
    storage: &mut MemoryStorage,
    branch: &str,
    row_id: ObjectId,
    session: &str,
) -> Result<crate::query_manager::manager::DeleteHandle, QueryError> {
    let current_row = storage
        .load_visible_region_row("todos", branch, row_id)
        .expect("load row")
        .expect("row exists");
    let write_context = WriteContext::from_session(Session::new(session));
    qm.delete_existing_row_on_branch_with_schema_and_write_context(
        storage,
        crate::query_manager::writes::RowBranchDelete {
            table: "todos",
            branch,
            id: row_id,
            old_data_for_policy: &current_row.data,
            old_provenance_for_policy: &current_row.row_provenance(),
        },
        qm.schema.clone().as_ref(),
        Some(&write_context),
        true,
    )
}

#[test]
fn branch_read_policy_allows_owner_to_read_matching_rows() {
    let (mut qm, mut storage, project_id, _branch_row_id, branch) = setup_branch_permissions();
    qm.insert_on_branch(
        &mut storage,
        "todos",
        &branch,
        &[
            Value::Uuid(project_id),
            Value::Text("Write API docs".into()),
            Value::Text("alice".into()),
        ],
        None,
    )
    .expect("insert branch todo");

    let query = qm.query("todos").branch(&branch).build();
    let rows = execute_query_with_session(&mut qm, &mut storage, query, Session::new("alice"))
        .expect("query todos");

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].1[1], Value::Text("Write API docs".into()));
}

#[test]
fn branch_read_policy_denies_when_backing_row_is_not_readable() {
    let (mut qm, mut storage, project_id, _branch_row_id, branch) = setup_branch_permissions();
    qm.insert_on_branch(
        &mut storage,
        "todos",
        &branch,
        &[
            Value::Uuid(project_id),
            Value::Text("Hidden draft".into()),
            Value::Text("alice".into()),
        ],
        None,
    )
    .expect("insert branch todo");

    let query = qm.query("todos").branch(&branch).build();
    let rows = execute_query_with_session(&mut qm, &mut storage, query, Session::new("bob"))
        .expect("query todos");

    assert!(
        rows.is_empty(),
        "branch reads must not fall back to normal todos allowRead"
    );
}

#[test]
fn branch_reads_are_isolated_from_main_reads() {
    let (mut qm, mut storage, project_id, _branch_row_id, branch) = setup_branch_permissions();
    qm.insert_on_branch(
        &mut storage,
        "todos",
        &branch,
        &[
            Value::Uuid(project_id),
            Value::Text("Branch todo".into()),
            Value::Text("alice".into()),
        ],
        None,
    )
    .expect("insert branch todo");
    qm.insert(
        &mut storage,
        "todos",
        &[
            Value::Uuid(project_id),
            Value::Text("Main todo".into()),
            Value::Text("alice".into()),
        ],
    )
    .expect("insert main todo");

    let main_query = qm.query("todos").build();
    let main_rows =
        execute_query_with_session(&mut qm, &mut storage, main_query, Session::new("alice"))
            .expect("query main todos");
    let branch_query = qm.query("todos").branch(&branch).build();
    let branch_rows =
        execute_query_with_session(&mut qm, &mut storage, branch_query, Session::new("alice"))
            .expect("query branch todos");

    assert_eq!(main_rows.len(), 1);
    assert_eq!(main_rows[0].1[1], Value::Text("Main todo".into()));
    assert_eq!(branch_rows.len(), 1);
    assert_eq!(branch_rows[0].1[1], Value::Text("Branch todo".into()));
}

#[test]
fn branch_insert_policy_allows_owner_matching_branch_project_and_owner() {
    let (mut qm, mut storage, project_id, _branch_row_id, branch) = setup_branch_permissions();

    let inserted = insert_todo_on_branch_as(
        &mut qm,
        &mut storage,
        &branch,
        project_id,
        "Write API docs",
        "alice",
        "alice",
    )
    .expect("branch insert should be accepted");

    let rows = visible_todos_on_branch(&mut qm, &mut storage, &branch, "alice");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, inserted.row_id);
    assert_eq!(rows[0].1[1], Value::Text("Write API docs".into()));
}

#[test]
fn branch_insert_policy_denies_when_backing_row_is_not_readable() {
    let (mut qm, mut storage, project_id, _branch_row_id, branch) = setup_branch_permissions();

    let denied = insert_todo_on_branch_as(
        &mut qm,
        &mut storage,
        &branch,
        project_id,
        "Hidden draft",
        "bob",
        "bob",
    )
    .expect_err("branch insert should be rejected");

    assert!(matches!(
        denied,
        QueryError::PolicyDenied {
            operation: Operation::Insert,
            ..
        }
    ));
    assert!(visible_todos_on_branch(&mut qm, &mut storage, &branch, "alice").is_empty());
}

#[test]
fn branch_update_policy_allows_owner_when_row_matches_branch_project() {
    let (mut qm, mut storage, project_id, _branch_row_id, branch) = setup_branch_permissions();
    let inserted = insert_todo_on_branch_as(
        &mut qm,
        &mut storage,
        &branch,
        project_id,
        "Draft",
        "alice",
        "alice",
    )
    .expect("branch insert should be accepted");

    update_todo_on_branch_as(
        &mut qm,
        &mut storage,
        &branch,
        inserted.row_id,
        project_id,
        "Updated draft",
        "alice",
        "alice",
    )
    .expect("branch update should be accepted");

    let rows = visible_todos_on_branch(&mut qm, &mut storage, &branch, "alice");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].1[1], Value::Text("Updated draft".into()));
}

#[test]
fn branch_update_policy_denies_when_backing_row_is_not_readable() {
    let (mut qm, mut storage, project_id, _branch_row_id, branch) = setup_branch_permissions();
    let inserted = insert_todo_on_branch_as(
        &mut qm,
        &mut storage,
        &branch,
        project_id,
        "Draft",
        "alice",
        "alice",
    )
    .expect("branch insert should be accepted");

    let denied = update_todo_on_branch_as(
        &mut qm,
        &mut storage,
        &branch,
        inserted.row_id,
        project_id,
        "Bob edit",
        "alice",
        "bob",
    )
    .expect_err("branch update should be rejected");

    assert!(matches!(
        denied,
        QueryError::PolicyDenied {
            operation: Operation::Update,
            ..
        }
    ));
    let rows = visible_todos_on_branch(&mut qm, &mut storage, &branch, "alice");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].1[1], Value::Text("Draft".into()));
}

#[test]
fn branch_delete_policy_allows_owner_when_row_matches_branch_project() {
    let (mut qm, mut storage, project_id, _branch_row_id, branch) = setup_branch_permissions();
    let inserted = insert_todo_on_branch_as(
        &mut qm,
        &mut storage,
        &branch,
        project_id,
        "Draft",
        "alice",
        "alice",
    )
    .expect("branch insert should be accepted");

    delete_todo_on_branch_as(&mut qm, &mut storage, &branch, inserted.row_id, "alice")
        .expect("branch delete should be accepted");

    assert!(visible_todos_on_branch(&mut qm, &mut storage, &branch, "alice").is_empty());
}

#[test]
fn branch_delete_policy_denies_when_backing_row_is_not_readable() {
    let (mut qm, mut storage, project_id, _branch_row_id, branch) = setup_branch_permissions();
    let inserted = insert_todo_on_branch_as(
        &mut qm,
        &mut storage,
        &branch,
        project_id,
        "Draft",
        "alice",
        "alice",
    )
    .expect("branch insert should be accepted");

    let denied = delete_todo_on_branch_as(&mut qm, &mut storage, &branch, inserted.row_id, "bob")
        .expect_err("branch delete should be rejected");

    assert!(matches!(
        denied,
        QueryError::PolicyDenied {
            operation: Operation::Delete,
            ..
        }
    ));
    let rows = visible_todos_on_branch(&mut qm, &mut storage, &branch, "alice");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].1[1], Value::Text("Draft".into()));
}
