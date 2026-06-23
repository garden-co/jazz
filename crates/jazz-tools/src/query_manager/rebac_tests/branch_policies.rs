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
