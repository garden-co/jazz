use std::collections::{HashMap, HashSet};

use crate::object::ObjectId;
use crate::query_manager::policy::{CmpOp, PolicyValue};
use crate::query_manager::query::QueryBuilder;
use crate::query_manager::relation_ir::{
    ColumnRef, PredicateCmpOp, PredicateExpr, RelExpr, ValueRef,
};
use crate::query_manager::types::{OperationPolicy, SchemaBuilder};
use crate::query_manager::writes::RowBranchWrite;
use crate::sync_manager::ServerId;

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

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum SelectPolicyMode {
    True,
    False,
    Missing,
}

fn policies_with_select_mode(mode: SelectPolicyMode) -> TablePolicies {
    match mode {
        SelectPolicyMode::True => TablePolicies::new().with_select(PolicyExpr::True),
        SelectPolicyMode::False => TablePolicies::new().with_select(PolicyExpr::False),
        SelectPolicyMode::Missing => TablePolicies::new(),
    }
}

fn branch_query_matrix_schema(
    normal_select: SelectPolicyMode,
    branch_select: SelectPolicyMode,
    backing_select: SelectPolicyMode,
) -> Schema {
    let mut todo_policies = policies_with_select_mode(normal_select);
    todo_policies.for_branch = HashMap::from([(
        TableName::new("branches"),
        policies_with_select_mode(branch_select),
    )]);

    branch_schema_with_backing_and_todo_policies(
        policies_with_select_mode(backing_select),
        todo_policies,
    )
}

fn branch_schema_with_backing_project_dependency() -> Schema {
    SchemaBuilder::new()
        .table(
            TableSchema::builder("projects")
                .column("ownerId", ColumnType::Text)
                .policies(
                    TablePolicies::new()
                        .with_select(PolicyExpr::Cmp {
                            column: "ownerId".into(),
                            op: CmpOp::Eq,
                            value: PolicyValue::SessionRef(vec!["user_id".into()]),
                        })
                        .with_insert(PolicyExpr::True)
                        .with_update(Some(PolicyExpr::True), PolicyExpr::True),
                ),
        )
        .table(
            TableSchema::builder("branches")
                .fk_column("projectId", "projects")
                .column("ownerId", ColumnType::Text)
                .policies(
                    TablePolicies::new()
                        .with_select(PolicyExpr::Inherits {
                            operation: Operation::Select,
                            via_column: "projectId".into(),
                            max_depth: None,
                        })
                        .with_insert(PolicyExpr::True),
                ),
        )
        .table(
            TableSchema::builder("todos")
                .column("projectId", ColumnType::Uuid)
                .column("title", ColumnType::Text)
                .policies({
                    let mut todo_policies = TablePolicies::new().with_insert(PolicyExpr::True);
                    todo_policies.for_branch = HashMap::from([(
                        TableName::new("branches"),
                        TablePolicies::new().with_select(PolicyExpr::True),
                    )]);
                    todo_policies
                }),
        )
        .build()
}

fn branch_schema_with_parent_branch_policy_dependency() -> Schema {
    let project_matches_branch_owner = PolicyExpr::Cmp {
        column: "ownerId".into(),
        op: CmpOp::Eq,
        value: PolicyValue::BranchRef("ownerId".into()),
    };

    SchemaBuilder::new()
        .table(
            TableSchema::builder("branches")
                .column("ownerId", ColumnType::Text)
                .policies(
                    TablePolicies::new()
                        .with_select(PolicyExpr::True)
                        .with_insert(PolicyExpr::True),
                ),
        )
        .table(
            TableSchema::builder("projects")
                .column("ownerId", ColumnType::Text)
                .policies({
                    let mut project_policies = TablePolicies::new().with_select(PolicyExpr::False);
                    project_policies.for_branch = HashMap::from([(
                        TableName::new("branches"),
                        TablePolicies::new()
                            .with_select(project_matches_branch_owner)
                            .with_insert(PolicyExpr::True),
                    )]);
                    project_policies
                }),
        )
        .table(
            TableSchema::builder("todos")
                .fk_column("projectId", "projects")
                .column("title", ColumnType::Text)
                .policies({
                    let mut todo_policies = TablePolicies::new().with_insert(PolicyExpr::True);
                    todo_policies.for_branch = HashMap::from([(
                        TableName::new("branches"),
                        TablePolicies::new()
                            .with_select(PolicyExpr::Inherits {
                                operation: Operation::Select,
                                via_column: "projectId".into(),
                                max_depth: None,
                            })
                            .with_insert(PolicyExpr::True),
                    )]);
                    todo_policies
                }),
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

fn branch_schema_with_todo_branch_read_write_policies() -> Schema {
    let matches_branch_for_select = project_matches_branch_policy();
    let matches_branch_for_insert = project_matches_branch_policy();
    let matches_branch_for_update_using = project_matches_branch_policy();
    let matches_branch_for_update_check = project_matches_branch_policy();
    let matches_branch_for_delete = project_matches_branch_policy();
    let mut todo_policies = TablePolicies::default();
    todo_policies.for_branch = HashMap::from([(
        TableName::new("branches"),
        TablePolicies::new()
            .with_select(matches_branch_for_select)
            .with_insert(matches_branch_for_insert)
            .with_update(
                Some(matches_branch_for_update_using),
                matches_branch_for_update_check,
            )
            .with_delete(matches_branch_for_delete),
    )]);

    branch_schema_with_todo_policies(todo_policies)
}

fn todos_metadata() -> HashMap<String, String> {
    HashMap::from([(MetadataKey::Table.to_string(), "todos".to_string())])
}

fn projects_metadata() -> HashMap<String, String> {
    HashMap::from([(MetadataKey::Table.to_string(), "projects".to_string())])
}

fn branches_metadata() -> HashMap<String, String> {
    HashMap::from([(MetadataKey::Table.to_string(), "branches".to_string())])
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

fn strip_test_policies(schema: &Schema) -> Schema {
    schema
        .iter()
        .map(|(table_name, table_schema)| {
            let mut structural = table_schema.clone();
            structural.policies = TablePolicies::default();
            (*table_name, structural)
        })
        .collect()
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
// Missing branch policy:
//
//   backing branch select passes
//   todos.for_branch["branches"] is absent
//
//   Query                         Expected
//   ----------------------------  --------
//   internal branch-scoped todos  0 rows
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
// Matrix shape:
//
//   normal select  x  branch select  x  backing select
//   true/false/missing for each axis
//
//   Query                         Controlling policy
//   ----------------------------  ------------------
//   todos on main                 normal select
//   todos on branch               branch select AND backing select
fn branch_read_matrix_covers_missing_normal_branch_and_backing_select_policies() {
    let modes = [
        SelectPolicyMode::True,
        SelectPolicyMode::False,
        SelectPolicyMode::Missing,
    ];

    for normal_select in modes {
        for branch_select in modes {
            for backing_select in modes {
                let schema =
                    branch_query_matrix_schema(normal_select, branch_select, backing_select);
                let mut writer = create_query_manager_with_policy_mode(
                    SyncManager::new(),
                    schema.clone(),
                    RowPolicyMode::PermissiveLocal,
                );
                let mut storage = seeded_memory_storage(&writer.schema_context().current_schema);
                let project_id = ObjectId::new();
                writer
                    .insert(
                        &mut storage,
                        "todos",
                        &[Value::Uuid(project_id), Value::Text("Main todo".into())],
                    )
                    .expect("seed main todo");
                let branch_id =
                    seed_branch_and_todo(&mut writer, &mut storage, &schema, project_id, "alice");
                let branch_name = branch_name_for(&schema, branch_id);
                let mut reader = create_query_manager_with_policy_mode(
                    SyncManager::new(),
                    schema,
                    RowPolicyMode::Enforcing,
                );

                let main_rows = query_rows(
                    &mut reader,
                    &mut storage,
                    QueryBuilder::new("todos").build(),
                    Some(Session::new("alice")),
                );
                let expected_main = usize::from(normal_select == SelectPolicyMode::True);
                assert_eq!(
                    main_rows.len(),
                    expected_main,
                    "normal select mode {normal_select:?} should control main reads"
                );

                let branch_rows = query_rows(
                    &mut reader,
                    &mut storage,
                    QueryBuilder::new("todos").branch(branch_name).build(),
                    Some(Session::new("alice")),
                );
                let expected_branch = usize::from(
                    branch_select == SelectPolicyMode::True
                        && backing_select == SelectPolicyMode::True,
                );
                assert_eq!(
                    branch_rows.len(),
                    expected_branch,
                    "branch select {branch_select:?} and backing select {backing_select:?} should control branch reads independently from normal select {normal_select:?}"
                );
            }
        }
    }
}

#[test]
// Server subscription path:
//
//   structural schema compiles graph
//             |
//             v
//   authorization schema applies for_branch select=false
//
//   Sent row batch for branch todo: no
//   Settled scope: empty
fn synced_branch_read_uses_branch_select_policy_after_structural_graph_compile() {
    // Regression coverage for browser/dev-server subscriptions: the server
    // compiles query scopes from the structural schema, then re-authorizes
    // candidate rows with the separately published permissions head. Branch
    // reads must therefore check the published `for_branch` select policy
    // instead of falling back to the normal table select policy.
    let mut todo_policies = TablePolicies::new()
        .with_select(PolicyExpr::True)
        .with_insert(PolicyExpr::True);
    todo_policies.for_branch = HashMap::from([(
        TableName::new("branches"),
        TablePolicies::new()
            .with_select(PolicyExpr::False)
            .with_insert(PolicyExpr::True),
    )]);
    let auth_schema = branch_schema_with_backing_and_todo_policies(
        TablePolicies::new()
            .with_select(PolicyExpr::True)
            .with_insert(PolicyExpr::True),
        todo_policies,
    );
    let structural_schema = strip_test_policies(&auth_schema);
    let mut qm = create_query_manager(SyncManager::new(), structural_schema.clone());
    let mut storage = seeded_memory_storage(&qm.schema_context().current_schema);
    let project_id = ObjectId::new();
    let branch_id = qm
        .insert(
            &mut storage,
            "branches",
            &[Value::Uuid(project_id), Value::Text("alice".into())],
        )
        .expect("seed branch row")
        .row_id;
    let branch_name = branch_name_for(&structural_schema, branch_id);
    let branch_todo = qm
        .insert_on_branch(
            &mut storage,
            "todos",
            &branch_name,
            &[
                Value::Uuid(project_id),
                Value::Text("Hidden branch todo".into()),
            ],
            None,
        )
        .expect("seed branch todo");
    qm.process(&mut storage);
    qm.set_authorization_schema(auth_schema);

    let client_id = ClientId::new();
    connect_client(&mut qm, &storage, client_id);
    qm.sync_manager_mut()
        .set_client_session(client_id, Session::new("alice"));
    qm.sync_manager_mut().take_outbox();

    qm.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Client(client_id),
        payload: SyncPayload::QuerySubscription {
            query_id: QueryId(1),
            query: Box::new(QueryBuilder::new("todos").branch(branch_name).build()),
            session: Some(Session::new("alice")),
            required_tier: None,
            propagation: crate::sync_manager::QueryPropagation::Full,
            policy_context_tables: vec![],
        },
    });

    qm.process(&mut storage);

    let outbox = qm.sync_manager_mut().take_outbox();
    let sent_rows: Vec<_> = outbox
        .iter()
        .filter_map(|entry| match &entry.payload {
            SyncPayload::RowBatchNeeded { row, .. } => Some(row.row_id),
            _ => None,
        })
        .collect();
    assert!(
        !sent_rows.contains(&branch_todo.row_id),
        "branch todo should not be sent when forBranch read is false"
    );
    let settled_scope = outbox.iter().find_map(|entry| match &entry.payload {
        SyncPayload::QuerySettled {
            query_id: QueryId(1),
            scope,
            ..
        } => Some(scope),
        _ => None,
    });
    assert_eq!(settled_scope, Some(&Vec::new()));
}

#[test]
fn synced_union_branch_read_uses_branch_select_policy_after_structural_graph_compile() {
    let mut todo_policies = TablePolicies::new()
        .with_select(PolicyExpr::True)
        .with_insert(PolicyExpr::True);
    todo_policies.for_branch = HashMap::from([(
        TableName::new("branches"),
        TablePolicies::new()
            .with_select(PolicyExpr::False)
            .with_insert(PolicyExpr::True),
    )]);
    let auth_schema = branch_schema_with_backing_and_todo_policies(
        TablePolicies::new()
            .with_select(PolicyExpr::True)
            .with_insert(PolicyExpr::True),
        todo_policies,
    );
    let structural_schema = strip_test_policies(&auth_schema);
    let mut qm = create_query_manager(SyncManager::new(), structural_schema.clone());
    let mut storage = seeded_memory_storage(&qm.schema_context().current_schema);
    let project_id = ObjectId::new();
    let branch_id = qm
        .insert(
            &mut storage,
            "branches",
            &[Value::Uuid(project_id), Value::Text("alice".into())],
        )
        .expect("seed branch row")
        .row_id;
    let branch_name = branch_name_for(&structural_schema, branch_id);
    let branch_todo = qm
        .insert_on_branch(
            &mut storage,
            "todos",
            &branch_name,
            &[
                Value::Uuid(project_id),
                Value::Text("Hidden branch todo".into()),
            ],
            None,
        )
        .expect("seed branch todo");
    qm.process(&mut storage);
    qm.set_authorization_schema(auth_schema);

    let client_id = ClientId::new();
    connect_client(&mut qm, &storage, client_id);
    qm.sync_manager_mut()
        .set_client_session(client_id, Session::new("alice"));
    qm.sync_manager_mut().take_outbox();

    let mut query = QueryBuilder::new("todos").build();
    query.relation_ir = RelExpr::Union {
        inputs: vec![RelExpr::Branch {
            input: Box::new(RelExpr::Filter {
                input: Box::new(RelExpr::TableScan {
                    table: TableName::new("todos"),
                }),
                predicate: PredicateExpr::Cmp {
                    left: ColumnRef::unscoped("projectId"),
                    op: PredicateCmpOp::Eq,
                    right: ValueRef::Literal(Value::Uuid(project_id)),
                },
            }),
            branches: vec![branch_name],
        }],
    };
    qm.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Client(client_id),
        payload: SyncPayload::QuerySubscription {
            query_id: QueryId(1),
            query: Box::new(query),
            session: Some(Session::new("alice")),
            required_tier: None,
            propagation: crate::sync_manager::QueryPropagation::Full,
            policy_context_tables: vec![],
        },
    });

    qm.process(&mut storage);

    let outbox = qm.sync_manager_mut().take_outbox();
    let sent_rows: Vec<_> = outbox
        .iter()
        .filter_map(|entry| match &entry.payload {
            SyncPayload::RowBatchNeeded { row, .. } => Some(row.row_id),
            _ => None,
        })
        .collect();
    assert!(
        !sent_rows.contains(&branch_todo.row_id),
        "branch todo should not be sent from a union when forBranch read is false"
    );
    let settled_scope = outbox.iter().find_map(|entry| match &entry.payload {
        SyncPayload::QuerySettled {
            query_id: QueryId(1),
            scope,
            ..
        } => Some(scope),
        _ => None,
    });
    assert_eq!(settled_scope, Some(&Vec::new()));
}

#[test]
fn local_union_branch_read_uses_branch_select_policy_in_graph_filter() {
    let schema = branch_query_matrix_schema(
        SelectPolicyMode::True,
        SelectPolicyMode::False,
        SelectPolicyMode::True,
    );
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
        .expect("seed branch row")
        .row_id;
    let branch_name = branch_name_for(&schema, branch_id);
    qm.insert_on_branch(
        &mut storage,
        "todos",
        &branch_name,
        &[
            Value::Uuid(project_id),
            Value::Text("Hidden branch todo".into()),
        ],
        None,
    )
    .expect("seed branch todo");

    let mut query = QueryBuilder::new("todos").build();
    query.relation_ir = RelExpr::Union {
        inputs: vec![RelExpr::Branch {
            input: Box::new(RelExpr::TableScan {
                table: TableName::new("todos"),
            }),
            branches: vec![branch_name],
        }],
    };
    let sub_id = qm
        .subscribe_with_session(query, Some(Session::new("alice")), None)
        .expect("subscribe union branch query");
    qm.process(&mut storage);

    assert!(qm.get_subscription_results(sub_id).is_empty());
}

#[test]
fn multi_branch_read_uses_each_rows_branch_for_branch_ref_policy() {
    let (mut qm, mut storage, schema) = manager_with_branch_schema(true);
    let project_a = ObjectId::new();
    let project_b = ObjectId::new();
    let branch_a = qm
        .insert(
            &mut storage,
            "branches",
            &[Value::Uuid(project_a), Value::Text("alice".into())],
        )
        .expect("insert branch A")
        .row_id;
    let branch_b = qm
        .insert(
            &mut storage,
            "branches",
            &[Value::Uuid(project_b), Value::Text("alice".into())],
        )
        .expect("insert branch B")
        .row_id;
    let branch_a_name = branch_name_for(&schema, branch_a);
    let branch_b_name = branch_name_for(&schema, branch_b);
    let allowed_a = qm
        .insert_on_branch(
            &mut storage,
            "todos",
            &branch_a_name,
            &[Value::Uuid(project_a), Value::Text("allowed A".into())],
            None,
        )
        .expect("insert branch A todo")
        .row_id;
    let allowed_b = qm
        .insert_on_branch(
            &mut storage,
            "todos",
            &branch_b_name,
            &[Value::Uuid(project_b), Value::Text("allowed B".into())],
            None,
        )
        .expect("insert branch B todo")
        .row_id;
    let denied_b = qm
        .insert_on_branch(
            &mut storage,
            "todos",
            &branch_b_name,
            &[Value::Uuid(project_a), Value::Text("denied B".into())],
            None,
        )
        .expect("insert branch B todo with branch A project")
        .row_id;

    let rows = query_rows(
        &mut qm,
        &mut storage,
        QueryBuilder::new("todos")
            .branches(&[branch_a_name.as_str(), branch_b_name.as_str()])
            .build(),
        Some(Session::new("alice")),
    );
    let returned_ids: HashSet<_> = rows.into_iter().map(|(id, _)| id).collect();

    assert_eq!(
        returned_ids,
        HashSet::from([allowed_a, allowed_b]),
        "each branch row should be checked against its own backing row"
    );
    assert!(!returned_ids.contains(&denied_b));
}

#[test]
fn multi_branch_read_uses_each_rows_branch_for_magic_permission_columns() {
    let schema = branch_schema_with_todo_branch_read_write_policies();
    let mut qm = create_query_manager_with_policy_mode(
        SyncManager::new(),
        schema.clone(),
        RowPolicyMode::Enforcing,
    );
    let mut storage = seeded_memory_storage(&qm.schema_context().current_schema);
    let project_a = ObjectId::new();
    let project_b = ObjectId::new();
    let branch_a = qm
        .insert(
            &mut storage,
            "branches",
            &[Value::Uuid(project_a), Value::Text("alice".into())],
        )
        .expect("insert branch A")
        .row_id;
    let branch_b = qm
        .insert(
            &mut storage,
            "branches",
            &[Value::Uuid(project_b), Value::Text("alice".into())],
        )
        .expect("insert branch B")
        .row_id;
    let branch_a_name = branch_name_for(&schema, branch_a);
    let branch_b_name = branch_name_for(&schema, branch_b);
    let allowed_a = qm
        .insert_on_branch(
            &mut storage,
            "todos",
            &branch_a_name,
            &[Value::Uuid(project_a), Value::Text("allowed A".into())],
            None,
        )
        .expect("insert branch A todo")
        .row_id;
    let allowed_b = qm
        .insert_on_branch(
            &mut storage,
            "todos",
            &branch_b_name,
            &[Value::Uuid(project_b), Value::Text("allowed B".into())],
            None,
        )
        .expect("insert branch B todo")
        .row_id;
    let denied_b = qm
        .insert_on_branch(
            &mut storage,
            "todos",
            &branch_b_name,
            &[Value::Uuid(project_a), Value::Text("denied B".into())],
            None,
        )
        .expect("insert branch B todo with branch A project")
        .row_id;

    let rows = query_rows(
        &mut qm,
        &mut storage,
        QueryBuilder::new("todos")
            .branches(&[branch_a_name.as_str(), branch_b_name.as_str()])
            .select(&["title", "$canRead", "$canEdit", "$canDelete"])
            .build(),
        Some(Session::new("alice")),
    );
    let values_by_id: HashMap<_, _> = rows.into_iter().collect();

    assert_eq!(
        values_by_id.get(&allowed_a),
        Some(&vec![
            Value::Text("allowed A".into()),
            Value::Boolean(true),
            Value::Boolean(true),
            Value::Boolean(true),
        ])
    );
    assert_eq!(
        values_by_id.get(&allowed_b),
        Some(&vec![
            Value::Text("allowed B".into()),
            Value::Boolean(true),
            Value::Boolean(true),
            Value::Boolean(true),
        ])
    );
    assert!(!values_by_id.contains_key(&denied_b));
}

#[test]
fn explicit_auth_drops_same_id_tuple_when_any_content_branch_is_denied() {
    let auth_schema = branch_schema(true);
    let structural_schema = strip_test_policies(&auth_schema);
    let mut qm = create_query_manager(SyncManager::new(), structural_schema.clone());
    qm.set_authorization_schema(auth_schema);
    let mut storage = seeded_memory_storage(&qm.schema_context().current_schema);
    let project_a = ObjectId::new();
    let project_b = ObjectId::new();
    let branch_a = qm
        .insert(
            &mut storage,
            "branches",
            &[Value::Uuid(project_a), Value::Text("alice".into())],
        )
        .expect("insert branch A")
        .row_id;
    let branch_b = qm
        .insert(
            &mut storage,
            "branches",
            &[Value::Uuid(project_b), Value::Text("alice".into())],
        )
        .expect("insert branch B")
        .row_id;
    let branch_a_name = branch_name_for(&structural_schema, branch_a);
    let branch_b_name = branch_name_for(&structural_schema, branch_b);
    let shared_todo_id = ObjectId::new();
    let todos_descriptor = structural_schema
        .get(&TableName::new("todos"))
        .expect("todos table should exist")
        .columns
        .clone();
    for (branch_name, project_id, title, timestamp) in [
        (&branch_b_name, project_a, "denied branch B content", 2_000),
        (&branch_a_name, project_a, "allowed branch A content", 1_000),
    ] {
        let commit = stored_row_commit(
            smallvec![],
            encode_row(
                &todos_descriptor,
                &[Value::Uuid(project_id), Value::Text(title.into())],
            )
            .unwrap(),
            timestamp,
            "alice",
            None,
        );
        qm.sync_manager_mut().push_inbox(InboxEntry {
            source: Source::Server(ServerId::new()),
            payload: row_batch_created_payload(
                shared_todo_id,
                branch_name,
                Some(RowMetadata {
                    id: shared_todo_id,
                    metadata: todos_metadata(),
                }),
                &commit,
            ),
        });
    }
    qm.process(&mut storage);

    let mut query = QueryBuilder::new("todos").build();
    query.relation_ir = RelExpr::Union {
        inputs: vec![
            RelExpr::Branch {
                input: Box::new(RelExpr::TableScan {
                    table: TableName::new("todos"),
                }),
                branches: vec![branch_b_name],
            },
            RelExpr::Branch {
                input: Box::new(RelExpr::TableScan {
                    table: TableName::new("todos"),
                }),
                branches: vec![branch_a_name],
            },
        ],
    };

    let rows = query_rows(&mut qm, &mut storage, query, Some(Session::new("alice")));

    assert!(
        rows.is_empty(),
        "same-id tuple should be dropped when it may carry denied branch content"
    );
}

#[test]
// Non-row-id branch path:
//
//   Branch name = dev/<schema-hash>/alice-draft
//   No backing row can be resolved from the branch name.
//
//   Normal select = true
//   Branch select = false
//   Expected: forBranch resolution fails and denies instead of using normal select.
fn branch_read_denies_when_for_branch_resolution_fails_for_non_row_id_branch() {
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
// Explicit-policy detection:
//
//   todos policies:
//     normal select/insert/update/delete = missing
//     for_branch insert                  = true
//
//   Schema should be enforcing because for_branch is explicit.
//   Branch read with missing select must deny.
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
// Subscription invalidation:
//
//   branch backing owner alice -> visible branch todo
//   branch backing owner bob   -> unreadable branch todo
//
//   Update branches[ownerId] should dirty the branch policy filter.
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
// Dependency invalidation:
//
//   projects[ownerId=alice]
//          ^
//          | branches.projectId inherits project select
//          ` branch todo visibility
//
//   Changing project owner to bob should remove the branch todo from
//   an alice subscription even though the branch row itself did not change.
fn branch_select_reacts_when_backing_row_policy_dependency_changes() {
    let schema = branch_schema_with_backing_project_dependency();
    let mut qm = create_query_manager_with_policy_mode(
        SyncManager::new(),
        schema.clone(),
        RowPolicyMode::Enforcing,
    );
    let mut storage = seeded_memory_storage(&qm.schema_context().current_schema);
    let project_id = qm
        .insert(&mut storage, "projects", &[Value::Text("alice".into())])
        .expect("seed readable project")
        .row_id;
    qm.process(&mut storage);
    let branch_id = seed_branch_and_todo(&mut qm, &mut storage, &schema, project_id, "alice");
    let branch_name = branch_name_for(&schema, branch_id);
    qm.process(&mut storage);
    let sub_id = qm
        .subscribe_with_session(
            QueryBuilder::new("todos").branch(branch_name).build(),
            Some(Session::new("alice")),
            None,
        )
        .expect("subscribe branch query");

    qm.process(&mut storage);
    assert_eq!(qm.get_subscription_results(sub_id).len(), 1);

    qm.update(&mut storage, project_id, &[Value::Text("bob".into())])
        .expect("make backing branch row unreadable through project dependency");
    qm.process(&mut storage);

    assert!(qm.get_subscription_results(sub_id).is_empty());
    qm.unsubscribe_with_sync(sub_id);
}

#[test]
// Branch-policy inheritance:
//
//   todos.for_branch.select inherits projects.select via projectId
//   projects.for_branch.select compares project.ownerId to $branch.ownerId
//   projects.select on the normal table is false
//
//   Expected: a branch todo can inherit through the parent table's branch
//   policy instead of falling back to the parent's normal select policy.
fn branch_select_inherits_parent_branch_policy() {
    let schema = branch_schema_with_parent_branch_policy_dependency();
    let mut qm = create_query_manager_with_policy_mode(
        SyncManager::new(),
        schema.clone(),
        RowPolicyMode::Enforcing,
    );
    let mut storage = seeded_memory_storage(&qm.schema_context().current_schema);
    let branch_id = qm
        .insert(&mut storage, "branches", &[Value::Text("alice".into())])
        .expect("insert branch backing row")
        .row_id;
    let branch_name = branch_name_for(&schema, branch_id);
    let project_id = qm
        .insert_on_branch(
            &mut storage,
            "projects",
            &branch_name,
            &[Value::Text("alice".into())],
            None,
        )
        .expect("insert branch project")
        .row_id;
    let todo_id = qm
        .insert_on_branch(
            &mut storage,
            "todos",
            &branch_name,
            &[
                Value::Uuid(project_id),
                Value::Text("Inherited branch todo".into()),
            ],
            None,
        )
        .expect("insert branch todo")
        .row_id;

    let rows = query_rows(
        &mut qm,
        &mut storage,
        QueryBuilder::new("todos").branch(branch_name).build(),
        Some(Session::new("alice")),
    );

    assert_eq!(
        rows,
        vec![(
            todo_id,
            vec![
                Value::Uuid(project_id),
                Value::Text("Inherited branch todo".into()),
            ],
        )],
        "branch inherit should evaluate the parent table's branch policy"
    );
}

#[test]
// BranchRef inside EXISTS:
//
//   branch row projectId = P
//   approvals on branch contains projectId = P
//   todos.for_branch select = exists approvals where projectId == $branch.projectId
//
//   Expected: branch todo is readable.
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
// Optional include with denied branch rows:
//
//   projects[P] on main
//          |
//          v
//   include todos inheriting branch B where branch todo read = false
//
//   Expected query result
//   ---------------------
//   project P remains visible with an empty todosViaProject array.
fn branch_include_keeps_outer_row_when_inner_branch_read_denies() {
    let mut todo_policies = TablePolicies::new().with_select(PolicyExpr::True);
    todo_policies.for_branch = HashMap::from([(
        TableName::new("branches"),
        TablePolicies::new()
            .with_select(PolicyExpr::False)
            .with_insert(PolicyExpr::True),
    )]);
    let schema = SchemaBuilder::new()
        .table(
            TableSchema::builder("projects")
                .column("name", ColumnType::Text)
                .policies(
                    TablePolicies::new()
                        .with_select(PolicyExpr::True)
                        .with_insert(PolicyExpr::True),
                ),
        )
        .table(
            TableSchema::builder("branches")
                .fk_column("projectId", "projects")
                .column("ownerId", ColumnType::Text)
                .policies(
                    TablePolicies::new()
                        .with_select(PolicyExpr::True)
                        .with_insert(PolicyExpr::True),
                ),
        )
        .table(
            TableSchema::builder("todos")
                .fk_column("projectId", "projects")
                .column("title", ColumnType::Text)
                .policies(todo_policies),
        )
        .build();
    let mut qm = create_query_manager_with_policy_mode(
        SyncManager::new(),
        schema.clone(),
        RowPolicyMode::Enforcing,
    );
    let mut storage = seeded_memory_storage(&qm.schema_context().current_schema);
    let project_id = qm
        .insert(&mut storage, "projects", &[Value::Text("Project".into())])
        .expect("insert project")
        .row_id;
    let branch_id = qm
        .insert(
            &mut storage,
            "branches",
            &[Value::Uuid(project_id), Value::Text("alice".into())],
        )
        .expect("insert branch row")
        .row_id;
    let branch_name = branch_name_for(&schema, branch_id);
    qm.insert_on_branch(
        &mut storage,
        "todos",
        &branch_name,
        &[Value::Uuid(project_id), Value::Text("Hidden".into())],
        None,
    )
    .expect("seed branch todo");

    let rows = query_rows(
        &mut qm,
        &mut storage,
        QueryBuilder::new("projects")
            .with_array("todosViaProject", |sub| {
                sub.from("todos").correlate("projectId", "projects.id")
            })
            .build(),
        Some(Session::new("alice")),
    );

    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].1[1], Value::Array(vec![]));
}

#[test]
// Server subscription include path:
//
//   structural schema compiles:
//     projects[P] --include todos--> todos on the inherited query branch
//
//   authorization schema applies:
//     projects.select = true
//     todos.for_branch.select = false
//
//   Expected server result:
//     project P remains in scope; branch todo is not sent.
fn synced_branch_include_keeps_outer_row_when_inner_branch_read_denies() {
    let mut todo_policies = TablePolicies::new()
        .with_select(PolicyExpr::True)
        .with_insert(PolicyExpr::True);
    todo_policies.for_branch = HashMap::from([(
        TableName::new("branches"),
        TablePolicies::new()
            .with_select(PolicyExpr::False)
            .with_insert(PolicyExpr::True),
    )]);
    let auth_schema = SchemaBuilder::new()
        .table(
            TableSchema::builder("projects")
                .column("name", ColumnType::Text)
                .policies(
                    TablePolicies::new()
                        .with_select(PolicyExpr::True)
                        .with_insert(PolicyExpr::True),
                ),
        )
        .table(
            TableSchema::builder("branches")
                .fk_column("projectId", "projects")
                .column("ownerId", ColumnType::Text)
                .policies(
                    TablePolicies::new()
                        .with_select(PolicyExpr::True)
                        .with_insert(PolicyExpr::True),
                ),
        )
        .table(
            TableSchema::builder("todos")
                .fk_column("projectId", "projects")
                .column("title", ColumnType::Text)
                .policies(todo_policies),
        )
        .build();
    let structural_schema = strip_test_policies(&auth_schema);
    let mut qm = create_query_manager(SyncManager::new(), structural_schema.clone());
    let mut storage = seeded_memory_storage(&qm.schema_context().current_schema);
    let project_id = qm
        .insert(&mut storage, "projects", &[Value::Text("Project".into())])
        .expect("insert project")
        .row_id;
    let branch_id = qm
        .insert(
            &mut storage,
            "branches",
            &[Value::Uuid(project_id), Value::Text("alice".into())],
        )
        .expect("insert branch row")
        .row_id;
    let branch_name = branch_name_for(&structural_schema, branch_id);
    let branch_todo = qm
        .insert_on_branch(
            &mut storage,
            "todos",
            &branch_name,
            &[Value::Uuid(project_id), Value::Text("Hidden".into())],
            None,
        )
        .expect("seed branch todo");
    qm.process(&mut storage);
    qm.set_authorization_schema(auth_schema);

    let client_id = ClientId::new();
    connect_client(&mut qm, &storage, client_id);
    qm.sync_manager_mut()
        .set_client_session(client_id, Session::new("alice"));
    qm.sync_manager_mut().take_outbox();

    qm.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Client(client_id),
        payload: SyncPayload::QuerySubscription {
            query_id: QueryId(1),
            query: Box::new(
                QueryBuilder::new("projects")
                    .with_array("todosViaProject", |sub| {
                        sub.from("todos").correlate("projectId", "projects.id")
                    })
                    .build(),
            ),
            session: Some(Session::new("alice")),
            required_tier: None,
            propagation: crate::sync_manager::QueryPropagation::Full,
            policy_context_tables: vec![],
        },
    });

    qm.process(&mut storage);

    let outbox = qm.sync_manager_mut().take_outbox();
    let sent_rows: Vec<_> = outbox
        .iter()
        .filter_map(|entry| match &entry.payload {
            SyncPayload::RowBatchNeeded { row, .. } => Some(row.row_id),
            _ => None,
        })
        .collect();
    assert!(
        sent_rows.contains(&project_id),
        "server should send the visible outer project row"
    );
    assert!(
        !sent_rows.contains(&branch_todo.row_id),
        "server should not send branch todo denied by forBranch read"
    );
    let settled_scope = outbox.iter().find_map(|entry| match &entry.payload {
        SyncPayload::QuerySettled {
            query_id: QueryId(1),
            scope,
            ..
        } => Some(scope),
        _ => None,
    });
    assert!(
        settled_scope.is_some_and(|scope| scope.iter().any(|(row_id, _)| *row_id == project_id)),
        "server query scope should keep the visible outer project row"
    );
}

#[test]
// Branch insert:
//
//   branches[projectId=P] -> todos insert projectId=P
//
//   Insert target                    Policy used
//   -------------------------------  -----------------------
//   insert_on_branch("todos", ...)   for_branch insert policy
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
// Non-row-id branch insert:
//
//   Branch name = dev/<schema-hash>/alice-draft
//   No backing row can be resolved from the branch name.
//
//   Normal insert = true
//   Branch insert = false
//   Expected: forBranch resolution fails and denies instead of using normal insert.
fn branch_insert_denies_when_for_branch_resolution_fails_for_non_row_id_branch() {
    let mut todo_policies = TablePolicies::new().with_insert(PolicyExpr::True);
    todo_policies.for_branch = HashMap::from([(
        TableName::new("branches"),
        TablePolicies::new().with_insert(PolicyExpr::False),
    )]);
    let schema = branch_schema_with_todo_policies(todo_policies);
    let mut qm = create_query_manager_with_policy_mode(
        SyncManager::new(),
        schema.clone(),
        RowPolicyMode::Enforcing,
    );
    let mut storage = seeded_memory_storage(&qm.schema_context().current_schema);
    let branch_name = ComposedBranchName::new("dev", SchemaHash::compute(&schema), "alice-draft")
        .to_branch_name()
        .as_str()
        .to_string();
    let write_context = WriteContext::from_session(Session::new("alice"));

    let denied = qm.insert_on_branch(
        &mut storage,
        "todos",
        &branch_name,
        &[Value::Uuid(ObjectId::new()), Value::Text("Draft".into())],
        Some(&write_context),
    );

    assert!(matches!(
        denied,
        Err(QueryError::PolicyDenied {
            operation: Operation::Insert,
            ..
        })
    ));
}

#[test]
// Branch update with only WITH CHECK:
//
//   old clause = missing
//   new clause = projectId == $branch.projectId
//
//   Expected: branch update can rely on the new-row check without requiring
//   both old and new policy clauses to be present.
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
// Synced branch insert:
//
//   Client write batch -> branch "dev/<schema>/<branch_id>"
//
//   Policy table
//   -----------------------------  --------
//   normal todos insert            missing
//   for_branch todos insert        matches P
//   expected server decision       accept
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
// Synced non-row-id branch insert:
//
//   Branch name = dev/<schema-hash>/alice-draft
//   Normal insert = true
//   Branch insert = false
//   Expected: forBranch resolution fails and denies instead of using normal insert.
fn synced_branch_insert_denies_when_for_branch_resolution_fails_for_non_row_id_branch() {
    let mut todo_policies = TablePolicies::new().with_insert(PolicyExpr::True);
    todo_policies.for_branch = HashMap::from([(
        TableName::new("branches"),
        TablePolicies::new().with_insert(PolicyExpr::False),
    )]);
    let schema = branch_schema_with_todo_policies(todo_policies);
    let mut qm = create_query_manager_with_policy_mode(
        SyncManager::new(),
        schema.clone(),
        RowPolicyMode::Enforcing,
    );
    let mut storage = seeded_memory_storage(&qm.schema_context().current_schema);
    let branch_name = ComposedBranchName::new("dev", SchemaHash::compute(&schema), "alice-draft")
        .to_branch_name()
        .as_str()
        .to_string();
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
        &[Value::Uuid(ObjectId::new()), Value::Text("Draft".into())],
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
    assert!(client_write_was_rejected(&outbox, client_id, batch_id));
    let tips = test_row_tip_ids(&storage, todo_id, &branch_name);
    assert!(tips.is_err() || !tips.unwrap().contains(&batch_id));
}

#[test]
// Synced branch insert with complex branch policy:
//
//   client todo insert on branch B
//              |
//              v
//   todos.for_branch insert = todo.projectId == $branch.projectId
//                             AND exists approvals(projectId == $branch.projectId)
//
//   Stored rows on branch B         Expected server decision
//   ------------------------------  ------------------------
//   approvals[projectId=P]          accept todo[projectId=P]
fn synced_branch_insert_allows_complex_branch_policy_after_simple_parts_pass() {
    let mut todo_policies = TablePolicies::default();
    todo_policies.for_branch = HashMap::from([(
        TableName::new("branches"),
        TablePolicies::new().with_insert(PolicyExpr::And(vec![
            project_matches_branch_policy(),
            PolicyExpr::Exists {
                table: "approvals".into(),
                condition: Box::new(project_matches_branch_policy()),
            },
        ])),
    )]);
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
                .policies(todo_policies),
        )
        .table(
            TableSchema::builder("approvals")
                .column("projectId", ColumnType::Uuid)
                .policies(
                    TablePolicies::new()
                        .with_select(PolicyExpr::True)
                        .with_insert(PolicyExpr::True),
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
    qm.insert_on_branch(
        &mut storage,
        "approvals",
        &branch_name,
        &[Value::Uuid(project_id)],
        None,
    )
    .expect("seed branch approval");

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
            Value::Text("Write branch sync docs through approval".into()),
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
// Synced branch delete:
//
//   Existing branch todo -> client soft-delete batch
//
//   Policy table
//   -----------------------------  --------
//   normal todos delete            missing
//   for_branch todos delete        matches P
//   expected server decision       accept
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
// Synced update bypass guard:
//
//   UUID branch has a readable backing row,
//   but todos has no for_branch block.
//
//   Write path                      Expected
//   ------------------------------  ----------------
//   branch update from client       reject
//   normal update policy            must not bypass
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
// Lens-aware local insert:
//
//   legacy todos(projectId,title)
//        --lens adds ownerId=alice-->
//   auth todos(projectId,title,ownerId)
//
//   Session alice insert: accept
//   Session bob insert: deny
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

fn branch_ref_read_lens_schemas() -> (Schema, Schema, Lens, SchemaHash, SchemaHash) {
    let (legacy, mut auth, lens, legacy_hash, auth_hash) = branch_ref_lens_schemas();
    let todo_policies = auth
        .get_mut(&TableName::new("todos"))
        .expect("todos table should exist")
        .policies
        .for_branch
        .get_mut(&TableName::new("branches"))
        .expect("branch policies should exist");
    todo_policies.select = OperationPolicy::using(PolicyExpr::Cmp {
        column: "createdBy".into(),
        op: CmpOp::Eq,
        value: PolicyValue::BranchRef("ownerId".into()),
    });
    (legacy, auth, lens, legacy_hash, auth_hash)
}

fn branch_backing_inherits_lens_schemas() -> (Schema, Schema, Lens, SchemaHash, SchemaHash) {
    let legacy = SchemaBuilder::new()
        .table(TableSchema::builder("projects").column("name", ColumnType::Text))
        .table(TableSchema::builder("branches").fk_column("projectId", "projects"))
        .table(
            TableSchema::builder("todos")
                .fk_column("projectId", "projects")
                .column("title", ColumnType::Text),
        )
        .build();

    let auth = SchemaBuilder::new()
        .table(
            TableSchema::builder("projects")
                .column("name", ColumnType::Text)
                .column("ownerId", ColumnType::Text)
                .policies(TablePolicies::new().with_select(PolicyExpr::Cmp {
                    column: "ownerId".into(),
                    op: CmpOp::Eq,
                    value: PolicyValue::SessionRef(vec!["user_id".into()]),
                })),
        )
        .table(
            TableSchema::builder("branches")
                .fk_column("projectId", "projects")
                .policies(TablePolicies::new().with_select(PolicyExpr::Inherits {
                    operation: Operation::Select,
                    via_column: "projectId".into(),
                    max_depth: None,
                })),
        )
        .table(
            TableSchema::builder("todos")
                .fk_column("projectId", "projects")
                .column("title", ColumnType::Text)
                .policies({
                    let mut todo_policies = TablePolicies::default();
                    todo_policies.for_branch = HashMap::from([(
                        TableName::new("branches"),
                        TablePolicies::new().with_select(PolicyExpr::True),
                    )]);
                    todo_policies
                }),
        )
        .build();

    let legacy_hash = SchemaHash::compute(&legacy);
    let auth_hash = SchemaHash::compute(&auth);
    let mut transform = LensTransform::new();
    transform.push(
        LensOp::AddColumn {
            table: "projects".to_string(),
            column: "ownerId".to_string(),
            column_type: ColumnType::Text,
            default: Value::Text("alice".to_string()),
        },
        false,
    );
    let lens = Lens::new(legacy_hash, auth_hash, transform);

    (legacy, auth, lens, legacy_hash, auth_hash)
}

#[test]
// BranchRef lens path for graph read filtering:
//
//   legacy backing branch row lacks ownerId
//        --lens adds ownerId=alice-->
//   auth branch select compares todo.createdBy == $branch.ownerId
//
//   Expected: graph policy filtering transforms the backing row before
//   evaluating `$branch.ownerId`.
fn branch_read_transforms_backing_row_before_branch_ref_policy() {
    let (legacy, auth, lens, legacy_hash, auth_hash) = branch_ref_read_lens_schemas();
    let mut qm = create_query_manager_with_policy_mode(
        SyncManager::new(),
        auth.clone(),
        RowPolicyMode::PermissiveLocal,
    );
    qm.set_known_schemas(Arc::new(HashMap::from([
        (legacy_hash, legacy.clone()),
        (auth_hash, auth.clone()),
    ])));
    qm.add_live_schema(legacy.clone());
    qm.register_lens(lens);
    let mut storage = seeded_memory_storage(&qm.schema_context().current_schema);
    let project_id = ObjectId::new();
    let legacy_main_branch = ComposedBranchName::new("dev", legacy_hash, "main")
        .to_branch_name()
        .as_str()
        .to_string();
    let branch_id = ObjectId::new();
    let legacy_branch_descriptor = legacy
        .get(&TableName::new("branches"))
        .expect("legacy branches table should exist")
        .columns
        .clone();
    let branch_commit = stored_row_commit(
        smallvec![],
        encode_row(&legacy_branch_descriptor, &[Value::Uuid(project_id)]).unwrap(),
        1_000,
        "alice",
        None,
    );
    qm.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Server(ServerId::new()),
        payload: row_batch_created_payload(
            branch_id,
            &legacy_main_branch,
            Some(RowMetadata {
                id: branch_id,
                metadata: branches_metadata(),
            }),
            &branch_commit,
        ),
    });
    let branch_name = branch_name_for(&legacy, branch_id);
    let todo_id = ObjectId::new();
    let legacy_todo_descriptor = legacy
        .get(&TableName::new("todos"))
        .expect("legacy todos table should exist")
        .columns
        .clone();
    let todo_commit = stored_row_commit(
        smallvec![],
        encode_row(
            &legacy_todo_descriptor,
            &[
                Value::Uuid(project_id),
                Value::Text("Read branch-ref lens docs".into()),
            ],
        )
        .unwrap(),
        1_001,
        "alice",
        None,
    );
    qm.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Server(ServerId::new()),
        payload: row_batch_created_payload(
            todo_id,
            &branch_name,
            Some(RowMetadata {
                id: todo_id,
                metadata: todos_metadata(),
            }),
            &todo_commit,
        ),
    });
    qm.process(&mut storage);

    let rows = query_rows(
        &mut qm,
        &mut storage,
        QueryBuilder::new("todos").branch(&branch_name).build(),
        Some(Session::new("alice")),
    );
    assert_eq!(
        rows.len(),
        1,
        "branch read should use transformed backing row for BranchRef"
    );
}

#[test]
// Inherited backing-row select through a lens:
//
//   legacy projects(name)
//        --lens adds ownerId=alice-->
//   auth branches.select inherits projects.select(ownerId == session.user_id)
//
//   Expected: graph policy filtering transforms the inherited project row before
//   evaluating the backing branch row's select policy.
fn branch_backing_select_transforms_related_rows_before_policy_eval() {
    let (legacy, auth, lens, legacy_hash, auth_hash) = branch_backing_inherits_lens_schemas();
    let mut qm = create_query_manager_with_policy_mode(
        SyncManager::new(),
        auth.clone(),
        RowPolicyMode::PermissiveLocal,
    );
    qm.set_known_schemas(Arc::new(HashMap::from([
        (legacy_hash, legacy.clone()),
        (auth_hash, auth.clone()),
    ])));
    qm.add_live_schema(legacy.clone());
    qm.register_lens(lens);
    let mut storage = seeded_memory_storage(&qm.schema_context().current_schema);

    let legacy_main_branch = ComposedBranchName::new("dev", legacy_hash, "main")
        .to_branch_name()
        .as_str()
        .to_string();

    let project_id = ObjectId::new();
    let legacy_project_descriptor = legacy
        .get(&TableName::new("projects"))
        .expect("legacy projects table should exist")
        .columns
        .clone();
    let project_commit = stored_row_commit(
        smallvec![],
        encode_row(
            &legacy_project_descriptor,
            &[Value::Text("Legacy project".into())],
        )
        .unwrap(),
        1_000,
        "alice",
        None,
    );
    qm.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Server(ServerId::new()),
        payload: row_batch_created_payload(
            project_id,
            &legacy_main_branch,
            Some(RowMetadata {
                id: project_id,
                metadata: projects_metadata(),
            }),
            &project_commit,
        ),
    });

    let branch_id = ObjectId::new();
    let legacy_branch_descriptor = legacy
        .get(&TableName::new("branches"))
        .expect("legacy branches table should exist")
        .columns
        .clone();
    let branch_commit = stored_row_commit(
        smallvec![],
        encode_row(&legacy_branch_descriptor, &[Value::Uuid(project_id)]).unwrap(),
        1_001,
        "alice",
        None,
    );
    qm.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Server(ServerId::new()),
        payload: row_batch_created_payload(
            branch_id,
            &legacy_main_branch,
            Some(RowMetadata {
                id: branch_id,
                metadata: branches_metadata(),
            }),
            &branch_commit,
        ),
    });

    let branch_name = branch_name_for(&legacy, branch_id);
    let todo_id = ObjectId::new();
    let legacy_todo_descriptor = legacy
        .get(&TableName::new("todos"))
        .expect("legacy todos table should exist")
        .columns
        .clone();
    let todo_commit = stored_row_commit(
        smallvec![],
        encode_row(
            &legacy_todo_descriptor,
            &[
                Value::Uuid(project_id),
                Value::Text("Read through inherited project policy".into()),
            ],
        )
        .unwrap(),
        1_002,
        "alice",
        None,
    );
    qm.sync_manager_mut().push_inbox(InboxEntry {
        source: Source::Server(ServerId::new()),
        payload: row_batch_created_payload(
            todo_id,
            &branch_name,
            Some(RowMetadata {
                id: todo_id,
                metadata: todos_metadata(),
            }),
            &todo_commit,
        ),
    });
    qm.process(&mut storage);

    let rows = query_rows(
        &mut qm,
        &mut storage,
        QueryBuilder::new("todos").branch(&branch_name).build(),
        Some(Session::new("alice")),
    );
    assert_eq!(
        rows.len(),
        1,
        "backing branch select should transform inherited project rows before policy eval"
    );
}

#[test]
// BranchRef lens path for local write:
//
//   legacy branches(projectId)
//        --lens adds ownerId=alice-->
//   auth branch policy compares todo.createdBy == $branch.ownerId
//
//   Expected: transformed backing row supplies ownerId for BranchRef.
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
// BranchRef lens path for synced write:
//
//   client row batch on legacy branch
//        -> server authorization schema
//        -> transformed backing branch row
//
//   Expected: BranchRef reads ownerId after lens transform and accepts write.
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
