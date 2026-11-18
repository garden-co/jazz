//! `InheritsFrom` and `InsertedByThisTransaction` are Jazz's own: it
//! derives them for its system tables. An app table's policy holding
//! either, anywhere, is refused by the deployment and by the validation it
//! shares with `jazz-tools validate`, with a typed error.

use std::collections::HashMap;

use jazz::tools::deployment::{
    DeploymentCatalogue, DeploymentError, DeploymentRequest, prepare_deployment,
};
use jazz::tools::public_schema::{Schema, SchemaHash, TablePolicies};
use jazz::tools::public_schema_convert::{UnsupportedPolicy, check_deployable_policies};
use jazz::tools::{ColumnType, Operation, PolicyExpr, SchemaBuilder, TableSchemaBuilder};

fn app(tasks_insert: PolicyExpr) -> Schema {
    SchemaBuilder::new()
        .table(TableSchemaBuilder::new("projects").column("name", ColumnType::Text))
        .table(
            TableSchemaBuilder::new("tasks")
                .column("scopeId", ColumnType::Uuid)
                .fk_column("projectId", "projects")
                .policies(TablePolicies::new().with_insert(tasks_insert)),
        )
        .build()
}

#[track_caller]
fn assert_refused(schema: &Schema, path: &str) {
    let error = check_deployable_policies(schema).expect_err("refused");
    assert_eq!(
        error.unsupported_policy(),
        Some(UnsupportedPolicy::SystemOnlyPolicyNode),
        "{error}"
    );
    assert!(error.to_string().contains(path), "{error}");
    let mut structural = schema.clone();
    for table in structural.values_mut() {
        table.policies = TablePolicies::default();
    }
    let hash = SchemaHash::compute(&structural);
    let deployed = prepare_deployment(
        &DeploymentCatalogue::default(),
        DeploymentRequest {
            target_schema_hash: hash,
            schemas: vec![(hash, structural)],
            migrations: Vec::new(),
            permissions: schema
                .iter()
                .map(|(name, table)| (*name, table.policies.clone()))
                .collect::<HashMap<_, _>>(),
            allow_dropping_history: false,
        },
    )
    .expect_err("the deployment is refused");
    assert!(
        matches!(deployed, DeploymentError::InvalidPermissions { .. }),
        "{deployed}"
    );
}

/// tasks.insert = true ∨ InheritsFrom(projects)                     ✗
/// tasks.insert = exists projects(InsertedByThisTransaction)          ✗
#[test]
fn an_app_policy_may_not_hold_a_system_only_node() {
    assert_refused(
        &app(PolicyExpr::Or(vec![
            PolicyExpr::True,
            PolicyExpr::InheritsFrom {
                operation: Operation::Select,
                table_column: "scopeId".to_owned(),
                id_column: "projectId".to_owned(),
                tables: vec!["projects".to_owned()],
            },
        ])),
        "$.tasks.policies.insert.with_check.Or[1]",
    );
    assert_refused(
        &app(PolicyExpr::Exists {
            table: "projects".to_owned(),
            condition: Box::new(PolicyExpr::InsertedByThisTransaction),
        }),
        "$.tasks.policies.insert.with_check.Exists",
    );
    check_deployable_policies(&app(PolicyExpr::True)).unwrap();
}
