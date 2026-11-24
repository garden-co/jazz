//! `InheritsFrom` and `InsertedByThisTransaction` are Jazz's own: it
//! derives them for its system tables. An app table's policy holding
//! either, anywhere, is refused by the deployment and by the validation it
//! shares with `jazz-tools validate`.

use crate::inherits_from_refusals::assert_refused;
use jazz::tools::public_schema::{Schema, TablePolicies};
use jazz::tools::public_schema_convert::check_deployable_policies;
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
        "`InheritsFrom` is for Jazz's system tables only",
        "$.tasks.policies.insert.with_check.Or[1]",
    );
    assert_refused(
        &app(PolicyExpr::Exists {
            table: "projects".to_owned(),
            condition: Box::new(PolicyExpr::InsertedByThisTransaction),
        }),
        "`InsertedByThisTransaction` is for Jazz's system tables only",
        "$.tasks.policies.insert.with_check.Exists.condition",
    );
    check_deployable_policies(&app(PolicyExpr::True)).unwrap();
}
