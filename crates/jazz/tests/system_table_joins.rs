//! Correlations on a system table's two-column address (a table identity
//! and a row id): schema validation accepts them although the address
//! columns cannot be declared references.
//!
//! The tables are ordinary tables; the members table carries the
//! system-table prefix, which is what scopes the validation rule.

use crate::common;

use jazz::schema::JazzSchema;
use jazz::tools::{
    CmpOp, ColumnType, Operation, PolicyExpr, PolicyValue, Schema, SchemaBuilder, TablePolicies,
    TableSchemaBuilder,
};

use common::session_eq;

const MEMBERS: &str = "__e2ee_members";

fn me() -> PolicyValue {
    PolicyValue::SessionRef(vec!["user".to_owned()])
}

fn this(column: &str) -> PolicyValue {
    PolicyValue::SessionRef(vec!["__jazz_outer_row".to_owned(), column.to_owned()])
}

fn cmp(column: &str, value: PolicyValue) -> PolicyExpr {
    PolicyExpr::Cmp {
        column: column.to_owned(),
        op: CmpOp::Eq,
        value,
    }
}

fn exists(table: &str, conditions: Vec<PolicyExpr>) -> PolicyExpr {
    PolicyExpr::Exists {
        table: table.to_owned(),
        condition: Box::new(PolicyExpr::And(conditions)),
    }
}

fn inherits_from(operation: Operation) -> PolicyExpr {
    PolicyExpr::InheritsFrom {
        operation,
        table_column: "scopeId".to_owned(),
        id_column: "identifier".to_owned(),
        tables: vec!["projects".to_owned()],
    }
}

/// The members of a space see each other: `MEMBER(session)`.
fn member_of_same_space() -> PolicyExpr {
    exists(
        MEMBERS,
        vec![
            cmp("scopeId", this("scopeId")),
            cmp("identifier", this("identifier")),
            cmp("accountId", me()),
        ],
    )
}

/// Each shape of correlation an address policy takes: `M(projects)` on the
/// space table (primary `identifier = this.id`), `MEMBER` on the members
/// table (primary `scopeId = this.scopeId`), the creator clause (a row-id
/// join on `identifier`) and `InheritsFrom` (the project's own policies
/// reached through `identifier`).
fn space_tables() -> SchemaBuilder {
    let member_of_this_project = exists(
        MEMBERS,
        vec![
            cmp(
                "scopeId",
                PolicyValue::TableIdentity {
                    table: "projects".to_owned(),
                },
            ),
            cmp("identifier", this("id")),
            cmp("accountId", me()),
        ],
    );
    let creator = exists(
        "projects",
        vec![
            cmp("id", this("identifier")),
            session_eq("$createdBy.account", &["user"]),
            PolicyExpr::InsertedByThisTransaction,
        ],
    );
    let manages = PolicyExpr::And(vec![
        inherits_from(Operation::Select),
        inherits_from(Operation::Update),
    ]);
    SchemaBuilder::new()
        .table(
            TableSchemaBuilder::new("projects")
                .column("name", ColumnType::Text)
                .column_with_default("hidden", ColumnType::Boolean, false.into())
                .policies(
                    TablePolicies::new()
                        .with_select(PolicyExpr::And(vec![
                            member_of_this_project.clone(),
                            PolicyExpr::eq_literal("hidden", false.into()),
                        ]))
                        .with_insert(PolicyExpr::True)
                        .with_update(Some(member_of_this_project), PolicyExpr::True),
                ),
        )
        .table(
            TableSchemaBuilder::new(MEMBERS)
                .column("scopeId", ColumnType::Uuid)
                .column("identifier", ColumnType::Uuid)
                .column("accountId", ColumnType::Uuid)
                .composite_index(["scopeId", "identifier", "accountId"])
                .policies(
                    TablePolicies::new()
                        .with_select(PolicyExpr::Or(vec![
                            cmp("accountId", me()),
                            member_of_same_space(),
                        ]))
                        .with_insert(PolicyExpr::Or(vec![
                            manages.clone(),
                            PolicyExpr::And(vec![cmp("accountId", me()), creator]),
                        ]))
                        .with_delete(PolicyExpr::Or(vec![cmp("accountId", me()), manages])),
                ),
        )
}

/// An address correlation with one side on a system table validates
/// without a declared reference, in every shape the address policies take.
/// In the same schema, an undeclared `uuid` equality between two
/// application tables is still refused, as it was: only the system-table
/// class is relaxed.
///
/// ```text
/// projects ◄── identifier ── __e2ee_members   ✓ (no reference declared)
/// projects ◄── projectId ─── tasks            ✗ (declare the reference)
/// ```
#[test]
fn an_address_correlation_validates_only_with_a_system_table_on_one_side() {
    let schema: Schema = space_tables().build();
    JazzSchema::new(&schema).expect("every address correlation validates");

    let with_tasks: Schema = space_tables()
        .table(
            TableSchemaBuilder::new("tasks")
                .column("projectId", ColumnType::Uuid)
                .policies(
                    TablePolicies::new()
                        .with_select(PolicyExpr::True)
                        .with_insert(exists("projects", vec![cmp("id", this("projectId"))])),
                ),
        )
        .build();
    let error = format!("{:?}", JazzSchema::new(&with_tasks).unwrap_err());
    assert!(
        error.contains("tasks") && error.contains("projectId") && error.contains("projects"),
        "{error}"
    );
    let message = JazzSchema::new(&with_tasks).unwrap_err().to_string();
    assert!(
        message.contains(
            "column tasks.projectId is matched against the ids of projects, but it is not \
             declared as a reference"
        ),
        "{message}"
    );
}
