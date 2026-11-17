//! `InheritsFrom` shapes the engine cannot evaluate are refused when a
//! deployment (or `jazz-tools validate`) checks its policies, with a typed
//! error, rather than accepted and then failing every check.
//!
//! The tables carry the system-table prefix only because schema validation
//! accepts an undeclared `uuid` address correlation for that table class
//! alone.

use jazz::tools::deployment::{
    DeploymentCatalogue, DeploymentError, DeploymentRequest, prepare_deployment,
};
use jazz::tools::public_schema::{Schema as PublicSchema, SchemaHash, TablePolicies};
use jazz::tools::public_schema_convert::{UnsupportedPolicy, check_deployable_policies};
use jazz::tools::{ColumnType, Operation, PolicyExpr, SchemaBuilder, TableSchemaBuilder};

const MEMBERS: &str = "__e2ee_members";
const SPACES: &str = "__e2ee_spaces";

fn inherits_from(operation: Operation, table: &str) -> PolicyExpr {
    PolicyExpr::InheritsFrom {
        operation,
        table_column: "scopeId".to_owned(),
        id_column: "identifier".to_owned(),
        tables: vec![table.to_owned()],
    }
}

fn projects() -> TableSchemaBuilder {
    TableSchemaBuilder::new("projects")
        .column("name", ColumnType::Text)
        .policies(
            TablePolicies::new()
                .with_select(PolicyExpr::True)
                .with_insert(PolicyExpr::True),
        )
}

/// A table addressing rows by `(scopeId, identifier)`, with `identifier`
/// declared a reference to `target` when one is given: the declaration is
/// what lets a nested `InheritsFrom`'s join validate.
fn address_table(name: &str, target: Option<&str>, policies: TablePolicies) -> TableSchemaBuilder {
    let table = TableSchemaBuilder::new(name).column("scopeId", ColumnType::Uuid);
    let table = match target {
        Some(target) => table.fk_column("identifier", target),
        None => table.column("identifier", ColumnType::Uuid),
    };
    table.policies(policies)
}

fn structural(schema: &PublicSchema) -> PublicSchema {
    let mut schema = schema.clone();
    for table in schema.values_mut() {
        table.policies = TablePolicies::default();
    }
    schema
}

fn deploy(source: &PublicSchema) -> Result<(), DeploymentError> {
    let target = structural(source);
    let hash = SchemaHash::compute(&target);
    prepare_deployment(
        &DeploymentCatalogue::default(),
        DeploymentRequest {
            target_schema_hash: hash,
            schemas: vec![(hash, target)],
            migrations: Vec::new(),
            permissions: source
                .iter()
                .map(|(name, table)| (*name, table.policies.clone()))
                .collect(),
            allow_dropping_history: false,
        },
    )
    .map(|_| ())
}

/// Refused by the deployment and by the validation it shares with
/// `jazz-tools validate`, as `shape`.
#[track_caller]
fn assert_refused(schema: &PublicSchema, shape: UnsupportedPolicy, path: &str) {
    let error = check_deployable_policies(schema).expect_err("the policy is refused");
    assert_eq!(error.unsupported_policy(), Some(shape), "{error}");
    assert!(
        error.to_string().contains(path),
        "it names the path: {error}"
    );
    let deployed = deploy(schema).expect_err("the deployment is refused");
    assert!(
        matches!(deployed, DeploymentError::InvalidPermissions { .. }),
        "{deployed}"
    );
}

/// An `InheritsFrom` reaches one row's policy; a candidate whose policy has
/// an `InheritsFrom` of its own (another table's, or one naming the same
/// table) is refused, under `Or` too and through both update clauses. One
/// level deploys.
///
/// ```text
/// members.insert ─IF─► spaces.insert ─IF─► projects.select          ✗
/// members.insert ─IF─► members.update (WITH CHECK) ─IF─► members    ✗
/// members.insert ─IF─► projects.select                              ✓
/// ```
#[test]
fn an_inherits_from_whose_candidate_has_another_is_refused() {
    let spaces = || {
        address_table(
            SPACES,
            Some("projects"),
            TablePolicies::new()
                .with_select(PolicyExpr::True)
                .with_insert(inherits_from(Operation::Select, "projects")),
        )
    };
    let nested = SchemaBuilder::new()
        .table(projects())
        .table(spaces())
        .table(address_table(
            MEMBERS,
            None,
            TablePolicies::new().with_insert(inherits_from(Operation::Insert, SPACES)),
        ))
        .build();
    assert_refused(
        &nested,
        UnsupportedPolicy::NestedInheritsFrom,
        "$.__e2ee_members.policies.insert.with_check",
    );

    let itself = SchemaBuilder::new()
        .table(address_table(
            MEMBERS,
            Some(MEMBERS),
            TablePolicies::new()
                .with_select(PolicyExpr::True)
                .with_insert(PolicyExpr::Or(vec![
                    PolicyExpr::False,
                    inherits_from(Operation::Update, MEMBERS),
                ]))
                .with_update(
                    Some(PolicyExpr::True),
                    inherits_from(Operation::Select, MEMBERS),
                ),
        ))
        .build();
    assert_refused(
        &itself,
        UnsupportedPolicy::NestedInheritsFrom,
        "$.__e2ee_members.policies.insert.with_check.Or[1]",
    );

    let one_level = SchemaBuilder::new()
        .table(projects())
        .table(spaces())
        .table(address_table(
            MEMBERS,
            None,
            TablePolicies::new().with_insert(inherits_from(Operation::Select, SPACES)),
        ))
        .build();
    check_deployable_policies(&one_level).unwrap();
    deploy(&one_level).unwrap();
}
