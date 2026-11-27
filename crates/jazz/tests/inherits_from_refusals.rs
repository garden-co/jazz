//! `InheritsFrom` shapes the engine cannot evaluate are refused when a
//! deployment (or `jazz-tools validate`) checks its policies, with a specific
//! diagnostic, rather than accepted and then failing every check.
//!
//! Deployments use the registered system-table fragment with test policies.

use crate::common;

use jazz::tools::deployment::artifact::system_tables::{MEMBERS, SPACES, with_e2ee_fragment};
use jazz::tools::deployment::{
    DeploymentCatalogue, DeploymentError, DeploymentRequest, prepare_deployment,
};
use jazz::tools::public_schema::{Schema as PublicSchema, SchemaHash, TableName, TablePolicies};
use jazz::tools::public_schema_convert::check_deployable_policies;
use jazz::tools::{ColumnType, Operation, PolicyExpr, SchemaBuilder, TableSchemaBuilder};

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

fn fixture(policies: impl IntoIterator<Item = (&'static str, TablePolicies)>) -> PublicSchema {
    let mut schema = with_e2ee_fragment(&SchemaBuilder::new().table(projects()).build());
    for (name, policies) in policies {
        schema.get_mut(&TableName::new(name)).unwrap().policies = policies;
    }
    schema
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

/// Assert that validation and deployment report the same policy path and
/// refusal reason.
#[track_caller]
pub(super) fn assert_refused(schema: &PublicSchema, message: &str, path: &str) {
    let diagnostic = assert_validation_refused(schema, message, path);
    let deployed = deploy(schema).expect_err("the deployment is refused");
    match deployed {
        DeploymentError::InvalidPermissions { reason, .. } => assert_eq!(reason, diagnostic),
        other => panic!("expected invalid permissions: {other}"),
    }
}

#[track_caller]
fn assert_validation_refused(schema: &PublicSchema, message: &str, path: &str) -> String {
    let error = check_deployable_policies(schema).expect_err("the policy is refused");
    assert!(error.message().contains(message), "{error}");
    let diagnostic = format!("{path}: {}", error.message());
    assert_eq!(error.to_string(), diagnostic);
    diagnostic
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
        TablePolicies::new()
            .with_select(PolicyExpr::True)
            .with_insert(inherits_from(Operation::Select, "projects"))
    };
    let nested = fixture([
        (SPACES, spaces()),
        (
            MEMBERS,
            TablePolicies::new().with_insert(inherits_from(Operation::Insert, SPACES)),
        ),
    ]);
    assert_refused(
        &nested,
        "policy has an `InheritsFrom` of its own",
        "$.__e2ee_members.policies.insert.with_check.InheritsFrom",
    );

    let itself = fixture([(
        MEMBERS,
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
    )]);
    assert_refused(
        &itself,
        "policy has an `InheritsFrom` of its own",
        "$.__e2ee_members.policies.insert.with_check.Or[1].InheritsFrom",
    );

    let one_level = fixture([
        (SPACES, spaces()),
        (
            MEMBERS,
            TablePolicies::new().with_insert(inherits_from(Operation::Select, SPACES)),
        ),
    ]);
    check_deployable_policies(&one_level).unwrap();
    deploy(&one_level).unwrap();
}

/// No read policy uses `InheritsFrom` (Jazz derives it for write policies
/// only), and no sync support ships for one: a read policy holding one,
/// directly or under `Or`, is refused. The read rules Jazz derives, an
/// `exists` over the members table, deploy.
///
/// ```text
/// members.select = IF(Select)                  ✗
/// members.select = mine ∨ IF(Select)           ✗
/// members.select = mine ∨ exists members(…)    ✓
/// ```
#[test]
fn an_inherits_from_in_a_read_policy_is_refused() {
    let mine = || PolicyExpr::eq_session("accountId", vec!["user".to_owned()]);
    let members = |read| {
        fixture([(
            MEMBERS,
            TablePolicies::new()
                .with_select(read)
                .with_insert(inherits_from(Operation::Select, "projects")),
        )])
    };
    let direct = members(inherits_from(Operation::Select, "projects"));
    assert_refused(
        &direct,
        "`InheritsFrom` is not supported in a read policy",
        "$.__e2ee_members.policies.select.using",
    );
    assert_refused(
        &members(PolicyExpr::Or(vec![
            mine(),
            inherits_from(Operation::Select, "projects"),
        ])),
        "`InheritsFrom` is not supported in a read policy",
        "$.__e2ee_members.policies.select.using.Or[1]",
    );

    let derived = members(PolicyExpr::Or(vec![
        mine(),
        common::exists(
            MEMBERS,
            vec![
                common::outer_eq("scopeId", "scopeId"),
                common::outer_eq("identifier", "identifier"),
                mine(),
            ],
        ),
    ]));
    check_deployable_policies(&derived).unwrap();
    deploy(&derived).unwrap();
}

/// A read policy that reaches an `InheritsFrom` only through an inherited
/// write policy is refused too: `tasks` reads by inheriting the insert
/// policy of the row its `spaceRow` names, and that policy is an
/// `InheritsFrom`.
///
/// ```text
/// tasks.select ─INHERITS insert─► spaces.insert ─IF─► projects.select   ✗
/// ```
#[test]
fn a_read_policy_reaching_inherits_from_through_an_inherited_policy_is_refused() {
    let mut schema = fixture([(
        SPACES,
        TablePolicies::new()
            .with_select(PolicyExpr::True)
            .with_insert(inherits_from(Operation::Select, "projects")),
    )]);
    let tasks = |table: TableSchemaBuilder, read| {
        SchemaBuilder::new()
            .table(table.policies(TablePolicies::new().with_select(read)))
            .build()
    };
    let message = "`InheritsFrom` is not supported in a read policy";
    let path = "$.tasks.policies.select.using";

    // The fragment uses plain UUID addresses and the deploy forbids app
    // foreign keys to system tables. Exercise reverse inheritance on the
    // registered fragment, and validate the forward-reference AST separately.
    schema.extend(tasks(
        TableSchemaBuilder::new("tasks").column("name", ColumnType::Text),
        PolicyExpr::inherits_referencing(Operation::Insert, SPACES, "identifier"),
    ));
    assert_refused(&schema, message, path);

    schema.extend(tasks(
        TableSchemaBuilder::new("tasks").fk_column("spaceRow", SPACES),
        PolicyExpr::inherits(Operation::Insert, "spaceRow"),
    ));
    assert_validation_refused(&schema, message, path);
}

/// The creator clause's `exists`: the `projects` row this member row names,
/// freshly inserted by the checking transaction.
fn created_here(inserted: PolicyExpr) -> PolicyExpr {
    common::exists(
        "projects",
        vec![common::outer_eq("id", "identifier"), inserted],
    )
}

/// `InsertedByThisTransaction` means something only inside an `exists` of
/// an insert or update WITH CHECK; anywhere else it is false, so under a
/// `NOT` it holds for every row. Every other placement is refused: at the
/// top, under a `NOT` outside the `exists` (at any depth), in a read,
/// update USING or delete policy, and in a read policy that inherits it.
/// Only Jazz's generator writes the node, so the policies are built here.
///
/// ```text
/// insert ✓  exists projects(… ∧ inserted)          update new ✓ (same)
/// insert ✓  exists projects(… ∧ ¬inserted)
/// insert ✗  inserted        insert ✗ ¬inserted     insert ✗ ¬exists projects(… ∧ inserted)
/// insert ✗  mine ∧ ¬(mine ∨ exists projects(… ∧ inserted))
/// read ✗ / update old ✗ / delete ✗  exists projects(… ∧ inserted)
/// tasks.read ─INHERITS insert─► members.insert = exists projects(… ∧ inserted)   ✗
/// ```
#[test]
fn inserted_by_this_transaction_outside_a_write_check_exists_is_refused() {
    let mine = || PolicyExpr::eq_session("accountId", vec!["user".to_owned()]);
    let inserted = || PolicyExpr::InsertedByThisTransaction;
    let not = |expr: PolicyExpr| PolicyExpr::Not(Box::new(expr));
    let members = |policies| fixture([(MEMBERS, policies)]);
    let insert = |check| {
        members(
            TablePolicies::new()
                .with_select(PolicyExpr::True)
                .with_insert(check),
        )
    };

    for accepted in [
        insert(PolicyExpr::And(vec![mine(), created_here(inserted())])),
        insert(created_here(not(inserted()))),
        members(TablePolicies::new().with_update(Some(mine()), created_here(inserted()))),
    ] {
        check_deployable_policies(&accepted).unwrap();
        deploy(&accepted).unwrap();
    }

    let refused = "`InsertedByThisTransaction` is supported only inside an `exists`";
    let insert_path = "$.__e2ee_members.policies.insert.with_check";
    assert_refused(
        &insert(inserted()),
        refused,
        &format!("{insert_path}.InsertedByThisTransaction"),
    );
    assert_refused(
        &insert(not(inserted())),
        refused,
        &format!("{insert_path}.Not.InsertedByThisTransaction"),
    );
    assert_refused(
        &insert(not(created_here(inserted()))),
        refused,
        &format!("{insert_path}.Not.Exists.condition.And[1].InsertedByThisTransaction"),
    );
    assert_refused(
        &insert(PolicyExpr::And(vec![
            mine(),
            not(PolicyExpr::Or(vec![mine(), created_here(inserted())])),
        ])),
        refused,
        &format!(
            "{insert_path}.And[1].Not.Or[1].Exists.condition.And[1].InsertedByThisTransaction"
        ),
    );
    for (policies, slot) in [
        (
            TablePolicies::new().with_select(created_here(inserted())),
            "select.using",
        ),
        (
            TablePolicies::new().with_update(Some(created_here(inserted())), mine()),
            "update.using",
        ),
        (
            TablePolicies::new().with_delete(created_here(inserted())),
            "delete.using",
        ),
    ] {
        let schema = members(policies);
        assert_refused(
            &schema,
            refused,
            &format!(
                "$.__e2ee_members.policies.{slot}.Exists.condition.And[1].InsertedByThisTransaction"
            ),
        );
    }

    let mut inherited = members(
        TablePolicies::new()
            .with_select(PolicyExpr::True)
            .with_insert(created_here(inserted())),
    );
    let path = "$.tasks.policies.select.using";
    inherited.extend(
        SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("tasks")
                    .column("name", ColumnType::Text)
                    .policies(
                        TablePolicies::new().with_select(PolicyExpr::inherits_referencing(
                            Operation::Insert,
                            MEMBERS,
                            "identifier",
                        )),
                    ),
            )
            .build(),
    );
    assert_refused(&inherited, refused, path);

    inherited.extend(
        SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("tasks")
                    .fk_column("memberRow", MEMBERS)
                    .policies(
                        TablePolicies::new()
                            .with_select(PolicyExpr::inherits(Operation::Insert, "memberRow")),
                    ),
            )
            .build(),
    );
    assert_validation_refused(&inherited, refused, path);
}

/// An address names one row: an `InheritsFrom` whose `id_column` or
/// `table_column` is an array would inherit from any one row it lists, so
/// it is refused, under `Or` and inside an `exists` too.
///
/// ```text
/// members.insert = exists addresses(IF(scopeId, identifiers: [uuid]))       ✗
/// members.insert = exists addresses(false ∨ IF(scopeIds: [uuid], identifier)) ✗
/// ```
#[test]
fn an_inherits_from_with_a_non_scalar_address_column_is_refused() {
    let refused = "must be a scalar column";
    let uuids = || ColumnType::Array {
        element: Box::new(ColumnType::Uuid),
    };
    let address = |table_column: &str, id_column: &str| PolicyExpr::InheritsFrom {
        operation: Operation::Select,
        table_column: table_column.to_owned(),
        id_column: id_column.to_owned(),
        tables: vec!["projects".to_owned()],
    };
    let members = |check| {
        fixture([(
            MEMBERS,
            TablePolicies::new()
                .with_select(PolicyExpr::True)
                .with_insert(check),
        )])
    };
    let insert_path = "$.__e2ee_members.policies.insert.with_check";

    let scalar = members(address("scopeId", "identifier"));
    check_deployable_policies(&scalar).unwrap();
    deploy(&scalar).unwrap();

    // The registered fragment has only scalar columns. An Exists in the
    // system write policy lets us exercise addresses on an app table.
    let within = |condition| {
        let mut schema = members(PolicyExpr::Exists {
            table: "addresses".to_owned(),
            condition: Box::new(condition),
        });
        schema.extend(
            SchemaBuilder::new()
                .table(
                    TableSchemaBuilder::new("addresses")
                        .column("scopeId", ColumnType::Uuid)
                        .fk_column("identifier", "projects")
                        .column("identifiers", uuids())
                        .column("scopeIds", uuids()),
                )
                .build(),
        );
        schema
    };
    check_deployable_policies(&within(address("scopeId", "identifier"))).unwrap();
    let non_scalar_id = within(address("scopeId", "identifiers"));
    assert_refused(
        &non_scalar_id,
        refused,
        &format!("{insert_path}.Exists.condition.InheritsFrom.id_column"),
    );
    let error = check_deployable_policies(&non_scalar_id).unwrap_err();
    assert!(
        error.message().contains("must be a scalar column"),
        "{error}"
    );
    assert_refused(
        &within(PolicyExpr::Or(vec![
            PolicyExpr::False,
            address("scopeIds", "identifier"),
        ])),
        refused,
        &format!("{insert_path}.Exists.condition.Or[1].InheritsFrom.table_column"),
    );
}
