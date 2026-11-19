//! `InheritsFrom` shapes the engine cannot evaluate are refused when a
//! deployment (or `jazz-tools validate`) checks its policies, with a typed
//! error, rather than accepted and then failing every check.
//!
//! The tables carry the system-table prefix only because schema validation
//! accepts an undeclared `uuid` address correlation for that table class
//! alone.

use crate::common;

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
    let members = |read: PolicyExpr| {
        SchemaBuilder::new()
            .table(projects())
            .table(
                address_table(
                    MEMBERS,
                    None,
                    TablePolicies::new()
                        .with_select(read)
                        .with_insert(inherits_from(Operation::Select, "projects")),
                )
                .column("accountId", ColumnType::Uuid),
            )
            .build()
    };
    let direct = members(inherits_from(Operation::Select, "projects"));
    assert_refused(
        &direct,
        UnsupportedPolicy::InheritsFromInReadPolicy,
        "$.__e2ee_members.policies.select.using",
    );
    assert_refused(
        &members(PolicyExpr::Or(vec![
            mine(),
            inherits_from(Operation::Select, "projects"),
        ])),
        UnsupportedPolicy::InheritsFromInReadPolicy,
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
    let schema = SchemaBuilder::new()
        .table(projects())
        .table(address_table(
            SPACES,
            None,
            TablePolicies::new()
                .with_select(PolicyExpr::True)
                .with_insert(inherits_from(Operation::Select, "projects")),
        ))
        .table(
            TableSchemaBuilder::new("tasks")
                .fk_column("spaceRow", SPACES)
                .policies(
                    TablePolicies::new()
                        .with_select(PolicyExpr::inherits(Operation::Insert, "spaceRow")),
                ),
        )
        .build();
    assert_refused(
        &schema,
        UnsupportedPolicy::InheritsFromInReadPolicy,
        "$.tasks.policies.select.using",
    );
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
    let members = |policies: TablePolicies| {
        SchemaBuilder::new()
            .table(projects())
            .table(
                address_table(MEMBERS, None, policies.with_select(PolicyExpr::True))
                    .column("accountId", ColumnType::Uuid),
            )
            .build()
    };
    let insert = |check: PolicyExpr| members(TablePolicies::new().with_insert(check));

    for accepted in [
        insert(PolicyExpr::And(vec![mine(), created_here(inserted())])),
        insert(created_here(not(inserted()))),
        members(TablePolicies::new().with_update(Some(mine()), created_here(inserted()))),
    ] {
        check_deployable_policies(&accepted).unwrap();
        deploy(&accepted).unwrap();
    }

    let refused = UnsupportedPolicy::MisplacedInsertedByThisTransaction;
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
        &format!("{insert_path}.And[1].Not.Or[1].Exists"),
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
        let schema = SchemaBuilder::new()
            .table(projects())
            .table(address_table(MEMBERS, None, policies).column("accountId", ColumnType::Uuid))
            .build();
        assert_refused(
            &schema,
            refused,
            &format!("$.__e2ee_members.policies.{slot}.Exists"),
        );
    }

    let inherited = SchemaBuilder::new()
        .table(projects())
        .table(
            address_table(
                MEMBERS,
                None,
                TablePolicies::new()
                    .with_select(PolicyExpr::True)
                    .with_insert(created_here(inserted())),
            )
            .column("accountId", ColumnType::Uuid),
        )
        .table(
            TableSchemaBuilder::new("tasks")
                .fk_column("memberRow", MEMBERS)
                .policies(
                    TablePolicies::new()
                        .with_select(PolicyExpr::inherits(Operation::Insert, "memberRow")),
                ),
        )
        .build();
    assert_refused(&inherited, refused, "$.tasks.policies.select.using");
}

/// An address names one row: an `InheritsFrom` whose `id_column` or
/// `table_column` is an array would inherit from any one row it lists, so
/// it is refused, under `Or` and inside an `exists` too.
///
/// ```text
/// members.insert = IF(scopeId, identifiers: [uuid])     ✗
/// members.insert = IF(scopeIds: [uuid], identifier)     ✗
/// members.insert = EXISTS members(IF(scopeId, identifiers: [uuid]))   ✗
/// ```
#[test]
fn an_inherits_from_with_a_non_scalar_address_column_is_refused() {
    let refused = UnsupportedPolicy::NonScalarAddressColumn;
    let uuids = || ColumnType::Array {
        element: Box::new(ColumnType::Uuid),
    };
    let address = |table_column: &str, id_column: &str| PolicyExpr::InheritsFrom {
        operation: Operation::Select,
        table_column: table_column.to_owned(),
        id_column: id_column.to_owned(),
        tables: vec!["projects".to_owned()],
    };
    let members = |check: PolicyExpr| {
        SchemaBuilder::new()
            .table(projects())
            .table(
                address_table(
                    MEMBERS,
                    None,
                    TablePolicies::new()
                        .with_select(PolicyExpr::True)
                        .with_insert(check),
                )
                .column("identifiers", uuids())
                .column("scopeIds", uuids()),
            )
            .build()
    };
    let insert_path = "$.__e2ee_members.policies.insert.with_check";

    let scalar = members(address("scopeId", "identifier"));
    check_deployable_policies(&scalar).unwrap();
    deploy(&scalar).unwrap();

    assert_refused(
        &members(address("scopeId", "identifiers")),
        refused,
        &format!("{insert_path}.InheritsFrom.id_column"),
    );
    assert_refused(
        &members(PolicyExpr::Or(vec![
            PolicyExpr::False,
            address("scopeIds", "identifier"),
        ])),
        refused,
        &format!("{insert_path}.Or[1].InheritsFrom.table_column"),
    );
    let error = check_deployable_policies(&members(address("scopeId", "identifiers")))
        .unwrap_err()
        .to_string();
    assert!(error.contains("must be a scalar column"), "{error}");

    // Inside an `exists`, the address columns are the consulted table's: a
    // scalar pair passes, a non-scalar one is refused there too.
    let within = |table_column: &str, id_column: &str| PolicyExpr::Exists {
        table: MEMBERS.to_owned(),
        condition: Box::new(address(table_column, id_column)),
    };
    check_deployable_policies(&members(within("scopeId", "identifier"))).unwrap();
    assert_refused(
        &members(within("scopeId", "identifiers")),
        refused,
        &format!("{insert_path}.Exists.condition.InheritsFrom.id_column"),
    );
    assert_refused(
        &members(within("scopeIds", "identifier")),
        refused,
        &format!("{insert_path}.Exists.condition.InheritsFrom.table_column"),
    );
}
