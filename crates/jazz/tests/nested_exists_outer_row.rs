//! An `exists` nested inside another `exists` that refers to the outer row
//! (`__jazz_outer_row`) is refused when it is deployed or validated, since
//! the engine does not agree which row it names. A store that already holds
//! such a policy (published before the refusal, or by hand) still opens and
//! serves its bundle: the refusal never runs on reopening a catalogue.

use crate::common;
use jazz_types::legacy_test_future::ResultFutureExt;

#[path = "common/duplex_transport.rs"]
mod duplex_transport;

use jazz::db::{Db, DbConfig, DbIdentity, FileOptions, block_on};
use jazz::ids::{AuthorSubject, NodeUuid};
use jazz::tools::deployment::{
    DeploymentCatalogue, DeploymentError, DeploymentRequest, prepare_deployment,
};
use jazz::tools::public_schema::{Schema as PublicSchema, SchemaHash, TablePolicies};
use jazz::tools::public_schema_convert::{NESTED_OUTER_ROW, check_deployable_policies};
use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TableSchemaBuilder};

use common::{allow_all_policies, compile_schema, exists, outer_eq, read_and_allow_all_writes};

/// `docs` readable by `read`; `members` and `teams` point at a doc.
fn schema(read: PolicyExpr) -> PublicSchema {
    SchemaBuilder::new()
        .table(
            TableSchemaBuilder::new("docs")
                .column("title", ColumnType::Text)
                .policies(read_and_allow_all_writes(read)),
        )
        .table(
            TableSchemaBuilder::new("members")
                .fk_column("doc", "docs")
                .policies(allow_all_policies()),
        )
        .table(
            TableSchemaBuilder::new("teams")
                .fk_column("doc", "docs")
                .policies(allow_all_policies()),
        )
        .build()
}

/// A doc with a member, and (nested in that `exists`) a team: the inner
/// `exists` refers to the outer row.
fn nested_outer() -> PolicyExpr {
    exists(
        "members",
        vec![
            outer_eq("doc", "id"),
            exists("teams", vec![outer_eq("doc", "id")]),
        ],
    )
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

#[test]
fn a_nested_exists_referring_to_the_outer_row_is_refused_on_deploy() {
    let error = deploy(&schema(nested_outer())).unwrap_err().to_string();
    assert!(error.contains(NESTED_OUTER_ROW), "{error}");
    assert!(
        error.contains("$.docs.policies.select.using.Exists.condition.And[1].Exists"),
        "it names the path: {error}"
    );
    assert!(error.contains("hopTo"), "and the way on: {error}");
    // `jazz-tools validate` refuses it alike.
    assert!(check_deployable_policies(&schema(nested_outer())).is_err());
}

#[test]
fn outer_row_references_outside_a_nested_exists_still_deploy() {
    // One level: the outer row is the row under check.
    deploy(&schema(exists("members", vec![outer_eq("doc", "id")]))).unwrap();
    // Nested, without referring to the outer row.
    deploy(&schema(exists(
        "members",
        vec![
            outer_eq("doc", "id"),
            PolicyExpr::Exists {
                table: "teams".into(),
                condition: Box::new(PolicyExpr::True),
            },
        ],
    )))
    .unwrap();
    // Two `exists` side by side, each over the row under check.
    deploy(&schema(PolicyExpr::and(vec![
        exists("members", vec![outer_eq("doc", "id")]),
        exists("teams", vec![outer_eq("doc", "id")]),
    ])))
    .unwrap();
}

#[test]
fn a_store_holding_such_a_policy_still_opens() {
    let config = || {
        DbConfig::new(
            compile_schema(&schema(nested_outer())),
            DbIdentity {
                node: NodeUuid::from_bytes([0; 16]),
                author: AuthorSubject::SYSTEM,
            },
        )
    };
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("core.sqlite");
    let bundle = {
        let db = block_on(Db::open_at(config(), &path, FileOptions::default())).unwrap();
        jazz::db::block_on(db.set_authority()).unwrap();
        // Published past the deployment check, as a store written before
        // it (or by hand) holds it.
        block_on(db.publish_bundle(&schema(nested_outer()))).unwrap()
    };
    let db = block_on(Db::open_at(config(), &path, FileOptions::default())).unwrap();
    jazz::db::block_on(db.set_authority()).unwrap();
    let rows = db.catalogue().unwrap();
    assert!(
        rows.bundles.iter().any(|row| row.id == bundle),
        "the bundle is still there"
    );
    // And it still serves sessions pinned to it.
    let (_client, server_side) = duplex_transport::duplex();
    db.accept_pinned_subscriber(
        server_side,
        AuthorSubject::for_test_bytes([0x55; 16]),
        None,
        Some(bundle),
    )
    .unwrap();
}
