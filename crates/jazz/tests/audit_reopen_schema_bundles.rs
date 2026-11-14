//! Audit (state-space, reopen-schema): a bundle names its write schema by
//! *structural hash*, but the store keeps one admitted schema per *version*
//! (`Inner::bundle` looks the hash up in `admitted`), and several hashes
//! compile to one version (an index or a default changes the hash, not the
//! version). Which hash `admitted` holds depends on history: live, the
//! first one admitted in this process; reopened, the one the catalogue
//! recorded (`_jazz_schemas.n`), replaced by the schema the store opens
//! under. A session pinned to the other hash's bundle may be refused.
//!
//! ```text
//! deploy 1: users(name)                 -> bundle A
//! deploy 2: users(name) + index(name)   -> bundle B   (same version id)
//! alice pinned to A / B, on the live store and after reopens
//! ```

use crate::common;

use jazz::catalogue::BundleId;
use jazz::db::{Db, DbConfig, DbIdentity, FileOptions, block_on};
use jazz::ids::{AuthorSubject, NodeUuid};
use jazz::schema::JazzSchema;
use jazz::tools::public_schema::Schema as PublicSchema;
use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TableSchemaBuilder};

use common::{compile_schema, read_and_allow_all_writes};

#[path = "common/duplex_transport.rs"]
mod duplex_transport;

fn users(indexed: bool) -> PublicSchema {
    let t = TableSchemaBuilder::new("users").column("name", ColumnType::Text);
    let t = if indexed { t.index_only(["name"]) } else { t };
    SchemaBuilder::new()
        .table(t.policies(read_and_allow_all_writes(PolicyExpr::True)))
        .build()
}

fn config(schema: JazzSchema) -> DbConfig {
    DbConfig::new(
        schema,
        DbIdentity {
            node: NodeUuid::from_bytes([0; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )
}

fn pin(server: &Db, bundle: BundleId) -> Result<(), String> {
    let (_client, server_side) = duplex_transport::duplex();
    server
        .accept_pinned_subscriber(
            server_side,
            AuthorSubject::for_test_bytes([0x55; 16]),
            None,
            Some(bundle),
        )
        .map(|_| ())
        .map_err(|e| e.to_string())
}

#[test]
fn audit_reopen_schema_bundle_of_a_same_version_schema_survives_reopen() {
    let a = compile_schema(&users(false));
    let b = compile_schema(&users(true));
    assert_eq!(
        a.version_id(),
        b.version_id(),
        "an index does not change the version"
    );
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store.sqlite");
    let mut outcomes = Vec::new();
    let (bundle_a, bundle_b) = {
        let server = block_on(Db::open_at(
            config(a.clone()),
            &path,
            FileOptions::default(),
        ))
        .unwrap();
        server.set_authority();
        let bundle_a = block_on(server.publish_bundle(&users(false))).unwrap();
        // Deploy 2 admits the indexed schema and publishes its bundle.
        let admitted = block_on(server.admit_schema_with_lenses(b.clone(), &[]));
        outcomes.push(format!(
            "admit b: {:?}",
            admitted.map_err(|e| e.to_string())
        ));
        let bundle_b = block_on(server.publish_bundle(&users(true))).unwrap();
        outcomes.push(format!("live pin A: {:?}", pin(&server, bundle_a)));
        outcomes.push(format!("live pin B: {:?}", pin(&server, bundle_b)));
        (bundle_a, bundle_b)
    };
    for (label, schema) in [("a", a.clone()), ("b", b.clone())] {
        let server = block_on(Db::open_at(config(schema), &path, FileOptions::default())).unwrap();
        server.set_authority();
        outcomes.push(format!(
            "reopen under {label}, pin A: {:?}",
            pin(&server, bundle_a)
        ));
        outcomes.push(format!(
            "reopen under {label}, pin B: {:?}",
            pin(&server, bundle_b)
        ));
    }
    for line in &outcomes {
        eprintln!("OUTCOME {line}");
    }
    assert!(
        outcomes.iter().all(|line| line.ends_with("Ok(())")),
        "every pin to a published bundle whose version the store admitted opens: {outcomes:#?}"
    );
}

/// The server's deploy pipeline (`prepare_deployment` ->
/// `prepare_catalogue_change` -> `Db::apply_catalogue`, as
/// `jazz-server/src/server/deployment.rs` runs it), for a second deploy that
/// only adds an index. Its target shares the admitted version, so
/// `prepare_catalogue_change` admits nothing, yet its bundle is new (it
/// would end every session pinned to the first bundle). Sessions the new
/// backend pins to it must open.
///
/// ```text
/// backend v1 ──POST /bundles {users}──────────► core   bundle A
/// backend v2 ──POST /bundles {users + index}──► core   bundle B (A's sessions end)
/// alice ──pinned to B──► core   refused?
/// ```
#[test]
fn audit_reopen_schema_index_only_deploy_pins_its_bundle() {
    use jazz::catalogue::CataloguePut;
    use jazz::tools::deployment::{
        DeploymentCatalogue, DeploymentRequest, prepare_catalogue_change, prepare_deployment,
    };
    use jazz::tools::public_schema::{SchemaHash, TablePolicies};

    let structural = |indexed| {
        let mut s = users(indexed);
        for t in s.values_mut() {
            t.policies = TablePolicies::default();
        }
        s
    };
    let deploy = |server: &Db, indexed: bool| -> BundleId {
        let rows = server.catalogue().unwrap();
        let stored = DeploymentCatalogue {
            schemas: rows.schemas.iter().map(|s| s.schema.clone()).collect(),
            migrations: Vec::new(),
        };
        let source = users(indexed);
        let target = structural(indexed);
        let hash = SchemaHash::compute(&target);
        let permissions: std::collections::HashMap<_, _> = source
            .iter()
            .map(|(n, t)| (*n, t.policies.clone()))
            .collect();
        let prepared = prepare_deployment(
            &stored,
            DeploymentRequest {
                target_schema_hash: hash,
                schemas: vec![(hash, target.clone())],
                migrations: Vec::new(),
                permissions: permissions.clone(),
            },
        )
        .unwrap();
        let permissions: jazz::catalogue::Permissions = permissions.into_iter().collect();
        let put = CataloguePut {
            schemas: prepared.schemas().iter().map(|s| (s.clone(), 1)).collect(),
            bundles: vec![(hash, permissions.clone(), 1)],
            ..Default::default()
        };
        let change = prepare_catalogue_change(&prepared, &rows, put).unwrap();
        eprintln!(
            "DEPLOY indexed={indexed}: admits {} version(s)",
            change.admit.len()
        );
        block_on(server.apply_catalogue(change)).unwrap();
        jazz::catalogue::bundle_id(&hash, &permissions).unwrap()
    };
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store.sqlite");
    // The core opens on the genesis schema the first deploy brings.
    let server = block_on(Db::open_at(
        config(compile_schema(&structural(false))),
        &path,
        FileOptions::default(),
    ))
    .unwrap();
    server.set_authority();
    let a = deploy(&server, false);
    let b = deploy(&server, true);
    let outcomes = vec![
        format!("pin A: {:?}", pin(&server, a)),
        format!("pin B: {:?}", pin(&server, b)),
    ];
    eprintln!("OUTCOMES {outcomes:#?}");
    assert!(outcomes[1].ends_with("Ok(())"), "{outcomes:#?}");
}
