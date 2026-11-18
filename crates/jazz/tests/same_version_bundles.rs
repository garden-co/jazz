//! Bundles of one schema version. A bundle names its write schema by
//! *structural hash*, and several hashes compile to one version: an index
//! or a column default changes the hash, not the version. A deploy that
//! only adds an index or changes a default therefore publishes a new bundle
//! for a version the store admitted already, and both bundles stay live
//! (canary beside prod, or the old deployment until it is retired).
//!
//! ```text
//! deploy 1: users(name)                 -> bundle A
//! deploy 2: users(name) + index(name)   -> bundle B   (same version id)
//! sessions pinned to A and to B, live and after reopening under either
//! ```
//!
//! The rule when both are live: a pinned session writes by its own
//! bundle's structural schema, so by its defaults; the store's indexes are
//! the union of every admitted hash's, and an index is never dropped; a
//! stored column's fill for rows written before it existed is the default
//! of the admission that added it, and a later default-only change never
//! rewrites it.

use crate::common;

use std::collections::BTreeMap;
use std::path::Path;

use jazz::catalogue::{BundleId, CataloguePut};
use jazz::db::{Db, DbConfig, DbIdentity, FileOptions, InsertOptions, ReadOpts, block_on};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::Query;
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::deployment::{
    DeploymentCatalogue, DeploymentRequest, prepare_catalogue_change, prepare_deployment,
};
use jazz::tools::public_schema::{Schema as PublicSchema, SchemaHash, TablePolicies};
use jazz::tools::{
    ColumnType, PolicyExpr, SchemaBuilder, TableSchemaBuilder, Value as PublicValue,
};

use common::{compile_schema, read_and_allow_all_writes};

#[path = "common/duplex_transport.rs"]
mod duplex_transport;

/// `users(name)`, with an index on `name` when `indexed`, and with a
/// `role` column defaulting to `role` when one is given.
fn users(indexed: bool, role: Option<&str>) -> PublicSchema {
    let t = TableSchemaBuilder::new("users").column("name", ColumnType::Text);
    let t = match role {
        Some(role) => {
            t.column_with_default("role", ColumnType::Text, PublicValue::Text(role.into()))
        }
        None => t,
    };
    let t = if indexed { t.index_only(["name"]) } else { t };
    SchemaBuilder::new()
        .table(t.policies(read_and_allow_all_writes(PolicyExpr::True)))
        .build()
}

/// `schema` without its policies, as a deployment carries it.
fn structural(schema: &PublicSchema) -> PublicSchema {
    let mut schema = schema.clone();
    for table in schema.values_mut() {
        table.policies = TablePolicies::default();
    }
    schema
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

/// The authority's file at `path`, opened under `schema`'s structure as a
/// server core opens it.
fn core_at(schema: &PublicSchema, path: &Path) -> Db {
    let db = block_on(Db::open_at(
        config(compile_schema(&structural(schema))),
        path,
        FileOptions::default(),
    ))
    .unwrap();
    db.set_authority();
    db
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

/// Deploy `source` through the server's own pipeline
/// (`prepare_deployment` -> `prepare_catalogue_change` ->
/// `Db::apply_catalogue`, as `jazz-server/src/server/deployment.rs` runs
/// it): its bundle, and how many schemas the change admitted.
fn deploy(server: &Db, source: &PublicSchema) -> (BundleId, usize) {
    let rows = server.catalogue().unwrap();
    let stored = DeploymentCatalogue {
        schemas: rows.schemas.iter().map(|s| s.schema.clone()).collect(),
        migrations: Vec::new(),
        ..Default::default()
    };
    let target = structural(source);
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
            allow_dropping_history: false,
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
    let admitted = change.admit.len();
    block_on(server.apply_catalogue(change)).unwrap();
    (
        jazz::catalogue::bundle_id(&hash, &permissions).unwrap(),
        admitted,
    )
}

/// The declared indexes SQLite holds on stored table `users` (not the
/// ones every table has).
fn user_indexes(path: &Path) -> Vec<String> {
    let conn = rusqlite::Connection::open(path).unwrap();
    let mut statement = conn
        .prepare(
            "SELECT name FROM sqlite_master WHERE type = 'index' AND tbl_name = 'users' \
             AND name LIKE 'users\\_\\_ix(%' ESCAPE '\\' ORDER BY name",
        )
        .unwrap();
    statement
        .query_map([], |r| r.get::<_, String>(0))
        .unwrap()
        .map(Result::unwrap)
        .collect()
}

/// Each `users` row's `name` and `role`, by row id.
fn names_and_roles(db: &Db) -> Vec<(String, String)> {
    let prepared = db.prepare_query(&Query::from("users")).unwrap();
    let mut rows: Vec<_> = block_on(db.all(&prepared, ReadOpts::default()))
        .unwrap()
        .iter()
        .map(|row| {
            let cell = |column: &str| match row
                .cells()
                .find(|(name, _)| *name == column)
                .and_then(|(_, value)| value.cloned())
            {
                Some(Value::String(text)) => text,
                other => panic!("{column}: {other:?}"),
            };
            (row.row_uuid(), (cell("name"), cell("role")))
        })
        .collect();
    rows.sort_by_key(|(id, _)| *id);
    rows.into_iter().map(|(_, row)| row).collect()
}

fn insert_name(db: &Db, n: u8, name: &str) {
    block_on(db.insert(
        "users",
        BTreeMap::from([("name".to_owned(), Value::String(name.to_owned()))]),
        InsertOptions {
            row_id: Some(RowUuid::from_bytes([n; 16])),
            ..Default::default()
        },
    ))
    .unwrap();
}

/// An index-only deploy (deploy 2) admits its schema although its version
/// is admitted, so the store builds the new index, and sessions pinned to
/// either deployment's bundle open: publishing bundle B ends A's sessions
/// and they reconnect pinned to B, which used to be refused ("this store
/// never admitted its write schema"), locking every user out.
///
/// ```text
/// backend v1 ──POST /bundles {users}──────────► core   bundle A
/// backend v2 ──POST /bundles {users + index}──► core   bundle B, index built
/// alice ──pinned to A──► ok      bob ──pinned to B──► ok (was: refused)
/// ```
#[test]
fn an_index_only_deploy_builds_its_index_and_pins_both_bundles() {
    let (plain, indexed) = (users(false, None), users(true, None));
    assert_eq!(
        compile_schema(&plain).version_id(),
        compile_schema(&indexed).version_id(),
        "an index does not change the version"
    );
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("core.sqlite");
    {
        // The core opens on the genesis schema the first deploy brings.
        let server = core_at(&plain, &path);
        let (a, _) = deploy(&server, &plain);
        let (b, admitted) = deploy(&server, &indexed);
        assert_ne!(a, b);
        assert_eq!(admitted, 1, "the index-only target is admitted");
        assert_eq!(pin(&server, a), Ok(()));
        assert_eq!(pin(&server, b), Ok(()));
        // Deploying it again (new permissions, same structure) still pins.
        let mut tightened = indexed.clone();
        for table in tightened.values_mut() {
            table.policies = read_and_allow_all_writes(PolicyExpr::False);
        }
        let (c, _) = deploy(&server, &tightened);
        assert_eq!(pin(&server, c), Ok(()));
    }
    assert_eq!(
        user_indexes(&path).len(),
        1,
        "the index deploy 2 declares is built: {:?}",
        user_indexes(&path)
    );
}

/// Deploying the index-only schema again admits nothing (admitting resets
/// every session): its row is stored and its version admitted, though `n`
/// stays with the hash first admitted as that version. Both bundles still
/// pin, and the index stays.
#[test]
fn a_repeated_index_only_deploy_admits_nothing() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("jazz.sqlite");
    let (a, b) = (users(false, None), users(true, None));
    let server = core_at(&a, &path);
    let (bundle_a, _) = deploy(&server, &a);
    let (bundle_b, admitted) = deploy(&server, &b);
    assert_eq!(admitted, 1, "the first index-only deploy admits B");
    let (again, admitted) = deploy(&server, &b);
    assert_eq!(again, bundle_b);
    assert_eq!(admitted, 0, "a repeated deploy of B admits nothing");
    pin(&server, bundle_a).unwrap();
    pin(&server, bundle_b).unwrap();
    assert_eq!(user_indexes(&path).len(), 1);
    drop(server);
    // Also after reopening under the genesis, as a server core does.
    let server = core_at(&a, &path);
    let (_, admitted) = deploy(&server, &b);
    assert_eq!(admitted, 0, "after reopen too");
    pin(&server, bundle_b).unwrap();
}

/// Both bundles of one version resolve on the live store and after
/// reopening under either of its schemas: a bundle's schema is read from
/// the catalogue by its hash, and only its version must be admitted.
#[test]
fn bundles_of_one_version_pin_live_and_after_reopen_under_either_schema() {
    let (plain, indexed) = (users(false, None), users(true, None));
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store.sqlite");
    let mut outcomes = Vec::new();
    let (bundle_a, bundle_b) = {
        let server = core_at(&plain, &path);
        let bundle_a = block_on(server.publish_bundle(&plain)).unwrap();
        // Deploy 2 admits the indexed schema and publishes its bundle.
        block_on(server.admit_schema_with_lenses(compile_schema(&structural(&indexed)), &[]))
            .unwrap();
        let bundle_b = block_on(server.publish_bundle(&indexed)).unwrap();
        outcomes.push(("live, pin A", pin(&server, bundle_a)));
        outcomes.push(("live, pin B", pin(&server, bundle_b)));
        (bundle_a, bundle_b)
    };
    assert_eq!(user_indexes(&path).len(), 1);
    for (label, schema) in [("a", &plain), ("b", &indexed), ("a again", &plain)] {
        let server = core_at(schema, &path);
        outcomes.push((
            if label == "a" {
                "reopen under a, pin A"
            } else {
                "reopen, pin A"
            },
            pin(&server, bundle_a),
        ));
        outcomes.push(("reopen, pin B", pin(&server, bundle_b)));
    }
    assert!(
        outcomes.iter().all(|(_, outcome)| outcome.is_ok()),
        "every pin to a published bundle whose version the store admitted opens: {outcomes:#?}"
    );
    assert_eq!(
        user_indexes(&path).len(),
        1,
        "reopening under the unindexed schema drops nothing"
    );
}

/// A default-only deploy: both bundles pin, and each writes by its own
/// default while both are live, before and after a reopen under either
/// schema. Rows already stored keep what they were written with.
///
/// ```text
/// deploy 1: users(name, role = "member")  -> bundle A
/// deploy 2: users(name, role = "guest")   -> bundle B   (same version id)
/// insert {name} pinned to A -> role "member";  pinned to B -> role "guest"
/// ```
#[test]
fn a_default_only_deploy_writes_by_each_bundles_own_default() {
    let (member, guest) = (users(false, Some("member")), users(false, Some("guest")));
    let version = compile_schema(&member).version_id();
    assert_eq!(
        version,
        compile_schema(&guest).version_id(),
        "a default does not change the version"
    );
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("core.sqlite");
    let write = |server: &Db, bundle: BundleId, n: u8| {
        let view = server
            .pinned_view(version, Some(&bundle))
            .unwrap()
            .expect("the bundle resolves");
        insert_name(&view, n, &format!("u{n}"));
    };
    let (a, b) = {
        let server = core_at(&member, &path);
        let (a, _) = deploy(&server, &member);
        let (b, admitted) = deploy(&server, &guest);
        assert_eq!(admitted, 1);
        assert_eq!(pin(&server, b), Ok(()));
        write(&server, a, 1);
        write(&server, b, 2);
        (a, b)
    };
    for (n, schema) in [(3u8, &guest), (5, &member)] {
        let server = core_at(schema, &path);
        assert_eq!(pin(&server, a), Ok(()));
        assert_eq!(pin(&server, b), Ok(()));
        write(&server, a, n);
        write(&server, b, n + 1);
    }
    let server = core_at(&member, &path);
    let role = |n: u8, role: &str| (format!("u{n}"), role.to_owned());
    assert_eq!(
        names_and_roles(&server),
        vec![
            role(1, "member"),
            role(2, "guest"),
            role(3, "member"),
            role(4, "guest"),
            role(5, "member"),
            role(6, "guest"),
        ]
    );
}

/// A bundle whose write version this store never admitted is still
/// refused, with the typed error naming why, and so is one never published.
#[test]
fn a_bundle_of_a_version_never_admitted_is_refused() {
    let plain = users(false, None);
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("core.sqlite");
    let server = core_at(&plain, &path);
    // Another version (a new column): published, never admitted.
    let other = users(false, Some("member"));
    assert_ne!(
        compile_schema(&plain).version_id(),
        compile_schema(&other).version_id()
    );
    let unadmitted = block_on(server.publish_bundle(&other)).unwrap();
    let refused = pin(&server, unadmitted).unwrap_err();
    assert!(
        refused.contains("never admitted its write schema"),
        "{refused}"
    );
    let unknown = BundleId::from_hex(&"cd".repeat(32)).unwrap();
    let refused = pin(&server, unknown).unwrap_err();
    assert!(
        refused.contains("is not published on this store"),
        "{refused}"
    );
}
