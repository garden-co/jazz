//! Sessions pinned to a published permissions bundle are governed by its
//! policies, whatever the store's own schema allows: two deployments of an
//! app serve their own sessions side by side, and a rollback is publishing
//! the older bundle again.

use crate::common;

#[path = "common/duplex_transport.rs"]
mod duplex_transport;

use std::collections::BTreeMap;

use duplex_transport::duplex;
use jazz::catalogue::BundleId;
use jazz::db::{Db, DbConfig, DbIdentity, FileOptions, ReadOpts, ReadTier, block_on};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::Query;
use jazz::records::Value;
use jazz::tools::public_schema::Schema as PublicSchema;
use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TableSchemaBuilder};

use common::{compile_schema, read_and_allow_all_writes};

fn users(read: PolicyExpr) -> PublicSchema {
    SchemaBuilder::new()
        .table(
            TableSchemaBuilder::new("users")
                .column("name", ColumnType::Text)
                .policies(read_and_allow_all_writes(read)),
        )
        .build()
}

fn config(node: u8, author: AuthorSubject, read: PolicyExpr) -> DbConfig {
    DbConfig::new(
        compile_schema(&users(read)),
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author,
        },
    )
}

/// An authority whose own schema reads everything, with one seeded row.
fn server_with_row(db: Db) -> (Db, RowUuid) {
    db.set_authority();
    let row = RowUuid::from_bytes([7; 16]);
    db.seed_settled_mergeable_for_bootstrap(
        "users",
        row,
        AuthorSubject::SYSTEM,
        BTreeMap::from([("name".to_owned(), Value::String("ada".into()))]),
    )
    .unwrap();
    (db, row)
}

fn client(node: u8, author: AuthorSubject) -> Db {
    block_on(Db::open(config(node, author, PolicyExpr::True))).unwrap()
}

fn local(db: &Db) -> Vec<RowUuid> {
    let prepared = db.prepare_query(&Query::from("users")).unwrap();
    let opts = ReadOpts {
        tier: ReadTier::LocalOnly,
        ..ReadOpts::default()
    };
    block_on(db.all(&prepared, opts))
        .unwrap()
        .iter()
        .map(|row| row.row_uuid())
        .collect()
}

fn pump(server: &Db, clients: &[&Db]) {
    for _ in 0..4 {
        for client in clients {
            block_on(client.tick()).unwrap();
        }
        block_on(server.tick()).unwrap();
    }
    for client in clients {
        block_on(client.tick()).unwrap();
    }
}

/// A client of `server` as `author`, pinned to `bundle`, subscribed to
/// every user.
fn pinned(server: &Db, node: u8, author: AuthorSubject, bundle: BundleId) -> (Db, impl Sized) {
    let db = client(node, author);
    let (client_side, server_side) = duplex();
    let upstream = block_on(db.connect_upstream(client_side));
    server
        .accept_pinned_subscriber(server_side, author, None, Some(bundle))
        .unwrap();
    let prepared = db.prepare_query(&Query::from("users")).unwrap();
    let stream = block_on(db.subscribe(&prepared, ReadOpts::default())).unwrap();
    (db, (upstream, stream))
}

#[test]
fn two_bundles_govern_their_own_sessions_side_by_side() {
    let (server, row) = server_with_row(
        block_on(Db::open(config(0, AuthorSubject::SYSTEM, PolicyExpr::True))).unwrap(),
    );
    let open = block_on(server.publish_bundle(&users(PolicyExpr::True))).unwrap();
    let closed = block_on(server.publish_bundle(&users(PolicyExpr::False))).unwrap();
    assert_ne!(open, closed, "different policies are different bundles");

    let alice = AuthorSubject::for_test_bytes([0x55; 16]);
    let bob = AuthorSubject::for_test_bytes([0x66; 16]);
    let (on_open, _a) = pinned(&server, 1, alice, open);
    let (on_closed, _b) = pinned(&server, 2, bob, closed);
    pump(&server, &[&on_open, &on_closed]);
    assert_eq!(local(&on_open), [row], "the open bundle reads the row");
    assert_eq!(local(&on_closed), [], "the closed bundle does not");

    // Publishing another bundle activates nothing: both keep their own.
    let _later = block_on(server.publish_bundle(&users(PolicyExpr::eq_session(
        "name",
        vec!["user".to_owned()],
    ))))
    .unwrap();
    pump(&server, &[&on_open, &on_closed]);
    assert_eq!(local(&on_open), [row]);
    assert_eq!(local(&on_closed), []);
}

#[test]
fn rolling_back_publishes_the_same_bundle_again() {
    let (server, row) = server_with_row(
        block_on(Db::open(config(0, AuthorSubject::SYSTEM, PolicyExpr::True))).unwrap(),
    );
    let first = block_on(server.publish_bundle(&users(PolicyExpr::True))).unwrap();
    let _second = block_on(server.publish_bundle(&users(PolicyExpr::False))).unwrap();
    let again = block_on(server.publish_bundle(&users(PolicyExpr::True))).unwrap();
    assert_eq!(again, first, "a bundle is addressed by its content");
    assert_eq!(server.catalogue().unwrap().bundles.len(), 2);

    let alice = AuthorSubject::for_test_bytes([0x55; 16]);
    let (db, _keep) = pinned(&server, 1, alice, again);
    pump(&server, &[&db]);
    assert_eq!(local(&db), [row]);
}

#[test]
fn a_bundle_this_store_never_published_is_refused() {
    let server = block_on(Db::open(config(0, AuthorSubject::SYSTEM, PolicyExpr::True))).unwrap();
    server.set_authority();
    let (_client_side, server_side) = duplex();
    let error = server
        .accept_pinned_subscriber(
            server_side,
            AuthorSubject::for_test_bytes([0x55; 16]),
            None,
            Some(BundleId([9; 32])),
        )
        .unwrap_err();
    assert!(error.to_string().contains("not published"), "{error}");
}

#[test]
fn a_bundle_whose_write_schema_was_never_admitted_is_refused() {
    let server = block_on(Db::open(config(0, AuthorSubject::SYSTEM, PolicyExpr::True))).unwrap();
    server.set_authority();
    // Its schema has a column the store never admitted.
    let unadmitted = SchemaBuilder::new()
        .table(
            TableSchemaBuilder::new("users")
                .column("name", ColumnType::Text)
                .column("nickname", ColumnType::Text)
                .policies(read_and_allow_all_writes(PolicyExpr::True)),
        )
        .build();
    let bundle = block_on(server.publish_bundle(&unadmitted)).unwrap();
    let (_client_side, server_side) = duplex();
    let error = server
        .accept_pinned_subscriber(
            server_side,
            AuthorSubject::for_test_bytes([0x55; 16]),
            None,
            Some(bundle),
        )
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("never admitted its write schema"),
        "{error}"
    );
}

#[test]
fn a_store_that_serves_bundles_refuses_unpinned_policy_sessions() {
    let (server, _row) = server_with_row(
        block_on(Db::open(config(0, AuthorSubject::SYSTEM, PolicyExpr::True))).unwrap(),
    );
    let alice = AuthorSubject::for_test_bytes([0x55; 16]);
    // Before any bundle, an unpinned session still opens under the
    // current permissions.
    let (_client_side, server_side) = duplex();
    server
        .accept_pinned_subscriber(server_side, alice, None, None)
        .unwrap();

    block_on(server.publish_bundle(&users(PolicyExpr::True))).unwrap();
    let (_client_side, server_side) = duplex();
    let error = server
        .accept_pinned_subscriber(server_side, alice, None, None)
        .unwrap_err();
    assert!(
        error.to_string().contains("served through createJazz"),
        "{error}"
    );

    // The trusted backend's own sessions need no pin.
    let (_client_side, server_side) = duplex();
    server
        .accept_pinned_subscriber(server_side, AuthorSubject::SYSTEM, None, None)
        .unwrap();
}

#[test]
fn bundles_govern_sessions_after_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("store.sqlite");
    let open = |read| {
        block_on(Db::open_at(
            config(0, AuthorSubject::SYSTEM, read),
            &path,
            FileOptions::default(),
        ))
        .unwrap()
    };
    let closed = {
        let (server, _) = server_with_row(open(PolicyExpr::True));
        block_on(server.publish_bundle(&users(PolicyExpr::False))).unwrap()
    };
    // Reopened with a schema that reads everything: the pinned session
    // still reads by its bundle.
    let server = open(PolicyExpr::True);
    server.set_authority();
    let alice = AuthorSubject::for_test_bytes([0x55; 16]);
    let (db, _keep) = pinned(&server, 1, alice, closed);
    pump(&server, &[&db]);
    assert_eq!(local(&db), []);
}

#[test]
fn a_pinned_session_writes_by_its_bundle() {
    let server = block_on(Db::open(config(0, AuthorSubject::SYSTEM, PolicyExpr::True))).unwrap();
    server.set_authority();
    let mut no_inserts = users(PolicyExpr::True);
    for table in no_inserts.values_mut() {
        table.policies.insert.with_check = Some(PolicyExpr::False);
    }
    let bundle = block_on(server.publish_bundle(&no_inserts)).unwrap();
    let alice = AuthorSubject::for_test_bytes([0x55; 16]);
    let (db, _keep) = pinned(&server, 1, alice, bundle);
    let written = block_on(db.insert(
        "users",
        BTreeMap::from([("name".to_owned(), Value::String("eve".into()))]),
        Default::default(),
    ));
    pump(&server, &[&db]);
    let _ = written;
    assert_eq!(
        server.catalogue().unwrap().bundles.len(),
        1,
        "nothing else was published"
    );
    let prepared = server.prepare_query(&Query::from("users")).unwrap();
    let rows = block_on(server.all(&prepared, ReadOpts::default())).unwrap();
    assert!(rows.is_empty(), "the bundle refused alice's insert");
}

/// Poll `future`, pumping `server` and `clients` between polls.
fn drive<T>(server: &Db, clients: &[&Db], future: impl std::future::Future<Output = T>) -> T {
    let mut future = std::pin::pin!(future);
    let mut context = std::task::Context::from_waker(std::task::Waker::noop());
    for _ in 0..64 {
        if let std::task::Poll::Ready(output) = future.as_mut().poll(&mut context) {
            return output;
        }
        pump(server, clients);
    }
    panic!("operation did not complete");
}

/// The authority validates a pinned session's serializable reads by the
/// session's bundle: a store whose own schema reads nothing must not see
/// the bundle-readable rows the session read as changed.
#[test]
fn a_pinned_sessions_serializable_reads_validate_by_its_bundle() {
    let (server, row) = server_with_row(
        block_on(Db::open(config(
            0,
            AuthorSubject::SYSTEM,
            PolicyExpr::False,
        )))
        .unwrap(),
    );
    let bundle = block_on(server.publish_bundle(&users(PolicyExpr::True))).unwrap();
    let alice = AuthorSubject::for_test_bytes([0x55; 16]);
    let (db, _keep) = pinned(&server, 1, alice, bundle);
    pump(&server, &[&db]);
    assert_eq!(local(&db), [row], "the bundle reads the row");

    let open = jazz::tools::OpenTransactionId::new();
    block_on(db.begin_serializable(open)).unwrap();
    let everyone = db.prepare_query(&Query::from("users")).unwrap();
    let read = drive(&server, &[&db], db.tx_ref(open).all_prepared(&everyone)).unwrap();
    assert_eq!(read.len(), 1);
    drive(
        &server,
        &[&db],
        db.tx_ref(open).insert(
            "users",
            BTreeMap::from([("name".to_owned(), Value::String("bea".into()))]),
            Default::default(),
        ),
    )
    .unwrap();
    let receipt = drive(&server, &[&db], db.commit_serializable_handle(open)).unwrap();
    drive(
        &server,
        &[&db],
        db.wait_for_transaction(receipt.0, jazz::tx::DurabilityTier::Global),
    )
    .expect("its reads still hold under its bundle");
}
