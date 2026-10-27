//! Session write checks: WITH CHECK compares the new row with
//! the other rows as they were before the write, and a trusted link's
//! session write is staged on the replica and checked as that session, with
//! the claims the link vouches for, at the authority. Before any policy,
//! the authority refuses every write by an anonymous identity and a write
//! timestamped too far ahead of its clock.

use crate::common;

#[path = "common/duplex_transport.rs"]
mod duplex_transport;

use std::collections::BTreeMap;

use duplex_transport::duplex;
use jazz::db::WriteIdentity;
use jazz::db::{
    Db, DbConfig, DbIdentity, DeleteOptions, InsertOptions, ReadOpts, ReadTier, UpdateOptions,
    UpsertOptions, block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::Query;
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder};

use common::{allow_all_policies, compile_schema, exists, outer_eq, session_eq};

const SUB: [&str; 2] = ["claims", "sub"];

fn schema() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("admins")
                    .column("user_id", ColumnType::Text)
                    .policies(TablePolicies::new().with_select(PolicyExpr::True)),
            )
            .table(
                TableSchemaBuilder::new("chats")
                    .column("name", ColumnType::Text)
                    .column("is_public", ColumnType::Boolean)
                    .policies(
                        TablePolicies::new()
                            .with_select(PolicyExpr::True)
                            // Only the name may change: the new row keeps
                            // the old row's visibility.
                            .with_update(
                                Some(PolicyExpr::True),
                                exists(
                                    "chats",
                                    vec![outer_eq("id", "id"), outer_eq("is_public", "is_public")],
                                ),
                            ),
                    ),
            )
            .table(
                TableSchemaBuilder::new("notes")
                    .column("data", ColumnType::Text)
                    .policies(
                        TablePolicies::new()
                            .with_select(exists("admins", vec![session_eq("user_id", &SUB)]))
                            .with_update(Some(PolicyExpr::True), PolicyExpr::True),
                    ),
            )
            .table(
                TableSchemaBuilder::new("protected")
                    .column("data", ColumnType::Text)
                    .policies(
                        TablePolicies::new()
                            .with_select(PolicyExpr::True)
                            .with_update(
                                Some(exists("admins", vec![session_eq("user_id", &SUB)])),
                                PolicyExpr::True,
                            ),
                    ),
            )
            .table(
                TableSchemaBuilder::new("board")
                    .column("body", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .table(
                // Only reads are declared: every write is denied.
                TableSchemaBuilder::new("drafts")
                    .column("body", ColumnType::Text)
                    .policies(TablePolicies::new().with_select(PolicyExpr::True)),
            )
            .build(),
    )
}

fn open(node: u8) -> Db {
    let config = DbConfig::new(
        schema(),
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author: AuthorSubject::SYSTEM,
        },
    );
    if node == 0 {
        block_on(Db::open_history_complete(config))
    } else {
        block_on(Db::open(config))
    }
    .unwrap()
}

fn id(kind: u8, n: u8) -> RowUuid {
    RowUuid::from_bytes([kind, n, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1])
}

fn seed(db: &Db, table: &str, row: RowUuid, pairs: &[(&str, Value)]) {
    let cells = pairs
        .iter()
        .map(|(k, v)| ((*k).to_owned(), v.clone()))
        .collect::<BTreeMap<_, _>>();
    db.seed_settled_mergeable_for_bootstrap(table, row, AuthorSubject::SYSTEM, cells)
        .unwrap();
}

fn text(s: &str) -> Value {
    Value::String(s.to_owned())
}

/// A user whose `claims.sub` is `sub`, as `db` knows them.
fn user(db: &Db, n: u8, sub: &str) -> AuthorSubject {
    let author = AuthorSubject::for_test_bytes([0x60 + n; 16]);
    db.set_identity_claims(
        author,
        BTreeMap::from([(jazz::query::provider_claim_key("sub"), text(sub))]),
    );
    author
}

fn as_session(author: AuthorSubject) -> UpdateOptions {
    UpdateOptions {
        identity: WriteIdentity::Session(author),
        ..Default::default()
    }
}

fn cell(db: &Db, table: &str, column: &str) -> Value {
    let prepared = db.prepare_query(&Query::from(table)).unwrap();
    let opts = ReadOpts {
        tier: ReadTier::LocalOnly,
        ..ReadOpts::default()
    };
    let rows = block_on(db.all(&prepared, opts)).unwrap();
    rows[0]
        .cells()
        .find_map(|(name, value)| (name == column).then(|| value.cloned()).flatten())
        .unwrap()
}

#[test]
fn with_check_compares_the_new_row_with_the_old_one() {
    let db = open(0);
    seed(
        &db,
        "chats",
        id(1, 1),
        &[("name", text("general")), ("is_public", Value::Bool(false))],
    );
    let alice = user(&db, 1, "alice");
    block_on(db.update(
        "chats",
        id(1, 1),
        BTreeMap::from([("name".to_owned(), text("project"))]),
        as_session(alice),
    ))
    .expect("the name may change");
    let error = block_on(db.update(
        "chats",
        id(1, 1),
        BTreeMap::from([("is_public".to_owned(), Value::Bool(true))]),
        as_session(alice),
    ))
    .err()
    .expect("visibility may not change");
    assert!(error.message.contains("WITH CHECK"), "{}", error.message);
}

/// A policy set is closed: an operation without a declared policy denies
/// every session write of that kind, while the database itself may write.
#[test]
fn an_operation_without_a_policy_denies_session_writes() {
    let db = open(0);
    seed(&db, "drafts", id(4, 1), &[("body", text("first"))]);
    let alice = user(&db, 1, "alice");
    let session = WriteIdentity::Session(alice);
    let insert = block_on(db.insert(
        "drafts",
        BTreeMap::from([("body".to_owned(), text("second"))]),
        InsertOptions {
            identity: session,
            ..Default::default()
        },
    ))
    .err()
    .expect("no insert policy");
    assert!(insert.is_authorization_denied(), "{}", insert.message);
    let update = block_on(db.update(
        "drafts",
        id(4, 1),
        BTreeMap::from([("body".to_owned(), text("edited"))]),
        as_session(alice),
    ))
    .err()
    .expect("no update policy");
    assert!(update.is_authorization_denied(), "{}", update.message);
    let delete = block_on(db.delete(
        "drafts",
        id(4, 1),
        DeleteOptions {
            identity: session,
            ..Default::default()
        },
    ))
    .err()
    .expect("no delete policy");
    assert!(delete.is_authorization_denied(), "{}", delete.message);
    assert_eq!(cell(&db, "drafts", "body"), text("first"));

    block_on(db.update(
        "drafts",
        id(4, 1),
        BTreeMap::from([("body".to_owned(), text("edited"))]),
        UpdateOptions::default(),
    ))
    .expect("the database writes without a policy");
    assert_eq!(cell(&db, "drafts", "body"), text("edited"));
}

/// SPEC 7, INV-RLS-15: an anonymous identity is read-only. Its inserts,
/// updates and deletes are denied before policies run, even where every
/// policy allows them.
#[test]
fn an_anonymous_session_cannot_write_where_policies_allow_it() {
    let db = open(0);
    seed(&db, "board", id(5, 1), &[("body", text("first"))]);
    let anonymous = AuthorSubject::reserved(AuthorSubject::ANONYMOUS_ISSUER, "visitor").unwrap();
    let session = WriteIdentity::Session(anonymous);
    let insert = block_on(db.insert(
        "board",
        BTreeMap::from([("body".to_owned(), text("second"))]),
        InsertOptions {
            identity: session,
            ..Default::default()
        },
    ))
    .err()
    .expect("anonymous insert");
    assert!(insert.is_authorization_denied(), "{}", insert.message);
    let update = block_on(db.update(
        "board",
        id(5, 1),
        BTreeMap::from([("body".to_owned(), text("edited"))]),
        as_session(anonymous),
    ))
    .err()
    .expect("anonymous update");
    assert!(update.is_authorization_denied(), "{}", update.message);
    let delete = block_on(db.delete(
        "board",
        id(5, 1),
        DeleteOptions {
            identity: session,
            ..Default::default()
        },
    ))
    .err()
    .expect("anonymous delete");
    assert!(delete.is_authorization_denied(), "{}", delete.message);
    assert_eq!(cell(&db, "board", "body"), text("first"));

    let alice = user(&db, 1, "alice");
    block_on(db.update(
        "board",
        id(5, 1),
        BTreeMap::from([("body".to_owned(), text("edited"))]),
        as_session(alice),
    ))
    .expect("the policy allows a signed-in session");
    assert_eq!(cell(&db, "board", "body"), text("edited"));
}

/// SPEC 3, INV-TX-7: a session's explicit write timestamp only stamps
/// provenance, and the authority refuses one more than the skew tolerance
/// ahead of its own clock rather than stamp history in the future.
#[test]
fn a_session_write_timestamped_too_far_ahead_is_rejected() {
    let db = open(0);
    seed(&db, "board", id(5, 1), &[("body", text("first"))]);
    let alice = user(&db, 1, "alice");
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    let at = |ms: u64| UpdateOptions {
        updated_at_ms: Some(ms),
        ..as_session(alice)
    };
    let ahead = now + jazz::db::SKEW_TOLERANCE_MS as u64 + 60_000;
    let refused = block_on(db.update(
        "board",
        id(5, 1),
        BTreeMap::from([("body".to_owned(), text("future"))]),
        at(ahead),
    ))
    .err()
    .expect("too far ahead");
    assert!(
        refused
            .message
            .starts_with(jazz::db::CLIENT_CLOCK_TOO_FAR_AHEAD),
        "{}",
        refused.message
    );
    assert_eq!(cell(&db, "board", "body"), text("first"));

    block_on(db.update(
        "board",
        id(5, 1),
        BTreeMap::from([("body".to_owned(), text("backfilled"))]),
        at(now - 3_600_000),
    ))
    .expect("an earlier timestamp is a backfill");
    block_on(db.update(
        "board",
        id(5, 1),
        BTreeMap::from([("body".to_owned(), text("slightly ahead"))]),
        at(now + 5_000),
    ))
    .expect("within the tolerance");
    assert_eq!(cell(&db, "board", "body"), text("slightly ahead"));
}

#[test]
fn a_trusted_links_session_write_is_checked_as_that_session_at_the_authority() {
    let server = open(0);
    seed(&server, "admins", id(2, 1), &[("user_id", text("alice"))]);
    seed(&server, "protected", id(3, 1), &[("data", text("initial"))]);
    let backend = open(1);
    let (client_side, server_side) = duplex();
    let _upstream = block_on(backend.connect_upstream(client_side));
    server.accept_subscriber(server_side, AuthorSubject::SYSTEM);
    let pump = || {
        for _ in 0..4 {
            block_on(backend.tick()).unwrap();
            block_on(server.tick()).unwrap();
        }
        block_on(backend.tick()).unwrap();
    };
    let prepared = backend.prepare_query(&Query::from("protected")).unwrap();
    let _stream = block_on(backend.subscribe(&prepared, ReadOpts::default())).unwrap();
    pump();

    // Bob is no admin: the replica stages his write, the authority refuses it.
    let bob = user(&backend, 2, "bob");
    block_on(backend.update(
        "protected",
        id(3, 1),
        BTreeMap::from([("data".to_owned(), text("bob"))]),
        as_session(bob),
    ))
    .expect("the replica stages the session write");
    pump();
    assert_eq!(cell(&server, "protected", "data"), text("initial"));
    assert_eq!(cell(&backend, "protected", "data"), text("initial"));

    let alice = user(&backend, 1, "alice");
    block_on(backend.update(
        "protected",
        id(3, 1),
        BTreeMap::from([("data".to_owned(), text("alice"))]),
        as_session(alice),
    ))
    .unwrap();
    pump();
    assert_eq!(cell(&server, "protected", "data"), text("alice"));
}

/// Read-for-write under a read policy that reads other rows: the replica
/// may lack the rows that grant access, so it stages an upsert and the
/// authority refuses a session that may not read the row. An update merges
/// into the session's view of the row, so the replica refuses it at once.
#[test]
fn a_replica_leaves_a_relational_read_for_write_upsert_to_the_authority() {
    let server = open(0);
    seed(&server, "admins", id(2, 1), &[("user_id", text("alice"))]);
    seed(&server, "notes", id(4, 1), &[("data", text("initial"))]);
    let backend = open(1);
    let (client_side, server_side) = duplex();
    let _upstream = block_on(backend.connect_upstream(client_side));
    server.accept_subscriber(server_side, AuthorSubject::SYSTEM);
    let pump = || {
        for _ in 0..4 {
            block_on(backend.tick()).unwrap();
            block_on(server.tick()).unwrap();
        }
        block_on(backend.tick()).unwrap();
    };
    let prepared = backend.prepare_query(&Query::from("notes")).unwrap();
    let _stream = block_on(backend.subscribe(&prepared, ReadOpts::default())).unwrap();
    pump();
    assert_eq!(cell(&backend, "notes", "data"), text("initial"));

    let bob = user(&backend, 2, "bob");
    user(&server, 2, "bob");
    let error = block_on(backend.update(
        "notes",
        id(4, 1),
        BTreeMap::from([("data".to_owned(), text("bob"))]),
        as_session(bob),
    ))
    .err()
    .expect("an update of a row hidden from the session is refused at once");
    assert!(
        error.message.contains("read policy denied UPDATE"),
        "{}",
        error.message
    );
    block_on(backend.upsert(
        "notes",
        id(4, 1),
        BTreeMap::from([("data".to_owned(), text("bob"))]),
        UpsertOptions {
            identity: WriteIdentity::Session(bob),
            ..Default::default()
        },
    ))
    .expect("the replica stages the session upsert");
    pump();
    assert_eq!(cell(&server, "notes", "data"), text("initial"));
    assert_eq!(cell(&backend, "notes", "data"), text("initial"));
}

/// A session link cannot vouch for the sessions its runtime writes as: the
/// authority checks those writes as the link's own identity, whatever
/// session they name. Bob's link fails as Bob even writing as Alice, an
/// admin; Alice's link passes as Alice even writing as Bob.
#[test]
fn a_session_links_session_write_is_checked_as_the_link() {
    let server = open(0);
    seed(&server, "admins", id(2, 1), &[("user_id", text("alice"))]);
    seed(&server, "protected", id(3, 1), &[("data", text("initial"))]);
    let link_as = |n: u8, sub: &str| {
        let author = user(&server, n, sub);
        let client = open(n);
        let (client_side, server_side) = duplex();
        let upstream = block_on(client.connect_upstream(client_side));
        server.accept_subscriber_with_claims(
            server_side,
            author,
            Some(BTreeMap::from([(
                jazz::query::provider_claim_key("sub"),
                text(sub),
            )])),
        );
        let prepared = client.prepare_query(&Query::from("protected")).unwrap();
        let stream = block_on(client.subscribe(&prepared, ReadOpts::default())).unwrap();
        (client, (upstream, stream))
    };
    let pump = |client: &Db| {
        for _ in 0..4 {
            block_on(client.tick()).unwrap();
            block_on(server.tick()).unwrap();
        }
        block_on(client.tick()).unwrap();
    };

    let (bob_client, _bob_link) = link_as(2, "bob");
    pump(&bob_client);
    let alice_on_bobs = user(&bob_client, 1, "alice");
    block_on(bob_client.update(
        "protected",
        id(3, 1),
        BTreeMap::from([("data".to_owned(), text("bob as alice"))]),
        as_session(alice_on_bobs),
    ))
    .expect("the replica stages the session write");
    pump(&bob_client);
    assert_eq!(cell(&server, "protected", "data"), text("initial"));

    let (alice_client, _alice_link) = link_as(1, "alice");
    pump(&alice_client);
    let bob_on_alices = user(&alice_client, 2, "bob");
    block_on(alice_client.update(
        "protected",
        id(3, 1),
        BTreeMap::from([("data".to_owned(), text("alice as bob"))]),
        as_session(bob_on_alices),
    ))
    .unwrap();
    pump(&alice_client);
    assert_eq!(cell(&server, "protected", "data"), text("alice as bob"));
}
