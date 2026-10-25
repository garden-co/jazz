//! Session write checks on SQLite: WITH CHECK compares the new row with
//! the other rows as they were before the write, and a trusted link's
//! session write is staged on the replica and checked as that session, with
//! the claims the link vouches for, at the authority. Not ported from Jazz.

mod common;

#[path = "common/duplex_transport.rs"]
mod duplex_transport;

use std::collections::BTreeMap;

use duplex_transport::duplex;
use jazz::db::WriteIdentity;
use jazz::db::{Db, DbConfig, DbIdentity, ReadOpts, ReadTier, UpdateOptions, block_on};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::Query;
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, PolicyExpr, SchemaBuilder, TablePolicies, TableSchemaBuilder};

use common::{compile_schema, exists, outer_eq, session_eq};

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

/// Alice's ordinary replica uploads its own optimistic write. A backend socket
/// cannot turn that write or client-local claims into SYSTEM authority. A real
/// Alice session still uses the claims admitted by the server, after reconnect.
#[test]
fn ordinary_replica_writes_keep_session_authorization_over_backend_links() {
    use jazz::db::{FileOptions, InsertOptions, RemoteLinkHint};
    use jazz::tx::{DurabilityTier, Fate, RejectionReason};
    let alice = AuthorSubject::authenticated("https://issuer.example", "alice").unwrap();
    let policy = PolicyExpr::eq_session("owner", vec!["claims".into(), "sub".into()]);
    let schema = compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("owner", ColumnType::Text)
                    .policies(
                        TablePolicies::new()
                            .with_select(PolicyExpr::True)
                            .with_insert(policy),
                    ),
            )
            .build(),
    );
    for authenticated_session in [false, true] {
        let server = block_on(Db::open_history_complete(DbConfig::new(
            schema.clone(),
            DbIdentity {
                node: NodeUuid::from_bytes([0xee; 16]),
                author: AuthorSubject::SYSTEM,
            },
        )))
        .unwrap();
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("alice.sqlite");
        let config = || {
            DbConfig::new(
                schema.clone(),
                DbIdentity {
                    node: NodeUuid::from_bytes([0xa1; 16]),
                    author: alice,
                },
            )
        };
        let client = block_on(Db::open_at(config(), &path, FileOptions::default())).unwrap();
        client.set_remote_link_hint(RemoteLinkHint::NoServer);
        let claims = BTreeMap::from([(jazz::query::provider_claim_key("sub"), text("alice"))]);
        client.set_identity_claims(alice, claims.clone());
        let write = block_on(client.insert(
            "notes",
            BTreeMap::from([("owner".into(), text("alice"))]),
            InsertOptions::default(),
        ))
        .unwrap();
        let tx = write.mergeable_tx_id();
        block_on(client.wait_for_transaction(tx, DurabilityTier::Local)).unwrap();
        drop(client);
        let client = block_on(Db::open_at(config(), &path, FileOptions::default())).unwrap();
        let (up, down) = duplex();
        block_on(client.connect_upstream(up));
        server.accept_subscriber_with_claims(
            down,
            if authenticated_session {
                alice
            } else {
                AuthorSubject::SYSTEM
            },
            Some(if authenticated_session {
                claims
            } else {
                BTreeMap::new()
            }),
        );
        for _ in 0..4 {
            block_on(client.tick()).unwrap();
            block_on(server.tick()).unwrap();
        }
        block_on(client.tick()).unwrap();
        let state = client.write_state(tx).unwrap();
        assert_eq!(
            state.fate,
            if authenticated_session {
                Fate::Accepted
            } else {
                Fate::Rejected(RejectionReason::AuthorizationDenied)
            }
        );
        assert_eq!(
            block_on(client.wait_for_transaction(tx, DurabilityTier::Global)).is_ok(),
            authenticated_session
        );
        let query = server.prepare_query(&Query::from("notes")).unwrap();
        assert_eq!(
            block_on(server.all(&query, ReadOpts::default()))
                .unwrap()
                .len(),
            usize::from(authenticated_session)
        );
    }
}
