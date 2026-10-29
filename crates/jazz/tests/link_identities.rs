//! A relay serves one identity: a store configured as a user uploads what
//! its sessions write under its own identity, so it refuses a session of
//! another identity rather than attribute that user's writes to itself.
//! A system store (a server) serves any identity; a server's links check
//! the replica's identity when they authenticate it.

use crate::common;

#[path = "common/duplex_transport.rs"]
mod duplex_transport;

use std::collections::BTreeMap;

use duplex_transport::duplex;

use jazz::db::{
    Db, DbConfig, DbIdentity, ErrorCode, InsertOptions, Reason, RemoteLinkHint, WriteIdentity,
    block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid};
use jazz::query::Query;
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};

use common::{allow_all_policies, compile_schema};

fn schema() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("body", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .build(),
    )
}

fn open(node: u8, author: AuthorSubject) -> Db {
    block_on(Db::open(DbConfig::new(
        schema(),
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author,
        },
    )))
    .unwrap()
}

fn user(n: u8) -> AuthorSubject {
    AuthorSubject::for_test_bytes([n; 16])
}

fn body(text: &str) -> BTreeMap<String, Value> {
    BTreeMap::from([("body".to_owned(), Value::String(text.to_owned()))])
}

#[test]
fn a_relay_refuses_a_session_of_another_identity() {
    let relay = open(5, user(5));
    let (_, server_side) = duplex();
    let error = relay.accept_subscriber(server_side, user(6)).unwrap_err();
    assert_eq!(error.code, ErrorCode::Protocol);
    assert_eq!(error.reason, Some(Reason::IdentityMismatch));
    assert_eq!(relay.open_subscribers(), 0);
    // Nor does it serve the system: a user's store vouches for no one.
    let (_, server_side) = duplex();
    let error = relay
        .accept_subscriber(server_side, AuthorSubject::SYSTEM)
        .unwrap_err();
    assert_eq!(error.reason, Some(Reason::IdentityMismatch));
    assert!(relay.check_serves(user(5)).is_ok());
}

#[test]
fn a_system_store_serves_any_identity() {
    let core = open(0, AuthorSubject::SYSTEM);
    for identity in [user(5), user(6), AuthorSubject::SYSTEM] {
        let (_, server_side) = duplex();
        core.accept_subscriber(server_side, identity).unwrap();
    }
    assert_eq!(core.open_subscribers(), 3);
}

#[test]
fn a_relays_own_sessions_write_as_its_identity_at_the_core() {
    // The shipped topology: a worker and its tabs, all one user.
    let core = open(0, AuthorSubject::SYSTEM);
    let relay = open(5, user(5));
    let tab = open(6, user(5));
    tab.set_non_durable_client();
    tab.set_remote_link_hint(RemoteLinkHint::Live);
    let (up, down) = duplex();
    let _relay_up = block_on(relay.connect_upstream(up));
    core.accept_subscriber(down, user(5)).unwrap();
    relay.set_remote_link_hint(RemoteLinkHint::Live);
    let (up, down) = duplex();
    let _tab_up = block_on(tab.connect_upstream(up));
    relay.accept_subscriber(down, user(5)).unwrap();

    let written = block_on(tab.insert("notes", body("hello"), Default::default())).unwrap();
    for _ in 0..6 {
        block_on(tab.tick()).unwrap();
        block_on(relay.tick()).unwrap();
        block_on(core.tick()).unwrap();
        block_on(relay.tick()).unwrap();
        block_on(tab.tick()).unwrap();
    }
    let prepared = core
        .prepare_query(&Query::from("notes").select(["$createdBy.identity.subject"]))
        .unwrap();
    let rows = block_on(core.all(&prepared, Default::default())).unwrap();
    assert_eq!(rows.len(), 1, "the tab's write reached the core");
    let created_by = format!("{:?}", rows[0].cells().collect::<Vec<_>>());
    assert!(
        created_by.contains(&user(5).principal_parts().1),
        "{created_by} for {:?}",
        written.row_uuid()
    );
}

#[test]
fn a_users_store_refuses_a_write_as_another_session() {
    // It would upload as the store's own identity, so the core would
    // attribute the write to the store's user.
    let relay = open(5, user(5));
    relay.become_replica();
    let as_session = |author| InsertOptions {
        identity: WriteIdentity::Session(author),
        ..Default::default()
    };
    let error =
        block_on(relay.insert("notes", body("as user 6"), as_session(user(6)))).unwrap_err();
    assert_eq!(error.reason, Some(Reason::IdentityMismatch));
    block_on(relay.insert("notes", body("as user 5"), as_session(user(5)))).unwrap();
    // A system store writes as whichever session it vouches for, and so
    // does a user's store that is no replica (nothing uploads its writes).
    let standalone = open(4, user(4));
    block_on(standalone.insert("notes", body("as user 6"), as_session(user(6)))).unwrap();
    let core = open(0, AuthorSubject::SYSTEM);
    block_on(core.insert("notes", body("as user 6"), as_session(user(6)))).unwrap();
}
