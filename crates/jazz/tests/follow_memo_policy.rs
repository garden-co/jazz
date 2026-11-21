//! The server tick's follow memo (`sync::FollowMemo`) under per-session
//! read policies: the memo shares the rows a followed key found before any
//! session's policy applies, and each session still filters them by its own
//! identity. Two sessions of different identities follow the same key in
//! one tick, where users are readable only by their owner: whichever session
//! is served first, neither receives the other's users, and each receives
//! its own.

use crate::common;

#[path = "common/duplex_transport.rs"]
mod duplex_transport;

use std::collections::BTreeMap;

use duplex_transport::duplex;
use jazz::db::{
    Db, DbConfig, DbIdentity, ReadOpts, ReadTier, block_on, follow_memo_stats_for_test,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::Query;
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};

use common::{allow_all_policies, compile_schema, read_and_allow_all_writes, session_eq};

/// Users readable only by the session whose subject is their `owner`;
/// messages readable by all.
fn schema() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("users")
                    .column("name", ColumnType::Text)
                    .nullable_column("owner", ColumnType::Text)
                    .policies(read_and_allow_all_writes(session_eq(
                        "owner",
                        &["identity", "subject"],
                    ))),
            )
            .table(
                TableSchemaBuilder::new("messages")
                    .fk_column("senderId", "users")
                    .column("body", ColumnType::Text)
                    .policies(allow_all_policies()),
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

fn cells(pairs: &[(&str, Value)]) -> BTreeMap<String, Value> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_owned(), v.clone()))
        .collect()
}

fn text(s: &str) -> Value {
    Value::String(s.to_owned())
}

fn pump(server: &Db, clients: &[Db]) {
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

/// The users `db` holds.
fn users(db: &Db) -> Vec<RowUuid> {
    let prepared = db.prepare_query(&Query::from("users")).unwrap();
    let opts = ReadOpts {
        tier: ReadTier::LocalOnly,
        ..ReadOpts::default()
    };
    let mut held: Vec<RowUuid> = block_on(db.all(&prepared, opts))
        .unwrap()
        .iter()
        .map(|row| row.row_uuid())
        .collect();
    held.sort();
    held
}

/// Alice owns users 1 and 3, Bob owns user 2. Both read the messages with
/// their senders; then message 1 moves to user 3, so in one tick both
/// sessions follow `users.id = 3`, and the session served second reuses
/// the first one's read.
fn two_identities_follow_one_key(alice_first: bool) {
    let server = open(0);
    let alice = AuthorSubject::for_test_bytes([0xa1; 16]);
    let bob = AuthorSubject::for_test_bytes([0xb0; 16]);
    let (alice_subject, bob_subject) = (alice.principal_parts().1, bob.principal_parts().1);
    for (n, owner) in [(1, &alice_subject), (2, &bob_subject), (3, &alice_subject)] {
        jazz::db::block_on(server.insert(
            "users",
            cells(&[("name", text(&format!("u{n}"))), ("owner", text(owner))]),
            jazz::db::InsertOptions {
                row_id: Some(id(1, n)),
                identity: jazz::db::WriteIdentity::Attribution(AuthorSubject::SYSTEM),
                ..Default::default()
            },
        ))
        .unwrap();
    }
    for (n, sender) in [(1, 1), (2, 2)] {
        jazz::db::block_on(server.insert(
            "messages",
            cells(&[
                ("senderId", Value::Uuid(id(1, sender).0)),
                ("body", text(&format!("m{n}"))),
            ]),
            jazz::db::InsertOptions {
                row_id: Some(id(2, n)),
                identity: jazz::db::WriteIdentity::Attribution(AuthorSubject::SYSTEM),
                ..Default::default()
            },
        ))
        .unwrap();
    }
    // Sessions are served in the order they were accepted.
    let order = if alice_first {
        [(1, alice), (2, bob)]
    } else {
        [(2, bob), (1, alice)]
    };
    let mut clients = Vec::new();
    let mut nodes = Vec::new();
    for (node, subject) in order {
        let client = open(node);
        let (client_side, server_side) = duplex();
        let _ = block_on(client.connect_upstream(client_side));
        server.accept_subscriber(server_side, subject).unwrap();
        clients.push(client);
        nodes.push(node);
    }
    let mut streams: Vec<Box<dyn std::any::Any>> = Vec::new();
    for client in &clients {
        let prepared = client
            .prepare_query(&Query::from("messages").include("senderId"))
            .unwrap();
        streams.push(Box::new(
            block_on(client.subscribe(&prepared, ReadOpts::default())).unwrap(),
        ));
    }
    pump(&server, &clients);
    let before = follow_memo_stats_for_test();
    block_on(server.update(
        "messages",
        id(2, 1),
        cells(&[("senderId", Value::Uuid(id(1, 3).0))]),
        Default::default(),
    ))
    .unwrap();
    pump(&server, &clients);
    let after = follow_memo_stats_for_test();
    assert!(
        after.hits > before.hits,
        "the second session reused the first one's read: {before:?} -> {after:?}"
    );
    for (node, client) in nodes.iter().zip(&clients) {
        let held = users(client);
        if *node == 1 {
            assert_eq!(held, [id(1, 1), id(1, 3)], "Alice holds her own users only");
        } else {
            assert_eq!(held, [id(1, 2)], "Bob holds his own user only");
        }
    }
}

/// M5a. The owner of the followed user is served first: the other session
/// reuses the read and must not receive the user.
#[test]
fn a_kept_follow_read_is_filtered_by_each_sessions_policy_owner_first() {
    two_identities_follow_one_key(true);
}

/// M5b. A session that may not read the followed user is served first: the
/// owner, served second, must still receive it.
#[test]
fn a_kept_follow_read_is_filtered_by_each_sessions_policy_other_first() {
    two_identities_follow_one_key(false);
}
