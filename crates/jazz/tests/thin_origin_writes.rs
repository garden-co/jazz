//! A thin client's write runs on a server as the client's own commit (its
//! node and that node's counter, SPEC 8 §8.14): a resend after a lost
//! answer, on the same server or another server of the same authority, is
//! not written a second time.

use crate::common;

#[path = "common/duplex_transport.rs"]
mod duplex_transport;

use std::collections::BTreeMap;

use duplex_transport::duplex;

use jazz::db::{
    Db, DbConfig, DbIdentity, InsertOptions, OriginWrite, ReadOpts, ReadTier, RemoteLinkHint,
    UpdateOptions, WriteIdentity, block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::Query;
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::time::TxTime;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
use jazz::tx::TxId;

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

fn open(node: u8) -> Db {
    block_on(Db::open(DbConfig::new(
        schema(),
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )))
    .unwrap()
}

fn user(n: u8) -> AuthorSubject {
    AuthorSubject::for_test_bytes([n; 16])
}

/// The thin client's id for its `n`th write.
fn origin(n: u64) -> TxId {
    TxId::new(TxTime(n), NodeUuid::from_bytes([0x7c; 16]))
}

fn body(text: &str) -> BTreeMap<String, Value> {
    BTreeMap::from([("body".to_owned(), Value::String(text.to_owned()))])
}

fn bodies(db: &Db) -> Vec<String> {
    let prepared = db.prepare_query(&Query::from("notes")).unwrap();
    let opts = ReadOpts {
        tier: ReadTier::LocalOnly,
        ..ReadOpts::default()
    };
    block_on(db.all(&prepared, opts))
        .unwrap()
        .iter()
        .flat_map(|row| {
            row.cells()
                .filter(|(name, _)| *name == "body")
                .filter_map(|(_, value)| match value {
                    Some(Value::String(text)) => Some(text.clone()),
                    _ => None,
                })
                .collect::<Vec<_>>()
        })
        .collect()
}

fn insert_as(db: &Db, n: u64, row: RowUuid, text: &str) -> OriginWrite {
    block_on(db.write_as_origin(origin(n), &user(1), async || {
        db.insert(
            "notes",
            body(text),
            InsertOptions {
                row_id: Some(row),
                identity: WriteIdentity::Session(user(1)),
                ..Default::default()
            },
        )
        .await
    }))
    .unwrap()
}

fn update_as(db: &Db, n: u64, row: RowUuid, text: &str) -> Result<OriginWrite, jazz::db::Error> {
    block_on(db.write_as_origin(origin(n), &user(1), async || {
        db.update(
            "notes",
            row,
            body(text),
            UpdateOptions {
                identity: WriteIdentity::Session(user(1)),
                ..Default::default()
            },
        )
        .await
    }))
}

#[test]
fn the_authority_answers_a_resent_write_with_the_first_one() {
    let core = open(0);
    let row = RowUuid::from_bytes([1; 16]);
    assert!(matches!(
        insert_as(&core, 1, row, "draft"),
        OriginWrite::Written(_)
    ));
    assert!(matches!(
        update_as(&core, 2, row, "mine").unwrap(),
        OriginWrite::Written(_)
    ));
    // Someone else writes after it; the resend of the update must not
    // undo theirs.
    block_on(core.update("notes", row, body("theirs"), Default::default())).unwrap();
    assert!(matches!(
        update_as(&core, 2, row, "mine").unwrap(),
        OriginWrite::Repeat
    ));
    assert!(matches!(
        insert_as(&core, 1, row, "draft"),
        OriginWrite::Repeat
    ));
    assert_eq!(bodies(&core), vec!["theirs".to_owned()]);
    // The client's next write is a new one.
    assert!(matches!(
        update_as(&core, 3, row, "later").unwrap(),
        OriginWrite::Written(_)
    ));
    assert_eq!(bodies(&core), vec!["later".to_owned()]);
}

#[test]
fn another_users_write_under_the_same_key_is_a_write_of_its_own() {
    // The commit's writer is derived from the author and the client's
    // node: another user naming the same key neither replays nor blocks the
    // first user's write.
    let core = open(0);
    let row = RowUuid::from_bytes([1; 16]);
    insert_as(&core, 1, row, "draft");
    let theirs = block_on(core.write_as_origin(origin(2), &user(2), async || {
        core.update(
            "notes",
            row,
            body("theirs"),
            UpdateOptions {
                identity: WriteIdentity::Session(user(2)),
                ..Default::default()
            },
        )
        .await
    }))
    .unwrap();
    assert!(matches!(theirs, OriginWrite::Written(_)));
    assert!(matches!(
        update_as(&core, 2, row, "mine").unwrap(),
        OriginWrite::Written(_)
    ));
    assert_eq!(bodies(&core), vec!["mine".to_owned()]);
}

#[test]
fn a_failed_write_leaves_its_key_to_no_other_commit() {
    let core = open(0);
    let replica = open(1);
    let (up, down) = duplex();
    let _link = block_on(replica.connect_upstream(up));
    jazz::db::block_on(core.accept_subscriber(down, AuthorSubject::SYSTEM)).unwrap();
    jazz::db::block_on(replica.set_remote_link_hint(RemoteLinkHint::Live));
    let settle = || {
        for _ in 0..8 {
            for db in [&replica, &core] {
                block_on(db.tick()).unwrap();
            }
        }
    };
    let missing = RowUuid::from_bytes([9; 16]);
    let prepared = replica.prepare_query(&Query::from("notes")).unwrap();
    let _live = block_on(replica.subscribe(&prepared, ReadOpts::default())).unwrap();
    for (key, db) in [(1, &core), (2, &replica)] {
        // The keyed write fails before it commits; the store's next commit
        // takes its seq, and must not take its key with it.
        assert!(update_as(db, key, missing, "nothing").is_err());
        let row = RowUuid::from_bytes([key as u8 + 2; 16]);
        block_on(db.insert(
            "notes",
            body("plain"),
            InsertOptions {
                row_id: Some(row),
                ..Default::default()
            },
        ))
        .unwrap();
        settle();
        // So the key is still unwritten, here and at the authority.
        assert!(matches!(
            update_as(db, key, row, "keyed").unwrap(),
            OriginWrite::Written(_)
        ));
        settle();
    }
    let mut seen = bodies(&core);
    seen.sort();
    assert_eq!(seen, vec!["keyed".to_owned(), "keyed".to_owned()]);
}

#[test]
fn a_resend_through_another_server_gets_the_first_fate_from_the_authority() {
    let core = open(0);
    let first = open(1);
    let second = open(2);
    let mut links = Vec::new();
    for server in [&first, &second] {
        let (up, down) = duplex();
        links.push(block_on(server.connect_upstream(up)));
        jazz::db::block_on(core.accept_subscriber(down, AuthorSubject::SYSTEM)).unwrap();
        jazz::db::block_on(server.set_remote_link_hint(RemoteLinkHint::Live));
    }
    let settle = || {
        for _ in 0..8 {
            for db in [&first, &second, &core] {
                block_on(db.tick()).unwrap();
            }
        }
    };
    let row = RowUuid::from_bytes([1; 16]);
    block_on(core.insert(
        "notes",
        body("draft"),
        InsertOptions {
            row_id: Some(row),
            ..Default::default()
        },
    ))
    .unwrap();
    // Both servers hold the row live, as a server serving it does.
    let prepared = core.prepare_query(&Query::from("notes")).unwrap();
    let _live: Vec<_> = [&first, &second]
        .iter()
        .map(|server| {
            let prepared = server.prepare_query(&Query::from("notes")).unwrap();
            block_on(server.subscribe(&prepared, ReadOpts::default())).unwrap()
        })
        .collect();
    let _ = prepared;
    settle();
    // The client's update lands through the first server, whose answer is
    // lost; someone else writes after it.
    assert!(matches!(
        update_as(&first, 1, row, "mine").unwrap(),
        OriginWrite::Written(_)
    ));
    settle();
    block_on(core.update("notes", row, body("theirs"), Default::default())).unwrap();
    settle();
    // The client resends through the second server, which never saw it.
    update_as(&second, 1, row, "mine").unwrap();
    settle();
    assert_eq!(bodies(&core), vec!["theirs".to_owned()]);
    assert_eq!(bodies(&second), vec!["theirs".to_owned()]);
    assert_eq!(bodies(&first), vec!["theirs".to_owned()]);
    // The first server knows it settled, so a resend there writes nothing.
    assert!(matches!(
        update_as(&first, 1, row, "mine").unwrap(),
        OriginWrite::Repeat
    ));
}

#[test]
fn two_users_writing_under_one_key_through_two_servers_both_land() {
    let core = open(0);
    let first = open(1);
    let second = open(2);
    let mut links = Vec::new();
    for server in [&first, &second] {
        let (up, down) = duplex();
        links.push(block_on(server.connect_upstream(up)));
        jazz::db::block_on(core.accept_subscriber(down, AuthorSubject::SYSTEM)).unwrap();
        jazz::db::block_on(server.set_remote_link_hint(RemoteLinkHint::Live));
    }
    let settle = || {
        for _ in 0..8 {
            for db in [&first, &second, &core] {
                block_on(db.tick()).unwrap();
            }
        }
    };
    // The second user writes first, under the key the first user's write
    // will name, through another server: still both are written.
    let theirs = block_on(second.write_as_origin(origin(1), &user(2), async || {
        second
            .insert(
                "notes",
                body("theirs"),
                InsertOptions {
                    row_id: Some(RowUuid::from_bytes([2; 16])),
                    identity: WriteIdentity::Session(user(2)),
                    ..Default::default()
                },
            )
            .await
    }))
    .unwrap();
    assert!(matches!(theirs, OriginWrite::Written(_)));
    settle();
    assert!(matches!(
        insert_as(&first, 1, RowUuid::from_bytes([1; 16]), "mine"),
        OriginWrite::Written(_)
    ));
    settle();
    let mut seen = bodies(&core);
    seen.sort();
    assert_eq!(seen, vec!["mine".to_owned(), "theirs".to_owned()]);
}

/// A session's own node that reads as version 8 (alpha.59 minted nodes as
/// raw random bytes) still commits; a session naming a thin client's
/// writer node does not, even its own author's.
#[test]
fn only_a_thin_clients_writer_node_is_refused_to_sessions() {
    let legacy = [
        0x3a, 0x51, 0x0c, 0x9e, 0x44, 0x02, 0x8f, 0x13, 0xa7, 0x60, 0x11, 0xd2, 0x5e, 0x09, 0xbb,
        0x4c,
    ];
    let derived = jazz::db::origin_node(&user(1), [7; 16]);
    let core = open(0);
    let mut links = Vec::new();
    let clients: Vec<Db> = [legacy, derived]
        .into_iter()
        .map(|node| {
            let client = block_on(Db::open(DbConfig::new(
                schema(),
                DbIdentity {
                    node: NodeUuid::from_bytes(node),
                    author: user(1),
                },
            )))
            .unwrap();
            let (up, down) = duplex();
            links.push(block_on(client.connect_upstream(up)));
            jazz::db::block_on(core.accept_subscriber(down, user(1))).unwrap();
            jazz::db::block_on(client.set_remote_link_hint(RemoteLinkHint::Live));
            client
        })
        .collect();
    for (n, client) in clients.iter().enumerate() {
        block_on(client.insert(
            "notes",
            body(if n == 0 { "legacy" } else { "derived" }),
            InsertOptions {
                row_id: Some(RowUuid::from_bytes([n as u8 + 1; 16])),
                ..Default::default()
            },
        ))
        .unwrap();
    }
    for _ in 0..8 {
        for db in clients.iter().chain([&core]) {
            block_on(db.tick()).unwrap();
        }
    }
    assert_eq!(bodies(&core), vec!["legacy".to_owned()]);
}
