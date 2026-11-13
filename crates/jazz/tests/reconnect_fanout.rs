//! Fan-out of writes a client replays after a reconnect: a commit made
//! offline uploads on the new session, and every other session whose
//! subscription covers it receives it live, the same as a write made
//! while connected. The writer's old server session is left open, as when
//! a client's network drops without a goodbye. Not ported from Jazz.

use crate::common;

use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};
use std::rc::Rc;

use jazz::db::{
    Db, DbConfig, DbIdentity, InsertOptions, ReadOpts, ReadTier, SyncMessage, Transport,
    TransportError, block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{OrderDirection, Query, col, eq, lit};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, OpenTransactionId, SchemaBuilder, TableSchemaBuilder};

use common::{
    allow_all_policies, compile_schema, exists, outer_eq, read_and_allow_all_writes, session_eq,
};

const ME: [&str; 2] = ["identity", "subject"];

/// BandChat's shape: rooms, memberships naming a member, and messages
/// readable by a room's members.
fn schema() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("rooms")
                    .column("name", ColumnType::Text)
                    .nullable_column("activity", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new("memberships")
                    .fk_column("room", "rooms")
                    .column("member", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new("messages")
                    .fk_column("room", "rooms")
                    .column("body", ColumnType::Text)
                    .policies(read_and_allow_all_writes(exists(
                        "memberships",
                        vec![outer_eq("room", "room"), session_eq("member", &ME)],
                    ))),
            )
            .build(),
    )
}

fn olive() -> AuthorSubject {
    AuthorSubject::for_test_bytes([0x01; 16])
}

fn gus() -> AuthorSubject {
    AuthorSubject::for_test_bytes([0x02; 16])
}

fn open(node: u8, author: AuthorSubject) -> Db {
    let config = DbConfig::new(
        schema(),
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author,
        },
    );
    if node == 0 {
        block_on(Db::open_history_complete(config))
    } else {
        block_on(Db::open(config))
    }
    .unwrap()
}

struct Pipe {
    out: Rc<RefCell<VecDeque<SyncMessage>>>,
    inbound: Rc<RefCell<VecDeque<SyncMessage>>>,
}

impl Transport for Pipe {
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        self.out.borrow_mut().push_back(message);
        Ok(())
    }

    fn try_recv(&mut self) -> Option<SyncMessage> {
        self.inbound.borrow_mut().pop_front()
    }
}

/// (client side, server side)
fn pipe() -> (Box<dyn Transport>, Box<dyn Transport>) {
    let up = Rc::new(RefCell::new(VecDeque::new()));
    let down = Rc::new(RefCell::new(VecDeque::new()));
    (
        Box::new(Pipe {
            out: up.clone(),
            inbound: down.clone(),
        }),
        Box::new(Pipe {
            out: down,
            inbound: up,
        }),
    )
}

fn id(kind: u8, n: u32) -> RowUuid {
    let mut bytes = [0u8; 16];
    bytes[0] = kind;
    bytes[12..].copy_from_slice(&n.to_be_bytes());
    RowUuid::from_bytes(bytes)
}

fn cells(pairs: &[(&str, Value)]) -> BTreeMap<String, Value> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_owned(), v.clone()))
        .collect()
}

fn connect(server: &Db, client: &Db, who: AuthorSubject) -> jazz::db::Upstream {
    let (client_side, server_side) = pipe();
    let upstream = block_on(client.connect_upstream(client_side));
    jazz::db::block_on(server.accept_subscriber(server_side, who)).unwrap();
    upstream
}

fn tick_all(server: &Db, clients: &[&Db]) {
    for _ in 0..6 {
        for client in clients {
            block_on(client.tick()).unwrap();
        }
        block_on(server.tick()).unwrap();
    }
    for client in clients {
        block_on(client.tick()).unwrap();
    }
}

/// The message bodies `db` holds, sorted.
fn bodies(db: &Db) -> Vec<String> {
    let prepared = db.prepare_query(&Query::from("messages")).unwrap();
    let rows = block_on(db.all(
        &prepared,
        ReadOpts {
            tier: ReadTier::LocalOnly,
            ..ReadOpts::default()
        },
    ))
    .unwrap();
    let mut bodies: Vec<String> = rows
        .iter()
        .map(|row| {
            match row
                .cells()
                .find(|(name, _)| *name == "body")
                .and_then(|(_, v)| v.cloned())
            {
                Some(Value::String(body)) => body,
                other => panic!("body: {other:?}"),
            }
        })
        .collect();
    bodies.sort();
    bodies
}

/// A server holding room 0 with olive and gus as members.
fn band() -> Db {
    let server = open(0, AuthorSubject::SYSTEM);
    let seed = |table: &str, row: RowUuid, values: BTreeMap<String, Value>| {
        server
            .seed_settled_mergeable_for_bootstrap(table, row, AuthorSubject::SYSTEM, values)
            .unwrap();
    };
    seed(
        "rooms",
        id(1, 0),
        cells(&[("name", Value::String("rehearsal".into()))]),
    );
    for (n, who) in [olive(), gus()].into_iter().enumerate() {
        seed(
            "memberships",
            id(2, n as u32),
            cells(&[
                ("room", Value::Uuid(id(1, 0).0)),
                ("member", Value::String(who.principal_parts().1)),
            ]),
        );
    }
    server
}

fn send(db: &Db, n: u32, body: &str) {
    block_on(db.insert(
        "messages",
        cells(&[
            ("room", Value::Uuid(id(1, 0).0)),
            ("body", Value::String(body.into())),
        ]),
        InsertOptions {
            row_id: Some(id(3, n)),
            ..Default::default()
        },
    ))
    .unwrap();
}

/// Gus's message written while offline reaches Olive's open subscription
/// once he reconnects, as his live messages do.
///
/// ```
/// gus ──live msg──► server ──fan-out──► olive        (works)
/// gus ✗ offline (his old session stays open on the server): msg commits locally
/// gus ──new session: resume + upload──► server ──fan-out──► olive
/// ```
#[test]
fn an_offline_write_fans_out_to_open_subscriptions_after_reconnect() {
    let server = band();
    let olive_db = open(1, olive());
    let gus_db = open(2, gus());
    let _olive_up = connect(&server, &olive_db, olive());
    let gus_up = connect(&server, &gus_db, gus());
    let olive_q = olive_db.prepare_query(&Query::from("messages")).unwrap();
    let _olive_sub = block_on(olive_db.subscribe(&olive_q, ReadOpts::default())).unwrap();
    let gus_q = gus_db.prepare_query(&Query::from("messages")).unwrap();
    let _gus_sub = block_on(gus_db.subscribe(&gus_q, ReadOpts::default())).unwrap();
    tick_all(&server, &[&olive_db, &gus_db]);

    send(&gus_db, 0, "spare snare");
    tick_all(&server, &[&olive_db, &gus_db]);
    assert_eq!(bodies(&olive_db), ["spare snare"], "a live write fans out");

    assert!(gus_db.detach_connection(&gus_up));
    send(&gus_db, 1, "running late");
    tick_all(&server, &[&olive_db, &gus_db]);
    assert_eq!(
        bodies(&olive_db),
        ["spare snare"],
        "nothing while gus is away"
    );

    let _gus_up = connect(&server, &gus_db, gus());
    tick_all(&server, &[&olive_db, &gus_db]);
    assert_eq!(
        bodies(&server),
        ["running late", "spare snare"],
        "the server stores the replayed write"
    );
    assert_eq!(
        bodies(&olive_db),
        ["running late", "spare snare"],
        "olive's open subscription receives the replayed write"
    );
}

/// Gus's message and the room's new activity, committed together, as
/// BandChat's composer does.
fn send_with_activity(db: &Db, n: u32, body: &str) {
    let tx = OpenTransactionId::new();
    block_on(db.begin_mergeable(tx)).unwrap();
    block_on(db.tx_ref(tx).insert(
        "messages",
        cells(&[
            ("room", Value::Uuid(id(1, 0).0)),
            ("body", Value::String(body.into())),
        ]),
        InsertOptions {
            row_id: Some(id(3, n)),
            ..Default::default()
        },
    ))
    .unwrap();
    block_on(db.tx_ref(tx).update(
        "rooms",
        id(1, 0),
        cells(&[("activity", Value::String(body.into()))]),
        Default::default(),
    ))
    .unwrap();
    block_on(db.commit_mergeable_handle(tx)).unwrap();
}

/// A room's newest messages, as BandChat's room view reads them.
fn newest(db: &Db) -> jazz::db::PreparedQuery {
    db.prepare_query(
        &Query::from("messages")
            .filter(eq(col("room"), lit(Value::Uuid(id(1, 0).0))))
            .order_by("$createdAt", OrderDirection::Desc)
            .limit(200),
    )
    .unwrap()
}

/// The same, over BandChat's room view: a windowed subscription of the
/// room's newest messages, and a send that also records the room's
/// activity in the same transaction.
#[test]
fn an_offline_write_fans_out_to_an_open_windowed_room_after_reconnect() {
    let server = band();
    let olive_db = open(1, olive());
    let gus_db = open(2, gus());
    let _olive_up = connect(&server, &olive_db, olive());
    let gus_up = connect(&server, &gus_db, gus());
    let _olive_sub = block_on(olive_db.subscribe(&newest(&olive_db), ReadOpts::default())).unwrap();
    let _gus_sub = block_on(gus_db.subscribe(&newest(&gus_db), ReadOpts::default())).unwrap();
    let rooms = olive_db.prepare_query(&Query::from("rooms")).unwrap();
    let _olive_rooms = block_on(olive_db.subscribe(&rooms, ReadOpts::default())).unwrap();
    let rooms = gus_db.prepare_query(&Query::from("rooms")).unwrap();
    let _gus_rooms = block_on(gus_db.subscribe(&rooms, ReadOpts::default())).unwrap();
    tick_all(&server, &[&olive_db, &gus_db]);

    send_with_activity(&gus_db, 0, "spare snare");
    tick_all(&server, &[&olive_db, &gus_db]);
    assert_eq!(bodies(&olive_db), ["spare snare"], "a live write fans out");

    assert!(gus_db.detach_connection(&gus_up));
    send_with_activity(&gus_db, 1, "running late");
    tick_all(&server, &[&olive_db, &gus_db]);
    assert_eq!(
        bodies(&olive_db),
        ["spare snare"],
        "nothing while gus is away"
    );

    let _gus_up = connect(&server, &gus_db, gus());
    tick_all(&server, &[&olive_db, &gus_db]);
    assert_eq!(bodies(&server), ["running late", "spare snare"]);
    assert_eq!(
        bodies(&olive_db),
        ["running late", "spare snare"],
        "olive's open room receives the replayed write"
    );
}
