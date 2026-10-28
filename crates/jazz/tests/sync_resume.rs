//! Reconnect resume: a client that held a subscription settled through
//! seq W resubscribes "Q since W". The server ships only result rows
//! changed after W, and, when a table deciding membership was written
//! after W, rows that entered the result without changing (by comparing
//! it with the result as of W) and rows that left it. The opening lists
//! the result's row ids, so the client drops cached rows that left.

use crate::common;

use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};
use std::rc::Rc;

use jazz::db::{
    Db, DbConfig, DbIdentity, ReadOpts, ReadTier, SyncMessage, Transport, TransportError, block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::Query;
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};

use common::{
    allow_all_policies, compile_schema, exists, outer_eq, read_and_allow_all_writes, session_eq,
};

const ME: [&str; 3] = ["user", "identity", "subject"];

fn schema() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("rooms")
                    .column("name", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new("memberships")
                    .fk_column("room", "rooms")
                    .column("member", ColumnType::Text)
                    .policies(read_and_allow_all_writes(session_eq("member", &ME))),
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

fn alice() -> AuthorSubject {
    AuthorSubject::for_test_bytes([0x55; 16])
}

fn open(node: u8, author: AuthorSubject) -> Db {
    let schema = schema();
    let config = DbConfig::new(
        schema,
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

/// A duplex that counts the row images the server ships.
struct Pipe {
    out: Rc<RefCell<VecDeque<SyncMessage>>>,
    inbound: Rc<RefCell<VecDeque<SyncMessage>>>,
    shipped: Option<Rc<RefCell<usize>>>,
}

impl Transport for Pipe {
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        if let (Some(count), SyncMessage::Rows { images }) = (&self.shipped, &message) {
            *count.borrow_mut() += images.len();
        }
        self.out.borrow_mut().push_back(message);
        Ok(())
    }

    fn try_recv(&mut self) -> Option<SyncMessage> {
        self.inbound.borrow_mut().pop_front()
    }
}

fn pipe() -> (Box<dyn Transport>, Box<dyn Transport>, Rc<RefCell<usize>>) {
    let up = Rc::new(RefCell::new(VecDeque::new()));
    let down = Rc::new(RefCell::new(VecDeque::new()));
    let shipped = Rc::new(RefCell::new(0));
    (
        Box::new(Pipe {
            out: up.clone(),
            inbound: down.clone(),
            shipped: None,
        }),
        Box::new(Pipe {
            out: down,
            inbound: up,
            shipped: Some(shipped.clone()),
        }),
        shipped,
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

fn subject() -> String {
    alice().principal_parts().1
}

fn pump(server: &Db, client: &Db) {
    for _ in 0..5 {
        block_on(client.tick()).unwrap();
        block_on(server.tick()).unwrap();
    }
    block_on(client.tick()).unwrap();
}

fn ids(db: &Db, identity: Option<AuthorSubject>) -> Vec<RowUuid> {
    let prepared = db.prepare_query(&Query::from("messages")).unwrap();
    let rows = match identity {
        Some(identity) => {
            block_on(db.all_for_identity(&prepared, ReadOpts::default(), identity)).unwrap()
        }
        None => block_on(db.all(
            &prepared,
            ReadOpts {
                tier: ReadTier::LocalOnly,
                ..ReadOpts::default()
            },
        ))
        .unwrap(),
    };
    let mut ids: Vec<RowUuid> = rows.iter().map(|row| row.row_uuid()).collect();
    ids.sort();
    ids
}

#[test]
fn a_resumed_subscription_ships_only_what_changed_and_converges() {
    let server = open(0, AuthorSubject::SYSTEM);
    let seed = |table: &str, row: RowUuid, values: BTreeMap<String, Value>| {
        server
            .seed_settled_mergeable_for_bootstrap(table, row, AuthorSubject::SYSTEM, values)
            .unwrap();
    };
    for r in 0..2 {
        seed(
            "rooms",
            id(1, r),
            cells(&[("name", Value::String(format!("room {r}")))]),
        );
    }
    seed(
        "memberships",
        id(2, 0),
        cells(&[
            ("room", Value::Uuid(id(1, 0).0)),
            ("member", Value::String(subject())),
        ]),
    );
    for n in 0..2000 {
        let room = id(1, n % 2);
        seed(
            "messages",
            id(3, n),
            cells(&[
                ("room", Value::Uuid(room.0)),
                ("body", Value::String(format!("m{n}"))),
            ]),
        );
    }

    let client = open(1, alice());
    let (client_side, server_side, shipped) = pipe();
    let upstream = block_on(client.connect_upstream(client_side));
    server.accept_subscriber(server_side, alice());
    let prepared = client.prepare_query(&Query::from("messages")).unwrap();
    let _stream = block_on(client.subscribe(&prepared, ReadOpts::default())).unwrap();
    pump(&server, &client);
    // The readable room's messages, and alice's membership their read
    // policy rests on.
    assert_eq!(
        *shipped.borrow(),
        1001,
        "the first opening ships the readable room and its membership"
    );
    assert_eq!(ids(&client, None), ids(&server, Some(alice())));

    // Offline: three edits, one delete, one insert, and alice joins room 1,
    // whose 1,000 messages become readable without changing.
    assert!(client.detach_connection(&upstream));
    for n in [0u32, 2, 4] {
        block_on(server.update(
            "messages",
            id(3, n),
            cells(&[("body", Value::String("edited".into()))]),
            Default::default(),
        ))
        .unwrap();
    }
    block_on(server.delete("messages", id(3, 6), Default::default())).unwrap();
    seed(
        "messages",
        id(3, 5000),
        cells(&[
            ("room", Value::Uuid(id(1, 0).0)),
            ("body", Value::String("new".into())),
        ]),
    );

    let (client_side, server_side, shipped) = pipe();
    let _upstream = block_on(client.connect_upstream(client_side));
    server.accept_subscriber(server_side, alice());
    pump(&server, &client);
    // The edits and the insert, the deleted version (its table was
    // written, so the result as of the resume point says it left), and
    // the membership the shipped rows' read policy rests on (a changed row
    // may rest on rows the client never held, so its support ships whole).
    assert_eq!(
        *shipped.borrow(),
        6,
        "the 3 edits, the insert, the deletion and the membership are shipped"
    );
    assert_eq!(
        ids(&client, None),
        ids(&server, Some(alice())),
        "the delete is dropped"
    );

    // Gaining access while away: the unchanged rows are fetched by id.
    assert!(client.detach_connection(&_upstream));
    seed(
        "memberships",
        id(2, 1),
        cells(&[
            ("room", Value::Uuid(id(1, 1).0)),
            ("member", Value::String(subject())),
        ]),
    );
    let (client_side, server_side, shipped) = pipe();
    let _upstream = block_on(client.connect_upstream(client_side));
    server.accept_subscriber(server_side, alice());
    pump(&server, &client);
    assert_eq!(ids(&client, None).len(), 2000);
    assert_eq!(ids(&client, None), ids(&server, Some(alice())));
    assert_eq!(
        *shipped.borrow(),
        1001,
        "the newly readable room and the membership that opened it, once"
    );
}

/// Timing probe (run with `--release -- --nocapture`): a caught-up client
/// reconnecting to a 10,000-row subscription.
#[test]
fn caught_up_resume_cost() {
    let server = open(0, AuthorSubject::SYSTEM);
    let seed = |table: &str, row: RowUuid, values: BTreeMap<String, Value>| {
        server
            .seed_settled_mergeable_for_bootstrap(table, row, AuthorSubject::SYSTEM, values)
            .unwrap();
    };
    seed(
        "rooms",
        id(1, 0),
        cells(&[("name", Value::String("room".into()))]),
    );
    seed(
        "memberships",
        id(2, 0),
        cells(&[
            ("room", Value::Uuid(id(1, 0).0)),
            ("member", Value::String(subject())),
        ]),
    );
    for n in 0..10_000 {
        seed(
            "messages",
            id(3, n),
            cells(&[
                ("room", Value::Uuid(id(1, 0).0)),
                ("body", Value::String(format!("m{n}"))),
            ]),
        );
    }
    let client = open(1, alice());
    let (client_side, server_side, _) = pipe();
    let mut upstream = block_on(client.connect_upstream(client_side));
    server.accept_subscriber(server_side, alice());
    let prepared = client.prepare_query(&Query::from("messages")).unwrap();
    let _stream = block_on(client.subscribe(&prepared, ReadOpts::default())).unwrap();
    pump(&server, &client);
    let mut times = Vec::new();
    for _ in 0..20 {
        assert!(client.detach_connection(&upstream));
        let (client_side, server_side, shipped) = pipe();
        let started = std::time::Instant::now();
        upstream = block_on(client.connect_upstream(client_side));
        server.accept_subscriber(server_side, alice());
        pump(&server, &client);
        times.push(started.elapsed());
        assert_eq!(*shipped.borrow(), 0);
    }
    times.sort();
    eprintln!(
        "caught-up resume of a 10,000-row subscription: median {:?}",
        times[times.len() / 2]
    );
}

/// A row the replica cached from an ended read is cold: nothing keeps it
/// current. Its deletion on the server reaches the replica once a
/// subscription covers it again, across a reconnect.
#[test]
fn a_deletion_reaches_a_cold_cached_row_once_a_subscription_covers_it_again() {
    let server = open(0, AuthorSubject::SYSTEM);
    let room = id(1, 1);
    server
        .seed_settled_mergeable_for_bootstrap(
            "rooms",
            room,
            AuthorSubject::SYSTEM,
            cells(&[("name", Value::String("lobby".into()))]),
        )
        .unwrap();
    let client = open(1, alice());
    let rooms = |db: &Db| {
        let prepared = db.prepare_query(&Query::from("rooms")).unwrap();
        let opts = ReadOpts {
            tier: ReadTier::LocalOnly,
            ..ReadOpts::default()
        };
        block_on(db.all(&prepared, opts))
            .unwrap()
            .iter()
            .map(|row| row.row_uuid())
            .collect::<Vec<_>>()
    };
    let (client_side, server_side, _) = pipe();
    let upstream = block_on(client.connect_upstream(client_side));
    server.accept_subscriber(server_side, alice());
    let prepared = client.prepare_query(&Query::from("rooms")).unwrap();
    let stream = block_on(client.subscribe(&prepared, ReadOpts::default())).unwrap();
    pump(&server, &client);
    drop(stream);
    pump(&server, &client);
    assert_eq!(rooms(&client), [room]);

    assert!(client.detach_connection(&upstream));
    let (client_side, server_side, _) = pipe();
    let _upstream = block_on(client.connect_upstream(client_side));
    server.accept_subscriber(server_side, alice());
    pump(&server, &client);

    block_on(server.delete("rooms", room, Default::default())).unwrap();
    pump(&server, &client);
    assert_eq!(rooms(&client), [room], "the cold row is not kept current");

    let _stream = block_on(client.subscribe(&prepared, ReadOpts::default())).unwrap();
    pump(&server, &client);
    assert_eq!(rooms(&client), [], "the deletion reaches the covered row");
}

/// Seeds rooms 0 and 1, alice's membership of room 0, and `per_room`
/// messages in each room.
fn seeded(per_room: u32) -> Db {
    let server = open(0, AuthorSubject::SYSTEM);
    let seed = |table: &str, row: RowUuid, values: BTreeMap<String, Value>| {
        server
            .seed_settled_mergeable_for_bootstrap(table, row, AuthorSubject::SYSTEM, values)
            .unwrap();
    };
    for r in 0..2 {
        seed(
            "rooms",
            id(1, r),
            cells(&[("name", Value::String(format!("room {r}")))]),
        );
    }
    seed(
        "memberships",
        id(2, 0),
        cells(&[
            ("room", Value::Uuid(id(1, 0).0)),
            ("member", Value::String(subject())),
        ]),
    );
    for n in 0..per_room * 2 {
        seed(
            "messages",
            id(3, n),
            cells(&[
                ("room", Value::Uuid(id(1, n % 2).0)),
                ("body", Value::String(format!("m{n}"))),
            ]),
        );
    }
    server
}

/// Nico's review, point 1: a subscription's rows stop syncing once it
/// ends. The replica keeps them (cold), and the server ships nothing more
/// for them.
#[test]
fn an_ended_subscription_leaves_its_rows_cold() {
    let server = seeded(3);
    let client = open(1, alice());
    let (client_side, server_side, shipped) = pipe();
    let _upstream = block_on(client.connect_upstream(client_side));
    server.accept_subscriber(server_side, alice());
    let prepared = client.prepare_query(&Query::from("messages")).unwrap();
    let stream = block_on(client.subscribe(&prepared, ReadOpts::default())).unwrap();
    pump(&server, &client);
    assert_eq!(ids(&client, None).len(), 3);
    drop(stream);
    pump(&server, &client);

    let before = *shipped.borrow();
    block_on(server.update(
        "messages",
        id(3, 0),
        cells(&[("body", Value::String("edited".into()))]),
        Default::default(),
    ))
    .unwrap();
    pump(&server, &client);
    assert_eq!(*shipped.borrow(), before, "nothing ships for a cold row");
    assert_eq!(ids(&client, None).len(), 3, "the replica keeps its copies");
}

/// Losing access while away, without the rows changing: the resumed
/// opening compares the result with the one as of its resume point, and
/// the rows that left are evicted.
#[test]
fn a_resumed_subscription_evicts_rows_whose_access_was_lost_while_away() {
    let server = seeded(5);
    let client = open(1, alice());
    let (client_side, server_side, _) = pipe();
    let upstream = block_on(client.connect_upstream(client_side));
    server.accept_subscriber(server_side, alice());
    let prepared = client.prepare_query(&Query::from("messages")).unwrap();
    let _stream = block_on(client.subscribe(&prepared, ReadOpts::default())).unwrap();
    pump(&server, &client);
    assert_eq!(ids(&client, None).len(), 5);

    assert!(client.detach_connection(&upstream));
    block_on(server.delete("memberships", id(2, 0), Default::default())).unwrap();
    let (client_side, server_side, shipped) = pipe();
    let _upstream = block_on(client.connect_upstream(client_side));
    server.accept_subscriber(server_side, alice());
    pump(&server, &client);
    assert_eq!(ids(&client, None), ids(&server, Some(alice())));
    assert!(
        ids(&client, None).is_empty(),
        "the room's messages are evicted"
    );
    assert_eq!(*shipped.borrow(), 0, "only evictions ship");
}

/// A caught-up subscription resumes without shipping a row: no table its
/// result depends on was written since.
#[test]
fn a_caught_up_subscription_resumes_without_shipping_rows() {
    let server = seeded(50);
    let client = open(1, alice());
    let (client_side, server_side, _) = pipe();
    let upstream = block_on(client.connect_upstream(client_side));
    server.accept_subscriber(server_side, alice());
    let prepared = client.prepare_query(&Query::from("messages")).unwrap();
    let _stream = block_on(client.subscribe(&prepared, ReadOpts::default())).unwrap();
    pump(&server, &client);
    assert!(client.detach_connection(&upstream));
    // A write to a table the read does not depend on.
    block_on(server.update(
        "rooms",
        id(1, 1),
        cells(&[("name", Value::String("renamed".into()))]),
        Default::default(),
    ))
    .unwrap();
    let (client_side, server_side, shipped) = pipe();
    let _upstream = block_on(client.connect_upstream(client_side));
    server.accept_subscriber(server_side, alice());
    pump(&server, &client);
    assert_eq!(*shipped.borrow(), 0);
    assert_eq!(ids(&client, None), ids(&server, Some(alice())));
}

/// A cold row evicted through another subscription, then readable again
/// without changing, reaches the replica when the first subscription
/// resumes: the eviction made its older resume point untrue.
#[test]
fn a_row_evicted_and_readable_again_returns_when_an_older_subscription_resumes() {
    use jazz::query::{col, eq, lit};
    let server = seeded(1);
    let client = open(1, alice());
    let (client_side, server_side, _) = pipe();
    let _upstream = block_on(client.connect_upstream(client_side));
    server.accept_subscriber(server_side, alice());
    let q1 = client
        .prepare_query(
            &Query::from("messages").filter(eq(col("body"), lit(Value::String("m0".into())))),
        )
        .unwrap();
    let s1 = block_on(client.subscribe(&q1, ReadOpts::default())).unwrap();
    pump(&server, &client);
    assert_eq!(ids(&client, None), [id(3, 0)], "q1 brings m0");
    drop(s1);
    pump(&server, &client);

    let q2 = client.prepare_query(&Query::from("messages")).unwrap();
    let s2 = block_on(client.subscribe(&q2, ReadOpts::default())).unwrap();
    pump(&server, &client);
    block_on(server.delete("memberships", id(2, 0), Default::default())).unwrap();
    pump(&server, &client);
    assert!(ids(&client, None).is_empty(), "q2 evicts m0 on access loss");
    drop(s2);
    pump(&server, &client);

    // Access regained, m0 itself unchanged.
    server
        .seed_settled_mergeable_for_bootstrap(
            "memberships",
            id(2, 1),
            AuthorSubject::SYSTEM,
            cells(&[
                ("room", Value::Uuid(id(1, 0).0)),
                ("member", Value::String(subject())),
            ]),
        )
        .unwrap();
    let _s1 = block_on(client.subscribe(&q1, ReadOpts::default())).unwrap();
    pump(&server, &client);
    assert_eq!(
        ids(&server, Some(alice())),
        [id(3, 0)],
        "server: m0 readable"
    );
    assert_eq!(ids(&client, None), [id(3, 0)], "client converges on m0");
}

/// A cold row a fresh opening's listing drops (no `Evict` crosses: the
/// new session never shipped it), then readable again without changing,
/// reaches the replica when an older subscription resumes.
#[test]
fn a_row_a_listing_dropped_returns_when_an_older_subscription_resumes() {
    use jazz::query::{col, eq, lit};
    let server = seeded(1);
    let client = open(1, alice());
    let (client_side, server_side, _) = pipe();
    let upstream = block_on(client.connect_upstream(client_side));
    server.accept_subscriber(server_side, alice());
    let q1 = client
        .prepare_query(
            &Query::from("messages").filter(eq(col("body"), lit(Value::String("m0".into())))),
        )
        .unwrap();
    let s1 = block_on(client.subscribe(&q1, ReadOpts::default())).unwrap();
    pump(&server, &client);
    assert_eq!(ids(&client, None), [id(3, 0)], "q1 brings m0");
    drop(s1);
    pump(&server, &client);

    // Access lost while away; a new session never shipped m0.
    assert!(client.detach_connection(&upstream));
    block_on(server.delete("memberships", id(2, 0), Default::default())).unwrap();
    let (client_side, server_side, _) = pipe();
    let _upstream = block_on(client.connect_upstream(client_side));
    server.accept_subscriber(server_side, alice());
    pump(&server, &client);

    let q2 = client.prepare_query(&Query::from("messages")).unwrap();
    let s2 = block_on(client.subscribe(&q2, ReadOpts::default())).unwrap();
    pump(&server, &client);
    assert!(ids(&client, None).is_empty(), "q2's listing drops m0");
    drop(s2);
    pump(&server, &client);

    server
        .seed_settled_mergeable_for_bootstrap(
            "memberships",
            id(2, 1),
            AuthorSubject::SYSTEM,
            cells(&[
                ("room", Value::Uuid(id(1, 0).0)),
                ("member", Value::String(subject())),
            ]),
        )
        .unwrap();
    let _s1 = block_on(client.subscribe(&q1, ReadOpts::default())).unwrap();
    pump(&server, &client);
    assert_eq!(
        ids(&server, Some(alice())),
        [id(3, 0)],
        "server: m0 readable"
    );
    assert_eq!(ids(&client, None), [id(3, 0)], "client converges on m0");
}
