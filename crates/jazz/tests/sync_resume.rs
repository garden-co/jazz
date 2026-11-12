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
use jazz::query::{OrderDirection, Query, col, eq, lit};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};

use common::{
    allow_all_policies, compile_schema, exists, outer_eq, read_and_allow_all_writes, session_eq,
};

const ME: [&str; 2] = ["identity", "subject"];

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
            .table(
                TableSchemaBuilder::new("notes")
                    .fk_column("room", "rooms")
                    .column("body", ColumnType::Text)
                    .array_fk_column("stickers", "stickers")
                    .policies(allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new("reactions")
                    .fk_column("note", "notes")
                    .column("body", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new("stickers")
                    .column("body", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new("likes")
                    .fk_column("sticker", "stickers")
                    .policies(allow_all_policies()),
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

/// A duplex that counts the row images the server ships, and records
/// whether each read the client sends resumes (carries a `since`).
struct Pipe {
    out: Rc<RefCell<VecDeque<SyncMessage>>>,
    inbound: Rc<RefCell<VecDeque<SyncMessage>>>,
    shipped: Option<Rc<RefCell<usize>>>,
    resumes: Option<Rc<RefCell<Vec<bool>>>>,
}

impl Transport for Pipe {
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        if let (Some(count), SyncMessage::Rows { images, .. }) = (&self.shipped, &message) {
            *count.borrow_mut() += images.len();
        }
        if let (Some(resumes), SyncMessage::Read { since, .. }) = (&self.resumes, &message) {
            resumes.borrow_mut().push(since.is_some());
        }
        self.out.borrow_mut().push_back(message);
        Ok(())
    }

    fn try_recv(&mut self) -> Option<SyncMessage> {
        self.inbound.borrow_mut().pop_front()
    }
}

fn pipe() -> (Box<dyn Transport>, Box<dyn Transport>, Rc<RefCell<usize>>) {
    let (client, server, shipped, _) = watched_pipe();
    (client, server, shipped)
}

/// Whether each read a client sent resumed, in order.
type Resumes = Rc<RefCell<Vec<bool>>>;

/// A pipe that also records, for each read the client sends, whether it
/// resumes.
fn watched_pipe() -> (
    Box<dyn Transport>,
    Box<dyn Transport>,
    Rc<RefCell<usize>>,
    Resumes,
) {
    let up = Rc::new(RefCell::new(VecDeque::new()));
    let down = Rc::new(RefCell::new(VecDeque::new()));
    let shipped = Rc::new(RefCell::new(0));
    let resumes = Rc::new(RefCell::new(Vec::new()));
    (
        Box::new(Pipe {
            out: up.clone(),
            inbound: down.clone(),
            shipped: None,
            resumes: Some(resumes.clone()),
        }),
        Box::new(Pipe {
            out: down,
            inbound: up,
            shipped: Some(shipped.clone()),
            resumes: None,
        }),
        shipped,
        resumes,
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
    server.accept_subscriber(server_side, alice()).unwrap();
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
    server.accept_subscriber(server_side, alice()).unwrap();
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
    server.accept_subscriber(server_side, alice()).unwrap();
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
    server.accept_subscriber(server_side, alice()).unwrap();
    let prepared = client.prepare_query(&Query::from("messages")).unwrap();
    let _stream = block_on(client.subscribe(&prepared, ReadOpts::default())).unwrap();
    pump(&server, &client);
    let mut times = Vec::new();
    for _ in 0..20 {
        assert!(client.detach_connection(&upstream));
        let (client_side, server_side, shipped) = pipe();
        let started = std::time::Instant::now();
        upstream = block_on(client.connect_upstream(client_side));
        server.accept_subscriber(server_side, alice()).unwrap();
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
    server.accept_subscriber(server_side, alice()).unwrap();
    let prepared = client.prepare_query(&Query::from("rooms")).unwrap();
    let stream = block_on(client.subscribe(&prepared, ReadOpts::default())).unwrap();
    pump(&server, &client);
    drop(stream);
    pump(&server, &client);
    assert_eq!(rooms(&client), [room]);

    assert!(client.detach_connection(&upstream));
    let (client_side, server_side, _) = pipe();
    let _upstream = block_on(client.connect_upstream(client_side));
    server.accept_subscriber(server_side, alice()).unwrap();
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
    server.accept_subscriber(server_side, alice()).unwrap();
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
    server.accept_subscriber(server_side, alice()).unwrap();
    let prepared = client.prepare_query(&Query::from("messages")).unwrap();
    let _stream = block_on(client.subscribe(&prepared, ReadOpts::default())).unwrap();
    pump(&server, &client);
    assert_eq!(ids(&client, None).len(), 5);

    assert!(client.detach_connection(&upstream));
    block_on(server.delete("memberships", id(2, 0), Default::default())).unwrap();
    let (client_side, server_side, shipped) = pipe();
    let _upstream = block_on(client.connect_upstream(client_side));
    server.accept_subscriber(server_side, alice()).unwrap();
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
    server.accept_subscriber(server_side, alice()).unwrap();
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
    server.accept_subscriber(server_side, alice()).unwrap();
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
    server.accept_subscriber(server_side, alice()).unwrap();
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

/// A cold row a fresh opening's `members` drops (no `AccessLost` crosses: the
/// new session never shipped it), then readable again without changing,
/// reaches the replica when an older subscription resumes.
#[test]
fn a_row_a_listing_dropped_returns_when_an_older_subscription_resumes() {
    use jazz::query::{col, eq, lit};
    let server = seeded(1);
    let client = open(1, alice());
    let (client_side, server_side, _) = pipe();
    let upstream = block_on(client.connect_upstream(client_side));
    server.accept_subscriber(server_side, alice()).unwrap();
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
    server.accept_subscriber(server_side, alice()).unwrap();
    pump(&server, &client);

    let q2 = client.prepare_query(&Query::from("messages")).unwrap();
    let s2 = block_on(client.subscribe(&q2, ReadOpts::default())).unwrap();
    pump(&server, &client);
    assert!(ids(&client, None).is_empty(), "q2's members drop m0");
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

/// A resumed subscription takes the fast path (nothing it depends on was
/// written since), then, in the same server tick, another client's
/// offline commit makes an older, unchanged row readable. That row joins
/// the result after the resume point was judged, so it ships: the client
/// never held it.
#[test]
fn a_row_readable_through_a_commit_in_the_resumes_tick_ships() {
    let server = seeded(1);
    let bob = AuthorSubject::for_test_bytes([0x66; 16]);
    let alice_db = open(1, alice());
    let bob_db = open(2, bob);
    let connect = |db: &Db, who: AuthorSubject| {
        let (client_side, server_side, _) = pipe();
        let upstream = block_on(db.connect_upstream(client_side));
        server.accept_subscriber(server_side, who).unwrap();
        upstream
    };
    let tick_all = || {
        for _ in 0..5 {
            block_on(alice_db.tick()).unwrap();
            block_on(bob_db.tick()).unwrap();
            block_on(server.tick()).unwrap();
        }
        block_on(alice_db.tick()).unwrap();
        block_on(bob_db.tick()).unwrap();
    };
    let alice_up = connect(&alice_db, alice());
    let bob_up = connect(&bob_db, bob);
    let prepared = alice_db.prepare_query(&Query::from("messages")).unwrap();
    let _stream = block_on(alice_db.subscribe(&prepared, ReadOpts::default())).unwrap();
    tick_all();
    assert_eq!(ids(&alice_db, None), [id(3, 0)], "alice reads room 0 only");

    // Both away; bob adds alice to room 1 offline. Nothing reaches the
    // server, so alice's subscription is caught up when it resumes.
    assert!(alice_db.detach_connection(&alice_up));
    assert!(bob_db.detach_connection(&bob_up));
    block_on(bob_db.insert(
        "memberships",
        cells(&[
            ("room", Value::Uuid(id(1, 1).0)),
            ("member", Value::String(subject())),
        ]),
        Default::default(),
    ))
    .unwrap();

    // Alice's resume and bob's upload reach the server in one tick, the
    // resume first.
    let _alice_up = connect(&alice_db, alice());
    let _bob_up = connect(&bob_db, bob);
    tick_all();
    assert_eq!(
        ids(&server, Some(alice())),
        [id(3, 0), id(3, 1)],
        "server: m1 readable"
    );
    assert_eq!(
        ids(&alice_db, None),
        [id(3, 0), id(3, 1)],
        "alice receives m1, unchanged since her resume point"
    );
}

/// Alice repeats a remote one-shot read without keeping a subscription. A
/// settled checkpoint suppresses unchanged rows; reconnect catches changes.
/// alice --read--> server --rows/settled--> alice --read since--> server
#[test]
fn one_shot_reads_resume_and_catch_changes_after_reconnect() {
    let server = seeded(3);
    let client = open(1, alice());
    let (up, down, shipped) = pipe();
    let connection = block_on(client.connect_upstream(up));
    server.accept_subscriber(down, alice()).unwrap();
    assert_eq!(read_once(&server, &client).len(), 3);
    let before = *shipped.borrow();
    assert_eq!(read_once(&server, &client).len(), 3);
    assert_eq!(
        *shipped.borrow(),
        before,
        "unchanged one-shot result is not resent"
    );
    assert!(client.detach_connection(&connection));
    block_on(server.update(
        "messages",
        id(3, 0),
        cells(&[("body", Value::String("changed".into()))]),
        Default::default(),
    ))
    .unwrap();
    block_on(server.delete("messages", id(3, 2), Default::default())).unwrap();
    let (up, down, shipped) = pipe();
    let _connection = block_on(client.connect_upstream(up));
    server.accept_subscriber(down, alice()).unwrap();
    assert_eq!(read_once(&server, &client).len(), 2);
    assert!(*shipped.borrow() > 0, "changed result is delivered");
    let before = *shipped.borrow();
    assert_eq!(read_once(&server, &client).len(), 2);
    assert_eq!(*shipped.borrow(), before);
}

fn read_once(server: &Db, client: &Db) -> Vec<RowUuid> {
    read_once_via(&[server], client)
}

fn read_once_via(upstream: &[&Db], client: &Db) -> Vec<RowUuid> {
    let mut ids = read_query_once(upstream, client, &Query::from("messages"));
    ids.sort();
    ids
}

/// A one-shot Remote read of `query`, its rows in the order it lists them.
fn read_query_once(upstream: &[&Db], client: &Db, query: &Query) -> Vec<RowUuid> {
    use std::future::Future;
    use std::pin::pin;
    use std::task::{Context, Poll, Waker};
    let query = postcard::to_allocvec(query).unwrap();
    let mut read = pin!(client.all_serialized_query(
        &query,
        ReadOpts {
            tier: ReadTier::Remote,
            ..ReadOpts::default()
        },
        None,
        None,
        None
    ));
    for _ in 0..30 {
        if let Poll::Ready(rows) = read.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
            let jazz::db::SerializedReadResult::Rows(rows) = rows.unwrap() else {
                panic!("flat read");
            };
            return rows.iter().map(|row| row.row_uuid()).collect();
        }
        for server in upstream {
            pump(server, client);
        }
    }
    panic!("one-shot read did not settle");
}

/// Alice's previously readable messages leave a repeated one-shot result
/// when Bob revokes membership, although the message rows did not change.
#[test]
fn one_shot_resume_rechecks_permissions() {
    let server = seeded(3);
    let client = open(1, alice());
    let (up, down, _) = pipe();
    let connection = block_on(client.connect_upstream(up));
    server.accept_subscriber(down, alice()).unwrap();
    assert_eq!(read_once(&server, &client).len(), 3);
    assert!(client.detach_connection(&connection));
    block_on(server.delete("memberships", id(2, 0), Default::default())).unwrap();
    let (up, down, _) = pipe();
    let _connection = block_on(client.connect_upstream(up));
    server.accept_subscriber(down, alice()).unwrap();
    assert!(read_once(&server, &client).is_empty());
    assert!(
        ids(&client, None).is_empty(),
        "revoked cached rows are removed"
    );
}

/// Alice closes her durable replica after a one-shot read, then reopens it.
/// Its persisted rows and checkpoint let Bob's server skip unchanged images.
#[test]
fn one_shot_checkpoint_survives_reopen() {
    let server = seeded(3);
    let dir = std::env::temp_dir().join(format!("jazz-once-resume-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("store.sqlite");
    let open = || {
        block_on(Db::open_at(
            DbConfig::new(
                schema(),
                DbIdentity {
                    node: NodeUuid::from_bytes([1; 16]),
                    author: alice(),
                },
            ),
            &path,
            Default::default(),
        ))
        .unwrap()
    };
    {
        let client = open();
        let (up, down, _) = pipe();
        let connection = block_on(client.connect_upstream(up));
        server.accept_subscriber(down, alice()).unwrap();
        assert_eq!(read_once(&server, &client).len(), 3);
        assert!(client.detach_connection(&connection));
    }
    {
        let client = open();
        let (up, down, shipped) = pipe();
        let _connection = block_on(client.connect_upstream(up));
        server.accept_subscriber(down, alice()).unwrap();
        assert_eq!(read_once(&server, &client).len(), 3);
        assert_eq!(
            *shipped.borrow(),
            0,
            "durable checkpoint suppresses cached images"
        );
    }
    std::fs::remove_dir_all(dir).unwrap();
}

/// Bob grants Alice access while she is offline. A repeated one-shot read
/// must ship newly readable messages even though their row seqs are old.
#[test]
fn one_shot_resume_includes_unchanged_new_members() {
    let server = seeded(1);
    let client = open(1, alice());
    let (up, down, _) = pipe();
    let connection = block_on(client.connect_upstream(up));
    server.accept_subscriber(down, alice()).unwrap();
    assert_eq!(read_once(&server, &client), [id(3, 0)]);
    assert!(client.detach_connection(&connection));
    server
        .seed_settled_mergeable_for_bootstrap(
            "memberships",
            id(2, 1),
            AuthorSubject::SYSTEM,
            cells(&[
                ("room", Value::Uuid(id(1, 1).0)),
                ("member", Value::String(subject())),
            ]),
        )
        .unwrap();
    let (up, down, shipped) = pipe();
    let _connection = block_on(client.connect_upstream(up));
    server.accept_subscriber(down, alice()).unwrap();
    assert_eq!(read_once(&server, &client), [id(3, 0), id(3, 1)]);
    assert!(*shipped.borrow() > 0);
}

/// Alice reads through a relay. The relay resumes from its own cached result,
/// while the foreground still receives the rows it needs from that relay.
#[test]
fn one_shot_relay_reuses_its_own_checkpoint() {
    let server = seeded(3);
    let relay = open(2, alice());
    let client = open(1, alice());
    let (up, down, shipped) = pipe();
    let _upstream = block_on(relay.connect_upstream(up));
    server.accept_subscriber(down, alice()).unwrap();
    let (up, down, _) = pipe();
    let _downstream = block_on(client.connect_upstream(up));
    relay.accept_subscriber(down, alice()).unwrap();
    assert_eq!(read_once_via(&[&relay, &server], &client).len(), 3);
    let before = *shipped.borrow();
    assert_eq!(read_once_via(&[&relay, &server], &client).len(), 3);
    assert_eq!(
        *shipped.borrow(),
        before,
        "authority does not resend relay's cached rows"
    );

    // A cold foreground must still get the relay's complete result.
    let cold = open(3, alice());
    let (up, down, _) = pipe();
    let _downstream = block_on(cold.connect_upstream(up));
    relay.accept_subscriber(down, alice()).unwrap();
    assert_eq!(read_once_via(&[&relay, &server], &cold).len(), 3);
    assert_eq!(*shipped.borrow(), before);
}

/// Alice reads messages once while Bob later revokes her membership.
/// The cache stays cold until Alice explicitly reads again, which rechecks access.
#[test]
fn one_shot_rows_and_support_go_cold_until_read_again() {
    let server = seeded(1);
    let client = open(1, alice());
    let (up, down, shipped) = pipe();
    let _connection = block_on(client.connect_upstream(up));
    server.accept_subscriber(down, alice()).unwrap();
    assert_eq!(read_once(&server, &client), [id(3, 0)]);
    let before = *shipped.borrow();
    block_on(server.delete("memberships", id(2, 0), Default::default())).unwrap();
    pump(&server, &client);
    assert_eq!(
        *shipped.borrow(),
        before,
        "no background supporting-row delivery"
    );
    assert_eq!(
        ids(&client, None),
        [id(3, 0)],
        "local reads retain the cold result"
    );
    assert!(
        read_once(&server, &client).is_empty(),
        "remote read reconciles revocation"
    );
}

/// Alice's own write settles without subscribing. Bob's later deletion does
/// not stream to Alice; a live read refreshes it and stays current until dropped.
#[test]
fn settled_writes_go_cold_but_live_reads_stay_current() {
    let server = seeded(1);
    let client = open(1, alice());
    let (up, down, _) = pipe();
    let _connection = block_on(client.connect_upstream(up));
    server.accept_subscriber(down, alice()).unwrap();
    assert_eq!(read_once(&server, &client), [id(3, 0)]);
    block_on(client.update(
        "messages",
        id(3, 0),
        cells(&[("body", Value::String("alice".into()))]),
        Default::default(),
    ))
    .unwrap();
    pump(&server, &client);
    assert_eq!(
        client.pending_upload_count(),
        0,
        "write settles without a watch"
    );
    block_on(server.delete("messages", id(3, 0), Default::default())).unwrap();
    pump(&server, &client);
    assert_eq!(
        ids(&client, None),
        [id(3, 0)],
        "settled write does not retain a watch"
    );
    let query = client.prepare_query(&Query::from("messages")).unwrap();
    let live = block_on(client.subscribe(&query, ReadOpts::default())).unwrap();
    pump(&server, &client);
    assert!(ids(&client, None).is_empty());
    server
        .seed_settled_mergeable_for_bootstrap(
            "messages",
            id(3, 2),
            AuthorSubject::SYSTEM,
            cells(&[
                ("room", Value::Uuid(id(1, 0).0)),
                ("body", Value::String("bob".into())),
            ]),
        )
        .unwrap();
    pump(&server, &client);
    assert_eq!(ids(&client, None), [id(3, 2)]);
    // A one-shot read overlapping the live one must not extend its lifetime.
    assert_eq!(read_once(&server, &client), [id(3, 2)]);
    drop(live);
    pump(&server, &client);
    block_on(server.delete("messages", id(3, 2), Default::default())).unwrap();
    pump(&server, &client);
    assert_eq!(ids(&client, None), [id(3, 2)]);
    assert!(read_once(&server, &client).is_empty());
}

/// Alice cancels one-shot reads before send, after send, and after link loss.
/// Internal counts are needed because leaked request metadata does not change query results.
#[test]
fn cancelled_one_shot_reads_release_request_state() {
    use std::future::Future;
    use std::task::{Context, Waker};
    let server = seeded(1);
    let client = open(1, alice());
    let query = postcard::to_allocvec(&Query::from("messages")).unwrap();
    for stage in 0..3 {
        let (up, down, _) = pipe();
        let connection = block_on(client.connect_upstream(up));
        server.accept_subscriber(down, alice()).unwrap();
        let mut read = Box::pin(client.all_serialized_query(
            &query,
            ReadOpts::for_read_tier(ReadTier::Remote),
            None,
            None,
            None,
        ));
        assert!(
            read.as_mut()
                .poll(&mut Context::from_waker(Waker::noop()))
                .is_pending()
        );
        assert_eq!(client.pending_once_state_for_test(), (1, 1, 1));
        if stage > 0 {
            block_on(client.tick()).unwrap(); // Send, but Bob's server does not answer yet.
            assert_eq!(client.pending_once_state_for_test(), (1, 1, 0));
        }
        if stage == 2 {
            assert!(client.detach_connection(&connection));
            assert_eq!(client.pending_once_state_for_test(), (0, 1, 0));
        }
        drop(read);
        assert_eq!(client.pending_once_state_for_test(), (0, 0, 0));
        pump(&server, &client); // A late answer cannot resurrect the request.
        assert_eq!(client.pending_once_state_for_test(), (0, 0, 0));
    }
}

/// Alice's tab reads LocalFirst through Bob's worker while its authority link
/// keeps dropping. Background refreshes have no waiting future to clean them.
/// Internal counts verify that query clones are released, which results cannot show.
#[test]
fn relay_background_checkpoints_end_with_their_connection() {
    use std::future::Future;
    use std::task::{Context, Poll, Waker};
    let relay = open(2, alice());
    let tab = open(1, alice());
    tab.set_non_durable_client();
    let (up, _down, _) = pipe();
    let mut upstream = block_on(relay.connect_upstream(up));
    let (up, down, _) = pipe();
    let _tab_link = block_on(tab.connect_upstream(up));
    relay.accept_subscriber(down, alice()).unwrap();
    let query = postcard::to_allocvec(&Query::from("messages")).unwrap();
    for round in 0..4 {
        let mut read = Box::pin(tab.all_serialized_query(
            &query,
            ReadOpts::for_read_tier(ReadTier::LocalFirst),
            None,
            None,
            None,
        ));
        let mut answered = false;
        for _ in 0..20 {
            if let Poll::Ready(result) = read.as_mut().poll(&mut Context::from_waker(Waker::noop()))
            {
                result.unwrap();
                answered = true;
                break;
            }
            pump(&relay, &tab);
        }
        assert!(answered, "LocalFirst does not await the upstream refresh");
        drop(read);
        assert_eq!(relay.pending_once_state_for_test(), (1, 0, 0));
        if round % 2 == 0 {
            assert!(relay.detach_connection(&upstream));
            assert_eq!(relay.pending_once_state_for_test(), (0, 0, 0));
        }
        // Also cover replacement without an explicit detach.
        let (up, _down, _) = pipe();
        upstream = block_on(relay.connect_upstream(up));
        assert_eq!(relay.pending_once_state_for_test(), (0, 0, 0));
    }
}

/// Alice's waiting Remote read through Bob's relay is retried after link loss.
/// Internal counts check cleanup, and the returned rows prove the retry still works.
#[test]
fn pending_relay_read_rebuilds_its_checkpoint_after_reconnect() {
    use std::future::Future;
    use std::task::{Context, Poll, Waker};
    let server = seeded(1);
    let relay = open(2, alice());
    let tab = open(1, alice());
    let (up, _down, _) = pipe();
    let upstream = block_on(relay.connect_upstream(up));
    let (up, down, _) = pipe();
    let _tab_link = block_on(tab.connect_upstream(up));
    relay.accept_subscriber(down, alice()).unwrap();
    let query = postcard::to_allocvec(&Query::from("messages")).unwrap();
    let mut read = Box::pin(tab.all_serialized_query(
        &query,
        ReadOpts::for_read_tier(ReadTier::Remote),
        None,
        None,
        None,
    ));
    assert!(
        read.as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    pump(&relay, &tab);
    assert_eq!(relay.pending_once_state_for_test(), (1, 1, 0));
    assert!(relay.detach_connection(&upstream));
    assert_eq!(relay.pending_once_state_for_test(), (0, 1, 0));
    let (up, down, _) = pipe();
    let _upstream = block_on(relay.connect_upstream(up));
    server.accept_subscriber(down, alice()).unwrap();
    assert_eq!(relay.pending_once_state_for_test(), (1, 1, 1));
    for _ in 0..30 {
        pump(&server, &relay);
        pump(&relay, &tab);
        if let Poll::Ready(result) = read.as_mut().poll(&mut Context::from_waker(Waker::noop())) {
            let jazz::db::SerializedReadResult::Rows(rows) = result.unwrap() else {
                panic!("flat read")
            };
            assert_eq!(rows.len(), 1);
            assert_eq!(relay.pending_once_state_for_test(), (0, 0, 0));
            assert_eq!(tab.pending_once_state_for_test(), (0, 0, 0));
            return;
        }
    }
    panic!("retried read did not settle");
}

/// Alice's first device deletes a message and reads once, so its
/// checkpoint is after the deletion; her second device restores it. The
/// first device's copy of the row is a cold tombstone (no live read covers
/// it, and its settled delete keeps no watch), so a local read may still
/// miss the restore, but its one-shot Remote read resumes from the
/// checkpoint and must bring the restored row back.
#[test]
fn a_one_shot_remote_read_after_a_delete_sees_another_replicas_restore() {
    let server = seeded(1);
    let a = open(1, alice());
    let b = open(2, alice());
    let (up, down, _) = pipe();
    let _a_link = block_on(a.connect_upstream(up));
    server.accept_subscriber(down, alice()).unwrap();
    let (up, down, _) = pipe();
    let _b_link = block_on(b.connect_upstream(up));
    server.accept_subscriber(down, alice()).unwrap();

    // b watches the table, so it holds the deleted version to restore.
    let prepared = b.prepare_query(&Query::from("messages")).unwrap();
    let _watching = block_on(b.subscribe(&prepared, ReadOpts::default())).unwrap();
    pump(&server, &b);
    assert_eq!(ids(&b, None), [id(3, 0)]);
    assert_eq!(read_once(&server, &a), [id(3, 0)]);
    block_on(a.delete("messages", id(3, 0), Default::default())).unwrap();
    pump(&server, &a);
    assert_eq!(a.pending_upload_count(), 0, "the deletion settled");
    assert!(
        read_once(&server, &a).is_empty(),
        "checkpoint after the delete"
    );
    pump(&server, &b);
    assert!(ids(&b, None).is_empty(), "the deletion reached b");

    block_on(b.restore("messages", id(3, 0), None, Default::default())).unwrap();
    pump(&server, &b);
    assert_eq!(b.pending_upload_count(), 0, "the restore settled");
    pump(&server, &a);
    assert_eq!(
        ids(&a, None),
        Vec::<RowUuid>::new(),
        "a's tombstone is a cold copy until a read covers it"
    );
    assert_eq!(
        read_once(&server, &a),
        [id(3, 0)],
        "the resumed one-shot Remote read ships the restore"
    );
    assert_eq!(ids(&a, None), [id(3, 0)], "and installs it locally");
}

/// Two subscriptions over one row, where it leaves one of them while the
/// client is away without changing (its membership input did): the row
/// stays on the replica for the other, whichever reopens first.
fn a_row_one_subscription_lost_stays_for_the_other(lost_first: bool) {
    let server = seeded(1);
    let client = open(1, alice());
    let (client_side, server_side, _) = pipe();
    let upstream = block_on(client.connect_upstream(client_side));
    server.accept_subscriber(server_side, alice()).unwrap();
    // Rooms alice is a member of, and every room.
    let member_of = client
        .prepare_query(&Query::from("rooms").join_via("memberships", "room", []))
        .unwrap();
    let every = client.prepare_query(&Query::from("rooms")).unwrap();
    let (first, second) = if lost_first {
        (&member_of, &every)
    } else {
        (&every, &member_of)
    };
    let _first = block_on(client.subscribe(first, ReadOpts::default())).unwrap();
    let _second = block_on(client.subscribe(second, ReadOpts::default())).unwrap();
    pump(&server, &client);
    let rooms = |db: &Db| {
        let prepared = db.prepare_query(&Query::from("rooms")).unwrap();
        let opts = ReadOpts {
            tier: ReadTier::LocalOnly,
            ..ReadOpts::default()
        };
        let mut rows: Vec<RowUuid> = block_on(db.all(&prepared, opts))
            .unwrap()
            .iter()
            .map(|row| row.row_uuid())
            .collect();
        rows.sort();
        rows
    };
    assert_eq!(rooms(&client), [id(1, 0), id(1, 1)]);

    // Away, alice leaves room 0: it leaves the first query unchanged.
    assert!(client.detach_connection(&upstream));
    block_on(server.delete("memberships", id(2, 0), Default::default())).unwrap();
    let (client_side, server_side, _) = pipe();
    let _upstream = block_on(client.connect_upstream(client_side));
    server.accept_subscriber(server_side, alice()).unwrap();
    pump(&server, &client);
    assert_eq!(
        rooms(&client),
        [id(1, 0), id(1, 1)],
        "every room is still the second query's"
    );
}

#[test]
fn a_row_one_subscription_lost_stays_when_the_losing_one_reopens_first() {
    a_row_one_subscription_lost_stays_for_the_other(true);
}

#[test]
fn a_row_one_subscription_lost_stays_when_the_holding_one_reopens_first() {
    a_row_one_subscription_lost_stays_for_the_other(false);
}

/// The rooms `db` holds, read locally.
fn local_rooms(db: &Db) -> Vec<RowUuid> {
    let prepared = db.prepare_query(&Query::from("rooms")).unwrap();
    let opts = ReadOpts {
        tier: ReadTier::LocalOnly,
        ..ReadOpts::default()
    };
    let mut rows: Vec<RowUuid> = block_on(db.all(&prepared, opts))
        .unwrap()
        .iter()
        .map(|row| row.row_uuid())
        .collect();
    rows.sort();
    rows
}

/// A row one subscription lost while the client was away, which another
/// holds only as an included row (a note's room), stays on the replica for
/// it: the server holds it for that one and would not ship it again.
fn an_included_row_one_subscription_lost_stays_for_the_other(lost_first: bool) {
    let server = seeded(1);
    server
        .seed_settled_mergeable_for_bootstrap(
            "notes",
            id(4, 0),
            AuthorSubject::SYSTEM,
            cells(&[
                ("room", Value::Uuid(id(1, 0).0)),
                ("body", Value::String("n0".into())),
                ("stickers", Value::Array(Vec::new())),
            ]),
        )
        .unwrap();
    let client = open(1, alice());
    let (client_side, server_side, _) = pipe();
    let upstream = block_on(client.connect_upstream(client_side));
    server.accept_subscriber(server_side, alice()).unwrap();
    // Rooms alice is a member of, and every note with its room.
    let member_of = client
        .prepare_query(&Query::from("rooms").join_via("memberships", "room", []))
        .unwrap();
    let notes = client
        .prepare_query(&Query::from("notes").include("room"))
        .unwrap();
    let (first, second) = if lost_first {
        (&member_of, &notes)
    } else {
        (&notes, &member_of)
    };
    let _first = block_on(client.subscribe(first, ReadOpts::default())).unwrap();
    let _second = block_on(client.subscribe(second, ReadOpts::default())).unwrap();
    pump(&server, &client);
    assert_eq!(local_rooms(&client), [id(1, 0)]);

    // Away, alice leaves room 0: the note still includes it, unchanged.
    assert!(client.detach_connection(&upstream));
    block_on(server.delete("memberships", id(2, 0), Default::default())).unwrap();
    let (client_side, server_side, _) = pipe();
    let _upstream = block_on(client.connect_upstream(client_side));
    server.accept_subscriber(server_side, alice()).unwrap();
    pump(&server, &client);
    assert_eq!(
        local_rooms(&client),
        [id(1, 0)],
        "room 0 is still the note's included room"
    );
}

#[test]
fn an_included_row_one_subscription_lost_stays_when_the_losing_one_reopens_first() {
    an_included_row_one_subscription_lost_stays_for_the_other(true);
}

#[test]
fn an_included_row_one_subscription_lost_stays_when_the_including_one_reopens_first() {
    an_included_row_one_subscription_lost_stays_for_the_other(false);
}

/// A row that left the other subscription through a live delta is no
/// longer that one's to keep, though its last listing still names it: the
/// first subscription's listing evicts it.
#[test]
fn a_row_that_left_the_other_subscription_live_is_evicted_by_a_listing() {
    use jazz::query::{col, eq, lit};
    let server = seeded(1);
    let client = open(1, alice());
    let (client_side, server_side, _) = pipe();
    let upstream = block_on(client.connect_upstream(client_side));
    server.accept_subscriber(server_side, alice()).unwrap();
    // Rooms alice is a member of (reopens first), and room 0 by name.
    let member_of = client
        .prepare_query(&Query::from("rooms").join_via("memberships", "room", []))
        .unwrap();
    let named = client
        .prepare_query(
            &Query::from("rooms").filter(eq(col("name"), lit(Value::String("room 0".into())))),
        )
        .unwrap();
    let _member_of = block_on(client.subscribe(&member_of, ReadOpts::default())).unwrap();
    let _named = block_on(client.subscribe(&named, ReadOpts::default())).unwrap();
    pump(&server, &client);
    assert_eq!(local_rooms(&client), [id(1, 0)]);

    // Room 0 is renamed while connected: it leaves the named query live.
    block_on(server.update(
        "rooms",
        id(1, 0),
        cells(&[("name", Value::String("renamed".into()))]),
        Default::default(),
    ))
    .unwrap();
    pump(&server, &client);
    // Away, alice leaves room 0.
    assert!(client.detach_connection(&upstream));
    block_on(server.delete("memberships", id(2, 0), Default::default())).unwrap();
    let (client_side, server_side, _) = pipe();
    let _upstream = block_on(client.connect_upstream(client_side));
    server.accept_subscriber(server_side, alice()).unwrap();
    pump(&server, &client);
    assert!(
        local_rooms(&client).is_empty(),
        "no live read holds room 0 any more"
    );
}

/// A client-to-server duplex that never delivers the client's commits: its
/// writes stay pending for as long as the test needs.
struct HoldCommits(Box<dyn Transport>);

impl Transport for HoldCommits {
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        if matches!(message, SyncMessage::Commit { .. }) {
            return Ok(());
        }
        self.0.send(message)
    }

    fn try_recv(&mut self) -> Option<SyncMessage> {
        self.0.try_recv()
    }
}

/// Support is followed from the row the server settled, not from a
/// pending local write: a note moved to another room, its write not yet
/// settled, still keeps the room the server holds for it (were the write
/// rejected, the server would never send that room again).
fn a_pending_write_does_not_move_what_another_read_holds(member_of_first: bool) {
    let server = seeded(1);
    server
        .seed_settled_mergeable_for_bootstrap(
            "notes",
            id(4, 0),
            AuthorSubject::SYSTEM,
            cells(&[
                ("room", Value::Uuid(id(1, 0).0)),
                ("body", Value::String("n0".into())),
                ("stickers", Value::Array(Vec::new())),
            ]),
        )
        .unwrap();
    let client = open(1, alice());
    let (client_side, server_side, _) = pipe();
    let upstream = block_on(client.connect_upstream(client_side));
    server.accept_subscriber(server_side, alice()).unwrap();
    let member_of = client
        .prepare_query(&Query::from("rooms").join_via("memberships", "room", []))
        .unwrap();
    let notes = client
        .prepare_query(&Query::from("notes").include("room"))
        .unwrap();
    let (first, second) = if member_of_first {
        (&member_of, &notes)
    } else {
        (&notes, &member_of)
    };
    let _first = block_on(client.subscribe(first, ReadOpts::default())).unwrap();
    let _second = block_on(client.subscribe(second, ReadOpts::default())).unwrap();
    pump(&server, &client);
    assert_eq!(local_rooms(&client), [id(1, 0)]);

    // Away, the note moves to room 1 locally and alice leaves room 0.
    assert!(client.detach_connection(&upstream));
    block_on(client.update(
        "notes",
        id(4, 0),
        cells(&[("room", Value::Uuid(id(1, 1).0))]),
        Default::default(),
    ))
    .unwrap();
    block_on(server.delete("memberships", id(2, 0), Default::default())).unwrap();
    let (client_side, server_side, _) = pipe();
    let _upstream = block_on(client.connect_upstream(Box::new(HoldCommits(client_side))));
    server.accept_subscriber(server_side, alice()).unwrap();
    pump(&server, &client);
    assert_eq!(
        client.pending_upload_count(),
        1,
        "the move is still pending"
    );
    assert_eq!(
        local_rooms(&client),
        [id(1, 0)],
        "room 0 is the settled note's room, which the server holds for it"
    );
}

#[test]
fn a_pending_write_does_not_move_what_another_read_holds_losing_one_first() {
    a_pending_write_does_not_move_what_another_read_holds(true);
}

#[test]
fn a_pending_write_does_not_move_what_another_read_holds_holding_one_first() {
    a_pending_write_does_not_move_what_another_read_holds(false);
}

/// The reactions `db` holds, read locally.
fn local_reactions(db: &Db) -> Vec<RowUuid> {
    let prepared = db.prepare_query(&Query::from("reactions")).unwrap();
    let opts = ReadOpts {
        tier: ReadTier::LocalOnly,
        ..ReadOpts::default()
    };
    let mut rows: Vec<RowUuid> = block_on(db.all(&prepared, opts))
        .unwrap()
        .iter()
        .map(|row| row.row_uuid())
        .collect();
    rows.sort();
    rows
}

/// Every hop of another read's support is followed as the server settled
/// it: a pending write that moves a note out of a room's relation does not
/// stop the note's reactions from being that read's, two hops down.
fn a_pending_write_does_not_move_support_below_the_first_hop(joined_first: bool) {
    use jazz::query::{ArraySubquery, col, eq, lit};
    let server = seeded(1);
    let seed = |table: &str, row: RowUuid, values: BTreeMap<String, Value>| {
        server
            .seed_settled_mergeable_for_bootstrap(table, row, AuthorSubject::SYSTEM, values)
            .unwrap();
    };
    seed(
        "notes",
        id(4, 0),
        cells(&[
            ("room", Value::Uuid(id(1, 0).0)),
            ("body", Value::String("n0".into())),
            ("stickers", Value::Array(Vec::new())),
        ]),
    );
    seed(
        "reactions",
        id(5, 1),
        cells(&[
            ("note", Value::Uuid(id(4, 0).0)),
            ("body", Value::String("b".into())),
        ]),
    );
    let client = open(1, alice());
    let (client_side, server_side, _) = pipe();
    let upstream = block_on(client.connect_upstream(client_side));
    server.accept_subscriber(server_side, alice()).unwrap();
    // Reactions to the note whose body is n0, and room 0 with its notes'
    // reactions.
    let joined = client
        .prepare_query(&Query::from("reactions").join_via_column(
            "notes",
            "id",
            "note",
            [eq(col("body"), lit(Value::String("n0".into())))],
        ))
        .unwrap();
    let mut notes = ArraySubquery::new("notes", "notes", "room", "id");
    notes
        .nested_arrays
        .push(ArraySubquery::new("reactions", "reactions", "note", "id"));
    let room = client
        .prepare_query(
            &Query::from("rooms")
                .filter(eq(col("name"), lit(Value::String("room 0".into()))))
                .array_subquery(notes),
        )
        .unwrap();
    let (first, second) = if joined_first {
        (&joined, &room)
    } else {
        (&room, &joined)
    };
    let _first = block_on(client.subscribe(first, ReadOpts::default())).unwrap();
    let _second = block_on(client.subscribe(second, ReadOpts::default())).unwrap();
    pump(&server, &client);
    assert_eq!(local_reactions(&client), [id(5, 1)]);

    // Away, the note moves to room 1 locally, and the server renames it:
    // reaction 1 leaves the joined read unchanged.
    assert!(client.detach_connection(&upstream));
    block_on(client.update(
        "notes",
        id(4, 0),
        cells(&[("room", Value::Uuid(id(1, 1).0))]),
        Default::default(),
    ))
    .unwrap();
    block_on(server.update(
        "notes",
        id(4, 0),
        cells(&[("body", Value::String("renamed".into()))]),
        Default::default(),
    ))
    .unwrap();
    let (client_side, server_side, _) = pipe();
    let _upstream = block_on(client.connect_upstream(Box::new(HoldCommits(client_side))));
    server.accept_subscriber(server_side, alice()).unwrap();
    pump(&server, &client);
    assert_eq!(
        client.pending_upload_count(),
        1,
        "the move is still pending"
    );
    assert_eq!(
        local_reactions(&client),
        [id(5, 1)],
        "reaction 1 is still the settled note's, which the room's read holds"
    );
}

#[test]
fn a_pending_write_does_not_move_support_below_the_first_hop_joined_first() {
    a_pending_write_does_not_move_support_below_the_first_hop(true);
}

#[test]
fn a_pending_write_does_not_move_support_below_the_first_hop_room_first() {
    a_pending_write_does_not_move_support_below_the_first_hop(false);
}

/// The stickers `db` holds, read locally.
fn local_stickers(db: &Db) -> Vec<RowUuid> {
    let prepared = db.prepare_query(&Query::from("stickers")).unwrap();
    let opts = ReadOpts {
        tier: ReadTier::LocalOnly,
        ..ReadOpts::default()
    };
    let mut rows: Vec<RowUuid> = block_on(db.all(&prepared, opts))
        .unwrap()
        .iter()
        .map(|row| row.row_uuid())
        .collect();
    rows.sort();
    rows
}

/// A support hop below the first follows the value the server settled: a
/// pending write that empties a note's stickers does not stop room 0's
/// read from holding the sticker the settled note names, when the read
/// that held it too loses it.
fn a_pending_write_does_not_drop_a_sticker_the_settled_note_names(liked_first: bool) {
    use jazz::query::{ArraySubquery, col, eq, lit};
    let server = seeded(1);
    let seed = |table: &str, row: RowUuid, values: BTreeMap<String, Value>| {
        server
            .seed_settled_mergeable_for_bootstrap(table, row, AuthorSubject::SYSTEM, values)
            .unwrap();
    };
    seed(
        "stickers",
        id(6, 0),
        cells(&[("body", Value::String("s0".into()))]),
    );
    seed(
        "notes",
        id(4, 0),
        cells(&[
            ("room", Value::Uuid(id(1, 0).0)),
            ("body", Value::String("n0".into())),
            ("stickers", Value::Array(vec![Value::Uuid(id(6, 0).0)])),
        ]),
    );
    seed(
        "likes",
        id(7, 0),
        cells(&[("sticker", Value::Uuid(id(6, 0).0))]),
    );
    let client = open(1, alice());
    let (client_side, server_side, _) = pipe();
    let upstream = block_on(client.connect_upstream(client_side));
    server.accept_subscriber(server_side, alice()).unwrap();
    // Liked stickers, and room 0 with its notes' stickers.
    let liked = client
        .prepare_query(&Query::from("stickers").join_via("likes", "sticker", []))
        .unwrap();
    let mut notes = ArraySubquery::new("notes", "notes", "room", "id");
    notes
        .nested_arrays
        .push(ArraySubquery::new("stickers", "stickers", "id", "stickers"));
    let room = client
        .prepare_query(
            &Query::from("rooms")
                .filter(eq(col("name"), lit(Value::String("room 0".into()))))
                .array_subquery(notes),
        )
        .unwrap();
    let (first, second) = if liked_first {
        (&liked, &room)
    } else {
        (&room, &liked)
    };
    let _first = block_on(client.subscribe(first, ReadOpts::default())).unwrap();
    let _second = block_on(client.subscribe(second, ReadOpts::default())).unwrap();
    pump(&server, &client);
    assert_eq!(local_stickers(&client), [id(6, 0)]);

    // Away, the note's stickers are emptied locally, and the server
    // deletes the like: the liked read loses sticker 0.
    assert!(client.detach_connection(&upstream));
    block_on(client.update(
        "notes",
        id(4, 0),
        cells(&[("stickers", Value::Array(Vec::new()))]),
        Default::default(),
    ))
    .unwrap();
    block_on(server.delete("likes", id(7, 0), Default::default())).unwrap();
    let (client_side, server_side, _) = pipe();
    let _upstream = block_on(client.connect_upstream(Box::new(HoldCommits(client_side))));
    server.accept_subscriber(server_side, alice()).unwrap();
    pump(&server, &client);
    assert_eq!(
        client.pending_upload_count(),
        1,
        "the edit is still pending"
    );
    assert_eq!(
        local_stickers(&client),
        [id(6, 0)],
        "sticker 0 is the settled note's, which the room's read holds"
    );
}

#[test]
fn a_pending_write_does_not_drop_a_sticker_the_settled_note_names_liked_first() {
    a_pending_write_does_not_drop_a_sticker_the_settled_note_names(true);
}

#[test]
fn a_pending_write_does_not_drop_a_sticker_the_settled_note_names_room_first() {
    a_pending_write_does_not_drop_a_sticker_the_settled_note_names(false);
}

/// `rooms.where({ name })` as the TS builder sends it: a filter under a
/// full-row projection.
fn rooms_named(name: &str) -> jazz::tools::public_schema::RelationExpr {
    use jazz::tools::public_schema::{
        RelationCmpOp, RelationColumnRef, RelationExpr, RelationPredicate, RelationProjectColumn,
        RelationProjectExpr, RelationRowIdRef, RelationValueRef,
    };
    let column = |column: &str| RelationColumnRef {
        scope: Some("rooms".to_owned()),
        column: column.to_owned(),
    };
    RelationExpr::Project {
        input: Box::new(RelationExpr::Filter {
            input: Box::new(RelationExpr::TableScan {
                table: "rooms".to_owned(),
                alias: None,
            }),
            predicate: RelationPredicate::Cmp {
                left: column("name"),
                op: RelationCmpOp::Eq,
                right: RelationValueRef::literal(jazz::tools::Value::Text(name.to_owned())),
            },
        }),
        columns: ["id", "name"]
            .iter()
            .map(|alias| RelationProjectColumn {
                alias: (*alias).to_owned(),
                expr: if *alias == "id" {
                    RelationProjectExpr::RowId(RelationRowIdRef::Current)
                } else {
                    RelationProjectExpr::Column(column(alias))
                },
            })
            .collect(),
    }
}

/// A row a union holds through one of its arms is the union's as much as
/// a plain read's: another read's listing that leaves it out does not
/// drop it, and the union's resumed opening, which ships only what
/// changed, finds it still there.
fn a_row_a_union_holds_stays_when_another_read_loses_it(lost_first: bool) {
    use jazz::query::RelationQuery;
    use jazz::tools::public_schema::{RelationExpr, RelationUnionArm};
    let server = seeded(1);
    let client = open(1, alice());
    let (client_side, server_side, _) = pipe();
    let upstream = block_on(client.connect_upstream(client_side));
    server.accept_subscriber(server_side, alice()).unwrap();
    // Rooms alice is a member of, and room 0 or room 1 by name.
    let member_of = client
        .prepare_query(&Query::from("rooms").join_via("memberships", "room", []))
        .unwrap();
    let mut named = Query::from("rooms");
    named.relation = Some(RelationQuery {
        rel: RelationExpr::Union {
            inputs: vec![
                RelationUnionArm {
                    label: "first".to_owned(),
                    input: rooms_named("room 0"),
                },
                RelationUnionArm {
                    label: "second".to_owned(),
                    input: rooms_named("room 1"),
                },
            ],
        },
    });
    let named = client.prepare_query(&named).unwrap();
    let (first, second) = if lost_first {
        (&member_of, &named)
    } else {
        (&named, &member_of)
    };
    let _first = block_on(client.subscribe(first, ReadOpts::default())).unwrap();
    let _second = block_on(client.subscribe(second, ReadOpts::default())).unwrap();
    pump(&server, &client);
    assert_eq!(local_rooms(&client), [id(1, 0), id(1, 1)]);

    // Away, alice leaves room 0: the union still names it, unchanged.
    assert!(client.detach_connection(&upstream));
    block_on(server.delete("memberships", id(2, 0), Default::default())).unwrap();
    let (client_side, server_side, _) = pipe();
    let _upstream = block_on(client.connect_upstream(client_side));
    server.accept_subscriber(server_side, alice()).unwrap();
    pump(&server, &client);
    assert_eq!(
        local_rooms(&client),
        [id(1, 0), id(1, 1)],
        "room 0 is still the union's"
    );
}

#[test]
fn a_row_a_union_holds_stays_when_the_losing_read_reopens_first() {
    a_row_a_union_holds_stays_when_another_read_loses_it(true);
}

#[test]
fn a_row_a_union_holds_stays_when_the_union_reopens_first() {
    a_row_a_union_holds_stays_when_another_read_loses_it(false);
}

/// A window over the readable room's messages: `offset` rows skipped,
/// `limit` kept, by body.
fn window(offset: usize, limit: usize) -> Query {
    Query::from("messages")
        .order_by("body", OrderDirection::Asc)
        .offset(offset)
        .limit(limit)
}

/// A window's rows as `db` lists them, in order: its own store's
/// (`identity` None), or what the server lists for `identity`.
fn window_ids(db: &Db, query: &Query, identity: Option<AuthorSubject>) -> Vec<RowUuid> {
    let prepared = db.prepare_query(query).unwrap();
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
    rows.iter().map(|row| row.row_uuid()).collect()
}

fn reconnect(server: &Db, client: &Db) -> (jazz::db::Upstream, Rc<RefCell<usize>>) {
    let (upstream, shipped, _) = reconnect_watched(server, client);
    (upstream, shipped)
}

/// Reconnect, also saying whether every read the client sent resumed.
fn reconnect_watched(server: &Db, client: &Db) -> (jazz::db::Upstream, Rc<RefCell<usize>>, bool) {
    let (client_side, server_side, shipped, resumes) = watched_pipe();
    let upstream = block_on(client.connect_upstream(client_side));
    server.accept_subscriber(server_side, alice()).unwrap();
    pump(server, client);
    let resumed = {
        let resumes = resumes.borrow();
        !resumes.is_empty() && resumes.iter().all(|&resumed| resumed)
    };
    (upstream, shipped, resumed)
}

/// A caught-up window resumes without shipping a row, as an unlimited
/// read does: nothing it depends on was written since. A window past an
/// offset too (its upstream read is the prefix through the window).
fn a_caught_up_window_resumes_without_shipping_rows(offset: usize, limit: usize) {
    let server = seeded(50);
    let client = open(1, alice());
    let (client_side, server_side, shipped) = pipe();
    let upstream = block_on(client.connect_upstream(client_side));
    server.accept_subscriber(server_side, alice()).unwrap();
    let query = window(offset, limit);
    let prepared = client.prepare_query(&query).unwrap();
    let _stream = block_on(client.subscribe(&prepared, ReadOpts::default())).unwrap();
    pump(&server, &client);
    assert!(*shipped.borrow() > 0, "the first opening ships the window");
    assert_eq!(window_ids(&client, &query, None).len(), limit);
    assert!(client.detach_connection(&upstream));
    // A write to a table the read does not depend on.
    block_on(server.update(
        "rooms",
        id(1, 1),
        cells(&[("name", Value::String("renamed".into()))]),
        Default::default(),
    ))
    .unwrap();
    let (_upstream, shipped) = reconnect(&server, &client);
    assert_eq!(*shipped.borrow(), 0, "a caught-up window ships nothing");
    assert_eq!(
        window_ids(&client, &query, None),
        window_ids(&server, &query, Some(alice()))
    );
}

#[test]
fn a_caught_up_limited_subscription_resumes_without_shipping_rows() {
    a_caught_up_window_resumes_without_shipping_rows(0, 20);
}

#[test]
fn a_caught_up_offset_window_resumes_without_shipping_rows() {
    a_caught_up_window_resumes_without_shipping_rows(10, 5);
}

/// While away, rows enter and leave a window without changing. A window
/// past an offset reads the prefix through its end, as its replica asks
/// for it: `offset 1 limit 4` is the first five rows by body.
///
/// ```text
/// seeded:  m0 m10 m12 m14 m16 | m18 m2 ...
/// away 1:  + a, + b, - m10     ─► a b m0 m12 m14 | m16 ...  (m16 pushed out)
/// away 2:  - a, - b            ─► m0 m12 m14 m16 m18        (m16, m18 let in)
/// ```
///
/// Each resumed opening ships only the rows that entered (changed or
/// not), the deletions, and the membership a shipped row rests on. The
/// rows still in the window stay where they are, and the one pushed out
/// is dropped from the replica's window.
#[test]
fn a_resumed_window_ships_only_rows_that_entered_it() {
    let server = seeded(10);
    let insert = |n: u32, body: &str| {
        server
            .seed_settled_mergeable_for_bootstrap(
                "messages",
                id(3, n),
                AuthorSubject::SYSTEM,
                cells(&[
                    ("room", Value::Uuid(id(1, 0).0)),
                    ("body", Value::String(body.into())),
                ]),
            )
            .unwrap();
    };
    let client = open(1, alice());
    let (client_side, server_side, _) = pipe();
    let upstream = block_on(client.connect_upstream(client_side));
    server.accept_subscriber(server_side, alice()).unwrap();
    let query = window(1, 4);
    let prepared = client.prepare_query(&query).unwrap();
    let _stream = block_on(client.subscribe(&prepared, ReadOpts::default())).unwrap();
    pump(&server, &client);
    assert_eq!(
        window_ids(&client, &query, None),
        [id(3, 10), id(3, 12), id(3, 14), id(3, 16)]
    );

    assert!(client.detach_connection(&upstream));
    insert(5000, "a");
    insert(5001, "b");
    block_on(server.delete("messages", id(3, 10), Default::default())).unwrap();
    let (upstream, shipped) = reconnect(&server, &client);
    assert_eq!(
        window_ids(&client, &query, None),
        [id(3, 5001), id(3, 0), id(3, 12), id(3, 14)]
    );
    assert_eq!(
        window_ids(&client, &query, None),
        window_ids(&server, &query, Some(alice()))
    );
    assert_eq!(
        *shipped.borrow(),
        4,
        "a, b, m10's deletion and the membership ship; m0, m12, m14 stay"
    );

    assert!(client.detach_connection(&upstream));
    block_on(server.delete("messages", id(3, 5000), Default::default())).unwrap();
    block_on(server.delete("messages", id(3, 5001), Default::default())).unwrap();
    let (_upstream, shipped) = reconnect(&server, &client);
    assert_eq!(
        window_ids(&client, &query, None),
        [id(3, 12), id(3, 14), id(3, 16), id(3, 18)]
    );
    assert_eq!(
        window_ids(&client, &query, None),
        window_ids(&server, &query, Some(alice()))
    );
    assert_eq!(
        *shipped.borrow(),
        5,
        "the two deletions, m16 and m18 (entering unchanged) and the membership ship"
    );
}

/// Differential: a client holds three windows (ascending, past an offset,
/// and newest first over a filter) while, between reconnects, the server
/// inserts rows, relabels them across the windows' edges, moves them
/// between rooms, deletes them, and gives and takes alice's access to a
/// second room. After each resume the client's
/// windows equal the server's and a fresh client's; most reconnects
/// resume, a resume ships no more than a fresh opening (nothing at all
/// when nothing changed), and in all they ship less than half as much.
#[test]
fn resumed_windows_match_fresh_ones_through_random_churn() {
    let mut resumed_total = 0usize;
    let mut fresh_total = 0usize;
    let mut resumed_rounds = 0usize;
    let mut rounds = 0usize;
    for seed_value in 1..=16u64 {
        let mut state = seed_value.wrapping_mul(0x9e37_79b9_7f4a_7c15) | 1;
        let mut next = move |bound: u64| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state % bound
        };
        let server = seeded(16);
        // Ascending, past an offset, and newest first over one room only
        // (a filter whose rows a move between rooms takes in or out).
        let queries = [
            window(0, 5),
            window(3, 4),
            Query::from("messages")
                .filter(eq(col("room"), lit(Value::Uuid(id(1, 1).0))))
                .order_by("body", OrderDirection::Desc)
                .limit(4),
        ];
        let client = open(1, alice());
        let (client_side, server_side, _) = pipe();
        let mut upstream = block_on(client.connect_upstream(client_side));
        server.accept_subscriber(server_side, alice()).unwrap();
        let _streams: Vec<_> = queries
            .iter()
            .map(|query| {
                let prepared = client.prepare_query(query).unwrap();
                block_on(client.subscribe(&prepared, ReadOpts::default())).unwrap()
            })
            .collect();
        pump(&server, &client);
        let mut rows: Vec<RowUuid> = (0..32).map(|n| id(3, n)).collect();
        let mut next_row = 1000u32;
        let mut membership: Option<RowUuid> = None;
        let mut next_membership = 10u32;
        for round in 0..12 {
            assert!(client.detach_connection(&upstream));
            let changes = next(6);
            for _ in 0..changes {
                match next(5) {
                    0 => {
                        let row = id(3, next_row);
                        next_row += 1;
                        server
                            .seed_settled_mergeable_for_bootstrap(
                                "messages",
                                row,
                                AuthorSubject::SYSTEM,
                                cells(&[
                                    ("room", Value::Uuid(id(1, next(2) as u32).0)),
                                    ("body", Value::String(format!("m{}", next(40)))),
                                ]),
                            )
                            .unwrap();
                        rows.push(row);
                    }
                    1 if rows.len() > 4 => {
                        let row = rows.remove(next(rows.len() as u64) as usize);
                        block_on(server.delete("messages", row, Default::default())).unwrap();
                    }
                    2 => match membership.take() {
                        Some(row) => {
                            block_on(server.delete("memberships", row, Default::default()))
                                .unwrap();
                        }
                        None => {
                            let row = id(2, next_membership);
                            next_membership += 1;
                            server
                                .seed_settled_mergeable_for_bootstrap(
                                    "memberships",
                                    row,
                                    AuthorSubject::SYSTEM,
                                    cells(&[
                                        ("room", Value::Uuid(id(1, 1).0)),
                                        ("member", Value::String(subject())),
                                    ]),
                                )
                                .unwrap();
                            membership = Some(row);
                        }
                    },
                    3 => {
                        // A move between rooms: in or out of the filter,
                        // and of alice's reach while she lacks room 1.
                        let row = rows[next(rows.len() as u64) as usize];
                        block_on(server.update(
                            "messages",
                            row,
                            cells(&[("room", Value::Uuid(id(1, next(2) as u32).0))]),
                            Default::default(),
                        ))
                        .unwrap();
                    }
                    _ => {
                        let row = rows[next(rows.len() as u64) as usize];
                        block_on(server.update(
                            "messages",
                            row,
                            cells(&[("body", Value::String(format!("m{}", next(40))))]),
                            Default::default(),
                        ))
                        .unwrap();
                    }
                }
            }
            let (now, shipped, all_resumed) = reconnect_watched(&server, &client);
            upstream = now;
            let resumed = *shipped.borrow();
            resumed_rounds += usize::from(all_resumed);
            rounds += 1;

            let fresh = open(10 + round as u8, alice());
            let (fresh_side, server_side, fresh_shipped) = pipe();
            let _fresh_upstream = block_on(fresh.connect_upstream(fresh_side));
            server.accept_subscriber(server_side, alice()).unwrap();
            let _fresh_streams: Vec<_> = queries
                .iter()
                .map(|query| {
                    let prepared = fresh.prepare_query(query).unwrap();
                    block_on(fresh.subscribe(&prepared, ReadOpts::default())).unwrap()
                })
                .collect();
            pump(&server, &fresh);
            let fresh_count = *fresh_shipped.borrow();

            for query in &queries {
                let expected = window_ids(&server, query, Some(alice()));
                assert_eq!(
                    window_ids(&client, query, None),
                    expected,
                    "seed {seed_value} round {round}: the resumed window is the server's"
                );
                assert_eq!(
                    window_ids(&fresh, query, None),
                    expected,
                    "seed {seed_value} round {round}: a fresh window is the server's"
                );
            }
            // A resume after losing access to a held row opens fresh: the
            // replica forgets its watermarks then (SPEC 8.11).
            if changes == 0 && all_resumed {
                assert_eq!(
                    resumed, 0,
                    "seed {seed_value} round {round}: nothing changed, nothing ships"
                );
            }
            assert!(
                resumed <= fresh_count,
                "seed {seed_value} round {round}: a resume ships {resumed}, a fresh opening {fresh_count}"
            );
            resumed_total += resumed;
            fresh_total += fresh_count;
        }
    }
    assert!(
        resumed_rounds * 3 > rounds * 2,
        "only {resumed_rounds} of {rounds} reconnects resumed their windows"
    );
    assert!(
        resumed_total * 2 < fresh_total,
        "resumes ship {resumed_total} rows against {fresh_total} for fresh openings"
    );
}

/// A message in the readable room, seeded on the server as settled.
fn seed_message(server: &Db, n: u32, body: &str) {
    server
        .seed_settled_mergeable_for_bootstrap(
            "messages",
            id(3, n),
            AuthorSubject::SYSTEM,
            cells(&[
                ("room", Value::Uuid(id(1, 0).0)),
                ("body", Value::String(body.into())),
            ]),
        )
        .unwrap();
}

/// A "latest N" window (newest first) that new rows push its oldest out of
/// while connected still resumes caught up: following the window live
/// forgets no watermark, so the next reconnect ships nothing, and one more
/// row while away ships alone (with the membership it rests on).
#[test]
fn a_latest_window_resumes_after_live_churn() {
    let server = seeded(10);
    let client = open(1, alice());
    let (client_side, server_side, _) = pipe();
    let upstream = block_on(client.connect_upstream(client_side));
    server.accept_subscriber(server_side, alice()).unwrap();
    let query = Query::from("messages")
        .order_by("body", OrderDirection::Desc)
        .limit(4);
    let prepared = client.prepare_query(&query).unwrap();
    let _stream = block_on(client.subscribe(&prepared, ReadOpts::default())).unwrap();
    pump(&server, &client);
    for (n, body) in [(5000, "z1"), (5001, "z2"), (5002, "z3")] {
        seed_message(&server, n, body);
        pump(&server, &client);
        assert_eq!(
            window_ids(&client, &query, None),
            window_ids(&server, &query, Some(alice()))
        );
    }

    assert!(client.detach_connection(&upstream));
    let (upstream, shipped, resumed) = reconnect_watched(&server, &client);
    assert!(resumed, "live churn left the window's watermark in place");
    assert_eq!(*shipped.borrow(), 0, "a caught-up window ships nothing");
    assert_eq!(
        window_ids(&client, &query, None),
        [id(3, 5002), id(3, 5001), id(3, 5000), id(3, 8)]
    );

    assert!(client.detach_connection(&upstream));
    seed_message(&server, 5003, "z4");
    let (_upstream, shipped, resumed) = reconnect_watched(&server, &client);
    assert!(resumed);
    assert_eq!(
        window_ids(&client, &query, None),
        [id(3, 5003), id(3, 5002), id(3, 5001), id(3, 5000)]
    );
    assert_eq!(*shipped.borrow(), 2, "the new row and its membership ship");
}

/// Ties at a window's edge break by row id, on the server and the replica
/// alike: when a tied row inside the window is deleted while away, the
/// next tied row by id enters unchanged and ships, and the replica's
/// window is the server's.
#[test]
fn a_resumed_window_breaks_ties_at_its_edge_by_row_id() {
    let server = seeded(2);
    // Five rows tied on "t", after m0 and m2 by body.
    for n in 100..105 {
        seed_message(&server, n, "t");
    }
    let client = open(1, alice());
    let (client_side, server_side, _) = pipe();
    let upstream = block_on(client.connect_upstream(client_side));
    server.accept_subscriber(server_side, alice()).unwrap();
    // m0, m2, then the first two tied rows: the edge cuts the tie.
    let query = window(1, 3);
    let prepared = client.prepare_query(&query).unwrap();
    let _stream = block_on(client.subscribe(&prepared, ReadOpts::default())).unwrap();
    pump(&server, &client);
    assert_eq!(
        window_ids(&server, &query, Some(alice())),
        [id(3, 2), id(3, 100), id(3, 101)]
    );
    assert_eq!(
        window_ids(&client, &query, None),
        window_ids(&server, &query, Some(alice()))
    );

    assert!(client.detach_connection(&upstream));
    block_on(server.delete("messages", id(3, 100), Default::default())).unwrap();
    let (_upstream, shipped, resumed) = reconnect_watched(&server, &client);
    assert!(resumed);
    assert_eq!(
        window_ids(&client, &query, None),
        [id(3, 2), id(3, 101), id(3, 102)]
    );
    assert_eq!(
        window_ids(&client, &query, None),
        window_ids(&server, &query, Some(alice()))
    );
    assert_eq!(
        *shipped.borrow(),
        3,
        "the deletion, the tied row entering and the membership ship"
    );
}

/// A one-shot read of a window resumes too: repeated unchanged it ships
/// nothing, and after a reconnect it ships only the row that entered,
/// then answers as the server does.
#[test]
fn a_one_shot_window_read_resumes_after_reconnect() {
    let server = seeded(10);
    let client = open(1, alice());
    let (up, down, shipped) = pipe();
    let connection = block_on(client.connect_upstream(up));
    server.accept_subscriber(down, alice()).unwrap();
    let query = window(2, 3);
    let expected = window_ids(&server, &query, Some(alice()));
    assert_eq!(read_query_once(&[&server], &client, &query), expected);
    let before = *shipped.borrow();
    assert_eq!(read_query_once(&[&server], &client, &query), expected);
    assert_eq!(
        *shipped.borrow(),
        before,
        "an unchanged window is not resent"
    );

    assert!(client.detach_connection(&connection));
    seed_message(&server, 5000, "a");
    let (up, down, shipped) = pipe();
    let _connection = block_on(client.connect_upstream(up));
    server.accept_subscriber(down, alice()).unwrap();
    let expected = window_ids(&server, &query, Some(alice()));
    assert_eq!(read_query_once(&[&server], &client, &query), expected);
    assert_eq!(
        *shipped.borrow(),
        2,
        "the row that entered the prefix and its membership ship"
    );
}

/// A write the replica made while away, sorting into its window, is the
/// replica's at once and the server's once uploaded: the resumed window
/// lists it, and the replica's window ends on the server's.
#[test]
fn a_resumed_window_keeps_a_pending_write_made_while_away() {
    let server = seeded(10);
    let client = open(1, alice());
    let (client_side, server_side, _) = pipe();
    let upstream = block_on(client.connect_upstream(client_side));
    server.accept_subscriber(server_side, alice()).unwrap();
    let query = window(1, 4);
    let prepared = client.prepare_query(&query).unwrap();
    let _stream = block_on(client.subscribe(&prepared, ReadOpts::default())).unwrap();
    pump(&server, &client);

    assert!(client.detach_connection(&upstream));
    block_on(client.insert(
        "messages",
        cells(&[
            ("room", Value::Uuid(id(1, 0).0)),
            ("body", Value::String("m1".into())),
        ]),
        Default::default(),
    ))
    .unwrap();
    let local = window_ids(&client, &query, None);
    assert_eq!(local.len(), 4);
    let written = local[0];
    assert!(
        !(0..20).any(|n| id(3, n) == written),
        "the pending row leads"
    );

    let (_upstream, _, resumed) = reconnect_watched(&server, &client);
    assert!(resumed);
    pump(&server, &client);
    let on_server = window_ids(&server, &query, Some(alice()));
    assert_eq!(on_server[0], written, "the upload reached the server");
    assert_eq!(window_ids(&client, &query, None), on_server);
}

/// Timing probe (run with `--release -- --nocapture`, `ROWS` per room;
/// the default 1,000 keeps it cheap in the suite): a 20-row window whose table took one row while
/// its client was away, resumed and opened fresh. `body` has no index, so
/// the resume's read of the window as of `since` scans the table, as any
/// SQL read of this window does; the fresh opening here reuses the window
/// the first client keeps live on the server. With the sort column indexed
/// the as-of read stays flat.
#[test]
fn window_resume_cost() {
    use std::time::Instant;
    let per_room = std::env::var("ROWS").map_or(1_000, |rows| rows.parse().unwrap());
    let server = seeded(per_room);
    let client = open(1, alice());
    let (client_side, server_side, _) = pipe();
    let upstream = block_on(client.connect_upstream(client_side));
    server.accept_subscriber(server_side, alice()).unwrap();
    let query = window(0, 20);
    let prepared = client.prepare_query(&query).unwrap();
    let _stream = block_on(client.subscribe(&prepared, ReadOpts::default())).unwrap();
    pump(&server, &client);
    assert!(client.detach_connection(&upstream));
    seed_message(&server, 900_000, "a");

    let started = Instant::now();
    let (_upstream, shipped, resumed) = reconnect_watched(&server, &client);
    assert!(resumed);
    eprintln!(
        "resumed: {:?}, {} images",
        started.elapsed(),
        shipped.borrow()
    );

    let fresh = open(9, alice());
    let started = Instant::now();
    let (fresh_side, server_side, fresh_shipped) = pipe();
    let _fresh_upstream = block_on(fresh.connect_upstream(fresh_side));
    server.accept_subscriber(server_side, alice()).unwrap();
    let prepared = fresh.prepare_query(&query).unwrap();
    let _fresh_stream = block_on(fresh.subscribe(&prepared, ReadOpts::default())).unwrap();
    pump(&server, &fresh);
    eprintln!(
        "fresh: {:?}, {} images",
        started.elapsed(),
        fresh_shipped.borrow()
    );
}
