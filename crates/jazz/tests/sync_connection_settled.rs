//! What a replica knows of its upstream moves by `ConnectionSettled`
//! alone (SPEC 8 §8.4): a read's `Settled` speaks for that read and a
//! commit's `Fate` for that commit. Every upstream sends the connection's
//! position after a tick's rows, settles and fates, so an unsharded
//! server's clients run the code a sharded gateway's do. Until it arrives,
//! the replica's own commits accepted past what it knows are named by
//! transaction.
//!
//! Each live result reads as of its own read's settle (or the connection's
//! position, when further), and a one-shot read as of the connection's:
//! while the store holds others' accepted versions past it, never part of
//! a commit its upstream has not settled for it, always with this
//! replica's own writes. Here an interposer holds messages back the way a
//! sharded gateway does when one read's shards are behind another's.

use crate::common;

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::pin::pin;
use std::rc::Rc;
use std::task::{Context, Poll, Waker};

use jazz::db::{
    Db, DbConfig, DbIdentity, InsertOptions, ReadOpts, ReadTier, SubscriptionEvent,
    SubscriptionStream, SyncMessage, Transport, TransportError, Upstream, block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::model::transaction::OpenTransactionId;
use jazz::protocol::{ReadViewSourceSpec, ReadViewSpec};
use jazz::query::{ArraySubquery, Query, col, eq, lit};
use jazz::records::Value;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};

use common::{allow_all_policies, compile_schema};

fn open(node: u8, author: AuthorSubject) -> Db {
    let schema = compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("docs")
                    .column("room", ColumnType::Text)
                    .column("title", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new("comments")
                    .column("doc", ColumnType::Uuid)
                    .column("body", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .build(),
    );
    let identity = DbIdentity {
        node: NodeUuid::from_bytes([node; 16]),
        author,
    };
    block_on(Db::open(DbConfig::new(schema, identity))).unwrap()
}

fn user(n: u8) -> AuthorSubject {
    AuthorSubject::for_test_bytes([n; 16])
}

fn row(n: u8) -> RowUuid {
    RowUuid::from_bytes([n; 16])
}

fn doc(room: &str, title: &str) -> BTreeMap<String, Value> {
    BTreeMap::from([
        ("room".to_owned(), Value::String(room.to_owned())),
        ("title".to_owned(), Value::String(title.to_owned())),
    ])
}

type Hold = Rc<RefCell<Option<Box<dyn Fn(&SyncMessage) -> bool>>>>;

/// The server side of a link: logs what it sends down and holds back
/// what `hold` matches.
struct Down {
    to_client: Rc<RefCell<VecDeque<SyncMessage>>>,
    from_client: Rc<RefCell<VecDeque<SyncMessage>>>,
    log: Rc<RefCell<Vec<SyncMessage>>>,
    hold: Hold,
    held: Rc<RefCell<Vec<SyncMessage>>>,
}

impl Transport for Down {
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        self.log.borrow_mut().push(message.clone());
        if self
            .hold
            .borrow()
            .as_ref()
            .is_some_and(|hold| hold(&message))
        {
            self.held.borrow_mut().push(message);
        } else {
            self.to_client.borrow_mut().push_back(message);
        }
        Ok(())
    }

    fn try_recv(&mut self) -> Option<SyncMessage> {
        self.from_client.borrow_mut().pop_front()
    }
}

struct Up {
    to_server: Rc<RefCell<VecDeque<SyncMessage>>>,
    from_server: Rc<RefCell<VecDeque<SyncMessage>>>,
}

impl Transport for Up {
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        self.to_server.borrow_mut().push_back(message);
        Ok(())
    }

    fn try_recv(&mut self) -> Option<SyncMessage> {
        self.from_server.borrow_mut().pop_front()
    }
}

struct Link {
    _upstream: Upstream,
    to_client: Rc<RefCell<VecDeque<SyncMessage>>>,
    log: Rc<RefCell<Vec<SyncMessage>>>,
    hold: Hold,
    held: Rc<RefCell<Vec<SyncMessage>>>,
}

impl Link {
    fn hold(&self, hold: impl Fn(&SyncMessage) -> bool + 'static) {
        *self.hold.borrow_mut() = Some(Box::new(hold));
    }

    /// Hold the connection's position back.
    fn hold_position(&self) {
        self.hold(|m| matches!(m, SyncMessage::ConnectionSettled { .. }));
    }

    /// Let everything held through, in order.
    fn release(&self) {
        *self.hold.borrow_mut() = None;
        let held = std::mem::take(&mut *self.held.borrow_mut());
        self.to_client.borrow_mut().extend(held);
    }

    /// The upstream read id of the next read the client settles.
    fn next_read(&self, settle: impl FnOnce()) -> u64 {
        let from = self.log.borrow().len();
        settle();
        self.log.borrow()[from..]
            .iter()
            .find_map(|m| match m {
                SyncMessage::Settled { id, .. } => Some(*id),
                _ => None,
            })
            .expect("the read settled")
    }
}

fn connect(server: &Db, client: &Db, identity: AuthorSubject) -> Link {
    let up = Rc::new(RefCell::new(VecDeque::new()));
    let down = Rc::new(RefCell::new(VecDeque::new()));
    let log = Rc::new(RefCell::new(Vec::new()));
    let hold: Hold = Rc::new(RefCell::new(None));
    let held = Rc::new(RefCell::new(Vec::new()));
    let upstream = block_on(client.connect_upstream(Box::new(Up {
        to_server: up.clone(),
        from_server: down.clone(),
    })));
    server
        .accept_subscriber(
            Box::new(Down {
                to_client: down.clone(),
                from_client: up,
                log: log.clone(),
                hold: hold.clone(),
                held: held.clone(),
            }),
            identity,
        )
        .unwrap();
    Link {
        _upstream: upstream,
        to_client: down,
        log,
        hold,
        held,
    }
}

fn pump(server: &Db, client: &Db) {
    for _ in 0..6 {
        block_on(client.tick()).unwrap();
        block_on(server.tick()).unwrap();
    }
    block_on(client.tick()).unwrap();
}

fn insert(db: &Db, n: u8, room: &str, title: &str) {
    block_on(db.insert(
        "docs",
        doc(room, title),
        InsertOptions {
            row_id: Some(row(n)),
            ..Default::default()
        },
    ))
    .unwrap();
}

/// One commit writing a doc in each of two rooms, as a cross-shard one
/// would.
fn insert_both(db: &Db, a: (u8, &str), b: (u8, &str)) {
    let tx = OpenTransactionId::new();
    block_on(db.begin_mergeable(tx)).unwrap();
    for (n, room, title) in [(a.0, "a", a.1), (b.0, "b", b.1)] {
        block_on(db.tx_ref(tx).insert(
            "docs",
            doc(room, title),
            InsertOptions {
                row_id: Some(row(n)),
                ..Default::default()
            },
        ))
        .unwrap();
    }
    block_on(db.commit_mergeable_handle(tx)).unwrap();
}

/// A live result of one room's docs, as the titles it shows.
struct Room {
    stream: SubscriptionStream,
    rows: BTreeMap<RowUuid, String>,
}

impl Room {
    fn open(client: &Db, room: &str) -> Self {
        let query = Query::from("docs").filter(eq(col("room"), lit(Value::String(room.into()))));
        let prepared = client.prepare_query(&query).unwrap();
        let stream = block_on(client.subscribe(&prepared, ReadOpts::default())).unwrap();
        Self {
            stream,
            rows: BTreeMap::new(),
        }
    }

    fn titles(&mut self) -> BTreeSet<String> {
        while let Some(event) = self.stream.try_next_event() {
            let SubscriptionEvent::Delta {
                reset,
                added,
                updated,
                removed,
                ..
            } = event
            else {
                continue;
            };
            if reset {
                self.rows.clear();
            }
            for gone in removed {
                self.rows.remove(&gone.row_uuid);
            }
            for row in added.into_iter().chain(updated) {
                let title = match row.row.cell("title") {
                    Some(Value::String(title)) => title,
                    other => panic!("title {other:?}"),
                };
                self.rows.insert(row.row.row_uuid(), title);
            }
        }
        self.rows.values().cloned().collect()
    }
}

fn titles(list: &[&str]) -> BTreeSet<String> {
    list.iter().map(|t| (*t).to_owned()).collect()
}

/// Every doc's title, read once as the replica's own store holds them.
fn local_titles(client: &Db) -> BTreeSet<String> {
    let prepared = client.prepare_query(&Query::from("docs")).unwrap();
    let opts = ReadOpts {
        tier: ReadTier::LocalOnly,
        ..ReadOpts::default()
    };
    block_on(client.all(&prepared, opts))
        .unwrap()
        .iter()
        .map(|row| match row.cell("title") {
            Some(Value::String(title)) => title,
            other => panic!("title {other:?}"),
        })
        .collect()
}

#[test]
fn an_unsharded_server_sends_the_connection_position_after_its_settles() {
    let server = open(0, AuthorSubject::SYSTEM);
    let client = open(1, user(1));
    let link = connect(&server, &client, user(1));
    let mut room = Room::open(&client, "a");
    pump(&server, &client);
    insert(&server, 1, "a", "from the server");
    link.log.borrow_mut().clear();
    pump(&server, &client);
    assert_eq!(room.titles(), titles(&["from the server"]));
    let log = link.log.borrow();
    let settled = log
        .iter()
        .rposition(|m| matches!(m, SyncMessage::Settled { .. }))
        .expect("the read settled the write");
    let Some(SyncMessage::ConnectionSettled { through }) = log.get(settled + 1) else {
        panic!("no connection position after the settle: {log:?}");
    };
    let Some(SyncMessage::Settled { through: read, .. }) = log.get(settled) else {
        unreachable!();
    };
    assert_eq!(through, read, "unsharded, both name the server's seq");
    // A tick with nothing to deliver sends nothing.
    drop(log);
    link.log.borrow_mut().clear();
    pump(&server, &client);
    assert!(link.log.borrow().is_empty(), "{:?}", link.log.borrow());
}

#[test]
fn a_settle_alone_leaves_what_the_replica_knows() {
    let server = open(0, AuthorSubject::SYSTEM);
    let client = open(1, user(1));
    let link = connect(&server, &client, user(1));
    let _room = Room::open(&client, "a");
    pump(&server, &client);
    let before = block_on(client.capture_snapshot()).unwrap();
    link.hold_position();
    insert(&server, 1, "a", "from the server");
    pump(&server, &client);
    let held = block_on(client.capture_snapshot()).unwrap();
    assert_eq!(
        held.global_base, before.global_base,
        "a read's settle is not the connection's position"
    );
    link.release();
    pump(&server, &client);
    let after = block_on(client.capture_snapshot()).unwrap();
    assert!(after.global_base > before.global_base);
}

#[test]
fn own_commits_accepted_past_the_known_position_are_named_by_transaction() {
    let server = open(0, AuthorSubject::SYSTEM);
    let client = open(1, user(1));
    let link = connect(&server, &client, user(1));
    let _room = Room::open(&client, "a");
    pump(&server, &client);
    link.hold_position();
    insert(&client, 1, "a", "mine");
    pump(&server, &client);
    let held = block_on(client.capture_snapshot()).unwrap();
    assert_eq!(
        held.dots.len(),
        1,
        "accepted, but past what the replica knows: named by tx"
    );
    // The authority resolves the snapshot as the client reads it.
    let at = |db: &Db| {
        let prepared = db.prepare_query(&Query::from("docs")).unwrap();
        let opts = ReadOpts {
            read_view: ReadViewSpec {
                source: ReadViewSourceSpec::Snapshot {
                    snapshot: held.clone(),
                },
                include_deleted: false,
            },
            ..ReadOpts::default()
        };
        block_on(db.all(&prepared, opts)).unwrap().len()
    };
    assert_eq!((at(&client), at(&server)), (1, 1));
    link.release();
    pump(&server, &client);
    let after = block_on(client.capture_snapshot()).unwrap();
    assert!(after.dots.is_empty(), "the connection's position covers it");
    assert!(after.global_base > held.global_base);
}

/// Room b's read is settled past a commit to both rooms; room a's is not,
/// nor is the connection. Each shows the commit only once its own read is
/// settled through it: never half of it.
#[test]
fn each_live_result_reads_as_of_its_own_settle() {
    let server = open(0, AuthorSubject::SYSTEM);
    let client = open(1, user(1));
    let link = connect(&server, &client, user(1));
    let mut a = Room::open(&client, "a");
    let read_a = link.next_read(|| pump(&server, &client));
    let mut b = Room::open(&client, "b");
    pump(&server, &client);
    insert(&server, 1, "a", "a before");
    pump(&server, &client);
    assert_eq!(a.titles(), titles(&["a before"]));

    link.hold(move |m| match m {
        SyncMessage::Settled { id, .. } => *id == read_a,
        SyncMessage::ConnectionSettled { .. } => true,
        _ => false,
    });
    insert_both(&server, (2, "a both"), (3, "b both"));
    pump(&server, &client);
    assert_eq!(
        b.titles(),
        titles(&["b both"]),
        "b's read is settled past it"
    );
    assert_eq!(
        a.titles(),
        titles(&["a before"]),
        "a's read is not: its rows arrived, but show as of its settle"
    );
    assert_eq!(
        local_titles(&client),
        titles(&["a before"]),
        "a one-shot read reads as of the connection's position"
    );

    link.release();
    pump(&server, &client);
    assert_eq!(a.titles(), titles(&["a before", "a both"]));
    assert_eq!(
        local_titles(&client),
        titles(&["a before", "a both", "b both"])
    );
}

/// A result reading as of an earlier position still shows this replica's
/// own writes: pending, then accepted past the position.
#[test]
fn own_writes_show_through_a_lagging_position() {
    let server = open(0, AuthorSubject::SYSTEM);
    let client = open(1, user(1));
    let link = connect(&server, &client, user(1));
    let mut a = Room::open(&client, "a");
    let read_a = link.next_read(|| pump(&server, &client));
    link.hold(move |m| match m {
        SyncMessage::Settled { id, .. } => *id == read_a,
        SyncMessage::ConnectionSettled { .. } => true,
        _ => false,
    });
    // Others' versions past the position put the result through a cut.
    insert(&server, 1, "a", "theirs");
    pump(&server, &client);
    assert_eq!(a.titles(), titles(&[]));

    insert(&client, 2, "a", "mine");
    assert_eq!(a.titles(), titles(&["mine"]), "pending");
    pump(&server, &client);
    assert_eq!(a.titles(), titles(&["mine"]), "accepted past the position");
    assert_eq!(local_titles(&client), titles(&["mine"]));

    link.release();
    pump(&server, &client);
    assert_eq!(a.titles(), titles(&["mine", "theirs"]));
}

/// A row that arrives before its read's settle and the connection's
/// position, which follow in a later drain (one server tick can reach the
/// client across several), is hidden until they arrive, then shown.
#[test]
fn a_row_shown_once_its_settle_arrives_in_a_later_drain() {
    let server = open(0, AuthorSubject::SYSTEM);
    let client = open(1, user(1));
    let link = connect(&server, &client, user(1));
    let mut a = Room::open(&client, "a");
    let read_a = link.next_read(|| pump(&server, &client));
    assert_eq!(a.titles(), titles(&[]));
    link.hold(move |m| match m {
        SyncMessage::Settled { id, .. } => *id == read_a,
        SyncMessage::ConnectionSettled { .. } => true,
        _ => false,
    });
    insert(&server, 1, "a", "theirs");
    pump(&server, &client);
    assert_eq!(a.titles(), titles(&[]), "its settle has not arrived");
    link.release();
    pump(&server, &client);
    assert_eq!(a.titles(), titles(&["theirs"]), "the live result shows it");
}

/// Drive `future` while ticking both stores.
fn drive<T>(server: &Db, client: &Db, future: impl Future<Output = T>) -> T {
    let mut future = pin!(future);
    let mut context = Context::from_waker(Waker::noop());
    for _ in 0..100 {
        if let Poll::Ready(output) = future.as_mut().poll(&mut context) {
            return output;
        }
        block_on(client.tick()).unwrap();
        block_on(server.tick()).unwrap();
    }
    panic!("did not complete");
}

/// A one-shot read its upstream answered shows what it fetched, though
/// the connection's position has not reached it yet.
#[test]
fn a_one_shot_read_shows_the_rows_it_fetched() {
    let server = open(0, AuthorSubject::SYSTEM);
    let client = open(1, user(1));
    let link = connect(&server, &client, user(1));
    pump(&server, &client);
    link.hold_position();
    insert(&server, 1, "a", "theirs");
    let query = postcard::to_allocvec(&Query::from("docs")).unwrap();
    let opts = ReadOpts {
        tier: ReadTier::Remote,
        ..ReadOpts::default()
    };
    let read = client.all_serialized_query(&query, opts, None, None, None);
    let rows = match drive(&server, &client, read).unwrap() {
        jazz::db::SerializedReadResult::Rows(rows) => rows,
        other => panic!("{other:?}"),
    };
    assert_eq!(rows.len(), 1, "the fetched row shows");
    assert_eq!(
        local_titles(&client),
        titles(&[]),
        "a local read reads as of the connection's position"
    );
    link.release();
    pump(&server, &client);
    assert_eq!(local_titles(&client), titles(&["theirs"]));
}

/// A live result's array relations read as of the same position as its
/// rows: a related row past its read's settle shows once the settle does.
#[test]
fn related_rows_read_as_of_the_results_settle() {
    let server = open(0, AuthorSubject::SYSTEM);
    let client = open(1, user(1));
    let link = connect(&server, &client, user(1));
    let query =
        Query::from("docs").array_subquery(ArraySubquery::new("comments", "comments", "doc", "id"));
    let prepared = client.prepare_query(&query).unwrap();
    let mut stream = block_on(client.subscribe(&prepared, ReadOpts::default())).unwrap();
    let read = link.next_read(|| pump(&server, &client));
    insert(&server, 1, "a", "doc");
    pump(&server, &client);
    let mut comments = BTreeMap::new();
    let mut count = |stream: &mut SubscriptionStream| {
        while let Some(event) = stream.try_next_event() {
            if let SubscriptionEvent::Delta { added, updated, .. } = event {
                for row in added.into_iter().chain(updated) {
                    let Some(Value::Array(list)) = row.row.cell("comments") else {
                        panic!("a comments array");
                    };
                    comments.insert(row.row.row_uuid(), list.len());
                }
            }
        }
        comments.get(&row(1)).copied()
    };
    assert_eq!(count(&mut stream), Some(0));

    link.hold(move |m| match m {
        SyncMessage::Settled { id, .. } => *id == read,
        SyncMessage::ConnectionSettled { .. } => true,
        _ => false,
    });
    block_on(server.insert(
        "comments",
        BTreeMap::from([
            ("doc".to_owned(), Value::Uuid(row(1).0)),
            ("body".to_owned(), Value::String("first".into())),
        ]),
        InsertOptions::default(),
    ))
    .unwrap();
    pump(&server, &client);
    // A one-shot read's relations read as of its position too.
    let local = ReadOpts {
        tier: ReadTier::LocalOnly,
        ..ReadOpts::default()
    };
    let bytes = postcard::to_allocvec(&query).unwrap();
    let nested = |opts: ReadOpts| match block_on(
        client.all_serialized_query(&bytes, opts, None, None, None),
    )
    .unwrap()
    {
        jazz::db::SerializedReadResult::Relation(snapshot) => snapshot.rows,
        jazz::db::SerializedReadResult::Rows(rows) => rows,
    };
    let docs = nested(local.clone());
    assert_eq!(
        docs[0].cell("comments"),
        Some(Value::Array(Vec::new())),
        "a one-shot read leaves the comment out until the position reaches it"
    );
    assert_eq!(
        count(&mut stream),
        Some(0),
        "the comment arrived past the result's settle"
    );
    link.release();
    pump(&server, &client);
    assert_eq!(count(&mut stream), Some(1));
    let docs = nested(local);
    assert!(
        matches!(docs[0].cell("comments"), Some(Value::Array(list)) if list.len() == 1),
        "then it shows"
    );
}
