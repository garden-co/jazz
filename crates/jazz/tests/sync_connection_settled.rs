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
use std::rc::Rc;

use jazz::db::{
    Db, DbConfig, DbIdentity, InsertOptions, ReadOpts, ReadTier, SubscriptionEvent,
    SubscriptionStream, SyncMessage, Transport, TransportError, Upstream, block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::model::transaction::OpenTransactionId;
use jazz::protocol::{ReadViewSourceSpec, ReadViewSpec};
use jazz::query::{Query, col, eq, lit};
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

fn pump3(server: &Db, relay: &Db, tab: &Db) {
    for _ in 0..8 {
        block_on(tab.tick()).unwrap();
        block_on(relay.tick()).unwrap();
        block_on(server.tick()).unwrap();
    }
    block_on(relay.tick()).unwrap();
    block_on(tab.tick()).unwrap();
}

/// A relay (a browser's SharedWorker) serves its tabs' live results as
/// its store holds them, each settled as far as its own upstream read:
/// the tab reads each as of that settle, the way a direct client does.
/// The upstream's position, arriving after the rows, reaches the tab on
/// its own.
#[test]
fn a_relay_forwards_each_reads_settle_and_the_connection_position() {
    let server = open(0, AuthorSubject::SYSTEM);
    let relay = open(1, user(1));
    let tab = open(2, user(1));
    tab.set_non_durable_client();
    let link = connect(&server, &relay, user(1));
    let _tab_link = connect(&relay, &tab, user(1));
    let mut a = Room::open(&tab, "a");
    let read_a = link.next_read(|| pump3(&server, &relay, &tab));
    let mut b = Room::open(&tab, "b");
    pump3(&server, &relay, &tab);
    insert(&server, 1, "a", "a before");
    pump3(&server, &relay, &tab);
    assert_eq!(a.titles(), titles(&["a before"]));

    link.hold(move |m| match m {
        SyncMessage::Settled { id, .. } => *id == read_a,
        SyncMessage::ConnectionSettled { .. } => true,
        _ => false,
    });
    insert_both(&server, (2, "a both"), (3, "b both"));
    pump3(&server, &relay, &tab);
    assert_eq!(
        b.titles(),
        titles(&["b both"]),
        "b's upstream read is settled past it"
    );
    assert_eq!(
        a.titles(),
        titles(&["a before"]),
        "a's is not: the relay holds its rows, the tab shows them as of a's settle"
    );

    link.release();
    pump3(&server, &relay, &tab);
    assert_eq!(a.titles(), titles(&["a before", "a both"]));
    assert_eq!(b.titles(), titles(&["b both"]));
}

/// One of the upstream's messages a relay held back moves on its own:
/// the relay passes on the read's settle, or the connection's position,
/// and the tab's result shows the rows the relay already holds for it.
fn relay_passes_on(release: fn(&SyncMessage) -> bool) {
    let server = open(0, AuthorSubject::SYSTEM);
    let relay = open(1, user(1));
    let tab = open(2, user(1));
    tab.set_non_durable_client();
    let link = connect(&server, &relay, user(1));
    let _tab_link = connect(&relay, &tab, user(1));
    let mut a = Room::open(&tab, "a");
    let read_a = link.next_read(|| pump3(&server, &relay, &tab));
    let _b = Room::open(&tab, "b");
    pump3(&server, &relay, &tab);
    insert(&server, 1, "a", "a before");
    pump3(&server, &relay, &tab);

    link.hold(move |m| match m {
        SyncMessage::Settled { id, .. } => *id == read_a,
        SyncMessage::ConnectionSettled { .. } => true,
        _ => false,
    });
    insert_both(&server, (2, "a both"), (3, "b both"));
    pump3(&server, &relay, &tab);
    assert_eq!(a.titles(), titles(&["a before"]));
    let held = std::mem::take(&mut *link.held.borrow_mut());
    let (go, stay): (Vec<_>, Vec<_>) = held.into_iter().partition(|m| release(m));
    assert!(!go.is_empty(), "something to release");
    *link.held.borrow_mut() = stay;
    link.to_client.borrow_mut().extend(go);
    pump3(&server, &relay, &tab);
    assert_eq!(a.titles(), titles(&["a before", "a both"]));
}

#[test]
fn a_relay_passes_on_a_reads_settle_alone() {
    relay_passes_on(|m| matches!(m, SyncMessage::Settled { .. }));
}

#[test]
fn a_relay_passes_on_the_connection_position_alone() {
    relay_passes_on(|m| matches!(m, SyncMessage::ConnectionSettled { .. }));
}

/// A tab with no live results still learns the upstream's position from
/// its relay: its own commits, accepted past what it knew, are covered.
#[test]
fn a_relay_passes_on_the_connection_position_with_no_results() {
    let server = open(0, AuthorSubject::SYSTEM);
    let relay = open(1, user(1));
    let tab = open(2, user(1));
    tab.set_non_durable_client();
    let link = connect(&server, &relay, user(1));
    let _tab_link = connect(&relay, &tab, user(1));
    pump3(&server, &relay, &tab);
    link.hold_position();
    insert(&tab, 1, "a", "mine");
    pump3(&server, &relay, &tab);
    let held = block_on(tab.capture_snapshot()).unwrap();
    assert_eq!(held.dots.len(), 1, "accepted past what the tab knows");
    link.release();
    pump3(&server, &relay, &tab);
    let after = block_on(tab.capture_snapshot()).unwrap();
    assert!(after.dots.is_empty(), "the relay passed the position on");
}
