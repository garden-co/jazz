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
use jazz::query::{ArraySubquery, Query, col, eq, lit};
use jazz::records::Value;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};

use common::{allow_all_policies, compile_schema};

fn open(node: u8, author: AuthorSubject) -> Db {
    block_on(Db::open(config(node, author))).unwrap()
}

/// A replica kept in a file, reopened as a restarted app would.
fn open_file(path: &std::path::Path, node: u8, author: AuthorSubject) -> Db {
    block_on(Db::open_at(
        config(node, author),
        path,
        jazz::db::FileOptions::default(),
    ))
    .unwrap()
}

fn scratch(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "jazz-connection-settled-{}-{name}",
        std::process::id()
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir.join("store.sqlite")
}

fn config(node: u8, author: AuthorSubject) -> DbConfig {
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
    DbConfig::new(schema, identity)
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

/// `future`, polled while the two ticks, for what waits on the server.
fn drive<T>(server: &Db, client: &Db, future: impl std::future::Future<Output = T>) -> T {
    let mut future = std::pin::pin!(future);
    let mut context = std::task::Context::from_waker(std::task::Waker::noop());
    for _ in 0..50 {
        if let std::task::Poll::Ready(output) = future.as_mut().poll(&mut context) {
            return output;
        }
        block_on(client.tick()).unwrap();
        block_on(server.tick()).unwrap();
    }
    panic!("did not complete");
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

/// Reopened, a replica still names its own commits accepted past what it
/// knows by transaction: its settled-transaction records say which.
#[test]
fn a_restart_keeps_own_commits_accepted_past_the_known_position() {
    let path = scratch("own");
    let server = open(0, AuthorSubject::SYSTEM);
    let held = {
        let client = open_file(&path, 1, user(1));
        let link = connect(&server, &client, user(1));
        let _room = Room::open(&client, "a");
        pump(&server, &client);
        link.hold_position();
        insert(&client, 1, "a", "mine");
        pump(&server, &client);
        let held = block_on(client.capture_snapshot()).unwrap();
        assert_eq!(held.dots.len(), 1);
        held
    };
    let client = open_file(&path, 1, user(1));
    let reopened = block_on(client.capture_snapshot()).unwrap();
    assert_eq!(reopened.global_base, held.global_base);
    assert_eq!(reopened.dots, held.dots, "still named by transaction");
    let _link = connect(&server, &client, user(1));
    let _room = Room::open(&client, "a");
    pump(&server, &client);
    let after = block_on(client.capture_snapshot()).unwrap();
    assert!(after.dots.is_empty(), "the connection's position covers it");
}

/// Reopened, a replica reads others' versions it holds past what it knows
/// as of `known`, as before the restart, until its upstream settles them.
#[test]
fn a_restart_reads_held_versions_as_of_the_known_position() {
    let path = scratch("foreign");
    let server = open(0, AuthorSubject::SYSTEM);
    {
        let client = open_file(&path, 1, user(1));
        let link = connect(&server, &client, user(1));
        let mut a = Room::open(&client, "a");
        pump(&server, &client);
        insert(&server, 1, "a", "settled");
        pump(&server, &client);
        assert_eq!(a.titles(), titles(&["settled"]));
        link.hold(|m| {
            matches!(
                m,
                SyncMessage::Settled { .. } | SyncMessage::ConnectionSettled { .. }
            )
        });
        insert(&server, 2, "a", "unsettled");
        pump(&server, &client);
        assert_eq!(local_titles(&client), titles(&["settled"]));
    }
    let client = open_file(&path, 1, user(1));
    assert_eq!(
        local_titles(&client),
        titles(&["settled"]),
        "the store holds the newer version, read as of known"
    );
    let mut a = Room::open(&client, "a");
    let link = connect(&server, &client, user(1));
    // The reopened result's rows arrive a drain before its settle does.
    link.hold(|m| {
        matches!(
            m,
            SyncMessage::Settled { .. } | SyncMessage::ConnectionSettled { .. }
        )
    });
    pump(&server, &client);
    assert_eq!(a.titles(), titles(&["settled"]), "not settled yet");
    link.release();
    pump(&server, &client);
    assert_eq!(
        a.titles(),
        titles(&["settled", "unsettled"]),
        "the open result shows it once settled"
    );
    assert_eq!(local_titles(&client), titles(&["settled", "unsettled"]));
}

/// A serializable transaction begun while this replica's own commit is
/// accepted past what it knows reads that commit, and the authority
/// validates its reads as they were read: the commit is accepted.
#[test]
fn a_serializable_commit_over_own_writes_accepted_past_the_position_holds() {
    let server = open(0, AuthorSubject::SYSTEM);
    let client = open(1, user(1));
    let link = connect(&server, &client, user(1));
    let _room = Room::open(&client, "a");
    pump(&server, &client);
    link.hold_position();
    insert(&client, 1, "a", "mine");
    pump(&server, &client);
    assert_eq!(block_on(client.capture_snapshot()).unwrap().dots.len(), 1);

    let open = OpenTransactionId::new();
    block_on(client.begin_serializable(open)).unwrap();
    let query = Query::from("docs").filter(eq(col("room"), lit(Value::String("a".into()))));
    let prepared = client.prepare_query(&query).unwrap();
    let read = drive(
        &server,
        &client,
        client.tx_ref(open).all_prepared(&prepared),
    )
    .unwrap();
    assert_eq!(
        read.len(),
        1,
        "the transaction reads its own accepted write"
    );
    block_on(client.tx_ref(open).update(
        "docs",
        row(1),
        doc("a", "mine, edited"),
        Default::default(),
    ))
    .unwrap();
    drive(&server, &client, client.commit_serializable_handle(open)).unwrap();
    pump(&server, &client);
    link.release();
    pump(&server, &client);
    let on_server = |db: &Db| {
        let prepared = db.prepare_query(&Query::from("docs")).unwrap();
        block_on(db.all(&prepared, ReadOpts::default()))
            .unwrap()
            .iter()
            .map(|row| format!("{:?}", row.cell("title")))
            .collect::<Vec<_>>()
    };
    assert_eq!(
        on_server(&server),
        vec![format!("{:?}", Some(Value::String("mine, edited".into())))],
        "the authority accepted it"
    );
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

/// A relay with no server numbers no accepted positions: a tab's result
/// it serves settles at the authority position the relay last synced,
/// while the relay's own write stays pending.
#[test]
fn a_relay_with_no_server_settles_its_tabs_at_the_authority_position_it_holds() {
    let server = open(0, AuthorSubject::SYSTEM);
    let relay = open(1, user(1));
    let tab = open(2, user(1));
    tab.set_non_durable_client();
    relay.set_remote_link_hint(jazz::db::RemoteLinkHint::NoServer);
    let tab_link = connect(&relay, &tab, user(1));
    let mut a = Room::open(&tab, "a");
    pump3(&server, &relay, &tab);
    insert(&relay, 1, "a", "the relay's");
    tab_link.log.borrow_mut().clear();
    pump3(&server, &relay, &tab);
    assert_eq!(a.titles(), titles(&["the relay's"]));
    let settled: Vec<u64> = tab_link
        .log
        .borrow()
        .iter()
        .filter_map(|m| match m {
            SyncMessage::Settled { through, .. } => Some(*through),
            _ => None,
        })
        .collect();
    assert!(!settled.is_empty(), "the tab's result settles");
    assert!(
        settled.iter().all(|&at| at == 0),
        "settled at the authority's position the relay holds: {settled:?}"
    );
}

/// Two writers insert the same explicit row id. The other's version
/// reaches this replica before its own commit's fate: its pending insert
/// still overlays it, and the refusing fate then drops it whole.
#[test]
fn a_pending_insert_overlays_another_writers_row_of_the_same_id_until_its_fate() {
    let server = open(0, AuthorSubject::SYSTEM);
    let client = open(1, user(1));
    let link = connect(&server, &client, user(1));
    let _all = Room::open(&client, "a");
    pump(&server, &client);
    insert(&server, 1, "a", "theirs");
    insert(&client, 1, "a", "mine");
    link.hold(|m| matches!(m, SyncMessage::Fate { .. }));
    pump(&server, &client);
    let current = |client: &Db| -> BTreeSet<String> {
        let prepared = client.prepare_query(&Query::from("docs")).unwrap();
        client
            .read(&prepared)
            .unwrap()
            .iter()
            .map(|row| match row.cell("title") {
                Some(Value::String(title)) => title,
                other => panic!("title {other:?}"),
            })
            .collect()
    };
    assert_eq!(current(&client), titles(&["mine"]), "until its fate");
    link.release();
    pump(&server, &client);
    assert_eq!(current(&client), titles(&["theirs"]), "refused, it drops");
}
