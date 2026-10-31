//! What a replica knows of its upstream moves by `ConnectionSettled`
//! alone (SPEC 8 §8.4): a read's `Settled` speaks for that read and a
//! commit's `Fate` for that commit. Every upstream sends the connection's
//! position after a tick's settles and fates, so an unsharded server's
//! clients run the code a sharded gateway's do. Until it arrives, the
//! replica's own commits accepted past what it knows are named by
//! transaction.

use crate::common;

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, VecDeque};
use std::rc::Rc;

use jazz::db::{
    Db, DbConfig, DbIdentity, InsertOptions, ReadOpts, SyncMessage, Transport, TransportError,
    Upstream, block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::protocol::{ReadViewSourceSpec, ReadViewSpec};
use jazz::query::Query;
use jazz::records::Value;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};

use common::{allow_all_policies, compile_schema};

fn open(node: u8, author: AuthorSubject) -> Db {
    let schema = compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("docs")
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

fn title(title: &str) -> BTreeMap<String, Value> {
    BTreeMap::from([("title".to_owned(), Value::String(title.to_owned()))])
}

/// The server side of a link: logs what it sends down and, while `hold`
/// is set, keeps the connection's position back from the client.
struct Down {
    to_client: Rc<RefCell<VecDeque<SyncMessage>>>,
    from_client: Rc<RefCell<VecDeque<SyncMessage>>>,
    log: Rc<RefCell<Vec<SyncMessage>>>,
    hold: Rc<Cell<bool>>,
    held: Rc<RefCell<Vec<SyncMessage>>>,
}

impl Transport for Down {
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        self.log.borrow_mut().push(message.clone());
        if self.hold.get() && matches!(message, SyncMessage::ConnectionSettled { .. }) {
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
    hold: Rc<Cell<bool>>,
    held: Rc<RefCell<Vec<SyncMessage>>>,
}

impl Link {
    /// Let the held connection positions through.
    fn release(&self) {
        self.hold.set(false);
        let held = std::mem::take(&mut *self.held.borrow_mut());
        self.to_client.borrow_mut().extend(held);
    }
}

fn connect(server: &Db, client: &Db, identity: AuthorSubject) -> Link {
    let up = Rc::new(RefCell::new(VecDeque::new()));
    let down = Rc::new(RefCell::new(VecDeque::new()));
    let log = Rc::new(RefCell::new(Vec::new()));
    let hold = Rc::new(Cell::new(false));
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

/// A live read of `docs`, settled.
fn settled_read(server: &Db, client: &Db) -> jazz::db::SubscriptionStream {
    let prepared = client.prepare_query(&Query::from("docs")).unwrap();
    let live = block_on(client.subscribe(&prepared, ReadOpts::default())).unwrap();
    pump(server, client);
    live
}

fn insert(db: &Db, n: u8, text: &str) {
    block_on(db.insert(
        "docs",
        title(text),
        InsertOptions {
            row_id: Some(RowUuid::from_bytes([n; 16])),
            ..Default::default()
        },
    ))
    .unwrap();
}

#[test]
fn an_unsharded_server_sends_the_connection_position_after_its_settles() {
    let server = open(0, AuthorSubject::SYSTEM);
    let client = open(1, user(1));
    let link = connect(&server, &client, user(1));
    let _live = settled_read(&server, &client);
    insert(&server, 1, "from the server");
    link.log.borrow_mut().clear();
    pump(&server, &client);
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
    // A tick with nothing to settle sends nothing.
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
    let _live = settled_read(&server, &client);
    let before = block_on(client.capture_snapshot()).unwrap();
    link.hold.set(true);
    insert(&server, 1, "from the server");
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
    let _live = settled_read(&server, &client);
    link.hold.set(true);
    insert(&client, 1, "mine");
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
