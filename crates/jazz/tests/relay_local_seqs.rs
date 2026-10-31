//! A worker and its tabs each number their own unsettled versions, from 1.
//! A tab's replica holds the worker's unsettled versions (other tabs'
//! writes the worker has not settled yet) next to its own, in a band of
//! their own (between the accepted versions and the tab's), so the two
//! numberings never collide in a tab's history.

#[path = "common/duplex_transport.rs"]
mod duplex_transport;

use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};
use std::rc::Rc;

use duplex_transport::duplex;
use jazz::db::{
    Db, DbConfig, DbIdentity, ErrorCode, Image, ReadOpts, RemoteLinkHint, SyncCell, SyncMessage,
    Transport, TransportError, Upstream, block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::Query;
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{
    CmpOp, ColumnType, PolicyExpr, PolicyValue, SchemaBuilder, TableSchemaBuilder,
    Value as PublicValue,
};
use jazz::tx::DurabilityTier;

use crate::common::{allow_all_policies, compile_schema, read_and_allow_all_writes};

const WORKER: u8 = 0xa0;

/// A wall clock tests run stores at (Unix ms), instead of sleeping.
const CLOCK: i64 = 1_800_000_000_000;

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

/// Every note is readable but one whose body is "secret".
fn secret_schema() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("body", ColumnType::Text)
                    .policies(read_and_allow_all_writes(PolicyExpr::Cmp {
                        column: "body".to_owned(),
                        op: CmpOp::Ne,
                        value: PolicyValue::Literal(PublicValue::Text("secret".into())),
                    })),
            )
            .build(),
    )
}

fn open(node: u8, author: AuthorSubject) -> Db {
    open_in(schema(), node, author)
}

fn open_in(schema: JazzSchema, node: u8, author: AuthorSubject) -> Db {
    block_on(Db::open(
        DbConfig::new(
            schema,
            DbIdentity {
                node: NodeUuid::from_bytes([node; 16]),
                author,
            },
        )
        // A clock stopped at the hybrid-clock epoch numbers 1, 2, 3, so these
        // tests can name the seqs they expect.
        .with_clock(|| jazz::db::shard::HLC_EPOCH_MS),
    ))
    .unwrap()
}

fn author(node: u8) -> AuthorSubject {
    AuthorSubject::for_test_bytes([node; 16])
}

fn body(text: &str) -> BTreeMap<String, Value> {
    BTreeMap::from([("body".to_owned(), Value::String(text.to_owned()))])
}

/// Records what one side sends.
struct Tap {
    inner: Box<dyn Transport>,
    sent: Rc<RefCell<VecDeque<SyncMessage>>>,
}

impl Transport for Tap {
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        self.sent.borrow_mut().push_back(message.clone());
        self.inner.send(message)
    }

    fn try_recv(&mut self) -> Option<SyncMessage> {
        self.inner.try_recv()
    }
}

/// The authority, one worker, and its tabs (each connected as the worker's
/// identity, as a browser's tabs are).
struct Net {
    core: Db,
    worker: Db,
    worker_up: Option<Upstream>,
    /// Whom the authority serves the worker as.
    worker_as: AuthorSubject,
    tabs: Vec<Db>,
    /// What the worker sent each tab.
    to_tabs: Vec<Rc<RefCell<VecDeque<SyncMessage>>>>,
}

impl Net {
    fn new(tabs: usize) -> Self {
        Self::with_schema(tabs, schema, author(WORKER), author(WORKER))
    }

    /// A worker configured as `worker_author` (and served as it), with
    /// tabs of `tab_author` (a relay applies a session's read policy
    /// unless the session is its own identity). Only a system worker
    /// serves tabs of another identity.
    fn with_schema(
        tabs: usize,
        schema: fn() -> JazzSchema,
        worker_author: AuthorSubject,
        tab_author: AuthorSubject,
    ) -> Self {
        let mut net = Net {
            core: open_in(schema(), 0xc0, AuthorSubject::SYSTEM),
            worker: open_in(schema(), WORKER, worker_author),
            worker_up: None,
            worker_as: worker_author,
            tabs: Vec::new(),
            to_tabs: Vec::new(),
        };
        net.worker_online();
        for i in 0..tabs {
            let tab = open_in(schema(), WORKER + 1 + i as u8, tab_author.clone());
            tab.set_non_durable_client();
            tab.set_remote_link_hint(RemoteLinkHint::Live);
            let (client_side, server_side) = duplex();
            block_on(tab.connect_upstream(client_side));
            let sent = Rc::default();
            net.worker
                .accept_subscriber(
                    Box::new(Tap {
                        inner: server_side,
                        sent: Rc::clone(&sent),
                    }),
                    tab_author.clone(),
                )
                .unwrap();
            net.to_tabs.push(sent);
            net.tabs.push(tab);
        }
        net
    }

    fn worker_online(&mut self) {
        let (client_side, server_side) = duplex();
        self.worker_up = Some(block_on(self.worker.connect_upstream(client_side)));
        self.core
            .accept_subscriber(server_side, self.worker_as.clone())
            .unwrap();
        self.worker.set_remote_link_hint(RemoteLinkHint::Live);
    }

    fn worker_offline(&mut self) {
        let upstream = self.worker_up.take().expect("online");
        assert!(self.worker.detach_connection(&upstream));
        self.worker.set_remote_link_hint(RemoteLinkHint::Failed);
    }

    fn tab(&self, i: usize) -> &Db {
        &self.tabs[i]
    }

    fn pump(&self) {
        for _ in 0..4 {
            for tab in &self.tabs {
                block_on(tab.tick()).unwrap();
            }
            block_on(self.worker.tick()).unwrap();
            if self.worker_up.is_some() {
                block_on(self.core.tick()).unwrap();
                block_on(self.worker.tick()).unwrap();
            }
            for tab in &self.tabs {
                block_on(tab.tick()).unwrap();
            }
        }
    }

    fn insert(&self, db: &Db, text: &str) -> RowUuid {
        block_on(db.insert("notes", body(text), Default::default()))
            .unwrap()
            .row_uuid()
    }

    fn update(&self, db: &Db, row: RowUuid, text: &str) -> u64 {
        block_on(db.update("notes", row, body(text), Default::default()))
            .unwrap_or_else(|error| panic!("writing {text:?}: {error}"))
            .mergeable_tx_id()
    }

    /// `db` subscribes to every note, so its replica holds the rows.
    fn watch(&self, db: &Db) -> jazz::db::SubscriptionStream {
        let prepared = db.prepare_query(&Query::from("notes")).unwrap();
        let stream = block_on(db.subscribe(&prepared, ReadOpts::default())).unwrap();
        self.pump();
        stream
    }

    fn bodies(&self, db: &Db) -> Vec<String> {
        let prepared = db.prepare_query(&Query::from("notes")).unwrap();
        block_on(db.all(&prepared, ReadOpts::default()))
            .unwrap()
            .iter()
            .filter_map(|row| match row.cells().find(|(name, _)| *name == "body") {
                Some((_, Some(Value::String(text)))) => Some(text.clone()),
                _ => None,
            })
            .collect()
    }

    fn settle(&self, db: &Db, tx: u64) {
        use std::future::Future;
        use std::pin::pin;
        use std::task::{Context, Poll, Waker};
        let mut wait = pin!(db.wait_for_transaction(tx, DurabilityTier::Global));
        let mut context = Context::from_waker(Waker::noop());
        for _ in 0..50 {
            if let Poll::Ready(result) = wait.as_mut().poll(&mut context) {
                result.expect("the write settles");
                return;
            }
            self.pump();
        }
        panic!("tx {tx} did not settle");
    }
}

/// A tab with unsettled writes on a row receives another tab's unsettled
/// version of it from the worker, then writes the row again. Numbered from
/// one base, the other tab's version (at the worker's next seq) took the
/// seq of the tab's next write, which then failed to keep its version as
/// history.
#[test]
fn a_tab_writes_again_over_another_tabs_unsettled_version_from_its_worker() {
    let mut net = Net::new(2);
    let row = net.insert(net.tab(0), "seed");
    let _watch = [net.watch(net.tab(0)), net.watch(net.tab(1))];
    net.pump();
    net.worker_offline();

    let mut txs = Vec::new();
    for (tab, text) in [(1, "b1"), (0, "a1"), (0, "a2"), (1, "b2"), (0, "a3")] {
        txs.push(net.update(net.tab(tab), row, text));
        net.pump();
    }

    net.worker_online();
    net.settle(net.tab(0), txs[4]);
    let expected = net.bodies(&net.core);
    assert_eq!(expected, ["a3"]);
    for db in [&net.worker, net.tab(0), net.tab(1)] {
        assert_eq!(net.bodies(db), expected, "replicas converge");
    }
}

/// The worker's version of a tab's own unsettled write, numbered from one
/// base, took the seq of the tab's next write. The worker ships the tab no
/// version of its own uploading writes, so the tab writes the row again.
#[test]
fn a_tab_writes_again_over_its_own_unsettled_write_at_its_worker() {
    let mut net = Net::new(2);
    net.worker_offline();
    // The tab's first commit is also the worker's first.
    let row = net.insert(net.tab(0), "seed");
    let _watch = [net.watch(net.tab(0)), net.watch(net.tab(1))];
    net.pump();

    // The worker orders the other tab's write second, and the tab's own
    // second write third.
    net.update(net.tab(1), row, "b1");
    net.update(net.tab(0), row, "a1");
    net.pump();
    let last = net.update(net.tab(0), row, "a2");
    net.pump();

    net.worker_online();
    net.settle(net.tab(0), last);
    let expected = net.bodies(&net.core);
    assert_eq!(expected, ["a2"]);
    for db in [&net.worker, net.tab(0), net.tab(1)] {
        assert_eq!(net.bodies(db), expected, "replicas converge");
    }
}

/// A tab's unsettled insert stays its own after the worker stored it at a
/// seq of its own (here not the tab's: the worker wrote first, unseen by
/// the tab): a serializable snapshot on the tab still reads it as the
/// tab's write.
#[test]
fn a_tabs_unsettled_insert_stays_its_own_once_its_worker_stored_it() {
    let mut net = Net::new(1);
    net.worker_offline();
    net.insert(&net.worker, "worker");
    let row = net.insert(net.tab(0), "own");
    net.pump();

    let tab = net.tab(0);
    let open = jazz::tools::OpenTransactionId::new();
    block_on(tab.begin_serializable(open)).unwrap();
    let prepared = tab.prepare_query(&Query::from("notes")).unwrap();
    let tx = tab.tx_ref(open);
    let mut read = std::pin::pin!(tx.all_prepared(&prepared));
    let mut context = std::task::Context::from_waker(std::task::Waker::noop());
    let rows = loop {
        if let std::task::Poll::Ready(rows) = read.as_mut().poll(&mut context) {
            break rows.unwrap();
        }
        net.pump();
    };
    assert!(
        rows.iter().any(|r| r.row_uuid() == row),
        "the snapshot holds the tab's own unsettled insert"
    );
}

/// An unsettled version is named by its writer's commit: one an upstream
/// ships without its writer is malformed, so the tab refuses it rather
/// than store it.
#[test]
fn a_tab_refuses_an_unsettled_version_with_no_writer() {
    let tab = open(WORKER + 1, author(WORKER));
    tab.set_non_durable_client();
    tab.set_remote_link_hint(RemoteLinkHint::Live);
    let (client_side, mut server_side) = duplex();
    block_on(tab.connect_upstream(client_side));
    server_side
        .send(SyncMessage::Rows {
            images: vec![Image {
                written: None,
                table: "notes".to_owned(),
                id: [7; 16],
                key: Vec::new(),
                at: jazz::db::Pos::pending(1),
                deleted: false,
                content: true,
                cells: Vec::new(),
            }],
            tx_ids: Vec::new(),
        })
        .unwrap();
    let error = block_on(tab.tick()).expect_err("the tab refuses the version");
    assert_eq!(error.code, ErrorCode::Query, "{error}");
    assert!(error.message.contains("is not supported yet"), "{error}");
    let prepared = tab.prepare_query(&Query::from("notes")).unwrap();
    let rows = block_on(tab.all(&prepared, ReadOpts::default())).unwrap();
    assert!(rows.is_empty(), "nothing was stored");
}

/// A worker with no server keeps a tab's commit pending, after a commit
/// of its own. A serializable transaction the tab began while that commit
/// was unsettled counts it as its own.
#[test]
fn a_tabs_transaction_over_its_own_commit_held_by_a_worker_without_a_server_commits() {
    let worker = open(WORKER, author(WORKER));
    worker.set_remote_link_hint(RemoteLinkHint::NoServer);
    let tab = open(WORKER + 1, author(WORKER));
    tab.set_non_durable_client();
    tab.set_remote_link_hint(RemoteLinkHint::Live);
    let (client_side, server_side) = duplex();
    block_on(tab.connect_upstream(client_side));
    worker
        .accept_subscriber(server_side, author(WORKER))
        .unwrap();
    let pump = || {
        for _ in 0..4 {
            block_on(tab.tick()).unwrap();
            block_on(worker.tick()).unwrap();
        }
    };
    // The worker's own commit first, so it holds the tab's at another seq.
    block_on(worker.insert("notes", body("worker"), Default::default())).unwrap();
    let row = block_on(tab.insert("notes", body("tab"), Default::default()))
        .unwrap()
        .row_uuid();
    let open = jazz::tools::OpenTransactionId::new();
    block_on(tab.begin_serializable(open)).unwrap();
    pump();
    let tx = tab.tx_ref(open);
    drive(&pump, tx.delete("notes", row, Default::default())).unwrap();
    drive(&pump, tab.commit_serializable_handle(open))
        .expect("the tab's own pending insert is no conflict");
}

fn drive<T>(pump: &impl Fn(), future: impl std::future::Future<Output = T>) -> T {
    let mut future = std::pin::pin!(future);
    let mut context = std::task::Context::from_waker(std::task::Waker::noop());
    for _ in 0..50 {
        if let std::task::Poll::Ready(output) = future.as_mut().poll(&mut context) {
            return output;
        }
        pump();
    }
    panic!("did not complete");
}

/// Becoming a non-durable client hands out no seq: a session that writes
/// nothing leaves its node's high water where it found it, and its first
/// write lands just above it.
#[test]
fn a_tabs_high_water_moves_only_when_it_writes() {
    let floor = 2;
    let tab = open(WORKER + 1, author(WORKER));
    tab.seed_seq_floor(floor);
    tab.set_non_durable_client();
    tab.set_remote_link_hint(RemoteLinkHint::Live);
    assert_eq!(tab.seq_high_water(), floor);
    let tx = block_on(tab.insert("notes", body("first"), Default::default()))
        .unwrap()
        .mergeable_tx_id();
    assert_eq!(tx, floor + 1);
    assert_eq!(tab.seq_high_water(), tx);
}

/// A tab's row stays readable through every order its worker and the
/// authority can take the tab's insert and update in: when the worker's
/// newest version of the row is the tab's own pending update, it ships the
/// tab the version that update sits on (here the settled insert), not
/// nothing.
#[test]
fn a_tabs_row_stays_while_its_worker_settles_the_insert_under_its_update() {
    for seed in 0u64..200 {
        let net = Net::new(1);
        let mut state = seed;
        let mut next = move || {
            state = state
                .wrapping_mul(6364136223846793005)
                .wrapping_add(1442695040888963407);
            (state >> 33) % 3
        };
        let row = net.insert(net.tab(0), "orig");
        let mut order = String::new();
        for step in 0..40 {
            if step == 6 {
                net.update(net.tab(0), row, "upd");
                order.push('U');
            }
            match next() {
                0 => block_on(net.tab(0).tick()).map(|_| order.push('t')),
                1 => block_on(net.worker.tick()).map(|_| order.push('w')),
                _ => block_on(net.core.tick()).map(|_| order.push('c')),
            }
            .unwrap();
            let bodies = net.bodies(net.tab(0));
            assert_eq!(bodies.len(), 1, "seed {seed}, ticks {order}: {bodies:?}");
        }
        net.pump();
        assert_eq!(net.bodies(net.tab(0)), ["upd"], "seed {seed}");
    }
}

/// The version a relay ships under a session's own pending write passes the
/// session's read policy as that version. Here another writer's version
/// the session may not read lands at the relay under the session's update
/// while it uploads: the relay ships the session no version of the row
/// until the update settles. (A session of the relay's own identity reads
/// everything the relay holds.)
#[test]
fn a_session_gets_no_unreadable_version_under_its_own_pending_write() {
    let net = Net::with_schema(1, secret_schema, AuthorSubject::SYSTEM, author(0x70));
    let inserted = block_on(
        net.tab(0)
            .insert("notes", body("plain"), Default::default()),
    )
    .unwrap();
    let row = inserted.row_uuid();
    let _watch = net.watch(net.tab(0));
    net.settle(net.tab(0), inserted.mergeable_tx_id());
    net.to_tabs[0].borrow_mut().clear();

    // The authority takes another writer's update first; the worker gets
    // the tab's update before that version.
    block_on(
        net.core
            .update("notes", row, body("secret"), Default::default()),
    )
    .unwrap();
    block_on(net.core.tick()).unwrap();
    let mine = net.update(net.tab(0), row, "mine");
    block_on(net.tab(0).tick()).unwrap();
    block_on(net.worker.tick()).unwrap();
    block_on(net.tab(0).tick()).unwrap();
    block_on(net.worker.tick()).unwrap();
    block_on(net.tab(0).tick()).unwrap();

    let secret = SyncCell::Text(b"secret".to_vec());
    for message in net.to_tabs[0].borrow().iter() {
        if let SyncMessage::Rows { images, .. } = message {
            for image in images {
                assert!(
                    !image.cells.iter().any(|(_, cell)| *cell == secret),
                    "the worker shipped the tab a version it may not read: {image:?}"
                );
            }
        }
    }
    net.settle(net.tab(0), mine);
    assert_eq!(net.bodies(net.tab(0)), ["mine"]);
}

/// A tab's commit the worker stored while offline uploads under the tab's
/// own id after the tab is gone and the worker has restarted, next to a
/// commit from the fresh node the tab's next open draws.
#[test]
fn a_closed_tabs_pending_commit_uploads_after_its_worker_restarts() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("worker.sqlite");
    let open_worker = || {
        block_on(Db::open_at(
            DbConfig::new(
                schema(),
                DbIdentity {
                    node: NodeUuid::from_bytes([WORKER; 16]),
                    author: author(WORKER),
                },
            ),
            &path,
            jazz::db::FileOptions::default(),
        ))
        .unwrap()
    };
    let attach_tab = |worker: &Db, node: u8| {
        let tab = open(node, author(WORKER));
        tab.set_non_durable_client();
        tab.set_remote_link_hint(RemoteLinkHint::Live);
        let (client_side, server_side) = duplex();
        block_on(tab.connect_upstream(client_side));
        worker
            .accept_subscriber(server_side, author(WORKER))
            .unwrap();
        tab
    };
    let pump = |dbs: &[&Db]| {
        for _ in 0..6 {
            for db in dbs {
                block_on(db.tick()).unwrap();
            }
        }
    };
    let bodies = |db: &Db| {
        let prepared = db.prepare_query(&Query::from("notes")).unwrap();
        let mut bodies: Vec<String> = block_on(db.all(&prepared, ReadOpts::default()))
            .unwrap()
            .iter()
            .filter_map(|row| match row.cells().find(|(name, _)| *name == "body") {
                Some((_, Some(Value::String(text)))) => Some(text.clone()),
                _ => None,
            })
            .collect();
        bodies.sort();
        bodies
    };
    let insert = |db: &Db, text: &str| {
        block_on(db.insert("notes", body(text), Default::default())).unwrap();
    };
    let core = open_in(schema(), 0xc0, AuthorSubject::SYSTEM);

    let uploads: Rc<RefCell<VecDeque<SyncMessage>>> = Rc::default();
    let connect = |worker: &Db| {
        let (client_side, server_side) = duplex();
        let up = block_on(worker.connect_upstream(Box::new(Tap {
            inner: client_side,
            sent: Rc::clone(&uploads),
        })));
        core.accept_subscriber(server_side, author(WORKER)).unwrap();
        worker.set_remote_link_hint(RemoteLinkHint::Live);
        up
    };

    // A worker with a server that is offline holds its tabs' commits for
    // upload.
    let worker = open_worker();
    let up = connect(&worker);
    pump(&[&worker, &core]);
    assert!(worker.detach_connection(&up));
    worker.set_remote_link_hint(RemoteLinkHint::Failed);
    let tab = attach_tab(&worker, WORKER + 1);
    insert(&tab, "first");
    pump(&[&tab, &worker]);
    assert_eq!(
        bodies(&worker),
        ["first"],
        "the worker stored the tab's commit"
    );
    assert_eq!(worker.pending_upload_count(), 1);
    drop(tab);
    block_on(worker.close()).unwrap();

    let worker = open_worker();
    let tab = attach_tab(&worker, WORKER + 2);
    insert(&tab, "second");
    pump(&[&tab, &worker]);
    assert_eq!(
        worker.pending_upload_count(),
        2,
        "the closed tab's commit survived"
    );
    let _up = connect(&worker);
    pump(&[&tab, &worker, &core]);

    assert_eq!(bodies(&core), ["first", "second"]);
    // Each commit went up under the id of the tab node that wrote it.
    let nodes: Vec<NodeUuid> = uploads
        .borrow()
        .iter()
        .filter_map(|message| match message {
            SyncMessage::Commit { id, .. } => Some(id.node),
            _ => None,
        })
        .collect();
    assert_eq!(
        nodes,
        [
            NodeUuid::from_bytes([WORKER + 1; 16]),
            NodeUuid::from_bytes([WORKER + 2; 16]),
        ]
    );
    assert_eq!(worker.pending_upload_count(), 0, "both commits settled");
}

/// A worker with no server numbers no accepted positions: its tabs'
/// commits, and its own, stay pending (stored, never `Global`), its other
/// tab sees them as the worker's unsettled versions, and once a server is
/// added they upload and settle at the authority's seqs.
#[test]
fn a_serverless_workers_tab_commits_stay_pending_until_a_server_is_added() {
    let worker = open(WORKER, author(WORKER));
    worker.set_remote_link_hint(RemoteLinkHint::NoServer);
    let attach_tab = |node: u8| {
        let tab = open(node, author(WORKER));
        tab.set_non_durable_client();
        tab.set_remote_link_hint(RemoteLinkHint::Live);
        let (client_side, server_side) = duplex();
        block_on(tab.connect_upstream(client_side));
        worker
            .accept_subscriber(server_side, author(WORKER))
            .unwrap();
        tab
    };
    let writer = attach_tab(WORKER + 1);
    let reader = attach_tab(WORKER + 2);
    let core = open_in(schema(), 0xc0, AuthorSubject::SYSTEM);
    let pump = || {
        for _ in 0..6 {
            for db in [&writer, &reader, &worker, &core] {
                block_on(db.tick()).unwrap();
            }
        }
    };
    let notes = reader.prepare_query(&Query::from("notes")).unwrap();
    let _watch = block_on(reader.subscribe(&notes, ReadOpts::default())).unwrap();
    let tab_tx = block_on(writer.insert("notes", body("tab"), Default::default()))
        .unwrap()
        .mergeable_tx_id();
    let own_tx = block_on(worker.insert("notes", body("worker"), Default::default()))
        .unwrap()
        .mergeable_tx_id();
    pump();
    assert_eq!(worker.pending_upload_count(), 2, "both commits pending");
    let shown = |db: &Db| {
        let mut bodies: Vec<String> = block_on(db.all(&notes, ReadOpts::default()))
            .unwrap()
            .iter()
            .filter_map(|row| match row.cells().find(|(name, _)| *name == "body") {
                Some((_, Some(Value::String(text)))) => Some(text.clone()),
                _ => None,
            })
            .collect();
        bodies.sort();
        bodies
    };
    assert_eq!(shown(&reader), ["tab", "worker"], "the other tab sees both");
    let global = |db: &Db, tx: u64| {
        let mut wait = std::pin::pin!(db.wait_for_transaction(tx, DurabilityTier::Global));
        let mut context = std::task::Context::from_waker(std::task::Waker::noop());
        for _ in 0..20 {
            if let std::task::Poll::Ready(fate) = wait.as_mut().poll(&mut context) {
                return Some(fate);
            }
            pump();
        }
        None
    };
    assert!(global(&writer, tab_tx).is_none(), "no authority, no Global");
    assert!(global(&worker, own_tx).is_none(), "no authority, no Global");
    assert!(shown(&core).is_empty(), "nothing reached a server");

    let (client_side, server_side) = duplex();
    let _up = block_on(worker.connect_upstream(client_side));
    core.accept_subscriber(server_side, author(WORKER)).unwrap();
    worker.set_remote_link_hint(RemoteLinkHint::Live);
    assert!(
        matches!(global(&writer, tab_tx), Some(Ok(_))),
        "the tab's lands"
    );
    assert!(
        matches!(global(&worker, own_tx), Some(Ok(_))),
        "the worker's lands"
    );
    assert_eq!(shown(&core), ["tab", "worker"]);
    assert_eq!(worker.pending_upload_count(), 0, "both commits settled");
    assert_eq!(shown(&reader), ["tab", "worker"]);
}

/// Without a server, a worker and its tabs order unsettled versions
/// last-writer-wins by their commits' clocks, not by arrival: a tab's write
/// made earlier but delivered later goes under the later one everywhere.
#[test]
fn tabs_converge_on_the_later_write_whatever_order_their_worker_gets_them() {
    let worker = open(WORKER, author(WORKER));
    worker.set_clock_for_test(Some(CLOCK));
    worker.set_remote_link_hint(RemoteLinkHint::NoServer);
    let attach_tab = |node: u8| {
        let tab = open(node, author(WORKER));
        tab.set_non_durable_client();
        tab.set_remote_link_hint(RemoteLinkHint::Live);
        let (client_side, server_side) = duplex();
        block_on(tab.connect_upstream(client_side));
        worker
            .accept_subscriber(server_side, author(WORKER))
            .unwrap();
        tab
    };
    let early = attach_tab(WORKER + 1);
    let late = attach_tab(WORKER + 2);
    let pump = || {
        for _ in 0..6 {
            for db in [&early, &late, &worker] {
                block_on(db.tick()).unwrap();
            }
        }
    };
    let notes = worker.prepare_query(&Query::from("notes")).unwrap();
    let watches: Vec<_> = [&early, &late]
        .map(|tab| block_on(tab.subscribe(&notes, ReadOpts::default())).unwrap())
        .into_iter()
        .collect();
    let row = block_on(early.insert("notes", body("before"), Default::default()))
        .unwrap()
        .row_uuid();
    pump();
    let shown = |db: &Db| -> Vec<String> {
        block_on(db.all(&notes, ReadOpts::default()))
            .unwrap()
            .iter()
            .filter_map(|row| match row.cells().find(|(name, _)| *name == "body") {
                Some((_, Some(Value::String(text)))) => Some(text.clone()),
                _ => None,
            })
            .collect()
    };
    assert_eq!(shown(&late), ["before"]);

    early.set_clock_for_test(Some(CLOCK));
    block_on(early.update("notes", row, body("early"), Default::default())).unwrap();
    late.set_clock_for_test(Some(CLOCK + 10));
    block_on(late.update("notes", row, body("late"), Default::default())).unwrap();
    // The later write reaches the worker first.
    for db in [&late, &worker, &late] {
        block_on(db.tick()).unwrap();
    }
    assert_eq!(shown(&worker), ["late"]);
    pump();
    for db in [&worker, &early, &late] {
        assert_eq!(shown(db), ["late"], "every store shows the later write");
    }
    drop(watches);
}

/// Notes are readable and writable, but the authority refuses inserting
/// one whose body is "refused".
fn refusing_schema() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("notes")
                    .column("body", ColumnType::Text)
                    .policies(allow_all_policies().with_insert(PolicyExpr::Cmp {
                        column: "body".to_owned(),
                        op: CmpOp::Ne,
                        value: PolicyValue::Literal(PublicValue::Text("refused".into())),
                    })),
            )
            .build(),
    )
}

/// A tab's one-shot local-first read, as the bindings run it: answered by
/// its worker, then read from the tab's replica.
fn one_shot_bodies(net: &Net, tab: &Db) -> Vec<String> {
    use jazz::db::SerializedReadResult;
    use std::future::Future;
    use std::pin::pin;
    use std::task::{Context, Poll, Waker};
    let bytes = postcard::to_allocvec(&Query::from("notes")).unwrap();
    let mut read = pin!(tab.all_serialized_query(&bytes, ReadOpts::default(), None, None, None));
    let mut context = Context::from_waker(Waker::noop());
    for _ in 0..50 {
        if let Poll::Ready(result) = read.as_mut().poll(&mut context) {
            let SerializedReadResult::Rows(rows) = result.unwrap() else {
                panic!("notes is a row query");
            };
            return rows
                .iter()
                .filter_map(|row| match row.cells().find(|(name, _)| *name == "body") {
                    Some((_, Some(Value::String(text)))) => Some(text.clone()),
                    _ => None,
                })
                .collect();
        }
        net.pump();
    }
    panic!("the one-shot read did not complete");
}

/// A tab that saw another tab's unsettled insert only through one-shot
/// reads holds no watch on it, yet the refusal still reaches it: the worker
/// reconciles what it shipped at an unsettled version once that settles.
///
/// ```text
/// tab1 ─insert(refused)─► worker (offline) ─one-shot─► tab0: [refused]
/// worker online ─► core ✗ refused ─► worker reverts ─► tab0: []
/// ```
#[test]
fn a_refused_write_a_tab_only_read_once_leaves_that_tab_too() {
    let mut net = Net::with_schema(2, refusing_schema, author(WORKER), author(WORKER));
    net.pump();
    net.worker_offline();
    net.insert(net.tab(1), "refused");
    net.pump();
    assert_eq!(one_shot_bodies(&net, net.tab(0)), ["refused"]);

    net.worker_online();
    for _ in 0..4 {
        net.pump();
    }
    assert!(net.bodies(&net.core).is_empty(), "the authority refused it");
    assert!(net.bodies(&net.worker).is_empty(), "the worker reverted it");
    assert!(
        net.bodies(net.tab(0)).is_empty(),
        "the tab that read it once dropped it"
    );
    assert!(one_shot_bodies(&net, net.tab(0)).is_empty());
}

/// A tab holds its worker's unsettled versions only while that worker is
/// its upstream: connected to another worker (a new identity) or to its
/// worker wiped, it drops them, and shows only what the new upstream
/// ships.
#[test]
fn a_tab_drops_its_old_workers_unsettled_versions_when_its_upstream_changes() {
    for (same_node, same_store) in [(false, false), (true, false), (true, true)] {
        let worker = open(WORKER, author(WORKER));
        worker.set_remote_link_hint(RemoteLinkHint::NoServer);
        let attach = |worker: &Db, tab: &Db| {
            let (client_side, server_side) = duplex();
            let up = block_on(tab.connect_upstream(client_side));
            worker
                .accept_subscriber(server_side, author(WORKER))
                .unwrap();
            up
        };
        let new_tab = |node: u8| {
            let tab = open(node, author(WORKER));
            tab.set_non_durable_client();
            tab.set_remote_link_hint(RemoteLinkHint::Live);
            tab
        };
        let writer = new_tab(WORKER + 1);
        let reader = new_tab(WORKER + 2);
        let _w = attach(&worker, &writer);
        let up = attach(&worker, &reader);
        let notes = reader.prepare_query(&Query::from("notes")).unwrap();
        let _watch = block_on(reader.subscribe(&notes, ReadOpts::default())).unwrap();
        let pump = |dbs: &[&Db]| {
            for _ in 0..6 {
                for db in dbs {
                    block_on(db.tick()).unwrap();
                }
            }
        };
        block_on(writer.insert("notes", body("old worker's"), Default::default())).unwrap();
        pump(&[&writer, &worker, &reader]);
        let shown = |db: &Db| -> Vec<String> {
            block_on(db.all(&notes, ReadOpts::default()))
                .unwrap()
                .iter()
                .filter_map(|row| match row.cells().find(|(name, _)| *name == "body") {
                    Some((_, Some(Value::String(text)))) => Some(text.clone()),
                    _ => None,
                })
                .collect()
        };
        assert_eq!(shown(&reader), ["old worker's"]);

        assert!(reader.detach_connection(&up));
        if same_store {
            // The same worker again: it ships its unsettled version anew.
            let _up = attach(&worker, &reader);
            pump(&[&worker, &reader]);
            assert_eq!(shown(&reader), ["old worker's"], "reconnected");
            continue;
        }
        let fresh = open(
            if same_node { WORKER } else { WORKER + 0x10 },
            author(WORKER),
        );
        fresh.set_remote_link_hint(RemoteLinkHint::NoServer);
        block_on(fresh.insert("notes", body("new worker's"), Default::default())).unwrap();
        let _up = attach(&fresh, &reader);
        pump(&[&fresh, &reader]);
        assert_eq!(
            shown(&reader),
            ["new worker's"],
            "the old worker's unsettled version is gone (same node: {same_node})"
        );
    }
}

/// A worker with no server (its clock at `CLOCK`) and tabs of these
/// nodes, each watching `query` so its replica holds the rows.
fn serverless(
    schema: fn() -> JazzSchema,
    nodes: &[u8],
    query: &Query,
) -> (Db, Vec<Db>, Vec<jazz::db::SubscriptionStream>) {
    let worker = open_in(schema(), WORKER, author(WORKER));
    worker.set_clock_for_test(Some(CLOCK));
    worker.set_remote_link_hint(RemoteLinkHint::NoServer);
    let mut tabs = Vec::new();
    let mut watches = Vec::new();
    for &node in nodes {
        let tab = open_in(schema(), node, author(WORKER));
        tab.set_clock_for_test(Some(CLOCK));
        tab.set_non_durable_client();
        tab.set_remote_link_hint(RemoteLinkHint::Live);
        let (client_side, server_side) = duplex();
        block_on(tab.connect_upstream(client_side));
        worker
            .accept_subscriber(server_side, author(WORKER))
            .unwrap();
        let prepared = tab.prepare_query(query).unwrap();
        watches.push(block_on(tab.subscribe(&prepared, ReadOpts::default())).unwrap());
        tabs.push(tab);
    }
    (worker, tabs, watches)
}

fn pump_all(dbs: &[&Db]) {
    for _ in 0..6 {
        for db in dbs {
            block_on(db.tick()).unwrap();
        }
    }
}

fn note_bodies(db: &Db) -> Vec<String> {
    let prepared = db.prepare_query(&Query::from("notes")).unwrap();
    let mut bodies: Vec<String> = block_on(db.all(&prepared, ReadOpts::default()))
        .unwrap()
        .iter()
        .filter_map(|row| match row.cell("body") {
            Some(Value::String(text)) => Some(text),
            _ => None,
        })
        .collect();
    bodies.sort();
    bodies
}

fn provenance(db: &Db, row: RowUuid) -> (u64, u64) {
    let select = db
        .prepare_query(&Query::from("notes").select(["body", "$createdAt", "$updatedAt"]))
        .unwrap();
    let rows = block_on(db.all(&select, ReadOpts::default())).unwrap();
    let row = rows
        .iter()
        .find(|r| r.row_uuid() == row)
        .expect("the row is held");
    let ms = |column| match row.cell(column) {
        Some(Value::U64(ms)) => ms,
        Some(Value::Nullable(Some(value))) => match *value {
            Value::U64(ms) => ms,
            other => panic!("{column}: {other:?}"),
        },
        other => panic!("{column}: {other:?}"),
    };
    (ms("$createdAt"), ms("$updatedAt"))
}

/// A write timestamped far ahead (a day, or past any i64) orders among
/// unsettled versions at its commit's clock, not its time, and moves no
/// other tab's own provenance stamps: a sibling's later write still
/// orders after it, at its own clock.
#[test]
fn a_far_future_write_time_neither_stamps_nor_outorders_a_sibling_tabs_later_write() {
    for far in [CLOCK as u64 + 24 * 60 * 60 * 1000, u64::MAX] {
        let notes = Query::from("notes");
        let (worker, tabs, _watches) = serverless(schema, &[WORKER + 1, WORKER + 2], &notes);
        let (ahead, sibling) = (&tabs[0], &tabs[1]);
        let all = [ahead, sibling, &worker];
        let row = block_on(ahead.insert("notes", body("before"), Default::default()))
            .unwrap()
            .row_uuid();
        pump_all(&all);
        block_on(ahead.update(
            "notes",
            row,
            body("ahead"),
            jazz::db::UpdateOptions {
                updated_at_ms: Some(far),
                ..Default::default()
            },
        ))
        .unwrap();
        pump_all(&all);
        assert_eq!(note_bodies(sibling), ["ahead"], "far: {far}");

        for db in all {
            db.set_clock_for_test(Some(CLOCK + 1_000));
        }
        let fresh = block_on(sibling.insert("notes", body("fresh"), Default::default()))
            .unwrap()
            .row_uuid();
        assert_eq!(
            provenance(sibling, fresh),
            (CLOCK as u64 + 1_000, CLOCK as u64 + 1_000),
            "the sibling stamps at its own clock (far: {far})"
        );
        block_on(sibling.update("notes", row, body("later"), Default::default())).unwrap();
        pump_all(&all);
        for db in all {
            assert_eq!(note_bodies(db), ["fresh", "later"], "far: {far}");
        }
        let fresh_at_worker = provenance(&worker, fresh);
        assert!(
            fresh_at_worker.0 < CLOCK as u64 + 30_000,
            "the worker stamps at its own clock: {fresh_at_worker:?} (far: {far})"
        );
    }
}

/// A worker's own write made after it relayed a tab's commit orders
/// after that commit, though the tab's clock runs ahead of the worker's
/// (within the 500 ms a store counts a received clock ahead of its own).
#[test]
fn a_workers_write_after_a_tabs_relayed_commit_orders_after_it() {
    let notes = Query::from("notes");
    let (worker, tabs, _watches) = serverless(schema, &[WORKER + 1], &notes);
    let tab = &tabs[0];
    tab.set_clock_for_test(Some(CLOCK + 400));
    let row = block_on(tab.insert("notes", body("tab"), Default::default()))
        .unwrap()
        .row_uuid();
    pump_all(&[tab, &worker]);
    worker.set_clock_for_test(Some(CLOCK + 10));
    block_on(worker.update("notes", row, body("worker"), Default::default())).unwrap();
    pump_all(&[tab, &worker]);
    assert_eq!(note_bodies(&worker), ["worker"]);
    assert_eq!(note_bodies(tab), ["worker"]);
}

/// A write's explicit time is provenance only: a tab's backfill
/// timestamped after another tab's later write still orders by its
/// commit's clock, under it, whichever the worker gets first.
#[test]
fn an_explicit_write_time_does_not_order_its_commit() {
    for backfill_first in [false, true] {
        let notes = Query::from("notes");
        let (worker, tabs, _watches) = serverless(schema, &[WORKER + 1, WORKER + 2], &notes);
        let (backfill, plain) = (&tabs[0], &tabs[1]);
        let row = block_on(plain.insert("notes", body("before"), Default::default()))
            .unwrap()
            .row_uuid();
        pump_all(&[backfill, plain, &worker]);
        plain.set_clock_for_test(Some(CLOCK + 10));
        block_on(plain.update("notes", row, body("plain"), Default::default())).unwrap();
        block_on(backfill.update(
            "notes",
            row,
            body("backfill"),
            jazz::db::UpdateOptions {
                updated_at_ms: Some(CLOCK as u64 + 20),
                ..Default::default()
            },
        ))
        .unwrap();
        let first = if backfill_first { backfill } else { plain };
        for db in [first, &worker, first] {
            block_on(db.tick()).unwrap();
        }
        pump_all(&[backfill, plain, &worker]);
        for db in [&worker, backfill, plain] {
            assert_eq!(
                note_bodies(db),
                ["plain"],
                "backfill first: {backfill_first}"
            );
        }
    }
}

/// Two tabs' writes at the same clock order by their writers' nodes:
/// every store shows the greater node's.
#[test]
fn equal_clocks_order_by_the_writers_node() {
    for greater_first in [false, true] {
        let notes = Query::from("notes");
        let (worker, tabs, _watches) = serverless(schema, &[WORKER + 1, WORKER + 2], &notes);
        let (lesser, greater) = (&tabs[0], &tabs[1]);
        let row = block_on(lesser.insert("notes", body("before"), Default::default()))
            .unwrap()
            .row_uuid();
        pump_all(&[lesser, greater, &worker]);
        block_on(lesser.update("notes", row, body("lesser"), Default::default())).unwrap();
        block_on(greater.update("notes", row, body("greater"), Default::default())).unwrap();
        let first = if greater_first { greater } else { lesser };
        for db in [first, &worker, first] {
            block_on(db.tick()).unwrap();
        }
        pump_all(&[lesser, greater, &worker]);
        for db in [&worker, lesser, greater] {
            assert_eq!(
                note_bodies(db),
                ["greater"],
                "greater first: {greater_first}"
            );
        }
    }
}

/// Two writers' pending versions of one row at the same commit HLC share
/// one `_t`: the history key's writer keeps both, ordered by (node, tx).
#[test]
fn equal_clock_versions_of_one_row_both_stay_in_history() {
    for greater_first in [false, true] {
        let notes = Query::from("notes");
        let (worker, tabs, _watches) = serverless(schema, &[WORKER + 1, WORKER + 2], &notes);
        let (lesser, greater) = (&tabs[0], &tabs[1]);
        let all = [lesser, greater, &worker];
        let row = block_on(lesser.insert("notes", body("before"), Default::default()))
            .unwrap()
            .row_uuid();
        pump_all(&all);
        block_on(lesser.update("notes", row, body("lesser"), Default::default())).unwrap();
        block_on(greater.update("notes", row, body("greater"), Default::default())).unwrap();
        let first = if greater_first { greater } else { lesser };
        for db in [first, &worker, first] {
            block_on(db.tick()).unwrap();
        }
        pump_all(&all);
        // A later write moves the current version into history too.
        worker.set_clock_for_test(Some(CLOCK + 100));
        block_on(worker.update("notes", row, body("later"), Default::default())).unwrap();
        pump_all(&all);

        let history = worker.history_keys_for_test("notes", row).unwrap();
        let shared: Vec<&(i64, Vec<u8>)> = history
            .iter()
            .filter(|(t, _)| history.iter().filter(|(u, _)| u == t).count() > 1)
            .collect();
        assert_eq!(
            shared.len(),
            2,
            "greater first: {greater_first}: {history:?}"
        );
        let writers: Vec<[u8; 16]> = shared
            .iter()
            .map(|(_, w)| {
                assert_eq!(w.len(), 24, "a pending version names its writer");
                w[..16].try_into().unwrap()
            })
            .collect();
        assert_eq!(
            writers,
            [[WORKER + 1; 16], [WORKER + 2; 16]],
            "ordered by node (greater first: {greater_first})"
        );
        for db in all {
            assert_eq!(note_bodies(db), ["later"], "greater first: {greater_first}");
        }
    }
}

/// A worker keeps its pending commits' order across a restart: a tab's
/// write delivered after the restart but made earlier goes under the one
/// it holds.
#[test]
fn a_workers_pending_commits_keep_their_order_across_a_restart() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("worker.sqlite");
    let open_worker = || {
        let worker = block_on(Db::open_at(
            DbConfig::new(
                schema(),
                DbIdentity {
                    node: NodeUuid::from_bytes([WORKER; 16]),
                    author: author(WORKER),
                },
            ),
            &path,
            jazz::db::FileOptions::default(),
        ))
        .unwrap();
        worker.set_clock_for_test(Some(CLOCK));
        worker.set_remote_link_hint(RemoteLinkHint::NoServer);
        worker
    };
    let attach = |worker: &Db, node: u8, clock: i64| {
        let tab = open(node, author(WORKER));
        tab.set_clock_for_test(Some(clock));
        tab.set_non_durable_client();
        tab.set_remote_link_hint(RemoteLinkHint::Live);
        let (client_side, server_side) = duplex();
        block_on(tab.connect_upstream(client_side));
        worker
            .accept_subscriber(server_side, author(WORKER))
            .unwrap();
        let prepared = tab.prepare_query(&Query::from("notes")).unwrap();
        let watch = block_on(tab.subscribe(&prepared, ReadOpts::default())).unwrap();
        (tab, watch)
    };
    let worker = open_worker();
    let row = block_on(worker.insert("notes", body("worker"), Default::default()))
        .unwrap()
        .row_uuid();
    let (early, _early_watch) = attach(&worker, WORKER + 1, CLOCK + 5);
    let (late, _late_watch) = attach(&worker, WORKER + 2, CLOCK + 10);
    pump_all(&[&early, &late, &worker]);
    // The earlier write stays with its tab; the later one reaches the
    // worker, which then restarts.
    block_on(early.update("notes", row, body("early"), Default::default())).unwrap();
    block_on(late.update("notes", row, body("late"), Default::default())).unwrap();
    pump_all(&[&late, &worker]);
    assert_eq!(note_bodies(&worker), ["late"]);
    drop((late, _late_watch));
    block_on(worker.close()).unwrap();

    let worker = open_worker();
    let (client_side, server_side) = duplex();
    block_on(early.connect_upstream(client_side));
    worker
        .accept_subscriber(server_side, author(WORKER))
        .unwrap();
    pump_all(&[&early, &worker]);
    assert_eq!(worker.pending_upload_count(), 3, "every commit is held");
    assert_eq!(note_bodies(&worker), ["late"], "the held commit is later");
    assert_eq!(note_bodies(&early), ["late"]);
}

/// Unsettled versions order by clock only until the authority's order
/// arrives: the authority accepts in upload order, and every store then
/// shows the version it accepted last, even where that flips a row
/// (SPEC 8 §8.13).
#[test]
fn a_row_shows_the_authoritys_order_once_its_versions_settle() {
    let notes = Query::from("notes");
    let (worker, tabs, _watches) = serverless(schema, &[WORKER + 1, WORKER + 2], &notes);
    let (early, late) = (&tabs[0], &tabs[1]);
    let row = block_on(early.insert("notes", body("before"), Default::default()))
        .unwrap()
        .row_uuid();
    pump_all(&[early, late, &worker]);
    let early_tx = block_on(early.update("notes", row, body("early"), Default::default()))
        .unwrap()
        .mergeable_tx_id();
    late.set_clock_for_test(Some(CLOCK + 10));
    let late_tx = block_on(late.update("notes", row, body("late"), Default::default()))
        .unwrap()
        .mergeable_tx_id();
    // The later write reaches the worker first, so it uploads first.
    for db in [late, &worker, late] {
        block_on(db.tick()).unwrap();
    }
    pump_all(&[early, late, &worker]);
    for db in [&worker, early, late] {
        assert_eq!(note_bodies(db), ["late"], "by clock while unsettled");
    }

    let core = open_in(schema(), 0xc0, AuthorSubject::SYSTEM);
    let (client_side, server_side) = duplex();
    let _up = block_on(worker.connect_upstream(client_side));
    core.accept_subscriber(server_side, author(WORKER)).unwrap();
    worker.set_remote_link_hint(RemoteLinkHint::Live);
    let all = [early, late, &worker, &core];
    for (db, tx) in [(early, early_tx), (late, late_tx)] {
        let mut wait = std::pin::pin!(db.wait_for_transaction(tx, DurabilityTier::Global));
        let mut context = std::task::Context::from_waker(std::task::Waker::noop());
        let mut settled = false;
        for _ in 0..20 {
            if let std::task::Poll::Ready(fate) = wait.as_mut().poll(&mut context) {
                fate.expect("the write settles");
                settled = true;
                break;
            }
            pump_all(&all);
        }
        assert!(settled, "tx {tx} settles");
    }
    pump_all(&all);
    for db in all {
        assert_eq!(note_bodies(db), ["early"], "the authority accepted it last");
    }
}

/// Todos branched by `branch_id`.
fn branched_schema() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("todos")
                    .column("branch_id", ColumnType::Uuid)
                    .column("title", ColumnType::Text)
                    .branch_by("branch_id")
                    .policies(allow_all_policies()),
            )
            .build(),
    )
}

fn branch(byte: u8) -> jazz::protocol::BranchSelector {
    jazz::protocol::BranchSelector::new([(
        "branch_id",
        Value::Uuid(uuid::Uuid::from_bytes([byte; 16])),
    )])
}

/// An upstream's later unsettled version of a row on one branch leaves a
/// tab's own pending write of that row on another branch showing: each
/// branch orders on its own.
#[test]
fn an_upstreams_later_version_on_one_branch_leaves_a_tabs_write_on_another() {
    let (k1, k2) = (branch(1), branch(2));
    let row = RowUuid::from_bytes([0x66; 16]);
    let view =
        |on: &jazz::protocol::BranchSelector| ReadOpts::default().branch_view(on.clone(), None);
    let todos = Query::from("todos");
    let worker = open_in(branched_schema(), WORKER, author(WORKER));
    worker.set_clock_for_test(Some(CLOCK));
    worker.set_remote_link_hint(RemoteLinkHint::NoServer);
    let mut tabs = Vec::new();
    let mut watches = Vec::new();
    for node in [WORKER + 1, WORKER + 2] {
        let tab = open_in(branched_schema(), node, author(WORKER));
        tab.set_clock_for_test(Some(CLOCK));
        tab.set_non_durable_client();
        tab.set_remote_link_hint(RemoteLinkHint::Live);
        let (client_side, server_side) = duplex();
        block_on(tab.connect_upstream(client_side));
        worker
            .accept_subscriber(server_side, author(WORKER))
            .unwrap();
        let prepared = tab.prepare_query(&todos).unwrap();
        for on in [&k1, &k2] {
            watches.push(block_on(tab.subscribe(&prepared, view(on))).unwrap());
        }
        tabs.push(tab);
    }
    let (writer, other) = (&tabs[0], &tabs[1]);
    let all = [writer, other, &worker];
    let title = |text: &str| BTreeMap::from([("title".to_owned(), Value::String(text.to_owned()))]);
    for (on, text) in [(&k1, "writer's on 1"), (&k2, "writer's on 2")] {
        block_on(writer.insert(
            "todos",
            title(text),
            jazz::db::InsertOptions {
                row_id: Some(row),
                target: jazz::db::ExactWriteTarget::Branch(on.clone()),
                ..Default::default()
            },
        ))
        .unwrap();
    }
    pump_all(&all);
    other.set_clock_for_test(Some(CLOCK + 10));
    block_on(other.update(
        "todos",
        row,
        title("other's on 1"),
        jazz::db::UpdateOptions {
            target: jazz::db::WriteTarget::BranchView {
                head: k1.clone(),
                base: None,
            },
            ..Default::default()
        },
    ))
    .unwrap();
    pump_all(&all);
    let titles = |db: &Db, on| -> Vec<String> {
        let prepared = db.prepare_query(&todos).unwrap();
        block_on(db.all(&prepared, view(on)))
            .unwrap()
            .iter()
            .filter_map(|row| match row.cell("title") {
                Some(Value::String(title)) => Some(title),
                _ => None,
            })
            .collect()
    };
    for db in all {
        assert_eq!(titles(db, &k1), ["other's on 1"], "the later write on 1");
        assert_eq!(titles(db, &k2), ["writer's on 2"], "the only write on 2");
    }
    drop(watches);
}
