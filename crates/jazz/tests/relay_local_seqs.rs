//! A worker and its tabs each number their own unsettled versions, from 1.
//! A tab's replica holds the worker's unsettled versions (other tabs'
//! writes the worker has not settled yet) next to its own; each is keyed
//! by the node that numbered it, so the two numberings never collide in a
//! tab's history.

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
    block_on(Db::open(DbConfig::new(
        schema,
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author,
        },
    )))
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

/// An upstream ships accepted versions and its own unsettled ones; a
/// version numbered by the tab's own node can only be a misbehaving
/// upstream's, so the tab refuses it rather than store it.
#[test]
fn a_tab_refuses_an_upstream_version_numbered_by_its_own_node() {
    let tab = open(WORKER + 1, author(WORKER));
    tab.set_non_durable_client();
    tab.set_remote_link_hint(RemoteLinkHint::Live);
    let (client_side, mut server_side) = duplex();
    block_on(tab.connect_upstream(client_side));
    server_side
        .send(SyncMessage::Rows {
            images: vec![Image {
                table: "notes".to_owned(),
                id: [7; 16],
                key: Vec::new(),
                seq: 1,
                by: Some([WORKER + 1; 16]),
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

/// A worker with no server settles a tab's commit at a seq of its own. A
/// serializable transaction the tab began while that commit was unsettled
/// still counts it as its own, wherever it landed.
#[test]
fn a_tabs_transaction_over_its_own_commit_settled_by_a_worker_without_a_server_commits() {
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
    // The worker's own commit first, so it settles the tab's at another seq.
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
        .expect("the tab's own settled insert is no conflict");
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
