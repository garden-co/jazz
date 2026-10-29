//! Whether an accepted write maybe conflicts (SPEC 4 §4.6, INV-HIST-21):
//! the authority derives it from the row's accepted history and the base
//! each write carries, and never stores it. A write is maybe conflicting
//! when another writer's accepted write lies between what its writer had
//! seen of the row and the write; its conflict analysis names those of them
//! whose authored columns overlap its own. A writer's own unsettled writes
//! never count against it, inserts and writes over no held image are never
//! maybe conflicting, and through a worker each tab is its own writer.

use crate::common;

#[path = "common/duplex_transport.rs"]
mod duplex_transport;

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

use duplex_transport::duplex;
use jazz::db::{
    BuiltOn, Db, DbConfig, DbIdentity, ExactWriteTarget, FileOptions, InsertOptions, LOCAL_BASE,
    ReadOpts, Reason, RemoteLinkHint, SubscriptionStream, SyncMessage, UpdateOptions, Upstream,
    WireBase, WireKind, WireOp, WireTarget, WireWrite, WriteConflict, WriteTarget, block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::protocol::BranchSelector;
use jazz::query::Query;
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};
use jazz::tx::DurabilityTier;

use common::{allow_all_policies, compile_schema};

const MAX_TURNS: usize = 400;
const CORE: u8 = 0xc0;
const WORKER: u8 = 0xa0;

fn schema() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("todos")
                    .column("title", ColumnType::Text)
                    .column("note", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new("drafts")
                    .column("branch_id", ColumnType::Uuid)
                    .column("title", ColumnType::Text)
                    .branch_by("branch_id")
                    .policies(allow_all_policies()),
            )
            .build(),
    )
}

fn draft_branch() -> BranchSelector {
    BranchSelector::new([("branch_id", Value::Uuid(uuid::Uuid::from_bytes([0xd5; 16])))])
}

fn open(node: u8, author: AuthorSubject) -> Db {
    block_on(Db::open(DbConfig::new(
        schema(),
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

fn cells(pairs: &[(&str, &str)]) -> BTreeMap<String, Value> {
    pairs
        .iter()
        .map(|(column, value)| ((*column).to_owned(), Value::String((*value).to_owned())))
        .collect()
}

/// A client's write: its node and its tx, as the authority names writers.
type Writer = ([u8; 16], u64);

/// One client of the network: its store, its link, and the subscription
/// that keeps the todos it holds current.
struct Client {
    node: u8,
    db: Db,
    up: Option<Upstream>,
    via: Option<usize>,
    _todos: Option<SubscriptionStream>,
}

/// The authority, clients connected to it directly, and optionally a worker
/// whose tabs reach the authority through it.
struct Net {
    core: Db,
    worker: Option<Client>,
    clients: Vec<Client>,
}

impl Net {
    fn new(clients: usize) -> Self {
        Self::around(open(CORE, AuthorSubject::SYSTEM), clients)
    }

    /// `clients` direct clients around authority `core`.
    fn around(core: Db, clients: usize) -> Self {
        let mut net = Net {
            core,
            worker: None,
            clients: Vec::new(),
        };
        for i in 0..clients {
            net.add_client(0x50 + i as u8, None);
        }
        net
    }

    /// A worker connected to the authority, with `tabs` tabs behind it,
    /// after the `direct` direct clients.
    fn with_worker(direct: usize, tabs: usize) -> Self {
        let mut net = Self::new(direct);
        net.add_worker();
        for i in 0..tabs {
            net.add_client(WORKER + 1 + i as u8, Some(0));
        }
        net
    }

    fn add_worker(&mut self) {
        let worker = open(WORKER, author(WORKER));
        worker.set_remote_link_hint(RemoteLinkHint::Live);
        self.worker = Some(Client {
            node: WORKER,
            db: worker,
            up: None,
            via: None,
            _todos: None,
        });
        self.connect_worker();
    }

    fn add_client(&mut self, node: u8, via: Option<usize>) {
        let identity = if via.is_some() { WORKER } else { node };
        let db = open(node, author(identity));
        if via.is_some() {
            db.set_non_durable_client();
        }
        db.set_remote_link_hint(RemoteLinkHint::Live);
        self.clients.push(Client {
            node,
            db,
            up: None,
            via,
            _todos: None,
        });
        let i = self.clients.len() - 1;
        self.connect(i);
        let prepared = self.clients[i]
            .db
            .prepare_query(&Query::from("todos"))
            .unwrap();
        let stream = self.drive(self.clients[i].db.subscribe(&prepared, ReadOpts::default()));
        self.clients[i]._todos = Some(stream.unwrap());
        self.pump();
    }

    fn connect(&mut self, i: usize) {
        let (client_side, server_side) = duplex();
        let client = &mut self.clients[i];
        client.up = Some(block_on(client.db.connect_upstream(client_side)));
        match client.via {
            None => self
                .core
                .accept_subscriber(server_side, author(client.node)),
            Some(_) => self
                .worker
                .as_ref()
                .expect("worker")
                .db
                .accept_subscriber(server_side, author(WORKER)),
        };
    }

    fn connect_worker(&mut self) {
        let (client_side, server_side) = duplex();
        let worker = self.worker.as_mut().expect("worker");
        worker.up = Some(block_on(worker.db.connect_upstream(client_side)));
        self.core.accept_subscriber(server_side, author(WORKER));
    }

    fn worker_offline(&mut self) {
        let worker = self.worker.as_mut().expect("worker");
        let up = worker.up.take().expect("online");
        assert!(worker.db.detach_connection(&up));
        worker.db.set_remote_link_hint(RemoteLinkHint::Failed);
    }

    fn worker_online(&mut self) {
        self.connect_worker();
        let worker = self.worker.as_ref().expect("worker");
        worker.db.set_remote_link_hint(RemoteLinkHint::Live);
        self.pump();
    }

    fn offline(&mut self, i: usize) {
        let client = &mut self.clients[i];
        let up = client.up.take().expect("online");
        assert!(client.db.detach_connection(&up));
        client.db.set_remote_link_hint(RemoteLinkHint::Failed);
    }

    fn online(&mut self, i: usize) {
        self.connect(i);
        self.clients[i]
            .db
            .set_remote_link_hint(RemoteLinkHint::Live);
        self.pump();
    }

    fn db(&self, i: usize) -> &Db {
        &self.clients[i].db
    }

    fn pump(&self) {
        for _ in 0..3 {
            for client in &self.clients {
                block_on(client.db.tick()).unwrap();
            }
            if let Some(worker) = &self.worker {
                block_on(worker.db.tick()).unwrap();
            }
            block_on(self.core.tick()).unwrap();
            if let Some(worker) = &self.worker {
                block_on(worker.db.tick()).unwrap();
            }
            for client in &self.clients {
                block_on(client.db.tick()).unwrap();
            }
        }
    }

    fn drive<T>(&self, future: impl Future<Output = T>) -> T {
        let mut future = pin!(future);
        let mut context = Context::from_waker(Waker::noop());
        for _ in 0..MAX_TURNS {
            if let Poll::Ready(output) = future.as_mut().poll(&mut context) {
                return output;
            }
            self.pump();
        }
        panic!("operation did not complete within {MAX_TURNS} turns");
    }

    fn settle(&self, i: usize, tx: u64) {
        self.drive(self.db(i).wait_for_transaction(tx, DurabilityTier::Global))
            .expect("the write settles");
    }

    fn writer(&self, i: usize, tx: u64) -> Writer {
        ([self.clients[i].node; 16], tx)
    }

    /// A todo client `i` inserts, settled and held by every client.
    fn seed(&self, i: usize) -> RowUuid {
        let write = block_on(self.db(i).insert(
            "todos",
            cells(&[("title", "seed"), ("note", "seed")]),
            Default::default(),
        ))
        .unwrap();
        self.settle(i, write.mergeable_tx_id());
        self.pump();
        write.row_uuid()
    }

    /// Client `i` edits `row`; returns its writer. It does not wait.
    fn edit(&self, i: usize, row: RowUuid, pairs: &[(&str, &str)]) -> Writer {
        let write = block_on(
            self.db(i)
                .update("todos", row, cells(pairs), Default::default()),
        )
        .unwrap();
        self.writer(i, write.mergeable_tx_id())
    }

    /// Client `i` edits `row` and waits until the edit settles.
    fn edit_settled(&self, i: usize, row: RowUuid, pairs: &[(&str, &str)]) -> Writer {
        let writer = self.edit(i, row, pairs);
        self.settle(i, writer.1);
        self.pump();
        writer
    }

    fn conflicts(&self, row: RowUuid) -> Vec<WriteConflict> {
        self.core.write_conflicts("todos", row, None).unwrap()
    }

    fn conflict(&self, row: RowUuid, writer: Writer) -> WriteConflict {
        self.conflicts(row)
            .into_iter()
            .find(|conflict| conflict.writer == writer)
            .unwrap_or_else(|| panic!("no accepted write by {writer:?}"))
    }

    /// The accepted writes the worker uploaded for its tabs, in the order
    /// it relayed them: the authority knows a relayed write by the
    /// worker's commit.
    fn relayed(&self, row: RowUuid) -> Vec<WriteConflict> {
        self.conflicts(row)
            .into_iter()
            .filter(|conflict| conflict.writer.0 == [WORKER; 16])
            .collect()
    }

    fn title(&self, row: RowUuid) -> Option<Value> {
        let prepared = self.core.prepare_query(&Query::from("todos")).unwrap();
        block_on(self.core.all(&prepared, ReadOpts::default()))
            .unwrap()
            .into_iter()
            .find(|current| current.row_uuid() == row)
            .and_then(|current| current.cell("title"))
    }
}

fn writers(overlapping: &[(u64, Writer)]) -> Vec<Writer> {
    overlapping.iter().map(|(_, writer)| *writer).collect()
}

#[test]
fn concurrent_overlapping_writes_resolve_by_arrival_and_the_later_names_the_earlier() {
    let mut net = Net::new(2);
    let row = net.seed(0);
    net.offline(0);
    net.offline(1);
    let alice = net.edit(0, row, &[("title", "alice")]);
    let bob = net.edit(1, row, &[("title", "bob")]);
    net.online(0);
    net.settle(0, alice.1);
    net.online(1);
    net.settle(1, bob.1);

    assert_eq!(net.title(row), Some(Value::String("bob".to_owned())));
    let alice_conflict = net.conflict(row, alice);
    assert!(
        !alice_conflict.maybe_conflicting,
        "alice wrote over the latest image"
    );
    assert_eq!(alice_conflict.resolved_base, alice_conflict.previous);
    let bob_conflict = net.conflict(row, bob);
    assert!(
        bob_conflict.maybe_conflicting,
        "bob never saw alice's title"
    );
    assert_eq!(bob_conflict.previous, Some(alice_conflict.seq));
    assert_ne!(bob_conflict.resolved_base, bob_conflict.previous);
    assert_eq!(writers(&bob_conflict.overlapping), vec![alice]);
}

#[test]
fn concurrent_disjoint_writes_both_apply_and_overlap_nothing() {
    let mut net = Net::new(2);
    let row = net.seed(0);
    net.offline(1);
    let alice = net.edit_settled(0, row, &[("title", "alice")]);
    let bob = net.edit(1, row, &[("note", "bob")]);
    net.online(1);
    net.settle(1, bob.1);

    assert!(!net.conflict(row, alice).maybe_conflicting);
    let bob_conflict = net.conflict(row, bob);
    assert!(
        bob_conflict.maybe_conflicting,
        "bob never saw alice's write"
    );
    assert!(
        bob_conflict.overlapping.is_empty(),
        "alice authored only the title, bob only the note"
    );
}

#[test]
fn offline_chain_applies_in_order_and_flags_only_interrupted_writes() {
    let mut net = Net::new(2);
    let row = net.seed(0);
    // An uninterrupted offline chain: nobody else wrote in between.
    net.offline(0);
    let chain: Vec<Writer> = (0..50)
        .map(|i| net.edit(0, row, &[("title", &format!("carol {i}"))]))
        .collect();
    net.online(0);
    net.settle(0, chain.last().unwrap().1);
    assert_eq!(net.title(row), Some(Value::String("carol 49".to_owned())));
    for writer in &chain {
        let conflict = net.conflict(row, *writer);
        assert!(
            !conflict.maybe_conflicting,
            "only carol's own chain lies before it"
        );
        assert_eq!(conflict.resolved_base, conflict.previous);
        assert!(conflict.overlapping.is_empty());
    }

    // The same chain, interrupted by dave after it went offline: every
    // write of it overrode the row without having seen dave's write.
    net.offline(0);
    let interrupted: Vec<Writer> = (0..20)
        .map(|i| {
            let column = if i % 2 == 0 { "title" } else { "note" };
            net.edit(0, row, &[(column, &format!("again {i}"))])
        })
        .collect();
    let dave = net.edit_settled(1, row, &[("title", "dave")]);
    net.online(0);
    net.settle(0, interrupted.last().unwrap().1);
    assert!(!net.conflict(row, dave).maybe_conflicting);
    for (i, writer) in interrupted.iter().enumerate() {
        let conflict = net.conflict(row, *writer);
        assert!(conflict.maybe_conflicting, "write {i} never saw dave's");
        let expected = if i % 2 == 0 { vec![dave] } else { Vec::new() };
        assert_eq!(writers(&conflict.overlapping), expected, "write {i}");
    }
}

#[test]
fn offline_chain_over_several_rows_and_transactions_flags_each_row() {
    let mut net = Net::new(2);
    let first = net.seed(0);
    let second = net.seed(0);
    net.offline(0);
    let tx = block_on(net.db(0).mergeable_tx()).unwrap();
    block_on(tx.update(
        "todos",
        first,
        cells(&[("title", "c1")]),
        Default::default(),
    ))
    .unwrap();
    block_on(tx.update(
        "todos",
        second,
        cells(&[("title", "c2")]),
        Default::default(),
    ))
    .unwrap();
    let both = block_on(tx.commit()).unwrap().0;
    let later = net.edit(0, second, &[("note", "c3")]);
    let dave = net.edit_settled(1, second, &[("note", "dave")]);
    net.online(0);
    net.settle(0, later.1);

    let both = net.writer(0, both);
    assert!(
        !net.conflict(first, both).maybe_conflicting,
        "nobody else wrote the first row"
    );
    let on_second = net.conflict(second, both);
    assert!(on_second.maybe_conflicting, "carol never saw dave's note");
    assert!(
        on_second.overlapping.is_empty(),
        "dave wrote the note, carol the title"
    );
    let later_conflict = net.conflict(second, later);
    assert!(later_conflict.maybe_conflicting);
    assert_eq!(writers(&later_conflict.overlapping), vec![dave]);
}

#[test]
fn a_write_built_on_a_rejected_write_is_rejected_too() {
    let mut net = Net::new(1);
    let row = net.seed(0);
    let far_ahead = web_time_now_ms() + 24 * 60 * 60 * 1000;
    // The predecessor is refused (its timestamp is a day ahead of the
    // authority's clock); the write built on it carries its title, so it
    // is refused too, and the row stays as seeded.
    net.offline(0);
    let refused = block_on(net.db(0).update(
        "todos",
        row,
        cells(&[("title", "from the future")]),
        UpdateOptions {
            updated_at_ms: Some(far_ahead),
            ..Default::default()
        },
    ))
    .unwrap();
    let chained = net.edit(0, row, &[("note", "chained")]);
    net.online(0);
    let fate = |tx| net.drive(net.db(0).wait_for_transaction(tx, DurabilityTier::Global));
    assert!(fate(refused.mergeable_tx_id()).is_err());
    let error = fate(chained.1).expect_err("built on a rejected write");
    assert_eq!(error.reason, Some(Reason::PredecessorRejected), "{error:?}");
    assert_eq!(net.title(row), Some(Value::String("seed".to_owned())));
    assert_eq!(net.conflicts(row).len(), 1, "only the seed was accepted");

    // A later write over the settled row applies.
    net.pump();
    let after = net.edit_settled(0, row, &[("note", "after")]);
    assert!(!net.conflict(row, after).maybe_conflicting);
}

fn web_time_now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64
}

#[test]
fn edit_after_an_accepted_delete_stays_deleted_and_is_flagged() {
    let mut net = Net::new(2);
    let row = net.seed(0);
    net.offline(0);
    let edit = net.edit(0, row, &[("title", "carol")]);
    let delete = block_on(net.db(1).delete("todos", row, Default::default())).unwrap();
    net.settle(1, delete.mergeable_tx_id());
    let delete = net.writer(1, delete.mergeable_tx_id());
    net.online(0);
    net.settle(0, edit.1);

    assert_eq!(net.title(row), None, "the row stays deleted");
    let conflict = net.conflict(row, edit);
    assert!(
        conflict.maybe_conflicting,
        "the delete came after carol's base"
    );
    assert!(
        conflict.overlapping.is_empty(),
        "the delete authored the deletion, carol the title"
    );
    assert_eq!(conflict.previous, Some(net.conflict(row, delete).seq));
}

#[test]
fn inserts_and_writes_over_no_held_image_are_never_flagged() {
    let mut net = Net::new(2);
    let row = net.seed(0);
    // Bob upserts a row he holds no image of while alice writes it: his
    // write has no base.
    let unseen = RowUuid::from_bytes([0x77; 16]);
    let alice = block_on(net.db(0).insert(
        "todos",
        cells(&[("title", "alice"), ("note", "alice")]),
        jazz::db::InsertOptions {
            row_id: Some(unseen),
            ..Default::default()
        },
    ))
    .unwrap();
    net.offline(1);
    net.settle(0, alice.mergeable_tx_id());
    let blind = block_on(net.db(1).upsert(
        "todos",
        unseen,
        cells(&[("title", "bob"), ("note", "bob")]),
        Default::default(),
    ))
    .unwrap();
    let blind = net.writer(1, blind.mergeable_tx_id());
    net.online(1);
    net.settle(1, blind.1);
    let conflict = net.conflict(unseen, blind);
    assert!(!conflict.maybe_conflicting, "bob held no image of the row");
    assert_eq!(conflict.resolved_base, None);
    assert!(conflict.overlapping.is_empty());

    // Inserts never are.
    for conflict in net.conflicts(row).iter().take(1) {
        assert!(!conflict.maybe_conflicting);
        assert_eq!(conflict.resolved_base, None);
        assert_eq!(conflict.previous, None);
    }
    assert!(
        !net.conflict(unseen, net.writer(0, alice.mergeable_tx_id()))
            .maybe_conflicting
    );
}

#[test]
fn chained_write_over_a_foreign_edit_its_writer_never_saw_is_flagged() {
    let mut net = Net::new(2);
    let row = net.seed(0);
    net.offline(0);
    let p1 = net.edit(0, row, &[("title", "p1")]);
    let dave = net.edit_settled(1, row, &[("note", "dave")]);
    let w = net.edit(0, row, &[("note", "w")]);
    net.online(0);
    net.settle(0, w.1);

    let p1_conflict = net.conflict(row, p1);
    assert!(p1_conflict.maybe_conflicting);
    assert!(p1_conflict.overlapping.is_empty());
    let w_conflict = net.conflict(row, w);
    assert!(
        w_conflict.maybe_conflicting,
        "carol's chain rests on the seed, not on dave's note"
    );
    assert_eq!(writers(&w_conflict.overlapping), vec![dave]);
    assert_eq!(
        w_conflict.resolved_base,
        Some(net.conflicts(row)[0].seq),
        "the newest version carol had seen is the seed"
    );
}

#[test]
fn chained_write_over_a_predecessor_accepted_before_a_foreign_edit_is_not_flagged() {
    let net = Net::new(2);
    let row = net.seed(0);
    // Carol's predecessor lands first, then she writes again over the image
    // she now holds: dave's write after both is not in her way.
    let p1 = net.edit_settled(0, row, &[("title", "p1")]);
    let w = net.edit_settled(0, row, &[("title", "w")]);
    let dave = net.edit_settled(1, row, &[("title", "dave")]);
    for writer in [p1, w, dave] {
        let conflict = net.conflict(row, writer);
        assert!(
            !conflict.maybe_conflicting,
            "{writer:?} wrote over the latest image"
        );
        assert_eq!(conflict.resolved_base, conflict.previous);
    }
}

#[test]
fn every_write_of_an_interrupted_chain_names_the_foreign_writes_it_overlaps() {
    let mut net = Net::new(3);
    let row = net.seed(0);
    net.offline(0);
    let titles = net.edit(0, row, &[("title", "carol")]);
    let notes = net.edit(0, row, &[("note", "carol")]);
    let both = net.edit(0, row, &[("title", "carol 2"), ("note", "carol 2")]);
    let dave = net.edit_settled(1, row, &[("title", "dave")]);
    let erin = net.edit_settled(2, row, &[("note", "erin")]);
    net.online(0);
    net.settle(0, both.1);

    assert!(!net.conflict(row, dave).maybe_conflicting);
    assert!(
        !net.conflict(row, erin).maybe_conflicting,
        "erin saw dave's write"
    );
    assert_eq!(writers(&net.conflict(row, titles).overlapping), vec![dave]);
    assert_eq!(writers(&net.conflict(row, notes).overlapping), vec![erin]);
    assert_eq!(
        writers(&net.conflict(row, both).overlapping),
        vec![dave, erin]
    );
    for writer in [titles, notes, both] {
        assert!(net.conflict(row, writer).maybe_conflicting);
    }
}

#[test]
fn a_tabs_offline_chain_through_its_worker_is_its_own() {
    let mut net = Net::with_worker(1, 2);
    let row = net.seed(0);
    net.worker_offline();
    let first = net.edit(1, row, &[("title", "tab 1")]);
    let second = net.edit(1, row, &[("title", "tab 2")]);
    net.worker_online();
    net.settle(1, second.1);

    let _ = first;
    let relayed = net.relayed(row);
    assert_eq!(relayed.len(), 2);
    for conflict in relayed {
        assert!(
            !conflict.maybe_conflicting,
            "only the tab's own chain lies before it"
        );
        assert_eq!(conflict.resolved_base, conflict.previous);
    }
}

#[test]
fn a_tabs_write_through_its_worker_is_flagged_over_a_write_it_never_saw() {
    let mut net = Net::with_worker(1, 2);
    let row = net.seed(0);
    net.worker_offline();
    let tab = net.edit(1, row, &[("title", "tab")]);
    let direct = net.edit_settled(0, row, &[("title", "direct")]);
    // The other tab saw the first tab's unsettled write through the worker:
    // a writer's chain is its own writes, so the first tab's counts.
    let other = net.edit(2, row, &[("note", "other tab")]);
    net.worker_online();
    net.settle(1, tab.1);
    net.settle(2, other.1);

    assert!(!net.conflict(row, direct).maybe_conflicting);
    let [tab_conflict, other_conflict] = <[_; 2]>::try_from(net.relayed(row)).unwrap();
    assert!(
        tab_conflict.maybe_conflicting,
        "the tab never saw the direct write"
    );
    assert_eq!(writers(&tab_conflict.overlapping), vec![direct]);
    assert!(other_conflict.maybe_conflicting);
    assert!(
        other_conflict.overlapping.is_empty(),
        "the other tab wrote only the note"
    );
}

#[test]
fn a_relays_commit_a_write_was_built_on_is_what_its_writer_saw() {
    // A raw relay link: what a worker uploads for its tabs, its bases
    // renamed to the worker's own commits.
    let core = open(CORE, AuthorSubject::SYSTEM);
    let (mut relay, server_side) = duplex();
    core.accept_subscriber(server_side, AuthorSubject::SYSTEM);
    let relay_node = [0x77; 16];
    let row = block_on(core.insert(
        "todos",
        cells(&[("title", "seed"), ("note", "seed")]),
        Default::default(),
    ))
    .unwrap()
    .row_uuid();
    let seed = core.write_conflicts("todos", row, None).unwrap()[0].seq;
    // Accepted before the relay's first upload, which never saw it.
    block_on(core.update(
        "todos",
        row,
        cells(&[("title", "core")]),
        Default::default(),
    ))
    .unwrap();
    let commit = |tx: u64, target: [u8; 16], title: &str, on: BuiltOn| SyncMessage::Commit {
        node: relay_node,
        tx,
        kind: WireKind::Mergeable,
        ops: vec![WireOp {
            bases: vec![WireBase {
                key: Vec::new(),
                on,
            }],
            ..WireWrite::Update {
                table: "todos".to_owned(),
                row: target,
                cells: cells(&[("title", title)]),
                target: WireTarget::Root,
                at: None,
            }
            .into()
        }],
        author: None,
        session: None,
        origin: Some(([0xa1; 16], LOCAL_BASE + tx)),
    };
    let tx = |n: u64| LOCAL_BASE + n;
    relay
        .send(commit(
            tx(1),
            *row.0.as_bytes(),
            "first",
            BuiltOn::Seq(seed),
        ))
        .unwrap();
    // Another session was shipped the relay's version of the first.
    let shipped = BuiltOn::Commit {
        node: relay_node,
        tx: tx(1),
    };
    relay
        .send(commit(tx(2), *row.0.as_bytes(), "second", shipped))
        .unwrap();
    // A commit the relay never sent, and one the authority rejected (it
    // updates a row that does not exist).
    let never = BuiltOn::Commit {
        node: relay_node,
        tx: tx(99),
    };
    relay
        .send(commit(tx(3), *row.0.as_bytes(), "unknown", never))
        .unwrap();
    relay
        .send(commit(tx(4), [0xee; 16], "missing", BuiltOn::Seq(seed)))
        .unwrap();
    let on_rejected = BuiltOn::Commit {
        node: relay_node,
        tx: tx(4),
    };
    relay
        .send(commit(
            tx(5),
            *row.0.as_bytes(),
            "over rejected",
            on_rejected,
        ))
        .unwrap();
    // Another node's commit: a base names only its sender's own.
    let foreign = BuiltOn::Commit {
        node: [0x99; 16],
        tx: tx(1),
    };
    relay
        .send(commit(tx(6), *row.0.as_bytes(), "foreign", foreign))
        .unwrap();
    block_on(core.tick()).unwrap();

    let mut fates = BTreeMap::new();
    while let Some(message) = relay.try_recv() {
        if let SyncMessage::Fate { tx, outcome, .. } = message {
            fates.insert(tx, outcome.result());
        }
    }
    assert!(fates[&tx(1)].is_ok() && fates[&tx(2)].is_ok());
    assert!(fates[&tx(4)].is_err());
    // A commit never received counts as rejected: its successor goes too.
    for over in [tx(3), tx(5)] {
        let fate = format!("{:?}", fates[&over]);
        assert!(fate.contains("predecessor_rejected"), "{fate}");
    }
    let fate = format!("{:?}", fates[&tx(6)]);
    assert!(
        fates[&tx(6)].is_err() && !fate.contains("predecessor_rejected"),
        "{fate}"
    );

    let conflicts = core.write_conflicts("todos", row, None).unwrap();
    assert_eq!(conflicts.len(), 4, "seed, core, first, second");
    let core_write = conflicts[1].writer;
    assert_eq!(core_write.0, [CORE; 16]);
    assert_eq!(conflicts[2].writer, (relay_node, tx(1)));
    assert_eq!(writers(&conflicts[2].overlapping), vec![core_write]);
    let second = &conflicts[3];
    assert_eq!(second.writer, (relay_node, tx(2)));
    assert!(second.maybe_conflicting);
    assert_eq!(
        writers(&second.overlapping),
        vec![core_write],
        "the first write was in the version it was shipped; the core's was not"
    );
    assert_eq!(second.resolved_base, Some(seed));
}

#[test]
fn a_session_the_relay_may_not_write_as_is_not_its_writer_and_keeps_its_base() {
    // A direct client claims a node first; a tab behind the worker then
    // uses the same node, which the worker's identity may not write as.
    let mut net = Net::new(0);
    net.add_client(WORKER + 1, None);
    let row = net.seed(0);
    net.add_worker();
    net.add_client(WORKER + 1, Some(0));
    // The tab writes over the seed; a write it never saw lands first.
    net.worker_offline();
    let tab_write =
        block_on(
            net.db(1)
                .update("todos", row, cells(&[("title", "tab")]), Default::default()),
        )
        .unwrap();
    net.edit_settled(0, row, &[("title", "claimer")]);
    net.worker_online();
    net.pump();
    assert_eq!(
        net.title(row),
        Some(Value::String("tab".to_owned())),
        "the tab's edit landed"
    );
    let _ = tab_write;

    let conflicts = net.conflicts(row);
    assert_eq!(
        conflicts
            .iter()
            .filter(|conflict| conflict.writer.0 == [WORKER + 1; 16])
            .count(),
        2,
        "only the claiming client's seed and edit name its node: the tab's commit is the worker's"
    );
    let worker = conflicts
        .iter()
        .find(|conflict| conflict.writer.0 == [WORKER; 16])
        .expect("the worker wrote the tab's edit");
    assert!(
        worker.maybe_conflicting,
        "the base is the worker's to name: the tab never saw the claimer's edit"
    );
}

#[test]
fn the_authoritys_own_write_counts_against_a_client_and_authors_every_column() {
    let mut net = Net::new(1);
    let row = net.seed(0);
    net.offline(0);
    let client = net.edit(0, row, &[("note", "client")]);
    block_on(net.core.update(
        "todos",
        row,
        cells(&[("title", "core")]),
        Default::default(),
    ))
    .unwrap();
    net.online(0);
    net.settle(0, client.1);

    let conflict = net.conflict(row, client);
    assert!(conflict.maybe_conflicting);
    let overlapping = writers(&conflict.overlapping);
    assert_eq!(overlapping.len(), 1, "{overlapping:?}");
    assert_eq!(
        overlapping[0].0, [CORE; 16],
        "the authority's write authors the note too"
    );
}

#[test]
fn a_branched_rows_writes_are_derived_on_its_branch() {
    let mut net = Net::new(1);
    let branch = draft_branch();
    let row = RowUuid::from_bytes([0xd6; 16]);
    let insert = block_on(net.db(0).insert(
        "drafts",
        cells(&[("title", "seed")]),
        InsertOptions {
            row_id: Some(row),
            target: ExactWriteTarget::Branch(branch.clone()),
            ..Default::default()
        },
    ))
    .unwrap();
    net.settle(0, insert.mergeable_tx_id());
    net.pump();
    net.offline(0);
    let on_branch = UpdateOptions {
        target: WriteTarget::BranchView {
            head: branch.clone(),
            base: None,
        },
        ..Default::default()
    };
    let edit = block_on(net.db(0).update(
        "drafts",
        row,
        cells(&[("title", "client")]),
        on_branch.clone(),
    ))
    .unwrap();
    block_on(
        net.core
            .update("drafts", row, cells(&[("title", "core")]), on_branch),
    )
    .unwrap();
    net.online(0);
    net.settle(0, edit.mergeable_tx_id());

    let conflicts = net
        .core
        .write_conflicts("drafts", row, Some(&branch))
        .unwrap();
    let client = conflicts
        .iter()
        .find(|conflict| conflict.writer == net.writer(0, edit.mergeable_tx_id()))
        .expect("the client's edit landed on the branch");
    assert!(client.maybe_conflicting);
    assert_eq!(client.overlapping.len(), 1);
    assert_eq!(client.overlapping[0].1.0, [CORE; 16]);
    assert!(
        net.core
            .write_conflicts("drafts", row, None)
            .unwrap()
            .is_empty(),
        "the row has no unbranched versions"
    );
}

#[test]
fn a_reopened_authority_still_derives_what_it_recorded() {
    let dir = std::env::temp_dir().join(format!("jazz-maybe-conflicting-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("core.sqlite");
    let open_core = || {
        block_on(Db::open_at(
            DbConfig::new(
                schema(),
                DbIdentity {
                    node: NodeUuid::from_bytes([CORE; 16]),
                    author: AuthorSubject::SYSTEM,
                },
            ),
            &path,
            FileOptions::default(),
        ))
        .unwrap()
    };
    let mut net = Net::around(open_core(), 2);
    let row = net.seed(0);
    net.offline(1);
    let alice = net.edit_settled(0, row, &[("title", "alice")]);
    let bob = net.edit(1, row, &[("title", "bob")]);
    net.online(1);
    net.settle(1, bob.1);
    let before = net.conflicts(row);
    drop(net);

    let core = open_core();
    let after = core.write_conflicts("todos", row, None).unwrap();
    assert_eq!(after, before);
    let bob_conflict = after.iter().find(|c| c.writer == bob).unwrap();
    assert!(bob_conflict.maybe_conflicting);
    assert_eq!(writers(&bob_conflict.overlapping), vec![alice]);
    drop(core);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn a_tab_built_on_another_tabs_unsettled_write_saw_it() {
    let mut net = Net::with_worker(1, 2);
    let row = net.seed(0);
    net.worker_offline();
    net.edit(1, row, &[("title", "tab 1")]);
    net.pump();
    // The second tab holds the worker's unsettled version of the first
    // tab's write, and edits the same column over it.
    let direct = net.edit_settled(0, row, &[("title", "direct")]);
    net.edit(2, row, &[("title", "tab 2")]);
    net.worker_online();
    net.pump();
    net.pump();

    let [first, second] = <[_; 2]>::try_from(net.relayed(row)).unwrap();
    assert_eq!(writers(&first.overlapping), vec![direct]);
    assert!(second.maybe_conflicting, "it never saw the direct write");
    assert_eq!(
        writers(&second.overlapping),
        vec![direct],
        "the first tab's write was in the version it was built on"
    );
    assert_eq!(net.title(row), Some(Value::String("tab 2".to_owned())));
}

#[test]
fn a_merge_base_is_the_version_the_write_was_built_on_when_it_missed_the_column() {
    let mut net = Net::new(2);
    let row = net.seed(0);
    let base = |net: &Net, writer: Writer, column: &str| {
        let seq = net.conflict(row, writer).seq;
        net.core.merge_base("todos", row, seq, column).unwrap()
    };
    let text = |value: &str| Some(Value::String(value.to_owned()));

    // Nothing missed: the fast path, no base.
    let alone = net.edit_settled(0, row, &[("title", "alone")]);
    assert_eq!(base(&net, alone, "title"), None);

    // Bob missed alice's title: his title merges over the version he
    // held; his note missed nothing that wrote the note.
    net.offline(1);
    let alice = net.edit_settled(0, row, &[("title", "alice")]);
    let bob = net.edit(1, row, &[("title", "bob"), ("note", "bob")]);
    net.online(1);
    net.settle(1, bob.1);
    let _ = alice;
    assert_eq!(base(&net, bob, "title"), text("alone"));
    assert_eq!(base(&net, bob, "note"), None);

    // A chain: the second write was built on the first as it was sent.
    net.pump();
    net.offline(1);
    let first = net.edit(1, row, &[("title", "first")]);
    let second = net.edit(1, row, &[("title", "second")]);
    net.edit_settled(0, row, &[("title", "carol")]);
    net.online(1);
    net.settle(1, second.1);
    assert_eq!(base(&net, first, "title"), text("bob"));
    assert_eq!(base(&net, second, "title"), text("first"));

    // A base over the inline threshold is stored chunked and read back whole.
    let large = "large ".repeat(20_000);
    net.pump();
    net.edit_settled(0, row, &[("title", large.as_str())]);
    net.pump();
    net.offline(1);
    net.edit_settled(0, row, &[("title", "dave")]);
    let eve = net.edit(1, row, &[("title", "eve")]);
    net.online(1);
    net.settle(1, eve.1);
    assert_eq!(base(&net, eve, "title"), text(&large));
}

#[test]
fn a_rejection_cascades_through_a_worker_down_a_three_deep_chain() {
    let mut net = Net::with_worker(1, 2);
    let row = net.seed(0);
    let far_ahead = web_time_now_ms() + 24 * 60 * 60 * 1000;
    // Tab 1 is refused; its chained write, and tab 2's write over that
    // (seen through the worker), carry its title, so all three go.
    net.worker_offline();
    let refused = block_on(net.db(1).update(
        "todos",
        row,
        cells(&[("title", "from the future")]),
        UpdateOptions {
            updated_at_ms: Some(far_ahead),
            ..Default::default()
        },
    ))
    .unwrap();
    let chained = net.edit(1, row, &[("note", "chained")]);
    net.pump();
    let over = net.edit(2, row, &[("note", "over the chain")]);
    net.worker_online();
    let fate = |i: usize, tx| net.drive(net.db(i).wait_for_transaction(tx, DurabilityTier::Global));
    assert!(fate(1, refused.mergeable_tx_id()).is_err());
    for (i, tx) in [(1, chained.1), (2, over.1)] {
        let error = fate(i, tx).expect_err("built on a rejected write");
        assert_eq!(error.reason, Some(Reason::PredecessorRejected), "{error:?}");
    }
    assert_eq!(net.title(row), Some(Value::String("seed".to_owned())));
    assert_eq!(net.conflicts(row).len(), 1, "only the seed was accepted");
}

#[test]
fn a_resent_write_keeps_the_base_it_was_accepted_with() {
    let mut net = Net::new(2);
    let row = net.seed(0);
    net.offline(0);
    let first = net.edit(0, row, &[("title", "first")]);
    let second = net.edit(0, row, &[("title", "second")]);
    let foreign = net.edit_settled(1, row, &[("title", "foreign")]);
    // The authority accepts the chain, but its fates never reach the
    // writer: it resends both, and hears the recorded fates.
    net.connect(0);
    block_on(net.db(0).tick()).unwrap();
    block_on(net.core.tick()).unwrap();
    let accepted = net.conflicts(row).len();
    assert_eq!(accepted, 4, "seed, foreign, first, second");
    net.offline(0);
    net.online(0);
    net.settle(0, second.1);

    assert_eq!(
        net.conflicts(row).len(),
        accepted,
        "a resend applies nothing twice"
    );
    assert_eq!(
        writers(&net.conflict(row, first).overlapping),
        vec![foreign]
    );
    let second = net.conflict(row, second);
    assert!(second.maybe_conflicting);
    assert_eq!(writers(&second.overlapping), vec![foreign]);
}

#[test]
fn behind_two_relays_a_write_over_the_upper_relays_version_is_kept_conservatively() {
    // core <- upper (durable) <- lower (durable) <- tab (non-durable). The
    // lower relay holds the upper's unsettled version at a seq in its own
    // local range, which is no commit of its own: it cannot name it
    // upstream, so the tab's write counts as built on what it holds
    // accepted. It is kept, flagged over the upper's write.
    const UPPER: u8 = 0xa1;
    const LOWER: u8 = 0xa2;
    const TAB: u8 = 0xa3;
    let core = open(CORE, AuthorSubject::SYSTEM);
    let upper = open(UPPER, author(UPPER));
    let lower = open(LOWER, author(LOWER));
    let tab = open(TAB, author(LOWER));
    tab.set_non_durable_client();
    let link = |client: &Db, server: &Db, as_: u8| {
        let (client_side, server_side) = duplex();
        client.set_remote_link_hint(RemoteLinkHint::Live);
        let up = block_on(client.connect_upstream(client_side));
        server.accept_subscriber(server_side, author(as_));
        up
    };
    let upper_up = link(&upper, &core, UPPER);
    let _lower_up = link(&lower, &upper, LOWER);
    let _tab_up = link(&tab, &lower, LOWER);
    let stores = [&tab, &lower, &upper, &core];
    let pump = || {
        for _ in 0..4 {
            for db in stores.iter().chain(stores.iter().rev()) {
                block_on(db.tick()).unwrap();
            }
        }
    };
    let todos = Query::from("todos");
    let _held: Vec<_> = [&upper, &lower, &tab]
        .map(|db| {
            let prepared = db.prepare_query(&todos).unwrap();
            let mut stream = pin!(db.subscribe(&prepared, ReadOpts::default()));
            let mut context = Context::from_waker(Waker::noop());
            loop {
                if let Poll::Ready(stream) = stream.as_mut().poll(&mut context) {
                    break stream.unwrap();
                }
                pump();
            }
        })
        .into_iter()
        .collect();
    let row = block_on(core.insert(
        "todos",
        cells(&[("title", "seed"), ("note", "seed")]),
        Default::default(),
    ))
    .unwrap()
    .row_uuid();
    pump();

    assert!(upper.detach_connection(&upper_up));
    upper.set_remote_link_hint(RemoteLinkHint::Failed);
    block_on(upper.update(
        "todos",
        row,
        cells(&[("title", "upper")]),
        Default::default(),
    ))
    .unwrap();
    pump();
    let write =
        block_on(tab.update("todos", row, cells(&[("title", "tab")]), Default::default())).unwrap();
    pump();
    let _upper_up = link(&upper, &core, UPPER);
    pump();
    let mut turn = pin!(tab.wait_for_transaction(write.mergeable_tx_id(), DurabilityTier::Global));
    let mut context = Context::from_waker(Waker::noop());
    let fate = loop {
        if let Poll::Ready(fate) = turn.as_mut().poll(&mut context) {
            break fate;
        }
        pump();
    };
    fate.expect("the tab's write is kept");

    let conflicts = core.write_conflicts("todos", row, None).unwrap();
    let [_seed, upper_write, tab_write] = <[_; 3]>::try_from(conflicts).unwrap();
    assert!(tab_write.maybe_conflicting);
    assert_eq!(writers(&tab_write.overlapping), vec![upper_write.writer]);
}

#[test]
fn an_app_column_cannot_take_a_stored_row_columns_name() {
    for column in ["_base", "_cols", "_st", "_b"] {
        let source = SchemaBuilder::new()
            .table(TableSchemaBuilder::new("todos").column(column, ColumnType::Text))
            .build();
        let error = JazzSchema::new(&source).unwrap_err().to_string();
        assert!(
            error.contains("reserved for the store's own row columns"),
            "{error}"
        );
    }
}
