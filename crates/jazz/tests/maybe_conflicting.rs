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
    Db, DbConfig, DbIdentity, ReadOpts, RemoteLinkHint, SubscriptionStream, UpdateOptions,
    Upstream, WriteConflict, block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
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
            .build(),
    )
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
        let mut net = Net {
            core: open(CORE, AuthorSubject::SYSTEM),
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
        let worker = open(WORKER, author(WORKER));
        worker.set_remote_link_hint(RemoteLinkHint::Live);
        net.worker = Some(Client {
            node: WORKER,
            db: worker,
            up: None,
            via: None,
            _todos: None,
        });
        net.connect_worker();
        for i in 0..tabs {
            net.add_client(WORKER + 1 + i as u8, Some(0));
        }
        net
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
        self.core.write_conflicts("todos", row, &[]).unwrap()
    }

    fn conflict(&self, row: RowUuid, writer: Writer) -> WriteConflict {
        self.conflicts(row)
            .into_iter()
            .find(|conflict| conflict.writer == writer)
            .unwrap_or_else(|| panic!("no accepted write by {writer:?}"))
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
fn chained_write_over_a_rejected_predecessor_resolves_to_the_chain_base() {
    let mut net = Net::new(2);
    let row = net.seed(0);
    let far_ahead = web_time_now_ms() + 24 * 60 * 60 * 1000;
    // Without an interruption: the predecessor is refused (its timestamp
    // is a day ahead of the authority's clock), the write over it applies
    // over the settled row and is not flagged.
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
    net.settle(0, chained.1);
    assert!(
        net.drive(
            net.db(0)
                .wait_for_transaction(refused.mergeable_tx_id(), DurabilityTier::Global)
        )
        .is_err()
    );
    let conflict = net.conflict(row, chained);
    assert!(!conflict.maybe_conflicting, "only the seed lies before it");
    assert_eq!(conflict.resolved_base, conflict.previous);

    // With dave's write after the chain's base it is flagged.
    net.offline(0);
    block_on(net.db(0).update(
        "todos",
        row,
        cells(&[("title", "from the future")]),
        UpdateOptions {
            updated_at_ms: Some(far_ahead),
            ..Default::default()
        },
    ))
    .unwrap();
    let chained = net.edit(0, row, &[("title", "chained again")]);
    let dave = net.edit_settled(1, row, &[("title", "dave")]);
    net.online(0);
    net.settle(0, chained.1);
    let conflict = net.conflict(row, chained);
    assert!(conflict.maybe_conflicting);
    assert_eq!(writers(&conflict.overlapping), vec![dave]);
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

    for writer in [first, second] {
        let conflict = net.conflict(row, writer);
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
    let tab_conflict = net.conflict(row, tab);
    assert!(
        tab_conflict.maybe_conflicting,
        "the tab never saw the direct write"
    );
    assert_eq!(writers(&tab_conflict.overlapping), vec![direct]);
    let other_conflict = net.conflict(row, other);
    assert!(other_conflict.maybe_conflicting);
    assert!(
        other_conflict.overlapping.is_empty(),
        "the other tab wrote only the note"
    );
}
