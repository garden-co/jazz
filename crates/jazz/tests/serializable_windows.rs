//! Serializable transactions that read a window (`limit`, `offset`, `one`)
//! after staging writes that move rows out of or into it (issue 5521).
//!
//! The app reads through the transaction's overlay of staged writes, but the
//! read is recorded, and re-run at commit by the authority, without it. A
//! staged write that moves a row out of the window promotes the next one into
//! what the app sees; the recorded read must hold that row too, or a
//! concurrent change to it goes unnoticed. The recorded window is widened by
//! the transaction's staged rows of the window's table (from offset
//! `offset - n` when there is one). A window that depends on other rows the
//! transaction wrote cannot be bounded that way and is refused as not
//! supported yet.

use crate::common;
use jazz_types::legacy_test_future::ResultFutureExt as _;

#[path = "common/duplex_transport.rs"]
mod duplex_transport;

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

use duplex_transport::duplex;
use jazz::db::{
    Db, DbConfig, DbIdentity, ReadOpts, ReadTier, RemoteLinkHint, SubscriptionStream, block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{ArraySubquery, ArraySubqueryRequirement, OrderDirection, Query, col, eq, lit};
use jazz::records::Value;
use jazz::schema::JazzSchema;
use jazz::tools::{ColumnType, OpenTransactionId, SchemaBuilder, TableSchemaBuilder};
use jazz::tx::DurabilityTier;

use common::{allow_all_policies, compile_schema};

const MAX_TURNS: usize = 200;

fn schema() -> JazzSchema {
    compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("jobs")
                    .column("queue", ColumnType::Text)
                    .column("status", ColumnType::Text)
                    .column("created", ColumnType::Integer)
                    .policies(allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new("claims")
                    .column("job", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new("queues")
                    .column("name", ColumnType::Text)
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

fn text(value: &str) -> Value {
    Value::String(value.to_owned())
}

fn status(status: &str) -> BTreeMap<String, Value> {
    BTreeMap::from([("status".to_owned(), text(status))])
}

/// The job queue: queued jobs, oldest first.
fn queued() -> Query {
    Query::from("jobs")
        .filter(eq(col("status"), lit(text("queued"))))
        .order_by("created", OrderDirection::Asc)
}

fn a() -> RowUuid {
    RowUuid::from_bytes([0xa1; 16])
}
fn b() -> RowUuid {
    RowUuid::from_bytes([0xb1; 16])
}
fn c() -> RowUuid {
    RowUuid::from_bytes([0xc1; 16])
}

const WORKER: u8 = 0xa0;
const ALICE: u8 = 0x60;
const BOB: u8 = 0x61;

/// The authority; alice and bob, clients connected to it directly; and a
/// worker (a replica of the authority) with alice's tab, which commits
/// through the worker as its relay.
struct Net {
    core: Db,
    alice: Db,
    bob: Db,
    worker: Db,
    tab: Db,
}

impl Net {
    fn new() -> Self {
        let core = open(0xc0, AuthorSubject::SYSTEM);
        let alice = open(ALICE, author(ALICE));
        let bob = open(BOB, author(BOB));
        let worker = open(WORKER, author(WORKER));
        let tab = open(WORKER + 1, author(WORKER));
        for (db, node) in [(&alice, ALICE), (&bob, BOB)] {
            let (client_side, server_side) = duplex();
            block_on(db.connect_upstream(client_side));
            core.accept_subscriber(server_side, author(node)).unwrap();
            jazz::db::block_on(db.set_remote_link_hint(RemoteLinkHint::Live));
        }
        let (client_side, server_side) = duplex();
        block_on(worker.connect_upstream(client_side));
        core.accept_subscriber(server_side, author(WORKER)).unwrap();
        jazz::db::block_on(worker.set_remote_link_hint(RemoteLinkHint::Live));
        jazz::db::block_on(tab.set_non_durable_client());
        jazz::db::block_on(tab.set_remote_link_hint(RemoteLinkHint::Live));
        let (client_side, server_side) = duplex();
        block_on(tab.connect_upstream(client_side));
        worker
            .accept_subscriber(server_side, author(WORKER))
            .unwrap();
        Net {
            core,
            alice,
            bob,
            worker,
            tab,
        }
    }

    fn pump(&self) {
        block_on(self.tab.tick()).unwrap();
        block_on(self.worker.tick()).unwrap();
        block_on(self.alice.tick()).unwrap();
        block_on(self.bob.tick()).unwrap();
        block_on(self.core.tick()).unwrap();
        block_on(self.alice.tick()).unwrap();
        block_on(self.bob.tick()).unwrap();
        block_on(self.worker.tick()).unwrap();
        block_on(self.tab.tick()).unwrap();
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

    /// Seed settled jobs at the authority: `(row, queue, created)`, queued.
    fn seed_jobs(&self, jobs: &[(RowUuid, &str, i32)]) {
        for &(row, queue, created) in jobs {
            let cells = BTreeMap::from([
                ("queue".to_owned(), text(queue)),
                ("status".to_owned(), text("queued")),
                ("created".to_owned(), Value::I32(created)),
            ]);
            self.core
                .insert(
                    "jobs",
                    cells,
                    jazz::db::InsertOptions {
                        row_id: Some(row),
                        identity: jazz::db::WriteIdentity::Attribution(AuthorSubject::SYSTEM),
                        ..Default::default()
                    },
                )
                .unwrap();
        }
    }

    fn seed_queues(&self, names: &[(u8, &str)]) {
        for &(byte, name) in names {
            let cells = BTreeMap::from([("name".to_owned(), text(name))]);
            self.core
                .insert(
                    "queues",
                    cells,
                    jazz::db::InsertOptions {
                        row_id: Some(RowUuid::from_bytes([byte; 16])),
                        identity: jazz::db::WriteIdentity::Attribution(AuthorSubject::SYSTEM),
                        ..Default::default()
                    },
                )
                .unwrap();
        }
    }

    /// `db` subscribes to every job (as a worker showing the queue would),
    /// so its replica holds them while the stream lives.
    fn hydrate(&self, db: &Db) -> SubscriptionStream {
        let prepared = db.prepare_query(&Query::from("jobs")).unwrap();
        let mut stream =
            block_on(db.subscribe(&prepared, ReadOpts::for_read_tier(ReadTier::Remote))).unwrap();
        for _ in 0..MAX_TURNS {
            if stream.try_next_event().is_some() {
                return stream;
            }
            self.pump();
        }
        panic!("the jobs subscription never opened");
    }

    /// Bob changes `job` and his change settles at the authority.
    fn bob_sets(&self, job: RowUuid, column: &str, to: &str) {
        let _jobs = self.hydrate(&self.bob);
        let cells = BTreeMap::from([(column.to_owned(), text(to))]);
        let write = block_on(self.bob.update("jobs", job, cells, Default::default())).unwrap();
        self.drive(
            self.bob
                .wait_for_transaction(write.mergeable_tx_id(), DurabilityTier::Global),
        )
        .unwrap();
    }

    /// The rows of `query` on `db`, inside transaction `open`.
    fn read(&self, db: &Db, open: OpenTransactionId, query: &Query) -> Vec<RowUuid> {
        let prepared = db.prepare_query(query).unwrap();
        self.drive(db.tx_ref(open).all_prepared(&prepared))
            .unwrap()
            .iter()
            .map(|row| row.row_uuid())
            .collect()
    }

    fn update(&self, db: &Db, open: OpenTransactionId, job: RowUuid, to: &str) {
        self.drive(
            db.tx_ref(open)
                .update("jobs", job, status(to), Default::default()),
        )
        .unwrap();
    }

    fn claim(&self, db: &Db, open: OpenTransactionId, job: RowUuid) {
        let cells = BTreeMap::from([("job".to_owned(), text(&format!("{job:?}")))]);
        self.drive(db.tx_ref(open).insert("claims", cells, Default::default()))
            .unwrap();
    }

    /// Commit `open` on `db` and wait for the authority's verdict.
    fn finish(&self, db: &Db, open: OpenTransactionId) -> Result<(), jazz::db::Error> {
        let receipt = self.drive(db.commit_serializable_handle(open))?;
        self.drive(db.wait_for_transaction(receipt.0, DurabilityTier::Global))
            .map(drop)
    }

    /// The claims the authority holds.
    fn claims(&self) -> usize {
        let prepared = self.core.prepare_query(&Query::from("claims")).unwrap();
        block_on(self.core.all(&prepared, ReadOpts::default()))
            .unwrap()
            .len()
    }
}

/// A conflict, refused at commit (the replica already knew) or by the
/// authority.
fn assert_conflict(outcome: Result<(), jazz::db::Error>) {
    let error = outcome.expect_err("the commit must conflict");
    if error.code == jazz::db::ErrorCode::TransactionConflict {
        return;
    }
    common::assert_serialization_conflict(&error);
}

/// A conflict the authority found when it re-ran the recorded reads: the
/// committing replica never heard of the change.
fn assert_rejected_by_authority(outcome: Result<(), jazz::db::Error>) {
    common::assert_serialization_conflict(&outcome.expect_err("the authority must reject it"));
}

fn seed_abc(net: &Net) {
    net.seed_jobs(&[(a(), "q", 1), (b(), "q", 2), (c(), "q", 3)]);
}

/// The issue's job queue on `db`: take the oldest queued job (A), mark it
/// done, take the next one (B, which only the overlay promotes into the
/// window), and claim it without writing it. `between` runs before the
/// commit.
fn claim_next(net: &Net, db: &Db, between: impl FnOnce()) -> Result<(), jazz::db::Error> {
    let open = OpenTransactionId::new();
    block_on(db.begin_serializable(open)).unwrap();
    assert_eq!(net.read(db, open, &queued().limit(1)), vec![a()]);
    net.update(db, open, a(), "done");
    assert_eq!(
        net.read(db, open, &queued().limit(1)),
        vec![b()],
        "the overlay promotes B"
    );
    net.claim(db, open, b());
    between();
    net.finish(db, open)
}

/// The issue's repro. Alice's replica holds the queue; she marks A done,
/// takes the next job, B (promoted into `one()` by her staged write), and
/// claims it. Bob claims B before she commits. Before the fix her read was
/// recorded as `[A]`, which still held at the authority, so B was claimed
/// twice; now it is recorded as the first two queued jobs, `[A, B]`.
///
/// ```
/// alice: one(queued) = A ─ A done ─ one(queued) = B ─ claim B ─ commit ──✗
/// bob:                                         B claimed ──► authority
/// ```
#[test]
fn a_change_to_a_row_a_staged_write_promoted_into_a_window_conflicts() {
    let net = Net::new();
    seed_abc(&net);
    let _jobs = net.hydrate(&net.alice);
    assert_conflict(claim_next(&net, &net.alice, || {
        net.bob_sets(b(), "status", "claimed")
    }));
    assert_eq!(net.claims(), 0, "B is not claimed twice");
}

/// The same from alice's tab, through its worker. The tab's replica holds
/// only what its reads hydrated, so it does not hear of bob's change: the
/// tab records the widened read, the relay forwards it unchanged, and the
/// authority's re-run of it rejects the claim.
///
/// ```
/// tab ──claim B (read [A, B])──► worker ──► authority ──✗
/// bob ──B claimed──────────────────────────► authority
/// ```
#[test]
fn a_relayed_change_to_a_promoted_row_conflicts() {
    let net = Net::new();
    seed_abc(&net);
    assert_rejected_by_authority(claim_next(&net, &net.tab, || {
        net.bob_sets(b(), "status", "claimed")
    }));
    assert_eq!(net.claims(), 0, "B is not claimed twice");
}

/// A replica that holds only what alice's reads asked for: a windowed read
/// after a staged write hydrates its widened window, so the overlay can
/// promote B at all (before the fix the replica held only A, and alice's
/// second read found no queued job). Alice's replica never hears of bob's
/// change; the authority, re-running her widened read, rejects the claim.
#[test]
fn a_windowed_read_hydrates_the_rows_a_staged_write_can_promote() {
    let net = Net::new();
    seed_abc(&net);
    assert_rejected_by_authority(claim_next(&net, &net.alice, || {
        net.bob_sets(b(), "status", "claimed")
    }));
    assert_eq!(net.claims(), 0, "B is not claimed twice");
}

/// Without a concurrent change the widened read still holds at the
/// authority: alice's claim commits, directly and from her tab, whether
/// her replica held the queue or hydrated only what she read.
#[test]
fn a_promoted_window_commits_when_nothing_changed() {
    for tab in [false, true] {
        for hydrated in [false, true] {
            let net = Net::new();
            seed_abc(&net);
            let db = if tab { &net.tab } else { &net.alice };
            let _jobs = hydrated.then(|| net.hydrate(db));
            claim_next(&net, db, || {}).expect("the claim commits");
            assert_eq!(net.claims(), 1, "tab: {tab}, hydrated: {hydrated}");
        }
    }
}

/// The widening is bounded by alice's staged rows of the window's table:
/// bob's change to C, past `limit + 1` in the queue, does not conflict.
#[test]
fn a_change_past_the_widened_window_commits() {
    let net = Net::new();
    seed_abc(&net);
    let _jobs = net.hydrate(&net.alice);
    claim_next(&net, &net.alice, || net.bob_sets(c(), "status", "claimed"))
        .expect("C is outside every recorded window");
    assert_eq!(net.claims(), 1);
}

/// A staged insert ahead of an offset window pushes a row in from before
/// it: alice's second page (`offset 1, limit 1`) shows A once she queues an
/// older job. Her read is recorded from offset 0, so bob moving A to
/// another queue (A stays queued, so the authority's second page is still
/// B) conflicts. Widening only the limit, to `offset 1, limit 2` = `[B, C]`,
/// would miss it.
///
/// ```
/// stored:  A B C        alice's overlay:  Z A B C      page 2: A
/// recorded: offset 0, limit 3 = [A, B, C]
/// ```
#[test]
fn an_offset_window_records_rows_pushed_in_from_before_it() {
    let net = Net::new();
    seed_abc(&net);
    let _jobs = net.hydrate(&net.alice);
    let db = &net.alice;
    let open = OpenTransactionId::new();
    block_on(db.begin_serializable(open)).unwrap();
    let cells = BTreeMap::from([
        ("queue".to_owned(), text("q")),
        ("status".to_owned(), text("queued")),
        ("created".to_owned(), Value::I32(0)),
    ]);
    net.drive(db.tx_ref(open).insert("jobs", cells, Default::default()))
        .unwrap();
    assert_eq!(
        net.read(db, open, &queued().offset(1).limit(1)),
        vec![a()],
        "the staged job ahead of A shifts A into the window"
    );
    net.claim(db, open, a());
    net.bob_sets(a(), "queue", "elsewhere");
    assert_conflict(net.finish(db, open));
    assert_eq!(net.claims(), 0);
}

/// A relation's window widens the same way, per parent: the queue's next
/// job is promoted by alice's staged write, and bob's change to it
/// conflicts.
#[test]
fn a_change_to_a_row_promoted_into_a_relation_window_conflicts() {
    let net = Net::new();
    seed_abc(&net);
    net.seed_queues(&[(0x01, "q")]);
    let _jobs = net.hydrate(&net.alice);
    let next = Query::from("queues").array_subquery(
        ArraySubquery::new("next", "jobs", "queue", "name")
            .filter(eq(col("status"), lit(text("queued"))))
            .order_by("created", OrderDirection::Asc)
            .limit(1),
    );
    let db = &net.alice;
    let next_job = |open| {
        let prepared = db.prepare_query(&next).unwrap();
        let rows = net.drive(db.tx_ref(open).all_prepared(&prepared)).unwrap();
        assert_eq!(rows.len(), 1);
        format!("{:?}", rows[0])
    };
    let open = OpenTransactionId::new();
    block_on(db.begin_serializable(open)).unwrap();
    let first = next_job(open);
    net.update(db, open, a(), "done");
    let second = next_job(open);
    assert_ne!(first, second, "the overlay promotes B into the relation");
    net.claim(db, open, b());
    net.bob_sets(b(), "status", "claimed");
    assert_conflict(net.finish(db, open));
    assert_eq!(net.claims(), 0);
}

/// A window whose membership depends on other rows: a queue is listed only
/// while it has a queued job. One staged write (A done) can drop any number
/// of queues, so no count of staged rows bounds what the overlay promotes,
/// and recording every busy queue would cost the whole table. Alice's read
/// is refused as not supported yet. A write to a table the window does not
/// depend on (a claim) leaves it readable.
#[test]
fn a_window_admitted_by_rows_the_transaction_wrote_is_not_supported_yet() {
    let net = Net::new();
    net.seed_jobs(&[(a(), "a", 1), (b(), "b", 2)]);
    net.seed_queues(&[(0x01, "a"), (0x02, "b")]);
    let _jobs = net.hydrate(&net.alice);
    let busy = Query::from("queues")
        .array_subquery(
            ArraySubquery::new("open", "jobs", "queue", "name")
                .filter(eq(col("status"), lit(text("queued"))))
                .requirement(ArraySubqueryRequirement::AtLeastOne),
        )
        .order_by("name", OrderDirection::Asc)
        .limit(1);
    let db = &net.alice;
    let open = OpenTransactionId::new();
    block_on(db.begin_serializable(open)).unwrap();
    let queue_a = vec![RowUuid::from_bytes([0x01; 16])];
    assert_eq!(net.read(db, open, &busy), queue_a);
    net.claim(db, open, a());
    assert_eq!(
        net.read(db, open, &busy),
        queue_a,
        "claims do not decide it"
    );
    net.update(db, open, a(), "done");
    let prepared = db.prepare_query(&busy).unwrap();
    let error = net
        .drive(db.tx_ref(open).all_prepared(&prepared))
        .expect_err("the read is refused");
    assert_eq!(error.code, jazz::db::ErrorCode::Query, "{error}");
    assert!(
        error.message.contains("`queues`")
            && error.message.contains("`jobs`")
            && error
                .message
                .contains("not supported in serializable transactions yet"),
        "{error}"
    );
}
