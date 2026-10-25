//! Serializable transactions from a tab through its worker: the worker is a
//! replica of the authority and serves its tabs as their upstream (a relay).
//! The authority validates a tab's commit as it validates any client's, so
//! a stale tab or worker can only cause a conflict, never a
//! non-serializable commit. A worker its host says has no server is the
//! last word on its tabs' commits and validates them itself. Not ported
//! from Jazz.

use crate::common;

#[path = "common/duplex_transport.rs"]
mod duplex_transport;

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

use duplex_transport::duplex;
use jazz::db::{
    Db, DbConfig, DbIdentity, ErrorCode, ReadOpts, RemoteLinkHint, SubscriptionStream, Upstream,
    block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::query::{Query, col, eq, lit};
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
                TableSchemaBuilder::new("invites")
                    .column("code", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new("members")
                    .column("code", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new("audit")
                    .column("note", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            .table(
                TableSchemaBuilder::new("seats")
                    .column("show", ColumnType::Text)
                    .column("holder", ColumnType::Text)
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

fn code(code: &str) -> BTreeMap<String, Value> {
    BTreeMap::from([("code".to_owned(), Value::String(code.to_owned()))])
}

fn holder(holder: &str) -> BTreeMap<String, Value> {
    BTreeMap::from([("holder".to_owned(), Value::String(holder.to_owned()))])
}

fn invite_query(c: &str) -> Query {
    Query::from("invites").filter(eq(col("code"), lit(Value::String(c.to_owned()))))
}

fn members_query(c: &str) -> Query {
    Query::from("members").filter(eq(col("code"), lit(Value::String(c.to_owned()))))
}

const WORKER: u8 = 0xa0;
const DIRECT: u8 = 0x60;

/// The authority, one worker with its tabs (a tab connects as its worker's
/// identity), and a client connected to the authority directly.
struct Net {
    core: Db,
    worker: Db,
    worker_up: Option<Upstream>,
    tabs: Vec<Db>,
    direct: Db,
}

impl Net {
    fn new(tabs: usize) -> Self {
        Self::with_worker_hint(tabs, RemoteLinkHint::Live)
    }

    fn with_worker_hint(tabs: usize, hint: RemoteLinkHint) -> Self {
        let core = open(0xc0, AuthorSubject::SYSTEM);
        let worker = open(WORKER, author(WORKER));
        worker.set_remote_link_hint(hint);
        let direct = open(DIRECT, author(DIRECT));
        let mut net = Net {
            core,
            worker,
            worker_up: None,
            tabs: Vec::new(),
            direct,
        };
        if hint != RemoteLinkHint::NoServer {
            net.connect_worker();
        }
        let (client_side, server_side) = duplex();
        block_on(net.direct.connect_upstream(client_side));
        net.core.accept_subscriber(server_side, author(DIRECT));
        for i in 0..tabs {
            let tab = open(WORKER + 1 + i as u8, author(WORKER));
            tab.set_non_durable_client();
            tab.set_remote_link_hint(RemoteLinkHint::Live);
            let (client_side, server_side) = duplex();
            block_on(tab.connect_upstream(client_side));
            net.worker.accept_subscriber(server_side, author(WORKER));
            net.tabs.push(tab);
        }
        net
    }

    fn connect_worker(&mut self) {
        let (client_side, server_side) = duplex();
        self.worker_up = Some(block_on(self.worker.connect_upstream(client_side)));
        self.core.accept_subscriber(server_side, author(WORKER));
    }

    fn worker_offline(&mut self) {
        let upstream = self.worker_up.take().expect("online");
        assert!(self.worker.detach_connection(&upstream));
        self.worker.set_remote_link_hint(RemoteLinkHint::Failed);
    }

    fn worker_online(&mut self) {
        self.connect_worker();
        self.worker.set_remote_link_hint(RemoteLinkHint::Live);
    }

    fn tab(&self, i: usize) -> &Db {
        &self.tabs[i]
    }

    fn pump(&self) {
        for tab in &self.tabs {
            block_on(tab.tick()).unwrap();
        }
        block_on(self.worker.tick()).unwrap();
        block_on(self.direct.tick()).unwrap();
        block_on(self.core.tick()).unwrap();
        block_on(self.direct.tick()).unwrap();
        block_on(self.worker.tick()).unwrap();
        for tab in &self.tabs {
            block_on(tab.tick()).unwrap();
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

    fn settle(&self, db: &Db, tx: u64) -> Result<u64, jazz::db::Error> {
        self.drive(db.wait_for_transaction(tx, DurabilityTier::Global))
    }

    /// `tx` was accepted where it was ordered, and is never `Global`:
    /// there is no authority to make it so.
    fn never_global(&self, db: &Db, tx: u64) {
        let mut wait = pin!(db.wait_for_transaction(tx, DurabilityTier::Global));
        let mut context = Context::from_waker(Waker::noop());
        for _ in 0..MAX_TURNS {
            assert!(
                wait.as_mut().poll(&mut context).is_pending(),
                "a commit no authority accepted reached Global"
            );
            self.pump();
        }
    }

    fn write_invite(&self, c: &str) -> RowUuid {
        let write = block_on(self.direct.insert("invites", code(c), Default::default())).unwrap();
        self.settle(&self.direct, write.mergeable_tx_id())
            .expect("invite settles");
        write.row_uuid()
    }

    fn revoke(&self, invite: RowUuid) {
        let write = block_on(self.direct.delete("invites", invite, Default::default())).unwrap();
        self.settle(&self.direct, write.mergeable_tx_id())
            .expect("revocation settles");
    }

    fn seed_seats(&self, seats: u8) -> Vec<RowUuid> {
        (0..seats)
            .map(|seat| {
                let row = RowUuid::from_bytes([0x90 + seat; 16]);
                let cells = BTreeMap::from([
                    ("show".to_owned(), Value::String("s".to_owned())),
                    ("holder".to_owned(), Value::String(String::new())),
                ]);
                self.core
                    .seed_settled_mergeable_for_bootstrap(
                        "seats",
                        row,
                        AuthorSubject::SYSTEM,
                        cells,
                    )
                    .unwrap();
                row
            })
            .collect()
    }

    /// The authority's rows of `query`.
    fn count(&self, query: &Query) -> usize {
        let prepared = self.core.prepare_query(query).unwrap();
        block_on(self.core.all(&prepared, ReadOpts::default()))
            .unwrap()
            .len()
    }

    fn holders(&self) -> Vec<String> {
        let prepared = self.core.prepare_query(&Query::from("seats")).unwrap();
        block_on(self.core.all(&prepared, ReadOpts::default()))
            .unwrap()
            .iter()
            .filter_map(|row| {
                match row
                    .cells()
                    .find(|(name, _)| *name == "holder")
                    .and_then(|(_, v)| v.cloned())
                {
                    Some(Value::String(h)) if !h.is_empty() => Some(h),
                    _ => None,
                }
            })
            .collect()
    }

    /// Commit `open` on `db` and wait for its fate: whether the commit
    /// held, or the conflict that refused it (at commit or from upstream).
    fn finish(&self, db: &Db, open: OpenTransactionId) -> Result<(), jazz::db::Error> {
        let receipt = self.drive(db.commit_serializable_handle(open))?;
        self.settle(db, receipt.0).map(drop)
    }

    /// Check the invite inside one serializable transaction on `db` and
    /// join only if it holds. `between` runs after the read and before the
    /// commit.
    fn redeem(&self, db: &Db, c: &str, between: impl FnOnce()) -> Result<(), jazz::db::Error> {
        let receipt = self.stage_redeem(db, c, between)?;
        self.settle(db, receipt).map(drop)
    }

    /// [`Net::redeem`] up to its commit: its receipt, not yet settled.
    fn stage_redeem(
        &self,
        db: &Db,
        c: &str,
        between: impl FnOnce(),
    ) -> Result<u64, jazz::db::Error> {
        let open = OpenTransactionId::new();
        block_on(db.begin_serializable(open)).unwrap();
        let invite = db.prepare_query(&invite_query(c)).unwrap();
        let live = !self
            .drive(db.serializable_tx_ref(open).all_prepared(&invite))
            .unwrap()
            .is_empty();
        assert!(live, "the invite is live at the snapshot");
        between();
        self.drive(
            db.serializable_tx_ref(open)
                .insert("members", code(c), Default::default()),
        )
        .unwrap();
        self.drive(db.commit_serializable_handle(open))
            .map(|receipt| receipt.0)
    }

    /// Begin a serializable transaction on `db` whose snapshot is pinned
    /// by an unrelated read.
    fn begin_pinned(&self, db: &Db) -> OpenTransactionId {
        let open = OpenTransactionId::new();
        block_on(db.begin_serializable(open)).unwrap();
        let audit = db.prepare_query(&Query::from("audit")).unwrap();
        self.drive(db.serializable_tx_ref(open).all_prepared(&audit))
            .unwrap();
        open
    }

    /// `db` subscribes to the seats (so its replica, and its worker's,
    /// hold them while the stream lives) and waits for the opening.
    fn watch_seats(&self, db: &Db) -> SubscriptionStream {
        let prepared = db.prepare_query(&Query::from("seats")).unwrap();
        let mut stream = block_on(db.subscribe(
            &prepared,
            ReadOpts::for_read_tier(jazz::db::ReadTier::Remote),
        ))
        .unwrap();
        for _ in 0..MAX_TURNS {
            if stream.try_next_event().is_some() {
                return stream;
            }
            self.pump();
        }
        panic!("the seats subscription never opened");
    }
}

fn assert_conflict(result: Result<(), jazz::db::Error>) {
    let error = result.expect_err("the commit conflicts");
    assert_eq!(error.code, ErrorCode::TransactionConflict, "{error}");
}

fn assert_rejected(result: Result<(), jazz::db::Error>) {
    common::assert_serialization_conflict(&result.expect_err("the authority rejects the commit"));
}

#[test]
fn a_tabs_serializable_redemption_through_its_worker_joins() {
    let net = Net::new(1);
    net.write_invite("abc");
    net.redeem(net.tab(0), "abc", || {})
        .expect("the authority accepts the tab's redemption");
    assert_eq!(net.count(&members_query("abc")), 1);
}

#[test]
fn a_revocation_after_a_tabs_read_rejects_its_redemption() {
    let net = Net::new(1);
    let invite = net.write_invite("abc");
    assert_rejected(net.redeem(net.tab(0), "abc", || net.revoke(invite)));
    assert_eq!(net.count(&members_query("abc")), 0);
}

/// A tab's write to a row it never read is checked against the row's
/// version at the authority: unchanged since the snapshot, it holds.
#[test]
fn a_tabs_blind_write_to_an_unchanged_row_is_accepted() {
    let net = Net::new(1);
    let seat = net.seed_seats(1)[0];
    let tab = net.tab(0);
    let _seats = net.watch_seats(tab);
    let open = net.begin_pinned(tab);
    net.drive(tab.serializable_tx_ref(open).update(
        "seats",
        seat,
        holder("tab"),
        Default::default(),
    ))
    .unwrap();
    net.finish(tab, open).expect("nothing changed the seat");
    assert_eq!(net.holders(), ["tab"]);
}

/// The seat changed at the authority after the tab's snapshot: the tab's
/// blind write would overwrite a version it never saw, so it conflicts,
/// whether or not the change reached the worker and the tab first.
#[test]
fn a_tabs_blind_write_over_a_newer_version_conflicts() {
    for reached_tab in [false, true] {
        let net = Net::new(1);
        let seat = net.seed_seats(1)[0];
        let tab = net.tab(0);
        let seats = net.watch_seats(tab);
        let _direct_seats = net.watch_seats(&net.direct);
        let open = net.begin_pinned(tab);
        if !reached_tab {
            // The tab keeps the copy it holds; nothing newer reaches it.
            drop(seats);
            net.pump();
        }
        let write =
            block_on(
                net.direct
                    .update("seats", seat, holder("direct"), Default::default()),
            )
            .unwrap();
        net.settle(&net.direct, write.mergeable_tx_id()).unwrap();
        net.drive(tab.serializable_tx_ref(open).update(
            "seats",
            seat,
            holder("tab"),
            Default::default(),
        ))
        .unwrap();
        assert_rejected(net.finish(tab, open));
        assert_eq!(
            net.holders(),
            ["direct"],
            "reached the tab first: {reached_tab}"
        );
    }
}

/// Two tabs of one worker and a direct client race for the same seats:
/// none is ever double-booked.
#[test]
fn racing_seat_bookings_from_tabs_and_a_client_never_double_book() {
    let net = Net::new(2);
    let seats = net.seed_seats(2);
    let racers: Vec<&Db> = vec![net.tab(0), net.tab(1), &net.direct];
    let _seats: Vec<SubscriptionStream> = racers.iter().map(|db| net.watch_seats(db)).collect();
    // Every racer stages a booking of the first seat before any commits.
    let staged: Vec<(&Db, OpenTransactionId)> = racers
        .iter()
        .enumerate()
        .map(|(i, db)| {
            let open = OpenTransactionId::new();
            block_on(db.begin_serializable(open)).unwrap();
            let free = db
                .prepare_query(
                    &Query::from("seats")
                        .filter(eq(col("holder"), lit(Value::String(String::new())))),
                )
                .unwrap();
            let rows = net
                .drive(db.serializable_tx_ref(open).all_prepared(&free))
                .unwrap();
            assert_eq!(rows.len(), 2, "both seats are free at every snapshot");
            net.drive(db.serializable_tx_ref(open).update(
                "seats",
                seats[0],
                holder(&format!("r{i}")),
                Default::default(),
            ))
            .unwrap();
            (*db, open)
        })
        .collect();
    let receipts: Vec<(&Db, Result<u64, jazz::db::Error>)> = staged
        .into_iter()
        .map(|(db, open)| {
            (
                db,
                block_on(db.commit_serializable_handle(open)).map(|r| r.0),
            )
        })
        .collect();
    let booked = receipts
        .into_iter()
        .filter(|(db, receipt)| receipt.as_ref().is_ok_and(|tx| net.settle(db, *tx).is_ok()))
        .count();
    assert_eq!(booked, 1, "exactly one racer books the seat");
    assert_eq!(net.holders().len(), 1);
}

/// A tab's snapshot holds its own write that its worker has not uploaded
/// yet (the worker is offline): the tab reads it, and the authority, once
/// the worker uploads both, places it where it settled.
#[test]
fn a_tabs_commit_over_its_own_unsettled_write_is_accepted() {
    let mut net = Net::new(1);
    net.worker_offline();
    let tab = net.tab(0);
    let invite = block_on(tab.insert("invites", code("own"), Default::default())).unwrap();
    net.pump();
    let open = OpenTransactionId::new();
    block_on(tab.begin_serializable(open)).unwrap();
    let query = tab.prepare_query(&invite_query("own")).unwrap();
    assert_eq!(
        net.drive(tab.serializable_tx_ref(open).all_prepared(&query))
            .unwrap()
            .len(),
        1,
        "the snapshot holds the tab's own unsettled insert"
    );
    net.drive(tab.serializable_tx_ref(open).update(
        "invites",
        invite.row_uuid(),
        code("own"),
        Default::default(),
    ))
    .unwrap();
    net.drive(
        tab.serializable_tx_ref(open)
            .insert("members", code("own"), Default::default()),
    )
    .unwrap();
    let receipt = net.drive(tab.commit_serializable_handle(open)).unwrap();
    net.worker_online();
    net.settle(net.tab(0), receipt.0)
        .expect("the authority accepts the tab's commit");
    assert_eq!(net.count(&members_query("own")), 1);
    assert_eq!(net.count(&invite_query("own")), 1);
}

/// A worker with no server is its tabs' last word: it validates their
/// serializable commits itself, ordering them so its tabs converge, but
/// none of them becomes `Global`, since there is no authority.
#[test]
fn a_worker_without_a_server_validates_its_tabs_serializable_commits() {
    let net = Net::with_worker_hint(2, RemoteLinkHint::NoServer);
    let write = block_on(
        net.tab(0)
            .insert("invites", code("abc"), Default::default()),
    )
    .unwrap();
    net.never_global(net.tab(0), write.mergeable_tx_id());
    let joined = net
        .stage_redeem(net.tab(1), "abc", || {})
        .expect("an unchanged read holds");
    net.never_global(net.tab(1), joined);
    let invite = write.row_uuid();
    let revoked = net.redeem(net.tab(1), "abc", || {
        let write = block_on(net.tab(0).delete("invites", invite, Default::default())).unwrap();
        net.never_global(net.tab(0), write.mergeable_tx_id());
    });
    assert_conflict(revoked);
    let members = net.worker.prepare_query(&members_query("abc")).unwrap();
    assert_eq!(
        block_on(net.worker.all(&members, ReadOpts::default()))
            .unwrap()
            .len(),
        1
    );
}
