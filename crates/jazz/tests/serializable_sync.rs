//! Serializable transactions between clients and the authority, modelled on
//! Jazz's serializable_snapshot_coverage invite-link scenarios: reads run at
//! the transaction's snapshot on a partial replica (hydrated from the
//! authority when online), and the authority re-runs every read at commit,
//! so a stale or partial replica can cause a conflict but never a
//! non-serializable commit. Not ported verbatim (Jazz's suite drives its
//! server shell and snapshot-capture internals).

use crate::common;

#[path = "common/duplex_transport.rs"]
mod duplex_transport;

use std::collections::BTreeMap;
use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

use duplex_transport::duplex;
use jazz::db::{Db, DbConfig, DbIdentity, ErrorCode, ReadOpts, RemoteLinkHint, Upstream, block_on};
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
    let schema = schema();
    block_on(Db::open(DbConfig::new(
        schema,
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author,
        },
    )))
    .unwrap()
}

fn code(code: &str) -> BTreeMap<String, Value> {
    BTreeMap::from([("code".to_owned(), Value::String(code.to_owned()))])
}

fn invite_query(c: &str) -> Query {
    Query::from("invites").filter(eq(col("code"), lit(Value::String(c.to_owned()))))
}

fn members_query(c: &str) -> Query {
    Query::from("members").filter(eq(col("code"), lit(Value::String(c.to_owned()))))
}

#[derive(Debug, PartialEq, Eq)]
enum Redeem {
    Joined,
    Invalid,
    Conflict,
}

struct Net {
    core: Db,
    clients: Vec<(Db, Option<Upstream>)>,
}

impl Net {
    fn new(clients: usize) -> Self {
        let core = open(0xc0, AuthorSubject::SYSTEM);
        let mut net = Net {
            core,
            clients: Vec::new(),
        };
        for i in 0..clients {
            let author = AuthorSubject::for_test_bytes([0x50 + i as u8; 16]);
            net.clients.push((open(0x50 + i as u8, author), None));
            net.connect(i);
        }
        net
    }

    fn db(&self, i: usize) -> &Db {
        &self.clients[i].0
    }

    fn connect(&mut self, i: usize) {
        let (client_side, server_side) = duplex();
        let author = AuthorSubject::for_test_bytes([0x50 + i as u8; 16]);
        let upstream = block_on(self.clients[i].0.connect_upstream(client_side));
        self.clients[i].1 = Some(upstream);
        self.core.accept_subscriber(server_side, author);
    }

    fn go_offline(&mut self, i: usize) {
        let upstream = self.clients[i].1.take().expect("online");
        assert!(self.clients[i].0.detach_connection(&upstream));
        self.clients[i]
            .0
            .set_remote_link_hint(RemoteLinkHint::Failed);
    }

    fn go_online(&mut self, i: usize) {
        self.connect(i);
        self.clients[i].0.set_remote_link_hint(RemoteLinkHint::Live);
    }

    fn pump(&self) {
        for (db, _) in &self.clients {
            block_on(db.tick()).unwrap();
        }
        block_on(self.core.tick()).unwrap();
        for (db, _) in &self.clients {
            block_on(db.tick()).unwrap();
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

    fn settle(&self, i: usize, tx: u64) -> Result<u64, jazz::db::Error> {
        self.drive(self.db(i).wait_for_transaction(tx, DurabilityTier::Global))
    }

    fn write_invite(&self, i: usize, c: &str) -> RowUuid {
        let write = block_on(self.db(i).insert("invites", code(c), Default::default())).unwrap();
        self.settle(i, write.mergeable_tx_id())
            .expect("invite settles");
        write.row_uuid()
    }

    fn revoke(&self, i: usize, invite: RowUuid) {
        let write = block_on(self.db(i).delete("invites", invite, Default::default())).unwrap();
        self.settle(i, write.mergeable_tx_id())
            .expect("revocation settles");
    }

    /// Advance the client's known server cut without delivering invites.
    fn unrelated_receipt(&self, i: usize) {
        let prepared = self.db(i).prepare_query(&Query::from("audit")).unwrap();
        let _stream = self.drive(async {
            let mut stream = self
                .db(i)
                .subscribe(
                    &prepared,
                    ReadOpts::for_read_tier(jazz::db::ReadTier::Remote),
                )
                .await
                .unwrap();
            for _ in 0..MAX_TURNS {
                if stream.try_next_event().is_some() {
                    break;
                }
                self.pump();
            }
            stream
        });
    }

    fn members(&self, c: &str) -> usize {
        let prepared = self.core.prepare_query(&members_query(c)).unwrap();
        block_on(self.core.all(&prepared, ReadOpts::default()))
            .unwrap()
            .len()
    }

    /// Check the invite (and, if `single_use`, that nobody redeemed it)
    /// inside one serializable transaction; join only if it holds.
    /// `between` runs after the reads and before the commit.
    fn redeem(&self, i: usize, c: &str, single_use: bool, between: impl FnOnce()) -> Redeem {
        let db = self.db(i);
        let open = OpenTransactionId::new();
        block_on(db.begin_serializable(open)).unwrap();
        let invite = db.prepare_query(&invite_query(c)).unwrap();
        let live = !self
            .drive(db.serializable_tx_ref(open).all_prepared(&invite))
            .unwrap()
            .is_empty();
        let taken = single_use && {
            let members = db.prepare_query(&members_query(c)).unwrap();
            !self
                .drive(db.serializable_tx_ref(open).all_prepared(&members))
                .unwrap()
                .is_empty()
        };
        if !live || taken {
            db.abandon_serializable_handle(open).unwrap();
            return Redeem::Invalid;
        }
        between();
        self.drive(
            db.serializable_tx_ref(open)
                .insert("members", code(c), Default::default()),
        )
        .unwrap();
        // A change the replica already holds conflicts at once; otherwise
        // the authority decides.
        let receipt = match self.drive(db.commit_serializable_handle(open)) {
            Ok(receipt) => receipt,
            Err(error) => {
                assert_eq!(error.code, ErrorCode::TransactionConflict, "{error}");
                return Redeem::Conflict;
            }
        };
        match self.settle(i, receipt.0) {
            Ok(_) => Redeem::Joined,
            Err(error) => {
                common::assert_serialization_conflict(&error);
                Redeem::Conflict
            }
        }
    }
}

const OWNER: usize = 0;
const BACKEND: usize = 1;
const OTHER: usize = 2;

/// The backend never received the invite: the read hydrates at its snapshot.
#[test]
fn a_serializable_read_sees_an_invite_the_backend_never_received() {
    let net = Net::new(3);
    net.write_invite(OWNER, "abc");
    net.unrelated_receipt(BACKEND);
    assert_eq!(net.redeem(BACKEND, "abc", false, || {}), Redeem::Joined);
    assert_eq!(net.members("abc"), 1);
}

/// The revocation lands between the read and the commit: the authority
/// rejects the membership.
#[test]
fn a_revocation_after_the_read_rejects_the_redemption() {
    let net = Net::new(3);
    let invite = net.write_invite(OWNER, "abc");
    let outcome = net.redeem(BACKEND, "abc", false, || net.revoke(OWNER, invite));
    assert_eq!(outcome, Redeem::Conflict);
    assert_eq!(net.members("abc"), 0);
}

/// An invite revoked before the backend's snapshot never adds a member,
/// even though the backend's replica still holds the live invite.
#[test]
fn an_invite_revoked_before_the_snapshot_never_adds_a_member() {
    let net = Net::new(3);
    let invite = net.write_invite(OWNER, "abc");
    // The backend caches the live invite...
    assert_eq!(net.redeem(BACKEND, "abc", false, || {}), Redeem::Joined);
    // ...then the owner revokes it; the backend only hears about audits.
    net.revoke(OWNER, invite);
    net.unrelated_receipt(BACKEND);
    for _ in 0..3 {
        let outcome = net.redeem(BACKEND, "abc", false, || {});
        assert_ne!(outcome, Redeem::Joined);
    }
    assert_eq!(net.members("abc"), 1);
}

/// A single-use invite redeemed on one backend is seen as taken on another.
#[test]
fn a_redemption_on_another_backend_is_seen_as_taken() {
    let net = Net::new(3);
    net.write_invite(OWNER, "abc");
    assert_eq!(net.redeem(BACKEND, "abc", true, || {}), Redeem::Joined);
    net.unrelated_receipt(OTHER);
    assert_ne!(net.redeem(OTHER, "abc", true, || {}), Redeem::Joined);
    assert_eq!(net.members("abc"), 1);
}

/// Two backends race for one single-use invite: exactly one joins.
#[test]
fn racing_redemptions_of_a_single_use_invite_admit_exactly_one() {
    let net = Net::new(3);
    net.write_invite(OWNER, "abc");
    let other = std::cell::Cell::new(None);
    let first = net.redeem(BACKEND, "abc", true, || {
        other.set(Some(net.redeem(OTHER, "abc", true, || {})))
    });
    let outcomes = [first, other.take().unwrap()];
    assert_eq!(
        outcomes.iter().filter(|o| **o == Redeem::Joined).count(),
        1,
        "{outcomes:?}"
    );
    assert_eq!(net.members("abc"), 1);
}

/// Offline, the read answers from the replica; the transaction commits
/// locally and the authority accepts it once it syncs, because its reads hold.
#[test]
fn an_offline_prepared_commit_succeeds_while_its_reads_hold() {
    let mut net = Net::new(3);
    net.write_invite(OWNER, "abc");
    assert_eq!(net.redeem(BACKEND, "abc", false, || {}), Redeem::Joined);
    net.go_offline(BACKEND);
    let db = net.db(BACKEND);
    let open = OpenTransactionId::new();
    block_on(db.begin_serializable(open)).unwrap();
    let invite = db.prepare_query(&invite_query("abc")).unwrap();
    assert_eq!(
        block_on(db.serializable_tx_ref(open).all_prepared(&invite))
            .unwrap()
            .len(),
        1,
        "the replica answers offline"
    );
    block_on(
        db.serializable_tx_ref(open)
            .insert("members", code("abc"), Default::default()),
    )
    .unwrap();
    let receipt = block_on(db.commit_serializable_handle(open)).unwrap();
    net.go_online(BACKEND);
    assert!(net.settle(BACKEND, receipt.0).is_ok());
    assert_eq!(net.members("abc"), 2);
}

/// An invite revoked while the backend was offline: its offline-prepared
/// redemption read the stale invite, so the authority rejects it.
#[test]
fn an_offline_prepared_redemption_of_a_revoked_invite_conflicts() {
    let mut net = Net::new(3);
    let invite = net.write_invite(OWNER, "abc");
    assert_eq!(net.redeem(BACKEND, "abc", false, || {}), Redeem::Joined);
    net.go_offline(BACKEND);
    net.revoke(OWNER, invite);
    let db = net.db(BACKEND);
    let open = OpenTransactionId::new();
    block_on(db.begin_serializable(open)).unwrap();
    let prepared = db.prepare_query(&invite_query("abc")).unwrap();
    assert_eq!(
        block_on(db.serializable_tx_ref(open).all_prepared(&prepared))
            .unwrap()
            .len(),
        1,
        "stale replica"
    );
    block_on(
        db.serializable_tx_ref(open)
            .insert("members", code("abc"), Default::default()),
    )
    .unwrap();
    let receipt = block_on(db.commit_serializable_handle(open)).unwrap();
    net.go_online(BACKEND);
    let error = net.settle(BACKEND, receipt.0).unwrap_err();
    common::assert_serialization_conflict(&error);
    assert_eq!(net.members("abc"), 1);
    // The rejected optimistic member is gone from the backend's replica.
    let members = net
        .db(BACKEND)
        .prepare_query(&members_query("abc"))
        .unwrap();
    assert_eq!(
        block_on(net.db(BACKEND).all(&members, ReadOpts::default()))
            .unwrap()
            .len(),
        1
    );
}

/// A serializable transaction that only reads still commits at the
/// authority, which re-runs its reads: a read that went stale conflicts.
#[test]
fn a_read_only_serializable_commit_validates_its_reads() {
    let mut net = Net::new(3);
    let invite = net.write_invite(OWNER, "abc");
    assert_eq!(net.redeem(BACKEND, "abc", false, || {}), Redeem::Joined);
    let read_only = |net: &Net| {
        let db = net.db(BACKEND);
        let open = OpenTransactionId::new();
        block_on(db.begin_serializable(open)).unwrap();
        let prepared = db.prepare_query(&invite_query("abc")).unwrap();
        assert_eq!(
            net.drive(db.serializable_tx_ref(open).all_prepared(&prepared))
                .unwrap()
                .len(),
            1
        );
        block_on(db.commit_serializable_handle(open)).unwrap()
    };
    let holds = read_only(&net);
    assert!(
        net.settle(BACKEND, holds.0).is_ok(),
        "an unchanged read commits"
    );
    net.go_offline(BACKEND);
    let stale = read_only(&net);
    net.revoke(OWNER, invite);
    net.go_online(BACKEND);
    let error = net.settle(BACKEND, stale.0).unwrap_err();
    common::assert_serialization_conflict(&error);
}

/// Offline, a backend that never received another backend's redemption
/// reads "nobody redeemed it"; that absence is a phantom at the authority.
#[test]
fn an_offline_absence_guard_conflicts_when_the_invite_was_taken() {
    let mut net = Net::new(3);
    net.write_invite(OWNER, "abc");
    // OTHER caches the invite and "no members" while online.
    {
        let db = net.db(OTHER);
        let open = OpenTransactionId::new();
        block_on(db.begin_serializable(open)).unwrap();
        for query in [invite_query("abc"), members_query("abc")] {
            let prepared = db.prepare_query(&query).unwrap();
            net.drive(db.serializable_tx_ref(open).all_prepared(&prepared))
                .unwrap();
        }
        db.abandon_serializable_handle(open).unwrap();
    }
    net.go_offline(OTHER);
    assert_eq!(net.redeem(BACKEND, "abc", true, || {}), Redeem::Joined);
    let db = net.db(OTHER);
    let open = OpenTransactionId::new();
    block_on(db.begin_serializable(open)).unwrap();
    let members = db.prepare_query(&members_query("abc")).unwrap();
    // BACKEND's membership never reached OTHER's replica.
    let seen = block_on(db.serializable_tx_ref(open).all_prepared(&members))
        .unwrap()
        .len();
    block_on(
        db.serializable_tx_ref(open)
            .insert("members", code("abc"), Default::default()),
    )
    .unwrap();
    let receipt = block_on(db.commit_serializable_handle(open)).unwrap();
    net.go_online(OTHER);
    assert_eq!(seen, 0);
    let all = net
        .drive(
            net.db(OTHER)
                .wait_for_pending_writes(DurabilityTier::Global),
        )
        .unwrap_err();
    common::assert_serialization_conflict(&all);
    let error = net.settle(OTHER, receipt.0).unwrap_err();
    assert_eq!(
        error.code,
        ErrorCode::WriteRejected,
        "read {seen} members; the authority has 1"
    );
    common::assert_serialization_conflict(&error);
    assert_eq!(net.members("abc"), 1);
}

/// A transaction from a genesis snapshot (the replica knows no server
/// state) reads its replica offline and still commits when its reads hold.
#[test]
fn an_offline_commit_from_a_genesis_snapshot_is_allowed() {
    let mut net = Net::new(2);
    net.go_offline(BACKEND);
    let db = net.db(BACKEND);
    let open = OpenTransactionId::new();
    block_on(db.begin_serializable(open)).unwrap();
    let prepared = db.prepare_query(&invite_query("abc")).unwrap();
    assert!(
        block_on(db.serializable_tx_ref(open).all_prepared(&prepared))
            .unwrap()
            .is_empty()
    );
    block_on(db.serializable_tx_ref(open).insert(
        "audit",
        BTreeMap::from([("note".to_owned(), Value::String("x".to_owned()))]),
        Default::default(),
    ))
    .unwrap();
    let receipt = block_on(db.commit_serializable_handle(open)).unwrap();
    net.go_online(BACKEND);
    assert!(net.settle(BACKEND, receipt.0).is_ok());
}

/// Many clients race for the seats of one show, each in a serializable
/// transaction that books a free seat only if it holds none yet: no seat is
/// ever double-booked and nobody holds two seats.
#[test]
fn racing_seat_bookings_never_double_book() {
    let net = Net::new(6);
    for seat in 0..3 {
        let cells = BTreeMap::from([
            ("show".to_owned(), Value::String("s".to_owned())),
            ("holder".to_owned(), Value::String(String::new())),
        ]);
        net.core
            .seed_settled_mergeable_for_bootstrap(
                "seats",
                RowUuid::from_bytes([0x90 + seat; 16]),
                AuthorSubject::SYSTEM,
                cells,
            )
            .unwrap();
    }
    let free = Query::from("seats").filter(eq(col("holder"), lit(Value::String(String::new()))));
    let mut booked = 0;
    for round in 0..4 {
        // Every client opens, reads, and stages before anyone commits.
        let mut staged = Vec::new();
        for i in 0..6 {
            let db = net.db(i);
            let me = Value::String(format!("c{i}"));
            let open = OpenTransactionId::new();
            block_on(db.begin_serializable(open)).unwrap();
            let mine = db
                .prepare_query(&Query::from("seats").filter(eq(col("holder"), lit(me.clone()))))
                .unwrap();
            let free = db.prepare_query(&free).unwrap();
            let holds = !net
                .drive(db.serializable_tx_ref(open).all_prepared(&mine))
                .unwrap()
                .is_empty();
            let seats = net
                .drive(db.serializable_tx_ref(open).all_prepared(&free))
                .unwrap();
            if holds || seats.is_empty() {
                db.abandon_serializable_handle(open).unwrap();
                continue;
            }
            let seat = seats[(i + round) % seats.len()].row_uuid();
            let cells = BTreeMap::from([("holder".to_owned(), me)]);
            block_on(
                db.serializable_tx_ref(open)
                    .update("seats", seat, cells, Default::default()),
            )
            .unwrap();
            staged.push((i, open));
        }
        let receipts: Vec<(usize, u64)> = staged
            .into_iter()
            .map(|(i, open)| {
                (
                    i,
                    block_on(net.db(i).commit_serializable_handle(open))
                        .unwrap()
                        .0,
                )
            })
            .collect();
        for (i, tx) in receipts {
            booked += net.settle(i, tx).is_ok() as usize;
        }
    }
    let all = net.core.prepare_query(&Query::from("seats")).unwrap();
    let holders: Vec<String> = block_on(net.core.all(&all, ReadOpts::default()))
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
        .collect();
    let distinct: std::collections::BTreeSet<_> = holders.iter().collect();
    assert_eq!(holders.len(), 3, "every seat booked: {holders:?}");
    assert_eq!(
        distinct.len(),
        holders.len(),
        "nobody holds two seats: {holders:?}"
    );
    assert_eq!(booked, 3, "exactly one accepted booking per seat");
}

/// The snapshot includes the replica's own writes that were still unsettled
/// at begin: the transaction reads them, and the authority, resolving each
/// to where it settled, accepts the commit.
#[test]
fn a_serializable_read_sees_the_replicas_own_unsettled_writes() {
    let mut net = Net::new(1);
    net.go_offline(OWNER);
    let db = net.db(OWNER);
    let invite = block_on(db.insert("invites", code("own"), Default::default())).unwrap();

    let open = OpenTransactionId::new();
    block_on(db.begin_serializable(open)).unwrap();
    let query = db.prepare_query(&invite_query("own")).unwrap();
    assert_eq!(
        block_on(db.serializable_tx_ref(open).all_prepared(&query))
            .unwrap()
            .len(),
        1,
        "the snapshot holds the replica's own unsettled insert"
    );
    // A write after begin is not in the snapshot.
    block_on(db.insert("invites", code("own"), Default::default())).unwrap();
    assert_eq!(
        block_on(db.serializable_tx_ref(open).all_prepared(&query))
            .unwrap()
            .len(),
        1
    );
    block_on(db.serializable_tx_ref(open).update(
        "invites",
        invite.row_uuid(),
        code("own, redeemed"),
        Default::default(),
    ))
    .unwrap();
    // The second insert changed the read's result, which the replica
    // already holds: the commit conflicts without asking the authority.
    assert_eq!(
        block_on(db.commit_serializable_handle(open))
            .unwrap_err()
            .code,
        ErrorCode::TransactionConflict
    );
}

#[test]
fn a_commit_over_the_replicas_own_unsettled_writes_is_accepted() {
    let mut net = Net::new(1);
    net.go_offline(OWNER);
    let db = net.db(OWNER);
    let invite = block_on(db.insert("invites", code("own"), Default::default())).unwrap();

    let open = OpenTransactionId::new();
    block_on(db.begin_serializable(open)).unwrap();
    let query = db.prepare_query(&invite_query("own")).unwrap();
    assert_eq!(
        block_on(db.serializable_tx_ref(open).all_prepared(&query))
            .unwrap()
            .len(),
        1
    );
    block_on(db.serializable_tx_ref(open).update(
        "invites",
        invite.row_uuid(),
        code("own"),
        Default::default(),
    ))
    .unwrap();
    block_on(
        db.serializable_tx_ref(open)
            .insert("members", code("own"), Default::default()),
    )
    .unwrap();
    let receipt = block_on(db.commit_serializable_handle(open)).unwrap();
    net.go_online(OWNER);
    assert!(net.settle(OWNER, receipt.0).is_ok());
    assert_eq!(net.members("own"), 1);
}

/// A read that lists deleted rows is re-run the same way by the authority:
/// a revoked invite read with `include_deleted` still holds at commit.
#[test]
fn a_read_of_deleted_rows_validates_as_it_was_read() {
    let net = Net::new(1);
    let invite = net.write_invite(OWNER, "gone");
    net.revoke(OWNER, invite);
    let db = net.db(OWNER);
    let open = OpenTransactionId::new();
    block_on(db.begin_serializable(open)).unwrap();
    let query = db.prepare_query(&invite_query("gone")).unwrap();
    let tx = db.serializable_tx_ref(open);
    assert!(net.drive(tx.all_prepared(&query)).unwrap().is_empty());
    let read = net.drive(tx.all_prepared_with(&query, true)).unwrap();
    assert_eq!(
        read.iter()
            .map(|row| (row.row_uuid(), row.is_deleted()))
            .collect::<Vec<_>>(),
        [(invite, true)],
        "the deleted invite is listed"
    );
    net.drive(tx.insert("members", code("gone"), Default::default()))
        .unwrap();
    let receipt = net.drive(db.commit_serializable_handle(open)).unwrap();
    net.settle(OWNER, receipt.0)
        .expect("the authority re-reads deleted rows too");
}

/// Two transactions on one replica (a backend serving concurrent
/// requests) both find a seeded id absent and insert it. The second commit
/// is a conflict, so a retry finds the first one's row, never a storage
/// error about an object that already exists.
#[test]
fn racing_inserts_of_one_id_on_one_replica_conflict() {
    let net = Net::new(1);
    let db = net.db(0);
    let id = RowUuid::from_bytes([0x77; 16]);
    let one = || Query::from("invites").filter(eq(col("id"), lit(Value::Uuid(id.0))));
    let mut opens = Vec::new();
    for _ in 0..2 {
        let open = OpenTransactionId::new();
        block_on(db.begin_serializable(open)).unwrap();
        let prepared = db.prepare_query(&one()).unwrap();
        assert!(
            net.drive(db.serializable_tx_ref(open).all_prepared(&prepared))
                .unwrap()
                .is_empty()
        );
        net.drive(db.serializable_tx_ref(open).insert(
            "invites",
            code("seeded"),
            jazz::db::InsertOptions {
                row_id: Some(id),
                ..Default::default()
            },
        ))
        .unwrap();
        opens.push(open);
    }
    let first = net.drive(db.commit_serializable_handle(opens[0])).unwrap();
    let second = net.drive(db.commit_serializable_handle(opens[1]));
    let error = second.expect_err("the second insert of the id conflicts");
    assert_eq!(error.code, ErrorCode::TransactionConflict, "{error}");
    net.settle(0, first.0).expect("the first commit settles");
    let all = net.core.prepare_query(&one()).unwrap();
    assert_eq!(
        block_on(net.core.all(&all, ReadOpts::default()))
            .unwrap()
            .len(),
        1
    );
}

/// Offline, a serializable transaction's `Remote` read waits for the
/// authority rather than answering from the replica; it answers once the
/// link is back.
#[test]
fn an_offline_remote_read_in_a_transaction_waits_for_the_authority() {
    let mut net = Net::new(1);
    net.write_invite(OWNER, "cached");
    net.go_offline(OWNER);
    let db = net.db(OWNER);
    let open = OpenTransactionId::new();
    block_on(db.begin_serializable(open)).unwrap();
    let query = db.prepare_query(&invite_query("not cached")).unwrap();
    let tx = db.serializable_tx_ref(open);
    let mut read = pin!(tx.all_prepared_at(&query, false, None, jazz::db::ReadTier::Remote));
    let mut context = Context::from_waker(Waker::noop());
    for _ in 0..5 {
        assert!(read.as_mut().poll(&mut context).is_pending());
        net.pump();
    }
    // Back online (as `Net::go_online`, which the pending read's borrow
    // rules out).
    let (client_side, server_side) = duplex();
    let _upstream = block_on(db.connect_upstream(client_side));
    net.core
        .accept_subscriber(server_side, AuthorSubject::for_test_bytes([0x50; 16]));
    db.set_remote_link_hint(RemoteLinkHint::Live);
    assert!(net.drive(read).unwrap().is_empty());
    db.abandon_serializable_handle(open).unwrap();
}
