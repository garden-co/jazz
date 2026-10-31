//! The shard role (SPEC 15 §15.6): a store opened with a clock draws
//! commit positions from a hybrid clock, closes at a position it observes,
//! and lands a cross-shard part at the position its gateway fixed, serving
//! nothing as settled past a part it still holds. An unsharded store takes
//! none of it.

use crate::common;

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, VecDeque};
use std::rc::Rc;

use jazz::db::shard::{
    CommitRef, HLC_COUNTER_BITS, HLC_MAX_OFFSET_MS, Held, HeldRow, Resolution, hlc, hlc_ms,
};
use jazz::db::{
    Db, DbConfig, DbIdentity, InsertOptions, Outcome, SyncMessage, Transport, TransportError,
    Upstream, block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::records::Value;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};

use common::{allow_all_policies, compile_schema};
use jazz::db::{Acting, WireOp};

const NOW: u64 = 1_790_000_000_000;

fn config(node: u8) -> DbConfig {
    let schema = compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("docs")
                    .column("title", ColumnType::Text)
                    .policies(allow_all_policies()),
            )
            // Writable by anyone, readable by no session: an update of a
            // note needs read permission on it, so a session's is refused.
            .table(
                TableSchemaBuilder::new("notes")
                    .column("title", ColumnType::Text)
                    .policies(common::allow_all_writes()),
            )
            .build(),
    );
    DbConfig::new(
        schema,
        DbIdentity {
            node: NodeUuid::from_bytes([node; 16]),
            author: AuthorSubject::SYSTEM,
        },
    )
}

/// A shard on a virtual clock the test moves.
fn shard(node: u8) -> (Db, Rc<Cell<u64>>) {
    let now = Rc::new(Cell::new(NOW));
    let clock = now.clone();
    let db = block_on(Db::open(config(node).with_clock(move || clock.get()))).unwrap();
    (db, now)
}

/// A shard kept in `path`, on the clock `now`.
fn shard_at(path: &std::path::Path, now: &Rc<Cell<u64>>) -> Db {
    let clock = now.clone();
    block_on(Db::open_at(
        config(1).with_clock(move || clock.get()),
        path,
        jazz::db::FileOptions::default(),
    ))
    .unwrap()
}

/// Holds `part` (writing `rows`), which must be held at once: its position.
fn held_at(db: &Db, part: CommitRef, rows: &[HeldRow]) -> u64 {
    match db.shard().hold(part, rows, b"the part").unwrap() {
        Held::At(at) => at,
        other => panic!("not held: {other:?}"),
    }
}

/// Polls `future` once, with a waker that does nothing.
fn poll_once<F: std::future::Future>(future: std::pin::Pin<&mut F>) -> std::task::Poll<F::Output> {
    future.poll(&mut std::task::Context::from_waker(std::task::Waker::noop()))
}

fn doc_row(row: RowUuid) -> HeldRow {
    HeldRow {
        table: "docs".to_owned(),
        row: *row.0.as_bytes(),
    }
}

/// The docs a store holds, by title.
fn titles(db: &Db) -> Vec<String> {
    let prepared = db.prepare_query(&jazz::query::Query::from("docs")).unwrap();
    let mut titles: Vec<String> = block_on(db.all(&prepared, Default::default()))
        .unwrap()
        .iter()
        .map(|row| match row.cell("title") {
            Some(Value::String(title)) => title,
            other => panic!("title {other:?}"),
        })
        .collect();
    titles.sort();
    titles
}

fn insert(db: &Db, title: &str) {
    block_on(db.insert(
        "docs",
        BTreeMap::from([("title".to_owned(), Value::String(title.to_owned()))]),
        InsertOptions::default(),
    ))
    .unwrap();
}

/// The position the store's newest commit landed at, as it serves it once
/// nothing is held.
fn newest(db: &Db) -> u64 {
    db.shard().served_through()
}

#[test]
fn an_unsharded_store_takes_no_shard_role() {
    let db = block_on(Db::open(config(1))).unwrap();
    let part = CommitRef {
        node: [9; 16],
        tx: 1,
    };
    assert!(!db.shard().is_shard());
    for error in [
        db.shard().observe(5).unwrap_err(),
        db.shard().hold(part, &[], b"").unwrap_err(),
        db.shard().fix(part, 5).unwrap_err(),
    ] {
        assert!(error.to_string().contains("not supported"), "{error}");
    }
    insert(&db, "one");
    insert(&db, "two");
    assert_eq!(db.shard().served_through(), 2, "positions count up by one");
    assert_eq!(db.shard().lowest_hold(), None);
}

#[test]
fn a_shard_draws_positions_from_its_clock() {
    assert_eq!(hlc_ms(hlc(NOW)), NOW);
    assert_eq!(hlc(NOW + 1) - hlc(NOW), 1 << HLC_COUNTER_BITS);
    let (db, now) = shard(1);
    assert!(db.shard().is_shard());
    insert(&db, "one");
    let first = newest(&db);
    assert_eq!(first, hlc(NOW), "the clock's first position");
    insert(&db, "two");
    assert_eq!(newest(&db), first + 1, "within a ms, the counter");
    now.set(NOW + 10);
    insert(&db, "three");
    assert_eq!(newest(&db), hlc(NOW + 10), "the clock moved on");
    // A clock that steps back never takes a position back.
    now.set(NOW);
    insert(&db, "four");
    assert_eq!(newest(&db), hlc(NOW + 10) + 1);
}

#[test]
fn observing_a_position_closes_the_shard_there() {
    let (db, _now) = shard(1);
    let at = hlc(NOW + 100);
    assert_eq!(db.shard().observe(at).unwrap(), at);
    assert_eq!(db.shard().served_through(), at, "settled through it");
    insert(&db, "after");
    assert_eq!(newest(&db), at + 1, "nothing lands at or below it");
}

/// A position further ahead of the clock than the offset bound is
/// observed only as far as the bound; observed again once the clock has
/// caught up, it closes the shard there.
#[test]
fn a_far_ahead_position_is_observed_up_to_the_offset_bound() {
    let (db, now) = shard(1);
    let bound = hlc(NOW + HLC_MAX_OFFSET_MS);
    let far = hlc(NOW + HLC_MAX_OFFSET_MS + 1_000);
    assert_eq!(db.shard().observe(far).unwrap(), bound);
    assert_eq!(db.shard().served_through(), bound);
    insert(&db, "after");
    assert_eq!(
        newest(&db),
        bound + 1,
        "nothing lands at or below the bound"
    );
    now.set(NOW + 1_000);
    assert_eq!(db.shard().observe(far).unwrap(), far);
    assert_eq!(db.shard().served_through(), far);
}

#[test]
fn a_hold_keeps_what_is_served_as_settled_below_it() {
    let (db, _now) = shard(1);
    insert(&db, "before");
    let part = CommitRef {
        node: [9; 16],
        tx: 1,
    };
    let held = held_at(&db, part, &[]);
    assert_eq!(held_at(&db, part, &[]), held, "held once");
    assert_eq!(db.shard().lowest_hold(), Some(held));
    insert(&db, "meanwhile");
    assert_eq!(
        db.shard().served_through(),
        held - 1,
        "a commit accepted past the hold is not yet settled"
    );
    let error = db.shard().fix(part, held - 1).unwrap_err();
    assert!(
        error.to_string().contains("below the part's hold"),
        "{error}"
    );
    let other = CommitRef {
        node: [9; 16],
        tx: 2,
    };
    assert!(db.shard().fix(other, held).is_err(), "fixed only once held");
    db.shard().release(part).unwrap();
    assert_eq!(db.shard().lowest_hold(), None);
    assert_eq!(db.shard().served_through(), held + 1);
}

struct Pipe {
    send: Rc<RefCell<VecDeque<SyncMessage>>>,
    recv: Rc<RefCell<VecDeque<SyncMessage>>>,
    log: Rc<RefCell<Vec<SyncMessage>>>,
}

impl Transport for Pipe {
    fn send(&mut self, message: SyncMessage) -> Result<(), TransportError> {
        self.log.borrow_mut().push(message.clone());
        self.send.borrow_mut().push_back(message);
        Ok(())
    }

    fn try_recv(&mut self) -> Option<SyncMessage> {
        self.recv.borrow_mut().pop_front()
    }
}

type Log = Rc<RefCell<Vec<SyncMessage>>>;

/// A writer connected to `db`, with what each side sent.
fn writer_of(db: &Db) -> (Db, Upstream, Log, Log) {
    writer_on(db, 2)
}

fn writer_on(db: &Db, node: u8) -> (Db, Upstream, Log, Log) {
    let writer = block_on(Db::open(config(node))).unwrap();
    let (upstream, sent_up, sent_down) = connect(&writer, db);
    (writer, upstream, sent_up, sent_down)
}

/// `writer` connected to `db`, with what each side sent.
fn connect(writer: &Db, db: &Db) -> (Upstream, Log, Log) {
    let up = Rc::new(RefCell::new(VecDeque::new()));
    let down = Rc::new(RefCell::new(VecDeque::new()));
    let sent_up = Rc::new(RefCell::new(Vec::new()));
    let sent_down = Rc::new(RefCell::new(Vec::new()));
    let upstream = block_on(writer.connect_upstream(Box::new(Pipe {
        send: up.clone(),
        recv: down.clone(),
        log: sent_up.clone(),
    })));
    db.accept_subscriber(
        Box::new(Pipe {
            send: down.clone(),
            recv: up.clone(),
            log: sent_down.clone(),
        }),
        AuthorSubject::SYSTEM,
    )
    .unwrap();
    for _ in 0..4 {
        block_on(writer.tick()).unwrap();
        block_on(db.tick()).unwrap();
    }
    (upstream, sent_up, sent_down)
}

/// The writer's next commit, uploaded but not yet arrived.
fn upload(writer: &Db, sent_up: &Log, row: Option<RowUuid>) -> CommitRef {
    block_on(writer.insert(
        "docs",
        BTreeMap::from([("title".to_owned(), Value::String("theirs".to_owned()))]),
        InsertOptions {
            row_id: row,
            ..Default::default()
        },
    ))
    .unwrap();
    block_on(writer.tick()).unwrap();
    sent_up
        .borrow()
        .iter()
        .rev()
        .find_map(|m| match m {
            SyncMessage::Commit { id, .. } => Some(CommitRef::from(id)),
            _ => None,
        })
        .expect("the writer uploaded its commit")
}

fn pump(db: &Db, writer: &Db) {
    for _ in 0..4 {
        block_on(db.tick()).unwrap();
        block_on(writer.tick()).unwrap();
    }
}

fn fate(sent_down: &Log) -> Outcome {
    sent_down
        .borrow()
        .iter()
        .rev()
        .find_map(|m| match m {
            SyncMessage::Fate { outcome, .. } => Some(outcome.clone()),
            _ => None,
        })
        .expect("a fate")
}

/// A writer's commit, held and fixed by its gateway before it arrives,
/// lands at the fixed position: under a commit the shard accepted since the
/// hold, and settled only once it landed.
#[test]
fn a_fixed_part_lands_at_its_fixed_position() {
    let (db, _now) = shard(1);
    let (writer, _upstream, sent_up, sent_down) = writer_of(&db);
    let part = upload(&writer, &sent_up, None);
    let held = held_at(&db, part, &[]);
    insert(&db, "the shard's own");
    let own = held + 1;
    assert_eq!(db.shard().served_through(), held - 1);
    // Every other shard's hold for the commit was lower: it lands at this
    // one's.
    db.shard().fix(part, held).unwrap();
    pump(&db, &writer);
    assert_eq!(db.shard().lowest_hold(), None, "it landed");
    assert_eq!(db.shard().served_through(), own, "and everything settled");
    let Outcome::Accepted { at, .. } = fate(&sent_down) else {
        panic!("refused");
    };
    assert_eq!(at, vec![(None, held)], "at its fixed position");
}

/// The shard's own local write to a row a part holds waits until the part
/// lands, then lands above it: the part is never refused for a newer
/// version of its row (INV-SHARD-19).
#[test]
fn a_local_write_of_a_held_row_waits_for_the_part() {
    let (db, _now) = shard(1);
    let (writer, _upstream, sent_up, sent_down) = writer_of(&db);
    let row = RowUuid::from_bytes([7; 16]);
    let part = upload(&writer, &sent_up, Some(row));
    let held = held_at(&db, part, &[doc_row(row)]);
    let mine = db.upsert(
        "docs",
        row,
        BTreeMap::from([("title".to_owned(), Value::String("mine".to_owned()))]),
        Default::default(),
    );
    let mut mine = std::pin::pin!(mine);
    assert!(
        poll_once(mine.as_mut()).is_pending(),
        "the local write waits"
    );
    db.shard().fix(part, held).unwrap();
    pump(&db, &writer);
    let Outcome::Accepted { at, .. } = fate(&sent_down) else {
        panic!("the part was refused");
    };
    assert_eq!(at, vec![(None, held)], "at its fixed position");
    let std::task::Poll::Ready(written) = poll_once(mine.as_mut()) else {
        panic!("the local write still waits once the part landed");
    };
    written.unwrap();
    assert_eq!(titles(&db), vec!["mine"]);
    assert!(newest(&db) > held, "above the part");
}

/// Another session's commit to a row a part holds waits while the hold
/// stands, with what that session sent after it, and lands above it once
/// the hold goes. Another part's hold of the row is queued, and granted
/// once the first part's hold goes.
#[test]
fn a_hold_makes_other_writers_of_its_rows_wait() {
    let (db, _now) = shard(1);
    let row = RowUuid::from_bytes([7; 16]);
    let part = CommitRef {
        node: [9; 16],
        tx: 1,
    };
    let held = held_at(&db, part, &[doc_row(row)]);
    let second = CommitRef {
        node: [9; 16],
        tx: 2,
    };
    assert_eq!(
        db.shard().hold(second, &[doc_row(row)], b"").unwrap(),
        Held::Queued
    );
    assert_eq!(
        db.shard().hold(second, &[doc_row(row)], b"").unwrap(),
        Held::Queued
    );
    assert!(db.shard().take_granted().unwrap().is_empty(), "not yet");
    let (writer, _upstream, sent_up, sent_down) = writer_of(&db);
    upload(&writer, &sent_up, Some(row));
    upload(&writer, &sent_up, None);
    pump(&db, &writer);
    assert!(
        !sent_down
            .borrow()
            .iter()
            .any(|m| matches!(m, SyncMessage::Fate { .. })),
        "the commit, and the one after it, wait"
    );
    assert!(titles(&db).is_empty());
    db.shard().release(part).unwrap();
    let granted = db.shard().take_granted().unwrap();
    assert_eq!(granted.len(), 1);
    assert_eq!(granted[0].0, second, "the queued hold is granted");
    assert!(granted[0].1 > held);
    // The session's commit now waits on the second part.
    pump(&db, &writer);
    assert!(titles(&db).is_empty(), "still held, by the second part");
    db.shard().release(second).unwrap();
    pump(&db, &writer);
    assert_eq!(titles(&db), vec!["theirs", "theirs"]);
    let Outcome::Accepted { at, .. } = fate(&sent_down) else {
        panic!("refused");
    };
    assert!(at[0].1 > held, "above the hold");
}

/// Holds and fixed positions are durable: a shard reopened between a
/// part's fix and its arrival still holds it, and lands it at the fixed
/// position when its writer sends it again.
#[test]
fn holds_and_fixes_survive_a_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("shard.sqlite");
    let now = Rc::new(Cell::new(NOW));
    let db = shard_at(&path, &now);
    let row = RowUuid::from_bytes([7; 16]);
    let writer = block_on(Db::open(config(2))).unwrap();
    let (upstream, sent_up, _) = connect(&writer, &db);
    // The writer's commit is in flight when the shard goes down.
    let part = upload(&writer, &sent_up, Some(row));
    let held = held_at(&db, part, &[doc_row(row)]);
    db.shard().fix(part, held).unwrap();
    drop(upstream);
    drop(db);

    let db = shard_at(&path, &now);
    assert_eq!(db.shard().lowest_hold(), Some(held), "the hold stands");
    assert_eq!(
        db.shard().part(part).unwrap().as_deref(),
        Some(&b"the part"[..]),
        "with its part's data"
    );
    assert_eq!(db.shard().holds(), vec![(part, held, Some(held))]);
    assert_eq!(db.shard().served_through(), held - 1);
    let (_upstream, _, sent_down) = connect(&writer, &db);
    pump(&db, &writer);
    let Outcome::Accepted { at, .. } = fate(&sent_down) else {
        panic!("refused");
    };
    assert_eq!(at, vec![(None, held)], "at its fixed position");
    assert_eq!(db.shard().lowest_hold(), None, "it landed");
    drop(db);
    let db = shard_at(&path, &now);
    assert_eq!(db.shard().lowest_hold(), None, "and its hold went with it");
}

/// A reopened shard keeps no record of the positions it observed past what
/// it numbered, so once it observed one it waits out its offset bound past
/// its clock before numbering: a session's
/// commit waits until its clock passes the bound, then lands above any
/// position the shard may have closed at before, at its clock (not ahead
/// of it, where its peers would refuse to fix a part).
#[test]
fn a_reopened_shard_waits_out_its_offset_bound() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("shard.sqlite");
    let now = Rc::new(Cell::new(NOW));
    let db = shard_at(&path, &now);
    insert(&db, "before");
    drop(db);
    let db = shard_at(&path, &now);
    assert_eq!(
        db.shard().waits_out(),
        None,
        "a shard that observed nothing past what it numbered waits nothing out"
    );
    let closed = db.shard().observe(hlc(NOW + 400)).unwrap();
    assert_eq!(db.shard().served_through(), closed);
    drop(db);
    let db = shard_at(&path, &now);
    assert_eq!(db.shard().waits_out(), Some(NOW + HLC_MAX_OFFSET_MS));
    let (writer, _upstream, sent_up, sent_down) = writer_on(&db, 2);
    upload(&writer, &sent_up, None);
    pump(&db, &writer);
    assert_eq!(titles(&db), vec!["before"], "the commit waits");
    now.set(NOW + HLC_MAX_OFFSET_MS + 1);
    pump(&db, &writer);
    assert_eq!(db.shard().waits_out(), None);
    let Outcome::Accepted { at, .. } = fate(&sent_down) else {
        panic!("refused");
    };
    assert_eq!(
        at,
        vec![(None, hlc(NOW + HLC_MAX_OFFSET_MS + 1))],
        "at its clock"
    );
    assert!(newest(&db) > closed, "nothing lands at or below the close");
}

/// A cross-shard commit is decided once on its decider: aborted when the
/// abort comes first (the part's resend is then refused, and a hold of it
/// answers the decision), committed when the part landed first (an abort
/// then answers that).
#[test]
fn a_decider_decides_a_cross_shard_commit_once() {
    let (db, _now) = shard(1);
    let (writer, _upstream, sent_up, sent_down) = writer_of(&db);
    let part = upload(&writer, &sent_up, None);
    held_at(&db, part, &[]);
    assert_eq!(db.shard().resolution(part).unwrap(), None);
    assert_eq!(db.shard().abort(part).unwrap(), Resolution::Aborted);
    assert_eq!(db.shard().lowest_hold(), None, "the hold went");
    assert_eq!(db.shard().abort(part).unwrap(), Resolution::Aborted);
    pump(&db, &writer);
    let Outcome::Refused { .. } = fate(&sent_down) else {
        panic!("the part landed after its commit was aborted");
    };
    assert!(titles(&db).is_empty());
    assert_eq!(
        db.shard().hold(part, &[], b"").unwrap(),
        Held::Decided(Resolution::Aborted)
    );

    let (writer, _upstream, sent_up, sent_down) = writer_on(&db, 3);
    let part = upload(&writer, &sent_up, None);
    let held = held_at(&db, part, &[]);
    db.shard().fix(part, held).unwrap();
    pump(&db, &writer);
    let Outcome::Accepted { .. } = fate(&sent_down) else {
        panic!("refused");
    };
    assert_eq!(db.shard().abort(part).unwrap(), Resolution::Committed(held));
    assert_eq!(
        db.shard().resolution(part).unwrap(),
        Some(Resolution::Committed(held))
    );
    assert_eq!(
        db.shard().hold(part, &[], b"").unwrap(),
        Held::Decided(Resolution::Committed(held))
    );
}

/// Queued holds are durable: a shard reopened while a hold waits behind
/// another part's still has it queued, with its part, and grants it once
/// the rows are free.
#[test]
fn queued_holds_survive_a_reopen() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("shard.sqlite");
    let now = Rc::new(Cell::new(NOW));
    let db = shard_at(&path, &now);
    let row = RowUuid::from_bytes([7; 16]);
    let first = CommitRef {
        node: [9; 16],
        tx: 1,
    };
    let second = CommitRef {
        node: [9; 16],
        tx: 2,
    };
    held_at(&db, first, &[doc_row(row)]);
    assert_eq!(
        db.shard()
            .hold(second, &[doc_row(row)], b"the second part")
            .unwrap(),
        Held::Queued
    );
    drop(db);

    let db = shard_at(&path, &now);
    assert_eq!(
        db.shard()
            .hold(second, &[doc_row(row)], b"the second part")
            .unwrap(),
        Held::Queued,
        "still queued, not held twice"
    );
    now.set(NOW + HLC_MAX_OFFSET_MS + 1);
    assert!(db.shard().take_granted().unwrap().is_empty(), "not yet");
    db.shard().release(first).unwrap();
    let granted = db.shard().take_granted().unwrap();
    assert_eq!(granted.len(), 1);
    assert_eq!(granted[0].0, second, "the queued hold is granted");
    assert_eq!(
        db.shard().part(second).unwrap().as_deref(),
        Some(&b"the second part"[..]),
        "with its part"
    );
    db.shard().release(second).unwrap();
    drop(db);
    let db = shard_at(&path, &now);
    assert!(db.shard().holds().is_empty());
    assert!(
        db.shard().take_granted().unwrap().is_empty(),
        "nothing queued"
    );
}

/// The ops of the writer's last uploaded commit.
fn uploaded_ops(sent_up: &Log) -> Vec<WireOp> {
    sent_up
        .borrow()
        .iter()
        .rev()
        .find_map(|m| match m {
            SyncMessage::Commit { ops, .. } => Some(ops.clone()),
            _ => None,
        })
        .expect("the writer uploaded its commit")
}

/// A shard checks a cross-shard part when it holds it, as its writer's
/// session would commit it, and writes nothing: a part its session may
/// write passes, one it may not is refused (and its hold is never taken,
/// so no fix of it can be refused later).
#[test]
fn a_part_is_checked_as_its_session_without_writing() {
    let (db, _now) = shard(1);
    let (writer, _upstream, sent_up, _sent_down) = writer_of(&db);
    let session = AuthorSubject::for_test_bytes([5; 16]);
    let part = upload(&writer, &sent_up, None);
    db.check_part(
        session,
        None,
        Acting::Own,
        (part.node, part.tx),
        uploaded_ops(&sent_up),
    )
    .expect("a doc anyone may write");
    assert!(titles(&db).is_empty(), "the check wrote nothing");
    assert_eq!(db.shard().resolution(part).unwrap(), None, "nor a fate");
    pump(&db, &writer);
    assert_eq!(titles(&db), vec!["theirs"], "the part itself lands after");

    // A note, then its update: the update needs read permission on it.
    let note = RowUuid::from_bytes([8; 16]);
    block_on(writer.insert(
        "notes",
        BTreeMap::from([("title".to_owned(), Value::String("a note".to_owned()))]),
        InsertOptions {
            row_id: Some(note),
            ..Default::default()
        },
    ))
    .unwrap();
    pump(&db, &writer);
    block_on(writer.update(
        "notes",
        note,
        BTreeMap::from([("title".to_owned(), Value::String("changed".to_owned()))]),
        Default::default(),
    ))
    .unwrap();
    block_on(writer.tick()).unwrap();
    let update = uploaded_ops(&sent_up);
    let error = db
        .check_part(session, None, Acting::Own, ([6; 16], 1), update)
        .expect_err("a session may not update a note it cannot read");
    assert!(error.to_string().contains("read"), "{error}");
}

/// A replica applies its leader's log at the leader's positions: a local
/// write, a hold and a fixed position land where the leader numbered them,
/// however far its own clock is off, and an observed position is followed
/// past the replica's offset bound. Taking over, it waits until its clock
/// passes the highest position it applied plus the offset bound.
#[test]
fn a_replica_numbers_at_its_leaders_positions_and_waits_out_the_bound_to_lead() {
    let (leader, leader_now) = shard(1);
    let (replica, replica_now) = shard(2);
    replica_now.set(NOW - 2_000);
    leader_now.set(NOW + 50);
    insert(&leader, "one");
    let one = newest(&leader);
    replica.shard().number_at(one).unwrap();
    insert(&replica, "one");
    assert_eq!(newest(&replica), one, "at the leader's position");
    assert!(
        replica.shard().number_at(one).is_err(),
        "never at or below what it numbered"
    );
    let part = CommitRef {
        node: [7; 16],
        tx: 3,
    };
    let rows = [doc_row(RowUuid::from_bytes([5; 16]))];
    let hold = held_at(&leader, part, &rows);
    replica.shard().number_at(hold).unwrap();
    assert_eq!(held_at(&replica, part, &rows), hold);
    let fixed = hold + 10;
    leader.shard().fix(part, fixed).unwrap();
    replica.shard().follow(fixed).unwrap();
    replica.shard().fix(part, fixed).unwrap();
    assert_eq!(replica.shard().holds(), leader.shard().holds());
    let far = hlc(NOW + 400);
    assert_eq!(leader.shard().observe(far).unwrap(), far);
    replica.shard().follow(far).unwrap();
    assert_eq!(replica.shard().numbered(), leader.shard().numbered());
    replica.shard().release(part).unwrap();
    replica.shard().lead().unwrap();
    assert_eq!(
        replica.shard().waits_out(),
        Some(hlc_ms(far) + HLC_MAX_OFFSET_MS),
        "the highest position it applied, plus the bound"
    );
    replica_now.set(hlc_ms(far) + HLC_MAX_OFFSET_MS + 1);
    assert_eq!(replica.shard().waits_out(), None);
    insert(&replica, "two");
    assert_eq!(newest(&replica), hlc(replica_now.get()), "at its own clock");
}
