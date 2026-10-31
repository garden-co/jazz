//! The shard role (SPEC 15 §15.6): a store opened with a clock draws
//! commit positions from a hybrid clock, closes at a position it observes,
//! and lands a cross-shard part at the position its gateway fixed, serving
//! nothing as settled past a part it still holds. An unsharded store takes
//! none of it.

use crate::common;

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, VecDeque};
use std::rc::Rc;

use jazz::db::shard::{CommitRef, HLC_COUNTER_BITS, HLC_MAX_OFFSET_MS, HeldRow, hlc, hlc_ms};
use jazz::db::{
    Db, DbConfig, DbIdentity, InsertOptions, Outcome, SyncMessage, Transport, TransportError,
    Upstream, block_on,
};
use jazz::ids::{AuthorSubject, NodeUuid, RowUuid};
use jazz::records::Value;
use jazz::tools::{ColumnType, SchemaBuilder, TableSchemaBuilder};

use common::{allow_all_policies, compile_schema};

const NOW: u64 = 1_790_000_000_000;

fn config(node: u8) -> DbConfig {
    let schema = compile_schema(
        &SchemaBuilder::new()
            .table(
                TableSchemaBuilder::new("docs")
                    .column("title", ColumnType::Text)
                    .policies(allow_all_policies()),
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
        db.shard().hold(part, &[]).unwrap_err(),
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
    let held = db.shard().hold(part, &[]).unwrap();
    assert_eq!(db.shard().hold(part, &[]).unwrap(), held, "held once");
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
    let held = db.shard().hold(part, &[]).unwrap();
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

/// The shard's own write (no session's commit, which would wait) to a row
/// past a part's hold cannot take the part under it: the part is refused,
/// said so, instead of landing below a newer version of its row.
#[test]
fn a_part_whose_row_the_shard_wrote_past_its_hold_is_refused() {
    let (db, _now) = shard(1);
    let (writer, _upstream, sent_up, sent_down) = writer_of(&db);
    let row = RowUuid::from_bytes([7; 16]);
    let part = upload(&writer, &sent_up, Some(row));
    let held = db.shard().hold(part, &[doc_row(row)]).unwrap();
    block_on(db.insert(
        "docs",
        BTreeMap::from([("title".to_owned(), Value::String("mine".to_owned()))]),
        InsertOptions {
            row_id: Some(row),
            ..Default::default()
        },
    ))
    .unwrap();
    db.shard().fix(part, held).unwrap();
    pump(&db, &writer);
    let Outcome::Refused { error, .. } = fate(&sent_down) else {
        panic!("accepted below a newer version of its row");
    };
    assert!(format!("{error:?}").contains("INV-SHARD-19"), "{error:?}");
    db.shard().release(part).unwrap();
    assert_eq!(db.shard().lowest_hold(), None);
}

/// Another session's commit to a row a part holds waits while the hold
/// stands, with what that session sent after it, and lands above it once
/// the hold goes. Another part's hold of the row is refused.
#[test]
fn a_hold_makes_other_writers_of_its_rows_wait() {
    let (db, _now) = shard(1);
    let row = RowUuid::from_bytes([7; 16]);
    let part = CommitRef {
        node: [9; 16],
        tx: 1,
    };
    let held = db.shard().hold(part, &[doc_row(row)]).unwrap();
    let error = db
        .shard()
        .hold(
            CommitRef {
                node: [9; 16],
                tx: 2,
            },
            &[doc_row(row)],
        )
        .unwrap_err();
    assert!(error.to_string().contains("held by another"), "{error}");
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
    let held = db.shard().hold(part, &[doc_row(row)]).unwrap();
    db.shard().fix(part, held).unwrap();
    drop(upstream);
    drop(db);

    let db = shard_at(&path, &now);
    assert_eq!(db.shard().lowest_hold(), Some(held), "the hold stands");
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
