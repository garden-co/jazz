//! The shard role (SPEC 15 §15.6): a store opened with a clock draws
//! commit positions from a hybrid clock, closes at a position it observes,
//! and lands a cross-shard part at the position its gateway fixed, serving
//! nothing as settled past a part it still holds. An unsharded store takes
//! none of it.

use crate::common;

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, VecDeque};
use std::rc::Rc;

use jazz::db::shard::{CommitRef, HLC_COUNTER_BITS, HLC_MAX_OFFSET_MS, hlc, hlc_ms};
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
        db.shard().hold(part).unwrap_err(),
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
    db.shard().observe(at).unwrap();
    assert_eq!(db.shard().served_through(), at, "settled through it");
    insert(&db, "after");
    assert_eq!(newest(&db), at + 1, "nothing lands at or below it");
    let error = db
        .shard()
        .observe(hlc(NOW + HLC_MAX_OFFSET_MS + 1))
        .unwrap_err();
    assert!(
        error.to_string().contains("ahead of this store's clock"),
        "{error}"
    );
    db.shard().observe(hlc(NOW + HLC_MAX_OFFSET_MS)).unwrap();
}

#[test]
fn a_hold_keeps_what_is_served_as_settled_below_it() {
    let (db, _now) = shard(1);
    insert(&db, "before");
    let part = CommitRef {
        node: [9; 16],
        tx: 1,
    };
    let held = db.shard().hold(part).unwrap();
    assert_eq!(db.shard().hold(part).unwrap(), held, "held once");
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
    db.shard().release(part);
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
    let writer = block_on(Db::open(config(2))).unwrap();
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
    (writer, upstream, sent_up, sent_down)
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
    let held = db.shard().hold(part).unwrap();
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

/// A row the shard wrote past a part's hold cannot take the part under
/// it: holds do not block other writers yet, so the part is refused, said
/// so, instead of landing below a newer version of its row.
#[test]
fn a_part_whose_row_was_written_past_its_hold_is_refused() {
    let (db, _now) = shard(1);
    let (writer, _upstream, sent_up, sent_down) = writer_of(&db);
    let row = RowUuid::from_bytes([7; 16]);
    let part = upload(&writer, &sent_up, Some(row));
    let held = db.shard().hold(part).unwrap();
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
    assert!(
        format!("{error:?}").contains("not supported yet"),
        "{error:?}"
    );
    db.shard().release(part);
    assert_eq!(db.shard().lowest_hold(), None);
}
