//! The shard role: what a sharded deployment's gateway needs of a jazz
//! store serving one shard (SPEC 15 §15.6). It is an engine API, with no
//! wire of its own: the gateway↔shard protocol that drives it lives with
//! the gateway.
//!
//! A shard draws commit positions from a hybrid clock ([`hlc`]), so the
//! positions of every shard compare by time. The gateway closes a quiet
//! shard at a position by having it [`ShardRole::observe`] that position,
//! and lands each part of a cross-shard commit at the one position all its
//! parts share: it [`ShardRole::hold`]s a position on every shard the
//! commit writes, then [`ShardRole::fix`]es the part's position at the
//! highest hold before the part arrives. While a hold stands, what the
//! store serves as settled ([`ShardRole::served_through`]) stays below it.
//!
//! Holds and fixed positions are durable: they persist with the store and
//! stand again when it reopens, and a reopened shard waits out
//! [`HLC_MAX_OFFSET_MS`] past its clock before numbering its sessions'
//! commits ([`ShardRole::waits_out`]), so it numbers above anything it may
//! have observed before. While a hold stands, a commit writing one of the rows
//! its part writes waits (another writer's), or is refused (another
//! cross-shard part's hold, which could otherwise wait on it in turn).
//!
//! A store opened without [`crate::options::DbConfig::with_clock`] takes
//! none of this: unsharded, it counts positions up by one and holds
//! nothing, so every call below except `served_through` is refused.

use std::collections::{BTreeMap, HashMap};

use super::{Db, Inner};
use crate::options::{Error, ErrorCode};
use crate::values::storage;

/// Commit positions are hybrid-clock timestamps: the wall clock in ms since
/// [`HLC_EPOCH_MS`] above [`HLC_COUNTER_BITS`] bits of counter, so every
/// position fits a JS number (Durable Object SQLite returns integers as
/// one): 42 bits of ms last 139 years, and a node handing out more than
/// 2,048 in a millisecond runs ahead of its clock until the clock catches
/// up. Positions of different nodes then compare by time, and every part
/// of a cross-shard commit can land at one.
pub const HLC_EPOCH_MS: u64 = 1_735_689_600_000;
/// The counter bits below the ms of an [`hlc`] position.
pub const HLC_COUNTER_BITS: u32 = 11;
/// How far ahead of this node's clock an observed position may be (ms):
/// further is a peer whose clock (or claim) cannot be trusted.
pub const HLC_MAX_OFFSET_MS: u64 = 500;

/// The first position at wall-clock time `ms` (Unix ms).
pub fn hlc(ms: u64) -> u64 {
    ms.saturating_sub(HLC_EPOCH_MS) << HLC_COUNTER_BITS
}

/// The wall-clock ms (Unix) a position was handed out at, or, for a node
/// that ran ahead of its clock, the ms it ran ahead to.
pub fn hlc_ms(position: u64) -> u64 {
    (position >> HLC_COUNTER_BITS) + HLC_EPOCH_MS
}

/// The system clock, in Unix ms.
pub fn wall_clock_ms() -> u64 {
    web_time::SystemTime::now()
        .duration_since(web_time::UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}

/// A commit by its writer: the node that wrote it and that node's own
/// number for it, as its upload names it.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct CommitRef {
    pub node: [u8; 16],
    pub tx: u64,
}

impl From<&crate::tx::TxId> for CommitRef {
    /// The commit a `SyncMessage::Commit` names by its id.
    fn from(id: &crate::tx::TxId) -> Self {
        Self {
            node: *id.node.as_bytes(),
            tx: id.time.0,
        }
    }
}

/// A row a cross-shard part writes, by the stored table its upload names
/// it in (see [`HeldRow::of`]).
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct HeldRow {
    pub table: String,
    pub row: [u8; 16],
}

impl HeldRow {
    /// The rows a part's ops write, each once.
    pub fn of(ops: &[super::WireOp]) -> Vec<HeldRow> {
        let mut rows: Vec<HeldRow> = ops
            .iter()
            .map(|op| HeldRow {
                table: op.table().to_owned(),
                row: op.write.row(),
            })
            .collect();
        rows.sort();
        rows.dedup();
        rows
    }
}

/// A store's shard state: its clock, and the positions it holds for
/// cross-shard parts still to land.
#[derive(Default)]
pub(crate) struct ShardState {
    /// The wall clock (Unix ms) positions are drawn from; `None` (an
    /// unsharded store) counts them up by one.
    pub(crate) clock: Option<Box<dyn Fn() -> u64>>,
    /// Held position → the part it is held for.
    holds: BTreeMap<u64, CommitRef>,
    /// Parts to land at a fixed position when they arrive.
    fixed: HashMap<CommitRef, u64>,
    /// The rows each held part writes, and who holds each row.
    rows: HashMap<HeldRow, CommitRef>,
    /// On a reopened shard, the clock reading (ms) it waits out before
    /// numbering: its clock at reopen plus the offset bound.
    resumes_after: Option<u64>,
    /// Whether [`SHARD_OBSERVED`] is set.
    observed: bool,
}

/// The `_jazz_meta` key a clocked store sets the first time it observes a
/// position past what it numbered: from then on, it may have served a
/// position it keeps no record of.
const SHARD_OBSERVED: &str = "shard_observed";

/// The durable record of holds: one row per held part, with its fixed
/// position once fixed, and one per row it writes. Created by the first
/// hold, so an unsharded store's file never holds them.
const HOLDS_DDL: &str = "\
    CREATE TABLE IF NOT EXISTS _jazz_shard_holds (at INTEGER PRIMARY KEY, node BLOB NOT NULL, tx INTEGER NOT NULL, fixed INTEGER, UNIQUE (node, tx));\
    CREATE TABLE IF NOT EXISTS _jazz_shard_held_rows (node BLOB NOT NULL, tx INTEGER NOT NULL, tbl TEXT NOT NULL, id BLOB NOT NULL, PRIMARY KEY (tbl, id));";

impl ShardState {
    pub(crate) fn new(clock: Option<Box<dyn Fn() -> u64>>) -> Self {
        Self {
            clock,
            ..Self::default()
        }
    }

    /// The lowest position held for a part still to land.
    pub(crate) fn lowest_hold(&self) -> Option<u64> {
        self.holds.keys().next().copied()
    }

    /// The position the part `commit` lands at, when one was fixed.
    pub(crate) fn fixed(&self, commit: CommitRef) -> Option<u64> {
        self.fixed.get(&commit).copied()
    }

    /// The part `commit` landed (or was refused): it holds nothing more.
    pub(crate) fn landed(&mut self, commit: CommitRef) {
        self.holds.retain(|_, held| *held != commit);
        self.fixed.remove(&commit);
        self.rows.retain(|_, held| *held != commit);
    }

    /// Whether a commit `by` writing `rows` waits: another part holds one
    /// of them.
    pub(crate) fn blocks(&self, by: CommitRef, rows: &[HeldRow]) -> bool {
        !self.rows.is_empty()
            && rows
                .iter()
                .any(|row| self.rows.get(row).is_some_and(|held| *held != by))
    }

    fn held(&self, commit: CommitRef) -> Option<u64> {
        self.holds
            .iter()
            .find_map(|(&at, held)| (*held == commit).then_some(at))
    }
}

impl Inner {
    /// The position the next commit lands at: above every position handed
    /// out or observed, and on a shard no earlier than its clock (past the
    /// offset bound it waits out after a reopen, for a write that does not
    /// wait).
    pub(crate) fn next_seq(&self) -> u64 {
        match &self.shard.clock {
            Some(clock) => {
                let floor = self.waits_out().map_or(0, |after| hlc(after) + 1);
                (self.seq + 1).max(hlc(clock())).max(floor)
            }
            None => self.seq + 1,
        }
    }

    /// While a reopened shard waits out its offset bound: the clock reading
    /// (ms) it waits to pass.
    pub(crate) fn waits_out(&self) -> Option<u64> {
        let clock = self.shard.clock.as_ref()?;
        self.shard.resumes_after.filter(|&after| clock() <= after)
    }

    /// Whether the session commit `by` writing `rows` waits: another part
    /// holds one of them, or the shard is waiting out its offset bound
    /// after a reopen and `by` is neither a part fixed at a position nor a
    /// resend of a commit that landed.
    pub(crate) fn commit_waits(&self, by: CommitRef, rows: &[HeldRow]) -> Result<bool, Error> {
        if self.shard.blocks(by, rows) {
            return Ok(true);
        }
        if self.waits_out().is_none() || self.shard.fixed(by).is_some() {
            return Ok(false);
        }
        Ok(self.applied_fate(by.node, by.tx)?.is_none())
    }

    /// A shard reopens: the holds it kept stand again, and it waits out its
    /// offset bound past its clock before numbering. What it observed
    /// before is not kept, but none of it was further ahead than that, so
    /// nothing it numbers from now lands at or below a position it closed
    /// at. Numbering from its clock afterwards (rather than from the bound
    /// at once) keeps its positions no further ahead of its peers' clocks
    /// than its own clock is. A hold whose part already landed is dropped.
    pub(crate) fn load_shard(&mut self) -> Result<(), Error> {
        let Some(clock) = &self.shard.clock else {
            return Ok(());
        };
        // A reopened shard that observed positions past what it numbered
        // may have served them, up to its clock plus the offset bound,
        // before it stopped; it waits that bound out. (What it numbered or
        // held, it reloads.)
        if self.meta(SHARD_OBSERVED)?.is_some() {
            self.shard.observed = true;
            self.shard.resumes_after = Some(clock().saturating_add(HLC_MAX_OFFSET_MS));
        }
        let kept: bool = self
            .conn
            .query_row(
                "SELECT count(*) > 0 FROM sqlite_master WHERE type = 'table' AND name = '_jazz_shard_holds'",
                [],
                |r| r.get(0),
            )
            .map_err(storage)?;
        if !kept {
            return Ok(());
        }
        let holds: Vec<(u64, CommitRef, Option<u64>)> = {
            let mut statement = self
                .conn
                .prepare("SELECT at, node, tx, fixed FROM _jazz_shard_holds")
                .map_err(storage)?;
            statement
                .query_map([], |r| {
                    let node: Vec<u8> = r.get(1)?;
                    Ok((
                        r.get::<_, i64>(0)? as u64,
                        CommitRef {
                            node: node.try_into().unwrap_or([0; 16]),
                            tx: r.get::<_, i64>(2)? as u64,
                        },
                        r.get::<_, Option<i64>>(3)?.map(|at| at as u64),
                    ))
                })
                .map_err(storage)?
                .collect::<Result<_, _>>()
                .map_err(storage)?
        };
        let rows: Vec<(HeldRow, CommitRef)> = {
            let mut statement = self
                .conn
                .prepare("SELECT node, tx, tbl, id FROM _jazz_shard_held_rows")
                .map_err(storage)?;
            statement
                .query_map([], |r| {
                    let node: Vec<u8> = r.get(0)?;
                    let id: Vec<u8> = r.get(3)?;
                    Ok((
                        HeldRow {
                            table: r.get(2)?,
                            row: id.try_into().unwrap_or([0; 16]),
                        },
                        CommitRef {
                            node: node.try_into().unwrap_or([0; 16]),
                            tx: r.get::<_, i64>(1)? as u64,
                        },
                    ))
                })
                .map_err(storage)?
                .collect::<Result<_, _>>()
                .map_err(storage)?
        };
        for (at, commit, fixed) in holds {
            if self.landed_commit(commit)? {
                self.forget_hold(commit)?;
                continue;
            }
            self.shard.holds.insert(at, commit);
            if let Some(fixed) = fixed {
                self.shard.fixed.insert(commit, fixed);
                self.seq = self.seq.max(fixed);
            }
            self.seq = self.seq.max(at);
        }
        for (row, commit) in rows {
            if self.shard.held(commit).is_some() {
                self.shard.rows.insert(row, commit);
            }
        }
        Ok(())
    }

    /// Whether this store already applied the part `commit` (its fate is
    /// recorded).
    fn landed_commit(&self, commit: CommitRef) -> Result<bool, Error> {
        let landed: Option<i64> = self
            .conn
            .query_row(
                "SELECT 1 FROM _jazz_transactions AS t JOIN _jazz_nodes AS n ON n.n = t.n \
                 WHERE n.node = ?1 AND t.tx = ?2 AND t.seq IS NOT NULL",
                rusqlite::params![commit.node.to_vec(), commit.tx as i64],
                |r| r.get(0),
            )
            .optional()
            .map_err(storage)?;
        Ok(landed.is_some())
    }

    /// Drop the durable record of `commit`'s hold. Run inside the savepoint
    /// that lands its part, so the two go together.
    pub(crate) fn forget_hold(&self, commit: CommitRef) -> Result<(), Error> {
        if self.shard.clock.is_none() {
            return Ok(());
        }
        let kept: bool = self
            .conn
            .query_row(
                "SELECT count(*) > 0 FROM sqlite_master WHERE type = 'table' AND name = '_jazz_shard_holds'",
                [],
                |r| r.get(0),
            )
            .map_err(storage)?;
        if !kept {
            return Ok(());
        }
        let params = rusqlite::params![commit.node.to_vec(), commit.tx as i64];
        self.exec(
            "DELETE FROM _jazz_shard_holds WHERE node = ?1 AND tx = ?2",
            params,
        )
        .map_err(storage)?;
        self.exec(
            "DELETE FROM _jazz_shard_held_rows WHERE node = ?1 AND tx = ?2",
            params,
        )
        .map_err(storage)?;
        Ok(())
    }
}

use rusqlite::OptionalExtension;

fn unsharded() -> Error {
    Error::new(
        ErrorCode::Protocol,
        "not supported: this store takes no shard role (open it with DbConfig::with_clock)",
    )
}

/// The shard role of a store (see the module docs), from [`Db::shard`].
pub struct ShardRole<'a> {
    db: &'a Db,
}

impl Db {
    /// This store's shard role: the calls a sharded gateway makes of the
    /// store serving one shard.
    pub fn shard(&self) -> ShardRole<'_> {
        ShardRole { db: self }
    }
}

impl ShardRole<'_> {
    /// Whether the store draws positions from a hybrid clock, and so takes
    /// the shard role.
    pub fn is_shard(&self) -> bool {
        self.db.inner.borrow().shard.clock.is_some()
    }

    /// The store must be a shard: clocked, and the last word on what it
    /// commits (an authority, not a replica of an upstream).
    fn require(&self, inner: &Inner) -> Result<(), Error> {
        if inner.shard.clock.is_none() {
            return Err(unsharded());
        }
        if inner.client.is_some() {
            return Err(Error::new(
                ErrorCode::Protocol,
                "not supported: a replica of an upstream takes no shard role",
            ));
        }
        Ok(())
    }

    /// Close the store at `position` (a hybrid-clock receive): nothing it
    /// commits later lands at or below it, so once
    /// [`Self::served_through`] reaches it the store has settled all it
    /// will ever number up to it. A `position` further ahead of the
    /// store's clock than [`HLC_MAX_OFFSET_MS`] is observed only as far as
    /// that: returns the position observed, which the caller observes again
    /// once the clock has caught up.
    pub fn observe(&self, position: u64) -> Result<u64, Error> {
        let mut inner = self.db.inner.borrow_mut();
        self.require(&inner)?;
        let observed = match &inner.shard.clock {
            Some(clock) => position.min(hlc(clock().saturating_add(HLC_MAX_OFFSET_MS))),
            None => position,
        };
        observe(&mut inner, observed)?;
        Ok(observed)
    }

    /// The position the store serves its sessions as settled: its newest,
    /// or just below its lowest hold while one stands.
    pub fn served_through(&self) -> u64 {
        self.db.inner.borrow().served_through()
    }

    /// While a reopened shard waits out its offset bound, the clock reading
    /// (ms) it waits to pass: until then its sessions' commits wait (but a
    /// fixed part's), and a caller holds nothing yet.
    pub fn waits_out(&self) -> Option<u64> {
        self.db.inner.borrow().waits_out()
    }

    /// The lowest position held for a part still to land, if any.
    pub fn lowest_hold(&self) -> Option<u64> {
        self.db.inner.borrow().shard.lowest_hold()
    }

    /// Hold a position for the cross-shard part `commit`, which writes
    /// `rows` and lands later at a position at or above the hold
    /// ([`Self::fix`]). Until it lands (or is [`Self::release`]d), what the
    /// store serves as settled stays below the hold, and another commit
    /// writing one of `rows` waits. Holding a part again returns its
    /// standing hold. A row another part holds refuses the hold: two
    /// cross-shard commits holding each other's rows on two shards would
    /// otherwise wait on each other.
    pub fn hold(&self, commit: CommitRef, rows: &[HeldRow]) -> Result<u64, Error> {
        let mut inner = self.db.inner.borrow_mut();
        self.require(&inner)?;
        if let Some(at) = inner.shard.held(commit) {
            return Ok(at);
        }
        if inner.shard.blocks(commit, rows) {
            return Err(Error::new(
                ErrorCode::Protocol,
                "not supported yet: a row of this cross-shard part is held by another \
                 cross-shard commit (retry once it lands)",
            ));
        }
        let at = inner.next_seq();
        inner.conn.execute_batch(HOLDS_DDL).map_err(storage)?;
        let node = commit.node.to_vec();
        inner
            .exec(
                "INSERT INTO _jazz_shard_holds (at, node, tx) VALUES (?1, ?2, ?3)",
                rusqlite::params![at as i64, node, commit.tx as i64],
            )
            .map_err(storage)?;
        for row in rows {
            inner
                .exec(
                    "INSERT INTO _jazz_shard_held_rows (node, tx, tbl, id) VALUES (?1, ?2, ?3, ?4)",
                    rusqlite::params![node, commit.tx as i64, row.table, row.row.to_vec()],
                )
                .map_err(storage)?;
        }
        inner.seq = at;
        inner.shard.holds.insert(at, commit);
        for row in rows {
            inner.shard.rows.insert(row.clone(), commit);
        }
        Ok(at)
    }

    /// Land the part `commit` at `position` when it arrives: the position
    /// every part of its cross-shard commit shares, at or above the part's
    /// hold here. Commits the store accepted since the hold may sit above
    /// it; none sits in what it served as settled. The part must be held.
    pub fn fix(&self, commit: CommitRef, position: u64) -> Result<(), Error> {
        let mut inner = self.db.inner.borrow_mut();
        self.require(&inner)?;
        let Some(held) = inner.shard.held(commit) else {
            return Err(Error::new(
                ErrorCode::Protocol,
                "a cross-shard part is fixed only once it is held",
            ));
        };
        if position < held {
            return Err(Error::new(
                ErrorCode::Protocol,
                format!("position {position} is below the part's hold at {held}"),
            ));
        }
        observe(&mut inner, position)?;
        inner
            .exec(
                "UPDATE _jazz_shard_holds SET fixed = ?3 WHERE node = ?1 AND tx = ?2",
                rusqlite::params![commit.node.to_vec(), commit.tx as i64, position as i64],
            )
            .map_err(storage)?;
        inner.shard.fixed.insert(commit, position);
        Ok(())
    }

    /// Drop the part `commit`'s hold without it landing: its cross-shard
    /// commit was refused. Releasing a part not held does nothing.
    pub fn release(&self, commit: CommitRef) -> Result<(), Error> {
        let mut inner = self.db.inner.borrow_mut();
        if inner.shard.held(commit).is_none() {
            return Ok(());
        }
        inner.forget_hold(commit)?;
        inner.shard.landed(commit);
        Ok(())
    }
}

/// Note a position a peer named: nothing the store commits later lands at
/// or below it. One further ahead of the clock than [`HLC_MAX_OFFSET_MS`]
/// is refused, unless the store already numbered or held past it.
fn observe(inner: &mut Inner, position: u64) -> Result<(), Error> {
    if let Some(clock) = &inner.shard.clock
        && position > inner.seq
        && position > hlc(clock().saturating_add(HLC_MAX_OFFSET_MS))
    {
        return Err(Error::new(
            ErrorCode::Protocol,
            format!(
                "position {position} is more than {HLC_MAX_OFFSET_MS} ms ahead of this store's clock"
            ),
        ));
    }
    if position > inner.seq && !inner.shard.observed && inner.shard.clock.is_some() {
        inner.persist_meta(SHARD_OBSERVED, 1)?;
        inner.shard.observed = true;
    }
    inner.seq = inner.seq.max(position);
    Ok(())
}
