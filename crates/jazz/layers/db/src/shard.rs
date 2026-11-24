//! The shard role: what a sharded deployment's gateway needs of a jazz
//! store serving one shard (SPEC 15 §15.3). It is an engine API, with no
//! wire of its own: the gateway↔shard protocol that drives it lives with
//! the gateway.
//!
//! Every authority is a shard: an unsharded server is the one shard of its
//! deployment, running the same code. A shard draws commit positions from
//! a hybrid clock ([`hlc`]), so the positions of every shard compare by
//! time. The gateway closes a quiet
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
//! have observed before. While a hold stands, every other writer of the rows
//! its part writes waits: a session's commit, the store's own local write,
//! and another cross-shard part's hold (queued, and granted once the rows
//! are free: [`ShardRole::take_granted`]). So a fixed part always lands.
//!
//! A hold keeps its part's data ([`ShardRole::part`]), so a shard can land
//! its part without its gateway. A cross-shard commit is decided once, by
//! one participant (its decider): committed when the decider's own part
//! lands, aborted when [`ShardRole::abort`] records it refused first. Either
//! is the commit's recorded fate there, kept like every fate, and
//! [`ShardRole::resolution`] reads it back.
//!
//! Only a replica of an upstream takes no shard role: its upstream numbers
//! what it commits, so every call below except `served_through` is
//! refused there.

use std::collections::{BTreeMap, HashMap};
use std::task::{Poll, Waker};

use super::{Db, Store};
use crate::options::{Error, ErrorCode};
use crate::values::storage;

/// Commit positions are hybrid-clock timestamps: the wall clock in ms since
/// [`HLC_EPOCH_MS`] above [`HLC_COUNTER_BITS`] bits of counter, so every
/// position fits a JS number (Durable Object SQLite returns integers as
/// one, and a pending `_clock` holds one below its pending bit): 42 bits of
/// ms last 139 years, and a node handing out more than 1,024 in a
/// millisecond runs ahead of its clock until the clock catches up.
/// Positions of different nodes then compare by time, and every part of a
/// cross-shard commit can land at one.
pub const HLC_EPOCH_MS: u64 = 1_735_689_600_000;
/// The counter bits below the ms of an [`hlc`] position.
pub const HLC_COUNTER_BITS: u32 = 10;
/// How far ahead of this node's clock an observed position may be (ms):
/// further is a peer whose clock (or claim) cannot be trusted.
pub const HLC_MAX_OFFSET_MS: u64 = 500;

/// The latest ms an [`hlc`] position can name: positions stay under 2^52,
/// the accepted kind of `_clock`.
const HLC_MAX_MS: u64 = (1 << (52 - HLC_COUNTER_BITS)) - 1;

/// The first position at wall-clock time `ms` (Unix ms). A reading past
/// what positions can name is taken as the last ms they can.
pub fn hlc(ms: u64) -> u64 {
    ms.saturating_sub(HLC_EPOCH_MS).min(HLC_MAX_MS) << HLC_COUNTER_BITS
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

/// What [`ShardRole::hold`] answers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Held {
    /// Held at this position.
    At(u64),
    /// Queued behind another part holding one of its rows.
    Queued,
    /// The commit is already decided here.
    Decided(Resolution),
}

/// How a cross-shard commit was decided.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Resolution {
    /// Committed: every part lands at this position.
    Committed(u64),
    /// Aborted: no part lands.
    Aborted,
}

/// A store's shard state: its clock, and the positions it holds for
/// cross-shard parts still to land.
#[derive(Default)]
pub(crate) struct ShardState {
    /// Held position → the part it is held for.
    holds: BTreeMap<u64, CommitRef>,
    /// Parts to land at a fixed position when they arrive.
    fixed: HashMap<CommitRef, u64>,
    /// The rows each held part writes, and who holds each row.
    rows: HashMap<HeldRow, CommitRef>,
    /// Holds asked for rows another part holds, in the order asked: each is
    /// granted once its rows are free ([`ShardRole::take_granted`]).
    queued: Vec<(CommitRef, Vec<HeldRow>, Vec<u8>)>,
    /// Local writes waiting for a held row to be free.
    waiters: Vec<Waker>,
    /// Whether [`SHARD_OBSERVED`] is set.
    observed: bool,
    /// On a replica applying its leader's log, the position the next thing
    /// it numbers takes ([`ShardRole::number_at`]), while above `seq`.
    pinned: Option<u64>,
}

/// The `_jazz_meta` key a clocked store sets the first time it observes a
/// position past what it numbered: from then on, it may have served a
/// position it keeps no record of.
const SHARD_OBSERVED: &str = "shard_observed";

/// The durable record of holds: one row per held part, with its data and
/// its fixed position once fixed, and one per row it writes; and of queued
/// holds, in the order asked, with their data and rows. Created by the
/// first hold, so the file of a store that never held a part has none.
const HOLDS_DDL: &str = "\
    CREATE TABLE IF NOT EXISTS _jazz_shard_holds (at INTEGER PRIMARY KEY, node BLOB NOT NULL, tx INTEGER NOT NULL, fixed INTEGER, part BLOB NOT NULL, UNIQUE (node, tx));\
    CREATE TABLE IF NOT EXISTS _jazz_shard_held_rows (node BLOB NOT NULL, tx INTEGER NOT NULL, tbl TEXT NOT NULL, id BLOB NOT NULL, PRIMARY KEY (tbl, id));\
    CREATE TABLE IF NOT EXISTS _jazz_shard_queued (asked INTEGER PRIMARY KEY, node BLOB NOT NULL, tx INTEGER NOT NULL, part BLOB NOT NULL, UNIQUE (node, tx));\
    CREATE TABLE IF NOT EXISTS _jazz_shard_queued_rows (node BLOB NOT NULL, tx INTEGER NOT NULL, tbl TEXT NOT NULL, id BLOB NOT NULL);";

/// Create the durable record of holds (see [`HOLDS_DDL`]). A store that
/// held a part before holds kept their part's data (jazz#4546) has
/// `_jazz_shard_holds` without its `part` column: it gains it, empty for
/// the holds it had. Only a store driven as a shard directly (a sharding
/// simulation) could have one: the gateway resends such a part.
async fn create_holds(conn: &crate::storage::Storage) -> Result<(), Error> {
    conn.execute_batch(HOLDS_DDL).await.map_err(storage)?;
    let has_part: bool = conn
        .query_row(
            "SELECT count(*) > 0 FROM pragma_table_info('_jazz_shard_holds') WHERE name = 'part'",
            [],
            |r| r.get(0),
        )
        .await
        .map_err(storage)?;
    if !has_part {
        conn.execute_batch(
            "ALTER TABLE _jazz_shard_holds ADD COLUMN part BLOB NOT NULL DEFAULT x''",
        )
        .await
        .map_err(storage)?;
    }
    Ok(())
}

impl ShardState {
    /// The lowest position held for a part still to land.
    pub(crate) fn lowest_hold(&self) -> Option<u64> {
        self.holds.keys().next().copied()
    }

    /// The position the part `commit` lands at, when one was fixed.
    pub(crate) fn fixed(&self, commit: CommitRef) -> Option<u64> {
        self.fixed.get(&commit).copied()
    }

    /// Whether `commit` is a cross-shard part this shard holds or fixed a
    /// position for: it has not landed here yet.
    pub(crate) fn awaits(&self, commit: CommitRef) -> bool {
        self.fixed.contains_key(&commit) || self.held(commit).is_some()
    }

    /// The part `commit` landed (or was refused): it holds nothing more.
    pub(crate) fn landed(&mut self, commit: CommitRef) {
        self.holds.retain(|_, held| *held != commit);
        self.fixed.remove(&commit);
        self.rows.retain(|_, held| *held != commit);
        self.queued.retain(|(queued, ..)| *queued != commit);
        for waiter in self.waiters.drain(..) {
            waiter.wake();
        }
    }

    /// Ready once no part holds one of `rows`; else the waker is woken when
    /// a part lands or is released.
    pub(crate) fn wait_unheld(
        &mut self,
        rows: &[[u8; 16]],
        cx: &mut std::task::Context<'_>,
    ) -> Poll<()> {
        if !self.holds_any(rows) {
            return Poll::Ready(());
        }
        self.waiters.push(cx.waker().clone());
        Poll::Pending
    }

    /// Whether a part holds one of `rows` (by row id, whatever table).
    pub(super) fn holds_any(&self, rows: &[[u8; 16]]) -> bool {
        !self.rows.is_empty() && self.rows.keys().any(|held| rows.contains(&held.row))
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

impl Store {
    /// The position the next commit lands at: above every position handed
    /// out or observed, and on a shard no earlier than its clock (past the
    /// offset bound it waits out after a reopen, for a write that does not
    /// wait).
    /// A replica numbers its own commits locally: their versions sit at
    /// their commit HLCs (see [`Store::stamp_commit`]). Its transaction ids
    /// are `(seq, node)`, and the authority deduplicates uploads by them
    /// (INV-EDGE-16). A replica in a file numbers no lower than its wall
    /// clock's first position ([`hlc`]): the file may reopen without commits
    /// it already uploaded (a lost tail, a restored backup), and its counter
    /// then resumes below them, but its clock has moved past every id it
    /// handed out before (INV-EDGE-28). An in-memory replica never reopens:
    /// a host that gives its node to a fresh one seeds the floor instead
    /// ([`crate::db::Db::seed_seq_floor`]).
    pub(crate) fn next_seq(&self) -> u64 {
        if let Some(pinned) = self.shard.borrow().pinned
            && pinned > self.seq.get()
        {
            return pinned;
        }
        let next = self.seq.get().max(self.seed_floor.get()) + 1;
        if self.me.get() != crate::db::sync::ACCEPTED {
            if !self.reopenable {
                return next;
            }
            return next.max(hlc(self.hlc_clock_ms()));
        }
        let floor = self.waits_out().map_or(0, |after| hlc(after) + 1);
        next.max(hlc((self.clock)())).max(floor)
    }

    /// While a reopened shard waits out its offset bound: the clock reading
    /// (ms) it waits to pass.
    pub(crate) fn waits_out(&self) -> Option<u64> {
        let clock = &self.clock;
        self.resumes_after.get().filter(|&after| clock() <= after)
    }

    /// Whether the session commit `by` writing `ops` waits: another part
    /// holds one of the rows they write, or the shard is waiting out its offset bound
    /// after a reopen and `by` is neither a part fixed at a position nor a
    /// resend of a commit that landed.
    pub(crate) async fn commit_waits(
        &self,
        by: CommitRef,
        ops: &[super::WireOp],
    ) -> Result<bool, Error> {
        // Most stores hold nothing: they skip naming the rows.
        if !self.shard.borrow().rows.is_empty() && self.shard.borrow().blocks(by, &HeldRow::of(ops))
        {
            return Ok(true);
        }
        // A held part lands only at the position fixed for its commit: until
        // it is fixed (or released), it waits (INV-SHARD-27).
        if self.shard.borrow().held(by).is_some() && self.shard.borrow().fixed(by).is_none() {
            return Ok(true);
        }
        if self.waits_out().is_none() || self.shard.borrow().fixed(by).is_some() {
            return Ok(false);
        }
        Ok(self.applied_fate(by.node, by.tx).await?.is_none())
    }

    /// A shard reopens: the holds it kept stand again, and it waits out its
    /// offset bound past its clock before numbering. What it observed
    /// before is not kept, but none of it was further ahead than that, so
    /// nothing it numbers from now lands at or below a position it closed
    /// at. Numbering from its clock afterwards (rather than from the bound
    /// at once) keeps its positions no further ahead of its peers' clocks
    /// than its own clock is. A hold whose part already landed is dropped.
    pub(crate) async fn load_shard(&self) -> Result<(), Error> {
        let clock = &self.clock;
        // A reopened shard that observed positions past what it numbered
        // may have served them, up to its clock plus the offset bound,
        // before it stopped; it waits that bound out. (What it numbered or
        // held, it reloads.)
        if self.meta(SHARD_OBSERVED).await?.is_some() {
            self.shard.borrow_mut().observed = true;
            self.resumes_after
                .set(Some(clock().saturating_add(HLC_MAX_OFFSET_MS)));
        }
        let kept: bool = self
            .conn
            .query_row(
                "SELECT count(*) > 0 FROM sqlite_master WHERE type = 'table' AND name = '_jazz_shard_holds'",
                [],
                |r| r.get(0),
            )
            .await.map_err(storage)?;
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
                .await
                .map_err(storage)?
                .collect::<_>()
                .await
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
                .await
                .map_err(storage)?
                .collect::<_>()
                .await
                .map_err(storage)?
        };
        for (at, commit, fixed) in holds {
            // Decided here (landed, or aborted on its decider): the hold
            // went with the decision, or would have but for a crash.
            if self.landed_commit(commit).await?
                || self.applied_fate(commit.node, commit.tx).await?.is_some()
            {
                self.exec(
                    "DELETE FROM _jazz_shard_holds WHERE node = ?1 AND tx = ?2",
                    crate::storage::params![commit.node.to_vec(), commit.tx as i64],
                )
                .await
                .map_err(storage)?;
                self.exec(
                    "DELETE FROM _jazz_shard_held_rows WHERE node = ?1 AND tx = ?2",
                    crate::storage::params![commit.node.to_vec(), commit.tx as i64],
                )
                .await
                .map_err(storage)?;
                continue;
            }
            self.shard.borrow_mut().holds.insert(at, commit);
            if let Some(fixed) = fixed {
                self.shard.borrow_mut().fixed.insert(commit, fixed);
                self.seq.set(self.seq.get().max(fixed));
            }
            self.seq.set(self.seq.get().max(at));
        }
        for (row, commit) in rows {
            if self.shard.borrow().held(commit).is_some() {
                self.shard.borrow_mut().rows.insert(row, commit);
            }
        }
        // Queued holds wait on, in the order they were asked; one whose
        // commit was decided meanwhile is dropped.
        create_holds(&self.conn).await?;
        let queued: Vec<(CommitRef, Vec<u8>)> = {
            let mut statement = self
                .conn
                .prepare("SELECT node, tx, part FROM _jazz_shard_queued ORDER BY asked")
                .map_err(storage)?;
            statement
                .query_map([], |r| {
                    let node: Vec<u8> = r.get(0)?;
                    Ok((
                        CommitRef {
                            node: node.try_into().unwrap_or([0; 16]),
                            tx: r.get::<_, i64>(1)? as u64,
                        },
                        r.get(2)?,
                    ))
                })
                .await
                .map_err(storage)?
                .collect::<_>()
                .await
                .map_err(storage)?
        };
        for (commit, part) in queued {
            if self.shard.borrow().held(commit).is_some()
                || self.resolution(commit).await?.is_some()
            {
                self.forget_queued(commit).await?;
                continue;
            }
            let rows: Vec<HeldRow> = {
                let mut statement = self
                    .conn
                    .prepare(
                        "SELECT tbl, id FROM _jazz_shard_queued_rows WHERE node = ?1 AND tx = ?2",
                    )
                    .map_err(storage)?;
                statement
                    .query_map(
                        crate::storage::params![commit.node.to_vec(), commit.tx as i64],
                        |r| {
                            let id: Vec<u8> = r.get(1)?;
                            Ok(HeldRow {
                                table: r.get(0)?,
                                row: id.try_into().unwrap_or([0; 16]),
                            })
                        },
                    )
                    .await
                    .map_err(storage)?
                    .collect::<_>()
                    .await
                    .map_err(storage)?
            };
            self.shard.borrow_mut().queued.push((commit, rows, part));
        }
        Ok(())
    }

    /// Queue a hold for `commit`, durably: it is granted in the order
    /// asked, after a restart too.
    async fn queue(&self, commit: CommitRef, rows: &[HeldRow], part: &[u8]) -> Result<(), Error> {
        create_holds(&self.conn).await?;
        let node = commit.node.to_vec();
        self.exec(
            "INSERT INTO _jazz_shard_queued (node, tx, part) VALUES (?1, ?2, ?3)",
            crate::storage::params![node, commit.tx as i64, part],
        )
        .await
        .map_err(storage)?;
        for row in rows {
            self.exec(
                "INSERT INTO _jazz_shard_queued_rows (node, tx, tbl, id) VALUES (?1, ?2, ?3, ?4)",
                crate::storage::params![node, commit.tx as i64, row.table, row.row.to_vec()],
            )
            .await
            .map_err(storage)?;
        }
        self.shard
            .borrow_mut()
            .queued
            .push((commit, rows.to_vec(), part.to_vec()));
        Ok(())
    }

    /// Drop the durable record of `commit`'s queued hold, if any.
    async fn forget_queued(&self, commit: CommitRef) -> Result<(), Error> {
        let params = crate::storage::params![commit.node.to_vec(), commit.tx as i64];
        self.exec(
            "DELETE FROM _jazz_shard_queued WHERE node = ?1 AND tx = ?2",
            params,
        )
        .await
        .map_err(storage)?;
        self.exec(
            "DELETE FROM _jazz_shard_queued_rows WHERE node = ?1 AND tx = ?2",
            params,
        )
        .await
        .map_err(storage)?;
        Ok(())
    }

    /// Hold a position for `commit` now (its rows are free).
    async fn hold_now(
        &self,
        commit: CommitRef,
        rows: &[HeldRow],
        part: &[u8],
    ) -> Result<u64, Error> {
        let at = self.next_seq();
        create_holds(&self.conn).await?;
        let node = commit.node.to_vec();
        self.exec(
            "INSERT INTO _jazz_shard_holds (at, node, tx, part) VALUES (?1, ?2, ?3, ?4)",
            crate::storage::params![at as i64, node, commit.tx as i64, part],
        )
        .await
        .map_err(storage)?;
        for row in rows {
            self.exec(
                "INSERT INTO _jazz_shard_held_rows (node, tx, tbl, id) VALUES (?1, ?2, ?3, ?4)",
                crate::storage::params![node, commit.tx as i64, row.table, row.row.to_vec()],
            )
            .await
            .map_err(storage)?;
        }
        self.seq.set(at);
        self.shard.borrow_mut().holds.insert(at, commit);
        for row in rows {
            self.shard.borrow_mut().rows.insert(row.clone(), commit);
        }
        Ok(at)
    }

    /// `commit`'s recorded fate here, as a decision.
    pub(crate) async fn resolution(&self, commit: CommitRef) -> Result<Option<Resolution>, Error> {
        let Some(fate) = self.applied_fate(commit.node, commit.tx).await? else {
            return Ok(None);
        };
        if fate.is_err() {
            return Ok(Some(Resolution::Aborted));
        }
        let at = self.settled(commit.node, commit.tx).await?.flatten();
        Ok(at.map(|at| Resolution::Committed(at.seq)))
    }

    /// Whether a part holds one of `rows`: a local write of them waits (or,
    /// where it cannot, is refused), so a fixed part never lands under it.
    pub(crate) fn refuse_held(&self, rows: &[[u8; 16]]) -> Result<(), Error> {
        if self.shard.borrow().holds_any(rows) {
            return Err(Error::new(
                ErrorCode::Backpressure,
                "a cross-shard commit holds a row this write writes: retry once it lands",
            ));
        }
        Ok(())
    }

    /// Whether this store already applied the part `commit` (its fate is
    /// recorded).
    async fn landed_commit(&self, commit: CommitRef) -> Result<bool, Error> {
        let landed: Option<i64> = self
            .conn
            .query_row(
                "SELECT 1 FROM _jazz_transactions AS t JOIN _jazz_nodes AS n ON n.n = t.n \
                 WHERE n.node = ?1 AND t.tx = ?2 AND t.t IS NOT NULL",
                crate::storage::params![commit.node.to_vec(), commit.tx as i64],
                |r| r.get(0),
            )
            .await
            .optional()
            .map_err(storage)?;
        Ok(landed.is_some())
    }

    /// Drop the durable record of `commit`'s hold. Run inside the savepoint
    /// that lands its part, so the two go together.
    pub(crate) async fn forget_hold(&self, commit: CommitRef) -> Result<(), Error> {
        // Every durable hold, standing or queued, is loaded on reopen, so a
        // store holding nothing in memory has none on disk either.
        if self.shard.borrow().holds.is_empty()
            && self.shard.borrow().fixed.is_empty()
            && self.shard.borrow().queued.is_empty()
        {
            return Ok(());
        }
        let kept: bool = self
            .conn
            .query_row(
                "SELECT count(*) > 0 FROM sqlite_master WHERE type = 'table' AND name = '_jazz_shard_holds'",
                [],
                |r| r.get(0),
            )
            .await.map_err(storage)?;
        if !kept {
            return Ok(());
        }
        let params = crate::storage::params![commit.node.to_vec(), commit.tx as i64];
        self.exec(
            "DELETE FROM _jazz_shard_holds WHERE node = ?1 AND tx = ?2",
            params,
        )
        .await
        .map_err(storage)?;
        self.exec(
            "DELETE FROM _jazz_shard_held_rows WHERE node = ?1 AND tx = ?2",
            params,
        )
        .await
        .map_err(storage)?;
        self.forget_queued(commit).await
    }
}

use crate::storage::OptionalExtension;

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
    /// Whether the store takes the shard role: it is the last word on what
    /// it commits (an authority, not a replica of an upstream).
    pub fn is_shard(&self) -> bool {
        !self.db.is_client()
    }

    /// The store must be a shard (see [`Self::is_shard`]).
    fn require(&self, inner: &Store) -> Result<(), Error> {
        if inner.client().is_some() {
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
    pub async fn observe(&self, position: u64) -> Result<u64, Error> {
        let _operation = self.db.write_operation().await;
        let inner = self.db.inner.without_overlay();
        self.require(inner)?;
        let observed = position.min(hlc((inner.clock)().saturating_add(HLC_MAX_OFFSET_MS)));
        observe(inner, observed).await?;
        Ok(observed)
    }

    /// The position the store serves its sessions as settled: its newest,
    /// or just below its lowest hold while one stands.
    pub fn served_through(&self) -> u64 {
        let store = &self.db.inner;
        if let Some(_lease) = store.operation.try_lock() {
            store.served_through()
        } else {
            store.prior_served.get()
        }
    }

    /// While a reopened shard waits out its offset bound, the clock reading
    /// (ms) it waits to pass: until then its sessions' commits wait (but a
    /// fixed part's), and a caller holds nothing yet.
    pub fn waits_out(&self) -> Option<u64> {
        self.db
            .inner
            .resumes_after
            .get()
            .filter(|after| (self.db.inner.clock)() <= *after)
    }

    /// The lowest position held for a part still to land, if any.
    pub fn lowest_hold(&self) -> Option<u64> {
        let store = &self.db.inner;
        if let Some(_lease) = store.operation.try_lock() {
            store.shard.borrow().lowest_hold()
        } else {
            store.prior_hold.get()
        }
    }

    /// Hold a position for the cross-shard part `commit`, which writes
    /// `rows` and lands later at a position at or above the hold
    /// ([`Self::fix`]); `part` is the part's data, kept with the hold (see
    /// [`Self::part`]). Until it lands (or is [`Self::release`]d), what the
    /// store serves as settled stays below the hold, and every other writer
    /// of one of `rows` waits. Holding a part again returns its standing
    /// hold, and a part whose commit is already decided here answers its
    /// [`Resolution`]. A row another part holds queues the hold: it is
    /// granted, in the order asked, once its rows are free
    /// ([`Self::take_granted`]).
    pub async fn hold(
        &self,
        commit: CommitRef,
        rows: &[HeldRow],
        part: &[u8],
    ) -> Result<Held, Error> {
        let _operation = self.db.write_operation().await;
        let inner = self.db.inner.without_overlay();
        self.require(inner)?;
        if let Some(at) = inner.shard.borrow().held(commit) {
            return Ok(Held::At(at));
        }
        if let Some(resolution) = inner.resolution(commit).await? {
            return Ok(Held::Decided(resolution));
        }
        // A row another part holds, or one a queued hold waits for, queues
        // this hold behind them: holds are granted in the order asked.
        let behind_queued = inner
            .shard
            .borrow()
            .queued
            .iter()
            .any(|(queued, ahead, _)| {
                *queued != commit && ahead.iter().any(|row| rows.contains(row))
            });
        if inner.shard.borrow().blocks(commit, rows) || behind_queued {
            if !inner
                .shard
                .borrow()
                .queued
                .iter()
                .any(|(queued, ..)| *queued == commit)
            {
                inner.queue(commit, rows, part).await?;
            }
            return Ok(Held::Queued);
        }
        inner.hold_now(commit, rows, part).await.map(Held::At)
    }

    /// Holds queued behind another part's rows that are granted now, in the
    /// order they were asked: each with its position.
    pub async fn take_granted(&self) -> Result<Vec<(CommitRef, u64)>, Error> {
        let _operation = self.db.write_operation().await;
        let inner = self.db.inner.without_overlay();
        if inner.shard.borrow().queued.is_empty() || inner.waits_out().is_some() {
            return Ok(Vec::new());
        }
        let mut granted = Vec::new();
        let queued = std::mem::take(&mut inner.shard.borrow_mut().queued);
        for (commit, rows, part) in queued {
            // Behind a queued hold of the same rows, a later one waits on.
            if inner.shard.borrow().blocks(commit, &rows)
                || inner
                    .shard
                    .borrow()
                    .queued
                    .iter()
                    .any(|(_, ahead, _)| ahead.iter().any(|row| rows.contains(row)))
            {
                inner.shard.borrow_mut().queued.push((commit, rows, part));
                continue;
            }
            inner.forget_queued(commit).await?;
            granted.push((commit, inner.hold_now(commit, &rows, &part).await?));
        }
        Ok(granted)
    }

    /// The data kept with the part `commit`'s hold, while it stands.
    pub async fn part(&self, commit: CommitRef) -> Result<Option<Vec<u8>>, Error> {
        let _operation = self.db.write_operation().await;
        let inner = self.db.inner.without_overlay();
        if inner.shard.borrow().held(commit).is_none() {
            return Ok(None);
        }
        inner
            .conn
            .query_row(
                "SELECT part FROM _jazz_shard_holds WHERE node = ?1 AND tx = ?2",
                crate::storage::params![commit.node.to_vec(), commit.tx as i64],
                |r| r.get(0),
            )
            .await
            .optional()
            .map_err(storage)
    }

    /// Every standing hold: its part, its position, and its fixed position
    /// once fixed.
    pub fn holds(&self) -> Vec<(CommitRef, u64, Option<u64>)> {
        let inner = &self.db.inner;
        inner
            .shard
            .borrow()
            .holds
            .iter()
            .map(|(&at, &commit)| (commit, at, inner.shard.borrow().fixed(commit)))
            .collect()
    }

    /// How the cross-shard commit `commit` was decided here, if it was: its
    /// part's recorded fate. On the commit's decider, that is the commit's
    /// one decision. A fate is never forgotten, so the answer holds however
    /// late it is asked.
    pub async fn resolution(&self, commit: CommitRef) -> Result<Option<Resolution>, Error> {
        let _operation = self.db.write_operation().await;
        self.db.inner.resolution(commit).await
    }

    /// Decide the cross-shard commit `commit` aborted unless it is decided
    /// already: its part here is recorded refused (a resend of it gets that
    /// fate), its hold dropped. Returns the decision that stands, which is
    /// `Committed` when the part landed first. Called on the commit's
    /// decider only.
    pub async fn abort(&self, commit: CommitRef) -> Result<Resolution, Error> {
        let _operation = self.db.write_operation().await;
        let inner = self.db.inner.without_overlay();
        self.require(inner)?;
        if let Some(resolution) = inner.resolution(commit).await? {
            return Ok(resolution);
        }
        // The fate and the hold's release are one write: a crash between
        // them must not leave the hold standing (INV-SHARD-20, INV-SHARD-23).
        let conn = inner.conn.clone();
        conn.db_scope("shard_abort", async || {
            inner.record_aborted(commit).await?;
            inner.forget_hold(commit).await
        })
        .await?;
        inner.shard.borrow_mut().landed(commit);
        Ok(Resolution::Aborted)
    }

    /// Land the part `commit` at `position` when it arrives: the position
    /// every part of its cross-shard commit shares, at or above the part's
    /// hold here. Commits the store accepted since the hold may sit above
    /// it; none sits in what it served as settled. The part must be held.
    pub async fn fix(&self, commit: CommitRef, position: u64) -> Result<(), Error> {
        let _operation = self.db.write_operation().await;
        let inner = self.db.inner.without_overlay();
        self.require(inner)?;
        let Some(held) = inner.shard.borrow().held(commit) else {
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
        // A fixed position is the commit's decision, checked against the
        // decider's clock, which is within the offset bound of this one: it
        // may be up to twice the bound ahead here, and is taken whole
        // (INV-SHARD-26). Further ahead, a peer broke the bound.
        if position > inner.seq.get()
            && position > hlc((inner.clock)().saturating_add(2 * HLC_MAX_OFFSET_MS))
        {
            return Err(Error::new(
                ErrorCode::Protocol,
                format!(
                    "fixed position {position} is more than twice {HLC_MAX_OFFSET_MS} ms ahead \
                     of this store's clock"
                ),
            ));
        }
        note_observed(inner, position).await?;
        inner
            .exec(
                "UPDATE _jazz_shard_holds SET fixed = ?3 WHERE node = ?1 AND tx = ?2",
                crate::storage::params![commit.node.to_vec(), commit.tx as i64, position as i64],
            )
            .await
            .map_err(storage)?;
        inner.shard.borrow_mut().fixed.insert(commit, position);
        Ok(())
    }

    /// The highest position the store numbered, held, fixed or observed:
    /// nothing it numbers later lands at or below it.
    pub fn numbered(&self) -> u64 {
        self.db.seq_high_water()
    }

    /// A replica applies its leader's log (SPEC 15 §15.3, replicas): the
    /// next position it numbers (a commit, a local write or a hold) is
    /// `position`, where its leader numbered the same entry. `position`
    /// must be above everything the store numbered or observed. A replica
    /// that follows numbers nothing of its own, so it does not wait out an
    /// offset bound after a reopen (see [`Self::lead`]).
    pub fn number_at(&self, position: u64) -> Result<(), Error> {
        let inner = &self.db.inner;
        self.require(inner)?;
        if position <= inner.seq.get() {
            return Err(Error::new(
                ErrorCode::Protocol,
                format!(
                    "position {position} is not above {}, which the store numbered or observed",
                    inner.seq.get()
                ),
            ));
        }
        inner.shard.borrow_mut().pinned = Some(position);
        inner.resumes_after.set(None);
        Ok(())
    }

    /// The time (Unix ms) the store stamped the writes it made at
    /// `position` with (their `$createdAt`/`$updatedAt`), if its last
    /// writes were made there.
    pub fn stamp_of(&self, position: u64) -> Option<i64> {
        let inner = &self.db.inner;
        (inner.last_stamp.get().0 == position).then_some(inner.last_stamp.get().1)
    }

    /// A replica applies its leader's writes at `position`: they are
    /// stamped at `ms`, as the leader stamped them ([`Self::stamp_of`]),
    /// not at the replica's own clock.
    pub fn stamp(&self, position: u64, ms: i64) {
        self.db.inner.last_stamp.set((position, ms));
    }

    /// A replica applies a position its leader fixed or observed: nothing
    /// it numbers later lands at or below `position`. Unlike
    /// [`Self::observe`] it is not capped by the replica's clock: its
    /// leader checked it against its own.
    pub async fn follow(&self, position: u64) -> Result<(), Error> {
        let _operation = self.db.write_operation().await;
        let inner = self.db.inner.without_overlay();
        self.require(inner)?;
        if position > inner.seq.get() && !inner.shard.borrow().observed {
            inner.persist_meta(SHARD_OBSERVED, 1).await?;
            inner.shard.borrow_mut().observed = true;
        }
        inner.seq.set(inner.seq.get().max(position));
        inner.resumes_after.set(None);
        Ok(())
    }

    /// A replica takes over as its shard's leader: before it numbers, it
    /// waits until its clock passes the highest position it applied plus
    /// [`HLC_MAX_OFFSET_MS`] (INV-SHARD-21), as a reopened shard does
    /// ([`Self::waits_out`]), so it numbers above anything its old leader
    /// may have handed out.
    pub async fn lead(&self) -> Result<(), Error> {
        let _operation = self.db.write_operation().await;
        let inner = self.db.inner.without_overlay();
        self.require(inner)?;
        inner.shard.borrow_mut().pinned = None;
        let after = hlc_ms(inner.seq.get()).saturating_add(HLC_MAX_OFFSET_MS);
        inner.resumes_after.set(Some(after));
        // A new leader that restarts mid-wait waits again on reopen, as a
        // shard that observed past what it numbered does (INV-SHARD-21).
        if !inner.shard.borrow().observed {
            inner.persist_meta(SHARD_OBSERVED, 1).await?;
            inner.shard.borrow_mut().observed = true;
        }
        Ok(())
    }

    /// Drop the part `commit`'s hold without it landing: its cross-shard
    /// commit was refused. A queued hold is dropped too. Releasing a part
    /// neither held nor queued does nothing.
    pub async fn release(&self, commit: CommitRef) -> Result<(), Error> {
        let _operation = self.db.write_operation().await;
        let inner = self.db.inner.without_overlay();
        self.require(inner)?;
        let queued = inner
            .shard
            .borrow()
            .queued
            .iter()
            .any(|(queued, ..)| *queued == commit);
        if inner.shard.borrow().held(commit).is_none() && !queued {
            return Ok(());
        }
        let conn = inner.conn.clone();
        conn.db_scope("shard_release", async || inner.forget_hold(commit).await)
            .await?;
        inner.shard.borrow_mut().landed(commit);
        Ok(())
    }
}

/// Note a position a peer named: nothing the store commits later lands at
/// or below it. One further ahead of the clock than [`HLC_MAX_OFFSET_MS`]
/// is refused, unless the store already numbered or held past it.
async fn observe(inner: &Store, position: u64) -> Result<(), Error> {
    if position > inner.seq.get()
        && position > hlc((inner.clock)().saturating_add(HLC_MAX_OFFSET_MS))
    {
        return Err(Error::new(
            ErrorCode::Protocol,
            format!(
                "position {position} is more than {HLC_MAX_OFFSET_MS} ms ahead of this store's clock"
            ),
        ));
    }
    note_observed(inner, position).await
}

/// Raise the store's numbering to `position`, noting (durably, once) that
/// it observed past what it numbered.
async fn note_observed(inner: &Store, position: u64) -> Result<(), Error> {
    if position > inner.seq.get() && !inner.shard.borrow().observed {
        inner.persist_meta(SHARD_OBSERVED, 1).await?;
        inner.shard.borrow_mut().observed = true;
    }
    inner.seq.set(inner.seq.get().max(position));
    Ok(())
}
