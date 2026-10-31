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
//! A store opened without [`crate::options::DbConfig::with_clock`] takes
//! none of this: unsharded, it counts positions up by one and holds
//! nothing, so every call below except `served_through` is refused.

use std::collections::{BTreeMap, HashMap};

use super::{Db, Inner};
use crate::options::{Error, ErrorCode};

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
}

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
    }

    fn held(&self, commit: CommitRef) -> Option<u64> {
        self.holds
            .iter()
            .find_map(|(&at, held)| (*held == commit).then_some(at))
    }
}

impl Inner {
    /// The position the next commit lands at: above every position handed
    /// out or observed, and on a shard no earlier than its clock.
    pub(crate) fn next_seq(&self) -> u64 {
        match &self.shard.clock {
            Some(clock) => (self.seq + 1).max(hlc(clock())),
            None => self.seq + 1,
        }
    }
}

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

    /// The lowest position held for a part still to land, if any.
    pub fn lowest_hold(&self) -> Option<u64> {
        self.db.inner.borrow().shard.lowest_hold()
    }

    /// Hold a position for the cross-shard part `commit`, which lands later
    /// at a position at or above it ([`Self::fix`]). Until it lands (or is
    /// [`Self::release`]d), what the store serves as settled stays below
    /// the hold. Holding a part again returns its standing hold.
    pub fn hold(&self, commit: CommitRef) -> Result<u64, Error> {
        let mut inner = self.db.inner.borrow_mut();
        self.require(&inner)?;
        if let Some(at) = inner.shard.held(commit) {
            return Ok(at);
        }
        let at = inner.next_seq();
        inner.seq = at;
        inner.shard.holds.insert(at, commit);
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
        inner.shard.fixed.insert(commit, position);
        Ok(())
    }

    /// Drop the part `commit`'s hold without it landing: its cross-shard
    /// commit was refused. Releasing a part not held does nothing.
    pub fn release(&self, commit: CommitRef) {
        self.db.inner.borrow_mut().shard.landed(commit);
    }
}

/// Note a position a peer named: nothing the store commits later lands at
/// or below it. One further ahead of the clock than [`HLC_MAX_OFFSET_MS`]
/// is refused.
fn observe(inner: &mut Inner, position: u64) -> Result<(), Error> {
    if let Some(clock) = &inner.shard.clock
        && position > hlc(clock().saturating_add(HLC_MAX_OFFSET_MS))
    {
        return Err(Error::new(
            ErrorCode::Protocol,
            format!(
                "position {position} is more than {HLC_MAX_OFFSET_MS} ms ahead of this store's clock"
            ),
        ));
    }
    inner.seq = inner.seq.max(position);
    Ok(())
}
