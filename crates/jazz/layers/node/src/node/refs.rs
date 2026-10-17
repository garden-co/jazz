//! Refs: where a row image a node holds comes from (SPEC 4 §4.7).
//!
//! Every row a node knows has a coordinate `(table, branch_key, row_uuid)`
//! (application branches, SPEC 11) and a ref. Origin is Core, the only
//! remote that mints seqs; its settled state is stored in full. The local
//! ref is origin plus a sparse delta: the newest pending image of each row
//! this node holds (its own writes and those it relays) that origin has not
//! fated yet. In git terms origin is `origin/main` and the local ref is the
//! local branch with unpushed commits; Core accepting a write is a rebase
//! with per-cell auto-resolve, and "maybe conflicting" (`INV-HIST-21`)
//! names a write that was rebased rather than fast-forwarded.
//!
//! The storage is unchanged by this vocabulary:
//!
//! | ref             | physical class                    |
//! | --------------- | --------------------------------- |
//! | `Remote(Origin)`| `Global` (`..._global_current`)   |
//! | `Local`         | `Ahead` (`..._ahead_current`) over `Global` |
//!
//! Only origin is durable. The local delta is derived: a non-durable Groove
//! table held in memory, folded from history@0 over origin when the node
//! opens and kept equal to that fold by every write, settlement, rejection
//! and rebase (SPEC 4 §4.7, `INV-REF-6`).
//!
//! A read bounded by an index masks the origin layer by the local delta's
//! presence (its row ids, whatever their indexed values), read whole or by
//! origin's candidates, whichever reads fewer rows. It needs no stored copy
//! of the overridden origin images.

use super::*;

/// Where a row image a node holds comes from.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::node) enum Ref {
    /// This node's own working state: settled state plus every pending write
    /// it holds (its own and those it relays), not yet fated by origin.
    Local,
    /// The settled state of a remote, as this node last received it.
    Remote(Remote),
}

/// A remote a node tracks.
///
/// Origin is Core, the only remote that mints seqs. A peer remote would be
/// one more sparse delta read as `[local, peer, origin]`; it is not
/// implemented (SPEC 4 §4.7 lists what it would need).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::node) enum Remote {
    Origin,
}

impl Ref {
    /// The refs a read at `tier` stacks, top first: `Global` reads origin
    /// alone, `Local` and `None` read the local delta over origin.
    pub(in crate::node) const fn stack(tier: DurabilityTier) -> &'static [Ref] {
        match tier {
            DurabilityTier::Global => &[Ref::Remote(Remote::Origin)],
            DurabilityTier::Local | DurabilityTier::None => {
                &[Ref::Local, Ref::Remote(Remote::Origin)]
            }
        }
    }

    /// The physical current class that stores this ref's layer. Origin is
    /// stored in full; the local ref only as its sparse delta over origin.
    pub(in crate::node) const fn physical_class(self) -> PhysicalCurrentClass {
        match self {
            Ref::Local => PhysicalCurrentClass::Ahead,
            Ref::Remote(Remote::Origin) => PhysicalCurrentClass::Global,
        }
    }
}
