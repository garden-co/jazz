//! Exclusive foreground transaction-node leases.
//!
//! Hosts own persistence and random allocation. This small core state machine
//! only makes the safety boundary explicit: an active node cannot be issued
//! twice; only a clean handoff of the runtime's issued sequence makes it
//! reusable; every uncertain lease is retired for this host lifetime. SQLite hosts
//! read `Db::seq_high_water` at clean close and call `Db::seed_seq_floor`
//! before exposing the next in-memory runtime. No storage or wire encoding
//! belongs to this pool; `TxTime` carries SQLite's issued sequence.

use std::collections::{BTreeMap, BTreeSet};

use crate::ids::NodeUuid;
use jazz_types::time::TxTime;

/// A node identity exclusively issued to one live foreground runtime.
///
/// `confirmed_tx_time` is the high-water mark reported by the runtime that
/// minted with this identity. It covers every minted transaction, including a
/// transaction that was rolled back or never submitted upstream.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ForegroundNodeLease {
    /// The wire-stable transaction-node identity.
    pub node: NodeUuid,
    /// The runtime-confirmed issued sequence for `node`.
    pub confirmed_tx_time: TxTime,
}

/// Failed foreground-node lease state transitions.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ForegroundNodeLeaseError {
    /// An allocator attempted to issue an identity that is still live, ready
    /// for reuse, or permanently retired.
    DuplicateNode,
    /// A clean handoff or retirement did not name the currently active lease.
    InactiveLease,
}

impl std::fmt::Display for ForegroundNodeLeaseError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::DuplicateNode => "foreground node identity is already known to this lease pool",
            Self::InactiveLease => "foreground node lease is not active",
        })
    }
}

impl std::error::Error for ForegroundNodeLeaseError {}

/// Host-owned lifecycle state for foreground transaction-node identities.
///
/// This type deliberately has no clock, storage, process, or random source.
/// An adapter persists its state atomically around [`Self::clean_handoff`] and
/// supplies fresh CSPRNG node identities to [`Self::acquire_fresh`]. A host may
/// reuse only a lease returned after it has durably recorded the exact issued
/// sequence reported by the native runtime; otherwise it calls
/// [`Self::retire`] and that identity is never issued again by this pool.
/// The host serializes lifecycle transitions and invalidates the previous
/// runtime's callbacks before reuse; a lease value is not an admission token.
#[derive(Debug, Default)]
pub struct ForegroundNodeLeasePool {
    reusable: BTreeMap<NodeUuid, TxTime>,
    active: BTreeSet<NodeUuid>,
    retired: BTreeSet<NodeUuid>,
}

impl ForegroundNodeLeasePool {
    /// Acquire one previously cleanly returned lease, if any.
    pub fn acquire_reusable(&mut self) -> Option<ForegroundNodeLease> {
        let (node, confirmed_tx_time) = self
            .reusable
            .iter()
            .next()
            .map(|(node, high_water)| (*node, *high_water))?;
        let removed = self.reusable.remove(&node);
        debug_assert_eq!(removed, Some(confirmed_tx_time));
        let inserted = self.active.insert(node);
        debug_assert!(inserted);
        Some(ForegroundNodeLease {
            node,
            confirmed_tx_time,
        })
    }

    /// Issue a never-before-seen identity to one live foreground runtime.
    pub fn acquire_fresh(
        &mut self,
        node: NodeUuid,
    ) -> Result<ForegroundNodeLease, ForegroundNodeLeaseError> {
        if self.active.contains(&node)
            || self.reusable.contains_key(&node)
            || self.retired.contains(&node)
        {
            return Err(ForegroundNodeLeaseError::DuplicateNode);
        }
        let inserted = self.active.insert(node);
        debug_assert!(inserted);
        Ok(ForegroundNodeLease {
            node,
            confirmed_tx_time: TxTime::default(),
        })
    }

    /// Mark an active lease reusable after the adapter has durably persisted
    /// this runtime-owned high-water mark.
    ///
    /// Callers must never construct this from JavaScript input. A failed native
    /// readout or failed persistence is an uncertain termination and must use
    /// [`Self::retire`] instead.
    pub fn clean_handoff(
        &mut self,
        lease: ForegroundNodeLease,
    ) -> Result<(), ForegroundNodeLeaseError> {
        if !self.active.remove(&lease.node) {
            return Err(ForegroundNodeLeaseError::InactiveLease);
        }
        let replaced = self.reusable.insert(lease.node, lease.confirmed_tx_time);
        debug_assert!(replaced.is_none());
        Ok(())
    }

    /// Permanently retire an active identity after unclean or uncertain end of
    /// life. Retired identities are never reused by this pool.
    pub fn retire(&mut self, lease: ForegroundNodeLease) -> Result<(), ForegroundNodeLeaseError> {
        if !self.active.remove(&lease.node) {
            return Err(ForegroundNodeLeaseError::InactiveLease);
        }
        let inserted = self.retired.insert(lease.node);
        debug_assert!(inserted);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(value: u8) -> NodeUuid {
        NodeUuid::from_bytes([value; 16])
    }

    /// Alice's host returns exactly the runtime-confirmed floor after a clean close.
    /// The pool is tested directly because its exclusivity is a host lifecycle contract.
    #[test]
    fn reuses_only_a_cleanly_returned_runtime_high_water() {
        let mut pool = ForegroundNodeLeasePool::default();
        let lease = pool.acquire_fresh(node(1)).unwrap();
        pool.clean_handoff(ForegroundNodeLease {
            confirmed_tx_time: TxTime(99),
            ..lease
        })
        .unwrap();

        assert_eq!(
            pool.acquire_reusable(),
            Some(ForegroundNodeLease {
                node: node(1),
                confirmed_tx_time: TxTime(99),
            })
        );
    }

    /// Alice's host never recycles a node after an uncertain close.
    /// The pool is tested directly because its exclusivity is a host lifecycle contract.
    #[test]
    fn unclean_lease_is_retired_and_cannot_be_reissued() {
        let mut pool = ForegroundNodeLeasePool::default();
        let lease = pool.acquire_fresh(node(2)).unwrap();
        pool.retire(lease).unwrap();

        assert_eq!(pool.acquire_reusable(), None);
        assert_eq!(
            pool.acquire_fresh(node(2)),
            Err(ForegroundNodeLeaseError::DuplicateNode)
        );
        assert_eq!(
            pool.clean_handoff(lease),
            Err(ForegroundNodeLeaseError::InactiveLease)
        );
    }

    /// Alice and Bob cannot concurrently receive one node or overwrite its clean return.
    /// The pool is tested directly because its exclusivity is a host lifecycle contract.
    #[test]
    fn checked_out_reusable_node_cannot_be_issued_concurrently_or_cleaned_twice() {
        let mut pool = ForegroundNodeLeasePool::default();
        let initial = pool.acquire_fresh(node(3)).unwrap();
        pool.clean_handoff(ForegroundNodeLease {
            confirmed_tx_time: TxTime(33),
            ..initial
        })
        .unwrap();

        let checked_out = pool.acquire_reusable().unwrap();
        assert_eq!(checked_out.node, node(3));
        assert_eq!(checked_out.confirmed_tx_time, TxTime(33));
        assert_eq!(pool.acquire_reusable(), None);
        assert_eq!(
            pool.acquire_fresh(node(3)),
            Err(ForegroundNodeLeaseError::DuplicateNode),
            "a live runtime remains exclusive even after a prior clean return"
        );

        pool.clean_handoff(ForegroundNodeLease {
            confirmed_tx_time: TxTime(34),
            ..checked_out
        })
        .unwrap();
        assert_eq!(
            pool.clean_handoff(checked_out),
            Err(ForegroundNodeLeaseError::InactiveLease),
            "a duplicate cleanup cannot overwrite the later runtime handoff"
        );
        assert_eq!(
            pool.acquire_reusable(),
            Some(ForegroundNodeLease {
                node: node(3),
                confirmed_tx_time: TxTime(34),
            })
        );
    }

    /// Alice's host ignores a late clean return after retiring the node.
    /// The pool is tested directly because its exclusivity is a host lifecycle contract.
    #[test]
    fn retirement_wins_over_a_late_clean_handoff() {
        let mut pool = ForegroundNodeLeasePool::default();
        let lease = pool.acquire_fresh(node(4)).unwrap();
        pool.retire(lease).unwrap();

        assert_eq!(
            pool.clean_handoff(ForegroundNodeLease {
                confirmed_tx_time: TxTime(44),
                ..lease
            }),
            Err(ForegroundNodeLeaseError::InactiveLease)
        );
        assert_eq!(pool.acquire_reusable(), None);
        assert_eq!(
            pool.acquire_fresh(node(4)),
            Err(ForegroundNodeLeaseError::DuplicateNode)
        );
    }
}
