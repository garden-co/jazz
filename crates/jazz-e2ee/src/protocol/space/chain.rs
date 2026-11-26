//! The space's root and its chain of versions (`E2EE_PROTOCOL.md` §3.2
//! "Several roots", §3.3 "Chain rule" and "Unresolved history").
//!
//! One rule decides the root and the current version for every member:
//! root validity and each successor's validity depend only on the space's
//! rows and on account state as of each record's position, so every client
//! reading the same rows reaches the same chain.

use std::collections::{BTreeMap, BTreeSet};

use super::super::domain::AccountDomain;
use super::super::error::{ProtocolError, ProtocolResult, UnsupportedCapability};
use super::super::position::{Accepted, Position};
use super::super::records::space::{SpaceAddress, SpaceRoot, SpaceScope, SpaceSuccessor};
use super::ledger::{AccountDirectory, AuthorCheck, SpaceLedger};

/// Why a root of the address is not valid (§3.2). A diagnostic only: the
/// protocol reports no code for it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum InvalidSpaceRoot {
    /// The row does not satisfy its own format.
    Malformed,
    /// The creator account had no counting root strictly before the root.
    NoAccountRoot,
    /// The root's device was not active with account epoch
    /// `accountEpochId` at the root's position.
    NotActive,
    /// The signature does not verify under the device's signing key.
    SignatureInvalid,
    /// The creator's account history is an intrinsic contradiction (§7).
    AccountBroken,
}

/// A root of the address that is not the space's root: a **lost
/// creation** (§3.2). Its creator's device repairs the rows it wrote under
/// its key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LostRoot {
    pub root_id: String,
    pub position: Position,
    pub creator_account_id: String,
    pub device_id: String,
    pub epoch_id: String,
    /// Why the root is not valid, for roots ordered before the space's
    /// root (or every root, when the address has no valid root). `None`
    /// for a root after the space's root, which is lost whether or not it
    /// is valid, and is not checked.
    pub invalid: Option<InvalidSpaceRoot>,
}

/// The roots of an address (§3.2): the space's root, if any, and every
/// other root.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpaceRoots {
    /// The first valid root of the address (position, then ID).
    pub root: Option<Accepted<SpaceRoot>>,
    /// Every other root of the address, by position then ID.
    pub lost: Vec<LostRoot>,
}

fn lost(root: &Accepted<SpaceRoot>, invalid: Option<InvalidSpaceRoot>) -> LostRoot {
    LostRoot {
        root_id: root.row.id.clone(),
        position: root.position,
        creator_account_id: root.row.creator_account_id.clone(),
        device_id: root.row.device_id.clone(),
        epoch_id: root.row.epoch_id.clone(),
        invalid,
    }
}

/// Chooses the space's root (§3.2, "Several roots"): a root is valid iff it
/// is well formed and of this address, its creator had an account root
/// strictly before it, its device was active with account epoch
/// `accountEpochId` at its position (§2.4), and its signature verifies in
/// the creator's domain. The space's root is the first valid root
/// (position, then ID); an invalid earlier root never blocks a valid later
/// one. A root whose check needs a capability this client lacks, before
/// any valid root, leaves the choice unresolved
/// ([`ProtocolError::Unsupported`]): the client does not skip it.
pub fn select_root(
    ledger: &SpaceLedger,
    accounts: &AccountDirectory<'_>,
) -> ProtocolResult<SpaceRoots> {
    check_read(ledger, Some(accounts))?;
    let mut roots: Vec<_> = ledger.address_roots().collect();
    super::by_position_then_id(&mut roots);
    let mut chosen = None;
    let mut lost_roots = Vec::new();
    for root in roots {
        if chosen.is_some() {
            lost_roots.push(lost(root, None));
            continue;
        }
        match check_root(root, &ledger.address, accounts)? {
            None => chosen = Some(root.clone()),
            Some(reason) => lost_roots.push(lost(root, Some(reason))),
        }
    }
    Ok(SpaceRoots {
        root: chosen,
        lost: lost_roots,
    })
}

/// Fails with [`ProtocolError::InconsistentEvidence`] unless `ledger` is
/// one read (every row at or before its cut, no row ID repeated in a table)
/// and `accounts`, when given, was read at the same cut.
fn check_read(ledger: &SpaceLedger, accounts: Option<&AccountDirectory<'_>>) -> ProtocolResult<()> {
    ledger.check()?;
    match accounts {
        Some(accounts) if accounts.through() != ledger.through => {
            Err(ProtocolError::InconsistentEvidence)
        }
        _ => Ok(()),
    }
}

/// The read a chain was replayed from: the space's address and the cut.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Source {
    address: SpaceAddress,
    through: Position,
}

/// The root's own checks; `None` when it is valid.
fn check_root(
    root: &Accepted<SpaceRoot>,
    address: &SpaceAddress,
    accounts: &AccountDirectory<'_>,
) -> ProtocolResult<Option<InvalidSpaceRoot>> {
    let row = &root.row;
    if row.validate().is_err() || row.address().ok().as_ref() != Some(address) {
        return Ok(Some(InvalidSpaceRoot::Malformed));
    }
    let application = accounts.domain().account(&row.creator_account_id)?;
    let Ok(bytes) = row.signed_bytes(&application) else {
        return Ok(Some(InvalidSpaceRoot::Malformed));
    };
    Ok(
        match accounts.verify_author(
            &row.creator_account_id,
            &row.device_id,
            &row.account_epoch_id,
            root.position,
            &bytes,
            &row.signature,
        )? {
            AuthorCheck::Valid => None,
            AuthorCheck::NoAccountRoot => Some(InvalidSpaceRoot::NoAccountRoot),
            AuthorCheck::NotActive => Some(InvalidSpaceRoot::NotActive),
            AuthorCheck::SignatureInvalid => Some(InvalidSpaceRoot::SignatureInvalid),
            AuthorCheck::AccountBroken(_) => Some(InvalidSpaceRoot::AccountBroken),
        },
    )
}

/// One key version of the chain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpaceVersion {
    pub epoch_id: String,
    pub position: Position,
    /// The accepted successor that opened this version; `None` for the
    /// root version.
    pub successor: Option<Accepted<SpaceSuccessor>>,
}

impl SpaceVersion {
    pub(crate) fn record_id<'v>(&'v self, root: &'v SpaceRoot) -> &'v str {
        self.successor
            .as_ref()
            .map_or(&root.id, |successor| &successor.row.id)
    }

    pub(crate) fn account_envelope<'v>(
        &'v self,
        chain: &'v SpaceChain,
    ) -> ProtocolResult<(Vec<u8>, &'v [u8])> {
        match &self.successor {
            None => Ok((
                chain
                    .root
                    .row
                    .account_envelope_context(&chain.application)?,
                &chain.root.row.account_envelope,
            )),
            Some(successor) => Ok((
                successor
                    .row
                    .account_envelope_context(&chain.application, &chain.scope)?,
                &successor.row.account_envelope,
            )),
        }
    }

    /// The account that wrote this version's record, and the account epoch
    /// its `accountEnvelope` is sealed to.
    pub fn writer<'v>(&'v self, root: &'v SpaceRoot) -> (&'v str, &'v str) {
        match &self.successor {
            None => (&root.creator_account_id, &root.account_epoch_id),
            Some(successor) => (
                &successor.row.author_account_id,
                &successor.row.author_epoch_id,
            ),
        }
    }
}

/// Where the chain stops being decidable (§3.3, "Unresolved history"): a
/// successor that passes every other check needs a capability this client
/// lacks. The chain is not extended past it and has no current version.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Unresolved {
    pub successor_id: String,
    pub position: Position,
    pub capability: UnsupportedCapability,
}

/// A space's root and its authenticated chain at one cut (§3.3). Built
/// only by [`SpaceChain::replay`], which records the read it came from:
/// every function that takes the chain together with a space ledger or an
/// account directory refuses evidence from another read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpaceChain {
    /// The domain of the root's creator, `Dₐ(creatorAccountId)`, in which
    /// every record of the space is signed, sealed and wrapped.
    pub application: AccountDomain,
    /// The address and root ID every record's context binds.
    pub scope: SpaceScope,
    pub root: Accepted<SpaceRoot>,
    /// The root version, then each accepted successor's, in chain order.
    pub versions: Vec<SpaceVersion>,
    pub unresolved: Option<Unresolved>,
    /// The other roots of the address (§3.2).
    pub lost: Vec<LostRoot>,
    source: Source,
}

impl SpaceChain {
    /// Chooses the root ([`select_root`]) and replays the chain from it.
    /// `None` when the address has no valid root: the space does not exist
    /// yet.
    ///
    /// The chain rule: from the root's version, the next version is the
    /// first successor of the address, in position order then ID, that
    /// names the current version as `predecessor`, is strictly later than
    /// it, has an epoch not used before, is well formed, whose
    /// `historyEpochId` is the predecessor or an earlier version of the
    /// chain, whose author device was active with account epoch
    /// `authorEpochId` at its position, and whose signature verifies. A
    /// successor that would be next but needs a capability this client
    /// lacks leaves the chain [`Unresolved`] there.
    pub fn replay(
        ledger: &SpaceLedger,
        accounts: &AccountDirectory<'_>,
    ) -> ProtocolResult<Option<Self>> {
        let SpaceRoots { root, lost } = select_root(ledger, accounts)?;
        let Some(root) = root else {
            return Ok(None);
        };
        let application = accounts.domain().account(&root.row.creator_account_id)?;
        let scope = root.row.scope()?;
        let mut chain = Self {
            application,
            scope,
            versions: vec![SpaceVersion {
                epoch_id: root.row.epoch_id.clone(),
                position: root.position,
                successor: None,
            }],
            root,
            unresolved: None,
            lost,
            source: Source {
                address: ledger.address.clone(),
                through: ledger.through,
            },
        };
        let mut used = BTreeSet::from([chain.root.row.epoch_id.clone()]);
        let mut candidates: Vec<_> = ledger.address_successors().collect();
        super::by_position_then_id(&mut candidates);
        for candidate in candidates {
            let tip = chain.last();
            let successor = &candidate.row;
            if successor.predecessor != tip.epoch_id
                || candidate.position <= tip.position
                || used.contains(&successor.epoch_id)
                || successor.validate(&chain.scope).is_err()
                || !used.contains(&successor.history_epoch_id)
            {
                continue;
            }
            let Ok(bytes) = successor.signed_bytes(&chain.application, &chain.scope) else {
                continue;
            };
            let check = accounts.verify_author(
                &successor.author_account_id,
                &successor.author_device_id,
                &successor.author_epoch_id,
                candidate.position,
                &bytes,
                &successor.signature,
            );
            match check {
                Ok(AuthorCheck::Valid) => {}
                Ok(_) => continue,
                Err(ProtocolError::Unsupported(capability)) => {
                    chain.unresolved = Some(Unresolved {
                        successor_id: successor.id.clone(),
                        position: candidate.position,
                        capability,
                    });
                    break;
                }
                Err(error) => return Err(error),
            }
            used.insert(successor.epoch_id.clone());
            chain.versions.push(SpaceVersion {
                epoch_id: successor.epoch_id.clone(),
                position: candidate.position,
                successor: Some(candidate.clone()),
            });
        }
        Ok(Some(chain))
    }

    pub fn address(&self) -> &SpaceAddress {
        &self.scope.address
    }

    /// The cut of the read this chain was replayed from.
    pub fn through(&self) -> Position {
        self.source.through
    }

    pub(super) fn at_matching_read(
        &self,
        ledger: &SpaceLedger,
        accounts: &AccountDirectory<'_>,
    ) -> ProtocolResult<Self> {
        check_read(ledger, Some(accounts))?;
        let mut chain = self.clone();
        chain.source.through = ledger.through;
        Ok(chain)
    }

    /// Fails with [`ProtocolError::InconsistentEvidence`] unless `ledger`
    /// is the read this chain was replayed from (same address and cut) and
    /// passes its own check, and `accounts`, when given, was read at the
    /// same cut. Every function that combines a chain with a space ledger
    /// or an account directory checks this first.
    pub(crate) fn check_read(
        &self,
        ledger: &SpaceLedger,
        accounts: Option<&AccountDirectory<'_>>,
    ) -> ProtocolResult<()> {
        check_read(ledger, accounts)?;
        if ledger.address != self.source.address || ledger.through != self.source.through {
            return Err(ProtocolError::InconsistentEvidence);
        }
        Ok(())
    }

    /// The account that created the space's root, whose domain the space's
    /// records use.
    pub fn creator(&self) -> &str {
        &self.root.row.creator_account_id
    }

    /// The current version, or the unsupported capability that leaves it
    /// unresolved.
    pub fn current(&self) -> ProtocolResult<&SpaceVersion> {
        match &self.unresolved {
            Some(unresolved) => Err(ProtocolError::Unsupported(unresolved.capability)),
            None => Ok(self.last()),
        }
    }

    /// The depth of the current version (root = 0), or the unsupported
    /// capability that leaves it unresolved.
    pub fn current_depth(&self) -> ProtocolResult<usize> {
        self.current().map(|_| self.versions.len() - 1)
    }

    /// The last authenticated version, whether or not the chain is
    /// resolved past it.
    pub fn last(&self) -> &SpaceVersion {
        self.versions.last().expect("a chain has its root version")
    }

    /// The depth of `epoch_id` in the chain (root = 0), if it is a version
    /// of this space.
    pub fn depth_of(&self, epoch_id: &str) -> Option<usize> {
        self.versions.iter().position(|version| {
            #[cfg(test)]
            HISTORY_EPOCH_PROBES.set(HISTORY_EPOCH_PROBES.get() + 1);
            version.epoch_id == epoch_id
        })
    }

    /// A history walk borrows the current versions, so the index stays in
    /// sync even if a caller edits the public versions between walks.
    pub(super) fn epoch_depths(&self) -> BTreeMap<&str, usize> {
        let mut depths = BTreeMap::new();
        for (depth, version) in self.versions.iter().enumerate() {
            depths.entry(version.epoch_id.as_str()).or_insert(depth);
        }
        depths
    }

    pub fn version(&self, depth: usize) -> Option<&SpaceVersion> {
        self.versions.get(depth)
    }

    /// The depth of the version whose key the `history` of the version at
    /// `depth` wraps: its predecessor's, except after a recovery rotation
    /// (§3.5). `None` for the root version.
    pub fn history_depth(&self, depth: usize) -> Option<usize> {
        self.depth_of(self.history_epoch(depth)?)
    }

    pub(super) fn indexed_history_depth(
        &self,
        depth: usize,
        depths: &BTreeMap<&str, usize>,
    ) -> Option<usize> {
        let epoch = self.history_epoch(depth)?;
        #[cfg(test)]
        HISTORY_EPOCH_PROBES.set(HISTORY_EPOCH_PROBES.get() + 1);
        depths.get(epoch).copied()
    }

    fn history_epoch(&self, depth: usize) -> Option<&str> {
        let successor = self.versions.get(depth)?.successor.as_ref()?;
        Some(&successor.row.history_epoch_id)
    }

    /// The depths whose keys the key of the version at `depth` opens
    /// through the `history` links, starting with `depth` itself and ending
    /// at the root.
    pub fn history_path(&self, depth: usize) -> Vec<usize> {
        let depths = self.epoch_depths();
        let mut path = vec![depth];
        let mut at = depth;
        while let Some(previous) = self.indexed_history_depth(at, &depths) {
            path.push(previous);
            at = previous;
        }
        path
    }

    /// The versions no `history` link from the last version reaches (§3.3,
    /// §3.5): skipped by a recovery rotation, so the chain gives no key for
    /// them and a device obtains them only from its own account envelope or
    /// a delivery of that version. By depth.
    pub fn skipped(&self) -> Vec<usize> {
        let reached: BTreeSet<usize> = self
            .history_path(self.versions.len() - 1)
            .into_iter()
            .collect();
        (0..self.versions.len())
            .filter(|depth| !reached.contains(depth))
            .collect()
    }

    /// The versions concerned by delivery (§3.4): the current version and
    /// the skipped versions, by depth, current last.
    pub fn concerned(&self) -> ProtocolResult<Vec<usize>> {
        let current = self.current_depth()?;
        let mut depths = self.skipped();
        depths.push(current);
        Ok(depths)
    }

    /// The root and the successors up to `depth`, in chain order: the
    /// lineage a held key of the version at `depth` is stored with (§6.2).
    pub fn lineage(&self, depth: usize) -> Option<(&SpaceRoot, Vec<&SpaceSuccessor>)> {
        let versions = self.versions.get(..=depth)?;
        Some((
            &self.root.row,
            versions
                .iter()
                .filter_map(|version| version.successor.as_ref().map(|successor| &successor.row))
                .collect(),
        ))
    }
}

#[cfg(test)]
std::thread_local! {
    static HISTORY_EPOCH_PROBES: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::domain::AppDomain;

    /// Alice follows the history of her rotated space. Count epoch lookups
    /// here because the public API exposes only the resulting path.
    #[test]
    fn rotated_history_looks_up_each_link_once() {
        let id = |number: usize| format!("00000000-0000-4000-8000-{number:012x}");
        let root = SpaceRoot {
            id: id(1),
            scope_id: id(2),
            identifier: id(3),
            creator_account_id: id(4),
            device_id: id(5),
            account_epoch_id: id(6),
            epoch_id: id(1000),
            mechanism: "test".into(),
            version: 1,
            verification: Vec::new(),
            account_envelope: Vec::new(),
            signature: Vec::new(),
        };
        let scope = root.scope().unwrap();
        let source = Source {
            address: scope.address.clone(),
            through: Position(101),
        };
        let versions = (0..=100)
            .map(|depth: usize| {
                let epoch_id = id(1000 + depth);
                let position = Position(1 + depth as u64);
                let successor = depth.checked_sub(1).map(|previous| {
                    Accepted::new(
                        position,
                        SpaceSuccessor {
                            id: id(100 + depth),
                            scope_id: root.scope_id.clone(),
                            identifier: root.identifier.clone(),
                            predecessor: id(1000 + previous),
                            history_epoch_id: id(1000 + previous),
                            epoch_id: epoch_id.clone(),
                            author_account_id: root.creator_account_id.clone(),
                            author_device_id: root.device_id.clone(),
                            author_epoch_id: root.account_epoch_id.clone(),
                            verification: Vec::new(),
                            history: Vec::new(),
                            account_envelope: Vec::new(),
                            signature: Vec::new(),
                        },
                    )
                });
                SpaceVersion {
                    epoch_id,
                    position,
                    successor,
                }
            })
            .collect();
        let application = AppDomain::new(&id(7), None)
            .unwrap()
            .account(&root.creator_account_id)
            .unwrap();
        let chain = SpaceChain {
            application,
            scope,
            root: Accepted::new(Position(1), root),
            versions,
            unresolved: None,
            lost: Vec::new(),
            source,
        };

        HISTORY_EPOCH_PROBES.set(0);
        assert_eq!(chain.history_path(100), (0..=100).rev().collect::<Vec<_>>());
        assert_eq!(HISTORY_EPOCH_PROBES.get(), 100);
    }
}
