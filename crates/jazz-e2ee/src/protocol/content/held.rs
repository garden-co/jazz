//! The space keys a device holds (`E2EE_PROTOCOL.md` §4.2, §5.2, §5.4,
//! §6.2), and the choices made over them.
//!
//! Keys are held per **space root**, not per address (§6.1): the key of a
//! refused or lost creation and the keys of the address's real root are
//! kept side by side. Each [`HeldSpace`] stores its root and the successors
//! up to its deepest held key (its lineage) and one key per held depth.
//! The root key of this device's own creation carries a [`KeyMark`]:
//! `unpublished` until the creation is accepted as the space's root,
//! `refused` for good once it is not (§3.2, §5.4).
//!
//! - A write uses the held key of greatest depth that is not `refused`, of
//!   one root only: the root of the chain the device read, or before any
//!   read the one root it holds such keys of (§5.2, §5.4).
//! - A read uses any held key of the epoch its cell names, of any mark
//!   (§4.2, §3.2): a refused key always opens the cells this device wrote
//!   under it. Only a key of the address's own root (the root of the chain
//!   the device last read, or before any read a key that is not `refused`)
//!   decides that a cell is invalid, or counts as holding its version.
//! - What an acquisition shows about a version a read needs settles into a
//!   data error (§4.2, §4.3); an epoch the acquisition shows is not in the
//!   space's chain is remembered until the chain the device knows advances
//!   ([`KnownChain`]).

use std::collections::{BTreeMap, BTreeSet};

use zeroize::Zeroizing;

use crate::registry::Registry;

use super::super::domain::{AccountDomain, AppDomain};
use super::super::error::{ProtocolError, ProtocolResult, UnsupportedCapability};
use super::super::json;
use super::super::records::space::{SpaceAddress, SpaceRoot, SpaceScope, SpaceSuccessor};
use super::super::space::build::RootRecords;
use super::super::space::keys::{Outcome, marker};
use super::super::space::{SpaceChain, SpaceKeys};
use super::DataError;

/// The state of one held key (§5.4, §6.2).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum KeyMark {
    /// Acquired from the space's chain, or this device's creation accepted
    /// as the space's root.
    Published,
    /// This device's creation, not yet accepted as the space's root. Carries
    /// the creation's durable transaction identity, by which it is
    /// reconciled after a restart.
    Unpublished { transaction: String },
    /// This device's creation, refused, failed, cancelled or lost to
    /// another root. Kept for good; never used for a new write (§3.2).
    Refused,
}

/// What became of the transaction that carried this device's space root
/// (§3.2, §5.4), as the runtime knows it.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum CreationFate {
    /// The transaction was accepted.
    Accepted,
    /// The root was refused, its transaction failed otherwise (at Local or
    /// Global), or it was cancelled before possible submission.
    NotAccepted,
    /// Pending, or of unknown outcome.
    Undecided,
    /// After a restart, the store does not know the transaction identity.
    UnknownToStore,
}

pub(crate) struct HeldEntry {
    pub(crate) key: Zeroizing<[u8; 32]>,
    pub(crate) mark: KeyMark,
}

/// The keys this device holds under one space root, with the root and the
/// successors up to the deepest of them (§6.2). Built only from an
/// authenticated acquisition ([`HeldSpace::from_acquisition`]), this
/// device's own creation ([`HeldSpace::from_creation`]) or held-key records
/// that pass their load-time checks ([`super::store`]), so every key passed
/// its version's marker. Keys are cleared on drop and never printed.
pub struct HeldSpace {
    pub(crate) application: AccountDomain,
    pub(crate) scope: SpaceScope,
    pub(crate) root: SpaceRoot,
    pub(crate) successors: Vec<SpaceSuccessor>,
    pub(crate) keys: BTreeMap<usize, HeldEntry>,
}

impl core::fmt::Debug for HeldSpace {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("HeldSpace")
            .field("scope", &self.scope)
            .field(
                "held",
                &self
                    .keys
                    .iter()
                    .map(|(depth, entry)| (depth, &entry.mark))
                    .collect::<Vec<_>>(),
            )
            .finish()
    }
}

impl HeldSpace {
    /// The keys of an acquisition (§3.3, §4.2: every key it accepted,
    /// earlier versions included, becomes a held key), with the lineage of
    /// `chain` up to the deepest of them. `keys` must have been
    /// authenticated against `chain`. `None` when it holds no key.
    pub fn from_acquisition(chain: &SpaceChain, keys: &SpaceKeys) -> ProtocolResult<Option<Self>> {
        keys.check_chain(chain)?;
        let Some(deepest) = keys.depths().last() else {
            return Ok(None);
        };
        let (root, successors) = chain
            .lineage(deepest)
            .ok_or(ProtocolError::InconsistentEvidence)?;
        let mut held = BTreeMap::new();
        for depth in keys.depths() {
            let epoch_id = &chain.versions[depth].epoch_id;
            let key = keys
                .key(epoch_id)
                .ok_or(ProtocolError::InconsistentEvidence)?;
            held.insert(
                depth,
                HeldEntry {
                    key: Zeroizing::new(*key),
                    mark: KeyMark::Published,
                },
            );
        }
        Ok(Some(Self {
            application: chain.application.clone(),
            scope: chain.scope.clone(),
            root: root.clone(),
            successors: successors.into_iter().cloned().collect(),
            keys: held,
        }))
    }

    /// This device's new root and its key `K₀`, held `unpublished` with the
    /// creation's durable transaction identity before the transaction
    /// commits (§5.4). The key must pass the root's marker.
    pub fn from_creation(
        registry: &Registry,
        domain: &AppDomain,
        records: &RootRecords,
        transaction: &str,
    ) -> ProtocolResult<Self> {
        let root = &records.root;
        root.validate()?;
        if transaction.is_empty() {
            return Err(ProtocolError::InvalidRecord("empty creation transaction"));
        }
        let application = domain.account(&root.creator_account_id)?;
        let context = root.marker_context(&application)?;
        match marker(registry, &records.key, &context, &root.verification)? {
            Outcome::Ok(()) => {}
            Outcome::Invalid | Outcome::Unsupported => {
                return Err(ProtocolError::InvalidRecord("key fails its root's marker"));
            }
        }
        Ok(Self {
            application,
            scope: root.scope()?,
            root: root.clone(),
            successors: Vec::new(),
            keys: BTreeMap::from([(
                0,
                HeldEntry {
                    key: Zeroizing::new(*records.key),
                    mark: KeyMark::Unpublished {
                        transaction: transaction.to_owned(),
                    },
                },
            )]),
        })
    }

    pub fn root_id(&self) -> &str {
        &self.root.id
    }

    pub fn address(&self) -> &SpaceAddress {
        &self.scope.address
    }

    pub fn scope(&self) -> &SpaceScope {
        &self.scope
    }

    pub fn root(&self) -> &SpaceRoot {
        &self.root
    }

    /// The successors from the root onwards, in chain order, as far as the
    /// deepest held key at least.
    pub fn successors(&self) -> &[SpaceSuccessor] {
        &self.successors
    }

    /// The domain of the root's creator, in which the space's records are
    /// wrapped and sealed.
    pub fn application(&self) -> &AccountDomain {
        &self.application
    }

    /// The epoch at `depth` of the lineage (the root's at depth 0).
    pub fn epoch_at(&self, depth: usize) -> Option<&str> {
        match depth {
            0 => Some(&self.root.epoch_id),
            _ => self
                .successors
                .get(depth - 1)
                .map(|successor| successor.epoch_id.as_str()),
        }
    }

    pub fn depth_of(&self, epoch_id: &str) -> Option<usize> {
        if self.root.epoch_id == epoch_id {
            return Some(0);
        }
        self.successors
            .iter()
            .position(|successor| successor.epoch_id == epoch_id)
            .map(|index| index + 1)
    }

    /// The held depths, ascending, with their marks.
    pub fn held(&self) -> impl Iterator<Item = (usize, &KeyMark)> {
        self.keys.iter().map(|(depth, entry)| (*depth, &entry.mark))
    }

    pub fn mark(&self, depth: usize) -> Option<&KeyMark> {
        self.keys.get(&depth).map(|entry| &entry.mark)
    }

    pub(crate) fn key_at(&self, depth: usize) -> Option<&[u8; 32]> {
        self.keys.get(&depth).map(|entry| &*entry.key)
    }

    /// Keeps `other`, a later holding of the same root: the longer lineage
    /// is kept (one must extend the other), and each of its keys replaces
    /// the record of the same epoch (§6.2), except that a `refused` mark is
    /// final (§3.2): a refused key never becomes usable for writes again.
    pub fn keep(&mut self, other: HeldSpace) -> ProtocolResult<()> {
        if other.root != self.root {
            return Err(ProtocolError::InconsistentEvidence);
        }
        let (short, long) = if other.successors.len() > self.successors.len() {
            (&self.successors, &other.successors)
        } else {
            (&other.successors, &self.successors)
        };
        if long[..short.len()] != short[..] {
            return Err(ProtocolError::InconsistentEvidence);
        }
        if other.successors.len() > self.successors.len() {
            self.successors = other.successors;
        }
        for (depth, entry) in other.keys {
            match self.keys.get(&depth) {
                Some(held) if held.mark == KeyMark::Refused => {}
                _ => {
                    self.keys.insert(depth, entry);
                }
            }
        }
        Ok(())
    }

    /// Applies a creation's outcome to the `unpublished` keys of this root
    /// (§5.4). `space_root_id` is the root of the space's chain at a read
    /// that includes the transaction's outcome, if a valid root is readable.
    ///
    /// - Accepted, or unknown to the store after a restart: the mark is
    ///   cleared when this is the space's root, and set to `refused` when
    ///   another root is; with no valid root readable it is kept.
    /// - Not accepted: `refused`.
    /// - Pending or unknown: kept.
    ///
    /// Published and refused keys are never changed.
    pub fn settle(&mut self, fate: CreationFate, space_root_id: Option<&str>) {
        let next = match (fate, space_root_id) {
            (CreationFate::NotAccepted, _) => KeyMark::Refused,
            (CreationFate::Undecided, _) | (_, None) => return,
            (_, Some(root_id)) if root_id == self.root.id => KeyMark::Published,
            (_, Some(_)) => KeyMark::Refused,
        };
        for entry in self.keys.values_mut() {
            if matches!(entry.mark, KeyMark::Unpublished { .. }) {
                entry.mark = next.clone();
            }
        }
    }
}

/// The key a write uses (§5.2): a held key of the row's space.
#[derive(Clone, Copy)]
pub struct WriteKey<'k> {
    pub(crate) space: &'k HeldSpace,
    pub(crate) depth: usize,
}

impl core::fmt::Debug for WriteKey<'_> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("WriteKey")
            .field("scope", &self.space.scope)
            .field("depth", &self.depth)
            .finish()
    }
}

impl<'k> WriteKey<'k> {
    pub fn address(&self) -> &'k SpaceAddress {
        self.space.address()
    }

    pub fn root_id(&self) -> &'k str {
        self.space.root_id()
    }

    pub fn epoch_id(&self) -> &'k str {
        self.space
            .epoch_at(self.depth)
            .expect("a held depth is in the lineage")
    }

    pub fn depth(&self) -> usize {
        self.depth
    }

    pub fn mark(&self) -> &'k KeyMark {
        &self.space.keys[&self.depth].mark
    }

    pub(crate) fn key(&self) -> &'k [u8; 32] {
        &self.space.keys[&self.depth].key
    }
}

/// What a write to a space can do with the keys held (§5.1, §5.2, §3.2).
#[derive(Debug, Clone, Copy)]
pub enum WriteChoice<'k> {
    /// Seal under this key.
    Key(WriteKey<'k>),
    /// No usable key: the runtime acquires one, or prepares the space's
    /// root, and the write is staged again (H2 `NeedsKey`).
    NeedsKey,
    /// The write fails `space-pending` without staging a root: the space
    /// has no known valid root and this device's creation of it was
    /// refused while the client runs; or keys of more than one root of the
    /// address are held and not `refused`, and this session has read no
    /// chain of it, so it cannot tell which root is the space's. Either
    /// way a later read of the chain decides.
    SpacePending,
}

/// The space's chain as the device last read it, for one address: which
/// epochs it has, and which epochs an acquisition showed are not in it
/// (§4.2). A cell of such an epoch is `invalid-ciphertext` without
/// acquiring again, until the chain the device knows advances.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct KnownChain {
    root_id: String,
    epochs: Vec<String>,
    /// The capability the chain is unresolved for, past its last version.
    unresolved: Option<UnsupportedCapability>,
    absent: BTreeSet<String>,
}

impl KnownChain {
    pub fn epochs(&self) -> &[String] {
        &self.epochs
    }

    pub fn root_id(&self) -> &str {
        &self.root_id
    }

    /// Whether an acquisition showed that `epoch_id` is not in the chain
    /// the device still knows.
    pub fn is_absent(&self, epoch_id: &str) -> bool {
        self.absent.contains(epoch_id)
    }

    /// Takes a read of the chain. The cut a read claims is not trusted
    /// (the server gives it); what the read shows decides. It replaces the
    /// known chain, forgetting the epochs remembered as absent, when it
    /// names another root; when its versions are not a prefix of the known
    /// ones (it extends them, or forks from them where both have a
    /// version); or when it is unresolved and does not show exactly the
    /// known versions already unresolved there: an unresolved read cannot
    /// confirm absence past its unresolved point. Any other read (a prefix
    /// of the known versions that is resolved, or the same read again) is
    /// older or shows less, and changes nothing. Honest reads only ever
    /// extend the chain or become unresolved at its end.
    fn observe(&mut self, chain: &SpaceChain) {
        let epochs: Vec<String> = chain
            .versions
            .iter()
            .map(|version| version.epoch_id.clone())
            .collect();
        let other_root = self.root_id != chain.root.row.id;
        let not_prefix = !self.epochs.starts_with(&epochs);
        let same_unresolved = epochs == self.epochs && self.unresolved.is_some();
        let unresolved = chain.unresolved.is_some() && !same_unresolved;
        if other_root || not_prefix || unresolved {
            self.root_id = chain.root.row.id.clone();
            self.epochs = epochs;
            self.unresolved = chain
                .unresolved
                .as_ref()
                .map(|unresolved| unresolved.capability);
            self.absent.clear();
        }
    }
}

/// What an acquisition showed about a version a read needs (§4.2, §4.3).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MissingKey {
    /// The device now holds the version: open the cell again.
    Held,
    /// Report the cell (`invalid-ciphertext`, `key-unavailable`) or the
    /// space (`key-pending`, `space-pending`).
    Report(DataError),
    /// The chain is unresolved before the version, for a capability this
    /// client lacks (§3.3): the cell is neither invalid nor known to be
    /// pending.
    Unresolved(UnsupportedCapability),
}

/// Every key this device holds, across spaces, with the chain it last read
/// for each address. The runtime builds it from its held-key documents and
/// acquisitions and keeps it in memory.
#[derive(Debug, Default)]
pub struct HeldKeys {
    spaces: Vec<HeldSpace>,
    known: BTreeMap<SpaceAddress, KnownChain>,
}

impl HeldKeys {
    pub fn new() -> Self {
        Self::default()
    }

    /// Holds `space`, keeping it with what is already held under the same
    /// root ([`HeldSpace::keep`]).
    pub fn hold(&mut self, space: HeldSpace) -> ProtocolResult<()> {
        match self
            .spaces
            .iter_mut()
            .find(|held| held.root.id == space.root.id)
        {
            Some(held) => held.keep(space),
            None => {
                self.spaces.push(space);
                Ok(())
            }
        }
    }

    pub fn spaces(&self) -> &[HeldSpace] {
        &self.spaces
    }

    pub fn space(&self, root_id: &str) -> Option<&HeldSpace> {
        self.spaces.iter().find(|space| space.root.id == root_id)
    }

    pub fn space_mut(&mut self, root_id: &str) -> Option<&mut HeldSpace> {
        self.spaces
            .iter_mut()
            .find(|space| space.root.id == root_id)
    }

    /// The roots held for `address`, in root ID order.
    pub fn roots_of<'s>(&'s self, address: &SpaceAddress) -> Vec<&'s HeldSpace> {
        let mut roots: Vec<_> = self
            .spaces
            .iter()
            .filter(|space| space.address() == address)
            .collect();
        roots.sort_by(|a, b| json::utf16_cmp(&a.root.id, &b.root.id));
        roots
    }

    /// Whether this device holds an `unpublished` key for `address`: while
    /// it does, it stages no second root for the space (§5.4).
    pub fn holds_unpublished(&self, address: &SpaceAddress) -> bool {
        self.roots_of(address).iter().any(|space| {
            space
                .held()
                .any(|(_, mark)| matches!(mark, KeyMark::Unpublished { .. }))
        })
    }

    /// The key a write to `address` uses (§5.2): the held key of greatest
    /// depth that is not `refused`, of one root. Once the device has read
    /// the address's chain, that is the chain's root: a creation that lost
    /// to it never takes a write. Before a read, keys of more than one root
    /// that are not `refused` take no write (the chain read is kept in
    /// memory only, so this holds after a restart until a read): the write
    /// fails `space-pending`. Keys of one root alone write without a read.
    /// Otherwise, with no key, the write fails
    /// `space-pending` when `creation_refused` (this device's creation of
    /// the space was refused while the client runs, §3.2) and no valid
    /// root of the address is known, and needs a key when not.
    pub fn write_key<'k>(
        &'k self,
        address: &SpaceAddress,
        creation_refused: bool,
    ) -> WriteChoice<'k> {
        let writable = |space: &&HeldSpace| space.held().any(|(_, mark)| *mark != KeyMark::Refused);
        let mut roots = self.roots_of(address).into_iter().filter(writable);
        let space = match self.known.get(address) {
            Some(known) => roots.find(|space| space.root.id == known.root_id),
            None => {
                let first = roots.next();
                if roots.next().is_some() {
                    return WriteChoice::SpacePending;
                }
                first
            }
        };
        let best = space.and_then(|space| {
            let depth = space
                .held()
                .filter(|(_, mark)| **mark != KeyMark::Refused)
                .map(|(depth, _)| depth)
                .max()?;
            Some(WriteKey { space, depth })
        });
        match best {
            Some(key) => WriteChoice::Key(key),
            None if creation_refused && !self.known.contains_key(address) => {
                WriteChoice::SpacePending
            }
            None => WriteChoice::NeedsKey,
        }
    }

    /// Whether the held key of `space` at `depth` is a key of its address's
    /// own root, which alone may decide that a cell is invalid and counts as
    /// holding the version (§4.2, §4.3): once the device has read the
    /// address's chain, a key of that chain's root; before, any key not
    /// `refused`. A key of another root may still open a cell this device
    /// wrote under it.
    fn decides(&self, space: &HeldSpace, depth: usize) -> bool {
        match self.known.get(space.address()) {
            Some(known) => known.root_id == space.root.id,
            None => space.mark(depth) != Some(&KeyMark::Refused),
        }
    }

    /// The held keys of `epoch_id` under `address`, in root ID order, each
    /// with whether it decides a cell ([`Self::decides`]): any mark opens a
    /// cell (§4.2). Normally one; two roots of one address may name the
    /// same epoch, and only the right key authenticates.
    pub(crate) fn keys_of<'k>(
        &'k self,
        address: &SpaceAddress,
        epoch_id: &str,
    ) -> Vec<(&'k HeldSpace, &'k [u8; 32], bool)> {
        self.roots_of(address)
            .into_iter()
            .filter_map(|space| {
                let depth = space.depth_of(epoch_id)?;
                Some((space, space.key_at(depth)?, self.decides(space, depth)))
            })
            .collect()
    }

    /// The held keys of `epoch_id` under every other address, by address
    /// then root ID: a cell of a row that moved between spaces may be
    /// sealed under one of them (migrations §3.9).
    pub(crate) fn keys_elsewhere<'k>(
        &'k self,
        address: &SpaceAddress,
        epoch_id: &str,
    ) -> Vec<(&'k HeldSpace, &'k [u8; 32])> {
        let mut found: Vec<_> = self
            .spaces
            .iter()
            .filter(|space| space.address() != address)
            .filter_map(|space| {
                let depth = space.depth_of(epoch_id)?;
                Some((space, space.key_at(depth)?))
            })
            .collect();
        found.sort_by(|a, b| {
            a.0.address()
                .cmp(b.0.address())
                .then_with(|| json::utf16_cmp(&a.0.root.id, &b.0.root.id))
        });
        found
    }

    /// Every held version of `address`, of any mark, as `(epoch, key)` in
    /// root ID then depth order: an equality query computes one token per
    /// held version (§4.5).
    pub(crate) fn versions_of<'k>(
        &'k self,
        address: &SpaceAddress,
    ) -> Vec<(&'k str, &'k [u8; 32])> {
        self.roots_of(address)
            .into_iter()
            .flat_map(|space| {
                space.keys.iter().map(move |(depth, entry)| {
                    (
                        space
                            .epoch_at(*depth)
                            .expect("a held depth is in the lineage"),
                        &*entry.key,
                    )
                })
            })
            .collect()
    }

    /// Whether this device holds the key of `epoch_id` of the address's own
    /// root ([`Self::decides`]): a key of another root that names the same
    /// epoch is not that version's key, and does not stop its acquisition.
    pub fn holds(&self, address: &SpaceAddress, epoch_id: &str) -> bool {
        self.roots_of(address).iter().any(|space| {
            space
                .depth_of(epoch_id)
                .is_some_and(|depth| space.keys.contains_key(&depth) && self.decides(space, depth))
        })
    }

    /// The chain last read for `address`, if any.
    pub fn known(&self, address: &SpaceAddress) -> Option<&KnownChain> {
        self.known.get(address)
    }

    /// Takes a read of `chain`: a later version, or another root, forgets
    /// the epochs remembered as absent (§4.2).
    pub fn observe_chain(&mut self, chain: &SpaceChain) {
        self.known
            .entry(chain.address().clone())
            .or_default()
            .observe(chain);
    }

    /// Whether a query's result may miss rows of `address`: the chain the
    /// device last read has a version it does not hold (§4.5,
    /// "Completeness").
    pub fn incomplete(&self, address: &SpaceAddress) -> bool {
        self.known.get(address).is_some_and(|known| {
            known
                .epochs
                .iter()
                .any(|epoch_id| !self.holds(address, epoch_id))
        })
    }

    /// Settles a read's missing version `epoch_id` of `address` after the
    /// acquisition it triggered (§4.2, §4.3). `chain` is the space's chain
    /// at the acquisition's read (`None`: no valid root), and `member`
    /// whether the account has a member row for the space.
    ///
    /// - Held now: open again.
    /// - Not in the chain the device knows after taking `chain` (an older
    ///   read never replaces it): `invalid-ciphertext`, remembered until
    ///   the chain advances; an unresolved chain leaves it
    ///   [`MissingKey::Unresolved`].
    /// - Otherwise, not a member: `key-unavailable`; a member with no valid
    ///   root: `space-pending`; a member awaiting a delivery: `key-pending`.
    pub fn settle_missing(
        &mut self,
        address: &SpaceAddress,
        epoch_id: &str,
        chain: Option<&SpaceChain>,
        member: bool,
    ) -> ProtocolResult<MissingKey> {
        if let Some(chain) = chain {
            if chain.address() != address {
                return Err(ProtocolError::InconsistentEvidence);
            }
            self.observe_chain(chain);
        }
        if self.holds(address, epoch_id) {
            return Ok(MissingKey::Held);
        }
        // Decided against the chain the device knows after taking this
        // read: an acquisition whose read is older than it decides nothing
        // about the chain.
        if chain.is_some() {
            let known = self.known.get_mut(address).expect("observed above");
            if !known.epochs.iter().any(|known| known == epoch_id) {
                if let Some(capability) = known.unresolved {
                    return Ok(MissingKey::Unresolved(capability));
                }
                known.absent.insert(epoch_id.to_owned());
                return Ok(MissingKey::Report(DataError::InvalidCiphertext));
            }
        }
        Ok(MissingKey::Report(if !member {
            DataError::KeyUnavailable
        } else if chain.is_none() {
            DataError::SpacePending
        } else {
            DataError::KeyPending
        }))
    }
}
