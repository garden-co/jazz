//! Acquiring and authenticating space keys (`E2EE_PROTOCOL.md` §3.3
//! "Acquiring", §3.4 "A delivery that does not open").
//!
//! The candidates for a version are the `accountEnvelope` of its root or
//! successor when the device's own account wrote it, then the deliveries of
//! it to the account. Each names an epoch of the account; the device opens
//! it with that epoch's sealing key, derived from the account epoch key it
//! reaches (§2.12). An opened key is accepted only if the version's marker
//! verifies and each `history` link unwraps, back to the root, a key whose
//! marker verifies.
//!
//! A candidate that provably fails to open or authenticate (malformed, or
//! failed authentication, or a marker other than `Z32`) is skipped. A
//! delivery is reported as an invalid delivery only for its own fault: its
//! envelope does not open, or the key it opens fails its own version's
//! marker (§3.4). An envelope that does not open because senders seal to a
//! sealing key record of this account that does not derive (§2.12) is not
//! the sender's fault: it is reported against that record and its signing
//! device instead. A key that opens and passes its own marker but whose
//! history fails further back is not accepted either, and is reported as a
//! broken history naming the successor whose link fails, never against the
//! delivery's sender: the fault is in the chain. A candidate naming an
//! account epoch the device cannot reach (another generation), a delivery
//! at or before the root's position, and a candidate of a mechanism this
//! reader does not route (unregistered or not allowed) are skipped and not
//! reported. A provider failure fails the operation. Once a version is
//! held, no further candidate of it is opened, so later bad deliveries of
//! it are not reported.

use std::collections::BTreeMap;

use zeroize::Zeroizing;

use crate::envelope;
use crate::error::Error;
use crate::registry::Registry;

use super::super::account::{AccountEpochKeys, NonDerivedSealingKey};
use super::super::derive;
use super::super::domain::AccountDomain;
use super::super::error::{ProtocolError, ProtocolResult};
pub(crate) use super::super::key_crypto::Outcome;
use super::super::key_crypto::{self, classify};
use super::super::records::space::{
    SpaceAddress, SpaceDelivery, SpaceRoot, SpaceScope, SpaceSuccessor,
};
use super::chain::SpaceChain;
use super::ledger::{AccountDirectory, SpaceLedger};

/// Keys an authenticated path reaches, with their depths.
type PathKeys = Vec<(usize, Zeroizing<[u8; 32]>)>;

/// Space readers skip envelopes that have no matching key.
fn space_candidate<T>(result: ProtocolResult<Outcome<T>>) -> ProtocolResult<Outcome<T>> {
    match result {
        Err(ProtocolError::Crypto(Error::NoKeyForMechanism)) => Ok(Outcome::Invalid),
        other => other,
    }
}

/// Opens an envelope sealed to an account's sealing key of the epoch whose
/// key is `epoch_key`: the key pair of the envelope's own mechanism is
/// derived from it (§2.12).
pub(crate) fn open_account_envelope(
    registry: &Registry,
    epoch_key: &[u8; 32],
    context: &[u8],
    envelope: &[u8],
) -> ProtocolResult<Outcome<Zeroizing<[u8; 32]>>> {
    let sealer = match space_candidate(classify(registry.key_sealer_for(envelope)))? {
        Outcome::Ok(sealer) => sealer,
        Outcome::Invalid => return Ok(Outcome::Invalid),
        Outcome::Unsupported => return Ok(Outcome::Unsupported),
    };
    let pair = derive::account_sealing_key_pair(sealer, epoch_key)?;
    space_candidate(classify(registry.open_sealed(
        core::slice::from_ref(&pair),
        context,
        envelope,
    )))
}

/// The marker of the version at `depth` under `key`.
fn version_marker(
    chain: &SpaceChain,
    registry: &Registry,
    depth: usize,
    key: &[u8; 32],
) -> ProtocolResult<Outcome<()>> {
    let version = chain
        .version(depth)
        .ok_or(ProtocolError::InvalidRecord("depth beyond the chain"))?;
    marker(
        registry,
        key,
        &chain.application,
        &chain.scope,
        &chain.root.row,
        version.successor.as_ref().map(|successor| &successor.row),
    )
}

/// Checks the marker of the root or the successor that introduced this key.
pub(crate) fn marker(
    registry: &Registry,
    key: &[u8; 32],
    application: &AccountDomain,
    scope: &SpaceScope,
    root: &SpaceRoot,
    successor: Option<&SpaceSuccessor>,
) -> ProtocolResult<Outcome<()>> {
    let (context, envelope) = match successor {
        None => (root.marker_context(application)?, &root.verification),
        Some(successor) => (
            successor.marker_context(application, scope)?,
            &successor.verification,
        ),
    };
    space_candidate(key_crypto::marker(registry, key, &context, envelope))
}

/// What authenticating one key against the chain showed.
enum PathCheck {
    /// Every key the path reaches, by depth.
    Ok(PathKeys),
    /// The envelope does not open.
    Unopened,
    /// The key fails its own version's marker.
    Invalid,
    /// The key passes its own version's marker, but the `history` link of
    /// the successor of the version at this depth, or the marker of the
    /// key it opens, fails: a fault of the chain, not of the candidate.
    BrokenHistory(usize),
    /// A marker or history link is of a mechanism this reader does not
    /// route.
    Unsupported,
}

/// Authenticates `key` as the key of the version at `depth`: its marker,
/// then each `history` link back to the root, each opened key checked by
/// its version's marker.
fn authenticate(
    chain: &SpaceChain,
    registry: &Registry,
    depth: usize,
    key: Zeroizing<[u8; 32]>,
) -> ProtocolResult<PathCheck> {
    match version_marker(chain, registry, depth, &key)? {
        Outcome::Ok(()) => {}
        Outcome::Invalid => return Ok(PathCheck::Invalid),
        Outcome::Unsupported => return Ok(PathCheck::Unsupported),
    }
    let mut keys = vec![(depth, key)];
    let mut at = depth;
    while let Some(previous) = chain.history_depth(at) {
        let successor = &chain.versions[at]
            .successor
            .as_ref()
            .expect("a version with a history link has a successor")
            .row;
        let context = successor.history_context(&chain.application, &chain.scope)?;
        let later = &keys.last().expect("the later key is held").1;
        let opened = match space_candidate(key_crypto::unwrap(
            registry,
            later,
            &context,
            &successor.history,
        ))? {
            Outcome::Ok(opened) => opened,
            Outcome::Invalid => return Ok(PathCheck::BrokenHistory(at)),
            Outcome::Unsupported => return Ok(PathCheck::Unsupported),
        };
        match version_marker(chain, registry, previous, &opened)? {
            Outcome::Ok(()) => {}
            Outcome::Invalid => return Ok(PathCheck::BrokenHistory(at)),
            Outcome::Unsupported => return Ok(PathCheck::Unsupported),
        }
        keys.push((previous, opened));
        at = previous;
    }
    Ok(PathCheck::Ok(keys))
}

/// A delivery to this account whose envelope provably failed to open, or
/// whose opened key failed its own version's marker (§3.4). Each of the
/// recipient's devices emits one `invalid-delivery` encryption event
/// `{space, sender, recipient, keyEpochId}` per delivery; nothing is
/// written. A key that fails only a history link further back is a
/// [`BrokenHistory`], never an invalid delivery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvalidDelivery {
    pub delivery_id: String,
    pub sender_account_id: String,
    pub recipient_account_id: String,
    pub key_epoch_id: String,
}

/// A delivery to this account whose envelope does not open because it was
/// sealed to the sealing key record senders select for the recipient epoch
/// and the envelope's mechanism, and that record does not derive from the
/// account's epoch key (§2.12, §3.4). It is reported against the record and
/// the device that signed it (`conflicting-account-key`), never as an
/// invalid delivery against the sender, who cannot check derivation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NonDerivedDelivery {
    pub delivery_id: String,
    pub key_epoch_id: String,
    /// The record the delivery was sealed to, with its signing device.
    pub sealing_key: NonDerivedSealingKey,
}

/// A key of the space that opened and passed its own version's marker, but
/// whose `history` fails further back (§3.3, §3.4): the `history` link of
/// the successor named here does not unwrap, or unwraps a key that fails
/// its version's marker. The key is not accepted. The fault is in that
/// successor, so it is reported against the successor's author, not
/// against whoever delivered the key; the runtime emits it as an event.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrokenHistory {
    pub space: SpaceAddress,
    /// The version the failing successor opened (its `epochId`).
    pub key_epoch_id: String,
    pub successor_id: String,
    pub author_account_id: String,
}

/// Authenticated keys of one space, by depth in its chain. Built only by
/// [`acquire`] or [`SpaceKeys::authenticate`], so every key passed its
/// version's marker and the `history` links to the root. Keys are cleared
/// on drop and never printed.
pub struct SpaceKeys {
    application: AccountDomain,
    scope: SpaceScope,
    /// The chain's epochs when the keys were authenticated, root first.
    epochs: Vec<String>,
    keys: BTreeMap<usize, Zeroizing<[u8; 32]>>,
}

impl core::fmt::Debug for SpaceKeys {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("SpaceKeys")
            .field("scope", &self.scope)
            .field("held", &self.keys.keys().collect::<Vec<_>>())
            .finish()
    }
}

/// One held version of a space: what a delivery of it is built from
/// (§3.4). It can only be taken from the [`SpaceKeys`] of that space's own
/// chain, so a delivery never names a version of another space.
pub struct HeldVersion<'k> {
    pub(crate) application: &'k AccountDomain,
    pub(crate) scope: &'k SpaceScope,
    pub(crate) epoch_id: &'k str,
    pub(crate) depth: usize,
    pub(crate) key: &'k [u8; 32],
}

impl HeldVersion<'_> {
    pub fn epoch_id(&self) -> &str {
        self.epoch_id
    }

    pub fn depth(&self) -> usize {
        self.depth
    }

    pub fn scope(&self) -> &SpaceScope {
        self.scope
    }
}

impl core::fmt::Debug for HeldVersion<'_> {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("HeldVersion")
            .field("scope", self.scope)
            .field("epoch_id", &self.epoch_id)
            .field("depth", &self.depth)
            .finish()
    }
}

/// The outcome of checking a key held from elsewhere against the chain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum KeyCheck {
    /// The key authenticated; it and every key its history links reach are
    /// now held.
    Accepted,
    /// The epoch is not a version of this chain, or the key provably fails
    /// its own version's marker.
    Invalid,
    /// The key passes its own version's marker, but a `history` link
    /// further back fails (§3.3, §3.4): as in
    /// [`Acquisition::broken_history`], the fault is the named successor's,
    /// and the key is not accepted.
    BrokenHistory(BrokenHistory),
    /// A marker or history link is of a mechanism this reader does not
    /// route.
    Unsupported,
}

impl SpaceKeys {
    /// No keys yet, for `chain`.
    pub fn new(chain: &SpaceChain) -> Self {
        Self {
            application: chain.application.clone(),
            scope: chain.scope.clone(),
            epochs: chain
                .versions
                .iter()
                .map(|version| version.epoch_id.clone())
                .collect(),
            keys: BTreeMap::new(),
        }
    }

    pub fn scope(&self) -> &SpaceScope {
        &self.scope
    }

    /// Fails unless `chain` is the chain these keys were authenticated
    /// against, or a later read of it: same root, same versions so far.
    pub(crate) fn check_chain(&self, chain: &SpaceChain) -> ProtocolResult<()> {
        let same = chain.scope == self.scope
            && chain.application == self.application
            && chain.versions.len() >= self.epochs.len()
            && chain
                .versions
                .iter()
                .zip(&self.epochs)
                .all(|(version, epoch)| version.epoch_id == *epoch);
        if same {
            Ok(())
        } else {
            Err(ProtocolError::InconsistentEvidence)
        }
    }

    /// The depths whose keys are held, ascending.
    pub fn depths(&self) -> impl Iterator<Item = usize> + '_ {
        self.keys.keys().copied()
    }

    /// The held epochs, root side first.
    pub fn epoch_ids(&self) -> impl Iterator<Item = &str> {
        self.keys.keys().map(|depth| self.epochs[*depth].as_str())
    }

    pub fn holds(&self, epoch_id: &str) -> bool {
        self.version(epoch_id).is_some()
    }

    /// The newest held version: the one a write uses (§5.2).
    pub fn newest(&self) -> Option<HeldVersion<'_>> {
        let depth = *self.keys.keys().next_back()?;
        self.at(depth)
    }

    /// The held version `epoch_id` of this space.
    pub fn version(&self, epoch_id: &str) -> Option<HeldVersion<'_>> {
        let depth = self.epochs.iter().position(|epoch| epoch == epoch_id)?;
        self.at(depth)
    }

    fn at(&self, depth: usize) -> Option<HeldVersion<'_>> {
        Some(HeldVersion {
            application: &self.application,
            scope: &self.scope,
            epoch_id: &self.epochs[depth],
            depth,
            key: self.keys.get(&depth)?,
        })
    }

    /// The key of `epoch_id`, if held.
    pub fn key(&self, epoch_id: &str) -> Option<&[u8; 32]> {
        self.version(epoch_id).map(|version| version.key)
    }

    /// Authenticates a key of `epoch_id` held from elsewhere (a held-key
    /// record, §6.2, or a creation's own key) against `chain`, exactly as an
    /// acquired key: its version's marker and each `history` link back to
    /// the root. On success it and every key the links reach are held. A
    /// key that passes its own marker but whose history fails further back
    /// is [`KeyCheck::BrokenHistory`], naming the failing successor.
    pub fn authenticate(
        &mut self,
        chain: &SpaceChain,
        registry: &Registry,
        epoch_id: &str,
        key: &[u8; 32],
    ) -> ProtocolResult<KeyCheck> {
        self.check_chain(chain)?;
        let Some(depth) = chain.depth_of(epoch_id) else {
            return Ok(KeyCheck::Invalid);
        };
        self.extend(chain);
        Ok(
            match authenticate(chain, registry, depth, Zeroizing::new(*key))? {
                PathCheck::Ok(keys) => {
                    self.insert(keys);
                    KeyCheck::Accepted
                }
                PathCheck::Unopened | PathCheck::Invalid => KeyCheck::Invalid,
                PathCheck::BrokenHistory(at) => KeyCheck::BrokenHistory(broken_history(chain, at)),
                PathCheck::Unsupported => KeyCheck::Unsupported,
            },
        )
    }

    /// Takes the later versions of a later read of the same chain.
    fn extend(&mut self, chain: &SpaceChain) {
        for version in &chain.versions[self.epochs.len()..] {
            self.epochs.push(version.epoch_id.clone());
        }
    }

    fn insert(&mut self, keys: PathKeys) {
        for (depth, key) in keys {
            self.keys.entry(depth).or_insert(key);
        }
    }
}

/// What one acquisition (§3.3) obtained and what it found.
#[derive(Debug)]
pub struct Acquisition {
    /// Every key accepted, including earlier versions the history links
    /// reach: all become held keys (§4.2).
    pub keys: SpaceKeys,
    /// Deliveries to this account that provably failed for their own
    /// fault (§3.4), by version (current first), then position and ID.
    pub invalid_deliveries: Vec<InvalidDelivery>,
    /// Deliveries to this account that did not open because they were
    /// sealed to a sealing key record of this account that does not derive
    /// (§2.12), in the same order: reported against the record, not the
    /// sender.
    pub non_derived_deliveries: Vec<NonDerivedDelivery>,
    /// Successors whose `history` link failed under a key that opened and
    /// passed its own marker, once each, in the order found.
    pub broken_history: Vec<BrokenHistory>,
    /// Row IDs of candidates (roots, successors or deliveries) skipped
    /// because an envelope, marker or history link is of a mechanism this
    /// reader does not route. They are not reported as invalid deliveries.
    pub unsupported: Vec<String>,
}

impl Acquisition {
    /// Whether the current version's key was obtained. When it was not,
    /// the device waits for a delivery (§3.4) and a member reports the space
    /// `key-pending` (§4.3). An unresolved chain has no current version.
    pub fn has_current(&self, chain: &SpaceChain) -> bool {
        chain
            .current()
            .is_ok_and(|current| self.keys.holds(&current.epoch_id))
    }
}

/// Acquires every key of `chain` this device can obtain (§3.3): the current
/// version first, then earlier ones, for versions the history links of a
/// newer key do not reach. `own` are the account epoch keys of the device's
/// own account, current generation (§2.12); `accounts` is the account
/// directory of the same read as `ledger`, from which the account's sealing
/// key records are checked against `own` afresh, so a delivery sealed to a
/// record that does not derive is attributed to that record (§2.12, §3.4)
/// whenever the record was published; `ledger` holds the deliveries
/// addressed to that account and must be the read `chain` was replayed
/// from, either of the whole space or filtered for this account (a read
/// filtered for another account is
/// [`ProtocolError::InconsistentEvidence`]). Over an unresolved chain, only
/// its resolved versions are acquired.
///
/// Once a version's key is held, no further candidate of it is opened: the
/// first candidate that authenticates ends the search for that version,
/// and later deliveries of it, good or bad, are neither opened nor
/// reported.
pub fn acquire(
    chain: &SpaceChain,
    ledger: &SpaceLedger,
    accounts: &AccountDirectory<'_>,
    own: &AccountEpochKeys,
) -> ProtocolResult<Acquisition> {
    chain.check_read(ledger, Some(accounts))?;
    let registry = accounts.registry();
    let account_id = own.account_id();
    // Derivation is checked against this read's records, never taken from
    // a report made at another read.
    let non_derived = match accounts.sealing_keys(account_id)? {
        Some(sealing) => sealing.check_derived(registry, own)?,
        None => return Err(ProtocolError::InconsistentEvidence),
    };
    if ledger
        .filtered_for()
        .is_some_and(|filtered| filtered != account_id)
    {
        return Err(ProtocolError::InconsistentEvidence);
    }
    let root_position = chain.root.position;
    let mut keys = SpaceKeys::new(chain);
    let mut invalid_deliveries = Vec::new();
    let mut non_derived_deliveries = Vec::new();
    let mut broken_history = Vec::new();
    let mut unsupported = Vec::new();
    let mut broken = |depth: usize| {
        let report = self::broken_history(chain, depth);
        if !broken_history.contains(&report) {
            broken_history.push(report);
        }
    };
    for depth in (0..chain.versions.len()).rev() {
        if keys.keys.contains_key(&depth) {
            continue;
        }
        let version = &chain.versions[depth];
        // The account envelope of the version's own record, when this
        // account wrote it.
        let (writer, writer_epoch) = version.writer(&chain.root.row);
        if writer == account_id
            && let Some(epoch_key) = own.get(writer_epoch)
        {
            let (context, envelope) = version.account_envelope(chain)?;
            match try_candidate(chain, registry, depth, epoch_key, &context, envelope)? {
                PathCheck::Ok(found) => {
                    keys.insert(found);
                    continue;
                }
                PathCheck::Unopened | PathCheck::Invalid => {}
                PathCheck::BrokenHistory(at) => broken(at),
                PathCheck::Unsupported => {
                    unsupported.push(version.record_id(&chain.root.row).to_owned())
                }
            }
        }
        for delivery in ledger.deliveries_of(&version.epoch_id) {
            let row = &delivery.row;
            if row.recipient_account_id != account_id || delivery.position <= root_position {
                continue;
            }
            let Some(epoch_key) = own.get(&row.recipient_account_epoch_id) else {
                continue;
            };
            let outcome = match row.seal_context(&chain.application, &chain.scope) {
                Ok(context) if row.validate(&chain.scope).is_ok() => {
                    try_candidate(chain, registry, depth, epoch_key, &context, &row.envelope)?
                }
                _ => PathCheck::Invalid,
            };
            match outcome {
                PathCheck::Ok(found) => {
                    keys.insert(found);
                    break;
                }
                // The delivery is not at fault: its key passed its own
                // version's marker.
                PathCheck::BrokenHistory(at) => broken(at),
                PathCheck::Unopened | PathCheck::Invalid => {
                    let non_derived = matches!(outcome, PathCheck::Unopened)
                        .then(|| sealed_to_non_derived(&non_derived, row))
                        .flatten();
                    match non_derived {
                        Some(record) => non_derived_deliveries.push(NonDerivedDelivery {
                            delivery_id: row.id.clone(),
                            key_epoch_id: row.key_epoch_id.clone(),
                            sealing_key: record.clone(),
                        }),
                        None => invalid_deliveries.push(InvalidDelivery {
                            delivery_id: row.id.clone(),
                            sender_account_id: row.sender_account_id.clone(),
                            recipient_account_id: row.recipient_account_id.clone(),
                            key_epoch_id: row.key_epoch_id.clone(),
                        }),
                    }
                }
                PathCheck::Unsupported => unsupported.push(row.id.clone()),
            }
        }
    }
    Ok(Acquisition {
        keys,
        invalid_deliveries,
        non_derived_deliveries,
        broken_history,
        unsupported,
    })
}

/// The report of a broken `history` link of the successor of the version
/// at `depth`.
fn broken_history(chain: &SpaceChain, depth: usize) -> BrokenHistory {
    let successor = &chain.versions[depth]
        .successor
        .as_ref()
        .expect("a version with a history link has a successor")
        .row;
    BrokenHistory {
        space: chain.address().clone(),
        key_epoch_id: successor.epoch_id.clone(),
        successor_id: successor.id.clone(),
        author_account_id: successor.author_account_id.clone(),
    }
}

/// The non-derived sealing key record of this account that senders select
/// for the delivery's recipient epoch and its envelope's mechanism, if any
/// (§2.12): a delivery sealed to it cannot open, through no fault of its
/// sender.
fn sealed_to_non_derived<'k>(
    non_derived: &'k [NonDerivedSealingKey],
    row: &SpaceDelivery,
) -> Option<&'k NonDerivedSealingKey> {
    let (mechanism, _) = envelope::parse(&row.envelope).ok()?;
    non_derived
        .iter()
        .find(|record| record.is_selected_for(&row.recipient_account_epoch_id, mechanism))
}

/// Opens one candidate for the version at `depth` and authenticates it.
fn try_candidate(
    chain: &SpaceChain,
    registry: &Registry,
    depth: usize,
    epoch_key: &[u8; 32],
    context: &[u8],
    envelope: &[u8],
) -> ProtocolResult<PathCheck> {
    match open_account_envelope(registry, epoch_key, context, envelope)? {
        Outcome::Ok(key) => authenticate(chain, registry, depth, key),
        Outcome::Invalid => Ok(PathCheck::Unopened),
        Outcome::Unsupported => Ok(PathCheck::Unsupported),
    }
}
