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
//! failed authentication, or a marker other than `Z32`) is skipped; a
//! delivery that does is reported as an invalid delivery. A candidate
//! naming an account epoch the device cannot reach (another generation),
//! a delivery at or before the root's position, and a candidate of a
//! mechanism this reader does not route (unregistered or not allowed) are
//! skipped and not reported. A provider failure fails the operation.

use std::collections::BTreeMap;

use zeroize::Zeroizing;

use crate::error::Error;
use crate::registry::Registry;

use super::super::account::AccountEpochKeys;
use super::super::derive;
use super::super::domain::AccountDomain;
use super::super::error::{ProtocolError, ProtocolResult};
use super::super::records::space::SpaceScope;
use super::chain::SpaceChain;
use super::ledger::SpaceLedger;

const Z32: [u8; 32] = [0; 32];

/// Keys an authenticated path reaches, with their depths.
type PathKeys = Vec<(usize, Zeroizing<[u8; 32]>)>;

/// What opening, unwrapping or checking one candidate showed.
pub(crate) enum Outcome<T> {
    Ok(T),
    /// Provably malformed or unauthentic.
    Invalid,
    /// An envelope of a mechanism this reader does not route: unsupported,
    /// never invalid.
    Unsupported,
}

/// Maps a reader error: authentication failures are [`Outcome::Invalid`],
/// an unrouted mechanism [`Outcome::Unsupported`]; anything else (a
/// provider failure included) fails the operation.
fn classify<T>(result: crate::Result<T>) -> ProtocolResult<Outcome<T>> {
    match result {
        Ok(value) => Ok(Outcome::Ok(value)),
        Err(Error::InvalidCiphertext | Error::NoKeyForMechanism) => Ok(Outcome::Invalid),
        Err(Error::UnsupportedFormat) => Ok(Outcome::Unsupported),
        Err(error) => Err(error.into()),
    }
}

/// Unwraps `envelope` under `key`.
pub(crate) fn unwrap(
    registry: &Registry,
    key: &[u8; 32],
    context: &[u8],
    envelope: &[u8],
) -> ProtocolResult<Outcome<Zeroizing<[u8; 32]>>> {
    let wrapper = match classify(registry.key_wrapper_for(envelope))? {
        Outcome::Ok(wrapper) => wrapper,
        Outcome::Invalid => return Ok(Outcome::Invalid),
        Outcome::Unsupported => return Ok(Outcome::Unsupported),
    };
    classify(
        wrapper
            .unwrap(key, context, envelope)
            .map_err(Error::after_routing),
    )
}

/// Whether the marker is valid for `key`: it unwraps to exactly `Z32`.
pub(crate) fn marker(
    registry: &Registry,
    key: &[u8; 32],
    context: &[u8],
    envelope: &[u8],
) -> ProtocolResult<Outcome<()>> {
    Ok(match unwrap(registry, key, context, envelope)? {
        Outcome::Ok(value) if *value == Z32 => Outcome::Ok(()),
        Outcome::Ok(_) | Outcome::Invalid => Outcome::Invalid,
        Outcome::Unsupported => Outcome::Unsupported,
    })
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
    let sealer = match classify(registry.key_sealer_for(envelope))? {
        Outcome::Ok(sealer) => sealer,
        Outcome::Invalid => return Ok(Outcome::Invalid),
        Outcome::Unsupported => return Ok(Outcome::Unsupported),
    };
    let pair = derive::account_sealing_key_pair(sealer, epoch_key)?;
    classify(registry.open_sealed(core::slice::from_ref(&pair), context, envelope))
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
    let (context, envelope) = match &version.successor {
        None => (
            chain.root.row.marker_context(&chain.application)?,
            &chain.root.row.verification,
        ),
        Some(successor) => (
            successor
                .row
                .marker_context(&chain.application, &chain.scope)?,
            &successor.row.verification,
        ),
    };
    marker(registry, key, &context, envelope)
}

/// Authenticates `key` as the key of the version at `depth`: its marker,
/// then each `history` link back to the root, each opened key checked by
/// its version's marker. Returns every key the path reaches, by depth.
fn authenticate(
    chain: &SpaceChain,
    registry: &Registry,
    depth: usize,
    key: Zeroizing<[u8; 32]>,
) -> ProtocolResult<Outcome<PathKeys>> {
    match version_marker(chain, registry, depth, &key)? {
        Outcome::Ok(()) => {}
        Outcome::Invalid => return Ok(Outcome::Invalid),
        Outcome::Unsupported => return Ok(Outcome::Unsupported),
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
        let opened = match unwrap(registry, later, &context, &successor.history)? {
            Outcome::Ok(opened) => opened,
            Outcome::Invalid => return Ok(Outcome::Invalid),
            Outcome::Unsupported => return Ok(Outcome::Unsupported),
        };
        match version_marker(chain, registry, previous, &opened)? {
            Outcome::Ok(()) => {}
            Outcome::Invalid => return Ok(Outcome::Invalid),
            Outcome::Unsupported => return Ok(Outcome::Unsupported),
        }
        keys.push((previous, opened));
        at = previous;
    }
    Ok(Outcome::Ok(keys))
}

/// A delivery to this account that provably failed to open or
/// authenticate (§3.4). Each of the recipient's devices emits one
/// `invalid-delivery` encryption event `{space, sender, recipient,
/// keyEpochId}` per delivery; nothing is written.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InvalidDelivery {
    pub delivery_id: String,
    pub sender_account_id: String,
    pub recipient_account_id: String,
    pub key_epoch_id: String,
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
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum KeyCheck {
    /// The key authenticated; it and every key its history links reach are
    /// now held.
    Accepted,
    /// The epoch is not a version of this chain, or the key provably fails
    /// its marker or a history link.
    Invalid,
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
    /// the root. On success it and every key the links reach are held.
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
                Outcome::Ok(keys) => {
                    self.insert(keys);
                    KeyCheck::Accepted
                }
                Outcome::Invalid => KeyCheck::Invalid,
                Outcome::Unsupported => KeyCheck::Unsupported,
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
    /// Deliveries to this account that provably failed (§3.4), by
    /// position then ID.
    pub invalid_deliveries: Vec<InvalidDelivery>,
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
/// own account, current generation (§2.12); `ledger` holds the deliveries
/// addressed to that account. Over an unresolved chain, only its resolved
/// versions are acquired.
pub fn acquire(
    chain: &SpaceChain,
    ledger: &SpaceLedger,
    own: &AccountEpochKeys,
    registry: &Registry,
) -> ProtocolResult<Acquisition> {
    if *chain.address() != ledger.address {
        return Err(ProtocolError::InconsistentEvidence);
    }
    ledger.check()?;
    let account_id = own.account_id();
    let root_position = chain.root.position;
    let mut keys = SpaceKeys::new(chain);
    let mut invalid_deliveries = Vec::new();
    let mut unsupported = Vec::new();
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
            let (record_id, context, envelope) = match &version.successor {
                None => (
                    &chain.root.row.id,
                    chain
                        .root
                        .row
                        .account_envelope_context(&chain.application)?,
                    &chain.root.row.account_envelope,
                ),
                Some(successor) => (
                    &successor.row.id,
                    successor
                        .row
                        .account_envelope_context(&chain.application, &chain.scope)?,
                    &successor.row.account_envelope,
                ),
            };
            match try_candidate(chain, registry, depth, epoch_key, &context, envelope)? {
                Outcome::Ok(found) => {
                    keys.insert(found);
                    continue;
                }
                Outcome::Invalid => {}
                Outcome::Unsupported => unsupported.push(record_id.clone()),
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
                _ => Outcome::Invalid,
            };
            match outcome {
                Outcome::Ok(found) => {
                    keys.insert(found);
                    break;
                }
                Outcome::Invalid => invalid_deliveries.push(InvalidDelivery {
                    delivery_id: row.id.clone(),
                    sender_account_id: row.sender_account_id.clone(),
                    recipient_account_id: row.recipient_account_id.clone(),
                    key_epoch_id: row.key_epoch_id.clone(),
                }),
                Outcome::Unsupported => unsupported.push(row.id.clone()),
            }
        }
    }
    Ok(Acquisition {
        keys,
        invalid_deliveries,
        unsupported,
    })
}

/// Opens one candidate for the version at `depth` and authenticates it.
fn try_candidate(
    chain: &SpaceChain,
    registry: &Registry,
    depth: usize,
    epoch_key: &[u8; 32],
    context: &[u8],
    envelope: &[u8],
) -> ProtocolResult<Outcome<PathKeys>> {
    match open_account_envelope(registry, epoch_key, context, envelope)? {
        Outcome::Ok(key) => authenticate(chain, registry, depth, key),
        Outcome::Invalid => Ok(Outcome::Invalid),
        Outcome::Unsupported => Ok(Outcome::Unsupported),
    }
}
