//! Acquiring and authenticating the account epoch key (`E2EE_PROTOCOL.md`
//! §2.6, §2.7, §2.9 "Restore").
//!
//! A required proof that is provably invalid (malformed, failed
//! authentication, or a marker other than `Z32`) makes the key
//! [`ProtocolError::AccountKeyUnavailable`]; a provably invalid optional
//! proof only drops its approval. Every other failure (a provider failure, an
//! unregistered mechanism) stops the operation.

use std::collections::BTreeSet;

use zeroize::Zeroizing;

use crate::agility::KeyPair;
use crate::error::Error;
use crate::registry::Registry;

use super::super::derive::RecoveryKeys;
use super::super::domain::AccountDomain;
use super::super::error::{ProtocolError, ProtocolResult, RecoveryFailure};
use super::super::json;
use super::super::position::{Accepted, Position};
use super::super::records::account::{
    AccountSuccessor, RecoveryDelivery, approval_context, delivery_marker_context,
    identity_context, recovery_delivery_context, successor_context,
};
use super::private::{PrivateEvidence, VerifiedApproval};
use super::replay::{AccountReplay, AccountState};

const Z32: [u8; 32] = [0; 32];

/// What the account key's authentication established.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuthenticatedAccountKey {
    pub epoch_id: String,
    /// Private approvals of the current epoch whose markers authenticate
    /// under the key and whose signers have authenticated lineage.
    pub approval_ids: BTreeSet<String>,
}

/// An account key with what authenticated it, obtainable only by
/// authentication. The key is cleared on drop and never printed.
pub struct AccountKey {
    key: Zeroizing<[u8; 32]>,
    authenticated: AuthenticatedAccountKey,
}

impl AccountKey {
    pub fn key(&self) -> &[u8; 32] {
        &self.key
    }

    pub fn authenticated(&self) -> &AuthenticatedAccountKey {
        &self.authenticated
    }

    pub fn epoch_id(&self) -> &str {
        &self.authenticated.epoch_id
    }
}

impl core::fmt::Debug for AccountKey {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("AccountKey")
            .field("key", &"..")
            .field("authenticated", &self.authenticated)
            .finish()
    }
}

/// The evidence the account key is checked against: public replay, its
/// current state, and the private records verified against exactly that
/// state. Built only by [`AccountKeyEvidence::new`], which refuses parts
/// from different reads or states.
#[derive(Clone, Copy)]
pub struct AccountKeyEvidence<'a> {
    replay: &'a AccountReplay<'a>,
    state: &'a AccountState,
    private: &'a PrivateEvidence,
}

/// Unwraps `envelope` under `key`; `None` when provably invalid.
fn unwrap(
    registry: &Registry,
    key: &[u8; 32],
    context: &[u8],
    envelope: &[u8],
) -> ProtocolResult<Option<Zeroizing<[u8; 32]>>> {
    let wrapper = match registry.key_wrapper_for(envelope) {
        Ok(wrapper) => wrapper,
        Err(Error::InvalidCiphertext) => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    match wrapper
        .unwrap(key, context, envelope)
        .map_err(Error::after_routing)
    {
        Ok(value) => Ok(Some(value)),
        Err(Error::InvalidCiphertext) => Ok(None),
        Err(error) => Err(error.into()),
    }
}

/// Whether the marker is valid for `key`: it unwraps to exactly `Z32`.
fn marker(
    registry: &Registry,
    key: &[u8; 32],
    context: &[u8],
    envelope: &[u8],
) -> ProtocolResult<bool> {
    Ok(unwrap(registry, key, context, envelope)?.is_some_and(|value| *value == Z32))
}

/// One epoch of the private key chain.
struct KeyEpoch<'e> {
    epoch_id: &'e str,
    position: Position,
    members: &'e BTreeSet<String>,
    successor: Option<&'e AccountSuccessor>,
}

impl<'a> AccountKeyEvidence<'a> {
    /// Binds the three parts. `private` must have been verified against
    /// exactly `state`, and `state` must be the current state of `replay`'s
    /// read; anything else is [`ProtocolError::InconsistentEvidence`].
    pub fn new(
        replay: &'a AccountReplay<'a>,
        state: &'a AccountState,
        private: &'a PrivateEvidence,
    ) -> ProtocolResult<Self> {
        if !private.is_bound_to(state, replay.ledger().through)
            || state.account_id != replay.ledger().account_id
        {
            return Err(ProtocolError::InconsistentEvidence);
        }
        Ok(Self {
            replay,
            state,
            private,
        })
    }

    pub fn state(&self) -> &AccountState {
        self.state
    }
}

impl AccountKeyEvidence<'_> {
    fn registry(&self) -> &Registry {
        self.replay.registry()
    }

    fn application(&self) -> &AccountDomain {
        self.replay.application()
    }

    fn account_id(&self) -> &str {
        &self.state.account_id
    }

    /// The public chain with each successor's authentic private row.
    fn chain(&self) -> ProtocolResult<Vec<KeyEpoch<'_>>> {
        self.state
            .epochs
            .iter()
            .map(|epoch| {
                let successor = match &epoch.successor {
                    None => None,
                    Some(public) => Some(
                        self.private
                            .successors
                            .iter()
                            .find(|private| {
                                private.row.id == public.row.id
                                    && private.position == public.position
                            })
                            .map(|private| &private.row)
                            .ok_or(ProtocolError::AccountKeyUnavailable)?,
                    ),
                };
                Ok(KeyEpoch {
                    epoch_id: &epoch.epoch_id,
                    position: epoch.position,
                    members: &epoch.members,
                    successor,
                })
            })
            .collect()
    }

    /// The required marker of `epoch`.
    fn confirm(&self, epoch: &KeyEpoch<'_>, key: &[u8; 32]) -> ProtocolResult<()> {
        let valid = match epoch.successor {
            Some(successor) => marker(
                self.registry(),
                key,
                &successor_context(
                    self.application(),
                    self.account_id(),
                    &successor.id,
                    &successor.epoch_id,
                    "verification",
                    "",
                )?,
                &successor.verification,
            )?,
            None => {
                let identity = &self
                    .private
                    .identity
                    .as_ref()
                    .ok_or(ProtocolError::AccountKeyUnavailable)?
                    .row;
                marker(
                    self.registry(),
                    key,
                    &identity_context(
                        self.application(),
                        self.account_id(),
                        &identity.id,
                        epoch.epoch_id,
                        "verification",
                        "",
                    )?,
                    &identity.verification,
                )?
            }
        };
        if valid {
            Ok(())
        } else {
            Err(ProtocolError::AccountKeyUnavailable)
        }
    }

    /// The private approvals of `epoch` before `before` that authenticate
    /// under `key` (§2.7): the signer has authenticated lineage before the
    /// batch, or the approval is recovery-authorised, and the approval's
    /// marker (and, if required, the delivery's) is valid.
    fn authenticated(
        &self,
        epoch: &KeyEpoch<'_>,
        key: &[u8; 32],
        require_deliveries: bool,
        before: Option<Position>,
    ) -> ProtocolResult<BTreeSet<String>> {
        let mut accepted = BTreeSet::new();
        let mut signers = epoch.members.clone();
        let mut before_batch = signers.clone();
        let mut batch = None;
        let candidates = self.private.approvals.iter().filter(|candidate| {
            candidate.challenge.epoch_id == epoch.epoch_id
                && candidate.approval.position > epoch.position
                && before.is_none_or(|before| candidate.approval.position < before)
        });
        for candidate in candidates {
            if batch != Some(candidate.approval.position) {
                batch = Some(candidate.approval.position);
                before_batch = signers.clone();
            }
            if !before_batch.contains(&candidate.approval.row.signer_id) && !candidate.recovery {
                continue;
            }
            if self.approval_markers(candidate, key, require_deliveries)? {
                accepted.insert(candidate.approval.row.id.clone());
                signers.insert(candidate.challenge.device_id.clone());
            }
        }
        Ok(accepted)
    }

    fn approval_markers(
        &self,
        candidate: &VerifiedApproval,
        key: &[u8; 32],
        require_delivery: bool,
    ) -> ProtocolResult<bool> {
        let coordinates = candidate.challenge.coordinates();
        let approval = approval_context(
            self.application(),
            self.account_id(),
            coordinates,
            "approval",
        )?;
        if !marker(
            self.registry(),
            key,
            &approval,
            &candidate.approval.row.verification,
        )? {
            return Ok(false);
        }
        if !require_delivery {
            return Ok(true);
        }
        let Some(delivery) = &candidate.delivery else {
            return Ok(false);
        };
        let context = delivery_marker_context(
            self.application(),
            self.account_id(),
            coordinates,
            &delivery.envelope,
        )?;
        marker(self.registry(), key, &context, &delivery.verification)
    }

    /// Authenticates `key` as the current account epoch key: its marker,
    /// then each successor's `history` back to the root, with each
    /// predecessor's marker and revision completeness. With
    /// `delivery_approval`, the key must also have come through that
    /// approval's delivery, authenticated with its delivery marker.
    pub fn authenticate(
        &self,
        key: &[u8; 32],
        delivery_approval: Option<&str>,
    ) -> ProtocolResult<AuthenticatedAccountKey> {
        let chain = self.chain()?;
        let current = chain.last().expect("a state has its root epoch");
        self.confirm(current, key)?;
        let mut later_key = Zeroizing::new(*key);
        for index in (1..chain.len()).rev() {
            let successor = chain[index]
                .successor
                .expect("successor epochs have successors");
            let previous = &chain[index - 1];
            let context = successor_context(
                self.application(),
                self.account_id(),
                &successor.id,
                &successor.epoch_id,
                "history",
                "",
            )?;
            let previous_key = unwrap(self.registry(), &later_key, &context, &successor.history)?
                .ok_or(ProtocolError::AccountKeyUnavailable)?;
            self.confirm(previous, &previous_key)?;
            let authenticated =
                self.authenticated(previous, &previous_key, false, Some(chain[index].position))?;
            let revision: BTreeSet<String> = json::decode_id_list(&successor.revision)
                .map_err(|_| ProtocolError::AccountKeyUnavailable)?
                .into_iter()
                .collect();
            if revision != authenticated {
                return Err(ProtocolError::AccountKeyUnavailable);
            }
            later_key = previous_key;
        }
        let approval_ids = self.authenticated(current, key, false, None)?;
        if let Some(approval_id) = delivery_approval
            && !self
                .authenticated(current, key, true, None)?
                .contains(approval_id)
        {
            return Err(ProtocolError::AccountKeyUnavailable);
        }
        Ok(AuthenticatedAccountKey {
            epoch_id: current.epoch_id.to_owned(),
            approval_ids,
        })
    }

    /// Opens one candidate and authenticates it. `Ok(None)` when the
    /// candidate provably fails; any other failure stops.
    fn try_candidate(
        &self,
        keys: &[KeyPair],
        context: &[u8],
        envelope: &[u8],
        delivery_approval: Option<&str>,
    ) -> ProtocolResult<Option<AccountKey>> {
        let key = match self.registry().open_sealed(keys, context, envelope) {
            Ok(key) => key,
            Err(Error::InvalidCiphertext) => return Ok(None),
            Err(error) => return Err(error.into()),
        };
        match self.authenticate(&key, delivery_approval) {
            Ok(authenticated) => Ok(Some(AccountKey { key, authenticated })),
            Err(ProtocolError::AccountKeyUnavailable) => Ok(None),
            Err(error) => Err(error),
        }
    }

    /// The current account key for `device_id`, which holds `keys` (§2.6):
    /// the successor delivery to it (or the identity envelope for the root
    /// device at the root epoch), then each verified approval delivery to it
    /// for the current epoch.
    pub fn acquire(&self, device_id: &str, keys: &[KeyPair]) -> ProtocolResult<AccountKey> {
        let chain = self.chain()?;
        let current = chain.last().expect("a state has its root epoch");
        let application = self.application();
        let account_id = self.account_id();
        match current.successor {
            Some(successor) => {
                let deliveries = json::decode_delivery_map(&successor.deliveries)
                    .map_err(|_| ProtocolError::AccountKeyUnavailable)?;
                if let Some((_, envelope)) =
                    deliveries.iter().find(|(device, _)| device == device_id)
                {
                    let context = successor_context(
                        application,
                        account_id,
                        &successor.id,
                        &successor.epoch_id,
                        "delivery",
                        device_id,
                    )?;
                    if let Some(key) = self.try_candidate(keys, &context, envelope, None)? {
                        return Ok(key);
                    }
                }
            }
            None => {
                if let Some(identity) = &self.private.identity
                    && identity.row.device_id == device_id
                {
                    let context = identity_context(
                        application,
                        account_id,
                        &identity.row.id,
                        current.epoch_id,
                        "envelope",
                        device_id,
                    )?;
                    if let Some(key) =
                        self.try_candidate(keys, &context, &identity.row.envelope, None)?
                    {
                        return Ok(key);
                    }
                }
            }
        }
        for candidate in &self.private.approvals {
            let (Some(delivery), true) = (
                &candidate.delivery,
                candidate.challenge.device_id == device_id
                    && candidate.challenge.epoch_id == current.epoch_id,
            ) else {
                continue;
            };
            let context = approval_context(
                application,
                account_id,
                candidate.challenge.coordinates(),
                "delivery",
            )?;
            if let Some(key) = self.try_candidate(
                keys,
                &context,
                &delivery.envelope,
                Some(&candidate.approval.row.id),
            )? {
                return Ok(key);
            }
        }
        Err(ProtocolError::AccountKeyUnavailable)
    }

    /// The current account key from a recovery delivery (§2.9, "Restore").
    /// The recovery keys must be those of a valid, unretired root.
    pub fn acquire_with_recovery(
        &self,
        recovery: &RecoveryKeys,
        deliveries: &[Accepted<RecoveryDelivery>],
    ) -> ProtocolResult<AccountKey> {
        let root = self
            .state
            .recovery_root(&recovery.root_id)
            .filter(|root| {
                root.row.keys.sealing_key() == recovery.sealing.public
                    && root.row.keys.signing_key() == recovery.signing.public
            })
            .ok_or(ProtocolError::Recovery(RecoveryFailure::RootMismatch))?;
        let candidates: Vec<_> = deliveries
            .iter()
            .filter(|delivery| {
                delivery.row.root_id == root.row.id
                    && delivery.row.epoch_id == self.state.epoch_id()
                    && delivery.row.validate().is_ok()
            })
            .collect();
        if candidates.is_empty() {
            return Err(ProtocolError::Recovery(RecoveryFailure::DeliveryMissing));
        }
        let keys = core::slice::from_ref(&recovery.sealing);
        for delivery in candidates {
            let context =
                recovery_delivery_context(self.application(), self.account_id(), &delivery.row)?;
            if let Some(key) = self.try_candidate(keys, &context, &delivery.row.envelope, None)? {
                return Ok(key);
            }
        }
        Err(ProtocolError::Recovery(RecoveryFailure::DeliveryUnusable))
    }
}
