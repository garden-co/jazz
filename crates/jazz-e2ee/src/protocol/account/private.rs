//! Private verification (`E2EE_PROTOCOL.md` §2.4, "Private verification").
//!
//! A device that wants the account key checks the account's private rows
//! against public replay at the same cut. The result describes which private
//! records are authentic; it never changes public membership. Markers that
//! need the account key are checked by [`super::key`].

use std::collections::BTreeMap;

use super::super::error::{ProtocolError, ProtocolResult};
use super::super::json;
use super::super::position::{Accepted, Position, sort_by_position_then_id};
use super::super::records::account::{
    AccountIdentity, AccountSuccessor, DeviceApproval, DeviceChallenge, DeviceDelivery,
    DeviceProof, DeviceRequest, approval_signed_bytes, challenge_signed_bytes, nonce_commitment,
    proof_signed_bytes,
};
use super::super::signing;
use super::ledger::{PrivateAccountLedger, PublicAccountLedger};
use super::replay::{AccountReplay, AccountState};

/// A private approval whose challenge, proof, request keys and signatures
/// verify, and whose public approval replay accepted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct VerifiedApproval {
    pub approval: Accepted<DeviceApproval>,
    pub challenge: DeviceChallenge,
    pub proof: DeviceProof,
    /// The delivery of the account key to the approved device, if any.
    pub delivery: Option<DeviceDelivery>,
    /// Whether the matching public approval is a recovery approval.
    pub recovery: bool,
}

/// The authentic private records of an account at one cut, bound to the
/// exact current state they were verified against. Only
/// [`verify_private`] builds it, and nothing changes it afterwards, so the
/// binding cannot be broken by editing the records.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrivateEvidence {
    state: AccountState,
    through: Position,
    identity: Option<Accepted<AccountIdentity>>,
    approvals: Vec<VerifiedApproval>,
    successors: Vec<Accepted<AccountSuccessor>>,
}

fn index<T: Clone + super::super::position::RowId>(
    rows: &[Accepted<T>],
) -> BTreeMap<&str, &Accepted<T>> {
    rows.iter().map(|row| (row.row.row_id(), row)).collect()
}

struct Requests<'a> {
    requests: BTreeMap<&'a str, &'a Accepted<DeviceRequest>>,
    public: &'a PublicAccountLedger,
}

impl Requests<'_> {
    /// The device's request, if it and every key projection of the device
    /// accepted at or before `position` exist and match (at least one).
    fn matching(&self, device_id: &str, position: Position) -> Option<&DeviceRequest> {
        let request = self.requests.get(device_id)?;
        if request.position > position || request.row.validate().is_err() {
            return None;
        }
        let mut projections = self
            .public
            .keys
            .iter()
            .filter(|key| key.row.device_id == device_id && key.position <= position)
            .peekable();
        projections.peek()?;
        projections
            .all(|key| key.row.matches(&request.row))
            .then_some(&request.row)
    }
}

impl PrivateEvidence {
    /// The identity of the canonical root, if it matches it.
    pub fn identity(&self) -> Option<&Accepted<AccountIdentity>> {
        self.identity.as_ref()
    }

    /// The verified private approvals, by position then ID.
    pub fn approvals(&self) -> &[VerifiedApproval] {
        &self.approvals
    }

    /// Private successors matching the accepted public ones.
    pub fn successors(&self) -> &[Accepted<AccountSuccessor>] {
        &self.successors
    }

    /// The cut the evidence was read at.
    pub fn through(&self) -> Position {
        self.through
    }

    /// Whether this evidence was verified against exactly `state`, the
    /// current state of a read at `through`. A state as of a position, or
    /// of another read, is refused.
    pub fn is_bound_to(&self, state: &AccountState, through: Position) -> bool {
        self.through == through && self.state == *state
    }
}

/// Verifies `private` against `state`, which must be exactly the current
/// state of `replay` (not a state as of a position). Unsupported signing
/// mechanisms and provider failures stop verification; anything else that
/// fails only leaves the record out.
pub fn verify_private(
    replay: &AccountReplay<'_>,
    state: &AccountState,
    private: &PrivateAccountLedger,
) -> ProtocolResult<PrivateEvidence> {
    private.check()?;
    if private.through != replay.ledger().through
        || state.account_id != replay.ledger().account_id
        || replay.current()?.as_ref() != Some(state)
    {
        return Err(ProtocolError::InconsistentEvidence);
    }
    let registry = replay.registry();
    let application = replay.application();
    let account_id = &state.account_id;
    let requests = Requests {
        requests: index(&private.requests),
        public: replay.ledger(),
    };
    let challenges = index(&private.challenges);
    let proofs = index(&private.proofs);
    let deliveries = index(&private.deliveries);

    // The identity of the canonical root: same generation, device, epoch
    // and ledger version, accepted at the root's position, with the account
    // ID as its ID at generation 0 (a reset identity's ID is random).
    let identity = private
        .identities
        .iter()
        .find(|identity| {
            identity.position == state.root.position
                && state.root.row.matches_identity(&identity.row)
                && identity.row.validate(account_id).is_ok()
                && requests
                    .matching(&identity.row.device_id, identity.position)
                    .is_some()
        })
        .cloned();
    // An identity the account wrote for the canonical generation names
    // another device or epoch than the canonical root: the public root is
    // not the one the account wrote (§2.4, `root-not-mine`). R5 and R6
    // admit an identity only with its root, in one transaction.
    if identity.is_none()
        && private.identities.iter().any(|identity| {
            identity.row.generation == state.generation
                && identity.row.validate(account_id).is_ok()
                && !state.root.row.matches_identity(&identity.row)
        })
    {
        return Err(ProtocolError::RootNotMine);
    }

    let mut approvals = Vec::new();
    for approval in &private.approvals {
        let position = approval.position;
        let Some(challenge) = challenges.get(approval.row.challenge_id.as_str()) else {
            continue;
        };
        let Some(proof) = proofs.get(challenge.row.id.as_str()) else {
            continue;
        };
        if approval.row.validate().is_err()
            || challenge.row.validate().is_err()
            || proof.row.validate().is_err()
            || challenge.position > position
            || proof.position > position
        {
            continue;
        }
        let coordinates = challenge.row.coordinates();
        let Some(public) = state.approvals.iter().find(|public| {
            public.row.id == approval.row.id
                && public.position == position
                && public.row.account_id == *account_id
                && public.row.epoch_id == challenge.row.epoch_id
                && public.row.signer_id == approval.row.signer_id
                && public.row.device_id == challenge.row.device_id
        }) else {
            continue;
        };
        let (Some(signer), Some(recipient)) = (
            requests.matching(&approval.row.signer_id, position),
            requests.matching(&challenge.row.device_id, position),
        ) else {
            continue;
        };
        // The approver signed the challenge over the requester keys it was
        // shown (§2.5); they must be the requester's. It is verified under
        // the approval signer's key, and the signer's ID is in the signed
        // column, so a challenge another device signed does not verify.
        // The proof's nonce opens the requester's commitment (§2.5).
        if !nonce_commitment(&recipient.id, &proof.row.nonce)
            .is_ok_and(|commitment| commitment[..] == recipient.nonce_commitment[..])
        {
            continue;
        }
        let Ok(challenge_bytes) = challenge_signed_bytes(
            application,
            account_id,
            coordinates,
            &challenge.row.signer_id,
            recipient,
            &challenge.row.envelope,
            &challenge.row.nonce,
        ) else {
            continue;
        };
        if !signing::verify(
            registry,
            &signer.keys.signing_key(),
            &challenge_bytes,
            &challenge.row.signature,
        )? {
            continue;
        }
        let (Ok(proof_bytes), Ok(approval_bytes)) = (
            proof_signed_bytes(
                application,
                account_id,
                coordinates,
                &proof.row.proof,
                &proof.row.nonce,
            ),
            approval_signed_bytes(
                application,
                account_id,
                coordinates,
                &approval.row.signer_id,
                &approval.row.verification,
            ),
        ) else {
            continue;
        };
        if !signing::verify(
            registry,
            &recipient.keys.signing_key(),
            &proof_bytes,
            &proof.row.signature,
        )? || !signing::verify(
            registry,
            &signer.keys.signing_key(),
            &approval_bytes,
            &approval.row.signature,
        )? {
            continue;
        }
        let delivery = deliveries
            .get(approval.row.id.as_str())
            .filter(|delivery| {
                delivery.row.challenge_id == challenge.row.id && delivery.row.validate().is_ok()
            })
            .map(|delivery| delivery.row.clone());
        approvals.push(VerifiedApproval {
            approval: approval.clone(),
            challenge: challenge.row.clone(),
            proof: proof.row.clone(),
            delivery,
            recovery: public.row.is_recovery(),
        });
    }
    sort_by_position_then_id(&mut approvals, |row| row.approval.position_and_id());

    let mut successors = Vec::new();
    for successor in &private.successors {
        let position = successor.position;
        let Some(public) = state
            .epochs
            .iter()
            .filter_map(|epoch| epoch.successor.as_ref())
            .find(|public| public.row.columns.id == successor.row.columns.id)
        else {
            continue;
        };
        if public.position != position || !successor.row.matches_public(&public.row) {
            continue;
        }
        let Some(signer) = requests.matching(&successor.row.columns.signer_id, position) else {
            continue;
        };
        if successor.row.validate().is_err() {
            continue;
        }
        let Ok(bytes) = successor.row.signed_bytes(application) else {
            continue;
        };
        let Ok(revision) = json::decode_id_list(&successor.row.revision) else {
            continue;
        };
        let complete = revision.iter().all(|id| {
            approvals.iter().any(|verified| {
                verified.approval.row.id == *id
                    && verified.challenge.epoch_id == successor.row.columns.predecessor
                    && verified.approval.position < position
            })
        });
        if !complete
            || !signing::verify(
                registry,
                &signer.keys.signing_key(),
                &bytes,
                &successor.row.signature,
            )?
        {
            continue;
        }
        successors.push(successor.clone());
    }

    Ok(PrivateEvidence {
        state: state.clone(),
        through: private.through,
        identity,
        approvals,
        successors,
    })
}
