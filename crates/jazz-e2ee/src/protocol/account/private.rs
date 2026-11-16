//! Private verification (`E2EE_PROTOCOL.md` §2.4, "Private verification").
//!
//! A device that wants the account key checks the account's private rows
//! against public replay at the same cut. The result describes which private
//! records are authentic; it never changes public membership. Markers that
//! need the account key are checked by [`super::key`].

use std::collections::BTreeMap;

use super::super::error::{ProtocolError, ProtocolResult};
use super::super::ids;
use super::super::json;
use super::super::position::{Accepted, Position};
use super::super::records::account::{
    AccountIdentity, AccountSuccessor, DeviceApproval, DeviceChallenge, DeviceDelivery,
    DeviceProof, DeviceRequest, approval_signed_bytes, proof_signed_bytes,
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

/// The authentic private records of an account at one cut.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PrivateEvidence {
    /// The identity of the canonical root, if it matches it.
    pub identity: Option<Accepted<AccountIdentity>>,
    pub approvals: Vec<VerifiedApproval>,
    /// Private successors matching the accepted public ones.
    pub successors: Vec<Accepted<AccountSuccessor>>,
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

/// Verifies `private` against `state`, the current state replayed from the
/// same cut. Unsupported signing mechanisms and provider failures stop
/// verification; anything else that fails only leaves the record out.
pub fn verify_private(
    replay: &AccountReplay<'_>,
    state: &AccountState,
    private: &PrivateAccountLedger,
) -> ProtocolResult<PrivateEvidence> {
    private.check()?;
    let same_root = replay
        .canonical_root()?
        .is_some_and(|root| root == state.root);
    if private.through != replay.ledger().through
        || state.account_id != replay.ledger().account_id
        || !same_root
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

    let identity_id = ids::account_identity_id(account_id, state.generation);
    let identity = private
        .identities
        .iter()
        .find(|identity| {
            identity.row.id == identity_id
                && identity.position == state.root.position
                && state.root.row.matches_identity(&identity.row)
                && identity.row.validate(account_id).is_ok()
                && requests
                    .matching(&identity.row.device_id, identity.position)
                    .is_some()
        })
        .cloned();
    // The account's own identity of this generation names another device
    // or epoch than the canonical root:
    // the public root is not the one the account wrote (§2.4,
    // `root-not-mine`). R5 and R6 allow one identity per generation, written
    // with its root.
    if identity.is_none()
        && private.identities.iter().any(|identity| {
            identity.row.id == identity_id
                && identity.row.generation == state.generation
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
            public.position == position
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
        let (Ok(proof_bytes), Ok(approval_bytes)) = (
            proof_signed_bytes(application, account_id, coordinates, &proof.row.proof),
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
    approvals.sort_by(|a, b| {
        a.approval
            .position
            .cmp(&b.approval.position)
            .then_with(|| json::utf16_cmp(&a.approval.row.id, &b.approval.row.id))
    });

    let mut successors = Vec::new();
    for successor in &private.successors {
        let position = successor.position;
        let Some(public) = state
            .epochs
            .iter()
            .filter_map(|epoch| epoch.successor.as_ref())
            .find(|public| public.row.id == successor.row.id)
        else {
            continue;
        };
        if public.position != position || !successor.row.matches_public(&public.row) {
            continue;
        }
        let Some(signer) = requests.matching(&successor.row.signer_id, position) else {
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
                    && verified.challenge.epoch_id == successor.row.predecessor
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
        identity,
        approvals,
        successors,
    })
}
