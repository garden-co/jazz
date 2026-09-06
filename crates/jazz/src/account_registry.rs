//! Ordered account-assignment state machine.
//!
//! Authentication and durable commit belong to the caller: principals passed
//! here must come from verified credentials, and a proposed state must be
//! durably committed before it replaces the admitted state or is acknowledged.

use serde::{Deserialize, Serialize};
mod codec;
pub mod storage;

use std::collections::BTreeMap;
use uuid::Uuid;

/// Exact authenticated provider identity; spelling is never normalized.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct Principal {
    /// Verified issuer.
    pub issuer: String,
    /// Verified subject.
    pub subject: String,
}

/// Stable account identity within one application registry.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct AccountId(pub Uuid);

/// Permanent assignment with independently revocable admission.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Assignment {
    /// Account this principal can never leave or replace.
    pub account: AccountId,
    /// Whether this principal can currently act for the account.
    pub active: bool,
    /// Whether it may manage identity bindings.
    pub manage_identities: bool,
    /// Admission generation; old approvals cannot survive revocation.
    pub generation: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
struct LinkIntent {
    account: AccountId,
    approver: Principal,
    approver_generation: u64,
    candidate: Principal,
    expires_at: u64,
    completed: bool,
}

/// Registry decisions are applied serially by one application authority.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct AccountRegistry {
    assignments: BTreeMap<Principal, Assignment>,
    intents: BTreeMap<Uuid, LinkIntent>,
}

/// Stable protocol failures, with no credential material.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum AccountError {
    /// Registration/linking cannot move an assigned identity.
    #[error("identity is already assigned")]
    AlreadyAssigned,
    /// Login never implicitly registers an identity.
    #[error("identity is not assigned")]
    NotAssigned,
    /// Current admission or management authority is missing.
    #[error("identity is not authorized")]
    NotAuthorized,
    /// The nonce does not identify an intent for this principal.
    #[error("invalid linking intent")]
    InvalidIntent,
    /// The approval has expired.
    #[error("linking intent expired")]
    Expired,
    /// Fresh IDs/nonces cannot overwrite existing objects.
    #[error("identifier already exists")]
    IdentifierCollision,
}

/// Authenticated registry operation. The server supplies the principal and time;
/// neither is trusted from the request body.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum AccountCommand {
    /// Create an external account explicitly.
    Register {
        /// Verified registering principal.
        principal: Principal,
        /// Fresh authority-generated account ID.
        account: AccountId,
    },
    /// Record approval from an admitted account manager.
    RequestLink {
        /// Verified account manager approving this operation.
        approver: Principal,
        /// Exact identity intended to join the account.
        candidate: Principal,
        /// Authority-generated single-use intent identifier.
        nonce: Uuid,
        /// Authority clock in Unix seconds.
        now: u64,
        /// Exclusive expiry in Unix seconds.
        expires_at: u64,
    },
    /// Confirm the targeted approval as its candidate.
    AcceptLink {
        /// Exact identity intended to join the account.
        candidate: Principal,
        /// Authority-generated single-use intent identifier.
        nonce: Uuid,
        /// Authority clock in Unix seconds.
        now: u64,
    },
    /// Remove admission without freeing an assignment.
    Revoke {
        /// Verified account manager approving this operation.
        approver: Principal,
        /// Identity whose admission is revoked.
        target: Principal,
    },
}

/// Result published only after the command has been durably committed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AccountCommandResult {
    /// Current account assignment.
    Assignment(Assignment),
    /// Approval or revocation was recorded.
    Recorded,
}

impl AccountRegistry {
    /// Apply one ordered authenticated command. Durable owners first apply to
    /// a candidate clone, persist the command, then publish that candidate.
    pub fn apply(
        &mut self,
        command: &AccountCommand,
    ) -> Result<AccountCommandResult, AccountError> {
        match command {
            AccountCommand::Register { principal, account } => self
                .register(principal.clone(), *account)
                .map(AccountCommandResult::Assignment),
            AccountCommand::RequestLink {
                approver,
                candidate,
                nonce,
                now,
                expires_at,
            } => {
                self.request_link(approver, candidate.clone(), *nonce, *now, *expires_at)?;
                Ok(AccountCommandResult::Recorded)
            }
            AccountCommand::AcceptLink {
                candidate,
                nonce,
                now,
            } => self
                .accept_link(candidate, *nonce, *now)
                .map(AccountCommandResult::Assignment),
            AccountCommand::Revoke { approver, target } => {
                self.revoke(approver, target)?;
                Ok(AccountCommandResult::Recorded)
            }
        }
    }

    /// Resolve an admitted identity without enrolling it.
    pub fn login(&self, principal: &Principal) -> Result<&Assignment, AccountError> {
        let assignment = self
            .assignments
            .get(principal)
            .ok_or(AccountError::NotAssigned)?;
        if !assignment.active {
            return Err(AccountError::NotAuthorized);
        }
        Ok(assignment)
    }

    /// Explicitly register a fresh principal and account.
    /// Account IDs are generated by the authority, not accepted from a client.
    pub fn register(
        &mut self,
        principal: Principal,
        account: AccountId,
    ) -> Result<Assignment, AccountError> {
        if self.assignments.contains_key(&principal) {
            return Err(AccountError::AlreadyAssigned);
        }
        if self
            .assignments
            .values()
            .any(|binding| binding.account == account)
        {
            return Err(AccountError::IdentifierCollision);
        }
        let assignment = Assignment {
            account,
            active: true,
            manage_identities: true,
            generation: 0,
        };
        self.assignments.insert(principal, assignment.clone());
        Ok(assignment)
    }

    /// Record a targeted approval. The authority generates nonce and expiry.
    pub fn request_link(
        &mut self,
        approver: &Principal,
        candidate: Principal,
        nonce: Uuid,
        now: u64,
        expires_at: u64,
    ) -> Result<(), AccountError> {
        let assignment = self.login(approver)?;
        if !assignment.manage_identities {
            return Err(AccountError::NotAuthorized);
        }
        if expires_at <= now {
            return Err(AccountError::Expired);
        }
        if self.assignments.contains_key(&candidate) {
            return Err(AccountError::AlreadyAssigned);
        }
        if self.intents.contains_key(&nonce) {
            return Err(AccountError::IdentifierCollision);
        }
        self.intents.insert(
            nonce,
            LinkIntent {
                account: assignment.account,
                approver: approver.clone(),
                approver_generation: assignment.generation,
                candidate,
                expires_at,
                completed: false,
            },
        );
        Ok(())
    }

    /// Accept as exactly the candidate principal and atomically reserve its
    /// assignment. Replays never reactivate a revoked assignment.
    pub fn accept_link(
        &mut self,
        candidate: &Principal,
        nonce: Uuid,
        now: u64,
    ) -> Result<Assignment, AccountError> {
        let intent = self
            .intents
            .get(&nonce)
            .ok_or(AccountError::InvalidIntent)?;
        if &intent.candidate != candidate {
            return Err(AccountError::InvalidIntent);
        }
        if intent.completed {
            return self.login(candidate).cloned();
        }
        if now >= intent.expires_at {
            return Err(AccountError::Expired);
        }
        let approver = self.login(&intent.approver)?;
        if !approver.manage_identities
            || approver.account != intent.account
            || approver.generation != intent.approver_generation
        {
            return Err(AccountError::NotAuthorized);
        }
        if self.assignments.contains_key(candidate) {
            return Err(AccountError::AlreadyAssigned);
        }
        let assignment = Assignment {
            account: intent.account,
            active: true,
            manage_identities: true,
            generation: 0,
        };
        self.assignments
            .insert(candidate.clone(), assignment.clone());
        self.intents
            .get_mut(&nonce)
            .expect("validated intent")
            .completed = true;
        Ok(assignment)
    }

    /// Revoke admission while retaining the permanent assignment.
    pub fn revoke(&mut self, approver: &Principal, target: &Principal) -> Result<(), AccountError> {
        let approval = self.login(approver)?;
        let target_binding = self
            .assignments
            .get(target)
            .ok_or(AccountError::NotAssigned)?;
        if !approval.manage_identities || approval.account != target_binding.account {
            return Err(AccountError::NotAuthorized);
        }
        let target_binding = self.assignments.get_mut(target).expect("validated target");
        if target_binding.active {
            target_binding.active = false;
            target_binding.generation = target_binding
                .generation
                .checked_add(1)
                .expect("admission generation exhausted");
        }
        Ok(())
    }
}

// These state-machine tests cover orderings without sockets or wall clocks.
// Transport/authentication and durable recovery require separate server E2Es;
// these tests deliberately do not claim to verify those boundaries.
#[cfg(test)]
mod tests {
    use super::*;

    fn principal(subject: &str) -> Principal {
        Principal {
            issuer: "https://issuer.example".into(),
            subject: subject.into(),
        }
    }

    #[test]
    fn competing_claims_have_one_winner_and_revocation_never_frees_assignment() {
        let mut registry = AccountRegistry::default();
        let alice = principal("alice");
        let bob = principal("bob");
        let candidate = principal("new");
        let account_a = AccountId(Uuid::from_u128(1));
        let account_b = AccountId(Uuid::from_u128(2));
        registry.register(alice.clone(), account_a).unwrap();
        registry.register(bob.clone(), account_b).unwrap();
        let first = Uuid::from_u128(3);
        let second = Uuid::from_u128(4);
        registry
            .request_link(&alice, candidate.clone(), first, 0, 10)
            .unwrap();
        registry
            .request_link(&bob, candidate.clone(), second, 0, 10)
            .unwrap();
        assert_eq!(
            registry.accept_link(&candidate, first, 1).unwrap().account,
            account_a
        );
        assert_eq!(
            registry.accept_link(&candidate, second, 1),
            Err(AccountError::AlreadyAssigned)
        );
        registry.revoke(&alice, &candidate).unwrap();
        assert_eq!(
            registry.accept_link(&candidate, first, 2),
            Err(AccountError::NotAuthorized)
        );
        assert_eq!(
            registry.register(candidate, AccountId(Uuid::from_u128(5))),
            Err(AccountError::AlreadyAssigned)
        );
    }

    #[test]
    fn nonce_does_not_authorize_another_identity_or_survive_approver_revocation() {
        let mut registry = AccountRegistry::default();
        let alice = principal("alice");
        let candidate = principal("new");
        registry
            .register(alice.clone(), AccountId(Uuid::from_u128(1)))
            .unwrap();
        let nonce = Uuid::from_u128(2);
        registry
            .request_link(&alice, candidate.clone(), nonce, 0, 10)
            .unwrap();
        assert_eq!(
            registry.accept_link(&principal("mallory"), nonce, 1),
            Err(AccountError::InvalidIntent)
        );
        registry.revoke(&alice, &alice).unwrap();
        assert_eq!(
            registry.accept_link(&candidate, nonce, 1),
            Err(AccountError::NotAuthorized)
        );
        assert_eq!(registry.login(&candidate), Err(AccountError::NotAssigned));
    }

    #[test]
    fn expiration_and_exact_issuer_are_enforced_without_consuming_valid_approval() {
        let mut registry = AccountRegistry::default();
        let alice = principal("alice");
        let candidate = principal("new");
        registry
            .register(alice.clone(), AccountId(Uuid::from_u128(1)))
            .unwrap();
        let nonce = Uuid::from_u128(2);
        registry
            .request_link(&alice, candidate.clone(), nonce, 0, 10)
            .unwrap();
        let other_issuer = Principal {
            issuer: "https://other.example".into(),
            ..candidate.clone()
        };
        assert_eq!(
            registry.accept_link(&other_issuer, nonce, 1),
            Err(AccountError::InvalidIntent)
        );
        assert_eq!(
            registry.accept_link(&candidate, nonce, 10),
            Err(AccountError::Expired)
        );
        assert_eq!(registry.login(&candidate), Err(AccountError::NotAssigned));
    }
}
