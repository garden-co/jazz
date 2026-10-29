//! Account-assignment decisions over the registry's current state.
//!
//! Authentication and durable commit belong to the caller: principals passed
//! here must come from verified credentials, and a command's writes to its
//! [`AccountState`] must be durably committed, all together, before its
//! result is acknowledged.

use serde::{Deserialize, Serialize};

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

impl Principal {
    /// Check the principal names a valid identity; `allow_local_first`
    /// admits local-first principals (a UUID subject).
    pub fn validate(&self, allow_local_first: bool) -> Result<(), AccountError> {
        if allow_local_first && self.issuer == crate::identity::LOCAL_FIRST_ISSUER {
            if uuid::Uuid::parse_str(&self.subject).is_ok() {
                return Ok(());
            }
            return Err(AccountError::InvalidPrincipal);
        }
        crate::ids::AuthorSubject::authenticated(&self.issuer, &self.subject)
            .map(|_| ())
            .map_err(|_| AccountError::InvalidPrincipal)
    }
}

/// Stable account identity within one application registry.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct AccountId(pub Uuid);

/// Reserved durable account identity used only for node-attributed system rows.
///
/// It is never an assignable registry account or an externally admitted claim.
pub const SYSTEM_ACCOUNT_ID: AccountId = AccountId(Uuid::nil());

impl AccountId {
    /// Whether this is the reserved system-attribution account.
    pub fn is_system(self) -> bool {
        self == SYSTEM_ACCOUNT_ID
    }
}

/// Deterministic founding account, scoped to the application's registry.
/// `subject` must be the identity derived from a verified local-first key.
/// This does not authenticate a caller or admit an arbitrary claimed subject.
pub fn local_first_account_id(app: Uuid, subject: &str) -> AccountId {
    let mut name = b"jazz-account-founder-v1\0".to_vec();
    name.extend_from_slice(subject.as_bytes());
    AccountId(Uuid::new_v5(&app, &name))
}

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

/// Stable protocol failures, with no credential material.
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
pub enum AccountError {
    /// System, read-only anonymous, or malformed principals cannot own accounts.
    #[error("invalid account principal")]
    InvalidPrincipal,
    /// The durable system-attribution account is not assignable to a user.
    #[error("reserved account identifier")]
    ReservedAccount,
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
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum AccountCommand {
    /// Register a verified local-first founding identity deterministically.
    FoundLocalFirst {
        /// Verified local-first principal derived from its signing key.
        principal: Principal,
        /// Application namespace selected by the serving authority.
        app: Uuid,
    },
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

/// A manager's approval for one exact identity to join their account.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LinkIntent {
    /// Account the candidate joins.
    pub account: AccountId,
    /// Manager who approved.
    pub approver: Principal,
    /// The approver's admission generation when approving; a revocation
    /// since voids the approval.
    pub approver_generation: u64,
    /// Exact identity that may accept.
    pub candidate: Principal,
    /// Exclusive expiry in Unix seconds.
    pub expires_at: u64,
    /// Accepted already; accepting again answers the assignment.
    pub completed: bool,
}

/// The registry's current state, wherever it is kept: the decisions below
/// read and write only through this. A durable owner runs one command's
/// reads and writes in one transaction, so the state it leaves is always
/// the result of whole commands; a decision error comes before any write.
pub trait AccountState {
    /// Storage failures, and the registry's own refusals.
    type Error: From<AccountError>;
    /// The principal's assignment.
    fn assignment(&mut self, principal: &Principal) -> Result<Option<Assignment>, Self::Error>;
    /// Whether any principal is assigned to `account`.
    fn account_assigned(&mut self, account: AccountId) -> Result<bool, Self::Error>;
    /// The approval with `nonce`.
    fn link(&mut self, nonce: Uuid) -> Result<Option<LinkIntent>, Self::Error>;
    /// Set the principal's assignment.
    fn put_assignment(
        &mut self,
        principal: &Principal,
        assignment: &Assignment,
    ) -> Result<(), Self::Error>;
    /// Set the approval with `nonce`.
    fn put_link(&mut self, nonce: Uuid, link: &LinkIntent) -> Result<(), Self::Error>;
    /// Forget approvals that expired at or before `now` without being
    /// accepted; accepting one afterwards finds no intent.
    fn prune_links(&mut self, now: u64) -> Result<(), Self::Error>;
}

/// Apply one authenticated command to `state`.
pub fn apply<S: AccountState>(
    state: &mut S,
    command: &AccountCommand,
) -> Result<AccountCommandResult, S::Error> {
    match command {
        AccountCommand::FoundLocalFirst { principal, app } => {
            principal.validate(true)?;
            if principal.issuer != crate::identity::LOCAL_FIRST_ISSUER {
                return Err(AccountError::InvalidPrincipal.into());
            }
            let account = local_first_account_id(*app, &principal.subject);
            if let Some(existing) = state.assignment(principal)? {
                if existing.account != account {
                    return Err(AccountError::AlreadyAssigned.into());
                }
                return login(state, principal).map(AccountCommandResult::Assignment);
            }
            register_inner(state, principal, account).map(AccountCommandResult::Assignment)
        }
        AccountCommand::Register { principal, account } => {
            principal.validate(false)?;
            register_inner(state, principal, *account).map(AccountCommandResult::Assignment)
        }
        AccountCommand::RequestLink {
            approver,
            candidate,
            nonce,
            now,
            expires_at,
        } => {
            request_link(state, approver, candidate, *nonce, *now, *expires_at)?;
            Ok(AccountCommandResult::Recorded)
        }
        AccountCommand::AcceptLink {
            candidate,
            nonce,
            now,
        } => accept_link(state, candidate, *nonce, *now).map(AccountCommandResult::Assignment),
        AccountCommand::Revoke { approver, target } => {
            revoke(state, approver, target)?;
            Ok(AccountCommandResult::Recorded)
        }
    }
}

/// Resolve an admitted identity without enrolling it.
pub fn login<S: AccountState>(
    state: &mut S,
    principal: &Principal,
) -> Result<Assignment, S::Error> {
    let assignment = state
        .assignment(principal)?
        .ok_or(AccountError::NotAssigned)?;
    if !assignment.active {
        return Err(AccountError::NotAuthorized.into());
    }
    Ok(assignment)
}

fn register_inner<S: AccountState>(
    state: &mut S,
    principal: &Principal,
    account: AccountId,
) -> Result<Assignment, S::Error> {
    if account.is_system() {
        return Err(AccountError::ReservedAccount.into());
    }
    if state.assignment(principal)?.is_some() {
        return Err(AccountError::AlreadyAssigned.into());
    }
    if state.account_assigned(account)? {
        return Err(AccountError::IdentifierCollision.into());
    }
    let assignment = Assignment {
        account,
        active: true,
        manage_identities: true,
        generation: 0,
    };
    state.put_assignment(principal, &assignment)?;
    Ok(assignment)
}

/// Record a targeted approval. The authority generates nonce and expiry.
fn request_link<S: AccountState>(
    state: &mut S,
    approver: &Principal,
    candidate: &Principal,
    nonce: Uuid,
    now: u64,
    expires_at: u64,
) -> Result<(), S::Error> {
    candidate.validate(false)?;
    let assignment = login(state, approver)?;
    if !assignment.manage_identities {
        return Err(AccountError::NotAuthorized.into());
    }
    if expires_at <= now {
        return Err(AccountError::Expired.into());
    }
    if state.assignment(candidate)?.is_some() {
        return Err(AccountError::AlreadyAssigned.into());
    }
    if state.link(nonce)?.is_some() {
        return Err(AccountError::IdentifierCollision.into());
    }
    state.prune_links(now)?;
    state.put_link(
        nonce,
        &LinkIntent {
            account: assignment.account,
            approver: approver.clone(),
            approver_generation: assignment.generation,
            candidate: candidate.clone(),
            expires_at,
            completed: false,
        },
    )
}

/// Accept as exactly the candidate principal and atomically reserve its
/// assignment. Repeats never reactivate a revoked assignment.
fn accept_link<S: AccountState>(
    state: &mut S,
    candidate: &Principal,
    nonce: Uuid,
    now: u64,
) -> Result<Assignment, S::Error> {
    let mut intent = state.link(nonce)?.ok_or(AccountError::InvalidIntent)?;
    if &intent.candidate != candidate {
        return Err(AccountError::InvalidIntent.into());
    }
    if intent.completed {
        return login(state, candidate);
    }
    if now >= intent.expires_at {
        return Err(AccountError::Expired.into());
    }
    let approver = login(state, &intent.approver)?;
    if !approver.manage_identities
        || approver.account != intent.account
        || approver.generation != intent.approver_generation
    {
        return Err(AccountError::NotAuthorized.into());
    }
    if state.assignment(candidate)?.is_some() {
        return Err(AccountError::AlreadyAssigned.into());
    }
    let assignment = Assignment {
        account: intent.account,
        active: true,
        manage_identities: true,
        generation: 0,
    };
    state.put_assignment(candidate, &assignment)?;
    intent.completed = true;
    state.put_link(nonce, &intent)?;
    Ok(assignment)
}

/// Revoke admission while retaining the permanent assignment.
fn revoke<S: AccountState>(
    state: &mut S,
    approver: &Principal,
    target: &Principal,
) -> Result<(), S::Error> {
    let approval = login(state, approver)?;
    let mut target_binding = state.assignment(target)?.ok_or(AccountError::NotAssigned)?;
    if !approval.manage_identities || approval.account != target_binding.account {
        return Err(AccountError::NotAuthorized.into());
    }
    if target_binding.active {
        target_binding.active = false;
        target_binding.generation = target_binding
            .generation
            .checked_add(1)
            .expect("admission generation exhausted");
        state.put_assignment(target, &target_binding)?;
    }
    Ok(())
}

/// The registry state in memory, for tests and simulations.
#[derive(Clone, Debug, Default)]
pub struct AccountRegistry {
    assignments: BTreeMap<Principal, Assignment>,
    intents: BTreeMap<Uuid, LinkIntent>,
}

impl AccountState for AccountRegistry {
    type Error = AccountError;

    fn assignment(&mut self, principal: &Principal) -> Result<Option<Assignment>, AccountError> {
        Ok(self.assignments.get(principal).cloned())
    }

    fn account_assigned(&mut self, account: AccountId) -> Result<bool, AccountError> {
        Ok(self
            .assignments
            .values()
            .any(|binding| binding.account == account))
    }

    fn link(&mut self, nonce: Uuid) -> Result<Option<LinkIntent>, AccountError> {
        Ok(self.intents.get(&nonce).cloned())
    }

    fn put_assignment(
        &mut self,
        principal: &Principal,
        assignment: &Assignment,
    ) -> Result<(), AccountError> {
        self.assignments
            .insert(principal.clone(), assignment.clone());
        Ok(())
    }

    fn put_link(&mut self, nonce: Uuid, link: &LinkIntent) -> Result<(), AccountError> {
        self.intents.insert(nonce, link.clone());
        Ok(())
    }

    fn prune_links(&mut self, now: u64) -> Result<(), AccountError> {
        self.intents
            .retain(|_, link| link.completed || link.expires_at > now);
        Ok(())
    }
}

impl AccountRegistry {
    /// Apply one ordered authenticated command.
    pub fn apply(
        &mut self,
        command: &AccountCommand,
    ) -> Result<AccountCommandResult, AccountError> {
        apply(self, command)
    }

    /// Resolve an admitted identity without enrolling it.
    pub fn login(&mut self, principal: &Principal) -> Result<Assignment, AccountError> {
        login(self, principal)
    }

    /// Explicitly register a fresh principal and account.
    /// Account IDs are generated by the authority, not accepted from a client.
    pub fn register(
        &mut self,
        principal: Principal,
        account: AccountId,
    ) -> Result<Assignment, AccountError> {
        principal.validate(false)?;
        register_inner(self, &principal, account)
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
        request_link(self, approver, &candidate, nonce, now, expires_at)
    }

    /// Accept as exactly the candidate principal.
    pub fn accept_link(
        &mut self,
        candidate: &Principal,
        nonce: Uuid,
        now: u64,
    ) -> Result<Assignment, AccountError> {
        accept_link(self, candidate, nonce, now)
    }

    /// Revoke admission while retaining the permanent assignment.
    pub fn revoke(&mut self, approver: &Principal, target: &Principal) -> Result<(), AccountError> {
        revoke(self, approver, target)
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

    #[test]
    fn register_command_rejects_reserved_system_account() {
        let mut registry = AccountRegistry::default();
        assert_eq!(
            registry.apply(&AccountCommand::Register {
                principal: principal("alice"),
                account: SYSTEM_ACCOUNT_ID,
            }),
            Err(AccountError::ReservedAccount)
        );
    }
}

#[cfg(test)]
mod founding_tests {
    use super::*;
    #[test]
    fn founding_identity_matches_portable_handle_fixture_and_cannot_reregister() {
        let app = crate::app_id::AppId::from_name("account-fixture");
        let principal = Principal {
            issuer: crate::identity::LOCAL_FIRST_ISSUER.into(),
            subject: "00000000-0000-4000-8000-000000000001".into(),
        };
        assert_eq!(
            local_first_account_id(*app.uuid(), &principal.subject)
                .0
                .to_string(),
            "30f0ec9a-0a5a-5940-b329-274194a6e24b"
        );
        let mut registry = AccountRegistry::default();
        let command = AccountCommand::FoundLocalFirst {
            principal: principal.clone(),
            app: *app.uuid(),
        };
        let first = registry.apply(&command).unwrap();
        assert_eq!(registry.apply(&command).unwrap(), first);
        registry.revoke(&principal, &principal).unwrap();
        assert_eq!(registry.apply(&command), Err(AccountError::NotAuthorized));
    }
}
