//! Caller-free, deterministic authorization-support hydration state.
#![allow(dead_code, missing_docs)]

use crate::protocol::{AuthorizationScopeReceipt, AuthorizationSupportScopeKey, SubscriptionKey};
use crate::query::{BindingId, ShapeId};
use crate::time::GlobalTime;
use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::rc::Rc;

pub const MAX_AUTHORIZATION_SCOPES: usize = 256;

/// The authority-owned, all-clause proof lifecycle shared by locally terminal
/// fate admission and wire-delivered authorization advice.  Membership is
/// canonical (shape/binding); transport subscription keys are allocated by the
/// authority and merely name one registered clause instance.
#[derive(Clone, Debug)]
pub(crate) struct AuthorityScopeAggregate {
    pub(crate) expected_support: BTreeSet<(ShapeId, BindingId)>,
    pub(crate) members: BTreeMap<SubscriptionKey, (ShapeId, BindingId)>,
    pub(crate) applied: BTreeMap<SubscriptionKey, (GlobalTime, u64)>,
}

impl AuthorityScopeAggregate {
    pub(crate) fn new(expected_support: BTreeSet<(ShapeId, BindingId)>) -> Self {
        Self {
            expected_support,
            members: BTreeMap::new(),
            applied: BTreeMap::new(),
        }
    }

    pub(crate) fn expected_support(&self) -> &BTreeSet<(ShapeId, BindingId)> {
        &self.expected_support
    }

    /// Register exactly one server-owned subscription for a canonical support
    /// clause.  A duplicate or an out-of-scope clause invalidates completion.
    pub(crate) fn register(
        &mut self,
        subscription: SubscriptionKey,
        clause: (ShapeId, BindingId),
    ) -> bool {
        if !self.expected_support.contains(&clause)
            || self.members.contains_key(&subscription)
            || self.members.values().any(|member| *member == clause)
        {
            return false;
        }
        self.members.insert(subscription, clause);
        true
    }

    pub(crate) fn forget(&mut self, subscription: SubscriptionKey) {
        self.members.remove(&subscription);
        self.applied.remove(&subscription);
    }

    pub(crate) fn has_no_members(&self) -> bool {
        self.members.is_empty()
    }

    /// Records a locally applied clause and returns the aggregate lower bounds
    /// only when every canonical clause has an applied current view.
    pub(crate) fn apply(
        &mut self,
        subscription: SubscriptionKey,
        settled_through: GlobalTime,
        authorization_progress: u64,
    ) -> Option<(GlobalTime, u64)> {
        if !self.members.contains_key(&subscription) {
            return None;
        }
        self.applied
            .insert(subscription, (settled_through, authorization_progress));
        self.bounds()
    }

    pub(crate) fn bounds(&self) -> Option<(GlobalTime, u64)> {
        if self.members.len() != self.expected_support.len()
            || self
                .expected_support
                .iter()
                .any(|expected| !self.members.values().any(|member| member == expected))
            || self
                .members
                .keys()
                .any(|member| !self.applied.contains_key(member))
        {
            return None;
        }
        Some((
            self.applied
                .values()
                .map(|(settled, _)| *settled)
                .min_by_key(|settled| settled.0)?,
            self.applied.values().map(|(_, progress)| *progress).min()?,
        ))
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AuthorityContext {
    pub authority: [u8; 16],
    pub link: crate::ids::AuthorSubject,
    /// Locally generated identity for one physical admitted upstream link.
    /// It never crosses the wire; it prevents a parallel connection to the
    /// same remote authority epoch from discharging another link's routes.
    pub connection_id: u64,
    pub connection_epoch: u64,
    pub claims_revision: u64,
    pub policy_epoch: u64,
    pub authorization_progress: u64,
    pub settled_through: u64,
}

impl AuthorityContext {
    /// Whether two snapshots describe the same authenticated physical
    /// connection. Scope receipts legitimately advance the remaining fields
    /// while that connection stays live; those receipt bounds must not make an
    /// already parked edge-fate route look as if it belonged to a stale link.
    pub(crate) fn same_admitted_link(self, other: Self) -> bool {
        self.authority == other.authority
            && self.link == other.link
            && self.connection_id == other.connection_id
            && self.connection_epoch == other.connection_epoch
    }
}

/// Capability to send exactly one hydration request for a lease generation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct AuthorizationScopeOwnerToken(u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AuthorizationScopeAcquisition {
    Owner(AuthorizationScopeOwnerToken),
    Waiting,
    Proven,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum AuthorizationScopeReadiness {
    Proven(AuthorizationScopeReceipt),
    Owner(AuthorizationScopeOwnerToken),
    Waiting,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AuthorizationScopeInstall {
    Installed,
    /// The same lone owner must retry with this new generation.
    Retry(AuthorizationScopeOwnerToken),
    Waiting,
    Rejected,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Phase {
    Idle,
    Hydrating { generation: u64, owner: u64 },
    Proven,
}

struct Entry {
    refs: usize,
    phase: Phase,
    receipt: Option<AuthorizationScopeReceipt>,
    waiters: VecDeque<u64>,
    next_generation: u64,
}

impl Default for Entry {
    fn default() -> Self {
        Self {
            refs: 0,
            phase: Phase::Idle,
            receipt: None,
            waiters: VecDeque::new(),
            next_generation: 1,
        }
    }
}

#[derive(Default)]
struct State {
    scopes: BTreeMap<AuthorizationSupportScopeKey, Entry>,
    next_lease: u64,
}

#[derive(Clone, Default)]
pub struct AuthorizationScopeRegistry(Rc<RefCell<State>>);

pub struct AuthorizationScopeLease {
    state: Rc<RefCell<State>>,
    key: AuthorizationSupportScopeKey,
    id: u64,
    owner_generation: Cell<Option<u64>>,
}

impl AuthorizationScopeRegistry {
    /// Acquire a retained scope lease and its immediately actionable state.
    pub(crate) fn acquire(
        &self,
        key: AuthorizationSupportScopeKey,
    ) -> Option<(AuthorizationScopeLease, AuthorizationScopeAcquisition)> {
        let mut s = self.0.borrow_mut();
        if !s.scopes.contains_key(&key) && s.scopes.len() >= MAX_AUTHORIZATION_SCOPES {
            return None;
        }
        let id = s.next_lease;
        s.next_lease += 1;
        let e = s.scopes.entry(key.clone()).or_default();
        e.refs += 1;
        let (owner_generation, acquisition) = match e.phase {
            Phase::Idle => {
                let token = AuthorizationScopeOwnerToken(Self::start(e, id));
                (Some(token.0), AuthorizationScopeAcquisition::Owner(token))
            }
            Phase::Hydrating { .. } => {
                e.waiters.push_back(id);
                (None, AuthorizationScopeAcquisition::Waiting)
            }
            Phase::Proven => (None, AuthorizationScopeAcquisition::Proven),
        };
        Some((
            AuthorizationScopeLease {
                state: Rc::clone(&self.0),
                key,
                id,
                owner_generation: Cell::new(owner_generation),
            },
            acquisition,
        ))
    }

    /// Consume the deterministic promotion token; only the next retained waiter can own it.
    pub(crate) fn take_promotion(
        &self,
        lease: &AuthorizationScopeLease,
    ) -> Option<AuthorizationScopeOwnerToken> {
        if !Rc::ptr_eq(&self.0, &lease.state) {
            return None;
        }
        let mut s = self.0.borrow_mut();
        let e = s.scopes.get_mut(&lease.key)?;
        if !matches!(e.phase, Phase::Idle) || e.waiters.front() != Some(&lease.id) {
            return None;
        }
        e.waiters.pop_front();
        let token = AuthorizationScopeOwnerToken(Self::start(e, lease.id));
        lease.owner_generation.set(Some(token.0));
        Some(token)
    }

    pub(crate) fn install(
        &self,
        lease: &AuthorizationScopeLease,
        token: AuthorizationScopeOwnerToken,
        ctx: AuthorityContext,
        receipt: AuthorizationScopeReceipt,
    ) -> AuthorizationScopeInstall {
        if !Rc::ptr_eq(&self.0, &lease.state)
            || receipt.key != lease.key
            || lease.owner_generation.get() != Some(token.0)
        {
            return AuthorizationScopeInstall::Rejected;
        }
        let mut s = self.0.borrow_mut();
        let Some(e) = s.scopes.get_mut(&receipt.key) else {
            return AuthorizationScopeInstall::Rejected;
        };
        if !matches!(e.phase, Phase::Hydrating { generation, owner } if generation == token.0 && owner == lease.id)
        {
            return AuthorizationScopeInstall::Rejected;
        }
        if Self::receipt_matches(
            &receipt,
            ctx,
            ctx.authorization_progress,
            ctx.settled_through,
        ) {
            e.phase = Phase::Proven;
            e.receipt = Some(receipt);
            lease.owner_generation.set(None);
            return AuthorizationScopeInstall::Installed;
        }
        Self::retry_or_idle(e, lease, token)
    }

    /// Read only a proof current for this authority connection and support-view cut.
    /// An obsolete proof transitions retained leases back into deterministic hydration.
    pub(crate) fn receipt(
        &self,
        lease: &AuthorizationScopeLease,
        ctx: AuthorityContext,
        required_progress: u64,
        required_cut: u64,
    ) -> AuthorizationScopeReadiness {
        if !Rc::ptr_eq(&self.0, &lease.state) {
            return AuthorizationScopeReadiness::Waiting;
        }
        let mut s = self.0.borrow_mut();
        let Some(e) = s.scopes.get_mut(&lease.key) else {
            return AuthorizationScopeReadiness::Waiting;
        };
        if let Some(receipt) = &e.receipt {
            if Self::receipt_matches(receipt, ctx, required_progress, required_cut) {
                return AuthorizationScopeReadiness::Proven(receipt.clone());
            }
            e.receipt = None;
            e.phase = Phase::Idle;
        }
        match e.phase {
            Phase::Idle if e.waiters.front().is_none() => {
                let token = AuthorizationScopeOwnerToken(Self::start(e, lease.id));
                lease.owner_generation.set(Some(token.0));
                AuthorizationScopeReadiness::Owner(token)
            }
            Phase::Hydrating { generation, owner } if owner == lease.id => {
                AuthorizationScopeReadiness::Owner(AuthorizationScopeOwnerToken(generation))
            }
            _ => AuthorizationScopeReadiness::Waiting,
        }
    }

    fn receipt_matches(
        receipt: &AuthorizationScopeReceipt,
        ctx: AuthorityContext,
        required_progress: u64,
        required_cut: u64,
    ) -> bool {
        receipt.authority == ctx.authority
            && receipt.link == ctx.link
            && receipt.authority_epoch == ctx.connection_epoch
            && receipt.claims_revision == ctx.claims_revision
            && receipt.policy_epoch == ctx.policy_epoch
            && receipt.authorization_progress >= ctx.authorization_progress.max(required_progress)
            && receipt.settled_through.0 >= ctx.settled_through.max(required_cut)
    }

    fn start(e: &mut Entry, owner: u64) -> u64 {
        let generation = e.next_generation;
        e.next_generation += 1;
        e.phase = Phase::Hydrating { generation, owner };
        generation
    }

    fn retry_or_idle(
        e: &mut Entry,
        lease: &AuthorizationScopeLease,
        token: AuthorizationScopeOwnerToken,
    ) -> AuthorizationScopeInstall {
        e.receipt = None;
        e.phase = Phase::Idle;
        if e.waiters.is_empty() {
            let retry = AuthorizationScopeOwnerToken(Self::start(e, lease.id));
            lease.owner_generation.set(Some(retry.0));
            AuthorizationScopeInstall::Retry(retry)
        } else {
            lease.owner_generation.set(None);
            // The deterministic front waiter receives its token through take_promotion.
            let _ = token;
            AuthorizationScopeInstall::Waiting
        }
    }

    pub fn len(&self) -> usize {
        self.0.borrow().scopes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.0.borrow().scopes.is_empty()
    }
}

impl Drop for AuthorizationScopeLease {
    fn drop(&mut self) {
        let mut s = self.state.borrow_mut();
        let Some(e) = s.scopes.get_mut(&self.key) else {
            return;
        };
        if self.owner_generation.get().is_none() {
            e.waiters.retain(|waiter| *waiter != self.id);
        } else if matches!(e.phase, Phase::Hydrating { generation, owner } if self.owner_generation.get() == Some(generation) && owner == self.id)
        {
            e.phase = Phase::Idle;
        }
        e.refs -= 1;
        if e.refs == 0 {
            s.scopes.remove(&self.key);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::AuthorSubject;
    use crate::query::{BindingId, ShapeId};
    use crate::time::GlobalTime;

    fn k(n: u8) -> AuthorizationSupportScopeKey {
        AuthorizationSupportScopeKey {
            support_shape_digest: [n; 32],
            subject: AuthorSubject::for_test_bytes([1; 16]),
            claims_digest: [2; 32],
            policy_digest: [3; 32],
        }
    }
    fn indexed_key(n: usize) -> AuthorizationSupportScopeKey {
        let mut support_shape_digest = [0; 32];
        support_shape_digest[..8].copy_from_slice(&(n as u64).to_le_bytes());
        AuthorizationSupportScopeKey {
            support_shape_digest,
            ..k(1)
        }
    }
    fn c() -> AuthorityContext {
        AuthorityContext {
            authority: [4; 16],
            link: AuthorSubject::for_test_bytes([5; 16]),
            connection_id: 1,
            connection_epoch: 1,
            claims_revision: 1,
            policy_epoch: 1,
            authorization_progress: 1,
            settled_through: 1,
        }
    }
    fn r(key: AuthorizationSupportScopeKey) -> AuthorizationScopeReceipt {
        AuthorizationScopeReceipt {
            key,
            authority: [4; 16],
            link: AuthorSubject::for_test_bytes([5; 16]),
            authority_epoch: 1,
            claims_revision: 1,
            policy_epoch: 1,
            settled_through: GlobalTime(1),
            authorization_progress: 1,
        }
    }
    fn owner(a: AuthorizationScopeAcquisition) -> AuthorizationScopeOwnerToken {
        match a {
            AuthorizationScopeAcquisition::Owner(token) => token,
            _ => panic!("expected initial owner"),
        }
    }

    #[test]
    fn aggregate_requires_every_registered_clause_even_when_views_arrive_in_reverse_order() {
        let first = SubscriptionKey {
            shape_id: ShapeId(uuid::Uuid::from_bytes([1; 16])),
            binding_id: BindingId(uuid::Uuid::from_bytes([2; 16])),
            read_view: Default::default(),
        };
        let second = SubscriptionKey {
            shape_id: ShapeId(uuid::Uuid::from_bytes([3; 16])),
            binding_id: BindingId(uuid::Uuid::from_bytes([4; 16])),
            read_view: Default::default(),
        };
        let mut aggregate = AuthorityScopeAggregate::new(BTreeSet::from([
            (first.shape_id, first.binding_id),
            (second.shape_id, second.binding_id),
        ]));
        assert!(aggregate.register(first, (first.shape_id, first.binding_id)));
        assert!(aggregate.register(second, (second.shape_id, second.binding_id)));
        assert_eq!(aggregate.apply(second, GlobalTime(9), 7), None);
        assert_eq!(
            aggregate.apply(first, GlobalTime(12), 11),
            Some((GlobalTime(9), 7))
        );
    }

    #[test]
    fn initial_acquisition_exposes_single_send_owner() {
        let x = AuthorizationScopeRegistry::default();
        let (_first_lease, first) = x.acquire(k(1)).unwrap();
        let (_, second) = x.acquire(k(1)).unwrap();
        assert!(matches!(first, AuthorizationScopeAcquisition::Owner(_)));
        assert_eq!(second, AuthorizationScopeAcquisition::Waiting);
    }
    #[test]
    fn waiter_drop_does_not_clear_owner() {
        let x = AuthorizationScopeRegistry::default();
        let (a, _) = x.acquire(k(1)).unwrap();
        let (b, _) = x.acquire(k(1)).unwrap();
        drop(b);
        let (c, _) = x.acquire(k(1)).unwrap();
        drop(a);
        assert!(x.take_promotion(&c).is_some());
    }
    #[test]
    fn owner_drop_promotes_exactly_one_waiter() {
        let x = AuthorizationScopeRegistry::default();
        let (a, _) = x.acquire(k(1)).unwrap();
        let (b, _) = x.acquire(k(1)).unwrap();
        let (c, _) = x.acquire(k(1)).unwrap();
        drop(a);
        assert!(x.take_promotion(&b).is_some());
        assert!(x.take_promotion(&c).is_none());
    }
    #[test]
    fn rejected_install_promotes_retry() {
        let x = AuthorizationScopeRegistry::default();
        let (a, a_token) = x.acquire(k(1)).unwrap();
        let (b, _) = x.acquire(k(1)).unwrap();
        assert_eq!(
            x.install(
                &a,
                owner(a_token),
                c(),
                AuthorizationScopeReceipt {
                    authority: [9; 16],
                    ..r(k(1))
                }
            ),
            AuthorizationScopeInstall::Waiting
        );
        assert!(x.take_promotion(&b).is_some());
    }
    #[test]
    fn lone_owner_failed_install_gets_retry_token() {
        let x = AuthorizationScopeRegistry::default();
        let (a, a_token) = x.acquire(k(1)).unwrap();
        let retry = match x.install(
            &a,
            owner(a_token),
            c(),
            AuthorizationScopeReceipt {
                authority: [9; 16],
                ..r(k(1))
            },
        ) {
            AuthorizationScopeInstall::Retry(token) => token,
            other => panic!("expected retry, got {other:?}"),
        };
        assert_eq!(
            x.install(&a, retry, c(), r(k(1))),
            AuthorizationScopeInstall::Installed
        );
    }
    #[test]
    fn stale_owner_completion_cannot_overwrite_new_owner() {
        let x = AuthorizationScopeRegistry::default();
        let (a, a_token) = x.acquire(k(1)).unwrap();
        let (b, _) = x.acquire(k(1)).unwrap();
        assert_eq!(
            x.install(
                &a,
                owner(a_token),
                c(),
                AuthorizationScopeReceipt {
                    authority: [9; 16],
                    ..r(k(1))
                }
            ),
            AuthorizationScopeInstall::Waiting
        );
        let b_token = x.take_promotion(&b).unwrap();
        assert_eq!(
            x.install(&a, owner(a_token), c(), r(k(1))),
            AuthorizationScopeInstall::Rejected
        );
        assert_eq!(
            x.install(&b, b_token, c(), r(k(1))),
            AuthorizationScopeInstall::Installed
        );
    }
    #[test]
    fn context_changes_invalidate_proven_scope_and_rehydrate() {
        let x = AuthorizationScopeRegistry::default();
        let (a, a_token) = x.acquire(k(1)).unwrap();
        assert_eq!(
            x.install(&a, owner(a_token), c(), r(k(1))),
            AuthorizationScopeInstall::Installed
        );
        for changed in [
            AuthorityContext {
                authority: [9; 16],
                ..c()
            },
            AuthorityContext {
                link: AuthorSubject::for_test_bytes([9; 16]),
                ..c()
            },
            AuthorityContext {
                connection_epoch: 2,
                ..c()
            },
            AuthorityContext {
                claims_revision: 2,
                ..c()
            },
            AuthorityContext {
                policy_epoch: 2,
                ..c()
            },
            AuthorityContext {
                authorization_progress: 2,
                ..c()
            },
            AuthorityContext {
                settled_through: 2,
                ..c()
            },
        ] {
            let token = match x.receipt(
                &a,
                changed,
                changed.authorization_progress,
                changed.settled_through,
            ) {
                AuthorizationScopeReadiness::Owner(token) => token,
                other => panic!("expected rehydration owner, got {other:?}"),
            };
            assert_eq!(
                x.install(
                    &a,
                    token,
                    changed,
                    AuthorizationScopeReceipt {
                        authority: changed.authority,
                        link: changed.link,
                        authority_epoch: changed.connection_epoch,
                        claims_revision: changed.claims_revision,
                        policy_epoch: changed.policy_epoch,
                        authorization_progress: changed.authorization_progress,
                        settled_through: GlobalTime(changed.settled_through),
                        ..r(k(1))
                    }
                ),
                AuthorizationScopeInstall::Installed
            );
        }
    }
    #[test]
    fn no_concurrent_duplicate_owners() {
        let x = AuthorizationScopeRegistry::default();
        let (a, _) = x.acquire(k(1)).unwrap();
        let (b, _) = x.acquire(k(1)).unwrap();
        let (c_lease, _) = x.acquire(k(1)).unwrap();
        drop(a);
        assert!(x.take_promotion(&b).is_some());
        assert!(x.take_promotion(&c_lease).is_none());
    }
    #[test]
    fn foreign_lease_cannot_install_or_read_a_receipt() {
        let x = AuthorizationScopeRegistry::default();
        let y = AuthorizationScopeRegistry::default();
        let (x_lease, x_acquisition) = x.acquire(k(1)).unwrap();
        let (y_lease, y_acquisition) = y.acquire(k(1)).unwrap();
        assert_eq!(
            x.install(&y_lease, owner(y_acquisition), c(), r(k(1))),
            AuthorizationScopeInstall::Rejected
        );
        assert!(matches!(
            x.receipt(&y_lease, c(), 1, 1),
            AuthorizationScopeReadiness::Waiting
        ));
        assert_eq!(
            x.install(&x_lease, owner(x_acquisition), c(), r(k(1))),
            AuthorizationScopeInstall::Installed
        );
    }
    #[test]
    fn capacity() {
        let x = AuthorizationScopeRegistry::default();
        let v = (0..MAX_AUTHORIZATION_SCOPES)
            .map(|i| x.acquire(indexed_key(i)).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(x.len(), MAX_AUTHORIZATION_SCOPES);
        assert!(x.acquire(indexed_key(MAX_AUTHORIZATION_SCOPES)).is_none());
        drop(v)
    }
}
