//! Public account replay (`E2EE_PROTOCOL.md` §2.4, §2.9 "Validity", §2.11).
//!
//! Replay is a pure function of one account's public rows at one cut. A
//! record that fails a check is skipped; a false signature verification is a
//! failed check. A provider failure, an unsupported capability and an
//! intrinsic contradiction stop replay with their own error.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};

use crate::registry::Registry;

use super::super::domain::{AccountDomain, AppDomain};
use super::super::error::{
    IntrinsicContradiction, ProtocolError, ProtocolResult, RootRejection, UnsupportedCapability,
};
use super::super::json;
use super::super::local_first;
use super::super::position::{Accepted, Position};
use super::super::records::KeyColumns;
use super::super::records::account::{
    AccountAction, AccountKind, AccountRoot, DeviceKeysRow, LEDGER_VERSION, PublicAccountSuccessor,
    PublicDeviceApproval, RecoveryRoot,
};
use super::super::signing;
use super::ledger::PublicAccountLedger;

/// One epoch of the account's current chain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountEpoch {
    pub epoch_id: String,
    pub position: Position,
    /// The accepted successor that opened this epoch; `None` for the root
    /// epoch.
    pub successor: Option<Accepted<PublicAccountSuccessor>>,
    /// The devices the epoch starts with: the root device, or the
    /// successor's membership.
    pub members: BTreeSet<String>,
}

/// A root that does not count, with the reason (§2.4 step 1).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RejectedRoot {
    pub root_id: String,
    pub position: Position,
    pub reason: RootRejection,
}

/// The roots of one view: the counting root of each generation of the
/// chain, and the roots that do not count.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RootSelection {
    /// The canonical root of generation 0, 1, …; the last is the current
    /// root. Empty when the account has no counting root.
    pub chain: Vec<Accepted<AccountRoot>>,
    pub rejected: Vec<RejectedRoot>,
}

/// An account's verified state at a cut (or as of a position).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountState {
    pub account_id: String,
    /// The canonical root's generation (§2.11).
    pub generation: u64,
    pub root: Accepted<AccountRoot>,
    /// The canonical root of every generation up to the current one.
    pub roots: Vec<Accepted<AccountRoot>>,
    /// Roots of this view that do not count, for reporting.
    pub rejected_roots: Vec<RejectedRoot>,
    /// The epoch chain from the canonical root; the last is current.
    pub epochs: Vec<AccountEpoch>,
    /// Active devices of the current epoch.
    pub active: BTreeSet<String>,
    /// Devices removed in this generation; never reactivated.
    pub removed: BTreeSet<String>,
    /// Public approvals replay accepted, of every epoch, by position then ID.
    pub approvals: Vec<Accepted<PublicDeviceApproval>>,
    /// Valid, unretired recovery roots, by position then ID.
    pub recovery_roots: Vec<Accepted<RecoveryRoot>>,
    /// Valid roots retired by an accepted successor, with its position.
    pub retired_recovery_roots: BTreeMap<String, Position>,
    /// Device keys by device ID, as of the cut.
    pub keys: BTreeMap<String, KeyColumns>,
}

impl AccountState {
    pub fn current_epoch(&self) -> &AccountEpoch {
        self.epochs.last().expect("a state has its root epoch")
    }

    pub fn epoch_id(&self) -> &str {
        &self.current_epoch().epoch_id
    }

    pub fn is_active(&self, device_id: &str) -> bool {
        self.active.contains(device_id)
    }

    /// Whether `device_id` is active with account epoch `epoch_id`: the
    /// author check of every signed governance record.
    pub fn is_active_in(&self, device_id: &str, epoch_id: &str) -> bool {
        self.is_active(device_id) && self.epoch_id() == epoch_id
    }

    pub fn is_removed(&self, device_id: &str) -> bool {
        self.removed.contains(device_id)
    }

    pub fn recovery_root(&self, root_id: &str) -> Option<&Accepted<RecoveryRoot>> {
        self.recovery_roots
            .iter()
            .find(|root| root.row.id == root_id)
    }

    /// What a device keeps of this state to refuse older evidence later
    /// (§2.4, "Floors"): the read's cut, the generation, the root of that
    /// generation and the current epoch. `own_device` is this device's ID
    /// when the state is of its own account; the floor records it if it is
    /// active.
    pub fn floor(&self, through: Position, own_device: Option<&str>) -> AccountFloor {
        let epoch = self.current_epoch();
        AccountFloor {
            through,
            generation: self.generation,
            root_id: self.root.row.id.clone(),
            epoch_id: epoch.epoch_id.clone(),
            epoch_position: epoch.position,
            active_device: own_device
                .filter(|device| self.is_active(device))
                .map(str::to_owned),
        }
    }

    /// Checks this state, replayed from a read at cut `through`, against a
    /// floor the device persisted earlier for the same account (§2.4,
    /// "Floors"), in this order:
    ///
    /// 1. Evidence older than the floor (an earlier cut, an earlier
    ///    generation, or, at the floor's generation, the floor's epoch
    ///    missing from the chain) is [`ProtocolError::StaleState`].
    /// 2. A chain whose root of the floor's generation is not the floor's
    ///    root is [`ProtocolError::RootNotMine`]: the root this device
    ///    verified was replaced.
    /// 3. When the floor records this device as active and it is not
    ///    active in this state (it was removed, or the account moved to a
    ///    generation it is not part of), the outcome is
    ///    [`ProtocolError::DeviceNoLongerActive`]: the device re-enrols. A
    ///    device that was offline while it was removed and the account was
    ///    then reset sees exactly this, so it is not a security alert.
    pub fn check_floor(&self, through: Position, floor: &AccountFloor) -> ProtocolResult<()> {
        if self.account_id.is_empty()
            || through < floor.through
            || self.generation < floor.generation
        {
            return Err(ProtocolError::StaleState);
        }
        let root = usize::try_from(floor.generation)
            .ok()
            .and_then(|generation| self.roots.get(generation));
        if root.is_none_or(|root| root.row.id != floor.root_id) {
            return Err(ProtocolError::RootNotMine);
        }
        if self.generation == floor.generation
            && !self.epochs.iter().any(|epoch| {
                epoch.epoch_id == floor.epoch_id && epoch.position == floor.epoch_position
            })
        {
            return Err(ProtocolError::StaleState);
        }
        if floor
            .active_device
            .as_deref()
            .is_some_and(|device| !self.is_active(device))
        {
            return Err(ProtocolError::DeviceNoLongerActive);
        }
        Ok(())
    }

    /// For a device holding the account's local-first identity: the
    /// canonical root must be a `local-first` root of that identity's key,
    /// or the root is not the account's own ([`ProtocolError::RootNotMine`]).
    pub fn check_local_first(
        &self,
        identity: &local_first::LocalFirstIdentity,
    ) -> ProtocolResult<()> {
        let own = self.root.row.kind == AccountKind::LocalFirst
            && self.root.row.local_first_public_key.as_deref() == Some(identity.public_key());
        if own {
            Ok(())
        } else {
            Err(ProtocolError::RootNotMine)
        }
    }
}

/// The newest account state a device has verified (§2.4, "Floors"), kept in
/// its private store. A later read is refused if it is older.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountFloor {
    pub through: Position,
    pub generation: u64,
    /// The canonical root of `generation`.
    pub root_id: String,
    pub epoch_id: String,
    pub epoch_position: Position,
    /// This device, if the floor is of its own account and it was active.
    pub active_device: Option<String>,
}

/// Replays one account's ledger read, and its states as of earlier
/// positions. Recovery root validity and each state as of a position are
/// computed once and kept for the life of the replay, so checking many
/// records' authors against one account replays each prefix once.
pub struct AccountReplay<'a> {
    ledger: &'a PublicAccountLedger,
    registry: &'a Registry,
    application: AccountDomain,
    domain: AppDomain,
    recovery_validity: RefCell<BTreeMap<String, bool>>,
    root_checks: RefCell<BTreeMap<String, Option<RootRejection>>>,
    local_first_key: RefCell<Option<Option<Vec<u8>>>>,
    states: RefCell<BTreeMap<Position, Option<AccountState>>>,
}

fn by_position_then_id<T: super::super::position::RowId>(rows: &mut [&Accepted<T>]) {
    rows.sort_by(|a, b| {
        a.position
            .cmp(&b.position)
            .then_with(|| json::utf16_cmp(a.row.row_id(), b.row.row_id()))
    });
}

/// A successor that passed every check but the same-transaction rule.
struct ValidSuccessor {
    candidate: Accepted<PublicAccountSuccessor>,
    accepted: Vec<Accepted<PublicDeviceApproval>>,
    target: String,
    also_retired: Vec<String>,
    members: BTreeSet<String>,
}

/// The set of devices active within one epoch, built batch by batch.
struct EpochMembers {
    active: BTreeSet<String>,
    accepted: Vec<Accepted<PublicDeviceApproval>>,
}

impl<'a> AccountReplay<'a> {
    pub fn new(
        ledger: &'a PublicAccountLedger,
        domain: &AppDomain,
        registry: &'a Registry,
    ) -> ProtocolResult<Self> {
        ledger.check()?;
        Ok(Self {
            application: domain.account(&ledger.account_id)?,
            domain: domain.clone(),
            ledger,
            registry,
            recovery_validity: RefCell::new(BTreeMap::new()),
            root_checks: RefCell::new(BTreeMap::new()),
            local_first_key: RefCell::new(None),
            states: RefCell::new(BTreeMap::new()),
        })
    }

    /// The account domain the replayed records are signed in.
    pub fn application(&self) -> &AccountDomain {
        &self.application
    }

    pub fn ledger(&self) -> &PublicAccountLedger {
        self.ledger
    }

    pub fn registry(&self) -> &Registry {
        self.registry
    }

    /// The current state at the cut, or `None` if the account has no counting
    /// root.
    pub fn current(&self) -> ProtocolResult<Option<AccountState>> {
        self.replay(self.ledger)
    }

    /// The state as of `position`: replay over the rows accepted strictly
    /// before it, with the generation chosen among the roots before it. A
    /// position beyond the cut (later than `through + 1`) needs rows this
    /// read does not have, and is [`ProtocolError::InconsistentEvidence`].
    pub fn as_of(&self, position: Position) -> ProtocolResult<Option<AccountState>> {
        if position.0 > self.ledger.through.0.saturating_add(1) {
            return Err(ProtocolError::InconsistentEvidence);
        }
        if let Some(state) = self.states.borrow().get(&position) {
            return Ok(state.clone());
        }
        let state = self.replay(&self.ledger.before(position))?;
        self.states.borrow_mut().insert(position, state.clone());
        Ok(state)
    }

    /// Whether `device_id` was active with account epoch `epoch_id` at
    /// `position`.
    pub fn author_active_at(
        &self,
        position: Position,
        device_id: &str,
        epoch_id: &str,
    ) -> ProtocolResult<bool> {
        Ok(self
            .as_of(position)?
            .is_some_and(|state| state.is_active_in(device_id, epoch_id)))
    }

    /// The account's generation as of `position`, if it had a root then.
    pub fn generation_at(&self, position: Position) -> ProtocolResult<Option<u64>> {
        Ok(self.as_of(position)?.map(|state| state.generation))
    }

    /// The canonical root at the cut, if the account has a counting root.
    pub fn canonical_root(&self) -> ProtocolResult<Option<Accepted<AccountRoot>>> {
        Ok(self.select_roots(self.ledger)?.chain.pop())
    }

    /// The roots at the cut that do not count, with their reasons.
    pub fn rejected_roots(&self) -> ProtocolResult<Vec<RejectedRoot>> {
        Ok(self.select_roots(self.ledger)?.rejected)
    }

    /// The local-first key of the account, if the cut has a valid
    /// `local-first` root (the earliest one's key). Decided once over the
    /// whole read, so every view of this replay agrees on the account's
    /// kind.
    fn account_local_first_key(&self) -> ProtocolResult<Option<Vec<u8>>> {
        if let Some(key) = self.local_first_key.borrow().as_ref() {
            return Ok(key.clone());
        }
        let mut roots: Vec<_> = self
            .ledger
            .roots
            .iter()
            .filter(|root| root.row.kind == AccountKind::LocalFirst)
            .collect();
        by_position_then_id(&mut roots);
        let mut found = None;
        for root in roots {
            if self.local_first_check(root)?.is_none() {
                found = root.row.local_first_public_key.clone();
                break;
            }
        }
        *self.local_first_key.borrow_mut() = Some(found.clone());
        Ok(found)
    }

    /// The checks every root needs before its kind's own: well formed, of
    /// this account, and supported.
    fn common_root_check(&self, root: &AccountRoot) -> Option<RootRejection> {
        if root.validate().is_err() {
            Some(RootRejection::Malformed)
        } else if root.account_id != self.ledger.account_id {
            Some(RootRejection::ForeignAccount)
        } else {
            None
        }
    }

    /// A `local-first` root's own checks: its key derives the account ID,
    /// and its signature over the root and its device's keys verifies.
    fn local_first_check(
        &self,
        root: &Accepted<AccountRoot>,
    ) -> ProtocolResult<Option<RootRejection>> {
        if let Some(rejection) = self.common_root_check(&root.row) {
            return Ok(Some(rejection));
        }
        let (Some(public_key), Some(signature)) = (
            root.row.local_first_public_key.as_deref(),
            root.row.local_first_signature.as_deref(),
        ) else {
            return Ok(Some(RootRejection::Malformed));
        };
        if !local_first::derives_account(&self.domain, public_key, &root.row.account_id) {
            return Ok(Some(RootRejection::KeyMismatch));
        }
        // The signature binds the root device's published keys, accepted
        // at or before the root. A root naming a device without them cannot
        // verify, so it is rejected like any other row that fails its own
        // check; only a valid root can be a contradiction.
        let Some(device) = root_device_keys(self.ledger, root)? else {
            return Ok(Some(RootRejection::DeviceKeysMissing));
        };
        let Ok(bytes) = root.row.signed_bytes(&self.application, &device) else {
            return Ok(Some(RootRejection::Malformed));
        };
        if local_first::verify(self.registry, public_key, &bytes, signature)? {
            Ok(None)
        } else {
            Ok(Some(RootRejection::SignatureInvalid))
        }
    }

    /// Whether `root` may count (§2.4 step 1), memoised by root ID. Checked
    /// against the whole read, so the answer is the same in every view.
    fn root_check(&self, root: &Accepted<AccountRoot>) -> ProtocolResult<Option<RootRejection>> {
        if let Some(check) = self.root_checks.borrow().get(&root.row.id) {
            return Ok(*check);
        }
        let check = match root.row.kind {
            AccountKind::LocalFirst => {
                let check = self.local_first_check(root)?;
                if check.is_none()
                    && self.account_local_first_key()?.as_deref()
                        != root.row.local_first_public_key.as_deref()
                {
                    // Two keys deriving one account ID: not reachable
                    // without a SHA-1 collision of the derivation.
                    return Err(ProtocolError::Intrinsic(
                        IntrinsicContradiction::ConflictingRoots,
                    ));
                }
                check
            }
            AccountKind::App => match self.common_root_check(&root.row) {
                Some(rejection) => Some(rejection),
                None if self.account_local_first_key()?.is_some() => {
                    Some(RootRejection::KindMismatch)
                }
                None => None,
            },
        };
        self.root_checks
            .borrow_mut()
            .insert(root.row.id.clone(), check);
        Ok(check)
    }

    /// The root chain of `view` (§2.4 step 1). Only roots that pass every
    /// check count. Generation 0's counting roots are all valid roots of
    /// generation 0; generation `g > 0`'s are the valid roots of generation
    /// `g` accepted strictly after the canonical root of `g − 1`. The
    /// canonical root of each generation is its earliest counting root (ties
    /// by smallest ID), and any other counting root of that generation that
    /// differs in device or epoch is a contradiction, at every generation of
    /// the chain, so current replay and every state as of a later position
    /// agree. A root with another ledger version is an unsupported
    /// capability, valid or not.
    pub fn select_roots(&self, view: &PublicAccountLedger) -> ProtocolResult<RootSelection> {
        if view
            .roots
            .iter()
            .any(|root| root.row.ledger_version != LEDGER_VERSION)
        {
            return Err(ProtocolError::Unsupported(
                UnsupportedCapability::LedgerVersion,
            ));
        }
        let mut roots: Vec<_> = view.roots.iter().collect();
        by_position_then_id(&mut roots);
        let mut rejected = Vec::new();
        let mut valid = Vec::new();
        for root in roots {
            match self.root_check(root)? {
                Some(reason) => rejected.push(RejectedRoot {
                    root_id: root.row.id.clone(),
                    position: root.position,
                    reason,
                }),
                None => valid.push(root),
            }
        }
        let mut chain: Vec<Accepted<AccountRoot>> = Vec::new();
        let mut counted = BTreeSet::new();
        let mut generation = 0u64;
        loop {
            let floor = chain.last().map(|root| root.position);
            let counting: Vec<_> = valid
                .iter()
                .copied()
                .filter(|root| {
                    root.row.generation == generation && floor.is_none_or(|f| root.position > f)
                })
                .collect();
            let Some(canonical) = counting.first() else {
                break;
            };
            if counting.iter().any(|root| {
                root.row.device_id != canonical.row.device_id
                    || root.row.epoch_id != canonical.row.epoch_id
            }) {
                return Err(ProtocolError::Intrinsic(
                    IntrinsicContradiction::ConflictingRoots,
                ));
            }
            counted.extend(counting.iter().map(|root| root.row.id.clone()));
            chain.push((*canonical).clone());
            let Some(next) = generation.checked_add(1) else {
                break;
            };
            generation = next;
        }
        for root in valid {
            if !counted.contains(&root.row.id) {
                rejected.push(RejectedRoot {
                    root_id: root.row.id.clone(),
                    position: root.position,
                    reason: RootRejection::Unchained,
                });
            }
        }
        rejected.sort_by(|a, b| {
            a.position
                .cmp(&b.position)
                .then_with(|| json::utf16_cmp(&a.root_id, &b.root_id))
        });
        Ok(RootSelection { chain, rejected })
    }

    /// Whether a successor `candidate` of `epoch` passes every check of
    /// §2.4 step 3 except the same-transaction rule, which the caller
    /// applies among the candidates that pass. Returns the members and the
    /// target on success.
    fn successor_valid(
        &self,
        run: &Run<'_, '_>,
        epoch: &AccountEpoch,
        used_epochs: &BTreeSet<String>,
        earlier_approvals: &[Accepted<PublicDeviceApproval>],
        candidate: &Accepted<PublicAccountSuccessor>,
    ) -> ProtocolResult<Option<ValidSuccessor>> {
        let successor = &candidate.row;
        let position = candidate.position;
        if successor.account_id != run.account_id
            || successor.predecessor != epoch.epoch_id
            || position <= epoch.position
            || used_epochs.contains(&successor.epoch_id)
        {
            return Ok(None);
        }
        let Ok(membership) = successor.validate() else {
            return Ok(None);
        };
        let Ok(bytes) = successor.signed_bytes(&self.application) else {
            return Ok(None);
        };
        let members = run.members(epoch, Some(position))?;
        let revision = json::encode_revision(members.accepted.iter().map(|a| &a.row.id));
        if revision.as_deref() != Ok(successor.revision.as_slice()) {
            return Ok(None);
        }
        if !members.active.contains(&successor.signer_id) {
            return Ok(None);
        }
        let Ok((target, also_retired)) = successor.targets() else {
            return Ok(None);
        };
        let mut expected = members.active.clone();
        match successor.action {
            AccountAction::RemoveDevice => {
                if !expected.remove(&target) {
                    return Ok(None);
                }
                // In an app account, removing a device retires every still
                // active root it held the key pair of: the roots it
                // registered and the roots it recovered with (§2.8).
                let required =
                    run.held_roots(&target, position, earlier_approvals, &members.accepted)?;
                if !also_retired.iter().eq(required.iter()) {
                    return Ok(None);
                }
            }
            AccountAction::RetireRecoveryRoot => {
                let Some(root) = run
                    .view
                    .recovery_roots
                    .iter()
                    .find(|root| root.row.id == target)
                else {
                    return Ok(None);
                };
                if root.position >= position
                    || run.retired.contains_key(&target)
                    || !run.recovery_valid(root)?
                {
                    return Ok(None);
                }
            }
        }
        if !run.verify_device(&successor.signer_id, &bytes, &successor.signature)? {
            return Ok(None);
        }
        if membership.len() != expected.len()
            || membership.iter().any(|device| !expected.contains(device))
        {
            return Ok(None);
        }
        Ok(Some(ValidSuccessor {
            candidate: candidate.clone(),
            accepted: members.accepted,
            target,
            also_retired,
            members: expected,
        }))
    }

    fn replay(&self, view: &PublicAccountLedger) -> ProtocolResult<Option<AccountState>> {
        let RootSelection {
            chain: roots,
            rejected: rejected_roots,
        } = self.select_roots(view)?;
        let Some(root) = roots.last().cloned() else {
            return Ok(None);
        };
        let keys = device_keys(view)?;
        if !keys.contains_key(&root.row.device_id) {
            return Err(ProtocolError::Intrinsic(
                IntrinsicContradiction::MissingRootKey,
            ));
        }
        let mut run = Run {
            replay: self,
            view,
            keys: &keys,
            account_id: &view.account_id,
            kind: root.row.kind,
            root_floor: root.position,
            removed: BTreeSet::new(),
            retired: BTreeMap::new(),
        };
        let mut epochs = vec![AccountEpoch {
            epoch_id: root.row.epoch_id.clone(),
            position: root.position,
            successor: None,
            members: BTreeSet::from([root.row.device_id.clone()]),
        }];
        let mut approvals = Vec::new();
        let mut used_epochs = BTreeSet::from([root.row.epoch_id.clone()]);

        let mut successors: Vec<_> = view.successors.iter().collect();
        by_position_then_id(&mut successors);
        // Successors are taken one transaction (position) at a time. Rows
        // accepted at one position were written in one transaction, which
        // only the account itself or the server can produce: if more than one
        // valid successor of the current epoch, or a successor and a valid
        // recovery root, share it, the order between them is undefined and
        // none of the successors is accepted. Invalid rows never take part.
        let mut index = 0;
        while index < successors.len() {
            let position = successors[index].position;
            let end = successors[index..]
                .iter()
                .position(|candidate| candidate.position != position)
                .map_or(successors.len(), |offset| index + offset);
            let batch = &successors[index..end];
            index = end;
            let epoch = epochs.last().expect("root epoch").clone();
            let mut valid = Vec::new();
            for candidate in batch {
                if let Some(found) =
                    self.successor_valid(&run, &epoch, &used_epochs, &approvals, candidate)?
                {
                    valid.push(found);
                }
            }
            if valid.len() != 1 {
                continue;
            }
            let mut tied = false;
            for root in view
                .recovery_roots
                .iter()
                .filter(|root| root.position == position)
            {
                if run.recovery_valid(root)? {
                    tied = true;
                    break;
                }
            }
            if tied {
                continue;
            }
            let ValidSuccessor {
                candidate,
                accepted,
                target,
                also_retired,
                members,
            } = valid.pop().expect("one valid successor");
            approvals.extend(accepted);
            match candidate.row.action {
                AccountAction::RemoveDevice => {
                    run.removed.insert(target);
                    for root in also_retired {
                        run.retired.insert(root, position);
                    }
                }
                AccountAction::RetireRecoveryRoot => {
                    run.retired.insert(target, position);
                }
            }
            used_epochs.insert(candidate.row.epoch_id.clone());
            epochs.push(AccountEpoch {
                epoch_id: candidate.row.epoch_id.clone(),
                position,
                successor: Some(candidate),
                members,
            });
        }

        let current = run.members(epochs.last().expect("root epoch"), None)?;
        approvals.extend(current.accepted);
        approvals.sort_by(|a, b| {
            a.position
                .cmp(&b.position)
                .then_with(|| json::utf16_cmp(&a.row.id, &b.row.id))
        });

        let mut recovery: Vec<_> = view.recovery_roots.iter().collect();
        by_position_then_id(&mut recovery);
        let mut recovery_roots = Vec::new();
        for candidate in recovery {
            if !run.retired.contains_key(&candidate.row.id) && run.recovery_valid(candidate)? {
                recovery_roots.push(candidate.clone());
            }
        }

        Ok(Some(AccountState {
            account_id: view.account_id.clone(),
            generation: root.row.generation,
            root,
            roots,
            rejected_roots,
            epochs,
            active: current.active,
            removed: run.removed,
            approvals,
            recovery_roots,
            retired_recovery_roots: run.retired,
            keys,
        }))
    }

    /// Validity of a recovery root by itself (§2.9): its signer was active
    /// with account epoch `epochId` at its position, with a supported signing
    /// mechanism, and its signature verifies. Memoised by root ID.
    fn recovery_valid_at_registration(
        &self,
        root: &Accepted<RecoveryRoot>,
    ) -> ProtocolResult<bool> {
        if let Some(valid) = self.recovery_validity.borrow().get(&root.row.id) {
            return Ok(*valid);
        }
        let valid = self.check_recovery_root(root)?;
        self.recovery_validity
            .borrow_mut()
            .insert(root.row.id.clone(), valid);
        Ok(valid)
    }

    fn check_recovery_root(&self, root: &Accepted<RecoveryRoot>) -> ProtocolResult<bool> {
        if root.row.account_id != self.ledger.account_id {
            return Ok(false);
        }
        if root.row.validate().is_err() {
            return Ok(false);
        }
        let Ok(bytes) = root.row.signed_bytes(&self.application) else {
            return Ok(false);
        };
        let Some(state) = self.as_of(root.position)? else {
            return Ok(false);
        };
        if !state.is_active_in(&root.row.signer_id, &root.row.epoch_id) {
            return Ok(false);
        }
        let key = state
            .keys
            .get(&root.row.signer_id)
            .ok_or(ProtocolError::Intrinsic(
                IntrinsicContradiction::MissingSignerKey,
            ))?;
        signing::verify(
            self.registry,
            &key.signing_key(),
            &bytes,
            &root.row.signature,
        )
    }
}

/// The mutable part of one replay.
struct Run<'r, 'a> {
    replay: &'r AccountReplay<'a>,
    view: &'r PublicAccountLedger,
    keys: &'r BTreeMap<String, KeyColumns>,
    account_id: &'r str,
    kind: AccountKind,
    /// The canonical root's position: rows at or before it are of an
    /// earlier history.
    root_floor: Position,
    removed: BTreeSet<String>,
    retired: BTreeMap<String, Position>,
}

impl Run<'_, '_> {
    /// The recovery roots a removal of `device` at `position` must retire
    /// with it (§2.8): in an app account, every valid root of this
    /// generation, unretired before `position`, whose key pair the device
    /// held: each root it registered before `position`, and each root named
    /// by any of its accepted recovery approvals. Empty for a local-first
    /// account, whose roots derive from the secret every device holds.
    fn held_roots(
        &self,
        device: &str,
        position: Position,
        earlier: &[Accepted<PublicDeviceApproval>],
        current: &[Accepted<PublicDeviceApproval>],
    ) -> ProtocolResult<BTreeSet<String>> {
        let mut held = BTreeSet::new();
        if self.kind != AccountKind::App {
            return Ok(held);
        }
        let recovered: BTreeSet<&str> = earlier
            .iter()
            .chain(current)
            .filter(|approval| approval.row.device_id == device)
            .filter_map(|approval| approval.row.recovery_root_id.as_deref())
            .collect();
        for root in &self.view.recovery_roots {
            let id = root.row.id.as_str();
            let registered = root.row.signer_id == device && root.position < position;
            if (registered || recovered.contains(id))
                && !self.retired.contains_key(id)
                && self.recovery_valid(root)?
            {
                held.insert(root.row.id.clone());
            }
        }
        Ok(held)
    }

    /// A root valid in this view: strictly after the canonical root (so of
    /// this generation) and valid at its registration.
    fn recovery_valid(&self, root: &Accepted<RecoveryRoot>) -> ProtocolResult<bool> {
        if root.position <= self.root_floor {
            return Ok(false);
        }
        self.replay.recovery_valid_at_registration(root)
    }

    /// Verifies a device signature with the device's published signing key.
    fn verify_device(
        &self,
        device_id: &str,
        bytes: &[u8],
        signature: &[u8],
    ) -> ProtocolResult<bool> {
        let key = self.keys.get(device_id).ok_or(ProtocolError::Intrinsic(
            IntrinsicContradiction::MissingSignerKey,
        ))?;
        signing::verify(self.replay.registry, &key.signing_key(), bytes, signature)
    }

    /// The active set of `epoch` before `cut` (or at the cut): its members
    /// plus the public approvals of the epoch strictly after its position,
    /// applied in batches of equal position (§2.4 step 2).
    fn members(&self, epoch: &AccountEpoch, cut: Option<Position>) -> ProtocolResult<EpochMembers> {
        let mut active = epoch.members.clone();
        let mut accepted = Vec::new();
        let mut candidates: Vec<_> = self
            .view
            .approvals
            .iter()
            .filter(|approval| {
                approval.row.account_id == self.account_id
                    && approval.row.epoch_id == epoch.epoch_id
                    && approval.position > epoch.position
                    && cut.is_none_or(|cut| approval.position < cut)
            })
            .collect();
        by_position_then_id(&mut candidates);
        let mut batch: Option<Position> = None;
        let mut before_batch = active.clone();
        for candidate in candidates {
            if batch != Some(candidate.position) {
                batch = Some(candidate.position);
                before_batch = active.clone();
            }
            if self.accept_approval(candidate, &before_batch)? {
                active.insert(candidate.row.device_id.clone());
                accepted.push(candidate.clone());
            }
        }
        Ok(EpochMembers { active, accepted })
    }

    fn accept_approval(
        &self,
        candidate: &Accepted<PublicDeviceApproval>,
        before_batch: &BTreeSet<String>,
    ) -> ProtocolResult<bool> {
        let approval = &candidate.row;
        if self.removed.contains(&approval.device_id) {
            return Ok(false);
        }
        // Refuses, among others, a non-recovery approval carrying a recovery
        // signature.
        if approval.validate().is_err() {
            return Ok(false);
        }
        let Ok(bytes) = approval.signed_bytes(&self.replay.application) else {
            return Ok(false);
        };
        match (&approval.recovery_root_id, &approval.recovery_signature) {
            (Some(root_id), Some(recovery_signature)) => {
                let Some(root) = self
                    .view
                    .recovery_roots
                    .iter()
                    .find(|root| &root.row.id == root_id)
                else {
                    return Ok(false);
                };
                let retired = self
                    .retired
                    .get(root_id)
                    .is_some_and(|at| *at <= candidate.position);
                if root.position >= candidate.position || retired || !self.recovery_valid(root)? {
                    return Ok(false);
                }
                let recovery_key = root.row.keys.signing_key();
                if !signing::verify(
                    self.replay.registry,
                    &recovery_key,
                    &bytes,
                    recovery_signature,
                )? {
                    return Ok(false);
                }
            }
            _ => {
                if !before_batch.contains(&approval.signer_id) {
                    return Ok(false);
                }
            }
        }
        if !self.verify_device(&approval.signer_id, &bytes, &approval.signature)? {
            return Ok(false);
        }
        if !self.keys.contains_key(&approval.device_id) {
            return Err(ProtocolError::Intrinsic(
                IntrinsicContradiction::MissingRecipientKey,
            ));
        }
        Ok(true)
    }
}

/// The keys of `root`'s device accepted at or before the root, if any. Two
/// rows of the device with different keys are a contradiction.
fn root_device_keys(
    view: &PublicAccountLedger,
    root: &Accepted<AccountRoot>,
) -> ProtocolResult<Option<KeyColumns>> {
    let mut keys =
        collect_keys(view.keys.iter().filter(|row| {
            row.position <= root.position && row.row.device_id == root.row.device_id
        }))?;
    Ok(keys.remove(&root.row.device_id))
}

/// The account's device keys by device ID. Two rows of one device with
/// different keys are a contradiction.
fn device_keys(view: &PublicAccountLedger) -> ProtocolResult<BTreeMap<String, KeyColumns>> {
    collect_keys(view.keys.iter())
}

pub(super) fn collect_keys<'a>(
    rows: impl Iterator<Item = &'a Accepted<DeviceKeysRow>>,
) -> ProtocolResult<BTreeMap<String, KeyColumns>> {
    let mut keys: BTreeMap<String, KeyColumns> = BTreeMap::new();
    for row in rows {
        if row.row.validate().is_err() {
            continue;
        }
        match keys.get(&row.row.device_id) {
            Some(existing) if *existing != row.row.keys => {
                return Err(ProtocolError::Intrinsic(
                    IntrinsicContradiction::ConflictingDeviceKeys,
                ));
            }
            Some(_) => {}
            None => {
                keys.insert(row.row.device_id.clone(), row.row.keys.clone());
            }
        }
    }
    Ok(keys)
}
