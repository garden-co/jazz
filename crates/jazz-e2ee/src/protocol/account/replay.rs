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
    IntrinsicContradiction, ProtocolError, ProtocolResult, UnsupportedCapability,
};
use super::super::json;
use super::super::position::{Accepted, Position};
use super::super::records::KeyColumns;
use super::super::records::account::{
    AccountAction, AccountRoot, LEDGER_VERSION, PublicAccountSuccessor, PublicDeviceApproval,
    RecoveryRoot,
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

/// An account's verified state at a cut (or as of a position).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AccountState {
    pub account_id: String,
    /// The canonical root's generation (§2.11).
    pub generation: u64,
    pub root: Accepted<AccountRoot>,
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
}

/// Replays one account's ledger read, and its states as of earlier
/// positions. Recovery root validity and each state as of a position are
/// computed once and kept for the life of the replay, so checking many
/// records' authors against one account replays each prefix once.
pub struct AccountReplay<'a> {
    ledger: &'a PublicAccountLedger,
    registry: &'a Registry,
    application: AccountDomain,
    recovery_validity: RefCell<BTreeMap<String, bool>>,
    states: RefCell<BTreeMap<Position, Option<AccountState>>>,
}

fn by_position_then_id<T: super::super::position::RowId>(rows: &mut [&Accepted<T>]) {
    rows.sort_by(|a, b| {
        a.position
            .cmp(&b.position)
            .then_with(|| json::utf16_cmp(a.row.row_id(), b.row.row_id()))
    });
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
            ledger,
            registry,
            recovery_validity: RefCell::new(BTreeMap::new()),
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

    fn replay(&self, view: &PublicAccountLedger) -> ProtocolResult<Option<AccountState>> {
        let Some(root) = canonical_root(view)? else {
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
        for candidate in successors {
            let epoch = epochs.last().expect("root epoch");
            let successor = &candidate.row;
            let position = candidate.position;
            if successor.account_id != view.account_id
                || successor.predecessor != epoch.epoch_id
                || position <= epoch.position
                || used_epochs.contains(&successor.epoch_id)
            {
                continue;
            }
            // Concurrent successors of one predecessor, or a successor
            // accepted with a recovery root, are unordered: neither wins.
            let tied = view.successors.iter().any(|other| {
                other.row.id != successor.id
                    && other.row.predecessor == successor.predecessor
                    && other.position == position
            }) || view
                .recovery_roots
                .iter()
                .any(|root| root.position == position);
            if tied {
                continue;
            }
            let Ok(membership) = successor.validate() else {
                continue;
            };
            let Ok(bytes) = successor.signed_bytes(&self.application) else {
                continue;
            };
            let members = run.members(epoch, Some(position))?;
            let revision = json::encode_revision(members.accepted.iter().map(|a| &a.row.id));
            if revision.as_deref() != Ok(successor.revision.as_slice()) {
                continue;
            }
            if !members.active.contains(&successor.signer_id) {
                continue;
            }
            let Ok(target) = successor.target_id().map(str::to_owned) else {
                continue;
            };
            let mut expected = members.active.clone();
            match successor.action {
                AccountAction::RemoveDevice => {
                    if !expected.remove(&target) {
                        continue;
                    }
                }
                AccountAction::RetireRecoveryRoot => {
                    let Some(root) = view
                        .recovery_roots
                        .iter()
                        .find(|root| root.row.id == target)
                    else {
                        continue;
                    };
                    if root.position >= position
                        || run.retired.contains_key(&target)
                        || !run.recovery_valid(root)?
                    {
                        continue;
                    }
                }
            }
            if !run.verify_device(&successor.signer_id, &bytes, &successor.signature)? {
                continue;
            }
            if membership.len() != expected.len()
                || membership.iter().any(|device| !expected.contains(device))
            {
                continue;
            }
            approvals.extend(members.accepted);
            match successor.action {
                AccountAction::RemoveDevice => {
                    run.removed.insert(target);
                }
                AccountAction::RetireRecoveryRoot => {
                    run.retired.insert(target, position);
                }
            }
            used_epochs.insert(successor.epoch_id.clone());
            epochs.push(AccountEpoch {
                epoch_id: successor.epoch_id.clone(),
                position,
                successor: Some(candidate.clone()),
                members: expected,
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
            root: root.clone(),
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
    /// The canonical root's position: rows at or before it are of an
    /// earlier history.
    root_floor: Position,
    removed: BTreeSet<String>,
    retired: BTreeMap<String, Position>,
}

impl Run<'_, '_> {
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

/// The canonical root (§2.4 step 1). A root of generation `g > 0` counts only
/// if a counting root of generation `g − 1` was accepted strictly before it.
/// Among counting roots of the highest generation, the earliest (ties by
/// smallest ID) is canonical; any other counting root of that generation
/// that differs in account, device or epoch is a contradiction.
pub fn canonical_root(
    view: &PublicAccountLedger,
) -> ProtocolResult<Option<&Accepted<AccountRoot>>> {
    if view
        .roots
        .iter()
        .any(|root| root.row.ledger_version != LEDGER_VERSION)
    {
        return Err(ProtocolError::Unsupported(
            UnsupportedCapability::LedgerVersion,
        ));
    }
    let mut roots: Vec<_> = view
        .roots
        .iter()
        .filter(|root| root.row.validate().is_ok())
        .collect();
    by_position_then_id(&mut roots);
    let mut generation = 0u64;
    let mut counting: Vec<&Accepted<AccountRoot>> = roots
        .iter()
        .copied()
        .filter(|root| root.row.generation == 0)
        .collect();
    if counting.is_empty() {
        return Ok(None);
    }
    loop {
        let floor = counting[0].position;
        let Some(next_generation) = generation.checked_add(1) else {
            break;
        };
        let next: Vec<_> = roots
            .iter()
            .copied()
            .filter(|root| root.row.generation == next_generation && root.position > floor)
            .collect();
        if next.is_empty() {
            break;
        }
        generation = next_generation;
        counting = next;
    }
    let canonical = counting[0];
    let contradicts = |root: &Accepted<AccountRoot>| {
        root.row.account_id != canonical.row.account_id
            || root.row.device_id != canonical.row.device_id
            || root.row.epoch_id != canonical.row.epoch_id
    };
    if canonical.row.account_id != view.account_id || counting.iter().any(|root| contradicts(root))
    {
        return Err(ProtocolError::Intrinsic(
            IntrinsicContradiction::ConflictingRoots,
        ));
    }
    Ok(Some(canonical))
}

/// The account's device keys by device ID. Two rows of one device with
/// different keys are a contradiction.
fn device_keys(view: &PublicAccountLedger) -> ProtocolResult<BTreeMap<String, KeyColumns>> {
    let mut keys: BTreeMap<String, KeyColumns> = BTreeMap::new();
    for row in &view.keys {
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
