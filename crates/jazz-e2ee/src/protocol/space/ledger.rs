//! One space's rows and the public account histories they name, from one
//! ledger read (`E2EE_PROTOCOL.md` §0 item 3, §1.3, §2.4 "State as of a
//! position").
//!
//! Every ledger must come from the same read: one cut for all. An account
//! whose public ledger the caller did not supply is
//! [`ProtocolError::InconsistentEvidence`]: evidence the replay needs was
//! left out, which is never read as "not active" or "no key". An account
//! whose history needs a capability this client lacks is
//! [`ProtocolError::Unsupported`]; the member helpers skip and report such
//! a member rather than fail for the whole space (§3.4).

use std::collections::{BTreeMap, BTreeSet};

use crate::envelope::Mechanism;
use crate::registry::Registry;

use super::super::account::{
    AccountEpochKeys, AccountReplay, AccountState, PublicAccountLedger, SealingKeys,
};
use super::super::domain::AppDomain;
use super::super::error::{IntrinsicContradiction, ProtocolError, ProtocolResult};
use super::super::ids::{self, IdForm};
use super::super::position::{Accepted, Position, check_read};
use super::super::records::account::AccountSealingKey;
use super::super::records::space::{
    SpaceAddress, SpaceDelivery, SpaceMember, SpaceRoot, SpaceSuccessor,
};
use super::super::signing;

/// The rows of one space at cut `through`: its member rows as the read sees
/// them, and its roots, successors and deliveries (R12, R14). Rows naming
/// another address are ignored by replay.
///
/// The read covers every delivery or only named epochs, and may also be
/// filtered for one account. A decision about all recipients of a version
/// requires all members and all deliveries for that epoch. Decisions about
/// the whole space require an unfiltered read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpaceLedger {
    pub address: SpaceAddress,
    pub through: Position,
    pub members: Vec<Accepted<SpaceMember>>,
    pub roots: Vec<Accepted<SpaceRoot>>,
    pub successors: Vec<Accepted<SpaceSuccessor>>,
    pub deliveries: Vec<Accepted<SpaceDelivery>>,
    kind: ReadKind,
}

/// What a space read covers.
#[derive(Debug, Clone, PartialEq, Eq)]
enum ReadKind {
    WholeSpace,
    OneAccount(String),
    Epochs {
        account: Option<String>,
        epochs: BTreeSet<String>,
    },
}

impl SpaceLedger {
    fn empty(address: SpaceAddress, through: Position, kind: ReadKind) -> Self {
        Self {
            address,
            through,
            members: Vec::new(),
            roots: Vec::new(),
            successors: Vec::new(),
            deliveries: Vec::new(),
            kind,
        }
    }

    /// An empty read of the whole space at `address` at `through`: the
    /// caller fills in every row of the address the read returned.
    pub fn whole_space(address: SpaceAddress, through: Position) -> Self {
        Self::empty(address, through, ReadKind::WholeSpace)
    }

    /// An empty read of `address` at `through` filtered for `account_id`:
    /// the roots and successors, and that account's member rows and the
    /// deliveries addressed to it. Only acquisition by a device of that
    /// account and the chain replay take it.
    pub fn one_account(address: SpaceAddress, through: Position, account_id: &str) -> Self {
        Self::empty(
            address,
            through,
            ReadKind::OneAccount(account_id.to_owned()),
        )
    }

    /// Restrict a read to deliveries of these epochs. Restrictions compose:
    /// taking a subset preserves its account filter and cannot widen coverage.
    pub fn for_epochs(mut self, mut epochs: BTreeSet<String>) -> Self {
        let account = self.filtered_for().map(str::to_owned);
        if let ReadKind::Epochs {
            epochs: previous, ..
        } = &self.kind
        {
            epochs.retain(|id| previous.contains(id));
        }
        self.deliveries
            .retain(|delivery| epochs.contains(&delivery.row.key_epoch_id));
        self.kind = ReadKind::Epochs { account, epochs };
        self
    }

    pub(crate) fn require_epoch(&self, epoch: &str) -> ProtocolResult<()> {
        match &self.kind {
            ReadKind::WholeSpace => Ok(()),
            ReadKind::Epochs {
                account: None,
                epochs,
            } if epochs.contains(epoch) => Ok(()),
            _ => Err(ProtocolError::InconsistentEvidence),
        }
    }

    /// The account a one-account read was filtered for; `None` for a read
    /// of the whole space.
    pub fn filtered_for(&self) -> Option<&str> {
        match &self.kind {
            ReadKind::WholeSpace => None,
            ReadKind::OneAccount(account_id) => Some(account_id),
            ReadKind::Epochs { account, .. } => account.as_deref(),
        }
    }

    /// Fails with [`ProtocolError::InconsistentEvidence`] unless this is a
    /// read of the whole space.
    pub(crate) fn require_whole_space(&self) -> ProtocolResult<()> {
        match self.kind {
            ReadKind::WholeSpace => Ok(()),
            _ => Err(ProtocolError::InconsistentEvidence),
        }
    }

    /// Every row is at or before the cut and no table repeats a row ID.
    pub fn check(&self) -> ProtocolResult<()> {
        if let ReadKind::Epochs { account, epochs } = &self.kind
            && self.deliveries.iter().any(|d| {
                !epochs.contains(&d.row.key_epoch_id)
                    || !self.names(&d.row.scope_id, &d.row.identifier)
                    || account
                        .as_ref()
                        .is_some_and(|id| id != &d.row.recipient_account_id)
            })
        {
            return Err(ProtocolError::InconsistentEvidence);
        }
        check_read(&self.members, self.through)?;
        check_read(&self.roots, self.through)?;
        check_read(&self.successors, self.through)?;
        check_read(&self.deliveries, self.through)
    }

    fn names(&self, scope_id: &str, identifier: &str) -> bool {
        self.address.scope_id == scope_id && self.address.identifier == identifier
    }

    /// The space's member accounts: the distinct `accountId` values of its
    /// member rows (§1.3). One row makes the account a member; the row ID
    /// carries no meaning.
    pub fn member_accounts(&self) -> BTreeSet<String> {
        self.members
            .iter()
            .filter(|member| {
                self.names(&member.row.scope_id, &member.row.identifier)
                    && ids::is_id(&member.row.account_id, IdForm::Any)
            })
            .map(|member| member.row.account_id.clone())
            .collect()
    }

    /// Whether `account_id` has a member row for the space.
    pub fn is_member(&self, account_id: &str) -> bool {
        self.members.iter().any(|member| {
            self.names(&member.row.scope_id, &member.row.identifier)
                && member.row.account_id == account_id
        })
    }

    /// The roots of this address.
    pub(crate) fn address_roots(&self) -> impl Iterator<Item = &Accepted<SpaceRoot>> {
        self.roots
            .iter()
            .filter(|root| self.names(&root.row.scope_id, &root.row.identifier))
    }

    /// The successors of this address.
    pub(crate) fn address_successors(&self) -> impl Iterator<Item = &Accepted<SpaceSuccessor>> {
        self.successors
            .iter()
            .filter(|successor| self.names(&successor.row.scope_id, &successor.row.identifier))
    }

    /// The deliveries of this address naming version `epoch_id`, by
    /// position then ID.
    pub(crate) fn deliveries_of(&self, epoch_id: &str) -> Vec<&Accepted<SpaceDelivery>> {
        let mut deliveries: Vec<_> = self
            .deliveries
            .iter()
            .filter(|delivery| {
                self.names(&delivery.row.scope_id, &delivery.row.identifier)
                    && delivery.row.key_epoch_id == epoch_id
            })
            .collect();
        super::by_position_then_id(&mut deliveries);
        deliveries
    }
}

/// The replayed public histories of the accounts a space's records name, all
/// read at one cut.
pub struct AccountDirectory<'a> {
    through: Position,
    domain: AppDomain,
    registry: &'a Registry,
    replays: BTreeMap<String, AccountReplay<'a>>,
}

impl<'a> AccountDirectory<'a> {
    /// `ledgers` are public account ledgers of one read at cut `through`:
    /// each must report that cut, and no account may appear twice.
    pub fn new(
        ledgers: &'a [PublicAccountLedger],
        domain: &AppDomain,
        registry: &'a Registry,
        through: Position,
    ) -> ProtocolResult<Self> {
        let mut replays = BTreeMap::new();
        for ledger in ledgers {
            if ledger.through != through {
                return Err(ProtocolError::InconsistentEvidence);
            }
            let replay = AccountReplay::new(ledger, domain, registry)?;
            if replays.insert(ledger.account_id.clone(), replay).is_some() {
                return Err(ProtocolError::InconsistentEvidence);
            }
        }
        Ok(Self {
            through,
            domain: domain.clone(),
            registry,
            replays,
        })
    }

    pub fn through(&self) -> Position {
        self.through
    }

    pub fn domain(&self) -> &AppDomain {
        &self.domain
    }

    pub fn registry(&self) -> &'a Registry {
        self.registry
    }

    /// Whether the caller supplied `account_id`'s ledger.
    pub fn has_ledger(&self, account_id: &str) -> bool {
        self.replays.contains_key(account_id)
    }

    /// The replay of `account_id`.
    pub fn replay(&self, account_id: &str) -> ProtocolResult<&AccountReplay<'a>> {
        self.replays
            .get(account_id)
            .ok_or(ProtocolError::InconsistentEvidence)
    }

    /// The account's state as of `position` (rows strictly before it).
    pub fn state_at(
        &self,
        account_id: &str,
        position: Position,
    ) -> ProtocolResult<Option<AccountState>> {
        self.replay(account_id)?.as_of(position)
    }

    /// The account's current state at the cut.
    pub fn current(&self, account_id: &str) -> ProtocolResult<Option<AccountState>> {
        self.state_at(account_id, Position(self.through.0.saturating_add(1)))
    }

    /// The valid sealing key records of the account's current chain
    /// (§2.12), or `None` if it has no counting root.
    pub fn sealing_keys(&self, account_id: &str) -> ProtocolResult<Option<SealingKeys>> {
        self.replay(account_id)?.sealing_keys()
    }

    /// Whether a record at `position` is signed by `device_id` of
    /// `account_id`, active with account epoch `epoch_id` there (§2.4): the
    /// author check of every signed space record. `Ok(false)` when the
    /// account had no root before `position`, the device was not active
    /// with that epoch, or the signature is invalid. A signing mechanism
    /// this registry lacks is [`ProtocolError::Unsupported`] and a provider
    /// failure [`ProtocolError::ProviderFailure`].
    pub fn verify_author(
        &self,
        account_id: &str,
        device_id: &str,
        epoch_id: &str,
        position: Position,
        bytes: &[u8],
        signature: &[u8],
    ) -> ProtocolResult<AuthorCheck> {
        // An author whose own account history contradicts itself cannot be
        // verified: its record is skipped, never a reason to stop the
        // space's replay (§3.4, "A member this client cannot evaluate").
        let state = match self.state_at(account_id, position) {
            Err(ProtocolError::Intrinsic(contradiction)) => {
                return Ok(AuthorCheck::AccountBroken(contradiction));
            }
            other => other?,
        };
        let Some(state) = state else {
            return Ok(AuthorCheck::NoAccountRoot);
        };
        if !state.is_active_in(device_id, epoch_id) {
            return Ok(AuthorCheck::NotActive);
        }
        let key = state.keys.get(device_id).ok_or(ProtocolError::Intrinsic(
            IntrinsicContradiction::MissingSignerKey,
        ))?;
        if signing::verify(self.registry, &key.signing_key(), bytes, signature)? {
            Ok(AuthorCheck::Valid)
        } else {
            Ok(AuthorCheck::SignatureInvalid)
        }
    }

    /// Where a delivery to `account_id` is sealed (§3.4): the account's
    /// current epoch and the first valid sealing key record of that epoch
    /// and `mechanism`. `None` when the account has no counting root or no
    /// such record: sealing to it would be "recipient unsupported", and the
    /// sender waits. The recipient is stamped with this directory's cut.
    pub fn recipient(
        &self,
        account_id: &str,
        mechanism: Mechanism<'_>,
    ) -> ProtocolResult<Option<Recipient>> {
        let Some(keys) = self.sealing_keys(account_id)? else {
            return Ok(None);
        };
        Ok(keys
            .current(mechanism)
            .map(|record| self.stamp(account_id, &keys, record)))
    }

    /// Where a device of `own`'s account seals a delivery to its own
    /// account: as [`recipient`](Self::recipient), but never to a sealing
    /// key record the device's derivation check reported as not derived
    /// ([`SealingKeys::current_for_own`]). `None` when no other valid
    /// record of the current epoch and `mechanism` exists: the device
    /// waits. `own` must be the account's keys of its current generation at
    /// this read's state; anything else is
    /// [`ProtocolError::InconsistentEvidence`].
    pub fn own_recipient(
        &self,
        own: &AccountEpochKeys,
        mechanism: Mechanism<'_>,
    ) -> ProtocolResult<Option<Recipient>> {
        let account_id = own.account_id();
        let Some(keys) = self.sealing_keys(account_id)? else {
            return Err(ProtocolError::InconsistentEvidence);
        };
        Ok(keys
            .current_for_own(self.registry, own, mechanism)?
            .map(|record| self.stamp(account_id, &keys, record)))
    }

    fn stamp(
        &self,
        account_id: &str,
        keys: &SealingKeys,
        record: &Accepted<AccountSealingKey>,
    ) -> Recipient {
        Recipient {
            account_id: account_id.to_owned(),
            account_epoch_id: keys.current_epoch_id().to_owned(),
            generation: keys.generation(),
            sealing_key: record.clone(),
            through: self.through,
        }
    }
}

/// The outcome of an author check (§2.4, "State as of a position").
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthorCheck {
    Valid,
    /// The account had no counting root strictly before the record.
    NoAccountRoot,
    /// The device was not active with the named account epoch at the
    /// record's position.
    NotActive,
    /// The signature does not verify under the device's signing key.
    SignatureInvalid,
    /// The author's account history is an intrinsic contradiction (§7):
    /// nothing it signed can be verified.
    AccountBroken(IntrinsicContradiction),
}

/// A member account's current epoch and the sealing key record a delivery
/// to it is sealed to (§2.12, §3.4), at the cut of the read it came from.
/// Built only by [`AccountDirectory::recipient`] or
/// [`AccountDirectory::own_recipient`], from the account's replayed public
/// history, so a delivery is never sealed to a record the caller assembled,
/// and the builders refuse it with a chain of another read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Recipient {
    account_id: String,
    account_epoch_id: String,
    generation: u64,
    sealing_key: Accepted<AccountSealingKey>,
    through: Position,
}

impl Recipient {
    pub fn account_id(&self) -> &str {
        &self.account_id
    }

    /// The account's current epoch at the read, which the delivery names.
    pub fn account_epoch_id(&self) -> &str {
        &self.account_epoch_id
    }

    /// The account's current generation (§2.11).
    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// The first valid sealing key record of the current epoch and the
    /// selected mechanism (§2.12).
    pub fn sealing_key(&self) -> &Accepted<AccountSealingKey> {
        &self.sealing_key
    }

    /// The cut of the read the recipient was built from.
    pub fn through(&self) -> Position {
        self.through
    }
}
