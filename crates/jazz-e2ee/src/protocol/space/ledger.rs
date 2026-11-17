//! One space's rows and the public account histories they name, from one
//! ledger read (`E2EE_PROTOCOL.md` §0 item 3, §1.3, §2.4 "State as of a
//! position").
//!
//! Every ledger must come from the same read: one cut for all. An account
//! whose public ledger the caller did not supply is
//! [`ProtocolError::InconsistentEvidence`]: evidence the replay needs was
//! left out, which is never read as "not active" or "no key".

use std::collections::{BTreeMap, BTreeSet};

use crate::envelope::Mechanism;
use crate::registry::Registry;

use super::super::account::{AccountReplay, AccountState, PublicAccountLedger, SealingKeys};
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
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SpaceLedger {
    pub address: SpaceAddress,
    pub through: Position,
    pub members: Vec<Accepted<SpaceMember>>,
    pub roots: Vec<Accepted<SpaceRoot>>,
    pub successors: Vec<Accepted<SpaceSuccessor>>,
    pub deliveries: Vec<Accepted<SpaceDelivery>>,
}

impl SpaceLedger {
    /// An empty read of `address` at `through`.
    pub fn new(address: SpaceAddress, through: Position) -> Self {
        Self {
            address,
            through,
            members: Vec::new(),
            roots: Vec::new(),
            successors: Vec::new(),
            deliveries: Vec::new(),
        }
    }

    /// Every row is at or before the cut and no table repeats a row ID.
    pub fn check(&self) -> ProtocolResult<()> {
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
        let Some(state) = self.state_at(account_id, position)? else {
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
    /// sender waits.
    pub fn recipient(
        &self,
        account_id: &str,
        mechanism: Mechanism<'_>,
    ) -> ProtocolResult<Option<Recipient>> {
        let Some(keys) = self.sealing_keys(account_id)? else {
            return Ok(None);
        };
        Ok(keys.current(mechanism).map(|record| Recipient {
            account_id: account_id.to_owned(),
            account_epoch_id: keys.current_epoch_id().to_owned(),
            generation: keys.generation(),
            sealing_key: record.clone(),
        }))
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
}

/// A member account's current epoch and the sealing key record a delivery
/// to it is sealed to (§2.12, §3.4).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Recipient {
    pub account_id: String,
    pub account_epoch_id: String,
    pub generation: u64,
    pub sealing_key: Accepted<AccountSealingKey>,
}
