//! Account sealing keys (`E2EE_PROTOCOL.md` §2.12): which
//! `__e2ee_account_keys` rows are valid, the record a sender seals to, and
//! the check a device of the account makes that a valid record derives from
//! the epoch key it holds.
//!
//! A record is valid iff its signature verifies under its signer's signing
//! key published before the record's position, and it is either
//! **co-published** (accepted at the same position as the accepted creation
//! of its epoch, signed by that creation's authorised signer: the canonical
//! root's device, or the accepted successor's signer) or a **later
//! publication** (accepted strictly after its epoch's creation, its epoch
//! the account's current epoch as of its position, and its signer active
//! with that epoch then). A record for an epoch that is not in the account's
//! chain, or accepted before its epoch's creation, is not valid. Only an
//! active device of the account can sign a valid record; a row the server
//! writes without that is not valid and is ignored.
//!
//! A record whose signer's signing mechanism this client does not support
//! is skipped and reported as unsupported; it never fails the account's
//! replay. A valid record that does not derive from the epoch key is
//! reported to the account's own devices and ignored by them; it never
//! stops them acquiring the account key.

use crate::envelope::Mechanism;
use crate::error::Error;
use crate::registry::Registry;

use super::super::derive;
use super::super::error::{ProtocolError, ProtocolResult, UnsupportedCapability};
use super::super::position::{Accepted, Position, by_position_then_id};
use super::super::records::account::AccountSealingKey;
use super::super::signing;
use super::key::AccountEpochKeys;
use super::replay::{AccountReplay, AccountState, collect_keys};

/// A valid sealing key record of the device's own account that does not
/// derive from the epoch key it names (§2.12, reported as
/// `conflicting-account-key`, §7). The device ignores it: it never seals to
/// or opens with it, and it never stops the device acquiring its account
/// key. Senders of other accounts cannot check derivation and keep sealing
/// to it while it is the record they select; the remedy is to remove the
/// device that signed it, which starts a new account epoch with a correct
/// co-published record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NonDerivedSealingKey {
    pub record_id: String,
    pub epoch_id: String,
    pub mechanism: String,
    pub version: u32,
    /// The device that signed it.
    pub signer_id: String,
    pub position: Position,
    /// Whether it is the first valid record of its epoch and mechanism: the
    /// record senders seal to.
    pub selected: bool,
}

impl NonDerivedSealingKey {
    /// Whether this is the record senders select for `epoch_id` and
    /// `mechanism`.
    pub fn is_selected_for(&self, epoch_id: &str, mechanism: Mechanism<'_>) -> bool {
        self.selected
            && self.epoch_id == epoch_id
            && Mechanism::new(&self.mechanism, self.version) == mechanism
    }
}

/// A sealing key record whose signer's signing mechanism this client does
/// not support (§2.12): skipped, never valid here and never invalid, and
/// reported as unsupported.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UnsupportedSealingKey {
    pub record_id: String,
    pub signer_id: String,
    pub capability: UnsupportedCapability,
}

/// The valid sealing key records of an account's current chain (the epochs
/// of its current generation) at one cut, by position then ID, and the
/// records skipped because their signer's signing mechanism is not
/// supported here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SealingKeys {
    account_id: String,
    generation: u64,
    root_id: String,
    epoch_id: String,
    valid: Vec<Accepted<AccountSealingKey>>,
    unsupported: Vec<UnsupportedSealingKey>,
}

impl SealingKeys {
    pub fn account_id(&self) -> &str {
        &self.account_id
    }

    /// The generation the records belong to (§2.11).
    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// The account's current epoch at the cut.
    pub fn current_epoch_id(&self) -> &str {
        &self.epoch_id
    }

    /// Every valid record, by position then ID.
    pub fn valid(&self) -> &[Accepted<AccountSealingKey>] {
        &self.valid
    }

    /// Records skipped because their signer's signing mechanism is not
    /// supported here, by position then ID.
    pub fn unsupported(&self) -> &[UnsupportedSealingKey] {
        &self.unsupported
    }

    /// Whether the record with this ID is valid.
    pub fn is_valid(&self, record_id: &str) -> bool {
        self.valid.iter().any(|record| record.row.id == record_id)
    }

    /// The first valid record (position, then ID) of `epoch_id` and
    /// `mechanism`: what a sender selects, whether or not it derives, since
    /// a sender cannot check derivation.
    pub fn first(
        &self,
        epoch_id: &str,
        mechanism: Mechanism<'_>,
    ) -> Option<&Accepted<AccountSealingKey>> {
        self.valid
            .iter()
            .find(|record| record.row.epoch_id == epoch_id && record.row.mechanism() == mechanism)
    }

    /// The record a sender seals to: the first valid record of the current
    /// epoch and the selected mechanism. `None` means "recipient
    /// unsupported": the sender waits (§2.12).
    pub fn current(&self, mechanism: Mechanism<'_>) -> Option<&Accepted<AccountSealingKey>> {
        self.first(&self.epoch_id, mechanism)
    }

    fn check_same_chain(&self, keys: &AccountEpochKeys) -> ProtocolResult<()> {
        if keys.account_id() != self.account_id
            || keys.generation() != self.generation
            || keys.root_id() != self.root_id
        {
            return Err(ProtocolError::InconsistentEvidence);
        }
        Ok(())
    }

    /// The check a device of the account makes (§2.12): which valid records
    /// of an epoch whose key `keys` holds do not derive from that key. Each
    /// is reported (`conflicting-account-key`) and ignored by the device;
    /// none fails the check. A record of a key sealing mechanism this
    /// registry does not have cannot be derived here, and is left
    /// unchecked. A provider failure fails the check. Account key
    /// authentication ([`AccountKeyEvidence`](super::AccountKeyEvidence))
    /// runs it on every key it accepts.
    pub fn check_derived(
        &self,
        registry: &Registry,
        keys: &AccountEpochKeys,
    ) -> ProtocolResult<Vec<NonDerivedSealingKey>> {
        self.check_same_chain(keys)?;
        let mut non_derived = Vec::new();
        for record in &self.valid {
            let Some(key) = keys.get(&record.row.epoch_id) else {
                continue;
            };
            let mechanism = record.row.mechanism();
            let sealer = match registry.key_sealer(mechanism) {
                Ok(sealer) => sealer,
                Err(Error::UnknownMechanism) => continue,
                Err(error) => return Err(error.into()),
            };
            let derived = derive::account_sealing_key_pair(sealer, key)?;
            if derived.public.bytes != record.row.public_key {
                let selected = self
                    .first(&record.row.epoch_id, mechanism)
                    .is_some_and(|first| first.row.id == record.row.id);
                non_derived.push(NonDerivedSealingKey {
                    record_id: record.row.id.clone(),
                    epoch_id: record.row.epoch_id.clone(),
                    mechanism: record.row.mechanism.clone(),
                    version: record.row.version,
                    signer_id: record.row.signer_id.clone(),
                    position: record.position,
                    selected,
                });
            }
        }
        Ok(non_derived)
    }
}

/// Whether one record is valid.
enum Validity {
    Valid,
    Invalid,
    Unsupported(UnsupportedCapability),
}

impl AccountReplay<'_> {
    /// The valid sealing key records of the account's current chain at the
    /// cut (§2.12), or `None` if the account has no counting root. Records
    /// of earlier generations, and of epochs outside the chain, are not
    /// valid here. A record whose signer's signing mechanism this registry
    /// lacks is skipped and reported in [`SealingKeys::unsupported`]; it
    /// never fails the replay.
    ///
    /// The cost grows with the account's sealing key records, which only
    /// the account's own devices may insert.
    pub fn sealing_keys(&self) -> ProtocolResult<Option<SealingKeys>> {
        let through = self.ledger().through;
        let Some(state) = self.as_of(Position(through.0.saturating_add(1)))? else {
            return Ok(None);
        };
        let mut records: Vec<_> = self.ledger().account_keys.iter().collect();
        by_position_then_id(&mut records);
        let mut valid = Vec::new();
        let mut unsupported = Vec::new();
        for record in records {
            match self.sealing_key_valid(&state, record)? {
                Validity::Valid => valid.push(record.clone()),
                Validity::Invalid => {}
                Validity::Unsupported(capability) => unsupported.push(UnsupportedSealingKey {
                    record_id: record.row.id.clone(),
                    signer_id: record.row.signer_id.clone(),
                    capability,
                }),
            }
        }
        Ok(Some(SealingKeys {
            account_id: state.account_id.clone(),
            generation: state.generation,
            root_id: state.root.row.id.clone(),
            epoch_id: state.epoch_id().to_owned(),
            valid,
            unsupported,
        }))
    }

    /// Whether `record` is a valid sealing key record of `state`'s chain.
    fn sealing_key_valid(
        &self,
        state: &AccountState,
        record: &Accepted<AccountSealingKey>,
    ) -> ProtocolResult<Validity> {
        let row = &record.row;
        if row.account_id != self.ledger().account_id || row.validate().is_err() {
            return Ok(Validity::Invalid);
        }
        let Some(epoch) = state
            .epochs
            .iter()
            .find(|epoch| epoch.epoch_id == row.epoch_id)
        else {
            return Ok(Validity::Invalid);
        };
        if record.position < epoch.position {
            return Ok(Validity::Invalid);
        }
        if record.position == epoch.position {
            // Co-published: the creation's own signer, authorised by the
            // creation itself and not by membership of the new epoch.
            let signer = match &epoch.successor {
                None => &state.root.row.device_id,
                Some(successor) => &successor.row.columns.signer_id,
            };
            if row.signer_id != *signer {
                return Ok(Validity::Invalid);
            }
        } else {
            // A later publication: the epoch was current, and the signer
            // active with it, as of the record's position.
            let Some(then) = self.as_of(record.position)? else {
                return Ok(Validity::Invalid);
            };
            if then.root.row.id != state.root.row.id
                || then.epoch_id() != epoch.epoch_id
                || !then.is_active(&row.signer_id)
            {
                return Ok(Validity::Invalid);
            }
        }
        let Ok(bytes) = row.signed_bytes(self.application()) else {
            return Ok(Validity::Invalid);
        };
        // The signer's signing key, published before the record.
        let keys = collect_keys(
            self.ledger()
                .keys
                .iter()
                .filter(|key| key.position < record.position),
        )?;
        let Some(signer) = keys.get(&row.signer_id) else {
            return Ok(Validity::Invalid);
        };
        match signing::verify(
            self.registry(),
            &signer.signing_key(),
            &bytes,
            &row.signature,
        ) {
            Ok(true) => Ok(Validity::Valid),
            Ok(false) => Ok(Validity::Invalid),
            Err(ProtocolError::Unsupported(capability)) => Ok(Validity::Unsupported(capability)),
            Err(error) => Err(error),
        }
    }
}
