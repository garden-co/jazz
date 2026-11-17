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
//! chain, or accepted before its epoch's creation, is not valid.

use crate::envelope::Mechanism;
use crate::error::Error;
use crate::registry::Registry;

use super::super::derive;
use super::super::error::{IntrinsicContradiction, ProtocolError, ProtocolResult};
use super::super::json;
use super::super::position::{Accepted, Position};
use super::super::records::account::AccountSealingKey;
use super::super::signing;
use super::key::AccountEpochKeys;
use super::replay::{AccountReplay, AccountState, collect_keys};

/// The valid sealing key records of an account's current chain (the epochs
/// of its current generation) at one cut, by position then ID.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SealingKeys {
    account_id: String,
    generation: u64,
    root_id: String,
    epoch_id: String,
    valid: Vec<Accepted<AccountSealingKey>>,
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

    /// Whether the record with this ID is valid.
    pub fn is_valid(&self, record_id: &str) -> bool {
        self.valid.iter().any(|record| record.row.id == record_id)
    }

    /// The first valid record (position, then ID) of `epoch_id` and
    /// `mechanism`.
    pub fn first(
        &self,
        epoch_id: &str,
        mechanism: Mechanism<'_>,
    ) -> Option<&Accepted<AccountSealingKey>> {
        self.valid.iter().find(|record| {
            record.row.epoch_id == epoch_id
                && Mechanism::new(&record.row.mechanism, record.row.version) == mechanism
        })
    }

    /// The record a sender seals to: the first valid record of the current
    /// epoch and the selected mechanism. `None` means "recipient
    /// unsupported": the sender waits (§2.12).
    pub fn current(&self, mechanism: Mechanism<'_>) -> Option<&Accepted<AccountSealingKey>> {
        self.first(&self.epoch_id, mechanism)
    }

    /// The check a device of the account makes (§2.12): every valid record
    /// of an epoch whose key `keys` holds derives from that key. One that
    /// does not is the intrinsic contradiction `conflicting-account-key`,
    /// since only the account's own devices can write a valid record. A
    /// record of a key sealing mechanism this registry does not have cannot
    /// be derived here, and is left unchecked.
    pub fn check_derived(
        &self,
        registry: &Registry,
        keys: &AccountEpochKeys,
    ) -> ProtocolResult<()> {
        if keys.account_id() != self.account_id
            || keys.generation() != self.generation
            || keys.root_id() != self.root_id
        {
            return Err(ProtocolError::InconsistentEvidence);
        }
        for record in &self.valid {
            let Some(key) = keys.get(&record.row.epoch_id) else {
                continue;
            };
            let sealer = match registry
                .key_sealer(Mechanism::new(&record.row.mechanism, record.row.version))
            {
                Ok(sealer) => sealer,
                Err(Error::UnknownMechanism) => continue,
                Err(error) => return Err(error.into()),
            };
            let derived = derive::account_sealing_key_pair(sealer, key)?;
            if derived.public.bytes != record.row.public_key {
                return Err(ProtocolError::Intrinsic(
                    IntrinsicContradiction::ConflictingAccountKey,
                ));
            }
        }
        Ok(())
    }
}

impl AccountReplay<'_> {
    /// The valid sealing key records of the account's current chain at the
    /// cut (§2.12), or `None` if the account has no counting root. Records
    /// of earlier generations, and of epochs outside the chain, are not
    /// valid here.
    pub fn sealing_keys(&self) -> ProtocolResult<Option<SealingKeys>> {
        let through = self.ledger().through;
        let Some(state) = self.as_of(Position(through.0.saturating_add(1)))? else {
            return Ok(None);
        };
        let mut records: Vec<_> = self.ledger().account_keys.iter().collect();
        records.sort_by(|a, b| {
            a.position
                .cmp(&b.position)
                .then_with(|| json::utf16_cmp(&a.row.id, &b.row.id))
        });
        let mut valid = Vec::new();
        for record in records {
            if self.sealing_key_valid(&state, record)? {
                valid.push(record.clone());
            }
        }
        Ok(Some(SealingKeys {
            account_id: state.account_id.clone(),
            generation: state.generation,
            root_id: state.root.row.id.clone(),
            epoch_id: state.epoch_id().to_owned(),
            valid,
        }))
    }

    /// Whether `record` is a valid sealing key record of `state`'s chain.
    fn sealing_key_valid(
        &self,
        state: &AccountState,
        record: &Accepted<AccountSealingKey>,
    ) -> ProtocolResult<bool> {
        let row = &record.row;
        if row.account_id != self.ledger().account_id || row.validate().is_err() {
            return Ok(false);
        }
        let Some(epoch) = state
            .epochs
            .iter()
            .find(|epoch| epoch.epoch_id == row.epoch_id)
        else {
            return Ok(false);
        };
        if record.position < epoch.position {
            return Ok(false);
        }
        if record.position == epoch.position {
            // Co-published: the creation's own signer, authorised by the
            // creation itself and not by membership of the new epoch.
            let signer = match &epoch.successor {
                None => &state.root.row.device_id,
                Some(successor) => &successor.row.signer_id,
            };
            if row.signer_id != *signer {
                return Ok(false);
            }
        } else {
            // A later publication: the epoch was current, and the signer
            // active with it, as of the record's position.
            let Some(then) = self.as_of(record.position)? else {
                return Ok(false);
            };
            if then.root.row.id != state.root.row.id
                || then.epoch_id() != epoch.epoch_id
                || !then.is_active(&row.signer_id)
            {
                return Ok(false);
            }
        }
        let Ok(bytes) = row.signed_bytes(self.application()) else {
            return Ok(false);
        };
        // The signer's signing key, published before the record.
        let keys = collect_keys(
            self.ledger()
                .keys
                .iter()
                .filter(|key| key.position < record.position),
        )?;
        let Some(signer) = keys.get(&row.signer_id) else {
            return Ok(false);
        };
        signing::verify(
            self.registry(),
            &signer.signing_key(),
            &bytes,
            &row.signature,
        )
    }
}
