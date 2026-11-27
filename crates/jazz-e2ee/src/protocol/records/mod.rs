//! Typed rows of the internal E2EE tables (`E2EE_PROTOCOL.md` §1.2, §1.3),
//! one type per table, each with its local invariants and the exact bytes
//! it signs or binds.
//!
//! - [`account`]: the account and device tables, including the account
//!   sealing keys (§1.2, §2).
//! - [`space`]: the members table and the space roots, successors and
//!   deliveries (§1.3, §3).
//!
//! Column types: record, epoch, device, recovery root and account IDs and
//! the space address are UUIDs, held as their canonical lowercase text;
//! key, envelope, signature, marker and canonical-JSON columns are bytes;
//! mechanism versions and the ledger version are 32-bit unsigned integers
//! (the common envelope's version field); generations are 64-bit unsigned
//! integers, written as decimal strings wherever they enter JSON. Optional
//! columns are `Option`.
//!
//! `validate` checks only what a row says about itself. Whether a row is
//! accepted by replay depends on other rows and is decided by replay.

pub mod account;
pub mod roster;
pub mod space;

use crate::agility::{MechanismId, PublicKey};
use crate::context::CryptoContext;
use crate::envelope::{self, Mechanism};

use super::error::{ProtocolError, ProtocolResult};
use super::ids::{self, IdForm};
use super::position::RowId;

/// Fails with [`ProtocolError::InvalidRecord`] naming `what` unless `ok`.
pub(crate) fn ensure(ok: bool, what: &'static str) -> ProtocolResult<()> {
    if ok {
        Ok(())
    } else {
        Err(ProtocolError::InvalidRecord(what))
    }
}

pub(crate) fn ensure_id(value: &str, form: IdForm, what: &'static str) -> ProtocolResult<()> {
    ensure(ids::is_id(value, form), what)
}

/// Account IDs are UUIDs of any version (§0, "Identifiers").
pub(crate) fn ensure_account(value: &str, what: &'static str) -> ProtocolResult<()> {
    ensure_id(value, IdForm::Any, what)
}

pub(crate) fn ensure_bytes(value: &[u8], what: &'static str) -> ProtocolResult<()> {
    ensure(!value.is_empty(), what)
}

pub(crate) fn ensure_mechanism(id: &str, version: u32) -> ProtocolResult<()> {
    ensure(
        Mechanism::new(id, version).validate().is_ok(),
        "invalid mechanism",
    )
}

/// `env(m, p)`: the common envelope of mechanism `(id, version)` around `p`.
pub(crate) fn env(id: &str, version: u32, payload: &[u8]) -> ProtocolResult<Vec<u8>> {
    envelope::encode(Mechanism::new(id, version), payload)
        .map_err(|_| ProtocolError::InvalidRecord("invalid mechanism"))
}

/// Encodes a canonical context; a field over the format's limits makes the
/// record invalid.
pub(crate) fn context(fields: CryptoContext<'_>) -> ProtocolResult<Vec<u8>> {
    fields
        .encode()
        .map_err(|_| ProtocolError::InvalidRecord("context field out of range"))
}

/// `C ‖ frame[…]`.
pub(crate) fn append_framed(mut prefix: Vec<u8>, parts: &[&[u8]]) -> ProtocolResult<Vec<u8>> {
    let framed = crate::context::frame_record(parts)
        .map_err(|_| ProtocolError::InvalidRecord("framed field too large"))?;
    prefix.extend_from_slice(&framed);
    Ok(prefix)
}

pub(crate) fn frame(parts: &[&[u8]]) -> ProtocolResult<Vec<u8>> {
    crate::context::frame_record(parts)
        .map_err(|_| ProtocolError::InvalidRecord("framed field too large"))
}

macro_rules! string_enum {
    ($(#[$meta:meta])* $name:ident { $($variant:ident => $text:literal),+ $(,)? }) => {
        $(#[$meta])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
        pub enum $name {
            $($variant),+
        }

        impl $name {
            /// The column value.
            pub const fn as_str(self) -> &'static str {
                match self {
                    $(Self::$variant => $text),+
                }
            }

            /// Parses a column value; anything else is invalid.
            pub fn parse(value: &str) -> $crate::protocol::ProtocolResult<Self> {
                match value {
                    $($text => Ok(Self::$variant),)+
                    _ => Err($crate::protocol::ProtocolError::InvalidRecord(concat!("invalid ", stringify!($name)))),
                }
            }
        }
    };
}
pub(crate) use string_enum;

/// The six key columns of a device request, its device-keys projection and
/// a recovery root: one sealing key and one signing key, each with its
/// mechanism.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct KeyColumns {
    pub public_key: Vec<u8>,
    pub mechanism: String,
    pub version: u32,
    pub signing_public_key: Vec<u8>,
    pub signing_mechanism: String,
    pub signing_version: u32,
}

impl KeyColumns {
    pub fn new(sealing: &PublicKey, signing: &PublicKey) -> Self {
        Self {
            public_key: sealing.bytes.clone(),
            mechanism: sealing.mechanism.id.clone(),
            version: sealing.mechanism.version,
            signing_public_key: signing.bytes.clone(),
            signing_mechanism: signing.mechanism.id.clone(),
            signing_version: signing.mechanism.version,
        }
    }

    /// Both mechanisms are valid IDs and versions, and both keys are 1 to
    /// 65,536 bytes.
    pub fn validate(&self) -> ProtocolResult<()> {
        let key_len = 1..=super::json::MAX_DELIVERY_BYTES;
        ensure(
            Mechanism::new(&self.mechanism, self.version)
                .validate()
                .is_ok()
                && Mechanism::new(&self.signing_mechanism, self.signing_version)
                    .validate()
                    .is_ok(),
            "invalid key mechanism",
        )?;
        ensure(
            key_len.contains(&self.public_key.len())
                && key_len.contains(&self.signing_public_key.len()),
            "invalid public key length",
        )
    }

    pub fn sealing_key(&self) -> PublicKey {
        PublicKey {
            mechanism: MechanismId::new(self.mechanism.clone(), self.version),
            bytes: self.public_key.clone(),
        }
    }

    pub fn signing_key(&self) -> PublicKey {
        PublicKey {
            mechanism: MechanismId::new(self.signing_mechanism.clone(), self.signing_version),
            bytes: self.signing_public_key.clone(),
        }
    }

    /// `env(signingMechanism, signingPublicKey)`.
    pub fn signing_envelope(&self) -> ProtocolResult<Vec<u8>> {
        env(
            &self.signing_mechanism,
            self.signing_version,
            &self.signing_public_key,
        )
    }

    /// `env(mechanism, publicKey)`.
    pub fn sealing_envelope(&self) -> ProtocolResult<Vec<u8>> {
        env(&self.mechanism, self.version, &self.public_key)
    }
}

/// Implements [`RowId`] for rows with an `id` field.
macro_rules! row_id {
    ($($row:ty),+ $(,)?) => {
        $(impl $crate::protocol::position::RowId for $row {
            fn row_id(&self) -> &str {
                &self.id
            }
        })+
    };
}
pub(crate) use row_id;

impl RowId for String {
    fn row_id(&self) -> &str {
        self
    }
}
