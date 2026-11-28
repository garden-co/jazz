//! The device document and the floor records of the private store
//! (`E2EE_PROTOCOL.md` §6.1, §6.3a), beside the held-key documents of
//! [`super::store`].
//!
//! ```text
//! device document: { format: "jazz-e2ee-local-devices-v3",
//!                    devices: [ { scope, id, mechanism, version, publicKey,
//!                                 privateKey, signingMechanism,
//!                                 signingVersion, signingPublicKey,
//!                                 signingPrivateKey, approval } … ] }
//! approval:        { format: "jazz-e2ee-approval-nonce-v1", commitment,
//!                    nonce } or { format, commitment, spent: true }
//! floor record:    { format: "jazz-e2ee-floor-v1", binding, account,
//!                    through: "<decimal>", generation: "<decimal>", rootId,
//!                    rootKeys, epochId, epochPosition: "<decimal>",
//!                    activeDevice?, signature }
//! ```
//!
//! Every value is canonical JSON, bytes as arrays; a decoder rebuilds the
//! record and refuses input whose canonical encoding differs. The device's
//! private keys and its approval nonce are stored only as the platform's
//! protected blobs (§6.1): this module never sees them in the clear, and the
//! runtime protects and unprotects them. The approval nonce answers at most
//! one challenge (§2.2): once the device reveals it, the document says
//! `spent` and holds no nonce, so a later load cannot answer again.
//!
//! A floor record is signed by the device over `B(format, record without
//! signature)`, like a lineage record, and is accepted on load only if its
//! binding is this device's, it names the expected account and its
//! signature verifies.

use subtle::ConstantTimeEq;
use zeroize::Zeroizing;

use crate::agility::{KeyPair, MechanismId, PublicKey, SecretKey};
use crate::registry::Registry;

use super::super::account::AccountFloor;
use super::super::account::build::{ApprovalNonce, LocalDevice};
use super::super::canonical::{self, Value, invalid, parse_canonical};
use super::super::domain::AppDomain;
use super::super::error::{ProtocolError, ProtocolResult};
use super::super::position::Position;
use super::super::records::KeyColumns;
use super::super::records::account::nonce_commitment;
use super::store::{Rejection, StoreOwner, verify_stored_signature};
use super::stored::{Binding, Fields, UnrecognisedDocument, ensure_canonical, stored_row};

/// The format of the device document.
pub const DEVICE_DOCUMENT_FORMAT: &str = "jazz-e2ee-local-devices-v3";
/// The format of a device's approval nonce state.
pub const APPROVAL_NONCE_FORMAT: &str = "jazz-e2ee-approval-nonce-v1";
/// The format of a floor record.
pub const FLOOR_FORMAT: &str = "jazz-e2ee-floor-v1";

fn decimal(value: u64) -> Value {
    Value::String(value.to_string())
}

/// The approval nonce of a stored device (§2.2): protected at rest until
/// the device answers a challenge with it, then only marked spent.
#[derive(Clone, PartialEq, Eq)]
pub enum StoredApproval {
    /// The nonce as the platform's protected blob.
    Unspent { protected_nonce: Vec<u8> },
    /// The device answered a challenge: it answers no other.
    Spent,
}

impl core::fmt::Debug for StoredApproval {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.write_str(match self {
            Self::Unspent { .. } => "Unspent(..)",
            Self::Spent => "Spent",
        })
    }
}

/// The one device of a device document, its secrets protected (§6.1).
#[derive(Clone, PartialEq, Eq)]
pub struct StoredDevice {
    /// The document's scope, `Dₐ(account)`.
    pub scope: String,
    pub id: String,
    pub sealing: PublicKey,
    pub protected_sealing_key: Vec<u8>,
    pub signing: PublicKey,
    pub protected_signing_key: Vec<u8>,
    /// The commitment to the approval nonce, which the request carries.
    pub approval_commitment: [u8; 32],
    pub approval: StoredApproval,
}

impl core::fmt::Debug for StoredDevice {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        f.debug_struct("StoredDevice")
            .field("scope", &self.scope)
            .field("id", &self.id)
            .field("approval", &self.approval)
            .finish_non_exhaustive()
    }
}

/// The secrets of a stored device, unprotected by the runtime.
pub struct DeviceSecrets {
    pub sealing: SecretKey,
    pub signing: SecretKey,
    /// The approval nonce, `None` once spent.
    pub approval_nonce: Option<Zeroizing<Vec<u8>>>,
}

impl StoredDevice {
    fn value(&self) -> Value {
        let mut approval = vec![
            ("format", Some(Value::string(APPROVAL_NONCE_FORMAT))),
            ("commitment", Some(Value::bytes(&self.approval_commitment))),
        ];
        match &self.approval {
            StoredApproval::Unspent { protected_nonce } => {
                approval.push(("nonce", Some(Value::bytes(protected_nonce))));
            }
            StoredApproval::Spent => approval.push(("spent", Some(Value::Bool(true)))),
        }
        Value::object(vec![
            ("scope", Some(Value::string(&self.scope))),
            ("id", Some(Value::string(&self.id))),
            ("mechanism", Some(Value::string(&self.sealing.mechanism.id))),
            (
                "version",
                Some(Value::Number(self.sealing.mechanism.version.into())),
            ),
            ("publicKey", Some(Value::bytes(&self.sealing.bytes))),
            (
                "privateKey",
                Some(Value::bytes(&self.protected_sealing_key)),
            ),
            (
                "signingMechanism",
                Some(Value::string(&self.signing.mechanism.id)),
            ),
            (
                "signingVersion",
                Some(Value::Number(self.signing.mechanism.version.into())),
            ),
            ("signingPublicKey", Some(Value::bytes(&self.signing.bytes))),
            (
                "signingPrivateKey",
                Some(Value::bytes(&self.protected_signing_key)),
            ),
            ("approval", Some(Value::object(approval))),
        ])
    }

    fn from_value(value: &Value) -> ProtocolResult<Self> {
        let fields = Fields::new(value)?;
        let approval = Fields::new(fields.get("approval")?)?;
        approval.format(APPROVAL_NONCE_FORMAT)?;
        let commitment: [u8; 32] = approval
            .bytes("commitment")?
            .try_into()
            .map_err(|_| ProtocolError::InvalidRecord("commitment length"))?;
        let nonce = fields.get("approval")?.field("nonce");
        let spent = fields.get("approval")?.field("spent");
        let state = match (nonce, spent) {
            (Some(nonce), None) => StoredApproval::Unspent {
                protected_nonce: nonce.as_bytes()?,
            },
            (None, Some(Value::Bool(true))) => StoredApproval::Spent,
            _ => return invalid("the approval nonce is either present or spent"),
        };
        let device = Self {
            scope: fields.string("scope")?,
            id: fields.string("id")?,
            sealing: PublicKey {
                mechanism: MechanismId::new(
                    fields.string("mechanism")?,
                    fields.version("version", "version range")?,
                ),
                bytes: fields.bytes("publicKey")?,
            },
            protected_sealing_key: fields.bytes("privateKey")?,
            signing: PublicKey {
                mechanism: MechanismId::new(
                    fields.string("signingMechanism")?,
                    fields.version("signingVersion", "version range")?,
                ),
                bytes: fields.bytes("signingPublicKey")?,
            },
            protected_signing_key: fields.bytes("signingPrivateKey")?,
            approval_commitment: commitment,
            approval: state,
        };
        ensure_canonical(value, &device.value())?;
        Ok(device)
    }

    /// The device document holding this device, the only one of its scope.
    pub fn encode_document(&self) -> ProtocolResult<Vec<u8>> {
        Value::object(vec![
            ("format", Some(Value::string(DEVICE_DOCUMENT_FORMAT))),
            ("devices", Some(Value::Array(vec![self.value()]))),
        ])
        .encode()
    }

    /// The device, with its secrets as the runtime unprotected them. The
    /// approval nonce must open the stored commitment.
    pub fn into_local(self, secrets: DeviceSecrets) -> ProtocolResult<LocalDevice> {
        let approval_nonce = match (&self.approval, secrets.approval_nonce) {
            (StoredApproval::Spent, None) => None,
            (StoredApproval::Unspent { .. }, Some(nonce)) => {
                let nonce: [u8; 32] = nonce
                    .as_slice()
                    .try_into()
                    .map_err(|_| ProtocolError::InvalidRecord("approval nonce length"))?;
                let nonce = Zeroizing::new(nonce);
                // Constant time: the nonce is secret until it is revealed.
                let commitment = nonce_commitment(&self.id, &nonce[..])?;
                if !bool::from(commitment.ct_eq(&self.approval_commitment)) {
                    return invalid("the approval nonce does not open its commitment");
                }
                Some(ApprovalNonce(nonce))
            }
            _ => return invalid("the approval nonce does not match its stored state"),
        };
        Ok(LocalDevice {
            id: self.id,
            sealing: KeyPair {
                public: self.sealing,
                secret: secrets.sealing,
            },
            signing: KeyPair {
                public: self.signing,
                secret: secrets.signing,
            },
            approval_commitment: self.approval_commitment,
            approval_nonce,
        })
    }
}

/// Decodes a device document: `None` when it holds no device. Text that is
/// not a device document of this format, or a device record that does not
/// decode, is [`UnrecognisedDocument`]: refused and never overwritten
/// (§6.1).
pub fn decode_device_document(bytes: &[u8]) -> Result<Option<StoredDevice>, UnrecognisedDocument> {
    let value = parse_canonical(bytes).map_err(|_| UnrecognisedDocument)?;
    let fields = Fields::new(&value).map_err(|_| UnrecognisedDocument)?;
    fields
        .format(DEVICE_DOCUMENT_FORMAT)
        .map_err(|_| UnrecognisedDocument)?;
    let devices = fields
        .get("devices")
        .and_then(Value::as_array)
        .map_err(|_| UnrecognisedDocument)?;
    let device = match devices {
        [] => None,
        [device] => Some(StoredDevice::from_value(device).map_err(|_| UnrecognisedDocument)?),
        _ => return Err(UnrecognisedDocument),
    };
    let rebuilt = Value::object(vec![
        ("format", Some(Value::string(DEVICE_DOCUMENT_FORMAT))),
        (
            "devices",
            Some(Value::Array(
                device.iter().map(StoredDevice::value).collect(),
            )),
        ),
    ]);
    if rebuilt != value {
        return Err(UnrecognisedDocument);
    }
    Ok(device)
}

stored_row! {
    key_columns_value, key_columns_from for KeyColumns {
        public_key: "publicKey" => bytes,
        mechanism: "mechanism" => string,
        version: "version" => version("version range"),
        signing_public_key: "signingPublicKey" => bytes,
        signing_mechanism: "signingMechanism" => string,
        signing_version: "signingVersion" => version("version range"),
    }
}

/// A floor (§2.4, "Floors"; §6.3a) as stored: the account it is of, bound
/// to and signed by the device.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FloorRecord {
    pub binding: Binding,
    pub account_id: String,
    pub floor: AccountFloor,
    pub signature: Vec<u8>,
}

impl FloorRecord {
    fn value(&self, signature: bool) -> Value {
        let floor = &self.floor;
        Value::object(vec![
            ("format", Some(Value::string(FLOOR_FORMAT))),
            ("binding", Some(self.binding.value())),
            ("account", Some(Value::string(&self.account_id))),
            ("through", Some(decimal(floor.through.0))),
            ("generation", Some(decimal(floor.generation))),
            ("rootId", Some(Value::string(&floor.root_id))),
            ("rootKeys", Some(key_columns_value(&floor.root_keys))),
            ("epochId", Some(Value::string(&floor.epoch_id))),
            ("epochPosition", Some(decimal(floor.epoch_position.0))),
            (
                "activeDevice",
                floor.active_device.as_deref().map(Value::string),
            ),
            (
                "signature",
                signature.then(|| Value::bytes(&self.signature)),
            ),
        ])
    }

    /// `B(format, record without signature)`: the bytes the device signs.
    pub fn signed_bytes(&self) -> ProtocolResult<Vec<u8>> {
        canonical::framed(FLOOR_FORMAT, self.value(false))
    }

    /// The stored record.
    pub fn encode(&self) -> ProtocolResult<Vec<u8>> {
        self.value(true).encode()
    }

    /// Decodes a stored floor record; another format, or text that is not
    /// the canonical encoding of a floor record, is an invalid record.
    pub fn decode(bytes: &[u8]) -> ProtocolResult<Self> {
        let value = parse_canonical(bytes)?;
        let fields = Fields::new(&value)?;
        fields.format(FLOOR_FORMAT)?;
        let decimal_field = |name| {
            let text = fields.get(name)?.as_str()?;
            if matches!(text.as_bytes(), [b'+', ..] | [b'0', _, ..]) {
                return invalid("not a canonical decimal");
            }
            text.parse::<u64>()
                .map_err(|_| ProtocolError::InvalidRecord("not a canonical decimal"))
        };
        let record = Self {
            binding: Binding::from_value(fields.get("binding")?)?,
            account_id: fields.string("account")?,
            floor: AccountFloor {
                through: Position(decimal_field("through")?),
                generation: decimal_field("generation")?,
                root_id: fields.string("rootId")?,
                root_keys: key_columns_from(fields.get("rootKeys")?)?,
                epoch_id: fields.string("epochId")?,
                epoch_position: Position(decimal_field("epochPosition")?),
                active_device: match value.field("activeDevice") {
                    None => None,
                    Some(device) => Some(device.as_str()?.to_owned()),
                },
            },
            signature: fields.bytes("signature")?,
        };
        ensure_canonical(&value, &record.value(true))?;
        Ok(record)
    }
}

impl StoreOwner<'_> {
    /// The floor record of `account_id`'s `floor`, signed by the device and
    /// verified again.
    pub fn seal_floor(
        &self,
        account_id: &str,
        floor: &AccountFloor,
    ) -> ProtocolResult<FloorRecord> {
        let mut record = FloorRecord {
            binding: self.binding()?,
            account_id: account_id.to_owned(),
            floor: floor.clone(),
            signature: Vec::new(),
        };
        record.signature = self.sign(&record.signed_bytes()?)?;
        Ok(record)
    }

    /// The floor of `account_id` a stored record holds, if it passes its
    /// checks: this device's binding, the account it is stored for, and the
    /// device's signature. A record that fails is skipped and reported; a
    /// provider failure fails the load.
    pub fn load_floor(
        &self,
        account_id: &str,
        record: &FloorRecord,
    ) -> ProtocolResult<Result<AccountFloor, Rejection>> {
        check_floor(
            self.registry,
            &self.device.signing.public,
            &self.binding()?,
            account_id,
            record,
        )
    }
}

/// Checks multiple floor records for the same device, computing its binding once.
pub struct FloorVerifier<'a> {
    registry: &'a Registry,
    signing: PublicKey,
    binding: Binding,
}

impl<'a> FloorVerifier<'a> {
    pub fn new(
        registry: &'a Registry,
        domain: &AppDomain,
        account_id: &str,
        device: &LocalDevice,
    ) -> ProtocolResult<Self> {
        let owner = StoreOwner {
            registry,
            domain,
            account_id,
            device,
        };
        Ok(Self {
            registry,
            signing: device.signing.public.clone(),
            binding: owner.binding()?,
        })
    }

    pub fn load_floor(
        &self,
        account_id: &str,
        record: &FloorRecord,
    ) -> ProtocolResult<Result<AccountFloor, Rejection>> {
        check_floor(
            self.registry,
            &self.signing,
            &self.binding,
            account_id,
            record,
        )
    }
}

fn check_floor(
    registry: &Registry,
    signing: &PublicKey,
    binding: &Binding,
    account_id: &str,
    record: &FloorRecord,
) -> ProtocolResult<Result<AccountFloor, Rejection>> {
    if record.binding != *binding || record.account_id != account_id {
        return Ok(Err(Rejection::Binding));
    }
    if let Err(reason) = verify_stored_signature(
        registry,
        signing,
        &record.signed_bytes()?,
        &record.signature,
    )? {
        return Ok(Err(reason));
    }
    Ok(Ok(record.floor.clone()))
}
