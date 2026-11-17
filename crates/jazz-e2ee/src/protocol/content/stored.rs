//! The records of a held-key document and their byte encoding
//! (`E2EE_PROTOCOL.md` §6.1, §6.2).
//!
//! ```text
//! held-key document: { format: "jazz-e2ee-held-space-v1", revision: n,
//!                      lineage: <lineage record>, keys: [held-key record …] }
//! lineage record:    { format: "jazz-e2ee-held-lineage-v1", binding, root,
//!                      successors: [ … ], signature }
//! held-key record:   { format: "jazz-e2ee-held-key-v3", binding, rootId,
//!                      epochId, depth: "<decimal>", prefix,
//!                      unpublished?: { transaction }, refused?: true,
//!                      envelope, signature }
//! ```
//!
//! Every value is canonical JSON ([`super::canonical`]); `B(f, x) =
//! UTF8(json(canonical([f, x])))`. Stored root and successor rows are
//! objects of their §1.3 columns plus `id`, bytes as arrays. A decoder
//! rebuilds each record and refuses input whose canonical encoding differs;
//! a document is decoded record by record, so such a record is skipped on
//! its own.

use crate::agility::{MechanismId, PublicKey};
use crate::primitives::hash::sha256;

use super::super::error::{ProtocolError, ProtocolResult};
use super::super::records::space::{SpaceRoot, SpaceSuccessor};
use super::canonical::{self, MAX_SAFE_INTEGER, Value};

/// The format of a held-key document (one per space root).
pub const DOCUMENT_FORMAT: &str = "jazz-e2ee-held-space-v1";
/// The format of a lineage record.
pub const LINEAGE_FORMAT: &str = "jazz-e2ee-held-lineage-v1";
/// The format of a held-key record.
pub const KEY_FORMAT: &str = "jazz-e2ee-held-key-v3";
/// The format `B` names in the device hash.
pub const DEVICE_HASH_FORMAT: &str = "jazz-e2ee-held-device-v1";
/// The format `B` names in the lineage prefix hash.
pub const PREFIX_FORMAT: &str = "jazz-e2ee-held-prefix-v1";

fn invalid<T>(what: &'static str) -> ProtocolResult<T> {
    Err(ProtocolError::InvalidRecord(what))
}

/// Lowercase hex.
fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn mechanism_value(mechanism: &MechanismId) -> Value {
    Value::object(vec![
        ("id", Some(Value::string(&mechanism.id))),
        ("version", Some(Value::Number(mechanism.version.into()))),
    ])
}

/// `<device hash>`: `sha256hex(B("jazz-e2ee-held-device-v1", [publicKey,
/// signingPublicKey, envelopeMechanism, signingMechanism]))`, the public
/// keys as their raw bytes and each mechanism as `{ id, version }`.
pub fn device_hash(sealing: &PublicKey, signing: &PublicKey) -> ProtocolResult<String> {
    let bytes = canonical::framed(
        DEVICE_HASH_FORMAT,
        Value::Array(vec![
            Value::bytes(&sealing.bytes),
            Value::bytes(&signing.bytes),
            mechanism_value(&sealing.mechanism),
            mechanism_value(&signing.mechanism),
        ]),
    )?;
    Ok(hex(&sha256(&bytes)))
}

/// A stored space root row: its §1.3 columns plus `id`.
pub(crate) fn root_value(root: &SpaceRoot) -> Value {
    Value::object(vec![
        ("id", Some(Value::string(&root.id))),
        ("scopeId", Some(Value::string(&root.scope_id))),
        ("identifier", Some(Value::string(&root.identifier))),
        (
            "creatorAccountId",
            Some(Value::string(&root.creator_account_id)),
        ),
        ("deviceId", Some(Value::string(&root.device_id))),
        (
            "accountEpochId",
            Some(Value::string(&root.account_epoch_id)),
        ),
        ("epochId", Some(Value::string(&root.epoch_id))),
        ("mechanism", Some(Value::string(&root.mechanism))),
        ("version", Some(Value::Number(root.version.into()))),
        ("verification", Some(Value::bytes(&root.verification))),
        (
            "accountEnvelope",
            Some(Value::bytes(&root.account_envelope)),
        ),
        ("signature", Some(Value::bytes(&root.signature))),
    ])
}

/// A stored space successor row: its §1.3 columns plus `id`.
pub(crate) fn successor_value(successor: &SpaceSuccessor) -> Value {
    Value::object(vec![
        ("id", Some(Value::string(&successor.id))),
        ("scopeId", Some(Value::string(&successor.scope_id))),
        ("identifier", Some(Value::string(&successor.identifier))),
        ("predecessor", Some(Value::string(&successor.predecessor))),
        (
            "historyEpochId",
            Some(Value::string(&successor.history_epoch_id)),
        ),
        ("epochId", Some(Value::string(&successor.epoch_id))),
        (
            "authorAccountId",
            Some(Value::string(&successor.author_account_id)),
        ),
        (
            "authorDeviceId",
            Some(Value::string(&successor.author_device_id)),
        ),
        (
            "authorEpochId",
            Some(Value::string(&successor.author_epoch_id)),
        ),
        ("verification", Some(Value::bytes(&successor.verification))),
        ("history", Some(Value::bytes(&successor.history))),
        (
            "accountEnvelope",
            Some(Value::bytes(&successor.account_envelope)),
        ),
        ("signature", Some(Value::bytes(&successor.signature))),
    ])
}

/// Reads object fields of a decoded value; a missing field or one of
/// another type is an invalid record.
struct Fields<'v>(&'v Value);

impl<'v> Fields<'v> {
    fn new(value: &'v Value) -> ProtocolResult<Self> {
        match value {
            Value::Object(_) => Ok(Self(value)),
            _ => invalid("expected an object"),
        }
    }

    fn get(&self, name: &str) -> ProtocolResult<&'v Value> {
        self.0
            .field(name)
            .ok_or(ProtocolError::InvalidRecord("missing field"))
    }

    fn string(&self, name: &str) -> ProtocolResult<String> {
        Ok(self.get(name)?.as_str()?.to_owned())
    }

    fn bytes(&self, name: &str) -> ProtocolResult<Vec<u8>> {
        self.get(name)?.as_bytes()
    }

    fn format(&self, expected: &str) -> ProtocolResult<()> {
        if self.get("format")?.as_str()? == expected {
            Ok(())
        } else {
            invalid("another record format")
        }
    }
}

/// Fails unless `value` is exactly the canonical value of what was decoded
/// from it: no field more, none of another form.
fn ensure_canonical(value: &Value, rebuilt: &Value) -> ProtocolResult<()> {
    if value == rebuilt {
        Ok(())
    } else {
        invalid("non-canonical record")
    }
}

pub(crate) fn root_from(value: &Value) -> ProtocolResult<SpaceRoot> {
    let fields = Fields::new(value)?;
    let root = SpaceRoot {
        id: fields.string("id")?,
        scope_id: fields.string("scopeId")?,
        identifier: fields.string("identifier")?,
        creator_account_id: fields.string("creatorAccountId")?,
        device_id: fields.string("deviceId")?,
        account_epoch_id: fields.string("accountEpochId")?,
        epoch_id: fields.string("epochId")?,
        mechanism: fields.string("mechanism")?,
        version: u32::try_from(fields.get("version")?.as_number()?)
            .map_err(|_| ProtocolError::InvalidRecord("version out of range"))?,
        verification: fields.bytes("verification")?,
        account_envelope: fields.bytes("accountEnvelope")?,
        signature: fields.bytes("signature")?,
    };
    ensure_canonical(value, &root_value(&root))?;
    Ok(root)
}

pub(crate) fn successor_from(value: &Value) -> ProtocolResult<SpaceSuccessor> {
    let fields = Fields::new(value)?;
    let successor = SpaceSuccessor {
        id: fields.string("id")?,
        scope_id: fields.string("scopeId")?,
        identifier: fields.string("identifier")?,
        predecessor: fields.string("predecessor")?,
        history_epoch_id: fields.string("historyEpochId")?,
        epoch_id: fields.string("epochId")?,
        author_account_id: fields.string("authorAccountId")?,
        author_device_id: fields.string("authorDeviceId")?,
        author_epoch_id: fields.string("authorEpochId")?,
        verification: fields.bytes("verification")?,
        history: fields.bytes("history")?,
        account_envelope: fields.bytes("accountEnvelope")?,
        signature: fields.bytes("signature")?,
    };
    ensure_canonical(value, &successor_value(&successor))?;
    Ok(successor)
}

/// `<lineage prefix hash>`: `sha256hex(B("jazz-e2ee-held-prefix-v1",
/// [root, [s₁, …, s_depth]]))`, `successors` being exactly the first
/// `depth` successors in chain order.
pub fn prefix_hash(root: &SpaceRoot, successors: &[SpaceSuccessor]) -> ProtocolResult<String> {
    let bytes = canonical::framed(
        PREFIX_FORMAT,
        Value::Array(vec![
            root_value(root),
            Value::Array(successors.iter().map(successor_value).collect()),
        ]),
    )?;
    Ok(hex(&sha256(&bytes)))
}

/// What a record is bound to: `{ application: Dₐ(self), device: deviceId,
/// keys: <device hash> }`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Binding {
    pub application: String,
    pub device: String,
    pub keys: String,
}

impl Binding {
    fn value(&self) -> Value {
        Value::object(vec![
            ("application", Some(Value::string(&self.application))),
            ("device", Some(Value::string(&self.device))),
            ("keys", Some(Value::string(&self.keys))),
        ])
    }

    fn from_value(value: &Value) -> ProtocolResult<Self> {
        let fields = Fields::new(value)?;
        Ok(Self {
            application: fields.string("application")?,
            device: fields.string("device")?,
            keys: fields.string("keys")?,
        })
    }
}

/// The lineage record: the space root and its successors, in chain order,
/// as far as the deepest held epoch, signed by the device.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LineageRecord {
    pub binding: Binding,
    pub root: SpaceRoot,
    pub successors: Vec<SpaceSuccessor>,
    pub signature: Vec<u8>,
}

impl LineageRecord {
    fn value(&self, signature: bool) -> Value {
        Value::object(vec![
            ("format", Some(Value::string(LINEAGE_FORMAT))),
            ("binding", Some(self.binding.value())),
            ("root", Some(root_value(&self.root))),
            (
                "successors",
                Some(Value::Array(
                    self.successors.iter().map(successor_value).collect(),
                )),
            ),
            (
                "signature",
                signature.then(|| Value::bytes(&self.signature)),
            ),
        ])
    }

    /// `B(format, record without signature)`: the bytes the device signs.
    pub fn signed_bytes(&self) -> ProtocolResult<Vec<u8>> {
        canonical::framed(LINEAGE_FORMAT, self.value(false))
    }

    /// The stored record.
    pub fn encode(&self) -> ProtocolResult<Vec<u8>> {
        self.value(true).encode()
    }

    pub fn decode(bytes: &[u8]) -> ProtocolResult<Self> {
        Self::from_value(&parse_canonical(bytes)?)
    }

    fn from_value(value: &Value) -> ProtocolResult<Self> {
        let fields = Fields::new(value)?;
        fields.format(LINEAGE_FORMAT)?;
        let record = Self {
            binding: Binding::from_value(fields.get("binding")?)?,
            root: root_from(fields.get("root")?)?,
            successors: fields
                .get("successors")?
                .as_array()?
                .iter()
                .map(successor_from)
                .collect::<ProtocolResult<_>>()?,
            signature: fields.bytes("signature")?,
        };
        ensure_canonical(value, &record.value(true))?;
        Ok(record)
    }
}

/// One held version, sealed to and signed by the device.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeldKeyRecord {
    pub binding: Binding,
    pub root_id: String,
    pub epoch_id: String,
    /// The version's depth in the chain (root = 0), a decimal string.
    pub depth: u64,
    /// `<lineage prefix hash>` of the lineage up to this depth.
    pub prefix: String,
    /// The durable transaction identity of a creation not yet accepted.
    pub unpublished: Option<String>,
    pub refused: bool,
    pub envelope: Vec<u8>,
    pub signature: Vec<u8>,
}

impl HeldKeyRecord {
    fn value(&self, envelope: bool, signature: bool) -> Value {
        Value::object(vec![
            ("format", Some(Value::string(KEY_FORMAT))),
            ("binding", Some(self.binding.value())),
            ("rootId", Some(Value::string(&self.root_id))),
            ("epochId", Some(Value::string(&self.epoch_id))),
            ("depth", Some(Value::String(self.depth.to_string()))),
            ("prefix", Some(Value::string(&self.prefix))),
            (
                "unpublished",
                self.unpublished.as_ref().map(|transaction| {
                    Value::object(vec![("transaction", Some(Value::string(transaction)))])
                }),
            ),
            ("refused", self.refused.then_some(Value::Bool(true))),
            ("envelope", envelope.then(|| Value::bytes(&self.envelope))),
            (
                "signature",
                signature.then(|| Value::bytes(&self.signature)),
            ),
        ])
    }

    /// `B(format, body)`, `body` the record without envelope and signature:
    /// the context the key is sealed with.
    pub fn body_bytes(&self) -> ProtocolResult<Vec<u8>> {
        canonical::framed(KEY_FORMAT, self.value(false, false))
    }

    /// `B(format, body + envelope)`: the bytes the device signs.
    pub fn signed_bytes(&self) -> ProtocolResult<Vec<u8>> {
        canonical::framed(KEY_FORMAT, self.value(true, false))
    }

    /// The stored record.
    pub fn encode(&self) -> ProtocolResult<Vec<u8>> {
        self.value(true, true).encode()
    }

    pub fn decode(bytes: &[u8]) -> ProtocolResult<Self> {
        Self::from_value(&parse_canonical(bytes)?)
    }

    fn from_value(value: &Value) -> ProtocolResult<Self> {
        let fields = Fields::new(value)?;
        fields.format(KEY_FORMAT)?;
        let depth = fields.get("depth")?.as_str()?;
        let unpublished = match value.field("unpublished") {
            None => None,
            Some(unpublished) => Some(Fields::new(unpublished)?.string("transaction")?),
        };
        let refused = match value.field("refused") {
            None => false,
            Some(Value::Bool(true)) => true,
            Some(_) => return invalid("refused is true when present"),
        };
        let record = Self {
            binding: Binding::from_value(fields.get("binding")?)?,
            root_id: fields.string("rootId")?,
            epoch_id: fields.string("epochId")?,
            depth: depth
                .parse()
                .map_err(|_| ProtocolError::InvalidRecord("depth is not a decimal"))?,
            prefix: fields.string("prefix")?,
            unpublished,
            refused,
            envelope: fields.bytes("envelope")?,
            signature: fields.bytes("signature")?,
        };
        ensure_canonical(value, &record.value(true, true))?;
        Ok(record)
    }
}

/// Parses text that must be the canonical encoding of what it parses to.
fn parse_canonical(bytes: &[u8]) -> ProtocolResult<Value> {
    fn sorted(value: &Value) -> bool {
        match value {
            Value::Array(items) => items.iter().all(sorted),
            Value::Object(fields) => {
                fields
                    .windows(2)
                    .all(|pair| super::super::json::utf16_cmp(&pair[0].0, &pair[1].0).is_lt())
                    && fields.iter().all(|(_, value)| sorted(value))
            }
            _ => true,
        }
    }
    let value = canonical::parse(bytes)?;
    if !sorted(&value) || value.encode()? != bytes {
        return invalid("non-canonical JSON");
    }
    Ok(value)
}

/// One held-key document: `{ format, revision, lineage, keys }`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HeldDocument {
    /// Advanced by the store on every removal, so in-flight reads notice
    /// and retry (§6.4).
    pub revision: u64,
    pub lineage: LineageRecord,
    pub keys: Vec<HeldKeyRecord>,
}

impl HeldDocument {
    pub fn encode(&self) -> ProtocolResult<Vec<u8>> {
        if self.revision > MAX_SAFE_INTEGER {
            return invalid("revision beyond the safe integer range");
        }
        Value::object(vec![
            ("format", Some(Value::string(DOCUMENT_FORMAT))),
            ("revision", Some(Value::Number(self.revision))),
            ("lineage", Some(self.lineage.value(true))),
            (
                "keys",
                Some(Value::Array(
                    self.keys.iter().map(|key| key.value(true, true)).collect(),
                )),
            ),
        ])
        .encode()
    }
}

/// A stored document whose records were decoded one by one: a record that
/// does not decode is `None`, to be skipped and reported, without failing
/// the others.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecodedDocument {
    pub revision: u64,
    pub lineage: Option<LineageRecord>,
    pub keys: Vec<Option<HeldKeyRecord>>,
}

impl From<HeldDocument> for DecodedDocument {
    fn from(document: HeldDocument) -> Self {
        Self {
            revision: document.revision,
            lineage: Some(document.lineage),
            keys: document.keys.into_iter().map(Some).collect(),
        }
    }
}

/// A stored value that is not a held-key document of this format: refused,
/// and never overwritten (§6.1).
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
#[error("Unrecognised E2EE held-key document")]
pub struct UnrecognisedDocument;

/// Decodes a stored held-key document. Each record is decoded on its
/// own: a record that does not decode, or is not in canonical form, is
/// `None`, to be skipped and reported (§6.2), and the others still load.
/// The document itself must be canonical around its records: text that is
/// not JSON of the store's shapes, an envelope in another form (whitespace,
/// key order, number form), another format, or fields other than
/// `format`, `revision`, `lineage` and `keys`, is [`UnrecognisedDocument`].
pub fn decode_document(bytes: &[u8]) -> Result<DecodedDocument, UnrecognisedDocument> {
    let fields = canonical::parse_object_parts(bytes).map_err(|_| UnrecognisedDocument)?;
    let [
        (format_name, format, _),
        (keys_name, _, keys),
        (lineage_name, _, lineage),
        (revision_name, revision, _),
    ] = fields.as_slice()
    else {
        return Err(UnrecognisedDocument);
    };
    let names = [format_name, keys_name, lineage_name, revision_name];
    if names != ["format", "keys", "lineage", "revision"]
        || format.as_str().ok() != Some(DOCUMENT_FORMAT)
    {
        return Err(UnrecognisedDocument);
    }
    let revision = revision.as_number().map_err(|_| UnrecognisedDocument)?;
    let keys = canonical::parse_array_parts(keys).map_err(|_| UnrecognisedDocument)?;
    // The envelope as the store writes it, around the records as stored.
    let mut envelope = Vec::with_capacity(bytes.len());
    envelope.extend_from_slice(b"{\"format\":");
    envelope.extend(
        Value::string(DOCUMENT_FORMAT)
            .encode()
            .map_err(|_| UnrecognisedDocument)?,
    );
    envelope.extend_from_slice(b",\"keys\":[");
    for (index, (_, key)) in keys.iter().enumerate() {
        if index > 0 {
            envelope.push(b',');
        }
        envelope.extend_from_slice(key);
    }
    envelope.extend_from_slice(b"],\"lineage\":");
    envelope.extend_from_slice(lineage);
    envelope.extend_from_slice(b",\"revision\":");
    envelope.extend(
        Value::Number(revision)
            .encode()
            .map_err(|_| UnrecognisedDocument)?,
    );
    envelope.push(b'}');
    if envelope != bytes {
        return Err(UnrecognisedDocument);
    }
    Ok(DecodedDocument {
        revision,
        lineage: LineageRecord::decode(lineage).ok(),
        keys: keys
            .iter()
            .map(|(_, key)| HeldKeyRecord::decode(key).ok())
            .collect(),
    })
}
