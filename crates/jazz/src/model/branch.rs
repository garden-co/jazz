//! Branch coordinates and snapshot references: the canonical encodings of
//! branch-column values and branch keys, and the dotted snapshot ref used by
//! historic reads. Re-exported from `crate::protocol`.

use std::collections::BTreeMap;

use groove::records::{RecordDescriptor, Value, ValueType};

use crate::ids::NodeUuid;
use crate::time::{GlobalTime, TxTime};
use crate::tx::{Snapshot, TxId};

/// Canonical named values for one schema-wide branch selector.
#[derive(
    Clone,
    Debug,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    serde::Deserialize,
    serde::Serialize,
)]
pub struct BranchSelector {
    /// Canonically encoded typed values keyed by branch-column name.
    pub values: BTreeMap<String, BranchColumnValue>,
}

impl BranchSelector {
    /// Construct a selector from named branch-column values.
    pub fn new(values: impl IntoIterator<Item = (impl Into<String>, Value)>) -> Self {
        Self {
            values: values
                .into_iter()
                .map(|(name, value)| (name.into(), BranchColumnValue::from(value)))
                .collect(),
        }
    }
}

impl BranchViewBase {
    /// Use the live current contents of a base branch.
    pub fn current(branch: BranchSelector) -> Self {
        Self::Current(branch)
    }

    /// Freeze a base branch at an application-resolved snapshot reference.
    pub fn snapshot(branch: BranchSelector, snapshot: SnapshotRef) -> Self {
        Self::Snapshot { branch, snapshot }
    }
}

/// Error decoding the frozen branch-coordinate codec.
#[derive(Debug, thiserror::Error)]
pub enum BranchCodecError {
    /// The envelope version, tag, length, ordering, or payload is invalid.
    #[error("invalid branch codec envelope")]
    InvalidEnvelope,
    /// The supplied value or declared column type cannot be a branch column.
    #[error("unsupported branch column type")]
    UnsupportedType,
    /// The encoded scalar tag does not match the declared schema type.
    #[error("branch column encoding does not match its declared type")]
    TypeMismatch,
    /// Groove rejected the declared-type payload.
    #[error("invalid Groove branch column payload: {0}")]
    Groove(#[from] groove::records::Error),
}

const BRANCH_COLUMN_CODEC_VERSION: u8 = 1;
const BRANCH_COLUMN_U8: u8 = 0;
const BRANCH_COLUMN_U16: u8 = 1;
const BRANCH_COLUMN_U32: u8 = 2;
const BRANCH_COLUMN_U64: u8 = 3;
const BRANCH_COLUMN_I32: u8 = 4;
const BRANCH_COLUMN_I64: u8 = 5;
const BRANCH_COLUMN_STRING: u8 = 6;
const BRANCH_COLUMN_UUID: u8 = 7;
const BRANCH_COLUMN_ENUM_TAG: u8 = 8;

/// Canonical wire/storage encoding of one branch-column value.
///
/// Byte zero is the codec version, byte one is a permanently assigned scalar
/// tag, and the remainder is the canonical Groove encoding under the declared
/// column type. Keeping the tag outside Groove lets a selector cross the wire
/// before a table is chosen while exact [`BranchKey`] construction still
/// re-encodes and validates the payload against that table's schema.
#[derive(
    Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Deserialize, serde::Serialize,
)]
pub struct BranchColumnValue(pub Vec<u8>);

impl From<Value> for BranchColumnValue {
    fn from(value: Value) -> Self {
        let value_type = match &value {
            Value::U8(_) => ValueType::U8,
            Value::U16(_) => ValueType::U16,
            Value::U32(_) => ValueType::U32,
            Value::U64(_) => ValueType::U64,
            Value::I32(_) => ValueType::I32,
            Value::I64(_) => ValueType::I64,
            Value::String(_) => ValueType::String,
            Value::Uuid(_) => ValueType::Uuid,
            Value::EnumTag(tag) => {
                return Self(vec![
                    BRANCH_COLUMN_CODEC_VERSION,
                    BRANCH_COLUMN_ENUM_TAG,
                    *tag,
                ]);
            }
            // Preserve `BranchSelector::new` as an infallible constructor.
            // Schema projection rejects this unknown tag before it can become
            // an exact key or persistent coordinate.
            _ => return Self(vec![BRANCH_COLUMN_CODEC_VERSION, u8::MAX]),
        };
        Self::encode_typed(&value, &value_type)
            .expect("inferred branch selector type accepts its value")
    }
}

impl BranchColumnValue {
    /// Encode one value using its schema-declared Groove column type.
    pub(crate) fn encode_typed(
        value: &Value,
        value_type: &ValueType,
    ) -> Result<Self, BranchCodecError> {
        let tag = branch_column_tag(value_type).ok_or(BranchCodecError::UnsupportedType)?;
        let descriptor = RecordDescriptor::new([("value", value_type.clone())]);
        let payload = descriptor.create(std::slice::from_ref(value))?;
        let mut bytes = Vec::with_capacity(2 + payload.len());
        bytes.extend([BRANCH_COLUMN_CODEC_VERSION, tag]);
        bytes.extend(payload);
        Ok(Self(bytes))
    }

    /// Decode a selector value before a table-specific type is available.
    pub fn decode(&self) -> Result<Value, BranchCodecError> {
        let (tag, payload) = self.envelope()?;
        if tag == BRANCH_COLUMN_ENUM_TAG {
            return match payload {
                [tag] => Ok(Value::EnumTag(*tag)),
                _ => Err(BranchCodecError::InvalidEnvelope),
            };
        }
        let value_type = branch_column_type(tag).ok_or(BranchCodecError::InvalidEnvelope)?;
        self.decode_as(&value_type)
    }

    /// Decode and canonically validate an exact key value against its schema.
    pub(crate) fn decode_as(&self, value_type: &ValueType) -> Result<Value, BranchCodecError> {
        let (tag, payload) = self.envelope()?;
        if branch_column_tag(value_type) != Some(tag) {
            return Err(BranchCodecError::TypeMismatch);
        }
        let descriptor = RecordDescriptor::new([("value", value_type.clone())]);
        let value = descriptor.get_idx(payload, 0)?;
        if descriptor.create(std::slice::from_ref(&value))? != payload {
            return Err(BranchCodecError::InvalidEnvelope);
        }
        Ok(value)
    }

    fn envelope(&self) -> Result<(u8, &[u8]), BranchCodecError> {
        match self.0.as_slice() {
            [BRANCH_COLUMN_CODEC_VERSION, tag, payload @ ..] => Ok((*tag, payload)),
            _ => Err(BranchCodecError::InvalidEnvelope),
        }
    }
}

fn branch_column_tag(value_type: &ValueType) -> Option<u8> {
    match value_type {
        ValueType::U8 => Some(BRANCH_COLUMN_U8),
        ValueType::U16 => Some(BRANCH_COLUMN_U16),
        ValueType::U32 => Some(BRANCH_COLUMN_U32),
        ValueType::U64 => Some(BRANCH_COLUMN_U64),
        ValueType::I32 => Some(BRANCH_COLUMN_I32),
        ValueType::I64 => Some(BRANCH_COLUMN_I64),
        ValueType::String => Some(BRANCH_COLUMN_STRING),
        ValueType::Uuid => Some(BRANCH_COLUMN_UUID),
        ValueType::EnumTag(_) => Some(BRANCH_COLUMN_ENUM_TAG),
        _ => None,
    }
}

fn branch_column_type(tag: u8) -> Option<ValueType> {
    match tag {
        BRANCH_COLUMN_U8 => Some(ValueType::U8),
        BRANCH_COLUMN_U16 => Some(ValueType::U16),
        BRANCH_COLUMN_U32 => Some(ValueType::U32),
        BRANCH_COLUMN_U64 => Some(ValueType::U64),
        BRANCH_COLUMN_I32 => Some(ValueType::I32),
        BRANCH_COLUMN_I64 => Some(ValueType::I64),
        BRANCH_COLUMN_STRING => Some(ValueType::String),
        BRANCH_COLUMN_UUID => Some(ValueType::Uuid),
        _ => None,
    }
}

const BRANCH_KEY_CODEC_VERSION: u8 = 1;

/// Exact, table-projected branch coordinate carried by every row version.
#[derive(
    Clone,
    Debug,
    Default,
    PartialEq,
    Eq,
    PartialOrd,
    Ord,
    Hash,
    serde::Deserialize,
    serde::Serialize,
)]
pub struct BranchKey {
    /// Values ordered by branch-column name.
    pub values: Vec<(String, BranchColumnValue)>,
}

impl BranchKey {
    /// Check and encode the physical branch-local row-key prefix.
    pub fn try_canonical_bytes(&self) -> Result<Vec<u8>, BranchCodecError> {
        if !self.is_canonical() {
            return Err(BranchCodecError::InvalidEnvelope);
        }
        let mut bytes = Vec::new();
        bytes.push(BRANCH_KEY_CODEC_VERSION);
        put_branch_component_len(&mut bytes, self.values.len());
        for (name, value) in &self.values {
            put_branch_component_len(&mut bytes, name.len());
            bytes.extend(name.as_bytes());
            put_branch_component_len(&mut bytes, value.0.len());
            bytes.extend(&value.0);
        }
        Ok(bytes)
    }

    /// Canonical bytes for an already-validated key.
    pub fn canonical_bytes(&self) -> Vec<u8> {
        self.try_canonical_bytes()
            .expect("branch keys must be canonical before serialization")
    }

    /// Decode a persisted exact branch key.
    pub fn from_canonical_bytes(bytes: &[u8]) -> Result<Self, BranchCodecError> {
        let [BRANCH_KEY_CODEC_VERSION, rest @ ..] = bytes else {
            return Err(BranchCodecError::InvalidEnvelope);
        };
        let mut cursor = rest;
        let count = take_branch_component_len(&mut cursor)?;
        if count > cursor.len() / 8 {
            return Err(BranchCodecError::InvalidEnvelope);
        }
        let mut values = Vec::with_capacity(count);
        for _ in 0..count {
            let name_len = take_branch_component_len(&mut cursor)?;
            let name = take_branch_component(&mut cursor, name_len)?;
            let name = std::str::from_utf8(name)
                .map_err(|_| BranchCodecError::InvalidEnvelope)?
                .to_owned();
            if name.is_empty() {
                return Err(BranchCodecError::InvalidEnvelope);
            }
            let value_len = take_branch_component_len(&mut cursor)?;
            let value = BranchColumnValue(take_branch_component(&mut cursor, value_len)?.to_vec());
            value.decode()?;
            if values.last().is_some_and(|(previous, _)| previous >= &name) {
                return Err(BranchCodecError::InvalidEnvelope);
            }
            values.push((name, value));
        }
        if !cursor.is_empty() {
            return Err(BranchCodecError::InvalidEnvelope);
        }
        Ok(Self { values })
    }

    /// Whether this key has the one portable ordering and value encoding.
    pub fn is_canonical(&self) -> bool {
        self.values.windows(2).all(|pair| pair[0].0 < pair[1].0)
            && self
                .values
                .iter()
                .all(|(name, value)| !name.is_empty() && value.decode().is_ok())
    }
}

fn put_branch_component_len(bytes: &mut Vec<u8>, len: usize) {
    let len = u32::try_from(len).expect("branch codec component exceeds u32");
    bytes.extend(len.to_le_bytes());
}

fn take_branch_component_len(cursor: &mut &[u8]) -> Result<usize, BranchCodecError> {
    let encoded = take_branch_component(cursor, 4)?;
    Ok(u32::from_le_bytes(
        encoded
            .try_into()
            .map_err(|_| BranchCodecError::InvalidEnvelope)?,
    ) as usize)
}

fn take_branch_component<'a>(
    cursor: &mut &'a [u8],
    len: usize,
) -> Result<&'a [u8], BranchCodecError> {
    let (value, rest) = cursor
        .split_at_checked(len)
        .ok_or(BranchCodecError::InvalidEnvelope)?;
    *cursor = rest;
    Ok(value)
}

/// Optional base composed underneath the live head of a branch view.
#[derive(
    Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Deserialize, serde::Serialize,
)]
pub enum BranchViewBase {
    /// A base that continues to observe current writes.
    Current(BranchSelector),
    /// A base frozen at one resolved snapshot.
    Snapshot {
        /// Branch key read at the frozen cut.
        branch: BranchSelector,
        /// Historic frontier shared by every source in the view.
        snapshot: SnapshotRef,
    },
}

/// Dotted snapshot ref used by historic read views.
#[derive(
    Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, serde::Deserialize, serde::Serialize,
)]
pub struct SnapshotRef {
    /// Node that owns the local snapshot prefix.
    pub owner: NodeUuid,
    /// Contiguous global base visible at snapshot time.
    pub global_base: GlobalTime,
    /// Owner-local HLC prefix visible at snapshot time.
    pub local_base: TxTime,
    /// Individual transaction dots above the frontier.
    #[serde(default)]
    pub dots: Vec<TxId>,
}

impl From<Snapshot> for SnapshotRef {
    fn from(snapshot: Snapshot) -> Self {
        Self {
            owner: snapshot.owner,
            global_base: snapshot.global_base,
            local_base: snapshot.local_base,
            dots: snapshot.dots,
        }
    }
}
