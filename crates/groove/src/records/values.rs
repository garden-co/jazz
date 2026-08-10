//! Logical record values, value types, and primitive encoders.
//!
//! This module owns [`Value`], [`ValueType`], enum schemas, and the recursive
//! encode/decode routines for scalars, tuples, arrays, and nullable values. It
//! does not know field names or physical record ordering; [`super`] wraps these
//! value encodings in [`super::RecordDescriptor`] layout and exposes
//! borrowed/owned record access. Query expressions and schemas refer to these
//! value types but do not perform byte-level encoding themselves.

use super::{Error, OwnedRecord, RecordDescriptor};
use std::collections::BTreeMap;

/// Stable compact identity for a physical enum/union occurrence.
pub fn variant_registry_id_for_path(path: &str) -> u64 {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in path.bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    if hash == 0 { 1 } else { hash }
}

#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub enum Value {
    U8(u8),
    U16(u16),
    U32(u32),
    U64(u64),
    F64(f64),
    Bool(bool),
    String(String),
    Bytes(Vec<u8>),
    Uuid(uuid::Uuid),
    Enum(u8),
    Tuple(Vec<Value>),
    Array(Vec<Value>),
    Nullable(Option<Box<Value>>),
    I64(i64),
    I32(i32),
    Record(OwnedRecord),
    Union(UnionValue),
}

/// One selected case of a [`UnionSchema`].
///
/// The tag is the declaration-order index of the case in its union schema.
/// It is encoded with the payload record as a bounded canonical `u32` varint.
#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Deserialize, serde::Serialize)]
pub struct UnionValue {
    tag: u32,
    record: OwnedRecord,
}

impl UnionValue {
    pub fn new(tag: u32, record: OwnedRecord) -> Self {
        Self { tag, record }
    }

    pub fn create(tag: u32, descriptor: RecordDescriptor, values: &[Value]) -> Result<Self, Error> {
        Ok(Self::new(
            tag,
            OwnedRecord::new(descriptor.create(values)?, descriptor),
        ))
    }

    pub fn tag(&self) -> u32 {
        self.tag
    }

    pub fn record(&self) -> &OwnedRecord {
        &self.record
    }

    pub fn into_record(self) -> OwnedRecord {
        self.record
    }
}

impl From<u8> for Value {
    fn from(value: u8) -> Self {
        Self::U8(value)
    }
}

impl From<u16> for Value {
    fn from(value: u16) -> Self {
        Self::U16(value)
    }
}

impl From<u32> for Value {
    fn from(value: u32) -> Self {
        Self::U32(value)
    }
}

impl From<u64> for Value {
    fn from(value: u64) -> Self {
        Self::U64(value)
    }
}

impl From<i32> for Value {
    fn from(value: i32) -> Self {
        Self::I32(value)
    }
}

impl From<i64> for Value {
    fn from(value: i64) -> Self {
        Self::I64(value)
    }
}

impl From<f64> for Value {
    fn from(value: f64) -> Self {
        Self::F64(value)
    }
}

impl From<bool> for Value {
    fn from(value: bool) -> Self {
        Self::Bool(value)
    }
}

impl From<String> for Value {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl From<&str> for Value {
    fn from(value: &str) -> Self {
        Self::String(value.to_owned())
    }
}

impl From<Vec<u8>> for Value {
    fn from(value: Vec<u8>) -> Self {
        Self::Bytes(value)
    }
}

impl From<&[u8]> for Value {
    fn from(value: &[u8]) -> Self {
        Self::Bytes(value.to_vec())
    }
}

impl From<uuid::Uuid> for Value {
    fn from(value: uuid::Uuid) -> Self {
        Self::Uuid(value)
    }
}

impl From<Vec<Value>> for Value {
    fn from(value: Vec<Value>) -> Self {
        Self::Array(value)
    }
}

impl From<Option<Value>> for Value {
    fn from(value: Option<Value>) -> Self {
        Self::Nullable(value.map(Box::new))
    }
}

/// Named enum schema stored as one order-preserving `u8` discriminant.
///
/// Declaration order is sort order. Appending variants is compatible with
/// existing stored rows; reordering or removing variants changes meaning.
#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Deserialize, serde::Serialize)]
pub struct EnumSchema {
    /// Durable identity of this enum occurrence. The enclosing table stamps
    /// unstamped schemas from their physical field path before persistence.
    #[serde(default)]
    pub registry_id: u64,
    pub name: String,
    pub variants: Vec<String>,
}

/// Named union schema whose declaration-order cases have stable `u32` tags.
///
/// A case name and its payload descriptor are part of the persistent schema.
/// Appending a case preserves existing tags; reordering, removing, or renaming
/// a case changes the meaning of stored values and is therefore incompatible.
#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Deserialize, serde::Serialize)]
pub struct UnionSchema {
    /// Durable identity of this union occurrence.
    #[serde(default)]
    pub registry_id: u64,
    pub name: String,
    pub cases: Vec<UnionCase>,
}

/// One named payload layout in a [`UnionSchema`].
#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Deserialize, serde::Serialize)]
pub struct UnionCase {
    pub name: String,
    pub payload: RecordDescriptor,
}

/// Persisted append-only case registry for one nested value occurrence.
#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Deserialize, serde::Serialize)]
pub enum VariantRegistry {
    Enum { variants: Vec<String> },
    Union { cases: Vec<String> },
}

impl UnionCase {
    pub fn new(name: impl Into<String>, payload: RecordDescriptor) -> Self {
        Self {
            name: name.into(),
            payload,
        }
    }
}

impl UnionSchema {
    pub fn new(
        name: impl Into<String>,
        cases: impl IntoIterator<Item = UnionCase>,
    ) -> Result<Self, Error> {
        let name = name.into();
        let cases = cases.into_iter().collect::<Vec<_>>();
        Self::validate_cases(&name, &cases)?;
        Ok(Self {
            registry_id: 0,
            name,
            cases,
        })
    }

    pub fn with_registry_id(mut self, registry_id: u64) -> Self {
        self.registry_id = registry_id;
        self
    }

    fn validate_cases(name: &str, cases: &[UnionCase]) -> Result<(), Error> {
        if !cases.is_empty() && u32::try_from(cases.len() - 1).is_err() {
            return Err(Error::UnionTooManyCases {
                name: name.to_owned(),
                cases: cases.len(),
            });
        }
        for (index, case) in cases.iter().enumerate() {
            if cases[..index]
                .iter()
                .any(|candidate| candidate.name == case.name)
            {
                return Err(Error::DuplicateUnionCaseName {
                    union_name: name.to_owned(),
                    case: case.name.clone(),
                });
            }
        }
        Ok(())
    }

    fn validate(&self) -> Result<(), Error> {
        Self::validate_cases(&self.name, &self.cases)
    }

    pub fn case(&self, tag: u32) -> Result<&UnionCase, Error> {
        self.cases
            .get(tag as usize)
            .ok_or_else(|| Error::UnknownUnionTag {
                union_name: self.name.clone(),
                tag,
            })
    }

    pub fn tag(&self, case: &str) -> Result<u32, Error> {
        self.cases
            .iter()
            .position(|candidate| candidate.name == case)
            .and_then(|index| u32::try_from(index).ok())
            .ok_or_else(|| Error::UnknownUnionCase {
                union_name: self.name.clone(),
                case: case.to_owned(),
            })
    }
}

fn assign_record_variant_registries(
    descriptor: &RecordDescriptor,
    path: &str,
    replace: bool,
) -> RecordDescriptor {
    RecordDescriptor::from_logical_fields(
        descriptor
            .fields()
            .iter()
            .enumerate()
            .map(|(index, field)| super::DescriptorField {
                name: field.name.clone(),
                value_type: {
                    let mut value_type = field.value_type.clone();
                    value_type.assign_variant_registries(&format!("{path}/field/{index}"), replace);
                    value_type
                },
            })
            .collect(),
    )
}

impl EnumSchema {
    pub fn new(
        name: impl Into<String>,
        variants: impl IntoIterator<Item = impl Into<String>>,
    ) -> Result<Self, Error> {
        let name = name.into();
        let variants = variants.into_iter().map(Into::into).collect::<Vec<_>>();
        if variants.len() > 256 {
            return Err(Error::EnumTooManyVariants {
                name,
                variants: variants.len(),
            });
        }
        Ok(Self {
            registry_id: 0,
            name,
            variants,
        })
    }

    pub fn with_registry_id(mut self, registry_id: u64) -> Self {
        self.registry_id = registry_id;
        self
    }

    pub fn discriminant(&self, variant: &str) -> Result<u8, Error> {
        self.variants
            .iter()
            .position(|candidate| candidate == variant)
            .and_then(|idx| u8::try_from(idx).ok())
            .ok_or_else(|| Error::UnknownEnumVariant {
                enum_name: self.name.clone(),
                variant: variant.to_owned(),
            })
    }

    pub fn variant(&self, discriminant: u8) -> Result<&str, Error> {
        self.variants
            .get(usize::from(discriminant))
            .map(String::as_str)
            .ok_or_else(|| Error::InvalidEnumDiscriminant {
                enum_name: self.name.clone(),
                discriminant,
            })
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Deserialize, serde::Serialize)]
pub enum ValueType {
    U8,
    U16,
    U32,
    U64,
    I32,
    I64,
    F64,
    Bool,
    String,
    Bytes,
    Uuid,
    Enum(EnumSchema),
    /// Fixed-width composite value encoded as concatenated member encodings.
    /// Variable-width members are deliberately rejected at schema construction.
    Tuple(Vec<ValueType>),
    Array(Box<ValueType>),
    Nullable(Box<ValueType>),
    /// A variable-width nested record interpreted by this inline descriptor.
    Record(Box<RecordDescriptor>),
    /// A variable-width tagged payload record selected by a stable union case.
    Union(Box<UnionSchema>),
}

impl ValueType {
    pub(crate) fn variant_registry_occurrence_count(&self) -> usize {
        match self {
            Self::Enum(_) => 1,
            Self::Union(schema) => {
                1 + schema
                    .cases
                    .iter()
                    .flat_map(|case| case.payload.fields())
                    .map(|field| field.value_type.variant_registry_occurrence_count())
                    .sum::<usize>()
            }
            Self::Tuple(members) => members
                .iter()
                .map(Self::variant_registry_occurrence_count)
                .sum(),
            Self::Array(inner) | Self::Nullable(inner) => inner.variant_registry_occurrence_count(),
            Self::Record(descriptor) => descriptor
                .fields()
                .iter()
                .map(|field| field.value_type.variant_registry_occurrence_count())
                .sum(),
            _ => 0,
        }
    }

    pub(crate) fn registry_compatible_with(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::Enum(left), Self::Enum(right)) if left.registry_id == right.registry_id => {
                left.variants.starts_with(&right.variants)
                    || right.variants.starts_with(&left.variants)
            }
            (Self::Union(left), Self::Union(right)) if left.registry_id == right.registry_id => {
                let shared = left.cases.len().min(right.cases.len());
                left.cases[..shared]
                    .iter()
                    .zip(&right.cases[..shared])
                    .all(|(a, b)| {
                        a.name == b.name
                            && a.payload.fields().len() == b.payload.fields().len()
                            && a.payload
                                .fields()
                                .iter()
                                .zip(b.payload.fields())
                                .all(|(x, y)| {
                                    x.name == y.name
                                        && x.value_type.registry_compatible_with(&y.value_type)
                                })
                    })
            }
            (Self::Tuple(left), Self::Tuple(right)) => {
                left.len() == right.len()
                    && left
                        .iter()
                        .zip(right)
                        .all(|(a, b)| a.registry_compatible_with(b))
            }
            (Self::Array(left), Self::Array(right))
            | (Self::Nullable(left), Self::Nullable(right)) => left.registry_compatible_with(right),
            (Self::Record(left), Self::Record(right)) => {
                left.fields().len() == right.fields().len()
                    && left.fields().iter().zip(right.fields()).all(|(a, b)| {
                        a.name == b.name && a.value_type.registry_compatible_with(&b.value_type)
                    })
            }
            _ => self == other,
        }
    }

    pub(crate) fn collect_variant_registries(&self, output: &mut BTreeMap<u64, VariantRegistry>) {
        match self {
            Self::Enum(schema) => {
                output.insert(
                    schema.registry_id,
                    VariantRegistry::Enum {
                        variants: schema.variants.clone(),
                    },
                );
            }
            Self::Tuple(members) => {
                for member in members {
                    member.collect_variant_registries(output);
                }
            }
            Self::Array(inner) | Self::Nullable(inner) => {
                inner.collect_variant_registries(output);
            }
            Self::Record(descriptor) => {
                for field in descriptor.fields() {
                    field.value_type.collect_variant_registries(output);
                }
            }
            Self::Union(schema) => {
                output.insert(
                    schema.registry_id,
                    VariantRegistry::Union {
                        cases: schema.cases.iter().map(|case| case.name.clone()).collect(),
                    },
                );
                for case in &schema.cases {
                    for field in case.payload.fields() {
                        field.value_type.collect_variant_registries(output);
                    }
                }
            }
            _ => {}
        }
    }

    /// Stamp every nested enum/union occurrence with its durable physical path.
    /// Existing explicit identities are retained so schema evolution can carry
    /// them across renames and descriptor reconstruction.
    pub(crate) fn stamp_variant_registries(mut self, path: &str) -> Self {
        self.assign_variant_registries(path, false);
        self
    }

    /// Bind the complete nested registry tree to a durable physical
    /// occurrence. This is used by catalogue lowerers after logical renames.
    pub fn rebind_variant_registries(mut self, path: &str) -> Self {
        self.assign_variant_registries(path, true);
        self
    }

    fn assign_variant_registries(&mut self, path: &str, replace: bool) {
        match self {
            Self::Enum(schema) => {
                if replace || schema.registry_id == 0 {
                    schema.registry_id = variant_registry_id_for_path(path);
                }
            }
            Self::Tuple(members) => {
                for (index, member) in members.iter_mut().enumerate() {
                    member.assign_variant_registries(&format!("{path}/tuple/{index}"), replace);
                }
            }
            Self::Array(inner) => {
                inner.assign_variant_registries(&format!("{path}/array"), replace);
            }
            Self::Nullable(inner) => {
                inner.assign_variant_registries(&format!("{path}/nullable"), replace);
            }
            Self::Record(descriptor) => {
                **descriptor = assign_record_variant_registries(
                    descriptor,
                    &format!("{path}/record"),
                    replace,
                );
            }
            Self::Union(schema) => {
                if replace || schema.registry_id == 0 {
                    schema.registry_id = variant_registry_id_for_path(path);
                }
                for (index, case) in schema.cases.iter_mut().enumerate() {
                    case.payload = assign_record_variant_registries(
                        &case.payload,
                        &format!("{path}/case/{index}"),
                        replace,
                    );
                }
            }
            _ => {}
        }
    }

    /// Wrap this type in an explicit nullable representation.
    pub fn nullable(self) -> Self {
        Self::Nullable(Box::new(self))
    }

    /// Wrap this type in a variable-length array representation.
    pub fn array_of(self) -> Self {
        Self::Array(Box::new(self))
    }

    /// Whether this type contains an inline record at any nesting depth.
    pub(crate) fn contains_record(&self) -> bool {
        match self {
            Self::Tuple(members) => members.iter().any(Self::contains_record),
            Self::Array(inner) | Self::Nullable(inner) => inner.contains_record(),
            Self::Record(_) | Self::Union(_) => true,
            _ => false,
        }
    }

    pub(super) fn fixed_size(&self) -> Option<usize> {
        match self {
            Self::U8 | Self::Bool => Some(1),
            Self::U16 => Some(2),
            Self::U64 | Self::I64 => Some(8),
            Self::U32 | Self::I32 => Some(4),
            Self::F64 => Some(8),
            Self::Uuid => Some(16),
            Self::Enum(_) => Some(1),
            Self::Tuple(members) => members
                .iter()
                .try_fold(0usize, |total, member| Some(total + member.fixed_size()?)),
            Self::Nullable(value_type) => value_type.fixed_size().map(|size| size + 1),
            Self::String | Self::Bytes | Self::Array(_) | Self::Record(_) | Self::Union(_) => None,
        }
    }

    pub(super) fn is_fixed_size(&self) -> bool {
        self.fixed_size().is_some()
    }
}

pub(super) fn encode_value(value: &Value, value_type: &ValueType) -> Result<Vec<u8>, Error> {
    let mut bytes = Vec::new();
    match (value, value_type) {
        (Value::String(value), ValueType::String) => bytes.extend(value.as_bytes()),
        (Value::Bytes(value), ValueType::Bytes) => bytes.extend(value),
        (Value::Uuid(value), ValueType::Uuid) => bytes.extend_from_slice(value.as_bytes()),
        (Value::String(value), ValueType::Enum(schema)) => bytes.push(schema.discriminant(value)?),
        (Value::Enum(value), ValueType::Enum(_)) => bytes.push(*value),
        (Value::Tuple(values), ValueType::Tuple(members)) => {
            encode_tuple(&mut bytes, values, members)?;
        }
        (Value::Array(values), ValueType::Array(element_type)) => {
            encode_array(&mut bytes, values, element_type)?;
        }
        (Value::Nullable(value), ValueType::Nullable(inner_type)) => {
            encode_nullable(&mut bytes, value.as_deref(), inner_type)?;
        }
        (Value::Record(record), ValueType::Record(_)) => {
            ensure_value_type(value, value_type)?;
            bytes.extend_from_slice(record.raw());
        }
        (Value::Union(union), ValueType::Union(schema)) => {
            ensure_union_value(union, schema)?;
            bytes.extend(super::encode_variant_record(union.tag, union.record.raw()));
        }
        _ if value_type.is_fixed_size() => encode_fixed_value(&mut bytes, value, value_type)?,
        _ => {
            return Err(Error::TypeMismatch {
                expected: value_type.clone(),
            });
        }
    }
    Ok(bytes)
}

pub(super) fn encode_fixed_value(
    bytes: &mut Vec<u8>,
    value: &Value,
    value_type: &ValueType,
) -> Result<(), Error> {
    match (value, value_type) {
        (Value::U8(value), ValueType::U8) => bytes.push(*value),
        (Value::U16(value), ValueType::U16) => bytes.extend(value.to_le_bytes()),
        (Value::U32(value), ValueType::U32) => bytes.extend(value.to_le_bytes()),
        (Value::U64(value), ValueType::U64) => bytes.extend(value.to_le_bytes()),
        (Value::I32(value), ValueType::I32) => bytes.extend(value.to_le_bytes()),
        (Value::I64(value), ValueType::I64) => bytes.extend(value.to_le_bytes()),
        (Value::F64(value), ValueType::F64) => {
            if value.is_nan() {
                return Err(Error::InvalidF64NaN);
            }
            bytes.extend(value.to_le_bytes());
        }
        (Value::Bool(value), ValueType::Bool) => bytes.push(u8::from(*value)),
        (Value::Uuid(value), ValueType::Uuid) => bytes.extend_from_slice(value.as_bytes()),
        (Value::String(value), ValueType::Enum(schema)) => bytes.push(schema.discriminant(value)?),
        (Value::Enum(value), ValueType::Enum(_)) => bytes.push(*value),
        (Value::Tuple(values), ValueType::Tuple(members)) => {
            encode_tuple(bytes, values, members)?;
        }
        (Value::Nullable(value), ValueType::Nullable(inner_type)) => {
            encode_nullable(bytes, value.as_deref(), inner_type)?;
        }
        (_, ValueType::Record(_)) => {
            return Err(Error::TypeMismatch {
                expected: value_type.clone(),
            });
        }
        (_, ValueType::Union(_)) => {
            return Err(Error::TypeMismatch {
                expected: value_type.clone(),
            });
        }
        _ => {
            return Err(Error::TypeMismatch {
                expected: value_type.clone(),
            });
        }
    }
    Ok(())
}

pub(super) fn decode_value(bytes: &[u8], value_type: &ValueType) -> Result<Value, Error> {
    match value_type {
        ValueType::U8 => Ok(Value::U8(read_exact::<1>(bytes)?[0])),
        ValueType::U16 => Ok(Value::U16(u16::from_le_bytes(read_exact::<2>(bytes)?))),
        ValueType::U32 => Ok(Value::U32(u32::from_le_bytes(read_exact::<4>(bytes)?))),
        ValueType::U64 => Ok(Value::U64(u64::from_le_bytes(read_exact::<8>(bytes)?))),
        ValueType::I32 => Ok(Value::I32(i32::from_le_bytes(read_exact::<4>(bytes)?))),
        ValueType::I64 => Ok(Value::I64(i64::from_le_bytes(read_exact::<8>(bytes)?))),
        ValueType::F64 => Ok(Value::F64(f64::from_le_bytes(read_exact::<8>(bytes)?))),
        ValueType::Bool => match read_exact::<1>(bytes)?[0] {
            0 => Ok(Value::Bool(false)),
            1 => Ok(Value::Bool(true)),
            value => Err(Error::InvalidBool(value)),
        },
        ValueType::String => String::from_utf8(bytes.to_vec())
            .map(Value::String)
            .map_err(|_| Error::InvalidUtf8),
        ValueType::Bytes => Ok(Value::Bytes(bytes.to_vec())),
        ValueType::Uuid => Ok(Value::Uuid(uuid::Uuid::from_bytes(read_exact::<16>(
            bytes,
        )?))),
        ValueType::Enum(schema) => {
            let discriminant = read_exact::<1>(bytes)?[0];
            schema
                .variant(discriminant)
                .map(|_| Value::Enum(discriminant))
        }
        ValueType::Tuple(members) => decode_tuple(bytes, members),
        ValueType::Array(element_type) => decode_array(bytes, element_type),
        ValueType::Nullable(inner_type) => decode_nullable(bytes, inner_type),
        ValueType::Record(descriptor) => {
            let values = descriptor.bind(bytes).to_values()?;
            let canonical = descriptor.create(&values)?;
            if canonical != bytes {
                return Err(Error::NonCanonicalRecord);
            }
            Ok(Value::Record(OwnedRecord::new(
                bytes.to_vec(),
                **descriptor,
            )))
        }
        ValueType::Union(schema) => {
            let (tag, payload) =
                super::split_variant_record(bytes).map_err(|error| match error {
                    Error::InvalidSchemaVersionHeader => Error::InvalidUnionHeader,
                    other => other,
                })?;
            let case = schema.case(tag)?;
            let values = case.payload.bind(payload).to_values()?;
            let canonical = case.payload.create(&values)?;
            if canonical != payload {
                return Err(Error::NonCanonicalRecord);
            }
            Ok(Value::Union(UnionValue::new(
                tag,
                OwnedRecord::new(payload.to_vec(), case.payload),
            )))
        }
    }
}

fn encode_nullable(
    bytes: &mut Vec<u8>,
    value: Option<&Value>,
    inner_type: &ValueType,
) -> Result<(), Error> {
    match value {
        Some(value) => {
            bytes.push(1);
            bytes.extend(encode_value(value, inner_type)?);
        }
        None => {
            bytes.push(0);
            if let Some(size) = inner_type.fixed_size() {
                // Fixed-width nulls reserve their payload width so the parent
                // fixed record layout stays seekable without offsets.
                bytes.resize(bytes.len() + size, 0);
            }
        }
    }
    Ok(())
}

fn decode_nullable(bytes: &[u8], inner_type: &ValueType) -> Result<Value, Error> {
    let (&flag, payload) = bytes.split_first().ok_or(Error::UnexpectedEof)?;
    match flag {
        0 => {
            if inner_type.fixed_size().is_some() {
                if payload.iter().any(|byte| *byte != 0) {
                    return Err(Error::InvalidOffset);
                }
            } else if !payload.is_empty() {
                return Err(Error::InvalidOffset);
            }
            Ok(Value::Nullable(None))
        }
        1 => decode_value(payload, inner_type).map(|value| Value::Nullable(Some(Box::new(value)))),
        value => Err(Error::InvalidNullFlag(value)),
    }
}

fn encode_array(
    bytes: &mut Vec<u8>,
    values: &[Value],
    element_type: &ValueType,
) -> Result<(), Error> {
    for value in values {
        ensure_value_type(value, element_type)?;
    }

    if element_type.is_fixed_size() {
        for value in values {
            encode_fixed_value(bytes, value, element_type)?;
        }
        return Ok(());
    }

    write_u32(bytes, usize_to_u32(values.len())?);
    let encoded_values = values
        .iter()
        .map(|value| encode_value(value, element_type))
        .collect::<Result<Vec<_>, _>>()?;
    let offset_table_size = encoded_values.len().saturating_sub(1) * 4;
    let mut next_offset = 4 + offset_table_size;
    for encoded in encoded_values
        .iter()
        .take(encoded_values.len().saturating_sub(1))
    {
        next_offset = checked_add(next_offset, encoded.len())?;
        write_u32(bytes, usize_to_u32(next_offset)?);
    }
    for encoded in encoded_values {
        bytes.extend(encoded);
    }
    Ok(())
}

fn decode_array(bytes: &[u8], element_type: &ValueType) -> Result<Value, Error> {
    if let Some(element_size) = element_type.fixed_size() {
        if element_size == 0 || !bytes.len().is_multiple_of(element_size) {
            return Err(Error::InvalidOffset);
        }
        return bytes
            .chunks_exact(element_size)
            .map(|chunk| decode_value(chunk, element_type))
            .collect::<Result<Vec<_>, _>>()
            .map(Value::Array);
    }

    let count = u32_to_usize(read_u32_at(bytes, 0)?)?;
    if count == 0 {
        return if bytes.len() == 4 {
            Ok(Value::Array(Vec::new()))
        } else {
            Err(Error::InvalidOffset)
        };
    }

    let offset_table_size = count.saturating_sub(1) * 4;
    let values_start = checked_add(4, offset_table_size)?;
    if bytes.len() < values_start {
        return Err(Error::UnexpectedEof);
    }

    let mut ends = read_offsets(bytes, 4, count.saturating_sub(1))?;
    ends.push(bytes.len());

    let mut values = Vec::with_capacity(count);
    let mut start = values_start;
    for end in ends {
        if end < start || end > bytes.len() {
            return Err(Error::InvalidOffset);
        }
        values.push(decode_value(&bytes[start..end], element_type)?);
        start = end;
    }

    Ok(Value::Array(values))
}

pub(super) fn ensure_value_type(value: &Value, value_type: &ValueType) -> Result<(), Error> {
    match (value, value_type) {
        (Value::U8(_), ValueType::U8)
        | (Value::U16(_), ValueType::U16)
        | (Value::U32(_), ValueType::U32)
        | (Value::U64(_), ValueType::U64)
        | (Value::I32(_), ValueType::I32)
        | (Value::I64(_), ValueType::I64)
        | (Value::Bool(_), ValueType::Bool)
        | (Value::String(_), ValueType::String)
        | (Value::Bytes(_), ValueType::Bytes)
        | (Value::Uuid(_), ValueType::Uuid) => Ok(()),
        (Value::F64(value), ValueType::F64) if !value.is_nan() => Ok(()),
        (Value::F64(_), ValueType::F64) => Err(Error::InvalidF64NaN),
        (Value::String(value), ValueType::Enum(schema)) => schema.discriminant(value).map(|_| ()),
        (Value::Enum(value), ValueType::Enum(schema)) => schema.variant(*value).map(|_| ()),
        (Value::Tuple(values), ValueType::Tuple(members)) => {
            if values.len() != members.len() {
                return Err(Error::ArityMismatch {
                    expected: members.len(),
                    actual: values.len(),
                });
            }
            for (value, member_type) in values.iter().zip(members) {
                if member_type.fixed_size().is_none() {
                    return Err(Error::InvalidTupleMember {
                        member_type: member_type.clone(),
                    });
                }
                ensure_value_type(value, member_type)?;
            }
            Ok(())
        }
        (Value::Array(values), ValueType::Array(element_type)) => {
            for value in values {
                ensure_value_type(value, element_type)?;
            }
            Ok(())
        }
        (Value::Nullable(None), ValueType::Nullable(_)) => Ok(()),
        (Value::Nullable(Some(value)), ValueType::Nullable(inner_type)) => {
            ensure_value_type(value, inner_type)
        }
        (Value::Record(record), ValueType::Record(descriptor)) => {
            if record.descriptor() != descriptor.as_ref() {
                return Err(Error::TypeMismatch {
                    expected: value_type.clone(),
                });
            }
            let values = record.to_values()?;
            if descriptor.create(&values)? != record.raw() {
                return Err(Error::NonCanonicalRecord);
            }
            Ok(())
        }
        (Value::Union(union), ValueType::Union(schema)) => ensure_union_value(union, schema),
        _ => Err(Error::TypeMismatch {
            expected: value_type.clone(),
        }),
    }
}

fn ensure_union_value(value: &UnionValue, schema: &UnionSchema) -> Result<(), Error> {
    let case = schema.case(value.tag)?;
    if value.record.descriptor() != &case.payload {
        return Err(Error::TypeMismatch {
            expected: ValueType::Union(Box::new(schema.clone())),
        });
    }
    let values = value.record.to_values()?;
    if case.payload.create(&values)? != value.record.raw() {
        return Err(Error::NonCanonicalRecord);
    }
    Ok(())
}

pub(super) fn validate_schema_value_type(value_type: &ValueType) -> Result<(), Error> {
    match value_type {
        ValueType::Tuple(members) => {
            for member in members {
                validate_schema_value_type(member)?;
                if member.fixed_size().is_none() {
                    return Err(Error::InvalidTupleMember {
                        member_type: member.clone(),
                    });
                }
            }
            Ok(())
        }
        ValueType::Array(inner) | ValueType::Nullable(inner) => validate_schema_value_type(inner),
        ValueType::Record(descriptor) => {
            for field in descriptor.fields() {
                validate_schema_value_type(&field.value_type)?;
            }
            Ok(())
        }
        ValueType::Union(schema) => {
            schema.validate()?;
            for case in &schema.cases {
                for field in case.payload.fields() {
                    validate_schema_value_type(&field.value_type)?;
                }
            }
            Ok(())
        }
        _ => Ok(()),
    }
}

fn encode_tuple(bytes: &mut Vec<u8>, values: &[Value], members: &[ValueType]) -> Result<(), Error> {
    if values.len() != members.len() {
        return Err(Error::ArityMismatch {
            expected: members.len(),
            actual: values.len(),
        });
    }
    for (value, member_type) in values.iter().zip(members) {
        encode_tuple_member(bytes, value, member_type)?;
    }
    Ok(())
}

fn encode_tuple_member(
    bytes: &mut Vec<u8>,
    value: &Value,
    value_type: &ValueType,
) -> Result<(), Error> {
    match (value, value_type) {
        (Value::U8(value), ValueType::U8) => bytes.push(*value),
        (Value::U16(value), ValueType::U16) => bytes.extend(value.to_be_bytes()),
        (Value::U32(value), ValueType::U32) => bytes.extend(value.to_be_bytes()),
        (Value::U64(value), ValueType::U64) => bytes.extend(value.to_be_bytes()),
        (Value::I32(value), ValueType::I32) => bytes.extend(order_preserving_i32(*value)),
        (Value::I64(value), ValueType::I64) => bytes.extend(order_preserving_i64(*value)),
        (Value::Bool(value), ValueType::Bool) => bytes.push(u8::from(*value)),
        (Value::Uuid(value), ValueType::Uuid) => bytes.extend_from_slice(value.as_bytes()),
        (Value::String(value), ValueType::Enum(schema)) => bytes.push(schema.discriminant(value)?),
        (Value::Enum(value), ValueType::Enum(_)) => bytes.push(*value),
        (Value::Tuple(values), ValueType::Tuple(members)) => encode_tuple(bytes, values, members)?,
        (Value::Nullable(value), ValueType::Nullable(inner_type)) => {
            bytes.push(u8::from(value.is_some()));
            if let Some(value) = value.as_deref() {
                encode_tuple_member(bytes, value, inner_type)?;
            } else if let Some(size) = inner_type.fixed_size() {
                bytes.resize(bytes.len() + size, 0);
            } else {
                return Err(Error::InvalidTupleMember {
                    member_type: inner_type.as_ref().clone(),
                });
            }
        }
        _ => {
            return Err(Error::TypeMismatch {
                expected: value_type.clone(),
            });
        }
    }
    Ok(())
}

fn decode_tuple(bytes: &[u8], members: &[ValueType]) -> Result<Value, Error> {
    let mut values = Vec::with_capacity(members.len());
    let mut offset = 0usize;
    for member_type in members {
        let width = member_type
            .fixed_size()
            .ok_or_else(|| Error::InvalidTupleMember {
                member_type: member_type.clone(),
            })?;
        let end = checked_add(offset, width)?;
        let member = bytes.get(offset..end).ok_or(Error::UnexpectedEof)?;
        values.push(decode_tuple_member(member, member_type)?);
        offset = end;
    }
    if offset != bytes.len() {
        return Err(Error::InvalidOffset);
    }
    Ok(Value::Tuple(values))
}

fn decode_tuple_member(bytes: &[u8], value_type: &ValueType) -> Result<Value, Error> {
    match value_type {
        ValueType::U8 => Ok(Value::U8(read_exact::<1>(bytes)?[0])),
        ValueType::U16 => Ok(Value::U16(u16::from_be_bytes(read_exact::<2>(bytes)?))),
        ValueType::U32 => Ok(Value::U32(u32::from_be_bytes(read_exact::<4>(bytes)?))),
        ValueType::U64 => Ok(Value::U64(u64::from_be_bytes(read_exact::<8>(bytes)?))),
        ValueType::I32 => Ok(Value::I32(i32_from_order_preserving(read_exact::<4>(
            bytes,
        )?))),
        ValueType::I64 => Ok(Value::I64(i64_from_order_preserving(read_exact::<8>(
            bytes,
        )?))),
        ValueType::Bool => match read_exact::<1>(bytes)?[0] {
            0 => Ok(Value::Bool(false)),
            1 => Ok(Value::Bool(true)),
            value => Err(Error::InvalidBool(value)),
        },
        ValueType::Uuid => Ok(Value::Uuid(uuid::Uuid::from_bytes(read_exact::<16>(
            bytes,
        )?))),
        ValueType::Enum(schema) => {
            let discriminant = read_exact::<1>(bytes)?[0];
            schema
                .variant(discriminant)
                .map(|_| Value::Enum(discriminant))
        }
        ValueType::Tuple(members) => decode_tuple(bytes, members),
        ValueType::Nullable(inner_type) => decode_nullable(bytes, inner_type),
        ValueType::F64
        | ValueType::String
        | ValueType::Bytes
        | ValueType::Array(_)
        | ValueType::Record(_)
        | ValueType::Union(_) => Err(Error::InvalidTupleMember {
            member_type: value_type.clone(),
        }),
    }
}

fn read_offsets(bytes: &[u8], start: usize, count: usize) -> Result<Vec<usize>, Error> {
    (0..count)
        .map(|idx| read_u32_at(bytes, start + idx * 4).and_then(u32_to_usize))
        .collect()
}

fn read_u32_at(bytes: &[u8], start: usize) -> Result<u32, Error> {
    let end = checked_add(start, 4)?;
    if end > bytes.len() {
        return Err(Error::UnexpectedEof);
    }
    Ok(u32::from_le_bytes(
        bytes[start..end]
            .try_into()
            .map_err(|_| Error::UnexpectedEof)?,
    ))
}

fn read_exact<const N: usize>(bytes: &[u8]) -> Result<[u8; N], Error> {
    if bytes.len() != N {
        return Err(Error::UnexpectedEof);
    }
    bytes.try_into().map_err(|_| Error::UnexpectedEof)
}

fn order_preserving_i64(value: i64) -> [u8; 8] {
    ((value as u64) ^ (1_u64 << 63)).to_be_bytes()
}

fn i64_from_order_preserving(bytes: [u8; 8]) -> i64 {
    (u64::from_be_bytes(bytes) ^ (1_u64 << 63)) as i64
}

fn order_preserving_i32(value: i32) -> [u8; 4] {
    ((value as u32) ^ (1_u32 << 31)).to_be_bytes()
}

fn i32_from_order_preserving(bytes: [u8; 4]) -> i32 {
    (u32::from_be_bytes(bytes) ^ (1_u32 << 31)) as i32
}

pub(super) fn write_u32(bytes: &mut Vec<u8>, value: u32) {
    bytes.extend(value.to_le_bytes());
}

pub(super) fn checked_add(left: usize, right: usize) -> Result<usize, Error> {
    left.checked_add(right).ok_or(Error::LengthOverflow)
}

pub(super) fn usize_to_u32(value: usize) -> Result<u32, Error> {
    value.try_into().map_err(|_| Error::LengthOverflow)
}

fn u32_to_usize(value: u32) -> Result<usize, Error> {
    value.try_into().map_err(|_| Error::LengthOverflow)
}
