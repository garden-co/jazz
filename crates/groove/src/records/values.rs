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

/// Reserved high bit marking an engine-owned registry shared at one explicit
/// internal relational boundary.
const SYSTEM_REGISTRY_MARKER: u64 = 1 << 63;

/// Stable compact identity for a physical enum occurrence.
pub fn variant_registry_id_for_path(path: &str) -> u64 {
    let mut hash = 0xcbf29ce484222325_u64;
    for byte in path.bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x100000001b3);
    }
    let hash = hash & !SYSTEM_REGISTRY_MARKER;
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
    /// Engine-owned indirect physical arm. Public result boundaries
    /// materialize this back into the declared logical scalar type.
    Large(crate::large_values::LargeValueRef),
    Uuid(uuid::Uuid),
    EnumTag(u8),
    Tuple(Vec<Value>),
    Array(Vec<Value>),
    Nullable(Option<Box<Value>>),
    I64(i64),
    I32(i32),
    Record(OwnedRecord),
    Enum(EnumValue),
}

/// One selected case of a [`EnumSchema`].
///
/// The tag is the declaration-order index of the case in its enum schema.
/// It is encoded with the payload record as a bounded canonical `u32` varint.
#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Deserialize, serde::Serialize)]
pub struct EnumValue {
    tag: u32,
    record: OwnedRecord,
}

impl EnumValue {
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
pub struct ScalarEnumSchema {
    /// Durable identity of this enum occurrence. The enclosing table stamps
    /// unstamped schemas from their physical field path before persistence.
    #[serde(default)]
    registry_id: u64,
    pub name: String,
    pub variants: Vec<String>,
}

/// Opaque token for an engine-owned enum registry shared at an explicitly
/// defined relational boundary.
#[derive(Clone, Copy, Debug)]
pub struct SystemVariantRegistry(u64);

impl SystemVariantRegistry {
    /// The internal deletion-state register that joins content and deletion
    /// facts in Jazz's query engine.
    pub fn deletion_state() -> Self {
        Self(variant_registry_id_for_path("jazz/internal/deletion") | SYSTEM_REGISTRY_MARKER)
    }
}

/// Named enum schema whose declaration-order cases have stable `u32` tags.
///
/// A case name and its payload descriptor are part of the persistent schema.
/// Appending a case preserves existing tags; reordering, removing, or renaming
/// a case changes the meaning of stored values and is therefore incompatible.
#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Deserialize, serde::Serialize)]
pub struct EnumSchema {
    /// Durable identity of this enum occurrence.
    #[serde(default)]
    pub registry_id: u64,
    pub name: String,
    pub cases: Vec<EnumCase>,
}

/// One named payload layout in a [`EnumSchema`].
#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Deserialize, serde::Serialize)]
pub struct EnumCase {
    pub name: String,
    pub payload: RecordDescriptor,
}

/// Persisted append-only case registry for one nested value occurrence.
#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Deserialize, serde::Serialize)]
pub enum VariantRegistry {
    EnumTag { variants: Vec<String> },
    Enum { cases: Vec<String> },
}

impl EnumCase {
    pub fn new(name: impl Into<String>, payload: RecordDescriptor) -> Self {
        Self {
            name: name.into(),
            payload,
        }
    }
}

impl EnumSchema {
    pub fn new(
        name: impl Into<String>,
        cases: impl IntoIterator<Item = EnumCase>,
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

    fn validate_cases(name: &str, cases: &[EnumCase]) -> Result<(), Error> {
        if !cases.is_empty() && u32::try_from(cases.len() - 1).is_err() {
            return Err(Error::EnumTooManyCases {
                name: name.to_owned(),
                cases: cases.len(),
            });
        }
        for (index, case) in cases.iter().enumerate() {
            if cases[..index]
                .iter()
                .any(|candidate| candidate.name == case.name)
            {
                return Err(Error::DuplicateEnumCaseName {
                    enum_name: name.to_owned(),
                    case: case.name.clone(),
                });
            }
        }
        Ok(())
    }

    fn validate(&self) -> Result<(), Error> {
        Self::validate_cases(&self.name, &self.cases)
    }

    pub fn case(&self, tag: u32) -> Result<&EnumCase, Error> {
        self.cases
            .get(tag as usize)
            .ok_or_else(|| Error::UnknownEnumTag {
                enum_name: self.name.clone(),
                tag,
            })
    }

    pub fn tag(&self, case: &str) -> Result<u32, Error> {
        self.cases
            .iter()
            .position(|candidate| candidate.name == case)
            .and_then(|index| u32::try_from(index).ok())
            .ok_or_else(|| Error::UnknownEnumCase {
                enum_name: self.name.clone(),
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

impl ScalarEnumSchema {
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
        self.registry_id = registry_id & !SYSTEM_REGISTRY_MARKER;
        self
    }

    pub fn registry_id(&self) -> u64 {
        self.registry_id
    }

    /// Mark an engine-owned enum identity that is intentionally shared across
    /// one explicit internal relational boundary.
    pub fn with_system_registry(mut self, registry: SystemVariantRegistry) -> Self {
        self.registry_id = registry.0;
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
    /// Private engine-only physical encodings. Its payload type is crate
    /// private, so public schema/binding callers cannot construct one.
    Internal(InternalValueType),
    Uuid,
    EnumTag(ScalarEnumSchema),
    /// Fixed-width composite value encoded as concatenated member encodings.
    /// Variable-width members are deliberately rejected at schema construction.
    Tuple(Vec<ValueType>),
    Array(Box<ValueType>),
    Nullable(Box<ValueType>),
    /// A variable-width nested record interpreted by this inline descriptor.
    Record(Box<RecordDescriptor>),
    /// A variable-width tagged payload record selected by a stable enum case.
    Enum(Box<EnumSchema>),
}

/// Opaque marker for physical-only value encodings beneath the public
/// `ValueType` algebra. Its sole field is private, so callers cannot construct
/// an internal type through `ValueType` or `ColumnType`.
#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Deserialize, serde::Serialize)]
pub struct InternalValueType(InternalValueTypeRepr);

#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Deserialize, serde::Serialize)]
enum InternalValueTypeRepr {
    RawString,
    RawBytes,
    StoredScalar(crate::large_values::LargeValueKind),
}

/// Whether a value can be used as an ordered `collect_by` key.
///
/// Nullable values retain the ordering of their inner scalar. Composite values
/// deliberately do not: collector keys must be independently ordered values.
pub fn collect_by_ordered_scalar(value_type: &ValueType) -> bool {
    match value_type {
        ValueType::Nullable(inner) => collect_by_ordered_scalar(inner),
        ValueType::U8
        | ValueType::U16
        | ValueType::U32
        | ValueType::U64
        | ValueType::I32
        | ValueType::I64
        | ValueType::F64
        | ValueType::Bool
        | ValueType::String
        | ValueType::Bytes
        | ValueType::Uuid
        | ValueType::EnumTag(_) => true,
        _ => false,
    }
}

impl ValueType {
    /// Whether this is an engine-only physical backing type. Public schema and
    /// binding layers use this predicate to reject it without gaining access to
    /// the private representation.
    pub fn is_internal_storage_type(&self) -> bool {
        matches!(self, Self::Internal(_))
    }

    pub(crate) fn raw_string() -> Self {
        Self::Internal(InternalValueType(InternalValueTypeRepr::RawString))
    }

    pub(crate) fn raw_bytes() -> Self {
        Self::Internal(InternalValueType(InternalValueTypeRepr::RawBytes))
    }

    pub(crate) fn stored_scalar(kind: crate::large_values::LargeValueKind) -> Self {
        Self::Internal(InternalValueType(InternalValueTypeRepr::StoredScalar(kind)))
    }

    /// Whether a value of this type can contain an indirect stored scalar.
    ///
    /// The IVM evaluator uses this schema-only proof to avoid decoding and
    /// rebuilding records when an operator inspects fields that cannot block
    /// on large-value hydration.
    pub(crate) fn may_contain_stored_scalar(&self) -> bool {
        match self {
            Self::Internal(InternalValueType(InternalValueTypeRepr::StoredScalar(_))) => true,
            Self::Tuple(members) => members.iter().any(Self::may_contain_stored_scalar),
            Self::Array(inner) | Self::Nullable(inner) => inner.may_contain_stored_scalar(),
            Self::Record(descriptor) => descriptor
                .fields()
                .iter()
                .any(|field| field.value_type.may_contain_stored_scalar()),
            Self::Enum(schema) => schema.cases.iter().any(|case| {
                case.payload
                    .fields()
                    .iter()
                    .any(|field| field.value_type.may_contain_stored_scalar())
            }),
            _ => false,
        }
    }
}

impl ValueType {
    pub(crate) fn variant_registry_occurrence_count(&self) -> usize {
        match self {
            Self::EnumTag(_) => 1,
            Self::Enum(schema) => {
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
            (Self::EnumTag(left), Self::EnumTag(right))
                if left.registry_id == right.registry_id =>
            {
                left.variants.starts_with(&right.variants)
                    || right.variants.starts_with(&left.variants)
            }
            (Self::Enum(left), Self::Enum(right)) if left.registry_id == right.registry_id => {
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

    /// True when two descriptors have exactly the same byte layout and differ
    /// only in the durable identities assigned to their enum registries.
    ///
    /// Unlike [`Self::registry_compatible_with`], this deliberately does not
    /// admit append-only growth: a raw record projector copies enum bytes, so
    /// the target must be able to decode every tag without a semantic remap.
    pub(crate) fn registry_rebound_layout_compatible_with(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::EnumTag(left), Self::EnumTag(right)) => left.variants == right.variants,
            (Self::Enum(left), Self::Enum(right)) => {
                left.cases.len() == right.cases.len()
                    && left.cases.iter().zip(&right.cases).all(|(a, b)| {
                        a.name == b.name
                            && a.payload.fields().len() == b.payload.fields().len()
                            && a.payload
                                .fields()
                                .iter()
                                .zip(b.payload.fields())
                                .all(|(x, y)| {
                                    x.name == y.name
                                        && x.value_type
                                            .registry_rebound_layout_compatible_with(&y.value_type)
                                })
                    })
            }
            (Self::Tuple(left), Self::Tuple(right)) => {
                left.len() == right.len()
                    && left
                        .iter()
                        .zip(right)
                        .all(|(a, b)| a.registry_rebound_layout_compatible_with(b))
            }
            (Self::Array(left), Self::Array(right))
            | (Self::Nullable(left), Self::Nullable(right)) => {
                left.registry_rebound_layout_compatible_with(right)
            }
            (Self::Record(left), Self::Record(right)) => {
                left.fields().len() == right.fields().len()
                    && left.fields().iter().zip(right.fields()).all(|(a, b)| {
                        a.name == b.name
                            && a.value_type
                                .registry_rebound_layout_compatible_with(&b.value_type)
                    })
            }
            _ => self == other,
        }
    }

    /// Whether this durable value occurrence may advance to `next` without
    /// changing the interpretation of any value already stored under `self`.
    ///
    /// `registry_compatible_with` is deliberately symmetric: it is useful at
    /// read/projection boundaries where either descriptor may describe an
    /// existing value.  Live table evolution is stricter.  It is directional:
    /// only `next` may append cases, while names, payload layouts, nesting and
    /// registry identities remain fixed.
    pub(crate) fn can_evolve_registry_to(&self, next: &Self) -> bool {
        match (self, next) {
            (Self::EnumTag(current), Self::EnumTag(next))
                if current.registry_id == next.registry_id =>
            {
                next.variants.starts_with(&current.variants)
            }
            (Self::Enum(current), Self::Enum(next)) if current.registry_id == next.registry_id => {
                next.cases.len() >= current.cases.len()
                    && current
                        .cases
                        .iter()
                        .zip(&next.cases)
                        .all(|(current, next)| {
                            current.name == next.name
                                && current.payload.fields().len() == next.payload.fields().len()
                                && current
                                    .payload
                                    .fields()
                                    .iter()
                                    .zip(next.payload.fields())
                                    .all(|(current, next)| {
                                        current.name == next.name
                                            && current
                                                .value_type
                                                .can_evolve_registry_to(&next.value_type)
                                    })
                        })
            }
            (Self::Tuple(current), Self::Tuple(next)) => {
                current.len() == next.len()
                    && current
                        .iter()
                        .zip(next)
                        .all(|(current, next)| current.can_evolve_registry_to(next))
            }
            (Self::Array(current), Self::Array(next))
            | (Self::Nullable(current), Self::Nullable(next)) => {
                current.can_evolve_registry_to(next)
            }
            (Self::Record(current), Self::Record(next)) => {
                current.fields().len() == next.fields().len()
                    && current
                        .fields()
                        .iter()
                        .zip(next.fields())
                        .all(|(current, next)| {
                            current.name == next.name
                                && current.value_type.can_evolve_registry_to(&next.value_type)
                        })
            }
            _ => self == next,
        }
    }

    pub(crate) fn collect_variant_registries(&self, output: &mut BTreeMap<u64, VariantRegistry>) {
        match self {
            Self::EnumTag(schema) => {
                output.insert(
                    schema.registry_id,
                    VariantRegistry::EnumTag {
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
            Self::Enum(schema) => {
                output.insert(
                    schema.registry_id,
                    VariantRegistry::Enum {
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

    /// Stamp every nested enum occurrence with its durable physical path.
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
            Self::EnumTag(schema) => {
                if schema.registry_id & SYSTEM_REGISTRY_MARKER == 0
                    && (replace || schema.registry_id == 0)
                {
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
            Self::Enum(schema) => {
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
            Self::Record(_) | Self::Enum(_) => true,
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
            Self::EnumTag(_) => Some(1),
            Self::Tuple(members) => members
                .iter()
                .try_fold(0usize, |total, member| Some(total + member.fixed_size()?)),
            Self::Nullable(value_type) => value_type.fixed_size().map(|size| size + 1),
            Self::String
            | Self::Bytes
            | Self::Internal(_)
            | Self::Array(_)
            | Self::Record(_)
            | Self::Enum(_) => None,
        }
    }

    pub(super) fn is_fixed_size(&self) -> bool {
        self.fixed_size().is_some()
    }
}

pub(super) fn encode_value(value: &Value, value_type: &ValueType) -> Result<Vec<u8>, Error> {
    let mut bytes = Vec::new();
    match (value, value_type) {
        (Value::String(value), ValueType::String) => {
            bytes.extend(crate::large_values::encode_stored_scalar(
                crate::large_values::LargeValueKind::String,
                &crate::large_values::StoredScalar::Primitive(value.as_bytes().to_vec()),
            )?)
        }
        (Value::Bytes(value), ValueType::Bytes) => {
            bytes.extend(crate::large_values::encode_stored_scalar(
                crate::large_values::LargeValueKind::Bytes,
                &crate::large_values::StoredScalar::Primitive(value.clone()),
            )?)
        }
        (
            Value::String(value),
            ValueType::Internal(InternalValueType(InternalValueTypeRepr::RawString)),
        ) => bytes.extend_from_slice(value.as_bytes()),
        (
            Value::Bytes(value),
            ValueType::Internal(InternalValueType(InternalValueTypeRepr::RawBytes)),
        ) => bytes.extend_from_slice(value),
        (
            Value::Bytes(value),
            ValueType::Internal(InternalValueType(InternalValueTypeRepr::StoredScalar(
                crate::large_values::LargeValueKind::Bytes,
            ))),
        ) => bytes.extend(crate::large_values::encode_stored_scalar(
            crate::large_values::LargeValueKind::Bytes,
            &crate::large_values::StoredScalar::Primitive(value.clone()),
        )?),
        (
            Value::String(value),
            ValueType::Internal(InternalValueType(InternalValueTypeRepr::StoredScalar(
                kind @ (crate::large_values::LargeValueKind::String
                | crate::large_values::LargeValueKind::Json),
            ))),
        ) => bytes.extend(crate::large_values::encode_stored_scalar(
            *kind,
            &crate::large_values::StoredScalar::Primitive(value.as_bytes().to_vec()),
        )?),
        (
            Value::Large(value),
            ValueType::Internal(InternalValueType(InternalValueTypeRepr::StoredScalar(kind))),
        ) if value.kind == *kind => bytes.extend(crate::large_values::encode_stored_scalar(
            *kind,
            &crate::large_values::StoredScalar::Chunked(value.clone()),
        )?),
        (Value::Large(value), ValueType::String)
            if value.kind == crate::large_values::LargeValueKind::String =>
        {
            bytes.extend(crate::large_values::encode_stored_scalar(
                crate::large_values::LargeValueKind::String,
                &crate::large_values::StoredScalar::Chunked(value.clone()),
            )?)
        }
        (Value::Large(value), ValueType::Bytes)
            if value.kind == crate::large_values::LargeValueKind::Bytes =>
        {
            bytes.extend(crate::large_values::encode_stored_scalar(
                crate::large_values::LargeValueKind::Bytes,
                &crate::large_values::StoredScalar::Chunked(value.clone()),
            )?)
        }
        (Value::Uuid(value), ValueType::Uuid) => bytes.extend_from_slice(value.as_bytes()),
        (Value::String(value), ValueType::EnumTag(schema)) => {
            bytes.push(schema.discriminant(value)?)
        }
        (Value::EnumTag(value), ValueType::EnumTag(_)) => bytes.push(*value),
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
        (Value::Enum(enum_value), ValueType::Enum(schema)) => {
            ensure_enum_value(enum_value, schema)?;
            bytes.extend(super::encode_variant_record(
                enum_value.tag,
                enum_value.record.raw(),
            ));
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
        (Value::String(value), ValueType::EnumTag(schema)) => {
            bytes.push(schema.discriminant(value)?)
        }
        (Value::EnumTag(value), ValueType::EnumTag(_)) => bytes.push(*value),
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
        (_, ValueType::Enum(_)) => {
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
        ValueType::F64 => {
            let value = f64::from_le_bytes(read_exact::<8>(bytes)?);
            if value.is_nan() {
                Err(Error::InvalidF64NaN)
            } else {
                Ok(Value::F64(value))
            }
        }
        ValueType::Bool => match read_exact::<1>(bytes)?[0] {
            0 => Ok(Value::Bool(false)),
            1 => Ok(Value::Bool(true)),
            value => Err(Error::InvalidBool(value)),
        },
        ValueType::String => match crate::large_values::decode_stored_scalar(
            crate::large_values::LargeValueKind::String,
            bytes,
        )
        .map_err(|error| match error {
            crate::large_values::Error::InvalidUtf8 => Error::InvalidUtf8,
            other => Error::LargeValue(other),
        })? {
            crate::large_values::StoredScalar::Primitive(bytes) => String::from_utf8(bytes)
                .map(Value::String)
                .map_err(|_| Error::InvalidUtf8),
            crate::large_values::StoredScalar::Chunked(value)
                if value.kind == crate::large_values::LargeValueKind::String =>
            {
                Ok(Value::Large(value))
            }
            crate::large_values::StoredScalar::Chunked(_) => Err(Error::TypeMismatch {
                expected: value_type.clone(),
            }),
        },
        ValueType::Bytes => match crate::large_values::decode_stored_scalar(
            crate::large_values::LargeValueKind::Bytes,
            bytes,
        )? {
            crate::large_values::StoredScalar::Primitive(bytes) => Ok(Value::Bytes(bytes)),
            crate::large_values::StoredScalar::Chunked(value)
                if value.kind == crate::large_values::LargeValueKind::Bytes =>
            {
                Ok(Value::Large(value))
            }
            crate::large_values::StoredScalar::Chunked(_) => Err(Error::TypeMismatch {
                expected: value_type.clone(),
            }),
        },
        ValueType::Internal(InternalValueType(InternalValueTypeRepr::RawString)) => {
            String::from_utf8(bytes.to_vec())
                .map(Value::String)
                .map_err(|_| Error::InvalidUtf8)
        }
        ValueType::Internal(InternalValueType(InternalValueTypeRepr::RawBytes)) => {
            Ok(Value::Bytes(bytes.to_vec()))
        }
        ValueType::Internal(InternalValueType(InternalValueTypeRepr::StoredScalar(kind))) => {
            match crate::large_values::decode_stored_scalar(*kind, bytes)? {
                crate::large_values::StoredScalar::Primitive(bytes) => match kind {
                    crate::large_values::LargeValueKind::Bytes => Ok(Value::Bytes(bytes)),
                    crate::large_values::LargeValueKind::String
                    | crate::large_values::LargeValueKind::Json => String::from_utf8(bytes)
                        .map(Value::String)
                        .map_err(|_| Error::InvalidUtf8),
                },
                crate::large_values::StoredScalar::Chunked(value) if value.kind == *kind => {
                    Ok(Value::Large(value))
                }
                crate::large_values::StoredScalar::Chunked(_) => Err(Error::TypeMismatch {
                    expected: value_type.clone(),
                }),
            }
        }
        ValueType::Uuid => Ok(Value::Uuid(uuid::Uuid::from_bytes(read_exact::<16>(
            bytes,
        )?))),
        ValueType::EnumTag(schema) => {
            let discriminant = read_exact::<1>(bytes)?[0];
            schema
                .variant(discriminant)
                .map(|_| Value::EnumTag(discriminant))
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
        ValueType::Enum(schema) => {
            let (tag, payload) =
                super::split_variant_record(bytes).map_err(|error| match error {
                    Error::InvalidSchemaVersionHeader => Error::InvalidEnumHeader,
                    other => other,
                })?;
            let case = schema.case(tag)?;
            let values = case.payload.bind(payload).to_values()?;
            let canonical = case.payload.create(&values)?;
            if canonical != payload {
                return Err(Error::NonCanonicalRecord);
            }
            Ok(Value::Enum(EnumValue::new(
                tag,
                OwnedRecord::new(payload.to_vec(), case.payload),
            )))
        }
    }
}

pub(super) fn validate_value(bytes: &[u8], value_type: &ValueType) -> Result<(), Error> {
    validate_value_inner(bytes, value_type, false)
}

pub(super) fn validate_canonical_value(bytes: &[u8], value_type: &ValueType) -> Result<(), Error> {
    validate_value_inner(bytes, value_type, true)
}

fn validate_value_inner(
    bytes: &[u8],
    value_type: &ValueType,
    require_constructible: bool,
) -> Result<(), Error> {
    match value_type {
        ValueType::U8 => read_exact::<1>(bytes).map(|_| ()),
        ValueType::U16 => read_exact::<2>(bytes).map(|_| ()),
        ValueType::U32 | ValueType::I32 => read_exact::<4>(bytes).map(|_| ()),
        ValueType::U64 | ValueType::I64 => read_exact::<8>(bytes).map(|_| ()),
        ValueType::F64 => {
            let value = f64::from_le_bytes(read_exact::<8>(bytes)?);
            if require_constructible && value.is_nan() {
                Err(Error::InvalidF64NaN)
            } else {
                Ok(())
            }
        }
        ValueType::Bool => match read_exact::<1>(bytes)?[0] {
            0 | 1 => Ok(()),
            value => Err(Error::InvalidBool(value)),
        },
        ValueType::String | ValueType::Bytes => {
            use crate::large_values::{Error as LargeValueError, LargeValueKind};
            let kind = match value_type {
                ValueType::String => LargeValueKind::String,
                ValueType::Bytes => LargeValueKind::Bytes,
                _ => unreachable!("matched string or bytes value type"),
            };
            match crate::large_values::inline_scalar_bytes(kind, bytes) {
                Ok(_) | Err(LargeValueError::RequiresEvaluation) => Ok(()),
                Err(error) => Err(error.into()),
            }
        }
        ValueType::Internal(InternalValueType(InternalValueTypeRepr::RawString)) => {
            std::str::from_utf8(bytes)
                .map(|_| ())
                .map_err(|_| Error::InvalidUtf8)
        }
        ValueType::Internal(InternalValueType(InternalValueTypeRepr::RawBytes)) => Ok(()),
        ValueType::Internal(InternalValueType(InternalValueTypeRepr::StoredScalar(kind))) => {
            match crate::large_values::inline_scalar_bytes(*kind, bytes) {
                Ok(_) | Err(crate::large_values::Error::RequiresEvaluation) => Ok(()),
                Err(error) => Err(error.into()),
            }
        }
        ValueType::Uuid => read_exact::<16>(bytes).map(|_| ()),
        ValueType::EnumTag(schema) => {
            let discriminant = read_exact::<1>(bytes)?[0];
            schema.variant(discriminant).map(|_| ())
        }
        ValueType::Tuple(members) => validate_tuple(bytes, members, require_constructible),
        ValueType::Array(element_type) => {
            validate_array(bytes, element_type, require_constructible)
        }
        ValueType::Nullable(inner_type) => {
            validate_nullable(bytes, inner_type, require_constructible)
        }
        ValueType::Record(descriptor) => descriptor.bind(bytes).validate_canonical(),
        ValueType::Enum(schema) => {
            let (tag, payload) =
                super::split_variant_record(bytes).map_err(|error| match error {
                    Error::InvalidSchemaVersionHeader => Error::InvalidEnumHeader,
                    other => other,
                })?;
            schema.case(tag)?.payload.bind(payload).validate_canonical()
        }
    }
}

fn validate_nullable(
    bytes: &[u8],
    inner_type: &ValueType,
    require_constructible: bool,
) -> Result<(), Error> {
    let (&flag, payload) = bytes.split_first().ok_or(Error::UnexpectedEof)?;
    match flag {
        0 if inner_type.fixed_size().is_some() && payload.iter().any(|byte| *byte != 0) => {
            Err(Error::InvalidOffset)
        }
        0 if inner_type.fixed_size().is_none() && !payload.is_empty() => Err(Error::InvalidOffset),
        0 => Ok(()),
        1 => validate_value_inner(payload, inner_type, require_constructible),
        value => Err(Error::InvalidNullFlag(value)),
    }
}

fn validate_array(
    bytes: &[u8],
    element_type: &ValueType,
    require_constructible: bool,
) -> Result<(), Error> {
    if let Some(element_size) = element_type.fixed_size() {
        if element_size == 0 || !bytes.len().is_multiple_of(element_size) {
            return Err(Error::InvalidOffset);
        }
        return bytes.chunks_exact(element_size).try_for_each(|chunk| {
            validate_value_inner(chunk, element_type, require_constructible)
        });
    }

    let count = u32_to_usize(read_u32_at(bytes, 0)?)?;
    if count == 0 {
        return if bytes.len() == 4 {
            Ok(())
        } else {
            Err(Error::InvalidOffset)
        };
    }
    let values_start = checked_add(4, count.saturating_sub(1) * 4)?;
    if bytes.len() < values_start {
        return Err(Error::UnexpectedEof);
    }
    let mut start = values_start;
    for index in 0..count {
        let end = if index + 1 == count {
            bytes.len()
        } else {
            u32_to_usize(read_u32_at(bytes, 4 + index * 4)?)?
        };
        if end < start || end > bytes.len() {
            return Err(Error::InvalidOffset);
        }
        validate_value_inner(&bytes[start..end], element_type, require_constructible)?;
        start = end;
    }
    Ok(())
}

fn validate_tuple(
    bytes: &[u8],
    members: &[ValueType],
    require_constructible: bool,
) -> Result<(), Error> {
    let mut offset = 0;
    for member in members {
        let width = member
            .fixed_size()
            .ok_or_else(|| Error::InvalidTupleMember {
                member_type: member.clone(),
            })?;
        let end = checked_add(offset, width)?;
        validate_value_inner(
            bytes.get(offset..end).ok_or(Error::UnexpectedEof)?,
            member,
            require_constructible,
        )?;
        offset = end;
    }
    if offset == bytes.len() {
        Ok(())
    } else {
        Err(Error::InvalidOffset)
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
        | (
            Value::String(_),
            ValueType::Internal(InternalValueType(InternalValueTypeRepr::RawString)),
        )
        | (
            Value::Bytes(_),
            ValueType::Internal(InternalValueType(InternalValueTypeRepr::RawBytes)),
        )
        | (
            Value::Bytes(_),
            ValueType::Internal(InternalValueType(InternalValueTypeRepr::StoredScalar(
                crate::large_values::LargeValueKind::Bytes,
            ))),
        )
        | (
            Value::String(_),
            ValueType::Internal(InternalValueType(InternalValueTypeRepr::StoredScalar(
                crate::large_values::LargeValueKind::String
                | crate::large_values::LargeValueKind::Json,
            ))),
        )
        | (Value::Uuid(_), ValueType::Uuid) => Ok(()),
        (
            Value::Large(value),
            ValueType::Internal(InternalValueType(InternalValueTypeRepr::StoredScalar(kind))),
        ) if value.kind == *kind => Ok(()),
        (Value::Large(value), ValueType::String)
            if value.kind == crate::large_values::LargeValueKind::String =>
        {
            Ok(())
        }
        (Value::Large(value), ValueType::Bytes)
            if value.kind == crate::large_values::LargeValueKind::Bytes =>
        {
            Ok(())
        }
        (Value::F64(value), ValueType::F64) if !value.is_nan() => Ok(()),
        (Value::F64(_), ValueType::F64) => Err(Error::InvalidF64NaN),
        (Value::String(value), ValueType::EnumTag(schema)) => {
            schema.discriminant(value).map(|_| ())
        }
        (Value::EnumTag(value), ValueType::EnumTag(schema)) => schema.variant(*value).map(|_| ()),
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
        (Value::Enum(enum_value), ValueType::Enum(schema)) => ensure_enum_value(enum_value, schema),
        _ => Err(Error::TypeMismatch {
            expected: value_type.clone(),
        }),
    }
}

fn ensure_enum_value(value: &EnumValue, schema: &EnumSchema) -> Result<(), Error> {
    let case = schema.case(value.tag)?;
    if value.record.descriptor() != &case.payload {
        return Err(Error::TypeMismatch {
            expected: ValueType::Enum(Box::new(schema.clone())),
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
        ValueType::Enum(schema) => {
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
        (Value::String(value), ValueType::EnumTag(schema)) => {
            bytes.push(schema.discriminant(value)?)
        }
        (Value::EnumTag(value), ValueType::EnumTag(_)) => bytes.push(*value),
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
        ValueType::EnumTag(schema) => {
            let discriminant = read_exact::<1>(bytes)?[0];
            schema
                .variant(discriminant)
                .map(|_| Value::EnumTag(discriminant))
        }
        ValueType::Tuple(members) => decode_tuple(bytes, members),
        ValueType::Nullable(inner_type) => decode_nullable(bytes, inner_type),
        ValueType::F64
        | ValueType::String
        | ValueType::Bytes
        | ValueType::Internal(_)
        | ValueType::Array(_)
        | ValueType::Record(_)
        | ValueType::Enum(_) => Err(Error::InvalidTupleMember {
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
