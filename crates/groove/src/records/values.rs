//! Logical record values, value types, and primitive encoders.
//!
//! This module owns [`Value`], [`ValueType`], enum schemas, and the recursive
//! encode/decode routines for scalars, tuples, arrays, and nullable values. It
//! does not know field names or physical record ordering; [`super`] wraps these
//! value encodings in [`super::RecordDescriptor`] layout and exposes
//! borrowed/owned record access. Query expressions and schemas refer to these
//! value types but do not perform byte-level encoding themselves.

use super::Error;

/// One logical value inside a record, in decoded form.
///
/// `Value` is what callers build when writing a row and what they get back
/// when reading one. The matching static description is [`ValueType`]: every
/// `Value` is encoded and decoded against exactly one `ValueType`, and a pair
/// that does not match fails with [`Error::TypeMismatch`].
///
/// For example, the row `album(id=13, title="Yellow")` is built from:
///
/// ```text
/// Value::U64(13)           // matches ValueType::U64
/// Value::String("Yellow")  // matches ValueType::String
/// ```
#[derive(Clone, Debug, PartialEq, serde::Deserialize, serde::Serialize)]
pub enum Value {
    /// Unsigned 8-bit integer.
    U8(u8),
    /// Unsigned 16-bit integer.
    U16(u16),
    /// Unsigned 32-bit integer.
    U32(u32),
    /// Unsigned 64-bit integer.
    U64(u64),
    /// 64-bit float. NaN is rejected everywhere: records are compared and
    /// deduplicated by their encoded bytes, and NaN breaks value equality.
    F64(f64),
    /// `true` or `false`.
    Bool(bool),
    /// UTF-8 text.
    String(String),
    /// Raw bytes, kept as-is.
    Bytes(Vec<u8>),
    /// 128-bit UUID.
    Uuid(uuid::Uuid),
    /// One variant of a named enum, stored as its position in the
    /// [`EnumSchema`] variant list. `Enum(2)` means "the third declared
    /// variant". When encoding, a `Value::String` with the variant *name* is
    /// accepted too.
    Enum(u8),
    /// Fixed-width sequence of member values, for example a composite key
    /// `(country_id, artist_id)`. Members must be fixed-size; see
    /// [`ValueType::Tuple`].
    Tuple(Vec<Value>),
    /// Variable-length list of values that all share one element type.
    Array(Vec<Value>),
    /// A value that may be missing: `Nullable(None)` is NULL,
    /// `Nullable(Some(v))` wraps the present value `v`.
    Nullable(Option<Box<Value>>),
    /// Signed 64-bit integer.
    I64(i64),
}

// The `From` impls below let callers write `13_u64.into()` or `"Yellow".into()`
// instead of spelling out `Value::U64(13)` / `Value::String(...)` when
// building rows.

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
    /// The enum's name, used in schema declarations and error messages,
    /// for example `"color"`.
    pub name: String,
    /// Variant names in declaration order. A variant's position is its stored
    /// discriminant: `variants[0]` is stored as byte `0`, `variants[1]` as
    /// byte `1`, and so on — which is also why declaration order is sort
    /// order.
    pub variants: Vec<String>,
}

impl EnumSchema {
    /// Builds an enum schema from a name and its variant names.
    ///
    /// * `name` — the enum's name, for example `"color"`.
    /// * `variants` — the variant names in order. Order matters twice: it
    ///   fixes the stored discriminant of each variant and the sort order of
    ///   the enum.
    ///
    /// Fails with [`Error::EnumTooManyVariants`] when more than 256 variants
    /// are given, because the discriminant must fit in one byte.
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
        Ok(Self { name, variants })
    }

    /// Looks up the stored discriminant for a variant name.
    ///
    /// * `variant` — the variant name, for example `"green"`.
    ///
    /// With variants `["red", "green"]`, `discriminant("green")` returns `1`.
    /// Unknown names fail with [`Error::UnknownEnumVariant`].
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

    /// Looks up the variant name for a stored discriminant.
    ///
    /// * `discriminant` — the byte read out of an encoded record.
    ///
    /// With variants `["red", "green"]`, `variant(1)` returns `"green"`.
    /// Out-of-range discriminants fail with
    /// [`Error::InvalidEnumDiscriminant`].
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

/// The static type of one record field: which [`Value`] shape is allowed and
/// how it is laid out in bytes.
///
/// Every `ValueType` is either *fixed-size* — the encoded width is known from
/// the type alone, so `U32` is always 4 bytes — or *variable-size* (`String`,
/// `Bytes`, `Array`), where the width depends on the value.
/// [`super::RecordDescriptor`] builds record layouts from this split: fixed
/// fields sit at known offsets, variable fields go behind an offset table.
#[derive(Clone, Debug, PartialEq, Eq, Hash, serde::Deserialize, serde::Serialize)]
pub enum ValueType {
    /// Unsigned 8-bit integer; 1 byte.
    U8,
    /// Unsigned 16-bit integer; 2 bytes.
    U16,
    /// Unsigned 32-bit integer; 4 bytes.
    U32,
    /// Unsigned 64-bit integer; 8 bytes.
    U64,
    /// 64-bit float; 8 bytes. NaN values are rejected when encoding.
    F64,
    /// `true`/`false`; 1 byte (`01`/`00`).
    Bool,
    /// UTF-8 text; variable size, stored as the raw bytes with no length
    /// prefix (the record layout knows where each field ends).
    String,
    /// Raw bytes; variable size, stored as-is.
    Bytes,
    /// 128-bit UUID; 16 bytes.
    Uuid,
    /// Named enum; 1 byte holding the variant's discriminant. The
    /// [`EnumSchema`] maps discriminants back to names.
    Enum(EnumSchema),
    /// Fixed-width composite value encoded as concatenated member encodings.
    /// Variable-width members are deliberately rejected at schema construction.
    ///
    /// Tuples double as ordered keys, so members use the order-preserving
    /// layout described on `encode_tuple_member` (big-endian integers,
    /// sign-flipped `I64`). `F64` cannot appear inside a tuple: it has no
    /// order-preserving encoding here.
    Tuple(Vec<ValueType>),
    /// Variable-length list of one element type; see `encode_array` for the
    /// layout.
    Array(Box<ValueType>),
    /// A value that may be NULL, encoded as a 1-byte present/absent flag
    /// followed by the payload; see `encode_nullable`.
    Nullable(Box<ValueType>),
    /// Signed 64-bit integer; 8 bytes.
    I64,
}

impl ValueType {
    /// Returns the encoded width in bytes when the type is fixed-size, or
    /// `None` for variable-size types.
    ///
    /// For example `U32` returns `Some(4)`; `Nullable(U32)` returns `Some(5)`
    /// (1 flag byte + 4 payload bytes, reserved even when NULL); `String`
    /// returns `None`.
    pub(super) fn fixed_size(&self) -> Option<usize> {
        match self {
            Self::U8 | Self::Bool => Some(1),
            Self::U16 => Some(2),
            Self::U64 | Self::I64 => Some(8),
            Self::U32 => Some(4),
            Self::F64 => Some(8),
            Self::Uuid => Some(16),
            Self::Enum(_) => Some(1),
            Self::Tuple(members) => members
                .iter()
                .try_fold(0usize, |total, member| Some(total + member.fixed_size()?)),
            Self::Nullable(value_type) => value_type.fixed_size().map(|size| size + 1),
            Self::String | Self::Bytes | Self::Array(_) => None,
        }
    }

    /// `true` when [`Self::fixed_size`] knows the width.
    pub(super) fn is_fixed_size(&self) -> bool {
        self.fixed_size().is_some()
    }
}

/// Encodes one value into a fresh byte buffer.
///
/// * `value` — the logical value to encode.
/// * `value_type` — the type to encode it as. The pair must match, otherwise
///   the result is [`Error::TypeMismatch`].
///
/// Standalone scalars use little-endian layout. The exception is tuple
/// members, which use the big-endian order-preserving layout of
/// [`encode_tuple_member`] so that tuple bytes sort like tuple values.
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
        _ if value_type.is_fixed_size() => encode_fixed_value(&mut bytes, value, value_type)?,
        _ => {
            return Err(Error::TypeMismatch {
                expected: value_type.clone(),
            });
        }
    }
    Ok(bytes)
}

/// Encodes a fixed-size value, appending to an existing buffer.
///
/// * `bytes` — output buffer; the encoding is appended at the end.
/// * `value` — the logical value to encode.
/// * `value_type` — the type to encode it as; must be fixed-size and must
///   match `value`.
///
/// Integers here are little-endian (`U16(1)` becomes `01 00`). NaN floats are
/// rejected with [`Error::InvalidF64NaN`]: records are compared by their
/// encoded bytes, and NaN would make byte-equal rows compare unequal as
/// values.
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
        _ => {
            return Err(Error::TypeMismatch {
                expected: value_type.clone(),
            });
        }
    }
    Ok(())
}

/// Decodes one value from exactly its own bytes.
///
/// * `bytes` — the complete encoding of one value, nothing more and nothing
///   less. A length that does not match the type fails with
///   [`Error::UnexpectedEof`] (or [`Error::InvalidOffset`] for composite
///   layouts).
/// * `value_type` — the type to decode as; it drives the width checks and
///   picks the resulting [`Value`] variant.
pub(super) fn decode_value(bytes: &[u8], value_type: &ValueType) -> Result<Value, Error> {
    match value_type {
        ValueType::U8 => Ok(Value::U8(read_exact::<1>(bytes)?[0])),
        ValueType::U16 => Ok(Value::U16(u16::from_le_bytes(read_exact::<2>(bytes)?))),
        ValueType::U32 => Ok(Value::U32(u32::from_le_bytes(read_exact::<4>(bytes)?))),
        ValueType::U64 => Ok(Value::U64(u64::from_le_bytes(read_exact::<8>(bytes)?))),
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
    }
}

/// Encodes an optional value as a 1-byte flag plus payload.
///
/// * `bytes` — output buffer.
/// * `value` — `Some(v)` writes flag `1` then the encoding of `v`; `None`
///   writes flag `0`.
/// * `inner_type` — the type of the value when it is present.
///
/// ```text
/// Some(U32(7)) -> [01, 07 00 00 00]
/// None         -> [00, 00 00 00 00]  // fixed-size inner: payload zeroed
/// None (String inner) -> [00]        // variable-size inner: no padding
/// ```
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

/// Decodes the flag-plus-payload layout written by [`encode_nullable`].
///
/// For a NULL, the payload must be exactly the zero padding (fixed-size
/// inner) or empty (variable-size inner); anything else means the bytes are
/// corrupt and fails with [`Error::InvalidOffset`].
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

/// Encodes a list of same-typed values.
///
/// * `bytes` — output buffer.
/// * `values` — the elements; each one must match `element_type`.
/// * `element_type` — the element type shared by the whole array.
///
/// Fixed-size elements are simply concatenated — the element width is enough
/// to find each one again:
///
/// ```text
/// [U16(1), U16(2)] -> [01 00][02 00]
/// ```
///
/// Variable-size elements need an offset table. The layout is: a `u32`
/// element count, then the end offset of every element except the last (the
/// last element ends where the array ends), then the element bytes back to
/// back. Offsets are relative to the start of the array encoding:
///
/// ```text
/// ["hi", "world"] ->
/// [02 00 00 00]     count = 2
/// [0a 00 00 00]     "hi" ends at byte 10 (4 count + 4 offset + 2 payload)
/// [68 69]           "hi"
/// [77 6f 72 6c 64]  "world"
/// ```
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

/// Decodes an array written by [`encode_array`].
///
/// Fixed-size elements are read by slicing `bytes` into equal-width chunks.
/// Variable-size elements are read through the offset table; the last element
/// ends at the end of `bytes`. Malformed counts or offsets fail with
/// [`Error::InvalidOffset`] / [`Error::UnexpectedEof`].
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

/// Checks that `value` matches `value_type` without encoding anything.
///
/// * `value` — the logical value to check.
/// * `value_type` — the type it must conform to.
///
/// This applies the same rules encoding would (NaN floats rejected, enum
/// variants must exist, tuple members must be fixed-size, arrays are checked
/// element by element), so callers can validate a row up front and report
/// errors before any bytes are written.
pub(super) fn ensure_value_type(value: &Value, value_type: &ValueType) -> Result<(), Error> {
    match (value, value_type) {
        (Value::U8(_), ValueType::U8)
        | (Value::U16(_), ValueType::U16)
        | (Value::U32(_), ValueType::U32)
        | (Value::U64(_), ValueType::U64)
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
        _ => Err(Error::TypeMismatch {
            expected: value_type.clone(),
        }),
    }
}

/// Checks that a type is allowed to appear in a schema at all.
///
/// The one rule enforced here: every tuple member must be fixed-size.
/// A variable-size member (string, bytes, array) would break the
/// seek-by-width tuple layout, so it is rejected when the schema is built
/// instead of failing later at first encode.
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
        _ => Ok(()),
    }
}

/// Encodes tuple members one after another in the order-preserving layout.
///
/// * `bytes` — output buffer.
/// * `values` — the member values; the count must match `members`, otherwise
///   the result is [`Error::ArityMismatch`].
/// * `members` — one type per member, in order.
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

/// Encodes one tuple member so that byte order equals value order.
///
/// Tuples double as ordered keys (primary keys, index keys), so members are
/// encoded so that comparing the bytes lexicographically gives the same
/// result as comparing the values:
///
/// * integers are big-endian — most significant byte first, so `U16(1)` is
///   `00 01` here while a standalone `U16(1)` is `01 00`;
/// * `I64` additionally flips its sign bit so negative values sort before
///   positive ones (see [`order_preserving_i64`]);
/// * `Nullable` writes its `0`/`1` flag first, so NULL sorts before every
///   present value, and zero-pads an absent fixed-size payload;
/// * variable-size members and `F64` are rejected — without a fixed width one
///   member's bytes would bleed into the next, and floats have no
///   order-preserving encoding here.
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

/// Decodes tuple members by walking their fixed widths left to right.
///
/// Every member type must report a fixed width, and the widths must exactly
/// cover `bytes`; leftover bytes fail with [`Error::InvalidOffset`].
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

/// Decodes one tuple member from exactly its own bytes.
///
/// Mirrors [`encode_tuple_member`]: big-endian integers, sign-flipped `I64`.
/// `F64`, `String`, `Bytes`, and arrays can never appear inside a tuple, so
/// meeting one here is [`Error::InvalidTupleMember`].
fn decode_tuple_member(bytes: &[u8], value_type: &ValueType) -> Result<Value, Error> {
    match value_type {
        ValueType::U8 => Ok(Value::U8(read_exact::<1>(bytes)?[0])),
        ValueType::U16 => Ok(Value::U16(u16::from_be_bytes(read_exact::<2>(bytes)?))),
        ValueType::U32 => Ok(Value::U32(u32::from_be_bytes(read_exact::<4>(bytes)?))),
        ValueType::U64 => Ok(Value::U64(u64::from_be_bytes(read_exact::<8>(bytes)?))),
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
        ValueType::F64 | ValueType::String | ValueType::Bytes | ValueType::Array(_) => {
            Err(Error::InvalidTupleMember {
                member_type: value_type.clone(),
            })
        }
    }
}

/// Reads `count` consecutive little-endian `u32` offsets from `bytes`,
/// starting at byte `start`.
fn read_offsets(bytes: &[u8], start: usize, count: usize) -> Result<Vec<usize>, Error> {
    (0..count)
        .map(|idx| read_u32_at(bytes, start + idx * 4).and_then(u32_to_usize))
        .collect()
}

/// Reads one little-endian `u32` at byte `start`, bounds-checked.
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

/// Copies `bytes` into a fixed `[u8; N]`, failing with
/// [`Error::UnexpectedEof`] unless the length is exactly `N`.
fn read_exact<const N: usize>(bytes: &[u8]) -> Result<[u8; N], Error> {
    if bytes.len() != N {
        return Err(Error::UnexpectedEof);
    }
    bytes.try_into().map_err(|_| Error::UnexpectedEof)
}

/// Encodes an `i64` so its bytes sort in numeric order.
///
/// Plain big-endian two's complement would sort negative numbers *after*
/// positive ones (their sign bit is `1`). Flipping the sign bit fixes that:
///
/// ```text
/// -1 -> 7f ff ff ff ff ff ff ff
///  0 -> 80 00 00 00 00 00 00 00
///  1 -> 80 00 00 00 00 00 00 01
/// ```
fn order_preserving_i64(value: i64) -> [u8; 8] {
    ((value as u64) ^ (1_u64 << 63)).to_be_bytes()
}

/// Reverses [`order_preserving_i64`].
fn i64_from_order_preserving(bytes: [u8; 8]) -> i64 {
    (u64::from_be_bytes(bytes) ^ (1_u64 << 63)) as i64
}

/// Appends a little-endian `u32` to `bytes`.
pub(super) fn write_u32(bytes: &mut Vec<u8>, value: u32) {
    bytes.extend(value.to_le_bytes());
}

/// `usize` addition that fails with [`Error::LengthOverflow`] instead of
/// overflowing.
pub(super) fn checked_add(left: usize, right: usize) -> Result<usize, Error> {
    left.checked_add(right).ok_or(Error::LengthOverflow)
}

/// Narrows a `usize` length into the `u32` stored in offset tables, failing
/// with [`Error::LengthOverflow`] when it does not fit.
pub(super) fn usize_to_u32(value: usize) -> Result<u32, Error> {
    value.try_into().map_err(|_| Error::LengthOverflow)
}

/// Widens a stored `u32` offset back to `usize`.
fn u32_to_usize(value: u32) -> Result<usize, Error> {
    value.try_into().map_err(|_| Error::LengthOverflow)
}
