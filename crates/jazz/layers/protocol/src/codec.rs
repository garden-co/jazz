//! Hand-written binary codecs.
//!
//! Every Jazz byte format is described once, as a codec per type, and both
//! directions derive from that description so encode and decode cannot drift.
//! [`Wire`] is a codec value: a small value that knows how to write and read
//! one type. [`wire_struct!`](crate::wire_struct) and
//! [`wire_enum!`](crate::wire_enum) describe a struct or a tagged enum once,
//! as its fields' codecs in wire order.
//!
//! Two grammars share this machinery:
//!
//! - catalogue payloads (`jazz-server`) spell counts and lengths as
//!   little-endian `u32` and bring their own codec values;
//! - the sync wire spells them as canonical unsigned LEB128 varints. Types on
//!   the sync wire implement [`WireCodec`], and the [`Auto`] codec value
//!   selects that implementation. This grammar is byte-for-byte the layout
//!   the sync wire has always had (signed integers zigzag-encoded, enum tags
//!   as varints, `Option` as a `0`/`1` byte); it is now owned here instead of
//!   being the default layout of a serializer.

use std::collections::{BTreeMap, BTreeSet};

use crate::protocol_limits::PolicyExpressionLimitError;

/// Encoding and decoding errors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CodecError {
    /// Data too short.
    TruncatedData {
        /// Bytes needed.
        expected: usize,
        /// Bytes available.
        actual: usize,
    },
    /// Unknown version byte.
    UnsupportedVersion {
        /// Version found.
        found: u8,
        /// Version expected.
        expected: u8,
    },
    /// Invalid type tag.
    InvalidTypeTag {
        /// The tag read.
        tag: u8,
        /// What was being decoded.
        context: &'static str,
    },
    /// Invalid UTF-8 string.
    InvalidUtf8 {
        /// What was being decoded.
        context: &'static str,
    },
    /// A value is outside its type's range or is not spelled canonically.
    Invalid {
        /// What was being decoded.
        context: &'static str,
    },
    /// A named recursive-policy protocol boundary was crossed.
    ProtocolLimit(PolicyExpressionLimitError),
    /// Generic decode error.
    DecodeError {
        /// Diagnostic.
        message: String,
    },
    /// A value could not be encoded.
    Encode {
        /// Diagnostic.
        message: String,
    },
}

impl std::fmt::Display for CodecError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CodecError::TruncatedData { expected, actual } => {
                write!(f, "truncated data: expected {expected} bytes, got {actual}")
            }
            CodecError::UnsupportedVersion { found, expected } => {
                write!(f, "unsupported version: found {found}, expected {expected}")
            }
            CodecError::InvalidTypeTag { tag, context } => {
                write!(f, "invalid type tag {tag} in {context}")
            }
            CodecError::InvalidUtf8 { context } => write!(f, "invalid UTF-8 in {context}"),
            CodecError::Invalid { context } => write!(f, "invalid {context}"),
            CodecError::ProtocolLimit(error) => std::fmt::Display::fmt(error, f),
            CodecError::DecodeError { message } => write!(f, "decode error: {message}"),
            CodecError::Encode { message } => write!(f, "encode error: {message}"),
        }
    }
}

impl std::error::Error for CodecError {}

impl From<PolicyExpressionLimitError> for CodecError {
    fn from(error: PolicyExpressionLimitError) -> Self {
        Self::ProtocolLimit(error)
    }
}

/// Read one byte.
pub fn read_u8(data: &[u8], offset: &mut usize) -> Result<u8, CodecError> {
    let Some(&value) = data.get(*offset) else {
        return Err(CodecError::TruncatedData {
            expected: *offset + 1,
            actual: data.len(),
        });
    };
    *offset += 1;
    Ok(value)
}

/// Read exactly `len` bytes.
pub fn read_bytes<'a>(
    data: &'a [u8],
    offset: &mut usize,
    len: usize,
) -> Result<&'a [u8], CodecError> {
    let end = offset.checked_add(len).filter(|end| *end <= data.len());
    let Some(end) = end else {
        return Err(CodecError::TruncatedData {
            expected: offset.saturating_add(len),
            actual: data.len(),
        });
    };
    let bytes = &data[*offset..end];
    *offset = end;
    Ok(bytes)
}

/// One description of a wire shape, used for both directions so encode and
/// decode cannot drift. Codecs are small values; a `&'static str` they hold
/// names the context that decode errors report.
pub trait Wire<T> {
    /// Append `value`.
    fn put(&self, buf: &mut Vec<u8>, value: &T);
    /// Read a value at `offset`, advancing it.
    fn get(&self, data: &[u8], offset: &mut usize) -> Result<T, CodecError>;
}

/// A tuple of codecs describes a tuple of values written in order.
macro_rules! tuple_wire {
    ($(($($codec:ident $value:ident $index:tt),+)),+) => {$(
        impl<$($value, $codec: Wire<$value>),+> Wire<($($value,)+)> for ($($codec,)+) {
            fn put(&self, buf: &mut Vec<u8>, value: &($($value,)+)) {
                $(self.$index.put(buf, &value.$index);)+
            }

            fn get(
                &self,
                data: &[u8],
                offset: &mut usize,
            ) -> Result<($($value,)+), CodecError> {
                Ok(($(self.$index.get(data, offset)?,)+))
            }
        }
    )+};
}

tuple_wire!((A TA 0, B TB 1), (A TA 0, B TB 1, C TC 2), (A TA 0, B TB 1, C TC 2, D TD 3));

/// Selects a field's codec: the one named, or [`Auto`] when none is.
#[doc(hidden)]
#[macro_export]
macro_rules! __wire_codec {
    () => {
        $crate::codec::Auto
    };
    ($codec:expr) => {
        $codec
    };
}

/// Describes a struct once, as its fields' codecs in wire order, and derives
/// both its writer and its reader from that description.
///
/// `wire_struct! { Name: Type { field: codec, .. } }` defines a codec value
/// `Name` implementing [`Wire<Type>`]; an optional `(param: Type)` after
/// `Name` works as for [`wire_enum!`](crate::wire_enum).
///
/// `wire_struct! { for Type { field, other: codec, .. } }` implements
/// [`WireCodec`] for `Type` itself; a field without a codec uses [`Auto`].
#[macro_export]
macro_rules! wire_struct {
    (for $ty:ty {
        $($field:ident $(: $codec:expr)?),+ $(,)?
    }) => {
        impl $crate::codec::WireCodec for $ty {
            fn put(&self, buf: &mut Vec<u8>) {
                $($crate::codec::Wire::put(&$crate::__wire_codec!($($codec)?), buf, &self.$field);)+
            }

            fn get(data: &[u8], offset: &mut usize) -> Result<Self, $crate::codec::CodecError> {
                Ok(Self { $($field: $crate::codec::Wire::get(&$crate::__wire_codec!($($codec)?), data, offset)?),+ })
            }
        }
    };
    ($name:ident $(($param:ident: $param_ty:ty))?: $ty:ident {
        $($field:ident: $codec:expr),+ $(,)?
    }) => {
        struct $name$(($param_ty))?;

        impl $crate::codec::Wire<$ty> for $name {
            fn put(&self, buf: &mut Vec<u8>, value: &$ty) {
                $(let $param: $param_ty = self.0;)?
                $($crate::codec::Wire::put(&$codec, buf, &value.$field);)+
            }

            fn get(&self, data: &[u8], offset: &mut usize) -> Result<$ty, $crate::codec::CodecError> {
                $(let $param: $param_ty = self.0;)?
                Ok($ty { $($field: $crate::codec::Wire::get(&$codec, data, offset)?),+ })
            }
        }
    };
}

/// Describes an enum once, as a tag → variant table listing each variant's
/// field codecs in wire order (`Unit`, `Tuple(binding: codec)` or
/// `Struct { field: codec, .. }`), and derives both its writer and its
/// reader. Tags are below 128, so a tag is one byte in either grammar.
///
/// `wire_enum! { Name: Type, "context" { .. } }` defines a codec value `Name`
/// with a complete table: it implements [`Wire<Type>`] and an unknown tag
/// decodes to `InvalidTypeTag` with that context. With `partial` instead of a
/// context it covers only the listed variants, and `get_variant` returns
/// `None` for the rest, which the caller codes by hand. The caller's encoder
/// matches every variant exhaustively and names the listed ones in the arm
/// that calls `put_variant`, so a new variant fails to build until it is
/// placed. An optional `(param: Type)` makes the codec a value carrying a
/// parameter that the field codecs can use.
///
/// `wire_enum! { for Type { .. } }` implements [`WireCodec`] for `Type`
/// itself with a complete table; a binding or field without a codec uses
/// [`Auto`].
#[macro_export]
macro_rules! wire_enum {
    (for $ty:ident {
        $($tag:literal => $variant:ident $(($binding:ident $(: $codec:expr)?))?
            $({ $($field:ident $(: $field_codec:expr)?),* $(,)? })?),+ $(,)?
    }) => {
        impl $crate::codec::WireCodec for $ty {
            fn put(&self, buf: &mut Vec<u8>) {
                match self {
                    $($ty::$variant $(($binding))? $({ $($field),* })? => {
                        buf.push($tag);
                        $($crate::codec::Wire::put(&$crate::__wire_codec!($($codec)?), buf, $binding);)?
                        $($($crate::codec::Wire::put(&$crate::__wire_codec!($($field_codec)?), buf, $field);)*)?
                    })+
                }
            }

            fn get(data: &[u8], offset: &mut usize) -> Result<Self, $crate::codec::CodecError> {
                let tag = $crate::codec::read_u8(data, offset)?;
                Ok(match tag {
                    $($tag => $ty::$variant $(($crate::codec::Wire::get(&$crate::__wire_codec!($($codec)?), data, offset)?))?
                        $({ $($field: $crate::codec::Wire::get(&$crate::__wire_codec!($($field_codec)?), data, offset)?),* })?,)+
                    _ => {
                        return Err($crate::codec::CodecError::InvalidTypeTag {
                            tag,
                            context: stringify!($ty),
                        })
                    }
                })
            }
        }
    };
    (partial $($rest:tt)*) => {
        $crate::wire_enum!(@impl [_ => unreachable!("encoder arm names a variant missing from the table"),] $($rest)*);
    };
    (@impl [$($fallback:tt)*] $name:ident $(($param:ident: $param_ty:ty))?: $ty:ident
        $(, $context:literal)? {
        $($tag:tt => $variant:ident $(($binding:ident: $codec:expr))?
            $({ $($field:ident: $field_codec:expr),* $(,)? })?),+ $(,)?
    }) => {
        struct $name$(($param_ty))?;

        impl $name {
            fn put_variant(&self, buf: &mut Vec<u8>, value: &$ty) {
                $(let $param: $param_ty = self.0;)?
                match value {
                    $($ty::$variant $(($binding))? $({ $($field),* })? => {
                        buf.push($tag);
                        $($crate::codec::Wire::put(&$codec, buf, $binding);)?
                        $($($crate::codec::Wire::put(&$field_codec, buf, $field);)*)?
                    })+
                    $($fallback)*
                }
            }

            #[allow(unused_variables)]
            fn get_variant(
                &self,
                tag: u8,
                data: &[u8],
                offset: &mut usize,
            ) -> Result<Option<$ty>, $crate::codec::CodecError> {
                $(let $param: $param_ty = self.0;)?
                Ok(Some(match tag {
                    $($tag => $ty::$variant $(($crate::codec::Wire::get(&$codec, data, offset)?))?
                        $({ $($field: $crate::codec::Wire::get(&$field_codec, data, offset)?),* })?,)+
                    _ => return Ok(None),
                }))
            }
        }

        $(impl $crate::codec::Wire<$ty> for $name {
            fn put(&self, buf: &mut Vec<u8>, value: &$ty) {
                self.put_variant(buf, value);
            }

            fn get(&self, data: &[u8], offset: &mut usize) -> Result<$ty, $crate::codec::CodecError> {
                let tag = $crate::codec::read_u8(data, offset)?;
                self.get_variant(tag, data, offset)?
                    .ok_or($crate::codec::CodecError::InvalidTypeTag { tag, context: $context })
            }
        })?
    };
    ($($rest:tt)*) => { $crate::wire_enum!(@impl [] $($rest)*); };
}

// ============================================================================
// Sync-wire grammar
// ============================================================================

/// A type with one canonical encoding in the sync-wire grammar.
///
/// Decoders reject what the encoder never writes where that is cheap to see
/// (overlong varints, a presence or boolean byte other than `0`/`1`, map keys
/// out of order); [`decode_exact`] additionally re-encodes the value and
/// compares, so any other alternate spelling is rejected as well.
pub trait WireCodec: Sized {
    /// Append the encoding of `self`.
    fn put(&self, buf: &mut Vec<u8>);
    /// Read a value at `offset`, advancing it.
    fn get(data: &[u8], offset: &mut usize) -> Result<Self, CodecError>;
}

/// The codec value for a type's own [`WireCodec`] encoding.
#[derive(Clone, Copy)]
pub struct Auto;

impl<T: WireCodec> Wire<T> for Auto {
    #[inline]
    fn put(&self, buf: &mut Vec<u8>, value: &T) {
        value.put(buf);
    }

    #[inline]
    fn get(&self, data: &[u8], offset: &mut usize) -> Result<T, CodecError> {
        T::get(data, offset)
    }
}

/// Encode a value.
///
/// Infallible codecs only: an encoder that records an error (see
/// [`record_encode_error`]) needs an entry point that takes it.
pub fn encode<T: WireCodec>(value: &T) -> Vec<u8> {
    let mut buf = Vec::new();
    value.put(&mut buf);
    buf
}

/// Decode one value from the start of `bytes`, returning it and the number
/// of bytes it used. Trailing bytes are left to the caller.
pub fn decode_prefix<T: WireCodec>(bytes: &[u8]) -> Result<(T, usize), CodecError> {
    let mut offset = 0;
    let value = T::get(bytes, &mut offset)?;
    Ok((value, offset))
}

/// Decode one value that must be exactly `bytes`: the value consumes the
/// whole input and its canonical encoding is `bytes`. A frame and its
/// semantic payload are each exactly one value, so neither a suffix nor an
/// alternate spelling may give one physical frame two interpretations.
pub fn decode_exact<T: WireCodec>(bytes: &[u8]) -> Result<T, CodecError> {
    let (value, used) = decode_prefix::<T>(bytes)?;
    if used != bytes.len() {
        return Err(CodecError::Invalid {
            context: "trailing bytes",
        });
    }
    let mut canonical = Vec::with_capacity(bytes.len());
    value.put(&mut canonical);
    if canonical != bytes {
        return Err(CodecError::Invalid {
            context: "non-canonical encoding",
        });
    }
    Ok(value)
}

/// Largest scratch buffer kept between encodes.
const MAX_RETAINED_SCRATCH_BYTES: usize = 1 << 20;

thread_local! {
    static SCRATCH: std::cell::Cell<Vec<u8>> = const { std::cell::Cell::new(Vec::new()) };
}

/// Run `f` with this thread's empty scratch buffer. Encoding into it and
/// copying out once gives an exact-size result without growing a fresh
/// allocation, and measuring an encoding allocates nothing in steady state.
/// Nested calls get a buffer of their own.
pub fn with_scratch<R>(f: impl FnOnce(&mut Vec<u8>) -> R) -> R {
    let mut buf = SCRATCH.with(std::cell::Cell::take);
    buf.clear();
    let result = f(&mut buf);
    if buf.capacity() <= MAX_RETAINED_SCRATCH_BYTES {
        SCRATCH.with(|scratch| scratch.set(buf));
    }
    result
}

thread_local! {
    static ENCODE_ERROR: std::cell::Cell<Option<CodecError>> = const { std::cell::Cell::new(None) };
}

/// Record that a value could not be encoded. [`WireCodec::put`] cannot fail,
/// so an encoder that can (a row whose descriptor does not encode) records
/// its error here and writes nothing; the entry point that started the
/// encode takes it with [`take_encode_error`]. The first error is kept.
pub fn record_encode_error(error: CodecError) {
    ENCODE_ERROR.with(|slot| {
        let previous = slot.take();
        slot.set(Some(previous.unwrap_or(error)));
    });
}

/// Take the error recorded by [`record_encode_error`] since the last take.
pub fn take_encode_error() -> Option<CodecError> {
    ENCODE_ERROR.with(std::cell::Cell::take)
}

/// Encoded length of a value, measured in the scratch buffer.
pub fn encoded_len<T: WireCodec>(value: &T) -> Result<usize, CodecError> {
    with_scratch(|buf| {
        value.put(buf);
        take_encode_error().map_or(Ok(buf.len()), Err)
    })
}

/// Append an unsigned LEB128 varint.
#[inline]
pub fn put_varint(buf: &mut Vec<u8>, mut value: u64) {
    while value >= 0x80 {
        buf.push(value as u8 | 0x80);
        value >>= 7;
    }
    buf.push(value as u8);
}

/// Read a canonical (shortest) unsigned LEB128 varint no larger than `max`.
pub fn get_varint(data: &[u8], offset: &mut usize, max: u64) -> Result<u64, CodecError> {
    let mut value = 0u64;
    let mut shift = 0u32;
    loop {
        let byte = read_u8(data, offset)?;
        let low = u64::from(byte & 0x7f);
        if shift == 63 && low > 1 {
            return Err(CodecError::Invalid { context: "varint" });
        }
        value |= low << shift;
        if byte & 0x80 == 0 {
            // A zero final byte after the first is an overlong spelling.
            if shift > 0 && byte == 0 {
                return Err(CodecError::Invalid { context: "varint" });
            }
            break;
        }
        shift += 7;
        if shift > 63 {
            return Err(CodecError::Invalid { context: "varint" });
        }
    }
    if value > max {
        return Err(CodecError::Invalid { context: "varint" });
    }
    Ok(value)
}

/// Append a length or count.
#[inline]
pub fn put_len(buf: &mut Vec<u8>, len: usize) {
    put_varint(buf, len as u64);
}

/// Read a length or count, bounded by the bytes remaining: every item of a
/// non-empty encoding takes at least one byte, so a larger count is corrupt
/// and must not reach an allocation.
pub fn get_len(data: &[u8], offset: &mut usize) -> Result<usize, CodecError> {
    let len = get_varint(data, offset, u32::MAX as u64)? as usize;
    Ok(len)
}

/// Capacity to reserve for `len` items: never more than the bytes left.
fn bounded_capacity(data: &[u8], offset: usize, len: usize) -> usize {
    len.min(data.len().saturating_sub(offset))
}

macro_rules! varint_codec {
    ($($ty:ty),+) => {$(
        impl WireCodec for $ty {
            #[inline]
            fn put(&self, buf: &mut Vec<u8>) {
                put_varint(buf, *self as u64);
            }

            #[inline]
            fn get(data: &[u8], offset: &mut usize) -> Result<Self, CodecError> {
                Ok(get_varint(data, offset, <$ty>::MAX as u64)? as $ty)
            }
        }
    )+};
}

varint_codec!(u16, u32, u64, usize);

macro_rules! zigzag_codec {
    ($($ty:ty: $unsigned:ty),+) => {$(
        impl WireCodec for $ty {
            #[inline]
            fn put(&self, buf: &mut Vec<u8>) {
                let zigzag = ((*self << 1) ^ (*self >> (<$ty>::BITS - 1))) as $unsigned;
                put_varint(buf, zigzag as u64);
            }

            #[inline]
            fn get(data: &[u8], offset: &mut usize) -> Result<Self, CodecError> {
                let zigzag = get_varint(data, offset, <$unsigned>::MAX as u64)? as $unsigned;
                Ok(((zigzag >> 1) as $ty) ^ -((zigzag & 1) as $ty))
            }
        }
    )+};
}

zigzag_codec!(i16: u16, i32: u32, i64: u64);

impl WireCodec for u8 {
    #[inline]
    fn put(&self, buf: &mut Vec<u8>) {
        buf.push(*self);
    }

    #[inline]
    fn get(data: &[u8], offset: &mut usize) -> Result<Self, CodecError> {
        read_u8(data, offset)
    }
}

impl WireCodec for i8 {
    fn put(&self, buf: &mut Vec<u8>) {
        buf.push(*self as u8);
    }

    fn get(data: &[u8], offset: &mut usize) -> Result<Self, CodecError> {
        Ok(read_u8(data, offset)? as i8)
    }
}

impl WireCodec for bool {
    #[inline]
    fn put(&self, buf: &mut Vec<u8>) {
        buf.push(u8::from(*self));
    }

    fn get(data: &[u8], offset: &mut usize) -> Result<Self, CodecError> {
        match read_u8(data, offset)? {
            0 => Ok(false),
            1 => Ok(true),
            tag => Err(CodecError::InvalidTypeTag {
                tag,
                context: "bool",
            }),
        }
    }
}

impl WireCodec for f32 {
    fn put(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&self.to_le_bytes());
    }

    fn get(data: &[u8], offset: &mut usize) -> Result<Self, CodecError> {
        Ok(f32::from_le_bytes(
            read_bytes(data, offset, 4)?.try_into().expect("4 bytes"),
        ))
    }
}

impl WireCodec for f64 {
    fn put(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(&self.to_le_bytes());
    }

    fn get(data: &[u8], offset: &mut usize) -> Result<Self, CodecError> {
        Ok(f64::from_le_bytes(
            read_bytes(data, offset, 8)?.try_into().expect("8 bytes"),
        ))
    }
}

impl WireCodec for () {
    fn put(&self, _: &mut Vec<u8>) {}

    fn get(_: &[u8], _: &mut usize) -> Result<Self, CodecError> {
        Ok(())
    }
}

/// Append a length-prefixed byte string.
#[inline]
pub fn put_bytes(buf: &mut Vec<u8>, bytes: &[u8]) {
    put_len(buf, bytes.len());
    buf.extend_from_slice(bytes);
}

/// Read a length-prefixed byte string.
pub fn get_bytes<'a>(data: &'a [u8], offset: &mut usize) -> Result<&'a [u8], CodecError> {
    let len = get_len(data, offset)?;
    read_bytes(data, offset, len)
}

/// Read a length-prefixed UTF-8 string.
pub fn get_str<'a>(data: &'a [u8], offset: &mut usize) -> Result<&'a str, CodecError> {
    std::str::from_utf8(get_bytes(data, offset)?)
        .map_err(|_| CodecError::InvalidUtf8 { context: "string" })
}

impl WireCodec for String {
    #[inline]
    fn put(&self, buf: &mut Vec<u8>) {
        put_bytes(buf, self.as_bytes());
    }

    fn get(data: &[u8], offset: &mut usize) -> Result<Self, CodecError> {
        get_str(data, offset).map(str::to_owned)
    }
}

/// A byte string: a length followed by the raw bytes. Identical on the wire
/// to a `Vec<u8>` written through [`Auto`], but copied in bulk.
#[derive(Clone, Copy)]
pub struct Bytes;

impl Wire<Vec<u8>> for Bytes {
    #[inline]
    fn put(&self, buf: &mut Vec<u8>, value: &Vec<u8>) {
        put_bytes(buf, value);
    }

    fn get(&self, data: &[u8], offset: &mut usize) -> Result<Vec<u8>, CodecError> {
        get_bytes(data, offset).map(<[u8]>::to_vec)
    }
}

impl<const N: usize> WireCodec for [u8; N] {
    #[inline]
    fn put(&self, buf: &mut Vec<u8>) {
        buf.extend_from_slice(self);
    }

    fn get(data: &[u8], offset: &mut usize) -> Result<Self, CodecError> {
        Ok(read_bytes(data, offset, N)?.try_into().expect("N bytes"))
    }
}

impl WireCodec for uuid::Uuid {
    #[inline]
    fn put(&self, buf: &mut Vec<u8>) {
        put_bytes(buf, self.as_bytes());
    }

    fn get(data: &[u8], offset: &mut usize) -> Result<Self, CodecError> {
        let bytes = get_bytes(data, offset)?;
        uuid::Uuid::from_slice(bytes).map_err(|_| CodecError::Invalid { context: "uuid" })
    }
}

impl<T: WireCodec> WireCodec for Option<T> {
    #[inline]
    fn put(&self, buf: &mut Vec<u8>) {
        match self {
            Some(value) => {
                buf.push(1);
                value.put(buf);
            }
            None => buf.push(0),
        }
    }

    fn get(data: &[u8], offset: &mut usize) -> Result<Self, CodecError> {
        match read_u8(data, offset)? {
            0 => Ok(None),
            1 => T::get(data, offset).map(Some),
            tag => Err(CodecError::InvalidTypeTag {
                tag,
                context: "option",
            }),
        }
    }
}

impl<T: WireCodec> WireCodec for Box<T> {
    #[inline]
    fn put(&self, buf: &mut Vec<u8>) {
        (**self).put(buf);
    }

    fn get(data: &[u8], offset: &mut usize) -> Result<Self, CodecError> {
        T::get(data, offset).map(Box::new)
    }
}

/// Append a count followed by each item.
pub fn put_seq<T>(buf: &mut Vec<u8>, items: &[T], mut put: impl FnMut(&mut Vec<u8>, &T)) {
    put_len(buf, items.len());
    for item in items {
        put(buf, item);
    }
}

/// Read a count followed by that many items.
pub fn get_seq<T>(
    data: &[u8],
    offset: &mut usize,
    mut get: impl FnMut(&[u8], &mut usize) -> Result<T, CodecError>,
) -> Result<Vec<T>, CodecError> {
    let len = get_len(data, offset)?;
    let mut items = Vec::with_capacity(bounded_capacity(data, *offset, len));
    for _ in 0..len {
        items.push(get(data, offset)?);
    }
    Ok(items)
}

impl<T: WireCodec> WireCodec for Vec<T> {
    fn put(&self, buf: &mut Vec<u8>) {
        put_seq(buf, self, |buf, item| item.put(buf));
    }

    fn get(data: &[u8], offset: &mut usize) -> Result<Self, CodecError> {
        get_seq(data, offset, T::get)
    }
}

/// A sequence whose items use codec `C`.
#[derive(Clone, Copy)]
pub struct Seq<C>(pub C);

impl<T, C: Wire<T>> Wire<Vec<T>> for Seq<C> {
    fn put(&self, buf: &mut Vec<u8>, value: &Vec<T>) {
        put_seq(buf, value, |buf, item| self.0.put(buf, item));
    }

    fn get(&self, data: &[u8], offset: &mut usize) -> Result<Vec<T>, CodecError> {
        get_seq(data, offset, |data, offset| self.0.get(data, offset))
    }
}

/// An optional value whose payload uses codec `C`.
#[derive(Clone, Copy)]
pub struct Opt<C>(pub C);

impl<T, C: Wire<T>> Wire<Option<T>> for Opt<C> {
    fn put(&self, buf: &mut Vec<u8>, value: &Option<T>) {
        match value {
            Some(value) => {
                buf.push(1);
                self.0.put(buf, value);
            }
            None => buf.push(0),
        }
    }

    fn get(&self, data: &[u8], offset: &mut usize) -> Result<Option<T>, CodecError> {
        match read_u8(data, offset)? {
            0 => Ok(None),
            1 => self.0.get(data, offset).map(Some),
            tag => Err(CodecError::InvalidTypeTag {
                tag,
                context: "option",
            }),
        }
    }
}

impl<K: WireCodec + Ord, V: WireCodec> WireCodec for BTreeMap<K, V> {
    fn put(&self, buf: &mut Vec<u8>) {
        put_len(buf, self.len());
        for (key, value) in self {
            key.put(buf);
            value.put(buf);
        }
    }

    fn get(data: &[u8], offset: &mut usize) -> Result<Self, CodecError> {
        let len = get_len(data, offset)?;
        let mut map = BTreeMap::new();
        for _ in 0..len {
            let key = K::get(data, offset)?;
            let value = V::get(data, offset)?;
            // The encoder writes keys in ascending order, each once.
            if map.last_key_value().is_some_and(|(last, _)| *last >= key) {
                return Err(CodecError::Invalid {
                    context: "map key order",
                });
            }
            map.insert(key, value);
        }
        Ok(map)
    }
}

impl<T: WireCodec + Ord> WireCodec for BTreeSet<T> {
    fn put(&self, buf: &mut Vec<u8>) {
        put_len(buf, self.len());
        for item in self {
            item.put(buf);
        }
    }

    fn get(data: &[u8], offset: &mut usize) -> Result<Self, CodecError> {
        let len = get_len(data, offset)?;
        let mut set = BTreeSet::new();
        for _ in 0..len {
            let item = T::get(data, offset)?;
            if set.last().is_some_and(|last| *last >= item) {
                return Err(CodecError::Invalid {
                    context: "set order",
                });
            }
            set.insert(item);
        }
        Ok(set)
    }
}

macro_rules! tuple_codec {
    ($(($($name:ident $index:tt),+)),+) => {$(
        impl<$($name: WireCodec),+> WireCodec for ($($name,)+) {
            fn put(&self, buf: &mut Vec<u8>) {
                $(self.$index.put(buf);)+
            }

            fn get(data: &[u8], offset: &mut usize) -> Result<Self, CodecError> {
                Ok(($($name::get(data, offset)?,)+))
            }
        }
    )+};
}

tuple_codec!((A 0, B 1), (A 0, B 1, C 2), (A 0, B 1, C 2, D 3));

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip<T: WireCodec + PartialEq + std::fmt::Debug>(value: T, bytes: &[u8]) {
        assert_eq!(encode(&value), bytes, "{value:?}");
        assert_eq!(decode_exact::<T>(bytes).unwrap(), value);
    }

    #[test]
    fn varints_are_shortest_leb128() {
        round_trip(0u32, &[0]);
        round_trip(127u32, &[0x7f]);
        round_trip(128u32, &[0x80, 0x01]);
        round_trip(u32::MAX, &[0xff, 0xff, 0xff, 0xff, 0x0f]);
        round_trip(
            u64::MAX,
            &[0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01],
        );
        round_trip(300u16, &[0xac, 0x02]);
        // Overlong spellings and out-of-range values are rejected.
        assert!(decode_exact::<u32>(&[0x80, 0x00]).is_err());
        assert!(decode_exact::<u32>(&[0xff, 0xff, 0xff, 0xff, 0x1f]).is_err());
        assert!(decode_exact::<u16>(&[0xff, 0xff, 0x04]).is_err());
        assert!(
            decode_exact::<u64>(&[0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x02])
                .is_err()
        );
    }

    #[test]
    fn signed_integers_are_zigzag_varints() {
        round_trip(0i64, &[0]);
        round_trip(-1i64, &[1]);
        round_trip(1i64, &[2]);
        round_trip(-64i32, &[0x7f]);
        round_trip(64i32, &[0x80, 0x01]);
        round_trip(
            i64::MIN,
            &[0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01],
        );
        round_trip(i16::MAX, &[0xfe, 0xff, 0x03]);
    }

    #[test]
    fn containers_match_the_sync_wire_layout() {
        round_trip(Some(5u8), &[1, 5]);
        round_trip(None::<u8>, &[0]);
        round_trip("hé".to_owned(), &[3, b'h', 0xc3, 0xa9]);
        round_trip(vec![1u32, 300], &[2, 1, 0xac, 0x02]);
        round_trip([7u8; 3], &[7, 7, 7]);
        round_trip(
            uuid::Uuid::from_bytes([9; 16]),
            &[[16].as_slice(), &[9; 16]].concat(),
        );
        round_trip(BTreeMap::from([(1u8, true), (2, false)]), &[2, 1, 1, 2, 0]);
        round_trip((1u8, -1i32), &[1, 1]);
        assert!(decode_exact::<Option<u8>>(&[2, 0]).is_err());
        assert!(decode_exact::<bool>(&[2]).is_err());
        assert!(decode_exact::<BTreeMap<u8, bool>>(&[2, 2, 1, 1, 0]).is_err());
        assert!(decode_exact::<BTreeMap<u8, bool>>(&[2, 1, 1, 1, 0]).is_err());
        assert!(decode_exact::<u8>(&[1, 2]).is_err());
        assert!(decode_exact::<String>(&[1, 0xff]).is_err());
        // A declared count larger than the input fails on the input, not on
        // an allocation.
        assert!(decode_exact::<Vec<u8>>(&[0xff, 0xff, 0xff, 0xff, 0x0f]).is_err());
    }

    #[test]
    fn bytes_codec_matches_a_byte_vector() {
        let value = vec![1u8, 2, 3];
        let mut buf = Vec::new();
        Bytes.put(&mut buf, &value);
        assert_eq!(buf, encode(&value));
        assert_eq!(Bytes.get(&buf, &mut 0).unwrap(), value);
    }

    #[derive(Debug, PartialEq)]
    struct Pair {
        left: u32,
        right: Vec<u8>,
    }

    wire_struct! {
        for Pair {
            left,
            right: Bytes,
        }
    }

    #[derive(Debug, PartialEq)]
    enum Shape {
        Empty,
        Tuple(Pair),
        Named { size: u64, label: Option<String> },
    }

    wire_enum! {
        for Shape {
            0 => Empty,
            1 => Tuple(pair),
            2 => Named { size, label },
        }
    }

    #[test]
    fn described_structs_and_enums_round_trip() {
        round_trip(Shape::Empty, &[0]);
        round_trip(
            Shape::Tuple(Pair {
                left: 128,
                right: vec![9],
            }),
            &[1, 0x80, 0x01, 1, 9],
        );
        round_trip(
            Shape::Named {
                size: 1,
                label: Some("a".to_owned()),
            },
            &[2, 1, 1, 1, b'a'],
        );
        assert_eq!(
            decode_exact::<Shape>(&[3]),
            Err(CodecError::InvalidTypeTag {
                tag: 3,
                context: "Shape"
            })
        );
    }
}
