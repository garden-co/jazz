//! Memcomparable key primitives shared by every persisted and in-memory key
//! codec: arrangement keys, direct-store primary keys, persisted indexes and
//! tuple-encoded record fields.
//!
//! The tags and byte layouts here are stored on disk, so they must never
//! change. Each scalar is a one-byte type tag followed by big-endian bytes
//! that sort like the value: signed integers flip the sign bit, floats use
//! [`f64_order_bits`], and byte strings use [`encode_ordered_bytes`].

use super::{Error, Value, u48_be_bytes};

pub(crate) const TAG_U8: u8 = 0;
pub(crate) const TAG_U16: u8 = 1;
pub(crate) const TAG_U32: u8 = 2;
pub(crate) const TAG_U64: u8 = 3;
pub(crate) const TAG_F64: u8 = 4;
pub(crate) const TAG_BOOL: u8 = 5;
pub(crate) const TAG_STRING: u8 = 6;
pub(crate) const TAG_BYTES: u8 = 7;
pub(crate) const TAG_NULL: u8 = 8;
pub(crate) const TAG_SOME: u8 = 9;
pub(crate) const TAG_UUID: u8 = 10;
pub(crate) const TAG_TUPLE: u8 = 11;
pub(crate) const TAG_I64: u8 = 13;
pub(crate) const TAG_I32: u8 = 14;
pub(crate) const TAG_U48: u8 = 16;

pub(crate) fn i32_order_bits(value: i32) -> u32 {
    (value as u32) ^ (1_u32 << 31)
}

pub(crate) fn i32_from_order_bits(bits: u32) -> i32 {
    (bits ^ (1_u32 << 31)) as i32
}

pub(crate) fn i64_order_bits(value: i64) -> u64 {
    (value as u64) ^ (1_u64 << 63)
}

pub(crate) fn i64_from_order_bits(bits: u64) -> i64 {
    (bits ^ (1_u64 << 63)) as i64
}

/// Flip positive signs and invert negatives; the resulting unsigned integer
/// sorts like IEEE numeric order for non-NaN values.
pub(crate) fn f64_order_bits(value: f64) -> u64 {
    let bits = value.to_bits();
    if bits & (1 << 63) == 0 {
        bits ^ (1 << 63)
    } else {
        !bits
    }
}

pub(crate) fn f64_from_order_bits(ordered: u64) -> f64 {
    f64::from_bits(if ordered & (1 << 63) != 0 {
        ordered ^ (1 << 63)
    } else {
        !ordered
    })
}

/// 0x00 terminates the byte string; embedded NULs are escaped as 00 ff.
pub(crate) fn encode_ordered_bytes(key: &mut Vec<u8>, value: &[u8]) {
    for byte in value {
        if *byte == 0 {
            key.extend([0, 0xff]);
        } else {
            key.push(*byte);
        }
    }
    key.extend([0, 0]);
}

/// Inverse of [`encode_ordered_bytes`]; `None` for truncated or malformed
/// input, which callers report in their own error type.
pub(crate) fn decode_ordered_bytes(bytes: &mut &[u8]) -> Option<Vec<u8>> {
    let mut out = Vec::new();
    loop {
        let (&byte, rest) = bytes.split_first()?;
        *bytes = rest;
        if byte != 0 {
            out.push(byte);
            continue;
        }
        let (&escape, rest) = bytes.split_first()?;
        *bytes = rest;
        match escape {
            0 => return Some(out),
            0xff => out.push(0),
            _ => return None,
        }
    }
}

// One tagged writer per scalar key type. They are generic over the caller's
// error type so typed fast paths can use them without converting.

type KeyResult<E> = Result<(), E>;

#[inline]
pub(crate) fn put_u8<E>(key: &mut Vec<u8>, value: u8) -> KeyResult<E> {
    key.extend([TAG_U8, value]);
    Ok(())
}

#[inline]
pub(crate) fn put_u16<E>(key: &mut Vec<u8>, value: u16) -> KeyResult<E> {
    key.push(TAG_U16);
    key.extend(value.to_be_bytes());
    Ok(())
}

#[inline]
pub(crate) fn put_u32<E>(key: &mut Vec<u8>, value: u32) -> KeyResult<E> {
    key.push(TAG_U32);
    key.extend(value.to_be_bytes());
    Ok(())
}

#[inline]
pub(crate) fn put_u64<E>(key: &mut Vec<u8>, value: u64) -> KeyResult<E> {
    key.push(TAG_U64);
    key.extend(value.to_be_bytes());
    Ok(())
}

#[inline]
pub(crate) fn put_u48<E: From<Error>>(key: &mut Vec<u8>, value: u64) -> KeyResult<E> {
    key.push(TAG_U48);
    key.extend(u48_be_bytes(value)?);
    Ok(())
}

#[inline]
pub(crate) fn put_i32<E>(key: &mut Vec<u8>, value: i32) -> KeyResult<E> {
    key.push(TAG_I32);
    key.extend(i32_order_bits(value).to_be_bytes());
    Ok(())
}

#[inline]
pub(crate) fn put_i64<E>(key: &mut Vec<u8>, value: i64) -> KeyResult<E> {
    key.push(TAG_I64);
    key.extend(i64_order_bits(value).to_be_bytes());
    Ok(())
}

/// Callers that may see NaN must reject it first.
#[inline]
pub(crate) fn put_f64<E>(key: &mut Vec<u8>, value: f64) -> KeyResult<E> {
    key.push(TAG_F64);
    key.extend(f64_order_bits(value).to_be_bytes());
    Ok(())
}

#[inline]
pub(crate) fn put_bool<E>(key: &mut Vec<u8>, value: bool) -> KeyResult<E> {
    key.extend([TAG_BOOL, u8::from(value)]);
    Ok(())
}

#[inline]
pub(crate) fn put_str<E>(key: &mut Vec<u8>, value: &str) -> KeyResult<E> {
    put_bytes_tagged(key, TAG_STRING, value.as_bytes())
}

#[inline]
pub(crate) fn put_bytes<E>(key: &mut Vec<u8>, value: &[u8]) -> KeyResult<E> {
    put_bytes_tagged(key, TAG_BYTES, value)
}

#[inline]
pub(crate) fn put_bytes_tagged<E>(key: &mut Vec<u8>, tag: u8, value: &[u8]) -> KeyResult<E> {
    key.push(tag);
    encode_ordered_bytes(key, value);
    Ok(())
}

#[inline]
pub(crate) fn put_uuid<E>(key: &mut Vec<u8>, value: uuid::Uuid) -> KeyResult<E> {
    key.push(TAG_UUID);
    key.extend_from_slice(value.as_bytes());
    Ok(())
}

/// `Value::Nullable` framing: [`TAG_NULL`] for null, else [`TAG_SOME`]
/// followed by the value.
#[inline]
pub(crate) fn put_nullable<T, E>(
    key: &mut Vec<u8>,
    value: Option<T>,
    put: impl FnOnce(&mut Vec<u8>, T) -> KeyResult<E>,
) -> KeyResult<E> {
    match value {
        Some(value) => {
            key.push(TAG_SOME);
            put(key, value)
        }
        None => {
            key.push(TAG_NULL);
            Ok(())
        }
    }
}

/// Writes one scalar `Value`. Returns `Ok(false)` without writing for
/// composite values, whose framing differs between key codecs, so each
/// caller decides how to encode or reject them. F64 is written as-is;
/// callers that may see NaN reject it first.
#[inline]
pub(crate) fn put_scalar(key: &mut Vec<u8>, value: &Value) -> Result<bool, Error> {
    let written: Result<(), Error> = match value {
        Value::U8(value) | Value::EnumTag(value) => put_u8(key, *value),
        Value::U16(value) => put_u16(key, *value),
        Value::U32(value) => put_u32(key, *value),
        Value::U64(value) => put_u64(key, *value),
        Value::U48(value) => put_u48(key, *value),
        Value::I32(value) => put_i32(key, *value),
        Value::I64(value) => put_i64(key, *value),
        Value::F64(value) => put_f64(key, *value),
        Value::Bool(value) => put_bool(key, *value),
        Value::String(value) => put_str(key, value),
        Value::Bytes(value) => put_bytes(key, value),
        Value::Uuid(value) => put_uuid(key, *value),
        Value::Tuple(_)
        | Value::Nullable(_)
        | Value::Array(_)
        | Value::Record(_)
        | Value::Enum(_)
        | Value::Large(_) => return Ok(false),
    };
    written.map(|()| true)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn order_bits_round_trip_and_sort_like_the_values() {
        let ints = [i64::MIN, -2, -1, 0, 1, i64::MAX];
        for pair in ints.windows(2) {
            assert!(i64_order_bits(pair[0]) < i64_order_bits(pair[1]));
        }
        for value in ints {
            assert_eq!(i64_from_order_bits(i64_order_bits(value)), value);
            let narrow = value as i32;
            assert_eq!(i32_from_order_bits(i32_order_bits(narrow)), narrow);
        }
        let floats = [
            f64::NEG_INFINITY,
            -1.5,
            -0.0,
            0.0,
            1e-300,
            2.0,
            f64::INFINITY,
        ];
        for pair in floats.windows(2) {
            assert!(f64_order_bits(pair[0]) < f64_order_bits(pair[1]));
        }
        for value in floats {
            assert_eq!(
                f64_from_order_bits(f64_order_bits(value)).to_bits(),
                value.to_bits()
            );
        }
    }

    #[test]
    fn ordered_bytes_round_trip_and_reject_malformed_input() {
        for value in [&b""[..], b"a", b"a\0b", b"\0\0", b"\xff"] {
            let mut key = Vec::new();
            encode_ordered_bytes(&mut key, value);
            key.push(0x42);
            let mut cursor = key.as_slice();
            assert_eq!(decode_ordered_bytes(&mut cursor).as_deref(), Some(value));
            assert_eq!(cursor, [0x42]);
        }
        assert_eq!(decode_ordered_bytes(&mut &b"ab"[..]), None);
        assert_eq!(decode_ordered_bytes(&mut &b"a\0"[..]), None);
        assert_eq!(decode_ordered_bytes(&mut &b"a\0\x01"[..]), None);
    }
}
