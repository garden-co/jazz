//! WebSocket wire helpers shared by the client transport and server route.
//!
//! The outer frame stays intentionally tiny: a 4-byte big-endian payload
//! length followed by one tagged transport value. The transport value is
//! MessagePack, optionally compressed with LZ4 when that reduces the payload.

use std::{borrow::Cow, error::Error, fmt};

use serde::{Serialize, de::DeserializeOwned};

const WIRE_MESSAGEPACK: u8 = 0;
const WIRE_LZ4_MESSAGEPACK: u8 = 1;

#[derive(Debug)]
pub enum DecodeError {
    EmptyPayload,
    UnknownWireKind(u8),
    Lz4(String),
    MessagePack(rmp_serde::decode::Error),
}

impl fmt::Display for DecodeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyPayload => f.write_str("empty transport payload"),
            Self::UnknownWireKind(kind) => write!(f, "unknown transport payload kind {kind}"),
            Self::Lz4(err) => write!(f, "invalid LZ4 transport payload: {err}"),
            Self::MessagePack(err) => write!(f, "invalid MessagePack transport payload: {err}"),
        }
    }
}

impl Error for DecodeError {
    fn source(&self) -> Option<&(dyn Error + 'static)> {
        match self {
            Self::MessagePack(err) => Some(err),
            Self::EmptyPayload | Self::UnknownWireKind(_) | Self::Lz4(_) => None,
        }
    }
}

/// Encode a payload as a 4-byte big-endian length-prefixed frame.
pub fn frame_encode(payload: &[u8]) -> Vec<u8> {
    debug_assert!(
        payload.len() <= u32::MAX as usize,
        "frame payload exceeds u32 limit"
    );
    let mut out = Vec::with_capacity(4 + payload.len());
    out.extend_from_slice(&(payload.len() as u32).to_be_bytes());
    out.extend_from_slice(payload);
    out
}

/// Decode a 4-byte big-endian length-prefixed frame, returning the payload slice.
pub fn frame_decode(data: &[u8]) -> Option<&[u8]> {
    if data.len() < 4 {
        return None;
    }
    let len = u32::from_be_bytes(data[0..4].try_into().unwrap()) as usize;
    if data.len() < 4 + len {
        return None;
    }
    Some(&data[4..4 + len])
}

pub fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, rmp_serde::encode::Error> {
    let mut buf = Vec::new();
    let mut serializer = rmp_serde::Serializer::new(&mut buf).with_struct_map();
    value.serialize(&mut serializer)?;

    let compressed = lz4_flex::compress_prepend_size(&buf);
    if compressed.len() < buf.len() {
        let mut out = Vec::with_capacity(1 + compressed.len());
        out.push(WIRE_LZ4_MESSAGEPACK);
        out.extend_from_slice(&compressed);
        Ok(out)
    } else {
        let mut out = Vec::with_capacity(1 + buf.len());
        out.push(WIRE_MESSAGEPACK);
        out.extend_from_slice(&buf);
        Ok(out)
    }
}

pub fn decode<T: DeserializeOwned>(payload: &[u8]) -> Result<T, DecodeError> {
    let (kind, inner) = payload.split_first().ok_or(DecodeError::EmptyPayload)?;
    let messagepack = match *kind {
        WIRE_MESSAGEPACK => Cow::Borrowed(inner),
        WIRE_LZ4_MESSAGEPACK => Cow::Owned(
            lz4_flex::decompress_size_prepended(inner)
                .map_err(|err| DecodeError::Lz4(err.to_string()))?,
        ),
        other => return Err(DecodeError::UnknownWireKind(other)),
    };

    rmp_serde::from_slice(&messagepack).map_err(DecodeError::MessagePack)
}

pub fn encode_frame<T: Serialize>(value: &T) -> Result<Vec<u8>, rmp_serde::encode::Error> {
    let payload = encode(value)?;
    Ok(frame_encode(&payload))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn raw_messagepack<T: Serialize>(value: &T) -> Vec<u8> {
        let mut buf = Vec::new();
        let mut serializer = rmp_serde::Serializer::new(&mut buf).with_struct_map();
        value
            .serialize(&mut serializer)
            .expect("encode raw MessagePack");
        buf
    }

    #[test]
    fn encode_prefers_messagepack_binary_for_row_bytes() {
        #[derive(Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
        struct Payload {
            bytes: crate::query_manager::types::RowBytes,
        }

        let payload = Payload {
            bytes: vec![1_u8, 2, 3].into(),
        };
        let encoded = encode(&payload).expect("encode bytes");

        assert!(
            encoded
                .windows(5)
                .any(|window| window == [0xc4, 3, 1, 2, 3]),
            "row bytes should use MessagePack bin8, not an integer array"
        );

        let decoded: Payload = decode(&encoded).expect("decode bytes");
        assert_eq!(decoded, payload);
    }

    #[test]
    fn encode_compresses_large_payloads_when_lz4_is_smaller() {
        #[derive(Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
        struct Payload {
            bytes: crate::query_manager::types::RowBytes,
        }

        let payload = Payload {
            bytes: vec![7_u8; 4096].into(),
        };
        let raw = raw_messagepack(&payload);
        let encoded = encode(&payload).expect("encode payload");

        assert!(
            encoded.len() < raw.len(),
            "large repetitive payload should be smaller on the wire: raw={} wire={}",
            raw.len(),
            encoded.len()
        );

        let decoded: Payload = decode(&encoded).expect("decode payload");
        assert_eq!(decoded, payload);
    }

    #[test]
    fn encode_keeps_small_payloads_uncompressed() {
        #[derive(Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
        struct Payload {
            bytes: crate::query_manager::types::RowBytes,
        }

        let payload = Payload {
            bytes: vec![1_u8, 2, 3].into(),
        };
        let raw = raw_messagepack(&payload);
        let encoded = encode(&payload).expect("encode payload");

        assert_eq!(
            encoded.len(),
            raw.len() + 1,
            "small payloads should only pay the compression tag byte"
        );

        let decoded: Payload = decode(&encoded).expect("decode payload");
        assert_eq!(decoded, payload);
    }
}
