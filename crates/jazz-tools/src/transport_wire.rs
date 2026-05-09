//! WebSocket wire helpers shared by the client transport and server route.
//!
//! The outer frame stays intentionally tiny: a 4-byte big-endian payload
//! length followed by one MessagePack-encoded transport value.

use serde::{Serialize, de::DeserializeOwned};

/// Encode a payload as a 4-byte big-endian length-prefixed frame.
pub(crate) fn frame_encode(payload: &[u8]) -> Vec<u8> {
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
pub(crate) fn frame_decode(data: &[u8]) -> Option<&[u8]> {
    if data.len() < 4 {
        return None;
    }
    let len = u32::from_be_bytes(data[0..4].try_into().unwrap()) as usize;
    if data.len() < 4 + len {
        return None;
    }
    Some(&data[4..4 + len])
}

pub(crate) fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, rmp_serde::encode::Error> {
    let mut buf = Vec::new();
    let mut serializer = rmp_serde::Serializer::new(&mut buf).with_struct_map();
    value.serialize(&mut serializer)?;
    Ok(buf)
}

pub(crate) fn decode<T: DeserializeOwned>(payload: &[u8]) -> Result<T, rmp_serde::decode::Error> {
    rmp_serde::from_slice(payload)
}

pub(crate) fn encode_frame<T: Serialize>(value: &T) -> Result<Vec<u8>, rmp_serde::encode::Error> {
    let payload = encode(value)?;
    Ok(frame_encode(&payload))
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
