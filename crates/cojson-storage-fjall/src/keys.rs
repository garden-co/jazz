/// Key encoding utilities for fjall keyspaces.
///
/// All numeric keys use big-endian encoding for correct lexicographic ordering.

/// Encode a u64 as 8 big-endian bytes.
#[inline]
pub fn encode_u64(n: u64) -> [u8; 8] {
    n.to_be_bytes()
}

/// Decode a u64 from 8 big-endian bytes.
#[inline]
pub fn decode_u64(bytes: &[u8]) -> u64 {
    let mut buf = [0u8; 8];
    buf.copy_from_slice(&bytes[..8]);
    u64::from_be_bytes(buf)
}

/// Encode a u32 as 4 big-endian bytes.
#[inline]
pub fn encode_u32(n: u32) -> [u8; 4] {
    n.to_be_bytes()
}

/// Decode a u32 from 4 big-endian bytes.
#[inline]
pub fn decode_u32(bytes: &[u8]) -> u32 {
    let mut buf = [0u8; 4];
    buf.copy_from_slice(&bytes[..4]);
    u32::from_be_bytes(buf)
}

/// Encode a composite key: u64 prefix + arbitrary suffix.
/// Used for session keys (coValueRowID + sessionID) and
/// transaction keys (sessionRowID + idx).
pub fn encode_u64_suffix(prefix: u64, suffix: &[u8]) -> Vec<u8> {
    let mut key = Vec::with_capacity(8 + suffix.len());
    key.extend_from_slice(&encode_u64(prefix));
    key.extend_from_slice(suffix);
    key
}

/// Encode a transaction/signature key: u64 session row + u32 index.
pub fn encode_tx_key(session_row_id: u64, idx: u32) -> [u8; 12] {
    let mut key = [0u8; 12];
    key[..8].copy_from_slice(&encode_u64(session_row_id));
    key[8..12].copy_from_slice(&encode_u32(idx));
    key
}

/// Encode an unsynced key: coValueID + \x00 + peerID.
pub fn encode_unsynced_key(co_value_id: &str, peer_id: &str) -> Vec<u8> {
    let mut key = Vec::with_capacity(co_value_id.len() + 1 + peer_id.len());
    key.extend_from_slice(co_value_id.as_bytes());
    key.push(0x00);
    key.extend_from_slice(peer_id.as_bytes());
    key
}

/// Extract coValueID from an unsynced key (everything before the first \x00).
pub fn decode_unsynced_co_value_id(key: &[u8]) -> Option<&str> {
    let sep = key.iter().position(|&b| b == 0x00)?;
    std::str::from_utf8(&key[..sep]).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn u64_roundtrip() {
        for val in [0, 1, 255, 65535, u64::MAX] {
            assert_eq!(decode_u64(&encode_u64(val)), val);
        }
    }

    #[test]
    fn u32_roundtrip() {
        for val in [0, 1, 255, 65535, u32::MAX] {
            assert_eq!(decode_u32(&encode_u32(val)), val);
        }
    }

    #[test]
    fn u64_lexicographic_order() {
        let a = encode_u64(1);
        let b = encode_u64(256);
        let c = encode_u64(u64::MAX);
        assert!(a < b);
        assert!(b < c);
    }

    #[test]
    fn tx_key_roundtrip() {
        let key = encode_tx_key(42, 100);
        assert_eq!(decode_u64(&key[..8]), 42);
        assert_eq!(decode_u32(&key[8..12]), 100);
    }

    #[test]
    fn unsynced_key_roundtrip() {
        let key = encode_unsynced_key("co_abc123", "peer_1");
        let id = decode_unsynced_co_value_id(&key);
        assert_eq!(id, Some("co_abc123"));
    }
}
