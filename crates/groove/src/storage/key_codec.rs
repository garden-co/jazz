//! Column-family key namespacing for single-keyspace backends.
//!
//! Backends like OPFS have one flat ordered keyspace, not RocksDB-style
//! column families. This codec folds a `(column family, key)` pair into one
//! byte string — `version, cf-length, cf-name, key` — so that keys of one CF
//! stay contiguous and sort by name then key, matching the `OrderedKvStorage`
//! contract. Multi-column-family backends do not use it.

use super::Error;

/// Format version byte, so the on-disk key layout can evolve.
const KEY_VERSION: u8 = 1;
/// Column-family names are length-prefixed with a `u16`, so this is the cap.
const MAX_COLUMN_FAMILY_LEN: usize = u16::MAX as usize;

/// Folds `(cf, key)` into one namespaced key: `version` byte, big-endian
/// `u16` CF-name length, the CF name, then the key. Fails when the CF name
/// exceeds [`MAX_COLUMN_FAMILY_LEN`].
pub fn encode_column_family_key(cf: &str, key: &[u8]) -> Result<Vec<u8>, Error> {
    let cf_bytes = cf.as_bytes();
    if cf_bytes.len() > MAX_COLUMN_FAMILY_LEN {
        return Err(Error::InvalidStorageKey(format!(
            "column family name is too long: {} bytes",
            cf_bytes.len()
        )));
    }

    let mut encoded = Vec::with_capacity(1 + 2 + cf_bytes.len() + key.len());
    encoded.push(KEY_VERSION);
    encoded.extend_from_slice(&(cf_bytes.len() as u16).to_be_bytes());
    encoded.extend_from_slice(cf_bytes);
    encoded.extend_from_slice(key);
    Ok(encoded)
}

/// Reverses [`encode_column_family_key`], borrowing the CF name and key back
/// out of the namespaced bytes. Rejects a wrong version byte or a truncated
/// key.
pub fn decode_column_family_key(encoded: &[u8]) -> Result<(&str, &[u8]), Error> {
    if encoded.len() < 3 || encoded[0] != KEY_VERSION {
        return Err(Error::InvalidStorageKey(
            "unsupported OPFS storage key".to_string(),
        ));
    }

    let cf_len = u16::from_be_bytes([encoded[1], encoded[2]]) as usize;
    let key_offset = 3 + cf_len;
    if encoded.len() < key_offset {
        return Err(Error::InvalidStorageKey(
            "truncated OPFS storage key".to_string(),
        ));
    }

    let cf = std::str::from_utf8(&encoded[3..key_offset])
        .map_err(|_| Error::InvalidStorageKey("invalid UTF-8 column family name".to_string()))?;
    Ok((cf, &encoded[key_offset..]))
}

/// The next byte string after `bytes` in lexicographic order, formed by
/// incrementing the last non-`0xff` byte and dropping the trailing `0xff`
/// run. Returns `None` when `bytes` is all `0xff` (no successor exists).
pub fn increment_bytes(bytes: &[u8]) -> Option<Vec<u8>> {
    let mut next = bytes.to_vec();
    for index in (0..next.len()).rev() {
        if next[index] != u8::MAX {
            next[index] += 1;
            next.truncate(index + 1);
            return Some(next);
        }
    }
    None
}

/// The smallest key strictly greater than every key starting with `prefix` —
/// the exclusive upper bound turning a prefix scan into a range scan.
pub fn prefix_upper_bound(prefix: &[u8]) -> Option<Vec<u8>> {
    increment_bytes(prefix)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encoded_key_layout_matches_legacy_opfs_btree_codec() {
        assert_eq!(
            encode_column_family_key("rows", &[0, 1, 255]).unwrap(),
            vec![1, 0, 4, b'r', b'o', b'w', b's', 0, 1, 255]
        );
    }

    #[test]
    fn round_trips_column_family_key() {
        let encoded = encode_column_family_key("rows", &[0, 1, 255]).expect("encode");
        let (cf, key) = decode_column_family_key(&encoded).expect("decode");

        assert_eq!(cf, "rows");
        assert_eq!(key, &[0, 1, 255]);
    }

    #[test]
    fn preserves_encoded_ordering_within_column_family() {
        let mut encoded = [
            encode_column_family_key("rows", b"b").unwrap(),
            encode_column_family_key("rows", b"a").unwrap(),
            encode_column_family_key("rows", b"aa").unwrap(),
        ];
        encoded.sort();

        let decoded = encoded
            .iter()
            .map(|key| decode_column_family_key(key).unwrap().1.to_vec())
            .collect::<Vec<_>>();
        assert_eq!(decoded, vec![b"a".to_vec(), b"aa".to_vec(), b"b".to_vec()]);
    }

    #[test]
    fn keeps_column_family_prefixes_contiguous_and_ordered_by_name() {
        let rows = encode_column_family_key("rows", b"").unwrap();
        let rows_key = encode_column_family_key("rows", b"k").unwrap();
        let rowset = encode_column_family_key("rowset", b"").unwrap();

        assert!(rows_key.starts_with(&rows));
        assert!(rows < rowset);
        assert!(!rowset.starts_with(&rows));
    }

    #[test]
    fn prefix_upper_bound_is_minimal_exclusive_bound() {
        assert_eq!(prefix_upper_bound(&[1, 2, 255]), Some(vec![1, 3]));
        assert_eq!(prefix_upper_bound(&[255, 255]), None);
    }
}
