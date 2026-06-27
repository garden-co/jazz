use super::Error;

const KEY_VERSION: u8 = 1;
const MAX_COLUMN_FAMILY_LEN: usize = u16::MAX as usize;

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
