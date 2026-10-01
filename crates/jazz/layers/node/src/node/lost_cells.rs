//! Sparse cells: the codec of a history record's `lost_cells`.
//!
//! When Core keeps an accepted value over a write's authored cell (SPEC 4
//! §4.6, "Merge rule"), the write's own value is recorded on the write's
//! history record as a **lost cell**. A record's lost cells are one byte
//! string, empty in the common case:
//!
//! - a varint count `n >= 1`;
//! - `n` strictly increasing varint keys;
//! - one groove record whose `n` fields are the keyed cells, as nullable
//!   values of their column types, in key order.
//!
//! Key presence says which cell lost; a lost null is a present key holding
//! null. In storage the keys are node-local physical column ids
//! (`DELETION_COLUMN_ID` is `_deletion`); on the wire they are slots of the
//! authored table (`0` is `_deletion`, `i + 1` the `i`-th user column), since
//! physical ids never cross the wire. Values are encoded with the cell types
//! of the record's own schema version in both carriers.

use super::Error;
use groove::records::{RecordDescriptor, Value, ValueType};

/// Encode `(key, cell type, value)` entries, sorted by strictly increasing
/// key. No entry encodes as the empty string.
pub(super) fn encode(entries: &[(u64, ValueType, Value)]) -> Result<Vec<u8>, Error> {
    if entries.is_empty() {
        return Ok(Vec::new());
    }
    if entries.windows(2).any(|pair| pair[0].0 >= pair[1].0) {
        return Err(Error::InvalidStoredValue(
            "lost cell keys must be strictly increasing",
        ));
    }
    let mut bytes = Vec::new();
    write_varint(&mut bytes, entries.len() as u64);
    for (key, _, _) in entries {
        write_varint(&mut bytes, *key);
    }
    let descriptor = descriptor(entries.iter().map(|(_, value_type, _)| value_type.clone()));
    let values = entries
        .iter()
        .map(|(_, _, value)| value.clone())
        .collect::<Vec<_>>();
    bytes.extend(descriptor.create(&values)?);
    Ok(bytes)
}

/// Decode a sparse-cell string; `type_of` names each key's cell type and
/// rejects an unknown key. Noncanonical input is rejected.
pub(super) fn decode(
    bytes: &[u8],
    mut type_of: impl FnMut(u64) -> Result<ValueType, Error>,
) -> Result<Vec<(u64, Value)>, Error> {
    if bytes.is_empty() {
        return Ok(Vec::new());
    }
    let malformed = || Error::InvalidStoredValue("malformed lost cells");
    let mut cursor = 0;
    let count = read_varint(bytes, &mut cursor).ok_or_else(malformed)?;
    if count == 0 || count > bytes.len() as u64 {
        return Err(malformed());
    }
    let mut keys = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let key = read_varint(bytes, &mut cursor).ok_or_else(malformed)?;
        if keys.last().is_some_and(|previous| *previous >= key) {
            return Err(malformed());
        }
        keys.push(key);
    }
    let types = keys
        .iter()
        .map(|key| type_of(*key))
        .collect::<Result<Vec<_>, _>>()?;
    let descriptor = descriptor(types.iter().cloned());
    let raw = &bytes[cursor..];
    let values = descriptor.bind(raw).to_values().map_err(|_| malformed())?;
    if descriptor.create(&values).map_err(|_| malformed())? != raw {
        return Err(malformed());
    }
    Ok(keys.into_iter().zip(values).collect())
}

/// Re-key a sparse-cell string, keeping each value's bytes. `rekey` maps an
/// old key to its new key and cell type; the result is sorted by new key.
pub(super) fn rekey(
    bytes: &[u8],
    mut rekey: impl FnMut(u64) -> Result<(u64, ValueType), Error>,
) -> Result<Vec<u8>, Error> {
    let mut mapped = Vec::new();
    let mut types = std::collections::BTreeMap::new();
    for (key, value) in decode(bytes, |key| {
        let (new_key, value_type) = rekey(key)?;
        types.insert(key, (new_key, value_type.clone()));
        Ok(value_type)
    })? {
        let (new_key, value_type) = types[&key].clone();
        mapped.push((new_key, value_type, value));
    }
    mapped.sort_by_key(|(key, _, _)| *key);
    encode(&mapped)
}

fn descriptor(types: impl Iterator<Item = ValueType>) -> RecordDescriptor {
    RecordDescriptor::new(
        types
            .enumerate()
            .map(|(index, value_type)| (format!("c{index}"), value_type)),
    )
}

fn write_varint(bytes: &mut Vec<u8>, mut value: u64) {
    loop {
        let low = (value & 0x7f) as u8;
        value >>= 7;
        if value == 0 {
            bytes.push(low);
            return;
        }
        bytes.push(low | 0x80);
    }
}

fn read_varint(bytes: &[u8], cursor: &mut usize) -> Option<u64> {
    let mut value = 0u64;
    for shift in (0..64).step_by(7) {
        let byte = *bytes.get(*cursor)?;
        *cursor += 1;
        let low = u64::from(byte & 0x7f);
        if shift == 63 && low > 1 {
            return None;
        }
        value |= low << shift;
        if byte & 0x80 == 0 {
            // Canonical: no trailing zero group after the first.
            if byte == 0 && shift > 0 {
                return None;
            }
            return Some(value);
        }
    }
    None
}

// Internal test: the sparse-cell bytes are a storage and wire carrier that no
// public API exposes byte for byte, so this pins them directly.
#[cfg(test)]
mod tests {
    use super::*;

    fn string() -> ValueType {
        ValueType::Nullable(Box::new(ValueType::String))
    }

    fn some(value: Value) -> Value {
        Value::Nullable(Some(Box::new(value)))
    }

    #[test]
    fn sparse_cells_are_count_keys_then_one_record_and_reject_noncanonical_input() {
        assert_eq!(encode(&[]).unwrap(), Vec::<u8>::new());
        assert!(decode(&[], |_| Ok(string())).unwrap().is_empty());

        let bytes = encode(&[
            (3, string(), some(Value::String("x".to_owned()))),
            (200, string(), Value::Nullable(None)),
        ])
        .unwrap();
        // count 2, keys 3 and 200 (varint `c8 01`), then the record.
        assert_eq!(&bytes[..4], &[2, 3, 0xc8, 0x01]);
        let record = RecordDescriptor::new([("c0", string()), ("c1", string())])
            .create(&[some(Value::String("x".to_owned())), Value::Nullable(None)])
            .unwrap();
        assert_eq!(&bytes[4..], &record[..]);
        assert_eq!(
            decode(&bytes, |_| Ok(string())).unwrap(),
            vec![
                (3, some(Value::String("x".to_owned()))),
                // A lost null is a present key holding null.
                (200, Value::Nullable(None)),
            ]
        );

        assert!(
            encode(&[
                (2, string(), Value::Nullable(None)),
                (2, string(), Value::Nullable(None))
            ])
            .is_err()
        );
        assert!(decode(&[0], |_| Ok(string())).is_err());
        assert!(decode(&[2, 5, 3], |_| Ok(string())).is_err());
        // A non-minimal varint key.
        let mut padded = vec![1, 0x83, 0x00];
        padded.extend(&bytes[4..]);
        assert!(decode(&padded, |_| Ok(string())).is_err());
        // Trailing bytes after the record.
        let mut trailing = bytes.clone();
        trailing.push(0);
        assert!(decode(&trailing, |_| Ok(string())).is_err());
        // An unknown key is the caller's refusal.
        assert!(
            decode(&bytes, |key| if key == 3 {
                Ok(string())
            } else {
                Err(Error::InvalidStoredValue("unknown"))
            })
            .is_err()
        );

        let rekeyed = rekey(&bytes, |key| Ok((if key == 3 { 9 } else { 1 }, string()))).unwrap();
        assert_eq!(
            decode(&rekeyed, |_| Ok(string())).unwrap(),
            vec![
                (1, Value::Nullable(None)),
                (9, some(Value::String("x".to_owned()))),
            ]
        );
    }
}
