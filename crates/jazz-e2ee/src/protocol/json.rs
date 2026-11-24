//! Canonical JSON (`E2EE_PROTOCOL.md` §0): ECMAScript `JSON.stringify` output
//! as UTF-8, without whitespace, for arrays of strings, nested arrays and
//! (in delivery maps) byte values.
//!
//! Decoding is strict: a value is parsed, re-encoded, and refused unless the
//! re-encoding is byte-identical, so every accepted encoding is canonical.

use core::cmp::Ordering;

use super::canonical::{self, Value, invalid};
use super::error::{ProtocolError, ProtocolResult};
use super::ids::{self, IdForm};

/// The largest delivery envelope a delivery map carries.
pub const MAX_DELIVERY_BYTES: usize = 65_536;

/// Appends `value` as a JSON string, escaped as `JSON.stringify` does.
pub fn push_string(out: &mut String, value: &str) {
    out.push('"');
    for ch in value.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\u{8}' => out.push_str("\\b"),
            '\u{c}' => out.push_str("\\f"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            ch if (ch as u32) < 0x20 => {
                out.push_str(&format!("\\u{:04x}", ch as u32));
            }
            ch => out.push(ch),
        }
    }
    out.push('"');
}

/// `json([a, b, …])` of strings.
pub fn strings(values: &[&str]) -> String {
    let mut out = String::from("[");
    for (index, value) in values.iter().enumerate() {
        if index > 0 {
            out.push(',');
        }
        push_string(&mut out, value);
    }
    out.push(']');
    out
}

/// Ascending by UTF-16 code unit, the protocol's `sorted`.
pub fn utf16_cmp(a: &str, b: &str) -> Ordering {
    a.encode_utf16().cmp(b.encode_utf16())
}

/// Parses `bytes` and refuses anything whose re-encoding differs.
fn parse_canonical(bytes: &[u8]) -> ProtocolResult<Value> {
    canonical::parse_canonical_list(bytes)
        .map_err(|_| ProtocolError::InvalidRecord("non-canonical JSON"))
}

fn into_strings(value: Value) -> ProtocolResult<Vec<String>> {
    let Value::Array(items) = value else {
        return invalid("non-canonical JSON");
    };
    items
        .into_iter()
        .map(|item| match item {
            Value::String(text) => Ok(text),
            _ => invalid("non-canonical JSON"),
        })
        .collect()
}

fn strictly_increasing<T>(items: &[T], key: impl Fn(&T) -> &str) -> bool {
    items
        .windows(2)
        .all(|pair| utf16_cmp(key(&pair[0]), key(&pair[1])) == Ordering::Less)
}

fn sorted_strings(ids: impl IntoIterator<Item = impl AsRef<str>>) -> Vec<String> {
    let mut sorted: Vec<String> = ids.into_iter().map(|id| id.as_ref().to_owned()).collect();
    sorted.sort_by(|a, b| utf16_cmp(a, b));
    sorted
}

fn check_id_list(ids: &[String], form: Option<IdForm>) -> ProtocolResult<()> {
    let valid = strictly_increasing(ids, String::as_str)
        && ids
            .iter()
            .all(|id| !id.is_empty() && form.is_none_or(|form| ids::is_id(id, form)));
    if valid {
        Ok(())
    } else {
        Err(ProtocolError::InvalidRecord("invalid ID list"))
    }
}

fn encode_list(
    ids: impl IntoIterator<Item = impl AsRef<str>>,
    form: Option<IdForm>,
) -> ProtocolResult<Vec<u8>> {
    let ids = sorted_strings(ids);
    check_id_list(&ids, form)?;
    let refs: Vec<&str> = ids.iter().map(String::as_str).collect();
    Ok(strings(&refs).into_bytes())
}

fn decode_list(bytes: &[u8], form: Option<IdForm>) -> ProtocolResult<Vec<String>> {
    let ids = into_strings(parse_canonical(bytes)?)?;
    check_id_list(&ids, form)?;
    Ok(ids)
}

/// An ID list (`encodeEpochIds`): `json(sorted ids)`, strictly increasing,
/// every ID a version 4 or 7 UUID. Duplicates are refused.
pub fn encode_id_list(ids: impl IntoIterator<Item = impl AsRef<str>>) -> ProtocolResult<Vec<u8>> {
    encode_list(ids, Some(IdForm::Random))
}

pub fn decode_id_list(bytes: &[u8]) -> ProtocolResult<Vec<String>> {
    decode_list(bytes, Some(IdForm::Random))
}

/// A recovery root ID list: the ID-list encoding over recovery root IDs,
/// which may also be derived (version 8). May be empty.
pub fn encode_root_id_list(
    ids: impl IntoIterator<Item = impl AsRef<str>>,
) -> ProtocolResult<Vec<u8>> {
    encode_list(ids, Some(IdForm::RandomOrDerived))
}

pub fn decode_root_id_list(bytes: &[u8]) -> ProtocolResult<Vec<String>> {
    decode_list(bytes, Some(IdForm::RandomOrDerived))
}

/// A revision: the ID-list encoding over non-empty strings that need not be
/// UUIDs.
pub fn encode_revision(ids: impl IntoIterator<Item = impl AsRef<str>>) -> ProtocolResult<Vec<u8>> {
    encode_list(ids, None)
}

pub fn decode_revision(bytes: &[u8]) -> ProtocolResult<Vec<String>> {
    decode_list(bytes, None)
}

/// A delivery map (`encodeEpochDeliveries`):
/// `json([[deviceId, [byte, …]], …])` sorted by device ID, strictly
/// increasing, device IDs version 4 or 7 UUIDs, each envelope 1–65,536 bytes.
pub fn encode_delivery_map(
    deliveries: impl IntoIterator<Item = (impl AsRef<str>, impl AsRef<[u8]>)>,
) -> ProtocolResult<Vec<u8>> {
    let mut entries: Vec<(String, Vec<u8>)> = deliveries
        .into_iter()
        .map(|(device, envelope)| (device.as_ref().to_owned(), envelope.as_ref().to_vec()))
        .collect();
    entries.sort_by(|a, b| utf16_cmp(&a.0, &b.0));
    check_delivery_map(&entries)?;
    Value::Array(
        entries
            .iter()
            .map(|(device, envelope)| {
                Value::Array(vec![Value::string(device), Value::bytes(envelope)])
            })
            .collect(),
    )
    .encode()
}

fn check_delivery_map(entries: &[(String, Vec<u8>)]) -> ProtocolResult<()> {
    let valid = strictly_increasing(entries, |entry| entry.0.as_str())
        && entries.iter().all(|(device, envelope)| {
            ids::is_id(device, IdForm::Random) && (1..=MAX_DELIVERY_BYTES).contains(&envelope.len())
        });
    if valid {
        Ok(())
    } else {
        Err(ProtocolError::InvalidRecord("invalid delivery map"))
    }
}

pub fn decode_delivery_map(bytes: &[u8]) -> ProtocolResult<Vec<(String, Vec<u8>)>> {
    let Value::Array(items) = parse_canonical(bytes)? else {
        return invalid("non-canonical JSON");
    };
    let mut entries = Vec::with_capacity(items.len());
    for item in items {
        let Value::Array(pair) = item else {
            return invalid("non-canonical JSON");
        };
        let [Value::String(device), Value::Array(values)] = <[Value; 2]>::try_from(pair)
            .map_err(|_| ProtocolError::InvalidRecord("invalid delivery map"))?
        else {
            return invalid("non-canonical JSON");
        };
        let envelope = values
            .into_iter()
            .map(|value| match value {
                Value::Number(byte) => u8::try_from(byte)
                    .map_err(|_| ProtocolError::InvalidRecord("invalid delivery map")),
                _ => invalid("non-canonical JSON"),
            })
            .collect::<ProtocolResult<Vec<u8>>>()?;
        entries.push((device, envelope));
    }
    check_delivery_map(&entries)?;
    Ok(entries)
}
