//! Canonical JSON (`E2EE_PROTOCOL.md` §0): ECMAScript `JSON.stringify` output
//! as UTF-8, without whitespace, for arrays of strings, nested arrays and
//! (in delivery maps) byte values.
//!
//! Decoding is strict: a value is parsed, re-encoded, and refused unless the
//! re-encoding is byte-identical, so every accepted encoding is canonical.

use core::cmp::Ordering;

use super::error::{ProtocolError, ProtocolResult};
use super::ids::{self, IdForm};

const MAX_DEPTH: usize = 3;
/// The largest delivery envelope a delivery map carries.
pub const MAX_DELIVERY_BYTES: usize = 65_536;

/// A parsed canonical JSON value of the shapes the protocol uses.
#[derive(Debug, Clone, PartialEq, Eq)]
enum Value {
    String(String),
    Number(u64),
    Array(Vec<Value>),
}

/// Appends `value` as a JSON string, escaped as `JSON.stringify` does.
fn push_string(out: &mut String, value: &str) {
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

fn push_value(out: &mut String, value: &Value) {
    match value {
        Value::String(text) => push_string(out, text),
        Value::Number(number) => out.push_str(&number.to_string()),
        Value::Array(items) => {
            out.push('[');
            for (index, item) in items.iter().enumerate() {
                if index > 0 {
                    out.push(',');
                }
                push_value(out, item);
            }
            out.push(']');
        }
    }
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

struct Parser<'a> {
    bytes: &'a [u8],
    at: usize,
}

impl Parser<'_> {
    fn fail<T>() -> ProtocolResult<T> {
        Err(ProtocolError::InvalidRecord("non-canonical JSON"))
    }

    fn peek(&self) -> Option<u8> {
        self.bytes.get(self.at).copied()
    }

    fn expect(&mut self, byte: u8) -> ProtocolResult<()> {
        if self.peek() == Some(byte) {
            self.at += 1;
            Ok(())
        } else {
            Self::fail()
        }
    }

    fn value(&mut self, depth: usize) -> ProtocolResult<Value> {
        match self.peek() {
            Some(b'"') => self.string().map(Value::String),
            Some(b'[') if depth < MAX_DEPTH => self.array(depth + 1),
            Some(b'0'..=b'9') => self.number(),
            _ => Self::fail(),
        }
    }

    fn array(&mut self, depth: usize) -> ProtocolResult<Value> {
        self.expect(b'[')?;
        let mut items = Vec::new();
        if self.peek() == Some(b']') {
            self.at += 1;
            return Ok(Value::Array(items));
        }
        loop {
            items.push(self.value(depth)?);
            match self.peek() {
                Some(b',') => self.at += 1,
                Some(b']') => {
                    self.at += 1;
                    return Ok(Value::Array(items));
                }
                _ => return Self::fail(),
            }
        }
    }

    fn number(&mut self) -> ProtocolResult<Value> {
        let start = self.at;
        while matches!(self.peek(), Some(b'0'..=b'9')) {
            self.at += 1;
        }
        let digits = core::str::from_utf8(&self.bytes[start..self.at])
            .map_err(|_| ProtocolError::InvalidRecord("non-canonical JSON"))?;
        digits
            .parse::<u64>()
            .map(Value::Number)
            .map_err(|_| ProtocolError::InvalidRecord("non-canonical JSON"))
    }

    fn hex4(&mut self) -> ProtocolResult<u32> {
        let digits = self
            .bytes
            .get(self.at..self.at + 4)
            .ok_or(ProtocolError::InvalidRecord("non-canonical JSON"))?;
        self.at += 4;
        let text = core::str::from_utf8(digits)
            .map_err(|_| ProtocolError::InvalidRecord("non-canonical JSON"))?;
        u32::from_str_radix(text, 16)
            .map_err(|_| ProtocolError::InvalidRecord("non-canonical JSON"))
    }

    fn string(&mut self) -> ProtocolResult<String> {
        self.expect(b'"')?;
        let mut out = String::new();
        loop {
            let start = self.at;
            while !matches!(self.peek(), None | Some(b'"' | b'\\')) {
                self.at += 1;
            }
            out.push_str(
                core::str::from_utf8(&self.bytes[start..self.at])
                    .map_err(|_| ProtocolError::InvalidRecord("non-canonical JSON"))?,
            );
            match self.peek() {
                Some(b'"') => {
                    self.at += 1;
                    return Ok(out);
                }
                Some(b'\\') => {
                    self.at += 1;
                    let escape = self
                        .peek()
                        .ok_or(ProtocolError::InvalidRecord("non-canonical JSON"))?;
                    self.at += 1;
                    let ch = match escape {
                        b'"' => '"',
                        b'\\' => '\\',
                        b'/' => '/',
                        b'b' => '\u{8}',
                        b'f' => '\u{c}',
                        b'n' => '\n',
                        b'r' => '\r',
                        b't' => '\t',
                        b'u' => {
                            let unit = self.hex4()?;
                            char::from_u32(unit)
                                .ok_or(ProtocolError::InvalidRecord("non-canonical JSON"))?
                        }
                        _ => return Self::fail(),
                    };
                    out.push(ch);
                }
                _ => return Self::fail(),
            }
        }
    }
}

/// Parses `bytes` and refuses anything whose re-encoding differs.
fn parse_canonical(bytes: &[u8]) -> ProtocolResult<Value> {
    let mut parser = Parser { bytes, at: 0 };
    let value = parser.value(0)?;
    if parser.at != bytes.len() {
        return Parser::fail();
    }
    let mut encoded = String::new();
    push_value(&mut encoded, &value);
    if encoded.as_bytes() != bytes {
        return Parser::fail();
    }
    Ok(value)
}

fn into_strings(value: Value) -> ProtocolResult<Vec<String>> {
    let Value::Array(items) = value else {
        return Parser::fail();
    };
    items
        .into_iter()
        .map(|item| match item {
            Value::String(text) => Ok(text),
            _ => Parser::fail(),
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

fn encode_list(ids: &[String]) -> Vec<u8> {
    let refs: Vec<&str> = ids.iter().map(String::as_str).collect();
    strings(&refs).into_bytes()
}

/// An ID list (`encodeEpochIds`): `json(sorted ids)`, strictly increasing,
/// every ID a version 4 or 7 UUID. Duplicates are refused.
pub fn encode_id_list(ids: impl IntoIterator<Item = impl AsRef<str>>) -> ProtocolResult<Vec<u8>> {
    let sorted = sorted_strings(ids);
    check_id_list(&sorted, Some(IdForm::Random))?;
    Ok(encode_list(&sorted))
}

pub fn decode_id_list(bytes: &[u8]) -> ProtocolResult<Vec<String>> {
    let ids = into_strings(parse_canonical(bytes)?)?;
    check_id_list(&ids, Some(IdForm::Random))?;
    Ok(ids)
}

/// A recovery root ID list: the ID-list encoding over recovery root IDs,
/// which may also be derived (version 8). May be empty.
pub fn encode_root_id_list(
    ids: impl IntoIterator<Item = impl AsRef<str>>,
) -> ProtocolResult<Vec<u8>> {
    let sorted = sorted_strings(ids);
    check_id_list(&sorted, Some(IdForm::RandomOrDerived))?;
    Ok(encode_list(&sorted))
}

pub fn decode_root_id_list(bytes: &[u8]) -> ProtocolResult<Vec<String>> {
    let ids = into_strings(parse_canonical(bytes)?)?;
    check_id_list(&ids, Some(IdForm::RandomOrDerived))?;
    Ok(ids)
}

/// A revision: the ID-list encoding over non-empty strings that need not be
/// UUIDs.
pub fn encode_revision(ids: impl IntoIterator<Item = impl AsRef<str>>) -> ProtocolResult<Vec<u8>> {
    let sorted = sorted_strings(ids);
    check_id_list(&sorted, None)?;
    Ok(encode_list(&sorted))
}

pub fn decode_revision(bytes: &[u8]) -> ProtocolResult<Vec<String>> {
    let ids = into_strings(parse_canonical(bytes)?)?;
    check_id_list(&ids, None)?;
    Ok(ids)
}

/// A member map (`encodeGroupMembership`): `json([[accountId, epochId], …])`
/// sorted by account ID, strictly increasing, epoch IDs version 4 or 7 UUIDs.
pub fn encode_member_map(
    members: impl IntoIterator<Item = (impl AsRef<str>, impl AsRef<str>)>,
) -> ProtocolResult<Vec<u8>> {
    let mut entries: Vec<(String, String)> = members
        .into_iter()
        .map(|(account, epoch)| (account.as_ref().to_owned(), epoch.as_ref().to_owned()))
        .collect();
    entries.sort_by(|a, b| utf16_cmp(&a.0, &b.0));
    check_member_map(&entries)?;
    let value = Value::Array(
        entries
            .into_iter()
            .map(|(account, epoch)| {
                Value::Array(vec![Value::String(account), Value::String(epoch)])
            })
            .collect(),
    );
    let mut out = String::new();
    push_value(&mut out, &value);
    Ok(out.into_bytes())
}

fn check_member_map(entries: &[(String, String)]) -> ProtocolResult<()> {
    let valid = strictly_increasing(entries, |entry| entry.0.as_str())
        && entries
            .iter()
            .all(|(account, epoch)| !account.is_empty() && ids::is_id(epoch, IdForm::Random));
    if valid {
        Ok(())
    } else {
        Err(ProtocolError::InvalidRecord("invalid member map"))
    }
}

pub fn decode_member_map(bytes: &[u8]) -> ProtocolResult<Vec<(String, String)>> {
    let Value::Array(items) = parse_canonical(bytes)? else {
        return Parser::fail();
    };
    let mut entries = Vec::with_capacity(items.len());
    for item in items {
        let mut pair = into_strings(item)?;
        if pair.len() != 2 {
            return Parser::fail();
        }
        let epoch = pair.pop().expect("two items");
        let account = pair.pop().expect("two items");
        entries.push((account, epoch));
    }
    check_member_map(&entries)?;
    Ok(entries)
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
    let value = Value::Array(
        entries
            .into_iter()
            .map(|(device, envelope)| {
                Value::Array(vec![
                    Value::String(device),
                    Value::Array(
                        envelope
                            .into_iter()
                            .map(|byte| Value::Number(byte.into()))
                            .collect(),
                    ),
                ])
            })
            .collect(),
    );
    let mut out = String::new();
    push_value(&mut out, &value);
    Ok(out.into_bytes())
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
        return Parser::fail();
    };
    let mut entries = Vec::with_capacity(items.len());
    for item in items {
        let Value::Array(pair) = item else {
            return Parser::fail();
        };
        let [Value::String(device), Value::Array(values)] = <[Value; 2]>::try_from(pair)
            .map_err(|_| ProtocolError::InvalidRecord("invalid delivery map"))?
        else {
            return Parser::fail();
        };
        let envelope = values
            .into_iter()
            .map(|value| match value {
                Value::Number(byte) => u8::try_from(byte)
                    .map_err(|_| ProtocolError::InvalidRecord("invalid delivery map")),
                _ => Parser::fail(),
            })
            .collect::<ProtocolResult<Vec<u8>>>()?;
        entries.push((device, envelope));
    }
    check_delivery_map(&entries)?;
    Ok(entries)
}
