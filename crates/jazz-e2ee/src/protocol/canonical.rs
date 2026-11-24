//! The protocol's canonical JSON (`E2EE_PROTOCOL.md`
//! §6.2): ECMAScript `JSON.stringify` output as UTF-8, without whitespace,
//! with object keys sorted by UTF-16 code unit, absent fields dropped,
//! bytes as arrays of numbers and big integers as decimal strings.
//! `B(f, x) = UTF8(json(canonical([f, x])))`.
//!
//! Parsing accepts JSON text of these shapes only (no whitespace, no
//! fractions or exponents, no negative numbers); a decoder then rebuilds the
//! record and refuses input whose canonical encoding differs from it. A
//! document of records is parsed into its parts with
//! [`parse_object_parts`] and [`parse_array_parts`], which also accept
//! whitespace, so that each record is checked on its own.

use super::error::{ProtocolError, ProtocolResult};
use super::json;

/// The largest integer ECMAScript represents exactly; larger numbers are
/// written as decimal strings.
pub(crate) const MAX_SAFE_INTEGER: u64 = (1 << 53) - 1;
/// Nesting deeper than any record of the store is refused.
const MAX_DEPTH: usize = 16;

/// A JSON value of the shapes the protocol uses.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Value {
    Bool(bool),
    Number(u64),
    String(String),
    Array(Vec<Value>),
    /// Fields in canonical order; built only by [`Value::object`] or the
    /// parser.
    Object(Vec<(String, Value)>),
}

pub(super) fn invalid<T>(what: &'static str) -> ProtocolResult<T> {
    Err(ProtocolError::InvalidRecord(what))
}

impl Value {
    pub(crate) fn string(value: &str) -> Self {
        Self::String(value.to_owned())
    }

    /// A byte string: an array of numbers.
    pub(crate) fn bytes(value: &[u8]) -> Self {
        Self::Array(
            value
                .iter()
                .map(|byte| Self::Number((*byte).into()))
                .collect(),
        )
    }

    /// An object of the present fields, keys sorted by UTF-16 code unit.
    pub(crate) fn object(fields: Vec<(&str, Option<Value>)>) -> Self {
        let mut fields: Vec<(String, Value)> = fields
            .into_iter()
            .filter_map(|(name, value)| Some((name.to_owned(), value?)))
            .collect();
        fields.sort_by(|a, b| json::utf16_cmp(&a.0, &b.0));
        Self::Object(fields)
    }

    fn push(&self, out: &mut String) -> ProtocolResult<()> {
        match self {
            Self::Bool(value) => out.push_str(if *value { "true" } else { "false" }),
            Self::Number(value) if *value <= MAX_SAFE_INTEGER => out.push_str(&value.to_string()),
            Self::Number(_) => return invalid("number beyond the safe integer range"),
            Self::String(value) => json::push_string(out, value),
            Self::Array(items) => {
                out.push('[');
                for (index, item) in items.iter().enumerate() {
                    if index > 0 {
                        out.push(',');
                    }
                    item.push(out)?;
                }
                out.push(']');
            }
            Self::Object(fields) => {
                out.push('{');
                for (index, (name, value)) in fields.iter().enumerate() {
                    if index > 0 {
                        out.push(',');
                    }
                    json::push_string(out, name);
                    out.push(':');
                    value.push(out)?;
                }
                out.push('}');
            }
        }
        Ok(())
    }

    /// `json(canonical(self))` as UTF-8.
    pub(crate) fn encode(&self) -> ProtocolResult<Vec<u8>> {
        let mut out = String::new();
        self.push(&mut out)?;
        Ok(out.into_bytes())
    }

    /// The field `name` of an object.
    pub(crate) fn field(&self, name: &str) -> Option<&Value> {
        match self {
            Self::Object(fields) => fields
                .iter()
                .find(|(field, _)| field == name)
                .map(|(_, value)| value),
            _ => None,
        }
    }

    pub(crate) fn as_str(&self) -> ProtocolResult<&str> {
        match self {
            Self::String(value) => Ok(value),
            _ => invalid("expected a string"),
        }
    }

    pub(crate) fn as_number(&self) -> ProtocolResult<u64> {
        match self {
            Self::Number(value) => Ok(*value),
            _ => invalid("expected a number"),
        }
    }

    pub(crate) fn as_array(&self) -> ProtocolResult<&[Value]> {
        match self {
            Self::Array(items) => Ok(items),
            _ => invalid("expected an array"),
        }
    }

    /// A byte string: an array of numbers 0–255.
    pub(crate) fn as_bytes(&self) -> ProtocolResult<Vec<u8>> {
        self.as_array()?
            .iter()
            .map(|item| match item {
                Self::Number(byte) => u8::try_from(*byte)
                    .map_err(|_| ProtocolError::InvalidRecord("byte out of range")),
                _ => invalid("expected a byte"),
            })
            .collect()
    }
}

/// `B(f, x) = UTF8(json(canonical([f, x])))`.
pub(crate) fn framed(format: &str, value: Value) -> ProtocolResult<Vec<u8>> {
    Value::Array(vec![Value::string(format), value]).encode()
}

/// Parses text that must be the canonical encoding of what it parses to.
pub(crate) fn parse_canonical(bytes: &[u8]) -> ProtocolResult<Value> {
    fn sorted(value: &Value) -> bool {
        match value {
            Value::Array(items) => items.iter().all(sorted),
            Value::Object(fields) => {
                fields
                    .windows(2)
                    .all(|pair| json::utf16_cmp(&pair[0].0, &pair[1].0).is_lt())
                    && fields.iter().all(|(_, value)| sorted(value))
            }
            _ => true,
        }
    }
    let value = parse(bytes)?;
    if !sorted(&value) || value.encode()? != bytes {
        return invalid("non-canonical JSON");
    }
    Ok(value)
}

/// Parses JSON text of the store's shapes.
pub(crate) fn parse(bytes: &[u8]) -> ProtocolResult<Value> {
    let mut parser = Parser {
        bytes,
        at: 0,
        lenient: false,
    };
    let value = parser.value(0)?;
    if parser.at != bytes.len() {
        return invalid("trailing bytes after JSON");
    }
    Ok(value)
}

/// One field of an object parsed by [`parse_object_parts`]: its name, its
/// value and the exact bytes the value was parsed from.
pub(crate) type ObjectPart<'a> = (String, Value, &'a [u8]);

/// Parses a JSON object of the store's shapes, whitespace allowed, into its
/// fields, each with the exact bytes its value was parsed from, so that a
/// caller can check each part's canonical form on its own.
pub(crate) fn parse_object_parts(bytes: &[u8]) -> ProtocolResult<Vec<ObjectPart<'_>>> {
    let mut parser = Parser {
        bytes,
        at: 0,
        lenient: true,
    };
    parser.skip_whitespace();
    parser.expect(b'{')?;
    let mut fields: Vec<ObjectPart<'_>> = Vec::new();
    if parser.token() != Some(b'}') {
        loop {
            let name = parser.string()?;
            parser.expect(b':')?;
            let (value, raw) = parser.part(1)?;
            if fields.iter().any(|(field, _, _)| *field == name) {
                return invalid("repeated JSON field");
            }
            fields.push((name, value, raw));
            match parser.token() {
                Some(b',') => parser.at += 1,
                Some(b'}') => break,
                _ => return invalid("malformed JSON object"),
            }
        }
    }
    parser.expect(b'}')?;
    if parser.at != bytes.len() {
        return invalid("trailing bytes after JSON");
    }
    Ok(fields)
}

/// Parses a JSON array of the store's shapes, whitespace allowed, into its
/// items, each with the exact bytes it was parsed from.
pub(crate) fn parse_array_parts(bytes: &[u8]) -> ProtocolResult<Vec<(Value, &[u8])>> {
    let mut parser = Parser {
        bytes,
        at: 0,
        lenient: true,
    };
    parser.skip_whitespace();
    parser.expect(b'[')?;
    let mut items = Vec::new();
    if parser.token() != Some(b']') {
        loop {
            items.push(parser.part(1)?);
            match parser.token() {
                Some(b',') => parser.at += 1,
                Some(b']') => break,
                _ => return invalid("malformed JSON array"),
            }
        }
    }
    parser.expect(b']')?;
    if parser.at != bytes.len() {
        return invalid("trailing bytes after JSON");
    }
    Ok(items)
}

struct Parser<'a> {
    bytes: &'a [u8],
    at: usize,
    /// Whitespace between tokens is skipped rather than refused.
    lenient: bool,
}

impl<'a> Parser<'a> {
    fn peek(&self) -> Option<u8> {
        self.bytes.get(self.at).copied()
    }

    /// The first byte of the next token, after any whitespace a lenient
    /// parser skips.
    fn token(&mut self) -> Option<u8> {
        self.skip_whitespace();
        self.peek()
    }

    fn skip_whitespace(&mut self) {
        if self.lenient {
            while matches!(self.bytes.get(self.at), Some(b' ' | b'\t' | b'\n' | b'\r')) {
                self.at += 1;
            }
        }
    }

    /// A value and the exact bytes it was parsed from.
    fn part(&mut self, depth: usize) -> ProtocolResult<(Value, &'a [u8])> {
        self.skip_whitespace();
        let start = self.at;
        let value = self.value(depth)?;
        Ok((value, &self.bytes[start..self.at]))
    }

    fn expect(&mut self, byte: u8) -> ProtocolResult<()> {
        if self.token() == Some(byte) {
            self.at += 1;
            Ok(())
        } else {
            invalid("malformed JSON")
        }
    }

    fn literal(&mut self, text: &[u8]) -> ProtocolResult<()> {
        if self.bytes[self.at..].starts_with(text) {
            self.at += text.len();
            Ok(())
        } else {
            invalid("malformed JSON")
        }
    }

    fn value(&mut self, depth: usize) -> ProtocolResult<Value> {
        if depth > MAX_DEPTH {
            return invalid("JSON nested too deeply");
        }
        match self.token() {
            Some(b'"') => self.string().map(Value::String),
            Some(b'[') => {
                self.at += 1;
                let mut items = Vec::new();
                if self.token() == Some(b']') {
                    self.at += 1;
                    return Ok(Value::Array(items));
                }
                loop {
                    items.push(self.value(depth + 1)?);
                    match self.token() {
                        Some(b',') => self.at += 1,
                        Some(b']') => {
                            self.at += 1;
                            return Ok(Value::Array(items));
                        }
                        _ => return invalid("malformed JSON array"),
                    }
                }
            }
            Some(b'{') => {
                self.at += 1;
                let mut fields: Vec<(String, Value)> = Vec::new();
                if self.token() == Some(b'}') {
                    self.at += 1;
                    return Ok(Value::Object(fields));
                }
                loop {
                    let name = self.string()?;
                    self.expect(b':')?;
                    let value = self.value(depth + 1)?;
                    if fields.iter().any(|(field, _)| *field == name) {
                        return invalid("repeated JSON field");
                    }
                    fields.push((name, value));
                    match self.token() {
                        Some(b',') => self.at += 1,
                        Some(b'}') => {
                            self.at += 1;
                            return Ok(Value::Object(fields));
                        }
                        _ => return invalid("malformed JSON object"),
                    }
                }
            }
            Some(b't') => self.literal(b"true").map(|()| Value::Bool(true)),
            Some(b'f') => self.literal(b"false").map(|()| Value::Bool(false)),
            Some(b'0'..=b'9') => self.number(),
            _ => invalid("malformed JSON"),
        }
    }

    fn number(&mut self) -> ProtocolResult<Value> {
        let start = self.at;
        while matches!(self.peek(), Some(b'0'..=b'9')) {
            self.at += 1;
        }
        let digits = core::str::from_utf8(&self.bytes[start..self.at]).expect("ASCII digits");
        if matches!(self.peek(), Some(b'.' | b'e' | b'E')) {
            return invalid("not an integer");
        }
        match digits.parse::<u64>() {
            Ok(value) if value <= MAX_SAFE_INTEGER => Ok(Value::Number(value)),
            _ => invalid("number beyond the safe integer range"),
        }
    }

    fn hex4(&mut self) -> ProtocolResult<u32> {
        let digits = self
            .bytes
            .get(self.at..self.at + 4)
            .and_then(|digits| core::str::from_utf8(digits).ok())
            .and_then(|digits| u32::from_str_radix(digits, 16).ok())
            .ok_or(ProtocolError::InvalidRecord("malformed JSON escape"))?;
        self.at += 4;
        Ok(digits)
    }

    fn string(&mut self) -> ProtocolResult<String> {
        self.expect(b'"')?;
        let mut out = String::new();
        loop {
            let start = self.at;
            while !matches!(self.peek(), None | Some(b'"' | b'\\')) {
                if self.peek().is_some_and(|byte| byte < 0x20) {
                    return invalid("control character in JSON string");
                }
                self.at += 1;
            }
            out.push_str(
                core::str::from_utf8(&self.bytes[start..self.at])
                    .map_err(|_| ProtocolError::InvalidRecord("JSON string is not UTF-8"))?,
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
                        .ok_or(ProtocolError::InvalidRecord("malformed JSON escape"))?;
                    self.at += 1;
                    match escape {
                        b'"' => out.push('"'),
                        b'\\' => out.push('\\'),
                        b'/' => out.push('/'),
                        b'b' => out.push('\u{8}'),
                        b'f' => out.push('\u{c}'),
                        b'n' => out.push('\n'),
                        b'r' => out.push('\r'),
                        b't' => out.push('\t'),
                        b'u' => {
                            let unit = self.hex4()?;
                            let code = if (0xd800..0xdc00).contains(&unit) {
                                self.literal(b"\\u")?;
                                let low = self.hex4()?;
                                if !(0xdc00..0xe000).contains(&low) {
                                    return invalid("unpaired surrogate in JSON string");
                                }
                                0x10000 + ((unit - 0xd800) << 10) + (low - 0xdc00)
                            } else {
                                unit
                            };
                            out.push(char::from_u32(code).ok_or(ProtocolError::InvalidRecord(
                                "unpaired surrogate in JSON string",
                            ))?);
                        }
                        _ => return invalid("malformed JSON escape"),
                    }
                }
                _ => return invalid("unterminated JSON string"),
            }
        }
    }
}
