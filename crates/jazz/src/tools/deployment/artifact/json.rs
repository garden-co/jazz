//! JSON as the app's files need it: object keys keep the order they are
//! written in (a table's `columns` are its declared columns, in order), and
//! the canonical printer lays a value out on one line when it fits.

use std::fmt::Write as _;

/// A JSON value with its object keys in file order.
#[derive(Debug, Clone, PartialEq)]
pub enum Json {
    Null,
    Bool(bool),
    /// The number's text, as written: integers and decimals stay exact.
    Number(String),
    String(String),
    Array(Vec<Json>),
    Object(Vec<(String, Json)>),
}

impl Json {
    pub fn object(entries: Vec<(&str, Json)>) -> Json {
        Json::Object(
            entries
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect(),
        )
    }

    pub fn string(value: impl Into<String>) -> Json {
        Json::String(value.into())
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Json::String(text) => Some(text),
            _ => None,
        }
    }

    pub fn as_object(&self) -> Option<&[(String, Json)]> {
        match self {
            Json::Object(entries) => Some(entries),
            _ => None,
        }
    }

    pub fn get(&self, key: &str) -> Option<&Json> {
        self.as_object()?
            .iter()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v)
    }

    pub fn kind(&self) -> &'static str {
        match self {
            Json::Null => "null",
            Json::Bool(_) => "a boolean",
            Json::Number(_) => "a number",
            Json::String(_) => "a string",
            Json::Array(_) => "an array",
            Json::Object(_) => "an object",
        }
    }
}

/// Parse `text`; the error says where (line and column) and what.
pub fn parse(text: &[u8]) -> Result<Json, String> {
    let text = std::str::from_utf8(text).map_err(|_| "not UTF-8".to_string())?;
    let mut parser = Parser {
        text,
        bytes: text.as_bytes(),
        at: 0,
        depth: 0,
    };
    parser.skip_ws();
    let value = parser.value()?;
    parser.skip_ws();
    if parser.at != parser.bytes.len() {
        return Err(parser.error("unexpected text after the JSON value"));
    }
    Ok(value)
}

/// Nesting deeper than this is refused rather than recursed into.
const MAX_DEPTH: usize = 128;

struct Parser<'a> {
    text: &'a str,
    bytes: &'a [u8],
    at: usize,
    depth: usize,
}

impl Parser<'_> {
    fn error(&self, what: &str) -> String {
        let before = &self.text[..self.at.min(self.text.len())];
        let line = before.matches('\n').count() + 1;
        let column = before.rsplit('\n').next().map_or(0, |l| l.chars().count()) + 1;
        format!("not JSON: {what} at line {line}, column {column}")
    }

    fn skip_ws(&mut self) {
        while self.at < self.bytes.len()
            && matches!(self.bytes[self.at], b' ' | b'\n' | b'\r' | b'\t')
        {
            self.at += 1;
        }
    }

    fn eat(&mut self, byte: u8) -> bool {
        if self.bytes.get(self.at) == Some(&byte) {
            self.at += 1;
            true
        } else {
            false
        }
    }

    fn value(&mut self) -> Result<Json, String> {
        match self.bytes.get(self.at) {
            None => Err(self.error("unexpected end")),
            Some(b'{') => self.nested(|p| p.object()),
            Some(b'[') => self.nested(|p| p.array()),
            Some(b'"') => Ok(Json::String(self.string()?)),
            Some(b't') => self.word("true", Json::Bool(true)),
            Some(b'f') => self.word("false", Json::Bool(false)),
            Some(b'n') => self.word("null", Json::Null),
            Some(b'-' | b'0'..=b'9') => self.number(),
            Some(_) => Err(self.error("unexpected character")),
        }
    }

    fn nested(
        &mut self,
        f: impl FnOnce(&mut Self) -> Result<Json, String>,
    ) -> Result<Json, String> {
        self.depth += 1;
        if self.depth > MAX_DEPTH {
            return Err(self.error("nesting too deep"));
        }
        let value = f(self);
        self.depth -= 1;
        value
    }

    fn word(&mut self, word: &str, value: Json) -> Result<Json, String> {
        if self.text[self.at..].starts_with(word) {
            self.at += word.len();
            Ok(value)
        } else {
            Err(self.error("unexpected word"))
        }
    }

    fn number(&mut self) -> Result<Json, String> {
        let start = self.at;
        self.eat(b'-');
        let digits = |p: &mut Self| {
            let from = p.at;
            while p.at < p.bytes.len() && p.bytes[p.at].is_ascii_digit() {
                p.at += 1;
            }
            p.at > from
        };
        if !digits(self) {
            return Err(self.error("a number needs digits"));
        }
        if self.eat(b'.') && !digits(self) {
            return Err(self.error("a number needs digits after `.`"));
        }
        if self.eat(b'e') || self.eat(b'E') {
            if !self.eat(b'+') {
                self.eat(b'-');
            }
            if !digits(self) {
                return Err(self.error("an exponent needs digits"));
            }
        }
        Ok(Json::Number(self.text[start..self.at].to_string()))
    }

    fn string(&mut self) -> Result<String, String> {
        self.at += 1;
        let mut out = String::new();
        loop {
            let Some(&byte) = self.bytes.get(self.at) else {
                return Err(self.error("unterminated string"));
            };
            match byte {
                b'"' => {
                    self.at += 1;
                    return Ok(out);
                }
                b'\\' => {
                    self.at += 1;
                    let Some(&escape) = self.bytes.get(self.at) else {
                        return Err(self.error("unterminated escape"));
                    };
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
                            let mut code = self.hex4()?;
                            if (0xD800..0xDC00).contains(&code) {
                                if !(self.eat(b'\\') && self.eat(b'u')) {
                                    return Err(self.error("unpaired surrogate"));
                                }
                                let low = self.hex4()?;
                                code = 0x10000
                                    + ((code - 0xD800) << 10)
                                    + (low.wrapping_sub(0xDC00) & 0x3FF);
                            }
                            out.push(
                                char::from_u32(code).ok_or_else(|| self.error("bad \\u escape"))?,
                            );
                        }
                        _ => return Err(self.error("unknown escape")),
                    }
                }
                0..=0x1f => return Err(self.error("control character in a string")),
                _ => {
                    let rest = &self.text[self.at..];
                    let ch = rest.chars().next().expect("in bounds");
                    out.push(ch);
                    self.at += ch.len_utf8();
                }
            }
        }
    }

    fn hex4(&mut self) -> Result<u32, String> {
        let digits = self
            .text
            .get(self.at..self.at + 4)
            .ok_or_else(|| self.error("short \\u escape"))?;
        let code = u32::from_str_radix(digits, 16).map_err(|_| self.error("bad \\u escape"))?;
        self.at += 4;
        Ok(code)
    }

    fn array(&mut self) -> Result<Json, String> {
        self.at += 1;
        let mut items = Vec::new();
        self.skip_ws();
        if self.eat(b']') {
            return Ok(Json::Array(items));
        }
        loop {
            self.skip_ws();
            items.push(self.value()?);
            self.skip_ws();
            if self.eat(b']') {
                return Ok(Json::Array(items));
            }
            if !self.eat(b',') {
                return Err(self.error("expected `,` or `]`"));
            }
        }
    }

    fn object(&mut self) -> Result<Json, String> {
        self.at += 1;
        let mut entries: Vec<(String, Json)> = Vec::new();
        self.skip_ws();
        if self.eat(b'}') {
            return Ok(Json::Object(entries));
        }
        loop {
            self.skip_ws();
            if self.bytes.get(self.at) != Some(&b'"') {
                return Err(self.error("expected a key"));
            }
            let key = self.string()?;
            if entries.iter().any(|(k, _)| *k == key) {
                return Err(self.error(&format!("key `{key}` repeats")));
            }
            self.skip_ws();
            if !self.eat(b':') {
                return Err(self.error("expected `:`"));
            }
            self.skip_ws();
            let value = self.value()?;
            entries.push((key, value));
            self.skip_ws();
            if self.eat(b'}') {
                return Ok(Json::Object(entries));
            }
            if !self.eat(b',') {
                return Err(self.error("expected `,` or `}`"));
            }
        }
    }
}

/// Width a value may take on one line before it is expanded.
const WIDTH: usize = 100;

/// The canonical text of `value`: two-space indent, a value on one line when
/// that line fits in 100 columns, a trailing newline.
pub fn print(value: &Json) -> String {
    let mut out = String::new();
    write_value(&mut out, value, 0, 0);
    out.push('\n');
    out
}

fn inline(value: &Json) -> String {
    match value {
        Json::Null => "null".into(),
        Json::Bool(b) => b.to_string(),
        Json::Number(n) => n.clone(),
        Json::String(s) => quote(s),
        Json::Array(items) => {
            let inner: Vec<_> = items.iter().map(inline).collect();
            format!("[{}]", inner.join(", "))
        }
        Json::Object(entries) if entries.is_empty() => "{}".into(),
        Json::Object(entries) => {
            let inner: Vec<_> = entries
                .iter()
                .map(|(k, v)| format!("{}: {}", quote(k), inline(v)))
                .collect();
            format!("{{ {} }}", inner.join(", "))
        }
    }
}

/// `prefix` is how many columns precede the value on its line.
fn write_value(out: &mut String, value: &Json, indent: usize, prefix: usize) {
    let flat = inline(value);
    let fits = prefix + flat.chars().count() <= WIDTH && !flat.contains('\n');
    match value {
        Json::Array(items) if !fits && !items.is_empty() => {
            out.push_str("[\n");
            for (i, item) in items.iter().enumerate() {
                pad(out, indent + 2);
                write_value(out, item, indent + 2, indent + 2);
                if i + 1 < items.len() {
                    out.push(',');
                }
                out.push('\n');
            }
            pad(out, indent);
            out.push(']');
        }
        Json::Object(entries) if !fits && !entries.is_empty() => {
            out.push_str("{\n");
            for (i, (key, item)) in entries.iter().enumerate() {
                pad(out, indent + 2);
                let head = format!("{}: ", quote(key));
                out.push_str(&head);
                write_value(out, item, indent + 2, indent + 2 + head.chars().count());
                if i + 1 < entries.len() {
                    out.push(',');
                }
                out.push('\n');
            }
            pad(out, indent);
            out.push('}');
        }
        _ => out.push_str(&flat),
    }
}

fn pad(out: &mut String, indent: usize) {
    out.extend(std::iter::repeat_n(' ', indent));
}

fn quote(text: &str) -> String {
    let mut out = String::with_capacity(text.len() + 2);
    out.push('"');
    for ch in text.chars() {
        match ch {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if (c as u32) < 0x20 => {
                let _ = write!(out, "\\u{:04x}", c as u32);
            }
            c => out.push(c),
        }
    }
    out.push('"');
    out
}
