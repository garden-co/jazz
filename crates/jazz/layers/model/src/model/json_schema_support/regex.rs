//! Which `pattern`s the browser runtime can evaluate.
//!
//! A port of the parser in
//! `packages/jazz-tools/src/runtime/json-schema-regex.ts`, keeping only what
//! decides whether it refuses a pattern: the constructs it records as
//! unsupported, the syntax `fancy-regex` reads differently from
//! `regex-syntax`, and the escapes and Unicode classes its JS translation
//! cannot express. It builds no syntax tree. Keep the two in step: the
//! parity fixture (`tests/fixtures/json_schema_parity.json`) runs both.

use std::collections::{HashMap, HashSet};
use std::sync::OnceLock;

/// Why the browser runtime refuses `pattern`, which the native runtime
/// accepts; `None` if it evaluates it.
pub(super) fn browser_refusal(pattern: &str) -> Option<String> {
    let mut parser = Parser::new(pattern);
    parser.parse().err()
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Item {
    /// `(?flags)`, which nothing can repeat.
    Flags,
    /// A lookaround.
    Look,
    Other,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Escape {
    Literal(char),
    Other,
}

const META_CHARACTERS: &str = "\\.+*?()|[]{}^$#&-~";
const FLAG_CHARACTERS: &str = "imsUuRx";
const ASCII_KINDS: [&str; 14] = [
    "alnum", "alpha", "ascii", "blank", "cntrl", "digit", "graph", "lower", "print", "punct",
    "space", "upper", "word", "xdigit",
];

/// `regex_syntax::is_escapeable_character`.
fn is_escapeable(char: char) -> bool {
    if META_CHARACTERS.contains(char) {
        return true;
    }
    char.is_ascii() && !(char.is_ascii_alphanumeric() || char == '<' || char == '>')
}

type Parse<T> = Result<T, String>;

struct Parser {
    chars: Vec<char>,
    index: usize,
    /// The native runtime compiles the pattern untranslated (lookaround,
    /// backreference).
    fancy: bool,
    captures: u64,
    names: HashSet<String>,
    unsupported: Vec<String>,
    fancy_invalid: Vec<String>,
    bell_outside_class: bool,
    backreferences: Vec<u64>,
    unicode_classes: Vec<String>,
}

impl Parser {
    fn new(pattern: &str) -> Self {
        Self {
            chars: pattern.chars().collect(),
            index: 0,
            fancy: false,
            captures: 0,
            names: HashSet::new(),
            unsupported: Vec::new(),
            fancy_invalid: Vec::new(),
            bell_outside_class: false,
            backreferences: Vec::new(),
            unicode_classes: Vec::new(),
        }
    }

    /// The browser's verdict, in the order it reaches it: parse errors, then
    /// unsupported constructs, then what its translation cannot express.
    fn parse(&mut self) -> Parse<()> {
        self.parse_alternation()?;
        if !self.eof() {
            return Err("unopened group".to_owned());
        }
        if !self.fancy && self.bell_outside_class {
            return Err("`\\a` is not supported".to_owned());
        }
        if let Some(reason) = self.fancy_invalid.first() {
            return Err(reason.clone());
        }
        if let Some(reason) = self.unsupported.first() {
            return Err(reason.clone());
        }
        if let Some(index) = self
            .backreferences
            .iter()
            .find(|&&index| index == 0 || index > self.captures)
        {
            return Err(format!("backreference `\\{index}`"));
        }
        if let Some(name) = self
            .unicode_classes
            .iter()
            .find(|name| !js_knows_unicode_class(name))
        {
            return Err(format!("the Unicode class `{name}`"));
        }
        Ok(())
    }

    fn eof(&self) -> bool {
        self.index >= self.chars.len()
    }

    fn char(&self) -> char {
        self.chars[self.index]
    }

    fn peek(&self) -> Option<char> {
        self.chars.get(self.index + 1).copied()
    }

    fn starts_with(&self, text: &str) -> bool {
        for (position, expected) in (self.index..).zip(text.chars()) {
            if self.chars.get(position) != Some(&expected) {
                return false;
            }
        }
        true
    }

    fn parse_alternation(&mut self) -> Parse<()> {
        self.parse_concat()?;
        while !self.eof() && self.char() == '|' {
            self.index += 1;
            self.parse_concat()?;
        }
        Ok(())
    }

    fn parse_concat(&mut self) -> Parse<()> {
        let mut items = Vec::new();
        // Quantifiers in a row: `regex-syntax` nests them, but `fancy-regex`
        // (which parses the pattern again) reads a `+` after a quantifier as
        // possessive, a `{` as a literal, and rejects anything else.
        let mut quantifiers = 0;
        while !self.eof() {
            let char = self.char();
            if char == '|' || char == ')' {
                break;
            }
            if matches!(char, '?' | '*' | '+' | '{') {
                quantifiers += 1;
                if quantifiers == 2 && char == '+' {
                    self.unsupported.push("possessive quantifiers".to_owned());
                } else if quantifiers == 2 && char == '{' {
                    self.unsupported
                        .push("a counted repetition right after another quantifier".to_owned());
                } else if quantifiers >= 2 {
                    self.fancy_invalid
                        .push("a quantifier right after another quantifier".to_owned());
                }
            } else {
                quantifiers = 0;
            }
            match char {
                '(' => items.push(self.parse_group()?),
                '[' => {
                    self.parse_bracketed()?;
                    items.push(Item::Other);
                }
                '?' | '*' | '+' => {
                    self.index += 1;
                    if !self.eof() && self.char() == '?' {
                        self.index += 1;
                    }
                    self.repeat_last(&mut items)?;
                }
                '{' => self.parse_counted_repetition(&mut items)?,
                _ => {
                    self.parse_primitive()?;
                    items.push(Item::Other);
                }
            }
        }
        Ok(())
    }

    fn repeat_last(&mut self, items: &mut Vec<Item>) -> Parse<()> {
        match items.pop() {
            None | Some(Item::Flags) => {
                return Err("repetition operator missing expression".to_owned());
            }
            Some(Item::Look) => self
                .fancy_invalid
                .push("a quantifier on a lookaround".to_owned()),
            Some(Item::Other) => {}
        }
        items.push(Item::Other);
        Ok(())
    }

    fn skip_whitespace_in_repetition(&mut self) {
        let mut skipped = false;
        while !self.eof() && self.char().is_whitespace() {
            self.index += 1;
            skipped = true;
        }
        // `fancy-regex` reads `{` as a literal when a count has spaces.
        if skipped {
            self.unsupported
                .push("spaces inside a counted repetition".to_owned());
        }
    }

    fn parse_decimal(&mut self) -> Parse<Option<u64>> {
        self.skip_whitespace_in_repetition();
        let mut digits = String::new();
        while !self.eof() && self.char().is_ascii_digit() {
            digits.push(self.char());
            self.index += 1;
        }
        self.skip_whitespace_in_repetition();
        if digits.is_empty() {
            return Ok(None);
        }
        match digits.parse::<u64>() {
            Ok(value) if value <= 0xffff_ffff => Ok(Some(value)),
            _ => Err("repetition count is too large".to_owned()),
        }
    }

    fn parse_counted_repetition(&mut self, items: &mut Vec<Item>) -> Parse<()> {
        let unclosed = || Err("unclosed counted repetition".to_owned());
        let empty = || "repetition count is empty".to_owned();
        if matches!(items.last(), None | Some(Item::Flags)) {
            return Err("repetition operator missing expression".to_owned());
        }
        self.index += 1;
        if self.eof() {
            return unclosed();
        }
        let min = self.parse_decimal()?;
        if self.eof() {
            return unclosed();
        }
        let (min, max) = if self.char() == ',' {
            self.index += 1;
            if self.eof() {
                return unclosed();
            }
            if self.char() != '}' {
                let end = self.parse_decimal()?;
                match (min, end) {
                    (Some(min), Some(end)) => (min, Some(end)),
                    _ => return Err(empty()),
                }
            } else {
                (min.ok_or_else(empty)?, None)
            }
        } else {
            let min = min.ok_or_else(empty)?;
            (min, Some(min))
        };
        if self.eof() || self.char() != '}' {
            return unclosed();
        }
        self.index += 1;
        if !self.eof() && self.char() == '?' {
            self.index += 1;
        }
        if max.is_some_and(|max| min > max) {
            return Err("invalid repetition range".to_owned());
        }
        self.repeat_last(items)
    }

    fn parse_group(&mut self) -> Parse<Item> {
        let at_start = self.index == 0;
        self.index += 1;
        for op in ["?=", "?!", "?<=", "?<!"] {
            if self.starts_with(op) {
                self.fancy = true;
                self.index += op.len();
                self.parse_group_body()?;
                return Ok(Item::Look);
            }
        }
        if self.starts_with("?P<") || self.starts_with("?<") {
            self.index += if self.starts_with("?P<") { 3 } else { 2 };
            self.captures += 1;
            self.parse_capture_name()?;
            self.parse_group_body()?;
            return Ok(Item::Other);
        }
        if !self.eof() && self.char() == '?' {
            self.index += 1;
            if self.eof() {
                return Err("unclosed group".to_owned());
            }
            let sets_case_insensitive = self.parse_flags()?;
            let end = self.char();
            self.index += 1;
            let misplaced_i = "`(?i)` anywhere but at the start of the pattern".to_owned();
            if end == ')' {
                // `(?flags)` applies to the rest of the enclosing group.
                if sets_case_insensitive.is_some_and(|on| !(on && at_start)) {
                    self.unsupported.push(misplaced_i);
                }
                return Ok(Item::Flags);
            }
            if sets_case_insensitive.is_some() {
                self.unsupported.push(misplaced_i);
            }
            self.parse_group_body()?;
            return Ok(Item::Other);
        }
        self.captures += 1;
        self.parse_group_body()?;
        Ok(Item::Other)
    }

    fn parse_group_body(&mut self) -> Parse<()> {
        self.parse_alternation()?;
        if self.eof() {
            return Err("unclosed group".to_owned());
        }
        self.index += 1;
        Ok(())
    }

    fn parse_capture_name(&mut self) -> Parse<()> {
        let unexpected_end = || Err("unexpected end of group name".to_owned());
        if self.eof() {
            return unexpected_end();
        }
        let mut name = String::new();
        while !self.eof() && self.char() != '>' {
            let char = self.char();
            let valid = if name.is_empty() {
                char == '_' || char.is_alphabetic()
            } else {
                matches!(char, '_' | '.' | '[' | ']') || char.is_alphabetic() || char.is_numeric()
            };
            if !valid {
                return Err("invalid group name".to_owned());
            }
            name.push(char);
            self.index += 1;
        }
        if self.eof() {
            return unexpected_end();
        }
        self.index += 1;
        if name.is_empty() {
            return Err("empty group name".to_owned());
        }
        if !self.names.insert(name.clone()) {
            return Err(format!("duplicate group name \"{name}\""));
        }
        Ok(())
    }

    /// Parse `imsUuRx` flags up to `:` or `)`. Returns whether they set or
    /// clear case-insensitivity, if they touch it.
    fn parse_flags(&mut self) -> Parse<Option<bool>> {
        let mut seen = HashSet::new();
        let mut negated = false;
        let mut last_was_negation = false;
        let mut case_insensitive = None;
        while self.char() != ':' && self.char() != ')' {
            let char = self.char();
            if char == '-' {
                if negated {
                    return Err("repeated negation in flags".to_owned());
                }
                negated = true;
                last_was_negation = true;
            } else {
                last_was_negation = false;
                if !FLAG_CHARACTERS.contains(char) {
                    return Err(format!("unrecognized flag \"{char}\""));
                }
                if !seen.insert(char) {
                    return Err(format!("duplicate flag \"{char}\""));
                }
                let on = !negated;
                match char {
                    'i' => case_insensitive = Some(on),
                    'x' => self.unsupported.push("the `x` flag".to_owned()),
                    'R' => self.unsupported.push("the `R` flag".to_owned()),
                    'u' if !on => self.unsupported.push("turning off the `u` flag".to_owned()),
                    _ => {}
                }
            }
            self.index += 1;
            if self.eof() {
                return Err("unexpected end of flags".to_owned());
            }
        }
        if last_was_negation {
            return Err("dangling flag negation".to_owned());
        }
        if seen.is_empty() && self.char() == ')' {
            return Err("repetition operator missing expression".to_owned());
        }
        Ok(case_insensitive)
    }

    fn parse_primitive(&mut self) -> Parse<()> {
        if self.char() == '\\' {
            self.parse_escape(false)?;
        } else {
            self.index += 1;
        }
        Ok(())
    }

    /// `\` escapes, inside a class or outside one.
    fn parse_escape(&mut self, in_class: bool) -> Parse<Escape> {
        self.index += 1;
        if self.eof() {
            return Err("incomplete escape".to_owned());
        }
        let char = self.char();
        if char.is_ascii_digit() {
            // `regex-syntax` rejects these as backreferences, sending the
            // native runtime down its untranslated path.
            self.fancy = true;
            let mut digits = String::new();
            while !self.eof() && self.char().is_ascii_digit() {
                digits.push(self.char());
                self.index += 1;
            }
            if in_class || digits.starts_with('0') {
                self.unsupported.push(format!("`\\{digits}`"));
            }
            if !in_class {
                self.backreferences.push(digits.parse().unwrap_or(u64::MAX));
            }
            return Ok(Escape::Other);
        }
        if matches!(char, 'x' | 'u' | 'U') {
            return self.parse_hex(char).map(Escape::Literal);
        }
        if char == 'p' || char == 'P' {
            self.parse_unicode_class()?;
            return Ok(Escape::Other);
        }
        if "dswDSW".contains(char) {
            self.index += 1;
            return Ok(Escape::Other);
        }
        self.index += 1;
        if is_escapeable(char) {
            return Ok(Escape::Literal(char));
        }
        let special = match char {
            'f' => Some('\x0c'),
            't' => Some('\t'),
            'n' => Some('\n'),
            'r' => Some('\r'),
            'v' => Some('\x0b'),
            _ => None,
        };
        if let Some(special) = special {
            return Ok(Escape::Literal(special));
        }
        if char == 'a' {
            // The native translator rejects `\a` outside character classes.
            if !in_class {
                self.bell_outside_class = true;
            }
            return Ok(Escape::Literal('\x07'));
        }
        if char == 'c' {
            // Rewritten to the control character before parsing, as natively.
            let letter = (!self.eof()).then(|| self.char());
            let Some(letter) = letter.filter(char::is_ascii_alphabetic) else {
                return Err("invalid `\\c` escape".to_owned());
            };
            self.index += 1;
            return Ok(Escape::Literal(char::from(letter as u8 % 32)));
        }
        if in_class && "AzbB<>".contains(char) {
            return Err(format!("`\\{char}` is not allowed in a class"));
        }
        match char {
            'A' | 'z' | 'B' | '<' | '>' => Ok(Escape::Other),
            'b' => self.parse_special_word_boundary().map(|()| Escape::Other),
            _ => Err(format!("unrecognized escape `\\{char}`")),
        }
    }

    fn parse_special_word_boundary(&mut self) -> Parse<()> {
        if self.eof() || self.char() != '{' {
            return Ok(());
        }
        let start = self.index;
        self.index += 1;
        if self.eof() {
            return Err("unexpected end of special word boundary".to_owned());
        }
        let is_name_char = |char: char| char.is_ascii_alphabetic() || char == '-';
        if !is_name_char(self.char()) {
            // Not a special word boundary: `{` starts a counted repetition.
            self.index = start;
            return Ok(());
        }
        let mut name = String::new();
        while !self.eof() && is_name_char(self.char()) {
            name.push(self.char());
            self.index += 1;
        }
        if self.eof() || self.char() != '}' {
            return Err("unclosed special word boundary".to_owned());
        }
        self.index += 1;
        if ["start", "end", "start-half", "end-half"].contains(&name.as_str()) {
            Ok(())
        } else {
            Err(format!("unrecognized special word boundary \"{name}\""))
        }
    }

    fn parse_hex(&mut self, kind: char) -> Parse<char> {
        let incomplete = || Err("incomplete hex escape".to_owned());
        let invalid_digit = || Err("invalid hex digit".to_owned());
        self.index += 1;
        if self.eof() {
            return incomplete();
        }
        let mut digits = String::new();
        if self.char() == '{' {
            self.index += 1;
            while !self.eof() && self.char() != '}' {
                if !self.char().is_ascii_hexdigit() {
                    return invalid_digit();
                }
                digits.push(self.char());
                self.index += 1;
            }
            if self.eof() {
                return incomplete();
            }
            self.index += 1;
            if digits.is_empty() {
                return Err("empty hex escape".to_owned());
            }
        } else {
            let length = match kind {
                'x' => 2,
                'u' => 4,
                _ => 8,
            };
            for _ in 0..length {
                if self.eof() {
                    return incomplete();
                }
                if !self.char().is_ascii_hexdigit() {
                    return invalid_digit();
                }
                digits.push(self.char());
                self.index += 1;
            }
        }
        (digits.len() <= 8)
            .then(|| u32::from_str_radix(&digits, 16).ok())
            .flatten()
            .and_then(char::from_u32)
            .ok_or_else(|| "invalid Unicode scalar value".to_owned())
    }

    fn parse_unicode_class(&mut self) -> Parse<()> {
        self.index += 1;
        if self.eof() {
            return Err("incomplete Unicode class".to_owned());
        }
        if self.char() == '{' {
            self.index += 1;
            let mut body = String::new();
            while !self.eof() && self.char() != '}' {
                body.push(self.char());
                self.index += 1;
            }
            if self.eof() {
                return Err("incomplete Unicode class".to_owned());
            }
            self.index += 1;
            let name = match body.split_once("!=") {
                Some((property, value)) => format!("{property}={value}"),
                None => body.replacen(':', "=", 1),
            };
            self.unicode_classes.push(name);
            return Ok(());
        }
        let letter = self.char();
        if letter == '\\' {
            return Err("invalid Unicode class".to_owned());
        }
        self.index += 1;
        self.unicode_classes.push(letter.to_string());
        Ok(())
    }

    fn parse_bracketed(&mut self) -> Parse<()> {
        let unclosed = || Err("unclosed class".to_owned());
        self.index += 1;
        if self.eof() {
            return unclosed();
        }
        if self.char() == '^' {
            self.index += 1;
            if self.eof() {
                return unclosed();
            }
        }
        let mut leading_dashes = false;
        while self.char() == '-' {
            leading_dashes = true;
            self.index += 1;
            if self.eof() {
                return unclosed();
            }
        }
        if !leading_dashes && self.char() == ']' {
            self.index += 1;
            if self.eof() {
                return unclosed();
            }
        }
        loop {
            if self.eof() {
                return unclosed();
            }
            let char = self.char();
            if char == '[' {
                if !self.maybe_parse_ascii_class() {
                    self.parse_bracketed()?;
                }
            } else if char == ']' {
                self.index += 1;
                return Ok(());
            } else if matches!(char, '&' | '-' | '~') && self.peek() == Some(char) {
                self.index += 2;
            } else {
                self.parse_class_range()?;
            }
        }
    }

    /// `[:name:]` or `[:^name:]` for an ASCII class name.
    fn maybe_parse_ascii_class(&mut self) -> bool {
        if self.chars.get(self.index + 1) != Some(&':') {
            return false;
        }
        let mut position = self.index + 2;
        if self.chars.get(position) == Some(&'^') {
            position += 1;
        }
        let mut name = String::new();
        while let Some(&char) = self.chars.get(position) {
            if char == ':' {
                break;
            }
            name.push(char);
            position += 1;
        }
        if self.chars.get(position) != Some(&':')
            || self.chars.get(position + 1) != Some(&']')
            || !ASCII_KINDS.contains(&name.as_str())
        {
            return false;
        }
        self.index = position + 2;
        true
    }

    fn parse_class_item(&mut self) -> Parse<Escape> {
        if self.char() == '\\' {
            return self.parse_escape(true);
        }
        let char = self.char();
        self.index += 1;
        Ok(Escape::Literal(char))
    }

    fn parse_class_range(&mut self) -> Parse<()> {
        let first = self.parse_class_item()?;
        if self.eof() {
            return Err("unclosed class".to_owned());
        }
        if self.char() != '-' || matches!(self.peek(), Some(']' | '-')) {
            return Ok(());
        }
        self.index += 1;
        if self.eof() {
            return Err("unclosed class".to_owned());
        }
        let second = self.parse_class_item()?;
        let (Escape::Literal(start), Escape::Literal(end)) = (first, second) else {
            return Err("class ranges need literal endpoints".to_owned());
        };
        if start > end {
            return Err("invalid class range".to_owned());
        }
        Ok(())
    }
}

/// The Unicode property names a browser knows, by kind.
struct JsUnicodeProperties {
    binary: HashSet<String>,
    general_category: HashSet<String>,
    script: HashSet<String>,
}

fn js_unicode_properties() -> &'static JsUnicodeProperties {
    static PROPERTIES: OnceLock<JsUnicodeProperties> = OnceLock::new();
    PROPERTIES.get_or_init(|| {
        let mut table: HashMap<String, serde_json::Value> =
            serde_json::from_str(include_str!("js_unicode_properties.json"))
                .expect("the JS Unicode property table parses");
        let mut names = |kind: &str| -> HashSet<String> {
            serde_json::from_value(table.remove(kind).unwrap_or_default())
                .expect("the JS Unicode property table lists names")
        };
        JsUnicodeProperties {
            binary: names("binary"),
            general_category: names("generalCategory"),
            script: names("script"),
        }
    })
}

/// Whether the browser translates the native class `\p{name}`: as JS
/// `\p{name}`, or as `\p{Script=name}`. JS matches names exactly, where
/// the native runtime ignores case, spaces and underscores.
fn js_knows_unicode_class(name: &str) -> bool {
    let properties = js_unicode_properties();
    let known = |name: &str| match name.split_once('=') {
        Some(("General_Category" | "gc", value)) => properties.general_category.contains(value),
        Some(("Script" | "sc" | "Script_Extensions" | "scx", value)) => {
            properties.script.contains(value)
        }
        Some(_) => false,
        None => properties.binary.contains(name) || properties.general_category.contains(name),
    };
    known(name) || known(&format!("Script={name}"))
}
