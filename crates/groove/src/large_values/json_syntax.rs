#[derive(Clone, Copy)]
enum Frame {
    Array(ArrayState),
    Object(ObjectState),
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ArrayState {
    FirstValueOrEnd,
    Value,
    CommaOrEnd,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum ObjectState {
    FirstKeyOrEnd,
    Key,
    Colon,
    Value,
    CommaOrEnd,
}

enum Token {
    None,
    String {
        key: bool,
        escaped: bool,
        unicode_digits: u8,
    },
    Literal {
        expected: &'static [u8],
        offset: usize,
    },
    Number(NumberState),
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum NumberState {
    Minus,
    Zero,
    Integer,
    Dot,
    Fraction,
    Exponent,
    ExponentSign,
    ExponentDigits,
}

/// Syntax-only JSON validation that retains nesting state but never token
/// contents, so one huge JSON string remains bounded by tree chunking.
pub(super) struct StreamingJsonValidator {
    stack: Vec<Frame>,
    token: Token,
    root_done: bool,
}

impl StreamingJsonValidator {
    pub(super) fn new() -> Self {
        Self {
            stack: Vec::new(),
            token: Token::None,
            root_done: false,
        }
    }

    pub(super) fn push(&mut self, bytes: &[u8]) -> Result<(), ()> {
        for &byte in bytes {
            self.consume(byte)?;
        }
        Ok(())
    }

    pub(super) fn finish(mut self) -> Result<(), ()> {
        if matches!(self.token, Token::Number(_)) {
            self.finish_number()?;
        }
        if !matches!(self.token, Token::None) || !self.stack.is_empty() || !self.root_done {
            return Err(());
        }
        Ok(())
    }

    fn consume(&mut self, byte: u8) -> Result<(), ()> {
        match &mut self.token {
            Token::String {
                key,
                escaped,
                unicode_digits,
            } => {
                if *unicode_digits > 0 {
                    if !byte.is_ascii_hexdigit() {
                        return Err(());
                    }
                    *unicode_digits -= 1;
                    return Ok(());
                }
                if *escaped {
                    *escaped = false;
                    if byte == b'u' {
                        *unicode_digits = 4;
                    } else if !matches!(
                        byte,
                        b'"' | b'\\' | b'/' | b'b' | b'f' | b'n' | b'r' | b't'
                    ) {
                        return Err(());
                    }
                    return Ok(());
                }
                match byte {
                    b'\\' => *escaped = true,
                    b'"' => {
                        let key = *key;
                        self.token = Token::None;
                        if key {
                            match self.stack.last_mut() {
                                Some(Frame::Object(
                                    state @ (ObjectState::FirstKeyOrEnd | ObjectState::Key),
                                )) => {
                                    *state = ObjectState::Colon;
                                }
                                _ => return Err(()),
                            }
                        } else {
                            self.value_complete()?;
                        }
                    }
                    0x00..=0x1f => return Err(()),
                    _ => {}
                }
                Ok(())
            }
            Token::Literal { expected, offset } => {
                if expected.get(*offset) != Some(&byte) {
                    return Err(());
                }
                *offset += 1;
                if *offset == expected.len() {
                    self.token = Token::None;
                    self.value_complete()?;
                }
                Ok(())
            }
            Token::Number(state) => {
                if advance_number(state, byte)? {
                    Ok(())
                } else {
                    self.finish_number()?;
                    self.consume_idle(byte)
                }
            }
            Token::None => self.consume_idle(byte),
        }
    }

    fn consume_idle(&mut self, byte: u8) -> Result<(), ()> {
        if byte.is_ascii_whitespace() {
            return Ok(());
        }
        match self.stack.last().copied() {
            Some(Frame::Array(ArrayState::CommaOrEnd)) => match byte {
                b',' => self.set_array_state(ArrayState::Value),
                b']' => self.close_array(),
                _ => Err(()),
            },
            Some(Frame::Object(ObjectState::CommaOrEnd)) => match byte {
                b',' => self.set_object_state(ObjectState::Key),
                b'}' => self.close_object(),
                _ => Err(()),
            },
            Some(Frame::Object(ObjectState::Colon)) => {
                if byte != b':' {
                    return Err(());
                }
                self.set_object_state(ObjectState::Value)
            }
            Some(Frame::Object(ObjectState::FirstKeyOrEnd | ObjectState::Key)) => {
                if byte == b'}'
                    && matches!(
                        self.stack.last(),
                        Some(Frame::Object(ObjectState::FirstKeyOrEnd))
                    )
                {
                    self.close_object()
                } else if byte == b'"' {
                    self.token = Token::String {
                        key: true,
                        escaped: false,
                        unicode_digits: 0,
                    };
                    Ok(())
                } else {
                    Err(())
                }
            }
            Some(Frame::Array(ArrayState::FirstValueOrEnd)) if byte == b']' => self.close_array(),
            Some(Frame::Array(ArrayState::FirstValueOrEnd | ArrayState::Value))
            | Some(Frame::Object(ObjectState::Value))
            | None
                if !self.root_done =>
            {
                self.start_value(byte)
            }
            _ => Err(()),
        }
    }

    fn start_value(&mut self, byte: u8) -> Result<(), ()> {
        match byte {
            b'{' => {
                if self.stack.len() >= super::MAX_JSON_NESTING_DEPTH {
                    return Err(());
                }
                self.stack.push(Frame::Object(ObjectState::FirstKeyOrEnd));
                Ok(())
            }
            b'[' => {
                if self.stack.len() >= super::MAX_JSON_NESTING_DEPTH {
                    return Err(());
                }
                self.stack.push(Frame::Array(ArrayState::FirstValueOrEnd));
                Ok(())
            }
            b'"' => {
                self.token = Token::String {
                    key: false,
                    escaped: false,
                    unicode_digits: 0,
                };
                Ok(())
            }
            b't' => self.start_literal(b"rue"),
            b'f' => self.start_literal(b"alse"),
            b'n' => self.start_literal(b"ull"),
            b'-' => {
                self.token = Token::Number(NumberState::Minus);
                Ok(())
            }
            b'0' => {
                self.token = Token::Number(NumberState::Zero);
                Ok(())
            }
            b'1'..=b'9' => {
                self.token = Token::Number(NumberState::Integer);
                Ok(())
            }
            _ => Err(()),
        }
    }

    fn start_literal(&mut self, expected: &'static [u8]) -> Result<(), ()> {
        self.token = Token::Literal {
            expected,
            offset: 0,
        };
        Ok(())
    }

    fn finish_number(&mut self) -> Result<(), ()> {
        match self.token {
            Token::Number(
                NumberState::Zero
                | NumberState::Integer
                | NumberState::Fraction
                | NumberState::ExponentDigits,
            ) => {
                self.token = Token::None;
                self.value_complete()
            }
            _ => Err(()),
        }
    }

    fn value_complete(&mut self) -> Result<(), ()> {
        match self.stack.last_mut() {
            Some(Frame::Array(state @ (ArrayState::FirstValueOrEnd | ArrayState::Value))) => {
                *state = ArrayState::CommaOrEnd;
            }
            Some(Frame::Object(state @ ObjectState::Value)) => *state = ObjectState::CommaOrEnd,
            None if !self.root_done => self.root_done = true,
            _ => return Err(()),
        }
        Ok(())
    }

    fn close_array(&mut self) -> Result<(), ()> {
        match self.stack.pop() {
            Some(Frame::Array(_)) => self.value_complete(),
            _ => Err(()),
        }
    }

    fn close_object(&mut self) -> Result<(), ()> {
        match self.stack.pop() {
            Some(Frame::Object(_)) => self.value_complete(),
            _ => Err(()),
        }
    }

    fn set_array_state(&mut self, next: ArrayState) -> Result<(), ()> {
        match self.stack.last_mut() {
            Some(Frame::Array(state)) => {
                *state = next;
                Ok(())
            }
            _ => Err(()),
        }
    }

    fn set_object_state(&mut self, next: ObjectState) -> Result<(), ()> {
        match self.stack.last_mut() {
            Some(Frame::Object(state)) => {
                *state = next;
                Ok(())
            }
            _ => Err(()),
        }
    }
}

fn advance_number(state: &mut NumberState, byte: u8) -> Result<bool, ()> {
    use NumberState::*;
    *state = match (*state, byte) {
        (Minus, b'0') => Zero,
        (Minus, b'1'..=b'9') => Integer,
        (Zero | Integer, b'.') => Dot,
        (Integer, b'0'..=b'9') => Integer,
        (Dot | Fraction, b'0'..=b'9') => Fraction,
        (Zero | Integer | Fraction, b'e' | b'E') => Exponent,
        (Exponent, b'+' | b'-') => ExponentSign,
        (Exponent | ExponentSign | ExponentDigits, b'0'..=b'9') => ExponentDigits,
        (Zero | Integer | Fraction | ExponentDigits, _) => return Ok(false),
        _ => return Err(()),
    };
    Ok(true)
}

#[cfg(test)]
mod tests {
    use super::StreamingJsonValidator;
    use crate::large_values::MAX_JSON_NESTING_DEPTH;

    fn validate_one_byte_at_a_time(json: &[u8]) -> Result<(), ()> {
        let mut validator = StreamingJsonValidator::new();
        for byte in json {
            validator.push(std::slice::from_ref(byte))?;
        }
        validator.finish()
    }

    #[test]
    fn accepts_fragmented_json_grammar() {
        for json in [
            br#"null"#.as_slice(),
            br#" true "#,
            br#"-0"#,
            br#"0.125"#,
            br#"-12.5e+7"#,
            br#""escaped\nstring\u263a""#,
            br#"[]"#,
            br#"{}"#,
            br#"{"a":[1,false,null,{"b":"c"}]}"#,
        ] {
            assert!(validate_one_byte_at_a_time(json).is_ok(), "{json:?}");
        }
    }

    #[test]
    fn rejects_fragmented_invalid_json_grammar() {
        for json in [
            b"".as_slice(),
            b"01",
            b"1.",
            b"1e",
            b"[1,]",
            b"{\"a\":1,}",
            b"{\"a\" 1}",
            b"true false",
            b"\"bad\\xescape\"",
            b"\"unterminated",
            b"[",
        ] {
            assert!(validate_one_byte_at_a_time(json).is_err(), "{json:?}");
        }
    }

    #[test]
    fn accepts_json_at_the_explicit_nesting_bound() {
        let mut json = vec![b'['; MAX_JSON_NESTING_DEPTH];
        json.push(b'0');
        json.extend(std::iter::repeat_n(b']', MAX_JSON_NESTING_DEPTH));
        assert!(validate_one_byte_at_a_time(&json).is_ok());
    }

    #[test]
    fn rejects_json_over_the_explicit_nesting_bound_without_retaining_frames() {
        let mut validator = StreamingJsonValidator::new();
        validator.push(&vec![b'['; MAX_JSON_NESTING_DEPTH]).unwrap();
        assert!(validator.push(b"[").is_err());
    }
}
