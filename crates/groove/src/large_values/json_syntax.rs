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
        unicode_value: u16,
        high_surrogate: bool,
    },
    Literal {
        expected: &'static [u8],
        offset: usize,
    },
    Number(Number),
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

// The default serde_json::Value parser starts with a recursion budget of 128
// and rejects the container that decrements it to zero. Keep its accepted
// domain separate from our validator's normative memory ceiling.
const SERDE_JSON_MAX_CONTAINERS: usize = 127;

/// JSON validation retaining only bounded numeric, escape and nesting state,
/// never token contents. UTF-8 validation belongs to the surrounding reader.
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
                unicode_value,
                high_surrogate,
            } => {
                if *unicode_digits > 0 {
                    let digit = match byte {
                        b'0'..=b'9' => byte - b'0',
                        b'a'..=b'f' => byte - b'a' + 10,
                        b'A'..=b'F' => byte - b'A' + 10,
                        _ => return Err(()),
                    };
                    *unicode_value = (*unicode_value << 4) | u16::from(digit);
                    *unicode_digits -= 1;
                    if *unicode_digits == 0 {
                        if *high_surrogate {
                            if !(0xdc00..=0xdfff).contains(unicode_value) {
                                return Err(());
                            }
                            *high_surrogate = false;
                        } else {
                            if (0xdc00..=0xdfff).contains(unicode_value) {
                                return Err(());
                            }
                            *high_surrogate = (0xd800..=0xdbff).contains(unicode_value);
                        }
                    }
                    return Ok(());
                }
                if *escaped {
                    *escaped = false;
                    if byte == b'u' {
                        *unicode_digits = 4;
                        *unicode_value = 0;
                    } else if *high_surrogate
                        || !matches!(byte, b'"' | b'\\' | b'/' | b'b' | b'f' | b'n' | b'r' | b't')
                    {
                        return Err(());
                    }
                    return Ok(());
                }
                if *high_surrogate && byte != b'\\' {
                    return Err(());
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
            Token::Number(number) => {
                if number.advance(byte)? {
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
        if matches!(byte, b' ' | b'\t' | b'\n' | b'\r') {
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
                        unicode_value: 0,
                        high_surrogate: false,
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
                if self.stack.len() >= super::MAX_JSON_NESTING_DEPTH.min(SERDE_JSON_MAX_CONTAINERS)
                {
                    return Err(());
                }
                self.stack.push(Frame::Object(ObjectState::FirstKeyOrEnd));
                Ok(())
            }
            b'[' => {
                if self.stack.len() >= super::MAX_JSON_NESTING_DEPTH.min(SERDE_JSON_MAX_CONTAINERS)
                {
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
                    unicode_value: 0,
                    high_surrogate: false,
                };
                Ok(())
            }
            b't' => self.start_literal(b"rue"),
            b'f' => self.start_literal(b"alse"),
            b'n' => self.start_literal(b"ull"),
            b'-' | b'0'..=b'9' => {
                self.token = Token::Number(Number::new(byte));
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
        match &self.token {
            Token::Number(number) => {
                number.finish()?;
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

/// The default serde_json parser accumulates a u64, then discards overflowing
/// integer digits (counting their decimal places) and overflowing fraction
/// digits (without counting them). It does not use roundtrip parsing's sticky
/// digits. Keep that reduction here, but let serde_json perform the final
/// binary rounding so its power-of-ten table is not duplicated.
struct Number {
    state: NumberState,
    significand: u64,
    decimal_exponent: i32,
    integer_overflow: bool,
    fraction_overflow: bool,
    exponent: i32,
    exponent_negative: bool,
    exponent_overflow: bool,
}

impl Number {
    fn new(byte: u8) -> Self {
        Self {
            state: match byte {
                b'-' => NumberState::Minus,
                b'0' => NumberState::Zero,
                _ => NumberState::Integer,
            },
            significand: if byte == b'-' {
                0
            } else {
                u64::from(byte - b'0')
            },
            decimal_exponent: 0,
            integer_overflow: false,
            fraction_overflow: false,
            exponent: 0,
            exponent_negative: false,
            exponent_overflow: false,
        }
    }

    fn advance(&mut self, byte: u8) -> Result<bool, ()> {
        if !advance_number(&mut self.state, byte)? {
            return Ok(false);
        }
        match self.state {
            NumberState::Zero | NumberState::Integer => {
                if !self.integer_overflow {
                    if let Some(next) = self.append_digit(byte) {
                        self.significand = next;
                    } else {
                        self.integer_overflow = true;
                    }
                }
                if self.integer_overflow {
                    // Reject before serde_json's unchecked long-mantissa
                    // counter would overflow (panic or wrap on later reads).
                    self.decimal_exponent = self.decimal_exponent.checked_add(1).ok_or(())?;
                }
            }
            NumberState::Fraction => {
                if !self.fraction_overflow {
                    if let Some(next) = self.append_digit(byte) {
                        self.significand = next;
                        self.decimal_exponent = self.decimal_exponent.checked_sub(1).ok_or(())?;
                    } else {
                        self.fraction_overflow = true;
                    }
                }
            }
            NumberState::ExponentSign => self.exponent_negative = byte == b'-',
            NumberState::ExponentDigits => {
                if !self.exponent_overflow {
                    if let Some(next) = self
                        .exponent
                        .checked_mul(10)
                        .and_then(|value| value.checked_add(i32::from(byte - b'0')))
                    {
                        self.exponent = next;
                    } else {
                        self.exponent_overflow = true;
                    }
                }
            }
            _ => {}
        }
        Ok(true)
    }

    fn append_digit(&self, byte: u8) -> Option<u64> {
        self.significand
            .checked_mul(10)?
            .checked_add(u64::from(byte - b'0'))
    }

    fn finish(&self) -> Result<(), ()> {
        if !matches!(
            self.state,
            NumberState::Zero
                | NumberState::Integer
                | NumberState::Fraction
                | NumberState::ExponentDigits
        ) {
            return Err(());
        }
        if self.exponent_overflow {
            // serde_json handles exponent overflow before combining it with
            // the mantissa's decimal places.
            return if self.exponent_negative || self.significand == 0 {
                Ok(())
            } else {
                Err(())
            };
        }
        let exponent = if self.exponent_negative {
            self.decimal_exponent.saturating_sub(self.exponent)
        } else {
            self.decimal_exponent.saturating_add(self.exponent)
        };
        if exponent <= 0 || self.significand == 0 {
            // A u64 divided by a positive power of ten is always finite.
            return Ok(());
        }

        // At most 20 significand digits, 'e', and 10 positive exponent digits.
        // This is reduced parser metadata, not retained source-token scratch.
        use std::io::Write;
        let mut bytes = [0_u8; 31];
        let mut output = std::io::Cursor::new(bytes.as_mut_slice());
        write!(output, "{}e{}", self.significand, exponent).map_err(|_| ())?;
        let length = output.position() as usize;
        serde_json::from_slice::<f64>(&bytes[..length])
            .map(|_| ())
            .map_err(|_| ())
    }
}

#[cfg(test)]
mod tests {
    use super::StreamingJsonValidator;

    // The public equivalent needs over 2 GiB of one numeric token. Seed only
    // its counter here to defend the fail-closed arithmetic transition.
    #[test]
    fn numeric_mantissa_counter_limit_fails_closed() {
        let mut integer = super::Number::new(b'1');
        integer.integer_overflow = true;
        integer.decimal_exponent = i32::MAX - 1;
        assert_eq!(integer.advance(b'0'), Ok(true));
        assert_eq!(integer.advance(b'0'), Err(()));

        let mut fraction = super::Number::new(b'0');
        fraction.advance(b'.').unwrap();
        fraction.decimal_exponent = i32::MIN + 1;
        assert_eq!(fraction.advance(b'0'), Ok(true));
        assert_eq!(fraction.advance(b'0'), Err(()));
    }

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
    fn accepts_json_at_serde_nesting_bound() {
        let mut json = vec![b'['; 127];
        json.push(b'0');
        json.extend(std::iter::repeat_n(b']', 127));
        assert!(validate_one_byte_at_a_time(&json).is_ok());
    }

    #[test]
    fn rejects_json_over_serde_nesting_bound() {
        let mut validator = StreamingJsonValidator::new();
        validator.push(&vec![b'['; 127]).unwrap();
        assert!(validator.push(b"[").is_err());
    }
}
