use std::fmt;
use std::str::FromStr;

use crate::sql::row::Row;

/// Crockford Base32 alphabet (excludes I, L, O, U to avoid confusion).
const CROCKFORD_ALPHABET: &[u8; 32] = b"0123456789ABCDEFGHJKMNPQRSTVWXYZ";

/// Decode table for Crockford Base32 (maps ASCII byte to 5-bit value, or 0xFF for invalid).
const CROCKFORD_DECODE: [u8; 128] = {
    let mut table = [0xFFu8; 128];
    // Digits
    table[b'0' as usize] = 0;
    table[b'1' as usize] = 1;
    table[b'2' as usize] = 2;
    table[b'3' as usize] = 3;
    table[b'4' as usize] = 4;
    table[b'5' as usize] = 5;
    table[b'6' as usize] = 6;
    table[b'7' as usize] = 7;
    table[b'8' as usize] = 8;
    table[b'9' as usize] = 9;
    // Letters (uppercase)
    table[b'A' as usize] = 10;
    table[b'B' as usize] = 11;
    table[b'C' as usize] = 12;
    table[b'D' as usize] = 13;
    table[b'E' as usize] = 14;
    table[b'F' as usize] = 15;
    table[b'G' as usize] = 16;
    table[b'H' as usize] = 17;
    table[b'J' as usize] = 18; // I is skipped
    table[b'K' as usize] = 19;
    table[b'M' as usize] = 20; // L is skipped
    table[b'N' as usize] = 21;
    table[b'P' as usize] = 22; // O is skipped
    table[b'Q' as usize] = 23;
    table[b'R' as usize] = 24;
    table[b'S' as usize] = 25;
    table[b'T' as usize] = 26;
    table[b'V' as usize] = 27; // U is skipped
    table[b'W' as usize] = 28;
    table[b'X' as usize] = 29;
    table[b'Y' as usize] = 30;
    table[b'Z' as usize] = 31;
    // Letters (lowercase) - map to same values
    table[b'a' as usize] = 10;
    table[b'b' as usize] = 11;
    table[b'c' as usize] = 12;
    table[b'd' as usize] = 13;
    table[b'e' as usize] = 14;
    table[b'f' as usize] = 15;
    table[b'g' as usize] = 16;
    table[b'h' as usize] = 17;
    table[b'j' as usize] = 18;
    table[b'k' as usize] = 19;
    table[b'm' as usize] = 20;
    table[b'n' as usize] = 21;
    table[b'p' as usize] = 22;
    table[b'q' as usize] = 23;
    table[b'r' as usize] = 24;
    table[b's' as usize] = 25;
    table[b't' as usize] = 26;
    table[b'v' as usize] = 27;
    table[b'w' as usize] = 28;
    table[b'x' as usize] = 29;
    table[b'y' as usize] = 30;
    table[b'z' as usize] = 31;
    // Common substitutions
    table[b'I' as usize] = 1; // I -> 1
    table[b'i' as usize] = 1;
    table[b'L' as usize] = 1; // L -> 1
    table[b'l' as usize] = 1;
    table[b'O' as usize] = 0; // O -> 0
    table[b'o' as usize] = 0;
    table
};

/// Object ID - a 128-bit unique identifier.
///
/// Displayed and parsed as Crockford Base32 (26 characters for 128 bits).
#[derive(Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct ObjectId(pub u128);

impl ObjectId {
    /// Create a new ObjectId from a u128 value.
    pub const fn new(value: u128) -> Self {
        ObjectId(value)
    }

    /// Get the inner u128 value.
    pub const fn inner(self) -> u128 {
        self.0
    }

    /// Convert to little-endian bytes.
    pub fn to_le_bytes(self) -> [u8; 16] {
        self.0.to_le_bytes()
    }

    /// Create from little-endian bytes.
    pub fn from_le_bytes(bytes: [u8; 16]) -> Self {
        ObjectId(u128::from_le_bytes(bytes))
    }

    /// Encode as Crockford Base32 string.
    /// Returns a 26-character string (128 bits = 26 * 5 bits, with 2 bits padding).
    fn to_base32(&self) -> String {
        let mut result = [0u8; 26];
        let mut value = self.0;

        // Encode from right to left (least significant first)
        for i in (0..26).rev() {
            result[i] = CROCKFORD_ALPHABET[(value & 0x1F) as usize];
            value >>= 5;
        }

        // Safety: CROCKFORD_ALPHABET only contains ASCII characters
        unsafe { String::from_utf8_unchecked(result.to_vec()) }
    }

    /// Parse from Crockford Base32 string.
    fn from_base32(s: &str) -> Result<Self, ObjectIdParseError> {
        let s = s.trim();

        if s.is_empty() {
            return Err(ObjectIdParseError::Empty);
        }

        // Allow variable length - pad with leading zeros
        if s.len() > 26 {
            return Err(ObjectIdParseError::TooLong);
        }

        let mut value: u128 = 0;

        for c in s.bytes() {
            if c >= 128 {
                return Err(ObjectIdParseError::InvalidChar(c as char));
            }

            let digit = CROCKFORD_DECODE[c as usize];
            if digit == 0xFF {
                return Err(ObjectIdParseError::InvalidChar(c as char));
            }

            // Check for overflow
            if value > (u128::MAX >> 5) {
                return Err(ObjectIdParseError::Overflow);
            }

            value = (value << 5) | (digit as u128);
        }

        Ok(ObjectId(value))
    }
}

impl fmt::Display for ObjectId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_base32())
    }
}

impl fmt::Debug for ObjectId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ObjectId({})", self.to_base32())
    }
}

impl FromStr for ObjectId {
    type Err = ObjectIdParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        ObjectId::from_base32(s)
    }
}

impl From<u128> for ObjectId {
    fn from(value: u128) -> Self {
        ObjectId(value)
    }
}

impl From<ObjectId> for u128 {
    fn from(id: ObjectId) -> Self {
        id.0
    }
}

/// Error parsing an ObjectId from a string.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ObjectIdParseError {
    /// Empty string.
    Empty,
    /// String too long (more than 26 characters).
    TooLong,
    /// Invalid character in string.
    InvalidChar(char),
    /// Value overflow (shouldn't happen with <= 26 chars).
    Overflow,
}

impl fmt::Display for ObjectIdParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ObjectIdParseError::Empty => write!(f, "empty object ID string"),
            ObjectIdParseError::TooLong => write!(f, "object ID string too long (max 26 characters)"),
            ObjectIdParseError::InvalidChar(c) => write!(f, "invalid character '{}' in object ID", c),
            ObjectIdParseError::Overflow => write!(f, "object ID value overflow"),
        }
    }
}

impl std::error::Error for ObjectIdParseError {}

/// Schema ID type alias (object ID of schema object).
pub type SchemaId = ObjectId;

/// Key for a reference index: (source_table, source_column).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct IndexKey {
    pub source_table: String,
    pub source_column: String,
}

impl IndexKey {
    pub fn new(source_table: impl Into<String>, source_column: impl Into<String>) -> Self {
        IndexKey {
            source_table: source_table.into(),
            source_column: source_column.into(),
        }
    }
}

/// State of a query subscription.
#[derive(Debug, Clone)]
pub enum QueryState {
    /// Query is loading.
    Loading,
    /// Query has results.
    Loaded(Vec<Row>),
    /// Query encountered an error.
    Error(String),
}

impl QueryState {
    /// Check if query is in loading state.
    pub fn is_loading(&self) -> bool {
        matches!(self, QueryState::Loading)
    }

    /// Check if query is loaded.
    pub fn is_loaded(&self) -> bool {
        matches!(self, QueryState::Loaded(_))
    }

    /// Check if query has error.
    pub fn is_error(&self) -> bool {
        matches!(self, QueryState::Error(_))
    }

    /// Get rows if loaded.
    pub fn rows(&self) -> Option<Vec<Row>> {
        match self {
            QueryState::Loaded(rows) => Some(rows.clone()),
            _ => None,
        }
    }

    /// Get error message if error.
    pub fn error(&self) -> Option<&str> {
        match self {
            QueryState::Error(msg) => Some(msg),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn object_id_roundtrip() {
        let test_values = [
            0u128,
            1,
            255,
            256,
            u128::MAX,
            0x0123456789ABCDEF0123456789ABCDEF,
        ];

        for value in test_values {
            let id = ObjectId::new(value);
            let s = id.to_string();
            let parsed: ObjectId = s.parse().unwrap();
            assert_eq!(id, parsed, "roundtrip failed for {:#x}", value);
        }
    }

    #[test]
    fn object_id_display_format() {
        // Zero should be all zeros in base32
        let id = ObjectId::new(0);
        assert_eq!(id.to_string(), "00000000000000000000000000");

        // Max value
        let id = ObjectId::new(u128::MAX);
        // 128 bits = 26 * 5 - 2 = 128 bits with 2 padding bits
        // So max is 0x3FFFFFFF... which in base32 is 7ZZZZZZZZZZZZZZZZZZZZZZZZZ
        assert_eq!(id.to_string(), "7ZZZZZZZZZZZZZZZZZZZZZZZZZ");
    }

    #[test]
    fn object_id_case_insensitive() {
        let lower: ObjectId = "abc123".parse().unwrap();
        let upper: ObjectId = "ABC123".parse().unwrap();
        assert_eq!(lower, upper);
    }

    #[test]
    fn object_id_common_substitutions() {
        // I, L -> 1
        let id1: ObjectId = "1".parse().unwrap();
        let id_i: ObjectId = "I".parse().unwrap();
        let id_l: ObjectId = "L".parse().unwrap();
        assert_eq!(id1, id_i);
        assert_eq!(id1, id_l);

        // O -> 0
        let id0: ObjectId = "0".parse().unwrap();
        let id_o: ObjectId = "O".parse().unwrap();
        assert_eq!(id0, id_o);
    }

    #[test]
    fn object_id_debug_format() {
        let id = ObjectId::new(42);
        let debug = format!("{:?}", id);
        assert!(debug.starts_with("ObjectId("));
        assert!(debug.ends_with(")"));
    }

    #[test]
    fn object_id_bytes_roundtrip() {
        let id = ObjectId::new(0x0123456789ABCDEF0123456789ABCDEF);
        let bytes = id.to_le_bytes();
        let parsed = ObjectId::from_le_bytes(bytes);
        assert_eq!(id, parsed);
    }

    #[test]
    fn object_id_from_u128() {
        let id: ObjectId = 42u128.into();
        assert_eq!(id.inner(), 42);
    }

    #[test]
    fn object_id_into_u128() {
        let id = ObjectId::new(42);
        let value: u128 = id.into();
        assert_eq!(value, 42);
    }

    #[test]
    fn object_id_parse_errors() {
        assert!(matches!("".parse::<ObjectId>(), Err(ObjectIdParseError::Empty)));
        assert!(matches!("000000000000000000000000000".parse::<ObjectId>(), Err(ObjectIdParseError::TooLong)));
        assert!(matches!("hello!".parse::<ObjectId>(), Err(ObjectIdParseError::InvalidChar('!'))));
    }
}
