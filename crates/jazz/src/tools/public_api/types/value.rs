use std::fmt;

use serde::de::{self, Visitor};
use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::tools::object::ObjectId;

use super::*;

/// Value type for API boundary (insert input, query output).
/// Internally, rows are stored as binary.
///
/// `PartialEq`/`Eq` are implemented manually because `f64` does not implement
/// `Eq`. We use bitwise comparison (`f64::to_bits`) so that NaN == NaN and
/// -0.0 != 0.0, which is the correct semantics for storage identity.
#[derive(Clone)]
pub enum Value {
    Integer(i32),
    BigInt(i64),
    /// 8-byte IEEE 754 double-precision float.
    Double(f64),
    Boolean(bool),
    Text(String),
    Timestamp(u64),
    Uuid(ObjectId),
    TransactionId([u8; 16]),
    Bytea(Vec<u8>),
    /// Homogeneous array of values.
    Array(Vec<Value>),
    /// Heterogeneous row/tuple of values (for nested rows in arrays).
    /// The schema is external (from ColumnType::Row).
    /// `id` carries the originating object's id when this Row came from an
    /// included relation so the TS layer can surface it.
    Row {
        id: Option<ObjectId>,
        values: Vec<Value>,
    },
    /// Selected case and positional payload values for a column-local enum.
    Enum {
        case: String,
        values: Vec<Value>,
    },
    Null,
}

impl fmt::Debug for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Integer(value) => f.debug_tuple("Integer").field(value).finish(),
            Self::BigInt(value) => f.debug_tuple("BigInt").field(value).finish(),
            Self::Double(value) => f.debug_tuple("Double").field(value).finish(),
            Self::Boolean(value) => f.debug_tuple("Boolean").field(value).finish(),
            Self::Text(value) => f.debug_tuple("Text").field(value).finish(),
            Self::Timestamp(value) => f.debug_tuple("Timestamp").field(value).finish(),
            Self::Uuid(value) => f.debug_tuple("Uuid").field(value).finish(),
            // Transaction ids are fixed-size semantic identifiers, unlike payloads.
            Self::TransactionId(value) => write!(f, "TransactionId({})", hex::encode(value)),
            Self::Bytea(value) => f.debug_struct("Bytea").field("len", &value.len()).finish(),
            // Avoid a collection of nested byte values making an error log unbounded.
            Self::Array(values) => f.debug_struct("Array").field("len", &values.len()).finish(),
            Self::Row { id, values } => f
                .debug_struct("Row")
                .field("id", id)
                .field("values_len", &values.len())
                .finish(),
            Self::Enum { case, values } => f
                .debug_struct("Enum")
                .field("case", case)
                .field("values_len", &values.len())
                .finish(),
            Self::Null => f.write_str("Null"),
        }
    }
}

/// Use internally-tagged enum for JSON serialization.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "value")]
enum ValueHuman {
    Integer(i32),
    BigInt(
        #[serde(
            serialize_with = "serialize_bigint_value",
            deserialize_with = "deserialize_bigint_value"
        )]
        i64,
    ),
    Double(f64),
    Boolean(bool),
    Text(String),
    Timestamp(#[serde(deserialize_with = "deserialize_timestamp_value")] u64),
    Uuid(ObjectId),
    TransactionId([u8; 16]),
    Bytea(Vec<u8>),
    Array(Vec<ValueHuman>),
    Row(RowHuman),
    Enum(EnumHuman),
    Null,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct RowHuman {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    id: Option<ObjectId>,
    values: Vec<ValueHuman>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
struct EnumHuman {
    case: String,
    values: Vec<ValueHuman>,
}

/// Public `Value` is a host-facing shape, not a generic postcard payload.
///
/// JSON preserves all row metadata. Production binary row boundaries instead
/// use the descriptor/raw-record codecs in `admin_catalogue_row_format` and the
/// native runtime, which carry an optional nested-row id explicitly.
const NON_HUMAN_VALUE_CODEC_ERROR: &str =
    "public Value does not support non-human serde codecs; use the descriptor/raw row codec";

fn deserialize_timestamp_value<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: Deserializer<'de>,
{
    // JS numbers crossing the NAPI JSON boundary can surface as integral f64s.
    struct TimestampValueVisitor;

    impl Visitor<'_> for TimestampValueVisitor {
        type Value = u64;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("a non-negative integer timestamp")
        }

        fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E> {
            Ok(value)
        }

        fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            u64::try_from(value).map_err(|_| E::custom("timestamp must be non-negative"))
        }

        fn visit_f64<E>(self, value: f64) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            if !value.is_finite() {
                return Err(E::custom("timestamp must be finite"));
            }
            if value < 0.0 {
                return Err(E::custom("timestamp must be non-negative"));
            }
            if value.fract() != 0.0 {
                return Err(E::custom("timestamp must be an integer"));
            }
            if value > u64::MAX as f64 {
                return Err(E::custom("timestamp is out of range"));
            }

            Ok(value as u64)
        }
    }

    deserializer.deserialize_any(TimestampValueVisitor)
}

fn serialize_bigint_value<S>(value: &i64, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(&value.to_string())
}

fn deserialize_bigint_value<'de, D>(deserializer: D) -> Result<i64, D::Error>
where
    D: Deserializer<'de>,
{
    struct BigIntValueVisitor;

    impl Visitor<'_> for BigIntValueVisitor {
        type Value = i64;

        fn expecting(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
            formatter.write_str("a signed 64-bit integer or decimal string")
        }

        fn visit_i64<E>(self, value: i64) -> Result<Self::Value, E> {
            Ok(value)
        }

        fn visit_u64<E>(self, value: u64) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            i64::try_from(value).map_err(|_| E::custom("bigint is out of signed 64-bit range"))
        }

        fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            value
                .parse::<i64>()
                .map_err(|_| E::custom("bigint string must be a signed 64-bit integer"))
        }

        fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
        where
            E: de::Error,
        {
            self.visit_str(&value)
        }
    }

    deserializer.deserialize_any(BigIntValueVisitor)
}

impl From<&Value> for ValueHuman {
    fn from(value: &Value) -> Self {
        match value {
            Value::Integer(v) => ValueHuman::Integer(*v),
            Value::BigInt(v) => ValueHuman::BigInt(*v),
            Value::Double(v) => ValueHuman::Double(*v),
            Value::Boolean(v) => ValueHuman::Boolean(*v),
            Value::Text(v) => ValueHuman::Text(v.clone()),
            Value::Timestamp(v) => ValueHuman::Timestamp(*v),
            Value::Uuid(v) => ValueHuman::Uuid(*v),
            Value::TransactionId(v) => ValueHuman::TransactionId(*v),
            Value::Bytea(v) => ValueHuman::Bytea(v.clone()),
            Value::Array(v) => ValueHuman::Array(v.iter().map(ValueHuman::from).collect()),
            Value::Row { id, values } => ValueHuman::Row(RowHuman {
                id: *id,
                values: values.iter().map(ValueHuman::from).collect(),
            }),
            Value::Enum { case, values } => ValueHuman::Enum(EnumHuman {
                case: case.clone(),
                values: values.iter().map(ValueHuman::from).collect(),
            }),
            Value::Null => ValueHuman::Null,
        }
    }
}

impl From<ValueHuman> for Value {
    fn from(value: ValueHuman) -> Self {
        match value {
            ValueHuman::Integer(v) => Value::Integer(v),
            ValueHuman::BigInt(v) => Value::BigInt(v),
            ValueHuman::Double(v) => Value::Double(v),
            ValueHuman::Boolean(v) => Value::Boolean(v),
            ValueHuman::Text(v) => Value::Text(v),
            ValueHuman::Timestamp(v) => Value::Timestamp(v),
            ValueHuman::Uuid(v) => Value::Uuid(v),
            ValueHuman::TransactionId(v) => Value::TransactionId(v),
            ValueHuman::Bytea(v) => Value::Bytea(v),
            ValueHuman::Array(v) => Value::Array(v.into_iter().map(Value::from).collect()),
            ValueHuman::Row(r) => Value::Row {
                id: r.id,
                values: r.values.into_iter().map(Value::from).collect(),
            },
            ValueHuman::Enum(e) => Value::Enum {
                case: e.case,
                values: e.values.into_iter().map(Value::from).collect(),
            },
            ValueHuman::Null => Value::Null,
        }
    }
}

impl Serialize for Value {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if serializer.is_human_readable() {
            ValueHuman::from(self).serialize(serializer)
        } else {
            Err(serde::ser::Error::custom(NON_HUMAN_VALUE_CODEC_ERROR))
        }
    }
}

impl<'de> Deserialize<'de> for Value {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        if deserializer.is_human_readable() {
            ValueHuman::deserialize(deserializer).map(Value::from)
        } else {
            Err(serde::de::Error::custom(NON_HUMAN_VALUE_CODEC_ERROR))
        }
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::Integer(a), Value::Integer(b)) => a == b,
            (Value::BigInt(a), Value::BigInt(b)) => a == b,
            (Value::Double(a), Value::Double(b)) => a.to_bits() == b.to_bits(),
            (Value::Boolean(a), Value::Boolean(b)) => a == b,
            (Value::Text(a), Value::Text(b)) => a == b,
            (Value::Timestamp(a), Value::Timestamp(b)) => a == b,
            (Value::Uuid(a), Value::Uuid(b)) => a == b,
            (Value::TransactionId(a), Value::TransactionId(b)) => a == b,
            (Value::Bytea(a), Value::Bytea(b)) => a == b,
            (
                Value::Enum {
                    case: a_case,
                    values: a_values,
                },
                Value::Enum {
                    case: b_case,
                    values: b_values,
                },
            ) => a_case == b_case && a_values == b_values,
            (Value::Array(a), Value::Array(b)) => a == b,
            (
                Value::Row {
                    id: id_a,
                    values: a,
                },
                Value::Row {
                    id: id_b,
                    values: b,
                },
            ) => id_a == id_b && a == b,
            (Value::Null, Value::Null) => true,
            _ => false,
        }
    }
}

impl Eq for Value {}

impl Value {
    /// Returns the column type this value represents, or None for Null/Row.
    /// Row returns None because its schema is external.
    pub fn column_type(&self) -> Option<ColumnType> {
        match self {
            Value::Integer(_) => Some(ColumnType::Integer),
            Value::BigInt(_) => Some(ColumnType::BigInt),
            Value::Double(_) => Some(ColumnType::Double),
            Value::Boolean(_) => Some(ColumnType::Boolean),
            Value::Text(_) => Some(ColumnType::Text),
            Value::Timestamp(_) => Some(ColumnType::Timestamp),
            Value::Uuid(_) => Some(ColumnType::Uuid),
            Value::TransactionId(_) => Some(ColumnType::TransactionId),
            Value::Bytea(_) => Some(ColumnType::Bytea),
            Value::Array(elements) => {
                // Infer element type from first element; empty arrays have no inferable type
                elements
                    .iter()
                    .find_map(|v| v.column_type())
                    .map(|elem_type| ColumnType::Array {
                        element: Box::new(elem_type),
                    })
            }
            // Row type requires external schema, can't be inferred
            Value::Row { .. } => None,
            Value::Enum { .. } => None,
            Value::Null => None,
        }
    }

    /// Returns true if this is a Null value.
    pub fn is_null(&self) -> bool {
        matches!(self, Value::Null)
    }

    /// Returns true if this is an Array value.
    pub fn is_array(&self) -> bool {
        matches!(self, Value::Array(_))
    }

    /// Returns true if this is a Row value.
    pub fn is_row(&self) -> bool {
        matches!(self, Value::Row { .. })
    }

    /// Returns the array elements if this is an Array, None otherwise.
    pub fn as_array(&self) -> Option<&[Value]> {
        match self {
            Value::Array(elements) => Some(elements),
            _ => None,
        }
    }

    /// Returns the row values if this is a Row, None otherwise.
    pub fn as_row(&self) -> Option<&[Value]> {
        match self {
            Value::Row { values, .. } => Some(values),
            _ => None,
        }
    }

    /// Returns the row id if this is a Row with an id, None otherwise.
    pub fn row_id(&self) -> Option<ObjectId> {
        match self {
            Value::Row { id, .. } => *id,
            _ => None,
        }
    }
}

// ── From impls for ergonomic Value construction ─────────────────────
// Note: no `From<u64>` for `Timestamp` — a bare u64 is ambiguous (could be a
// large integer or a millisecond timestamp). Use `Value::Timestamp(ts)` explicitly.

impl From<bool> for Value {
    fn from(v: bool) -> Self {
        Value::Boolean(v)
    }
}

impl From<i32> for Value {
    fn from(v: i32) -> Self {
        Value::Integer(v)
    }
}

impl From<i64> for Value {
    fn from(v: i64) -> Self {
        Value::BigInt(v)
    }
}

impl From<f64> for Value {
    fn from(v: f64) -> Self {
        Value::Double(v)
    }
}

impl From<&str> for Value {
    fn from(v: &str) -> Self {
        Value::Text(v.to_string())
    }
}

impl From<String> for Value {
    fn from(v: String) -> Self {
        Value::Text(v)
    }
}

impl From<ObjectId> for Value {
    fn from(v: ObjectId) -> Self {
        Value::Uuid(v)
    }
}

impl From<[u8; 16]> for Value {
    fn from(v: [u8; 16]) -> Self {
        Value::TransactionId(v)
    }
}

/// Produces `Bytea`, not `Array` of integers. Use `Value::Array(...)` explicitly
/// if you need an integer array.
impl From<Vec<u8>> for Value {
    fn from(v: Vec<u8>) -> Self {
        Value::Bytea(v)
    }
}

impl From<Vec<Value>> for Value {
    fn from(v: Vec<Value>) -> Self {
        Value::Array(v)
    }
}

impl<T: Into<Value>> From<Option<T>> for Value {
    fn from(v: Option<T>) -> Self {
        match v {
            Some(inner) => inner.into(),
            None => Value::Null,
        }
    }
}

/// Build a `HashMap<String, Value>` from key-value pairs with heterogeneous types.
///
/// Each value can be any type that implements `Into<Value>`.
///
/// ```ignore
/// row_input!("name" => "Alice", "age" => 30i32)
/// ```
#[macro_export]
macro_rules! row_input {
    ($( $col:expr => $val:expr ),* $(,)?) => {{
        #[allow(unused_mut)]
        let mut map = std::collections::HashMap::<String, $crate::tools::Value>::new();
        $( map.insert($col.to_string(), <_ as Into<$crate::tools::Value>>::into($val)); )*
        map
    }};
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    #[test]
    fn binary_value_debug_is_bounded_and_content_safe_even_when_nested() {
        assert_eq!(format!("{:?}", Value::Bytea(vec![])), "Bytea { len: 0 }");

        let secret = b"do-not-log-this-payload";
        let nested = Value::Row {
            id: None,
            values: vec![Value::Array(vec![Value::Bytea(vec![b'x'; 1_000_000])])],
        };
        let debug = format!("{nested:?}");
        assert_eq!(debug, "Row { id: None, values_len: 1 }");
        assert!(debug.len() < 64);
        assert!(!debug.contains(std::str::from_utf8(secret).unwrap()));
    }

    #[test]
    fn transaction_id_debug_keeps_the_compact_semantic_id() {
        assert_eq!(
            format!("{:?}", Value::TransactionId([0xab; 16])),
            "TransactionId(abababababababababababababababab)"
        );
    }

    // ── From<bool> ──────────────────────────────────────────────────

    #[test]
    fn from_bool_true() {
        let v: Value = true.into();
        assert_eq!(v, Value::Boolean(true));
    }

    #[test]
    fn from_bool_false() {
        let v: Value = false.into();
        assert_eq!(v, Value::Boolean(false));
    }

    // ── From<i32> ───────────────────────────────────────────────────

    #[test]
    fn from_i32() {
        let v: Value = 42i32.into();
        assert_eq!(v, Value::Integer(42));
    }

    #[test]
    fn from_i32_negative() {
        let v: Value = (-1i32).into();
        assert_eq!(v, Value::Integer(-1));
    }

    // ── From<i64> ───────────────────────────────────────────────────

    #[test]
    fn from_i64() {
        let v: Value = 1_000_000_000_000i64.into();
        assert_eq!(v, Value::BigInt(1_000_000_000_000));
    }

    // ── From<f64> ───────────────────────────────────────────────────

    #[test]
    fn from_f64() {
        let v: Value = std::f64::consts::PI.into();
        assert_eq!(v, Value::Double(std::f64::consts::PI));
    }

    // ── From<&str> ──────────────────────────────────────────────────

    #[test]
    fn from_str_ref() {
        let v: Value = "hello".into();
        assert_eq!(v, Value::Text("hello".to_string()));
    }

    // ── From<String> ────────────────────────────────────────────────

    #[test]
    fn from_string() {
        let v: Value = String::from("world").into();
        assert_eq!(v, Value::Text("world".to_string()));
    }

    // ── From<ObjectId> ──────────────────────────────────────────────

    #[test]
    fn from_object_id() {
        let oid = ObjectId::new();
        let v: Value = oid.into();
        assert_eq!(v, Value::Uuid(oid));
    }

    #[test]
    fn from_batch_id_bytes() {
        let bytes = [7u8; 16];
        let v: Value = bytes.into();
        assert_eq!(v, Value::TransactionId(bytes));
    }

    // ── From<Vec<u8>> ───────────────────────────────────────────────

    #[test]
    fn from_vec_u8() {
        let bytes = vec![0xDE, 0xAD, 0xBE, 0xEF];
        let v: Value = bytes.clone().into();
        assert_eq!(v, Value::Bytea(bytes));
    }

    #[test]
    fn transaction_id_json_round_trip() {
        let value = Value::TransactionId([9u8; 16]);
        let json = serde_json::to_string(&value).expect("serialize transaction id");
        let decoded: Value = serde_json::from_str(&json).expect("deserialize transaction id");
        assert_eq!(decoded, value);
    }

    #[test]
    fn json_round_trip_preserves_nested_row_identity() {
        let value = Value::Array(vec![Value::Row {
            id: Some(ObjectId::from_uuid(uuid::Uuid::from_u128(0x42))),
            values: vec![Value::Text("included child".to_owned())],
        }]);

        let json = serde_json::to_string(&value).expect("serialize public value");
        let decoded = serde_json::from_str::<Value>(&json).expect("deserialize public value");

        assert_eq!(decoded, value);
    }

    #[test]
    fn postcard_rejects_public_values_instead_of_dropping_nested_row_identity() {
        let value = Value::Array(vec![Value::Row {
            id: Some(ObjectId::from_uuid(uuid::Uuid::from_u128(0x42))),
            values: vec![Value::Text("included child".to_owned())],
        }]);

        assert_eq!(
            postcard::to_allocvec(&value).unwrap_err(),
            postcard::Error::SerdeSerCustom
        );
        assert_eq!(
            postcard::from_bytes::<Value>(&[]).unwrap_err(),
            postcard::Error::SerdeDeCustom
        );
    }

    // ── From<Vec<Value>> ────────────────────────────────────────────

    #[test]
    fn from_vec_value() {
        let arr = vec![Value::Integer(1), Value::Integer(2)];
        let v: Value = arr.clone().into();
        assert_eq!(v, Value::Array(arr));
    }

    // ── From<Option<T>> ─────────────────────────────────────────────

    #[test]
    fn from_option_some() {
        let v: Value = Some(42i32).into();
        assert_eq!(v, Value::Integer(42));
    }

    #[test]
    fn from_option_none() {
        let v: Value = Option::<i32>::None.into();
        assert_eq!(v, Value::Null);
    }

    #[test]
    fn from_option_some_string() {
        let v: Value = Some("hey".to_string()).into();
        assert_eq!(v, Value::Text("hey".to_string()));
    }

    // ── row_input! ──────────────────────────────────────────────────

    #[test]
    fn row_input_builds_hashmap() {
        let map = row_input!("name" => "Alice", "age" => 30i32);

        let mut expected = HashMap::new();
        expected.insert("name".to_string(), Value::Text("Alice".to_string()));
        expected.insert("age".to_string(), Value::Integer(30));

        assert_eq!(map, expected);
    }

    #[test]
    fn row_input_empty() {
        let map: HashMap<String, Value> = row_input!();
        assert!(map.is_empty());
    }

    #[test]
    fn row_input_with_option_null() {
        let map = row_input!(
            "present" => Some("yes"),
            "absent" => Option::<&str>::None,
        );

        assert_eq!(map["present"], Value::Text("yes".to_string()));
        assert_eq!(map["absent"], Value::Null);
    }
}
