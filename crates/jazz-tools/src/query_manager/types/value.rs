use serde::{Deserialize, Deserializer, Serialize, Serializer};

use crate::object::ObjectId;

use super::*;

/// Value type for API boundary (insert input, query output).
/// Internally, rows are stored as binary.
///
/// `PartialEq`/`Eq` are implemented manually because `f64` does not implement
/// `Eq`. We use bitwise comparison (`f64::to_bits`) so that NaN == NaN and
/// -0.0 != 0.0, which is the correct semantics for storage identity.
#[derive(Debug, Clone)]
pub enum Value {
    Integer(i32),
    BigInt(i64),
    /// 8-byte IEEE 754 double-precision float.
    Double(f64),
    Boolean(bool),
    Text(String),
    Timestamp(u64),
    Uuid(ObjectId),
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
    Null,
}

/// Use internally-tagged enum for JSON serialization.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", content = "value")]
enum ValueHuman {
    Integer(i32),
    BigInt(i64),
    Double(f64),
    Boolean(bool),
    Text(String),
    Timestamp(u64),
    Uuid(ObjectId),
    Bytea(Vec<u8>),
    Array(Vec<ValueHuman>),
    Row(Vec<ValueHuman>),
    Null,
}

/// Use externally-tagged enum for binary serialization (postcard does not support internally-tagged enums).
#[derive(Debug, Clone, Serialize, Deserialize)]
enum ValueBinary {
    Integer(i32),
    BigInt(i64),
    Double(f64),
    Boolean(bool),
    Text(String),
    Timestamp(u64),
    Uuid(ObjectId),
    Bytea(Vec<u8>),
    Array(Vec<ValueBinary>),
    Row(Vec<ValueBinary>),
    Null,
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
            Value::Bytea(v) => ValueHuman::Bytea(v.clone()),
            Value::Array(v) => ValueHuman::Array(v.iter().map(ValueHuman::from).collect()),
            Value::Row(v) => ValueHuman::Row(v.iter().map(ValueHuman::from).collect()),
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
            ValueHuman::Bytea(v) => Value::Bytea(v),
            ValueHuman::Array(v) => Value::Array(v.into_iter().map(Value::from).collect()),
            ValueHuman::Row(v) => Value::Row(v.into_iter().map(Value::from).collect()),
            ValueHuman::Null => Value::Null,
        }
    }
}

impl From<&Value> for ValueBinary {
    fn from(value: &Value) -> Self {
        match value {
            Value::Integer(v) => ValueBinary::Integer(*v),
            Value::BigInt(v) => ValueBinary::BigInt(*v),
            Value::Double(v) => ValueBinary::Double(*v),
            Value::Boolean(v) => ValueBinary::Boolean(*v),
            Value::Text(v) => ValueBinary::Text(v.clone()),
            Value::Timestamp(v) => ValueBinary::Timestamp(*v),
            Value::Uuid(v) => ValueBinary::Uuid(*v),
            Value::Bytea(v) => ValueBinary::Bytea(v.clone()),
            Value::Array(v) => ValueBinary::Array(v.iter().map(ValueBinary::from).collect()),
            Value::Row(v) => ValueBinary::Row(v.iter().map(ValueBinary::from).collect()),
            Value::Null => ValueBinary::Null,
        }
    }
}

impl From<ValueBinary> for Value {
    fn from(value: ValueBinary) -> Self {
        match value {
            ValueBinary::Integer(v) => Value::Integer(v),
            ValueBinary::BigInt(v) => Value::BigInt(v),
            ValueBinary::Double(v) => Value::Double(v),
            ValueBinary::Boolean(v) => Value::Boolean(v),
            ValueBinary::Text(v) => Value::Text(v),
            ValueBinary::Timestamp(v) => Value::Timestamp(v),
            ValueBinary::Uuid(v) => Value::Uuid(v),
            ValueBinary::Bytea(v) => Value::Bytea(v),
            ValueBinary::Array(v) => Value::Array(v.into_iter().map(Value::from).collect()),
            ValueBinary::Row(v) => Value::Row(v.into_iter().map(Value::from).collect()),
            ValueBinary::Null => Value::Null,
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
            ValueBinary::from(self).serialize(serializer)
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
            ValueBinary::deserialize(deserializer).map(Value::from)
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
            (Value::Bytea(a), Value::Bytea(b)) => a == b,
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
