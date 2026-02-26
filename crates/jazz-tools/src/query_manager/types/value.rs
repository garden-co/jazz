use serde::{Deserialize, Serialize};

use crate::object::ObjectId;

use super::*;

/// Value type for API boundary (insert input, query output).
/// Internally, rows are stored as binary.
///
/// `PartialEq`/`Eq` are implemented manually because `f64` does not implement
/// `Eq`. We use bitwise comparison (`f64::to_bits`) so that NaN == NaN and
/// -0.0 != 0.0, which is the correct semantics for storage identity.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Value {
    Integer(i32),
    BigInt(i64),
    /// 8-byte IEEE 754 double-precision float.
    Double(f64),
    Boolean(bool),
    Text(String),
    Timestamp(u64),
    Uuid(ObjectId),
    /// Homogeneous array of values.
    Array(Vec<Value>),
    /// Heterogeneous row/tuple of values (for nested rows in arrays).
    /// The schema is external (from ColumnType::Row).
    Row(Vec<Value>),
    Null,
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
            (Value::Array(a), Value::Array(b)) => a == b,
            (Value::Row(a), Value::Row(b)) => a == b,
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
            Value::Array(elements) => {
                // Infer element type from first element; empty arrays have no inferable type
                elements
                    .iter()
                    .find_map(|v| v.column_type())
                    .map(|elem_type| ColumnType::Array(Box::new(elem_type)))
            }
            // Row type requires external schema, can't be inferred
            Value::Row(_) => None,
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
        matches!(self, Value::Row(_))
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
            Value::Row(values) => Some(values),
            _ => None,
        }
    }
}
