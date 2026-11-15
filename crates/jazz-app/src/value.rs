//! Rust types of column values, and their conversion to and from Jazz
//! [`Value`]s.

use jazz::tools::{ObjectId, Value};

/// A Rust type a column's values convert to and from.
///
/// Implemented for the types generated bindings use: `String`, `bool`,
/// `i32`, `i64`, `f64`, [`ObjectId`], `Vec<u8>` (bytes), `Vec<f32>`
/// (vectors), [`Timestamp`],
/// [`Json`], `Vec<T>` (arrays) and `Option<T>` (nullable columns), and by
/// generated enums.
pub trait ColumnValue: Sized + Send + 'static {
    fn into_value(self) -> Value;
    /// `value` back when it is not of this type.
    fn from_value(value: Value) -> Result<Self, Value>;
}

macro_rules! scalar {
    ($ty:ty, $variant:ident) => {
        impl ColumnValue for $ty {
            fn into_value(self) -> Value {
                Value::$variant(self)
            }
            fn from_value(value: Value) -> Result<Self, Value> {
                match value {
                    Value::$variant(value) => Ok(value),
                    other => Err(other),
                }
            }
        }
    };
}

scalar!(String, Text);
scalar!(bool, Boolean);
scalar!(i32, Integer);
scalar!(i64, BigInt);
scalar!(f64, Double);
scalar!(ObjectId, Uuid);
scalar!(Vec<u8>, Bytea);

/// A `vector` column's value (SPEC 20.1), carried as its `f32le/1` bytes.
/// The column's dimensions and finiteness are checked when the row is
/// written.
impl ColumnValue for Vec<f32> {
    fn into_value(self) -> Value {
        Value::Bytea(self.iter().flat_map(|x| x.to_le_bytes()).collect())
    }
    fn from_value(value: Value) -> Result<Self, Value> {
        match value {
            Value::Bytea(bytes) if bytes.len() % 4 == 0 => Ok(bytes
                .chunks_exact(4)
                .map(|c| f32::from_le_bytes([c[0], c[1], c[2], c[3]]))
                .collect()),
            other => Err(other),
        }
    }
}

impl<T: ColumnValue> ColumnValue for Option<T> {
    fn into_value(self) -> Value {
        self.map_or(Value::Null, T::into_value)
    }
    fn from_value(value: Value) -> Result<Self, Value> {
        match value {
            Value::Null => Ok(None),
            value => T::from_value(value).map(Some),
        }
    }
}

impl<T: ColumnValue> ColumnValue for Vec<T> {
    fn into_value(self) -> Value {
        Value::Array(self.into_iter().map(T::into_value).collect())
    }
    /// An element of another type is the value given back.
    fn from_value(value: Value) -> Result<Self, Value> {
        match value {
            Value::Array(values) => values.into_iter().map(T::from_value).collect(),
            other => Err(other),
        }
    }
}

/// A timestamp column's value: milliseconds since the Unix epoch.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct Timestamp(pub u64);

impl Timestamp {
    pub fn now() -> Self {
        let elapsed = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default();
        Self(u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX))
    }
}

impl ColumnValue for Timestamp {
    fn into_value(self) -> Value {
        Value::Timestamp(self.0)
    }
    fn from_value(value: Value) -> Result<Self, Value> {
        match value {
            Value::Timestamp(millis) => Ok(Self(millis)),
            other => Err(other),
        }
    }
}

/// A json column's value. Jazz stores it as JSON text.
#[derive(Debug, Clone, PartialEq)]
pub struct Json(pub serde_json::Value);

impl ColumnValue for Json {
    fn into_value(self) -> Value {
        Value::Text(self.0.to_string())
    }
    fn from_value(value: Value) -> Result<Self, Value> {
        match value {
            Value::Text(text) => serde_json::from_str(&text)
                .map(Self)
                .map_err(|_| Value::Text(text)),
            other => Err(other),
        }
    }
}

/// Implements [`ColumnValue`] for a fieldless enum stored as one of its
/// variants' names, as generated bindings declare enum columns.
///
/// ```
/// #[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// pub enum Priority { Low, High }
/// jazz_app::text_enum!(Priority { Low => "low", High => "high" });
/// ```
#[macro_export]
macro_rules! text_enum {
    ($ty:ident { $($variant:ident => $name:literal),+ $(,)? }) => {
        impl $ty {
            /// The name Jazz stores this variant as.
            pub fn as_str(self) -> &'static str {
                match self {
                    $(Self::$variant => $name,)+
                }
            }
        }
        impl $crate::ColumnValue for $ty {
            fn into_value(self) -> $crate::jazz::tools::Value {
                $crate::jazz::tools::Value::Text(::std::borrow::ToOwned::to_owned(self.as_str()))
            }
            fn from_value(
                value: $crate::jazz::tools::Value,
            ) -> ::std::result::Result<Self, $crate::jazz::tools::Value> {
                match value {
                    $crate::jazz::tools::Value::Text(text) => match text.as_str() {
                        $($name => ::std::result::Result::Ok(Self::$variant),)+
                        _ => ::std::result::Result::Err($crate::jazz::tools::Value::Text(text)),
                    },
                    other => ::std::result::Result::Err(other),
                }
            }
        }
    };
}
