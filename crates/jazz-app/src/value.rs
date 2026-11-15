//! Rust types of column values, and their conversion to and from Jazz
//! [`Value`]s.

use jazz::tools::{ObjectId, Value};

/// A Rust type a column's values convert to and from.
///
/// Implemented for the types generated bindings use: `String`, `bool`,
/// `i32`, `i64`, `f64`, [`ObjectId`], `Vec<u8>` (bytes), [`Timestamp`],
/// [`Json`], `Vec<T>` (arrays) and `Option<T>` (nullable columns), and by
/// generated enums.
pub trait ColumnValue: Sized + Send + 'static {
    fn into_value(self) -> Value;
    /// `None` when `value` is not of this type.
    fn from_value(value: Value) -> Option<Self>;
}

macro_rules! scalar {
    ($ty:ty, $variant:ident) => {
        impl ColumnValue for $ty {
            fn into_value(self) -> Value {
                Value::$variant(self)
            }
            fn from_value(value: Value) -> Option<Self> {
                match value {
                    Value::$variant(value) => Some(value),
                    _ => None,
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

impl<T: ColumnValue> ColumnValue for Option<T> {
    fn into_value(self) -> Value {
        self.map_or(Value::Null, T::into_value)
    }
    fn from_value(value: Value) -> Option<Self> {
        match value {
            Value::Null => Some(None),
            value => T::from_value(value).map(Some),
        }
    }
}

impl<T: ColumnValue> ColumnValue for Vec<T> {
    fn into_value(self) -> Value {
        Value::Array(self.into_iter().map(T::into_value).collect())
    }
    fn from_value(value: Value) -> Option<Self> {
        match value {
            Value::Array(values) => values.into_iter().map(T::from_value).collect(),
            _ => None,
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
    fn from_value(value: Value) -> Option<Self> {
        match value {
            Value::Timestamp(millis) => Some(Self(millis)),
            _ => None,
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
    fn from_value(value: Value) -> Option<Self> {
        match value {
            Value::Text(text) => serde_json::from_str(&text).ok().map(Self),
            _ => None,
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
                $crate::jazz::tools::Value::Text(self.as_str().to_owned())
            }
            fn from_value(value: $crate::jazz::tools::Value) -> Option<Self> {
                match value {
                    $crate::jazz::tools::Value::Text(text) => match text.as_str() {
                        $($name => Some(Self::$variant),)+
                        _ => None,
                    },
                    _ => None,
                }
            }
        }
    };
}
