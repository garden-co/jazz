use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::collections::BTreeMap;
use std::ops::Index;

#[derive(Clone, Debug, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    Number(serde_json::Number),
    String(String),
    Array(Vec<Value>),
    Object(BTreeMap<String, Value>),
    Bytes(Vec<u8>),
}

impl Eq for Value {}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub(crate) enum WireValue {
    Null,
    Bool(bool),
    I64(i64),
    U64(u64),
    F64(f64),
    String(String),
    Array(Vec<WireValue>),
    Object(Vec<(String, WireValue)>),
    Bytes(Vec<u8>),
}

impl Serialize for Value {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        self.clone().into_json().serialize(serializer)
    }
}

impl<'de> Deserialize<'de> for Value {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let json = serde_json::Value::deserialize(deserializer)?;
        if let Some(bytes) = json
            .as_object()
            .and_then(|object| object.get("$bytes"))
            .and_then(serde_json::Value::as_str)
            .and_then(|hex| hex_to_bytes(hex).ok())
        {
            Ok(Self::Bytes(bytes))
        } else {
            Ok(Self::from_json(json))
        }
    }
}

impl From<&Value> for WireValue {
    fn from(value: &Value) -> Self {
        match value {
            Value::Null => Self::Null,
            Value::Bool(value) => Self::Bool(*value),
            Value::Number(value) => {
                if let Some(value) = value.as_i64() {
                    Self::I64(value)
                } else if let Some(value) = value.as_u64() {
                    Self::U64(value)
                } else {
                    Self::F64(value.as_f64().unwrap_or(0.0))
                }
            }
            Value::String(value) => Self::String(value.clone()),
            Value::Array(values) => Self::Array(values.iter().map(Self::from).collect()),
            Value::Object(values) => Self::Object(
                values
                    .iter()
                    .map(|(key, value)| (key.clone(), Self::from(value)))
                    .collect(),
            ),
            Value::Bytes(value) => Self::Bytes(value.clone()),
        }
    }
}

impl From<WireValue> for Value {
    fn from(value: WireValue) -> Self {
        match value {
            WireValue::Null => Self::Null,
            WireValue::Bool(value) => Self::Bool(value),
            WireValue::I64(value) => Self::Number(serde_json::Number::from(value)),
            WireValue::U64(value) => Self::Number(serde_json::Number::from(value)),
            WireValue::F64(value) => serde_json::Number::from_f64(value)
                .map(Self::Number)
                .unwrap_or(Self::Null),
            WireValue::String(value) => Self::String(value),
            WireValue::Array(values) => Self::Array(values.into_iter().map(Self::from).collect()),
            WireValue::Object(values) => Self::Object(
                values
                    .into_iter()
                    .map(|(key, value)| (key, Self::from(value)))
                    .collect(),
            ),
            WireValue::Bytes(value) => Self::Bytes(value),
        }
    }
}

impl PartialEq<serde_json::Value> for Value {
    fn eq(&self, other: &serde_json::Value) -> bool {
        self.clone().into_json() == *other
    }
}

impl PartialEq<Value> for serde_json::Value {
    fn eq(&self, other: &Value) -> bool {
        *self == other.clone().into_json()
    }
}

impl Value {
    pub fn from_json(value: serde_json::Value) -> Self {
        match value {
            serde_json::Value::Null => Self::Null,
            serde_json::Value::Bool(value) => Self::Bool(value),
            serde_json::Value::Number(value) => Self::Number(value),
            serde_json::Value::String(value) => Self::String(value),
            serde_json::Value::Array(values) => {
                Self::Array(values.into_iter().map(Self::from_json).collect())
            }
            serde_json::Value::Object(values) => Self::Object(
                values
                    .into_iter()
                    .map(|(key, value)| (key, Self::from_json(value)))
                    .collect(),
            ),
        }
    }

    pub fn into_json(self) -> serde_json::Value {
        match self {
            Self::Null => serde_json::Value::Null,
            Self::Bool(value) => serde_json::Value::Bool(value),
            Self::Number(value) => serde_json::Value::Number(value),
            Self::String(value) => serde_json::Value::String(value),
            Self::Array(values) => {
                serde_json::Value::Array(values.into_iter().map(Self::into_json).collect())
            }
            Self::Object(values) => serde_json::Value::Object(
                values
                    .into_iter()
                    .map(|(key, value)| (key, value.into_json()))
                    .collect(),
            ),
            Self::Bytes(value) => serde_json::json!({ "$bytes": bytes_to_hex(&value) }),
        }
    }

    pub fn is_null(&self) -> bool {
        matches!(self, Self::Null)
    }

    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::String(value) => Some(value),
            _ => None,
        }
    }

    pub fn is_string(&self) -> bool {
        matches!(self, Self::String(_))
    }

    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Self::Bytes(value) => Some(value),
            _ => None,
        }
    }

    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Self::Bool(value) => Some(*value),
            _ => None,
        }
    }

    pub fn as_u64(&self) -> Option<u64> {
        match self {
            Self::Number(value) => value.as_u64(),
            _ => None,
        }
    }

    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Self::Number(value) => value.as_f64(),
            _ => None,
        }
    }

    pub fn as_array(&self) -> Option<&Vec<Value>> {
        match self {
            Self::Array(values) => Some(values),
            _ => None,
        }
    }

    pub fn get(&self, key: &str) -> Option<&Value> {
        match self {
            Self::Object(value) => value.get(key),
            _ => None,
        }
    }
}

pub trait IntoValueMap {
    fn into_value_map(self) -> BTreeMap<String, Value>;
}

impl<V> IntoValueMap for BTreeMap<String, V>
where
    V: Into<Value>,
{
    fn into_value_map(self) -> BTreeMap<String, Value> {
        self.into_iter()
            .map(|(key, value)| (key, value.into()))
            .collect()
    }
}

impl std::fmt::Display for Value {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.clone().into_json())
    }
}

impl Index<&str> for Value {
    type Output = Value;

    fn index(&self, key: &str) -> &Self::Output {
        self.get(key).unwrap_or(&Value::Null)
    }
}

impl From<serde_json::Value> for Value {
    fn from(value: serde_json::Value) -> Self {
        Self::from_json(value)
    }
}

impl From<Value> for serde_json::Value {
    fn from(value: Value) -> Self {
        value.into_json()
    }
}

impl From<&str> for Value {
    fn from(value: &str) -> Self {
        Self::String(value.to_owned())
    }
}

impl From<String> for Value {
    fn from(value: String) -> Self {
        Self::String(value)
    }
}

impl From<bool> for Value {
    fn from(value: bool) -> Self {
        Self::Bool(value)
    }
}

impl From<i64> for Value {
    fn from(value: i64) -> Self {
        Self::Number(serde_json::Number::from(value))
    }
}

impl From<u64> for Value {
    fn from(value: u64) -> Self {
        Self::Number(serde_json::Number::from(value))
    }
}

impl From<usize> for Value {
    fn from(value: usize) -> Self {
        Self::Number(serde_json::Number::from(value))
    }
}

impl From<f64> for Value {
    fn from(value: f64) -> Self {
        serde_json::Number::from_f64(value)
            .map(Self::Number)
            .unwrap_or(Self::Null)
    }
}

impl From<Vec<u8>> for Value {
    fn from(value: Vec<u8>) -> Self {
        Self::Bytes(value)
    }
}

pub fn bytes_to_hex(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

pub fn hex_to_bytes(value: &str) -> crate::Result<Vec<u8>> {
    if !value.len().is_multiple_of(2) {
        return Err(crate::Error::new("hex bytes value has odd length"));
    }
    let mut bytes = Vec::with_capacity(value.len() / 2);
    let raw = value.as_bytes();
    for idx in (0..raw.len()).step_by(2) {
        let high = hex_nibble(raw[idx])?;
        let low = hex_nibble(raw[idx + 1])?;
        bytes.push((high << 4) | low);
    }
    Ok(bytes)
}

fn hex_nibble(byte: u8) -> crate::Result<u8> {
    match byte {
        b'0'..=b'9' => Ok(byte - b'0'),
        b'a'..=b'f' => Ok(byte - b'a' + 10),
        b'A'..=b'F' => Ok(byte - b'A' + 10),
        _ => Err(crate::Error::new("invalid hex bytes value")),
    }
}
