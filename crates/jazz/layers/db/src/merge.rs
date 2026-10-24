//! Column merge strategies: how concurrent writes to one column combine at
//! the authority instead of the last write winning.
//!
//! A counter write travels as the change it made: the writer turns the
//! value it set into the difference from the value it saw, and every node
//! that applies the write adds that difference to the value it holds, so
//! concurrent changes sum. A g-set write is unioned with the stored set,
//! kept in one canonical order, so no write removes an element.

use crate::options::Error;
use crate::records::{Value, ValueType};

fn counter_error(message: impl Into<String>) -> Error {
    crate::db::rejected(message.into())
}

fn integer(value: &Value) -> Option<i128> {
    Some(match value {
        Value::U8(v) => (*v).into(),
        Value::U16(v) => (*v).into(),
        Value::U32(v) => (*v).into(),
        Value::U64(v) | Value::U48(v) => (*v).into(),
        Value::I32(v) => (*v).into(),
        Value::I64(v) => (*v).into(),
        _ => return None,
    })
}

fn of_type(ty: &ValueType, value: i128) -> Option<Value> {
    Some(match ty {
        ValueType::U8 => Value::U8(value.try_into().ok()?),
        ValueType::U16 => Value::U16(value.try_into().ok()?),
        ValueType::U32 => Value::U32(value.try_into().ok()?),
        ValueType::U64 => Value::U64(value.try_into().ok()?),
        ValueType::I32 => Value::I32(value.try_into().ok()?),
        ValueType::I64 => Value::I64(value.try_into().ok()?),
        _ => return None,
    })
}

/// The change a counter write makes: what it set, less what it saw.
pub(crate) fn counter_delta(
    column: &str,
    set: &Value,
    seen: Option<&Value>,
) -> Result<Value, Error> {
    let set = integer(set)
        .ok_or_else(|| counter_error(format!("counter `{column}` takes an integer")))?;
    let seen = seen.and_then(integer).unwrap_or(0);
    i64::try_from(set - seen)
        .map(Value::I64)
        .map_err(|_| counter_error(format!("the change to counter `{column}` is out of range")))
}

/// A counter's value once `delta` is added to what the node holds.
pub(crate) fn counter_apply(
    column: &str,
    ty: &ValueType,
    held: Option<&Value>,
    delta: &Value,
) -> Result<Value, Error> {
    let delta = integer(delta)
        .ok_or_else(|| counter_error(format!("counter `{column}` takes an integer change")))?;
    let held = held.and_then(integer).unwrap_or(0);
    of_type(ty, held + delta)
        .ok_or_else(|| counter_error(format!("counter `{column}` would leave its range")))
}

/// A set element's place in the canonical order: by type, then by value
/// (floats by their bits, so `0.0` and `-0.0` stay distinct).
fn element_key(value: &Value) -> Vec<u8> {
    let mut key = Vec::new();
    match value {
        Value::Nullable(None) => key.push(0),
        Value::Nullable(Some(inner)) => return element_key(inner),
        Value::Bool(v) => key.extend([1, *v as u8]),
        value if integer(value).is_some() => {
            key.push(2);
            let v = integer(value).expect("integer");
            key.extend(((v as u128) ^ (1 << 127)).to_be_bytes());
        }
        Value::F64(v) => {
            key.push(3);
            key.extend(v.to_bits().to_be_bytes());
        }
        Value::String(v) => {
            key.push(4);
            key.extend(v.as_bytes());
        }
        Value::Uuid(v) => {
            key.push(5);
            key.extend(v.as_bytes());
        }
        Value::EnumTag(v) => {
            key.push(6);
            key.extend(v.to_be_bytes());
        }
        Value::Array(items) => {
            key.push(7);
            for item in items {
                let item = element_key(item);
                key.extend((item.len() as u32).to_be_bytes());
                key.extend(item);
            }
        }
        other => {
            key.push(8);
            key.extend(format!("{other:?}").into_bytes());
        }
    }
    key
}

/// The union of a g-set's held and written elements, in canonical order
/// with duplicates removed.
pub(crate) fn gset_union(
    column: &str,
    held: Option<&Value>,
    written: &Value,
) -> Result<Value, Error> {
    let items = |value: &Value| -> Result<Vec<Value>, Error> {
        match value {
            Value::Array(items) => Ok(items.clone()),
            _ => Err(counter_error(format!("g-set `{column}` takes an array"))),
        }
    };
    let mut all = items(written)?;
    if let Some(held) = held {
        all.extend(items(held)?);
    }
    let mut keyed: Vec<(Vec<u8>, Value)> = all.into_iter().map(|v| (element_key(&v), v)).collect();
    keyed.sort_by(|a, b| a.0.cmp(&b.0));
    keyed.dedup_by(|a, b| a.0 == b.0);
    Ok(Value::Array(keyed.into_iter().map(|(_, v)| v).collect()))
}
