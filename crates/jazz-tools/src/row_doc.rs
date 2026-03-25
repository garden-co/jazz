use crate::object::ObjectId;
use crate::query_manager::types::Value;
use std::collections::HashMap;
use yrs::{Any as YrsAny, Doc, Map, MapRef, Out, ReadTxn, TransactionMut};

pub struct RowDoc {
    pub id: ObjectId,
    pub doc: Doc,
    pub root_map: MapRef, // cached at construction — avoids deadlock with active transactions
    pub metadata: HashMap<String, String>,
    pub branches: HashMap<String, ObjectId>,
    pub origin: Option<(ObjectId, Vec<u8>)>, // (parent_id, state_vector_at_fork)
}

/// Write a Jazz2 `Value` into a Yrs `MapRef` at the given key.
///
/// Ambiguous types (Integer, Timestamp, Uuid) store a companion type-tag key
/// `_type:{key}` so that `read_column` can reconstruct the original variant.
/// Null removes the key (and any tag) from the map.
pub fn write_column(map: &MapRef, txn: &mut TransactionMut, key: &str, value: &Value) {
    let tag_key = format!("_type:{key}");
    // Always clear previous type tag first.
    map.remove(txn, &tag_key);

    match value {
        Value::Text(s) => {
            map.insert(txn, key, s.as_str());
        }
        Value::Integer(i) => {
            map.insert(txn, key, *i as f64);
            map.insert(txn, tag_key.as_str(), "integer");
        }
        Value::BigInt(i) => {
            // Use Any::BigInt directly to avoid the From<i64> auto-downgrade to Number.
            map.insert(txn, key, YrsAny::BigInt(*i));
        }
        Value::Double(f) => {
            map.insert(txn, key, *f);
        }
        Value::Boolean(b) => {
            map.insert(txn, key, *b);
        }
        Value::Timestamp(t) => {
            map.insert(txn, key, YrsAny::BigInt(*t as i64));
            map.insert(txn, tag_key.as_str(), "timestamp");
        }
        Value::Uuid(id) => {
            map.insert(txn, key, id.uuid().as_bytes().as_slice());
            map.insert(txn, tag_key.as_str(), "uuid");
        }
        Value::Bytea(b) => {
            map.insert(txn, key, b.as_slice());
        }
        Value::Array(arr) => {
            let yrs_arr: Vec<YrsAny> = arr.iter().map(value_to_any).collect();
            map.insert(txn, key, YrsAny::Array(yrs_arr.into()));
        }
        Value::Row { id, values } => {
            let mut row_map = std::collections::HashMap::new();
            if let Some(obj_id) = id {
                row_map.insert(
                    "_id".to_string(),
                    YrsAny::Buffer(obj_id.uuid().as_bytes().to_vec().into()),
                );
            }
            for (i, v) in values.iter().enumerate() {
                row_map.insert(i.to_string(), value_to_any(v));
            }
            map.insert(txn, key, YrsAny::Map(row_map.into()));
        }
        Value::Null => {
            map.remove(txn, key);
        }
    }
}

/// Read a Jazz2 `Value` from a Yrs `MapRef` at the given key.
///
/// Returns `None` if the key is absent (which also represents `Null`).
pub fn read_column(map: &MapRef, txn: &impl ReadTxn, key: &str) -> Option<Value> {
    let out = map.get(txn, key)?;
    let tag_key = format!("_type:{key}");
    let type_tag = map.get(txn, &tag_key).and_then(|o| match o {
        Out::Any(YrsAny::String(s)) => Some(s.to_string()),
        _ => None,
    });
    Some(out_to_value(out, type_tag.as_deref()))
}

fn out_to_value(out: Out, type_tag: Option<&str>) -> Value {
    match out {
        Out::Any(any) => any_to_value(any, type_tag),
        _ => Value::Null, // Nested Yrs types (YMap, YArray, etc.) not mapped here.
    }
}

fn any_to_value(any: YrsAny, type_tag: Option<&str>) -> Value {
    match any {
        YrsAny::String(s) => Value::Text(s.to_string()),
        YrsAny::Number(n) => match type_tag {
            Some("integer") => Value::Integer(n.round() as i32),
            _ => Value::Double(n),
        },
        YrsAny::BigInt(i) => match type_tag {
            Some("timestamp") => Value::Timestamp(i as u64),
            _ => Value::BigInt(i),
        },
        YrsAny::Bool(b) => Value::Boolean(b),
        YrsAny::Buffer(buf) => match type_tag {
            Some("uuid") if buf.len() == 16 => {
                let bytes: [u8; 16] = buf[..16].try_into().unwrap();
                let uuid = uuid::Uuid::from_bytes(bytes);
                Value::Uuid(ObjectId::from_uuid(uuid))
            }
            _ => Value::Bytea(buf.to_vec()),
        },
        YrsAny::Array(arr) => {
            Value::Array(arr.iter().map(|a| any_to_value(a.clone(), None)).collect())
        }
        YrsAny::Map(m) => {
            let id = m.get("_id").and_then(|v| match v {
                YrsAny::Buffer(buf) if buf.len() == 16 => {
                    let bytes: [u8; 16] = buf[..16].try_into().unwrap();
                    Some(ObjectId::from_uuid(uuid::Uuid::from_bytes(bytes)))
                }
                _ => None,
            });
            let mut values = Vec::new();
            let mut i = 0usize;
            while let Some(v) = m.get(&i.to_string()) {
                values.push(any_to_value(v.clone(), None));
                i += 1;
            }
            Value::Row { id, values }
        }
        YrsAny::Null | YrsAny::Undefined => Value::Null,
    }
}

fn value_to_any(value: &Value) -> YrsAny {
    match value {
        Value::Text(s) => YrsAny::String(s.as_str().into()),
        Value::Integer(i) => YrsAny::Number(*i as f64),
        Value::BigInt(i) => YrsAny::BigInt(*i),
        Value::Double(f) => YrsAny::Number(*f),
        Value::Boolean(b) => YrsAny::Bool(*b),
        Value::Timestamp(t) => YrsAny::BigInt(*t as i64),
        Value::Uuid(id) => YrsAny::Buffer(id.uuid().as_bytes().to_vec().into()),
        Value::Bytea(b) => YrsAny::Buffer(b.clone().into()),
        Value::Array(arr) => YrsAny::Array(arr.iter().map(value_to_any).collect()),
        Value::Row { id, values } => {
            let mut m = std::collections::HashMap::new();
            if let Some(obj_id) = id {
                m.insert(
                    "_id".to_string(),
                    YrsAny::Buffer(obj_id.uuid().as_bytes().to_vec().into()),
                );
            }
            for (i, v) in values.iter().enumerate() {
                m.insert(i.to_string(), value_to_any(v));
            }
            YrsAny::Map(m.into())
        }
        Value::Null => YrsAny::Null,
    }
}

impl RowDoc {
    pub fn new(id: ObjectId, metadata: HashMap<String, String>) -> Self {
        let doc = Doc::new();
        let root_map = doc.get_or_insert_map("row");
        Self {
            id,
            doc,
            root_map,
            metadata,
            branches: HashMap::new(),
            origin: None,
        }
    }
}
