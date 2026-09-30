//! Durable Groove encoding of policy claims in the policy-binding directory.
//! Re-exported from `crate::protocol`.

use std::collections::BTreeMap;

use groove::records::{
    EnumCase, EnumSchema, EnumValue, OwnedRecord, RecordDescriptor, ScalarEnumSchema, Value,
    ValueType,
};

/// The durable, typed payload used by the policy-binding directory.
///
/// This deliberately flattens a recursively shaped claims map into ordinary
/// Groove records instead of inventing another opaque byte codec.  Each root
/// node carries its claim name; containers name their children only by their
/// position.  The representation supports the policy-claim value vocabulary
/// admitted at public boundaries (scalars, nullable values, arrays, and
/// tuples), and rejects engine-owned values such as rows or large-value refs.
/// Those values are not valid policy claims because their physical identity is
/// local storage state rather than a portable session assertion.
pub fn policy_binding_directory_claims_value(
    claims: &BTreeMap<String, Value>,
) -> Result<Value, String> {
    let mut nodes = Vec::new();
    for (name, value) in claims {
        encode_policy_claim_node(&mut nodes, Some(name), value)?;
    }
    Ok(Value::Array(nodes))
}

/// Decode and validate the normal Groove representation of policy claims.
pub fn policy_binding_directory_claims_from_value(
    value: Value,
) -> Result<BTreeMap<String, Value>, String> {
    let Value::Array(nodes) = value else {
        return Err("policy binding directory claims must be an array".to_owned());
    };
    if nodes.len() > POLICY_CLAIM_DIRECTORY_MAX_NODES {
        return Err("policy binding directory claims exceed node limit".to_owned());
    }
    let mut cursor = 0;
    let mut claims = BTreeMap::new();
    while cursor < nodes.len() {
        let (name, value) = decode_policy_claim_node(&nodes, &mut cursor, true)?;
        let Some(name) = name else {
            return Err("policy binding directory root claim is unnamed".to_owned());
        };
        if claims.insert(name, value).is_some() {
            return Err("policy binding directory contains duplicate claim names".to_owned());
        }
    }
    Ok(claims)
}

pub fn policy_directory_descriptor() -> RecordDescriptor {
    RecordDescriptor::new([
        ("derived_v1", ValueType::U8),
        (
            "claims_v1",
            ValueType::Array(Box::new(ValueType::Record(Box::new(
                *policy_claim_node_descriptor(),
            )))),
        ),
    ])
}

pub fn policy_directory_payload(presence: u8, claims: Value) -> Result<Value, String> {
    let descriptor = policy_directory_descriptor();
    Ok(Value::Record(OwnedRecord::new(
        descriptor
            .create(&[Value::U8(presence), claims])
            .map_err(|error| error.to_string())?,
        descriptor,
    )))
}

/// Direct-store value type for the collision-checked policy-binding directory.
pub fn policy_binding_directory_claims_value_type() -> ValueType {
    ValueType::Record(Box::new(policy_directory_descriptor()))
}

const POLICY_CLAIM_DIRECTORY_MAX_NODES: usize = 1024;

const POLICY_CLAIM_U8: u32 = 0;
const POLICY_CLAIM_U16: u32 = 1;
const POLICY_CLAIM_U32: u32 = 2;
const POLICY_CLAIM_U64: u32 = 3;
const POLICY_CLAIM_I32: u32 = 4;
const POLICY_CLAIM_I64: u32 = 5;
const POLICY_CLAIM_F64: u32 = 6;
const POLICY_CLAIM_BOOL: u32 = 7;
const POLICY_CLAIM_STRING: u32 = 8;
const POLICY_CLAIM_BYTES: u32 = 9;
const POLICY_CLAIM_UUID: u32 = 10;
const POLICY_CLAIM_ENUM_TAG: u32 = 11;
const POLICY_CLAIM_NULL: u32 = 12;
const POLICY_CLAIM_TUPLE: u32 = 13;
const POLICY_CLAIM_ARRAY: u32 = 14;
const POLICY_CLAIM_NULLABLE: u32 = 15;

fn policy_claim_kind_type() -> ValueType {
    ValueType::EnumTag(
        ScalarEnumSchema::new(
            "jazz.internal.policy_claim_directory_node_kind.v1",
            [
                "u8", "u16", "u32", "u64", "i32", "i64", "f64", "bool", "string", "bytes", "uuid",
                "enum_tag", "null", "tuple", "array", "nullable",
            ],
        )
        .expect("fixed policy-claim node kinds are valid"),
    )
}

fn policy_claim_value_schema() -> &'static EnumSchema {
    static SCHEMA: std::sync::OnceLock<EnumSchema> = std::sync::OnceLock::new();
    SCHEMA.get_or_init(|| {
        let empty = || RecordDescriptor::new(Vec::<(String, ValueType)>::new());
        let scalar = |name: &str, value_type: ValueType| {
            EnumCase::new(name, RecordDescriptor::new([("value", value_type)]))
        };
        EnumSchema::new(
            "jazz.internal.policy_claim_directory_value.v1",
            [
                scalar("u8", ValueType::U8),
                scalar("u16", ValueType::U16),
                scalar("u32", ValueType::U32),
                scalar("u64", ValueType::U64),
                scalar("i32", ValueType::I32),
                scalar("i64", ValueType::I64),
                scalar("f64", ValueType::F64),
                scalar("bool", ValueType::Bool),
                scalar("string", ValueType::String),
                scalar("bytes", ValueType::Bytes),
                scalar("uuid", ValueType::Uuid),
                scalar("enum_tag", ValueType::U8),
                EnumCase::new("null", empty()),
                EnumCase::new("tuple", empty()),
                EnumCase::new("array", empty()),
                EnumCase::new("nullable", empty()),
            ],
        )
        .expect("fixed policy-claim value enum is valid")
    })
}

fn policy_claim_node_descriptor() -> &'static RecordDescriptor {
    static DESCRIPTOR: std::sync::OnceLock<RecordDescriptor> = std::sync::OnceLock::new();
    DESCRIPTOR.get_or_init(|| {
        RecordDescriptor::new([
            ("name", ValueType::Nullable(Box::new(ValueType::String))),
            ("kind", policy_claim_kind_type()),
            ("children", ValueType::U32),
            (
                "value",
                ValueType::Enum(Box::new(policy_claim_value_schema().clone())),
            ),
        ])
    })
}

fn encode_policy_claim_node(
    nodes: &mut Vec<Value>,
    name: Option<&str>,
    value: &Value,
) -> Result<(), String> {
    if nodes.len() >= POLICY_CLAIM_DIRECTORY_MAX_NODES {
        return Err("policy binding directory claims exceed node limit".to_owned());
    }
    let (kind, enum_value, children): (u8, EnumValue, Vec<&Value>) = match value {
        Value::U8(value) => (
            POLICY_CLAIM_U8 as u8,
            policy_claim_scalar(POLICY_CLAIM_U8, Value::U8(*value))?,
            vec![],
        ),
        Value::U16(value) => (
            POLICY_CLAIM_U16 as u8,
            policy_claim_scalar(POLICY_CLAIM_U16, Value::U16(*value))?,
            vec![],
        ),
        Value::U32(value) => (
            POLICY_CLAIM_U32 as u8,
            policy_claim_scalar(POLICY_CLAIM_U32, Value::U32(*value))?,
            vec![],
        ),
        Value::U64(value) => (
            POLICY_CLAIM_U64 as u8,
            policy_claim_scalar(POLICY_CLAIM_U64, Value::U64(*value))?,
            vec![],
        ),
        Value::I32(value) => (
            POLICY_CLAIM_I32 as u8,
            policy_claim_scalar(POLICY_CLAIM_I32, Value::I32(*value))?,
            vec![],
        ),
        Value::I64(value) => (
            POLICY_CLAIM_I64 as u8,
            policy_claim_scalar(POLICY_CLAIM_I64, Value::I64(*value))?,
            vec![],
        ),
        Value::F64(value) => (
            POLICY_CLAIM_F64 as u8,
            policy_claim_scalar(POLICY_CLAIM_F64, Value::F64(*value))?,
            vec![],
        ),
        Value::Bool(value) => (
            POLICY_CLAIM_BOOL as u8,
            policy_claim_scalar(POLICY_CLAIM_BOOL, Value::Bool(*value))?,
            vec![],
        ),
        Value::String(value) => (
            POLICY_CLAIM_STRING as u8,
            policy_claim_scalar(POLICY_CLAIM_STRING, Value::String(value.clone()))?,
            vec![],
        ),
        Value::Bytes(value) => (
            POLICY_CLAIM_BYTES as u8,
            policy_claim_scalar(POLICY_CLAIM_BYTES, Value::Bytes(value.clone()))?,
            vec![],
        ),
        Value::Uuid(value) => (
            POLICY_CLAIM_UUID as u8,
            policy_claim_scalar(POLICY_CLAIM_UUID, Value::Uuid(*value))?,
            vec![],
        ),
        Value::EnumTag(value) => (
            POLICY_CLAIM_ENUM_TAG as u8,
            policy_claim_scalar(POLICY_CLAIM_ENUM_TAG, Value::U8(*value))?,
            vec![],
        ),
        Value::Nullable(None) => (
            POLICY_CLAIM_NULL as u8,
            policy_claim_container(POLICY_CLAIM_NULL)?,
            vec![],
        ),
        Value::Nullable(Some(value)) => (
            POLICY_CLAIM_NULLABLE as u8,
            policy_claim_container(POLICY_CLAIM_NULLABLE)?,
            vec![value],
        ),
        Value::Tuple(values) => (
            POLICY_CLAIM_TUPLE as u8,
            policy_claim_container(POLICY_CLAIM_TUPLE)?,
            values.iter().collect(),
        ),
        Value::Array(values) => (
            POLICY_CLAIM_ARRAY as u8,
            policy_claim_container(POLICY_CLAIM_ARRAY)?,
            values.iter().collect(),
        ),
        Value::Record(_) | Value::Enum(_) | Value::Large(_) => {
            return Err(
                "policy binding directory does not admit engine-owned claim values".to_owned(),
            );
        }
    };
    let child_count = u32::try_from(children.len())
        .map_err(|_| "policy binding directory has too many child claims".to_owned())?;
    let descriptor = policy_claim_node_descriptor();
    let raw = descriptor
        .create(&[
            Value::Nullable(name.map(|name| Box::new(Value::String(name.to_owned())))),
            Value::EnumTag(kind),
            Value::U32(child_count),
            Value::Enum(enum_value),
        ])
        .map_err(|error| format!("policy binding directory claim node is invalid: {error}"))?;
    nodes.push(Value::Record(OwnedRecord::new(raw, *descriptor)));
    for child in children {
        encode_policy_claim_node(nodes, None, child)?;
    }
    Ok(())
}

fn policy_claim_scalar(tag: u32, value: Value) -> Result<EnumValue, String> {
    let schema = policy_claim_value_schema();
    EnumValue::create(
        tag,
        schema.case(tag).expect("fixed tag").payload.clone(),
        &[value],
    )
    .map_err(|error| format!("policy binding directory scalar is invalid: {error}"))
}

fn policy_claim_container(tag: u32) -> Result<EnumValue, String> {
    let schema = policy_claim_value_schema();
    EnumValue::create(
        tag,
        schema.case(tag).expect("fixed tag").payload.clone(),
        &[],
    )
    .map_err(|error| format!("policy binding directory container is invalid: {error}"))
}

fn decode_policy_claim_node(
    nodes: &[Value],
    cursor: &mut usize,
    root: bool,
) -> Result<(Option<String>, Value), String> {
    let node = nodes
        .get(*cursor)
        .ok_or_else(|| "policy binding directory claim tree ended early".to_owned())?;
    *cursor += 1;
    let Value::Record(record) = node else {
        return Err("policy binding directory node must be a record".to_owned());
    };
    if record.descriptor() != policy_claim_node_descriptor() {
        return Err("policy binding directory node has unexpected descriptor".to_owned());
    }
    let values = record
        .to_values()
        .map_err(|error| format!("policy binding directory node cannot decode: {error}"))?;
    let [
        name,
        Value::EnumTag(kind),
        Value::U32(children),
        Value::Enum(enum_value),
    ] = values.as_slice()
    else {
        return Err("policy binding directory node has invalid fields".to_owned());
    };
    let name = match name {
        Value::Nullable(Some(name)) => match name.as_ref() {
            Value::String(name) => Some(name.clone()),
            _ => return Err("policy binding directory name must be string".to_owned()),
        },
        Value::Nullable(None) => None,
        _ => return Err("policy binding directory name must be nullable string".to_owned()),
    };
    if root != name.is_some() {
        return Err(if root {
            "policy binding directory root claim is unnamed".to_owned()
        } else {
            "policy binding directory child claim is named".to_owned()
        });
    }
    let expected_tag = u32::from(*kind);
    if enum_value.tag() != expected_tag || expected_tag > POLICY_CLAIM_NULLABLE {
        return Err("policy binding directory kind and value disagree".to_owned());
    }
    let payload = enum_value
        .record()
        .to_values()
        .map_err(|error| format!("policy binding directory value cannot decode: {error}"))?;
    let child_count = usize::try_from(*children)
        .map_err(|_| "policy binding directory child count overflows".to_owned())?;
    let scalar = |expected: u32| -> Result<Value, String> {
        if expected_tag != expected || child_count != 0 || payload.len() != 1 {
            return Err(
                "policy binding directory scalar has invalid children or payload".to_owned(),
            );
        }
        Ok(payload[0].clone())
    };
    let value = match expected_tag {
        POLICY_CLAIM_U8 => scalar(POLICY_CLAIM_U8)?,
        POLICY_CLAIM_U16 => scalar(POLICY_CLAIM_U16)?,
        POLICY_CLAIM_U32 => scalar(POLICY_CLAIM_U32)?,
        POLICY_CLAIM_U64 => scalar(POLICY_CLAIM_U64)?,
        POLICY_CLAIM_I32 => scalar(POLICY_CLAIM_I32)?,
        POLICY_CLAIM_I64 => scalar(POLICY_CLAIM_I64)?,
        POLICY_CLAIM_F64 => scalar(POLICY_CLAIM_F64)?,
        POLICY_CLAIM_BOOL => scalar(POLICY_CLAIM_BOOL)?,
        POLICY_CLAIM_STRING => scalar(POLICY_CLAIM_STRING)?,
        POLICY_CLAIM_BYTES => scalar(POLICY_CLAIM_BYTES)?,
        POLICY_CLAIM_UUID => scalar(POLICY_CLAIM_UUID)?,
        POLICY_CLAIM_ENUM_TAG => match scalar(POLICY_CLAIM_ENUM_TAG)? {
            Value::U8(value) => Value::EnumTag(value),
            _ => return Err("policy binding directory enum tag must be u8".to_owned()),
        },
        POLICY_CLAIM_NULL => {
            if child_count != 0 || !payload.is_empty() {
                return Err("policy binding directory null has payload or children".to_owned());
            }
            Value::Nullable(None)
        }
        POLICY_CLAIM_TUPLE | POLICY_CLAIM_ARRAY | POLICY_CLAIM_NULLABLE => {
            if !payload.is_empty() {
                return Err("policy binding directory container has payload".to_owned());
            }
            if expected_tag == POLICY_CLAIM_NULLABLE && child_count != 1 {
                return Err("policy binding directory nullable must have one child".to_owned());
            }
            let mut children = Vec::with_capacity(child_count);
            for _ in 0..child_count {
                let (_, child) = decode_policy_claim_node(nodes, cursor, false)?;
                children.push(child);
            }
            match expected_tag {
                POLICY_CLAIM_TUPLE => Value::Tuple(children),
                POLICY_CLAIM_ARRAY => Value::Array(children),
                POLICY_CLAIM_NULLABLE => {
                    Value::Nullable(Some(Box::new(children.pop().expect("one child"))))
                }
                _ => unreachable!(),
            }
        }
        _ => return Err("policy binding directory node kind is unknown".to_owned()),
    };
    Ok((name, value))
}
