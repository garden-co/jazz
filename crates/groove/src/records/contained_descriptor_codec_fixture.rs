// Test-only snapshot of descriptor codec at 4c6eafaef5ad038a71b4583f650b22ccfd7b71db.
// Sole source adaptation: DescriptorField construction supplies identity: None.
// Preserve this independent writer/reader; it is evidence, not a production fallback.
use super::super::*;

// A self-contained record descriptor occasionally has to cross a durable
// engine boundary (for example, a maintained result payload). Serde derives
// are intentionally not that boundary: their layout belongs to Rust types,
// not Groove's settled storage algebra. These nodes are therefore ordinary
// Groove records under one fixed descriptor. The tree is flattened in prefix
// order, so the carrier remains a non-recursive `array<record>` while still
// representing recursive `ValueType`s exactly.
const DESCRIPTOR_CODEC_MAX_NODES: usize = 1024;

const DESCRIPTOR_NODE_DESCRIPTOR: u8 = 0;
const DESCRIPTOR_NODE_FIELD: u8 = 1;
const DESCRIPTOR_NODE_U8: u8 = 2;
const DESCRIPTOR_NODE_U16: u8 = 3;
const DESCRIPTOR_NODE_U32: u8 = 4;
const DESCRIPTOR_NODE_U64: u8 = 5;
const DESCRIPTOR_NODE_I32: u8 = 6;
const DESCRIPTOR_NODE_I64: u8 = 7;
const DESCRIPTOR_NODE_F64: u8 = 8;
const DESCRIPTOR_NODE_BOOL: u8 = 9;
const DESCRIPTOR_NODE_STRING: u8 = 10;
const DESCRIPTOR_NODE_BYTES: u8 = 11;
const DESCRIPTOR_NODE_RAW_STRING: u8 = 12;
const DESCRIPTOR_NODE_RAW_BYTES: u8 = 13;
const DESCRIPTOR_NODE_STORED_BYTES: u8 = 14;
const DESCRIPTOR_NODE_STORED_STRING: u8 = 15;
const DESCRIPTOR_NODE_STORED_JSON: u8 = 16;
const DESCRIPTOR_NODE_UUID: u8 = 17;
const DESCRIPTOR_NODE_ENUM_TAG: u8 = 18;
const DESCRIPTOR_NODE_TUPLE: u8 = 19;
const DESCRIPTOR_NODE_ARRAY: u8 = 20;
const DESCRIPTOR_NODE_NULLABLE: u8 = 21;
const DESCRIPTOR_NODE_RECORD: u8 = 22;
const DESCRIPTOR_NODE_ENUM: u8 = 23;
const DESCRIPTOR_NODE_ENUM_CASE: u8 = 24;

#[derive(Clone, Debug, PartialEq, Eq)]
struct DescriptorCodecNode {
    tag: u8,
    name: Option<String>,
    registry_id: u64,
    children: u32,
    strings: Vec<String>,
}

fn descriptor_codec_node_descriptor() -> &'static RecordDescriptor {
    static DESCRIPTOR: OnceLock<RecordDescriptor> = OnceLock::new();
    DESCRIPTOR.get_or_init(|| {
        RecordDescriptor::new([
            ("tag", ValueType::U8),
            (
                "name",
                ValueType::Nullable(Box::new(ValueType::raw_string())),
            ),
            ("registry_id", ValueType::U64),
            ("children", ValueType::U32),
            (
                "strings",
                ValueType::Array(Box::new(ValueType::raw_string())),
            ),
        ])
    })
}

fn descriptor_codec_envelope() -> &'static RecordDescriptor {
    static DESCRIPTOR: OnceLock<RecordDescriptor> = OnceLock::new();
    DESCRIPTOR.get_or_init(|| {
        RecordDescriptor::new([(
            "nodes",
            ValueType::Array(Box::new(ValueType::Record(Box::new(
                *descriptor_codec_node_descriptor(),
            )))),
        )])
    })
}

fn descriptor_codec_node_value(node: DescriptorCodecNode) -> Result<Value, Error> {
    let descriptor = *descriptor_codec_node_descriptor();
    let raw = descriptor.create(&[
        Value::U8(node.tag),
        Value::Nullable(node.name.map(|name| Box::new(Value::String(name)))),
        Value::U64(node.registry_id),
        Value::U32(node.children),
        Value::Array(node.strings.into_iter().map(Value::String).collect()),
    ])?;
    Ok(Value::Record(OwnedRecord::new(raw, descriptor)))
}

fn descriptor_codec_node_from_value(value: Value) -> Result<DescriptorCodecNode, Error> {
    let Value::Record(record) = value else {
        return Err(Error::NonCanonicalRecord);
    };
    if record.descriptor() != descriptor_codec_node_descriptor() {
        return Err(Error::NonCanonicalRecord);
    }
    let values = record.to_values()?;
    let [
        Value::U8(tag),
        Value::Nullable(name),
        Value::U64(registry_id),
        Value::U32(children),
        Value::Array(strings),
    ] = values.as_slice()
    else {
        return Err(Error::NonCanonicalRecord);
    };
    let name = match name.as_deref() {
        None => None,
        Some(Value::String(name)) => Some(name.clone()),
        Some(_) => return Err(Error::NonCanonicalRecord),
    };
    let strings = strings
        .iter()
        .map(|value| match value {
            Value::String(value) => Ok(value.clone()),
            _ => Err(Error::NonCanonicalRecord),
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(DescriptorCodecNode {
        tag: *tag,
        name,
        registry_id: *registry_id,
        children: *children,
        strings,
    })
}

fn descriptor_codec_push(
    nodes: &mut Vec<DescriptorCodecNode>,
    node: DescriptorCodecNode,
) -> Result<(), Error> {
    if nodes.len() == DESCRIPTOR_CODEC_MAX_NODES {
        return Err(Error::LengthOverflow);
    }
    nodes.push(node);
    Ok(())
}

fn descriptor_codec_push_descriptor(
    nodes: &mut Vec<DescriptorCodecNode>,
    descriptor: &RecordDescriptor,
) -> Result<(), Error> {
    descriptor_codec_push(
        nodes,
        DescriptorCodecNode {
            tag: DESCRIPTOR_NODE_DESCRIPTOR,
            name: None,
            registry_id: 0,
            children: u32::try_from(descriptor.fields().len())
                .map_err(|_| Error::LengthOverflow)?,
            strings: Vec::new(),
        },
    )?;
    for field in descriptor.fields() {
        descriptor_codec_push(
            nodes,
            DescriptorCodecNode {
                tag: DESCRIPTOR_NODE_FIELD,
                name: field.name.clone(),
                registry_id: 0,
                children: 1,
                strings: Vec::new(),
            },
        )?;
        descriptor_codec_push_value_type(nodes, &field.value_type)?;
    }
    Ok(())
}

fn descriptor_codec_push_value_type(
    nodes: &mut Vec<DescriptorCodecNode>,
    value_type: &ValueType,
) -> Result<(), Error> {
    let scalar = |tag| DescriptorCodecNode {
        tag,
        name: None,
        registry_id: 0,
        children: 0,
        strings: Vec::new(),
    };
    match value_type {
        ValueType::U8 => descriptor_codec_push(nodes, scalar(DESCRIPTOR_NODE_U8)),
        ValueType::U16 => descriptor_codec_push(nodes, scalar(DESCRIPTOR_NODE_U16)),
        ValueType::U32 => descriptor_codec_push(nodes, scalar(DESCRIPTOR_NODE_U32)),
        ValueType::U64 => descriptor_codec_push(nodes, scalar(DESCRIPTOR_NODE_U64)),
        ValueType::I32 => descriptor_codec_push(nodes, scalar(DESCRIPTOR_NODE_I32)),
        ValueType::I64 => descriptor_codec_push(nodes, scalar(DESCRIPTOR_NODE_I64)),
        ValueType::F64 => descriptor_codec_push(nodes, scalar(DESCRIPTOR_NODE_F64)),
        ValueType::Bool => descriptor_codec_push(nodes, scalar(DESCRIPTOR_NODE_BOOL)),
        ValueType::String => descriptor_codec_push(nodes, scalar(DESCRIPTOR_NODE_STRING)),
        ValueType::Bytes => descriptor_codec_push(nodes, scalar(DESCRIPTOR_NODE_BYTES)),
        ValueType::Internal(InternalValueType(InternalValueTypeRepr::RawString)) => {
            descriptor_codec_push(nodes, scalar(DESCRIPTOR_NODE_RAW_STRING))
        }
        ValueType::Internal(InternalValueType(InternalValueTypeRepr::RawBytes)) => {
            descriptor_codec_push(nodes, scalar(DESCRIPTOR_NODE_RAW_BYTES))
        }
        ValueType::Internal(InternalValueType(InternalValueTypeRepr::StoredScalar(
            crate::large_values::LargeValueKind::Bytes,
        ))) => descriptor_codec_push(nodes, scalar(DESCRIPTOR_NODE_STORED_BYTES)),
        ValueType::Internal(InternalValueType(InternalValueTypeRepr::StoredScalar(
            crate::large_values::LargeValueKind::String,
        ))) => descriptor_codec_push(nodes, scalar(DESCRIPTOR_NODE_STORED_STRING)),
        ValueType::Internal(InternalValueType(InternalValueTypeRepr::StoredScalar(
            crate::large_values::LargeValueKind::Json,
        ))) => descriptor_codec_push(nodes, scalar(DESCRIPTOR_NODE_STORED_JSON)),
        ValueType::Uuid => descriptor_codec_push(nodes, scalar(DESCRIPTOR_NODE_UUID)),
        ValueType::EnumTag(schema) => descriptor_codec_push(
            nodes,
            DescriptorCodecNode {
                tag: DESCRIPTOR_NODE_ENUM_TAG,
                name: Some(schema.name.clone()),
                registry_id: schema.registry_id,
                children: 0,
                strings: schema.variants.clone(),
            },
        ),
        ValueType::Tuple(members) => {
            descriptor_codec_push(
                nodes,
                DescriptorCodecNode {
                    tag: DESCRIPTOR_NODE_TUPLE,
                    name: None,
                    registry_id: 0,
                    children: u32::try_from(members.len()).map_err(|_| Error::LengthOverflow)?,
                    strings: Vec::new(),
                },
            )?;
            for member in members {
                descriptor_codec_push_value_type(nodes, member)?;
            }
            Ok(())
        }
        ValueType::Array(inner) | ValueType::Nullable(inner) => {
            let tag = if matches!(value_type, ValueType::Array(_)) {
                DESCRIPTOR_NODE_ARRAY
            } else {
                DESCRIPTOR_NODE_NULLABLE
            };
            descriptor_codec_push(
                nodes,
                DescriptorCodecNode {
                    tag,
                    name: None,
                    registry_id: 0,
                    children: 1,
                    strings: Vec::new(),
                },
            )?;
            descriptor_codec_push_value_type(nodes, inner)
        }
        ValueType::Record(descriptor) => {
            descriptor_codec_push(
                nodes,
                DescriptorCodecNode {
                    tag: DESCRIPTOR_NODE_RECORD,
                    name: None,
                    registry_id: 0,
                    children: 1,
                    strings: Vec::new(),
                },
            )?;
            descriptor_codec_push_descriptor(nodes, descriptor)
        }
        ValueType::Enum(schema) => {
            descriptor_codec_push(
                nodes,
                DescriptorCodecNode {
                    tag: DESCRIPTOR_NODE_ENUM,
                    name: Some(schema.name.clone()),
                    registry_id: schema.registry_id,
                    children: u32::try_from(schema.cases.len())
                        .map_err(|_| Error::LengthOverflow)?,
                    strings: Vec::new(),
                },
            )?;
            for case in &schema.cases {
                descriptor_codec_push(
                    nodes,
                    DescriptorCodecNode {
                        tag: DESCRIPTOR_NODE_ENUM_CASE,
                        name: Some(case.name.clone()),
                        registry_id: 0,
                        children: 1,
                        strings: Vec::new(),
                    },
                )?;
                descriptor_codec_push_descriptor(nodes, &case.payload)?;
            }
            Ok(())
        }
    }
}

fn descriptor_codec_take<'a>(
    nodes: &'a [DescriptorCodecNode],
    cursor: &mut usize,
) -> Result<&'a DescriptorCodecNode, Error> {
    let node = nodes.get(*cursor).ok_or(Error::UnexpectedEof)?;
    *cursor += 1;
    Ok(node)
}

fn descriptor_codec_metadata_empty(node: &DescriptorCodecNode) -> Result<(), Error> {
    if node.name.is_none() && node.registry_id == 0 && node.children == 0 && node.strings.is_empty()
    {
        Ok(())
    } else {
        Err(Error::NonCanonicalRecord)
    }
}

fn descriptor_codec_decode_descriptor(
    nodes: &[DescriptorCodecNode],
    cursor: &mut usize,
) -> Result<RecordDescriptor, Error> {
    let node = descriptor_codec_take(nodes, cursor)?;
    if node.tag != DESCRIPTOR_NODE_DESCRIPTOR
        || node.name.is_some()
        || node.registry_id != 0
        || !node.strings.is_empty()
    {
        return Err(Error::NonCanonicalRecord);
    }
    let mut fields =
        Vec::with_capacity(usize::try_from(node.children).map_err(|_| Error::LengthOverflow)?);
    for _ in 0..node.children {
        let field = descriptor_codec_take(nodes, cursor)?;
        if field.tag != DESCRIPTOR_NODE_FIELD
            || field.registry_id != 0
            || field.children != 1
            || !field.strings.is_empty()
        {
            return Err(Error::NonCanonicalRecord);
        }
        fields.push(DescriptorField {
            identity: None,
            name: field.name.clone(),
            value_type: descriptor_codec_decode_value_type(nodes, cursor)?,
        });
    }
    // `from_logical_fields` is the trusted schema constructor and deliberately
    // panics when a programmer supplies an impossible descriptor. These bytes
    // are durable/untrusted instead, so validate before crossing that trusted
    // constructor boundary.
    for field in &fields {
        validate_schema_value_type(&field.value_type)?;
    }
    Ok(RecordDescriptor::from_logical_fields(fields))
}

fn descriptor_codec_decode_value_type(
    nodes: &[DescriptorCodecNode],
    cursor: &mut usize,
) -> Result<ValueType, Error> {
    let node = descriptor_codec_take(nodes, cursor)?;
    let scalar = |expected, value| {
        if node.tag == expected {
            descriptor_codec_metadata_empty(node)?;
            Ok(value)
        } else {
            Err(Error::NonCanonicalRecord)
        }
    };
    match node.tag {
        DESCRIPTOR_NODE_U8 => scalar(DESCRIPTOR_NODE_U8, ValueType::U8),
        DESCRIPTOR_NODE_U16 => scalar(DESCRIPTOR_NODE_U16, ValueType::U16),
        DESCRIPTOR_NODE_U32 => scalar(DESCRIPTOR_NODE_U32, ValueType::U32),
        DESCRIPTOR_NODE_U64 => scalar(DESCRIPTOR_NODE_U64, ValueType::U64),
        DESCRIPTOR_NODE_I32 => scalar(DESCRIPTOR_NODE_I32, ValueType::I32),
        DESCRIPTOR_NODE_I64 => scalar(DESCRIPTOR_NODE_I64, ValueType::I64),
        DESCRIPTOR_NODE_F64 => scalar(DESCRIPTOR_NODE_F64, ValueType::F64),
        DESCRIPTOR_NODE_BOOL => scalar(DESCRIPTOR_NODE_BOOL, ValueType::Bool),
        DESCRIPTOR_NODE_STRING => scalar(DESCRIPTOR_NODE_STRING, ValueType::String),
        DESCRIPTOR_NODE_BYTES => scalar(DESCRIPTOR_NODE_BYTES, ValueType::Bytes),
        DESCRIPTOR_NODE_RAW_STRING => scalar(DESCRIPTOR_NODE_RAW_STRING, ValueType::raw_string()),
        DESCRIPTOR_NODE_RAW_BYTES => scalar(DESCRIPTOR_NODE_RAW_BYTES, ValueType::raw_bytes()),
        DESCRIPTOR_NODE_STORED_BYTES => scalar(
            DESCRIPTOR_NODE_STORED_BYTES,
            ValueType::stored_scalar(crate::large_values::LargeValueKind::Bytes),
        ),
        DESCRIPTOR_NODE_STORED_STRING => scalar(
            DESCRIPTOR_NODE_STORED_STRING,
            ValueType::stored_scalar(crate::large_values::LargeValueKind::String),
        ),
        DESCRIPTOR_NODE_STORED_JSON => scalar(
            DESCRIPTOR_NODE_STORED_JSON,
            ValueType::stored_scalar(crate::large_values::LargeValueKind::Json),
        ),
        DESCRIPTOR_NODE_UUID => scalar(DESCRIPTOR_NODE_UUID, ValueType::Uuid),
        DESCRIPTOR_NODE_ENUM_TAG => {
            if node.name.as_deref().is_none() || node.children != 0 {
                return Err(Error::NonCanonicalRecord);
            }
            if node.strings.len() > 256 {
                return Err(Error::EnumTooManyVariants {
                    name: node.name.clone().expect("checked"),
                    variants: node.strings.len(),
                });
            }
            Ok(ValueType::EnumTag(ScalarEnumSchema {
                registry_id: node.registry_id,
                name: node.name.clone().expect("checked"),
                variants: node.strings.clone(),
            }))
        }
        DESCRIPTOR_NODE_TUPLE => {
            if node.name.is_some() || node.registry_id != 0 || !node.strings.is_empty() {
                return Err(Error::NonCanonicalRecord);
            }
            let mut members = Vec::with_capacity(
                usize::try_from(node.children).map_err(|_| Error::LengthOverflow)?,
            );
            for _ in 0..node.children {
                members.push(descriptor_codec_decode_value_type(nodes, cursor)?);
            }
            // Reuse Groove's constructor-time tuple validation rather than
            // accepting an impossible variable-width tuple descriptor.
            let value_type = ValueType::Tuple(members);
            if value_type.fixed_size().is_none() {
                return Err(Error::InvalidTupleMember {
                    member_type: value_type,
                });
            }
            Ok(value_type)
        }
        DESCRIPTOR_NODE_ARRAY | DESCRIPTOR_NODE_NULLABLE => {
            if node.name.is_some()
                || node.registry_id != 0
                || node.children != 1
                || !node.strings.is_empty()
            {
                return Err(Error::NonCanonicalRecord);
            }
            let inner = Box::new(descriptor_codec_decode_value_type(nodes, cursor)?);
            Ok(if node.tag == DESCRIPTOR_NODE_ARRAY {
                ValueType::Array(inner)
            } else {
                ValueType::Nullable(inner)
            })
        }
        DESCRIPTOR_NODE_RECORD => {
            if node.name.is_some()
                || node.registry_id != 0
                || node.children != 1
                || !node.strings.is_empty()
            {
                return Err(Error::NonCanonicalRecord);
            }
            Ok(ValueType::Record(Box::new(
                descriptor_codec_decode_descriptor(nodes, cursor)?,
            )))
        }
        DESCRIPTOR_NODE_ENUM => {
            if node.name.as_deref().is_none() || !node.strings.is_empty() {
                return Err(Error::NonCanonicalRecord);
            }
            let mut cases = Vec::with_capacity(
                usize::try_from(node.children).map_err(|_| Error::LengthOverflow)?,
            );
            for _ in 0..node.children {
                let case = descriptor_codec_take(nodes, cursor)?;
                if case.tag != DESCRIPTOR_NODE_ENUM_CASE
                    || case.name.as_deref().is_none()
                    || case.registry_id != 0
                    || case.children != 1
                    || !case.strings.is_empty()
                {
                    return Err(Error::NonCanonicalRecord);
                }
                cases.push(EnumCase::new(
                    case.name.clone().expect("checked"),
                    descriptor_codec_decode_descriptor(nodes, cursor)?,
                ));
            }
            let mut schema = EnumSchema::new(node.name.clone().expect("checked"), cases)?;
            schema.registry_id = node.registry_id;
            Ok(ValueType::Enum(Box::new(schema)))
        }
        _ => Err(Error::NonCanonicalRecord),
    }
}

/// Encode a record descriptor through Groove's ordinary canonical record/value
/// algebra. The inverse rejects non-canonical, trailing, or incomplete trees.
pub fn encode_record_descriptor(descriptor: &RecordDescriptor) -> Result<Vec<u8>, Error> {
    let mut nodes = Vec::new();
    descriptor_codec_push_descriptor(&mut nodes, descriptor)?;
    let nodes = nodes
        .into_iter()
        .map(descriptor_codec_node_value)
        .collect::<Result<Vec<_>, _>>()?;
    descriptor_codec_envelope().create(&[Value::Array(nodes)])
}

/// Decode one exact canonical descriptor encoding produced by
/// [`encode_record_descriptor`].
pub fn decode_record_descriptor(encoded: &[u8]) -> Result<RecordDescriptor, Error> {
    let envelope = descriptor_codec_envelope();
    let values = envelope.bind(encoded).to_values()?;
    if envelope.create(&values)? != encoded {
        return Err(Error::NonCanonicalRecord);
    }
    let [Value::Array(nodes)] = values.as_slice() else {
        return Err(Error::NonCanonicalRecord);
    };
    if nodes.len() > DESCRIPTOR_CODEC_MAX_NODES {
        return Err(Error::LengthOverflow);
    }
    let nodes = nodes
        .iter()
        .cloned()
        .map(descriptor_codec_node_from_value)
        .collect::<Result<Vec<_>, _>>()?;
    let mut cursor = 0;
    let descriptor = descriptor_codec_decode_descriptor(&nodes, &mut cursor)?;
    if cursor != nodes.len() || encode_record_descriptor(&descriptor)? != encoded {
        return Err(Error::NonCanonicalRecord);
    }
    Ok(descriptor)
}
