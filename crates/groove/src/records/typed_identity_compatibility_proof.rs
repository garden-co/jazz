//! Decision evidence for #2558: descriptors cross a byte boundary which public
//! Jazz queries cannot independently select. Internal codec tests are necessary
//! to compare both readers/writers without changing production persistence.
//! These are component proofs, not a bidirectional Jazz database reopen receipt.
use super::*;

#[path = "contained_descriptor_codec_fixture.rs"]
mod contained;

fn logical_output(descriptor: RecordDescriptor) -> RecordDescriptor {
    fn logical_type(ty: &ValueType) -> ValueType {
        match ty {
            ValueType::Record(child) => ValueType::Record(Box::new(logical_output(**child))),
            ValueType::Array(child) => ValueType::Array(Box::new(logical_type(child))),
            ValueType::Nullable(child) => ValueType::Nullable(Box::new(logical_type(child))),
            ValueType::Tuple(children) => {
                ValueType::Tuple(children.iter().map(logical_type).collect())
            }
            ValueType::Enum(schema) => {
                let mut schema = (**schema).clone();
                for case in &mut schema.cases {
                    case.payload = logical_output(case.payload);
                }
                ValueType::Enum(Box::new(schema))
            }
            other => other.clone(),
        }
    }
    RecordDescriptor::new_with_fields(descriptor.fields().iter().map(|field| DescriptorField {
        name: field.logical_name().map(str::to_owned),
        identity: None,
        value_type: logical_type(&field.value_type),
    }))
}

fn typed_child(slot: u64) -> RecordDescriptor {
    RecordDescriptor::new_with_fields([
        DescriptorField::new("user_7", ValueType::U64).with_identity(FieldIdentity::NamedSlot {
            name: "score".into(),
            slot,
        }),
        DescriptorField::new("user_user_7", ValueType::String)
            .with_identity(FieldIdentity::Name("user_7".into())),
    ])
}

#[test]
fn current_descriptor_codecs_reject_each_others_bytes_even_for_plain_scalar() {
    let cases = [
        RecordDescriptor::new([("value", ValueType::U64)]),
        RecordDescriptor::new([
            ("row_uuid", ValueType::Uuid),
            ("_app_title", ValueType::String),
        ]),
        RecordDescriptor::new([(
            "children",
            ValueType::Array(Box::new(ValueType::Record(Box::new(
                RecordDescriptor::new([("title", ValueType::String)]),
            )))),
        )]),
    ];
    for descriptor in cases {
        let old = contained::encode_record_descriptor(&descriptor).unwrap();
        let new = encode_record_descriptor(&descriptor).unwrap();
        assert_ne!(old, new);
        assert!(decode_record_descriptor(&old).is_err());
        assert!(contained::decode_record_descriptor(&new).is_err());
        assert_eq!(
            contained::decode_record_descriptor(&old).unwrap(),
            descriptor
        );
        assert_eq!(decode_record_descriptor(&new).unwrap(), descriptor);
        eprintln!(
            "descriptor bytes: contained={} typed={}",
            old.len(),
            new.len()
        );
    }
}

#[test]
fn recursive_logical_output_matches_contained_bytes_without_carrier_name_collisions() {
    let typed = RecordDescriptor::new([(
        "children",
        ValueType::Array(Box::new(ValueType::Nullable(Box::new(ValueType::Record(
            Box::new(typed_child(7)),
        ))))),
    )]);
    let public_child =
        RecordDescriptor::new([("score", ValueType::U64), ("user_7", ValueType::String)]);
    let public = RecordDescriptor::new([(
        "children",
        ValueType::Array(Box::new(ValueType::Nullable(Box::new(ValueType::Record(
            Box::new(public_child),
        ))))),
    )]);
    let normalized = logical_output(typed);
    assert_eq!(normalized, public);
    assert_eq!(
        contained::encode_record_descriptor(&normalized).unwrap(),
        contained::encode_record_descriptor(&public).unwrap()
    );
    // Normalizing names does not turn the new codec into the contained codec.
    assert_ne!(
        encode_record_descriptor(&normalized).unwrap(),
        contained::encode_record_descriptor(&public).unwrap()
    );
    let child_values = [Value::U64(42), Value::String("literal user prefix".into())];
    assert_eq!(
        typed_child(7).create(&child_values).unwrap(),
        public_child.create(&child_values).unwrap()
    );
    // Nested values must be rebound to the normalized child descriptor. Merely
    // erasing descriptor metadata on the parent is not a valid value conversion.
    let typed_value = Value::Array(vec![Value::Nullable(Some(Box::new(Value::Record(
        OwnedRecord::new(
            typed_child(7).create(&child_values).unwrap(),
            typed_child(7),
        ),
    ))))]);
    assert!(normalized.create(&[typed_value]).is_err());
    let public_value = Value::Array(vec![Value::Nullable(Some(Box::new(Value::Record(
        OwnedRecord::new(public_child.create(&child_values).unwrap(), public_child),
    ))))]);
    assert_eq!(
        normalized.create(&[public_value.clone()]).unwrap(),
        public.create(&[public_value]).unwrap()
    );
}

#[test]
fn logical_output_erases_arbitrary_nested_slot_binding_irrecoverably() {
    let first = typed_child(7);
    let second = typed_child(8);
    assert_ne!(first, second);
    assert_ne!(
        encode_record_descriptor(&first).unwrap(),
        encode_record_descriptor(&second).unwrap()
    );
    assert_eq!(logical_output(first), logical_output(second));
    // Even before public normalization, the contained format has no slot bits.
    assert_eq!(
        contained::encode_record_descriptor(&first).unwrap(),
        contained::encode_record_descriptor(&second).unwrap()
    );
    // Therefore reconstructing arbitrary executable NamedSlot from a standalone
    // persisted public descriptor is impossible. A catalogue/query boundary is
    // required if a recovered public result is to re-enter execution.
}

#[test]
fn identity_only_codec_change_changes_aggregate_identity_and_flat_join_preimage() {
    let descriptor = RecordDescriptor::new([("value", ValueType::U64)]);
    let raw = descriptor.create(&[Value::U64(4)]).unwrap();
    let old = contained::encode_record_descriptor(&descriptor).unwrap();
    let new = encode_record_descriptor(&descriptor).unwrap();
    // Matches the fixed pair used by Jazz settled_result_value_storage_bytes.
    let outer = RecordDescriptor::new([
        ("descriptor", ValueType::Bytes),
        ("value", ValueType::Bytes),
    ]);
    let old_key = outer
        .create(&[Value::Bytes(old.clone()), Value::Bytes(raw.clone())])
        .unwrap();
    let new_key = outer
        .create(&[Value::Bytes(new.clone()), Value::Bytes(raw.clone())])
        .unwrap();
    assert_ne!(old_key, new_key);
    // Descriptor length and bytes are part of Jazz's revision preimage, even
    // though row bytes and logical values are unchanged. Hashing does not cure it.
    let framed = |descriptor: &[u8]| {
        let mut bytes = b"JFRD\x01".to_vec();
        bytes.extend_from_slice(&1u32.to_be_bytes());
        bytes.extend_from_slice(&(descriptor.len() as u32).to_be_bytes());
        bytes.extend_from_slice(descriptor);
        bytes.extend_from_slice(&(raw.len() as u32).to_be_bytes());
        bytes.extend_from_slice(&raw);
        bytes
    };
    assert_ne!(blake3::hash(&framed(&old)), blake3::hash(&framed(&new)));
}

#[test]
fn physical_index_spelling_changes_the_durable_key_namespace() {
    use crate::ivm::runtime::durable_index_key_prefix;
    // Exact formats from both revisions of Jazz physical/catalogue.rs.
    let table = "jazz_1_global_current";
    let contained = durable_index_key_prefix(table, "by_physical_app_v1_7");
    let typed = durable_index_key_prefix(table, "by_physical_user_v1_7");
    assert_ne!(contained, typed);
    assert_eq!(contained, b"jazz_1_global_current\0by_physical_app_v1_7\0");
    assert_eq!(typed, b"jazz_1_global_current\0by_physical_user_v1_7\0");
}

#[test]
fn canonical_persisted_descriptor_preserves_frozen_names_and_nested_value_bytes() {
    let descriptor = RecordDescriptor::new([
        ("name", ValueType::String),
        ("labels", ValueType::Array(Box::new(ValueType::String))),
        (
            "optional_count",
            ValueType::Nullable(Box::new(ValueType::U64)),
        ),
    ]);
    let encoded = encode_persisted_record_descriptor(&descriptor).unwrap();
    // Existing Jazz durable golden, unchanged from the contained implementation.
    assert_eq!(
        blake3::hash(&encoded).to_hex().as_str(),
        "e7fcf66bb23dd514678c3b3960b69f020935d01a366c83d7b6fda963d2346e0a"
    );
    assert_eq!(
        encoded,
        contained::encode_record_descriptor(&descriptor).unwrap()
    );
    assert_eq!(
        decode_persisted_record_descriptor(&encoded).unwrap(),
        descriptor
    );
    assert_eq!(
        contained::decode_record_descriptor(&encoded).unwrap(),
        descriptor
    );
    let mut trailing = encoded.clone();
    trailing.push(0);
    assert!(decode_persisted_record_descriptor(&trailing).is_err());

    // Retain exact durable names, even when execution uses distinct logical
    // bindings; there is deliberately no generic public-name normalization here.
    let child = typed_child(7);
    let parent = RecordDescriptor::new([(
        "children",
        ValueType::Array(Box::new(ValueType::Record(Box::new(child)))),
    )]);
    let child_raw = child
        .create(&[Value::U64(4), Value::String("payload".into())])
        .unwrap();
    let raw = parent
        .create(&[Value::Array(vec![Value::Record(OwnedRecord::new(
            child_raw, child,
        ))])])
        .unwrap();
    let encoded = encode_persisted_record_descriptor(&parent).unwrap();
    assert_eq!(
        encoded,
        contained::encode_record_descriptor(&parent).unwrap()
    );
    let decoded = decode_persisted_record_descriptor(&encoded).unwrap();
    assert_ne!(decoded, parent); // executable bindings are deliberately absent
    assert_eq!(
        encode_persisted_record_descriptor(&decoded).unwrap(),
        encoded
    );
    assert_eq!(
        decoded
            .create(&decoded.bind(&raw).to_values().unwrap())
            .unwrap(),
        raw
    );
    assert_eq!(
        contained::decode_record_descriptor(&encoded).unwrap(),
        decoded
    );
}
