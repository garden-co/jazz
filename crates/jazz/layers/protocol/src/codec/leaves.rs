//! Sync-wire codecs for the leaf types the protocol borrows from `jazz-types`
//! and Groove: identifiers, clocks, authors, result keys, record values and
//! large-value descriptors. Each spells exactly what the type's serde
//! implementation writes through a non-human-readable serializer.

use groove::large_values::{LargeValueKind, Locator, physical_storage_value_type};
use groove::records::{
    DescriptorField, EnumValue, FieldIdentity, OwnedRecord, RecordDescriptor, ScalarEnumSchema,
    Value, ValueType,
};

use super::{CodecError, WireCodec, get_bytes, get_str, put_bytes, read_u8};
use crate::ids::{
    AuthorSubject, GlobalPhysicalColumnId, GlobalPhysicalEnumVariantId, GlobalPhysicalTableId,
    MigrationLensId, NodeUuid, PhysicalTableId, RowUuid, SchemaLineagePublicationId,
    SchemaVersionId,
};
use crate::object::{ObjectId, OutputOccurrenceId, ResultKey};
use crate::time::{GlobalTime, TxTime};

fn decode_error(message: impl ToString) -> CodecError {
    CodecError::DecodeError {
        message: message.to_string(),
    }
}

/// Newtypes are their single field.
macro_rules! newtype_codec {
    ($($ty:ty),+ $(,)?) => {$(
        impl WireCodec for $ty {
            #[inline]
            fn put(&self, buf: &mut Vec<u8>) {
                self.0.put(buf);
            }

            fn get(data: &[u8], offset: &mut usize) -> Result<Self, CodecError> {
                WireCodec::get(data, offset).map(Self)
            }
        }
    )+};
}

newtype_codec!(
    NodeUuid,
    SchemaVersionId,
    PhysicalTableId,
    GlobalPhysicalTableId,
    GlobalPhysicalColumnId,
    GlobalPhysicalEnumVariantId,
    MigrationLensId,
    SchemaLineagePublicationId,
    RowUuid,
    GlobalTime,
    TxTime,
    groove::large_values::ContentHash,
);

/// Interned strings are spelled as the string.
impl WireCodec for groove::Intern<String> {
    fn put(&self, buf: &mut Vec<u8>) {
        put_bytes(buf, self.as_bytes());
    }

    fn get(data: &[u8], offset: &mut usize) -> Result<Self, CodecError> {
        get_str(data, offset).map(groove::Intern::from_ref)
    }
}

/// An author is its canonical JSON string, validated on decode.
impl WireCodec for AuthorSubject {
    fn put(&self, buf: &mut Vec<u8>) {
        put_bytes(buf, self.canonical().as_bytes());
    }

    fn get(data: &[u8], offset: &mut usize) -> Result<Self, CodecError> {
        AuthorSubject::from_canonical(get_str(data, offset)?).map_err(decode_error)
    }
}

impl WireCodec for ObjectId {
    fn put(&self, buf: &mut Vec<u8>) {
        self.uuid().put(buf);
    }

    fn get(data: &[u8], offset: &mut usize) -> Result<Self, CodecError> {
        uuid::Uuid::get(data, offset).map(ObjectId::from_uuid)
    }
}

/// `(root, joined)`; typed union discriminators never travel in this carrier.
impl WireCodec for OutputOccurrenceId {
    fn put(&self, buf: &mut Vec<u8>) {
        self.root_source().put(buf);
        super::put_seq(buf, self.joined_sources(), |buf, id| id.put(buf));
    }

    fn get(data: &[u8], offset: &mut usize) -> Result<Self, CodecError> {
        let root = ObjectId::get(data, offset)?;
        let joined = Vec::<ObjectId>::get(data, offset)?;
        Ok(OutputOccurrenceId::new(root, joined))
    }
}

/// A result key is a byte string holding its versioned typed identity.
impl WireCodec for ResultKey {
    fn put(&self, buf: &mut Vec<u8>) {
        put_bytes(buf, &self.to_wire_bytes());
    }

    fn get(data: &[u8], offset: &mut usize) -> Result<Self, CodecError> {
        ResultKey::from_wire_bytes(get_bytes(data, offset)?).map_err(decode_error)
    }
}

// ----------------------------------------------------------------------------
// Groove records
// ----------------------------------------------------------------------------

crate::wire_enum! { for Value {
    0 => U8(v),
    1 => U16(v),
    2 => U32(v),
    3 => U64(v),
    4 => F64(v),
    5 => Bool(v),
    6 => String(v),
    7 => Bytes(v: crate::codec::Bytes),
    8 => Large(v),
    9 => Uuid(v),
    10 => EnumTag(v),
    11 => Tuple(v),
    12 => Array(v),
    13 => Nullable(v),
    14 => I64(v),
    15 => I32(v),
    16 => Record(v),
    17 => Enum(v),
    18 => U48(v),
} }

impl WireCodec for EnumValue {
    fn put(&self, buf: &mut Vec<u8>) {
        self.tag().put(buf);
        self.record().put(buf);
    }

    fn get(data: &[u8], offset: &mut usize) -> Result<Self, CodecError> {
        let tag = u32::get(data, offset)?;
        Ok(EnumValue::new(
            tag,
            <OwnedRecord as WireCodec>::get(data, offset)?,
        ))
    }
}

/// `{ descriptor, raw }`.
impl WireCodec for OwnedRecord {
    fn put(&self, buf: &mut Vec<u8>) {
        self.descriptor().put(buf);
        put_bytes(buf, self.raw());
    }

    fn get(data: &[u8], offset: &mut usize) -> Result<Self, CodecError> {
        let descriptor = <RecordDescriptor as WireCodec>::get(data, offset)?;
        Ok(OwnedRecord::new(
            get_bytes(data, offset)?.to_vec(),
            descriptor,
        ))
    }
}

/// A descriptor is its field list; decoding rebuilds the layout the same way
/// deserialization does.
impl WireCodec for RecordDescriptor {
    fn put(&self, buf: &mut Vec<u8>) {
        super::put_seq(buf, self.fields(), |buf, field| field.put(buf));
    }

    fn get(data: &[u8], offset: &mut usize) -> Result<Self, CodecError> {
        Vec::<DescriptorField>::get(data, offset).map(RecordDescriptor::new_with_fields)
    }
}

/// `identity` is `skip_serializing_if = "Option::is_none"`: an absent identity
/// writes nothing, while the reader always expects the option byte (as
/// postcard's did). Descriptors give every named field an identity, so only
/// an unnamed, unidentified field hits that asymmetry.
impl WireCodec for DescriptorField {
    fn put(&self, buf: &mut Vec<u8>) {
        self.name.put(buf);
        if self.identity.is_some() {
            self.identity.put(buf);
        }
        self.value_type.put(buf);
    }

    fn get(data: &[u8], offset: &mut usize) -> Result<Self, CodecError> {
        Ok(DescriptorField {
            name: WireCodec::get(data, offset)?,
            identity: WireCodec::get(data, offset)?,
            value_type: WireCodec::get(data, offset)?,
        })
    }
}

crate::wire_enum! { for FieldIdentity {
    0 => Name(name),
    1 => Slot(slot),
    2 => NamedSlot { name, slot },
} }

crate::wire_enum! { partial ValueTypeTable: ValueType {
    0 => U8,
    1 => U16,
    2 => U32,
    3 => U64,
    4 => I32,
    5 => I64,
    6 => F64,
    7 => Bool,
    8 => String,
    9 => Bytes,
    11 => Uuid,
    12 => EnumTag(schema: crate::codec::Auto),
    13 => Tuple(members: crate::codec::Auto),
    14 => Array(inner: crate::codec::Auto),
    15 => Nullable(inner: crate::codec::Auto),
    16 => Record(descriptor: crate::codec::Auto),
    17 => Enum(schema: crate::codec::Auto),
    18 => U48,
} }

/// `ValueType::Internal` wraps a private representation, so tag 10 is spelled
/// by hand from its public constructors: raw string `0`, raw bytes `1`,
/// stored scalar `2` + kind, interned `3` + dictionary + logical type.
const VALUE_TYPE_INTERNAL: u8 = 10;
const LARGE_VALUE_KINDS: [LargeValueKind; 3] = [
    LargeValueKind::Bytes,
    LargeValueKind::String,
    LargeValueKind::Json,
];

impl WireCodec for ValueType {
    fn put(&self, buf: &mut Vec<u8>) {
        if !matches!(self, ValueType::Internal(_)) {
            return ValueTypeTable.put_variant(buf, self);
        }
        buf.push(VALUE_TYPE_INTERNAL);
        if let Some((dictionary, logical)) = self.as_interned() {
            buf.push(3);
            put_bytes(buf, dictionary.as_bytes());
            logical.put(buf);
        } else if *self == ValueType::raw_string() {
            buf.push(0);
        } else if *self == ValueType::raw_bytes() {
            buf.push(1);
        } else {
            let kind = LARGE_VALUE_KINDS
                .into_iter()
                .find(|kind| *self == physical_storage_value_type(*kind))
                .expect("every internal value type is raw, stored or interned");
            buf.push(2);
            kind.put(buf);
        }
    }

    fn get(data: &[u8], offset: &mut usize) -> Result<Self, CodecError> {
        let tag = read_u8(data, offset)?;
        if tag != VALUE_TYPE_INTERNAL {
            return ValueTypeTable.get_variant(tag, data, offset)?.ok_or(
                CodecError::InvalidTypeTag {
                    tag,
                    context: "ValueType",
                },
            );
        }
        Ok(match read_u8(data, offset)? {
            0 => ValueType::raw_string(),
            1 => ValueType::raw_bytes(),
            2 => physical_storage_value_type(LargeValueKind::get(data, offset)?),
            3 => {
                let dictionary = String::get(data, offset)?;
                ValueType::interned(dictionary, ValueType::get(data, offset)?)
            }
            tag => {
                return Err(CodecError::InvalidTypeTag {
                    tag,
                    context: "InternalValueType",
                });
            }
        })
    }
}

/// `{ registry_id, name, variants }`, validated on decode.
impl WireCodec for ScalarEnumSchema {
    fn put(&self, buf: &mut Vec<u8>) {
        self.registry_id().put(buf);
        self.name.put(buf);
        self.variants.put(buf);
    }

    fn get(data: &[u8], offset: &mut usize) -> Result<Self, CodecError> {
        let registry_id = u64::get(data, offset)?;
        let name = String::get(data, offset)?;
        let variants = Vec::<String>::get(data, offset)?;
        ScalarEnumSchema::from_parts(registry_id, name, variants).map_err(decode_error)
    }
}

crate::wire_struct! { for groove::records::EnumSchema { registry_id, name, cases } }
crate::wire_struct! { for groove::records::EnumCase { name, payload } }

// ----------------------------------------------------------------------------
// Groove large values
// ----------------------------------------------------------------------------

crate::wire_enum! { for LargeValueKind {
    0 => Bytes,
    1 => String,
    2 => Json,
} }

/// A locator is spelled as a byte string that must be exactly
/// [`groove::large_values::LOCATOR_BYTES`] long.
impl WireCodec for Locator {
    fn put(&self, buf: &mut Vec<u8>) {
        put_bytes(buf, self.as_bytes());
    }

    fn get(data: &[u8], offset: &mut usize) -> Result<Self, CodecError> {
        let bytes = get_bytes(data, offset)?;
        let bytes = bytes.try_into().map_err(|_| {
            decode_error(format!(
                "chunk locator must be exactly {} bytes, got {}",
                groove::large_values::LOCATOR_BYTES,
                bytes.len()
            ))
        })?;
        Ok(Locator::from_wire_bytes(bytes))
    }
}

crate::wire_struct! { for groove::large_values::NodeRef { object_hash, locator } }

crate::wire_struct! { for groove::large_values::ReplaceEdit {
    offset,
    delete_length,
    insert_bytes: crate::codec::Bytes,
    utf16_offset,
    delete_utf16_length,
    insert_utf16_length,
} }

crate::wire_struct! { for groove::large_values::LargeValueRef {
    kind,
    format_version,
    logical_hash,
    root,
    byte_length,
    utf16_length,
    edit_tail,
} }

crate::wire_struct! { for groove::large_values::StagedChunk {
    node_ref,
    encoded: crate::codec::Bytes,
} }

#[cfg(test)]
mod tests {
    use groove::large_values::{ContentHash, LargeValueRef, NodeRef, ReplaceEdit, StagedChunk};
    use groove::records::{EnumCase, EnumSchema, SystemVariantRegistry};

    use super::*;
    use crate::codec::{decode_exact, encode};

    /// Lower-level differential check: the hand codec must spell exactly what
    /// postcard spelled from the serde implementations, and read it back.
    fn same_as_postcard<T>(value: &T) -> Vec<u8>
    where
        T: WireCodec + serde::Serialize + serde::de::DeserializeOwned + PartialEq + std::fmt::Debug,
    {
        let bytes = postcard::to_allocvec(value).unwrap();
        assert_eq!(encode(value), bytes, "{value:?}");
        assert_eq!(&decode_exact::<T>(&bytes).unwrap(), value);
        assert_eq!(&postcard::from_bytes::<T>(&bytes).unwrap(), value);
        bytes
    }

    fn uuid(n: u128) -> uuid::Uuid {
        uuid::Uuid::from_u128(n)
    }

    fn record() -> OwnedRecord {
        let descriptor = RecordDescriptor::new([("a", ValueType::U64), ("b", ValueType::String)]);
        let raw = descriptor
            .create(&[Value::U64(300), Value::String("x".into())])
            .unwrap();
        OwnedRecord::new(raw, descriptor)
    }

    fn large_value_ref(edits: usize) -> LargeValueRef {
        LargeValueRef {
            kind: LargeValueKind::String,
            format_version: 200,
            logical_hash: ContentHash([3; 32]),
            root: NodeRef {
                object_hash: ContentHash([4; 32]),
                locator: Locator::from_wire_bytes([5; 32]),
            },
            byte_length: u64::MAX,
            utf16_length: (edits > 0).then_some(17),
            edit_tail: (0..edits)
                .map(|i| ReplaceEdit {
                    offset: i as u64,
                    delete_length: 128,
                    insert_bytes: vec![0xff; i],
                    utf16_offset: 1,
                    delete_utf16_length: 0,
                    insert_utf16_length: u64::MAX,
                })
                .collect(),
        }
    }

    /// Identifier, clock and author leaves match postcard.
    #[test]
    fn id_time_and_author_leaves_match_postcard() {
        same_as_postcard(&NodeUuid(uuid(1)));
        same_as_postcard(&SchemaVersionId(uuid(u128::MAX)));
        same_as_postcard(&PhysicalTableId(0));
        same_as_postcard(&PhysicalTableId(u64::MAX));
        same_as_postcard(&GlobalPhysicalTableId(uuid(2)));
        same_as_postcard(&GlobalPhysicalColumnId(uuid(3)));
        same_as_postcard(&GlobalPhysicalEnumVariantId(uuid(4)));
        same_as_postcard(&MigrationLensId(uuid(5)));
        same_as_postcard(&SchemaLineagePublicationId(uuid(6)));
        same_as_postcard(&RowUuid(uuid(7)));
        same_as_postcard(&GlobalTime(127));
        same_as_postcard(&TxTime(u64::MAX));
        same_as_postcard(&groove::Intern::new(String::new()));
        same_as_postcard(&groove::Intern::new("todos".to_owned()));
        same_as_postcard(&ObjectId::from_uuid(uuid(8)));

        for author in [
            AuthorSubject::SYSTEM,
            AuthorSubject::system_at(NodeUuid(uuid(9))),
            AuthorSubject::authenticated("https://issuer", "alice").unwrap(),
            AuthorSubject::authenticated("https://issuer", "bob")
                .unwrap()
                .with_account(crate::account_registry::AccountId(uuid(10))),
            AuthorSubject::for_test_bytes([0x42; 16]),
        ] {
            same_as_postcard(&author);
        }
        let forged = postcard::to_allocvec(&"[\"only-one\"]").unwrap();
        assert!(matches!(
            decode_exact::<AuthorSubject>(&forged),
            Err(CodecError::DecodeError { .. })
        ));
    }

    /// Output occurrences and result keys match postcard, including the
    /// typed union sidecar and its validation.
    #[test]
    fn occurrence_and_result_key_match_postcard() {
        let root = ObjectId::from_uuid(uuid(1));
        let joined = [ObjectId::from_uuid(uuid(2)), ObjectId::from_uuid(uuid(3))];
        same_as_postcard(&OutputOccurrenceId::single_source(root));
        same_as_postcard(&OutputOccurrenceId::new(root, joined));
        same_as_postcard(&ResultKey::from(root));
        same_as_postcard(&ResultKey::from_occurrence(OutputOccurrenceId::new(
            root, joined,
        )));
        same_as_postcard(
            &ResultKey::from_union_occurrence(root, joined, [(2, "b".into()), (0, "a".into())])
                .unwrap(),
        );
        for malformed in [vec![], vec![2], vec![1, 0]] {
            let bytes = postcard::to_allocvec(&serde_bytes::ByteBuf::from(malformed)).unwrap();
            assert!(postcard::from_bytes::<ResultKey>(&bytes).is_err());
            assert!(decode_exact::<ResultKey>(&bytes).is_err());
        }
    }

    /// Every `Value` arm, including records and tagged enums, matches postcard.
    #[test]
    fn values_match_postcard() {
        let enum_descriptor = RecordDescriptor::new([("n", ValueType::I32)]);
        let values = [
            Value::U8(255),
            Value::U16(u16::MAX),
            Value::U32(128),
            Value::U64(u64::MAX),
            Value::F64(-1.5),
            Value::Bool(true),
            Value::String("héllo".into()),
            Value::Bytes(vec![]),
            Value::Bytes(vec![0, 1, 2]),
            Value::Large(Box::new(large_value_ref(2))),
            Value::Uuid(uuid(11)),
            Value::EnumTag(3),
            Value::Tuple(vec![Value::U8(1), Value::I64(-2)]),
            Value::Array(vec![]),
            Value::Array(vec![Value::String("a".into())]),
            Value::Nullable(None),
            Value::Nullable(Some(Box::new(Value::I32(i32::MIN)))),
            Value::I64(i64::MIN),
            Value::I32(-1),
            Value::Record(record()),
            Value::Enum(EnumValue::create(7, enum_descriptor, &[Value::I32(5)]).unwrap()),
            Value::U48(groove::records::U48_MAX),
        ];
        for value in &values {
            same_as_postcard(value);
        }
        assert!(matches!(
            decode_exact::<Value>(&[19]),
            Err(CodecError::InvalidTypeTag { tag: 19, .. })
        ));
    }

    /// Every `ValueType` arm, including the engine-only internal types and
    /// descriptors with explicit field identities, matches postcard.
    #[test]
    fn value_types_and_descriptors_match_postcard() {
        let identified = RecordDescriptor::new_with_fields([
            DescriptorField::new("plain", ValueType::Bool),
            DescriptorField::new("slot", ValueType::U8).with_identity(FieldIdentity::Slot(9)),
            DescriptorField::new("named", ValueType::Uuid).with_identity(
                FieldIdentity::NamedSlot {
                    name: "public".into(),
                    slot: 1 << 40,
                },
            ),
        ]);
        let enum_schema = EnumSchema::new(
            "Shape",
            [
                EnumCase::new("Empty", RecordDescriptor::default()),
                EnumCase::new("Point", identified),
            ],
        )
        .unwrap()
        .with_registry_id(77);
        let value_types = [
            ValueType::U8,
            ValueType::U16,
            ValueType::U32,
            ValueType::U64,
            ValueType::I32,
            ValueType::I64,
            ValueType::F64,
            ValueType::Bool,
            ValueType::String,
            ValueType::Bytes,
            ValueType::raw_string(),
            ValueType::raw_bytes(),
            physical_storage_value_type(LargeValueKind::Bytes),
            physical_storage_value_type(LargeValueKind::String),
            physical_storage_value_type(LargeValueKind::Json),
            ValueType::interned("authors", ValueType::Nullable(Box::new(ValueType::String))),
            ValueType::Uuid,
            ValueType::EnumTag(ScalarEnumSchema::new("e", ["a", "b"]).unwrap()),
            ValueType::EnumTag(
                ScalarEnumSchema::new("d", ["x"])
                    .unwrap()
                    .with_system_registry(SystemVariantRegistry::deletion_state()),
            ),
            ValueType::Tuple(vec![ValueType::U64, ValueType::Uuid]),
            ValueType::Array(Box::new(ValueType::String)),
            ValueType::Nullable(Box::new(ValueType::U48)),
            ValueType::Record(Box::new(identified)),
            ValueType::Record(Box::new(RecordDescriptor::default())),
            ValueType::Enum(Box::new(enum_schema.clone())),
            ValueType::U48,
        ];
        for value_type in &value_types {
            same_as_postcard(value_type);
        }
        same_as_postcard(&enum_schema);
        same_as_postcard(&FieldIdentity::Name("n".into()));

        // A scalar enum is validated on decode, as deserialization does.
        let duplicate = (0_u64, "e", vec!["a", "a"]);
        let mut bytes = vec![12];
        bytes.extend(postcard::to_allocvec(&duplicate).unwrap());
        assert!(postcard::from_bytes::<ValueType>(&bytes).is_err());
        assert!(matches!(
            decode_exact::<ValueType>(&bytes),
            Err(CodecError::DecodeError { .. })
        ));
        assert!(decode_exact::<ValueType>(&[10, 4]).is_err());
    }

    /// Large-value descriptors, chunks and node references match postcard,
    /// and a locator of the wrong length is rejected.
    #[test]
    fn large_value_leaves_match_postcard() {
        same_as_postcard(&large_value_ref(0));
        same_as_postcard(&large_value_ref(3));
        same_as_postcard(&StagedChunk {
            node_ref: large_value_ref(0).root,
            encoded: vec![9; 300],
        });
        for kind in LARGE_VALUE_KINDS {
            same_as_postcard(&kind);
        }
        for length in [31, 33] {
            let bytes = postcard::to_allocvec(&vec![0x11_u8; length]).unwrap();
            assert!(postcard::from_bytes::<Locator>(&bytes).is_err());
            assert!(decode_exact::<Locator>(&bytes).is_err());
        }
    }
}
