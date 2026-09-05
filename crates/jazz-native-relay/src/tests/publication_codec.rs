//! Test consumer for SPEC 18's publication grammar. Execution descriptor serde
//! is intentionally absent: the native host receives roles and recursive types.
use jazz::groove::records::{
    DescriptorField, EnumCase, EnumSchema, RecordDescriptor, ScalarEnumSchema, ValueType,
};
use serde::{Deserialize, Serialize};

type NamedFields = Vec<(Option<String>, NativeType)>;

#[derive(Deserialize)]
enum PublicationName {
    StoredColumn { id: u64, output_name: String },
    ResultField { name: String },
    HiddenMetadata { name: String },
}

#[derive(Deserialize)]
struct PublicationField {
    name: PublicationName,
    value_type: NativeType,
}

// These discriminants belong to the published host grammar, not to compiler
// identities. Composite payloads recursively contain name/type descriptors.
#[derive(Deserialize, Serialize)]
enum NativeType {
    U8,
    U16,
    U32,
    U64,
    I32,
    I64,
    F64,
    Bool,
    String,
    Bytes,
    Internal(InternalType),
    Uuid,
    EnumTag(ScalarEnumSchema),
    Tuple(Vec<NativeType>),
    Array(Box<NativeType>),
    Nullable(Box<NativeType>),
    Record(NamedFields),
    Enum {
        registry_id: u64,
        name: String,
        cases: Vec<(String, NamedFields)>,
    },
}

#[derive(Deserialize, Serialize)]
enum InternalType {
    RawString,
    RawBytes,
    StoredScalar(jazz::groove::large_values::LargeValueKind),
}

fn nested_descriptor(fields: NamedFields) -> RecordDescriptor {
    RecordDescriptor::new_with_fields(fields.into_iter().map(|(name, value_type)| {
        DescriptorField {
            identity: name.clone().map(jazz::groove::records::FieldIdentity::Name),
            name,
            value_type: value_type.into_value_type(),
        }
    }))
}

impl NativeType {
    fn into_value_type(self) -> ValueType {
        match self {
            Self::Tuple(values) => {
                ValueType::Tuple(values.into_iter().map(Self::into_value_type).collect())
            }
            Self::Array(value) => ValueType::Array(Box::new(value.into_value_type())),
            Self::Nullable(value) => ValueType::Nullable(Box::new(value.into_value_type())),
            Self::Record(fields) => ValueType::Record(Box::new(nested_descriptor(fields))),
            Self::Enum {
                registry_id,
                name,
                cases,
            } => ValueType::Enum(Box::new(
                EnumSchema::new(
                    name,
                    cases
                        .into_iter()
                        .map(|(name, fields)| EnumCase::new(name, nested_descriptor(fields))),
                )
                .unwrap()
                .with_registry_id(registry_id),
            )),
            // Scalar ABI tags/payloads contain no descriptors or execution IDs.
            scalar => postcard::from_bytes(&postcard::to_allocvec(&scalar).unwrap()).unwrap(),
        }
    }
}

pub(super) fn descriptor<'de, D: serde::Deserializer<'de>>(
    decoder: D,
) -> Result<RecordDescriptor, D::Error> {
    let fields = Vec::<PublicationField>::deserialize(decoder)?;
    Ok(RecordDescriptor::new(fields.into_iter().map(|field| {
        let name = match field.name {
            PublicationName::StoredColumn { id, output_name } => {
                // The host uses this node-local identity for catalogue lookups;
                // decoding this fixture's inline cells only needs its exact name.
                let _ = id;
                output_name
            }
            PublicationName::ResultField { name } | PublicationName::HiddenMetadata { name } => {
                name
            }
        };
        (name, field.value_type.into_value_type())
    })))
}
