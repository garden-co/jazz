//! Frozen native type envelope. Execution field identities are reconstructed
//! by the compiler, never serialized in a native application's nested schema.
use groove::records::{EnumSchema, RecordDescriptor, ValueType};
use serde::{Serialize, Serializer};

pub(super) fn serialize<S: Serializer>(
    value: &ValueType,
    serializer: S,
) -> Result<S::Ok, S::Error> {
    NativeValueType(value).serialize(serializer)
}

pub(super) struct NativeValueType<'a>(pub(super) &'a ValueType);

impl Serialize for NativeValueType<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        match self.0 {
            ValueType::Tuple(members) => serializer.serialize_newtype_variant(
                "ValueType",
                13,
                "Tuple",
                &members.iter().map(NativeValueType).collect::<Vec<_>>(),
            ),
            ValueType::Array(inner) => serializer.serialize_newtype_variant(
                "ValueType",
                14,
                "Array",
                &NativeValueType(inner),
            ),
            ValueType::Nullable(inner) => serializer.serialize_newtype_variant(
                "ValueType",
                15,
                "Nullable",
                &NativeValueType(inner),
            ),
            ValueType::Record(descriptor) => serializer.serialize_newtype_variant(
                "ValueType",
                16,
                "Record",
                &NativeDescriptor(descriptor),
            ),
            ValueType::Enum(schema) => serializer.serialize_newtype_variant(
                "ValueType",
                17,
                "Enum",
                &NativeEnumSchema(schema),
            ),
            scalar => scalar.serialize(serializer),
        }
    }
}

struct NativeDescriptor<'a>(&'a RecordDescriptor);
impl Serialize for NativeDescriptor<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        #[derive(Serialize)]
        struct Field<'a> {
            name: Option<&'a str>,
            value_type: NativeValueType<'a>,
        }
        self.0
            .fields()
            .iter()
            .map(|field| Field {
                name: field.name.as_deref(),
                value_type: NativeValueType(&field.value_type),
            })
            .collect::<Vec<_>>()
            .serialize(serializer)
    }
}

struct NativeEnumSchema<'a>(&'a EnumSchema);
impl Serialize for NativeEnumSchema<'_> {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        #[derive(Serialize)]
        struct Case<'a> {
            name: &'a str,
            payload: NativeDescriptor<'a>,
        }
        #[derive(Serialize)]
        struct Schema<'a> {
            registry_id: u64,
            name: &'a str,
            cases: Vec<Case<'a>>,
        }
        Schema {
            registry_id: self.0.registry_id,
            name: &self.0.name,
            cases: self
                .0
                .cases
                .iter()
                .map(|case| Case {
                    name: &case.name,
                    payload: NativeDescriptor(&case.payload),
                })
                .collect(),
        }
        .serialize(serializer)
    }
}
