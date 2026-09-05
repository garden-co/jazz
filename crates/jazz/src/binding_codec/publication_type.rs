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

pub(super) struct NativeDescriptor<'a>(pub(super) &'a RecordDescriptor);
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

// Host input is an explicit name/type tree, not RecordDescriptor's execution
// serde. Bound both total nodes and recursion before allocating child vectors.
pub(super) fn read_descriptor(input: &mut &[u8]) -> Result<RecordDescriptor, String> {
    read_fields(input, &mut 1024, 0)
}

fn take<'a, T: serde::Deserialize<'a>>(input: &mut &'a [u8]) -> Result<T, String> {
    let (value, rest) = postcard::take_from_bytes(input).map_err(|error| error.to_string())?;
    *input = rest;
    Ok(value)
}

fn count(input: &mut &[u8], budget: usize) -> Result<usize, String> {
    let count: usize = take(input)?;
    if count > budget || count > input.len() {
        return Err("binding descriptor node limit exceeded".to_owned());
    }
    Ok(count)
}

fn read_fields(
    input: &mut &[u8],
    budget: &mut usize,
    depth: usize,
) -> Result<RecordDescriptor, String> {
    let count = count(input, *budget)?;
    let mut fields = Vec::with_capacity(count);
    for _ in 0..count {
        let name: Option<String> = take(input)?;
        let value_type = read_type(input, budget, depth)?;
        fields.push(groove::records::DescriptorField {
            identity: name.clone().map(groove::records::FieldIdentity::Name),
            name,
            value_type,
        });
    }
    Ok(RecordDescriptor::new_with_fields(fields))
}

fn read_type(input: &mut &[u8], budget: &mut usize, depth: usize) -> Result<ValueType, String> {
    if *budget == 0 || depth >= 128 {
        return Err("binding descriptor node or depth limit exceeded".to_owned());
    }
    *budget -= 1;
    let start = *input;
    let tag: u32 = take(input)?;
    Ok(match tag {
        // These scalar payloads contain no record descriptors. Their fixed
        // discriminants are the same ones pinned by NativeValueType's writer.
        0..=12 => {
            *input = start;
            take(input)?
        }
        13 => {
            let count = count(input, *budget)?;
            let mut members = Vec::with_capacity(count);
            for _ in 0..count {
                members.push(read_type(input, budget, depth + 1)?);
            }
            ValueType::Tuple(members)
        }
        14 => ValueType::Array(Box::new(read_type(input, budget, depth + 1)?)),
        15 => ValueType::Nullable(Box::new(read_type(input, budget, depth + 1)?)),
        16 => ValueType::Record(Box::new(read_fields(input, budget, depth + 1)?)),
        17 => {
            let registry_id = take(input)?;
            let name: String = take(input)?;
            let count = count(input, *budget)?;
            let mut cases = Vec::with_capacity(count);
            for _ in 0..count {
                *budget = budget
                    .checked_sub(1)
                    .ok_or("binding descriptor node limit exceeded")?;
                let name: String = take(input)?;
                let payload = read_fields(input, budget, depth + 1)?;
                cases.push(groove::records::EnumCase::new(name, payload));
            }
            ValueType::Enum(Box::new(
                EnumSchema::new(name, cases)
                    .map_err(|error| error.to_string())?
                    .with_registry_id(registry_id),
            ))
        }
        _ => return Err("unknown binding value type".to_owned()),
    })
}
