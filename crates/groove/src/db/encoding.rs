use super::*;
use crate::records::ValidatedVariantRecord;

pub(super) fn resolve_record_input(
    table: &TableSchema,
    input: &RecordInput,
    descriptor: RecordDescriptor,
) -> Result<(u32, RecordDescriptor, Vec<u8>), Error> {
    match input {
        RecordInput::Values(values) => {
            let variant_tag = 0;
            let record = encode_record(table, descriptor, values)?;
            Ok((variant_tag, descriptor, record))
        }
        RecordInput::Record(record) => resolve_variant_record(table, record, descriptor),
    }
}

pub(super) fn resolve_raw_record_input(
    table: &TableSchema,
    input: &RawRecordInput,
    descriptor: RecordDescriptor,
) -> Result<(u32, RecordDescriptor, Vec<u8>), Error> {
    match input {
        RawRecordInput::Payload(payload) => {
            let variant_tag = 0;
            Ok((variant_tag, descriptor, payload.clone()))
        }
        RawRecordInput::Record(record) => resolve_variant_record(table, record, descriptor),
        RawRecordInput::ValidatedRecord(record) => {
            resolve_validated_variant_record(table, record, descriptor)
        }
    }
}

pub(super) fn resolve_owned_record_input(
    table: &TableSchema,
    input: RecordInput,
    descriptor: RecordDescriptor,
) -> Result<(u32, RecordDescriptor, Vec<u8>), Error> {
    match input {
        RecordInput::Values(values) => {
            let variant_tag = 0;
            let record = encode_record(table, descriptor, &values)?;
            Ok((variant_tag, descriptor, record))
        }
        RecordInput::Record(record) => resolve_owned_variant_record(table, record, descriptor),
    }
}

pub(super) fn resolve_owned_raw_record_input(
    table: &TableSchema,
    input: RawRecordInput,
    descriptor: RecordDescriptor,
) -> Result<(u32, RecordDescriptor, Vec<u8>), Error> {
    match input {
        RawRecordInput::Payload(payload) => Ok((0, descriptor, payload)),
        RawRecordInput::Record(record) => resolve_owned_variant_record(table, record, descriptor),
        RawRecordInput::ValidatedRecord(record) => {
            resolve_owned_validated_variant_record(table, record, descriptor)
        }
    }
}

fn resolve_validated_variant_record(
    table: &TableSchema,
    record: &ValidatedVariantRecord,
    descriptor: RecordDescriptor,
) -> Result<(u32, RecordDescriptor, Vec<u8>), Error> {
    let variant_tag = record.variant_tag();
    if !record.descriptor().registry_compatible_with(&descriptor) {
        return Err(Error::SchemaVersionDescriptorMismatch {
            table: table.name.clone(),
            version: u64::from(variant_tag),
        });
    }
    let (_, record) = record.clone().into_parts();
    Ok((variant_tag, descriptor, record.into_raw()))
}

fn resolve_owned_validated_variant_record(
    table: &TableSchema,
    record: ValidatedVariantRecord,
    descriptor: RecordDescriptor,
) -> Result<(u32, RecordDescriptor, Vec<u8>), Error> {
    let variant_tag = record.variant_tag();
    if !record.descriptor().registry_compatible_with(&descriptor) {
        return Err(Error::SchemaVersionDescriptorMismatch {
            table: table.name.clone(),
            version: u64::from(variant_tag),
        });
    }
    let (_, record) = record.into_parts();
    Ok((variant_tag, descriptor, record.into_raw()))
}

fn resolve_owned_variant_record(
    table: &TableSchema,
    record: VariantRecord,
    descriptor: RecordDescriptor,
) -> Result<(u32, RecordDescriptor, Vec<u8>), Error> {
    let variant_tag = record.variant_tag();
    if !record
        .record()
        .descriptor()
        .registry_compatible_with(&descriptor)
    {
        return Err(Error::SchemaVersionDescriptorMismatch {
            table: table.name.clone(),
            version: u64::from(variant_tag),
        });
    }
    record.record().validate()?;
    let (_, record) = record.into_parts();
    Ok((variant_tag, descriptor, record.into_raw()))
}

pub(super) fn resolve_variant_record(
    table: &TableSchema,
    record: &VariantRecord,
    descriptor: RecordDescriptor,
) -> Result<(u32, RecordDescriptor, Vec<u8>), Error> {
    let variant_tag = record.variant_tag();
    if !record
        .record()
        .descriptor()
        .registry_compatible_with(&descriptor)
    {
        return Err(Error::SchemaVersionDescriptorMismatch {
            table: table.name.clone(),
            version: u64::from(variant_tag),
        });
    }
    record.record().validate()?;
    Ok((variant_tag, descriptor, record.record().raw().to_vec()))
}

pub(super) fn encode_record(
    table: &TableSchema,
    descriptor: RecordDescriptor,
    values: &[Value],
) -> Result<Vec<u8>, Error> {
    if table.columns.len() != values.len() {
        return Err(records::Error::ArityMismatch {
            expected: table.columns.len(),
            actual: values.len(),
        }
        .into());
    }
    // Callers provide values in SQL declaration order. RecordDescriptor stores
    // fixed-width fields first, so we reorder here before positional encoding.
    let values_by_descriptor_order = descriptor
        .fields()
        .iter()
        .map(|field| {
            let name = field
                .name
                .as_deref()
                .ok_or(records::Error::FieldNotFound("<unnamed>".to_owned()))?;
            let declaration_idx = table
                .columns
                .iter()
                .position(|column| column.name == name)
                .ok_or_else(|| records::Error::FieldNotFound(name.to_owned()))?;
            values
                .get(declaration_idx)
                .cloned()
                .ok_or(records::Error::ArityMismatch {
                    expected: table.columns.len(),
                    actual: values.len(),
                })
        })
        .collect::<Result<Vec<_>, records::Error>>()?;
    Ok(descriptor.create(&values_by_descriptor_order)?)
}

pub(super) fn primary_key_bytes(
    table: &TableSchema,
    variant_tag: u32,
    record_schema: RecordDescriptor,
    record: &[u8],
) -> Result<Vec<u8>, Error> {
    let primary_key = table
        .primary_key
        .as_ref()
        .ok_or_else(|| Error::MissingPrimaryKey(table.name.clone()))?;

    let mut bytes = Vec::new();
    for column in &primary_key.columns {
        let local_name = if table.variants.is_empty() {
            column.column.as_str()
        } else {
            table
                .variant(variant_tag)
                .and_then(|variant| variant.payload_name_for_shared(&column.column))
                .ok_or_else(|| Error::SchemaVersionMissingPrimaryKey {
                    table: table.name.clone(),
                    version: u64::from(variant_tag),
                    column: column.column.clone(),
                })?
        };
        let value = record_schema.get(record, local_name)?;
        ensure_primary_key_value_type(table, column, &value)?;
        encode_primary_key_part(&mut bytes, &value)?;
    }
    Ok(bytes)
}

impl Database {
    pub(super) fn decode_stored_key_value<'a>(
        &self,
        table: &TableSchema,
        key: Vec<u8>,
        stored: Vec<u8>,
    ) -> Result<EncodedKeyValue<'a>, Error> {
        let (variant_tag, payload) = split_variant_record(&stored)?;
        let cached = self
            .stored_record_descriptors
            .borrow()
            .get(&table.name)
            .and_then(|variants| variants.get(&variant_tag))
            .copied();
        let descriptor = if let Some(descriptor) = cached {
            descriptor
        } else {
            let descriptor = table
                .record_schema_for_variant(variant_tag)
                .ok_or_else(|| Error::UnknownTableVariant {
                    table: table.name.clone(),
                    version: u64::from(variant_tag),
                })?;
            self.stored_record_descriptors
                .borrow_mut()
                .entry(table.name.clone())
                .or_default()
                .insert(variant_tag, descriptor);
            descriptor
        };
        Ok(EncodedKeyValue::from_variant(
            key,
            VariantRecord::new(variant_tag, OwnedRecord::new(payload.to_vec(), descriptor)),
        ))
    }
}

pub(crate) fn persisted_index_primary_key(
    table: &TableSchema,
    index_name: &str,
    index: &IndexSchema,
    storage_key: &[u8],
    stored_value: &Value,
) -> Result<Vec<u8>, Error> {
    let logical_key = persisted_index_logical_key(table, index_name, storage_key)?;
    if index_key_covers_primary_key(table, index)? {
        return primary_key_from_index_columns(table, index_name, index, &logical_key);
    }
    if let Some(primary_key) =
        primary_key_from_appended_index_suffix(table, index_name, index, &logical_key)?
    {
        return Ok(primary_key);
    }
    let Value::Bytes(primary_key) = stored_value else {
        return Err(Error::InvalidPersistedIndex(index_name.to_owned()));
    };
    validate_primary_key_bytes(table, index_name, primary_key)?;
    Ok(primary_key.clone())
}

pub(super) fn persisted_index_logical_key(
    table: &TableSchema,
    index_name: &str,
    storage_key: &[u8],
) -> Result<Vec<u8>, Error> {
    let prefix = durable_index_key_prefix(&table.name, index_name);
    let mut remaining = storage_key
        .strip_prefix(prefix.as_slice())
        .ok_or_else(|| Error::InvalidPersistedIndex(index_name.to_owned()))?;
    expect_persisted_index_key_tag(&mut remaining, index_name, 7)?;
    let logical_key = decode_persisted_index_ordered_bytes(&mut remaining, index_name)?;
    if !remaining.is_empty() {
        return Err(Error::InvalidPersistedIndex(index_name.to_owned()));
    }
    Ok(logical_key)
}

pub(super) fn index_key_covers_primary_key(
    table: &TableSchema,
    index: &IndexSchema,
) -> Result<bool, Error> {
    let primary_key = table
        .primary_key
        .as_ref()
        .ok_or_else(|| Error::MissingPrimaryKey(table.name.clone()))?;
    Ok(primary_key
        .columns
        .iter()
        .all(|primary_key_column| index.columns.contains(&primary_key_column.column)))
}

pub(super) fn primary_key_from_index_columns(
    table: &TableSchema,
    index_name: &str,
    index: &IndexSchema,
    logical_key: &[u8],
) -> Result<Vec<u8>, Error> {
    let primary_key = table
        .primary_key
        .as_ref()
        .ok_or_else(|| Error::MissingPrimaryKey(table.name.clone()))?;
    let mut remaining = logical_key;
    let mut index_values = Vec::with_capacity(index.columns.len());
    for column_name in &index.columns {
        let column = table
            .columns
            .iter()
            .find(|column| column.name == *column_name)
            .ok_or_else(|| Error::InvalidPersistedIndex(index_name.to_owned()))?;
        index_values.push(decode_index_key_part(
            &mut remaining,
            &column.column_type,
            index_name,
        )?);
    }
    if !remaining.is_empty() {
        return Err(Error::InvalidPersistedIndex(index_name.to_owned()));
    }

    let mut bytes = Vec::new();
    for primary_key_column in &primary_key.columns {
        let index_position = index
            .columns
            .iter()
            .position(|column| column == &primary_key_column.column)
            .ok_or_else(|| Error::InvalidPersistedIndex(index_name.to_owned()))?;
        let value = index_values
            .get(index_position)
            .ok_or_else(|| Error::InvalidPersistedIndex(index_name.to_owned()))?;
        ensure_primary_key_value_type(table, primary_key_column, value)?;
        encode_primary_key_part(&mut bytes, value)?;
    }
    Ok(bytes)
}

pub(super) fn primary_key_from_appended_index_suffix(
    table: &TableSchema,
    index_name: &str,
    index: &IndexSchema,
    logical_key: &[u8],
) -> Result<Option<Vec<u8>>, Error> {
    let mut remaining = logical_key;
    for column_name in &index.columns {
        let column = table
            .columns
            .iter()
            .find(|column| column.name == *column_name)
            .ok_or_else(|| Error::InvalidPersistedIndex(index_name.to_owned()))?;
        let _ = decode_index_key_part(&mut remaining, &column.column_type, index_name)?;
    }
    if remaining.first() != Some(&0xff) {
        return Ok(None);
    }
    let primary_key = remaining[1..].to_vec();
    validate_primary_key_bytes(table, index_name, &primary_key)?;
    Ok(Some(primary_key))
}

pub(super) fn validate_primary_key_bytes(
    table: &TableSchema,
    index_name: &str,
    primary_key: &[u8],
) -> Result<(), Error> {
    let table_primary_key = table
        .primary_key
        .as_ref()
        .ok_or_else(|| Error::MissingPrimaryKey(table.name.clone()))?;
    let mut remaining = primary_key;
    for column in &table_primary_key.columns {
        decode_primary_key_part(&mut remaining, &column.key_type.column_type().clone())
            .map_err(|_| Error::InvalidPersistedIndex(index_name.to_owned()))?;
    }
    if !remaining.is_empty() {
        return Err(Error::InvalidPersistedIndex(index_name.to_owned()));
    }
    Ok(())
}

pub(super) fn ensure_primary_key_value_type(
    table: &TableSchema,
    column: &PrimaryKeyColumn,
    value: &Value,
) -> Result<(), Error> {
    match (&column.key_type, value) {
        (PrimaryKeyType::Integer(IntegerKeyType::U8), Value::U8(_))
        | (PrimaryKeyType::Integer(IntegerKeyType::U16), Value::U16(_))
        | (PrimaryKeyType::Integer(IntegerKeyType::U32), Value::U32(_))
        | (PrimaryKeyType::Integer(IntegerKeyType::U64), Value::U64(_))
        | (PrimaryKeyType::Bool, Value::Bool(_))
        | (PrimaryKeyType::String, Value::String(_))
        | (PrimaryKeyType::Bytes, Value::Bytes(_))
        | (PrimaryKeyType::Uuid, Value::Uuid(_)) => Ok(()),
        _ => Err(Error::PrimaryKeyTypeMismatch {
            table: table.name.clone(),
            column: column.column.clone(),
        }),
    }
}

pub(super) fn encode_primary_key_part(key: &mut Vec<u8>, value: &Value) -> Result<(), Error> {
    match value {
        Value::U8(value) => {
            key.push(0);
            key.push(*value);
        }
        Value::U16(value) => {
            key.push(1);
            key.extend(value.to_be_bytes());
        }
        Value::U32(value) => {
            key.push(2);
            key.extend(value.to_be_bytes());
        }
        Value::U64(value) => {
            key.push(3);
            key.extend(value.to_be_bytes());
        }
        Value::I32(value) => {
            key.push(14);
            key.extend(order_preserving_i32_bits(*value).to_be_bytes());
        }
        Value::I64(value) => {
            key.push(13);
            key.extend(order_preserving_i64_bits(*value).to_be_bytes());
        }
        Value::Bool(value) => {
            key.push(5);
            key.push(u8::from(*value));
        }
        Value::String(value) => {
            key.push(6);
            encode_ordered_bytes(key, value.as_bytes());
        }
        Value::EnumTag(value) => {
            key.push(0);
            key.push(*value);
        }
        Value::Bytes(value) => {
            key.push(7);
            encode_ordered_bytes(key, value);
        }
        Value::Uuid(value) => {
            key.push(10);
            key.extend_from_slice(value.as_bytes());
        }
        Value::Tuple(values) => {
            key.push(11);
            for value in values {
                encode_primary_key_part(key, value)?;
            }
        }
        Value::F64(_)
        | Value::Array(_)
        | Value::Nullable(_)
        | Value::Record(_)
        | Value::Large(_)
        // Direct-store keys require a declared total order; enums do not have one.
        | Value::Enum(_) => {
            return Err(Error::InvalidDirectRecordStoreKey(
                "unsupported direct record store key type".to_owned(),
            ));
        }
    }
    Ok(())
}

pub(super) fn encode_ordered_bytes(key: &mut Vec<u8>, value: &[u8]) {
    for byte in value {
        if *byte == 0 {
            key.extend([0, 0xff]);
        } else {
            key.push(*byte);
        }
    }
    key.extend([0, 0]);
}

pub(super) fn order_preserving_i64_bits(value: i64) -> u64 {
    (value as u64) ^ (1_u64 << 63)
}

pub(super) fn order_preserving_i32_bits(value: i32) -> u32 {
    (value as u32) ^ (1_u32 << 31)
}

pub(super) fn decode_primary_key_part(
    bytes: &mut &[u8],
    value_type: &records::ValueType,
) -> Result<Value, Error> {
    match value_type {
        records::ValueType::U8 => {
            expect_key_tag(bytes, 0)?;
            let value = take_key_bytes(bytes, 1)?[0];
            Ok(Value::U8(value))
        }
        records::ValueType::U16 => {
            expect_key_tag(bytes, 1)?;
            let value = u16::from_be_bytes(
                take_key_bytes(bytes, 2)?
                    .try_into()
                    .expect("slice has u16 length"),
            );
            Ok(Value::U16(value))
        }
        records::ValueType::U32 => {
            expect_key_tag(bytes, 2)?;
            let value = u32::from_be_bytes(
                take_key_bytes(bytes, 4)?
                    .try_into()
                    .expect("slice has u32 length"),
            );
            Ok(Value::U32(value))
        }
        records::ValueType::U64 => {
            expect_key_tag(bytes, 3)?;
            let value = u64::from_be_bytes(
                take_key_bytes(bytes, 8)?
                    .try_into()
                    .expect("slice has u64 length"),
            );
            Ok(Value::U64(value))
        }
        records::ValueType::I32 => {
            expect_key_tag(bytes, 14)?;
            let value = u32::from_be_bytes(
                take_key_bytes(bytes, 4)?
                    .try_into()
                    .expect("slice has i32 length"),
            );
            Ok(Value::I32((value ^ (1_u32 << 31)) as i32))
        }
        records::ValueType::I64 => {
            expect_key_tag(bytes, 13)?;
            let value = u64::from_be_bytes(
                take_key_bytes(bytes, 8)?
                    .try_into()
                    .expect("slice has i64 length"),
            );
            Ok(Value::I64((value ^ (1_u64 << 63)) as i64))
        }
        records::ValueType::Bool => {
            expect_key_tag(bytes, 5)?;
            match take_key_bytes(bytes, 1)?[0] {
                0 => Ok(Value::Bool(false)),
                1 => Ok(Value::Bool(true)),
                _ => Err(Error::InvalidDirectRecordStoreKey("bool".to_owned())),
            }
        }
        records::ValueType::String => {
            expect_key_tag(bytes, 6)?;
            let value = decode_ordered_bytes(bytes)?;
            Ok(Value::String(String::from_utf8(value).map_err(|_| {
                Error::InvalidDirectRecordStoreKey("string".to_owned())
            })?))
        }
        records::ValueType::Bytes => {
            expect_key_tag(bytes, 7)?;
            Ok(Value::Bytes(decode_ordered_bytes(bytes)?))
        }
        records::ValueType::Uuid => {
            expect_key_tag(bytes, 10)?;
            let value = uuid::Uuid::from_bytes(
                take_key_bytes(bytes, 16)?
                    .try_into()
                    .expect("slice has uuid length"),
            );
            Ok(Value::Uuid(value))
        }
        records::ValueType::EnumTag(_) => {
            expect_key_tag(bytes, 0)?;
            let value = take_key_bytes(bytes, 1)?[0];
            Ok(Value::EnumTag(value))
        }
        records::ValueType::Tuple(members) => {
            expect_key_tag(bytes, 11)?;
            let mut values = Vec::with_capacity(members.len());
            for member in members {
                values.push(decode_primary_key_part(bytes, member)?);
            }
            Ok(Value::Tuple(values))
        }
        records::ValueType::F64
        | records::ValueType::Internal(_)
        | records::ValueType::Array(_)
        | records::ValueType::Nullable(_)
        | records::ValueType::Record(_)
        | records::ValueType::Enum(_) => Err(Error::InvalidDirectRecordStoreKey(
            "unsupported direct record store key type".to_owned(),
        )),
    }
}

pub(super) fn decode_index_key_part(
    bytes: &mut &[u8],
    column_type: &ColumnType,
    index_name: &str,
) -> Result<Value, Error> {
    match column_type {
        ColumnType::U8 => {
            expect_persisted_index_key_tag(bytes, index_name, 0)?;
            Ok(Value::U8(
                take_persisted_index_key_bytes(bytes, index_name, 1)?[0],
            ))
        }
        ColumnType::U16 => {
            expect_persisted_index_key_tag(bytes, index_name, 1)?;
            Ok(Value::U16(u16::from_be_bytes(
                take_persisted_index_key_bytes(bytes, index_name, 2)?
                    .try_into()
                    .expect("slice has u16 length"),
            )))
        }
        ColumnType::U32 => {
            expect_persisted_index_key_tag(bytes, index_name, 2)?;
            Ok(Value::U32(u32::from_be_bytes(
                take_persisted_index_key_bytes(bytes, index_name, 4)?
                    .try_into()
                    .expect("slice has u32 length"),
            )))
        }
        ColumnType::U64 => {
            expect_persisted_index_key_tag(bytes, index_name, 3)?;
            Ok(Value::U64(u64::from_be_bytes(
                take_persisted_index_key_bytes(bytes, index_name, 8)?
                    .try_into()
                    .expect("slice has u64 length"),
            )))
        }
        ColumnType::I32 => {
            expect_persisted_index_key_tag(bytes, index_name, 14)?;
            Ok(Value::I32(
                (u32::from_be_bytes(
                    take_persisted_index_key_bytes(bytes, index_name, 4)?
                        .try_into()
                        .expect("slice has i32 length"),
                ) ^ (1_u32 << 31)) as i32,
            ))
        }
        ColumnType::I64 => {
            expect_persisted_index_key_tag(bytes, index_name, 13)?;
            Ok(Value::I64(
                (u64::from_be_bytes(
                    take_persisted_index_key_bytes(bytes, index_name, 8)?
                        .try_into()
                        .expect("slice has i64 length"),
                ) ^ (1_u64 << 63)) as i64,
            ))
        }
        ColumnType::F64 => {
            expect_persisted_index_key_tag(bytes, index_name, 4)?;
            let ordered = u64::from_be_bytes(
                take_persisted_index_key_bytes(bytes, index_name, 8)?
                    .try_into()
                    .expect("slice has u64 length"),
            );
            let bits = if ordered & (1 << 63) != 0 {
                ordered ^ (1 << 63)
            } else {
                !ordered
            };
            let value = f64::from_bits(bits);
            if value.is_nan() {
                return Err(Error::InvalidPersistedIndex(index_name.to_owned()));
            }
            Ok(Value::F64(value))
        }
        ColumnType::Bool => {
            expect_persisted_index_key_tag(bytes, index_name, 5)?;
            match take_persisted_index_key_bytes(bytes, index_name, 1)?[0] {
                0 => Ok(Value::Bool(false)),
                1 => Ok(Value::Bool(true)),
                _ => Err(Error::InvalidPersistedIndex(index_name.to_owned())),
            }
        }
        ColumnType::String => {
            expect_persisted_index_key_tag(bytes, index_name, 6)?;
            let value = decode_persisted_index_ordered_bytes(bytes, index_name)?;
            Ok(Value::String(String::from_utf8(value).map_err(|_| {
                Error::InvalidPersistedIndex(index_name.to_owned())
            })?))
        }
        ColumnType::Bytes => {
            expect_persisted_index_key_tag(bytes, index_name, 7)?;
            Ok(Value::Bytes(decode_persisted_index_ordered_bytes(
                bytes, index_name,
            )?))
        }
        ColumnType::Uuid => {
            expect_persisted_index_key_tag(bytes, index_name, 10)?;
            Ok(Value::Uuid(uuid::Uuid::from_bytes(
                take_persisted_index_key_bytes(bytes, index_name, 16)?
                    .try_into()
                    .expect("slice has uuid length"),
            )))
        }
        ColumnType::EnumTag(schema) => {
            expect_persisted_index_key_tag(bytes, index_name, 0)?;
            let discriminant = take_persisted_index_key_bytes(bytes, index_name, 1)?[0];
            schema
                .variant(discriminant)
                .map_err(|_| Error::InvalidPersistedIndex(index_name.to_owned()))?;
            Ok(Value::EnumTag(discriminant))
        }
        ColumnType::Tuple(members) => {
            expect_persisted_index_key_tag(bytes, index_name, 11)?;
            let mut values = Vec::with_capacity(members.len());
            for member in members {
                values.push(decode_index_key_part(bytes, member, index_name)?);
            }
            Ok(Value::Tuple(values))
        }
        ColumnType::Nullable(inner) => {
            match take_persisted_index_key_bytes(bytes, index_name, 1)?[0] {
                8 => Ok(Value::Nullable(None)),
                9 => Ok(Value::Nullable(Some(Box::new(decode_index_key_part(
                    bytes, inner, index_name,
                )?)))),
                _ => Err(Error::InvalidPersistedIndex(index_name.to_owned())),
            }
        }
        ColumnType::Internal(_)
        | ColumnType::Array(_)
        | ColumnType::Record(_)
        | ColumnType::Enum(_) => Err(Error::InvalidPersistedIndex(index_name.to_owned())),
    }
}

pub(super) fn expect_key_tag(bytes: &mut &[u8], expected: u8) -> Result<(), Error> {
    let actual = take_key_bytes(bytes, 1)?[0];
    if actual == expected {
        Ok(())
    } else {
        Err(Error::InvalidDirectRecordStoreKey("tag".to_owned()))
    }
}

pub(super) fn take_key_bytes<'a>(bytes: &mut &'a [u8], len: usize) -> Result<&'a [u8], Error> {
    if bytes.len() < len {
        return Err(Error::InvalidDirectRecordStoreKey("truncated".to_owned()));
    }
    let (head, tail) = bytes.split_at(len);
    *bytes = tail;
    Ok(head)
}

pub(super) fn expect_persisted_index_key_tag(
    bytes: &mut &[u8],
    index_name: &str,
    expected: u8,
) -> Result<(), Error> {
    let actual = take_persisted_index_key_bytes(bytes, index_name, 1)?[0];
    if actual == expected {
        Ok(())
    } else {
        Err(Error::InvalidPersistedIndex(index_name.to_owned()))
    }
}

pub(super) fn take_persisted_index_key_bytes<'a>(
    bytes: &mut &'a [u8],
    index_name: &str,
    len: usize,
) -> Result<&'a [u8], Error> {
    if bytes.len() < len {
        return Err(Error::InvalidPersistedIndex(index_name.to_owned()));
    }
    let (head, tail) = bytes.split_at(len);
    *bytes = tail;
    Ok(head)
}

pub(super) fn decode_persisted_index_ordered_bytes(
    bytes: &mut &[u8],
    index_name: &str,
) -> Result<Vec<u8>, Error> {
    let mut out = Vec::new();
    loop {
        let byte = take_persisted_index_key_bytes(bytes, index_name, 1)?[0];
        if byte != 0 {
            out.push(byte);
            continue;
        }
        match take_persisted_index_key_bytes(bytes, index_name, 1)?[0] {
            0 => return Ok(out),
            0xff => out.push(0),
            _ => return Err(Error::InvalidPersistedIndex(index_name.to_owned())),
        }
    }
}

pub(super) fn decode_ordered_bytes(bytes: &mut &[u8]) -> Result<Vec<u8>, Error> {
    let mut out = Vec::new();
    loop {
        let byte = take_key_bytes(bytes, 1)?[0];
        if byte != 0 {
            out.push(byte);
            continue;
        }
        match take_key_bytes(bytes, 1)?[0] {
            0 => return Ok(out),
            0xff => out.push(0),
            _ => return Err(Error::InvalidDirectRecordStoreKey("bytes".to_owned())),
        }
    }
}

pub(crate) fn index_record_descriptor() -> RecordDescriptor {
    static DESCRIPTOR: std::sync::OnceLock<RecordDescriptor> = std::sync::OnceLock::new();
    *DESCRIPTOR.get_or_init(|| {
        RecordDescriptor::new([
            ("key", records::ValueType::Bytes),
            ("value", records::ValueType::Bytes),
        ])
    })
}

pub(super) fn encode_index_prefix_part(
    key: &mut Vec<u8>,
    value: &Value,
    column_type: &ColumnType,
) -> Result<(), Error> {
    match (value, column_type) {
        (Value::String(variant), ColumnType::EnumTag(schema)) => {
            encode_key_part(key, &Value::U8(schema.discriminant(variant)?))
                .map_err(Error::IvmRuntime)
        }
        (Value::EnumTag(discriminant), ColumnType::EnumTag(schema)) => {
            schema
                .variant(*discriminant)
                .map_err(Error::RecordEncoding)?;
            encode_key_part(key, &Value::U8(*discriminant)).map_err(Error::IvmRuntime)
        }
        (Value::Nullable(None), ColumnType::Nullable(_)) => {
            encode_key_part(key, &Value::Nullable(None)).map_err(Error::IvmRuntime)
        }
        (Value::Nullable(Some(value)), ColumnType::Nullable(inner)) => {
            let mut encoded = Vec::new();
            encode_index_prefix_part(&mut encoded, value, inner)?;
            let mut wrapped = Vec::new();
            wrapped.push(9);
            wrapped.extend(encoded);
            key.extend(wrapped);
            Ok(())
        }
        _ => encode_key_part(key, value).map_err(Error::IvmRuntime),
    }
}
