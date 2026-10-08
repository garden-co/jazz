//! Role-specific runtime result publication schemas. Execution identities remain local.

use super::Error;
use super::query_engine::AggregateResultSchema;
use crate::protocol::ResultMemberPayloadEntry;
use groove::records::{self, BorrowedRecord, DescriptorField, OwnedRecord, RecordDescriptor};

const RESULT_DESCRIPTOR_MAGIC: &[u8; 5] = b"JRPD\x01";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum ResultDescriptorRole {
    Current = 0,
    Aggregate = 1,
}

pub(super) fn encode_result_descriptor(
    role: ResultDescriptorRole,
    descriptor: &RecordDescriptor,
) -> Result<Vec<u8>, Error> {
    validate_role_fields(role, descriptor)?;
    let mut bytes = RESULT_DESCRIPTOR_MAGIC.to_vec();
    bytes.push(role as u8);
    bytes.extend(records::encode_persisted_record_descriptor(descriptor)?);
    Ok(bytes)
}

fn validate_role_fields(
    role: ResultDescriptorRole,
    descriptor: &RecordDescriptor,
) -> Result<(), Error> {
    let invalid = || Error::InvalidStoredValue("result descriptor field roles are not canonical");
    let mut group_ordinal = 0usize;
    let mut value_ordinal = 0usize;
    let mut values_started = false;
    let mut occurrence_ordinal = 1usize;
    let mut last_union_position = None;
    let fields = descriptor.fields();
    let fields = if role == ResultDescriptorRole::Current {
        let first = fields.first().ok_or_else(invalid)?;
        if first.name.as_deref() != Some("metadata/row_uuid")
            || first.value_type != records::ValueType::Uuid
        {
            return Err(invalid());
        }
        &fields[1..]
    } else {
        fields
    };
    for (current_ordinal, field) in fields.iter().enumerate() {
        let name = field.name.as_deref().ok_or_else(invalid)?;
        let mut parts = name.splitn(3, '/');
        let tag = parts.next().ok_or_else(invalid)?;
        let ordinal = parts.next().ok_or_else(invalid)?;
        let _logical_name = parts.next().ok_or_else(invalid)?;
        let expected = match role {
            ResultDescriptorRole::Current
                if matches!(tag, "source" | "result" | "provenance" | "metadata")
                    && !values_started =>
            {
                current_ordinal
            }
            ResultDescriptorRole::Current
                if tag == "occurrence" && last_union_position.is_none() =>
            {
                if field.value_type != records::ValueType::Uuid {
                    return Err(invalid());
                }
                values_started = true;
                let index = occurrence_ordinal;
                occurrence_ordinal += 1;
                index
            }
            ResultDescriptorRole::Current if tag == "occurrence_union" => {
                let position = ordinal.parse::<usize>().map_err(|_| invalid())?;
                if field.value_type != records::ValueType::String
                    // Root is source position 0; joined sources occupy 1..N.
                    || position >= occurrence_ordinal
                    || last_union_position.is_some_and(|last| position <= last)
                {
                    return Err(invalid());
                }
                last_union_position = Some(position);
                position
            }
            ResultDescriptorRole::Aggregate if tag == "group" && !values_started => {
                let index = group_ordinal;
                group_ordinal += 1;
                index
            }
            ResultDescriptorRole::Aggregate if tag == "value" => {
                values_started = true;
                let index = value_ordinal;
                value_ordinal += 1;
                index
            }
            _ => return Err(invalid()),
        };
        if ordinal != expected.to_string() {
            return Err(invalid());
        }
    }
    Ok(())
}

fn aggregate_role_schema(schema: &AggregateResultSchema) -> Result<RecordDescriptor, Error> {
    if schema.group_names.len() != schema.group_key_fields.len()
        || schema.value_names.len() != schema.value_fields.len()
    {
        return Err(Error::InvalidStoredValue(
            "aggregate role schema has inconsistent identity counts",
        ));
    }
    let mut fields = Vec::new();
    for (role, names, source_fields) in [
        ("group", &schema.group_names, &schema.group_key_fields),
        ("value", &schema.value_names, &schema.value_fields),
    ] {
        fields.extend(names.iter().zip(source_fields).enumerate().map(
            |(ordinal, (name, field))| {
                DescriptorField::new(format!("{role}/{ordinal}/{name}"), field.value_type.clone())
            },
        ));
    }
    let descriptor = RecordDescriptor::new_with_fields(fields);
    // This round trip canonicalizes nested descriptor bindings too. The names
    // above are role-owned; nested names/types are schema-owned application data.
    Ok(records::decode_persisted_record_descriptor(
        &records::encode_persisted_record_descriptor(&descriptor)?,
    )?)
}

pub(super) struct PreparedAggregatePayload {
    pub descriptor: Vec<u8>,
    pub record: Vec<u8>,
    pub row_identity: Vec<u8>,
    pub replacement_identity: Vec<u8>,
}

fn aggregate_row_identity(
    schema: &AggregateResultSchema,
    canonical: &RecordDescriptor,
    values: &[records::Value],
) -> Result<Vec<u8>, Error> {
    match schema.group_key_fields.as_slice() {
        [] => super::codec::runtime_result_identity_bytes(
            &records::Value::String("global".to_owned()),
            &records::ValueType::String,
        ),
        [_] => super::codec::runtime_result_identity_bytes(
            &values[0],
            &canonical.fields()[0].value_type,
        ),
        _ => Err(Error::InvalidStoredValue(
            "aggregate payload has unsupported group identity",
        )),
    }
}

fn aggregate_replacement_identity(
    schema: &AggregateResultSchema,
    canonical: &RecordDescriptor,
    values: &[records::Value],
    descriptor: &[u8],
    record: &[u8],
) -> Result<Vec<u8>, Error> {
    match schema.value_fields.len() {
        0 => super::codec::runtime_result_identity_bytes(
            &records::Value::String("empty".to_owned()),
            &records::ValueType::String,
        ),
        1 => {
            let index = schema.group_key_fields.len();
            super::codec::runtime_result_identity_bytes(
                &values[index],
                &canonical.fields()[index].value_type,
            )
        }
        _ => {
            let mut hasher = blake3::Hasher::new();
            hasher.update(b"jazz aggregate replacement v1");
            hasher.update(&(descriptor.len() as u64).to_le_bytes());
            hasher.update(descriptor);
            hasher.update(&(record.len() as u64).to_le_bytes());
            hasher.update(record);
            super::codec::runtime_result_identity_bytes(
                &records::Value::Bytes(hasher.finalize().as_bytes().to_vec()),
                &records::ValueType::Bytes,
            )
        }
    }
}

pub(super) fn encode_aggregate_payload_record(
    record: BorrowedRecord<'_>,
    schema: &AggregateResultSchema,
) -> Result<PreparedAggregatePayload, Error> {
    let runtime_fields = schema.group_key_fields.iter().chain(&schema.value_fields);
    let values = runtime_fields
        .clone()
        .map(|field| {
            let index = record
                .descriptor()
                .field_index_by_identity(field.identity.as_ref().ok_or(
                    Error::InvalidStoredValue("aggregate role field has no execution binding"),
                )?)
                .ok_or(Error::InvalidStoredValue(
                    "aggregate record is missing its compiled role field",
                ))?;
            if record.descriptor().fields()[index].value_type != field.value_type {
                return Err(Error::InvalidStoredValue(
                    "aggregate role field type differs from compiler schema",
                ));
            }
            Ok(record.get_idx(index)?)
        })
        .collect::<Result<Vec<_>, Error>>()?;
    let runtime = RecordDescriptor::new_with_fields(runtime_fields.cloned());
    let raw = runtime.create(&values)?;
    let canonical = aggregate_role_schema(schema)?;
    let canonical_values = canonical.bind(&raw).to_values()?;
    if canonical.create(&canonical_values)? != raw {
        return Err(Error::InvalidStoredValue(
            "aggregate payload row is not canonical",
        ));
    }
    let descriptor = encode_result_descriptor(ResultDescriptorRole::Aggregate, &canonical)?;
    let row_identity = aggregate_row_identity(schema, &canonical, &canonical_values)?;
    let replacement_identity =
        aggregate_replacement_identity(schema, &canonical, &canonical_values, &descriptor, &raw)?;
    Ok(PreparedAggregatePayload {
        descriptor,
        record: raw,
        row_identity,
        replacement_identity,
    })
}

pub(super) fn decode_aggregate_payload_record(
    payload: &ResultMemberPayloadEntry,
    schema: &AggregateResultSchema,
) -> Result<OwnedRecord, Error> {
    let canonical = aggregate_role_schema(schema)?;
    if encode_result_descriptor(ResultDescriptorRole::Aggregate, &canonical)? != payload.descriptor
    {
        return Err(Error::InvalidStoredValue(
            "aggregate payload role schema differs from compiled output",
        ));
    }
    let values = canonical.bind(&payload.record).to_values()?;
    if canonical.create(&values)? != payload.record {
        return Err(Error::InvalidStoredValue(
            "aggregate payload row is not canonical",
        ));
    }
    let crate::protocol::ResultMemberEntry::Synthetic {
        row, replacement, ..
    } = &payload.member
    else {
        return Err(Error::InvalidStoredValue(
            "aggregate payload has no synthetic member identity",
        ));
    };
    if aggregate_row_identity(schema, &canonical, &values)? != *row
        || aggregate_replacement_identity(
            schema,
            &canonical,
            &values,
            &payload.descriptor,
            &payload.record,
        )? != replacement.encoded_record()
    {
        return Err(Error::InvalidStoredValue(
            "aggregate payload differs from its member identity",
        ));
    }
    // Exact canonical role/name/type equality above establishes the layout;
    // only now may the compiler's execution bindings interpret these bytes.
    let runtime = RecordDescriptor::new_with_fields(
        schema
            .group_key_fields
            .iter()
            .chain(&schema.value_fields)
            .cloned(),
    );
    Ok(OwnedRecord::new(payload.record.clone(), runtime))
}

fn current_role_schema(
    schema: &super::query_engine::ResultMembershipSchema,
) -> Result<RecordDescriptor, Error> {
    use super::{
        CurrentRowPublicationField as Publication, CurrentRowResultVisibility as Visibility,
    };
    let unique_sources = schema
        .occurrence_id_fields
        .iter()
        .collect::<std::collections::BTreeSet<_>>();
    if schema.occurrence_id_fields.first() != Some(&schema.row_field)
        || unique_sources.len() != schema.occurrence_id_fields.len()
        || schema
            .occurrence_union_arm_fields
            .keys()
            // Source positions are the actual contributor positions: root is
            // position 0 and joined sources occupy positions 1..N.
            .any(|position| *position >= schema.occurrence_id_fields.len())
    {
        return Err(Error::InvalidStoredValue(
            "result occurrence source declarations are invalid",
        ));
    }
    let mut fields = vec![DescriptorField::new(
        "metadata/row_uuid",
        records::ValueType::Uuid,
    )];
    for (ordinal, field) in schema
        .payload_fields
        .iter()
        .filter(|field| field.name != schema.row_field)
        .enumerate()
    {
        let binding =
            schema
                .payload_publication_fields
                .get(&field.name)
                .ok_or(Error::InvalidStoredValue(
                    "result payload field has no publication identity",
                ))?;
        let (role, name) = match binding {
            Publication::StoredColumn { output_name, .. }
            | Publication::UnresolvedSourceCell { output_name } => ("source", output_name),
            Publication::ResultField {
                name,
                visibility: Visibility::ApplicationCell,
            } => ("result", name),
            Publication::ResultField {
                name,
                visibility: Visibility::PublicProvenance,
            } => ("provenance", name),
            Publication::ResultField {
                name,
                visibility: Visibility::HiddenMetadata,
            } => ("metadata", name),
        };
        fields.push(DescriptorField::new(
            format!("{role}/{ordinal}/{name}"),
            field.ty.clone(),
        ));
    }
    for (ordinal, name) in schema.occurrence_id_fields.iter().enumerate().skip(1) {
        fields.push(DescriptorField::new(
            format!("occurrence/{ordinal}/{name}"),
            records::ValueType::Uuid,
        ));
    }
    for (position, name) in &schema.occurrence_union_arm_fields {
        fields.push(DescriptorField::new(
            format!("occurrence_union/{position}/{name}"),
            records::ValueType::String,
        ));
    }
    let descriptor = RecordDescriptor::new_with_fields(fields);
    Ok(records::decode_persisted_record_descriptor(
        &records::encode_persisted_record_descriptor(&descriptor)?,
    )?)
}

/// One compiled field mapping per source descriptor and publication schema.
/// The caller scopes reuse to a single terminal's delta batch.
pub(super) struct CurrentPayloadEncodePlan {
    projector: records::RecordProjector,
    descriptor: Vec<u8>,
}

impl CurrentPayloadEncodePlan {
    pub(super) fn new(
        descriptor: RecordDescriptor,
        schema: &super::query_engine::ResultMembershipSchema,
    ) -> Result<Self, Error> {
        let selected = std::iter::once(schema.row_field.as_str())
            .chain(
                schema
                    .payload_fields
                    .iter()
                    .filter(|field| field.name != schema.row_field)
                    .map(|field| field.name.as_str()),
            )
            .chain(
                schema
                    .occurrence_id_fields
                    .iter()
                    .skip(1)
                    .map(String::as_str),
            )
            .chain(
                schema
                    .occurrence_union_arm_fields
                    .values()
                    .map(String::as_str),
            );
        let mut mapping = Vec::new();
        let mut runtime_fields = Vec::new();
        for name in selected {
            let index = descriptor
                .fields()
                .iter()
                .position(|field| field.name.as_deref() == Some(name))
                .ok_or(Error::InvalidStoredValue(
                    "result payload is missing its declared carrier",
                ))?;
            if descriptor.fields()[index + 1..]
                .iter()
                .any(|field| field.name.as_deref() == Some(name))
            {
                return Err(Error::InvalidStoredValue(
                    "result payload carrier binding is ambiguous",
                ));
            }
            mapping.push((index, mapping.len()));
            runtime_fields.push(descriptor.fields()[index].clone());
        }
        let runtime = RecordDescriptor::new_with_fields(runtime_fields);
        let canonical = current_role_schema(schema)?;
        // A complete named/type tree comparison precedes any byte-layout reuse.
        for (source, target) in runtime.fields().iter().zip(canonical.fields()) {
            let source_type = RecordDescriptor::new([("value", source.value_type.clone())]);
            let target_type = RecordDescriptor::new([("value", target.value_type.clone())]);
            if records::encode_persisted_record_descriptor(&source_type)?
                != records::encode_persisted_record_descriptor(&target_type)?
            {
                return Err(Error::InvalidStoredValue(
                    "result payload type differs from its publication schema",
                ));
            }
        }
        let projector =
            records::RecordProjector::new_registry_rebound(descriptor, canonical, mapping)?;
        Ok(Self {
            projector,
            descriptor: encode_result_descriptor(ResultDescriptorRole::Current, &canonical)?,
        })
    }

    pub(super) fn encode(&self, record: BorrowedRecord<'_>) -> Result<(Vec<u8>, Vec<u8>), Error> {
        // Both descriptors are trusted compiled schemas. Copy the selected
        // encoded fields; do not decode and re-encode our own encoder's output.
        Ok((
            self.descriptor.clone(),
            self.projector.project(record)?.into_raw(),
        ))
    }
}

#[cfg(test)]
pub(super) fn encode_current_payload_record(
    record: BorrowedRecord<'_>,
    schema: &super::query_engine::ResultMembershipSchema,
) -> Result<(Vec<u8>, Vec<u8>), Error> {
    CurrentPayloadEncodePlan::new(record.descriptor(), schema)?.encode(record)
}

pub(super) fn decode_current_payload_record(
    table: &str,
    payload: &ResultMemberPayloadEntry,
    schema: &super::query_engine::ResultMembershipSchema,
) -> Result<super::CurrentRow, Error> {
    use super::{CurrentRowPublicationField, CurrentRowResultVisibility};
    let canonical = current_role_schema(schema)?;
    if encode_result_descriptor(ResultDescriptorRole::Current, &canonical)? != payload.descriptor {
        return Err(Error::InvalidStoredValue(
            "result payload role schema differs from compiled output",
        ));
    }
    let values = canonical.bind(&payload.record).to_values()?;
    if canonical.create(&values)? != payload.record {
        return Err(Error::InvalidStoredValue(
            "result payload row is not canonical",
        ));
    }
    if payload
        .member
        .as_row()
        .is_none_or(|member| values[0] != records::Value::Uuid(member.1.0))
    {
        return Err(Error::InvalidStoredValue(
            "result payload differs from its member row identity",
        ));
    }
    let mut fields = vec![DescriptorField::new("row_uuid", records::ValueType::Uuid)];
    let mut publications = vec![CurrentRowPublicationField::ResultField {
        name: "row_uuid".to_owned(),
        visibility: CurrentRowResultVisibility::HiddenMetadata,
    }];
    for field in schema
        .payload_fields
        .iter()
        .filter(|field| field.name != schema.row_field)
    {
        fields.push(DescriptorField::new(&field.name, field.ty.clone()));
        publications.push(
            schema
                .payload_publication_fields
                .get(&field.name)
                .cloned()
                .ok_or(Error::InvalidStoredValue(
                    "result payload field has no publication identity",
                ))?,
        );
    }
    let member_occurrence = payload.member.output_occurrence_id();
    if member_occurrence
        .as_ref()
        .map_or(0, |id| id.joined_sources().len())
        + 1
        != schema.occurrence_id_fields.len()
        || member_occurrence.as_ref().is_some_and(|id| {
            values[0] != records::Value::Uuid(*id.root_source().uuid())
                || id.union_arms().len() != schema.occurrence_union_arm_fields.len()
        })
    {
        return Err(Error::InvalidStoredValue(
            "result occurrence differs from compiled sources",
        ));
    }
    if schema.occurrence_id_fields.len() > 1 {
        let occurrence = payload
            .member
            .output_occurrence_id()
            .ok_or(Error::InvalidStoredValue(
                "joined result payload has no occurrence identity",
            ))?;
        if occurrence.joined_sources().len() + 1 != schema.occurrence_id_fields.len()
            || occurrence.union_arms().len() != schema.occurrence_union_arm_fields.len()
        {
            return Err(Error::InvalidStoredValue(
                "result occurrence differs from compiled sources",
            ));
        }
        for (name, source) in schema
            .occurrence_id_fields
            .iter()
            .skip(1)
            .zip(occurrence.joined_sources())
        {
            if values[fields.len()] != records::Value::Uuid(*source.uuid()) {
                return Err(Error::InvalidStoredValue(
                    "result payload differs from its joined source identity",
                ));
            }
            fields.push(DescriptorField::new(name, records::ValueType::Uuid));
            publications.push(CurrentRowPublicationField::ResultField {
                name: name.clone(),
                visibility: CurrentRowResultVisibility::HiddenMetadata,
            });
        }
        for ((position, name), (member_position, arm)) in schema
            .occurrence_union_arm_fields
            .iter()
            .zip(occurrence.union_arms())
        {
            if position != member_position
                || values[fields.len()] != records::Value::String(arm.clone())
            {
                return Err(Error::InvalidStoredValue(
                    "result payload differs from its union discriminator",
                ));
            }
            fields.push(DescriptorField::new(name, records::ValueType::String));
            publications.push(CurrentRowPublicationField::ResultField {
                name: name.clone(),
                visibility: CurrentRowResultVisibility::HiddenMetadata,
            });
        }
    }
    let descriptor = RecordDescriptor::new_with_fields(fields);
    let raw = descriptor.create(&values)?;
    Ok(super::CurrentRow::new_with_publication_fields(
        table,
        OwnedRecord::new(raw, descriptor),
        publications,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::query_engine::SyntheticResultMembershipSchema;
    use crate::protocol::{ResultMemberEntry, SyntheticReplacementToken};
    use groove::records::{FieldIdentity, Value, ValueType};
    use std::collections::BTreeSet;

    fn aggregate_schema(group_slot: u64, value_slot: u64) -> AggregateResultSchema {
        AggregateResultSchema {
            synthetic: SyntheticResultMembershipSchema {
                table_field: "table".to_owned(),
                row_field: "row".to_owned(),
                replacement_field: "replacement".to_owned(),
                routing_param_fields: BTreeSet::new(),
            },
            group_key_fields: vec![
                DescriptorField::new("private_group", ValueType::U64).with_identity(
                    FieldIdentity::NamedSlot {
                        name: "count".to_owned(),
                        slot: group_slot,
                    },
                ),
            ],
            group_names: vec!["count".to_owned()],
            value_fields: vec![
                DescriptorField::new("private_value", ValueType::U64).with_identity(
                    FieldIdentity::NamedSlot {
                        name: "count".to_owned(),
                        slot: value_slot,
                    },
                ),
            ],
            value_names: vec!["count".to_owned()],
            routing_param_fields: BTreeSet::new(),
        }
    }

    /// Alice's canonical multi-value publication distinguishes non-first changes
    /// and rejects mismatched members after Bob relocates execution bindings.
    /// Internal because public builders cannot select nested execution identities.
    #[test]
    fn complete_aggregate_identity_validates_non_first_nested_values_after_rebinding() {
        use groove::records::{EnumCase, EnumSchema, EnumValue};

        let schema = |slot| {
            let mut schema = aggregate_schema(slot, slot + 1);
            let child = RecordDescriptor::new_with_fields([DescriptorField::new(
                "label",
                ValueType::String,
            )
            .with_identity(FieldIdentity::Slot(slot + 3))]);
            let event = EnumSchema::new("event", [EnumCase::new("value", child)])
                .unwrap()
                .with_registry_id(987);
            schema.value_fields.push(
                DescriptorField::new("private_event", ValueType::Enum(Box::new(event)))
                    .with_identity(FieldIdentity::Slot(slot + 2)),
            );
            schema.value_names.push("event".to_owned());
            schema
        };
        let prepare = |schema: &AggregateResultSchema, label: &str| {
            let ValueType::Enum(event) = &schema.value_fields[1].value_type else {
                unreachable!()
            };
            let value = Value::Enum(
                EnumValue::create(
                    0,
                    event.cases[0].payload,
                    &[Value::String(label.to_owned())],
                )
                .unwrap(),
            );
            let runtime = RecordDescriptor::new_with_fields(
                schema
                    .group_key_fields
                    .iter()
                    .chain(&schema.value_fields)
                    .cloned(),
            );
            let raw = runtime
                .create(&[Value::U64(1), Value::U64(2), value])
                .unwrap();
            encode_aggregate_payload_record(runtime.bind(&raw), schema).unwrap()
        };
        let original = schema(10);
        let before = prepare(&original, "before");
        let after = prepare(&original, "after");
        assert_eq!(before.row_identity, after.row_identity);
        assert_ne!(before.replacement_identity, after.replacement_identity);
        let relocated = schema(800);
        let rebound = prepare(&relocated, "after");
        assert_eq!(after.descriptor, rebound.descriptor);
        assert_eq!(after.record, rebound.record);
        assert_eq!(after.replacement_identity, rebound.replacement_identity);
        let mut payload = ResultMemberPayloadEntry {
            member: ResultMemberEntry::Synthetic {
                table: "aggregate_result".to_owned(),
                row: after.row_identity,
                replacement: SyntheticReplacementToken::from_encoded_record(
                    after.replacement_identity,
                ),
            },
            descriptor: after.descriptor,
            record: after.record,
        };
        let decoded = decode_aggregate_payload_record(&payload, &relocated).unwrap();
        let Value::Enum(event) = decoded.get_idx(2).unwrap() else {
            panic!("typed nested event");
        };
        assert_eq!(
            event.record().get_idx(0),
            Ok(Value::String("after".to_owned()))
        );
        assert_eq!(
            decoded.descriptor().fields()[2].identity,
            relocated.value_fields[1].identity
        );
        let mut wrong_name = relocated.clone();
        wrong_name.value_names[1] = "another_event".to_owned();
        assert!(decode_aggregate_payload_record(&payload, &wrong_name).is_err());
        let mut wrong_registry = relocated.clone();
        let ValueType::Enum(event) = &mut wrong_registry.value_fields[1].value_type else {
            unreachable!()
        };
        **event = event.clone().with_registry_id(988);
        assert!(decode_aggregate_payload_record(&payload, &wrong_registry).is_err());
        payload.record = before.record;
        assert!(decode_aggregate_payload_record(&payload, &relocated).is_err());
    }

    /// Alice's empty and single-value totals keep their scalar identity bytes
    /// and scalar size bound independently of the complete-summary digest.
    /// Internal because these opaque byte contracts are not public row cells.
    #[test]
    fn empty_and_single_aggregate_identities_preserve_scalar_framing_and_limit() {
        let mut schema = aggregate_schema(1, 2);
        schema.group_key_fields.clear();
        schema.group_names.clear();
        let runtime = RecordDescriptor::new_with_fields(schema.value_fields.clone());
        let raw = runtime.create(&[Value::U64(2)]).unwrap();
        let prepared = encode_aggregate_payload_record(runtime.bind(&raw), &schema).unwrap();
        assert_eq!(
            prepared.replacement_identity,
            super::super::codec::runtime_result_identity_bytes(&Value::U64(2), &ValueType::U64)
                .unwrap(),
        );
        schema.value_fields.clear();
        schema.value_names.clear();
        let empty = RecordDescriptor::new_with_fields([]);
        let raw = empty.create(&[]).unwrap();
        let prepared = encode_aggregate_payload_record(empty.bind(&raw), &schema).unwrap();
        assert_eq!(
            prepared.replacement_identity,
            super::super::codec::runtime_result_identity_bytes(
                &Value::String("empty".to_owned()),
                &ValueType::String,
            )
            .unwrap(),
        );
        schema.value_fields.push(
            DescriptorField::new("large", ValueType::String).with_identity(FieldIdentity::Slot(3)),
        );
        schema.value_names.push("large".to_owned());
        let runtime = RecordDescriptor::new_with_fields(schema.value_fields.clone());
        let raw = runtime
            .create(&[Value::String("x".repeat(1024 * 1024))])
            .unwrap();
        assert!(encode_aggregate_payload_record(runtime.bind(&raw), &schema).is_err());
    }

    fn current_schema(
        local_id: u64,
        carrier: &str,
    ) -> crate::node::query_engine::ResultMembershipSchema {
        use crate::node::query_engine::{
            ContentVersionFields, ResultMembershipSchema, ResultMembershipVersionSchema,
            TypedOutputField,
        };
        ResultMembershipSchema {
            table_field: "table".to_owned(),
            row_field: "row_uuid".to_owned(),
            occurrence_id_fields: vec!["row_uuid".to_owned()],
            occurrence_union_arm_fields: std::collections::BTreeMap::new(),
            payload_fields: vec![TypedOutputField {
                name: carrier.to_owned(),
                ty: ValueType::String,
            }],
            payload_publication_fields: std::collections::BTreeMap::from([(
                carrier.to_owned(),
                crate::node::CurrentRowPublicationField::StoredColumn {
                    id: crate::ids::PhysicalColumnId(local_id),
                    output_name: "title".to_owned(),
                },
            )]),
            branch_or_prefix_field: None,
            version: ResultMembershipVersionSchema::Content(ContentVersionFields {
                tx_time_field: "tx_time".to_owned(),
                tx_node_field: "tx_node".to_owned(),
            }),
            settle_position_field: None,
            routing_param_fields: BTreeSet::new(),
        }
    }

    // Internal byte-compatibility test: the public API exposes values, not
    // runtime publication bytes or descriptor rebinding. Compare projection
    // against independently encoding the selected fields, including nested
    // enum payloads, nullable values, arrays and reordered source fields.
    #[test]
    fn current_payload_plan_preserves_encoded_fields_across_reuse() {
        use groove::records::{EnumCase, EnumSchema, EnumValue};
        let child = RecordDescriptor::new([("code", ValueType::U64), ("label", ValueType::String)]);
        let event = EnumSchema::new("event", [EnumCase::new("value", child)])
            .unwrap()
            .with_registry_id(987);
        let nested = RecordDescriptor::new([
            ("words", ValueType::Array(Box::new(ValueType::String))),
            (
                "event",
                ValueType::Nullable(Box::new(ValueType::Enum(Box::new(event)))),
            ),
        ]);
        let ty = ValueType::Record(Box::new(nested));
        let mut schema = current_schema(1, "payload");
        schema.payload_fields[0].ty = ty.clone();
        let source = RecordDescriptor::new([
            ("ignored", ty.clone()),
            ("payload", ty.clone()),
            ("row_uuid", ValueType::Uuid),
        ]);
        let selected = RecordDescriptor::new([("row_uuid", ValueType::Uuid), ("payload", ty)]);
        let plan = CurrentPayloadEncodePlan::new(source, &schema).unwrap();
        let canonical = current_role_schema(&schema).unwrap();
        for i in 0..32 {
            let value = Value::Record(OwnedRecord::new(
                nested
                    .create(&[
                        Value::Array(vec![
                            Value::String(format!("row-{i}")),
                            Value::String("longer value".repeat(i)),
                        ]),
                        Value::Nullable(if i % 2 == 0 {
                            Some(Box::new(Value::Enum(
                                EnumValue::create(
                                    0,
                                    child,
                                    &[Value::U64(i as u64), Value::String("nested".to_owned())],
                                )
                                .unwrap(),
                            )))
                        } else {
                            None
                        }),
                    ])
                    .unwrap(),
                nested,
            ));
            let ignored = Value::Record(OwnedRecord::new(
                nested
                    .create(&[Value::Array(vec![]), Value::Nullable(None)])
                    .unwrap(),
                nested,
            ));
            let id = Value::Uuid(uuid::Uuid::from_u128(i as u128 + 1));
            let raw = source
                .create(&[ignored, value.clone(), id.clone()])
                .unwrap();
            let (descriptor, projected) = plan.encode(source.bind(&raw)).unwrap();
            assert_eq!(projected, selected.create(&[id, value]).unwrap());
            assert_eq!(
                descriptor,
                encode_result_descriptor(ResultDescriptorRole::Current, &canonical).unwrap()
            );
        }
        let wrong_source = RecordDescriptor::new([
            ("row_uuid", ValueType::Uuid),
            ("payload", ValueType::String),
        ]);
        assert!(plan.encode(wrong_source.bind(&[])).is_err());
    }

    // Malformed member/schema pairings cannot be authored through the public
    // query API. Exercise the role decoder directly to prove it rejects them.
    #[test]
    fn current_roles_bind_ordered_join_sources_and_union_discriminators() {
        use crate::object::{ObjectId, OutputOccurrenceId};
        use crate::protocol::RealRowMemberEntry;
        let mut schema = current_schema(1, "_app_1");
        schema.occurrence_id_fields.extend([
            "__flat_join_row_1".to_owned(),
            "__flat_join_row_2".to_owned(),
        ]);
        schema
            .occurrence_union_arm_fields
            .insert(0, "__root_join_arm_0".to_owned());
        let root = uuid::Uuid::from_bytes([1; 16]);
        let first = uuid::Uuid::from_bytes([2; 16]);
        let second = uuid::Uuid::from_bytes([3; 16]);
        let runtime = RecordDescriptor::new([
            ("row_uuid", ValueType::Uuid),
            ("_app_1", ValueType::String),
            ("__flat_join_row_1", ValueType::Uuid),
            ("__flat_join_row_2", ValueType::Uuid),
            ("__root_join_arm_0", ValueType::String),
        ]);
        let raw = runtime
            .create(&[
                Value::Uuid(root),
                Value::String("published".to_owned()),
                Value::Uuid(first),
                Value::Uuid(second),
                Value::String("left".to_owned()),
            ])
            .unwrap();
        let (descriptor, record) =
            encode_current_payload_record(runtime.bind(&raw), &schema).unwrap();
        let member = |joined: Vec<uuid::Uuid>, arm: &str| {
            RealRowMemberEntry::current_content((
                "items".to_owned().into(),
                crate::ids::RowUuid(root),
                crate::tx::TxId::new(crate::time::TxTime::from(1), crate::ids::NodeUuid(root)),
            ))
            .with_occurrence_id(
                OutputOccurrenceId::with_union_arms(
                    ObjectId::from_uuid(root),
                    joined.into_iter().map(ObjectId::from_uuid),
                    [(0, arm.to_owned())],
                )
                .unwrap(),
            )
            .into()
        };
        let payload = ResultMemberPayloadEntry {
            member: member(vec![first, second], "left"),
            descriptor,
            record,
        };
        let decoded = decode_current_payload_record("items", &payload, &schema).unwrap();
        assert_eq!(
            decoded.raw_field("__flat_join_row_1"),
            Some(Value::Uuid(first))
        );
        assert_eq!(
            decoded.raw_field("__flat_join_row_2"),
            Some(Value::Uuid(second))
        );
        assert_eq!(
            decoded.raw_field("__root_join_arm_0"),
            Some(Value::String("left".to_owned()))
        );
        for wrong in [
            member(vec![second, first], "left"),
            member(vec![first], "left"),
            member(vec![first, second], "right"),
        ] {
            let mut bad = payload.clone();
            bad.member = wrong;
            assert!(decode_current_payload_record("items", &bad, &schema).is_err());
        }
        let single_schema = current_schema(1, "_app_1");
        let (single_descriptor, single_record) =
            encode_current_payload_record(runtime.bind(&raw), &single_schema).unwrap();
        let extra_sources = ResultMemberPayloadEntry {
            member: member(vec![first, second], "left"),
            descriptor: single_descriptor,
            record: single_record,
        };
        assert!(decode_current_payload_record("items", &extra_sources, &single_schema).is_err());
        let mut wrong_root = payload.clone();
        let mut row = payload.member.as_real_row().unwrap().clone();
        row.occurrence_id = Some(
            OutputOccurrenceId::with_union_arms(
                ObjectId::from_uuid(second),
                [ObjectId::from_uuid(first), ObjectId::from_uuid(second)],
                [(0, "left".to_owned())],
            )
            .unwrap(),
        );
        wrong_root.member = ResultMemberEntry::Row(row);
        assert!(decode_current_payload_record("items", &wrong_root, &schema).is_err());
        let mut reordered = schema.clone();
        reordered.occurrence_id_fields.swap(1, 2);
        assert!(decode_current_payload_record("items", &payload, &reordered).is_err());
        let mut changed_arm_role = schema.clone();
        let arm = changed_arm_role
            .occurrence_union_arm_fields
            .remove(&0)
            .unwrap();
        changed_arm_role.occurrence_union_arm_fields.insert(1, arm);
        assert!(decode_current_payload_record("items", &payload, &changed_arm_role).is_err());
    }

    #[test]
    fn current_roles_validate_logical_identity_then_rebind_local_columns() {
        let schema = current_schema(1, "_app_1");
        let row_uuid = crate::ids::RowUuid::from_bytes([1; 16]);
        let descriptor =
            RecordDescriptor::new([("row_uuid", ValueType::Uuid), ("_app_1", ValueType::String)]);
        let raw = descriptor
            .create(&[
                Value::Uuid(row_uuid.0),
                Value::String("published".to_owned()),
            ])
            .unwrap();
        let (descriptor, record) =
            encode_current_payload_record(descriptor.bind(&raw), &schema).unwrap();
        let member = ResultMemberEntry::row((
            "items".to_owned().into(),
            row_uuid,
            crate::tx::TxId::new(
                crate::time::TxTime::from(1),
                crate::ids::NodeUuid(uuid::Uuid::from_bytes([2; 16])),
            ),
        ));
        let payload = ResultMemberPayloadEntry {
            member,
            descriptor,
            record,
        };
        let relocated = current_schema(99, "_app_99");
        let decoded = decode_current_payload_record("items", &payload, &relocated).unwrap();
        assert_eq!(decoded.row_uuid(), row_uuid);
        assert_eq!(
            decoded.record.get_idx(1),
            Ok(Value::String("published".to_owned()))
        );
        assert_eq!(
            decoded.publication_fields[1],
            crate::node::CurrentRowPublicationField::StoredColumn {
                id: crate::ids::PhysicalColumnId(99),
                output_name: "title".to_owned(),
            }
        );
        let (reencoded, raw) =
            encode_current_payload_record(decoded.record.borrowed(), &relocated).unwrap();
        assert_eq!(reencoded, payload.descriptor);
        assert_eq!(raw, payload.record);
        let mut wrong_name = relocated.clone();
        wrong_name.payload_publication_fields.insert(
            "_app_99".to_owned(),
            crate::node::CurrentRowPublicationField::StoredColumn {
                id: crate::ids::PhysicalColumnId(99),
                output_name: "other_title".to_owned(),
            },
        );
        assert!(decode_current_payload_record("items", &payload, &wrong_name).is_err());
        let mut wrong_role = relocated.clone();
        wrong_role.payload_publication_fields.insert(
            "_app_99".to_owned(),
            crate::node::CurrentRowPublicationField::ResultField {
                name: "title".to_owned(),
                visibility: crate::node::CurrentRowResultVisibility::ApplicationCell,
            },
        );
        assert!(decode_current_payload_record("items", &payload, &wrong_role).is_err());
        let mut wrong_member = payload;
        wrong_member.member = ResultMemberEntry::row((
            "items".to_owned().into(),
            crate::ids::RowUuid::from_bytes([3; 16]),
            crate::tx::TxId::new(
                crate::time::TxTime::from(1),
                crate::ids::NodeUuid(uuid::Uuid::from_bytes([2; 16])),
            ),
        ));
        assert!(decode_current_payload_record("items", &wrong_member, &relocated).is_err());
    }

    /// Alice's aggregate payload distinguishes group and value roles with the
    /// same logical name; Bob's reader validates roles, names, types and row
    /// bytes before rebinding its execution slots.
    ///
    /// Internal codec test: public queries cannot inject malformed role
    /// descriptors or choose the receiving compiler's execution bindings.
    #[test]
    fn aggregate_roles_preserve_duplicate_names_and_validate_before_rebinding() {
        let schema = aggregate_schema(7, 9);
        let descriptor = RecordDescriptor::new_with_fields(
            schema
                .group_key_fields
                .iter()
                .chain(&schema.value_fields)
                .cloned(),
        );
        let raw = descriptor.create(&[Value::U64(1), Value::U64(2)]).unwrap();
        let prepared = encode_aggregate_payload_record(descriptor.bind(&raw), &schema).unwrap();
        let payload = ResultMemberPayloadEntry {
            member: ResultMemberEntry::Synthetic {
                table: "aggregate_result".to_owned(),
                row: crate::node::codec::runtime_result_identity_bytes(
                    &Value::U64(1),
                    &ValueType::U64,
                )
                .unwrap(),
                replacement: SyntheticReplacementToken::from_encoded_record(
                    crate::node::codec::runtime_result_identity_bytes(
                        &Value::U64(2),
                        &ValueType::U64,
                    )
                    .unwrap(),
                ),
            },
            descriptor: prepared.descriptor,
            record: prepared.record,
        };
        assert_eq!(
            blake3::hash(&payload.descriptor).to_hex().as_str(),
            "89fb1b80e90645fc68aaeccf9fbcd4082bcefdd2b606c8ed859827bd99465fff"
        );
        assert_eq!(
            hex::encode(&payload.record),
            "01000000000000000200000000000000"
        );
        let rebound_schema = aggregate_schema(70, 90);
        let rebound = decode_aggregate_payload_record(&payload, &rebound_schema).unwrap();
        assert_eq!(rebound.get_idx(0), Ok(Value::U64(1)));
        assert_eq!(rebound.get_idx(1), Ok(Value::U64(2)));
        assert_eq!(
            rebound.descriptor().fields()[0].identity,
            rebound_schema.group_key_fields[0].identity
        );
        let reencoded =
            encode_aggregate_payload_record(rebound.borrowed(), &rebound_schema).unwrap();
        assert_eq!(reencoded.descriptor, payload.descriptor);

        let mut wrong_name = rebound_schema.clone();
        wrong_name.group_names[0] = "another_group".to_owned();
        assert!(decode_aggregate_payload_record(&payload, &wrong_name).is_err());
        let mut same_width_wrong_type = rebound_schema.clone();
        same_width_wrong_type.value_fields[0].value_type = ValueType::I64;
        assert!(decode_aggregate_payload_record(&payload, &same_width_wrong_type).is_err());
        let mut wrong_role = payload.clone();
        wrong_role.descriptor[5] = ResultDescriptorRole::Current as u8;
        assert!(decode_aggregate_payload_record(&wrong_role, &rebound_schema).is_err());
        let mut trailing = payload.clone();
        trailing.record.push(0);
        assert!(decode_aggregate_payload_record(&trailing, &rebound_schema).is_err());
    }

    /// Alice's nested aggregate payload retains its canonical schema and values
    /// when Bob's reader relocates execution bindings, rejecting substitutions
    /// in nested names, types and member identity.
    ///
    /// Internal codec test: public queries cannot independently choose nested
    /// execution slots while keeping the authoritative result-role schema fixed.
    ///
    /// ```text
    /// alice's canonical payload --> bob's relocated reader --> same values
    /// ```
    #[test]
    fn review_nested_role_equality_and_runtime_rebinding() {
        fn nested_schema(slot: u64, name: &str, ty: ValueType) -> AggregateResultSchema {
            let mut schema = aggregate_schema(slot, slot + 1);
            let child = RecordDescriptor::new_with_fields([
                DescriptorField::new(name, ty).with_identity(FieldIdentity::Slot(slot + 2))
            ]);
            schema.value_fields[0].value_type =
                ValueType::Array(Box::new(ValueType::Record(Box::new(child))));
            schema
        }
        let schema = nested_schema(10, "literal/name", ValueType::U64);
        let ValueType::Array(element) = &schema.value_fields[0].value_type else {
            unreachable!()
        };
        let ValueType::Record(child) = element.as_ref() else {
            unreachable!()
        };
        let nested = Value::Array(vec![Value::Record(OwnedRecord::new(
            child.create(&[Value::U64(42)]).unwrap(),
            **child,
        ))]);
        let runtime = RecordDescriptor::new_with_fields(
            schema
                .group_key_fields
                .iter()
                .chain(&schema.value_fields)
                .cloned(),
        );
        let raw = runtime.create(&[Value::U64(1), nested.clone()]).unwrap();
        let prepared = encode_aggregate_payload_record(runtime.bind(&raw), &schema).unwrap();
        let payload = ResultMemberPayloadEntry {
            member: ResultMemberEntry::Synthetic {
                table: "aggregate_result".to_owned(),
                row: super::super::codec::runtime_result_identity_bytes(
                    &Value::U64(1),
                    &ValueType::U64,
                )
                .unwrap(),
                replacement: SyntheticReplacementToken::from_encoded_record(
                    super::super::codec::runtime_result_identity_bytes(
                        &nested,
                        &schema.value_fields[0].value_type,
                    )
                    .unwrap(),
                ),
            },
            descriptor: prepared.descriptor,
            record: prepared.record,
        };
        let relocated = nested_schema(800, "literal/name", ValueType::U64);
        let decoded = decode_aggregate_payload_record(&payload, &relocated).unwrap();
        let reencoded = encode_aggregate_payload_record(decoded.borrowed(), &relocated).unwrap();
        assert_eq!(reencoded.descriptor, payload.descriptor);
        assert_eq!(reencoded.record, payload.record);
        assert!(
            decode_aggregate_payload_record(
                &payload,
                &nested_schema(800, "another/name", ValueType::U64),
            )
            .is_err(),
            "nested logical-name substitution must reject"
        );
        assert!(
            decode_aggregate_payload_record(
                &payload,
                &nested_schema(800, "literal/name", ValueType::I64),
            )
            .is_err(),
            "nested equal-width type substitution must reject"
        );
        let mut wrong_member = payload.clone();
        let ResultMemberEntry::Synthetic { replacement, .. } = &mut wrong_member.member else {
            unreachable!()
        };
        *replacement = SyntheticReplacementToken::from_encoded_record(
            super::super::codec::runtime_result_identity_bytes(&Value::U64(42), &ValueType::U64)
                .unwrap(),
        );
        assert!(decode_aggregate_payload_record(&wrong_member, &relocated).is_err());
    }
}
