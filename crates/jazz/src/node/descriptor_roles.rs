//! Role-specific durable result schemas. Execution identities remain local.

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

pub(super) fn decode_result_descriptor(
    bytes: &[u8],
) -> Result<(ResultDescriptorRole, RecordDescriptor), Error> {
    if !bytes.starts_with(RESULT_DESCRIPTOR_MAGIC) {
        return Err(Error::InvalidStoredValue(
            "result descriptor role/version is invalid",
        ));
    }
    let role = match bytes.get(5) {
        Some(0) => ResultDescriptorRole::Current,
        Some(1) => ResultDescriptorRole::Aggregate,
        _ => {
            return Err(Error::InvalidStoredValue(
                "result descriptor role/version is invalid",
            ));
        }
    };
    let descriptor = records::decode_persisted_record_descriptor(&bytes[6..])?;
    validate_role_fields(role, &descriptor)?;
    Ok((role, descriptor))
}

fn validate_role_fields(
    role: ResultDescriptorRole,
    descriptor: &RecordDescriptor,
) -> Result<(), Error> {
    let invalid = || Error::InvalidStoredValue("result descriptor field roles are not canonical");
    let mut group_ordinal = 0usize;
    let mut value_ordinal = 0usize;
    let mut values_started = false;
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
                if matches!(tag, "source" | "result" | "provenance" | "metadata") =>
            {
                current_ordinal
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

pub(super) fn encode_aggregate_payload_record(
    record: BorrowedRecord<'_>,
    schema: &AggregateResultSchema,
) -> Result<(Vec<u8>, Vec<u8>), Error> {
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
    Ok((
        encode_result_descriptor(ResultDescriptorRole::Aggregate, &canonical)?,
        raw,
    ))
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
    let (group_value, group_type) = match schema.group_key_fields.as_slice() {
        [] => (
            records::Value::String("global".to_owned()),
            records::ValueType::String,
        ),
        [_group] => (values[0].clone(), canonical.fields()[0].value_type.clone()),
        _ => {
            return Err(Error::InvalidStoredValue(
                "aggregate payload has unsupported group identity",
            ));
        }
    };
    let (replacement_value, replacement_type) = match schema.value_fields.first() {
        Some(_field) => (
            values[schema.group_key_fields.len()].clone(),
            canonical.fields()[schema.group_key_fields.len()]
                .value_type
                .clone(),
        ),
        None => (
            records::Value::String("empty".to_owned()),
            records::ValueType::String,
        ),
    };
    if super::codec::settled_result_value_storage_bytes(&group_value, &group_type)? != *row
        || super::codec::settled_result_value_storage_bytes(&replacement_value, &replacement_type)?
            != replacement.encoded_record()
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
    let descriptor = RecordDescriptor::new_with_fields(fields);
    Ok(records::decode_persisted_record_descriptor(
        &records::encode_persisted_record_descriptor(&descriptor)?,
    )?)
}

pub(super) fn encode_current_payload_record(
    record: BorrowedRecord<'_>,
    schema: &super::query_engine::ResultMembershipSchema,
) -> Result<(Vec<u8>, Vec<u8>), Error> {
    let descriptor = record.descriptor();
    let selected = std::iter::once(schema.row_field.as_str()).chain(
        schema
            .payload_fields
            .iter()
            .filter(|field| field.name != schema.row_field)
            .map(|field| field.name.as_str()),
    );
    let mut values = Vec::new();
    let mut runtime_fields = Vec::new();
    for name in selected {
        let index = descriptor
            .fields()
            .iter()
            .position(|field| field.name.as_deref() == Some(name))
            .ok_or(Error::InvalidStoredValue(
                "result payload is missing its declared carrier",
            ))?;
        values.push(record.get_idx(index)?);
        runtime_fields.push(descriptor.fields()[index].clone());
    }
    let runtime = RecordDescriptor::new_with_fields(runtime_fields);
    let raw = runtime.create(&values)?;
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
    let values = canonical.bind(&raw).to_values()?;
    if canonical.create(&values)? != raw {
        return Err(Error::InvalidStoredValue(
            "result payload row is not canonical",
        ));
    }
    Ok((
        encode_result_descriptor(ResultDescriptorRole::Current, &canonical)?,
        raw,
    ))
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
    Ok(super::CurrentRow::new_with_publication_fields(
        table,
        OwnedRecord::new(
            payload.record.clone(),
            RecordDescriptor::new_with_fields(fields),
        ),
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
        let (descriptor_bytes, row_bytes) =
            encode_aggregate_payload_record(descriptor.bind(&raw), &schema).unwrap();
        let payload = ResultMemberPayloadEntry {
            member: ResultMemberEntry::Synthetic {
                table: "aggregate_result".to_owned(),
                row: crate::node::codec::settled_result_value_storage_bytes(
                    &Value::U64(1),
                    &ValueType::U64,
                )
                .unwrap(),
                replacement: SyntheticReplacementToken::from_encoded_record(
                    crate::node::codec::settled_result_value_storage_bytes(
                        &Value::U64(2),
                        &ValueType::U64,
                    )
                    .unwrap(),
                ),
            },
            descriptor: descriptor_bytes,
            record: row_bytes,
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
        let (reencoded, _) =
            encode_aggregate_payload_record(rebound.borrowed(), &rebound_schema).unwrap();
        assert_eq!(reencoded, payload.descriptor);

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

    // Internal fixture: public queries cannot independently choose nested
    // execution slots while keeping the authoritative result-role schema fixed.
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
        let (descriptor, record) =
            encode_aggregate_payload_record(runtime.bind(&raw), &schema).unwrap();
        let payload = ResultMemberPayloadEntry {
            member: ResultMemberEntry::Synthetic {
                table: "aggregate_result".to_owned(),
                row: super::super::codec::settled_result_value_storage_bytes(
                    &Value::U64(1),
                    &ValueType::U64,
                )
                .unwrap(),
                replacement: SyntheticReplacementToken::from_encoded_record(
                    super::super::codec::settled_result_value_storage_bytes(
                        &nested,
                        &schema.value_fields[0].value_type,
                    )
                    .unwrap(),
                ),
            },
            descriptor,
            record,
        };
        let relocated = nested_schema(800, "literal/name", ValueType::U64);
        let decoded = decode_aggregate_payload_record(&payload, &relocated).unwrap();
        let (descriptor, record) =
            encode_aggregate_payload_record(decoded.borrowed(), &relocated).unwrap();
        assert_eq!(descriptor, payload.descriptor);
        assert_eq!(record, payload.record);
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
            super::super::codec::settled_result_value_storage_bytes(
                &Value::U64(42),
                &ValueType::U64,
            )
            .unwrap(),
        );
        assert!(decode_aggregate_payload_record(&wrong_member, &relocated).is_err());
    }
}
