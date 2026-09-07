//! Binary row payloads shared by the NAPI and WASM bindings.
//!
//! The JavaScript hosts differ only after this boundary.  Keeping the postcard
//! envelope here makes row batching, relation snapshots, and occurrence-key
//! sidecars one production contract rather than two lookalike serializers.

mod publication_type;

use serde::Serialize;

use crate::db::{RemovedRow, SubscriptionOutputRow};
use crate::ids::{PhysicalColumnId, RowUuid};
use crate::node::{CurrentRow, CurrentRowPublicationField, RelationSnapshot};
use crate::tools::ResultKey;
use groove::ivm::TerminalOperation;

/// Encode only the descriptor portion of the named-cell input role. This is
/// also the owner for cross-language descriptor corpus generation.
pub fn encode_named_cell_descriptor(
    descriptor: &groove::records::RecordDescriptor,
) -> Result<Vec<u8>, String> {
    postcard::to_allocvec(&publication_type::NativeDescriptor(descriptor))
        .map_err(|error| error.to_string())
}

/// Encode the named-cell input ABI used by both native hosts. The descriptor
/// contains names and recursive value types, never compiler field identities.
pub fn encode_named_cells(record: &groove::records::OwnedRecord) -> Result<Vec<u8>, String> {
    postcard::to_allocvec(&(
        publication_type::NativeDescriptor(record.descriptor()),
        record.raw(),
    ))
    .map_err(|error| error.to_string())
}

/// Decode one canonical named-cell input envelope before constructing RowCells.
/// Both NAPI and WASM use this owner; execution descriptor serde is not accepted.
pub fn decode_named_cells(bytes: &[u8]) -> Result<crate::db::RowCells, String> {
    let mut remaining = bytes;
    let descriptor = publication_type::read_descriptor(&mut remaining)?;
    let (raw, trailing): (Vec<u8>, _) =
        postcard::take_from_bytes(remaining).map_err(|error| error.to_string())?;
    if !trailing.is_empty() {
        return Err("trailing bytes in named-cell envelope".to_owned());
    }
    let record = groove::records::OwnedRecord::new(raw, descriptor);
    if encode_named_cells(&record)? != bytes {
        return Err("noncanonical named-cell envelope".to_owned());
    }
    let values = record.to_values().map_err(|error| error.to_string())?;
    if descriptor
        .create(&values)
        .map_err(|error| error.to_string())?
        != record.raw()
    {
        return Err("noncanonical named-cell record".to_owned());
    }
    let mut cells = crate::db::RowCells::new();
    for (field, value) in descriptor.fields().iter().zip(values) {
        let name = field
            .name
            .as_ref()
            .ok_or("encoded cells must use named fields")?;
        if cells.insert(name.clone(), value).is_some() {
            return Err("encoded cells contain duplicate names".to_owned());
        }
    }
    Ok(cells)
}

/// Rust-owned frozen v1 binding corpus. Test harnesses may expose this through
/// a generated host artifact, but consumers must still decode its payloads
/// through their ordinary production readers.
pub const BINDING_CODEC_GOLDEN_FIXTURE: &str =
    include_str!("../fixtures/binding_codec_golden.json");

/// The explicit binding provenance of one record-descriptor field.
///
/// This is deliberately part of the native-host descriptor, rather than
/// inferred from a field name. A collector descriptor may mix stored
/// application columns with derived fields whose names equal private carriers.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize)]
pub enum RowDescriptorFieldName<'a> {
    /// A stored application column with catalogue identity and exact output name.
    StoredColumn {
        /// Authoritative physical catalogue identity.
        id: PhysicalColumnId,
        /// Exact application output name.
        output_name: &'a str,
    },
    /// A derived or metadata result field with its exact name.
    ResultField {
        /// Exact result name.
        name: &'a str,
    },
    /// Explicit engine metadata; consumers must never hide a result by its name.
    HiddenMetadata {
        /// Exact metadata field name.
        name: &'a str,
    },
}

/// One native binding descriptor entry.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct RowDescriptorField<'a> {
    /// Explicit field provenance and exact descriptor name.
    pub name: RowDescriptorFieldName<'a>,
    /// Exact Groove type used to decode this record slot.
    #[serde(serialize_with = "publication_type::serialize")]
    pub value_type: groove::records::ValueType,
}

/// A contiguous run of rows with one table and record descriptor.
#[derive(Clone, Debug, Serialize)]
pub struct RowBatch<'a> {
    /// Logical table name.
    pub table: &'a str,
    /// Exact tagged descriptor required to decode `rows.raw`.
    pub descriptor: Vec<RowDescriptorField<'a>>,
    /// Rows in producer order.
    pub rows: Vec<Row<'a>>,
}

/// One packed row inside a [`RowBatch`].
#[derive(Clone, Debug, Serialize)]
pub struct Row<'a> {
    /// Stable source row identity.
    pub row_id: RowUuid,
    /// Whether the row is an opt-in deleted historical row.
    pub deleted: bool,
    /// Packed record bytes described by the enclosing batch.
    pub raw: &'a [u8],
}

/// Relation snapshot envelope used by both native hosts.
#[derive(Clone, Debug, Serialize)]
pub struct RelationSnapshotPayload<'a> {
    /// Number of root rows at the beginning of the flattened batch sequence.
    pub root_count: u64,
    /// Root and related rows in producer order.
    pub rows: Vec<RowBatch<'a>>,
}

/// Incremental subscription envelope used by both native hosts.
#[derive(Clone, Debug, Serialize)]
pub struct SubscriptionDeltaPayload<'a> {
    /// Newly visible rows.
    pub added: Vec<RowBatch<'a>>,
    /// Still-visible rows with changed bytes.
    pub updated: Vec<RowBatch<'a>>,
    /// Rows no longer visible.
    pub removed: Vec<RemovedRowPayload>,
    /// One opaque occurrence key per added row.
    pub added_occurrence_keys: Vec<ResultKey>,
    /// One opaque occurrence key per updated row.
    pub updated_occurrence_keys: Vec<ResultKey>,
    /// One opaque occurrence key per removed row.
    pub removed_occurrence_keys: Vec<ResultKey>,
    /// Authoritative post-frame position for each added row.
    pub added_indices: Vec<u64>,
    /// Authoritative pre-frame position for each updated row.
    pub updated_previous_indices: Vec<u64>,
    /// Authoritative post-frame position for each updated row.
    pub updated_indices: Vec<u64>,
    /// Authoritative pre-frame position for each removed row.
    pub removed_indices: Vec<u64>,
}

/// Removed-row wire identity.
#[derive(Clone, Debug, Serialize)]
pub struct RemovedRowPayload {
    /// Logical table name.
    pub table: String,
    /// Stable source row identity.
    pub row_id: RowUuid,
}

/// Encode a flat row sequence, preserving contiguous batch boundaries.
pub fn encode_rows(rows: &[CurrentRow]) -> Result<Vec<u8>, postcard::Error> {
    postcard::to_allocvec(&row_batches(rows)?)
}

/// Encode a relation snapshot.
pub fn encode_relation_snapshot(snapshot: &RelationSnapshot) -> Result<Vec<u8>, postcard::Error> {
    postcard::to_allocvec(&RelationSnapshotPayload {
        root_count: snapshot.root_count as u64,
        rows: row_batches(&snapshot.rows)?,
    })
}

/// Encode an incremental subscription delta with aligned occurrence sidecars.
pub fn encode_subscription_delta(
    added: &[SubscriptionOutputRow],
    updated: &[SubscriptionOutputRow],
    removed: &[RemovedRow],
) -> Result<Vec<u8>, postcard::Error> {
    let added_rows = added.iter().map(|row| row.row.clone()).collect::<Vec<_>>();
    let updated_rows = updated
        .iter()
        .map(|row| row.row.clone())
        .collect::<Vec<_>>();
    postcard::to_allocvec(&SubscriptionDeltaPayload {
        added: row_batches(&added_rows)?,
        updated: row_batches(&updated_rows)?,
        removed: removed
            .iter()
            .map(|row| RemovedRowPayload {
                table: row.table.clone(),
                row_id: row.row_uuid,
            })
            .collect(),
        added_occurrence_keys: added
            .iter()
            .map(|row| ResultKey::from_occurrence(row.occurrence_id.clone()))
            .collect(),
        updated_occurrence_keys: updated
            .iter()
            .map(|row| ResultKey::from_occurrence(row.occurrence_id.clone()))
            .collect(),
        removed_occurrence_keys: removed
            .iter()
            .map(|row| ResultKey::from_occurrence(row.occurrence_id.clone()))
            .collect(),
        added_indices: added.iter().map(|row| row.index as u64).collect(),
        updated_previous_indices: updated
            .iter()
            .map(|row| row.previous_index.unwrap_or(row.index) as u64)
            .collect(),
        updated_indices: updated.iter().map(|row| row.index as u64).collect(),
        removed_indices: removed.iter().map(|row| row.index as u64).collect(),
    })
}

/// Group only adjacent rows with equal table and tagged descriptor.
pub fn row_batches(rows: &[CurrentRow]) -> Result<Vec<RowBatch<'_>>, postcard::Error> {
    let mut batches: Vec<RowBatch<'_>> = Vec::new();
    for row in rows {
        let (descriptor, raw) = row.encoded_record();
        let binding_descriptor = descriptor
            .fields()
            .iter()
            .zip(row.publication_fields())
            .map(|(field, binding)| {
                let name = match binding {
                    CurrentRowPublicationField::StoredColumn { id, output_name } => {
                        RowDescriptorFieldName::StoredColumn {
                            id: *id,
                            output_name,
                        }
                    }
                    CurrentRowPublicationField::ResultField { name, visibility } => {
                        match visibility {
                            crate::node::CurrentRowResultVisibility::HiddenMetadata => {
                                RowDescriptorFieldName::HiddenMetadata { name }
                            }
                            crate::node::CurrentRowResultVisibility::ApplicationCell
                            | crate::node::CurrentRowResultVisibility::PublicProvenance => {
                                RowDescriptorFieldName::ResultField { name }
                            }
                        }
                    }
                    CurrentRowPublicationField::UnresolvedSourceCell { .. } => {
                        return Err(postcard::Error::SerdeSerCustom);
                    }
                };
                Ok(RowDescriptorField {
                    name,
                    value_type: field.value_type.clone(),
                })
            })
            .collect::<Result<Vec<_>, postcard::Error>>()?;
        match batches.last_mut() {
            Some(batch) if batch.table == row.table() && batch.descriptor == binding_descriptor => {
                batch.rows.push(Row {
                    row_id: row.row_uuid(),
                    deleted: row.is_deleted(),
                    raw,
                });
            }
            _ => batches.push(RowBatch {
                table: row.table(),
                descriptor: binding_descriptor,
                rows: vec![Row {
                    row_id: row.row_uuid(),
                    deleted: row.is_deleted(),
                    raw,
                }],
            }),
        }
    }
    Ok(batches)
}

/// Encode terminal operations in the JavaScript-native object shape.
pub fn terminal_operations_to_json(
    operations: &[TerminalOperation],
) -> Result<serde_json::Value, serde_json::Error> {
    let mut encoded = serde_json::to_value(operations)?;
    if operations.is_empty() {
        return Ok(encoded);
    }
    let encoded_operations = encoded
        .as_array_mut()
        .expect("terminal operations serialize as an array");
    for wire in encoded_operations {
        let serde_json::Value::Object(wire) = wire else {
            unreachable!("terminal operation serializes as an object");
        };
        wire.remove("root_descriptor");
    }
    Ok(encoded)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::CurrentRowBindingRole;
    use groove::records::{OwnedRecord, RecordDescriptor, Value, ValueType};

    // This direct boundary test is necessary to pin host input bytes independently
    // of Rust execution serde; database tests alone cannot identify that framing.
    #[test]
    fn named_cell_input_uses_explicit_canonical_recursive_binding_types() {
        use groove::records::{DescriptorField, FieldIdentity};
        let fixture = b"\x01\x01\x05score\x03\x08\x2a\x00\x00\x00\x00\x00\x00\x00";
        let cells = decode_named_cells(fixture).unwrap();
        assert_eq!(cells.get("score"), Some(&Value::U64(42)));
        let descriptor =
            RecordDescriptor::new_with_fields([DescriptorField::new("score", ValueType::U64)
                .with_identity(FieldIdentity::Slot(900))]);
        let record = OwnedRecord::new(descriptor.create(&[Value::U64(42)]).unwrap(), descriptor);
        assert_eq!(encode_named_cells(&record).unwrap(), fixture);
        assert!(
            decode_named_cells(&postcard::to_allocvec(&(descriptor, record.raw())).unwrap())
                .is_err()
        );
        let mut trailing = fixture.to_vec();
        trailing.push(0);
        assert!(decode_named_cells(&trailing).is_err());
        let mut overlong = vec![0x81, 0];
        overlong.extend_from_slice(&fixture[1..]);
        assert!(decode_named_cells(&overlong).is_err());
        let nested =
            RecordDescriptor::new_with_fields([DescriptorField::new("literal", ValueType::U64)
                .with_identity(FieldIdentity::NamedSlot {
                    name: "execution_alias".to_owned(),
                    slot: 73,
                })]);
        let descriptor = RecordDescriptor::new([(
            "payload",
            ValueType::Array(Box::new(ValueType::Record(Box::new(nested)))),
        )]);
        let record = OwnedRecord::new(
            descriptor
                .create(&[Value::Array(vec![Value::Record(OwnedRecord::new(
                    nested.create(&[Value::U64(7)]).unwrap(),
                    nested,
                ))])])
                .unwrap(),
            descriptor,
        );
        let encoded = encode_named_cells(&record).unwrap();
        let cells = decode_named_cells(&encoded).unwrap();
        let Some(Value::Array(items)) = cells.get("payload") else {
            panic!("array payload")
        };
        let Value::Record(inner) = &items[0] else {
            panic!("nested record")
        };
        assert_eq!(inner.get("literal"), Ok(Value::U64(7)));
        assert_eq!(
            inner.descriptor().fields()[0].identity,
            Some(FieldIdentity::Name("literal".to_owned()))
        );

        // The recursive descriptor grammar must reject a non-minimal varint
        // below the outer envelope as well. A top-level round-trip alone would
        // not prove that a nested field name cannot smuggle alternate bytes.
        let nested_name = b"literal";
        let name_start = encoded
            .windows(nested_name.len())
            .position(|window| window == nested_name)
            .expect("nested descriptor contains its field name");
        assert_eq!(encoded[name_start - 1], nested_name.len() as u8);
        let mut nested_nonminimal = encoded;
        nested_nonminimal.splice(name_start - 1..name_start, [0x87, 0x00]);
        assert!(
            decode_named_cells(&nested_nonminimal).is_err(),
            "nested descriptor lengths must use their canonical minimal encoding"
        );
    }

    #[test]
    fn named_cell_input_rejects_ambiguous_and_unbounded_descriptors_and_decodes_payload_enums() {
        use groove::records::{DescriptorField, EnumCase, EnumSchema, EnumValue};
        for descriptor in [
            RecordDescriptor::new([("same", ValueType::U64), ("same", ValueType::U64)]),
            RecordDescriptor::new_with_fields([DescriptorField {
                name: None,
                value_type: ValueType::U64,
                identity: None,
            }]),
        ] {
            let raw = descriptor
                .create(&vec![Value::U64(1); descriptor.fields().len()])
                .unwrap();
            assert!(
                decode_named_cells(
                    &encode_named_cells(&OwnedRecord::new(raw, descriptor)).unwrap()
                )
                .is_err()
            );
        }
        assert!(
            decode_named_cells(b"\x01\x01\x01x\x12").is_err(),
            "unknown type tag"
        );
        let mut deep = b"\x01\x01\x01x".to_vec();
        deep.extend(std::iter::repeat_n(14, 128));
        deep.push(0);
        assert!(decode_named_cells(&deep).is_err(), "recursive type bound");
        let wide = RecordDescriptor::new_with_fields(
            (0..1025).map(|i| DescriptorField::new(format!("field{i}"), ValueType::U8)),
        );
        let record = OwnedRecord::new(wide.create(&vec![Value::U8(1); 1025]).unwrap(), wide);
        assert!(
            decode_named_cells(&encode_named_cells(&record).unwrap()).is_err(),
            "total node bound"
        );
        let payload = RecordDescriptor::new([("literal", ValueType::U64)]);
        let enum_schema = EnumSchema::new("choice", [EnumCase::new("selected", payload)])
            .unwrap()
            .with_registry_id(7);
        let descriptor =
            RecordDescriptor::new([("choice", ValueType::Enum(Box::new(enum_schema.clone())))]);
        let value = Value::Enum(EnumValue::create(0, payload, &[Value::U64(42)]).unwrap());
        let record = OwnedRecord::new(
            descriptor.create(std::slice::from_ref(&value)).unwrap(),
            descriptor,
        );
        assert_eq!(
            decode_named_cells(&encode_named_cells(&record).unwrap())
                .unwrap()
                .get("choice"),
            Some(&value)
        );
    }

    /// The contained 4c6eafaef5 binding enum, independently declared to pin
    /// its postcard variant order and fields before migrating publication.
    #[derive(Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
    enum FrozenPublicationField {
        StoredColumn { id: u64, output_name: String },
        ResultField { name: String },
    }

    #[test]
    fn publication_compatibility_proof_pins_stored_id_and_nested_descriptor_gaps() {
        let stored = FrozenPublicationField::StoredColumn {
            id: 7,
            output_name: "score".to_owned(),
        };
        let result = FrozenPublicationField::ResultField {
            name: "_app_score".to_owned(),
        };
        let stored_bytes = postcard::to_allocvec(&stored).unwrap();
        let result_bytes = postcard::to_allocvec(&result).unwrap();
        // Exact contained bytes: variant0, physicalid7, UTF-8 length5, score.
        assert_eq!(stored_bytes, b"\x00\x07\x05score");
        assert_eq!(result_bytes, b"\x01\x0a_app_score");
        assert_eq!(
            postcard::from_bytes::<FrozenPublicationField>(&stored_bytes).unwrap(),
            stored
        );
        assert_eq!(
            postcard::from_bytes::<FrozenPublicationField>(&result_bytes).unwrap(),
            result
        );

        #[derive(Serialize)]
        enum ExecutionPhysicalName<'a> {
            PhysicalColumn(&'a str),
        }
        let execution_bytes =
            postcard::to_allocvec(&ExecutionPhysicalName::PhysicalColumn("_app_score")).unwrap();
        assert_ne!(execution_bytes, stored_bytes);
        assert!(postcard::from_bytes::<FrozenPublicationField>(&execution_bytes).is_err());

        // The same native type envelope recursively serializes DescriptorField.
        // Contained Record([score: U64]) is tag16, one named field, U64 tag3.
        let contained_nested = b"\x10\x01\x01\x05score\x03";
        let descriptor = RecordDescriptor::new([("score".to_owned(), ValueType::U64)]);
        let nested = ValueType::Record(Box::new(descriptor));
        assert_eq!(
            postcard::to_allocvec(&publication_type::NativeValueType(&nested)).unwrap(),
            contained_nested
        );
        assert_ne!(postcard::to_allocvec(&nested).unwrap(), contained_nested);
        assert!(postcard::from_bytes::<ValueType>(contained_nested).is_err());
    }

    #[test]
    fn publication_writer_matches_frozen_relation_snapshot_and_rejects_unresolved_cells() {
        use crate::node::CurrentRowPublicationField as Binding;
        let stored = |id: u8, title: Option<&str>| {
            let descriptor = RecordDescriptor::new([
                ("row_uuid", ValueType::Uuid),
                (
                    "_app_title",
                    ValueType::Nullable(Box::new(ValueType::String)),
                ),
            ]);
            let raw = descriptor
                .create(&[
                    Value::Uuid(uuid::Uuid::from_bytes([id; 16])),
                    Value::Nullable(title.map(|title| Box::new(Value::String(title.to_owned())))),
                ])
                .unwrap();
            CurrentRow::new_with_publication_fields(
                "todos",
                OwnedRecord::new(raw, descriptor),
                vec![
                    Binding::ResultField {
                        name: "row_uuid".to_owned(),
                        visibility: crate::node::CurrentRowResultVisibility::HiddenMetadata,
                    },
                    Binding::StoredColumn {
                        id: PhysicalColumnId(1),
                        output_name: "title".to_owned(),
                    },
                ],
            )
        };
        let descriptor =
            RecordDescriptor::new([("row_uuid", ValueType::Uuid), ("title", ValueType::String)]);
        let raw = descriptor
            .create(&[
                Value::Uuid(uuid::Uuid::from_bytes([0x21; 16])),
                Value::String("note".to_owned()),
            ])
            .unwrap();
        let note = CurrentRow::new_with_publication_fields(
            "notes",
            OwnedRecord::new(raw, descriptor),
            vec![
                Binding::ResultField {
                    name: "row_uuid".to_owned(),
                    visibility: crate::node::CurrentRowResultVisibility::HiddenMetadata,
                },
                Binding::ResultField {
                    name: "title".to_owned(),
                    visibility: crate::node::CurrentRowResultVisibility::ApplicationCell,
                },
            ],
        );
        let snapshot = RelationSnapshot {
            root_count: 4,
            rows: vec![
                stored(0x11, Some("first")),
                stored(0x12, Some("second")),
                note,
                stored(0x13, None).into_deleted(),
            ],
            edges: vec![],
        };
        let actual = encode_relation_snapshot(&snapshot).unwrap();
        let fixture: serde_json::Value =
            serde_json::from_str(BINDING_CODEC_GOLDEN_FIXTURE).unwrap();
        let expected_hex = fixture["relation_snapshots"][1]["payload_hex"]
            .as_str()
            .unwrap();
        let actual_hex = actual
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect::<String>();
        assert_eq!(
            actual_hex, expected_hex,
            "the shared native publication golden bytes are frozen"
        );

        #[derive(Serialize, serde::Deserialize)]
        enum SharedPublicationField {
            StoredColumn { id: u64, output_name: String },
            ResultField { name: String },
            HiddenMetadata { name: String },
        }
        #[derive(Serialize, serde::Deserialize)]
        struct FrozenField {
            name: SharedPublicationField,
            value_type: ValueType,
        }
        #[derive(Serialize, serde::Deserialize)]
        struct FrozenRow {
            row_id: RowUuid,
            deleted: bool,
            raw: Vec<u8>,
        }
        #[derive(Serialize, serde::Deserialize)]
        struct FrozenBatch {
            table: String,
            descriptor: Vec<FrozenField>,
            rows: Vec<FrozenRow>,
        }
        #[derive(Serialize, serde::Deserialize)]
        struct FrozenSnapshot {
            root_count: u64,
            rows: Vec<FrozenBatch>,
        }
        let read: FrozenSnapshot = postcard::from_bytes(&actual).unwrap();
        assert_eq!(postcard::to_allocvec(&read).unwrap(), actual);
        assert!(matches!(
            read.rows[0].descriptor[1].name,
            SharedPublicationField::StoredColumn { id: 1, .. }
        ));

        let mut unresolved = stored(0x11, Some("first"));
        unresolved = CurrentRow::new_with_publication_fields(
            "todos",
            OwnedRecord::new(
                unresolved.encoded_record().1.to_vec(),
                unresolved.encoded_record().0.clone(),
            ),
            vec![
                Binding::ResultField {
                    name: "row_uuid".to_owned(),
                    visibility: crate::node::CurrentRowResultVisibility::HiddenMetadata,
                },
                Binding::UnresolvedSourceCell {
                    output_name: "title".to_owned(),
                },
            ],
        );
        assert!(
            encode_rows(&[unresolved]).is_err(),
            "native codec must not invent a catalogue ID"
        );
    }

    #[test]
    fn explicit_hidden_metadata_tag_preserves_same_named_public_alias() {
        use crate::node::CurrentRowPublicationField as Binding;
        use crate::node::CurrentRowResultVisibility as Visibility;
        // Native publication roles are not observable through a Rust row map;
        // pin the host descriptor and its aligned values at this ABI boundary.
        let descriptor = RecordDescriptor::new([
            ("row_uuid", ValueType::Uuid),
            ("schema_version", ValueType::U64),
            ("aggregate_alias", ValueType::U64),
            ("$createdAt", ValueType::U64),
        ]);
        let raw = descriptor
            .create(&[
                Value::Uuid(uuid::Uuid::from_bytes([0x63; 16])),
                Value::U64(99),
                Value::U64(1),
                Value::U64(123),
            ])
            .unwrap();
        let row = CurrentRow::new_with_publication_fields(
            "items",
            OwnedRecord::new(raw, descriptor),
            vec![
                Binding::ResultField {
                    name: "row_uuid".into(),
                    visibility: Visibility::HiddenMetadata,
                },
                Binding::ResultField {
                    name: "schema_version".into(),
                    visibility: Visibility::HiddenMetadata,
                },
                Binding::ResultField {
                    name: "schema_version".into(),
                    visibility: Visibility::ApplicationCell,
                },
                Binding::ResultField {
                    name: "$createdAt".into(),
                    visibility: Visibility::PublicProvenance,
                },
            ],
        );
        let rows = [row];
        let batches = row_batches(&rows).unwrap();
        assert_eq!(
            batches[0].descriptor[1].name,
            RowDescriptorFieldName::HiddenMetadata {
                name: "schema_version"
            }
        );
        assert_eq!(
            batches[0].descriptor[2].name,
            RowDescriptorFieldName::ResultField {
                name: "schema_version"
            }
        );
        assert_eq!(
            batches[0].descriptor[3].name,
            RowDescriptorFieldName::ResultField { name: "$createdAt" }
        );
        assert_eq!(
            postcard::to_allocvec(&batches[0].descriptor[1].name).unwrap(),
            b"\x02\x0eschema_version"
        );
        assert_eq!(
            postcard::to_allocvec(&batches[0].descriptor[2].name).unwrap(),
            b"\x01\x0eschema_version"
        );
        assert!(!encode_rows(&rows).unwrap().is_empty());
    }

    #[test]
    fn tagged_descriptor_preserves_hybrid_physical_and_logical_user_check_fields() {
        // The field tags exist only at this internal host ABI boundary. Keep
        // this direct regression here so a hybrid collector cannot silently
        // collapse physical `check` and logical `user_check` into one plan.
        let descriptor = RecordDescriptor::new([
            ("row_uuid".to_owned(), ValueType::Uuid),
            ("user_check".to_owned(), ValueType::String),
            ("user_check".to_owned(), ValueType::Bytes),
        ]);
        let raw = descriptor
            .create(&[
                Value::Uuid(uuid::Uuid::from_bytes([0x5c; 16])),
                Value::String("physical".to_owned()),
                Value::Bytes(b"logical".to_vec()),
            ])
            .expect("encode hybrid record");
        let row = CurrentRow::new_with_publication_fields(
            "notes",
            OwnedRecord::new(raw, descriptor),
            vec![
                CurrentRowPublicationField::ResultField {
                    name: "row_uuid".to_owned(),
                    visibility: crate::node::CurrentRowResultVisibility::HiddenMetadata,
                },
                CurrentRowPublicationField::StoredColumn {
                    id: PhysicalColumnId(7),
                    output_name: "check".to_owned(),
                },
                CurrentRowPublicationField::ResultField {
                    name: "user_check".to_owned(),
                    visibility: crate::node::CurrentRowResultVisibility::ApplicationCell,
                },
            ],
        );

        let rows = [row];
        let batches = row_batches(&rows).unwrap();
        assert_eq!(batches.len(), 1);
        assert!(matches!(
            batches[0].descriptor[1].name,
            RowDescriptorFieldName::StoredColumn {
                id: PhysicalColumnId(7),
                output_name: "check"
            }
        ));
        assert!(matches!(
            batches[0].descriptor[2].name,
            RowDescriptorFieldName::ResultField { name: "user_check" }
        ));
    }

    #[test]
    fn tagged_descriptor_uses_terminal_public_name_for_logical_source_carrier() {
        // A lowered projection may retain `user_title` as its source slot but
        // expose `title`. The producer supplies that mapping so hosts do not
        // accidentally apply a prefix heuristic to all logical fields.
        let descriptor = RecordDescriptor::new([
            ("row_uuid".to_owned(), ValueType::Uuid),
            ("user_title".to_owned(), ValueType::String),
        ]);
        let raw = descriptor
            .create(&[
                Value::Uuid(uuid::Uuid::from_bytes([0x5d; 16])),
                Value::String("projected".to_owned()),
            ])
            .expect("encode projected record");
        let row = CurrentRow::new_with_explicit_binding_fields_and_names(
            "todos",
            OwnedRecord::new(raw, descriptor),
            vec![
                CurrentRowBindingRole::LogicalField,
                CurrentRowBindingRole::LogicalField,
            ],
            vec![None, Some("title".to_owned())],
        );

        let rows = [row];
        let batches = row_batches(&rows).unwrap();
        assert!(matches!(
            batches[0].descriptor[1].name,
            RowDescriptorFieldName::ResultField { name: "title" }
        ));
    }
}
