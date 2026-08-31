//! Schema-aware database facade over records, storage, and the IVM runtime.
//!
//! This module owns the public [`Database`] API: opening a schema on an
//! [`OrderedKvStorage`], encoding user rows through [`RecordDescriptor`],
//! maintaining primary/secondary durable storage entries, and synchronously
//! ticking [`IvmRuntime`] after committed batches. Query planning and graph
//! execution live in [`crate::ivm`]; binary row layout lives in
//! [`crate::records`]; storage durability lives below the [`OrderedKvStorage`]
//! seam. New readers should start here to see how commits become table deltas
//! and how subscriptions are exposed above the engine.

use std::cell::{Cell, RefCell};
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::rc::Rc;
use std::str;
use std::sync::OnceLock;
use std::sync::{Arc, Weak};
use std::task::{Poll, Waker};

use futures::lock::Mutex as AsyncMutex;
use web_time::{Duration, Instant};

use crate::ivm::runtime::{durable_index_key_prefix, encode_key_part};
use crate::ivm::{
    IvmRuntime, PlannerError, PublicationId, QueryParameter, RecordDelta, RecordDeltas,
    RuntimeStats, TableDelta, TickMetrics, plan_prepared_shape, plan_query,
};
use crate::queries::Query;
use crate::records::{
    self, BorrowedRecord, EnumSchema, OwnedRecord, Record, RecordDescriptor, Value, VariantRecord,
    encode_variant_record, split_variant_record,
};
use crate::schema::{
    ColumnType, DatabaseSchema, DirectRecordStoreSchema, IndexSchema, IntegerKeyType, PrimaryKey,
    PrimaryKeyColumn, PrimaryKeyType, TableSchema, TableVariant,
};
use crate::storage::{
    BoxedStorage, LayoutStorage, OrderedKvStorage, OwnedStorage, OwnedWriteOperation, RecordStore,
    ReopenableStorage, StagedWriteOverlay, StagedWriteState, StorageLayout, WriteManyOutcome,
};
use thiserror::Error;

/// Reserved Groove-owned metadata plane for staged roots and persisted
/// reference accounting. It is never exposed as an application table.
pub const LARGE_VALUE_METADATA_CF: &str = "__groove_large_values";

fn staged_large_value_key(id: crate::large_values::StagedLargeValueId) -> Vec<u8> {
    let mut key = b"staged/".to_vec();
    key.extend_from_slice(&id.0);
    key
}

fn pending_large_value_upload_key(id: crate::large_values::StagedLargeValueId) -> Vec<u8> {
    let mut key = b"upload/".to_vec();
    key.extend_from_slice(&id.0);
    key
}

/// Bind an engine-owned lifecycle record to the fixed-width opaque identity in
/// its metadata key.  The key is part of the durable authority: copying a
/// receipt under a different key must never make it resumable or evictable as
/// that other lifecycle handle.
fn staged_large_value_id_from_metadata_key(
    key: &[u8],
    prefix: &'static [u8],
    record_name: &'static str,
) -> Result<crate::large_values::StagedLargeValueId, Error> {
    let encoded = key.strip_prefix(prefix).ok_or_else(|| {
        Error::InvalidLargeValueMetadata(format!(
            "{record_name} has an invalid metadata key prefix"
        ))
    })?;
    let id: [u8; 16] = encoded.try_into().map_err(|_| {
        Error::InvalidLargeValueMetadata(format!(
            "{record_name} metadata key must contain exactly 16 staging-id bytes"
        ))
    })?;
    Ok(crate::large_values::StagedLargeValueId(id))
}

fn completed_large_value_upload_key(id: crate::large_values::StagedLargeValueId) -> Vec<u8> {
    let mut key = b"completed-upload/".to_vec();
    key.extend_from_slice(&id.0);
    key
}

fn completed_large_value_receipt_key(id: crate::large_values::StagedLargeValueId) -> Vec<u8> {
    let mut key = b"completed-receipt/".to_vec();
    key.extend_from_slice(&id.0);
    key
}

fn large_value_root_key(node_ref: &crate::large_values::NodeRef) -> Result<Vec<u8>, Error> {
    let mut key = b"root/".to_vec();
    key.extend(
        crate::large_values::encode_node_ref(node_ref).map_err(|error| {
            Error::InvalidLargeValueMetadata(format!("cannot encode root identity: {error}"))
        })?,
    );
    Ok(key)
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct LargeValueRootReferences {
    durable: u64,
    staged: u64,
    node_active: bool,
}

fn large_value_node_key(node_ref: &crate::large_values::NodeRef) -> Result<Vec<u8>, Error> {
    let mut key = b"node/".to_vec();
    key.extend(
        crate::large_values::encode_node_ref(node_ref).map_err(|error| {
            Error::InvalidLargeValueMetadata(format!("cannot encode node identity: {error}"))
        })?,
    );
    Ok(key)
}

fn large_value_reclaim_key(node_ref: &crate::large_values::NodeRef) -> Result<Vec<u8>, Error> {
    let mut key = b"reclaim/".to_vec();
    key.extend(
        crate::large_values::encode_node_ref(node_ref).map_err(|error| {
            Error::InvalidLargeValueMetadata(format!("cannot encode reclaim identity: {error}"))
        })?,
    );
    Ok(key)
}

/// Decode the identity embedded in a reclaim-work key. The queue key and its
/// value deliberately duplicate the same canonical `NodeRef`; reclaim checks
/// that agreement before treating either as authority to delete a blob.
pub(crate) fn large_value_reclaim_node_ref_from_key(
    key: &[u8],
) -> Result<crate::large_values::NodeRef, Error> {
    let encoded = key.strip_prefix(b"reclaim/").ok_or_else(|| {
        Error::InvalidLargeValueMetadata("reclaim entry has an invalid key prefix".to_owned())
    })?;
    crate::large_values::decode_node_ref(encoded).map_err(|error| {
        Error::InvalidLargeValueMetadata(format!("cannot decode reclaim key identity: {error}"))
    })
}

/// Durable marker for a remotely hydrated immutable node whose byte write has
/// happened but whose Groove reference metadata still needs installation.
/// This lives in Groove's metadata plane rather than in the blob backend: a
/// locator remains an opaque capability to that backend.
fn large_value_pending_install_key(
    node_ref: &crate::large_values::NodeRef,
) -> Result<Vec<u8>, Error> {
    let mut key = b"install/".to_vec();
    key.extend(
        crate::large_values::encode_node_ref(node_ref).map_err(|error| {
            Error::InvalidLargeValueMetadata(format!(
                "cannot encode pending install identity: {error}"
            ))
        })?,
    );
    Ok(key)
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct LargeValueNodeReferences {
    references: u64,
    upload_references: u64,
    children: Vec<crate::large_values::NodeRef>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LargeValueReclaimDecision {
    /// Both durable and resident views still retain the node. Any queue entry
    /// predates their activation and can be discarded.
    ClearStaleQueue,
    /// The latest resident publication deactivates a still-durable node. Keep
    /// the queue entry for the publication's eventual durable-zero transition.
    WaitForDurableZero,
    /// Durable zero authorizes deletion, but a resident activation vetoes it.
    ResidentVeto,
    /// Durable zero authorizes deletion and the resident view does not veto it.
    Delete,
}

/// Combine Groove's two reference views without creating another reclamation
/// authority: durable zero is the sole authorization, and resident references
/// can only veto it.
fn large_value_reclaim_decision(
    durable: &LargeValueNodeReferences,
    resident: &LargeValueNodeReferences,
) -> LargeValueReclaimDecision {
    let durable_retained = durable.references != 0 || durable.upload_references != 0;
    let resident_retained = resident.references != 0 || resident.upload_references != 0;
    match (durable_retained, resident_retained) {
        (true, true) => LargeValueReclaimDecision::ClearStaleQueue,
        (true, false) => LargeValueReclaimDecision::WaitForDurableZero,
        (false, true) => LargeValueReclaimDecision::ResidentVeto,
        (false, false) => LargeValueReclaimDecision::Delete,
    }
}

fn unique_large_value_children(
    node: &crate::large_values::ChunkNode,
) -> Vec<crate::large_values::NodeRef> {
    match node {
        crate::large_values::ChunkNode::Leaf { .. } => Vec::new(),
        crate::large_values::ChunkNode::Branch { children, .. } => {
            canonical_large_value_children(children.iter().map(|child| child.node_ref.clone()))
        }
    }
}

/// Metadata child edges describe physical ownership rather than a logical
/// byte order. Normalize them on every read/write boundary so historical
/// logical-order vectors and duplicate child occurrences retain their exact
/// one-edge-per-physical-child meaning.
fn canonical_large_value_children(
    children: impl IntoIterator<Item = crate::large_values::NodeRef>,
) -> Vec<crate::large_values::NodeRef> {
    children
        .into_iter()
        .collect::<BTreeSet<_>>()
        .into_iter()
        .collect()
}

// The key namespace is the metadata record's discriminant: `root/`, `node/`,
// `staged/`, and `upload/` cannot be confused. Values therefore use a plain
// canonical Groove record, rather than a second private magic/version/tag
// envelope. These identifiers are durable *one-based record slots*, not
// declaration order. A retired slot stays in the physical layout as an empty
// nullable placeholder; it is never compacted away.
const ROOT_REF_DURABLE_FIELD: u16 = 1;
const ROOT_REF_STAGED_FIELD: u16 = 2;
const ROOT_REF_NODE_ACTIVE_FIELD: u16 = 3;

const NODE_REF_REFERENCES_FIELD: u16 = 1;
const NODE_REF_UPLOAD_REFERENCES_FIELD: u16 = 2;
const NODE_REF_CHILDREN_FIELD: u16 = 3;

const STAGED_VALUE_ID_FIELD: u16 = 1;
const STAGED_VALUE_REF_FIELD: u16 = 2;
const STAGED_VALUE_ENCODED_BYTES_FIELD: u16 = 3;
const STAGED_VALUE_NODE_COUNT_FIELD: u16 = 4;
const STAGED_VALUE_CREATED_AT_MS_FIELD: u16 = 5;

const PENDING_UPLOAD_ID_FIELD: u16 = 1;
const PENDING_UPLOAD_DESCRIPTOR_FIELD: u16 = 2;
const PENDING_UPLOAD_RECEIPT_ID_FIELD: u16 = 3;
const PENDING_UPLOAD_ENCODED_BYTES_FIELD: u16 = 4;
const PENDING_UPLOAD_NODE_COUNT_FIELD: u16 = 5;
const PENDING_UPLOAD_CREATED_AT_MS_FIELD: u16 = 6;
const PENDING_UPLOAD_CHUNKS_FIELD: u16 = 7;

#[derive(Clone)]
struct DurableMetadataRecordSchema {
    slots: Vec<DurableMetadataRecordSlot>,
    descriptor: records::RecordDescriptor,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum DurableMetadataRecordSlot {
    Known(u16),
    Reserved(u16),
}

/// Construct a fixed engine-owned record layout from permanent, one-based
/// positional field IDs. Sorting source declarations is a no-op, but an ID is
/// also its actual ordinal: gaps become reserved nullable slots so renumbering
/// a field changes the physical record schema rather than being normalized by
/// a name sort.
fn durable_metadata_record_schema(
    fields: impl IntoIterator<Item = (u16, &'static str, records::ValueType)>,
) -> DurableMetadataRecordSchema {
    let mut fields = fields.into_iter().collect::<Vec<_>>();
    fields.sort_by_key(|(id, _, _)| *id);
    assert!(
        fields.iter().all(|(id, _, _)| *id != 0),
        "durable metadata record field IDs start at one"
    );
    assert!(
        fields.windows(2).all(|fields| fields[0].0 != fields[1].0),
        "durable metadata record has duplicate field IDs"
    );
    let max_field_id = fields
        .last()
        .expect("durable metadata record must have at least one field")
        .0;
    let mut fields = fields.into_iter().peekable();
    let mut slots = Vec::with_capacity(usize::from(max_field_id));
    let mut descriptor_fields = Vec::with_capacity(usize::from(max_field_id));
    for slot_id in 1..=max_field_id {
        match fields.peek() {
            Some((id, _, _)) if *id == slot_id => {
                let (_, name, value_type) = fields.next().expect("peeked field must exist");
                slots.push(DurableMetadataRecordSlot::Known(slot_id));
                descriptor_fields.push((format!("f{slot_id:04}_{name}"), value_type));
            }
            _ => {
                slots.push(DurableMetadataRecordSlot::Reserved(slot_id));
                descriptor_fields.push((
                    format!("f{slot_id:04}_reserved"),
                    records::ValueType::Nullable(Box::new(records::ValueType::raw_bytes())),
                ));
            }
        }
    }
    debug_assert!(fields.next().is_none());
    let descriptor = records::RecordDescriptor::new(descriptor_fields);
    DurableMetadataRecordSchema { slots, descriptor }
}

fn large_value_root_references_schema() -> &'static DurableMetadataRecordSchema {
    static SCHEMA: OnceLock<DurableMetadataRecordSchema> = OnceLock::new();
    SCHEMA.get_or_init(|| {
        durable_metadata_record_schema([
            (ROOT_REF_DURABLE_FIELD, "durable", records::ValueType::U64),
            (ROOT_REF_STAGED_FIELD, "staged", records::ValueType::U64),
            (
                ROOT_REF_NODE_ACTIVE_FIELD,
                "node_active",
                records::ValueType::Bool,
            ),
        ])
    })
}

fn large_value_node_references_schema() -> &'static DurableMetadataRecordSchema {
    static SCHEMA: OnceLock<DurableMetadataRecordSchema> = OnceLock::new();
    SCHEMA.get_or_init(|| {
        durable_metadata_record_schema([
            (
                NODE_REF_REFERENCES_FIELD,
                "references",
                records::ValueType::U64,
            ),
            (
                NODE_REF_UPLOAD_REFERENCES_FIELD,
                "upload_references",
                records::ValueType::U64,
            ),
            (
                NODE_REF_CHILDREN_FIELD,
                "children",
                records::ValueType::Array(Box::new(records::ValueType::Record(Box::new(
                    crate::large_values::node_ref_descriptor(),
                )))),
            ),
        ])
    })
}

fn staged_large_value_schema() -> &'static DurableMetadataRecordSchema {
    static SCHEMA: OnceLock<DurableMetadataRecordSchema> = OnceLock::new();
    SCHEMA.get_or_init(|| {
        durable_metadata_record_schema([
            (STAGED_VALUE_ID_FIELD, "id", records::ValueType::raw_bytes()),
            (
                STAGED_VALUE_REF_FIELD,
                "value_ref",
                // Keep the canonical descriptor as opaque bytes at the
                // metadata envelope boundary. Its own raw decoder must select
                // format before inspecting V1 root/edit layout.
                records::ValueType::raw_bytes(),
            ),
            (
                STAGED_VALUE_ENCODED_BYTES_FIELD,
                "encoded_bytes",
                records::ValueType::U64,
            ),
            (
                STAGED_VALUE_NODE_COUNT_FIELD,
                "node_count",
                records::ValueType::U64,
            ),
            (
                STAGED_VALUE_CREATED_AT_MS_FIELD,
                "created_at_ms",
                records::ValueType::U64,
            ),
        ])
    })
}

fn pending_large_value_upload_schema() -> &'static DurableMetadataRecordSchema {
    static SCHEMA: OnceLock<DurableMetadataRecordSchema> = OnceLock::new();
    SCHEMA.get_or_init(|| {
        durable_metadata_record_schema([
            (
                PENDING_UPLOAD_ID_FIELD,
                "id",
                records::ValueType::raw_bytes(),
            ),
            (
                PENDING_UPLOAD_DESCRIPTOR_FIELD,
                "descriptor",
                records::ValueType::Nullable(Box::new(records::ValueType::raw_bytes())),
            ),
            (
                PENDING_UPLOAD_RECEIPT_ID_FIELD,
                "receipt_id",
                records::ValueType::Nullable(Box::new(records::ValueType::raw_bytes())),
            ),
            (
                PENDING_UPLOAD_ENCODED_BYTES_FIELD,
                "encoded_bytes",
                records::ValueType::U64,
            ),
            (
                PENDING_UPLOAD_NODE_COUNT_FIELD,
                "node_count",
                records::ValueType::U64,
            ),
            (
                PENDING_UPLOAD_CREATED_AT_MS_FIELD,
                "created_at_ms",
                records::ValueType::U64,
            ),
            (
                PENDING_UPLOAD_CHUNKS_FIELD,
                "chunks",
                records::ValueType::Array(Box::new(records::ValueType::Record(Box::new(
                    crate::large_values::node_ref_descriptor(),
                )))),
            ),
        ])
    })
}

fn encode_large_value_metadata_record(
    schema: &DurableMetadataRecordSchema,
    input_values: impl IntoIterator<Item = (u16, records::Value)>,
    name: &'static str,
) -> Result<Vec<u8>, Error> {
    let mut values = BTreeMap::<u16, records::Value>::new();
    for (id, value) in input_values {
        if values.insert(id, value).is_some() {
            return Err(Error::InvalidLargeValueMetadata(format!(
                "cannot encode {name}: duplicate field id {id}"
            )));
        }
    }
    let mut ordered = Vec::with_capacity(schema.slots.len());
    for slot in &schema.slots {
        match slot {
            DurableMetadataRecordSlot::Known(id) => {
                ordered.push(values.remove(id).ok_or_else(|| {
                    Error::InvalidLargeValueMetadata(format!(
                        "cannot encode {name}: missing field id {id}"
                    ))
                })?)
            }
            DurableMetadataRecordSlot::Reserved(_) => ordered.push(records::Value::Nullable(None)),
        }
    }
    if let Some((id, _)) = values.into_iter().next() {
        return Err(Error::InvalidLargeValueMetadata(format!(
            "cannot encode {name}: unknown field id {id}"
        )));
    }
    schema
        .descriptor
        .create(&ordered)
        .map_err(|error| Error::InvalidLargeValueMetadata(format!("cannot encode {name}: {error}")))
}

fn decode_large_value_metadata_record(
    encoded: &[u8],
    schema: &DurableMetadataRecordSchema,
    name: &'static str,
) -> Result<BTreeMap<u16, records::Value>, Error> {
    let values = schema
        .descriptor
        .bind(encoded)
        .to_values()
        .map_err(|error| {
            Error::InvalidLargeValueMetadata(format!("cannot decode {name}: {error}"))
        })?;
    let canonical = schema.descriptor.create(&values).map_err(|error| {
        Error::InvalidLargeValueMetadata(format!("cannot decode {name}: {error}"))
    })?;
    if canonical != encoded {
        return Err(Error::InvalidLargeValueMetadata(format!(
            "cannot decode {name}: non-canonical record"
        )));
    }
    let mut decoded = BTreeMap::new();
    for (slot, value) in schema.slots.iter().zip(values) {
        match slot {
            DurableMetadataRecordSlot::Known(id) => {
                decoded.insert(*id, value);
            }
            DurableMetadataRecordSlot::Reserved(id) => {
                if !matches!(value, records::Value::Nullable(None)) {
                    return Err(Error::InvalidLargeValueMetadata(format!(
                        "cannot decode {name}: reserved field id {id} is nonempty"
                    )));
                }
            }
        }
    }
    Ok(decoded)
}

fn staged_large_value_id_value(id: crate::large_values::StagedLargeValueId) -> records::Value {
    records::Value::Bytes(id.0.to_vec())
}

fn staged_large_value_id_from_value(
    value: &records::Value,
) -> Result<crate::large_values::StagedLargeValueId, Error> {
    let records::Value::Bytes(bytes) = value else {
        return Err(Error::InvalidLargeValueMetadata(
            "large-value staging id must be bytes".to_owned(),
        ));
    };
    let id = bytes.as_slice().try_into().map_err(|_| {
        Error::InvalidLargeValueMetadata("large-value staging id must be 16 bytes".to_owned())
    })?;
    Ok(crate::large_values::StagedLargeValueId(id))
}

fn canonical_node_ref_values(
    values: impl IntoIterator<Item = crate::large_values::NodeRef>,
) -> Vec<records::Value> {
    canonical_large_value_children(values)
        .iter()
        .map(crate::large_values::node_ref_value)
        .collect()
}

fn canonical_node_refs_from_value(
    value: &records::Value,
    name: &'static str,
) -> Result<Vec<crate::large_values::NodeRef>, Error> {
    let records::Value::Array(values) = value else {
        return Err(Error::InvalidLargeValueMetadata(format!(
            "{name} must be an array"
        )));
    };
    let mut refs = Vec::with_capacity(values.len());
    let mut previous = None;
    for value in values {
        let node_ref = crate::large_values::node_ref_from_value(value).map_err(|error| {
            Error::InvalidLargeValueMetadata(format!("cannot decode {name}: {error}"))
        })?;
        if previous
            .as_ref()
            .is_some_and(|previous| previous >= &node_ref)
        {
            return Err(Error::InvalidLargeValueMetadata(format!(
                "{name} must be strictly increasing"
            )));
        }
        previous = Some(node_ref.clone());
        refs.push(node_ref);
    }
    Ok(refs)
}

fn take_metadata_field(
    values: &mut BTreeMap<u16, records::Value>,
    id: u16,
    name: &'static str,
) -> Result<records::Value, Error> {
    values.remove(&id).ok_or_else(|| {
        Error::InvalidLargeValueMetadata(format!("cannot decode {name}: missing field id {id}"))
    })
}

fn encode_large_value_root_references(
    references: &LargeValueRootReferences,
) -> Result<Vec<u8>, Error> {
    encode_large_value_metadata_record(
        large_value_root_references_schema(),
        [
            (
                ROOT_REF_DURABLE_FIELD,
                records::Value::U64(references.durable),
            ),
            (
                ROOT_REF_STAGED_FIELD,
                records::Value::U64(references.staged),
            ),
            (
                ROOT_REF_NODE_ACTIVE_FIELD,
                records::Value::Bool(references.node_active),
            ),
        ],
        "root references",
    )
}

fn decode_large_value_root_references(encoded: &[u8]) -> Result<LargeValueRootReferences, Error> {
    let mut values = decode_large_value_metadata_record(
        encoded,
        large_value_root_references_schema(),
        "root references",
    )?;
    let records::Value::U64(durable) =
        take_metadata_field(&mut values, ROOT_REF_DURABLE_FIELD, "root references")?
    else {
        return Err(Error::InvalidLargeValueMetadata(
            "cannot decode root references: invalid fields".to_owned(),
        ));
    };
    let records::Value::U64(staged) =
        take_metadata_field(&mut values, ROOT_REF_STAGED_FIELD, "root references")?
    else {
        return Err(Error::InvalidLargeValueMetadata(
            "cannot decode root references: invalid fields".to_owned(),
        ));
    };
    let records::Value::Bool(node_active) =
        take_metadata_field(&mut values, ROOT_REF_NODE_ACTIVE_FIELD, "root references")?
    else {
        return Err(Error::InvalidLargeValueMetadata(
            "cannot decode root references: invalid fields".to_owned(),
        ));
    };
    Ok(LargeValueRootReferences {
        durable,
        staged,
        node_active,
    })
}

fn encode_large_value_node_references(
    references: &LargeValueNodeReferences,
) -> Result<Vec<u8>, Error> {
    encode_large_value_metadata_record(
        large_value_node_references_schema(),
        [
            (
                NODE_REF_REFERENCES_FIELD,
                records::Value::U64(references.references),
            ),
            (
                NODE_REF_UPLOAD_REFERENCES_FIELD,
                records::Value::U64(references.upload_references),
            ),
            (
                NODE_REF_CHILDREN_FIELD,
                records::Value::Array(canonical_node_ref_values(references.children.clone())),
            ),
        ],
        "large-value node references",
    )
}

fn decode_large_value_node_references(encoded: &[u8]) -> Result<LargeValueNodeReferences, Error> {
    let mut values = decode_large_value_metadata_record(
        encoded,
        large_value_node_references_schema(),
        "large-value node references",
    )?;
    let records::Value::U64(references) = take_metadata_field(
        &mut values,
        NODE_REF_REFERENCES_FIELD,
        "large-value node references",
    )?
    else {
        return Err(Error::InvalidLargeValueMetadata(
            "cannot decode large-value node references: invalid fields".to_owned(),
        ));
    };
    let records::Value::U64(upload_references) = take_metadata_field(
        &mut values,
        NODE_REF_UPLOAD_REFERENCES_FIELD,
        "large-value node references",
    )?
    else {
        return Err(Error::InvalidLargeValueMetadata(
            "cannot decode large-value node references: invalid fields".to_owned(),
        ));
    };
    let children = take_metadata_field(
        &mut values,
        NODE_REF_CHILDREN_FIELD,
        "large-value node references",
    )?;
    Ok(LargeValueNodeReferences {
        references,
        upload_references,
        children: canonical_node_refs_from_value(&children, "large-value node children")?,
    })
}

fn encode_staged_large_value(
    staged: &crate::large_values::StagedLargeValue,
) -> Result<Vec<u8>, Error> {
    encode_large_value_metadata_record(
        staged_large_value_schema(),
        [
            (
                STAGED_VALUE_ID_FIELD,
                staged_large_value_id_value(staged.id),
            ),
            (
                STAGED_VALUE_REF_FIELD,
                records::Value::Bytes(
                    crate::large_values::encode_large_value_ref(&staged.value_ref).map_err(
                        |error| {
                            Error::InvalidLargeValueMetadata(format!(
                                "cannot encode staged large value: {error}"
                            ))
                        },
                    )?,
                ),
            ),
            (
                STAGED_VALUE_ENCODED_BYTES_FIELD,
                records::Value::U64(staged.accounting.encoded_bytes),
            ),
            (
                STAGED_VALUE_NODE_COUNT_FIELD,
                records::Value::U64(staged.accounting.node_count),
            ),
            (
                STAGED_VALUE_CREATED_AT_MS_FIELD,
                records::Value::U64(staged.created_at_ms),
            ),
        ],
        "staged large value",
    )
}

fn decode_staged_large_value(
    encoded: &[u8],
) -> Result<crate::large_values::StagedLargeValue, Error> {
    let mut values = decode_large_value_metadata_record(
        encoded,
        staged_large_value_schema(),
        "staged large value",
    )?;
    let id = take_metadata_field(&mut values, STAGED_VALUE_ID_FIELD, "staged large value")?;
    let records::Value::Bytes(value_ref) =
        take_metadata_field(&mut values, STAGED_VALUE_REF_FIELD, "staged large value")?
    else {
        return Err(Error::InvalidLargeValueMetadata(
            "cannot decode staged large value: invalid descriptor bytes".to_owned(),
        ));
    };
    let records::Value::U64(encoded_bytes) = take_metadata_field(
        &mut values,
        STAGED_VALUE_ENCODED_BYTES_FIELD,
        "staged large value",
    )?
    else {
        return Err(Error::InvalidLargeValueMetadata(
            "cannot decode staged large value: invalid fields".to_owned(),
        ));
    };
    let records::Value::U64(node_count) = take_metadata_field(
        &mut values,
        STAGED_VALUE_NODE_COUNT_FIELD,
        "staged large value",
    )?
    else {
        return Err(Error::InvalidLargeValueMetadata(
            "cannot decode staged large value: invalid fields".to_owned(),
        ));
    };
    let records::Value::U64(created_at_ms) = take_metadata_field(
        &mut values,
        STAGED_VALUE_CREATED_AT_MS_FIELD,
        "staged large value",
    )?
    else {
        return Err(Error::InvalidLargeValueMetadata(
            "cannot decode staged large value: invalid fields".to_owned(),
        ));
    };
    Ok(crate::large_values::StagedLargeValue {
        id: staged_large_value_id_from_value(&id)?,
        value_ref: crate::large_values::decode_large_value_ref(&value_ref).map_err(|error| {
            Error::InvalidLargeValueMetadata(format!(
                "cannot decode staged large value descriptor: {error}"
            ))
        })?,
        accounting: crate::large_values::StagedLargeValueAccounting {
            encoded_bytes,
            node_count,
        },
        created_at_ms,
    })
}

fn decode_staged_large_value_at_key(
    key: &[u8],
    encoded: &[u8],
) -> Result<crate::large_values::StagedLargeValue, Error> {
    let key_id = staged_large_value_id_from_metadata_key(key, b"staged/", "staged large value")?;
    let staged = decode_staged_large_value(encoded)?;
    if staged.id != key_id {
        return Err(Error::InvalidLargeValueMetadata(
            "staged large value key and receipt id differ".to_owned(),
        ));
    }
    Ok(staged)
}

fn encode_pending_large_value_upload(
    upload: &crate::large_values::PendingLargeValueUpload,
) -> Result<Vec<u8>, Error> {
    let descriptor = upload
        .descriptor
        .as_ref()
        .map(crate::large_values::encode_large_value_ref)
        .transpose()
        .map_err(|error| {
            Error::InvalidLargeValueMetadata(format!(
                "cannot encode pending large-value upload descriptor: {error}"
            ))
        })?;
    encode_large_value_metadata_record(
        pending_large_value_upload_schema(),
        [
            (
                PENDING_UPLOAD_ID_FIELD,
                staged_large_value_id_value(upload.id),
            ),
            (
                PENDING_UPLOAD_DESCRIPTOR_FIELD,
                records::Value::Nullable(
                    descriptor.map(|bytes| Box::new(records::Value::Bytes(bytes))),
                ),
            ),
            (
                PENDING_UPLOAD_RECEIPT_ID_FIELD,
                records::Value::Nullable(
                    upload
                        .receipt_id
                        .map(staged_large_value_id_value)
                        .map(Box::new),
                ),
            ),
            (
                PENDING_UPLOAD_ENCODED_BYTES_FIELD,
                records::Value::U64(upload.accounting.encoded_bytes),
            ),
            (
                PENDING_UPLOAD_NODE_COUNT_FIELD,
                records::Value::U64(upload.accounting.node_count),
            ),
            (
                PENDING_UPLOAD_CREATED_AT_MS_FIELD,
                records::Value::U64(upload.created_at_ms),
            ),
            (
                PENDING_UPLOAD_CHUNKS_FIELD,
                records::Value::Array(canonical_node_ref_values(upload.chunks.clone())),
            ),
        ],
        "pending large-value upload",
    )
}

fn decode_pending_large_value_upload(
    encoded: &[u8],
) -> Result<crate::large_values::PendingLargeValueUpload, Error> {
    let mut values = decode_large_value_metadata_record(
        encoded,
        pending_large_value_upload_schema(),
        "pending large-value upload",
    )?;
    let id = take_metadata_field(
        &mut values,
        PENDING_UPLOAD_ID_FIELD,
        "pending large-value upload",
    )?;
    let records::Value::Nullable(descriptor) = take_metadata_field(
        &mut values,
        PENDING_UPLOAD_DESCRIPTOR_FIELD,
        "pending large-value upload",
    )?
    else {
        return Err(Error::InvalidLargeValueMetadata(
            "cannot decode pending large-value upload: invalid fields".to_owned(),
        ));
    };
    let records::Value::Nullable(receipt_id) = take_metadata_field(
        &mut values,
        PENDING_UPLOAD_RECEIPT_ID_FIELD,
        "pending large-value upload",
    )?
    else {
        return Err(Error::InvalidLargeValueMetadata(
            "cannot decode pending large-value upload: invalid fields".to_owned(),
        ));
    };
    let records::Value::U64(encoded_bytes) = take_metadata_field(
        &mut values,
        PENDING_UPLOAD_ENCODED_BYTES_FIELD,
        "pending large-value upload",
    )?
    else {
        return Err(Error::InvalidLargeValueMetadata(
            "cannot decode pending large-value upload: invalid fields".to_owned(),
        ));
    };
    let records::Value::U64(node_count) = take_metadata_field(
        &mut values,
        PENDING_UPLOAD_NODE_COUNT_FIELD,
        "pending large-value upload",
    )?
    else {
        return Err(Error::InvalidLargeValueMetadata(
            "cannot decode pending large-value upload: invalid fields".to_owned(),
        ));
    };
    let records::Value::U64(created_at_ms) = take_metadata_field(
        &mut values,
        PENDING_UPLOAD_CREATED_AT_MS_FIELD,
        "pending large-value upload",
    )?
    else {
        return Err(Error::InvalidLargeValueMetadata(
            "cannot decode pending large-value upload: invalid fields".to_owned(),
        ));
    };
    let chunks = take_metadata_field(
        &mut values,
        PENDING_UPLOAD_CHUNKS_FIELD,
        "pending large-value upload",
    )?;
    let descriptor = descriptor
        .as_deref()
        .map(|value| match value {
            records::Value::Bytes(bytes) => crate::large_values::decode_large_value_ref(bytes),
            _ => Err(crate::large_values::Error::MalformedScalar),
        })
        .transpose()
        .map_err(|error| {
            Error::InvalidLargeValueMetadata(format!(
                "cannot decode pending large-value upload descriptor: {error}"
            ))
        })?;
    let receipt_id = receipt_id
        .as_deref()
        .map(staged_large_value_id_from_value)
        .transpose()?;
    Ok(crate::large_values::PendingLargeValueUpload {
        id: staged_large_value_id_from_value(&id)?,
        descriptor,
        receipt_id,
        accounting: crate::large_values::StagedLargeValueAccounting {
            encoded_bytes,
            node_count,
        },
        created_at_ms,
        chunks: canonical_node_refs_from_value(&chunks, "pending large-value upload chunks")?,
    })
}

fn decode_pending_large_value_upload_at_key(
    key: &[u8],
    encoded: &[u8],
) -> Result<crate::large_values::PendingLargeValueUpload, Error> {
    let key_id = staged_large_value_id_from_metadata_key(key, b"upload/", "pending upload")?;
    let upload = decode_pending_large_value_upload(encoded)?;
    if upload.id != key_id {
        return Err(Error::InvalidLargeValueMetadata(
            "pending upload key and journal id differ".to_owned(),
        ));
    }
    Ok(upload)
}

/// Decode the forward completed-upload journal only when its embedded upload
/// identity is the canonical fixed-width suffix of its metadata key.
fn decode_completed_large_value_upload_at_key(
    key: &[u8],
    encoded: &[u8],
) -> Result<crate::large_values::PendingLargeValueUpload, Error> {
    let key_id =
        staged_large_value_id_from_metadata_key(key, b"completed-upload/", "completed upload")?;
    let completed = decode_pending_large_value_upload(encoded)?;
    if completed.id != key_id {
        return Err(Error::InvalidLargeValueMetadata(
            "completed upload key and journal id differ".to_owned(),
        ));
    }
    Ok(completed)
}

/// Decode the reverse completed-receipt journal only when its embedded receipt
/// identity is the canonical fixed-width suffix of its metadata key.
fn decode_completed_large_value_receipt_at_key(
    key: &[u8],
    encoded: &[u8],
) -> Result<crate::large_values::PendingLargeValueUpload, Error> {
    let key_id =
        staged_large_value_id_from_metadata_key(key, b"completed-receipt/", "completed receipt")?;
    let completed = decode_pending_large_value_upload(encoded)?;
    if completed.receipt_id != Some(key_id) {
        return Err(Error::InvalidLargeValueMetadata(
            "completed receipt key and receipt id differ".to_owned(),
        ));
    }
    Ok(completed)
}

/// Apply physical-node ownership transitions against one read-your-own-write
/// overlay. Each active parent contributes one reference to each distinct
/// child node, regardless of how many logical occurrences of that child the
/// parent's branch contains. Shared descendants reached through distinct
/// active parents still receive one reference from each parent.
async fn large_value_node_transition_operations<S>(
    storage: &S,
    mut node_updates: BTreeMap<crate::large_values::NodeRef, LargeValueNodeReferences>,
    mut pending: Vec<(crate::large_values::NodeRef, i8)>,
    allow_missing_positive_metadata: bool,
) -> Result<Vec<OwnedWriteOperation>, Error>
where
    S: OrderedKvStorage + ?Sized,
{
    let mut node_budget = crate::large_values::PhysicalTraversalNodeBudget::new();
    node_budget
        .consume_many(node_updates.len())
        .map_err(crate::ivm::runtime::IvmRuntimeError::from)?;
    let mut reclaim_candidates = BTreeSet::new();
    while let Some((node_ref, delta)) = pending.pop() {
        let mut metadata = if let Some(metadata) = node_updates.remove(&node_ref) {
            metadata
        } else {
            node_budget
                .consume()
                .map_err(crate::ivm::runtime::IvmRuntimeError::from)?;
            match storage
                .get(
                    LARGE_VALUE_METADATA_CF.to_owned(),
                    large_value_node_key(&node_ref)?,
                )
                .await?
            {
                Some(encoded) => decode_large_value_node_references(&encoded)?,
                None if delta > 0 && allow_missing_positive_metadata => {
                    LargeValueNodeReferences::default()
                }
                None => {
                    return Err(Error::InvalidLargeValueMetadata(
                        "active node reference metadata is missing".to_owned(),
                    ));
                }
            }
        };
        metadata.children = canonical_large_value_children(metadata.children);
        let crossed_zero = if delta > 0 {
            let crossed = metadata.references == 0;
            metadata.references = metadata.references.checked_add(1).ok_or_else(|| {
                Error::InvalidLargeValueMetadata("node reference count overflow".to_owned())
            })?;
            reclaim_candidates.remove(&node_ref);
            crossed
        } else {
            metadata.references = metadata.references.checked_sub(1).ok_or_else(|| {
                Error::InvalidLargeValueMetadata("node reference count underflow".to_owned())
            })?;
            let crossed = metadata.references == 0;
            if crossed {
                reclaim_candidates.insert(node_ref.clone());
            }
            crossed
        };
        if crossed_zero {
            pending.extend(
                metadata
                    .children
                    .iter()
                    .cloned()
                    .map(|child| (child, delta)),
            );
        }
        node_updates.insert(node_ref, metadata);
    }
    let mut operations = Vec::new();
    for (node_ref, metadata) in node_updates {
        operations.push(OwnedWriteOperation::Set {
            cf: LARGE_VALUE_METADATA_CF.to_owned(),
            key: large_value_node_key(&node_ref)?,
            value: encode_large_value_node_references(&metadata)?,
        });
        if metadata.references == 0 && reclaim_candidates.contains(&node_ref) {
            operations.push(OwnedWriteOperation::Set {
                cf: LARGE_VALUE_METADATA_CF.to_owned(),
                key: large_value_reclaim_key(&node_ref)?,
                value: crate::large_values::encode_node_ref(&node_ref).map_err(|error| {
                    Error::InvalidLargeValueMetadata(format!(
                        "cannot encode reclaim entry: {error}"
                    ))
                })?,
            });
        }
    }
    Ok(operations)
}

#[derive(Clone)]
struct MetadataChunkInstallObserver {
    storage: std::rc::Weak<LayoutStorage>,
    lifecycle: Weak<AsyncMutex<()>>,
    resident_install: Option<ResidentLifecycleInstall>,
}

/// The recovery journal is intentionally separate from byte storage and from
/// metadata installation. It only answers whether a particular exact chunk
/// needs reconciliation, allowing normal resident reads to avoid both the
/// observer and the large-value lifecycle mutex.
#[derive(Clone)]
struct MetadataChunkInstallJournal {
    storage: std::rc::Weak<LayoutStorage>,
}

impl crate::chunks::ChunkInstallJournal for MetadataChunkInstallJournal {
    fn mark_pending(
        &self,
        node_ref: crate::large_values::NodeRef,
    ) -> crate::chunks::ChunkFuture<'_, Result<(), crate::chunks::ChunkError>> {
        Box::pin(async move {
            let storage = self.storage.upgrade().ok_or_else(|| {
                crate::chunks::ChunkError::Backend(
                    "database storage closed while recording chunk installation".to_owned(),
                )
            })?;
            let key = large_value_pending_install_key(&node_ref)
                .map_err(|error| crate::chunks::ChunkError::Backend(error.to_string()))?;
            let existing = storage
                .put_if_absent(LARGE_VALUE_METADATA_CF.to_owned(), key, Vec::new())
                .await
                .map_err(|error| crate::chunks::ChunkError::Backend(error.to_string()))?;
            if existing.is_some_and(|value| !value.is_empty()) {
                return Err(crate::chunks::ChunkError::Integrity);
            }
            Ok(())
        })
    }

    fn is_pending(
        &self,
        node_ref: crate::large_values::NodeRef,
    ) -> crate::chunks::ChunkFuture<'_, Result<bool, crate::chunks::ChunkError>> {
        Box::pin(async move {
            let storage = self.storage.upgrade().ok_or_else(|| {
                crate::chunks::ChunkError::Backend(
                    "database storage closed while reading chunk installation journal".to_owned(),
                )
            })?;
            let key = large_value_pending_install_key(&node_ref)
                .map_err(|error| crate::chunks::ChunkError::Backend(error.to_string()))?;
            match storage
                .get(LARGE_VALUE_METADATA_CF.to_owned(), key)
                .await
                .map_err(|error| crate::chunks::ChunkError::Backend(error.to_string()))?
            {
                None => Ok(false),
                Some(value) if value.is_empty() => Ok(true),
                Some(_) => Err(crate::chunks::ChunkError::Integrity),
            }
        })
    }

    fn complete(
        &self,
        node_ref: crate::large_values::NodeRef,
    ) -> crate::chunks::ChunkFuture<'_, Result<(), crate::chunks::ChunkError>> {
        Box::pin(async move {
            let storage = self.storage.upgrade().ok_or_else(|| {
                crate::chunks::ChunkError::Backend(
                    "database storage closed while completing chunk installation".to_owned(),
                )
            })?;
            let key = large_value_pending_install_key(&node_ref)
                .map_err(|error| crate::chunks::ChunkError::Backend(error.to_string()))?;
            storage
                .compare_and_delete(LARGE_VALUE_METADATA_CF.to_owned(), key, Vec::new())
                .await
                .map_err(|error| crate::chunks::ChunkError::Backend(error.to_string()))?;
            Ok(())
        })
    }
}

#[derive(Clone)]
struct ResidentLifecycleInstall {
    storage: OwnedStorage<'static>,
    staged: Rc<RefCell<StagedWriteState>>,
    /// Whether the database currently owns `lifecycle` on behalf of resident
    /// publications. A late installer takes the regular lock only after that
    /// guard is released.
    lifecycle_held: Rc<Cell<bool>>,
    /// Before durability, installation metadata belongs in the publication
    /// snapshot; afterwards it is a serialized follow-on write.
    durable: Rc<Cell<bool>>,
    install_failures: crate::chunks::PublicationInstallFailures,
}

impl crate::chunks::ChunkInstallObserver for MetadataChunkInstallObserver {
    fn installed(
        &self,
        node_ref: crate::large_values::NodeRef,
        encoded: bytes::Bytes,
    ) -> crate::chunks::ChunkFuture<'_, Result<(), crate::chunks::ChunkError>> {
        Box::pin(async move {
            let storage = self.storage.upgrade().ok_or_else(|| {
                crate::chunks::ChunkError::Backend(
                    "database storage closed during chunk installation".to_owned(),
                )
            })?;
            let resident_install = self.resident_install.clone();
            let _lifecycle = if resident_install
                .as_ref()
                .is_none_or(|install| !install.lifecycle_held.get())
            {
                Some(
                    self.lifecycle
                        .upgrade()
                        .ok_or_else(|| {
                            crate::chunks::ChunkError::Backend(
                                "database lifecycle closed during chunk installation".to_owned(),
                            )
                        })?
                        .lock_owned()
                        .await,
                )
            } else {
                None
            };
            let read_storage: &dyn OrderedKvStorage = match resident_install.as_ref() {
                Some(install) if !install.durable.get() => install.storage.as_ref(),
                _ => storage.as_ref(),
            };
            let node = crate::large_values::decode_node_untyped_authenticated(
                node_ref.object_hash,
                &encoded,
            )
            .map_err(|_| crate::chunks::ChunkError::Integrity)?;
            let children = unique_large_value_children(&node);
            let node_key = large_value_node_key(&node_ref)
                .map_err(|error| crate::chunks::ChunkError::Backend(error.to_string()))?;
            let existing = read_storage
                .get(LARGE_VALUE_METADATA_CF.to_owned(), node_key.clone())
                .await
                .map_err(|error| crate::chunks::ChunkError::Backend(error.to_string()))?;
            let mut metadata: LargeValueNodeReferences = existing
                .as_deref()
                .map(decode_large_value_node_references)
                .transpose()
                .map_err(|error| crate::chunks::ChunkError::Backend(error.to_string()))?
                .unwrap_or_default();
            let existing_children =
                canonical_large_value_children(std::mem::take(&mut metadata.children));
            if !existing_children.is_empty() && existing_children != children {
                return Err(crate::chunks::ChunkError::Integrity);
            }
            let newly_discovered_active_children =
                metadata.references > 0 && existing_children.is_empty() && !children.is_empty();
            metadata.children = children.clone();

            let root_key = large_value_root_key(&node_ref)
                .map_err(|error| crate::chunks::ChunkError::Backend(error.to_string()))?;
            let root_encoded = read_storage
                .get(LARGE_VALUE_METADATA_CF.to_owned(), root_key.clone())
                .await
                .map_err(|error| crate::chunks::ChunkError::Backend(error.to_string()))?;
            let mut root_references: LargeValueRootReferences = root_encoded
                .as_deref()
                .map(decode_large_value_root_references)
                .transpose()
                .map_err(|error| crate::chunks::ChunkError::Backend(error.to_string()))?
                .unwrap_or_default();
            let activate_root = root_references
                .durable
                .saturating_add(root_references.staged)
                > 0
                && !root_references.node_active;
            if activate_root {
                root_references.node_active = true;
            }
            let mut initial = BTreeMap::from([(node_ref.clone(), metadata)]);
            let mut transitions = Vec::new();
            if activate_root {
                transitions.push((node_ref.clone(), 1));
            }
            if newly_discovered_active_children {
                transitions.extend(children.into_iter().map(|child| (child, 1)));
            }
            let mut operations = large_value_node_transition_operations(
                read_storage,
                std::mem::take(&mut initial),
                transitions,
                true,
            )
            .await
            .map_err(|error| crate::chunks::ChunkError::Backend(error.to_string()))?;
            if activate_root {
                operations.push(OwnedWriteOperation::Set {
                    cf: LARGE_VALUE_METADATA_CF.to_owned(),
                    key: root_key,
                    value: encode_large_value_root_references(&root_references)
                        .map_err(|error| crate::chunks::ChunkError::Backend(error.to_string()))?,
                });
            }
            // The recovery marker and all node/root metadata share the same
            // durability boundary. In particular, a resident publication
            // keeps both staged until its persistence receipt settles; it
            // cannot leave resident bytes with neither metadata nor a retry
            // marker after a cancelled/failed publication.
            operations.push(OwnedWriteOperation::Delete {
                cf: LARGE_VALUE_METADATA_CF.to_owned(),
                key: large_value_pending_install_key(&node_ref)
                    .map_err(|error| crate::chunks::ChunkError::Backend(error.to_string()))?,
            });
            if let Some(install) = resident_install {
                if install.durable.get() {
                    match storage.write_many(operations).await {
                        Ok(()) => Ok(()),
                        Err(error) => {
                            let error = crate::chunks::ChunkError::Backend(error.to_string());
                            install.install_failures.record(node_ref, error.clone());
                            Err(error)
                        }
                    }
                } else {
                    install.staged.borrow_mut().extend(operations);
                    Ok(())
                }
            } else {
                storage
                    .write_many(operations)
                    .await
                    .map_err(|error| crate::chunks::ChunkError::Backend(error.to_string()))
            }
        })
    }

    fn completes_install_journal(&self) -> bool {
        true
    }
}

pub use crate::ivm::{
    CollectByField, GraphBuilder, IvmRuntimeError, MultisinkDeltas, MultisinkSubscription,
    PredicateExpr, PreparedShapeId, ProjectField, PublicationUpdate, RoutedMultisinkTerminal,
    Subscription, SubscriptionError, SubscriptionEvent, SubscriptionId,
};

/// Schema-aware database facade over storage and IVM subscriptions.
pub struct Database {
    storage: Rc<LayoutStorage>,
    chunk_storage: Rc<dyn crate::chunks::ChunkStorage>,
    chunk_resolver: Rc<dyn crate::chunks::MissingChunkResolver>,
    /// Owns query/index maintenance over the storage-backed base tables.
    ivm_runtime: IvmRuntime,
    last_commit_metrics: Option<CommitMetrics>,
    last_tick_metrics: Option<TickMetrics>,
    storage_read_metrics: Rc<RefCell<StorageReadMetrics>>,
    /// Dense record descriptors are invariant for one table variant. Keep the
    /// interned handles beside the database schema so scans do not rebuild and
    /// re-hash the same logical field list once per stored row.
    stored_record_descriptors: RefCell<BTreeMap<String, BTreeMap<u32, RecordDescriptor>>>,
    next_publication_id: u64,
    durable_publication_frontier: Option<PublicationId>,
    resident_publications: BTreeMap<PublicationId, Rc<RefCell<StagedWriteState>>>,
    persisted_publications: BTreeSet<PublicationId>,
    resident_writes: Rc<RefCell<StagedWriteState>>,
    publication_persistence: Rc<RefCell<PersistenceOrder>>,
    /// Serializes the durable upload journal, separate blob staging, expiry,
    /// promotion, and reclamation lifecycle. The blob backend may be separate
    /// from metadata storage, so this boundary prevents both intent eviction
    /// during an in-flight put and lost reference-count updates across uploads.
    large_value_lifecycle: Arc<AsyncMutex<()>>,
    /// Retains the lifecycle mutex while resident publications contain a
    /// root/node transition that has not crossed the durable frontier. Later
    /// resident publications can join the same protected sequence without
    /// waiting for themselves to persist; independent chunk installation is
    /// held outside it until every such transition is durable.
    large_value_publication_lifecycle_guard: Option<futures::lock::OwnedMutexGuard<()>>,
    /// Shared with resident install observers so follow-on writes retain
    /// lifecycle serialization after their publication becomes durable.
    large_value_lifecycle_held: Rc<Cell<bool>>,
    large_value_lifecycle_publications: BTreeSet<PublicationId>,
    abandoned_application: Rc<Cell<bool>>,
    poisoned: bool,
}

/// An in-memory checkpoint of the schema-derived IVM registry.
///
/// Live schema admission is deliberately append-only on success, but callers
/// which couple it to another activation boundary need a way to make the
/// registry invisible until that boundary commits.  This checkpoint excludes
/// storage, rows, publications, and chunk lifecycle state.  It does include
/// the complete IVM runtime because variant descriptors and projection cases
/// are spread throughout the runtime graph alongside live subscriptions.
#[doc(hidden)]
#[derive(Clone)]
pub struct RuntimeRegistryCheckpoint {
    ivm_runtime: IvmRuntime,
    stored_record_descriptors: BTreeMap<String, BTreeMap<u32, RecordDescriptor>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum AppliedBatchLifecycle {
    Applied,
    Persisting,
    PersistenceComplete,
    Finished,
    Abandoned,
}

/// One resident publication whose ordered storage write can progress without
/// borrowing the database runtime.
#[must_use = "an immediate publication must be persisted and settled"]
pub struct AppliedBatch {
    publication: PublicationId,
    storage: Rc<LayoutStorage>,
    operations: Rc<RefCell<StagedWriteState>>,
    resident_install_durable: Option<Rc<Cell<bool>>>,
    order: Rc<RefCell<PersistenceOrder>>,
    ivm_tick_time: Duration,
    tick: TickMetrics,
    notifications_deferred: bool,
    lifecycle: Rc<Cell<AppliedBatchLifecycle>>,
    abandoned_application: Rc<Cell<bool>>,
}

impl AppliedBatch {
    pub fn publication(&self) -> PublicationId {
        self.publication
    }

    pub async fn persist(&self) -> PersistedBatch {
        assert_eq!(
            self.lifecycle.replace(AppliedBatchLifecycle::Persisting),
            AppliedBatchLifecycle::Applied,
            "an applied batch may have only one persistence attempt at a time",
        );
        let mut attempt = PersistenceAttempt {
            lifecycle: Rc::clone(&self.lifecycle),
            order: Rc::clone(&self.order),
            publication: self.publication,
            abandoned_application: Rc::clone(&self.abandoned_application),
            write_started: false,
            completed: false,
        };
        let turn = std::future::poll_fn(|cx| {
            let mut order = self.order.borrow_mut();
            if let Some(message) = &order.failure {
                return Poll::Ready(Err(crate::storage::Error::Backend {
                    backend: "publication order",
                    message: message.clone(),
                }));
            }
            if order.next == self.publication.0 {
                return Poll::Ready(Ok(()));
            }
            order.waiters.insert(self.publication.0, cx.waker().clone());
            Poll::Pending
        })
        .await;
        let operations = self.operations.borrow().clone().into_operations();
        let storage_writes = StorageWriteMetrics::from_operations(
            &operations
                .iter()
                .map(OwnedWriteOperation::as_write_operation)
                .collect::<Vec<_>>(),
        );
        let storage_start = Instant::now();
        let outcome = match turn {
            Ok(()) => {
                attempt.write_started = true;
                self.storage.write_many_outcome(operations).await
            }
            Err(error) => WriteManyOutcome::Uncommitted(error),
        };
        let result = match outcome {
            WriteManyOutcome::Committed => Ok(()),
            WriteManyOutcome::Uncommitted(error) => Err(error),
            WriteManyOutcome::PossiblyCommitted(error) => {
                // Runtime state already reflects this publication. Without a
                // definite non-commit receipt it cannot be rolled back or
                // retried safely; every database entry point must fail closed
                // even before the host settles the returned receipt.
                self.abandoned_application.set(true);
                Err(error)
            }
        };
        let storage_write_time = storage_start.elapsed();
        if result.is_ok()
            && let Some(durable) = &self.resident_install_durable
        {
            durable.set(true);
        }
        self.lifecycle
            .set(AppliedBatchLifecycle::PersistenceComplete);
        attempt.completed = true;
        let waiter = {
            let mut order = self.order.borrow_mut();
            if result.is_ok() {
                order.next = order.next.saturating_add(1);
                let next = order.next;
                order.waiters.remove(&next)
            } else {
                order.failure = Some(
                    result
                        .as_ref()
                        .expect_err("failed persistence has an error")
                        .to_string(),
                );
                let waiters = std::mem::take(&mut order.waiters);
                for (_, waiter) in waiters {
                    waiter.wake();
                }
                None
            }
        };
        if let Some(waiter) = waiter {
            waiter.wake();
        }
        PersistedBatch {
            publication: self.publication,
            result,
            notifications_deferred: self.notifications_deferred,
            metrics: CommitMetrics {
                storage_write_time,
                ivm_tick_time: self.ivm_tick_time,
                storage_write_count: storage_writes.total.count,
                storage_write_bytes: storage_writes.total.bytes,
                storage_writes,
                tick: self.tick.clone(),
            },
            receipt: PersistenceReceipt {
                lifecycle: Rc::clone(&self.lifecycle),
                order: Rc::clone(&self.order),
                abandoned_application: Rc::clone(&self.abandoned_application),
            },
        }
    }
}

struct PersistenceAttempt {
    lifecycle: Rc<Cell<AppliedBatchLifecycle>>,
    order: Rc<RefCell<PersistenceOrder>>,
    publication: PublicationId,
    abandoned_application: Rc<Cell<bool>>,
    write_started: bool,
    completed: bool,
}

impl Drop for PersistenceAttempt {
    fn drop(&mut self) {
        if !self.completed && self.lifecycle.get() == AppliedBatchLifecycle::Persisting {
            if self.write_started {
                self.lifecycle.set(AppliedBatchLifecycle::Abandoned);
                self.abandoned_application.set(true);
                let waiters = {
                    let mut order = self.order.borrow_mut();
                    order.failure = Some(format!(
                        "publication {:?} persistence was cancelled after its atomic write started",
                        self.publication
                    ));
                    std::mem::take(&mut order.waiters)
                };
                for (_, waiter) in waiters {
                    waiter.wake();
                }
            } else {
                self.lifecycle.set(AppliedBatchLifecycle::Applied);
            }
        }
    }
}

impl Drop for AppliedBatch {
    fn drop(&mut self) {
        if self.lifecycle.get() == AppliedBatchLifecycle::Applied {
            self.lifecycle.set(AppliedBatchLifecycle::Abandoned);
            self.abandoned_application.set(true);
        }
    }
}

struct PersistenceOrder {
    next: u64,
    waiters: BTreeMap<u64, Waker>,
    failure: Option<String>,
}

/// Completion of one owned publication persistence operation.
#[must_use = "persistence completion must be settled on its database"]
pub struct PersistedBatch {
    publication: PublicationId,
    result: Result<(), crate::storage::Error>,
    notifications_deferred: bool,
    metrics: CommitMetrics,
    receipt: PersistenceReceipt,
}

struct PersistenceReceipt {
    lifecycle: Rc<Cell<AppliedBatchLifecycle>>,
    order: Rc<RefCell<PersistenceOrder>>,
    abandoned_application: Rc<Cell<bool>>,
}

impl PersistenceReceipt {
    fn finish(&self) {
        self.lifecycle.set(AppliedBatchLifecycle::Finished);
    }
}

impl Drop for PersistenceReceipt {
    fn drop(&mut self) {
        if self.lifecycle.get() == AppliedBatchLifecycle::PersistenceComplete {
            self.lifecycle.set(AppliedBatchLifecycle::Abandoned);
            self.abandoned_application.set(true);
        }
    }
}

mod batch;
mod commit;
mod encoding;
mod facade;
mod primary_storage;
mod query;
mod schema_admission;
mod storage_helpers;

pub use batch::*;
use encoding::*;
pub(crate) use encoding::{index_record_descriptor, persisted_index_primary_key};
use schema_admission::*;
pub(crate) use storage_helpers::MeteredStorage;
use storage_helpers::*;
pub use storage_helpers::{
    CommitMetrics, DirectRecordStore, DirectRecordStoreEntry, DirectRecordStoreWrite,
    EncodedKeyValue, PreparedShape, StorageReadBucket, StorageReadMetrics, StorageWriteBucket,
    StorageWriteMetrics,
};

#[derive(Debug, Error)]
pub enum Error {
    #[error("database instance is poisoned after a failed atomic commit")]
    DatabasePoisoned,
    #[error("publication does not belong to this database: {0:?}")]
    PublicationNotFound(PublicationId),
    #[error("subscription ended")]
    SubscriptionEnded,
    #[error(transparent)]
    SubscriptionFailed(#[from] SubscriptionError),
    #[error("duplicate primary key for table {table}: {key:?}")]
    DuplicatePrimaryKey { table: String, key: Vec<u8> },
    #[error("duplicate schema version {version} for table {table}")]
    DuplicateTableVariant { table: String, version: u64 },
    #[error("table {table} variant tag {tag} exceeds the bounded u32 tag space")]
    TableVariantTagOutOfRange { table: String, tag: u64 },
    #[error("duplicate query parameter binding: {0}")]
    DuplicateParameter(String),
    #[error(transparent)]
    IvmRuntime(#[from] IvmRuntimeError),
    #[error("invalid persisted index contents: {0}")]
    InvalidPersistedIndex(String),
    #[error("invalid persisted large-value metadata: {0}")]
    InvalidLargeValueMetadata(String),
    #[error("pending large-value upload limit reached: {limit}")]
    PendingLargeValueUploadLimitExceeded { limit: usize },
    #[error("index key arity mismatch for {index}: expected at most {expected}, got {actual}")]
    IndexKeyArity {
        index: String,
        expected: usize,
        actual: usize,
    },
    #[error("index not found: {table}.{index}")]
    IndexNotFound { table: String, index: String },
    #[error("missing query parameter binding: {0}")]
    MissingParameter(String),
    #[error("table has no primary key: {0}")]
    MissingPrimaryKey(String),
    #[error("invalid field {field} in schema version {version} for table {table}")]
    InvalidTableVariantField {
        table: String,
        version: u64,
        field: String,
    },
    #[error("primary key arity mismatch for {table}: expected at most {expected}, got {actual}")]
    PrimaryKeyArity {
        table: String,
        expected: usize,
        actual: usize,
    },
    #[error("primary key type mismatch for {table}.{column}")]
    PrimaryKeyTypeMismatch { table: String, column: String },
    #[error(transparent)]
    QueryPlanning(#[from] PlannerError),
    #[error(transparent)]
    RecordEncoding(#[from] records::Error),
    #[error("direct record store not found: {0}")]
    DirectRecordStoreNotFound(String),
    #[error("invalid direct record store key: {0}")]
    InvalidDirectRecordStoreKey(String),
    #[error("invalid application storage name {name:?}: {reason}")]
    InvalidApplicationStorageName { name: String, reason: String },
    #[error("application table/direct-record-store storage name is duplicated: {0}")]
    DuplicateApplicationStorageName(String),
    #[error(transparent)]
    Storage(Box<crate::storage::Error>),
    #[error("table not found: {0}")]
    TableNotFound(String),
    #[error("table already exists: {0}")]
    TableAlreadyExists(String),
    #[error("field definition does not match the live catalogue: {table}.{field}")]
    TableFieldDefinitionMismatch { table: String, field: String },
    #[error("index definition does not match the live catalogue: {table}.{index}")]
    TableIndexDefinitionMismatch { table: String, index: String },
    #[error("cannot register index {table}.{index} while database publications remain resident")]
    TableIndexRegistrationWhilePublicationsResident { table: String, index: String },
    #[error("index {table}.{index} references unknown field {field}")]
    TableIndexFieldNotFound {
        table: String,
        index: String,
        field: String,
    },
    #[error("schema version {version} for table {table} omits primary-key column {column}")]
    SchemaVersionMissingPrimaryKey {
        table: String,
        version: u64,
        column: String,
    },
    #[error("schema-variant table uses foreign keys, which are not supported yet: {0}")]
    UnsupportedSchemaVariantTableFeature(String),
    #[error("record descriptor does not match schema version {version} for table {table}")]
    SchemaVersionDescriptorMismatch { table: String, version: u64 },
    #[error("schema version 0 is reserved for Groove's implicit table layout: {0}")]
    ReservedTableVariant(String),
    #[error("cannot add the first explicit schema version to a live homogeneous table: {0}")]
    CannotPromoteLiveTableToSchemaVariants(String),
    #[error("unknown schema version {version} for table {table}")]
    UnknownTableVariant { table: String, version: u64 },
    #[error("unknown query parameter binding: {0}")]
    UnknownParameter(String),
}

impl From<crate::storage::Error> for Error {
    fn from(error: crate::storage::Error) -> Self {
        Self::Storage(Box::new(error))
    }
}

#[cfg(test)]
mod tests;
