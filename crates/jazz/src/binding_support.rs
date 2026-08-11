//! Binding-neutral helpers for encoded client-runtime boundaries.
//!
//! JavaScript runtimes use the same postcard and JSON payloads even though
//! their FFI frameworks differ. Keeping those conversions here prevents the
//! native bindings from developing subtly incompatible wire formats.

use std::cell::RefCell;
use std::collections::{BTreeMap, VecDeque};
use std::num::NonZeroUsize;
use std::rc::Rc;

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use thiserror::Error;

use crate::db::{
    Db, DbConfig, DbIdentity, Error as DbError, ErrorCode, InitialSyncFlushCadence, LocalUpdates,
    Propagation, ReadOpts, RowCells, SeededRowIdSource, SubscriptionEvent, WriteState, block_on,
};
use crate::groove::records::{BorrowedRecord, RecordDescriptor, Value};
use crate::groove::storage::{OrderedKvStorage, ReopenableStorage};
use crate::ids::{AuthorId, NodeUuid, RowUuid};
use crate::node::{CurrentRow, RelationEdge, RelationSnapshot};
use crate::query::{Query, RelationExpr, RelationQuery};
use crate::schema::JazzSchema;
use crate::tx::{DurabilityTier, Fate, TxId};
use crate::wire::{TransportError, WireTransport};

/// The frame queues shared between a binding's transport handle and the
/// [`WireTransport`] the core drives.
///
/// Both sides are cloned handles onto the same queues: the binding pushes
/// inbound frames and drains outbound ones, while [`QueueWireTransport`] does
/// the mirror image from inside the core.
#[derive(Clone, Default)]
pub struct WireQueues {
    inbound: Rc<RefCell<VecDeque<Vec<u8>>>>,
    outbound: Rc<RefCell<VecDeque<Vec<u8>>>>,
}

impl WireQueues {
    /// Enqueue frames received from the peer for the core to consume.
    pub fn push_inbound(&self, frames: impl IntoIterator<Item = Vec<u8>>) {
        self.inbound.borrow_mut().extend(frames);
    }

    /// Take every frame the core has produced for the peer.
    pub fn drain_outbound(&self) -> Vec<Vec<u8>> {
        self.outbound.borrow_mut().drain(..).collect()
    }

    /// Build the core-side transport for these queues.
    pub fn transport(&self) -> QueueWireTransport {
        QueueWireTransport {
            queues: self.clone(),
        }
    }
}

/// Core-side [`WireTransport`] backed by a [`WireQueues`] pair.
pub struct QueueWireTransport {
    queues: WireQueues,
}

impl WireTransport for QueueWireTransport {
    fn send_frame(&mut self, frame: Vec<u8>) -> Result<(), TransportError> {
        self.queues.outbound.borrow_mut().push_back(frame);
        Ok(())
    }

    fn try_recv_frame(&mut self) -> Option<Vec<u8>> {
        self.queues.inbound.borrow_mut().pop_front()
    }
}

/// A binding-neutral conversion or wait-state failure.
#[derive(Debug, Error)]
pub enum BindingError {
    /// The foreign payload could not be decoded or validated.
    #[error("{0}")]
    InvalidPayload(String),
    /// The Jazz database rejected the operation.
    #[error(transparent)]
    Core(#[from] DbError),
    /// A response could not be encoded.
    #[error("{0}")]
    Encode(String),
    /// A write has not reached the requested state.
    #[error("{code:?}: {detail}")]
    WaitState {
        /// Stable core error marker consumed by runtime adapters.
        code: ErrorCode,
        /// Human-readable state detail.
        detail: String,
    },
}

/// Binding-neutral subscription event with postcard payloads kept as bytes.
///
/// Object-oriented FFIs can carry this shape without expanding binary deltas
/// into JSON integer arrays. Adapters that expose JSON can convert it at their
/// outer boundary with [`subscription_event_to_json`].
#[derive(Clone, Debug, PartialEq)]
pub enum EncodedSubscriptionEvent {
    /// Incremental or reset result change.
    Delta {
        /// Whether this delta replaces all previously observed state.
        reset: bool,
        /// Postcard-encoded row delta.
        delta: Vec<u8>,
        /// Postcard-encoded relation delta.
        relation_delta: Vec<u8>,
        /// Whether the requested read tier is settled.
        settled: bool,
        /// Debug-stable durability-tier spelling used by JavaScript adapters.
        tier: String,
    },
    /// The serving peer rejected the subscription.
    Rejected {
        /// Small structured rejection payload retained as JSON metadata.
        reason: JsonValue,
    },
    /// The producer closed the stream.
    Closed,
}

/// Encoded database identity shared by the JavaScript runtimes.
#[derive(Clone, Copy, Debug, Deserialize, Serialize)]
pub struct OpenDbIdentity {
    /// Stable node identity. Persistent callers must reuse it on reopen.
    pub node: NodeUuid,
    /// Default author identity.
    pub author: AuthorId,
}

impl From<OpenDbIdentity> for DbIdentity {
    fn from(identity: OpenDbIdentity) -> Self {
        Self {
            node: identity.node,
            author: identity.author,
        }
    }
}

/// Encoded database-open configuration shared by the JavaScript runtimes.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct OpenDbConfig {
    /// Database identity.
    pub identity: OpenDbIdentity,
    /// Optional deterministic row-id source used by tests.
    pub row_id_seed: Option<u64>,
    /// Whether this database can answer complete-history reads.
    pub history_complete: bool,
    /// Optional initial-sync durable-flush cadence.
    pub initial_sync_flush_every: Option<u32>,
}

/// Decode the postcard schema and database-open configuration.
pub fn decode_open_args(
    schema: &[u8],
    config: &[u8],
) -> Result<(JazzSchema, OpenDbConfig), BindingError> {
    let schema = postcard::from_bytes(schema)
        .map_err(|error| BindingError::InvalidPayload(format!("decode schema: {error}")))?;
    let config = postcard::from_bytes(config)
        .map_err(|error| BindingError::InvalidPayload(format!("decode open config: {error}")))?;
    Ok((schema, config))
}

/// Open a Jazz database over a binding-selected storage backend.
pub fn open_db<S>(schema: JazzSchema, storage: S, config: OpenDbConfig) -> Result<Db<S>, DbError>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    let mut db_config = DbConfig::new(schema, storage, config.identity.into());
    if let Some(seed) = config.row_id_seed {
        db_config = db_config.with_id_source(SeededRowIdSource::new(seed));
    }
    let initial_sync_flush_every = config.initial_sync_flush_every;
    let db = if config.history_complete {
        block_on(Db::open_history_complete(db_config))?
    } else {
        block_on(Db::open(db_config))?
    };
    if let Some(every) =
        initial_sync_flush_every.and_then(|value| NonZeroUsize::new(value as usize))
    {
        db.set_initial_sync_flush_cadence(InitialSyncFlushCadence::every(every))?;
    }
    Ok(db)
}

/// Decode a postcard query payload.
pub fn decode_query(bytes: &[u8]) -> Result<Query, BindingError> {
    postcard::from_bytes(bytes)
        .map_err(|error| BindingError::InvalidPayload(format!("decode query: {error}")))
}

/// Decode the named-field record used for inserts and patches.
pub fn decode_cells(bytes: &[u8]) -> Result<RowCells, BindingError> {
    let (descriptor, raw): (RecordDescriptor, Vec<u8>) = postcard::from_bytes(bytes)
        .map_err(|error| BindingError::InvalidPayload(format!("decode cells: {error}")))?;
    let record = BorrowedRecord::new(&raw, &descriptor);
    let values = record
        .to_values()
        .map_err(|error| BindingError::InvalidPayload(format!("decode cell record: {error}")))?;
    let mut cells = RowCells::new();
    for (field, value) in descriptor.fields().iter().zip(values) {
        let Some(name) = &field.name else {
            return Err(BindingError::InvalidPayload(
                "encoded cells must use named fields".to_owned(),
            ));
        };
        cells.insert(name.clone(), value);
    }
    Ok(cells)
}

/// Decode a 16-byte row identifier.
pub fn row_uuid_from_bytes(bytes: &[u8]) -> Result<RowUuid, BindingError> {
    let bytes: [u8; 16] = bytes
        .try_into()
        .map_err(|_| BindingError::InvalidPayload("row id must be 16 bytes".to_owned()))?;
    Ok(RowUuid::from_bytes(bytes))
}

/// Decode a 16-byte author identifier.
pub fn author_id_from_bytes(bytes: &[u8]) -> Result<AuthorId, BindingError> {
    let bytes: [u8; 16] = bytes
        .try_into()
        .map_err(|_| BindingError::InvalidPayload("author id must be 16 bytes".to_owned()))?;
    Ok(AuthorId::from_bytes(bytes))
}

/// Decode identity claims from their JSON boundary and add subject aliases.
pub fn claims_from_json(
    author: AuthorId,
    claims: Option<JsonValue>,
) -> Result<BTreeMap<String, Value>, BindingError> {
    let mut claims = match claims {
        None | Some(JsonValue::Null) => BTreeMap::new(),
        Some(JsonValue::Object(map)) => map
            .into_iter()
            .map(|(key, value)| claim_value_from_json(value).map(|value| (key, value)))
            .collect::<Result<BTreeMap<_, _>, _>>()?,
        Some(_) => {
            return Err(BindingError::InvalidPayload(
                "identity claims must be an object".to_owned(),
            ));
        }
    };
    let subject = author.0.to_string();
    claims
        .entry("subject".to_owned())
        .or_insert_with(|| Value::String(subject.clone()));
    claims
        .entry("sub".to_owned())
        .or_insert_with(|| Value::String(subject.clone()));
    claims
        .entry("user_id".to_owned())
        .or_insert_with(|| Value::String(subject));
    Ok(claims)
}

fn claim_value_from_json(value: JsonValue) -> Result<Value, BindingError> {
    Ok(match value {
        JsonValue::Null => Value::Nullable(None),
        JsonValue::Bool(value) => Value::Bool(value),
        JsonValue::Number(value) => {
            if let Some(value) = value.as_u64() {
                Value::U64(value)
            } else if let Some(value) = value.as_f64() {
                Value::F64(value)
            } else {
                return Err(BindingError::InvalidPayload(
                    "unsupported numeric claim value".to_owned(),
                ));
            }
        }
        JsonValue::String(value) => Value::String(value),
        JsonValue::Array(values) => Value::Array(
            values
                .into_iter()
                .map(claim_value_from_json)
                .collect::<Result<Vec<_>, _>>()?,
        ),
        JsonValue::Object(_) => {
            return Err(BindingError::InvalidPayload(
                "nested object claims are not supported".to_owned(),
            ));
        }
    })
}

/// Decode JSON read options used by all native runtimes.
pub fn read_opts_from_json(value: Option<JsonValue>) -> Result<ReadOpts, BindingError> {
    let mut opts = ReadOpts::default();
    let Some(value) = value else {
        return Ok(opts);
    };
    if value.is_null() {
        return Ok(opts);
    }
    if let Some(tier) = optional_json_string_prop(&value, "tier")? {
        opts.tier = durability_tier_from_str(&tier)?;
    }
    if let Some(local_updates) = optional_json_string_prop(&value, "local_updates")? {
        opts.local_updates = match local_updates.as_str() {
            "Immediate" | "immediate" => LocalUpdates::Immediate,
            "Deferred" | "deferred" => LocalUpdates::Deferred,
            other => {
                return Err(BindingError::InvalidPayload(format!(
                    "unknown local_updates {other}"
                )));
            }
        };
    }
    if optional_json_bool_prop(&value, "propagate")? == Some(false) {
        opts.propagation = Propagation::LocalOnly;
    }
    if let Some(propagation) = optional_json_string_prop(&value, "propagation")? {
        opts.propagation = match propagation.as_str() {
            "Full" | "full" => Propagation::Full,
            "LocalOnly" | "local_only" | "localOnly" | "local-only" => Propagation::LocalOnly,
            other => {
                return Err(BindingError::InvalidPayload(format!(
                    "unknown propagation {other}"
                )));
            }
        };
    }
    if let Some(include_deleted) = optional_json_bool_prop(&value, "include_deleted")? {
        opts.include_deleted = include_deleted;
    }
    if value
        .get("read_view")
        .or_else(|| value.get("readView"))
        .filter(|read_view| !read_view.is_null())
        .is_some()
    {
        return Err(BindingError::InvalidPayload(
            "non-default read_view is not supported yet".to_owned(),
        ));
    }
    Ok(opts)
}

/// Decode a JSON string into read options.
pub fn read_opts_from_json_str(value: Option<&str>) -> Result<ReadOpts, BindingError> {
    let value = value
        .map(|value| {
            serde_json::from_str(value).map_err(|error| {
                BindingError::InvalidPayload(format!("decode read options json: {error}"))
            })
        })
        .transpose()?;
    read_opts_from_json(value)
}

/// Parse a durability-tier name.
pub fn durability_tier_from_str(tier: &str) -> Result<DurabilityTier, BindingError> {
    match tier {
        "None" | "none" => Ok(DurabilityTier::None),
        "Local" | "local" => Ok(DurabilityTier::Local),
        "Edge" | "edge" => Ok(DurabilityTier::Edge),
        "Global" | "global" => Ok(DurabilityTier::Global),
        other => Err(BindingError::InvalidPayload(format!(
            "unknown durability tier {other}"
        ))),
    }
}

fn optional_json_string_prop(
    value: &JsonValue,
    name: &str,
) -> Result<Option<String>, BindingError> {
    match value.get(name) {
        Some(JsonValue::String(value)) => Ok(Some(value.clone())),
        Some(JsonValue::Null) | None => Ok(None),
        Some(_) => Err(BindingError::InvalidPayload(format!(
            "{name} must be a string"
        ))),
    }
}

fn optional_json_bool_prop(value: &JsonValue, name: &str) -> Result<Option<bool>, BindingError> {
    match value.get(name) {
        Some(JsonValue::Bool(value)) => Ok(Some(*value)),
        Some(JsonValue::Null) | None => Ok(None),
        Some(_) => Err(BindingError::InvalidPayload(format!(
            "{name} must be a boolean"
        ))),
    }
}

/// Parse a relation query from the adapter's JSON envelope.
pub fn relation_query_from_json(query_json: &str) -> Result<RelationQuery, BindingError> {
    let value: JsonValue = serde_json::from_str(query_json)
        .map_err(|error| BindingError::InvalidPayload(format!("decode query json: {error}")))?;
    let relation_ir = value
        .get("relation_ir")
        .ok_or_else(|| {
            BindingError::InvalidPayload("relation query json is missing relation_ir".to_owned())
        })?
        .clone();
    let rel: RelationExpr = serde_json::from_value(relation_ir)
        .map_err(|error| BindingError::InvalidPayload(format!("decode relation_ir: {error}")))?;
    Ok(RelationQuery { rel })
}

/// Check whether a write has reached a requested durability tier.
fn check_write_state(state: &WriteState, tier: DurabilityTier) -> Result<(), BindingError> {
    if tier <= DurabilityTier::Local {
        return Ok(());
    }
    match &state.fate {
        Fate::Rejected(reason) => {
            return Err(BindingError::WaitState {
                code: ErrorCode::WriteRejected,
                detail: format!("transaction was rejected: {reason:?}"),
            });
        }
        Fate::Pending if tier >= DurabilityTier::Edge => {
            return Err(BindingError::WaitState {
                code: ErrorCode::NotObserved,
                detail: format!("transaction has not been accepted at requested tier {tier:?}"),
            });
        }
        Fate::Pending | Fate::Accepted => {}
    }
    if state.durability >= tier {
        return Ok(());
    }
    Err(BindingError::WaitState {
        code: ErrorCode::NotObserved,
        detail: format!("transaction has not reached requested tier {tier:?}"),
    })
}

/// Read and validate a transaction's current write state.
pub fn wait_for_tx<S>(db: &Db<S>, tx_id: TxId, tier: DurabilityTier) -> Result<(), BindingError>
where
    S: OrderedKvStorage + ReopenableStorage + 'static,
{
    if tier <= DurabilityTier::Local {
        return Ok(());
    }
    let state = db.write_state(tx_id)?;
    check_write_state(&state, tier)
}

/// Serialize a write state as JSON.
pub fn write_state_to_json(state: &WriteState) -> JsonValue {
    serde_json::to_value(state).unwrap_or_else(|_| serde_json::json!({}))
}

/// Serialize the write payload consumed by the TypeScript runtime.
pub fn encode_write_result(row_id: RowUuid, tx_id: TxId) -> Result<Vec<u8>, BindingError> {
    postcard::to_allocvec(&WriteResult { row_id, tx_id })
        .map_err(|error| BindingError::Encode(format!("encode write result: {error}")))
}

/// Serialize rows in the runtime's descriptor-grouped postcard format.
pub fn encode_rows(rows: &[CurrentRow]) -> Result<Vec<u8>, BindingError> {
    postcard::to_allocvec(&row_batches(rows))
        .map_err(|error| BindingError::Encode(format!("encode rows: {error}")))
}

/// Serialize a relation snapshot in the runtime postcard format.
pub fn encode_relation_snapshot(snapshot: &RelationSnapshot) -> Result<Vec<u8>, BindingError> {
    postcard::to_allocvec(&BindingRelationSnapshot {
        cursor: 0,
        root_count: snapshot.root_count as u64,
        rows: row_batches(&snapshot.rows),
        edges: snapshot.edges.iter().map(relation_edge).collect(),
    })
    .map_err(|error| BindingError::Encode(format!("encode relation snapshot: {error}")))
}

/// Encode a subscription event while retaining binary deltas as byte buffers.
pub fn encode_subscription_event(
    event: &SubscriptionEvent,
) -> Result<EncodedSubscriptionEvent, BindingError> {
    match event {
        SubscriptionEvent::Delta {
            reset,
            added,
            updated,
            removed,
            added_related,
            added_edges,
            removed_edges,
            settled,
            tier,
        } => {
            let added = added
                .iter()
                .map(|output| output.row.clone())
                .collect::<Vec<_>>();
            let updated = updated
                .iter()
                .map(|output| output.row.clone())
                .collect::<Vec<_>>();
            let delta = encode_subscription_delta(&added, &updated, removed)?;
            let relation_delta = encode_relation_subscription_delta(
                &added,
                &updated,
                removed,
                added_related,
                added_edges,
                removed_edges,
            )?;
            Ok(EncodedSubscriptionEvent::Delta {
                reset: *reset,
                delta,
                relation_delta,
                settled: *settled,
                tier: format!("{tier:?}"),
            })
        }
        SubscriptionEvent::Rejected { reason } => {
            let reason = match reason {
                crate::protocol::SubscribeRejectReason::UnsupportedShapeCapability { detail } => {
                    serde_json::json!({
                        "type": "UnsupportedShapeCapability",
                        "detail": detail,
                    })
                }
                crate::protocol::SubscribeRejectReason::ShapeRegistrationPendingCatalogueAdmission => {
                    serde_json::json!({
                        "type": "ShapeRegistrationPendingCatalogueAdmission",
                    })
                }
                crate::protocol::SubscribeRejectReason::ServerFailure { code } => {
                    serde_json::json!({
                        "type": "ServerFailure",
                        "code": format!("{code:?}"),
                    })
                }
            };
            Ok(EncodedSubscriptionEvent::Rejected { reason })
        }
        SubscriptionEvent::Closed => Ok(EncodedSubscriptionEvent::Closed),
    }
}

/// Convert a subscription event to the JSON object shared by JSON adapters.
pub fn subscription_event_to_json(event: &SubscriptionEvent) -> Result<JsonValue, BindingError> {
    match encode_subscription_event(event)? {
        EncodedSubscriptionEvent::Delta {
            reset,
            delta,
            relation_delta,
            settled,
            tier,
        } => Ok(serde_json::json!({
            "type": "delta",
            "reset": reset,
            "delta": delta,
            "relation_delta": relation_delta,
            "settled": settled,
            "tier": tier,
        })),
        EncodedSubscriptionEvent::Rejected { reason } => {
            Ok(serde_json::json!({ "type": "rejected", "reason": reason }))
        }
        EncodedSubscriptionEvent::Closed => Ok(serde_json::json!({ "type": "closed" })),
    }
}

fn encode_subscription_delta(
    added: &[CurrentRow],
    updated: &[CurrentRow],
    removed: &[crate::db::RemovedRow],
) -> Result<Vec<u8>, BindingError> {
    postcard::to_allocvec(&BindingSubscriptionDelta {
        added: row_batches(added),
        updated: row_batches(updated),
        removed: removed
            .iter()
            .map(|row| BindingRemovedRow {
                table: row.table.clone(),
                row_id: row.row_uuid,
            })
            .collect(),
    })
    .map_err(|error| BindingError::Encode(format!("encode subscription delta: {error}")))
}

fn encode_relation_subscription_delta(
    added: &[CurrentRow],
    updated: &[CurrentRow],
    removed: &[crate::db::RemovedRow],
    added_related: &[CurrentRow],
    added_edges: &[RelationEdge],
    removed_edges: &[crate::db::RemovedRelationEdge],
) -> Result<Vec<u8>, BindingError> {
    let mut relation_added = Vec::with_capacity(added.len() + added_related.len());
    relation_added.extend_from_slice(added);
    relation_added.extend_from_slice(added_related);
    postcard::to_allocvec(&BindingRelationSubscriptionDelta {
        base_cursor: None,
        cursor: 0,
        added: row_batches(&relation_added),
        updated: row_batches(updated),
        removed: removed
            .iter()
            .map(|row| BindingRemovedRow {
                table: row.table.clone(),
                row_id: row.row_uuid,
            })
            .collect(),
        added_edges: added_edges.iter().map(relation_edge).collect(),
        removed_edges: removed_edges.iter().map(relation_edge).collect(),
    })
    .map_err(|error| BindingError::Encode(format!("encode relation subscription delta: {error}")))
}

fn row_batches(rows: &[CurrentRow]) -> Vec<BindingRowBatch<'_>> {
    let mut batches: Vec<BindingRowBatch<'_>> = Vec::new();
    for row in rows {
        let (descriptor, raw) = row.encoded_record();
        match batches.last_mut() {
            Some(batch) if batch.table == row.table() && batch.descriptor == *descriptor => {
                batch.rows.push(binding_row(row, raw));
            }
            _ => batches.push(BindingRowBatch {
                table: row.table(),
                descriptor: *descriptor,
                rows: vec![binding_row(row, raw)],
            }),
        }
    }
    batches
}

fn binding_row<'a>(row: &CurrentRow, raw: &'a [u8]) -> BindingRow<'a> {
    BindingRow {
        row_id: row.row_uuid(),
        deleted: row.is_deleted(),
        raw,
    }
}

fn relation_edge(edge: &RelationEdge) -> BindingRelationEdge {
    BindingRelationEdge {
        source_table: edge.source_table.clone(),
        source_row_id: edge.source_row,
        relation: edge.relation.clone(),
        target_table: edge.target_table.clone(),
        target_row_id: edge.target_row,
    }
}

#[derive(Serialize)]
struct WriteResult {
    row_id: RowUuid,
    tx_id: TxId,
}

#[derive(Serialize)]
struct BindingRowBatch<'a> {
    table: &'a str,
    descriptor: RecordDescriptor,
    rows: Vec<BindingRow<'a>>,
}

#[derive(Serialize)]
struct BindingRow<'a> {
    row_id: RowUuid,
    deleted: bool,
    raw: &'a [u8],
}

#[derive(Serialize)]
struct BindingRelationSnapshot<'a> {
    cursor: u64,
    root_count: u64,
    rows: Vec<BindingRowBatch<'a>>,
    edges: Vec<BindingRelationEdge>,
}

#[derive(Serialize)]
struct BindingSubscriptionDelta<'a> {
    added: Vec<BindingRowBatch<'a>>,
    updated: Vec<BindingRowBatch<'a>>,
    removed: Vec<BindingRemovedRow>,
}

#[derive(Serialize)]
struct BindingRelationSubscriptionDelta<'a> {
    base_cursor: Option<u64>,
    cursor: u64,
    added: Vec<BindingRowBatch<'a>>,
    updated: Vec<BindingRowBatch<'a>>,
    removed: Vec<BindingRemovedRow>,
    added_edges: Vec<BindingRelationEdge>,
    removed_edges: Vec<BindingRelationEdge>,
}

#[derive(Serialize)]
struct BindingRemovedRow {
    table: String,
    row_id: RowUuid,
}

#[derive(Serialize)]
struct BindingRelationEdge {
    source_table: String,
    source_row_id: RowUuid,
    relation: String,
    target_table: String,
    target_row_id: RowUuid,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tx::RejectionReason;

    #[test]
    fn wait_state_errors_keep_adapter_markers() {
        // This is the public binding contract: adapters classify wait errors
        // by these stable core-code markers rather than crate-specific prose.
        let rejected = WriteState {
            fate: Fate::Rejected(RejectionReason::AuthorizationDenied),
            durability: DurabilityTier::Local,
        };
        assert_eq!(
            check_write_state(&rejected, DurabilityTier::Edge)
                .unwrap_err()
                .to_string(),
            "WriteRejected: transaction was rejected: AuthorizationDenied"
        );

        let pending = WriteState {
            fate: Fate::Pending,
            durability: DurabilityTier::Local,
        };
        assert!(
            check_write_state(&pending, DurabilityTier::Edge)
                .unwrap_err()
                .to_string()
                .starts_with("NotObserved:")
        );
    }
}
