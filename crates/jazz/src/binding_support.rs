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
    Propagation, ReadOpts, RowCells, SeededRowIdSource, SubscriptionEvent, SubscriptionOutputRow,
    WriteState, block_on,
};
use crate::groove::ivm::TerminalOperation;
use crate::groove::records::{BorrowedRecord, RecordDescriptor, Value};
use crate::groove::storage::{OrderedKvStorage, ReopenableStorage};
use crate::ids::{AuthorId, NodeUuid, RowUuid};
use crate::node::{CurrentRow, RelationSnapshot};
use crate::query::{Query, RelationExpr, RelationQuery};
use crate::schema::JazzSchema;
use crate::tools::{OutputOccurrenceId, ResultKey};
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
        /// Postcard-encoded row delta. Empty of rows when
        /// `terminal_operations` carries the change instead.
        delta: Vec<u8>,
        /// Typed structural edits to already hydrated terminal rows.
        terminal_operations: Vec<TerminalOperation>,
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
    // Two fields, in this order — `readNativeRelationSubscriptionSnapshot`
    // reads `root_count` then `rows` positionally. The pre-swap cursor/edges
    // fields are gone from the carrier; emitting them would decode as
    // garbage row counts on the client, not fail.
    postcard::to_allocvec(&BindingRelationSnapshot {
        root_count: snapshot.root_count as u64,
        rows: row_batches(&snapshot.rows),
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
            terminal_operations,
            settled,
            tier,
        } => {
            // The row delta and the terminal operations are alternatives, not
            // companions: when the core sends typed patches, the receiver
            // applies those instead of whole rows. Sending both would make the
            // client apply every change twice. `jazz-napi` and `jazz-wasm`
            // gate the same way.
            let carries_rows = terminal_operations.is_empty();
            let empty_removed: Vec<crate::db::RemovedRow> = Vec::new();
            let delta = if carries_rows {
                encode_subscription_delta(added, updated, removed)?
            } else {
                encode_subscription_delta(&[], &[], &empty_removed)?
            };
            Ok(EncodedSubscriptionEvent::Delta {
                reset: *reset,
                delta,
                terminal_operations: terminal_operations.clone(),
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
            terminal_operations,
            settled,
            tier,
        } => Ok(serde_json::json!({
            "type": "delta",
            "reset": reset,
            "delta": delta,
            "terminalOperations": terminal_operations,
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
    added: &[SubscriptionOutputRow],
    updated: &[SubscriptionOutputRow],
    removed: &[crate::db::RemovedRow],
) -> Result<Vec<u8>, BindingError> {
    // Field order is the wire contract: `readNativeSubscriptionDelta` in
    // native-row-codec.ts reads these six positionally and then asserts each
    // occurrence-key vector matches its row count. Reordering or omitting a
    // vector is not a compile error here — it surfaces as a decode failure on
    // the client.
    let added_rows = added.iter().map(|row| row.row.clone()).collect::<Vec<_>>();
    let updated_rows = updated
        .iter()
        .map(|row| row.row.clone())
        .collect::<Vec<_>>();
    postcard::to_allocvec(&BindingSubscriptionDelta {
        added: row_batches(&added_rows),
        updated: row_batches(&updated_rows),
        removed: removed
            .iter()
            .map(|row| BindingRemovedRow {
                table: row.table.clone(),
                row_id: row.row_uuid,
            })
            .collect(),
        added_occurrence_keys: occurrence_keys(added.iter().map(|row| &row.occurrence_id)),
        updated_occurrence_keys: occurrence_keys(updated.iter().map(|row| &row.occurrence_id)),
        removed_occurrence_keys: occurrence_keys(removed.iter().map(|row| &row.occurrence_id)),
    })
    .map_err(|error| BindingError::Encode(format!("encode subscription delta: {error}")))
}

fn occurrence_keys<'a>(
    occurrences: impl Iterator<Item = &'a OutputOccurrenceId>,
) -> Vec<ResultKey> {
    occurrences
        .map(|occurrence| ResultKey::from_occurrence(occurrence.clone()))
        .collect()
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
        // Transaction-overlay queries carry include-deleted state as the
        // query-engine marker, while ordinary current-row reads set the
        // `CurrentRow` flag. Normalize both representations at the binding
        // boundary so ReadOpts::include_deleted has one native contract.
        deleted: row.is_deleted()
            || matches!(row.raw_field("__jazz_deleted"), Some(Value::Bool(true))),
        raw,
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
    root_count: u64,
    rows: Vec<BindingRowBatch<'a>>,
}

#[derive(Serialize)]
struct BindingSubscriptionDelta<'a> {
    added: Vec<BindingRowBatch<'a>>,
    updated: Vec<BindingRowBatch<'a>>,
    removed: Vec<BindingRemovedRow>,
    added_occurrence_keys: Vec<ResultKey>,
    updated_occurrence_keys: Vec<ResultKey>,
    removed_occurrence_keys: Vec<ResultKey>,
}

#[derive(Serialize)]
struct BindingRemovedRow {
    table: String,
    row_id: RowUuid,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::RemovedRow;
    use crate::groove::ivm::{TerminalEdit, TerminalOperation};
    use crate::node::{RelationEdge, RelationSnapshot};
    use crate::tools::ObjectId;
    use crate::tx::RejectionReason;

    type DecodedRows = Vec<(String, RecordDescriptor, Vec<(RowUuid, bool, Vec<u8>)>)>;
    type DecodedRemoved = Vec<(String, RowUuid)>;

    fn occurrence(row: &CurrentRow) -> OutputOccurrenceId {
        OutputOccurrenceId::single_source(ObjectId::from_uuid(row.row_uuid().0))
    }

    fn test_row(row_byte: u8, title: &str) -> CurrentRow {
        let db = block_on(crate::db::doctest_support::open_todos_db()).expect("open test db");
        let row_id = RowUuid::from_bytes([row_byte; 16]);
        db.insert_with_id(
            "todos",
            row_id,
            crate::db::doctest_support::todo_cells(title, false),
        )
        .expect("insert test row");
        db.local_current_row("todos", row_id)
            .expect("read current row")
            .expect("current row exists")
    }

    fn decode_delta(
        bytes: &[u8],
    ) -> (
        DecodedRows,
        DecodedRows,
        DecodedRemoved,
        Vec<ResultKey>,
        Vec<ResultKey>,
        Vec<ResultKey>,
    ) {
        postcard::from_bytes(bytes).expect("decode subscription delta")
    }

    fn row_count(batches: &DecodedRows) -> usize {
        batches.iter().map(|(_, _, rows)| rows.len()).sum()
    }

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

    #[test]
    fn subscription_delta_carries_occurrence_sidecars_for_every_row() {
        let added_row = test_row(0x31, "added");
        let updated_row = test_row(0x32, "updated");
        let removed_row = test_row(0x33, "removed");
        let event = SubscriptionEvent::Delta {
            reset: false,
            added: vec![SubscriptionOutputRow {
                occurrence_id: occurrence(&added_row),
                row: added_row,
            }],
            updated: vec![SubscriptionOutputRow {
                occurrence_id: occurrence(&updated_row),
                row: updated_row,
            }],
            removed: vec![RemovedRow {
                table: removed_row.table().to_owned(),
                row_uuid: removed_row.row_uuid(),
                occurrence_id: occurrence(&removed_row),
            }],
            terminal_operations: Vec::new(),
            settled: true,
            tier: DurabilityTier::Local,
        };

        let EncodedSubscriptionEvent::Delta { delta, .. } =
            encode_subscription_event(&event).expect("encode delta")
        else {
            panic!("expected delta event");
        };
        let (added, updated, removed, added_keys, updated_keys, removed_keys) =
            decode_delta(&delta);
        assert_eq!(row_count(&added), 1);
        assert_eq!(row_count(&updated), 1);
        assert_eq!(removed.len(), 1);
        assert_eq!(added_keys.len(), row_count(&added));
        assert_eq!(updated_keys.len(), row_count(&updated));
        assert_eq!(removed_keys.len(), removed.len());
    }

    #[test]
    fn terminal_operations_replace_row_delta_and_use_the_public_json_key() {
        let row = test_row(0x41, "terminal");
        let operation = TerminalOperation {
            root_key: vec![1],
            path: Vec::new(),
            edit: TerminalEdit::Remove { key: vec![2] },
        };
        let event = SubscriptionEvent::Delta {
            reset: false,
            added: vec![SubscriptionOutputRow {
                occurrence_id: occurrence(&row),
                row,
            }],
            updated: Vec::new(),
            removed: Vec::new(),
            terminal_operations: vec![operation.clone()],
            settled: true,
            tier: DurabilityTier::Local,
        };

        let EncodedSubscriptionEvent::Delta {
            delta,
            terminal_operations,
            ..
        } = encode_subscription_event(&event).expect("encode terminal delta")
        else {
            panic!("expected delta event");
        };
        let (added, updated, removed, added_keys, updated_keys, removed_keys) =
            decode_delta(&delta);
        assert_eq!(row_count(&added), 0);
        assert_eq!(row_count(&updated), 0);
        assert!(removed.is_empty());
        assert!(added_keys.is_empty());
        assert!(updated_keys.is_empty());
        assert!(removed_keys.is_empty());
        assert_eq!(terminal_operations, [operation]);

        let json = subscription_event_to_json(&event).expect("encode subscription json");
        assert_eq!(json["terminalOperations"].as_array().map(Vec::len), Some(1));
        assert!(json.get("relation_delta").is_none());
    }

    #[test]
    fn relation_snapshot_is_exactly_root_count_then_rows() {
        let row = test_row(0x51, "snapshot");
        let row_id = row.row_uuid();
        let snapshot = RelationSnapshot {
            root_count: 1,
            rows: vec![row],
            // Edges remain a core result-tree concern, but are deliberately
            // absent from the native snapshot carrier.
            edges: vec![RelationEdge {
                source_table: "todos".to_owned(),
                source_row: row_id,
                relation: "children".to_owned(),
                target_table: "todos".to_owned(),
                target_row: row_id,
            }],
        };

        let bytes = encode_relation_snapshot(&snapshot).expect("encode relation snapshot");
        let (root_count, rows): (u64, DecodedRows) =
            postcard::from_bytes(&bytes).expect("decode exact two-field snapshot");
        assert_eq!(root_count, 1);
        assert_eq!(row_count(&rows), 1);
    }
}
