//! Shared helpers used by native bindings.
//!
//! These utilities keep wrapper crates thin by centralizing JSON parsing,
//! schema-alignment, and subscription payload shaping in the core crate.

use std::collections::HashMap;

use serde::Deserialize;
use serde_json::{Value as JsonValue, json};
use uuid::Uuid;

use crate::batch_fate::{BatchMode, BatchSettlement, LocalBatchRecord, VisibleBatchMember};
use crate::client_core::{ClientError, ClientRuntimeHost, JazzClientCore, WriteOptions};
use crate::object::{BranchName, ObjectId};
use crate::query_manager::manager::LocalUpdates;
use crate::query_manager::parse_query_json;
use crate::query_manager::query::Query;
use crate::query_manager::session::{Session, WriteContext};
use crate::query_manager::types::{RowDescriptor, RowPolicyMode, Schema, TableName, Value};
use crate::row_format::decode_row;
use crate::row_histories::BatchId;
use crate::runtime_core::{
    DirectInsertResult, QueryLocalOverlay, ReadDurabilityOptions, SubscriptionDelta,
};
use crate::schema_manager::{AppId, SchemaManager};
use crate::sync_manager::{DurabilityTier, QueryPropagation, SyncManager};

#[derive(Debug, Clone, Deserialize, Default)]
struct QueryExecutionOptionsWire {
    propagation: Option<String>,
    local_updates: Option<String>,
    transaction_overlay: Option<QueryTransactionOverlayWire>,
}

#[derive(Debug, Clone, Deserialize)]
struct QueryTransactionOverlayWire {
    batch_id: String,
    branch_name: String,
    row_ids: Vec<String>,
}

#[derive(Debug)]
pub struct QueryExecutionOptions {
    pub durability: ReadDurabilityOptions,
    pub propagation: QueryPropagation,
    pub transaction_overlay: Option<QueryLocalOverlay>,
}

#[derive(Debug)]
pub struct SubscriptionInput {
    pub query: Query,
    pub session: Option<Session>,
    pub durability: ReadDurabilityOptions,
    pub propagation: QueryPropagation,
    pub transaction_overlay: Option<QueryLocalOverlay>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RuntimeSchemaInput {
    pub schema: Schema,
    pub loaded_policy_bundle: bool,
    pub is_envelope: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlainSchemaPolicyMode {
    PermissiveLocal,
    InferFromSchema,
}

pub struct RuntimeSchemaBootstrapOptions<'a> {
    pub schema_json: &'a str,
    pub app_id: &'a str,
    pub env: &'a str,
    pub user_branch: &'a str,
    pub node_tier: Option<&'a str>,
    pub plain_schema_policy_mode: PlainSchemaPolicyMode,
}

pub struct RuntimeSchemaBootstrap {
    pub app_id: AppId,
    pub declared_schema: Schema,
    pub schema_manager: SchemaManager,
    pub default_durability_tier: Option<DurabilityTier>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RuntimeSchemaEnvelopeWire {
    #[serde(rename = "__jazzRuntimeSchema")]
    version: u8,
    schema: Schema,
    #[serde(default)]
    loaded_policy_bundle: bool,
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum RuntimeSchemaWire {
    Envelope(RuntimeSchemaEnvelopeWire),
    Schema(Schema),
}

pub fn query_rows_can_be_schema_aligned(query: &Query) -> bool {
    query.joins.is_empty()
        && query.array_subqueries.is_empty()
        && query.recursive.is_none()
        && query.select_columns.is_none()
        && query.result_element_index.is_none()
}

fn reorder_values_by_column_name(
    source_descriptor: &RowDescriptor,
    target_descriptor: &RowDescriptor,
    values: &[Value],
) -> Option<Vec<Value>> {
    if values.len() != source_descriptor.columns.len()
        || source_descriptor.columns.len() != target_descriptor.columns.len()
    {
        return None;
    }

    let mut values_by_column = HashMap::with_capacity(values.len());
    for (column, value) in source_descriptor.columns.iter().zip(values.iter()) {
        values_by_column.insert(column.name, value.clone());
    }

    let mut reordered_values = Vec::with_capacity(values.len());
    for column in &target_descriptor.columns {
        reordered_values.push(values_by_column.remove(&column.name)?);
    }

    Some(reordered_values)
}

pub fn align_values_to_declared_schema(
    declared_schema: &Schema,
    table: &TableName,
    source_descriptor: &RowDescriptor,
    values: Vec<Value>,
) -> Vec<Value> {
    let Some(declared_table) = declared_schema.get(table) else {
        return values;
    };

    reorder_values_by_column_name(source_descriptor, &declared_table.columns, &values)
        .unwrap_or(values)
}

pub fn align_row_values_to_declared_schema(
    declared_schema: &Schema,
    runtime_schema: &Schema,
    table: &TableName,
    values: Vec<Value>,
) -> Vec<Value> {
    let Some(runtime_table) = runtime_schema.get(table) else {
        return values;
    };

    align_values_to_declared_schema(declared_schema, table, &runtime_table.columns, values)
}

pub fn align_query_rows_to_declared_schema(
    declared_schema: &Schema,
    runtime_schema: &Schema,
    query: &Query,
    rows: Vec<(ObjectId, Vec<Value>)>,
) -> Vec<(ObjectId, Vec<Value>)> {
    if !query_rows_can_be_schema_aligned(query) {
        return rows;
    }

    let Some(declared_table) = declared_schema.get(&query.table) else {
        return rows;
    };
    let Some(runtime_table) = runtime_schema.get(&query.table) else {
        return rows;
    };

    rows.into_iter()
        .map(|(id, values)| {
            let values = reorder_values_by_column_name(
                &runtime_table.columns,
                &declared_table.columns,
                &values,
            )
            .unwrap_or(values);
            (id, values)
        })
        .collect()
}

pub fn serialize_query_rows_json(
    declared_schema: &Schema,
    runtime_schema: &Schema,
    query: &Query,
    rows: Vec<(ObjectId, Vec<Value>)>,
) -> JsonValue {
    let rows = align_query_rows_to_declared_schema(declared_schema, runtime_schema, query, rows);

    JsonValue::Array(
        rows.into_iter()
            .map(|(id, values)| {
                json!({
                    "id": id.uuid().to_string(),
                    "values": values,
                })
            })
            .collect(),
    )
}

pub fn parse_query_input(query_json: &str) -> Result<Query, String> {
    parse_query_json(query_json)
}

pub fn parse_runtime_schema_input(schema_json: &str) -> Result<RuntimeSchemaInput, String> {
    match serde_json::from_str::<RuntimeSchemaWire>(schema_json).map_err(|err| err.to_string())? {
        RuntimeSchemaWire::Envelope(envelope) => {
            if envelope.version != 1 {
                return Err(format!(
                    "unsupported runtime schema envelope version {}",
                    envelope.version
                ));
            }
            Ok(RuntimeSchemaInput {
                schema: envelope.schema,
                loaded_policy_bundle: envelope.loaded_policy_bundle,
                is_envelope: true,
            })
        }
        RuntimeSchemaWire::Schema(schema) => Ok(RuntimeSchemaInput {
            schema,
            loaded_policy_bundle: false,
            is_envelope: false,
        }),
    }
}

pub fn build_runtime_schema_bootstrap(
    options: RuntimeSchemaBootstrapOptions<'_>,
) -> Result<RuntimeSchemaBootstrap, String> {
    let runtime_schema = parse_runtime_schema_input(options.schema_json)?;
    let declared_schema = runtime_schema.schema.clone();
    let default_durability_tier = options.node_tier.map(parse_durability_tier).transpose()?;
    let sync_manager = match default_durability_tier {
        Some(tier) => SyncManager::new().with_durability_tier(tier),
        None => SyncManager::new(),
    };
    let app_id =
        AppId::from_string(options.app_id).unwrap_or_else(|_| AppId::from_name(options.app_id));

    let should_infer_plain_policy = !runtime_schema.is_envelope
        && matches!(
            options.plain_schema_policy_mode,
            PlainSchemaPolicyMode::InferFromSchema
        );

    let schema_manager = if should_infer_plain_policy {
        SchemaManager::new(
            sync_manager,
            runtime_schema.schema,
            app_id,
            options.env,
            options.user_branch,
        )
    } else {
        let row_policy_mode = if runtime_schema.loaded_policy_bundle {
            RowPolicyMode::Enforcing
        } else {
            RowPolicyMode::PermissiveLocal
        };
        SchemaManager::new_with_policy_mode(
            sync_manager,
            runtime_schema.schema,
            app_id,
            options.env,
            options.user_branch,
            row_policy_mode,
        )
    }
    .map_err(|error| format!("{error:?}"))?;

    Ok(RuntimeSchemaBootstrap {
        app_id,
        declared_schema,
        schema_manager,
        default_durability_tier,
    })
}

pub fn parse_session_input(session_json: Option<&str>) -> Result<Option<Session>, String> {
    match session_json {
        Some(json) => serde_json::from_str(json)
            .map(Some)
            .map_err(|err| err.to_string()),
        None => Ok(None),
    }
}

#[derive(Debug, Deserialize)]
#[serde(untagged)]
enum WriteContextWire {
    Session(Session),
    Context(WriteContextPayloadWire),
}

#[derive(Debug, Deserialize)]
struct WriteContextPayloadWire {
    #[serde(default)]
    session: Option<Session>,
    #[serde(default)]
    attribution: Option<String>,
    #[serde(default)]
    updated_at: Option<u64>,
    #[serde(default)]
    batch_mode: Option<String>,
    #[serde(default)]
    batch_id: Option<String>,
    #[serde(default)]
    target_branch_name: Option<String>,
}

impl TryFrom<WriteContextPayloadWire> for WriteContext {
    type Error = String;

    fn try_from(value: WriteContextPayloadWire) -> Result<Self, Self::Error> {
        let batch_mode = match value.batch_mode.as_deref() {
            None => None,
            Some("direct") | Some("Direct") => Some(BatchMode::Direct),
            Some("transactional") | Some("Transactional") => Some(BatchMode::Transactional),
            Some(other) => {
                return Err(format!(
                    "Invalid batch mode '{other}'. Must be 'direct' or 'transactional'."
                ));
            }
        };
        let batch_id = value
            .batch_id
            .as_deref()
            .map(parse_batch_id_input)
            .transpose()?;

        Ok(WriteContext {
            session: value.session,
            attribution: value.attribution,
            updated_at: value.updated_at,
            batch_mode,
            batch_id,
            target_branch_name: value.target_branch_name,
        })
    }
}

pub fn parse_write_context_input(
    write_context_json: Option<&str>,
) -> Result<Option<WriteContext>, String> {
    match write_context_json {
        Some(json) => match serde_json::from_str::<WriteContextWire>(json) {
            Ok(WriteContextWire::Session(session)) => Ok(Some(WriteContext::from_session(session))),
            Ok(WriteContextWire::Context(context)) => context.try_into().map(Some),
            Err(err) => Err(err.to_string()),
        },
        None => Ok(None),
    }
}

pub fn parse_durability_tier(tier: &str) -> Result<DurabilityTier, String> {
    match tier {
        "local" => Ok(DurabilityTier::Local),
        "edge" => Ok(DurabilityTier::EdgeServer),
        "global" => Ok(DurabilityTier::GlobalServer),
        _ => Err(format!(
            "Invalid tier '{}'. Must be 'local', 'edge', or 'global'.",
            tier
        )),
    }
}

pub fn parse_batch_id_input(batch_id: &str) -> Result<BatchId, String> {
    batch_id
        .parse()
        .map_err(|err: String| format!("Invalid BatchId: {err}"))
}

pub fn parse_batch_mode_input(mode: &str) -> Result<BatchMode, String> {
    match mode {
        "direct" => Ok(BatchMode::Direct),
        "transactional" => Ok(BatchMode::Transactional),
        _ => Err(format!(
            "Invalid batch mode '{}'. Must be 'direct' or 'transactional'.",
            mode
        )),
    }
}

pub fn serialize_durability_tier(tier: DurabilityTier) -> &'static str {
    match tier {
        DurabilityTier::Local => "local",
        DurabilityTier::EdgeServer => "edge",
        DurabilityTier::GlobalServer => "global",
    }
}

fn serialize_batch_mode(mode: BatchMode) -> &'static str {
    match mode {
        BatchMode::Direct => "direct",
        BatchMode::Transactional => "transactional",
    }
}

fn serialize_visible_batch_member(member: &VisibleBatchMember) -> JsonValue {
    json!({
        "objectId": member.object_id.uuid().to_string(),
        "branchName": member.branch_name.to_string(),
        "batchId": member.batch_id.to_string(),
    })
}

fn serialize_batch_settlement(settlement: &BatchSettlement) -> JsonValue {
    match settlement {
        BatchSettlement::Rejected {
            batch_id,
            code,
            reason,
        } => json!({
            "kind": "rejected",
            "batchId": batch_id.to_string(),
            "code": code,
            "reason": reason,
        }),
        BatchSettlement::DurableDirect {
            batch_id,
            confirmed_tier,
            visible_members,
        } => json!({
            "kind": "durableDirect",
            "batchId": batch_id.to_string(),
            "confirmedTier": serialize_durability_tier(*confirmed_tier),
            "visibleMembers": visible_members
                .iter()
                .map(serialize_visible_batch_member)
                .collect::<Vec<_>>(),
        }),
        BatchSettlement::AcceptedTransaction {
            batch_id,
            confirmed_tier,
            visible_members,
        } => json!({
            "kind": "acceptedTransaction",
            "batchId": batch_id.to_string(),
            "confirmedTier": serialize_durability_tier(*confirmed_tier),
            "visibleMembers": visible_members
                .iter()
                .map(serialize_visible_batch_member)
                .collect::<Vec<_>>(),
        }),
        BatchSettlement::Missing { batch_id } => json!({
            "kind": "missing",
            "batchId": batch_id.to_string(),
        }),
    }
}

pub fn serialize_local_batch_record(record: &LocalBatchRecord) -> JsonValue {
    json!({
        "batchId": record.batch_id.to_string(),
        "mode": serialize_batch_mode(record.mode),
        "sealed": record.sealed,
        "latestSettlement": record.latest_settlement.as_ref().map(serialize_batch_settlement),
    })
}

pub fn serialize_local_batch_records(records: &[LocalBatchRecord]) -> JsonValue {
    JsonValue::Array(records.iter().map(serialize_local_batch_record).collect())
}

pub fn serialize_write_handle(batch_id: BatchId) -> JsonValue {
    json!({
        "batchId": batch_id.to_string(),
    })
}

pub fn serialize_write_result(
    declared_schema: &Schema,
    runtime_schema: &Schema,
    table: &TableName,
    result: DirectInsertResult,
) -> JsonValue {
    let ((row_id, row_values), batch_id) = result;
    let values =
        align_row_values_to_declared_schema(declared_schema, runtime_schema, table, row_values);

    json!({
        "id": row_id.uuid().to_string(),
        "values": values,
        "batchId": batch_id.to_string(),
    })
}

pub fn binding_write_options(
    object_id: Option<ObjectId>,
    write_context: Option<WriteContext>,
) -> Option<WriteOptions> {
    if object_id.is_none() && write_context.is_none() {
        return None;
    }

    Some(WriteOptions {
        object_id,
        write_context,
        ..Default::default()
    })
}

pub fn record_to_updates(record: HashMap<String, Value>) -> Vec<(String, Value)> {
    record.into_iter().collect()
}

pub fn parse_object_id_input(object_id: Option<&str>) -> Result<ObjectId, String> {
    parse_external_object_id(object_id)?.ok_or_else(|| "Object id is required".to_string())
}

pub fn write_batch_context_json<H: ClientRuntimeHost>(
    client: &JazzClientCore<H>,
    mode: BatchMode,
) -> JsonValue {
    let context = client.begin_write_batch_context(mode);
    json!({
        "batchMode": serialize_batch_mode(context.mode()),
        "batchId": context.batch_id().to_string(),
        "targetBranchName": context.target_branch_name(),
    })
}

pub fn insert_sealed_json<H: ClientRuntimeHost>(
    client: &mut JazzClientCore<H>,
    declared_schema: &Schema,
    table: &str,
    values: HashMap<String, Value>,
    options: Option<WriteOptions>,
) -> Result<JsonValue, ClientError> {
    let result = client.insert(table, values, options)?;
    let current_schema = client.current_schema();
    Ok(serialize_write_result(
        declared_schema,
        &current_schema,
        &TableName::new(table),
        result,
    ))
}

pub fn update_sealed_json<H: ClientRuntimeHost>(
    client: &mut JazzClientCore<H>,
    object_id: ObjectId,
    updates: Vec<(String, Value)>,
    options: Option<WriteOptions>,
) -> Result<JsonValue, ClientError> {
    let handle = client.update(object_id, updates, options)?;
    Ok(serialize_write_handle(handle))
}

pub fn delete_sealed_json<H: ClientRuntimeHost>(
    client: &mut JazzClientCore<H>,
    object_id: ObjectId,
    options: Option<WriteOptions>,
) -> Result<JsonValue, ClientError> {
    let handle = client.delete(object_id, options)?;
    Ok(serialize_write_handle(handle))
}

pub fn insert_unsealed_json<H: ClientRuntimeHost>(
    client: &mut JazzClientCore<H>,
    declared_schema: &Schema,
    table: &str,
    values: HashMap<String, Value>,
    options: Option<WriteOptions>,
) -> Result<JsonValue, ClientError> {
    let result = client.insert_unsealed(table, values, options)?;
    let current_schema = client.current_schema();
    Ok(serialize_write_result(
        declared_schema,
        &current_schema,
        &TableName::new(table),
        result,
    ))
}

pub fn update_unsealed_json<H: ClientRuntimeHost>(
    client: &mut JazzClientCore<H>,
    object_id: ObjectId,
    updates: Vec<(String, Value)>,
    options: Option<WriteOptions>,
) -> Result<JsonValue, ClientError> {
    let handle = client.update_unsealed(object_id, updates, options)?;
    Ok(serialize_write_handle(handle))
}

pub fn delete_unsealed_json<H: ClientRuntimeHost>(
    client: &mut JazzClientCore<H>,
    object_id: ObjectId,
    options: Option<WriteOptions>,
) -> Result<JsonValue, ClientError> {
    let handle = client.delete_unsealed(object_id, options)?;
    Ok(serialize_write_handle(handle))
}

pub fn local_batch_record_json<H: ClientRuntimeHost>(
    client: &JazzClientCore<H>,
    batch_id: BatchId,
) -> Result<JsonValue, ClientError> {
    let record = client
        .with_runtime(|runtime| runtime.local_batch_record(batch_id))
        .map_err(|error| ClientError::new(error.to_string()))?;
    Ok(record
        .as_ref()
        .map(serialize_local_batch_record)
        .unwrap_or(JsonValue::Null))
}

pub fn local_batch_records_json<H: ClientRuntimeHost>(
    client: &JazzClientCore<H>,
) -> Result<JsonValue, ClientError> {
    let records = client
        .with_runtime(|runtime| runtime.local_batch_records())
        .map_err(|error| ClientError::new(error.to_string()))?;
    Ok(serialize_local_batch_records(&records))
}

pub fn drain_rejected_batch_id_strings<H: ClientRuntimeHost>(
    client: &mut JazzClientCore<H>,
) -> Vec<String> {
    client
        .with_runtime_mut(|runtime| runtime.drain_rejected_batch_ids())
        .into_iter()
        .map(|batch_id| batch_id.to_string())
        .collect()
}

pub fn acknowledge_rejected_batch_for_binding<H: ClientRuntimeHost>(
    client: &mut JazzClientCore<H>,
    batch_id: BatchId,
) -> Result<bool, ClientError> {
    client
        .with_runtime_mut(|runtime| runtime.acknowledge_rejected_batch(batch_id))
        .map_err(|error| ClientError::new(error.to_string()))
}

pub fn seal_batch_for_binding<H: ClientRuntimeHost>(
    client: &mut JazzClientCore<H>,
    batch_id: BatchId,
) -> Result<(), ClientError> {
    client.seal_batch(batch_id)
}

pub fn client_error_message(operation: &str, error: &ClientError) -> String {
    format!("{} failed: {}", operation, error.binding_message())
}

pub fn default_read_durability_options(tier: Option<DurabilityTier>) -> ReadDurabilityOptions {
    ReadDurabilityOptions {
        tier,
        local_updates: LocalUpdates::Immediate,
    }
}

pub fn parse_query_execution_options(
    tier: Option<&str>,
    options_json: Option<&str>,
) -> Result<QueryExecutionOptions, String> {
    let parsed_tier = tier.map(parse_durability_tier).transpose()?;
    let Some(raw) = options_json else {
        return Ok(QueryExecutionOptions {
            durability: default_read_durability_options(parsed_tier),
            propagation: QueryPropagation::Full,
            transaction_overlay: None,
        });
    };

    let options: QueryExecutionOptionsWire =
        serde_json::from_str(raw).map_err(|err| format!("Invalid query options JSON: {}", err))?;

    let propagation = match options.propagation.as_deref() {
        None | Some("full") => Ok(QueryPropagation::Full),
        Some("local-only") => Ok(QueryPropagation::LocalOnly),
        Some(other) => Err(format!(
            "Invalid propagation '{}'. Must be 'full' or 'local-only'.",
            other
        )),
    }?;

    let local_updates = match options.local_updates.as_deref() {
        None | Some("immediate") => Ok(LocalUpdates::Immediate),
        Some("deferred") => Ok(LocalUpdates::Deferred),
        Some(other) => Err(format!(
            "Invalid localUpdates '{}'. Must be 'immediate' or 'deferred'.",
            other
        )),
    }?;

    let transaction_overlay = match options.transaction_overlay {
        None => None,
        Some(overlay) => Some(QueryLocalOverlay {
            batch_id: parse_batch_id_input(&overlay.batch_id)
                .map_err(|err| format!("Invalid query batch id: {err}"))?,
            branch_name: BranchName::new(&overlay.branch_name),
            row_ids: overlay
                .row_ids
                .into_iter()
                .map(|row_id| {
                    parse_external_object_id(Some(&row_id))
                        .and_then(|maybe| maybe.ok_or_else(|| "missing query row id".to_string()))
                        .map_err(|err| format!("Invalid query row id: {err}"))
                })
                .collect::<Result<Vec<_>, _>>()?,
        }),
    };

    Ok(QueryExecutionOptions {
        durability: ReadDurabilityOptions {
            tier: parsed_tier,
            local_updates,
        },
        propagation,
        transaction_overlay,
    })
}

pub fn parse_read_durability_options(
    tier: Option<&str>,
    options_json: Option<&str>,
) -> Result<(ReadDurabilityOptions, QueryPropagation), String> {
    let options = parse_query_execution_options(tier, options_json)?;
    Ok((options.durability, options.propagation))
}

pub fn parse_subscription_input(
    query_json: &str,
    session_json: Option<&str>,
    tier: Option<&str>,
    options_json: Option<&str>,
) -> Result<SubscriptionInput, String> {
    let query = parse_query_input(query_json)?;
    let session = parse_session_input(session_json)?;
    let options = parse_query_execution_options(tier, options_json)?;

    Ok(SubscriptionInput {
        query,
        session,
        durability: options.durability,
        propagation: options.propagation,
        transaction_overlay: options.transaction_overlay,
    })
}

pub fn subscription_delta_to_json(
    delta: &SubscriptionDelta,
    declared_schema: Option<&Schema>,
    table: Option<&TableName>,
) -> serde_json::Value {
    let row_to_json = |row: &crate::query_manager::types::Row,
                       descriptor: &crate::query_manager::types::RowDescriptor|
     -> serde_json::Value {
        let values = decode_row(descriptor, &row.data)
            .map(|vals| vals.into_iter().collect::<Vec<_>>())
            .unwrap_or_default();
        let values = match (declared_schema, table) {
            (Some(schema), Some(table)) => {
                align_values_to_declared_schema(schema, table, descriptor, values)
            }
            _ => values,
        };
        serde_json::json!({
            "id": row.id.uuid().to_string(),
            "values": values,
        })
    };

    let descriptor = &delta.descriptor;
    let delta_obj = delta
        .ordered_delta
        .removed
        .iter()
        .map(|change| {
            serde_json::json!({
                "kind": 1,
                "id": change.id.uuid().to_string(),
                "index": change.index
            })
        })
        .chain(delta.ordered_delta.updated.iter().map(|change| {
            serde_json::json!({
                "kind": 2,
                "id": change.id.uuid().to_string(),
                "index": change.new_index,
                "row": change.row.as_ref().map(|row| row_to_json(row, descriptor))
            })
        }))
        .chain(delta.ordered_delta.added.iter().map(|change| {
            serde_json::json!({
                "kind": 0,
                "id": change.id.uuid().to_string(),
                "index": change.index,
                "row": row_to_json(&change.row, descriptor)
            })
        }))
        .collect::<Vec<_>>();

    serde_json::Value::Array(delta_obj)
}

pub fn generate_id() -> String {
    ObjectId::new().uuid().to_string()
}

pub fn parse_external_object_id(object_id: Option<&str>) -> Result<Option<ObjectId>, String> {
    let Some(object_id) = object_id else {
        return Ok(None);
    };

    let uuid = Uuid::parse_str(object_id).map_err(|err| format!("Invalid ObjectId: {err}"))?;
    Ok(Some(ObjectId::from_uuid(uuid)))
}

pub fn current_timestamp_ms() -> i64 {
    use std::time::{SystemTime, UNIX_EPOCH};

    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as i64
}

#[cfg(test)]
mod tests {
    use super::{
        PlainSchemaPolicyMode, RuntimeSchemaBootstrapOptions,
        acknowledge_rejected_batch_for_binding, align_query_rows_to_declared_schema,
        align_values_to_declared_schema, binding_write_options, build_runtime_schema_bootstrap,
        client_error_message, delete_sealed_json, delete_unsealed_json,
        drain_rejected_batch_id_strings, insert_sealed_json, insert_unsealed_json,
        local_batch_record_json, local_batch_records_json, parse_object_id_input,
        parse_query_execution_options, parse_read_durability_options, parse_runtime_schema_input,
        parse_subscription_input, parse_write_context_input, query_rows_can_be_schema_aligned,
        record_to_updates, seal_batch_for_binding, serialize_query_rows_json, update_sealed_json,
        update_unsealed_json, write_batch_context_json,
    };
    use crate::batch_fate::BatchMode;
    use crate::client_core::{ClientConfig, JazzClientCore, LocalRuntimeHost};
    use crate::object::ObjectId;
    use crate::query_manager::query::Query;
    use crate::query_manager::types::{
        ColumnDescriptor, ColumnType, RowDescriptor, Schema, SchemaBuilder, TableName, TableSchema,
        Value,
    };
    use crate::row_histories::BatchId;
    use crate::runtime_core::{NoopScheduler, RuntimeCore};
    use crate::schema_manager::{AppId, SchemaManager};
    use crate::storage::MemoryStorage;
    use crate::sync_manager::{DurabilityTier, QueryPropagation, SyncManager};
    use std::cell::RefCell;
    use std::collections::HashMap;
    use std::rc::Rc;

    fn declared_todo_schema() -> Schema {
        SchemaBuilder::new()
            .table(
                TableSchema::builder("todos")
                    .column("title", ColumnType::Text)
                    .column("done", ColumnType::Boolean)
                    .column("description", ColumnType::Text),
            )
            .build()
    }

    fn runtime_todo_schema() -> Schema {
        SchemaBuilder::new()
            .table(
                TableSchema::builder("todos")
                    .column("description", ColumnType::Text)
                    .column("done", ColumnType::Boolean)
                    .column("title", ColumnType::Text),
            )
            .build()
    }

    fn binding_support_test_runtime(schema: Schema) -> RuntimeCore<MemoryStorage, NoopScheduler> {
        let app_id = AppId::from_name("binding-support-write-facade");
        let schema_manager =
            SchemaManager::new(SyncManager::new(), schema, app_id, "dev", "main").unwrap();
        let mut runtime = RuntimeCore::new(schema_manager, MemoryStorage::new(), NoopScheduler);
        runtime.immediate_tick();
        runtime
    }

    fn binding_support_test_client()
    -> JazzClientCore<LocalRuntimeHost<MemoryStorage, NoopScheduler>> {
        JazzClientCore::from_runtime_host(
            ClientConfig::new("dev", "main"),
            LocalRuntimeHost::new(Rc::new(RefCell::new(binding_support_test_runtime(
                runtime_todo_schema(),
            )))),
        )
    }

    fn todo_values(title: &str, done: bool, description: &str) -> HashMap<String, Value> {
        HashMap::from([
            ("title".to_string(), Value::Text(title.to_string())),
            ("done".to_string(), Value::Boolean(done)),
            (
                "description".to_string(),
                Value::Text(description.to_string()),
            ),
        ])
    }

    #[test]
    fn query_rows_are_reordered_back_to_declared_schema() {
        let rows = vec![(
            ObjectId::new(),
            vec![
                Value::Text("note".to_string()),
                Value::Boolean(false),
                Value::Text("buy milk".to_string()),
            ],
        )];
        let query = Query::new("todos");

        let aligned = align_query_rows_to_declared_schema(
            &declared_todo_schema(),
            &runtime_todo_schema(),
            &query,
            rows,
        );

        assert_eq!(
            aligned[0].1,
            vec![
                Value::Text("buy milk".to_string()),
                Value::Boolean(false),
                Value::Text("note".to_string()),
            ]
        );
    }

    #[test]
    fn binding_support_query_rows_json_aligns_declared_schema() {
        let row_id = ObjectId::new();
        let query = Query::new("todos");

        let payload = serialize_query_rows_json(
            &declared_todo_schema(),
            &runtime_todo_schema(),
            &query,
            vec![(
                row_id,
                vec![
                    Value::Text("note".to_string()),
                    Value::Boolean(false),
                    Value::Text("buy milk".to_string()),
                ],
            )],
        );

        assert_eq!(
            payload,
            serde_json::json!([
                {
                    "id": row_id.uuid().to_string(),
                    "values": [
                        Value::Text("buy milk".to_string()),
                        Value::Boolean(false),
                        Value::Text("note".to_string()),
                    ],
                }
            ])
        );
    }

    #[test]
    fn binding_support_insert_json_aligns_declared_schema() {
        let declared_schema = declared_todo_schema();
        let mut client = binding_support_test_client();
        let object_id = ObjectId::new();

        let payload = insert_unsealed_json(
            &mut client,
            &declared_schema,
            "todos",
            todo_values("ship facade", true, "shared write path"),
            binding_write_options(Some(object_id), None),
        )
        .expect("insert json");

        assert_eq!(
            payload,
            serde_json::json!({
                "id": object_id.uuid().to_string(),
                "values": [
                    Value::Text("ship facade".to_string()),
                    Value::Boolean(true),
                    Value::Text("shared write path".to_string())
                ],
                "batchId": payload["batchId"],
            })
        );
    }

    #[test]
    fn binding_support_update_and_delete_json_serialize_batch_handles() {
        let declared_schema = declared_todo_schema();
        let mut client = binding_support_test_client();
        let object_id = ObjectId::new();
        insert_unsealed_json(
            &mut client,
            &declared_schema,
            "todos",
            todo_values("before", false, "keep"),
            binding_write_options(Some(object_id), None),
        )
        .expect("seed row");

        let update = update_unsealed_json(
            &mut client,
            object_id,
            record_to_updates(HashMap::from([(
                "title".to_string(),
                Value::Text("after".to_string()),
            )])),
            None,
        )
        .expect("update json");
        let delete = delete_unsealed_json(&mut client, object_id, None).expect("delete json");

        assert!(update["batchId"].as_str().is_some());
        assert!(delete["batchId"].as_str().is_some());
        assert_ne!(update["batchId"], delete["batchId"]);
    }

    #[test]
    fn binding_support_sealed_json_writes_seal_batches() {
        let declared_schema = declared_todo_schema();
        let mut client = binding_support_test_client();
        let insert_id = ObjectId::new();
        let update_id = ObjectId::new();
        let delete_id = ObjectId::new();

        let inserted = insert_sealed_json(
            &mut client,
            &declared_schema,
            "todos",
            todo_values("inserted", false, "sealed"),
            binding_write_options(Some(insert_id), None),
        )
        .expect("sealed insert json");
        insert_unsealed_json(
            &mut client,
            &declared_schema,
            "todos",
            todo_values("update seed", false, "sealed"),
            binding_write_options(Some(update_id), None),
        )
        .expect("seed update row");
        insert_unsealed_json(
            &mut client,
            &declared_schema,
            "todos",
            todo_values("delete seed", false, "sealed"),
            binding_write_options(Some(delete_id), None),
        )
        .expect("seed delete row");

        let updated = update_sealed_json(
            &mut client,
            update_id,
            record_to_updates(HashMap::from([("done".to_string(), Value::Boolean(true))])),
            None,
        )
        .expect("sealed update json");
        let deleted = delete_sealed_json(&mut client, delete_id, None).expect("sealed delete json");

        for payload in [inserted, updated, deleted] {
            let batch_id = payload["batchId"]
                .as_str()
                .expect("batch id string")
                .parse()
                .expect("parse batch id");
            let record = local_batch_record_json(&client, batch_id).expect("load local batch");
            assert_eq!(record["sealed"], true);
        }
    }

    #[test]
    fn binding_support_write_batch_context_json_serializes_direct_and_transactional_contexts() {
        let client = binding_support_test_client();

        let direct = write_batch_context_json(&client, BatchMode::Direct);
        let transactional = write_batch_context_json(&client, BatchMode::Transactional);

        assert_eq!(direct["batchMode"], "direct");
        assert!(direct["batchId"].as_str().is_some());
        assert_eq!(
            direct["targetBranchName"]
                .as_str()
                .expect("direct target branch")
                .starts_with("dev-"),
            true
        );
        assert_eq!(transactional["batchMode"], "transactional");
        assert!(transactional["batchId"].as_str().is_some());
        assert_ne!(direct["batchId"], transactional["batchId"]);
    }

    #[test]
    fn binding_support_rust_batch_context_json_drives_unsealed_writes() {
        let declared_schema = declared_todo_schema();
        let mut client = binding_support_test_client();
        let object_id = ObjectId::new();
        let context_json = write_batch_context_json(&client, BatchMode::Direct);
        let write_context_json = serde_json::json!({
            "batch_mode": context_json["batchMode"],
            "batch_id": context_json["batchId"],
            "target_branch_name": context_json["targetBranchName"],
        });
        let write_context = parse_write_context_input(Some(&write_context_json.to_string()))
            .expect("parse generated write context")
            .expect("generated batch context");

        let inserted = insert_unsealed_json(
            &mut client,
            &declared_schema,
            "todos",
            todo_values("draft", false, "batch"),
            binding_write_options(Some(object_id), Some(write_context.clone())),
        )
        .expect("batch insert json");
        let updated = update_unsealed_json(
            &mut client,
            object_id,
            record_to_updates(HashMap::from([("done".to_string(), Value::Boolean(true))])),
            binding_write_options(None, Some(write_context.clone())),
        )
        .expect("batch update json");
        let deleted = delete_unsealed_json(
            &mut client,
            object_id,
            binding_write_options(None, Some(write_context)),
        )
        .expect("batch delete json");

        assert_eq!(inserted["batchId"], updated["batchId"]);
        assert_eq!(inserted["batchId"], deleted["batchId"]);

        let batch_id = inserted["batchId"]
            .as_str()
            .expect("batch id string")
            .parse()
            .expect("parse batch id");
        seal_batch_for_binding(&mut client, batch_id).expect("seal generated batch");
        let record = local_batch_record_json(&client, batch_id).expect("load batch record");
        assert_eq!(record["sealed"], true);
    }

    #[test]
    fn binding_support_local_batch_record_json_serializes_existing_shape() {
        let declared_schema = declared_todo_schema();
        let mut client = binding_support_test_client();
        let object_id = ObjectId::new();
        let inserted = insert_unsealed_json(
            &mut client,
            &declared_schema,
            "todos",
            todo_values("draft", false, "local batch"),
            binding_write_options(Some(object_id), None),
        )
        .expect("insert json");
        let batch_id = inserted["batchId"]
            .as_str()
            .expect("batch id string")
            .parse()
            .expect("parse batch id");
        seal_batch_for_binding(&mut client, batch_id).expect("seal batch");

        let payload = local_batch_record_json(&client, batch_id).expect("record json");

        assert_eq!(payload["batchId"], inserted["batchId"]);
        assert_eq!(payload["mode"], "direct");
        assert_eq!(payload["sealed"], true);
        assert_eq!(payload["latestSettlement"]["kind"], "durableDirect");
        assert_eq!(payload["latestSettlement"]["batchId"], inserted["batchId"]);
    }

    #[test]
    fn binding_support_missing_local_batch_record_json_returns_null() {
        let client = binding_support_test_client();

        let payload = local_batch_record_json(&client, BatchId::new()).expect("record json");

        assert_eq!(payload, serde_json::Value::Null);
    }

    #[test]
    fn binding_support_local_batch_records_json_returns_array() {
        let declared_schema = declared_todo_schema();
        let mut client = binding_support_test_client();
        let inserted = insert_unsealed_json(
            &mut client,
            &declared_schema,
            "todos",
            todo_values("draft", false, "local batches"),
            None,
        )
        .expect("insert json");
        let batch_id = inserted["batchId"]
            .as_str()
            .expect("batch id string")
            .parse()
            .expect("parse batch id");
        seal_batch_for_binding(&mut client, batch_id).expect("seal batch");

        let payload = local_batch_records_json(&client).expect("records json");

        assert_eq!(payload.as_array().expect("records array").len(), 1);
        assert_eq!(payload[0]["batchId"], inserted["batchId"]);
    }

    #[test]
    fn binding_support_rejected_batch_helpers_return_binding_values() {
        let mut client = binding_support_test_client();

        assert_eq!(
            drain_rejected_batch_id_strings(&mut client),
            Vec::<String>::new()
        );
        assert!(
            !acknowledge_rejected_batch_for_binding(&mut client, BatchId::new())
                .expect("acknowledge missing rejected batch")
        );
    }

    #[test]
    fn binding_support_client_error_message_preserves_runtime_debug_shape() {
        let runtime_error = crate::runtime_core::RuntimeError::WriteError(
            "policy denied INSERT on table todos".to_string(),
        );
        let error = crate::client_core::ClientError::from_runtime(&runtime_error);

        assert_eq!(
            error.to_string(),
            "Write error: policy denied INSERT on table todos"
        );
        assert_eq!(
            client_error_message("Insert", &error),
            r#"Insert failed: WriteError("policy denied INSERT on table todos")"#
        );
    }

    #[test]
    fn binding_support_seal_batch_helper_marks_record_sealed() {
        let declared_schema = declared_todo_schema();
        let mut client = binding_support_test_client();
        let inserted = insert_unsealed_json(
            &mut client,
            &declared_schema,
            "todos",
            todo_values("draft", false, "seal helper"),
            None,
        )
        .expect("insert json");
        let batch_id = inserted["batchId"]
            .as_str()
            .expect("batch id string")
            .parse()
            .expect("parse batch id");

        seal_batch_for_binding(&mut client, batch_id).expect("seal batch");
        let payload = local_batch_record_json(&client, batch_id).expect("record json");

        assert_eq!(payload["sealed"], true);
    }

    #[test]
    fn binding_support_required_object_id_parser_rejects_missing_and_invalid_ids() {
        assert_eq!(
            parse_object_id_input(None).expect_err("missing object id should fail"),
            "Object id is required"
        );
        assert!(
            parse_object_id_input(Some("not-a-uuid"))
                .expect_err("invalid object id should fail")
                .contains("Invalid ObjectId")
        );

        let object_id = ObjectId::new();
        assert_eq!(
            parse_object_id_input(Some(&object_id.uuid().to_string())).expect("parse object id"),
            object_id
        );
    }

    #[test]
    fn parse_external_object_id_accepts_any_valid_uuid() {
        let parsed = super::parse_external_object_id(Some("550e8400-e29b-41d4-a716-446655440000"))
            .expect("parse valid uuid");

        assert_eq!(
            parsed.expect("object id").uuid().to_string(),
            "550e8400-e29b-41d4-a716-446655440000"
        );
    }

    #[test]
    fn descriptor_values_are_reordered_back_to_declared_schema() {
        let runtime_descriptor = RowDescriptor::new(vec![
            ColumnDescriptor::new("description", ColumnType::Text),
            ColumnDescriptor::new("done", ColumnType::Boolean),
            ColumnDescriptor::new("title", ColumnType::Text),
        ]);

        let aligned = align_values_to_declared_schema(
            &declared_todo_schema(),
            &TableName::new("todos"),
            &runtime_descriptor,
            vec![
                Value::Text("note".to_string()),
                Value::Boolean(true),
                Value::Text("ship fix".to_string()),
            ],
        );

        assert_eq!(
            aligned,
            vec![
                Value::Text("ship fix".to_string()),
                Value::Boolean(true),
                Value::Text("note".to_string()),
            ]
        );
    }

    #[test]
    fn simple_queries_are_schema_alignable() {
        assert!(query_rows_can_be_schema_aligned(&Query::new("todos")));
    }

    #[test]
    fn read_durability_options_default_to_full_and_immediate() {
        let (durability, propagation) =
            parse_read_durability_options(Some("local"), None).expect("parse options");

        assert_eq!(
            durability.tier,
            Some(crate::sync_manager::DurabilityTier::Local)
        );
        assert_eq!(
            durability.local_updates,
            crate::query_manager::manager::LocalUpdates::Immediate
        );
        assert_eq!(propagation, crate::sync_manager::QueryPropagation::Full);
    }

    #[test]
    fn binding_support_query_execution_options_parse_overlay() {
        let batch_id = crate::row_histories::BatchId::new();
        let row_id = ObjectId::new();
        let options_json = format!(
            r#"{{
                "propagation": "local-only",
                "local_updates": "deferred",
                "transaction_overlay": {{
                    "batch_id": "{batch_id}",
                    "branch_name": "dev-111111111111-main",
                    "row_ids": ["{row_id}"]
                }}
            }}"#
        );

        let options = parse_query_execution_options(Some("edge"), Some(&options_json))
            .expect("parse query execution options");

        assert_eq!(options.durability.tier, Some(DurabilityTier::EdgeServer));
        assert_eq!(
            options.durability.local_updates,
            crate::query_manager::manager::LocalUpdates::Deferred
        );
        assert_eq!(options.propagation, QueryPropagation::LocalOnly);
        let overlay = options
            .transaction_overlay
            .expect("transaction overlay should parse");
        assert_eq!(overlay.batch_id, batch_id);
        assert_eq!(overlay.branch_name.to_string(), "dev-111111111111-main");
        assert_eq!(overlay.row_ids, vec![row_id]);
    }

    #[test]
    fn binding_support_query_execution_options_reject_invalid_values() {
        let error = parse_query_execution_options(
            None,
            Some(r#"{ "propagation": "nearby", "local_updates": "immediate" }"#),
        )
        .expect_err("invalid propagation should fail");
        assert!(error.contains("Invalid propagation"));

        let error = parse_query_execution_options(
            None,
            Some(r#"{ "propagation": "full", "local_updates": "later" }"#),
        )
        .expect_err("invalid local updates should fail");
        assert!(error.contains("Invalid localUpdates"));
    }

    #[test]
    fn binding_support_subscription_input_parses_query_session_and_options() {
        let query_json = serde_json::to_string(&Query::new("todos")).expect("query json");
        let input = parse_subscription_input(
            &query_json,
            Some(r#"{ "user_id": "alice", "claims": {}, "authMode": "external" }"#),
            Some("global"),
            Some(r#"{ "propagation": "local-only", "local_updates": "deferred" }"#),
        )
        .expect("parse subscription input");

        assert_eq!(input.query.table, TableName::new("todos"));
        assert_eq!(input.session.expect("session").user_id, "alice");
        assert_eq!(input.durability.tier, Some(DurabilityTier::GlobalServer));
        assert_eq!(
            input.durability.local_updates,
            crate::query_manager::manager::LocalUpdates::Deferred
        );
        assert_eq!(input.propagation, QueryPropagation::LocalOnly);
        assert!(input.transaction_overlay.is_none());
    }

    #[test]
    fn binding_support_runtime_bootstrap_accepts_plain_schema() {
        let schema_json = serde_json::to_string(&declared_todo_schema()).expect("schema json");

        let bootstrap = build_runtime_schema_bootstrap(RuntimeSchemaBootstrapOptions {
            schema_json: &schema_json,
            app_id: "binding-bootstrap-app",
            env: "dev",
            user_branch: "main",
            node_tier: Some("local"),
            plain_schema_policy_mode: PlainSchemaPolicyMode::PermissiveLocal,
        })
        .expect("bootstrap runtime schema");

        assert_eq!(bootstrap.app_id, AppId::from_name("binding-bootstrap-app"));
        assert_eq!(
            bootstrap.default_durability_tier,
            Some(DurabilityTier::Local)
        );
        assert!(
            bootstrap
                .schema_manager
                .current_schema()
                .contains_key(&TableName::new("todos"))
        );
        assert!(
            bootstrap
                .declared_schema
                .contains_key(&TableName::new("todos"))
        );
    }

    #[test]
    fn binding_support_runtime_bootstrap_accepts_schema_envelope() {
        let schema_json = r#"{
            "__jazzRuntimeSchema": 1,
            "schema": {
                "todos": {
                    "columns": [
                        {
                            "name": "title",
                            "column_type": { "type": "Text" },
                            "nullable": false
                        }
                    ]
                }
            },
            "loadedPolicyBundle": true
        }"#;

        let bootstrap = build_runtime_schema_bootstrap(RuntimeSchemaBootstrapOptions {
            schema_json,
            app_id: "binding-envelope-app",
            env: "dev",
            user_branch: "main",
            node_tier: None,
            plain_schema_policy_mode: PlainSchemaPolicyMode::InferFromSchema,
        })
        .expect("bootstrap runtime schema envelope");

        assert_eq!(bootstrap.app_id, AppId::from_name("binding-envelope-app"));
        assert_eq!(bootstrap.default_durability_tier, None);
        assert!(
            bootstrap
                .schema_manager
                .current_schema()
                .contains_key(&TableName::new("todos"))
        );
    }

    #[test]
    fn runtime_schema_envelope_reads_ts_policy_bundle_flag() {
        let schema_json = r#"{
            "__jazzRuntimeSchema": 1,
            "schema": {
                "todos": {
                    "columns": [
                        {
                            "name": "title",
                            "column_type": { "type": "Text" },
                            "nullable": false
                        },
                        {
                            "name": "done",
                            "column_type": { "type": "Boolean" },
                            "nullable": false
                        }
                    ]
                }
            },
            "loadedPolicyBundle": true
        }"#;

        let input = parse_runtime_schema_input(schema_json).expect("parse runtime schema");

        assert!(input.loaded_policy_bundle);
        assert!(input.schema.contains_key(&TableName::new("todos")));
    }

    #[test]
    fn runtime_schema_envelope_defaults_missing_policy_bundle_flag_to_permissive_local() {
        let schema_json = r#"{
            "__jazzRuntimeSchema": 1,
            "schema": {
                "todos": {
                    "columns": [
                        {
                            "name": "title",
                            "column_type": { "type": "Text" },
                            "nullable": false
                        }
                    ]
                }
            }
        }"#;

        let input = parse_runtime_schema_input(schema_json).expect("parse runtime schema");

        assert!(!input.loaded_policy_bundle);
        assert!(input.schema.contains_key(&TableName::new("todos")));
    }

    #[test]
    fn parse_write_context_accepts_ts_batch_id_strings() {
        let batch_id = "0123456789abcdef0123456789abcdef";
        let input = format!(
            r#"{{
                "session": {{
                    "user_id": "alice",
                    "claims": {{}},
                    "authMode": "external"
                }},
                "batch_mode": "transactional",
                "batch_id": "{batch_id}",
                "target_branch_name": "dev-123456789abc-main"
            }}"#
        );

        let context = parse_write_context_input(Some(&input))
            .expect("parse write context")
            .expect("write context");

        assert_eq!(
            context
                .batch_id()
                .map(|parsed| parsed.to_string())
                .as_deref(),
            Some(batch_id)
        );
        assert_eq!(context.target_branch_name(), Some("dev-123456789abc-main"));
    }

    #[test]
    fn parse_write_context_rejects_legacy_batch_id_arrays() {
        let input = r#"{
            "session": {
                "user_id": "alice",
                "claims": {},
                "authMode": "external"
            },
            "batch_mode": "transactional",
            "batch_id": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
            "target_branch_name": "dev-123456789abc-main"
        }"#;

        let error =
            parse_write_context_input(Some(input)).expect_err("legacy batch_id should fail");
        assert!(error.contains("WriteContextWire"));
    }

    #[test]
    fn write_context_accepts_lowercase_transactional_batch_mode() {
        let context = parse_write_context_input(Some(
            r#"{
                "batch_mode": "transactional",
                "batch_id": "0196721ac2617f10a4bebbc7f7ffdb3f",
                "target_branch_name": "dev-111111111111-main"
            }"#,
        ))
        .expect("parse write context")
        .expect("write context present");

        assert_eq!(context.batch_mode(), BatchMode::Transactional);
        assert_eq!(context.target_branch_name(), Some("dev-111111111111-main"));
        assert!(context.batch_id().is_some());
    }
}
