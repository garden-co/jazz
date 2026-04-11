//! Shared helpers used by native bindings.
//!
//! These utilities keep wrapper crates thin by centralizing JSON parsing,
//! schema-alignment, and subscription payload shaping in the core crate.

use std::collections::HashMap;

use serde::Deserialize;
use serde_json::{Value as JsonValue, json};

use crate::batch_fate::{BatchMode, BatchSettlement, LocalBatchRecord, VisibleBatchMember};
use crate::object::ObjectId;
use crate::query_manager::manager::LocalUpdates;
use crate::query_manager::parse_query_json;
use crate::query_manager::query::Query;
use crate::query_manager::session::{Session, WriteContext};
use crate::query_manager::types::{RowDescriptor, Schema, TableName, Value};
use crate::row_format::decode_row;
use crate::row_histories::BatchId;
use crate::runtime_core::{ReadDurabilityOptions, SubscriptionDelta};
use crate::sync_manager::{Destination, DurabilityTier, OutboxEntry, QueryPropagation};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SerializedOutboxEntry {
    pub destination_kind: String,
    pub destination_id: String,
    pub payload_json: String,
    pub is_catalogue: bool,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct QueryExecutionOptionsWire {
    propagation: Option<String>,
    local_updates: Option<String>,
    strict_transactions: Option<bool>,
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

pub fn parse_query_input(query_json: &str) -> Result<Query, String> {
    parse_query_json(query_json)
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
    Context(BindingWriteContext),
}

#[derive(Debug, Deserialize)]
struct BindingWriteContext {
    #[serde(default)]
    session: Option<Session>,
    #[serde(default)]
    attribution: Option<String>,
    #[serde(default, alias = "batchMode")]
    batch_mode: Option<String>,
    #[serde(default, alias = "batchId")]
    batch_id: Option<String>,
}

impl BindingWriteContext {
    fn into_write_context(self) -> Result<WriteContext, String> {
        if self.session.is_none()
            && self.attribution.is_none()
            && self.batch_mode.is_none()
            && self.batch_id.is_none()
        {
            return Err("write context did not contain any recognized fields".to_string());
        }

        let batch_mode = match self.batch_mode {
            Some(mode) => Some(parse_batch_mode_input(&mode)?),
            None => None,
        };
        let batch_id = match self.batch_id {
            Some(batch_id) => Some(parse_batch_id_input(&batch_id)?),
            None => None,
        };

        Ok(WriteContext {
            session: self.session,
            attribution: self.attribution,
            batch_mode,
            batch_id,
        })
    }
}

pub fn parse_write_context_input(
    write_context_json: Option<&str>,
) -> Result<Option<WriteContext>, String> {
    match write_context_json {
        Some(json) => match serde_json::from_str::<WriteContextWire>(json) {
            Ok(WriteContextWire::Session(session)) => Ok(Some(WriteContext::from_session(session))),
            Ok(WriteContextWire::Context(context)) => context.into_write_context().map(Some),
            Err(err) => Err(err.to_string()),
        },
        None => Ok(None),
    }
}

fn parse_batch_mode_input(raw: &str) -> Result<BatchMode, String> {
    match raw {
        "direct" | "Direct" => Ok(BatchMode::Direct),
        "transactional" | "Transactional" => Ok(BatchMode::Transactional),
        other => Err(format!("Invalid batch mode: {other}")),
    }
}

pub fn parse_durability_tier(tier: &str) -> Result<DurabilityTier, String> {
    match tier {
        "worker" => Ok(DurabilityTier::Worker),
        "edge" => Ok(DurabilityTier::EdgeServer),
        "global" => Ok(DurabilityTier::GlobalServer),
        _ => Err(format!(
            "Invalid tier '{}'. Must be 'worker', 'edge', or 'global'.",
            tier
        )),
    }
}

pub fn serialize_durability_tier(tier: DurabilityTier) -> &'static str {
    match tier {
        DurabilityTier::Worker => "worker",
        DurabilityTier::EdgeServer => "edge",
        DurabilityTier::GlobalServer => "global",
    }
}

pub fn parse_batch_id_input(batch_id: &str) -> Result<BatchId, String> {
    uuid::Uuid::parse_str(batch_id)
        .map(BatchId)
        .map_err(|err| format!("Invalid BatchId: {err}"))
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
        "batchId": member.batch_id.0.to_string(),
    })
}

pub fn serialize_batch_settlement(settlement: &BatchSettlement) -> JsonValue {
    match settlement {
        BatchSettlement::Missing { batch_id } => json!({
            "kind": "missing",
            "batchId": batch_id.0.to_string(),
        }),
        BatchSettlement::Rejected {
            batch_id,
            code,
            reason,
        } => json!({
            "kind": "rejected",
            "batchId": batch_id.0.to_string(),
            "code": code,
            "reason": reason,
        }),
        BatchSettlement::DurableDirect {
            batch_id,
            confirmed_tier,
            visible_members,
        } => json!({
            "kind": "durable_direct",
            "batchId": batch_id.0.to_string(),
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
            "kind": "accepted_transaction",
            "batchId": batch_id.0.to_string(),
            "confirmedTier": serialize_durability_tier(*confirmed_tier),
            "visibleMembers": visible_members
                .iter()
                .map(serialize_visible_batch_member)
                .collect::<Vec<_>>(),
        }),
    }
}

pub fn serialize_local_batch_record(record: &LocalBatchRecord) -> JsonValue {
    json!({
        "batchId": record.batch_id.0.to_string(),
        "mode": serialize_batch_mode(record.mode),
        "requestedTier": serialize_durability_tier(record.requested_tier),
        "latestSettlement": record.latest_settlement.as_ref().map(serialize_batch_settlement),
    })
}

pub fn serialize_local_batch_records(records: &[LocalBatchRecord]) -> JsonValue {
    JsonValue::Array(records.iter().map(serialize_local_batch_record).collect())
}

pub fn default_read_durability_options(tier: Option<DurabilityTier>) -> ReadDurabilityOptions {
    ReadDurabilityOptions {
        tier,
        local_updates: LocalUpdates::Immediate,
        strict_transactions: false,
    }
}

pub fn parse_read_durability_options(
    tier: Option<&str>,
    options_json: Option<&str>,
) -> Result<(ReadDurabilityOptions, QueryPropagation), String> {
    let parsed_tier = tier.map(parse_durability_tier).transpose()?;
    let Some(raw) = options_json else {
        return Ok((
            default_read_durability_options(parsed_tier),
            QueryPropagation::Full,
        ));
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

    Ok((
        ReadDurabilityOptions {
            tier: parsed_tier,
            local_updates,
            strict_transactions: options.strict_transactions.unwrap_or(false),
        },
        propagation,
    ))
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

pub fn serialize_outbox_entry(message: &OutboxEntry) -> Result<SerializedOutboxEntry, String> {
    let payload_json = serde_json::to_string(&message.payload).map_err(|err| err.to_string())?;
    let is_catalogue = message.payload.is_catalogue();
    let (destination_kind, destination_id) = match message.destination {
        Destination::Server(server_id) => ("server".to_string(), server_id.0.to_string()),
        Destination::Client(client_id) => ("client".to_string(), client_id.0.to_string()),
    };

    Ok(SerializedOutboxEntry {
        destination_kind,
        destination_id,
        payload_json,
        is_catalogue,
    })
}

pub fn generate_id() -> String {
    ObjectId::new().uuid().to_string()
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
        align_query_rows_to_declared_schema, align_values_to_declared_schema,
        parse_read_durability_options, parse_write_context_input, query_rows_can_be_schema_aligned,
        serialize_outbox_entry,
    };
    use crate::batch_fate::BatchMode;
    use crate::object::ObjectId;
    use crate::query_manager::query::Query;
    use crate::query_manager::types::{
        ColumnDescriptor, ColumnType, RowDescriptor, Schema, SchemaBuilder, TableName, TableSchema,
        Value,
    };
    use crate::row_histories::BatchId;
    use crate::sync_manager::{Destination, OutboxEntry, QueryId, ServerId, SyncPayload};
    use serde_json::json;

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
            parse_read_durability_options(Some("worker"), None).expect("parse options");

        assert_eq!(
            durability.tier,
            Some(crate::sync_manager::DurabilityTier::Worker)
        );
        assert_eq!(
            durability.local_updates,
            crate::query_manager::manager::LocalUpdates::Immediate
        );
        assert_eq!(propagation, crate::sync_manager::QueryPropagation::Full);
    }

    #[test]
    fn outbox_entries_are_serialized_for_bindings() {
        let message = OutboxEntry {
            destination: Destination::Server(ServerId::new()),
            payload: SyncPayload::QueryUnsubscription {
                query_id: QueryId(7),
            },
        };

        let serialized = serialize_outbox_entry(&message).expect("serialize outbox");

        assert_eq!(serialized.destination_kind, "server");
        assert!(!serialized.destination_id.is_empty());
        assert!(!serialized.payload_json.is_empty());
        assert!(!serialized.is_catalogue);
    }

    #[test]
    fn write_context_parser_accepts_binding_batch_fields() {
        let batch_id = BatchId::new();
        let parsed = parse_write_context_input(Some(
            &json!({
                "session": {
                    "user_id": "alice",
                    "claims": {}
                },
                "batch_mode": "transactional",
                "batch_id": batch_id.0.to_string(),
            })
            .to_string(),
        ))
        .expect("parse binding write context")
        .expect("binding write context should exist");

        assert_eq!(
            parsed.session().map(|session| session.user_id.as_str()),
            Some("alice")
        );
        assert_eq!(parsed.batch_mode(), BatchMode::Transactional);
        assert_eq!(parsed.batch_id(), Some(batch_id));
    }
}
