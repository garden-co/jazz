//! Shared helpers used by native bindings.
//!
//! These utilities keep wrapper crates thin by centralizing JSON parsing,
//! schema-alignment, and subscription payload shaping in the core crate.

use std::collections::HashMap;

use serde::Deserialize;

use crate::object::ObjectId;
use crate::query_manager::manager::LocalUpdates;
use crate::query_manager::parse_query_json;
use crate::query_manager::query::Query;
use crate::query_manager::session::{Session, WriteContext};
use crate::query_manager::types::{RowDescriptor, Schema, TableName, Value};
use crate::row_format::decode_row;
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
    Context(WriteContext),
}

pub fn parse_write_context_input(
    write_context_json: Option<&str>,
) -> Result<Option<WriteContext>, String> {
    match write_context_json {
        Some(json) => match serde_json::from_str::<WriteContextWire>(json) {
            Ok(WriteContextWire::Session(session)) => Ok(Some(WriteContext::from_session(session))),
            Ok(WriteContextWire::Context(context)) => Ok(Some(context)),
            Err(err) => Err(err.to_string()),
        },
        None => Ok(None),
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

pub fn default_read_durability_options(tier: Option<DurabilityTier>) -> ReadDurabilityOptions {
    ReadDurabilityOptions {
        tier,
        local_updates: LocalUpdates::Immediate,
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
        parse_read_durability_options, query_rows_can_be_schema_aligned, serialize_outbox_entry,
    };
    use crate::object::ObjectId;
    use crate::query_manager::query::Query;
    use crate::query_manager::types::{
        ColumnDescriptor, ColumnType, RowDescriptor, Schema, SchemaBuilder, TableName, TableSchema,
        Value,
    };
    use crate::sync_manager::{Destination, OutboxEntry, QueryId, ServerId, SyncPayload};

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
}
