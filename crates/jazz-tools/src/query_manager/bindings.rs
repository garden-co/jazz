//! Shared helpers used by native bindings.
//!
//! These utilities keep wrapper crates thin by centralizing JSON parsing
//! and subscription payload shaping in the core crate.

use serde::Deserialize;
use serde_json::{Value as JsonValue, json};
use uuid::Uuid;

use crate::batch_fate::{BatchFate, BatchMode, LocalBatchRecord};
use crate::object::ObjectId;
use crate::query_manager::manager::LocalUpdates;
use crate::query_manager::parse_query_json;
use crate::query_manager::query::Query;
use crate::query_manager::session::{Session, WriteContext};
use crate::query_manager::types::{BranchPolicies, Schema};
use crate::row_format::decode_row;
use crate::row_histories::BatchId;
use crate::runtime_core::{MutationErrorEvent, ReadDurabilityOptions, SubscriptionDelta};
use crate::sync_manager::{DurabilityTier, QueryPropagation};

#[derive(Debug, Clone, Deserialize, Default)]
struct QueryExecutionOptionsWire {
    propagation: Option<String>,
    local_updates: Option<String>,
    transaction_batch_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct RuntimeSchemaInput {
    pub schema: Schema,
    pub loaded_policy_bundle: bool,
    pub branch_policies: BranchPolicies,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct RuntimeSchemaEnvelopeWire {
    #[serde(rename = "__jazzRuntimeSchema")]
    version: u8,
    schema: JsonValue,
    #[serde(default)]
    loaded_policy_bundle: bool,
    #[serde(default)]
    branch_policies: BranchPolicies,
}

pub fn parse_query_input(query_json: &str) -> Result<Query, String> {
    parse_query_json(query_json)
}

pub fn parse_runtime_schema_input(schema_json: &str) -> Result<RuntimeSchemaInput, String> {
    let value = serde_json::from_str::<JsonValue>(schema_json).map_err(|err| err.to_string())?;

    if value
        .as_object()
        .and_then(|object| object.get("__jazzRuntimeSchema"))
        .is_some()
    {
        let envelope = serde_json::from_value::<RuntimeSchemaEnvelopeWire>(value)
            .map_err(|err| err.to_string())?;
        if envelope.version != 1 {
            return Err(format!(
                "unsupported runtime schema envelope version {}",
                envelope.version
            ));
        }
        return Ok(RuntimeSchemaInput {
            schema: parse_schema_value(envelope.schema)?,
            loaded_policy_bundle: envelope.loaded_policy_bundle,
            branch_policies: envelope.branch_policies,
        });
    }

    Ok(RuntimeSchemaInput {
        schema: parse_schema_value(value)?,
        loaded_policy_bundle: false,
        branch_policies: BranchPolicies::default(),
    })
}

fn parse_schema_value(mut schema: JsonValue) -> Result<Schema, String> {
    if let JsonValue::Object(object) = &mut schema {
        object.remove("branch_policies");
    }
    serde_json::from_value(schema).map_err(|err| err.to_string())
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
        let batch_mode = value
            .batch_mode
            .as_deref()
            .map(parse_batch_mode_input)
            .transpose()?;
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

pub fn parse_batch_mode_input(batch_mode: &str) -> Result<BatchMode, String> {
    match batch_mode {
        "direct" | "Direct" => Ok(BatchMode::Direct),
        "transactional" | "Transactional" => Ok(BatchMode::Transactional),
        other => Err(format!(
            "Invalid batch mode '{other}'. Must be 'direct' or 'transactional'."
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

pub fn serialize_batch_mode(mode: BatchMode) -> &'static str {
    match mode {
        BatchMode::Direct => "direct",
        BatchMode::Transactional => "transactional",
    }
}

pub fn serialize_batch_fate(settlement: &BatchFate) -> JsonValue {
    match settlement {
        BatchFate::Rejected {
            batch_id,
            code,
            reason,
        } => json!({
            "kind": "rejected",
            "batchId": batch_id.to_string(),
            "code": code,
            "reason": reason,
        }),
        BatchFate::DurableDirect {
            batch_id,
            confirmed_tier,
        } => json!({
            "kind": "durableDirect",
            "batchId": batch_id.to_string(),
            "confirmedTier": serialize_durability_tier(*confirmed_tier),
        }),
        BatchFate::AcceptedTransaction {
            batch_id,
            confirmed_tier,
        } => json!({
            "kind": "acceptedTransaction",
            "batchId": batch_id.to_string(),
            "confirmedTier": serialize_durability_tier(*confirmed_tier),
        }),
        BatchFate::Missing { batch_id } => json!({
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
        "latestSettlement": record.latest_fate.as_ref().map(serialize_batch_fate),
    })
}

pub fn serialize_local_batch_records(records: &[LocalBatchRecord]) -> JsonValue {
    JsonValue::Array(records.iter().map(serialize_local_batch_record).collect())
}

pub fn serialize_mutation_error_event(event: &MutationErrorEvent) -> JsonValue {
    json!({
        "code": event.code.as_str(),
        "reason": event.reason.as_str(),
        "batch": serialize_local_batch_record(&event.batch),
    })
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
) -> Result<(ReadDurabilityOptions, QueryPropagation, Option<BatchId>), String> {
    let parsed_tier = tier.map(parse_durability_tier).transpose()?;
    let Some(raw) = options_json else {
        return Ok((
            default_read_durability_options(parsed_tier),
            QueryPropagation::Full,
            None,
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

    let transaction_batch_id = options
        .transaction_batch_id
        .as_deref()
        .map(parse_batch_id_input)
        .transpose()?;

    Ok((
        ReadDurabilityOptions {
            tier: parsed_tier,
            local_updates,
        },
        propagation,
        transaction_batch_id,
    ))
}

pub fn subscription_delta_to_json(delta: &SubscriptionDelta) -> serde_json::Value {
    let row_to_json = |row: &crate::query_manager::types::Row,
                       descriptor: &crate::query_manager::types::RowDescriptor|
     -> serde_json::Value {
        let values = decode_row(descriptor, &row.data)
            .map(|vals| vals.into_iter().collect::<Vec<_>>())
            .unwrap_or_default();
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
        parse_read_durability_options, parse_runtime_schema_input, parse_write_context_input,
    };
    use crate::batch_fate::BatchMode;
    use crate::query_manager::types::TableName;
    use crate::row_histories::BatchId;

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
    fn read_durability_options_default_to_full_and_immediate() {
        let (durability, propagation, transaction_batch_id) =
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
        assert_eq!(transaction_batch_id, None);
    }

    #[test]
    fn read_durability_options_parse_transaction_batch_id() {
        let batch_id = BatchId::new();
        let options_json = format!(r#"{{"transaction_batch_id":"{batch_id}"}}"#);

        let (_, _, parsed_batch_id) =
            parse_read_durability_options(None, Some(&options_json)).expect("parse options");

        assert_eq!(parsed_batch_id, Some(batch_id));
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
    fn runtime_schema_envelope_reads_branch_policies() {
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
                },
                "branch_policies": {
                    "branches": {
                        "todos": {}
                    }
                }
            },
            "branchPolicies": {
                "branches": {
                    "todos": {}
                }
            }
        }"#;

        let input = parse_runtime_schema_input(schema_json).expect("parse runtime schema");

        assert_eq!(input.branch_policies.len(), 1);
        assert!(
            !input
                .schema
                .contains_key(&TableName::new("branch_policies"))
        );
        assert!(
            input
                .branch_policies
                .get(&TableName::new("branches"))
                .expect("branch table policies")
                .contains_key(&TableName::new("todos"))
        );
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
