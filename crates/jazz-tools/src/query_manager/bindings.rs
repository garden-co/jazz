//! Shared helpers used by native bindings.
//!
//! These utilities keep wrapper crates thin by centralizing JSON parsing
//! and subscription payload shaping in the core crate.

use serde::Deserialize;
use uuid::Uuid;

use crate::object::ObjectId;
use crate::query_manager::parse_query_json;
use crate::query_manager::query::Query;
use crate::query_manager::session::{Session, WriteContext};
use crate::query_manager::types::Schema;
use crate::sync::DurabilityTier;
use crate::transaction::BatchId;
use crate::transaction::BatchMode;

#[derive(Debug, Clone, PartialEq)]
pub struct RuntimeSchemaInput {
    pub schema: Schema,
    pub loaded_policy_bundle: bool,
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
            })
        }
        RuntimeSchemaWire::Schema(schema) => Ok(RuntimeSchemaInput {
            schema,
            loaded_policy_bundle: false,
        }),
    }
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
    batch_id: Option<String>,
    #[serde(default)]
    target_branch_name: Option<String>,
}

impl TryFrom<WriteContextPayloadWire> for WriteContext {
    type Error = String;

    fn try_from(value: WriteContextPayloadWire) -> Result<Self, Self::Error> {
        let batch_id = value
            .batch_id
            .as_deref()
            .map(parse_batch_id_input)
            .transpose()?;

        Ok(WriteContext {
            session: value.session,
            attribution: value.attribution,
            updated_at: value.updated_at,
            batch_id,
            target_branch_name: value.target_branch_name,
        })
    }
}

pub fn parse_batch_id_input(batch_id: &str) -> Result<BatchId, String> {
    batch_id
        .parse()
        .map_err(|err: String| format!("Invalid BatchId: {err}"))
}

pub fn parse_transaction_id_input(transaction_id: &str) -> Result<BatchId, String> {
    transaction_id
        .parse()
        .map_err(|err: String| format!("Invalid transaction id: {err}"))
}

pub fn parse_transaction_kind_input(transaction_kind: &str) -> Result<BatchMode, String> {
    match transaction_kind {
        "mergeable" | "Mergeable" | "direct" | "Direct" => Ok(BatchMode::Direct),
        "exclusive" | "Exclusive" | "transactional" | "Transactional" => {
            Ok(BatchMode::Transactional)
        }
        other => Err(format!(
            "Invalid transaction kind '{other}'. Must be 'mergeable' or 'exclusive'."
        )),
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

pub fn serialize_durability_tier(tier: DurabilityTier) -> &'static str {
    match tier {
        DurabilityTier::Local => "local",
        DurabilityTier::EdgeServer => "edge",
        DurabilityTier::GlobalServer => "global",
    }
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
    use super::{parse_runtime_schema_input, parse_write_context_input};
    use crate::query_manager::types::TableName;

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
            "batch_id": [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15],
            "target_branch_name": "dev-123456789abc-main"
        }"#;

        let error =
            parse_write_context_input(Some(input)).expect_err("legacy batch_id should fail");
        assert!(error.contains("WriteContextWire"));
    }

    #[test]
    fn write_context_accepts_batch_id_for_transaction_correlation() {
        let context = parse_write_context_input(Some(
            r#"{
                "batch_id": "0196721ac2617f10a4bebbc7f7ffdb3f",
                "target_branch_name": "dev-111111111111-main"
            }"#,
        ))
        .expect("parse write context")
        .expect("write context present");

        assert_eq!(context.target_branch_name(), Some("dev-111111111111-main"));
        assert_eq!(
            context.batch_id().map(|id| id.to_string()).as_deref(),
            Some("0196721ac2617f10a4bebbc7f7ffdb3f")
        );
    }
}
