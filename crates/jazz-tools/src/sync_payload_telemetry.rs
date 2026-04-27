use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::ops::Deref;

use crate::batch_fate::BatchSettlement;
use crate::object::{BranchName, ObjectId};
use crate::query_manager::types::SchemaHash;
use crate::row_histories::BatchId;
use crate::sync_manager::{DurabilityTier, QueryId, SyncError, SyncPayload};

pub const SYNC_PAYLOAD_TELEMETRY_SEVERITY_TEXT: &str = "DEBUG";
pub const FIELD_DERIVATION_NOT_DERIVED: &str = "not_derived";

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SyncPayloadTelemetryScope {
    WorkerBridge,
    Websocket,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SyncPayloadTelemetryDirection {
    MainToWorker,
    WorkerToMain,
    ClientToServer,
    ServerToClient,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum SyncPayloadTelemetryMessageEncoding {
    Binary,
    Utf8,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FieldDerivation {
    pub table_name: Option<String>,
    pub table_name_error: Option<String>,
    pub schema_hash: Option<SchemaHash>,
    pub schema_hash_error: Option<String>,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct FieldDerivations {
    pub default: FieldDerivation,
    pub members: Vec<FieldDerivation>,
}

impl FieldDerivations {
    pub fn by_member(members: Vec<FieldDerivation>) -> Self {
        Self {
            default: FieldDerivation::default(),
            members,
        }
    }

    fn for_member(&self, member_index: Option<usize>) -> FieldDerivation {
        member_index
            .and_then(|index| self.members.get(index).cloned())
            .unwrap_or_else(|| self.default.clone())
    }
}

impl From<FieldDerivation> for FieldDerivations {
    fn from(default: FieldDerivation) -> Self {
        Self {
            default,
            members: Vec::new(),
        }
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncPayloadTelemetryFields {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload_variant: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub row_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub table_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub table_name_error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub branch_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub batch_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub query_id: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema_hash: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub schema_hash_error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub durability_tier: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_variant: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_code: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub member_index: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub member_count: Option<usize>,
}

impl SyncPayloadTelemetryFields {
    pub fn records_from_payload(
        payload: &SyncPayload,
        derivations: impl Into<FieldDerivations>,
    ) -> Vec<SyncPayloadTelemetryFields> {
        let derivations = derivations.into();
        Self::records_from_payload_with_derivations(payload, &derivations)
    }

    fn records_from_payload_with_derivations(
        payload: &SyncPayload,
        derivations: &FieldDerivations,
    ) -> Vec<SyncPayloadTelemetryFields> {
        match payload {
            SyncPayload::RowBatchCreated { row, .. } | SyncPayload::RowBatchNeeded { row, .. } => {
                vec![Self::row_payload(
                    payload,
                    row.row_id,
                    BranchName::new(row.branch.as_str()),
                    row.batch_id,
                    row.confirmed_tier,
                    derivations.for_member(None),
                )]
            }
            SyncPayload::RowBatchStateChanged {
                row_id,
                branch_name,
                batch_id,
                confirmed_tier,
                ..
            } => {
                vec![Self::row_payload(
                    payload,
                    *row_id,
                    *branch_name,
                    *batch_id,
                    *confirmed_tier,
                    derivations.for_member(None),
                )]
            }
            SyncPayload::BatchSettlement { settlement } => {
                Self::batch_settlement_payload(payload, settlement, derivations)
            }
            SyncPayload::SealBatch { submission } => {
                let member_count = submission.members.len();
                submission
                    .members
                    .iter()
                    .enumerate()
                    .map(|(member_index, member)| {
                        let mut fields = Self::row_payload(
                            payload,
                            member.object_id,
                            submission.target_branch_name,
                            submission.batch_id,
                            None,
                            derivations.for_member(Some(member_index)),
                        );
                        fields.member_index = Some(member_index);
                        fields.member_count = Some(member_count);
                        fields
                    })
                    .collect()
            }
            SyncPayload::QuerySubscription { query_id, .. }
            | SyncPayload::QueryUnsubscription { query_id }
            | SyncPayload::QueryScopeSnapshot { query_id, .. } => {
                let mut fields = Self::base(payload);
                fields.query_id = Some(query_id.0);
                vec![fields]
            }
            SyncPayload::QuerySettled { query_id, tier, .. } => {
                let mut fields = Self::base(payload);
                fields.query_id = Some(query_id.0);
                fields.durability_tier = Some(durability_tier_name(*tier));
                vec![fields]
            }
            SyncPayload::SchemaWarning(warning) => {
                let mut fields = Self::base(payload);
                fields.query_id = Some(warning.query_id.0);
                fields.table_name = Some(warning.table_name.clone());
                fields.schema_hash = Some(warning.to_hash.to_string());
                vec![fields]
            }
            SyncPayload::Error(error) => {
                let mut fields = Self::base(payload);
                let error_fields = sync_error_fields(error);
                fields.error_variant = Some(error_fields.variant.to_string());
                fields.error_code = error_fields.code;
                fields.query_id = error_fields.query_id.map(|id| id.0);
                if let Some((row_id, branch_name)) = error_fields.row {
                    fields.row_id = Some(row_id.to_string());
                    fields.branch_name = Some(branch_name.to_string());
                }
                vec![fields]
            }
            _ => vec![Self::base(payload)],
        }
    }

    fn base(payload: &SyncPayload) -> Self {
        Self {
            payload_variant: Some(payload.variant_name().to_string()),
            ..Default::default()
        }
    }

    fn row_payload(
        payload: &SyncPayload,
        row_id: ObjectId,
        branch_name: BranchName,
        batch_id: BatchId,
        confirmed_tier: Option<DurabilityTier>,
        derivation: FieldDerivation,
    ) -> Self {
        let mut fields = Self::base(payload);
        fields.row_id = Some(row_id.to_string());
        fields.branch_name = Some(branch_name.to_string());
        fields.batch_id = Some(batch_id.to_string());
        fields.durability_tier = confirmed_tier.map(durability_tier_name);
        fields.apply_derivation(derivation, true);
        fields
    }

    fn batch_settlement_payload(
        payload: &SyncPayload,
        settlement: &BatchSettlement,
        derivations: &FieldDerivations,
    ) -> Vec<Self> {
        match settlement {
            BatchSettlement::DurableDirect {
                batch_id,
                confirmed_tier,
                visible_members,
            }
            | BatchSettlement::AcceptedTransaction {
                batch_id,
                confirmed_tier,
                visible_members,
            } => {
                let member_count = visible_members.len();
                visible_members
                    .iter()
                    .enumerate()
                    .map(|(member_index, member)| {
                        let mut fields = Self::row_payload(
                            payload,
                            member.object_id,
                            member.branch_name,
                            *batch_id,
                            Some(*confirmed_tier),
                            derivations.for_member(Some(member_index)),
                        );
                        fields.batch_id = Some(member.batch_id.to_string());
                        fields.member_index = Some(member_index);
                        fields.member_count = Some(member_count);
                        fields
                    })
                    .collect()
            }
            BatchSettlement::Missing { batch_id } => {
                let mut fields = Self::base(payload);
                fields.batch_id = Some(batch_id.to_string());
                vec![fields]
            }
            BatchSettlement::Rejected { batch_id, code, .. } => {
                let mut fields = Self::base(payload);
                fields.batch_id = Some(batch_id.to_string());
                fields.error_variant = Some("BatchSettlementRejected".to_string());
                fields.error_code = Some(code.clone());
                vec![fields]
            }
        }
    }

    fn apply_derivation(&mut self, derivation: FieldDerivation, require_row_derivation: bool) {
        self.table_name = derivation.table_name;
        self.schema_hash = derivation.schema_hash.map(|hash| hash.to_string());

        self.table_name_error = derivation.table_name_error.or_else(|| {
            (require_row_derivation && self.table_name.is_none())
                .then(|| FIELD_DERIVATION_NOT_DERIVED.to_string())
        });
        self.schema_hash_error = derivation.schema_hash_error.or_else(|| {
            (require_row_derivation && self.schema_hash.is_none())
                .then(|| FIELD_DERIVATION_NOT_DERIVED.to_string())
        });
    }
}

#[derive(Debug, Clone)]
pub struct SyncPayloadTelemetryRecordInput<'a> {
    pub app_id: String,
    pub scope: SyncPayloadTelemetryScope,
    pub direction: SyncPayloadTelemetryDirection,
    pub client_id: Option<String>,
    pub connection_id: Option<String>,
    pub sequence: Option<u64>,
    pub source_frame_id: Option<String>,
    pub source_payload_index: Option<usize>,
    pub source_payload_count: Option<usize>,
    pub source_frame_bytes: Option<u64>,
    pub message_bytes: u64,
    pub message_encoding: SyncPayloadTelemetryMessageEncoding,
    pub recorded_at: u64,
    pub decode_error: Option<String>,
    pub payload: &'a SyncPayload,
    pub derivations: FieldDerivations,
}

#[derive(Debug, Clone)]
pub struct SyncPayloadTelemetryDecodeFailureInput {
    pub app_id: Option<String>,
    pub scope: SyncPayloadTelemetryScope,
    pub direction: SyncPayloadTelemetryDirection,
    pub client_id: Option<String>,
    pub connection_id: Option<String>,
    pub sequence: Option<u64>,
    pub source_frame_id: Option<String>,
    pub source_payload_index: Option<usize>,
    pub source_payload_count: Option<usize>,
    pub source_frame_bytes: Option<u64>,
    pub message_bytes: u64,
    pub message_encoding: SyncPayloadTelemetryMessageEncoding,
    pub recorded_at: u64,
    pub decode_error: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncPayloadTelemetryRecord {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub app_id: Option<String>,
    pub severity_text: String,
    pub scope: SyncPayloadTelemetryScope,
    pub direction: SyncPayloadTelemetryDirection,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub client_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub connection_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sequence: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_frame_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_payload_index: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_payload_count: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_frame_bytes: Option<u64>,
    pub message_bytes: u64,
    pub message_encoding: SyncPayloadTelemetryMessageEncoding,
    pub recorded_at: u64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub decode_error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub log_body: Option<Value>,
    #[serde(flatten)]
    pub fields: SyncPayloadTelemetryFields,
}

impl SyncPayloadTelemetryRecord {
    pub fn records_from_payload(
        payload: &SyncPayload,
        derivations: impl Into<FieldDerivations>,
    ) -> Vec<SyncPayloadTelemetryFields> {
        SyncPayloadTelemetryFields::records_from_payload(payload, derivations)
    }

    pub fn records_from_input(input: SyncPayloadTelemetryRecordInput<'_>) -> Vec<Self> {
        SyncPayloadTelemetryFields::records_from_payload_with_derivations(
            input.payload,
            &input.derivations,
        )
        .into_iter()
        .map(|fields| Self::from_fields(input.clone(), fields))
        .collect()
    }

    pub fn decode_failure(input: SyncPayloadTelemetryDecodeFailureInput) -> Self {
        Self {
            app_id: input.app_id,
            severity_text: SYNC_PAYLOAD_TELEMETRY_SEVERITY_TEXT.to_string(),
            scope: input.scope,
            direction: input.direction,
            client_id: input.client_id,
            connection_id: input.connection_id,
            sequence: input.sequence,
            source_frame_id: input.source_frame_id,
            source_payload_index: input.source_payload_index,
            source_payload_count: input.source_payload_count,
            source_frame_bytes: input.source_frame_bytes,
            message_bytes: input.message_bytes,
            message_encoding: input.message_encoding,
            recorded_at: input.recorded_at,
            decode_error: Some(input.decode_error),
            log_body: None,
            fields: SyncPayloadTelemetryFields::default(),
        }
    }

    fn from_fields(
        input: SyncPayloadTelemetryRecordInput<'_>,
        fields: SyncPayloadTelemetryFields,
    ) -> Self {
        let log_body = log_body_for_payload(input.payload);

        Self {
            app_id: Some(input.app_id),
            severity_text: SYNC_PAYLOAD_TELEMETRY_SEVERITY_TEXT.to_string(),
            scope: input.scope,
            direction: input.direction,
            client_id: input.client_id,
            connection_id: input.connection_id,
            sequence: input.sequence,
            source_frame_id: input.source_frame_id,
            source_payload_index: input.source_payload_index,
            source_payload_count: input.source_payload_count,
            source_frame_bytes: input.source_frame_bytes,
            message_bytes: input.message_bytes,
            message_encoding: input.message_encoding,
            recorded_at: input.recorded_at,
            decode_error: input.decode_error,
            log_body,
            fields,
        }
    }
}

impl Deref for SyncPayloadTelemetryRecord {
    type Target = SyncPayloadTelemetryFields;

    fn deref(&self) -> &Self::Target {
        &self.fields
    }
}

pub fn records_from_payload(
    payload: &SyncPayload,
    derivations: impl Into<FieldDerivations>,
) -> Vec<SyncPayloadTelemetryFields> {
    SyncPayloadTelemetryFields::records_from_payload(payload, derivations)
}

fn log_body_for_payload(payload: &SyncPayload) -> Option<Value> {
    if !is_error_or_failure_payload(payload) {
        return None;
    }

    payload
        .to_json()
        .ok()
        .and_then(|json| serde_json::from_str(&json).ok())
}

fn is_error_or_failure_payload(payload: &SyncPayload) -> bool {
    matches!(
        payload,
        SyncPayload::Error(_)
            | SyncPayload::BatchSettlement {
                settlement: BatchSettlement::Rejected { .. }
            }
    )
}

struct SyncErrorTelemetryFields {
    variant: &'static str,
    code: Option<String>,
    query_id: Option<QueryId>,
    row: Option<(ObjectId, BranchName)>,
}

fn sync_error_fields(error: &SyncError) -> SyncErrorTelemetryFields {
    match error {
        SyncError::PermissionDenied {
            object_id,
            branch_name,
            code,
            ..
        } => SyncErrorTelemetryFields {
            variant: "PermissionDenied",
            code: Some(code.clone()),
            query_id: None,
            row: Some((*object_id, *branch_name)),
        },
        SyncError::SessionRequired {
            object_id,
            branch_name,
        } => SyncErrorTelemetryFields {
            variant: "SessionRequired",
            code: None,
            query_id: None,
            row: Some((*object_id, *branch_name)),
        },
        SyncError::CatalogueWriteDenied {
            object_id,
            branch_name,
        } => SyncErrorTelemetryFields {
            variant: "CatalogueWriteDenied",
            code: None,
            query_id: None,
            row: Some((*object_id, *branch_name)),
        },
        SyncError::QuerySubscriptionRejected { query_id, code, .. } => SyncErrorTelemetryFields {
            variant: "QuerySubscriptionRejected",
            code: Some(code.clone()),
            query_id: Some(*query_id),
            row: None,
        },
    }
}

fn durability_tier_name(tier: DurabilityTier) -> String {
    match tier {
        DurabilityTier::Local => "Local",
        DurabilityTier::EdgeServer => "EdgeServer",
        DurabilityTier::GlobalServer => "GlobalServer",
    }
    .to_string()
}

#[cfg(test)]
mod tests {
    use serde_json::json;

    use crate::batch_fate::{BatchSettlement, VisibleBatchMember};
    use crate::metadata::RowProvenance;
    use crate::object::{BranchName, ObjectId};
    use crate::query_manager::types::SchemaHash;
    use crate::row_histories::{BatchId, RowState, StoredRowBatch};
    use crate::sync_manager::{DurabilityTier, QueryId, SyncError, SyncPayload};
    use crate::sync_payload_telemetry::{
        FieldDerivation, FieldDerivations, SyncPayloadTelemetryDecodeFailureInput,
        SyncPayloadTelemetryDirection, SyncPayloadTelemetryFields,
        SyncPayloadTelemetryMessageEncoding, SyncPayloadTelemetryRecord,
        SyncPayloadTelemetryRecordInput, SyncPayloadTelemetryScope,
    };

    #[test]
    fn query_settled_fields_are_structured() {
        let payload = SyncPayload::QuerySettled {
            query_id: QueryId(42),
            tier: DurabilityTier::EdgeServer,
            through_seq: 99,
        };

        let records =
            SyncPayloadTelemetryRecord::records_from_input(SyncPayloadTelemetryRecordInput {
                app_id: "app_todos".to_string(),
                scope: SyncPayloadTelemetryScope::Websocket,
                direction: SyncPayloadTelemetryDirection::ServerToClient,
                client_id: Some("alice".to_string()),
                connection_id: Some("conn-1".to_string()),
                sequence: Some(7),
                source_frame_id: None,
                source_payload_index: None,
                source_payload_count: None,
                source_frame_bytes: None,
                message_bytes: 32,
                message_encoding: SyncPayloadTelemetryMessageEncoding::Binary,
                recorded_at: 1_775_000_000_000,
                decode_error: None,
                payload: &payload,
                derivations: FieldDerivation::default().into(),
            });

        assert_eq!(records.len(), 1);
        let record = &records[0];
        assert_eq!(record.payload_variant.as_deref(), Some("QuerySettled"));
        assert_eq!(record.query_id, Some(42));
        assert_eq!(record.durability_tier.as_deref(), Some("EdgeServer"));
        assert_eq!(record.log_body, None);
        assert_eq!(record.severity_text, "DEBUG");
    }

    #[test]
    fn row_payload_records_derivation_errors_when_table_and_schema_are_missing() {
        let row_id = ObjectId::new();
        let batch_id = BatchId::new();
        let payload = SyncPayload::RowBatchCreated {
            metadata: None,
            row: row_batch(row_id, batch_id, "main"),
        };

        let fields =
            SyncPayloadTelemetryRecord::records_from_payload(&payload, FieldDerivation::default());

        assert_eq!(fields.len(), 1);
        assert_eq!(
            fields[0].payload_variant.as_deref(),
            Some("RowBatchCreated")
        );
        assert_eq!(
            fields[0].row_id.as_deref(),
            Some(row_id.to_string().as_str())
        );
        assert_eq!(fields[0].branch_name.as_deref(), Some("main"));
        assert_eq!(
            fields[0].batch_id.as_deref(),
            Some(batch_id.to_string().as_str())
        );
        assert_eq!(fields[0].table_name_error.as_deref(), Some("not_derived"));
        assert_eq!(fields[0].schema_hash_error.as_deref(), Some("not_derived"));
    }

    #[test]
    fn sync_error_payload_sets_error_fields_and_log_body() {
        let payload = SyncPayload::Error(SyncError::QuerySubscriptionRejected {
            query_id: QueryId(9),
            code: "query_compile_failed".to_string(),
            reason: "invalid predicate".to_string(),
        });

        let records =
            SyncPayloadTelemetryRecord::records_from_input(SyncPayloadTelemetryRecordInput {
                app_id: "app_todos".to_string(),
                scope: SyncPayloadTelemetryScope::WorkerBridge,
                direction: SyncPayloadTelemetryDirection::WorkerToMain,
                client_id: None,
                connection_id: None,
                sequence: Some(3),
                source_frame_id: Some("frame-1".to_string()),
                source_payload_index: Some(0),
                source_payload_count: Some(1),
                source_frame_bytes: Some(128),
                message_bytes: 80,
                message_encoding: SyncPayloadTelemetryMessageEncoding::Utf8,
                recorded_at: 1_775_000_000_001,
                decode_error: None,
                payload: &payload,
                derivations: FieldDerivation::default().into(),
            });

        assert_eq!(records.len(), 1);
        let record = &records[0];
        assert_eq!(record.payload_variant.as_deref(), Some("Error"));
        assert_eq!(
            record.error_variant.as_deref(),
            Some("QuerySubscriptionRejected")
        );
        assert_eq!(record.error_code.as_deref(), Some("query_compile_failed"));
        assert_eq!(
            record.log_body,
            Some(json!({
                "Error": {
                    "QuerySubscriptionRejected": {
                        "query_id": 9,
                        "code": "query_compile_failed",
                        "reason": "invalid predicate"
                    }
                }
            }))
        );
    }

    #[test]
    fn multi_member_payloads_expand_per_member() {
        let batch_id = BatchId::new();
        let alice_row = ObjectId::new();
        let bob_row = ObjectId::new();
        let payload = SyncPayload::BatchSettlement {
            settlement: BatchSettlement::AcceptedTransaction {
                batch_id,
                confirmed_tier: DurabilityTier::GlobalServer,
                visible_members: vec![
                    VisibleBatchMember {
                        object_id: alice_row,
                        branch_name: BranchName::new("main"),
                        batch_id,
                    },
                    VisibleBatchMember {
                        object_id: bob_row,
                        branch_name: BranchName::new("draft"),
                        batch_id,
                    },
                ],
            },
        };

        let fields = SyncPayloadTelemetryRecord::records_from_payload(
            &payload,
            FieldDerivation {
                table_name: Some("todos".to_string()),
                schema_hash: Some(SchemaHash::from_bytes([7; 32])),
                ..Default::default()
            },
        );

        assert_eq!(fields.len(), 2);
        assert_eq!(
            fields[0].payload_variant.as_deref(),
            Some("BatchSettlement")
        );
        assert_eq!(
            fields[0].row_id.as_deref(),
            Some(alice_row.to_string().as_str())
        );
        assert_eq!(fields[0].branch_name.as_deref(), Some("main"));
        assert_eq!(fields[0].member_index, Some(0));
        assert_eq!(fields[0].member_count, Some(2));
        assert_eq!(fields[0].table_name.as_deref(), Some("todos"));
        let expected_schema_hash = "07".repeat(32);
        assert_eq!(
            fields[0].schema_hash.as_deref(),
            Some(expected_schema_hash.as_str())
        );
        assert_eq!(
            fields[1].row_id.as_deref(),
            Some(bob_row.to_string().as_str())
        );
        assert_eq!(fields[1].branch_name.as_deref(), Some("draft"));
        assert_eq!(fields[1].member_index, Some(1));
        assert_eq!(fields[1].member_count, Some(2));
    }

    #[test]
    fn record_input_expands_multi_member_payloads() {
        let batch_id = BatchId::new();
        let alice_row = ObjectId::new();
        let bob_row = ObjectId::new();
        let payload = SyncPayload::BatchSettlement {
            settlement: BatchSettlement::DurableDirect {
                batch_id,
                confirmed_tier: DurabilityTier::EdgeServer,
                visible_members: vec![
                    VisibleBatchMember {
                        object_id: alice_row,
                        branch_name: BranchName::new("main"),
                        batch_id,
                    },
                    VisibleBatchMember {
                        object_id: bob_row,
                        branch_name: BranchName::new("main"),
                        batch_id,
                    },
                ],
            },
        };

        let records =
            SyncPayloadTelemetryRecord::records_from_input(SyncPayloadTelemetryRecordInput {
                app_id: "app_todos".to_string(),
                scope: SyncPayloadTelemetryScope::Websocket,
                direction: SyncPayloadTelemetryDirection::ServerToClient,
                client_id: Some("alice".to_string()),
                connection_id: Some("conn-1".to_string()),
                sequence: None,
                source_frame_id: Some("frame-1".to_string()),
                source_payload_index: Some(0),
                source_payload_count: Some(1),
                source_frame_bytes: Some(256),
                message_bytes: 96,
                message_encoding: SyncPayloadTelemetryMessageEncoding::Binary,
                recorded_at: 1_775_000_000_000,
                decode_error: None,
                payload: &payload,
                derivations: FieldDerivation::default().into(),
            });

        assert_eq!(records.len(), 2);
        assert_eq!(
            records[0].row_id.as_deref(),
            Some(alice_row.to_string().as_str())
        );
        assert_eq!(records[0].sequence, None);
        assert_eq!(
            records[1].row_id.as_deref(),
            Some(bob_row.to_string().as_str())
        );
        assert_eq!(records[1].member_index, Some(1));
    }

    #[test]
    fn rejected_batch_settlement_includes_full_parsed_payload_body() {
        let batch_id = BatchId::new();
        let payload = SyncPayload::BatchSettlement {
            settlement: BatchSettlement::Rejected {
                batch_id,
                code: "foreign_key_failed".to_string(),
                reason: "todo list missing".to_string(),
            },
        };

        let records =
            SyncPayloadTelemetryRecord::records_from_input(SyncPayloadTelemetryRecordInput {
                app_id: "app_todos".to_string(),
                scope: SyncPayloadTelemetryScope::Websocket,
                direction: SyncPayloadTelemetryDirection::ClientToServer,
                client_id: Some("alice".to_string()),
                connection_id: Some("conn-1".to_string()),
                sequence: Some(11),
                source_frame_id: Some("frame-2".to_string()),
                source_payload_index: Some(0),
                source_payload_count: Some(1),
                source_frame_bytes: Some(128),
                message_bytes: 80,
                message_encoding: SyncPayloadTelemetryMessageEncoding::Binary,
                recorded_at: 1_775_000_000_001,
                decode_error: None,
                payload: &payload,
                derivations: FieldDerivation::default().into(),
            });

        assert_eq!(records.len(), 1);
        let record = &records[0];
        assert_eq!(
            record.error_variant.as_deref(),
            Some("BatchSettlementRejected")
        );
        assert_eq!(record.error_code.as_deref(), Some("foreign_key_failed"));
        assert_eq!(
            record.log_body,
            Some(json!({
                "BatchSettlement": {
                    "settlement": {
                        "Rejected": {
                            "batch_id": batch_id.to_string(),
                            "code": "foreign_key_failed",
                            "reason": "todo list missing"
                        }
                    }
                }
            }))
        );
    }

    #[test]
    fn multi_member_payloads_keep_member_specific_derivation() {
        let batch_id = BatchId::new();
        let alice_row = ObjectId::new();
        let bob_row = ObjectId::new();
        let payload = SyncPayload::BatchSettlement {
            settlement: BatchSettlement::AcceptedTransaction {
                batch_id,
                confirmed_tier: DurabilityTier::GlobalServer,
                visible_members: vec![
                    VisibleBatchMember {
                        object_id: alice_row,
                        branch_name: BranchName::new("main"),
                        batch_id,
                    },
                    VisibleBatchMember {
                        object_id: bob_row,
                        branch_name: BranchName::new("draft"),
                        batch_id,
                    },
                ],
            },
        };

        let records =
            SyncPayloadTelemetryRecord::records_from_input(SyncPayloadTelemetryRecordInput {
                app_id: "app_todos".to_string(),
                scope: SyncPayloadTelemetryScope::Websocket,
                direction: SyncPayloadTelemetryDirection::ServerToClient,
                client_id: Some("alice".to_string()),
                connection_id: Some("conn-1".to_string()),
                sequence: Some(12),
                source_frame_id: Some("frame-3".to_string()),
                source_payload_index: Some(0),
                source_payload_count: Some(1),
                source_frame_bytes: Some(256),
                message_bytes: 96,
                message_encoding: SyncPayloadTelemetryMessageEncoding::Binary,
                recorded_at: 1_775_000_000_002,
                decode_error: None,
                payload: &payload,
                derivations: FieldDerivations::by_member(vec![
                    FieldDerivation {
                        table_name: Some("todos".to_string()),
                        schema_hash: Some(SchemaHash::from_bytes([1; 32])),
                        ..Default::default()
                    },
                    FieldDerivation {
                        table_name: Some("comments".to_string()),
                        schema_hash: Some(SchemaHash::from_bytes([2; 32])),
                        ..Default::default()
                    },
                ]),
            });

        assert_eq!(records.len(), 2);
        assert_eq!(records[0].table_name.as_deref(), Some("todos"));
        assert_eq!(
            records[0].schema_hash.as_deref(),
            Some("01".repeat(32).as_str())
        );
        assert_eq!(records[1].table_name.as_deref(), Some("comments"));
        assert_eq!(
            records[1].schema_hash.as_deref(),
            Some("02".repeat(32).as_str())
        );
    }

    #[test]
    fn decode_failure_record_has_context_but_no_payload_body_or_variant() {
        let record =
            SyncPayloadTelemetryRecord::decode_failure(SyncPayloadTelemetryDecodeFailureInput {
                app_id: None,
                scope: SyncPayloadTelemetryScope::WorkerBridge,
                direction: SyncPayloadTelemetryDirection::MainToWorker,
                client_id: Some("alice".to_string()),
                connection_id: None,
                sequence: Some(13),
                source_frame_id: Some("frame-4".to_string()),
                source_payload_index: Some(0),
                source_payload_count: Some(1),
                source_frame_bytes: Some(24),
                message_bytes: 24,
                message_encoding: SyncPayloadTelemetryMessageEncoding::Utf8,
                recorded_at: 1_775_000_000_003,
                decode_error: "invalid sync payload".to_string(),
            });

        assert_eq!(record.app_id, None);
        assert_eq!(record.decode_error.as_deref(), Some("invalid sync payload"));
        assert_eq!(record.payload_variant, None);
        assert_eq!(record.log_body, None);
    }

    #[test]
    fn serialized_record_uses_browser_ingest_contract_keys() {
        let record = SyncPayloadTelemetryRecord {
            app_id: Some("app_todos".to_string()),
            severity_text: "DEBUG".to_string(),
            scope: SyncPayloadTelemetryScope::WorkerBridge,
            direction: SyncPayloadTelemetryDirection::ClientToServer,
            client_id: Some("alice".to_string()),
            connection_id: None,
            sequence: Some(14),
            source_frame_id: Some("frame-5".to_string()),
            source_payload_index: Some(0),
            source_payload_count: Some(1),
            source_frame_bytes: Some(100),
            message_bytes: 50,
            message_encoding: SyncPayloadTelemetryMessageEncoding::Binary,
            recorded_at: 1_775_000_000_004,
            decode_error: None,
            log_body: Some(json!({ "Error": { "SessionRequired": {} } })),
            fields: SyncPayloadTelemetryFields {
                payload_variant: Some("Error".to_string()),
                error_variant: Some("SessionRequired".to_string()),
                table_name_error: Some("not_derived".to_string()),
                ..Default::default()
            },
        };

        assert_eq!(
            serde_json::to_value(&record).unwrap(),
            json!({
                "appId": "app_todos",
                "severityText": "DEBUG",
                "scope": "worker_bridge",
                "direction": "client_to_server",
                "clientId": "alice",
                "sequence": 14,
                "sourceFrameId": "frame-5",
                "sourcePayloadIndex": 0,
                "sourcePayloadCount": 1,
                "sourceFrameBytes": 100,
                "messageBytes": 50,
                "messageEncoding": "binary",
                "recordedAt": 1_775_000_000_004_u64,
                "logBody": { "Error": { "SessionRequired": {} } },
                "payloadVariant": "Error",
                "tableNameError": "not_derived",
                "errorVariant": "SessionRequired"
            })
        );

        let without_app: SyncPayloadTelemetryRecord = serde_json::from_value(json!({
            "severityText": "DEBUG",
            "scope": "worker_bridge",
            "direction": "main_to_worker",
            "messageBytes": 12,
            "messageEncoding": "utf8",
            "recordedAt": 1_775_000_000_005_u64
        }))
        .unwrap();
        assert_eq!(without_app.app_id, None);
    }

    fn row_batch(row_id: ObjectId, batch_id: BatchId, branch: &str) -> StoredRowBatch {
        StoredRowBatch::new_with_batch_id(
            batch_id,
            row_id,
            branch,
            [],
            vec![1, 2, 3],
            RowProvenance {
                created_by: "alice".to_string(),
                created_at: 1,
                updated_by: "alice".to_string(),
                updated_at: 2,
            },
            Default::default(),
            RowState::VisibleDirect,
            Some(DurabilityTier::Local),
        )
    }
}
