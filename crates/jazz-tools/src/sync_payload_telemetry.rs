use serde::{Deserialize, Serialize};
use serde_json::{Value as JsonValue, json};

use crate::batch_fate::{BatchSettlement, VisibleBatchMember};
use crate::metadata::MetadataKey;
use crate::row_histories::StoredRowBatch;
use crate::sync_manager::{SyncError, SyncPayload};

pub const DEFAULT_TELEMETRY_COLLECTOR_URL: &str = "http://localhost:4318";

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SyncPayloadTelemetryScope {
    #[default]
    WorkerBridge,
    Websocket,
}

impl SyncPayloadTelemetryScope {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::WorkerBridge => "worker_bridge",
            Self::Websocket => "websocket",
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SyncPayloadTelemetryDirection {
    #[default]
    MainToWorker,
    WorkerToMain,
    ClientToServer,
    ServerToClient,
}

impl SyncPayloadTelemetryDirection {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::MainToWorker => "main_to_worker",
            Self::WorkerToMain => "worker_to_main",
            Self::ClientToServer => "client_to_server",
            Self::ServerToClient => "server_to_client",
        }
    }
}

#[derive(Debug, Clone, Default)]
pub struct SyncPayloadTelemetryContext {
    pub app_id: Option<String>,
    pub scope: SyncPayloadTelemetryScope,
    pub direction: SyncPayloadTelemetryDirection,
    pub client_id: Option<String>,
    pub connection_id: Option<String>,
    pub sequence: Option<u64>,
    pub source_frame_id: Option<String>,
    pub source_payload_index: Option<usize>,
    pub source_payload_count: Option<usize>,
    pub source_frame_bytes: Option<usize>,
    pub message_bytes: Option<usize>,
    pub message_encoding: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
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
    pub source_frame_bytes: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message_bytes: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message_encoding: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub recorded_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub decode_error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub log_body: Option<JsonValue>,
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
    pub query_id: Option<String>,
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

impl SyncPayloadTelemetryRecord {
    pub fn decode_failure(context: &SyncPayloadTelemetryContext, error: impl Into<String>) -> Self {
        let mut record = Self::from_context(context);
        record.decode_error = Some(error.into());
        record
    }

    fn from_context(context: &SyncPayloadTelemetryContext) -> Self {
        Self {
            app_id: context.app_id.clone(),
            severity_text: "DEBUG".to_string(),
            scope: context.scope,
            direction: context.direction,
            client_id: context.client_id.clone(),
            connection_id: context.connection_id.clone(),
            sequence: context.sequence,
            source_frame_id: context.source_frame_id.clone(),
            source_payload_index: context.source_payload_index,
            source_payload_count: context.source_payload_count,
            source_frame_bytes: context.source_frame_bytes,
            message_bytes: context.message_bytes,
            message_encoding: context.message_encoding.clone(),
            recorded_at: Some(unix_timestamp_millis().to_string()),
            decode_error: None,
            log_body: None,
            payload_variant: None,
            row_id: None,
            table_name: None,
            table_name_error: None,
            branch_name: None,
            batch_id: None,
            query_id: None,
            schema_hash: None,
            schema_hash_error: None,
            durability_tier: None,
            error_variant: None,
            error_code: None,
            member_index: None,
            member_count: None,
        }
    }
}

pub fn resolve_telemetry_collector_url(telemetry: Option<&TelemetryOptions>) -> Option<String> {
    match telemetry {
        Some(TelemetryOptions::Enabled(true)) => Some(DEFAULT_TELEMETRY_COLLECTOR_URL.to_string()),
        Some(TelemetryOptions::Enabled(false)) | None => None,
        Some(TelemetryOptions::Config(config)) => config
            .collector_url
            .as_deref()
            .map(str::trim)
            .filter(|value| !value.is_empty())
            .map(ToOwned::to_owned)
            .or_else(|| Some(DEFAULT_TELEMETRY_COLLECTOR_URL.to_string())),
    }
}

#[derive(Debug, Clone, Deserialize)]
#[serde(untagged)]
pub enum TelemetryOptions {
    Enabled(bool),
    Config(TelemetryConfig),
}

#[derive(Debug, Clone, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct TelemetryConfig {
    pub collector_url: Option<String>,
}

pub fn records_for_payload(
    context: &SyncPayloadTelemetryContext,
    payload: &SyncPayload,
) -> Vec<SyncPayloadTelemetryRecord> {
    match payload {
        SyncPayload::RowBatchCreated { row, .. } | SyncPayload::RowBatchNeeded { row, .. } => {
            vec![row_record(context, payload, row)]
        }
        SyncPayload::RowBatchStateChanged {
            row_id,
            branch_name,
            batch_id,
            confirmed_tier,
            ..
        } => {
            let mut record = base_payload_record(context, payload);
            record.row_id = Some(row_id.to_string());
            record.branch_name = Some(branch_name.to_string());
            record.batch_id = Some(batch_id.to_string());
            record.durability_tier = confirmed_tier.map(|tier| format!("{tier:?}"));
            vec![record]
        }
        SyncPayload::BatchSettlement { settlement } => {
            settlement_records(context, payload, settlement)
        }
        SyncPayload::SealBatch { submission } => {
            let member_count = submission.members.len();
            if member_count == 0 {
                let mut record = base_payload_record(context, payload);
                record.batch_id = Some(submission.batch_id.to_string());
                record.member_count = Some(0);
                return vec![record];
            }
            submission
                .members
                .iter()
                .enumerate()
                .map(|(index, member)| {
                    let mut record = base_payload_record(context, payload);
                    record.batch_id = Some(submission.batch_id.to_string());
                    record.row_id = Some(member.object_id.to_string());
                    record.branch_name = Some(submission.target_branch_name.to_string());
                    record.member_index = Some(index);
                    record.member_count = Some(member_count);
                    record
                })
                .collect()
        }
        SyncPayload::BatchSettlementNeeded { batch_ids } => {
            let mut record = base_payload_record(context, payload);
            record.member_count = Some(batch_ids.len());
            vec![record]
        }
        SyncPayload::QuerySubscription { query_id, .. }
        | SyncPayload::QueryUnsubscription { query_id }
        | SyncPayload::QueryScopeSnapshot { query_id, .. }
        | SyncPayload::QuerySettled { query_id, .. }
        | SyncPayload::SchemaWarning(crate::sync_manager::SchemaWarning { query_id, .. }) => {
            let mut record = base_payload_record(context, payload);
            record.query_id = Some(query_id.0.to_string());
            vec![record]
        }
        SyncPayload::Error(error) => {
            let mut record = base_payload_record(context, payload);
            apply_error_fields(&mut record, error);
            record.log_body = serde_json::to_value(payload).ok();
            vec![record]
        }
        SyncPayload::CatalogueEntryUpdated { entry } => {
            let mut record = base_payload_record(context, payload);
            record.row_id = Some(entry.object_id.to_string());
            vec![record]
        }
        SyncPayload::ConnectionSchemaDiagnostics(_) => vec![base_payload_record(context, payload)],
    }
}

pub fn normalize_otlp_endpoint(collector_url: &str, signal: &str) -> String {
    let trimmed = collector_url.trim().trim_end_matches('/');
    let suffix = match signal {
        "traces" => "/v1/traces",
        _ => "/v1/logs",
    };
    if let Some(base) = trimmed.strip_suffix("/v1/logs") {
        return if signal == "logs" {
            trimmed.to_string()
        } else {
            format!("{base}{suffix}")
        };
    }
    if let Some(base) = trimmed.strip_suffix("/v1/traces") {
        return if signal == "traces" {
            trimmed.to_string()
        } else {
            format!("{base}{suffix}")
        };
    }
    format!("{trimmed}{suffix}")
}

fn row_record(
    context: &SyncPayloadTelemetryContext,
    payload: &SyncPayload,
    row: &StoredRowBatch,
) -> SyncPayloadTelemetryRecord {
    let mut record = base_payload_record(context, payload);
    record.row_id = Some(row.row_id.to_string());
    record.branch_name = Some(row.branch.to_string());
    record.batch_id = Some(row.batch_id.to_string());
    record.durability_tier = row.confirmed_tier.map(|tier| format!("{tier:?}"));
    match row.metadata.get(MetadataKey::Table.as_str()) {
        Some(table_name) => record.table_name = Some(table_name.clone()),
        None => record.table_name_error = Some("missing row table metadata".to_string()),
    }
    match row.metadata.get(MetadataKey::OriginSchemaHash.as_str()) {
        Some(schema_hash) => record.schema_hash = Some(schema_hash.clone()),
        None => {
            record.schema_hash_error = Some("missing row origin_schema_hash metadata".to_string())
        }
    }
    record
}

fn settlement_records(
    context: &SyncPayloadTelemetryContext,
    payload: &SyncPayload,
    settlement: &BatchSettlement,
) -> Vec<SyncPayloadTelemetryRecord> {
    match settlement {
        BatchSettlement::Missing { batch_id } => {
            let mut record = base_payload_record(context, payload);
            record.batch_id = Some(batch_id.to_string());
            record.error_variant = Some("Missing".to_string());
            vec![record]
        }
        BatchSettlement::Rejected {
            batch_id,
            code,
            reason: _,
        } => {
            let mut record = base_payload_record(context, payload);
            record.batch_id = Some(batch_id.to_string());
            record.error_variant = Some("Rejected".to_string());
            record.error_code = Some(code.clone());
            record.log_body = Some(json!({ "settlement": settlement }));
            vec![record]
        }
        BatchSettlement::DurableDirect {
            batch_id,
            confirmed_tier,
            visible_members,
        } => visible_member_records(
            context,
            payload,
            *batch_id,
            *confirmed_tier,
            visible_members,
        ),
        BatchSettlement::AcceptedTransaction {
            batch_id,
            confirmed_tier,
            visible_members,
        } => visible_member_records(
            context,
            payload,
            *batch_id,
            *confirmed_tier,
            visible_members,
        ),
    }
}

fn visible_member_records(
    context: &SyncPayloadTelemetryContext,
    payload: &SyncPayload,
    settlement_batch_id: crate::row_histories::BatchId,
    confirmed_tier: crate::sync_manager::DurabilityTier,
    visible_members: &[VisibleBatchMember],
) -> Vec<SyncPayloadTelemetryRecord> {
    if visible_members.is_empty() {
        let mut record = base_payload_record(context, payload);
        record.batch_id = Some(settlement_batch_id.to_string());
        record.durability_tier = Some(format!("{confirmed_tier:?}"));
        record.member_count = Some(0);
        return vec![record];
    }

    visible_members
        .iter()
        .enumerate()
        .map(|(index, member)| {
            let mut record = base_payload_record(context, payload);
            record.batch_id = Some(member.batch_id.to_string());
            record.row_id = Some(member.object_id.to_string());
            record.branch_name = Some(member.branch_name.to_string());
            record.durability_tier = Some(format!("{confirmed_tier:?}"));
            record.member_index = Some(index);
            record.member_count = Some(visible_members.len());
            record
        })
        .collect()
}

fn base_payload_record(
    context: &SyncPayloadTelemetryContext,
    payload: &SyncPayload,
) -> SyncPayloadTelemetryRecord {
    let mut record = SyncPayloadTelemetryRecord::from_context(context);
    record.payload_variant = Some(payload.variant_name().to_string());
    record
}

fn apply_error_fields(record: &mut SyncPayloadTelemetryRecord, error: &SyncError) {
    match error {
        SyncError::PermissionDenied {
            object_id,
            branch_name,
            code,
            ..
        } => {
            record.error_variant = Some("PermissionDenied".to_string());
            record.error_code = Some(code.clone());
            record.row_id = Some(object_id.to_string());
            record.branch_name = Some(branch_name.to_string());
        }
        SyncError::SessionRequired {
            object_id,
            branch_name,
        } => {
            record.error_variant = Some("SessionRequired".to_string());
            record.row_id = Some(object_id.to_string());
            record.branch_name = Some(branch_name.to_string());
        }
        SyncError::CatalogueWriteDenied {
            object_id,
            branch_name,
        } => {
            record.error_variant = Some("CatalogueWriteDenied".to_string());
            record.row_id = Some(object_id.to_string());
            record.branch_name = Some(branch_name.to_string());
        }
        SyncError::QuerySubscriptionRejected { query_id, code, .. } => {
            record.error_variant = Some("QuerySubscriptionRejected".to_string());
            record.error_code = Some(code.clone());
            record.query_id = Some(query_id.0.to_string());
        }
    }
}

#[cfg(any(feature = "server", feature = "test-utils"))]
#[derive(Clone)]
pub struct SyncPayloadTelemetrySink {
    sender: tokio::sync::mpsc::Sender<SyncPayloadTelemetryRecord>,
}

#[cfg(any(feature = "server", feature = "test-utils"))]
impl SyncPayloadTelemetrySink {
    pub fn new(collector_url: String) -> Self {
        let (sender, mut receiver) = tokio::sync::mpsc::channel(1024);
        tokio::spawn(async move {
            let client = reqwest::Client::new();
            let endpoint = normalize_otlp_endpoint(&collector_url, "logs");
            while let Some(record) = receiver.recv().await {
                let body = build_otlp_log_request(&record);
                let _ = client.post(&endpoint).json(&body).send().await;
            }
        });
        Self { sender }
    }

    pub fn emit(&self, record: SyncPayloadTelemetryRecord) {
        let _ = self.sender.try_send(record);
    }

    pub fn emit_many(&self, records: Vec<SyncPayloadTelemetryRecord>) {
        for record in records {
            self.emit(record);
        }
    }
}

#[cfg(any(feature = "server", feature = "test-utils"))]
fn build_otlp_log_request(record: &SyncPayloadTelemetryRecord) -> JsonValue {
    json!({
        "resourceLogs": [{
            "resource": {
                "attributes": [
                    otlp_string_attr("service.name", "jazz-server"),
                    otlp_string_attr("telemetry.sdk.language", "rust"),
                ],
            },
            "scopeLogs": [{
                "scope": { "name": "jazz-server.sync-payload" },
                "logRecords": [{
                    "timeUnixNano": unix_timestamp_nanos().to_string(),
                    "severityNumber": 5,
                    "severityText": "DEBUG",
                    "body": { "stringValue": serde_json::to_string(record).unwrap_or_default() },
                    "attributes": record_attributes(record),
                }]
            }]
        }]
    })
}

#[cfg(any(feature = "server", feature = "test-utils"))]
fn record_attributes(record: &SyncPayloadTelemetryRecord) -> Vec<JsonValue> {
    let mut attrs = Vec::new();
    push_string_attr(&mut attrs, "jazz.app_id", record.app_id.as_deref());
    push_string_attr(&mut attrs, "jazz.scope", Some(record.scope.as_str()));
    push_string_attr(
        &mut attrs,
        "jazz.direction",
        Some(record.direction.as_str()),
    );
    push_string_attr(&mut attrs, "jazz.client_id", record.client_id.as_deref());
    push_string_attr(
        &mut attrs,
        "jazz.connection_id",
        record.connection_id.as_deref(),
    );
    push_int_attr(&mut attrs, "jazz.sequence", record.sequence);
    push_string_attr(
        &mut attrs,
        "jazz.source_frame_id",
        record.source_frame_id.as_deref(),
    );
    push_int_attr(
        &mut attrs,
        "jazz.source_payload_index",
        record.source_payload_index.map(|value| value as u64),
    );
    push_int_attr(
        &mut attrs,
        "jazz.source_payload_count",
        record.source_payload_count.map(|value| value as u64),
    );
    push_int_attr(
        &mut attrs,
        "jazz.source_frame_bytes",
        record.source_frame_bytes.map(|value| value as u64),
    );
    push_int_attr(
        &mut attrs,
        "jazz.message_bytes",
        record.message_bytes.map(|value| value as u64),
    );
    push_string_attr(
        &mut attrs,
        "jazz.message_encoding",
        record.message_encoding.as_deref(),
    );
    push_string_attr(
        &mut attrs,
        "jazz.decode_error",
        record.decode_error.as_deref(),
    );
    push_string_attr(
        &mut attrs,
        "jazz.payload_variant",
        record.payload_variant.as_deref(),
    );
    push_string_attr(&mut attrs, "jazz.row_id", record.row_id.as_deref());
    push_string_attr(&mut attrs, "jazz.table_name", record.table_name.as_deref());
    push_string_attr(
        &mut attrs,
        "jazz.table_name_error",
        record.table_name_error.as_deref(),
    );
    push_string_attr(
        &mut attrs,
        "jazz.branch_name",
        record.branch_name.as_deref(),
    );
    push_string_attr(&mut attrs, "jazz.batch_id", record.batch_id.as_deref());
    push_string_attr(&mut attrs, "jazz.query_id", record.query_id.as_deref());
    push_string_attr(
        &mut attrs,
        "jazz.schema_hash",
        record.schema_hash.as_deref(),
    );
    push_string_attr(
        &mut attrs,
        "jazz.schema_hash_error",
        record.schema_hash_error.as_deref(),
    );
    push_string_attr(
        &mut attrs,
        "jazz.durability_tier",
        record.durability_tier.as_deref(),
    );
    push_string_attr(
        &mut attrs,
        "jazz.error_variant",
        record.error_variant.as_deref(),
    );
    push_string_attr(&mut attrs, "jazz.error_code", record.error_code.as_deref());
    push_int_attr(
        &mut attrs,
        "jazz.member_index",
        record.member_index.map(|value| value as u64),
    );
    push_int_attr(
        &mut attrs,
        "jazz.member_count",
        record.member_count.map(|value| value as u64),
    );
    attrs
}

#[cfg(any(feature = "server", feature = "test-utils"))]
fn push_string_attr(attrs: &mut Vec<JsonValue>, key: &str, value: Option<&str>) {
    if let Some(value) = value {
        attrs.push(otlp_string_attr(key, value));
    }
}

#[cfg(any(feature = "server", feature = "test-utils"))]
fn push_int_attr(attrs: &mut Vec<JsonValue>, key: &str, value: Option<u64>) {
    if let Some(value) = value {
        attrs.push(json!({ "key": key, "value": { "intValue": value.to_string() } }));
    }
}

#[cfg(any(feature = "server", feature = "test-utils"))]
fn otlp_string_attr(key: &str, value: &str) -> JsonValue {
    json!({ "key": key, "value": { "stringValue": value } })
}

fn unix_timestamp_millis() -> u128 {
    web_time::SystemTime::now()
        .duration_since(web_time::UNIX_EPOCH)
        .map(|duration| duration.as_millis())
        .unwrap_or(0)
}

#[cfg(any(feature = "server", feature = "test-utils"))]
fn unix_timestamp_nanos() -> u128 {
    web_time::SystemTime::now()
        .duration_since(web_time::UNIX_EPOCH)
        .map(|duration| duration.as_nanos())
        .unwrap_or(0)
}
