use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::ops::Deref;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicU64, Ordering},
    mpsc::{SyncSender, TrySendError, sync_channel},
};
use std::thread::{self, JoinHandle};

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

pub trait SyncPayloadTelemetrySink: Send + Sync + 'static {
    fn emit(&self, record: SyncPayloadTelemetryRecord);
}

#[derive(Debug, Clone, Default)]
pub struct NoopSyncPayloadTelemetrySink;

impl SyncPayloadTelemetrySink for NoopSyncPayloadTelemetrySink {
    fn emit(&self, _record: SyncPayloadTelemetryRecord) {}
}

#[derive(Debug, Clone, Default)]
pub struct InMemorySyncPayloadTelemetrySink {
    records: Arc<Mutex<Vec<SyncPayloadTelemetryRecord>>>,
}

impl InMemorySyncPayloadTelemetrySink {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn records(&self) -> Vec<SyncPayloadTelemetryRecord> {
        self.records.lock().unwrap().clone()
    }

    pub fn clear(&self) {
        self.records.lock().unwrap().clear();
    }
}

impl SyncPayloadTelemetrySink for InMemorySyncPayloadTelemetrySink {
    fn emit(&self, record: SyncPayloadTelemetryRecord) {
        self.records.lock().unwrap().push(record);
    }
}

#[derive(Debug, Clone)]
pub struct BoundedSyncPayloadTelemetrySink {
    inner: Arc<BoundedSyncPayloadTelemetrySinkInner>,
}

#[derive(Debug)]
struct BoundedSyncPayloadTelemetrySinkInner {
    sender: Mutex<Option<SyncSender<SyncPayloadTelemetryRecord>>>,
    worker: Mutex<Option<JoinHandle<()>>>,
    dropped: AtomicU64,
}

impl BoundedSyncPayloadTelemetrySink {
    pub fn new(capacity: usize, downstream: impl SyncPayloadTelemetrySink) -> Self {
        assert!(
            capacity > 0,
            "sync payload telemetry queue capacity must be greater than zero"
        );

        let (sender, receiver) = sync_channel(capacity);
        let worker = thread::Builder::new()
            .name("sync-payload-telemetry".to_string())
            .spawn(move || {
                while let Ok(record) = receiver.recv() {
                    downstream.emit(record);
                }
            })
            .expect("failed to spawn sync payload telemetry worker");

        Self {
            inner: Arc::new(BoundedSyncPayloadTelemetrySinkInner {
                sender: Mutex::new(Some(sender)),
                worker: Mutex::new(Some(worker)),
                dropped: AtomicU64::new(0),
            }),
        }
    }

    pub fn dropped_count(&self) -> u64 {
        self.inner.dropped.load(Ordering::Relaxed)
    }

    pub fn shutdown(&self) {
        self.inner.shutdown();
    }
}

impl SyncPayloadTelemetrySink for BoundedSyncPayloadTelemetrySink {
    fn emit(&self, record: SyncPayloadTelemetryRecord) {
        let sender = self.inner.sender.lock().unwrap().clone();
        if let Some(sender) = sender {
            match sender.try_send(record) {
                Ok(()) => {}
                Err(TrySendError::Full(_)) | Err(TrySendError::Disconnected(_)) => {
                    self.inner.dropped.fetch_add(1, Ordering::Relaxed);
                }
            }
        } else {
            self.inner.dropped.fetch_add(1, Ordering::Relaxed);
        }
    }
}

impl BoundedSyncPayloadTelemetrySinkInner {
    fn shutdown(&self) {
        self.sender.lock().unwrap().take();
        if let Some(worker) = self.worker.lock().unwrap().take() {
            let _ = worker.join();
        }
    }
}

impl Drop for BoundedSyncPayloadTelemetrySinkInner {
    fn drop(&mut self) {
        if let Ok(sender) = self.sender.get_mut() {
            sender.take();
        }
        if let Ok(worker) = self.worker.get_mut()
            && let Some(worker) = worker.take()
        {
            let _ = worker.join();
        }
    }
}

#[cfg(feature = "otel-logs")]
pub mod otel_logs {
    use std::time::{Duration, UNIX_EPOCH};

    use opentelemetry::logs::{
        AnyValue, LogRecord as _, Logger as _, LoggerProvider as _, Severity,
    };
    use opentelemetry::{Key, KeyValue};
    use opentelemetry_otlp::WithExportConfig;
    use opentelemetry_sdk::logs::{SdkLogger, SdkLoggerProvider};

    use super::{
        BoundedSyncPayloadTelemetrySink, SyncPayloadTelemetryRecord, SyncPayloadTelemetrySink,
    };

    #[derive(Debug, Clone)]
    pub struct OtelLogsSyncPayloadTelemetrySink {
        logger: SdkLogger,
        _provider: SdkLoggerProvider,
    }

    impl OtelLogsSyncPayloadTelemetrySink {
        pub fn new(
            collector_url: impl Into<String>,
        ) -> Result<Self, opentelemetry_otlp::ExporterBuildError> {
            let exporter = opentelemetry_otlp::LogExporter::builder()
                .with_tonic()
                .with_endpoint(collector_url.into())
                .build()?;
            let provider = SdkLoggerProvider::builder()
                .with_resource(
                    opentelemetry_sdk::Resource::builder()
                        .with_service_name("jazz-server")
                        .with_attribute(KeyValue::new("service.version", env!("CARGO_PKG_VERSION")))
                        .build(),
                )
                .with_batch_exporter(exporter)
                .build();
            let logger = provider.logger("jazz-server.sync-payload");

            Ok(Self {
                logger,
                _provider: provider,
            })
        }
    }

    impl SyncPayloadTelemetrySink for OtelLogsSyncPayloadTelemetrySink {
        fn emit(&self, record: SyncPayloadTelemetryRecord) {
            let mut log_record = self.logger.create_log_record();
            log_record.set_event_name("jazz.sync_payload");
            log_record.set_target("jazz-tools.sync_payload_telemetry");
            log_record.set_severity_number(Severity::Debug);
            log_record.set_severity_text(Severity::Debug.name());
            log_record.set_timestamp(UNIX_EPOCH + Duration::from_millis(record.recorded_at));
            log_record.set_body(AnyValue::String(
                serde_json::to_string(&record)
                    .unwrap_or_else(|_| "sync_payload_telemetry_record".to_string())
                    .into(),
            ));
            log_record.add_attributes(record_attributes(&record));
            self.logger.emit(log_record);
        }
    }

    pub fn bounded_otel_logs_sink(
        collector_url: impl Into<String>,
        queue_capacity: usize,
    ) -> Result<BoundedSyncPayloadTelemetrySink, opentelemetry_otlp::ExporterBuildError> {
        Ok(BoundedSyncPayloadTelemetrySink::new(
            queue_capacity,
            OtelLogsSyncPayloadTelemetrySink::new(collector_url)?,
        ))
    }

    fn record_attributes(record: &SyncPayloadTelemetryRecord) -> Vec<(Key, AnyValue)> {
        let mut attributes = Vec::new();
        push_string(&mut attributes, "jazz.app_id", record.app_id.as_deref());
        push_string(
            &mut attributes,
            "jazz.scope",
            Some(scope_name(record.scope)),
        );
        push_string(
            &mut attributes,
            "jazz.direction",
            Some(direction_name(record.direction)),
        );
        push_string(
            &mut attributes,
            "jazz.client_id",
            record.client_id.as_deref(),
        );
        push_string(
            &mut attributes,
            "jazz.connection_id",
            record.connection_id.as_deref(),
        );
        push_u64(&mut attributes, "jazz.sequence", record.sequence);
        push_string(
            &mut attributes,
            "jazz.source_frame_id",
            record.source_frame_id.as_deref(),
        );
        push_usize(
            &mut attributes,
            "jazz.source_payload_index",
            record.source_payload_index,
        );
        push_usize(
            &mut attributes,
            "jazz.source_payload_count",
            record.source_payload_count,
        );
        push_u64(
            &mut attributes,
            "jazz.source_frame_bytes",
            record.source_frame_bytes,
        );
        push_u64(
            &mut attributes,
            "jazz.message_bytes",
            Some(record.message_bytes),
        );
        push_string(
            &mut attributes,
            "jazz.message_encoding",
            Some(message_encoding_name(record.message_encoding)),
        );
        push_string(
            &mut attributes,
            "jazz.decode_error",
            record.decode_error.as_deref(),
        );
        push_string(
            &mut attributes,
            "jazz.payload_variant",
            record.payload_variant.as_deref(),
        );
        push_string(&mut attributes, "jazz.row_id", record.row_id.as_deref());
        push_string(
            &mut attributes,
            "jazz.table_name",
            record.table_name.as_deref(),
        );
        push_string(
            &mut attributes,
            "jazz.table_name_error",
            record.table_name_error.as_deref(),
        );
        push_string(
            &mut attributes,
            "jazz.branch_name",
            record.branch_name.as_deref(),
        );
        push_string(&mut attributes, "jazz.batch_id", record.batch_id.as_deref());
        push_u64(&mut attributes, "jazz.query_id", record.query_id);
        push_string(
            &mut attributes,
            "jazz.schema_hash",
            record.schema_hash.as_deref(),
        );
        push_string(
            &mut attributes,
            "jazz.schema_hash_error",
            record.schema_hash_error.as_deref(),
        );
        push_string(
            &mut attributes,
            "jazz.durability_tier",
            record.durability_tier.as_deref(),
        );
        push_string(
            &mut attributes,
            "jazz.error_variant",
            record.error_variant.as_deref(),
        );
        push_string(
            &mut attributes,
            "jazz.error_code",
            record.error_code.as_deref(),
        );
        push_usize(&mut attributes, "jazz.member_index", record.member_index);
        push_usize(&mut attributes, "jazz.member_count", record.member_count);
        attributes
    }

    fn push_string(attributes: &mut Vec<(Key, AnyValue)>, key: &'static str, value: Option<&str>) {
        if let Some(value) = value {
            attributes.push((Key::from_static_str(key), value.to_string().into()));
        }
    }

    fn push_u64(attributes: &mut Vec<(Key, AnyValue)>, key: &'static str, value: Option<u64>) {
        if let Some(value) = value {
            attributes.push((Key::from_static_str(key), to_i64(value).into()));
        }
    }

    fn push_usize(attributes: &mut Vec<(Key, AnyValue)>, key: &'static str, value: Option<usize>) {
        if let Some(value) = value {
            attributes.push((Key::from_static_str(key), to_i64(value as u64).into()));
        }
    }

    fn to_i64(value: u64) -> i64 {
        i64::try_from(value).unwrap_or(i64::MAX)
    }

    fn scope_name(scope: super::SyncPayloadTelemetryScope) -> &'static str {
        match scope {
            super::SyncPayloadTelemetryScope::WorkerBridge => "worker_bridge",
            super::SyncPayloadTelemetryScope::Websocket => "websocket",
        }
    }

    fn direction_name(direction: super::SyncPayloadTelemetryDirection) -> &'static str {
        match direction {
            super::SyncPayloadTelemetryDirection::MainToWorker => "main_to_worker",
            super::SyncPayloadTelemetryDirection::WorkerToMain => "worker_to_main",
            super::SyncPayloadTelemetryDirection::ClientToServer => "client_to_server",
            super::SyncPayloadTelemetryDirection::ServerToClient => "server_to_client",
        }
    }

    fn message_encoding_name(encoding: super::SyncPayloadTelemetryMessageEncoding) -> &'static str {
        match encoding {
            super::SyncPayloadTelemetryMessageEncoding::Binary => "binary",
            super::SyncPayloadTelemetryMessageEncoding::Utf8 => "utf8",
        }
    }
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
        BoundedSyncPayloadTelemetrySink, FieldDerivation, FieldDerivations,
        InMemorySyncPayloadTelemetrySink, SyncPayloadTelemetryDecodeFailureInput,
        SyncPayloadTelemetryDirection, SyncPayloadTelemetryFields,
        SyncPayloadTelemetryMessageEncoding, SyncPayloadTelemetryRecord,
        SyncPayloadTelemetryRecordInput, SyncPayloadTelemetryScope, SyncPayloadTelemetrySink,
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

    #[test]
    fn in_memory_sink_keeps_emitted_records_for_route_tests() {
        let sink = InMemorySyncPayloadTelemetrySink::default();
        let record = telemetry_record(1);

        sink.emit(record.clone());

        assert_eq!(sink.records(), vec![record]);
    }

    #[test]
    fn bounded_sink_drops_when_worker_queue_is_full() {
        let downstream = BlockingTestSink::default();
        let sink = BoundedSyncPayloadTelemetrySink::new(1, downstream.clone());

        sink.emit(telemetry_record(1));
        downstream.wait_until_blocked();
        sink.emit(telemetry_record(2));
        sink.emit(telemetry_record(3));

        assert_eq!(sink.dropped_count(), 1);

        downstream.release();
        downstream.wait_for_records(2);
        assert_eq!(
            downstream
                .records()
                .iter()
                .map(|record| record.sequence)
                .collect::<Vec<_>>(),
            vec![Some(1), Some(2)]
        );
        sink.shutdown();
    }

    #[test]
    fn bounded_sink_shutdown_drains_queued_records() {
        let downstream = InMemorySyncPayloadTelemetrySink::default();
        let sink = BoundedSyncPayloadTelemetrySink::new(8, downstream.clone());

        sink.emit(telemetry_record(1));
        sink.emit(telemetry_record(2));
        sink.shutdown();

        assert_eq!(
            downstream
                .records()
                .iter()
                .map(|record| record.sequence)
                .collect::<Vec<_>>(),
            vec![Some(1), Some(2)]
        );
        assert_eq!(sink.dropped_count(), 0);
    }

    #[test]
    fn bounded_sink_counts_emit_after_shutdown_as_dropped() {
        let downstream = InMemorySyncPayloadTelemetrySink::default();
        let sink = BoundedSyncPayloadTelemetrySink::new(8, downstream.clone());

        sink.shutdown();
        sink.emit(telemetry_record(1));

        assert_eq!(downstream.records(), Vec::new());
        assert_eq!(sink.dropped_count(), 1);
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

    fn telemetry_record(sequence: u64) -> SyncPayloadTelemetryRecord {
        SyncPayloadTelemetryRecord {
            app_id: Some("app_todos".to_string()),
            severity_text: "DEBUG".to_string(),
            scope: SyncPayloadTelemetryScope::Websocket,
            direction: SyncPayloadTelemetryDirection::ServerToClient,
            client_id: Some("alice".to_string()),
            connection_id: Some("conn-1".to_string()),
            sequence: Some(sequence),
            source_frame_id: None,
            source_payload_index: None,
            source_payload_count: None,
            source_frame_bytes: None,
            message_bytes: 24,
            message_encoding: SyncPayloadTelemetryMessageEncoding::Binary,
            recorded_at: 1_775_000_000_000 + sequence,
            decode_error: None,
            log_body: None,
            fields: SyncPayloadTelemetryFields {
                payload_variant: Some("QuerySettled".to_string()),
                query_id: Some(sequence),
                ..Default::default()
            },
        }
    }

    #[derive(Clone, Default)]
    struct BlockingTestSink {
        state: std::sync::Arc<BlockingTestSinkState>,
    }

    #[derive(Default)]
    struct BlockingTestSinkState {
        records: std::sync::Mutex<Vec<SyncPayloadTelemetryRecord>>,
        records_condvar: std::sync::Condvar,
        blocked: std::sync::Mutex<bool>,
        blocked_condvar: std::sync::Condvar,
        release: std::sync::Mutex<bool>,
        release_condvar: std::sync::Condvar,
    }

    impl SyncPayloadTelemetrySink for BlockingTestSink {
        fn emit(&self, record: SyncPayloadTelemetryRecord) {
            {
                let mut records = self.state.records.lock().unwrap();
                records.push(record);
                self.state.records_condvar.notify_all();
            }

            let mut blocked = self.state.blocked.lock().unwrap();
            *blocked = true;
            self.state.blocked_condvar.notify_all();
            drop(blocked);

            let mut release = self.state.release.lock().unwrap();
            while !*release {
                release = self.state.release_condvar.wait(release).unwrap();
            }
        }
    }

    impl BlockingTestSink {
        fn wait_until_blocked(&self) {
            let mut blocked = self.state.blocked.lock().unwrap();
            while !*blocked {
                blocked = self.state.blocked_condvar.wait(blocked).unwrap();
            }
        }

        fn release(&self) {
            let mut release = self.state.release.lock().unwrap();
            *release = true;
            self.state.release_condvar.notify_all();
        }

        fn wait_for_records(&self, count: usize) {
            let mut records = self.state.records.lock().unwrap();
            while records.len() < count {
                records = self.state.records_condvar.wait(records).unwrap();
            }
        }

        fn records(&self) -> Vec<SyncPayloadTelemetryRecord> {
            self.state.records.lock().unwrap().clone()
        }
    }
}
