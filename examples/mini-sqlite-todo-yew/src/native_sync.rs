use mini_jazz_sqlite::{
    protocol::{
        ClientMessage, CloseReason, DataOp, RetryHint, ServerMessage, SettlementTier,
        TxConflictMode, TxStatusKind,
    },
    sync::{ReadRecord, RowDataUpdate},
};
use serde::{Deserialize, Serialize};

const TRACE_TARGET: &str = "mini_sqlite_todo_yew::native_sync";
const SUMMARY_ITEM_LIMIT: usize = 8;
const SUMMARY_MAX_CHARS: usize = 512;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NativeSyncProbe {
    pub probe_id: String,
    pub operation: String,
    pub table: String,
    pub row_id: String,
    pub origin_browser_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NativeTraceContext {
    pub traceparent: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub probe: Option<NativeSyncProbe>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NativeClientFrame {
    pub client_messages: Vec<ClientMessage>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trace_context: Option<NativeTraceContext>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NativeServerFrame {
    pub server_messages: Vec<ServerMessage>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub trace_context: Option<NativeTraceContext>,
}

pub fn encode_client_frame(client_messages: Vec<ClientMessage>) -> Result<String, String> {
    encode_client_frame_with_context(client_messages, None)
}

pub fn encode_client_frame_with_context(
    client_messages: Vec<ClientMessage>,
    trace_context: Option<NativeTraceContext>,
) -> Result<String, String> {
    serde_json::to_string(&NativeClientFrame {
        client_messages,
        trace_context,
    })
    .map_err(|error| error.to_string())
}

pub fn decode_client_frame(encoded: &str) -> Result<NativeClientFrame, String> {
    serde_json::from_str(encoded).map_err(|error| error.to_string())
}

pub fn encode_server_frame(server_messages: Vec<ServerMessage>) -> Result<String, String> {
    encode_server_frame_with_context(server_messages, None)
}

pub fn encode_server_frame_with_context(
    server_messages: Vec<ServerMessage>,
    trace_context: Option<NativeTraceContext>,
) -> Result<String, String> {
    serde_json::to_string(&NativeServerFrame {
        server_messages,
        trace_context,
    })
    .map_err(|error| error.to_string())
}

pub fn decode_server_frame(encoded: &str) -> Result<NativeServerFrame, String> {
    serde_json::from_str(encoded).map_err(|error| error.to_string())
}

pub fn trace_client_messages(
    direction: &'static str,
    trace_context: Option<&NativeTraceContext>,
    messages: &[ClientMessage],
) {
    for (index, message) in messages.iter().enumerate() {
        let summary = client_message_summary(message);
        trace_sync_message(direction, trace_context, index, messages.len(), summary);
    }
}

pub fn trace_server_messages(
    direction: &'static str,
    trace_context: Option<&NativeTraceContext>,
    messages: &[ServerMessage],
) {
    for (index, message) in messages.iter().enumerate() {
        let summary = server_message_summary(message);
        trace_sync_message(direction, trace_context, index, messages.len(), summary);
    }
}

fn trace_sync_message(
    direction: &'static str,
    trace_context: Option<&NativeTraceContext>,
    index: usize,
    count: usize,
    summary: SyncMessageSummary,
) {
    let span = tracing::info_span!(
        target: TRACE_TARGET,
        "sync.message",
        sync_direction = direction,
        sync_message_kind = summary.kind,
        sync_message_index = index,
        sync_message_count = count,
        sync_subscription_count = summary.subscription_count,
        sync_data_record_count = summary.data_record_count,
        sync_read_record_count = summary.read_record_count,
        sync_bundle_row_count = summary.bundle_row_count,
        sync_bundle_tx_count = summary.bundle_tx_count,
        sync_tx_id = tracing::field::Empty,
        sync_tx_status = tracing::field::Empty,
        sync_tx_rejection_code = tracing::field::Empty,
        sync_tx_global_epoch = tracing::field::Empty,
        sync_tx_conflict_mode = tracing::field::Empty,
        sync_branch_id = tracing::field::Empty,
        sync_subscription_id = tracing::field::Empty,
        sync_message_id = tracing::field::Empty,
        sync_cursor = tracing::field::Empty,
        sync_settlement_tier = tracing::field::Empty,
        sync_error_code = tracing::field::Empty,
        sync_retry_hint = tracing::field::Empty,
        sync_close_reason = tracing::field::Empty,
        sync_data_records = tracing::field::Empty,
        sync_read_records = tracing::field::Empty,
        sync_bundle_tx_ids = tracing::field::Empty,
    );
    record_summary_fields(&span, &summary);
    #[cfg(target_arch = "wasm32")]
    let _ = trace_context;
    #[cfg(not(target_arch = "wasm32"))]
    if let Some(parent) = trace_context.and_then(parent_context_from_traceparent) {
        use tracing_opentelemetry::OpenTelemetrySpanExt as _;

        let _ = span.set_parent(parent);
    }
    let _entered = span.enter();
}

fn record_summary_fields(span: &tracing::Span, summary: &SyncMessageSummary) {
    record_field(span, "sync_tx_id", summary.tx_id.as_deref());
    record_field(span, "sync_tx_status", summary.tx_status.as_deref());
    record_field(
        span,
        "sync_tx_rejection_code",
        summary.tx_rejection_code.as_deref(),
    );
    record_field(
        span,
        "sync_tx_global_epoch",
        summary.tx_global_epoch.as_deref(),
    );
    record_field(
        span,
        "sync_tx_conflict_mode",
        summary.tx_conflict_mode.as_deref(),
    );
    record_field(span, "sync_branch_id", summary.branch_id.as_deref());
    record_field(
        span,
        "sync_subscription_id",
        summary.subscription_id.as_deref(),
    );
    record_field(span, "sync_message_id", summary.message_id.as_deref());
    record_field(span, "sync_cursor", summary.cursor.as_deref());
    record_field(
        span,
        "sync_settlement_tier",
        summary.settlement_tier.as_deref(),
    );
    record_field(span, "sync_error_code", summary.error_code.as_deref());
    record_field(span, "sync_retry_hint", summary.retry_hint.as_deref());
    record_field(span, "sync_close_reason", summary.close_reason.as_deref());
    record_field(span, "sync_data_records", summary.data_records.as_deref());
    record_field(span, "sync_read_records", summary.read_records.as_deref());
    record_field(span, "sync_bundle_tx_ids", summary.bundle_tx_ids.as_deref());
}

fn record_field(span: &tracing::Span, name: &'static str, value: Option<&str>) {
    if let Some(value) = value {
        span.record(name, value);
    }
}

#[cfg(not(target_arch = "wasm32"))]
fn parent_context_from_traceparent(
    trace_context: &NativeTraceContext,
) -> Option<opentelemetry::Context> {
    use opentelemetry::trace::{
        SpanContext, SpanId, TraceContextExt as _, TraceFlags, TraceId, TraceState,
    };

    let mut parts = trace_context.traceparent.split('-');
    let version = parts.next()?;
    let trace_id = parts.next()?;
    let span_id = parts.next()?;
    let trace_flags = parts.next()?;

    if parts.next().is_some()
        || version != "00"
        || trace_id.len() != 32
        || span_id.len() != 16
        || trace_flags.len() != 2
    {
        return None;
    }

    let trace_id = TraceId::from_hex(trace_id).ok()?;
    let span_id = SpanId::from_hex(span_id).ok()?;
    if trace_id == TraceId::INVALID || span_id == SpanId::INVALID {
        return None;
    }

    let trace_flags = TraceFlags::new(u8::from_str_radix(trace_flags, 16).ok()?);
    let span_context =
        SpanContext::new(trace_id, span_id, trace_flags, true, TraceState::default());
    Some(opentelemetry::Context::new().with_remote_span_context(span_context))
}

#[derive(Debug)]
struct SyncMessageSummary {
    kind: &'static str,
    subscription_count: usize,
    data_record_count: usize,
    read_record_count: usize,
    bundle_row_count: usize,
    bundle_tx_count: usize,
    tx_id: Option<String>,
    tx_status: Option<String>,
    tx_rejection_code: Option<String>,
    tx_global_epoch: Option<String>,
    tx_conflict_mode: Option<String>,
    branch_id: Option<String>,
    subscription_id: Option<String>,
    message_id: Option<String>,
    cursor: Option<String>,
    settlement_tier: Option<String>,
    error_code: Option<String>,
    retry_hint: Option<String>,
    close_reason: Option<String>,
    data_records: Option<String>,
    read_records: Option<String>,
    bundle_tx_ids: Option<String>,
}

impl SyncMessageSummary {
    fn kind(kind: &'static str) -> Self {
        Self {
            kind,
            subscription_count: 0,
            data_record_count: 0,
            read_record_count: 0,
            bundle_row_count: 0,
            bundle_tx_count: 0,
            tx_id: None,
            tx_status: None,
            tx_rejection_code: None,
            tx_global_epoch: None,
            tx_conflict_mode: None,
            branch_id: None,
            subscription_id: None,
            message_id: None,
            cursor: None,
            settlement_tier: None,
            error_code: None,
            retry_hint: None,
            close_reason: None,
            data_records: None,
            read_records: None,
            bundle_tx_ids: None,
        }
    }
}

fn client_message_summary(message: &ClientMessage) -> SyncMessageSummary {
    match message {
        ClientMessage::Hello(_) => SyncMessageSummary::kind("client.hello"),
        ClientMessage::Subscribe {
            subscription_id,
            requested_tier,
            ..
        } => SyncMessageSummary {
            subscription_id: Some(to_json_string(subscription_id)),
            settlement_tier: Some(settlement_tier_name(requested_tier).to_owned()),
            ..SyncMessageSummary::kind("client.subscribe")
        },
        ClientMessage::Replay { subscriptions } => SyncMessageSummary {
            subscription_count: subscriptions.len(),
            ..SyncMessageSummary::kind("client.replay")
        },
        ClientMessage::UploadTx { tx, data, reads } => SyncMessageSummary {
            data_record_count: data.len(),
            read_record_count: reads.len(),
            tx_id: Some(tx.tx_id.clone()),
            tx_conflict_mode: Some(tx_conflict_mode_name(&tx.conflict_mode).to_owned()),
            branch_id: tx.branch_id.clone(),
            data_records: limited_join(data.iter().map(client_data_record_summary)),
            read_records: limited_join(reads.iter().map(read_record_summary)),
            ..SyncMessageSummary::kind("client.upload_tx")
        },
        ClientMessage::Unsubscribe { subscription_id } => SyncMessageSummary {
            subscription_id: Some(to_json_string(subscription_id)),
            ..SyncMessageSummary::kind("client.unsubscribe")
        },
        ClientMessage::Ack { message_id, cursor } => SyncMessageSummary {
            message_id: Some(message_id.0.to_string()),
            cursor: cursor.map(|cursor| cursor.0.to_string()),
            ..SyncMessageSummary::kind("client.ack")
        },
        ClientMessage::Close(reason) => SyncMessageSummary {
            close_reason: Some(close_reason_name(reason).to_owned()),
            ..SyncMessageSummary::kind("client.close")
        },
    }
}

fn server_message_summary(message: &ServerMessage) -> SyncMessageSummary {
    match message {
        ServerMessage::Hello(_) => SyncMessageSummary::kind("server.hello"),
        ServerMessage::Data {
            message_id,
            subscription_id,
            cursor,
            bundle,
        } => SyncMessageSummary {
            message_id: Some(message_id.0.to_string()),
            subscription_id: subscription_id.as_ref().map(to_json_string),
            cursor: Some(cursor.0.to_string()),
            read_record_count: bundle.reads.len(),
            bundle_row_count: bundle.rows.len() + bundle.obfuscated.len(),
            bundle_tx_count: bundle.txs.len(),
            data_records: limited_join(bundle.rows.iter().map(row_data_update_summary)),
            read_records: limited_join(bundle.reads.iter().map(read_record_summary)),
            bundle_tx_ids: limited_join(bundle.txs.iter().map(|tx| tx.tx_id.clone())),
            ..SyncMessageSummary::kind("server.data")
        },
        ServerMessage::UploadAck { tx_id } => SyncMessageSummary {
            tx_id: Some(tx_id.clone()),
            ..SyncMessageSummary::kind("server.upload_ack")
        },
        ServerMessage::TxStatus { tx_id, status } => {
            let (tx_status, tx_rejection_code, tx_global_epoch) = tx_status_summary(status);
            SyncMessageSummary {
                tx_id: Some(tx_id.clone()),
                tx_status: Some(tx_status.to_owned()),
                tx_rejection_code,
                tx_global_epoch,
                ..SyncMessageSummary::kind("server.tx_status")
            }
        }
        ServerMessage::Settled {
            subscription_id,
            tier,
            cursor,
        } => SyncMessageSummary {
            subscription_id: Some(to_json_string(subscription_id)),
            settlement_tier: Some(settlement_tier_name(tier).to_owned()),
            cursor: Some(cursor.0.to_string()),
            ..SyncMessageSummary::kind("server.settled")
        },
        ServerMessage::Error(error) => SyncMessageSummary {
            error_code: Some(error.code.clone()),
            subscription_id: error.subscription_id.as_ref().map(to_json_string),
            message_id: error.message_id.map(|message_id| message_id.0.to_string()),
            retry_hint: Some(retry_hint_name(&error.retry_hint).to_owned()),
            ..SyncMessageSummary::kind("server.error")
        },
        ServerMessage::Close(reason) => SyncMessageSummary {
            close_reason: Some(close_reason_name(reason).to_owned()),
            ..SyncMessageSummary::kind("server.close")
        },
    }
}

fn tx_status_summary(status: &TxStatusKind) -> (&'static str, Option<String>, Option<String>) {
    match status {
        TxStatusKind::EdgeAccepted => ("edge_accepted", None, None),
        TxStatusKind::GlobalAccepted { global_epoch } => {
            ("global_accepted", None, Some(global_epoch.to_string()))
        }
        TxStatusKind::Rejected { code, .. } => ("rejected", Some(code.clone()), None),
    }
}

fn client_data_record_summary(record: &mini_jazz_sqlite::protocol::ClientDataRecord) -> String {
    format!(
        "{}:{}:{}",
        record.table,
        record.row_id,
        data_op_name(&record.op)
    )
}

fn row_data_update_summary(row: &RowDataUpdate) -> String {
    format!(
        "{}:{}:{}",
        row.table,
        row.row_id,
        stored_data_op_name(row.op)
    )
}

fn read_record_summary(read: &ReadRecord) -> String {
    let mut summary = format!("{}:{}:reason={}", read.table, read.row_id, read.reason);
    if let Some(observed_tx_id) = &read.observed_tx_id {
        summary.push_str(":observed=");
        summary.push_str(observed_tx_id);
    }
    summary
}

fn limited_join(items: impl Iterator<Item = String>) -> Option<String> {
    let mut output = String::new();
    let mut total = 0;
    for item in items {
        total += 1;
        if total <= SUMMARY_ITEM_LIMIT {
            if !output.is_empty() {
                output.push(',');
            }
            output.push_str(&item);
        }
    }
    if total == 0 {
        return None;
    }
    if total > SUMMARY_ITEM_LIMIT {
        output.push_str(",...");
        output.push_str(&format!("+{}", total - SUMMARY_ITEM_LIMIT));
    }
    if output.len() > SUMMARY_MAX_CHARS {
        output.truncate(SUMMARY_MAX_CHARS);
        output.push_str("...");
    }
    Some(output)
}

fn to_json_string<T: Serialize>(value: &T) -> String {
    serde_json::to_value(value)
        .ok()
        .and_then(|value| value.as_str().map(ToOwned::to_owned))
        .unwrap_or_else(|| "<unknown>".to_owned())
}

fn tx_conflict_mode_name(mode: &TxConflictMode) -> &'static str {
    match mode {
        TxConflictMode::Mergeable => "mergeable",
        TxConflictMode::Exclusive => "exclusive",
    }
}

fn data_op_name(op: &DataOp) -> &'static str {
    match op {
        DataOp::Insert => "insert",
        DataOp::Update => "update",
        DataOp::Delete => "delete",
    }
}

fn stored_data_op_name(op: i64) -> &'static str {
    match op {
        1 => "insert",
        2 => "update",
        3 => "delete",
        _ => "unknown",
    }
}

fn settlement_tier_name(tier: &SettlementTier) -> &'static str {
    match tier {
        SettlementTier::Local => "local",
        SettlementTier::Edge => "edge",
        SettlementTier::Global => "global",
    }
}

fn retry_hint_name(hint: &RetryHint) -> &'static str {
    match hint {
        RetryHint::Retryable => "retryable",
        RetryHint::ReplayRequired => "replay_required",
        RetryHint::Fatal => "fatal",
    }
}

fn close_reason_name(reason: &CloseReason) -> &'static str {
    match reason {
        CloseReason::ClientClosed => "client_closed",
        CloseReason::ProtocolError => "protocol_error",
        CloseReason::TransportFailed => "transport_failed",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use mini_jazz_sqlite::{
        protocol::{
            ClientDataRecord, ClientMessage, ClientTx, CloseReason, DataOp, MessageId,
            ReplayCursor, ServerMessage, SubscriptionId, TxConflictMode, TxStatusKind,
        },
        sync::{BranchRecord, Bundle, HistoryRecord, ReadRecord, RowDataUpdate, TxRecord},
    };
    use serde_json::json;
    use std::collections::BTreeMap;

    #[test]
    fn native_sync_frames_round_trip_through_json() {
        let client_messages = vec![ClientMessage::Close(CloseReason::ClientClosed)];
        let encoded = encode_client_frame(client_messages.clone()).unwrap();
        let decoded = decode_client_frame(&encoded).unwrap();

        assert!(matches!(
            decoded.client_messages.as_slice(),
            [ClientMessage::Close(CloseReason::ClientClosed)]
        ));

        let server_messages = vec![ServerMessage::Close(CloseReason::ClientClosed)];
        let encoded = encode_server_frame(server_messages.clone()).unwrap();
        let decoded = decode_server_frame(&encoded).unwrap();

        assert!(matches!(
            decoded.server_messages.as_slice(),
            [ServerMessage::Close(CloseReason::ClientClosed)]
        ));
    }

    #[test]
    fn native_sync_frames_propagate_trace_context() {
        let context = NativeTraceContext {
            traceparent: "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01".to_owned(),
            probe: Some(NativeSyncProbe {
                probe_id: "probe-1".to_owned(),
                operation: "insert".to_owned(),
                table: "todos".to_owned(),
                row_id: "todo-1".to_owned(),
                origin_browser_id: "browser-a".to_owned(),
            }),
        };

        let encoded = encode_client_frame_with_context(
            vec![ClientMessage::Close(CloseReason::ClientClosed)],
            Some(context.clone()),
        )
        .unwrap();
        let decoded = decode_client_frame(&encoded).unwrap();
        assert_eq!(decoded.trace_context, Some(context.clone()));

        let encoded = encode_server_frame_with_context(
            vec![ServerMessage::Close(CloseReason::ClientClosed)],
            Some(context.clone()),
        )
        .unwrap();
        let decoded = decode_server_frame(&encoded).unwrap();
        assert_eq!(decoded.trace_context, Some(context));
    }

    #[test]
    fn client_upload_summary_includes_debug_correlation_without_row_values() {
        let mut values = BTreeMap::new();
        values.insert("title".to_owned(), json!("Buy milk"));

        let summary = client_message_summary(&ClientMessage::UploadTx {
            tx: ClientTx {
                tx_id: "tx-insert".to_owned(),
                branch_id: Some("main".to_owned()),
                conflict_mode: TxConflictMode::Mergeable,
                created_at: 123,
                author: Some("alice".to_owned()),
            },
            data: vec![ClientDataRecord {
                table: "todos".to_owned(),
                row_id: "todo-1".to_owned(),
                op: DataOp::Insert,
                values,
            }],
            reads: vec![ReadRecord {
                tx_id: "tx-insert".to_owned(),
                table: "todos".to_owned(),
                row_id: "todo-1".to_owned(),
                reason: 7,
                observed_tx_id: Some("tx-before".to_owned()),
            }],
        });

        assert_eq!(summary.tx_id.as_deref(), Some("tx-insert"));
        assert_eq!(summary.branch_id.as_deref(), Some("main"));
        assert_eq!(summary.tx_conflict_mode.as_deref(), Some("mergeable"));
        assert_eq!(summary.data_records.as_deref(), Some("todos:todo-1:insert"));
        assert_eq!(
            summary.read_records.as_deref(),
            Some("todos:todo-1:reason=7:observed=tx-before")
        );
        assert!(!format!("{summary:?}").contains("Buy milk"));
    }

    #[test]
    fn server_status_and_data_summaries_include_protocol_correlation() {
        let status = server_message_summary(&ServerMessage::TxStatus {
            tx_id: "tx-delete".to_owned(),
            status: TxStatusKind::Rejected {
                code: "policy_denied".to_owned(),
                detail: Some(json!({"message": "not allowed"})),
            },
        });
        assert_eq!(status.tx_id.as_deref(), Some("tx-delete"));
        assert_eq!(status.tx_status.as_deref(), Some("rejected"));
        assert_eq!(status.tx_rejection_code.as_deref(), Some("policy_denied"));
        assert!(!format!("{status:?}").contains("not allowed"));

        let data = server_message_summary(&ServerMessage::Data {
            message_id: MessageId(42),
            subscription_id: Some(SubscriptionId::new("todos")),
            cursor: ReplayCursor(9),
            bundle: bundle_with_row(RowDataUpdate {
                table: "todos".to_owned(),
                row_id: "todo-1".to_owned(),
                branch_id: "main".to_owned(),
                tx_id: "tx-delete".to_owned(),
                op: 3,
                values: BTreeMap::from([("title".to_owned(), json!("Buy milk"))]),
                created_at: 1,
                updated_at: 2,
                created_by: "alice".to_owned(),
                updated_by: "alice".to_owned(),
            }),
        });
        assert_eq!(data.message_id.as_deref(), Some("42"));
        assert_eq!(data.subscription_id.as_deref(), Some("todos"));
        assert_eq!(data.cursor.as_deref(), Some("9"));
        assert_eq!(data.bundle_tx_ids.as_deref(), Some("tx-delete"));
        assert_eq!(data.data_records.as_deref(), Some("todos:todo-1:delete"));
        assert_eq!(data.read_record_count, 1);
        assert_eq!(
            data.read_records.as_deref(),
            Some("todos:todo-1:reason=8:observed=tx-insert")
        );
        assert!(!format!("{data:?}").contains("Buy milk"));
    }

    fn bundle_with_row(row: RowDataUpdate) -> Bundle {
        Bundle {
            protocol_version: 1,
            schema_fingerprint: "schema".to_owned(),
            policy_fingerprint: "policy".to_owned(),
            branches: vec![BranchRecord {
                branch_id: "main".to_owned(),
                base_global_epoch: None,
                source_branch_ids: Vec::new(),
                source_version: 0,
            }],
            txs: vec![TxRecord {
                tx_id: row.tx_id.clone(),
                node_id: "server".to_owned(),
                local_epoch: 1,
                global_epoch: None,
                conflict_mode: 1,
                outcome: 1,
                auth_user: None,
                rejection_code: None,
                rejection_detail: None,
                receipt_tiers: vec![2],
                created_at: 1,
            }],
            reads: vec![ReadRecord {
                tx_id: row.tx_id.clone(),
                table: row.table.clone(),
                row_id: row.row_id.clone(),
                reason: 8,
                observed_tx_id: Some("tx-insert".to_owned()),
            }],
            query_reads: Vec::new(),
            rows: vec![row],
            obfuscated: Vec::new(),
            history: Vec::<HistoryRecord>::new(),
        }
    }
}
