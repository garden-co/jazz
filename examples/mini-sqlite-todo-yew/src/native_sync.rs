use mini_jazz_sqlite::{
    protocol::{
        ClientMessage, CloseReason, DataOp, RetryHint, ServerMessage, SettlementTier,
        TxConflictMode, TxStatusKind,
    },
    sync::{ReadRecord, RowDataUpdate},
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

const LOG_TARGET: &str = "mini_sqlite_todo_yew::native_sync";
const SUMMARY_ITEM_LIMIT: usize = 8;
const SUMMARY_MAX_CHARS: usize = 512;

pub const DIRECTION_MAIN_TO_WORKER: &str = "main.to_worker";
pub const DIRECTION_WORKER_FROM_MAIN: &str = "worker.from_main";
pub const DIRECTION_WORKER_TO_MAIN: &str = "worker.to_main";
pub const DIRECTION_MAIN_FROM_WORKER: &str = "main.from_worker";
pub const DIRECTION_WORKER_TO_SERVER: &str = "worker.to_server";
pub const DIRECTION_SERVER_FROM_WORKER: &str = "server.from_worker";
pub const DIRECTION_SERVER_TO_WORKER: &str = "server.to_worker";
pub const DIRECTION_WORKER_FROM_SERVER: &str = "worker.from_server";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NativeSyncProbe {
    pub probe_id: String,
    pub operation: String,
    pub table: String,
    pub row_id: String,
    pub origin_browser_id: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct NativeSyncLogContext {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub probe: Option<NativeSyncProbe>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NativeClientFrame {
    pub client_messages: Vec<ClientMessage>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sync_context: Option<NativeSyncLogContext>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct NativeServerFrame {
    pub server_messages: Vec<ServerMessage>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub sync_context: Option<NativeSyncLogContext>,
}

pub fn encode_client_frame(client_messages: Vec<ClientMessage>) -> Result<String, String> {
    encode_client_frame_with_context(client_messages, None)
}

pub fn encode_client_frame_with_context(
    client_messages: Vec<ClientMessage>,
    sync_context: Option<NativeSyncLogContext>,
) -> Result<String, String> {
    serde_json::to_string(&NativeClientFrame {
        client_messages,
        sync_context,
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
    sync_context: Option<NativeSyncLogContext>,
) -> Result<String, String> {
    serde_json::to_string(&NativeServerFrame {
        server_messages,
        sync_context,
    })
    .map_err(|error| error.to_string())
}

pub fn decode_server_frame(encoded: &str) -> Result<NativeServerFrame, String> {
    serde_json::from_str(encoded).map_err(|error| error.to_string())
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SyncLogRecord {
    pub event_name: &'static str,
    pub body: String,
    pub attributes: BTreeMap<String, String>,
}

impl SyncLogRecord {
    pub fn attribute(&self, key: &str) -> Option<&str> {
        self.attributes.get(key).map(String::as_str)
    }
}

pub fn client_sync_log_records(
    direction: &'static str,
    sync_context: Option<&NativeSyncLogContext>,
    connection_id: Option<u64>,
    messages: &[ClientMessage],
) -> Vec<SyncLogRecord> {
    messages
        .iter()
        .enumerate()
        .map(|(index, message)| {
            let summary = client_message_summary(message);
            sync_log_record(
                direction,
                sync_context,
                connection_id,
                index,
                messages.len(),
                summary,
                message,
            )
        })
        .collect()
}

pub fn server_sync_log_records(
    direction: &'static str,
    sync_context: Option<&NativeSyncLogContext>,
    connection_id: Option<u64>,
    messages: &[ServerMessage],
) -> Vec<SyncLogRecord> {
    messages
        .iter()
        .enumerate()
        .map(|(index, message)| {
            let summary = server_message_summary(message);
            sync_log_record(
                direction,
                sync_context,
                connection_id,
                index,
                messages.len(),
                summary,
                message,
            )
        })
        .collect()
}

pub fn log_client_messages(
    direction: &'static str,
    sync_context: Option<&NativeSyncLogContext>,
    connection_id: Option<u64>,
    messages: &[ClientMessage],
) {
    for record in client_sync_log_records(direction, sync_context, connection_id, messages) {
        emit_sync_log_record(&record);
    }
}

pub fn log_server_messages(
    direction: &'static str,
    sync_context: Option<&NativeSyncLogContext>,
    connection_id: Option<u64>,
    messages: &[ServerMessage],
) {
    for record in server_sync_log_records(direction, sync_context, connection_id, messages) {
        emit_sync_log_record(&record);
    }
}

fn sync_log_record<T: Serialize>(
    direction: &'static str,
    sync_context: Option<&NativeSyncLogContext>,
    connection_id: Option<u64>,
    index: usize,
    count: usize,
    summary: SyncMessageSummary,
    message: &T,
) -> SyncLogRecord {
    let body = serde_json::json!({
        "event": "sync.message",
        "direction": direction,
        "message_kind": summary.kind,
        "message_index": index,
        "message_count": count,
        "message": message,
    })
    .to_string();
    let mut attributes = sync_log_attributes(
        direction,
        sync_context,
        connection_id,
        index,
        count,
        &summary,
    );
    attributes.insert("sync.event".to_owned(), "sync.message".to_owned());
    SyncLogRecord {
        event_name: "sync.message",
        body,
        attributes,
    }
}

fn sync_log_attributes(
    direction: &'static str,
    sync_context: Option<&NativeSyncLogContext>,
    connection_id: Option<u64>,
    index: usize,
    count: usize,
    summary: &SyncMessageSummary,
) -> BTreeMap<String, String> {
    let mut attributes = BTreeMap::new();
    attributes.insert("sync.direction".to_owned(), direction.to_owned());
    attributes.insert("sync.message_kind".to_owned(), summary.kind.to_owned());
    attributes.insert("sync.message_index".to_owned(), index.to_string());
    attributes.insert("sync.message_count".to_owned(), count.to_string());
    attributes.insert(
        "sync.subscription_count".to_owned(),
        summary.subscription_count.to_string(),
    );
    attributes.insert(
        "sync.data_record_count".to_owned(),
        summary.data_record_count.to_string(),
    );
    attributes.insert(
        "sync.read_record_count".to_owned(),
        summary.read_record_count.to_string(),
    );
    attributes.insert(
        "sync.bundle_row_count".to_owned(),
        summary.bundle_row_count.to_string(),
    );
    attributes.insert(
        "sync.bundle_tx_count".to_owned(),
        summary.bundle_tx_count.to_string(),
    );
    if let Some(connection_id) = connection_id {
        attributes.insert("sync.connection_id".to_owned(), connection_id.to_string());
    }
    if let Some(context) = sync_context {
        if let Some(session_id) = &context.session_id {
            attributes.insert("sync.session_id".to_owned(), session_id.clone());
        }
        if let Some(probe) = &context.probe {
            attributes.insert("sync.probe.id".to_owned(), probe.probe_id.clone());
            attributes.insert("sync.operation".to_owned(), probe.operation.clone());
            attributes.insert("sync.table".to_owned(), probe.table.clone());
            attributes.insert("sync.row_id".to_owned(), probe.row_id.clone());
            attributes.insert(
                "sync.origin_browser_id".to_owned(),
                probe.origin_browser_id.clone(),
            );
        }
    }
    insert_optional(&mut attributes, "sync.tx_id", summary.tx_id.as_deref());
    insert_optional(
        &mut attributes,
        "sync.tx_status",
        summary.tx_status.as_deref(),
    );
    insert_optional(
        &mut attributes,
        "sync.tx_rejection_code",
        summary.tx_rejection_code.as_deref(),
    );
    insert_optional(
        &mut attributes,
        "sync.tx_global_epoch",
        summary.tx_global_epoch.as_deref(),
    );
    insert_optional(
        &mut attributes,
        "sync.tx_conflict_mode",
        summary.tx_conflict_mode.as_deref(),
    );
    insert_optional(
        &mut attributes,
        "sync.branch_id",
        summary.branch_id.as_deref(),
    );
    insert_optional(
        &mut attributes,
        "sync.subscription_id",
        summary.subscription_id.as_deref(),
    );
    insert_optional(
        &mut attributes,
        "sync.message_id",
        summary.message_id.as_deref(),
    );
    insert_optional(&mut attributes, "sync.cursor", summary.cursor.as_deref());
    insert_optional(
        &mut attributes,
        "sync.settlement_tier",
        summary.settlement_tier.as_deref(),
    );
    insert_optional(
        &mut attributes,
        "sync.error_code",
        summary.error_code.as_deref(),
    );
    insert_optional(
        &mut attributes,
        "sync.retry_hint",
        summary.retry_hint.as_deref(),
    );
    insert_optional(
        &mut attributes,
        "sync.close_reason",
        summary.close_reason.as_deref(),
    );
    insert_optional(
        &mut attributes,
        "sync.data_records",
        summary.data_records.as_deref(),
    );
    insert_optional(
        &mut attributes,
        "sync.read_records",
        summary.read_records.as_deref(),
    );
    insert_optional(
        &mut attributes,
        "sync.bundle_tx_ids",
        summary.bundle_tx_ids.as_deref(),
    );
    attributes
}

fn insert_optional(attributes: &mut BTreeMap<String, String>, key: &str, value: Option<&str>) {
    if let Some(value) = value {
        attributes.insert(key.to_owned(), value.to_owned());
    }
}

fn emit_sync_log_record(record: &SyncLogRecord) {
    let attr = |key: &str| record.attribute(key).unwrap_or("");
    tracing::info!(
        name: "sync.message",
        target: LOG_TARGET,
        {
            sync.session_id = attr("sync.session_id"),
            sync.connection_id = attr("sync.connection_id"),
            sync.direction = attr("sync.direction"),
            sync.message_kind = attr("sync.message_kind"),
            sync.message_index = attr("sync.message_index"),
            sync.message_count = attr("sync.message_count"),
            sync.subscription_count = attr("sync.subscription_count"),
            sync.data_record_count = attr("sync.data_record_count"),
            sync.read_record_count = attr("sync.read_record_count"),
            sync.bundle_row_count = attr("sync.bundle_row_count"),
            sync.bundle_tx_count = attr("sync.bundle_tx_count"),
            sync.tx_id = attr("sync.tx_id"),
            sync.tx_status = attr("sync.tx_status"),
            sync.tx_rejection_code = attr("sync.tx_rejection_code"),
            sync.tx_global_epoch = attr("sync.tx_global_epoch"),
            sync.tx_conflict_mode = attr("sync.tx_conflict_mode"),
            sync.branch_id = attr("sync.branch_id"),
            sync.subscription_id = attr("sync.subscription_id"),
            sync.message_id = attr("sync.message_id"),
            sync.cursor = attr("sync.cursor"),
            sync.settlement_tier = attr("sync.settlement_tier"),
            sync.error_code = attr("sync.error_code"),
            sync.retry_hint = attr("sync.retry_hint"),
            sync.close_reason = attr("sync.close_reason"),
            sync.data_records = attr("sync.data_records"),
            sync.read_records = attr("sync.read_records"),
            sync.bundle_tx_ids = attr("sync.bundle_tx_ids"),
            sync.probe.id = attr("sync.probe.id"),
            sync.operation = attr("sync.operation"),
            sync.table = attr("sync.table"),
            sync.row_id = attr("sync.row_id"),
            sync.origin_browser_id = attr("sync.origin_browser_id"),
        },
        "{}",
        record.body.as_str()
    );
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
        ClientMessage::ReconcileSymbols {
            subscription_id,
            symbols,
            ..
        } => SyncMessageSummary {
            subscription_id: Some(to_json_string(subscription_id)),
            data_record_count: symbols.len(),
            ..SyncMessageSummary::kind("client.reconcile_symbols")
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
        ServerMessage::ReconcileMore {
            subscription_id,
            requested_symbols,
            next_symbol_index,
            ..
        } => SyncMessageSummary {
            subscription_id: Some(to_json_string(subscription_id)),
            data_record_count: *requested_symbols as usize,
            cursor: Some(next_symbol_index.to_string()),
            ..SyncMessageSummary::kind("server.reconcile_more")
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
    fn sync_direction_names_describe_actor_hops() {
        assert_eq!(DIRECTION_MAIN_TO_WORKER, "main.to_worker");
        assert_eq!(DIRECTION_WORKER_FROM_MAIN, "worker.from_main");
        assert_eq!(DIRECTION_WORKER_TO_MAIN, "worker.to_main");
        assert_eq!(DIRECTION_MAIN_FROM_WORKER, "main.from_worker");
        assert_eq!(DIRECTION_WORKER_TO_SERVER, "worker.to_server");
        assert_eq!(DIRECTION_SERVER_FROM_WORKER, "server.from_worker");
        assert_eq!(DIRECTION_SERVER_TO_WORKER, "server.to_worker");
        assert_eq!(DIRECTION_WORKER_FROM_SERVER, "worker.from_server");
    }

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
    fn native_sync_frames_propagate_sync_log_context() {
        let context = NativeSyncLogContext {
            session_id: Some("server-session-1".to_owned()),
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
        assert_eq!(decoded.sync_context, Some(context.clone()));

        let encoded = encode_server_frame_with_context(
            vec![ServerMessage::Close(CloseReason::ClientClosed)],
            Some(context.clone()),
        )
        .unwrap();
        let decoded = decode_server_frame(&encoded).unwrap();
        assert_eq!(decoded.sync_context, Some(context));
    }

    #[test]
    fn client_upload_log_record_includes_full_protocol_payload() {
        let mut values = BTreeMap::new();
        values.insert("title".to_owned(), json!("Buy milk"));

        let message = ClientMessage::UploadTx {
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
        };
        let records = client_sync_log_records(
            DIRECTION_SERVER_FROM_WORKER,
            Some(&NativeSyncLogContext {
                session_id: Some("server-session-1".to_owned()),
                probe: None,
            }),
            Some(7),
            &[message],
        );

        assert_eq!(records.len(), 1);
        let record = &records[0];
        assert_eq!(
            record.attribute("sync.session_id"),
            Some("server-session-1")
        );
        assert_eq!(record.attribute("sync.connection_id"), Some("7"));
        assert_eq!(
            record.attribute("sync.direction"),
            Some("server.from_worker")
        );
        assert_eq!(
            record.attribute("sync.message_kind"),
            Some("client.upload_tx")
        );
        assert_eq!(record.attribute("sync.tx_id"), Some("tx-insert"));
        assert_eq!(record.attribute("sync.branch_id"), Some("main"));
        assert_eq!(record.attribute("sync.tx_conflict_mode"), Some("mergeable"));
        assert_eq!(
            record.attribute("sync.data_records"),
            Some("todos:todo-1:insert")
        );
        assert_eq!(
            record.attribute("sync.read_records"),
            Some("todos:todo-1:reason=7:observed=tx-before")
        );
        assert!(record.body.contains("client.upload_tx"));
        assert!(record.body.contains("Buy milk"));
    }

    #[test]
    fn server_data_log_record_includes_full_protocol_payload() {
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

        let message = ServerMessage::Data {
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
        };
        let records = server_sync_log_records(
            DIRECTION_WORKER_FROM_SERVER,
            Some(&NativeSyncLogContext {
                session_id: Some("server-session-1".to_owned()),
                probe: None,
            }),
            None,
            &[message],
        );

        assert_eq!(records.len(), 1);
        let record = &records[0];
        assert_eq!(record.attribute("sync.message_id"), Some("42"));
        assert_eq!(record.attribute("sync.subscription_id"), Some("todos"));
        assert_eq!(record.attribute("sync.cursor"), Some("9"));
        assert_eq!(record.attribute("sync.bundle_tx_ids"), Some("tx-delete"));
        assert_eq!(
            record.attribute("sync.data_records"),
            Some("todos:todo-1:delete")
        );
        assert_eq!(record.attribute("sync.read_record_count"), Some("1"));
        assert_eq!(
            record.attribute("sync.read_records"),
            Some("todos:todo-1:reason=8:observed=tx-insert")
        );
        assert!(record.body.contains("server.data"));
        assert!(record.body.contains("Buy milk"));
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
