use crate::sync::Bundle;
use crate::BuiltQuery;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub const SUPPORTED_PROTOCOL_VERSION: ProtocolVersion = ProtocolVersion(2);

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ProtocolVersion(pub u32);

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SessionId(String);

impl SessionId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct MessageId(pub u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct ReplayCursor(pub u64);

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct SubscriptionId(String);

impl SubscriptionId {
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum SettlementTier {
    Local,
    Edge,
    Global,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientHello {
    pub protocol_version: ProtocolVersion,
    pub session_id: SessionId,
    pub node_id: String,
    pub schema_fingerprint: String,
    pub policy_fingerprint: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServerHello {
    pub protocol_version: ProtocolVersion,
    pub session_id: SessionId,
    pub node_id: String,
    pub capabilities: ProtocolCapabilities,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProtocolCapabilities {
    pub replay: bool,
    pub acknowledgements: bool,
    pub query_settlement: bool,
    pub tx_upload: bool,
}

impl Default for ProtocolCapabilities {
    fn default() -> Self {
        Self {
            replay: true,
            acknowledgements: true,
            query_settlement: true,
            tx_upload: true,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TxConflictMode {
    Mergeable,
    Exclusive,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum DataOp {
    Insert,
    Update,
    Delete,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientTx {
    pub tx_id: String,
    pub branch_id: Option<String>,
    pub conflict_mode: TxConflictMode,
    pub created_at: i64,
    pub author: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClientDataRecord {
    pub table: String,
    pub row_id: String,
    pub op: DataOp,
    pub values: BTreeMap<String, serde_json::Value>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum TxStatusKind {
    EdgeAccepted,
    GlobalAccepted {
        global_epoch: i64,
    },
    Rejected {
        code: String,
        detail: Option<serde_json::Value>,
    },
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplaySubscription {
    pub subscription_id: SubscriptionId,
    pub query: BuiltQuery,
    pub requested_tier: SettlementTier,
    pub last_applied_cursor: Option<ReplayCursor>,
    pub reconciliation: Option<ReconciliationSketch>,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct RowHeadItem {
    pub branch_id: String,
    pub table: String,
    pub row_id: String,
    pub head_tx_id: String,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReconcileSet {
    RowHeads,
    PolicyDeps,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReconcileAlgorithm {
    Exact,
    Rateless,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReconciliationSketch {
    pub set: ReconcileSet,
    pub algorithm: ReconcileAlgorithm,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub parameters: Option<ReconcileParameters>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub symbols: Vec<ReconcileSymbol>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub row_heads: Vec<RowHeadItem>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReconcileParameters {
    pub seed: u64,
    pub estimated_items: u64,
    pub target_degree: u8,
    pub symbol_count: u32,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReconcileSymbol {
    pub index: u32,
    pub count: i64,
    pub item_len_xor: u64,
    pub item_bytes_xor: String,
    pub item_hash_xor: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProtocolError {
    pub code: String,
    pub message: String,
    pub subscription_id: Option<SubscriptionId>,
    pub message_id: Option<MessageId>,
    pub retry_hint: RetryHint,
}

impl ProtocolError {
    pub fn new(code: impl Into<String>, message: impl Into<String>, retry_hint: RetryHint) -> Self {
        Self {
            code: code.into(),
            message: message.into(),
            subscription_id: None,
            message_id: None,
            retry_hint,
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum RetryHint {
    Retryable,
    ReplayRequired,
    Fatal,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum CloseReason {
    ClientClosed,
    ProtocolError,
    TransportFailed,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ClientMessage {
    Hello(ClientHello),
    Subscribe {
        subscription_id: SubscriptionId,
        query: BuiltQuery,
        requested_tier: SettlementTier,
        reconciliation: Option<ReconciliationSketch>,
    },
    Replay {
        subscriptions: Vec<ReplaySubscription>,
    },
    UploadTx {
        tx: ClientTx,
        data: Vec<ClientDataRecord>,
        reads: Vec<crate::sync::ReadRecord>,
    },
    Unsubscribe {
        subscription_id: SubscriptionId,
    },
    Ack {
        message_id: MessageId,
        cursor: Option<ReplayCursor>,
    },
    ReconcileSymbols {
        subscription_id: SubscriptionId,
        set: ReconcileSet,
        parameters: ReconcileParameters,
        symbols: Vec<ReconcileSymbol>,
    },
    Close(CloseReason),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ServerMessage {
    Hello(ServerHello),
    Data {
        message_id: MessageId,
        subscription_id: Option<SubscriptionId>,
        cursor: ReplayCursor,
        bundle: Bundle,
    },
    UploadAck {
        tx_id: String,
    },
    TxStatus {
        tx_id: String,
        status: TxStatusKind,
    },
    Settled {
        subscription_id: SubscriptionId,
        tier: SettlementTier,
        cursor: ReplayCursor,
    },
    ReconcileMore {
        subscription_id: SubscriptionId,
        set: ReconcileSet,
        parameters: ReconcileParameters,
        next_symbol_index: u32,
        requested_symbols: u32,
    },
    Error(ProtocolError),
    Close(CloseReason),
}
