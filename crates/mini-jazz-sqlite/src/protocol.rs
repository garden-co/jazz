use crate::sync::Bundle;
use crate::BuiltQuery;
use serde::{Deserialize, Serialize};

pub const SUPPORTED_PROTOCOL_VERSION: ProtocolVersion = ProtocolVersion(1);

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
}

impl Default for ProtocolCapabilities {
    fn default() -> Self {
        Self {
            replay: true,
            acknowledgements: true,
            query_settlement: true,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplaySubscription {
    pub subscription_id: SubscriptionId,
    pub query: BuiltQuery,
    pub requested_tier: SettlementTier,
    pub last_applied_cursor: Option<ReplayCursor>,
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
    },
    Replay {
        subscriptions: Vec<ReplaySubscription>,
    },
    Ack {
        message_id: MessageId,
        cursor: Option<ReplayCursor>,
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
    Settled {
        subscription_id: SubscriptionId,
        tier: SettlementTier,
        cursor: ReplayCursor,
    },
    Error(ProtocolError),
    Close(CloseReason),
}
