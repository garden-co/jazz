use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct SessionID(pub String);

/// A unique identifier for a CoValue.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct CoID(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TransactionID {
    #[serde(rename = "branch")]
    pub branch: Option<CoID>,
    #[serde(rename = "sessionID")]
    pub session_id: SessionID,
    #[serde(rename = "txIndex")]
    pub tx_index: u32,
}
