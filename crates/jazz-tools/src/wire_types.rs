use serde::{Deserialize, Serialize};

use crate::query_manager::types::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscriptionRow {
    pub id: String,
    pub values: Vec<Value>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[serde(transparent)]
pub struct SubscriptionRowDelta(pub Vec<SubscriptionRowChange>);

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscriptionRowAdded {
    pub kind: u8,
    pub id: String,
    pub index: usize,
    pub row: SubscriptionRow,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscriptionRowRemoved {
    pub kind: u8,
    pub id: String,
    pub index: usize,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SubscriptionRowUpdated {
    pub kind: u8,
    pub id: String,
    pub index: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub row: Option<SubscriptionRow>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum SubscriptionRowChange {
    Added(SubscriptionRowAdded),
    Removed(SubscriptionRowRemoved),
    Updated(SubscriptionRowUpdated),
}
