use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::object::ObjectId;
use crate::sync_manager::DurabilityTier;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BatchId(pub Uuid);

impl BatchId {
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }
}

impl Default for BatchId {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RowState {
    StagingPending,
    Rejected,
    VisibleDirect,
    VisibleTransactional,
}

impl RowState {
    pub fn is_visible(self) -> bool {
        matches!(self, Self::VisibleDirect | Self::VisibleTransactional)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HistoryScan {
    Branch,
    Row { row_id: ObjectId },
    AsOf { ts: u64 },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredRowVersion {
    pub row_id: ObjectId,
    pub branch: String,
    pub updated_at: u64,
    pub created_by: String,
    pub created_at: u64,
    pub updated_by: String,
    pub batch_id: BatchId,
    pub state: RowState,
    pub confirmed_tier: Option<DurabilityTier>,
    pub is_deleted: bool,
    pub data: Vec<u8>,
    pub metadata: HashMap<String, String>,
}
