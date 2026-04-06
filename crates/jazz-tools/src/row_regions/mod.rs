use std::collections::{BTreeMap, HashMap};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::commit::{Commit, CommitAckState, CommitId, StoredState};
use crate::metadata::{DeleteKind, MetadataKey, RowProvenance};
use crate::object::ObjectId;
use crate::sync_manager::DurabilityTier;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct BatchId(pub Uuid);

impl BatchId {
    pub fn new() -> Self {
        Self(Uuid::now_v7())
    }

    pub fn from_commit_id(commit_id: CommitId) -> Self {
        Self(Uuid::new_v5(&Uuid::NAMESPACE_OID, &commit_id.0))
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
    pub parents: Vec<crate::commit::CommitId>,
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

impl StoredRowVersion {
    pub fn from_commit(
        row_id: ObjectId,
        branch: impl Into<String>,
        commit_id: CommitId,
        commit: &Commit,
        state: RowState,
    ) -> Self {
        let provenance = commit
            .row_provenance()
            .unwrap_or_else(|| RowProvenance::for_insert(commit.author.clone(), commit.timestamp));

        Self {
            row_id,
            branch: branch.into(),
            parents: commit.parents.iter().copied().collect(),
            updated_at: provenance.updated_at,
            created_by: provenance.created_by,
            created_at: provenance.created_at,
            updated_by: provenance.updated_by,
            batch_id: BatchId::from_commit_id(commit_id),
            state,
            confirmed_tier: commit.ack_state.confirmed_tiers.iter().copied().max(),
            is_deleted: commit.is_soft_deleted() || commit.is_hard_deleted(),
            data: commit.content.clone(),
            metadata: commit
                .metadata
                .as_ref()
                .map(|metadata| {
                    metadata
                        .iter()
                        .map(|(key, value)| (key.clone(), value.clone()))
                        .collect()
                })
                .unwrap_or_default(),
        }
    }

    pub fn row_provenance(&self) -> RowProvenance {
        RowProvenance {
            created_by: self.created_by.clone(),
            created_at: self.created_at,
            updated_by: self.updated_by.clone(),
            updated_at: self.updated_at,
        }
    }

    pub fn version_id(&self) -> CommitId {
        self.to_commit().id()
    }

    pub fn is_soft_deleted(&self) -> bool {
        self.metadata
            .get(MetadataKey::Delete.as_str())
            .map(|value| value == DeleteKind::Soft.as_str())
            .unwrap_or(false)
    }

    pub fn is_hard_deleted(&self) -> bool {
        self.metadata
            .get(MetadataKey::Delete.as_str())
            .map(|value| value == DeleteKind::Hard.as_str())
            .unwrap_or(false)
            || (self.is_deleted && self.data.is_empty())
    }

    pub fn to_commit(&self) -> Commit {
        let mut ack_state = CommitAckState::default();
        if let Some(tier) = self.confirmed_tier {
            ack_state.confirmed_tiers.insert(tier);
        }

        Commit {
            parents: self.parents.iter().copied().collect(),
            content: self.data.clone(),
            timestamp: self.updated_at,
            author: self.updated_by.clone(),
            metadata: (!self.metadata.is_empty()).then(|| {
                self.metadata
                    .iter()
                    .map(|(key, value)| (key.clone(), value.clone()))
                    .collect::<BTreeMap<_, _>>()
            }),
            stored_state: StoredState::Stored,
            ack_state,
        }
    }
}
