use std::collections::{BTreeMap, HashMap};

use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[cfg(test)]
use crate::commit::Commit;
use crate::commit::{CommitId, compute_commit_id};
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

fn tier_satisfies(confirmed_tier: Option<DurabilityTier>, required_tier: DurabilityTier) -> bool {
    confirmed_tier.is_some_and(|confirmed| confirmed >= required_tier)
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
    fn commit_metadata(&self) -> Option<BTreeMap<String, String>> {
        (!self.metadata.is_empty()).then(|| {
            self.metadata
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect::<BTreeMap<_, _>>()
        })
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new(
        row_id: ObjectId,
        branch: impl Into<String>,
        parents: Vec<CommitId>,
        data: Vec<u8>,
        provenance: RowProvenance,
        metadata: HashMap<String, String>,
        state: RowState,
        confirmed_tier: Option<DurabilityTier>,
    ) -> Self {
        let is_deleted = metadata
            .get(MetadataKey::Delete.as_str())
            .is_some_and(|value| {
                value == DeleteKind::Soft.as_str() || value == DeleteKind::Hard.as_str()
            });
        let metadata_btree = (!metadata.is_empty()).then(|| {
            metadata
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect::<BTreeMap<_, _>>()
        });
        let version_id = compute_commit_id(
            &parents,
            &data,
            provenance.updated_at,
            &provenance.updated_by,
            metadata_btree.as_ref(),
        );

        Self {
            row_id,
            branch: branch.into(),
            parents,
            updated_at: provenance.updated_at,
            created_by: provenance.created_by,
            created_at: provenance.created_at,
            updated_by: provenance.updated_by,
            batch_id: BatchId::from_commit_id(version_id),
            state,
            confirmed_tier,
            is_deleted,
            data,
            metadata,
        }
    }

    #[cfg(test)]
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
        compute_commit_id(
            &self.parents,
            &self.data,
            self.updated_at,
            &self.updated_by,
            self.commit_metadata().as_ref(),
        )
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
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VisibleRowEntry {
    pub current_row: StoredRowVersion,
    pub worker_version_id: Option<CommitId>,
    pub edge_version_id: Option<CommitId>,
    pub global_version_id: Option<CommitId>,
}

impl VisibleRowEntry {
    pub fn new(current_row: StoredRowVersion) -> Self {
        Self {
            current_row,
            worker_version_id: None,
            edge_version_id: None,
            global_version_id: None,
        }
    }

    pub fn rebuild(current_row: StoredRowVersion, history_rows: &[StoredRowVersion]) -> Self {
        let current_version_id = current_row.version_id();
        let worker = latest_visible_version_for_tier(history_rows, DurabilityTier::Worker);
        let worker_version_id = worker.filter(|version_id| *version_id != current_version_id);

        let edge = latest_visible_version_for_tier(history_rows, DurabilityTier::EdgeServer);
        let edge_version_id = edge.filter(|version_id| *version_id != current_version_id);

        let global = latest_visible_version_for_tier(history_rows, DurabilityTier::GlobalServer);
        let global_version_id = global.filter(|version_id| *version_id != current_version_id);

        Self {
            current_row,
            worker_version_id,
            edge_version_id,
            global_version_id,
        }
    }

    pub fn current_version_id(&self) -> CommitId {
        self.current_row.version_id()
    }

    pub fn version_id_for_tier(&self, tier: DurabilityTier) -> Option<CommitId> {
        let current = self.current_version_id();
        if tier_satisfies(self.current_row.confirmed_tier, tier) {
            return Some(current);
        }

        match tier {
            DurabilityTier::Worker => self.worker_version_id,
            DurabilityTier::EdgeServer => self.edge_version_id,
            DurabilityTier::GlobalServer => self.global_version_id,
        }
    }
}

fn latest_visible_version_for_tier(
    history_rows: &[StoredRowVersion],
    required_tier: DurabilityTier,
) -> Option<CommitId> {
    history_rows
        .iter()
        .filter(|row| row.state.is_visible() && tier_satisfies(row.confirmed_tier, required_tier))
        .max_by_key(|row| (row.updated_at, row.version_id()))
        .map(StoredRowVersion::version_id)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::metadata::RowProvenance;

    fn visible_row(updated_at: u64, confirmed_tier: Option<DurabilityTier>) -> StoredRowVersion {
        StoredRowVersion::new(
            ObjectId::new(),
            "main",
            Vec::new(),
            vec![updated_at as u8],
            RowProvenance::for_insert("alice".to_string(), updated_at),
            HashMap::new(),
            RowState::VisibleDirect,
            confirmed_tier,
        )
    }

    #[test]
    fn visible_row_entry_omits_tier_pointers_when_current_is_globally_confirmed() {
        let current = visible_row(30, Some(DurabilityTier::GlobalServer));
        let entry = VisibleRowEntry::rebuild(current.clone(), std::slice::from_ref(&current));

        assert_eq!(entry.worker_version_id, None);
        assert_eq!(entry.edge_version_id, None);
        assert_eq!(entry.global_version_id, None);
    }

    #[test]
    fn visible_row_entry_resolves_tier_fallback_chain() {
        let global = visible_row(10, Some(DurabilityTier::GlobalServer));
        let edge = visible_row(20, Some(DurabilityTier::EdgeServer));
        let current = visible_row(30, Some(DurabilityTier::Worker));
        let history = vec![global.clone(), edge.clone(), current.clone()];

        let entry = VisibleRowEntry::rebuild(current.clone(), &history);

        assert_eq!(entry.worker_version_id, None);
        assert_eq!(entry.edge_version_id, Some(edge.version_id()));
        assert_eq!(entry.global_version_id, Some(global.version_id()));
        assert_eq!(
            entry.version_id_for_tier(DurabilityTier::Worker),
            Some(current.version_id())
        );
        assert_eq!(
            entry.version_id_for_tier(DurabilityTier::EdgeServer),
            Some(edge.version_id())
        );
        assert_eq!(
            entry.version_id_for_tier(DurabilityTier::GlobalServer),
            Some(global.version_id())
        );
    }

    #[test]
    fn visible_row_entry_returns_none_when_no_version_meets_required_tier() {
        let current = visible_row(30, Some(DurabilityTier::Worker));
        let entry = VisibleRowEntry::rebuild(current.clone(), std::slice::from_ref(&current));

        assert_eq!(entry.version_id_for_tier(DurabilityTier::EdgeServer), None);
        assert_eq!(
            entry.version_id_for_tier(DurabilityTier::GlobalServer),
            None
        );
    }
}
