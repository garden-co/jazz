//! Data types: BatchId, RowState, QueryRowBatch, StoredRowBatch, VisibleRowEntry, etc.

use std::collections::HashMap;
use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use smallvec::SmallVec;
use uuid::Uuid;

use crate::digest::Digest32;
use crate::metadata::{DeleteKind, MetadataKey, RowProvenance};
use crate::object::ObjectId;
use crate::query_manager::types::{RowBytes, RowDescriptor, SharedString, Value};
use crate::row_format::{EncodingError, encode_row};
use crate::storage::{RowLocator, Storage, StorageError};
use crate::sync_manager::DurabilityTier;

use super::codecs::{compute_row_digest, flat_user_values, malformed, tier_satisfies};
use super::resolution::{
    assign_winner_ordinals, branch_frontier, build_computed_visible_preview,
    latest_visible_version_for_tier, preview_override_sidecar,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct BatchId(pub [u8; 16]);

impl BatchId {
    pub fn new() -> Self {
        Self::from_uuid(Uuid::now_v7())
    }

    pub fn from_uuid(uuid: Uuid) -> Self {
        Self(*uuid.as_bytes())
    }

    pub fn as_bytes(&self) -> &[u8; 16] {
        &self.0
    }
}

impl Default for BatchId {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for BatchId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", hex::encode(self.0))
    }
}

impl FromStr for BatchId {
    type Err = String;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        let bytes = hex::decode(raw).map_err(|err| format!("invalid batch id hex: {err}"))?;
        let len = bytes.len();
        let bytes: [u8; 16] = bytes
            .try_into()
            .map_err(|_| format!("expected 16-byte batch id, got {len}"))?;
        Ok(Self(bytes))
    }
}

impl From<BatchId> for Digest32 {
    fn from(value: BatchId) -> Self {
        let mut bytes = [0u8; 32];
        bytes[..16].copy_from_slice(&value.0);
        Digest32(bytes)
    }
}

impl From<Digest32> for BatchId {
    fn from(value: Digest32) -> Self {
        let mut bytes = [0u8; 16];
        bytes.copy_from_slice(&value.0[..16]);
        Self(bytes)
    }
}

impl Serialize for BatchId {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        if serializer.is_human_readable() {
            self.to_string().serialize(serializer)
        } else {
            self.0.serialize(serializer)
        }
    }
}

impl<'de> Deserialize<'de> for BatchId {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        if deserializer.is_human_readable() {
            let raw = String::deserialize(deserializer)?;
            raw.parse().map_err(serde::de::Error::custom)
        } else {
            <[u8; 16]>::deserialize(deserializer).map(Self)
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RowState {
    StagingPending,
    Superseded,
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

/// Visibility change emitted when a row object's winning version changes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RowVisibilityChange {
    pub object_id: ObjectId,
    pub row_locator: RowLocator,
    pub row: StoredRowBatch,
    pub previous_row: Option<StoredRowBatch>,
    pub is_new_object: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ApplyRowBatchResult {
    pub batch_id: BatchId,
    pub row_locator: RowLocator,
    pub visibility_change: Option<RowVisibilityChange>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RowHistoryError {
    ObjectNotFound(ObjectId),
    ParentNotFound(BatchId),
    StorageError(StorageError),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct QueryRowBatch {
    pub batch_id: BatchId,
    pub branch: SharedString,
    pub updated_at: u64,
    pub created_by: SharedString,
    pub created_at: u64,
    pub updated_by: SharedString,
    pub state: RowState,
    pub delete_kind: Option<DeleteKind>,
    pub data: RowBytes,
}

impl QueryRowBatch {
    pub fn batch_id(&self) -> BatchId {
        self.batch_id
    }

    pub fn row_provenance(&self) -> RowProvenance {
        RowProvenance {
            created_by: self.created_by.to_string(),
            created_at: self.created_at,
            updated_by: self.updated_by.to_string(),
            updated_at: self.updated_at,
        }
    }

    pub fn is_soft_deleted(&self) -> bool {
        self.delete_kind == Some(DeleteKind::Soft)
    }

    pub fn is_hard_deleted(&self) -> bool {
        self.delete_kind == Some(DeleteKind::Hard)
    }
}

impl From<&StoredRowBatch> for QueryRowBatch {
    fn from(row: &StoredRowBatch) -> Self {
        Self {
            batch_id: row.batch_id,
            branch: row.branch.clone(),
            updated_at: row.updated_at,
            created_by: row.created_by.clone(),
            created_at: row.created_at,
            updated_by: row.updated_by.clone(),
            state: row.state,
            delete_kind: row.delete_kind,
            data: row.data.clone(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StoredRowBatch {
    pub row_id: ObjectId,
    pub batch_id: BatchId,
    pub branch: SharedString,
    pub parents: SmallVec<[BatchId; 2]>,
    pub updated_at: u64,
    pub created_by: SharedString,
    pub created_at: u64,
    pub updated_by: SharedString,
    pub state: RowState,
    pub confirmed_tier: Option<DurabilityTier>,
    pub delete_kind: Option<DeleteKind>,
    pub is_deleted: bool,
    pub data: RowBytes,
    pub metadata: RowMetadata,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct RowMetadata(SmallVec<[(String, String); 4]>);

impl RowMetadata {
    pub fn from_entries(mut entries: Vec<(String, String)>) -> Self {
        entries.sort_by(|(left_key, _), (right_key, _)| left_key.cmp(right_key));
        entries.dedup_by(|(left_key, _), (right_key, _)| left_key == right_key);
        Self(SmallVec::from_vec(entries))
    }

    pub fn from_hash_map(metadata: HashMap<String, String>) -> Self {
        Self::from_entries(metadata.into_iter().collect())
    }

    pub fn get(&self, key: &str) -> Option<&String> {
        self.0
            .iter()
            .find_map(|(entry_key, value)| (entry_key == key).then_some(value))
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&str, &str)> {
        self.0
            .iter()
            .map(|(key, value)| (key.as_str(), value.as_str()))
    }
}

fn delete_kind_from_metadata(metadata: &HashMap<String, String>) -> Option<DeleteKind> {
    match metadata
        .get(MetadataKey::Delete.as_str())
        .map(String::as_str)
    {
        Some("soft") => Some(DeleteKind::Soft),
        Some("hard") => Some(DeleteKind::Hard),
        _ => None,
    }
}

impl StoredRowBatch {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        row_id: ObjectId,
        branch: impl Into<String>,
        parents: impl IntoIterator<Item = BatchId>,
        data: Vec<u8>,
        provenance: RowProvenance,
        metadata: HashMap<String, String>,
        state: RowState,
        confirmed_tier: Option<DurabilityTier>,
    ) -> Self {
        Self::new_with_batch_id(
            BatchId::new(),
            row_id,
            branch,
            parents,
            data,
            provenance,
            metadata,
            state,
            confirmed_tier,
        )
    }

    #[allow(clippy::too_many_arguments)]
    pub fn new_with_batch_id(
        batch_id: BatchId,
        row_id: ObjectId,
        branch: impl Into<String>,
        parents: impl IntoIterator<Item = BatchId>,
        data: Vec<u8>,
        provenance: RowProvenance,
        metadata: HashMap<String, String>,
        state: RowState,
        confirmed_tier: Option<DurabilityTier>,
    ) -> Self {
        let delete_kind = delete_kind_from_metadata(&metadata);
        let is_deleted = delete_kind.is_some();
        let metadata = RowMetadata::from_hash_map(
            metadata
                .into_iter()
                .filter(|(key, _)| key != MetadataKey::Delete.as_str())
                .collect(),
        );
        let branch = SharedString::from(branch.into());
        let parents = parents.into_iter().collect::<SmallVec<[BatchId; 2]>>();

        Self {
            row_id,
            batch_id,
            branch,
            parents,
            updated_at: provenance.updated_at,
            created_by: provenance.created_by.into(),
            created_at: provenance.created_at,
            updated_by: provenance.updated_by.into(),
            state,
            confirmed_tier,
            delete_kind,
            is_deleted,
            data: data.into(),
            metadata,
        }
    }

    pub fn row_provenance(&self) -> RowProvenance {
        RowProvenance {
            created_by: self.created_by.to_string(),
            created_at: self.created_at,
            updated_by: self.updated_by.to_string(),
            updated_at: self.updated_at,
        }
    }

    pub fn batch_id(&self) -> BatchId {
        self.batch_id
    }

    pub fn content_digest(&self) -> Digest32 {
        compute_row_digest(
            &self.branch,
            &self.parents,
            &self.data,
            self.updated_at,
            &self.updated_by,
            (!self.metadata.is_empty()).then_some(&self.metadata),
        )
    }

    pub fn accepted_transaction_output(&self, confirmed_tier: DurabilityTier) -> Self {
        let mut row = self.clone();
        row.parents = self.parents.clone();
        row.state = RowState::VisibleTransactional;
        row.confirmed_tier = Some(confirmed_tier);
        row
    }

    pub fn is_soft_deleted(&self) -> bool {
        self.delete_kind == Some(DeleteKind::Soft)
    }

    pub fn is_hard_deleted(&self) -> bool {
        self.delete_kind == Some(DeleteKind::Hard) || (self.is_deleted && self.data.is_empty())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VisibleRowEntry {
    pub current_row: StoredRowBatch,
    pub branch_frontier: Vec<BatchId>,
    pub worker_batch_id: Option<BatchId>,
    pub edge_batch_id: Option<BatchId>,
    pub global_batch_id: Option<BatchId>,
    pub winner_batch_pool: Vec<BatchId>,
    pub current_winner_ordinals: Option<Vec<u16>>,
    pub worker_winner_ordinals: Option<Vec<u16>>,
    pub edge_winner_ordinals: Option<Vec<u16>>,
    pub global_winner_ordinals: Option<Vec<u16>>,
    pub merge_artifacts: Option<Vec<u8>>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct ComputedVisiblePreview {
    pub(super) row: StoredRowBatch,
    pub(super) winner_batch_ids: Option<Vec<BatchId>>,
}

impl VisibleRowEntry {
    pub fn new(current_row: StoredRowBatch) -> Self {
        Self {
            branch_frontier: vec![current_row.batch_id()],
            current_row,
            worker_batch_id: None,
            edge_batch_id: None,
            global_batch_id: None,
            winner_batch_pool: Vec::new(),
            current_winner_ordinals: None,
            worker_winner_ordinals: None,
            edge_winner_ordinals: None,
            global_winner_ordinals: None,
            merge_artifacts: None,
        }
    }

    pub fn rebuild(current_row: StoredRowBatch, history_rows: &[StoredRowBatch]) -> Self {
        let current_batch_id = current_row.batch_id();
        let branch_frontier = branch_frontier(history_rows);
        let worker = latest_visible_version_for_tier(history_rows, DurabilityTier::Local);
        let worker_batch_id = worker.filter(|batch_id| *batch_id != current_batch_id);

        let edge = latest_visible_version_for_tier(history_rows, DurabilityTier::EdgeServer);
        let edge_batch_id = edge.filter(|batch_id| *batch_id != current_batch_id);

        let global = latest_visible_version_for_tier(history_rows, DurabilityTier::GlobalServer);
        let global_batch_id = global.filter(|batch_id| *batch_id != current_batch_id);

        Self {
            current_row,
            branch_frontier,
            worker_batch_id,
            edge_batch_id,
            global_batch_id,
            winner_batch_pool: Vec::new(),
            current_winner_ordinals: None,
            worker_winner_ordinals: None,
            edge_winner_ordinals: None,
            global_winner_ordinals: None,
            merge_artifacts: None,
        }
    }

    pub fn rebuild_with_descriptor(
        user_descriptor: &RowDescriptor,
        history_rows: &[StoredRowBatch],
    ) -> Result<Option<Self>, EncodingError> {
        let Some(current_preview) =
            build_computed_visible_preview(user_descriptor, history_rows, None)?
        else {
            return Ok(None);
        };
        let current_row = current_preview.row.clone();
        let branch_frontier = branch_frontier(history_rows);
        let worker_preview = build_computed_visible_preview(
            user_descriptor,
            history_rows,
            Some(DurabilityTier::Local),
        )?;
        let edge_preview = build_computed_visible_preview(
            user_descriptor,
            history_rows,
            Some(DurabilityTier::EdgeServer),
        )?;
        let global_preview = build_computed_visible_preview(
            user_descriptor,
            history_rows,
            Some(DurabilityTier::GlobalServer),
        )?;

        let mut winner_batch_pool = Vec::new();
        let mut pool_ordinals = HashMap::new();
        let current_winner_ordinals = assign_winner_ordinals(
            current_preview.winner_batch_ids.as_deref(),
            &mut winner_batch_pool,
            &mut pool_ordinals,
        )?;
        let (worker_batch_id, worker_winner_ordinals) = preview_override_sidecar(
            &current_preview,
            worker_preview.as_ref(),
            &mut winner_batch_pool,
            &mut pool_ordinals,
        )?;
        let (edge_batch_id, edge_winner_ordinals) = preview_override_sidecar(
            &current_preview,
            edge_preview.as_ref(),
            &mut winner_batch_pool,
            &mut pool_ordinals,
        )?;
        let (global_batch_id, global_winner_ordinals) = preview_override_sidecar(
            &current_preview,
            global_preview.as_ref(),
            &mut winner_batch_pool,
            &mut pool_ordinals,
        )?;

        Ok(Some(Self {
            current_row,
            branch_frontier,
            worker_batch_id,
            edge_batch_id,
            global_batch_id,
            winner_batch_pool,
            current_winner_ordinals,
            worker_winner_ordinals,
            edge_winner_ordinals,
            global_winner_ordinals,
            merge_artifacts: None,
        }))
    }

    pub fn current_batch_id(&self) -> BatchId {
        self.current_row.batch_id()
    }

    pub fn batch_id_for_tier(&self, tier: DurabilityTier) -> Option<BatchId> {
        let current = self.current_batch_id();
        if tier_satisfies(self.current_row.confirmed_tier, tier) {
            return Some(current);
        }

        match tier {
            DurabilityTier::Local => self.worker_batch_id,
            DurabilityTier::EdgeServer => self.edge_batch_id,
            DurabilityTier::GlobalServer => self.global_batch_id,
        }
    }

    fn winner_ordinals_for_tier(&self, tier: DurabilityTier) -> Option<&[u16]> {
        match tier {
            DurabilityTier::Local => self.worker_winner_ordinals.as_deref(),
            DurabilityTier::EdgeServer => self.edge_winner_ordinals.as_deref(),
            DurabilityTier::GlobalServer => self.global_winner_ordinals.as_deref(),
        }
    }

    pub fn materialize_preview_for_tier_from_loaded_rows(
        &self,
        user_descriptor: &RowDescriptor,
        tier: DurabilityTier,
        row_by_batch_id: &HashMap<BatchId, StoredRowBatch>,
    ) -> Result<Option<StoredRowBatch>, EncodingError> {
        if tier_satisfies(self.current_row.confirmed_tier, tier) {
            return Ok(Some(self.current_row.clone()));
        }

        let Some(preview_batch_id) = self.batch_id_for_tier(tier) else {
            return Ok(None);
        };
        let Some(metadata_row) = row_by_batch_id.get(&preview_batch_id).cloned() else {
            return Err(malformed(format!(
                "missing preview metadata row for batch {preview_batch_id}"
            )));
        };
        let Some(ordinals) = self.winner_ordinals_for_tier(tier) else {
            return Ok(Some(metadata_row));
        };

        let mut decoded_rows = HashMap::<BatchId, Vec<Value>>::new();
        let mut merged_values = Vec::with_capacity(ordinals.len());
        let mut contributing_rows = Vec::new();
        for (column_index, ordinal) in ordinals.iter().enumerate() {
            let pool_index = usize::from(*ordinal);
            let Some(batch_id) = self.winner_batch_pool.get(pool_index).copied() else {
                return Err(malformed(format!(
                    "winner ordinal {pool_index} out of range for pool size {}",
                    self.winner_batch_pool.len()
                )));
            };
            let Some(row) = row_by_batch_id.get(&batch_id) else {
                return Err(malformed(format!(
                    "missing winner row for batch {batch_id} in preview reconstruction"
                )));
            };
            let values = if let Some(values) = decoded_rows.get(&batch_id) {
                values
            } else {
                let values = flat_user_values(user_descriptor, &row.data)?;
                decoded_rows.insert(batch_id, values);
                decoded_rows
                    .get(&batch_id)
                    .expect("decoded row values should be cached")
            };
            merged_values.push(values[column_index].clone());
            contributing_rows.push(row);
        }

        let mut confirmed_tier = metadata_row.confirmed_tier;
        for row in contributing_rows {
            confirmed_tier = match (confirmed_tier, row.confirmed_tier) {
                (Some(existing), Some(incoming)) => Some(existing.min(incoming)),
                _ => None,
            };
            if confirmed_tier.is_none() {
                break;
            }
        }

        let data = match metadata_row.delete_kind {
            Some(DeleteKind::Hard) => Vec::new(),
            _ => encode_row(user_descriptor, &merged_values)?,
        };

        Ok(Some(StoredRowBatch {
            confirmed_tier,
            data: data.into(),
            is_deleted: metadata_row.delete_kind.is_some(),
            ..metadata_row
        }))
    }

    pub fn materialize_preview_for_tier_with_storage<H: Storage + ?Sized>(
        &self,
        io: &H,
        table: &str,
        user_descriptor: &RowDescriptor,
        tier: DurabilityTier,
    ) -> Result<Option<StoredRowBatch>, StorageError> {
        if tier_satisfies(self.current_row.confirmed_tier, tier) {
            return Ok(Some(self.current_row.clone()));
        }

        let Some(preview_batch_id) = self.batch_id_for_tier(tier) else {
            return Ok(None);
        };

        let mut row_by_batch_id = HashMap::new();
        let Some(metadata_row) = io.load_history_row_batch(
            table,
            self.current_row.branch.as_str(),
            self.current_row.row_id,
            preview_batch_id,
        )?
        else {
            return Err(StorageError::IoError(format!(
                "missing history row for preview batch {preview_batch_id}"
            )));
        };
        row_by_batch_id.insert(preview_batch_id, metadata_row);

        if let Some(ordinals) = self.winner_ordinals_for_tier(tier) {
            for ordinal in ordinals {
                let pool_index = usize::from(*ordinal);
                let Some(batch_id) = self.winner_batch_pool.get(pool_index).copied() else {
                    return Err(StorageError::IoError(format!(
                        "winner ordinal {pool_index} out of range for pool size {}",
                        self.winner_batch_pool.len()
                    )));
                };
                if row_by_batch_id.contains_key(&batch_id) {
                    continue;
                }
                let Some(row) = io.load_history_row_batch(
                    table,
                    self.current_row.branch.as_str(),
                    self.current_row.row_id,
                    batch_id,
                )?
                else {
                    return Err(StorageError::IoError(format!(
                        "missing history row for winner batch {batch_id}"
                    )));
                };
                row_by_batch_id.insert(batch_id, row);
            }
        }

        self.materialize_preview_for_tier_from_loaded_rows(user_descriptor, tier, &row_by_batch_id)
            .map_err(|err| StorageError::IoError(format!("materialize tier preview: {err}")))
    }
}
