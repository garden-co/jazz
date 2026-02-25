use crate::commit::CommitId;
use crate::object::ObjectId;
use std::collections::HashMap;

/// A row with its object ID, binary data, and commit reference.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Row {
    pub id: ObjectId,
    /// Binary encoded row data.
    pub data: Vec<u8>,
    pub commit_id: CommitId,
}

impl Row {
    pub fn new(id: ObjectId, data: Vec<u8>, commit_id: CommitId) -> Self {
        Self {
            id,
            data,
            commit_id,
        }
    }
}

/// Delta for row-level changes (after materialization).
/// Contains full row data for processing by filter/sort/output nodes.
#[derive(Debug, Clone, Default)]
pub struct RowDelta {
    pub added: Vec<Row>,
    pub removed: Vec<Row>,
    /// Rows that stayed in-window but changed position.
    /// Semantics: detach these IDs from current order, then append in listed order.
    pub moved: Vec<ObjectId>,
    /// Updated rows as (old, new) pairs.
    pub updated: Vec<(Row, Row)>,
}

impl RowDelta {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_empty(&self) -> bool {
        self.added.is_empty()
            && self.removed.is_empty()
            && self.moved.is_empty()
            && self.updated.is_empty()
    }
}

#[derive(Debug, Clone)]
pub struct IndexedAdded {
    pub id: ObjectId,
    pub index: usize,
    pub row: Row,
}

#[derive(Debug, Clone)]
pub struct IndexedRemoved {
    pub id: ObjectId,
    pub index: usize,
}

#[derive(Debug, Clone)]
pub struct IndexedUpdated {
    pub id: ObjectId,
    pub old_index: usize,
    pub new_index: usize,
    pub row: Option<Row>,
}

#[derive(Debug, Clone, Default)]
pub struct IndexedRowDelta {
    pub added: Vec<IndexedAdded>,
    pub removed: Vec<IndexedRemoved>,
    pub updated: Vec<IndexedUpdated>,
    pub pending: bool,
}

impl IndexedRowDelta {
    pub fn is_empty(&self) -> bool {
        self.added.is_empty() && self.removed.is_empty() && self.updated.is_empty()
    }
}

#[derive(Debug, Clone)]
pub struct ProjectedRowDelta {
    pub delta: IndexedRowDelta,
    pub post_ids: Vec<ObjectId>,
}

/// Build an indexed, wire-ready delta using an explicit post-order.
///
/// This variant avoids reconstructing order from delta semantics and should be used
/// when the caller already has the exact post-settle ordered IDs.
pub fn project_row_delta_with_post_ids(
    current_ids: &[ObjectId],
    post_ids: &[ObjectId],
    delta: &RowDelta,
    pending: bool,
) -> ProjectedRowDelta {
    let pre_index_by_id: HashMap<_, _> = current_ids
        .iter()
        .enumerate()
        .map(|(index, id)| (*id, index))
        .collect();
    let post_index_by_id: HashMap<_, _> = post_ids
        .iter()
        .enumerate()
        .map(|(index, id)| (*id, index))
        .collect();

    let added = delta
        .added
        .iter()
        .map(|row| IndexedAdded {
            id: row.id,
            index: post_index_by_id.get(&row.id).copied().unwrap_or(0),
            row: row.clone(),
        })
        .collect();

    let removed = delta
        .removed
        .iter()
        .map(|row| IndexedRemoved {
            id: row.id,
            index: pre_index_by_id.get(&row.id).copied().unwrap_or(0),
        })
        .collect();

    let mut updated = delta
        .moved
        .iter()
        .map(|id| IndexedUpdated {
            id: *id,
            old_index: pre_index_by_id.get(id).copied().unwrap_or(0),
            new_index: post_index_by_id.get(id).copied().unwrap_or(0),
            row: None,
        })
        .collect::<Vec<_>>();

    for (old, new) in &delta.updated {
        let old_index = pre_index_by_id.get(&old.id).copied().unwrap_or(0);
        let new_index = post_index_by_id.get(&new.id).copied().unwrap_or(0);
        let row_changed = old.data != new.data || old.commit_id != new.commit_id;

        if row_changed {
            updated.push(IndexedUpdated {
                id: new.id,
                old_index,
                new_index,
                row: Some(new.clone()),
            });
        } else if old_index != new_index {
            updated.push(IndexedUpdated {
                id: new.id,
                old_index,
                new_index,
                row: None,
            });
        }
    }

    ProjectedRowDelta {
        delta: IndexedRowDelta {
            added,
            removed,
            updated,
            pending,
        },
        post_ids: post_ids.to_vec(),
    }
}
