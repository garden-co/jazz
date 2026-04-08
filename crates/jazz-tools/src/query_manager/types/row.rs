use std::ops::Deref;
use std::sync::Arc;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_bytes::ByteBuf;

use crate::commit::CommitId;
use crate::metadata::RowProvenance;
use crate::object::ObjectId;
use std::collections::HashMap;

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct RowBytes(Arc<[u8]>);

impl RowBytes {
    pub fn to_vec(&self) -> Vec<u8> {
        self.0.as_ref().to_vec()
    }
}

impl From<Vec<u8>> for RowBytes {
    fn from(value: Vec<u8>) -> Self {
        Self(Arc::from(value.into_boxed_slice()))
    }
}

impl From<&[u8]> for RowBytes {
    fn from(value: &[u8]) -> Self {
        Self(Arc::from(value))
    }
}

impl Deref for RowBytes {
    type Target = [u8];

    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}

impl AsRef<[u8]> for RowBytes {
    fn as_ref(&self) -> &[u8] {
        self.0.as_ref()
    }
}

impl PartialEq<Vec<u8>> for RowBytes {
    fn eq(&self, other: &Vec<u8>) -> bool {
        self.0.as_ref() == other.as_slice()
    }
}

impl PartialEq<RowBytes> for Vec<u8> {
    fn eq(&self, other: &RowBytes) -> bool {
        self.as_slice() == other.0.as_ref()
    }
}

impl Serialize for RowBytes {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_bytes(self.0.as_ref())
    }
}

impl<'de> Deserialize<'de> for RowBytes {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        Ok(Self::from(ByteBuf::deserialize(deserializer)?.into_vec()))
    }
}

/// A row with its object ID, binary data, and version reference.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Row {
    pub id: ObjectId,
    /// Binary encoded row data.
    pub data: RowBytes,
    pub version_id: CommitId,
    pub provenance: RowProvenance,
}

impl Row {
    pub fn new(
        id: ObjectId,
        data: impl Into<RowBytes>,
        version_id: CommitId,
        provenance: RowProvenance,
    ) -> Self {
        Self {
            id,
            data: data.into(),
            version_id,
            provenance,
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
pub struct OrderedAdded {
    pub id: ObjectId,
    pub index: usize,
    pub row: Row,
}

#[derive(Debug, Clone)]
pub struct OrderedRemoved {
    pub id: ObjectId,
    pub index: usize,
}

#[derive(Debug, Clone)]
pub struct OrderedUpdated {
    pub id: ObjectId,
    pub old_index: usize,
    pub new_index: usize,
    pub row: Option<Row>,
}

#[derive(Debug, Clone, Default)]
pub struct OrderedRowDelta {
    pub added: Vec<OrderedAdded>,
    pub removed: Vec<OrderedRemoved>,
    pub updated: Vec<OrderedUpdated>,
    pub pending: bool,
}

impl OrderedRowDelta {
    pub fn is_empty(&self) -> bool {
        self.added.is_empty() && self.removed.is_empty() && self.updated.is_empty()
    }
}

#[derive(Debug, Clone)]
pub struct OrderedDeltaResult {
    pub delta: OrderedRowDelta,
    pub ordered_ids_after: Vec<ObjectId>,
}

/// Build an ordered, wire-ready delta using an explicit post-order.
///
/// This variant avoids reconstructing order from delta semantics and should be used
/// when the caller already has the exact post-settle ordered IDs.
pub fn build_ordered_delta_with_post_ids(
    ordered_ids_before: &[ObjectId],
    ordered_ids_after: &[ObjectId],
    delta: &RowDelta,
    pending: bool,
) -> OrderedDeltaResult {
    let pre_index_by_id: HashMap<_, _> = ordered_ids_before
        .iter()
        .enumerate()
        .map(|(index, id)| (*id, index))
        .collect();
    let post_index_by_id: HashMap<_, _> = ordered_ids_after
        .iter()
        .enumerate()
        .map(|(index, id)| (*id, index))
        .collect();

    let added = delta
        .added
        .iter()
        .map(|row| OrderedAdded {
            id: row.id,
            index: post_index_by_id.get(&row.id).copied().unwrap_or(0),
            row: row.clone(),
        })
        .collect();

    let removed = delta
        .removed
        .iter()
        .map(|row| OrderedRemoved {
            id: row.id,
            index: pre_index_by_id.get(&row.id).copied().unwrap_or(0),
        })
        .collect();

    let mut updated = delta
        .moved
        .iter()
        .map(|id| OrderedUpdated {
            id: *id,
            old_index: pre_index_by_id.get(id).copied().unwrap_or(0),
            new_index: post_index_by_id.get(id).copied().unwrap_or(0),
            row: None,
        })
        .collect::<Vec<_>>();

    for (old, new) in &delta.updated {
        let old_index = pre_index_by_id.get(&old.id).copied().unwrap_or(0);
        let new_index = post_index_by_id.get(&new.id).copied().unwrap_or(0);
        let row_changed = old.data != new.data || old.version_id != new.version_id;

        if row_changed {
            updated.push(OrderedUpdated {
                id: new.id,
                old_index,
                new_index,
                row: Some(new.clone()),
            });
        } else if old_index != new_index {
            updated.push(OrderedUpdated {
                id: new.id,
                old_index,
                new_index,
                row: None,
            });
        }
    }

    OrderedDeltaResult {
        delta: OrderedRowDelta {
            added,
            removed,
            updated,
            pending,
        },
        ordered_ids_after: ordered_ids_after.to_vec(),
    }
}
