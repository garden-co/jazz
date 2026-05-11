use std::ops::Deref;
use std::sync::Arc;

use serde::de::Visitor;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_bytes::ByteBuf;

use crate::metadata::RowProvenance;
use crate::object::ObjectId;
use crate::row_histories::BatchId;
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

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
pub struct SharedString(Arc<str>);

impl SharedString {
    pub fn as_str(&self) -> &str {
        self.0.as_ref()
    }
}

impl From<String> for SharedString {
    fn from(value: String) -> Self {
        Self(Arc::from(value.into_boxed_str()))
    }
}

impl From<&str> for SharedString {
    fn from(value: &str) -> Self {
        Self(Arc::from(value))
    }
}

impl From<SharedString> for String {
    fn from(value: SharedString) -> Self {
        value.0.as_ref().to_owned()
    }
}

impl From<&SharedString> for String {
    fn from(value: &SharedString) -> Self {
        value.0.as_ref().to_owned()
    }
}

impl Deref for SharedString {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        self.0.as_ref()
    }
}

impl AsRef<str> for SharedString {
    fn as_ref(&self) -> &str {
        self.0.as_ref()
    }
}

impl std::borrow::Borrow<str> for SharedString {
    fn borrow(&self) -> &str {
        self.0.as_ref()
    }
}

impl PartialEq<&str> for SharedString {
    fn eq(&self, other: &&str) -> bool {
        self.0.as_ref() == *other
    }
}

impl PartialEq<SharedString> for &str {
    fn eq(&self, other: &SharedString) -> bool {
        *self == other.0.as_ref()
    }
}

impl PartialEq<String> for SharedString {
    fn eq(&self, other: &String) -> bool {
        self.0.as_ref() == other.as_str()
    }
}

impl PartialEq<SharedString> for String {
    fn eq(&self, other: &SharedString) -> bool {
        self.as_str() == other.0.as_ref()
    }
}

impl std::fmt::Display for SharedString {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.0.as_ref())
    }
}

impl Serialize for SharedString {
    fn serialize<S: Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(self.0.as_ref())
    }
}

impl<'de> Deserialize<'de> for SharedString {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct SharedStringVisitor;

        impl Visitor<'_> for SharedStringVisitor {
            type Value = SharedString;

            fn expecting(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                formatter.write_str("a string")
            }

            fn visit_borrowed_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(SharedString::from(value))
            }

            fn visit_str<E>(self, value: &str) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(SharedString::from(value))
            }

            fn visit_string<E>(self, value: String) -> Result<Self::Value, E>
            where
                E: serde::de::Error,
            {
                Ok(SharedString::from(value))
            }
        }

        deserializer.deserialize_str(SharedStringVisitor)
    }
}

/// A row with its object ID, binary data, and batch identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Row {
    pub id: ObjectId,
    /// Binary encoded row data.
    pub data: RowBytes,
    pub batch_id: BatchId,
    pub provenance: RowProvenance,
}

impl Row {
    pub fn new(
        id: ObjectId,
        data: impl Into<RowBytes>,
        batch_id: BatchId,
        provenance: RowProvenance,
    ) -> Self {
        Self {
            id,
            data: data.into(),
            batch_id,
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
        let row_changed = old.data != new.data || old.batch_id != new.batch_id;

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
