use std::ops::Deref;
use std::sync::Arc;

use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_bytes::ByteBuf;

use crate::tools::Value;
use crate::tools::metadata::RowProvenance;
use crate::tools::object::ResultKey;
use crate::tools::transaction::BatchId;

/// One named, typed field in a materialized query result.
#[derive(Debug, Clone, PartialEq)]
pub struct QueryResultField {
    /// Stable output name. Joined fields are source-qualified (for example,
    /// `author.name`).
    pub name: String,
    pub value: Value,
}

/// One materialized query result with its stable identity and named fields.
#[derive(Debug, Clone, PartialEq)]
pub struct QueryResult {
    /// Stable identity of this result, including every joined source row.
    pub key: ResultKey,
    /// Typed fields in the query's declared output order.
    pub fields: Vec<QueryResultField>,
}

impl QueryResult {
    pub fn new(key: ResultKey, fields: Vec<QueryResultField>) -> Self {
        Self { key, fields }
    }

    /// Look up a field by its stable output name.
    pub fn get(&self, name: &str) -> Option<&Value> {
        self.fields
            .iter()
            .find(|field| field.name == name)
            .map(|field| &field.value)
    }

    pub(crate) fn into_values(self) -> Vec<Value> {
        self.fields.into_iter().map(|field| field.value).collect()
    }
}

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

/// A maintained output row with its result key, binary data, and batch identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Row {
    pub id: ResultKey,
    /// Binary encoded row data.
    pub data: RowBytes,
    pub batch_id: BatchId,
    pub provenance: RowProvenance,
}

impl Row {
    pub fn new(
        id: ResultKey,
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
    pub moved: Vec<ResultKey>,
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
    pub id: ResultKey,
    pub index: usize,
    pub row: Row,
}

#[derive(Debug, Clone)]
pub struct OrderedRemoved {
    pub id: ResultKey,
    pub index: usize,
}

#[derive(Debug, Clone)]
pub struct OrderedUpdated {
    pub id: ResultKey,
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
