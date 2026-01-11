use std::collections::HashSet;

use crate::object::ObjectId;

/// Table row set: tracks which row IDs belong to a table.
/// Stored as an object for reactive updates.
#[derive(Debug, Clone, Default)]
pub struct TableRows {
    /// Set of row IDs in the table.
    row_ids: HashSet<ObjectId>,
}

impl TableRows {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn add(&mut self, row_id: ObjectId) {
        self.row_ids.insert(row_id);
    }

    pub fn remove(&mut self, row_id: ObjectId) {
        self.row_ids.remove(&row_id);
    }

    pub fn contains(&self, row_id: ObjectId) -> bool {
        self.row_ids.contains(&row_id)
    }

    pub fn iter(&self) -> impl Iterator<Item = ObjectId> + '_ {
        self.row_ids.iter().copied()
    }

    /// Convert to a vector of row IDs.
    pub fn into_vec(self) -> Vec<ObjectId> {
        self.row_ids.into_iter().collect()
    }

    pub fn len(&self) -> usize {
        self.row_ids.len()
    }

    pub fn is_empty(&self) -> bool {
        self.row_ids.is_empty()
    }

    /// Serialize to bytes.
    /// Format: [count: u32] [row_ids: u128...]
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();
        buf.extend_from_slice(&(self.row_ids.len() as u32).to_le_bytes());
        for row_id in &self.row_ids {
            buf.extend_from_slice(&row_id.0.to_le_bytes());
        }
        buf
    }

    /// Deserialize from bytes.
    pub fn from_bytes(data: &[u8]) -> Result<Self, String> {
        if data.len() < 4 {
            return Ok(Self::new());
        }

        let count =
            u32::from_le_bytes(data[0..4].try_into().map_err(|_| "invalid count")?) as usize;

        let mut row_ids = HashSet::new();
        let mut pos = 4;

        for _ in 0..count {
            if pos + 16 > data.len() {
                return Err("truncated row_id".to_string());
            }
            let row_id = ObjectId::from_le_bytes(
                data[pos..pos + 16]
                    .try_into()
                    .map_err(|_| "invalid row_id")?,
            );
            pos += 16;
            row_ids.insert(row_id);
        }

        Ok(TableRows { row_ids })
    }
}
