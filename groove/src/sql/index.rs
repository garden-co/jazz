use std::collections::{HashMap, HashSet};

use crate::sql::types::ObjectId;

/// Reference index: maps target_id -> set of source_row_ids.
/// One index per (source_table, source_column) pair.
#[derive(Debug, Clone, Default)]
pub struct RefIndex {
    /// target_id -> source_row_ids that reference it
    entries: HashMap<ObjectId, HashSet<ObjectId>>,
}

impl RefIndex {
    pub fn new() -> Self {
        Self::default()
    }

    /// Add a reference: source_row references target_id.
    pub fn add(&mut self, target_id: ObjectId, source_row_id: ObjectId) {
        self.entries
            .entry(target_id)
            .or_default()
            .insert(source_row_id);
    }

    /// Remove a reference.
    pub fn remove(&mut self, target_id: ObjectId, source_row_id: ObjectId) {
        if let Some(set) = self.entries.get_mut(&target_id) {
            set.remove(&source_row_id);
            if set.is_empty() {
                self.entries.remove(&target_id);
            }
        }
    }

    /// Get all source rows referencing a target.
    pub fn get(&self, target_id: ObjectId) -> impl Iterator<Item = ObjectId> + '_ {
        self.entries
            .get(&target_id)
            .into_iter()
            .flat_map(|set| set.iter().copied())
    }

    /// Serialize the index to bytes.
    /// Format: [entry_count: u32] [entries...]
    /// Each entry: [target_id: u128] [source_count: u32] [source_ids: u128...]
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::new();

        // Entry count
        buf.extend_from_slice(&(self.entries.len() as u32).to_le_bytes());

        for (target_id, source_ids) in &self.entries {
            // Target ID
            buf.extend_from_slice(&target_id.to_le_bytes());
            // Source count
            buf.extend_from_slice(&(source_ids.len() as u32).to_le_bytes());
            // Source IDs
            for source_id in source_ids {
                buf.extend_from_slice(&source_id.to_le_bytes());
            }
        }

        buf
    }

    /// Deserialize an index from bytes.
    pub fn from_bytes(data: &[u8]) -> Result<Self, String> {
        if data.len() < 4 {
            return Ok(Self::new()); // Empty index
        }

        let mut pos = 0;
        let entry_count = u32::from_le_bytes(
            data[pos..pos + 4].try_into().map_err(|_| "invalid entry count")?
        ) as usize;
        pos += 4;

        let mut entries = HashMap::new();

        for _ in 0..entry_count {
            if pos + 16 > data.len() {
                return Err("truncated target_id".to_string());
            }
            let target_id = u128::from_le_bytes(
                data[pos..pos + 16].try_into().map_err(|_| "invalid target_id")?
            );
            pos += 16;

            if pos + 4 > data.len() {
                return Err("truncated source_count".to_string());
            }
            let source_count = u32::from_le_bytes(
                data[pos..pos + 4].try_into().map_err(|_| "invalid source_count")?
            ) as usize;
            pos += 4;

            let mut source_ids = HashSet::new();
            for _ in 0..source_count {
                if pos + 16 > data.len() {
                    return Err("truncated source_id".to_string());
                }
                let source_id = u128::from_le_bytes(
                    data[pos..pos + 16].try_into().map_err(|_| "invalid source_id")?
                );
                pos += 16;
                source_ids.insert(source_id);
            }

            entries.insert(target_id, source_ids);
        }

        Ok(RefIndex { entries })
    }
}
