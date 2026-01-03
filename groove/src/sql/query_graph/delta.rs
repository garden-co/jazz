//! Delta types for representing row changes.

use std::collections::HashMap;

use crate::commit::CommitId;
use crate::sql::row::Row;
use crate::sql::ObjectId;

/// Reference to prior row state via commit graph.
///
/// Contains the frontier commit IDs before a write operation.
/// Old row values can be looked up on-demand from these tips.
#[derive(Clone, Debug, Default)]
pub struct PriorState {
    /// Frontier commit IDs before the write.
    /// Empty if the row was just created.
    pub prior_tips: Vec<CommitId>,
}

impl PriorState {
    /// Create a new PriorState with the given commit tips.
    pub fn new(prior_tips: Vec<CommitId>) -> Self {
        Self { prior_tips }
    }

    /// Create an empty PriorState (for newly created rows).
    pub fn empty() -> Self {
        Self::default()
    }

    /// Returns true if this row was just created (no prior state).
    pub fn is_new(&self) -> bool {
        self.prior_tips.is_empty()
    }
}

/// A change to a single row.
#[derive(Clone, Debug)]
pub enum RowDelta {
    /// Row was inserted (no prior state).
    Added(Row),

    /// Row was deleted.
    Removed {
        /// The ID of the deleted row.
        id: ObjectId,
        /// Prior tips for looking up deleted row data if needed.
        prior: PriorState,
    },

    /// Row was updated.
    Updated {
        /// The ID of the updated row.
        id: ObjectId,
        /// The new row values.
        new: Row,
        /// Prior tips for looking up old values on-demand.
        prior: PriorState,
    },
}

impl RowDelta {
    /// Get the row ID affected by this delta.
    pub fn row_id(&self) -> ObjectId {
        match self {
            RowDelta::Added(row) => row.id,
            RowDelta::Removed { id, .. } => *id,
            RowDelta::Updated { id, .. } => *id,
        }
    }

    /// Get the new row data if this delta has it.
    pub fn new_row(&self) -> Option<&Row> {
        match self {
            RowDelta::Added(row) => Some(row),
            RowDelta::Updated { new, .. } => Some(new),
            RowDelta::Removed { .. } => None,
        }
    }

    /// Returns true if this delta has prior state that can be looked up.
    pub fn has_prior(&self) -> bool {
        match self {
            RowDelta::Added(_) => false,
            RowDelta::Removed { prior, .. } => !prior.is_new(),
            RowDelta::Updated { prior, .. } => !prior.is_new(),
        }
    }
}

/// A batch of row changes.
#[derive(Clone, Debug, Default)]
pub struct DeltaBatch {
    deltas: Vec<RowDelta>,
}

impl DeltaBatch {
    /// Create an empty batch.
    pub fn new() -> Self {
        Self::default()
    }

    /// Create a batch with a single Added delta.
    pub fn added(row: Row) -> Self {
        Self {
            deltas: vec![RowDelta::Added(row)],
        }
    }

    /// Create a batch with a single Updated delta.
    pub fn updated(id: ObjectId, new: Row, prior_tips: Vec<CommitId>) -> Self {
        Self {
            deltas: vec![RowDelta::Updated {
                id,
                new,
                prior: PriorState::new(prior_tips),
            }],
        }
    }

    /// Create a batch with a single Removed delta.
    pub fn removed(id: ObjectId, prior_tips: Vec<CommitId>) -> Self {
        Self {
            deltas: vec![RowDelta::Removed {
                id,
                prior: PriorState::new(prior_tips),
            }],
        }
    }

    /// Add a delta to the batch.
    pub fn push(&mut self, delta: RowDelta) {
        self.deltas.push(delta);
    }

    /// Extend this batch with deltas from another batch.
    pub fn extend(&mut self, other: DeltaBatch) {
        self.deltas.extend(other.deltas);
    }

    /// Returns true if the batch is empty.
    pub fn is_empty(&self) -> bool {
        self.deltas.is_empty()
    }

    /// Returns the number of deltas in the batch.
    pub fn len(&self) -> usize {
        self.deltas.len()
    }

    /// Iterate over deltas by reference.
    pub fn iter(&self) -> impl Iterator<Item = &RowDelta> {
        self.deltas.iter()
    }

    /// Consume the batch and iterate over deltas.
    pub fn into_iter(self) -> impl Iterator<Item = RowDelta> {
        self.deltas.into_iter()
    }

    /// Compact the batch by removing redundant changes.
    ///
    /// For example, if a row is Added and then Removed, both entries
    /// cancel out. If a row is Updated multiple times, only the final
    /// state matters.
    pub fn compact(&mut self) {
        if self.deltas.len() <= 1 {
            return;
        }

        // Track final state per row: None = removed/not present, Some = added/updated
        let mut final_state: HashMap<ObjectId, Option<(Row, PriorState)>> = HashMap::new();
        // Track which rows existed before this batch (had prior state on first delta)
        let mut existed_before: HashMap<ObjectId, bool> = HashMap::new();

        for delta in self.deltas.drain(..) {
            let id = delta.row_id();

            // Remember if row existed before (only on first encounter)
            existed_before.entry(id).or_insert_with(|| delta.has_prior());

            match delta {
                RowDelta::Added(row) => {
                    final_state.insert(id, Some((row, PriorState::empty())));
                }
                RowDelta::Removed {  .. } => {
                    final_state.insert(id, None);
                    // Keep the prior state if this is the first delta for this row
                    if !existed_before.get(&id).copied().unwrap_or(false) {
                        // Row was added then removed - just remove from final_state entirely
                        final_state.remove(&id);
                    }
                }
                RowDelta::Updated { new, prior, .. } => {
                    // Keep prior from first delta for this row
                    let prior_to_use = if let Some(Some((_, existing_prior))) = final_state.get(&id)
                    {
                        existing_prior.clone()
                    } else {
                        prior
                    };
                    final_state.insert(id, Some((new, prior_to_use)));
                }
            }
        }

        // Rebuild deltas from final state
        for (id, state) in final_state {
            let existed = existed_before.get(&id).copied().unwrap_or(false);
            match state {
                Some((row, prior)) => {
                    if existed {
                        self.deltas.push(RowDelta::Updated { id, new: row, prior });
                    } else {
                        self.deltas.push(RowDelta::Added(row));
                    }
                }
                None => {
                    if existed {
                        // Row existed and was removed
                        self.deltas.push(RowDelta::Removed {
                            id,
                            prior: PriorState::empty(), // Prior was already used
                        });
                    }
                    // If row didn't exist and was removed, it was added then removed - no delta
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sql::row::Value;

    fn make_row(id: u128, name: &str) -> Row {
        Row::new(ObjectId::new(id), vec![Value::String(name.to_string())])
    }

    #[test]
    fn delta_batch_empty() {
        let batch = DeltaBatch::new();
        assert!(batch.is_empty());
        assert_eq!(batch.len(), 0);
    }

    #[test]
    fn delta_batch_added() {
        let row = make_row(1, "Alice");
        let batch = DeltaBatch::added(row.clone());

        assert!(!batch.is_empty());
        assert_eq!(batch.len(), 1);

        let delta = batch.iter().next().unwrap();
        assert_eq!(delta.row_id(), ObjectId::new(1));
        assert!(matches!(delta, RowDelta::Added(_)));
    }

    #[test]
    fn delta_batch_compact_add_remove() {
        let row = make_row(1, "Alice");
        let mut batch = DeltaBatch::new();

        batch.push(RowDelta::Added(row));
        batch.push(RowDelta::Removed {
            id: ObjectId::new(1),
            prior: PriorState::empty(),
        });

        batch.compact();

        // Add followed by remove with no prior state = nothing
        assert!(batch.is_empty());
    }

    #[test]
    fn delta_batch_compact_multiple_updates() {
        let row1 = make_row(1, "Alice");
        let row2 = make_row(1, "Alicia");
        let row3 = make_row(1, "Alex");

        let mut batch = DeltaBatch::new();
        batch.push(RowDelta::Updated {
            id: ObjectId::new(1),
            new: row1,
            prior: PriorState::new(vec![CommitId::from_bytes([1; 32])]), // Existed before
        });
        batch.push(RowDelta::Updated {
            id: ObjectId::new(1),
            new: row2,
            prior: PriorState::empty(),
        });
        batch.push(RowDelta::Updated {
            id: ObjectId::new(1),
            new: row3.clone(),
            prior: PriorState::empty(),
        });

        batch.compact();

        // Should have single update to final state
        assert_eq!(batch.len(), 1);
        let delta = batch.iter().next().unwrap();
        assert!(matches!(delta, RowDelta::Updated { new, .. } if new.values[0] == Value::String("Alex".to_string())));
    }

    #[test]
    fn row_delta_accessors() {
        let row = make_row(1, "Alice");

        let added = RowDelta::Added(row.clone());
        assert_eq!(added.row_id(), ObjectId::new(1));
        assert!(added.new_row().is_some());
        assert!(!added.has_prior());

        let removed = RowDelta::Removed {
            id: ObjectId::new(2),
            prior: PriorState::new(vec![CommitId::from_bytes([1; 32])]),
        };
        assert_eq!(removed.row_id(), ObjectId::new(2));
        assert!(removed.new_row().is_none());
        assert!(removed.has_prior());

        let updated = RowDelta::Updated {
            id: ObjectId::new(1),
            new: row,
            prior: PriorState::empty(),
        };
        assert_eq!(updated.row_id(), ObjectId::new(1));
        assert!(updated.new_row().is_some());
        assert!(!updated.has_prior());
    }
}
