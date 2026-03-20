use std::hash::{Hash, Hasher};

use ahash::AHashSet;

use crate::commit::CommitId;
use crate::object::{BranchName, ObjectId};

use super::encoding::{decode_row, encode_row};
use super::*;

// ============================================================================
// Unified Tuple Model - For JOIN support and progressive materialization
// ============================================================================

/// A single element in a tuple - either just an ID or a fully loaded row.
/// Used for progressive materialization: start with IDs, load data on demand.
#[derive(Clone, Debug)]
pub enum TupleElement {
    /// Just the ID - row data not yet loaded.
    Id(ObjectId),
    /// Fully materialized row with ID, content, and commit reference.
    Row {
        id: ObjectId,
        content: Vec<u8>,
        commit_id: CommitId,
    },
}

impl TupleElement {
    /// Get the object ID regardless of materialization state.
    pub fn id(&self) -> ObjectId {
        match self {
            TupleElement::Id(id) => *id,
            TupleElement::Row { id, .. } => *id,
        }
    }

    /// Check if this element has been fully materialized (row data loaded).
    pub fn is_materialized(&self) -> bool {
        matches!(self, TupleElement::Row { .. })
    }

    /// Get the row content if materialized.
    pub fn content(&self) -> Option<&[u8]> {
        match self {
            TupleElement::Id(_) => None,
            TupleElement::Row { content, .. } => Some(content),
        }
    }

    /// Get the commit ID if materialized.
    pub fn commit_id(&self) -> Option<CommitId> {
        match self {
            TupleElement::Id(_) => None,
            TupleElement::Row { commit_id, .. } => Some(*commit_id),
        }
    }

    /// Create a TupleElement from a Row.
    pub fn from_row(row: &Row) -> Self {
        TupleElement::Row {
            id: row.id,
            content: row.data.clone(),
            commit_id: row.commit_id,
        }
    }

    /// Convert to a Row if materialized.
    pub fn to_row(&self) -> Option<Row> {
        match self {
            TupleElement::Id(_) => None,
            TupleElement::Row {
                id,
                content,
                commit_id,
            } => Some(Row::new(*id, content.clone(), *commit_id)),
        }
    }
}

/// A tuple of elements with identity based on IDs only.
/// Length corresponds to number of tables in query (1 for single-table, 2 for join, etc.)
#[derive(Clone, Debug)]
pub struct Tuple(pub Vec<TupleElement>, pub TupleProvenance);

pub type ScopedObject = (ObjectId, BranchName);
pub type TupleProvenance = AHashSet<ScopedObject>;

#[derive(Clone, Debug)]
pub struct LoadedRow {
    pub data: Vec<u8>,
    pub commit_id: CommitId,
    pub provenance: TupleProvenance,
}

impl LoadedRow {
    pub fn new(data: Vec<u8>, commit_id: CommitId, provenance: TupleProvenance) -> Self {
        Self {
            data,
            commit_id,
            provenance,
        }
    }
}

impl Tuple {
    /// Create a new tuple from elements.
    pub fn new(elements: Vec<TupleElement>) -> Self {
        Self(elements, TupleProvenance::new())
    }

    /// Create a tuple with explicit contributing-object provenance.
    pub fn new_with_provenance(elements: Vec<TupleElement>, provenance: TupleProvenance) -> Self {
        Self(elements, provenance)
    }

    /// Create a single-element tuple from an ObjectId.
    pub fn from_id(id: ObjectId) -> Self {
        Self::new(vec![TupleElement::Id(id)])
    }

    /// Create a single-element tuple from an ObjectId scoped to a branch.
    pub fn from_scoped_id(id: ObjectId, branch: BranchName) -> Self {
        Self::new_with_provenance(
            vec![TupleElement::Id(id)],
            [(id, branch)].into_iter().collect(),
        )
    }

    /// Create a single-element tuple from a Row.
    pub fn from_row(row: &Row) -> Self {
        Self::new(vec![TupleElement::from_row(row)])
    }

    /// Get all IDs in the tuple.
    pub fn ids(&self) -> Vec<ObjectId> {
        self.id_iter().collect()
    }

    /// Iterate over the IDs that define tuple identity.
    pub fn id_iter(&self) -> impl Iterator<Item = ObjectId> + '_ {
        self.0.iter().map(TupleElement::id)
    }

    /// Get the first ID (convenience for single-table queries).
    pub fn first_id(&self) -> Option<ObjectId> {
        self.0.first().map(|e| e.id())
    }

    /// Get the number of elements (tables) in this tuple.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Check if the tuple is empty.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Get an element by index.
    pub fn get(&self, index: usize) -> Option<&TupleElement> {
        self.0.get(index)
    }

    /// Get a mutable element by index.
    pub fn get_mut(&mut self, index: usize) -> Option<&mut TupleElement> {
        self.0.get_mut(index)
    }

    /// Check if all elements are fully materialized.
    pub fn is_fully_materialized(&self) -> bool {
        self.0.iter().all(|e| e.is_materialized())
    }

    /// Get the first element as a Row (for single-table queries).
    pub fn to_single_row(&self) -> Option<Row> {
        self.0.first().and_then(|e| e.to_row())
    }

    /// Flatten a multi-element tuple into a single-element tuple.
    ///
    /// Decodes each element using its descriptor, combines all values, and re-encodes
    /// with a combined descriptor. The result is a single-element tuple that can be
    /// converted to a Row.
    ///
    /// Arguments:
    /// - `descriptors`: One descriptor per element in the tuple
    /// - `combined_descriptor`: The combined descriptor for encoding the merged row
    ///
    /// Returns None if any element is not materialized or if encoding fails.
    pub fn flatten_with_descriptors(
        &self,
        descriptors: &[RowDescriptor],
        combined_descriptor: &RowDescriptor,
    ) -> Option<Tuple> {
        if descriptors.len() != self.0.len() {
            return None;
        }

        // Collect all values from all elements
        let mut all_values = Vec::new();
        let mut first_commit_id = None;

        for (elem, desc) in self.0.iter().zip(descriptors.iter()) {
            let content = elem.content()?;
            let values = decode_row(desc, content).ok()?;
            all_values.extend(values);

            if first_commit_id.is_none() {
                first_commit_id = elem.commit_id();
            }
        }

        // Encode with combined descriptor
        let combined_content = encode_row(combined_descriptor, &all_values).ok()?;

        // Use first element's ID as the "primary" ID for the flattened row
        let first_id = self.first_id()?;
        let commit_id = first_commit_id.unwrap_or(CommitId([0; 32]));

        Some(
            Tuple::new(vec![TupleElement::Row {
                id: first_id,
                content: combined_content,
                commit_id,
            }])
            .with_provenance(self.provenance().clone()),
        )
    }

    /// Iterate over elements.
    pub fn iter(&self) -> impl Iterator<Item = &TupleElement> {
        self.0.iter()
    }

    /// Iterate mutably over elements.
    pub fn iter_mut(&mut self) -> impl Iterator<Item = &mut TupleElement> {
        self.0.iter_mut()
    }

    /// Get the contributing-object provenance for this tuple.
    pub fn provenance(&self) -> &TupleProvenance {
        &self.1
    }

    /// Replace the contributing-object provenance for this tuple.
    pub fn with_provenance(mut self, provenance: TupleProvenance) -> Self {
        self.1 = provenance;
        self
    }

    /// Merge another tuple's provenance into this tuple.
    pub fn merge_provenance_from(&mut self, other: &Tuple) {
        self.1.extend(other.1.iter().copied());
    }

    /// Merge an explicit provenance set into this tuple.
    pub fn merge_provenance(&mut self, provenance: &TupleProvenance) {
        self.1.extend(provenance.iter().copied());
    }
}

// Hash and Eq based on IDs only (not content).
// This allows tuples with the same IDs but different content to be treated as equal
// for set membership, while updates track content changes separately.
impl Hash for Tuple {
    fn hash<H: Hasher>(&self, state: &mut H) {
        for id in self.id_iter() {
            id.hash(state);
        }
    }
}

impl PartialEq for Tuple {
    fn eq(&self, other: &Self) -> bool {
        if self.0.len() != other.0.len() {
            return false;
        }
        self.id_iter().eq(other.id_iter())
    }
}

impl Eq for Tuple {}

/// Delta for tuple-level changes with progressive materialization.
/// Replaces both IdDelta (for unmaterialized) and RowDelta (for materialized).
#[derive(Clone, Debug, Default)]
pub struct TupleDelta {
    /// Tuples added to the result set.
    pub added: Vec<Tuple>,
    /// Tuples removed from the result set.
    pub removed: Vec<Tuple>,
    /// Tuples that stayed in-window but changed position.
    /// Semantics: detach these IDs, then append in listed order.
    pub moved: Vec<Tuple>,
    /// Updated tuples as (old, new) pairs - same IDs, different content.
    pub updated: Vec<(Tuple, Tuple)>,
}

impl TupleDelta {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn is_empty(&self) -> bool {
        self.added.is_empty()
            && self.removed.is_empty()
            && self.moved.is_empty()
            && self.updated.is_empty()
    }

    /// Convert to a RowDelta (for single-table queries where all tuples are length-1).
    /// Returns None if any tuple has multiple elements or is not fully materialized.
    pub fn to_row_delta(&self) -> Option<RowDelta> {
        let added: Option<Vec<Row>> = self
            .added
            .iter()
            .map(|t| {
                if t.len() == 1 {
                    t.to_single_row()
                } else {
                    None
                }
            })
            .collect();
        let removed: Option<Vec<Row>> = self
            .removed
            .iter()
            .map(|t| {
                if t.len() == 1 {
                    t.to_single_row()
                } else {
                    None
                }
            })
            .collect();

        let mut updated = Vec::with_capacity(self.updated.len());
        for (old, new) in &self.updated {
            if old.len() != 1 || new.len() != 1 {
                return None;
            }
            let old_row = old.to_single_row()?;
            let new_row = new.to_single_row()?;
            if old_row != new_row {
                updated.push((old_row, new_row));
            }
        }

        Some(RowDelta {
            added: added?,
            removed: removed?,
            moved: self.moved.iter().filter_map(|t| t.first_id()).collect(),
            updated,
        })
    }

    /// Convert to a RowDelta, flattening multi-element tuples using descriptors.
    ///
    /// This handles join results by merging all elements into single rows.
    /// For single-element tuples, this is equivalent to `to_row_delta()`.
    ///
    /// Arguments:
    /// - `descriptors`: One descriptor per element in each tuple
    /// - `combined_descriptor`: The combined descriptor for encoding merged rows
    ///
    /// Returns None if flattening fails for any tuple.
    pub fn flatten_to_row_delta(
        &self,
        descriptors: &[RowDescriptor],
        combined_descriptor: &RowDescriptor,
    ) -> Option<RowDelta> {
        let added: Option<Vec<Row>> = self
            .added
            .iter()
            .map(|t| {
                if t.len() == 1 {
                    t.to_single_row()
                } else {
                    t.flatten_with_descriptors(descriptors, combined_descriptor)?
                        .to_single_row()
                }
            })
            .collect();
        let removed: Option<Vec<Row>> = self
            .removed
            .iter()
            .map(|t| {
                if t.len() == 1 {
                    t.to_single_row()
                } else {
                    t.flatten_with_descriptors(descriptors, combined_descriptor)?
                        .to_single_row()
                }
            })
            .collect();
        let mut updated = Vec::with_capacity(self.updated.len());
        for (old, new) in &self.updated {
            let old_row = if old.len() == 1 {
                old.to_single_row()
            } else {
                old.flatten_with_descriptors(descriptors, combined_descriptor)?
                    .to_single_row()
            }?;
            let new_row = if new.len() == 1 {
                new.to_single_row()
            } else {
                new.flatten_with_descriptors(descriptors, combined_descriptor)?
                    .to_single_row()
            }?;
            if old_row != new_row {
                updated.push((old_row, new_row));
            }
        }

        Some(RowDelta {
            added: added?,
            removed: removed?,
            moved: self.moved.iter().filter_map(|t| t.first_id()).collect(),
            updated,
        })
    }

    /// Merge another TupleDelta into this one.
    pub fn merge(&mut self, other: TupleDelta) {
        self.added.extend(other.added);
        self.removed.extend(other.removed);
        self.moved.extend(other.moved);
        self.updated.extend(other.updated);
    }
}

// ============================================================================
// MaterializationState - Per-element materialization tracking
// ============================================================================

/// Per-element materialization tracking.
/// materialized[i] == true means element i has row content loaded.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MaterializationState {
    materialized: Vec<bool>,
}

impl MaterializationState {
    /// Create state where all elements are ID-only (not materialized).
    pub fn all_ids(element_count: usize) -> Self {
        Self {
            materialized: vec![false; element_count],
        }
    }

    /// Create state where all elements are materialized.
    pub fn all_materialized(element_count: usize) -> Self {
        Self {
            materialized: vec![true; element_count],
        }
    }

    /// Check if a specific element is materialized.
    pub fn is_materialized(&self, element_index: usize) -> bool {
        self.materialized
            .get(element_index)
            .copied()
            .unwrap_or(false)
    }

    /// Check if all specified elements are materialized.
    pub fn are_all_materialized(&self, elements: &std::collections::HashSet<usize>) -> bool {
        elements.iter().all(|&i| self.is_materialized(i))
    }

    /// Check if all elements are materialized.
    pub fn is_fully_materialized(&self) -> bool {
        self.materialized.iter().all(|&m| m)
    }

    /// Return a new state with the specified element marked as materialized.
    pub fn with_materialized(mut self, element_index: usize) -> Self {
        if element_index < self.materialized.len() {
            self.materialized[element_index] = true;
        }
        self
    }

    /// Return a new state with all specified elements marked as materialized.
    pub fn with_all_materialized(mut self, elements: &std::collections::HashSet<usize>) -> Self {
        for &i in elements {
            if i < self.materialized.len() {
                self.materialized[i] = true;
            }
        }
        self
    }

    /// Return a new state with ALL elements marked as materialized.
    pub fn materialize_all(mut self) -> Self {
        for m in &mut self.materialized {
            *m = true;
        }
        self
    }

    /// Concatenate two states (for join output).
    pub fn concat(&self, other: &Self) -> Self {
        let mut m = self.materialized.clone();
        m.extend(&other.materialized);
        Self { materialized: m }
    }

    /// Get the number of elements tracked.
    pub fn len(&self) -> usize {
        self.materialized.len()
    }

    /// Check if empty.
    pub fn is_empty(&self) -> bool {
        self.materialized.is_empty()
    }

    /// Get iterator over (element_index, is_materialized) pairs.
    pub fn iter(&self) -> impl Iterator<Item = (usize, bool)> + '_ {
        self.materialized.iter().enumerate().map(|(i, &m)| (i, m))
    }

    /// Get indices of unmaterialized elements.
    pub fn unmaterialized_elements(&self) -> std::collections::HashSet<usize> {
        self.materialized
            .iter()
            .enumerate()
            .filter_map(|(i, &m)| if !m { Some(i) } else { None })
            .collect()
    }
}
