//! One physical input frontier for one maintained authority scope.
//!
//! Query terminals contribute weights; transport consumes only net presence.
//! The journal remembers each touched row's pre-publication presence, so failed
//! sends retain their predecessor without a second full published membership set.

use crate::protocol::SupportingRow;
use std::collections::BTreeMap;

#[cfg(test)]
std::thread_local! {
    pub(super) static SOURCE_CLOSURE_POINT_LOOKUPS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    #[doc(hidden)]
    pub static SOURCE_CLOSURE_TRAVERSALS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

#[derive(Clone, Debug, Default)]
pub(super) struct SupportingFrontier {
    weights: BTreeMap<SupportingRow, [i64; 4]>,
    unpublished: Option<BTreeMap<SupportingRow, bool>>,
}

impl SupportingFrontier {
    fn has_weight(&self, row: &SupportingRow) -> bool {
        self.weights
            .get(row)
            .is_some_and(|weights| weights.iter().any(|weight| *weight > 0))
    }

    pub(super) fn contains(&self, row: &SupportingRow) -> bool {
        if !self.has_weight(row) {
            return false;
        }
        if row.is_current {
            return true;
        }
        let mut current = row.clone();
        current.is_current = true;
        !self.has_weight(&current)
    }

    pub(super) fn apply(&mut self, origin: usize, row: SupportingRow, weight: i64) -> Option<bool> {
        let before = self.contains(&row);
        // Current and historical consumers share one physical version. A current
        // declaration subsumes historical support; withdrawing it reveals any
        // remaining historical contribution without duplicating the version.
        let mut historical = row.clone();
        historical.is_current = false;
        let historical_before = self.contains(&historical);
        let has_historical_support = row.is_current && self.has_weight(&historical);
        if let Some(unpublished) = &mut self.unpublished {
            unpublished.entry(row.clone()).or_insert(before);
            if has_historical_support {
                unpublished.entry(historical).or_insert(historical_before);
            }
        }
        let weights = self.weights.entry(row.clone()).or_default();
        weights[origin] += weight;
        if weights.iter().all(|weight| *weight == 0) {
            self.weights.remove(&row);
        }
        let after = self.contains(&row);
        (before != after).then_some(after)
    }

    pub(super) fn rows(&self) -> impl Iterator<Item = &SupportingRow> {
        #[cfg(test)]
        SOURCE_CLOSURE_TRAVERSALS.with(|count| count.set(count.get() + 1));
        self.weights
            .iter()
            .filter(|(row, weights)| {
                weights.iter().any(|weight| *weight > 0)
                    && (row.is_current || {
                        let mut current = (*row).clone();
                        current.is_current = true;
                        !self.has_weight(&current)
                    })
            })
            .map(|(row, _)| row)
    }

    pub(super) fn acknowledged_rows(&self) -> impl Iterator<Item = &SupportingRow> {
        self.rows()
            .filter(|row| {
                self.unpublished
                    .as_ref()
                    .and_then(|changes| changes.get(*row))
                    != Some(&false)
            })
            .chain(
                self.unpublished
                    .iter()
                    .flat_map(|changes| changes.iter())
                    .filter(|(row, before)| **before && !self.contains(row))
                    .map(|(row, _)| row),
            )
    }

    pub(super) fn delta(&self) -> Option<(Vec<SupportingRow>, Vec<SupportingRow>)> {
        let mut adds = Vec::new();
        let mut removes = Vec::new();
        for (row, before) in self.unpublished.as_ref()? {
            #[cfg(test)]
            SOURCE_CLOSURE_POINT_LOOKUPS.with(|count| count.set(count.get() + 1));
            match (*before, self.contains(row)) {
                (false, true) => adds.push(row.clone()),
                (true, false) => removes.push(row.clone()),
                _ => {}
            }
        }
        Some((adds, removes))
    }

    pub(super) fn acknowledge(&mut self) {
        self.unpublished.get_or_insert_with(BTreeMap::new).clear();
    }

    pub(super) fn forget_predecessor(&mut self) {
        self.unpublished = None;
    }

    #[cfg(test)]
    pub(super) fn is_empty(&self) -> bool {
        self.weights.is_empty()
    }

    pub(super) fn retained_bytes(&self) -> usize {
        let row_bytes = |row: &SupportingRow| {
            std::mem::size_of::<SupportingRow>()
                + row
                    .version
                    .branch_or_prefix
                    .as_ref()
                    .map_or(0, Vec::capacity)
                + row.version.row_digest.as_ref().map_or(0, Vec::capacity)
        };
        self.weights
            .keys()
            .map(|row| row_bytes(row) + 4 * std::mem::size_of::<i64>())
            .sum::<usize>()
            + self
                .unpublished
                .as_ref()
                .map_or(0, |rows| rows.keys().map(|row| row_bytes(row) + 1).sum())
    }
}
