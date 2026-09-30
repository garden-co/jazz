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
    /// Diagnostic (#3815): the last contributions to each retained row.
    history: BTreeMap<SupportingRow, std::collections::VecDeque<String>>,
}

impl SupportingFrontier {
    pub(super) fn contains(&self, row: &SupportingRow) -> bool {
        self.weights
            .get(row)
            .is_some_and(|weights| weights.iter().any(|weight| *weight > 0))
    }

    pub(super) fn apply(&mut self, origin: usize, row: SupportingRow, weight: i64) -> Option<bool> {
        let before = self.contains(&row);
        if let Some(unpublished) = &mut self.unpublished {
            unpublished.entry(row.clone()).or_insert(before);
        }
        let weights = self.weights.entry(row.clone()).or_default();
        weights[origin] += weight;
        let after = weights.iter().any(|weight| *weight > 0);
        let removed = weights.iter().all(|weight| *weight == 0);
        if removed {
            self.weights.remove(&row);
            self.history.remove(&row);
        } else {
            let history = self.history.entry(row).or_default();
            history.push_back(format!("o{origin}{weight:+}"));
            while history.len() > 12 {
                history.pop_front();
            }
        }
        (before != after).then_some(after)
    }

    /// Diagnostic (#3815): annotate the latest contribution with its source.
    pub(super) fn note_source(&mut self, row: &SupportingRow, source: &dyn std::fmt::Debug) {
        if let Some(last) = self
            .history
            .get_mut(row)
            .and_then(|history| history.back_mut())
        {
            use std::fmt::Write as _;
            let _ = write!(last, "@{source:?}");
        }
    }

    /// Diagnostic (#3815): every physical coordinate retained at more than one
    /// version, with each version's per-origin weights, journal state and
    /// recent contributions. A well-formed frontier returns nothing.
    pub(super) fn coordinate_conflicts(&self) -> Vec<String> {
        let mut by_coordinate =
            BTreeMap::<super::CoveredInputCoordinate, Vec<&SupportingRow>>::new();
        for row in self.rows() {
            by_coordinate
                .entry(super::CoveredInputCoordinate::from(row))
                .or_default()
                .push(row);
        }
        by_coordinate
            .into_values()
            .filter(|rows| rows.len() > 1)
            .map(|rows| {
                rows.into_iter()
                    .map(|row| {
                        format!(
                            "[{} {} {:?} tx {:?} weights {:?} journal {:?} history {:?}]",
                            row.version_table.as_str(),
                            row.row.0,
                            row.version.layer,
                            row.version.tx,
                            self.weights.get(row),
                            self.unpublished.as_ref().map(|journal| journal.get(row)),
                            self.history.get(row),
                        )
                    })
                    .collect::<Vec<_>>()
                    .join(" vs ")
            })
            .collect()
    }

    pub(super) fn rows(&self) -> impl Iterator<Item = &SupportingRow> {
        #[cfg(test)]
        SOURCE_CLOSURE_TRAVERSALS.with(|count| count.set(count.get() + 1));
        self.weights
            .iter()
            .filter(|(_, weights)| weights.iter().any(|weight| *weight > 0))
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
