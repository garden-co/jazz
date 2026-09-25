//! Independent test-only physical-set interpreter. Production receivers do not
//! reconstruct whole wire manifests; assertions may use this model to inspect
//! retained membership without mistaking delta additions for the whole set.

use super::{SubscriptionKey, SupportingRow, SupportingRowsUpdate, SyncMessage};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Default)]
pub(crate) struct SupportingSetTestOracle {
    sets: BTreeMap<SubscriptionKey, ([u8; 16], BTreeSet<SupportingRow>)>,
}

impl SupportingSetTestOracle {
    pub(crate) fn observe(&mut self, message: &SyncMessage) -> SyncMessage {
        let SyncMessage::ViewUpdate(view) = message else {
            panic!("expected view update")
        };
        let rows = match &view.supporting_rows {
            SupportingRowsUpdate::Snapshot { rows, .. } => {
                let set = rows.iter().cloned().collect::<BTreeSet<_>>();
                assert_eq!(set.len(), rows.len(), "duplicate snapshot membership");
                set
            }
            SupportingRowsUpdate::Delta {
                predecessor,
                adds,
                removes,
                ..
            } => {
                let (revision, previous) = self
                    .sets
                    .get(&view.subscription)
                    .expect("delta requires snapshot");
                assert_eq!(predecessor, revision, "exact adjacent predecessor");
                let mut set = previous.clone();
                for row in removes {
                    assert!(set.remove(row), "removal must exist");
                }
                for row in adds {
                    assert!(set.insert(row.clone()), "addition must be absent");
                }
                set
            }
            SupportingRowsUpdate::CatchUp {
                predecessor,
                changed,
                left,
                ..
            } => {
                let (revision, previous) = self
                    .sets
                    .get(&view.subscription)
                    .expect("catch-up requires snapshot");
                assert_eq!(predecessor, revision, "catch-up names the held revision");
                let coordinate = |row: &SupportingRow| (row.physical_table, row.row);
                let replaced = changed
                    .iter()
                    .chain(left)
                    .map(coordinate)
                    .collect::<BTreeSet<_>>();
                let mut set = previous
                    .iter()
                    .filter(|row| !replaced.contains(&coordinate(row)))
                    .cloned()
                    .collect::<BTreeSet<_>>();
                set.extend(changed.iter().cloned());
                set
            }
        };
        self.sets.insert(
            view.subscription,
            (view.supporting_rows.revision(), rows.clone()),
        );
        let mut materialized = view.clone();
        materialized.supporting_rows = SupportingRowsUpdate::Snapshot {
            revision: view.supporting_rows.revision(),
            rows: rows.into_iter().collect(),
        };
        SyncMessage::ViewUpdate(materialized)
    }
}
