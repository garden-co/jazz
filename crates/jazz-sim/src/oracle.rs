//! Row-set oracles: what a read returns, and what a subscription's deltas
//! add up to, as plain maps that a scenario compares with its model.

use std::collections::{BTreeMap, BTreeSet};

use jazz::db::{
    CurrentRow, Db, PreparedQuery, ReadOpts, SubscriptionEvent, SubscriptionStream, block_on,
};
use jazz::ids::RowUuid;
use jazz::query::Query;
use jazz::records::Value;

/// A row's application cells (system `$` columns left out; null cells too).
pub type Cells = BTreeMap<String, Value>;

/// Rows by id.
pub type Rows = BTreeMap<RowUuid, Cells>;

/// A row's application cells.
pub fn cells(row: &CurrentRow) -> Cells {
    row.cells()
        .filter(|(name, _)| !name.starts_with('$'))
        .filter_map(|(name, value)| Some((name.to_owned(), value?.clone())))
        .collect()
}

/// One application cell of a row.
pub fn cell(row: &CurrentRow, column: &str) -> Option<Value> {
    row.cells()
        .find(|(name, _)| *name == column)
        .and_then(|(_, value)| value.cloned())
}

/// Every row `query` reads on `db`.
pub fn read(db: &Db, query: &Query, opts: ReadOpts) -> Rows {
    let prepared = db.prepare_query(query).expect("prepare oracle read");
    read_prepared(db, &prepared, opts)
}

pub fn read_prepared(db: &Db, prepared: &PreparedQuery, opts: ReadOpts) -> Rows {
    block_on(db.all(prepared, opts))
        .expect("oracle read")
        .iter()
        .map(|row| (row.row_uuid(), cells(row)))
        .collect()
}

/// The ids `query` reads on `db`.
pub fn ids(db: &Db, query: &Query, opts: ReadOpts) -> BTreeSet<RowUuid> {
    read(db, query, opts).into_keys().collect()
}

/// A subscription and the rows its deltas add up to.
pub struct Live {
    pub stream: SubscriptionStream,
    pub rows: Rows,
    /// Rows the deltas added, updated or removed, over the subscription's life.
    pub added: u64,
    pub updated: u64,
    pub removed: u64,
    /// Every id a delta ever carried (added or updated).
    pub seen: BTreeSet<RowUuid>,
    pub settled: bool,
    pub rejected: Option<String>,
}

impl Live {
    /// Subscribe to `query` on `db`.
    pub fn subscribe(db: &Db, query: &Query, opts: ReadOpts) -> Self {
        let prepared = db.prepare_query(query).expect("prepare subscription");
        Self::subscribe_prepared(db, &prepared, opts)
    }

    pub fn subscribe_prepared(db: &Db, prepared: &PreparedQuery, opts: ReadOpts) -> Self {
        let stream = block_on(db.subscribe(prepared, opts)).expect("subscribe");
        Self::from_stream(stream)
    }

    pub fn from_stream(stream: SubscriptionStream) -> Self {
        Self {
            stream,
            rows: Rows::new(),
            added: 0,
            updated: 0,
            removed: 0,
            seen: BTreeSet::new(),
            settled: false,
            rejected: None,
        }
    }

    /// Apply every queued event; true if any changed the rows.
    pub fn poll(&mut self) -> bool {
        let mut changed = false;
        while let Some(event) = self.stream.try_next_event() {
            match event {
                SubscriptionEvent::Delta {
                    reset,
                    added,
                    updated,
                    removed,
                    settled,
                    ..
                } => {
                    if reset {
                        self.rows.clear();
                    }
                    self.settled |= settled;
                    changed |=
                        reset || !added.is_empty() || !updated.is_empty() || !removed.is_empty();
                    for gone in &removed {
                        self.rows.remove(&gone.row_uuid);
                    }
                    for row in added.iter().chain(&updated) {
                        self.seen.insert(row.row_uuid());
                        self.rows.insert(row.row_uuid(), cells(row));
                    }
                    self.added += added.len() as u64;
                    self.updated += updated.len() as u64;
                    self.removed += removed.len() as u64;
                }
                SubscriptionEvent::Rejected { code } => {
                    self.rejected = Some(code.as_str().to_owned())
                }
                SubscriptionEvent::Closed => {}
            }
        }
        changed
    }

    pub fn ids(&self) -> BTreeSet<RowUuid> {
        self.rows.keys().copied().collect()
    }
}
