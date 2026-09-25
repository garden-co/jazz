//! The single sequencing authority ("Core") over linear history.

use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet};

use groove::storage::{
    Error, KeyValue, OrderedKvStorage, OwnedWriteOperation, ScanRequest, collect_scan,
    prefix_successor,
};

use crate::codec::*;
use crate::model::*;
use crate::storage::block_on;

#[derive(Clone, Copy, Debug)]
pub struct AuthorityOptions {
    /// Keep an overwrite-in-place current row next to history (`true`), or
    /// treat the newest history entry as current (`false`, one fewer write per
    /// row but current reads become reverse seeks and table scans read
    /// history).
    pub separate_current: bool,
}

impl Default for AuthorityOptions {
    fn default() -> Self {
        Self {
            separate_current: true,
        }
    }
}

/// How to answer an indexed query at a past cut.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SnapshotStrategy {
    /// Scan the table at the cut and filter: O(table).
    Forward,
    /// Current index result, corrected by the change log since the cut:
    /// O(result + changes since cut).
    Rewind,
}

/// One row changed after a cut: its image at the cut and now.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RowRewind {
    pub row: RowId,
    pub at: Option<RowImage>,
    pub current: RowImage,
}

pub struct Authority<S> {
    storage: S,
    schema: Schema,
    options: AuthorityOptions,
    last_seq: Seq,
}

fn set(cf: &str, key: Vec<u8>, value: Vec<u8>) -> OwnedWriteOperation {
    OwnedWriteOperation::Set {
        cf: cf.to_owned(),
        key,
        value,
    }
}

fn del(cf: &str, key: Vec<u8>) -> OwnedWriteOperation {
    OwnedWriteOperation::Delete {
        cf: cf.to_owned(),
        key,
    }
}

impl<S: OrderedKvStorage> Authority<S> {
    pub fn open(storage: S, schema: Schema, options: AuthorityOptions) -> Result<Self, Error> {
        let last_seq = block_on(storage.get(CF_META.to_owned(), META_LAST_SEQ.to_vec()))?
            .map_or(0, |b| Seq::from_be_bytes(b.as_slice().try_into().unwrap()));
        Ok(Self {
            storage,
            schema,
            options,
            last_seq,
        })
    }

    pub fn storage(&self) -> &S {
        &self.storage
    }

    pub fn last_seq(&self) -> Seq {
        self.last_seq
    }

    fn scan(&self, request: ScanRequest) -> Result<Vec<KeyValue>, Error> {
        block_on(async { collect_scan(self.storage.scan(request).await?).await })
    }

    /// Newest history entry at or before `at`.
    fn history_at(&self, table: TableId, row: RowId, at: Seq) -> Result<Option<RowImage>, Error> {
        let prefix = row_key(table, row);
        let request = match prefix_successor(&prefix) {
            Some(end) => {
                ScanRequest::range(CF_HISTORY.to_owned(), history_key(table, row, at), end)
            }
            None => ScanRequest::prefix(CF_HISTORY.to_owned(), prefix),
        };
        Ok(self
            .scan(request.with_max_items(1))?
            .into_iter()
            .map(|(key, value)| (history_key_row(&key).2, value))
            .find(|(seq, _)| *seq <= at)
            .map(|(_, v)| decode_row(&v)))
    }

    pub fn get(&self, table: TableId, row: RowId) -> Result<Option<RowImage>, Error> {
        if self.options.separate_current {
            Ok(
                block_on(self.storage.get(CF_CURRENT.to_owned(), row_key(table, row)))?
                    .map(|v| decode_row(&v)),
            )
        } else {
            self.history_at(table, row, Seq::MAX)
        }
    }

    /// Row image as of cut `at` (including deleted tombstones).
    pub fn get_at(&self, table: TableId, row: RowId, at: Seq) -> Result<Option<RowImage>, Error> {
        if self.options.separate_current {
            match self.get(table, row)? {
                Some(current) if current.seq <= at => Ok(Some(current)),
                Some(_) => self.history_at(table, row, at),
                None => Ok(None),
            }
        } else {
            self.history_at(table, row, at)
        }
    }

    /// Every row of `table` visible at cut `at`, in row order.
    pub fn scan_at(&self, table: TableId, at: Seq) -> Result<Vec<(RowId, RowImage)>, Error> {
        let prefix = table.to_be_bytes().to_vec();
        let mut out = Vec::new();
        if self.options.separate_current {
            for (key, value) in self.scan(ScanRequest::prefix(CF_CURRENT.to_owned(), prefix))? {
                let row = RowId::from_be_bytes(key[4..20].try_into().unwrap());
                let current = decode_row(&value);
                let image = if current.seq <= at {
                    Some(current)
                } else {
                    self.history_at(table, row, at)?
                };
                if let Some(image) = image.filter(RowImage::visible) {
                    out.push((row, image));
                }
            }
        } else {
            let mut newest: BTreeMap<RowId, RowImage> = BTreeMap::new();
            for (key, value) in self.scan(ScanRequest::prefix(CF_HISTORY.to_owned(), prefix))? {
                let (_, row, seq) = history_key_row(&key);
                if seq <= at {
                    // Newest-first within a row: keep the first qualifying entry.
                    newest.entry(row).or_insert_with(|| decode_row(&value));
                }
            }
            out.extend(newest.into_iter().filter(|(_, i)| i.visible()));
        }
        Ok(out)
    }

    fn changed_rows_since(&self, table: TableId, since: Seq) -> Result<BTreeSet<RowId>, Error> {
        let start = change_key(table, since.saturating_add(1), 0);
        let end = (table + 1).to_be_bytes().to_vec();
        Ok(self
            .scan(ScanRequest::range(CF_CHANGES.to_owned(), start, end))?
            .into_iter()
            .map(|(key, _)| change_key_parts(&key).1)
            .collect())
    }

    /// Sync unit: current images of rows changed after `since`.
    pub fn changes_since(
        &self,
        table: TableId,
        since: Seq,
    ) -> Result<Vec<(RowId, RowImage)>, Error> {
        self.changed_rows_since(table, since)?
            .into_iter()
            .map(|row| {
                Ok((
                    row,
                    self.get(table, row)?.expect("changed row has an image"),
                ))
            })
            .collect()
    }

    /// Rows changed after cut `at`, with their images at the cut and now.
    pub fn rewind(&self, table: TableId, at: Seq) -> Result<Vec<RowRewind>, Error> {
        self.changed_rows_since(table, at)?
            .into_iter()
            .map(|row| {
                Ok(RowRewind {
                    row,
                    at: self.get_at(table, row, at)?,
                    current: self.get(table, row)?.expect("changed row has an image"),
                })
            })
            .collect()
    }

    /// Visible rows whose indexed `column` equals `value` now.
    pub fn lookup_eq(
        &self,
        table: TableId,
        column: usize,
        value: &[u8],
    ) -> Result<BTreeSet<RowId>, Error> {
        Ok(self
            .scan(ScanRequest::prefix(
                CF_INDEX.to_owned(),
                index_prefix(table, column, value),
            ))?
            .into_iter()
            .map(|(key, _)| index_key_row(&key))
            .collect())
    }

    pub fn lookup_eq_at(
        &self,
        table: TableId,
        column: usize,
        value: &[u8],
        at: Seq,
        strategy: SnapshotStrategy,
    ) -> Result<BTreeSet<RowId>, Error> {
        let predicate = EqPredicate {
            table,
            column,
            value: Some(value.to_vec()),
        };
        match strategy {
            SnapshotStrategy::Forward => Ok(self
                .scan_at(table, at)?
                .into_iter()
                .filter(|(_, image)| predicate.matches(Some(image)))
                .map(|(row, _)| row)
                .collect()),
            SnapshotStrategy::Rewind => {
                let mut rows = self.lookup_eq(table, column, value)?;
                for change in self.rewind(table, at)? {
                    if predicate.matches(change.at.as_ref()) {
                        rows.insert(change.row);
                    } else {
                        rows.remove(&change.row);
                    }
                }
                Ok(rows)
            }
        }
    }

    /// Optimistic validation of an exclusive transaction against the change
    /// log in `(base, now]`.
    fn validate(&self, tx: &Tx) -> Result<Option<RejectReason>, Error> {
        let TxKind::Exclusive {
            base,
            rows_read,
            predicates,
        } = &tx.kind
        else {
            return Ok(None);
        };
        for (table, row) in rows_read {
            if self.get(*table, *row)?.is_some_and(|i| i.seq > *base) {
                return Ok(Some(RejectReason::RowConflict));
            }
        }
        for predicate in predicates {
            // Stop at the first changed row whose image entered or left the
            // predicate (or changed while matching it).
            for row in self.changed_rows_since(predicate.table, *base)? {
                let current = self.get(predicate.table, row)?;
                if predicate.matches(current.as_ref())
                    || predicate.matches(self.get_at(predicate.table, row, *base)?.as_ref())
                {
                    return Ok(Some(RejectReason::PredicateConflict));
                }
            }
        }
        Ok(None)
    }

    fn check_shape(&self, tx: &Tx) -> Option<RejectReason> {
        for write in &tx.writes {
            let Some(table) = self.schema.table(write.table) else {
                return Some(RejectReason::UnknownTable);
            };
            for (column, cell) in &write.cells {
                let Some(def) = table.columns.get(*column) else {
                    return Some(RejectReason::UnknownColumn);
                };
                if matches!(cell, CellWrite::ThreeWay { .. })
                    && matches!(def.strategy, Strategy::Lww)
                {
                    return Some(RejectReason::StrategyMismatch);
                }
            }
        }
        None
    }

    /// Decide and persist one transaction in a single atomic batch. Replaying
    /// an already-decided transaction id returns the recorded outcome.
    pub fn apply(&mut self, tx: &Tx) -> Result<Outcome, Error> {
        if let Some(prior) = block_on(self.storage.get(CF_TX.to_owned(), tx_key(tx.id)))? {
            return Ok(decode_outcome(&prior));
        }
        if let Some(reason) = match self.check_shape(tx) {
            Some(reason) => Some(reason),
            None => self.validate(tx)?,
        } {
            let outcome = Outcome::Rejected(reason);
            block_on(self.storage.write_many(vec![set(
                CF_TX,
                tx_key(tx.id),
                encode_outcome(outcome),
            )]))?;
            return Ok(outcome);
        }

        let seq = self.last_seq + 1;
        // (table, row) -> (before, after)
        let mut staged: BTreeMap<(TableId, RowId), (Option<RowImage>, RowImage)> = BTreeMap::new();
        for write in &tx.writes {
            let table = self.schema.table(write.table).expect("shape checked");
            let key = (write.table, write.row);
            let (_, after) = match staged.entry(key) {
                Entry::Occupied(entry) => entry.into_mut(),
                Entry::Vacant(entry) => {
                    let before = self.get(write.table, write.row)?;
                    let after = before
                        .clone()
                        .unwrap_or_else(|| RowImage::empty(table.columns.len()));
                    entry.insert((before, after))
                }
            };
            if let Some(deleted) = write.delete
                && tx.stamp >= after.deleted_stamp
            {
                after.deleted = deleted;
                after.deleted_stamp = tx.stamp;
            }
            for (column, cell_write) in &write.cells {
                let cell = &mut after.cells[*column];
                match cell_write {
                    CellWrite::Set(value) => {
                        if tx.stamp >= cell.stamp {
                            *cell = Cell {
                                stamp: tx.stamp,
                                value: value.clone(),
                            };
                        }
                    }
                    CellWrite::ThreeWay { base, value } => {
                        let Strategy::ThreeWay(merge) = table.columns[*column].strategy else {
                            unreachable!("shape checked");
                        };
                        let base_value = match base {
                            BaseRef::Inline(v) => v.clone(),
                            BaseRef::AtSeq(at) => self
                                .get_at(write.table, write.row, *at)?
                                .and_then(|i| i.cells[*column].value.clone()),
                        };
                        let merged = merge(
                            base_value.as_deref(),
                            cell.value.as_deref(),
                            value.as_deref(),
                        );
                        *cell = Cell {
                            stamp: cell.stamp.max(tx.stamp),
                            value: merged,
                        };
                    }
                }
            }
        }

        let mut ops = Vec::with_capacity(staged.len() * 3 + 2);
        for ((table_id, row), (before, mut after)) in staged {
            if before.as_ref().is_some_and(|b| b.same_content(&after)) {
                continue;
            }
            after.seq = seq;
            let encoded = encode_row(&after);
            if self.options.separate_current {
                ops.push(set(CF_CURRENT, row_key(table_id, row), encoded.clone()));
            }
            ops.push(set(CF_HISTORY, history_key(table_id, row, seq), encoded));
            ops.push(set(CF_CHANGES, change_key(table_id, seq, row), Vec::new()));
            let table = self.schema.table(table_id).unwrap();
            for (column, def) in table.columns.iter().enumerate() {
                if !def.indexed {
                    continue;
                }
                let old = before
                    .as_ref()
                    .filter(|b| b.visible())
                    .and_then(|b| b.value(column));
                let new = Some(&after)
                    .filter(|a| a.visible())
                    .and_then(|a| a.value(column));
                if old != new {
                    if let Some(old) = old {
                        ops.push(del(CF_INDEX, index_key(table_id, column, old, row)));
                    }
                    if let Some(new) = new {
                        ops.push(set(
                            CF_INDEX,
                            index_key(table_id, column, new, row),
                            Vec::new(),
                        ));
                    }
                }
            }
        }
        let outcome = Outcome::Accepted(seq);
        ops.push(set(CF_TX, tx_key(tx.id), encode_outcome(outcome)));
        ops.push(set(
            CF_META,
            META_LAST_SEQ.to_vec(),
            seq.to_be_bytes().to_vec(),
        ));
        block_on(self.storage.write_many(ops))?;
        self.last_seq = seq;
        Ok(outcome)
    }
}
