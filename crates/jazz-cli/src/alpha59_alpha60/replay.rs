//! How a row's settled alpha.59 versions land in the SQLite store, shared by
//! the import, which writes them, and verify, which predicts what it wrote.
//!
//! - alpha.59 settles versions in the authority's order, but one replaces
//!   its layer's winner (content or deletion) only when it was written on
//!   top of it or later than it ([`replaces`]).
//! - A row's latest deletion event is its current one, so a repeat (two
//!   clients deleting one row) replaces the event it repeats: deletion
//!   events are held per row and written when the row's next content, or
//!   the end of the dump, releases them. A held event that would not change
//!   the stored row (a restore of a live row) writes nothing.
//! - alpha.59 keeps content and deletion apart, so content can be written
//!   under a deletion and shows once the row is restored. The SQLite engine
//!   keeps a deleted row's content fixed, but a restore can bring new
//!   content: content written under a deletion is held, goes with the row's
//!   next restore, and is dropped if the row stays deleted.

use std::collections::{HashMap, HashSet};

use jazz_alpha59_alpha60_dump as dump;

use super::RowKey;

/// Whether `version` replaces its layer's `winner` in alpha.59: there is
/// none yet, it was written on top of it, or its transaction is later by
/// (time, node).
pub(crate) fn replaces(winner: Option<&dump::TxRef>, version: &dump::Version) -> bool {
    winner.is_none_or(|winner| {
        version.parents.contains(winner)
            || (version.tx_time, version.tx_node) > (winner.time, winner.node)
    })
}

/// A held deletion event the store takes, with the content written under
/// the deletion when it is a restore that brings some.
pub(crate) struct Released<E, C> {
    pub key: RowKey,
    pub event: E,
    pub content: Option<C>,
}

/// Per row replay state; `E` is what a deletion event carries, `C` what
/// content carries.
pub(crate) struct RowReplay<E, C> {
    winners: HashMap<(RowKey, bool), dump::TxRef>,
    /// Rows deleted after every event so far.
    deleted: HashSet<RowKey>,
    /// Rows deleted in the store, held events aside.
    stored_deleted: HashSet<RowKey>,
    pending: HashMap<RowKey, (bool, E)>,
    hidden: HashMap<RowKey, C>,
    /// Versions settled after a newer winner, which change nothing.
    pub superseded: u64,
    /// Deletion events that wrote nothing: replaced by a repeat, or a
    /// restore of a live row.
    pub coalesced: u64,
    /// Content versions written while their row was deleted.
    pub written_while_deleted: u64,
    /// Restores that brought content written while their row was deleted.
    pub restores_with_hidden: u64,
    /// Rows that stayed deleted, dropping content written under it.
    pub rows_dropping_hidden: u64,
}

impl<E, C> Default for RowReplay<E, C> {
    fn default() -> Self {
        Self {
            winners: HashMap::new(),
            deleted: HashSet::new(),
            stored_deleted: HashSet::new(),
            pending: HashMap::new(),
            hidden: HashMap::new(),
            superseded: 0,
            coalesced: 0,
            written_while_deleted: 0,
            restores_with_hidden: 0,
            rows_dropping_hidden: 0,
        }
    }
}

impl<E, C> RowReplay<E, C> {
    /// Whether `version` wins its layer of the row; it then becomes the
    /// layer's winner. A version that does not win changes nothing.
    pub fn wins(&mut self, key: &RowKey, version: &dump::Version) -> bool {
        let layer = (key.clone(), version.deletion.is_some());
        if !replaces(self.winners.get(&layer), version) {
            self.superseded += 1;
            return false;
        }
        let tx = dump::TxRef {
            node: version.tx_node,
            time: version.tx_time,
        };
        self.winners.insert(layer, tx);
        true
    }

    /// A deletion event that won its layer.
    pub fn event(&mut self, key: &RowKey, deleting: bool, event: E, out: &mut Vec<Released<E, C>>) {
        if deleting {
            self.deleted.insert(key.clone());
        } else {
            self.deleted.remove(key);
        }
        match self.pending.get(key) {
            Some((held, _)) if *held == deleting => self.coalesced += 1,
            Some(_) => self.release(key, out),
            None => {}
        }
        self.pending.insert(key.clone(), (deleting, event));
    }

    /// Content that won its layer: given back once the held event it
    /// follows is released into `out`, for the store to take now, or held
    /// under the row's deletion.
    pub fn content(
        &mut self,
        key: &RowKey,
        content: C,
        out: &mut Vec<Released<E, C>>,
    ) -> Option<C> {
        if self.deleted.contains(key) {
            self.written_while_deleted += 1;
            self.hidden.insert(key.clone(), content);
            return None;
        }
        self.release(key, out);
        Some(content)
    }

    /// Releases every held event, in row order, and gives back the content
    /// dropped under deletions that last.
    pub fn finish(&mut self, out: &mut Vec<Released<E, C>>) -> Vec<(RowKey, C)> {
        let mut keys: Vec<RowKey> = self.pending.keys().cloned().collect();
        keys.sort();
        for key in keys {
            self.release(&key, out);
        }
        let dropped: Vec<(RowKey, C)> = self.hidden.drain().collect();
        self.rows_dropping_hidden += dropped.len() as u64;
        dropped
    }

    fn release(&mut self, key: &RowKey, out: &mut Vec<Released<E, C>>) {
        let Some((deleting, event)) = self.pending.remove(key) else {
            return;
        };
        if self.stored_deleted.contains(key) == deleting {
            self.coalesced += 1;
            return;
        }
        let mut content = None;
        if deleting {
            self.stored_deleted.insert(key.clone());
        } else {
            self.stored_deleted.remove(key);
            content = self.hidden.remove(key);
            if content.is_some() {
                self.restores_with_hidden += 1;
            }
        }
        out.push(Released {
            key: key.clone(),
            event,
            content,
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    type Rows = RowReplay<(usize, bool), usize>;

    /// Replays content (`None`) and deletion events (`Some(deleting)`),
    /// each carrying its position; returns the writes, by op and position.
    fn replay(events: &[Option<bool>]) -> (Rows, Vec<(&'static str, usize)>) {
        let key: RowKey = ("notes".to_owned(), [1; 16]);
        let mut rows = RowReplay::default();
        let mut writes = Vec::new();
        let take = |out: Vec<Released<(usize, bool), usize>>, writes: &mut Vec<_>| {
            for released in out {
                let (at, deleting) = released.event;
                let op = match (deleting, released.content) {
                    (true, _) => "delete",
                    (false, None) => "restore",
                    (false, Some(_)) => "restore with content",
                };
                writes.push((op, at));
            }
        };
        for (at, event) in events.iter().enumerate() {
            let mut out = Vec::new();
            match event {
                None => {
                    let content = rows.content(&key, at, &mut out);
                    take(out, &mut writes);
                    if let Some(at) = content {
                        writes.push(("content", at));
                    }
                }
                Some(deleting) => {
                    rows.event(&key, *deleting, (at, *deleting), &mut out);
                    take(out, &mut writes);
                }
            }
        }
        let mut out = Vec::new();
        rows.finish(&mut out);
        take(out, &mut writes);
        (rows, writes)
    }

    const DELETE: Option<bool> = Some(true);
    const RESTORE: Option<bool> = Some(false);

    #[test]
    fn repeated_events_write_the_last_one() {
        let (rows, writes) = replay(&[None, DELETE, DELETE]);
        assert_eq!(writes, [("content", 0), ("delete", 2)]);
        assert_eq!(rows.coalesced, 1);
    }

    #[test]
    fn a_restore_of_a_live_row_writes_nothing() {
        let (rows, writes) = replay(&[None, RESTORE, RESTORE]);
        assert_eq!(writes, [("content", 0)]);
        assert_eq!(rows.coalesced, 2);
    }

    #[test]
    fn content_written_while_deleted_goes_with_the_restore() {
        let (rows, writes) = replay(&[None, DELETE, None, None, RESTORE]);
        assert_eq!(
            writes,
            [("content", 0), ("delete", 1), ("restore with content", 4)]
        );
        assert_eq!(
            (
                rows.written_while_deleted,
                rows.restores_with_hidden,
                rows.rows_dropping_hidden
            ),
            (2, 1, 0)
        );
    }

    #[test]
    fn delete_content_delete_restore() {
        let (rows, writes) = replay(&[None, DELETE, None, DELETE, RESTORE]);
        assert_eq!(
            writes,
            [("content", 0), ("delete", 3), ("restore with content", 4)]
        );
        assert_eq!(rows.coalesced, 1);
    }

    #[test]
    fn content_under_a_lasting_deletion_is_dropped() {
        let (rows, writes) = replay(&[None, DELETE, None, DELETE]);
        assert_eq!(writes, [("content", 0), ("delete", 3)]);
        assert_eq!(
            (rows.written_while_deleted, rows.rows_dropping_hidden),
            (1, 1)
        );
    }
}
