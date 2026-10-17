//! The non-durable storage class: tables whose rows live only in memory.
//!
//! A table declared [`crate::schema::TableSchema::non_durable`] holds derived
//! state its owner can rebuild from durable tables. Its rows and the entries
//! of its secondary indexes are kept by [`super::LayoutStorage`] in this
//! in-process map and never reach the backend. Everything above the layout
//! (batches, resident publications, IVM ticks, scans, keyed lookups) sees an
//! ordinary table; only a restart differs: the class opens empty, and the
//! owner refills it from its durable sources.
//!
//! The class keeps exact byte counts per partition, a partition being a
//! table's rows (and index entries) under one value of the leading key part:
//! the first primary-key part for rows, the first index-key part for index
//! entries. Each partition carries the tick of its last read or write, so a
//! memory budget can rank partitions by recency. This module only reports
//! them; it evicts nothing.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use super::{KeyValue, OwnedWriteOperation, ScanBounds, ScanDirection, prefix_successor};
use crate::ivm::runtime::{SharedIndexIds, leading_key_part_len, split_durable_index_key};

/// The logical family that holds every schema index.
const INDICES_CF: &str = "indices";

/// Orders reads and writes of non-durable partitions across the process.
static RESIDENCY_CLOCK: AtomicU64 = AtomicU64::new(1);
/// Bytes every non-durable class in the process holds.
static PROCESS_RESIDENT: AtomicUsize = AtomicUsize::new(0);

fn residency_tick() -> u64 {
    RESIDENCY_CLOCK.fetch_add(1, Ordering::Relaxed)
}

/// Bytes held by every non-durable table in the process.
pub fn process_non_durable_resident_bytes() -> usize {
    PROCESS_RESIDENT.load(Ordering::Relaxed)
}

/// One partition of a non-durable table, as of a
/// [`NonDurableResidency`] snapshot.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NonDurablePartition {
    /// The table's logical name.
    pub table: String,
    /// The encoded leading key part shared by the partition's rows.
    pub partition: Vec<u8>,
    /// Key plus value bytes of its rows and index entries.
    pub bytes: usize,
    /// Process-wide tick of its last read or write; larger is more recent.
    pub last_touched: u64,
}

/// What a database's non-durable tables hold in memory.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct NonDurableResidency {
    partitions: Vec<NonDurablePartition>,
}

impl NonDurableResidency {
    /// Key plus value bytes of every row and index entry held.
    pub fn bytes(&self) -> usize {
        self.partitions
            .iter()
            .map(|partition| partition.bytes)
            .sum()
    }

    /// Every non-empty partition, ordered by table and partition key.
    pub fn partitions(&self) -> impl Iterator<Item = &NonDurablePartition> {
        self.partitions.iter()
    }
}

#[derive(Clone, Copy, Debug)]
struct PartitionUse {
    bytes: usize,
    last_touched: u64,
}

#[derive(Default)]
struct State {
    tables: BTreeSet<String>,
    index_ids: Option<SharedIndexIds>,
    /// Logical family (a table, or `indices`) -> key -> value.
    families: BTreeMap<String, BTreeMap<Vec<u8>, Vec<u8>>>,
    /// Table -> partition key -> use. Nested so a read looks a partition up
    /// by borrowed `&str` and `&[u8]` without allocating.
    partitions: BTreeMap<String, BTreeMap<Vec<u8>, PartitionUse>>,
    /// This class's part of `PROCESS_RESIDENT`.
    resident: usize,
}

/// The in-memory store behind every non-durable table of one layout.
#[derive(Default)]
pub(crate) struct NonDurableClass {
    state: RefCell<State>,
}

impl Drop for NonDurableClass {
    fn drop(&mut self) {
        let resident = self.state.get_mut().resident;
        PROCESS_RESIDENT.fetch_sub(resident, Ordering::Relaxed);
    }
}

impl NonDurableClass {
    /// Declare `table` non-durable. Declarations are permanent for the
    /// process: a table must never move between classes while it holds rows.
    pub(crate) fn declare_table(&self, table: &str) {
        let mut state = self.state.borrow_mut();
        if !state.tables.contains(table) {
            state.tables.insert(table.to_owned());
        }
    }

    /// Let the class resolve `indices` keys to the table owning their index.
    pub(crate) fn attach_index_ids(&self, index_ids: SharedIndexIds) {
        self.state.borrow_mut().index_ids = Some(index_ids);
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.state.borrow().tables.is_empty()
    }

    /// The table that owns a key or key prefix of `cf` when it belongs to
    /// this class, borrowed from `tables`.
    fn owner<'a>(
        tables: &'a BTreeSet<String>,
        index_ids: Option<&SharedIndexIds>,
        cf: &str,
        key: &[u8],
    ) -> Option<&'a str> {
        if tables.is_empty() {
            return None;
        }
        if let Some(table) = tables.get(cf) {
            return Some(table);
        }
        if cf != INDICES_CF {
            return None;
        }
        let (id, _) = split_durable_index_key(key)?;
        let index_ids = index_ids?.borrow();
        let (table, _) = index_ids.names(id)?;
        tables.get(table).map(String::as_str)
    }

    /// Whether `cf` keys starting with `key` belong to this class.
    pub(crate) fn owns(&self, cf: &str, key: &[u8]) -> bool {
        let state = self.state.borrow();
        Self::owner(&state.tables, state.index_ids.as_ref(), cf, key).is_some()
    }

    /// Whether `bounds` of `cf` read this class. A range belongs to it when
    /// its start does; a range starting in one index and ending in another
    /// is never issued for a single index.
    pub(crate) fn owns_bounds(&self, cf: &str, bounds: &ScanBounds) -> bool {
        match bounds {
            ScanBounds::Prefix(prefix) => self.owns(cf, prefix),
            ScanBounds::Range { start, .. } => self.owns(cf, start),
        }
    }

    /// The partition key of a full or partial key: its leading key part,
    /// after the index id for an index entry. `None` when `key` is too short
    /// to hold a whole leading part.
    fn partition_of<'k>(cf: &str, key: &'k [u8]) -> Option<&'k [u8]> {
        let rest = if cf == INDICES_CF {
            split_durable_index_key(key)?.1
        } else {
            key
        };
        let len = leading_key_part_len(rest)?;
        Some(&rest[..len])
    }

    fn touch(state: &mut State, cf: &str, key: &[u8], bounds_start: Option<&[u8]>, tick: u64) {
        let State {
            tables,
            index_ids,
            partitions,
            ..
        } = state;
        let Some(table) = Self::owner(tables, index_ids.as_ref(), cf, key)
            .or_else(|| Self::owner(tables, index_ids.as_ref(), cf, bounds_start?))
        else {
            return;
        };
        let Some(table_partitions) = partitions.get_mut(table) else {
            return;
        };
        match Self::partition_of(cf, key) {
            Some(partition) => {
                if let Some(entry) = table_partitions.get_mut(partition) {
                    entry.last_touched = tick;
                }
            }
            None => {
                // A read wider than one partition touches every partition of
                // the table.
                for entry in table_partitions.values_mut() {
                    entry.last_touched = tick;
                }
            }
        }
    }

    pub(crate) fn get(&self, cf: &str, key: &[u8]) -> Option<Vec<u8>> {
        let mut state = self.state.borrow_mut();
        Self::touch(&mut state, cf, key, None, residency_tick());
        state
            .families
            .get(cf)
            .and_then(|family| family.get(key))
            .cloned()
    }

    pub(crate) fn scan(
        &self,
        cf: &str,
        bounds: &ScanBounds,
        direction: ScanDirection,
        max_items: Option<usize>,
    ) -> Vec<KeyValue> {
        let mut state = self.state.borrow_mut();
        match bounds {
            ScanBounds::Prefix(prefix) => {
                Self::touch(&mut state, cf, prefix, None, residency_tick());
            }
            ScanBounds::Range { start, end } => {
                let common = &start[..common_prefix_len(start, end)];
                Self::touch(&mut state, cf, common, Some(start), residency_tick());
            }
        }
        let Some(family) = state.families.get(cf) else {
            return Vec::new();
        };
        if bounds.is_empty_range() {
            return Vec::new();
        }
        let (start, end, prefix) = match bounds {
            ScanBounds::Prefix(prefix) => (prefix.clone(), prefix_successor(prefix), Some(prefix)),
            ScanBounds::Range { start, end } => (start.clone(), Some(end.clone()), None),
        };
        let range = match end {
            Some(end) => family.range(start..end),
            None => family.range(start..),
        };
        let matching = range.filter(|(key, _)| prefix.is_none_or(|prefix| key.starts_with(prefix)));
        let limit = max_items.unwrap_or(usize::MAX);
        match direction {
            ScanDirection::Forward => matching
                .take(limit)
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect(),
            ScanDirection::Reverse => matching
                .rev()
                .take(limit)
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect(),
        }
    }

    /// Install or remove one key. The caller has checked [`Self::owns`].
    pub(crate) fn write(&self, cf: &str, key: &[u8], value: Option<&[u8]>) {
        let mut state = self.state.borrow_mut();
        let state = &mut *state;
        let Some(table) = Self::owner(&state.tables, state.index_ids.as_ref(), cf, key) else {
            return;
        };
        let family = state.families.entry(cf.to_owned()).or_default();
        let previous = match value {
            Some(value) => family.insert(key.to_vec(), value.to_vec()),
            None => family.remove(key),
        };
        if family.is_empty() {
            state.families.remove(cf);
        }
        let removed = previous.map_or(0, |previous| key.len() + previous.len());
        let added = value.map_or(0, |value| key.len() + value.len());
        let partition = Self::partition_of(cf, key).unwrap_or_default();
        let tick = residency_tick();
        if !state.partitions.contains_key(table) {
            state.partitions.insert(table.to_owned(), BTreeMap::new());
        }
        let table_partitions = state
            .partitions
            .get_mut(table)
            .expect("table partitions were just ensured");
        if !table_partitions.contains_key(partition) {
            table_partitions.insert(
                partition.to_vec(),
                PartitionUse {
                    bytes: 0,
                    last_touched: tick,
                },
            );
        }
        let entry = table_partitions
            .get_mut(partition)
            .expect("partition was just ensured");
        entry.bytes = entry.bytes + added - removed;
        entry.last_touched = tick;
        if entry.bytes == 0 {
            table_partitions.remove(partition);
            if table_partitions.is_empty() {
                state.partitions.remove(table);
            }
        }
        if added >= removed {
            state.resident += added - removed;
            PROCESS_RESIDENT.fetch_add(added - removed, Ordering::Relaxed);
        } else {
            state.resident -= removed - added;
            PROCESS_RESIDENT.fetch_sub(removed - added, Ordering::Relaxed);
        }
    }

    /// Split `operations` into those this class owns, applied by
    /// [`Self::apply`] once the rest commit, and the rest.
    pub(crate) fn split(
        &self,
        operations: Vec<OwnedWriteOperation>,
    ) -> (Vec<OwnedWriteOperation>, Vec<OwnedWriteOperation>) {
        if self.is_empty() {
            return (Vec::new(), operations);
        }
        operations
            .into_iter()
            .partition(|operation| self.owns(operation.cf(), operation.key()))
    }

    pub(crate) fn apply(&self, operations: &[OwnedWriteOperation]) {
        for operation in operations {
            match operation {
                OwnedWriteOperation::Set { cf, key, value } => self.write(cf, key, Some(value)),
                OwnedWriteOperation::Delete { cf, key } => self.write(cf, key, None),
            }
        }
    }

    pub(crate) fn residency(&self) -> NonDurableResidency {
        let state = self.state.borrow();
        NonDurableResidency {
            partitions: state
                .partitions
                .iter()
                .flat_map(|(table, partitions)| {
                    partitions
                        .iter()
                        .map(move |(partition, entry)| NonDurablePartition {
                            table: table.clone(),
                            partition: partition.clone(),
                            bytes: entry.bytes,
                            last_touched: entry.last_touched,
                        })
                })
                .collect(),
        }
    }
}

fn common_prefix_len(left: &[u8], right: &[u8]) -> usize {
    left.iter()
        .zip(right)
        .take_while(|(left, right)| left == right)
        .count()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn writes_account_exact_bytes_per_partition_and_process() {
        let class = NonDurableClass::default();
        class.declare_table("t");
        let before = process_non_durable_resident_bytes();
        let a = [7, b'a', 0, 0, 1];
        let b = [7, b'b', 0, 0, 1];
        class.write("t", &a, Some(b"xyz"));
        class.write("t", &b, Some(b"q"));
        class.write("t", &a, Some(b"xy"));
        let residency = class.residency();
        assert_eq!(residency.bytes(), (5 + 2) + (5 + 1));
        assert_eq!(residency.partitions().count(), 2);
        assert!(process_non_durable_resident_bytes() >= before + 13);
        class.write("t", &a, None);
        class.write("t", &b, None);
        assert_eq!(class.residency().bytes(), 0);
        assert_eq!(class.residency().partitions().count(), 0);
    }

    #[test]
    fn scans_respect_bounds_direction_and_limit() {
        let class = NonDurableClass::default();
        class.declare_table("t");
        for key in [[7u8, b'a', 0, 0, 1], [7, b'a', 0, 0, 2], [7, b'b', 0, 0, 1]] {
            class.write("t", &key, Some(b"v"));
        }
        let prefix = ScanBounds::Prefix(vec![7, b'a', 0, 0]);
        assert_eq!(
            class.scan("t", &prefix, ScanDirection::Forward, None).len(),
            2
        );
        let reversed = class.scan("t", &prefix, ScanDirection::Reverse, Some(1));
        assert_eq!(reversed[0].0, vec![7, b'a', 0, 0, 2]);
        let all = ScanBounds::Prefix(Vec::new());
        assert_eq!(class.scan("t", &all, ScanDirection::Forward, None).len(), 3);
        assert!(!class.owns("u", &[]));
    }

    #[test]
    fn reads_touch_their_partition() {
        let class = NonDurableClass::default();
        class.declare_table("t");
        let a = [7, b'a', 0, 0, 1];
        let b = [7, b'b', 0, 0, 1];
        class.write("t", &a, Some(b"v"));
        class.write("t", &b, Some(b"v"));
        let stamp = |class: &NonDurableClass, partition: &[u8]| {
            class
                .residency()
                .partitions()
                .find(|entry| entry.partition == partition)
                .unwrap()
                .last_touched
        };
        let before = stamp(&class, &a[..4]);
        assert!(stamp(&class, &b[..4]) > before);
        class.get("t", &a);
        assert!(stamp(&class, &a[..4]) > stamp(&class, &b[..4]));
    }
}
