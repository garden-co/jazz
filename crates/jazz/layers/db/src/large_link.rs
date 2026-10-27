//! Chunked values across one connection (SPEC 19.8). Each direction keeps a
//! link table from root hash to the encoded edits of the last value on that
//! root that crossed it; both ends update their copies identically, in
//! message order. A value whose edits extend its root's entry travels as
//! the extending bytes ([`WireDelta`]), anything else as its descriptor.
//!
//! [`LargeLink`] wraps a connection's transport: it encodes as a message is
//! handed to the transport and decodes as one is taken from it, so the rest
//! of sync only ever sees whole descriptors.

use jazz_types::large_values::{self as lv, LargeValueRef, epoch};
use serde::{Deserialize, Serialize};

use super::{Cell, SyncMessage, Transport, WireOp};
use crate::records::Value;
use crate::wire::TransportError;

/// The edit bytes one direction's table holds.
pub(crate) const LINK_TABLE_BYTES: usize = 16 * 1024;
/// What an entry costs beyond its edit bytes, so the table also bounds
/// how many entries it holds.
const ENTRY_BYTES: usize = 64;

/// A chunked value as the encoded edits extending the entry of its root.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct WireDelta {
    pub root: [u8; 32],
    /// How many edits the entry holds: a check, as both tables agree.
    pub base: u32,
    #[serde(with = "serde_bytes")]
    pub edits: Vec<u8>,
}

/// A commit's chunked cell that travels as a [`WireDelta`]: its op's
/// cell holds NULL in its place, under `depth` nullable wrappers.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct CellDelta {
    pub op: u32,
    pub column: String,
    pub depth: u8,
    pub delta: WireDelta,
}

/// A value's tree (its descriptor without edits, with the tree's
/// lengths) and its encoded edits.
#[derive(Clone)]
struct Entry {
    root: [u8; 32],
    tree: LargeValueRef,
    edits: Vec<u8>,
    count: usize,
}

impl Entry {
    fn of(value: &LargeValueRef) -> Option<Self> {
        let (byte_length, utf16_length) = lv::tail_base_lengths(value).ok()?;
        let edits =
            epoch::encode_edits(value.kind, byte_length, utf16_length, &value.edit_tail).ok()?;
        Some(Self {
            root: value.root.object_hash.0,
            tree: LargeValueRef {
                kind: value.kind,
                format_version: value.format_version,
                logical_hash: value.logical_hash,
                root: value.root.clone(),
                byte_length,
                utf16_length,
                edit_tail: Vec::new(),
            },
            edits,
            count: value.edit_tail.len(),
        })
    }

    fn cost(&self) -> usize {
        self.edits.len() + ENTRY_BYTES
    }
}

/// One direction's table, most recently used entry first.
#[derive(Clone, Default)]
pub(crate) struct LinkTable {
    entries: Vec<Entry>,
    bytes: usize,
}

impl LinkTable {
    fn entry(&self, root: &[u8; 32]) -> Option<&Entry> {
        self.entries.iter().find(|entry| entry.root == *root)
    }

    /// `root`'s entry becomes `entry` (none: it is dropped).
    fn put(&mut self, root: [u8; 32], entry: Option<Entry>) {
        if let Some(at) = self.entries.iter().position(|entry| entry.root == root) {
            self.bytes -= self.entries.remove(at).cost();
        }
        let Some(entry) = entry.filter(|entry| entry.cost() <= LINK_TABLE_BYTES) else {
            return;
        };
        self.bytes += entry.cost();
        self.entries.insert(0, entry);
        while self.bytes > LINK_TABLE_BYTES {
            let dropped = self.entries.pop().expect("an entry over the budget");
            self.bytes -= dropped.cost();
        }
    }

    /// How `value` crosses: as a delta when its edits extend its root's
    /// entry, else whole (`None`).
    pub(crate) fn send(&mut self, value: &LargeValueRef) -> Option<WireDelta> {
        let root = value.root.object_hash.0;
        let entry = Entry::of(value);
        let delta = entry.as_ref().and_then(|next| {
            let held = self.entry(&root)?;
            (held.tree == next.tree && next.edits.starts_with(&held.edits)).then(|| WireDelta {
                root,
                base: held.count as u32,
                edits: next.edits[held.edits.len()..].to_vec(),
            })
        });
        self.put(root, entry);
        delta
    }

    /// A whole value crossed.
    pub(crate) fn received(&mut self, value: &LargeValueRef) {
        self.put(value.root.object_hash.0, Entry::of(value));
    }

    /// The value a delta names.
    pub(crate) fn resolve(&mut self, delta: &WireDelta) -> Result<LargeValueRef, String> {
        let held = self
            .entry(&delta.root)
            .filter(|held| held.count == delta.base as usize)
            .ok_or("a large-value delta names no value of this link")?;
        let mut edits = held.edits.clone();
        edits.extend_from_slice(&delta.edits);
        let tree = &held.tree;
        let (edit_tail, cursor) =
            epoch::decode_edits(tree.kind, tree.byte_length, tree.utf16_length, &edits, None)
                .map_err(|error| format!("a malformed large-value delta: {error}"))?;
        let value = LargeValueRef {
            byte_length: cursor.len,
            utf16_length: tree.utf16_length.map(|_| cursor.utf16_len),
            edit_tail,
            ..tree.clone()
        };
        let count = value.edit_tail.len();
        let tree = tree.clone();
        self.put(
            delta.root,
            Some(Entry {
                root: delta.root,
                tree,
                edits,
                count,
            }),
        );
        Ok(value)
    }
}

/// The innermost value of a cell, and how many nullable wrappers it is in.
fn unwrap(value: &mut Value) -> (&mut Value, u8) {
    let mut depth = 0;
    let mut value = value;
    while let Value::Nullable(Some(inner)) = value {
        value = inner;
        depth += 1;
    }
    (value, depth)
}

fn op_cells(op: &mut WireOp) -> Option<&mut std::collections::BTreeMap<String, Value>> {
    match op {
        WireOp::Insert { cells, .. }
        | WireOp::Update { cells, .. }
        | WireOp::Upsert { cells, .. }
        | WireOp::Restore {
            cells: Some(cells), ..
        } => Some(cells),
        _ => None,
    }
}

fn has_large(message: &SyncMessage) -> bool {
    match message {
        SyncMessage::Rows { images } | SyncMessage::RowsPart { images } => images
            .iter()
            .any(|image| image.cells.iter().any(|(_, c)| matches!(c, Cell::Large(_)))),
        SyncMessage::Commit { .. } => true,
        _ => false,
    }
}

/// `message` as it crosses, with `table` updated for what it carries.
fn encode(table: &mut LinkTable, message: &mut SyncMessage) {
    match message {
        SyncMessage::Rows { images } | SyncMessage::RowsPart { images } => {
            for (_, cell) in images.iter_mut().flat_map(|image| image.cells.iter_mut()) {
                let Cell::Large(descriptor) = cell else {
                    continue;
                };
                // A descriptor this store wrote decodes; one that does not
                // fails the receiver, which would not take it either.
                let Ok(value) = lv::decode_large_value_ref(descriptor) else {
                    continue;
                };
                if let Some(delta) = table.send(&value) {
                    *cell = Cell::Delta(delta);
                }
            }
        }
        SyncMessage::Commit { ops, deltas, .. } => {
            for (index, op) in ops.iter_mut().enumerate() {
                let Some(cells) = op_cells(op) else { continue };
                for (column, cell) in cells.iter_mut() {
                    let (value, depth) = unwrap(cell);
                    let Value::Large(large) = value else { continue };
                    if let Some(delta) = table.send(large) {
                        *value = Value::Nullable(None);
                        deltas.push(CellDelta {
                            op: index as u32,
                            column: column.clone(),
                            depth,
                            delta,
                        });
                    }
                }
            }
        }
        _ => {}
    }
}

/// `message` as it was sent, with `table` updated for what it carries.
fn decode(table: &mut LinkTable, message: &mut SyncMessage) -> Result<(), String> {
    match message {
        SyncMessage::Rows { images } | SyncMessage::RowsPart { images } => {
            for (_, cell) in images.iter_mut().flat_map(|image| image.cells.iter_mut()) {
                match cell {
                    Cell::Large(descriptor) => {
                        let value = lv::decode_large_value_ref(descriptor)
                            .map_err(|error| format!("a malformed large value: {error}"))?;
                        table.received(&value);
                    }
                    Cell::Delta(delta) => {
                        let value = table.resolve(delta)?;
                        let descriptor = lv::encode_large_value_ref(&value)
                            .map_err(|error| format!("a malformed large-value delta: {error}"))?;
                        *cell = Cell::Large(descriptor);
                    }
                    _ => {}
                }
            }
        }
        SyncMessage::Commit { ops, deltas, .. } => {
            let mut deltas = std::mem::take(deltas).into_iter().peekable();
            for (index, op) in ops.iter_mut().enumerate() {
                let Some(cells) = op_cells(op) else { continue };
                for (column, cell) in cells.iter_mut() {
                    let next = deltas
                        .next_if(|delta| delta.op as usize == index && delta.column == *column);
                    if let Some(CellDelta { depth, delta, .. }) = next {
                        let mut value = Value::Large(Box::new(table.resolve(&delta)?));
                        for _ in 0..depth {
                            value = Value::Nullable(Some(Box::new(value)));
                        }
                        *cell = value;
                        continue;
                    }
                    if let (Value::Large(large), _) = unwrap(cell) {
                        table.received(large);
                    }
                }
            }
            if deltas.next().is_some() {
                return Err("a commit's large-value delta names no cell".to_owned());
            }
        }
        _ => {}
    }
    Ok(())
}

/// A connection's transport with its two link tables.
pub(crate) struct LargeLink {
    transport: Box<dyn Transport>,
    sent: LinkTable,
    received: LinkTable,
    /// A message failed to decode: nothing more is read, and
    /// [`Transport::take_failure`] reports it once.
    failed: Option<Option<TransportError>>,
}

impl LargeLink {
    pub(crate) fn wrap(transport: Box<dyn Transport>) -> Box<dyn Transport> {
        Box::new(Self {
            transport,
            sent: LinkTable::default(),
            received: LinkTable::default(),
            failed: None,
        })
    }
}

impl Transport for LargeLink {
    fn send(&mut self, mut message: SyncMessage) -> Result<(), TransportError> {
        if !has_large(&message) {
            return self.transport.send(message);
        }
        // A message the transport does not take (backpressure) is sent
        // again later: the table changes only with a message it took.
        let mut table = self.sent.clone();
        encode(&mut table, &mut message);
        self.transport.send(message)?;
        self.sent = table;
        Ok(())
    }

    fn try_recv(&mut self) -> Option<SyncMessage> {
        if self.failed.is_some() {
            return None;
        }
        let mut message = self.transport.try_recv()?;
        match decode(&mut self.received, &mut message) {
            Ok(()) => Some(message),
            Err(error) => {
                self.failed = Some(Some(TransportError::Failed(error)));
                None
            }
        }
    }

    fn take_failure(&mut self) -> Option<TransportError> {
        match &mut self.failed {
            Some(failure) => failure.take(),
            None => self.transport.take_failure(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tree(seed: usize) -> LargeValueRef {
        let text = format!("{seed}{}", "x".repeat(lv::INLINE_VALUE_MAX_BYTES));
        lv::prepare(lv::LargeValueKind::String, text.as_bytes())
            .unwrap()
            .value_ref
    }

    fn extend(value: &LargeValueRef, appends: &[&str]) -> LargeValueRef {
        let mut value = value.clone();
        for suffix in appends {
            match lv::append_tail(&value, suffix.as_bytes().to_vec()).unwrap() {
                lv::TailOutcome::Updated(next) => value = next,
                lv::TailOutcome::ConsolidationRequired(_) => panic!("the tail is full"),
            }
        }
        value
    }

    #[test]
    fn extending_values_cross_as_their_new_edits() {
        let base = tree(0);
        let (mut sender, mut receiver) = (LinkTable::default(), LinkTable::default());
        let first = extend(&base, &["a"]);
        assert_eq!(sender.send(&first), None);
        receiver.received(&first);
        let second = extend(&first, &["bc"]);
        let delta = sender.send(&second).expect("a delta");
        assert_eq!(delta.base, 1);
        assert_eq!(delta.edits, [0x02, b'b', b'c']);
        assert_eq!(receiver.resolve(&delta).unwrap(), second);
        // The same value again is an empty delta.
        let again = sender.send(&second).expect("a delta");
        assert!(again.edits.is_empty());
        assert_eq!(receiver.resolve(&again).unwrap(), second);
        // A fork crosses whole, and replaces the entry.
        let fork = extend(&base, &["z"]);
        assert_eq!(sender.send(&fork), None);
        receiver.received(&fork);
        let after = extend(&fork, &["y"]);
        let delta = sender.send(&after).expect("a delta");
        assert_eq!(receiver.resolve(&delta).unwrap(), after);
    }

    #[test]
    fn a_delta_the_receiver_cannot_place_is_an_error() {
        let base = tree(0);
        let mut receiver = LinkTable::default();
        let delta = WireDelta {
            root: base.root.object_hash.0,
            base: 0,
            edits: vec![0x01, b'a'],
        };
        assert!(receiver.resolve(&delta).is_err());
        receiver.received(&extend(&base, &["a"]));
        assert!(receiver.resolve(&delta).is_err(), "the base count differs");
    }

    #[test]
    fn the_table_keeps_its_budget_most_recent_first() {
        let mut table = LinkTable::default();
        let values: Vec<LargeValueRef> = (0..LINK_TABLE_BYTES / ENTRY_BYTES + 8)
            .map(|seed| extend(&tree(seed), &["a"]))
            .collect();
        for value in &values {
            table.send(value);
        }
        assert!(table.bytes <= LINK_TABLE_BYTES);
        assert_eq!(
            table.bytes,
            table.entries.iter().map(Entry::cost).sum::<usize>()
        );
        let newest = values.last().unwrap();
        assert!(table.send(&extend(newest, &["b"])).is_some());
        assert!(table.send(&extend(&values[0], &["b"])).is_none(), "dropped");
    }
}
