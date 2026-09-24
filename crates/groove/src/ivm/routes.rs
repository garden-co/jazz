//! Route index for prepared-shape bindings (#3288).
//!
//! Every live binding of a prepared shape reads the shape's shared terminal
//! through its own `Filter(route fields == binding values)` node. Without an
//! index, a write that changes one binding's result activates every binding's
//! filter. A route barrier marks such a filter: activation stops there, and the
//! tick activates only the barriers whose route key occurs in the shared
//! terminal's delta.
//!
//! Soundness rests on the filter being exact equality on its route fields, so
//! a delta record matches a barrier iff its encoded route fields equal the
//! barrier's key. Only field types whose equality is their canonical encoding
//! are indexed; any other binding keeps ordinary activation.

use rustc_hash::{FxHashMap as HashMap, FxHashSet as HashSet};

use crate::records::{self, BorrowedRecord, Value, ValueType};

use super::NodeId;

/// One shared terminal's barriers, grouped by encoded route key.
#[derive(Clone, Debug, Default)]
pub(crate) struct RouteTable {
    pub(crate) field_indices: Vec<usize>,
    pub(crate) field_types: Vec<ValueType>,
    pub(crate) by_key: HashMap<Vec<u8>, HashSet<NodeId>>,
    /// Root-ordering nodes consumed below this terminal's barriers. They must
    /// collect positions before the barriers are known to be touched.
    pub(crate) root_ordering_nodes: HashSet<NodeId>,
}

impl RouteTable {
    /// Encode `values` of a routed record exactly as a barrier key.
    pub(crate) fn key_of_record(&self, record: &BorrowedRecord<'_>) -> Option<Vec<u8>> {
        let values = self
            .field_indices
            .iter()
            .map(|index| record.get_idx(*index).ok())
            .collect::<Option<Vec<_>>>()?;
        encode_route_key(&values, &self.field_types)
    }

    pub(crate) fn barriers(&self) -> impl Iterator<Item = NodeId> + '_ {
        self.by_key.values().flatten().copied()
    }
}

#[derive(Clone, Debug)]
struct Barrier {
    terminal: NodeId,
    key: Vec<u8>,
    refs: usize,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct RouteIndex {
    tables: HashMap<NodeId, RouteTable>,
    barriers: HashMap<NodeId, Barrier>,
}

impl RouteIndex {
    pub(crate) fn is_barrier(&self, node: NodeId) -> bool {
        self.barriers.contains_key(&node)
    }

    pub(crate) fn table(&self, terminal: NodeId) -> Option<&RouteTable> {
        self.tables.get(&terminal)
    }

    pub(crate) fn has_table(&self, terminal: NodeId) -> bool {
        self.tables.contains_key(&terminal)
    }

    /// Returns whether the topology changed (a new barrier).
    pub(crate) fn add(
        &mut self,
        terminal: NodeId,
        barrier: NodeId,
        field_indices: Vec<usize>,
        field_types: Vec<ValueType>,
        key: Vec<u8>,
        root_ordering_node: Option<NodeId>,
    ) -> bool {
        if let Some(existing) = self.barriers.get_mut(&barrier) {
            debug_assert_eq!(existing.terminal, terminal);
            debug_assert_eq!(existing.key, key);
            existing.refs += 1;
            return false;
        }
        let table = self.tables.entry(terminal).or_default();
        if table.field_indices.is_empty() {
            table.field_indices = field_indices;
            table.field_types = field_types;
        }
        debug_assert_eq!(table.field_indices.len(), table.field_types.len());
        table.by_key.entry(key.clone()).or_default().insert(barrier);
        if let Some(node) = root_ordering_node {
            table.root_ordering_nodes.insert(node);
        }
        self.barriers.insert(
            barrier,
            Barrier {
                terminal,
                key,
                refs: 1,
            },
        );
        true
    }

    /// Release one reference. Returns whether the barrier was removed.
    pub(crate) fn release(&mut self, barrier: NodeId) -> bool {
        let Some(entry) = self.barriers.get_mut(&barrier) else {
            return false;
        };
        entry.refs -= 1;
        if entry.refs > 0 {
            return false;
        }
        self.remove(barrier)
    }

    /// Forget a barrier regardless of references (its node left the graph).
    pub(crate) fn remove(&mut self, barrier: NodeId) -> bool {
        let Some(entry) = self.barriers.remove(&barrier) else {
            return false;
        };
        if let Some(table) = self.tables.get_mut(&entry.terminal) {
            if let Some(nodes) = table.by_key.get_mut(&entry.key) {
                nodes.remove(&barrier);
                if nodes.is_empty() {
                    table.by_key.remove(&entry.key);
                }
            }
            if table.by_key.is_empty() {
                self.tables.remove(&entry.terminal);
            }
        }
        true
    }
}

/// Fixed-width field types whose route equality is byte equality of the
/// canonical single-field encoding. Floats (±0, NaN), enums and composites
/// are excluded, as are strings and bytes: they may be stored out of line as
/// large values, whose equality is not their inline encoding.
pub(crate) fn is_routable_type(value_type: &ValueType) -> bool {
    match value_type {
        ValueType::U8
        | ValueType::U16
        | ValueType::U32
        | ValueType::U64
        | ValueType::I32
        | ValueType::I64
        | ValueType::Bool
        | ValueType::Uuid => true,
        ValueType::Nullable(inner) => is_routable_type(inner),
        _ => false,
    }
}

/// Length-delimited concatenation of each route value's canonical encoding.
/// `None` if a value does not have exactly its field's type.
pub(crate) fn encode_route_key(values: &[Value], types: &[ValueType]) -> Option<Vec<u8>> {
    if values.len() != types.len() {
        return None;
    }
    let mut key = Vec::new();
    for (value, value_type) in values.iter().zip(types) {
        let encoded = records::encode_single_field_value(value, value_type).ok()?;
        key.extend_from_slice(&(encoded.len() as u32).to_le_bytes());
        key.extend_from_slice(&encoded);
    }
    Some(key)
}
