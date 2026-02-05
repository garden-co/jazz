use ahash::AHashSet;

use crate::query_manager::encoding::decode_row;
use crate::query_manager::types::{
    Row, RowDelta, RowDescriptor, Tuple, TupleDelta, TupleDescriptor, Value,
};

use super::RowNode;

/// Output mode for query results.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputMode {
    /// Emit deltas as they happen.
    Delta,
    /// Emit full result set on each change.
    Full,
}

/// Query subscription identifier.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct QuerySubscriptionId(pub u64);

/// Output node - terminal node that delivers results to subscribers.
///
/// Materializes any remaining unmaterialized elements and flattens
/// multi-element tuples to single Row outputs.
#[derive(Debug)]
pub struct OutputNode {
    descriptor: RowDescriptor,
    /// Output tuple descriptor (always fully materialized).
    output_tuple_descriptor: TupleDescriptor,
    mode: OutputMode,
    /// Current result tuples (for RowNode trait).
    current_tuples: AHashSet<Tuple>,
    /// Ordered tuples for deterministic output (preserves sort order).
    ordered_tuples: Vec<Tuple>,
    /// Pending tuple deltas to deliver.
    pending_tuple_deltas: Vec<TupleDelta>,
    /// True if subscriber has received initial snapshot.
    subscriber_initialized: bool,
    dirty: bool,
}

impl OutputNode {
    /// Create an OutputNode with TupleDescriptor.
    /// The output descriptor is always fully materialized.
    pub fn with_tuple_descriptor(tuple_descriptor: TupleDescriptor, mode: OutputMode) -> Self {
        let descriptor = tuple_descriptor.combined_descriptor();
        // Output is always fully materialized
        let output_tuple_descriptor = tuple_descriptor.clone().with_all_materialized();
        Self {
            descriptor,
            output_tuple_descriptor,
            mode,
            current_tuples: AHashSet::new(),
            ordered_tuples: Vec::new(),
            pending_tuple_deltas: Vec::new(),
            subscriber_initialized: false,
            dirty: true,
        }
    }

    /// Get the output tuple descriptor.
    pub fn output_tuple_descriptor(&self) -> &TupleDescriptor {
        &self.output_tuple_descriptor
    }

    /// Get the output mode.
    pub fn mode(&self) -> OutputMode {
        self.mode
    }

    /// Take pending tuple deltas (for delta mode).
    pub fn take_tuple_deltas(&mut self) -> Vec<TupleDelta> {
        std::mem::take(&mut self.pending_tuple_deltas)
    }

    /// Get current result as rows (extracts from tuples).
    /// For single-table queries, converts length-1 tuples to rows.
    /// Returns rows in insertion order (preserves sort order from upstream nodes).
    pub fn current_rows(&self) -> Vec<Row> {
        self.ordered_tuples
            .iter()
            .filter_map(|t| t.to_single_row())
            .collect()
    }

    /// Decode current rows to Values (for output to user).
    pub fn decode_current(&self) -> Vec<Vec<Value>> {
        self.current_rows()
            .iter()
            .filter_map(|row| decode_row(&self.descriptor, &row.data).ok())
            .collect()
    }

    /// Decode a delta to Values.
    pub fn decode_delta(&self, delta: &RowDelta) -> DecodedDelta {
        DecodedDelta {
            added: delta
                .added
                .iter()
                .filter_map(|row| {
                    decode_row(&self.descriptor, &row.data)
                        .ok()
                        .map(|v| (row.id, v))
                })
                .collect(),
            removed: delta
                .removed
                .iter()
                .filter_map(|row| {
                    decode_row(&self.descriptor, &row.data)
                        .ok()
                        .map(|v| (row.id, v))
                })
                .collect(),
            updated: delta
                .updated
                .iter()
                .filter_map(|(old, new)| {
                    let old_v = decode_row(&self.descriptor, &old.data).ok()?;
                    let new_v = decode_row(&self.descriptor, &new.data).ok()?;
                    Some((old.id, old_v, new_v))
                })
                .collect(),
        }
    }
}

/// Decoded delta with Values instead of binary.
#[derive(Debug, Clone)]
pub struct DecodedDelta {
    pub added: Vec<(crate::object::ObjectId, Vec<Value>)>,
    pub removed: Vec<(crate::object::ObjectId, Vec<Value>)>,
    pub updated: Vec<(crate::object::ObjectId, Vec<Value>, Vec<Value>)>,
}

impl RowNode for OutputNode {
    fn output_descriptor(&self) -> &RowDescriptor {
        &self.descriptor
    }

    fn process(&mut self, input: TupleDelta) -> TupleDelta {
        // Apply changes to current_tuples and ordered_tuples
        for tuple in &input.removed {
            self.current_tuples.remove(tuple);
            self.ordered_tuples.retain(|t| t != tuple);
        }

        for tuple in &input.added {
            self.current_tuples.insert(tuple.clone());
            self.ordered_tuples.push(tuple.clone());
        }

        for (old_tuple, new_tuple) in &input.updated {
            self.current_tuples.remove(old_tuple);
            self.current_tuples.insert(new_tuple.clone());
            // Update in place in ordered_tuples to preserve position
            if let Some(pos) = self.ordered_tuples.iter().position(|t| t == old_tuple) {
                self.ordered_tuples[pos] = new_tuple.clone();
            }
        }

        self.dirty = false;

        // Deliver immediately
        self.subscriber_initialized = true;
        if !input.is_empty() {
            self.pending_tuple_deltas.push(input.clone());
        }

        input
    }

    fn current_tuples(&self) -> &AHashSet<Tuple> {
        &self.current_tuples
    }

    fn mark_dirty(&mut self) {
        self.dirty = true;
    }

    fn is_dirty(&self) -> bool {
        self.dirty
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commit::CommitId;
    use crate::object::ObjectId;
    use crate::query_manager::encoding::encode_row;
    use crate::query_manager::types::{ColumnDescriptor, ColumnType, TupleElement};

    fn test_descriptor() -> RowDescriptor {
        RowDescriptor::new(vec![
            ColumnDescriptor::new("id", ColumnType::Integer),
            ColumnDescriptor::new("name", ColumnType::Text),
        ])
    }

    fn make_tuple(id: ObjectId, n: i32, name: &str) -> Tuple {
        let descriptor = test_descriptor();
        let data = encode_row(&descriptor, &[Value::Integer(n), Value::Text(name.into())]).unwrap();
        Tuple::new(vec![TupleElement::Row {
            id,
            content: data,
            commit_id: CommitId([0; 32]),
        }])
    }

    fn make_output_node(mode: OutputMode) -> OutputNode {
        let descriptor = test_descriptor();
        let tuple_desc = TupleDescriptor::single_with_materialization("", descriptor, true);
        OutputNode::with_tuple_descriptor(tuple_desc, mode)
    }

    #[test]
    fn output_stores_deltas() {
        let mut node = make_output_node(OutputMode::Delta);

        let id1 = ObjectId::new();
        let tuple1 = make_tuple(id1, 1, "Alice");

        let delta = TupleDelta {
            added: vec![tuple1],
            removed: vec![],
            updated: vec![],
        };

        node.process(delta);

        let deltas = node.take_tuple_deltas();
        assert_eq!(deltas.len(), 1);
        assert_eq!(deltas[0].added.len(), 1);
    }

    #[test]
    fn output_decodes_current() {
        let mut node = make_output_node(OutputMode::Full);

        let id1 = ObjectId::new();
        let tuple1 = make_tuple(id1, 1, "Alice");

        node.process(TupleDelta {
            added: vec![tuple1],
            removed: vec![],
            updated: vec![],
        });

        let decoded = node.decode_current();
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0][0], Value::Integer(1));
        assert_eq!(decoded[0][1], Value::Text("Alice".into()));
    }

    #[test]
    fn output_decodes_delta() {
        let node = make_output_node(OutputMode::Delta);

        let id1 = ObjectId::new();
        let row1 = Row::new(
            id1,
            encode_row(
                &test_descriptor(),
                &[Value::Integer(1), Value::Text("Alice".into())],
            )
            .unwrap(),
            CommitId([0; 32]),
        );

        let delta = RowDelta {
            added: vec![row1],
            removed: vec![],
            updated: vec![],
        };

        let decoded = node.decode_delta(&delta);
        assert_eq!(decoded.added.len(), 1);
        assert_eq!(decoded.added[0].0, id1);
        assert_eq!(decoded.added[0].1[0], Value::Integer(1));
        assert_eq!(decoded.added[0].1[1], Value::Text("Alice".into()));
    }

    #[test]
    fn empty_delta_not_stored() {
        let mut node = make_output_node(OutputMode::Delta);

        let delta = TupleDelta::new();
        node.process(delta);

        let deltas = node.take_tuple_deltas();
        assert!(deltas.is_empty());
    }

    #[test]
    fn output_delivers_immediately() {
        let mut node = make_output_node(OutputMode::Delta);

        let id1 = ObjectId::new();
        let id2 = ObjectId::new();
        let tuple1 = make_tuple(id1, 1, "Alice");
        let tuple2 = make_tuple(id2, 2, "Bob");

        // First delta
        let delta1 = TupleDelta {
            added: vec![tuple1],
            removed: vec![],
            updated: vec![],
        };
        node.process(delta1);

        // Should deliver immediately
        let deltas = node.take_tuple_deltas();
        assert_eq!(deltas.len(), 1);
        assert_eq!(deltas[0].added.len(), 1);

        // Second delta
        let delta2 = TupleDelta {
            added: vec![tuple2],
            removed: vec![],
            updated: vec![],
        };
        node.process(delta2);

        // Should also deliver immediately
        let deltas = node.take_tuple_deltas();
        assert_eq!(deltas.len(), 1);
        assert_eq!(deltas[0].added.len(), 1);
    }
}
