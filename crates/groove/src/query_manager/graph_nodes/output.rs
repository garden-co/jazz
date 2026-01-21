use std::collections::HashSet;

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
    current_tuples: HashSet<Tuple>,
    /// Ordered tuples for deterministic output (preserves sort order).
    ordered_tuples: Vec<Tuple>,
    /// Pending tuple deltas to deliver.
    pending_tuple_deltas: Vec<TupleDelta>,
    /// True if we're holding back results due to pending rows.
    held_pending: bool,
    /// True if subscriber has received initial snapshot.
    subscriber_initialized: bool,
    /// Accumulated tuple changes while pending.
    held_tuple_changes: TupleDelta,
    dirty: bool,
}

impl OutputNode {
    /// Create an OutputNode with RowDescriptor (backward compatible).
    pub fn new(descriptor: RowDescriptor, mode: OutputMode) -> Self {
        let output_tuple_descriptor =
            TupleDescriptor::single_with_materialization("", descriptor.clone(), true);
        Self {
            descriptor,
            output_tuple_descriptor,
            mode,
            current_tuples: HashSet::new(),
            ordered_tuples: Vec::new(),
            pending_tuple_deltas: Vec::new(),
            held_pending: false,
            subscriber_initialized: false,
            held_tuple_changes: TupleDelta::new(),
            dirty: true,
        }
    }

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
            current_tuples: HashSet::new(),
            ordered_tuples: Vec::new(),
            pending_tuple_deltas: Vec::new(),
            held_pending: false,
            subscriber_initialized: false,
            held_tuple_changes: TupleDelta::new(),
            dirty: true,
        }
    }

    /// Get the output tuple descriptor.
    pub fn output_tuple_descriptor(&self) -> &TupleDescriptor {
        &self.output_tuple_descriptor
    }

    /// Check if we're holding back results due to pending rows.
    pub fn is_held_pending(&self) -> bool {
        self.held_pending
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
        // Apply changes to current_tuples and ordered_tuples (always update internal state)
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

        // Handle pending state for delivery
        if input.pending {
            // Still pending - hold back results, don't deliver to subscribers
            self.held_pending = true;

            // If subscriber is already initialized, accumulate changes
            if self.subscriber_initialized {
                self.held_tuple_changes.added.extend(input.added.clone());
                self.held_tuple_changes
                    .removed
                    .extend(input.removed.clone());
                self.held_tuple_changes
                    .updated
                    .extend(input.updated.clone());
            }

            // Return the input but don't add to pending_tuple_deltas
            return input;
        }

        // Not pending - check if we were previously holding back
        if self.held_pending {
            self.held_pending = false;

            if !self.subscriber_initialized {
                // First time: emit full current_tuples as the initial snapshot
                self.subscriber_initialized = true;

                if !self.current_tuples.is_empty() {
                    let snapshot = TupleDelta {
                        added: self.current_tuples.iter().cloned().collect(),
                        removed: vec![],
                        updated: vec![],
                        pending: false,
                    };
                    self.pending_tuple_deltas.push(snapshot.clone());
                    return snapshot;
                }
                return input;
            } else {
                // Subsequent pending period: emit only accumulated changes
                self.held_tuple_changes.added.extend(input.added.clone());
                self.held_tuple_changes
                    .removed
                    .extend(input.removed.clone());
                self.held_tuple_changes
                    .updated
                    .extend(input.updated.clone());

                let result = std::mem::take(&mut self.held_tuple_changes);
                if !result.is_empty() {
                    self.pending_tuple_deltas.push(result.clone());
                }
                return result;
            }
        }

        // Normal case - not pending, weren't holding back
        self.subscriber_initialized = true;
        if !input.is_empty() {
            self.pending_tuple_deltas.push(input.clone());
        }

        input
    }

    fn current_tuples(&self) -> &HashSet<Tuple> {
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

    fn contains_id(tuples: &[Tuple], id: ObjectId) -> bool {
        tuples.iter().any(|t| t.ids().contains(&id))
    }

    #[test]
    fn output_stores_deltas() {
        let descriptor = test_descriptor();
        let mut node = OutputNode::new(descriptor, OutputMode::Delta);

        let id1 = ObjectId::new();
        let tuple1 = make_tuple(id1, 1, "Alice");

        let delta = TupleDelta {
            pending: false,
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
        let descriptor = test_descriptor();
        let mut node = OutputNode::new(descriptor, OutputMode::Full);

        let id1 = ObjectId::new();
        let tuple1 = make_tuple(id1, 1, "Alice");

        node.process(TupleDelta {
            pending: false,
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
        let descriptor = test_descriptor();
        let node = OutputNode::new(descriptor, OutputMode::Delta);

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
            pending: false,
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
        let descriptor = test_descriptor();
        let mut node = OutputNode::new(descriptor, OutputMode::Delta);

        let delta = TupleDelta::new();
        node.process(delta);

        let deltas = node.take_tuple_deltas();
        assert!(deltas.is_empty());
    }

    #[test]
    fn output_holds_back_when_pending() {
        let descriptor = test_descriptor();
        let mut node = OutputNode::new(descriptor, OutputMode::Delta);

        let id1 = ObjectId::new();
        let tuple1 = make_tuple(id1, 1, "Alice");

        // Process delta with pending=true
        let delta = TupleDelta {
            pending: true,
            added: vec![tuple1],
            removed: vec![],
            updated: vec![],
        };

        node.process(delta);

        // Internal state should be updated
        assert_eq!(node.current_rows().len(), 1);
        assert!(node.is_held_pending());

        // But no deltas should be delivered
        let deltas = node.take_tuple_deltas();
        assert!(deltas.is_empty(), "Should hold back deltas when pending");
    }

    #[test]
    fn output_emits_full_state_when_pending_clears() {
        let descriptor = test_descriptor();
        let mut node = OutputNode::new(descriptor, OutputMode::Delta);

        let id1 = ObjectId::new();
        let id2 = ObjectId::new();
        let tuple1 = make_tuple(id1, 1, "Alice");
        let tuple2 = make_tuple(id2, 2, "Bob");

        // First delta with pending=true
        let delta1 = TupleDelta {
            pending: true,
            added: vec![tuple1],
            removed: vec![],
            updated: vec![],
        };
        node.process(delta1);
        assert!(node.is_held_pending());
        assert!(node.take_tuple_deltas().is_empty());

        // Second delta with pending=true (add another tuple)
        let delta2 = TupleDelta {
            pending: true,
            added: vec![tuple2],
            removed: vec![],
            updated: vec![],
        };
        node.process(delta2);
        assert!(node.is_held_pending());
        assert!(node.take_tuple_deltas().is_empty());

        // Now pending clears
        let delta3 = TupleDelta {
            pending: false,
            added: vec![],
            removed: vec![],
            updated: vec![],
        };
        node.process(delta3);

        // Should no longer be held pending
        assert!(!node.is_held_pending());

        // Should emit full current state as a single delta
        let deltas = node.take_tuple_deltas();
        assert_eq!(deltas.len(), 1);
        assert_eq!(
            deltas[0].added.len(),
            2,
            "Should contain all current tuples"
        );
    }

    #[test]
    fn output_normal_behavior_when_not_pending() {
        let descriptor = test_descriptor();
        let mut node = OutputNode::new(descriptor, OutputMode::Delta);

        let id1 = ObjectId::new();
        let id2 = ObjectId::new();
        let tuple1 = make_tuple(id1, 1, "Alice");
        let tuple2 = make_tuple(id2, 2, "Bob");

        // Normal delta (not pending)
        let delta1 = TupleDelta {
            pending: false,
            added: vec![tuple1],
            removed: vec![],
            updated: vec![],
        };
        node.process(delta1);

        // Should deliver immediately
        let deltas = node.take_tuple_deltas();
        assert_eq!(deltas.len(), 1);
        assert_eq!(deltas[0].added.len(), 1);

        // Second normal delta
        let delta2 = TupleDelta {
            pending: false,
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

    #[test]
    fn output_subsequent_pending_emits_only_new_changes() {
        // This tests the scenario:
        // 1. Initial pending → clears → full snapshot emitted
        // 2. Normal updates → delivered incrementally
        // 3. New pending → clears → only NEW changes emitted (not full snapshot)

        let descriptor = test_descriptor();
        let mut node = OutputNode::new(descriptor, OutputMode::Delta);

        let id1 = ObjectId::new();
        let id2 = ObjectId::new();
        let id3 = ObjectId::new();
        let tuple1 = make_tuple(id1, 1, "Alice");
        let tuple2 = make_tuple(id2, 2, "Bob");
        let tuple3 = make_tuple(id3, 3, "Charlie");

        // Step 1: Initial pending period
        let delta1 = TupleDelta {
            pending: true,
            added: vec![tuple1],
            removed: vec![],
            updated: vec![],
        };
        node.process(delta1);
        assert!(
            node.take_tuple_deltas().is_empty(),
            "Should hold back during initial pending"
        );

        // Step 2: Initial pending clears
        let delta2 = TupleDelta {
            pending: false,
            added: vec![tuple2],
            removed: vec![],
            updated: vec![],
        };
        node.process(delta2);

        // Should emit full snapshot (both tuples)
        let deltas = node.take_tuple_deltas();
        assert_eq!(deltas.len(), 1);
        assert_eq!(
            deltas[0].added.len(),
            2,
            "Initial clear should emit full snapshot"
        );

        // Step 3: Normal update (non-pending)
        let delta3 = TupleDelta {
            pending: false,
            added: vec![tuple3],
            removed: vec![],
            updated: vec![],
        };
        node.process(delta3);

        // Should deliver incrementally
        let deltas = node.take_tuple_deltas();
        assert_eq!(deltas.len(), 1);
        assert_eq!(
            deltas[0].added.len(),
            1,
            "Normal update should be incremental"
        );
        assert!(contains_id(&deltas[0].added, id3));

        // Step 4: New pending period starts
        let id4 = ObjectId::new();
        let tuple4 = make_tuple(id4, 4, "Dave");
        let delta4 = TupleDelta {
            pending: true,
            added: vec![tuple4],
            removed: vec![],
            updated: vec![],
        };
        node.process(delta4);
        assert!(
            node.take_tuple_deltas().is_empty(),
            "Should hold back during second pending"
        );

        // Step 5: Second pending clears
        let id5 = ObjectId::new();
        let tuple5 = make_tuple(id5, 5, "Eve");
        let delta5 = TupleDelta {
            pending: false,
            added: vec![tuple5],
            removed: vec![],
            updated: vec![],
        };
        node.process(delta5);

        // Should emit ONLY the changes during the pending period (tuple4, tuple5)
        // NOT the full snapshot (which would be all 5 tuples)
        let deltas = node.take_tuple_deltas();
        assert_eq!(deltas.len(), 1);
        assert_eq!(
            deltas[0].added.len(),
            2,
            "Subsequent pending clear should emit only accumulated changes, not full snapshot"
        );

        // Verify we got tuple4 and tuple5, not the old tuples
        assert!(contains_id(&deltas[0].added, id4), "Should contain tuple4");
        assert!(contains_id(&deltas[0].added, id5), "Should contain tuple5");
        assert!(
            !contains_id(&deltas[0].added, id1),
            "Should NOT contain tuple1 (from initial snapshot)"
        );
    }
}
