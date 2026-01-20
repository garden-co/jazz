use crate::query_manager::encoding::decode_row;
use crate::query_manager::types::{Row, RowDelta, RowDescriptor, Value};

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
#[derive(Debug)]
pub struct OutputNode {
    descriptor: RowDescriptor,
    mode: OutputMode,
    /// Current result rows.
    current_rows: Vec<Row>,
    /// Pending deltas to deliver.
    pending_deltas: Vec<RowDelta>,
    /// True if we're holding back results due to pending rows.
    held_pending: bool,
    /// True if subscriber has received initial snapshot.
    subscriber_initialized: bool,
    /// Accumulated changes while pending (for subsequent pending periods).
    held_changes: RowDelta,
    dirty: bool,
}

impl OutputNode {
    pub fn new(descriptor: RowDescriptor, mode: OutputMode) -> Self {
        Self {
            descriptor,
            mode,
            current_rows: Vec::new(),
            pending_deltas: Vec::new(),
            held_pending: false,
            subscriber_initialized: false,
            held_changes: RowDelta::new(),
            dirty: true,
        }
    }

    /// Check if we're holding back results due to pending rows.
    pub fn is_held_pending(&self) -> bool {
        self.held_pending
    }

    /// Get the output mode.
    pub fn mode(&self) -> OutputMode {
        self.mode
    }

    /// Take pending deltas (for delta mode).
    pub fn take_deltas(&mut self) -> Vec<RowDelta> {
        std::mem::take(&mut self.pending_deltas)
    }

    /// Decode current rows to Values (for output to user).
    pub fn decode_current(&self) -> Vec<Vec<Value>> {
        self.current_rows
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

    fn process(&mut self, input: RowDelta) -> RowDelta {
        // Apply changes to current_rows (always update internal state)
        for row in &input.removed {
            self.current_rows.retain(|r| r.id != row.id);
        }

        for row in &input.added {
            self.current_rows.push(row.clone());
        }

        for (old_row, new_row) in &input.updated {
            if let Some(pos) = self.current_rows.iter().position(|r| r.id == old_row.id) {
                self.current_rows[pos] = new_row.clone();
            }
        }

        self.dirty = false;

        // Handle pending state for delivery
        if input.pending {
            // Still pending - hold back results, don't deliver to subscribers
            self.held_pending = true;

            // If subscriber is already initialized, accumulate changes
            if self.subscriber_initialized {
                self.held_changes.added.extend(input.added.clone());
                self.held_changes.removed.extend(input.removed.clone());
                self.held_changes.updated.extend(input.updated.clone());
            }

            // Return the input but don't add to pending_deltas
            return input;
        }

        // Not pending - check if we were previously holding back
        if self.held_pending {
            self.held_pending = false;

            if !self.subscriber_initialized {
                // First time: emit full current_rows as the initial snapshot
                self.subscriber_initialized = true;

                if !self.current_rows.is_empty() {
                    let snapshot = RowDelta {
                        added: self.current_rows.clone(),
                        removed: vec![],
                        updated: vec![],
                        pending: false,
                    };
                    self.pending_deltas.push(snapshot.clone());
                    return snapshot;
                }
                return input;
            } else {
                // Subsequent pending period: emit only accumulated changes
                // Merge any final non-pending changes into held_changes
                self.held_changes.added.extend(input.added.clone());
                self.held_changes.removed.extend(input.removed.clone());
                self.held_changes.updated.extend(input.updated.clone());

                let result = std::mem::take(&mut self.held_changes);
                if !result.is_empty() {
                    self.pending_deltas.push(result.clone());
                }
                return result;
            }
        }

        // Normal case - not pending, weren't holding back
        self.subscriber_initialized = true;
        // Store delta for delivery
        if !input.is_empty() {
            self.pending_deltas.push(input.clone());
        }

        input
    }

    fn current_result(&self) -> &[Row] {
        &self.current_rows
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
    use crate::query_manager::types::{ColumnDescriptor, ColumnType};

    fn test_descriptor() -> RowDescriptor {
        RowDescriptor::new(vec![
            ColumnDescriptor::new("id", ColumnType::Integer),
            ColumnDescriptor::new("name", ColumnType::Text),
        ])
    }

    fn make_row(id: ObjectId, n: i32, name: &str) -> Row {
        let descriptor = test_descriptor();
        let data = encode_row(&descriptor, &[Value::Integer(n), Value::Text(name.into())]).unwrap();
        Row::new(id, data, CommitId([0; 32]))
    }

    #[test]
    fn output_stores_deltas() {
        let descriptor = test_descriptor();
        let mut node = OutputNode::new(descriptor, OutputMode::Delta);

        let id1 = ObjectId::new();
        let row1 = make_row(id1, 1, "Alice");

        let delta = RowDelta {
            pending: false,
            added: vec![row1],
            removed: vec![],
            updated: vec![],
        };

        node.process(delta);

        let deltas = node.take_deltas();
        assert_eq!(deltas.len(), 1);
        assert_eq!(deltas[0].added.len(), 1);
    }

    #[test]
    fn output_decodes_current() {
        let descriptor = test_descriptor();
        let mut node = OutputNode::new(descriptor, OutputMode::Full);

        let id1 = ObjectId::new();
        let row1 = make_row(id1, 1, "Alice");

        node.process(RowDelta {
            pending: false,
            added: vec![row1],
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
        let row1 = make_row(id1, 1, "Alice");

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

        let delta = RowDelta::new();
        node.process(delta);

        let deltas = node.take_deltas();
        assert!(deltas.is_empty());
    }

    #[test]
    fn output_holds_back_when_pending() {
        let descriptor = test_descriptor();
        let mut node = OutputNode::new(descriptor, OutputMode::Delta);

        let id1 = ObjectId::new();
        let row1 = make_row(id1, 1, "Alice");

        // Process delta with pending=true
        let delta = RowDelta {
            pending: true,
            added: vec![row1.clone()],
            removed: vec![],
            updated: vec![],
        };

        node.process(delta);

        // Internal state should be updated
        assert_eq!(node.current_result().len(), 1);
        assert!(node.is_held_pending());

        // But no deltas should be delivered
        let deltas = node.take_deltas();
        assert!(deltas.is_empty(), "Should hold back deltas when pending");
    }

    #[test]
    fn output_emits_full_state_when_pending_clears() {
        let descriptor = test_descriptor();
        let mut node = OutputNode::new(descriptor, OutputMode::Delta);

        let id1 = ObjectId::new();
        let id2 = ObjectId::new();
        let row1 = make_row(id1, 1, "Alice");
        let row2 = make_row(id2, 2, "Bob");

        // First delta with pending=true
        let delta1 = RowDelta {
            pending: true,
            added: vec![row1.clone()],
            removed: vec![],
            updated: vec![],
        };
        node.process(delta1);
        assert!(node.is_held_pending());
        assert!(node.take_deltas().is_empty());

        // Second delta with pending=true (add another row)
        let delta2 = RowDelta {
            pending: true,
            added: vec![row2.clone()],
            removed: vec![],
            updated: vec![],
        };
        node.process(delta2);
        assert!(node.is_held_pending());
        assert!(node.take_deltas().is_empty());

        // Now pending clears
        let delta3 = RowDelta {
            pending: false,
            added: vec![],
            removed: vec![],
            updated: vec![],
        };
        node.process(delta3);

        // Should no longer be held pending
        assert!(!node.is_held_pending());

        // Should emit full current state as a single delta
        let deltas = node.take_deltas();
        assert_eq!(deltas.len(), 1);
        assert_eq!(deltas[0].added.len(), 2, "Should contain all current rows");
    }

    #[test]
    fn output_normal_behavior_when_not_pending() {
        let descriptor = test_descriptor();
        let mut node = OutputNode::new(descriptor, OutputMode::Delta);

        let id1 = ObjectId::new();
        let id2 = ObjectId::new();
        let row1 = make_row(id1, 1, "Alice");
        let row2 = make_row(id2, 2, "Bob");

        // Normal delta (not pending)
        let delta1 = RowDelta {
            pending: false,
            added: vec![row1.clone()],
            removed: vec![],
            updated: vec![],
        };
        node.process(delta1);

        // Should deliver immediately
        let deltas = node.take_deltas();
        assert_eq!(deltas.len(), 1);
        assert_eq!(deltas[0].added.len(), 1);

        // Second normal delta
        let delta2 = RowDelta {
            pending: false,
            added: vec![row2.clone()],
            removed: vec![],
            updated: vec![],
        };
        node.process(delta2);

        // Should also deliver immediately
        let deltas = node.take_deltas();
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
        let row1 = make_row(id1, 1, "Alice");
        let row2 = make_row(id2, 2, "Bob");
        let row3 = make_row(id3, 3, "Charlie");

        // Step 1: Initial pending period
        let delta1 = RowDelta {
            pending: true,
            added: vec![row1.clone()],
            removed: vec![],
            updated: vec![],
        };
        node.process(delta1);
        assert!(
            node.take_deltas().is_empty(),
            "Should hold back during initial pending"
        );

        // Step 2: Initial pending clears
        let delta2 = RowDelta {
            pending: false,
            added: vec![row2.clone()],
            removed: vec![],
            updated: vec![],
        };
        node.process(delta2);

        // Should emit full snapshot (both rows)
        let deltas = node.take_deltas();
        assert_eq!(deltas.len(), 1);
        assert_eq!(
            deltas[0].added.len(),
            2,
            "Initial clear should emit full snapshot"
        );

        // Step 3: Normal update (non-pending)
        let delta3 = RowDelta {
            pending: false,
            added: vec![row3.clone()],
            removed: vec![],
            updated: vec![],
        };
        node.process(delta3);

        // Should deliver incrementally
        let deltas = node.take_deltas();
        assert_eq!(deltas.len(), 1);
        assert_eq!(
            deltas[0].added.len(),
            1,
            "Normal update should be incremental"
        );
        assert_eq!(deltas[0].added[0].id, id3);

        // Step 4: New pending period starts
        let id4 = ObjectId::new();
        let row4 = make_row(id4, 4, "Dave");
        let delta4 = RowDelta {
            pending: true,
            added: vec![row4.clone()],
            removed: vec![],
            updated: vec![],
        };
        node.process(delta4);
        assert!(
            node.take_deltas().is_empty(),
            "Should hold back during second pending"
        );

        // Step 5: Second pending clears
        let id5 = ObjectId::new();
        let row5 = make_row(id5, 5, "Eve");
        let delta5 = RowDelta {
            pending: false,
            added: vec![row5.clone()],
            removed: vec![],
            updated: vec![],
        };
        node.process(delta5);

        // Should emit ONLY the changes during the pending period (row4, row5)
        // NOT the full snapshot (which would be all 5 rows)
        let deltas = node.take_deltas();
        assert_eq!(deltas.len(), 1);
        assert_eq!(
            deltas[0].added.len(),
            2,
            "Subsequent pending clear should emit only accumulated changes, not full snapshot"
        );

        // Verify we got row4 and row5, not the old rows
        let added_ids: Vec<_> = deltas[0].added.iter().map(|r| r.id).collect();
        assert!(added_ids.contains(&id4), "Should contain row4");
        assert!(added_ids.contains(&id5), "Should contain row5");
        assert!(
            !added_ids.contains(&id1),
            "Should NOT contain row1 (from initial snapshot)"
        );
    }
}
