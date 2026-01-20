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
    dirty: bool,
}

impl OutputNode {
    pub fn new(descriptor: RowDescriptor, mode: OutputMode) -> Self {
        Self {
            descriptor,
            mode,
            current_rows: Vec::new(),
            pending_deltas: Vec::new(),
            dirty: true,
        }
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
        // Apply changes to current_rows
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

        // Store delta for delivery
        if !input.is_empty() {
            self.pending_deltas.push(input.clone());
        }

        self.dirty = false;
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
}
