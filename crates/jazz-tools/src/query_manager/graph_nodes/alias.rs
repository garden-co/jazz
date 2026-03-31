use ahash::AHashSet;

use crate::query_manager::types::{
    CombinedRowDescriptor, RowDescriptor, Tuple, TupleDelta, TupleDescriptor,
};

use super::RowNode;

/// Alias node for table aliasing in joins.
///
/// This node transforms the table namespace without modifying row data.
/// Used for self-joins where the same table appears multiple times with different aliases.
///
/// Example: `FROM users AS u1 JOIN users AS u2`
/// - First AliasNode transforms "users" → "u1"
/// - Second AliasNode transforms "users" → "u2"
#[derive(Debug)]
pub struct AliasNode {
    /// Original table name.
    original_table: String,
    /// Alias name for the table.
    alias: String,
    /// Original row descriptor (column definitions).
    row_descriptor: RowDescriptor,
    /// Output tuple descriptor with alias applied.
    output_tuple_descriptor: TupleDescriptor,
    /// Combined descriptor with alias applied.
    combined_descriptor: CombinedRowDescriptor,
    /// Current tuples (pass-through, unchanged).
    current_tuples: AHashSet<Tuple>,
    dirty: bool,
}

impl AliasNode {
    pub fn new(original_table: String, alias: String, row_descriptor: RowDescriptor) -> Self {
        // Create combined descriptor using the alias as the table name
        let combined_descriptor = CombinedRowDescriptor::single(&alias, row_descriptor.clone());
        // Create tuple descriptor with alias as table name
        let output_tuple_descriptor =
            TupleDescriptor::single_with_materialization(&alias, row_descriptor.clone(), true);

        Self {
            original_table,
            alias,
            row_descriptor,
            output_tuple_descriptor,
            combined_descriptor,
            current_tuples: AHashSet::new(),
            dirty: true,
        }
    }

    /// Create an AliasNode from an input TupleDescriptor.
    pub fn with_tuple_descriptor(
        original_table: String,
        alias: String,
        input_tuple_descriptor: TupleDescriptor,
    ) -> Self {
        let row_descriptor = input_tuple_descriptor.combined_descriptor();
        let combined_descriptor = CombinedRowDescriptor::single(&alias, row_descriptor.clone());
        // Output tuple descriptor uses alias, preserves materialization state
        let output_tuple_descriptor = TupleDescriptor::single_with_materialization(
            &alias,
            row_descriptor.clone(),
            input_tuple_descriptor
                .materialization()
                .is_fully_materialized(),
        );

        Self {
            original_table,
            alias,
            row_descriptor,
            output_tuple_descriptor,
            combined_descriptor,
            current_tuples: AHashSet::new(),
            dirty: true,
        }
    }

    /// Get the output tuple descriptor.
    pub fn output_tuple_descriptor(&self) -> &TupleDescriptor {
        &self.output_tuple_descriptor
    }

    /// Get the alias name.
    pub fn alias(&self) -> &str {
        &self.alias
    }

    /// Get the original table name.
    pub fn original_table(&self) -> &str {
        &self.original_table
    }

    /// Get the combined descriptor with alias applied.
    pub fn combined_descriptor(&self) -> &CombinedRowDescriptor {
        &self.combined_descriptor
    }
}

impl RowNode for AliasNode {
    fn output_descriptor(&self) -> &RowDescriptor {
        &self.row_descriptor
    }

    fn process(&mut self, input: TupleDelta) -> TupleDelta {
        // Pure pass-through - alias only affects namespace, not data
        for tuple in &input.removed {
            self.current_tuples.remove(tuple);
        }

        for tuple in &input.added {
            self.current_tuples.insert(tuple.clone());
        }

        for (old_tuple, new_tuple) in &input.updated {
            self.current_tuples.remove(old_tuple);
            self.current_tuples.insert(new_tuple.clone());
        }

        self.dirty = false;
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
    use crate::query_manager::types::{ColumnDescriptor, ColumnType, TupleElement, Value};

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
            row_provenance: crate::metadata::RowProvenance::for_insert("jazz:test", 0),
        }])
    }

    #[test]
    fn alias_passes_through_tuples() {
        let descriptor = test_descriptor();
        let mut node = AliasNode::new("users".to_string(), "u1".to_string(), descriptor);

        let id1 = ObjectId::new();
        let tuple1 = make_tuple(id1, 1, "Alice");

        let delta = TupleDelta {
            added: vec![tuple1.clone()],
            removed: vec![],
            moved: vec![],
            updated: vec![],
        };

        let result = node.process(delta);

        // Tuples pass through unchanged
        assert_eq!(result.added.len(), 1);
        assert_eq!(result.added[0], tuple1);
        assert_eq!(node.current_tuples().len(), 1);
    }

    #[test]
    fn alias_transforms_namespace() {
        let descriptor = test_descriptor();
        let node = AliasNode::new("users".to_string(), "u1".to_string(), descriptor);

        // Combined descriptor uses alias as table name
        let combined = node.combined_descriptor();
        assert_eq!(combined.table_count(), 1);

        // Column lookups work with alias
        assert_eq!(combined.resolve_column("u1", "id"), Some((0, 0)));
        assert_eq!(combined.resolve_column("u1", "name"), Some((0, 1)));

        // Original table name doesn't resolve
        assert_eq!(combined.resolve_column("users", "id"), None);
    }

    #[test]
    fn alias_tracks_removals() {
        let descriptor = test_descriptor();
        let mut node = AliasNode::new("users".to_string(), "u1".to_string(), descriptor);

        let id1 = ObjectId::new();
        let tuple1 = make_tuple(id1, 1, "Alice");

        // Add tuple
        node.process(TupleDelta {
            added: vec![tuple1.clone()],
            removed: vec![],
            moved: vec![],
            updated: vec![],
        });
        assert_eq!(node.current_tuples().len(), 1);

        // Remove tuple
        node.process(TupleDelta {
            added: vec![],
            removed: vec![tuple1],
            moved: vec![],
            updated: vec![],
        });
        assert_eq!(node.current_tuples().len(), 0);
    }
}
