use ahash::AHashSet;

use crate::query_manager::encoding::project_row;
use crate::query_manager::types::{
    ColumnDescriptor, RowDescriptor, Tuple, TupleDelta, TupleDescriptor, TupleElement,
};

use super::RowNode;

/// Project node for column selection.
///
/// Transforms tuples by selecting a subset of columns from the input rows.
/// Requires materialized tuples (needs to read and re-encode row data).
///
/// Example: `SELECT name, age FROM users` would project columns [1, 2] from
/// a table with columns [id, name, age, email].
#[derive(Debug)]
pub struct ProjectNode {
    /// Input row descriptor.
    input_descriptor: RowDescriptor,
    /// Output row descriptor (selected columns only).
    output_descriptor: RowDescriptor,
    /// Output tuple descriptor.
    output_tuple_descriptor: TupleDescriptor,
    /// Mapping from input column index to output column index.
    /// Vec of (src_col_idx, dst_col_idx) pairs.
    column_mapping: Vec<(usize, usize)>,
    /// Current projected tuples.
    current_tuples: AHashSet<Tuple>,
    dirty: bool,
}

impl ProjectNode {
    /// Create a new project node.
    ///
    /// # Arguments
    /// * `input_descriptor` - The input row descriptor
    /// * `select_columns` - Column names to select (in output order)
    pub fn new(input_descriptor: RowDescriptor, select_columns: &[&str]) -> Self {
        // Build output descriptor and column mapping
        let mut output_columns = Vec::new();
        let mut column_mapping = Vec::new();

        for (dst_idx, col_name) in select_columns.iter().enumerate() {
            if let Some(src_idx) = input_descriptor.column_index(col_name) {
                let col = &input_descriptor.columns[src_idx];
                output_columns.push(ColumnDescriptor {
                    name: col.name,
                    column_type: col.column_type.clone(),
                    nullable: col.nullable,
                    references: col.references,
                });
                column_mapping.push((src_idx, dst_idx));
            }
        }

        let output_descriptor = RowDescriptor::new(output_columns.clone());
        let output_tuple_descriptor = TupleDescriptor::single_with_materialization(
            "",
            RowDescriptor::new(output_columns),
            true,
        );

        Self {
            input_descriptor,
            output_descriptor,
            output_tuple_descriptor,
            column_mapping,
            current_tuples: AHashSet::new(),
            dirty: true,
        }
    }

    /// Create a new project node with TupleDescriptor.
    pub fn with_tuple_descriptor(
        input_tuple_descriptor: TupleDescriptor,
        select_columns: &[&str],
    ) -> Self {
        let input_descriptor = input_tuple_descriptor.combined_descriptor();
        let mut output_columns = Vec::new();
        let mut column_mapping = Vec::new();

        for (dst_idx, col_name) in select_columns.iter().enumerate() {
            if let Some(src_idx) = input_descriptor.column_index(col_name) {
                let col = &input_descriptor.columns[src_idx];
                output_columns.push(ColumnDescriptor {
                    name: col.name,
                    column_type: col.column_type.clone(),
                    nullable: col.nullable,
                    references: col.references,
                });
                column_mapping.push((src_idx, dst_idx));
            }
        }

        let output_descriptor = RowDescriptor::new(output_columns.clone());
        let output_tuple_descriptor = TupleDescriptor::single_with_materialization(
            "",
            RowDescriptor::new(output_columns),
            true,
        );

        Self {
            input_descriptor,
            output_descriptor,
            output_tuple_descriptor,
            column_mapping,
            current_tuples: AHashSet::new(),
            dirty: true,
        }
    }

    /// Get the output tuple descriptor.
    pub fn output_tuple_descriptor(&self) -> &TupleDescriptor {
        &self.output_tuple_descriptor
    }

    /// Project a single tuple to selected columns.
    fn project_tuple(&self, tuple: &Tuple) -> Option<Tuple> {
        let mut projected_elements = Vec::with_capacity(tuple.len());

        for element in tuple.iter() {
            match element {
                TupleElement::Id(id) => {
                    // Can't project unmaterialized tuple
                    projected_elements.push(TupleElement::Id(*id));
                }
                TupleElement::Row {
                    id,
                    content,
                    commit_id,
                } => {
                    // Project the row data
                    match project_row(
                        &self.input_descriptor,
                        content,
                        &self.output_descriptor,
                        &self.column_mapping,
                    ) {
                        Ok(projected_content) => {
                            projected_elements.push(TupleElement::Row {
                                id: *id,
                                content: projected_content,
                                commit_id: *commit_id,
                            });
                        }
                        Err(_) => {
                            // Projection failed, pass through as ID
                            projected_elements.push(TupleElement::Id(*id));
                        }
                    }
                }
            }
        }

        Some(Tuple::new(projected_elements))
    }
}

impl RowNode for ProjectNode {
    fn output_descriptor(&self) -> &RowDescriptor {
        &self.output_descriptor
    }

    fn process(&mut self, input: TupleDelta) -> TupleDelta {
        let mut result = TupleDelta::new();

        // Project removed tuples
        for tuple in input.removed {
            if let Some(projected) = self.project_tuple(&tuple) {
                self.current_tuples.remove(&projected);
                result.removed.push(projected);
            }
        }

        // Project added tuples
        for tuple in input.added {
            if let Some(projected) = self.project_tuple(&tuple) {
                self.current_tuples.insert(projected.clone());
                result.added.push(projected);
            }
        }

        // Project updated tuples
        for (old_tuple, new_tuple) in input.updated {
            if let (Some(old_projected), Some(new_projected)) = (
                self.project_tuple(&old_tuple),
                self.project_tuple(&new_tuple),
            ) {
                self.current_tuples.remove(&old_projected);
                self.current_tuples.insert(new_projected.clone());
                result.updated.push((old_projected, new_projected));
            }
        }

        self.dirty = false;
        result
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
    use crate::query_manager::encoding::{decode_row, encode_row};
    use crate::query_manager::types::{ColumnType, Value};

    fn test_descriptor() -> RowDescriptor {
        RowDescriptor::new(vec![
            ColumnDescriptor::new("id", ColumnType::Integer),
            ColumnDescriptor::new("name", ColumnType::Text),
            ColumnDescriptor::new("email", ColumnType::Text),
            ColumnDescriptor::new("age", ColumnType::Integer),
        ])
    }

    fn make_tuple(id: ObjectId, values: &[Value]) -> Tuple {
        let descriptor = test_descriptor();
        let data = encode_row(&descriptor, values).unwrap();
        Tuple::new(vec![TupleElement::Row {
            id,
            content: data,
            commit_id: CommitId([0; 32]),
        }])
    }

    #[test]
    fn project_selects_columns() {
        let descriptor = test_descriptor();
        let mut node = ProjectNode::new(descriptor, &["name", "age"]);

        let id1 = ObjectId::new();
        let tuple1 = make_tuple(
            id1,
            &[
                Value::Integer(1),
                Value::Text("Alice".into()),
                Value::Text("alice@example.com".into()),
                Value::Integer(30),
            ],
        );

        let delta = TupleDelta {
            added: vec![tuple1],
            removed: vec![],
            updated: vec![],
        };

        let result = node.process(delta);

        assert_eq!(result.added.len(), 1);

        // Verify projected tuple has only selected columns
        let projected = &result.added[0];
        let row = projected.to_single_row().unwrap();
        let values = decode_row(&node.output_descriptor, &row.data).unwrap();

        assert_eq!(values.len(), 2);
        assert_eq!(values[0], Value::Text("Alice".into()));
        assert_eq!(values[1], Value::Integer(30));
    }

    #[test]
    fn project_output_descriptor_has_selected_columns() {
        let descriptor = test_descriptor();
        let node = ProjectNode::new(descriptor, &["name", "age"]);

        let output = node.output_descriptor();
        assert_eq!(output.columns.len(), 2);
        assert_eq!(output.columns[0].name, "name");
        assert_eq!(output.columns[1].name, "age");
    }

    #[test]
    fn project_preserves_tuple_identity() {
        let descriptor = test_descriptor();
        let mut node = ProjectNode::new(descriptor, &["name"]);

        let id1 = ObjectId::new();
        let tuple1 = make_tuple(
            id1,
            &[
                Value::Integer(1),
                Value::Text("Alice".into()),
                Value::Text("alice@example.com".into()),
                Value::Integer(30),
            ],
        );

        let delta = TupleDelta {
            added: vec![tuple1],
            removed: vec![],
            updated: vec![],
        };

        node.process(delta);

        // Projected tuple should have same ID
        assert_eq!(node.current_tuples().len(), 1);
        let projected = node.current_tuples().iter().next().unwrap();
        assert_eq!(projected.ids()[0], id1);
    }

    #[test]
    fn project_handles_updates() {
        let descriptor = test_descriptor();
        let mut node = ProjectNode::new(descriptor, &["name", "age"]);

        let id1 = ObjectId::new();
        let old_tuple = make_tuple(
            id1,
            &[
                Value::Integer(1),
                Value::Text("Alice".into()),
                Value::Text("alice@example.com".into()),
                Value::Integer(30),
            ],
        );
        let new_tuple = make_tuple(
            id1,
            &[
                Value::Integer(1),
                Value::Text("Alice".into()),
                Value::Text("alice@newmail.com".into()),
                Value::Integer(31),
            ],
        );

        // Add old tuple
        node.process(TupleDelta {
            added: vec![old_tuple.clone()],
            removed: vec![],
            updated: vec![],
        });

        // Update tuple
        let result = node.process(TupleDelta {
            added: vec![],
            removed: vec![],
            updated: vec![(old_tuple, new_tuple)],
        });

        assert_eq!(result.updated.len(), 1);

        // Verify age changed in projected output
        let (_, new_projected) = &result.updated[0];
        let row = new_projected.to_single_row().unwrap();
        let values = decode_row(&node.output_descriptor(), &row.data).unwrap();
        assert_eq!(values[1], Value::Integer(31)); // age updated
    }

    #[test]
    fn project_ignores_unknown_columns() {
        let descriptor = test_descriptor();
        let node = ProjectNode::new(descriptor, &["name", "nonexistent", "age"]);

        // Only known columns should be in output
        let output = node.output_descriptor();
        assert_eq!(output.columns.len(), 2);
        assert_eq!(output.columns[0].name, "name");
        assert_eq!(output.columns[1].name, "age");
    }
}
