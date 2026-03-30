//! ArraySubqueryNode for correlated subqueries that produce array columns.
//!
//! This node implements the "dynamic graph instances" approach where each
//! unique outer row gets its own subgraph evaluation. This is intentionally
//! chosen over shared hash indices to explore subgraph patterns and collect
//! learnings for future optimizations.

use ahash::{AHashMap, AHashSet};

use crate::object::ObjectId;
use crate::query_manager::encoding::{decode_row, encode_row};
use crate::query_manager::query::ArraySubqueryRequirement;
use crate::query_manager::types::{
    ColumnDescriptor, ColumnType, LoadedRow, RowDescriptor, Schema, Tuple, TupleDelta,
    TupleDescriptor, TupleElement, TupleProvenance, Value,
};

use crate::storage::Storage;

use super::RowNode;
use super::subgraph::SubgraphTemplate;

/// Node that evaluates a correlated subquery for each outer row,
/// producing an array column with the results.
///
/// ## Architecture
///
/// ```text
/// OuterScan → Materialize → ArraySubqueryNode
///                               ↓
///                     For each outer tuple:
///                       - Bind correlation values
///                       - Evaluate subgraph
///                       - Collect results into array
///                               ↓
///                     outer tuple + array column
/// ```
///
/// ## Learnings to collect (for future sub-graph sharing optimization):
/// - Which parts of SubgraphInstances could be shared (index state? settled results?)
/// - Memory overhead per instance
/// - Update cost distribution (how many instances need re-settling on inner change?)
/// - Common subgraph patterns that could benefit from memoization
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Correlate {
    /// Correlate using a column value from the outer row.
    Col(usize),
    /// Correlate using the outer tuple's object id.
    Id,
}

#[derive(Debug)]
pub struct ArraySubqueryNode {
    /// Descriptor for outer tuples.
    outer_descriptor: TupleDescriptor,
    /// Output descriptor (outer columns + array column).
    output_descriptor: RowDescriptor,
    /// Output tuple descriptor.
    output_tuple_descriptor: TupleDescriptor,

    /// Template for the inner subgraph.
    subgraph_template: SubgraphTemplate,
    /// Schema for compiling subgraphs.
    schema: Schema,

    /// Source of the correlation value from the outer tuple.
    outer_correlation: Correlate,
    /// Requirement for whether the correlated result must exist.
    requirement: ArraySubqueryRequirement,

    /// Per-outer-row state: outer_id → latest outer tuple + evaluated array.
    instances: AHashMap<ObjectId, ArrayInstanceState>,

    /// Current output tuples.
    current_tuples: AHashSet<Tuple>,

    dirty: bool,
    /// True if the inner table changed (need to reevaluate all instances).
    inner_dirty: bool,
}

#[derive(Debug, Clone)]
struct ArrayInstanceState {
    outer_tuple: Tuple,
    correlation_value: Value,
    array_result: Value,
    provenance: TupleProvenance,
}

impl ArraySubqueryNode {
    /// Create a new ArraySubqueryNode.
    ///
    /// # Arguments
    /// * `outer_descriptor` - Descriptor for incoming outer tuples
    /// * `subgraph_template` - Template for creating inner subgraph instances
    /// * `outer_correlation` - Source for correlation value from outer tuple.
    /// * `array_column_name` - Name for the output array column
    /// * `schema` - Schema for compiling subgraphs
    pub fn new(
        outer_descriptor: TupleDescriptor,
        subgraph_template: SubgraphTemplate,
        outer_correlation: Correlate,
        requirement: ArraySubqueryRequirement,
        array_column_name: String,
        schema: Schema,
    ) -> Self {
        // Build output descriptor: outer columns + array column
        let outer_row_descriptor = outer_descriptor.combined_descriptor();
        let mut output_columns = outer_row_descriptor.columns.clone();

        // Array column type: Array<Row> with the subgraph's output columns.
        // The row id is carried in Value::Row { id: Some(...), .. } rather than
        // prepended as a column.
        let row_columns = subgraph_template.output_descriptor().columns.clone();
        let element_type = ColumnType::Array {
            element: Box::new(ColumnType::Row {
                columns: Box::new(RowDescriptor::new(row_columns)),
            }),
        };

        output_columns.push(ColumnDescriptor {
            name: array_column_name.clone().into(),
            column_type: element_type,
            nullable: false,
            references: None,
            default: None,
        });

        let output_descriptor = RowDescriptor::new(output_columns);
        let output_tuple_descriptor =
            TupleDescriptor::single_with_materialization("", output_descriptor.clone(), true);

        Self {
            outer_descriptor,
            output_descriptor,
            output_tuple_descriptor,
            subgraph_template,
            schema,
            outer_correlation,
            requirement,
            instances: AHashMap::new(),
            current_tuples: AHashSet::new(),
            dirty: true,
            inner_dirty: false,
        }
    }

    /// Mark this node as needing re-evaluation due to inner table changes.
    pub fn mark_inner_dirty(&mut self) {
        self.inner_dirty = true;
    }

    /// Check if the inner table changed (need to reevaluate all instances).
    pub fn is_inner_dirty(&self) -> bool {
        self.inner_dirty
    }

    /// Process outer deltas with access to Storage and object manager for subgraph settling.
    pub fn process_with_context<F>(
        &mut self,
        input: TupleDelta,
        io: &dyn Storage,
        mut row_loader: F,
    ) -> TupleDelta
    where
        F: FnMut(ObjectId, Option<&TupleProvenance>) -> Option<LoadedRow>,
    {
        let mut result = TupleDelta::new();

        // Process removed tuples
        for tuple in input.removed {
            if let Some(outer_id) = tuple.first_id() {
                let state = self.instances.remove(&outer_id);
                let old_array = state
                    .as_ref()
                    .map(|state| state.array_result.clone())
                    .unwrap_or_else(|| Value::Array(vec![]));
                let old_provenance = state
                    .as_ref()
                    .map(|state| state.provenance.clone())
                    .unwrap_or_default();
                let old_correlation = state
                    .as_ref()
                    .map(|state| state.correlation_value.clone())
                    .unwrap_or(Value::Null);
                if let Some(old_output) =
                    self.build_output_tuple(&tuple, &old_correlation, &old_array, &old_provenance)
                {
                    self.current_tuples.remove(&old_output);
                    result.removed.push(old_output);
                }
            }
        }

        // Process added tuples
        for tuple in input.added {
            if let Some(outer_id) = tuple.first_id() {
                // Get correlation value from outer tuple
                if let Some(correlation_value) = self.extract_correlation_value(&tuple) {
                    // Evaluate subgraph for this correlation value
                    let (array_result, provenance) =
                        self.evaluate_subgraph(&correlation_value, io, &mut row_loader);

                    // Store instance state
                    self.instances.insert(
                        outer_id,
                        ArrayInstanceState {
                            outer_tuple: tuple.clone(),
                            correlation_value: correlation_value.clone(),
                            array_result: array_result.clone(),
                            provenance: provenance.clone(),
                        },
                    );

                    // Build output tuple with array column
                    if let Some(output_tuple) = self.build_output_tuple(
                        &tuple,
                        &correlation_value,
                        &array_result,
                        &provenance,
                    ) {
                        self.current_tuples.insert(output_tuple.clone());
                        result.added.push(output_tuple);
                    }
                }
            }
        }

        // Process updated tuples
        for (old_tuple, new_tuple) in input.updated {
            let old_outer_id = old_tuple.first_id();
            let new_outer_id = new_tuple.first_id();
            let old_state = old_outer_id.and_then(|outer_id| self.instances.remove(&outer_id));

            let old_array = old_state
                .as_ref()
                .map(|state| state.array_result.clone())
                .unwrap_or_else(|| Value::Array(vec![]));
            let old_provenance = old_state
                .as_ref()
                .map(|state| state.provenance.clone())
                .unwrap_or_default();

            let old_correlation = old_state
                .as_ref()
                .map(|state| state.correlation_value.clone())
                .or_else(|| self.extract_correlation_value(&old_tuple));
            let new_correlation = self.extract_correlation_value(&new_tuple);

            let (new_array, new_provenance) = if old_correlation == new_correlation {
                (
                    old_state
                        .as_ref()
                        .map(|state| state.array_result.clone())
                        .unwrap_or_else(|| Value::Array(vec![])),
                    old_state
                        .as_ref()
                        .map(|state| state.provenance.clone())
                        .unwrap_or_default(),
                )
            } else if let Some(ref new_corr) = new_correlation {
                self.evaluate_subgraph(new_corr, io, &mut row_loader)
            } else {
                (Value::Array(vec![]), TupleProvenance::default())
            };

            if let (Some(outer_id), Some(correlation_value)) =
                (new_outer_id, new_correlation.clone())
            {
                self.instances.insert(
                    outer_id,
                    ArrayInstanceState {
                        outer_tuple: new_tuple.clone(),
                        correlation_value,
                        array_result: new_array.clone(),
                        provenance: new_provenance.clone(),
                    },
                );
            }

            let old_output = old_correlation.as_ref().and_then(|correlation| {
                self.build_output_tuple(&old_tuple, correlation, &old_array, &old_provenance)
            });
            let new_output = new_correlation.as_ref().and_then(|correlation| {
                self.build_output_tuple(&new_tuple, correlation, &new_array, &new_provenance)
            });

            match (old_output, new_output) {
                (Some(old_output), Some(new_output)) => {
                    self.current_tuples.remove(&old_output);
                    self.current_tuples.insert(new_output.clone());
                    result.updated.push((old_output, new_output));
                }
                (Some(old_output), None) => {
                    self.current_tuples.remove(&old_output);
                    result.removed.push(old_output);
                }
                (None, Some(new_output)) => {
                    self.current_tuples.insert(new_output.clone());
                    result.added.push(new_output);
                }
                (None, None) => {}
            }
        }

        self.dirty = false;
        result
    }

    /// Extract correlation value from an outer tuple.
    fn extract_correlation_value(&self, tuple: &Tuple) -> Option<Value> {
        match self.outer_correlation {
            Correlate::Id => tuple.first_id().map(Value::Uuid),
            Correlate::Col(col_idx) => {
                let element = tuple.get(0)?;
                let content = element.content()?;
                let outer_row_desc = self.outer_descriptor.combined_descriptor();
                let values = decode_row(&outer_row_desc, content).ok()?;
                values.get(col_idx).cloned()
            }
        }
    }

    /// Evaluate the subgraph for a given correlation value.
    /// Uses trait object to avoid recursion limit with nested generics.
    fn evaluate_subgraph(
        &self,
        correlation_value: &Value,
        io: &dyn Storage,
        row_loader: &mut dyn FnMut(ObjectId, Option<&TupleProvenance>) -> Option<LoadedRow>,
    ) -> (Value, TupleProvenance) {
        // UUID[] FK forward includes correlate an array of ids to scalar inner ids.
        // Evaluate each element independently so output preserves source order/duplicates.
        if let Value::Array(elements) = correlation_value {
            let mut materialized = Vec::new();
            let mut provenance = TupleProvenance::default();
            for element in elements {
                let (nested_value, nested_provenance) =
                    self.evaluate_subgraph_for_single(element, io, row_loader);
                let Value::Array(mut nested) = nested_value else {
                    continue;
                };
                materialized.append(&mut nested);
                provenance.extend(nested_provenance);
            }
            return (Value::Array(materialized), provenance);
        }

        self.evaluate_subgraph_for_single(correlation_value, io, row_loader)
    }

    fn evaluate_subgraph_for_single(
        &self,
        correlation_value: &Value,
        io: &dyn Storage,
        row_loader: &mut dyn FnMut(ObjectId, Option<&TupleProvenance>) -> Option<LoadedRow>,
    ) -> (Value, TupleProvenance) {
        let instance =
            self.subgraph_template
                .instantiate(correlation_value.clone(), &self.schema, Some(io));
        let mut instance = match instance {
            Some(i) => i,
            None => return (Value::Array(vec![]), TupleProvenance::default()),
        };

        let _row_delta = instance.graph.settle(io, row_loader);
        let mut provenance = TupleProvenance::default();
        let array_elements: Vec<Value> = instance
            .graph
            .current_output_rows_with_provenance()
            .iter()
            .filter_map(|(row, row_provenance)| {
                let output_desc = self.subgraph_template.output_descriptor();
                let values = decode_row(output_desc, &row.data).ok()?;
                provenance.extend(row_provenance.iter().copied());
                Some(Value::Row {
                    id: Some(row.id),
                    values,
                })
            })
            .collect();
        (Value::Array(array_elements), provenance)
    }

    /// Build output tuple from outer tuple + array result.
    fn build_output_tuple(
        &self,
        outer_tuple: &Tuple,
        correlation_value: &Value,
        array_result: &Value,
        inner_provenance: &TupleProvenance,
    ) -> Option<Tuple> {
        if !self.requirement_satisfied(correlation_value, array_result) {
            return None;
        }

        let element = outer_tuple.get(0)?;
        let outer_id = element.id();
        let outer_content = element.content()?;
        let commit_id = element.commit_id()?;

        // Decode outer values
        let outer_row_desc = self.outer_descriptor.combined_descriptor();
        let mut values = decode_row(&outer_row_desc, outer_content).ok()?;

        // Append array column
        values.push(array_result.clone());

        // Encode output
        let output_content = encode_row(&self.output_descriptor, &values).ok()?;

        let mut provenance = outer_tuple.provenance().clone();
        provenance.extend(inner_provenance.iter().copied());

        Some(Tuple::new_with_provenance(
            vec![TupleElement::Row {
                id: outer_id,
                content: output_content,
                commit_id,
            }],
            provenance,
        ))
    }

    fn requirement_satisfied(&self, correlation_value: &Value, array_result: &Value) -> bool {
        let Value::Array(rows) = array_result else {
            return self.requirement == ArraySubqueryRequirement::Optional;
        };

        match self.requirement {
            ArraySubqueryRequirement::Optional => true,
            ArraySubqueryRequirement::AtLeastOne => !rows.is_empty(),
            ArraySubqueryRequirement::MatchCorrelationCardinality => match correlation_value {
                Value::Array(elements) => rows.len() == elements.len(),
                Value::Null => false,
                _ => rows.len() == 1,
            },
        }
    }

    /// Re-evaluate all instances when inner data changes.
    /// Returns deltas for any arrays that changed.
    pub fn reevaluate_all<F>(&mut self, io: &dyn Storage, row_loader: &mut F) -> TupleDelta
    where
        F: FnMut(ObjectId, Option<&TupleProvenance>) -> Option<LoadedRow>,
    {
        let mut result = TupleDelta::new();

        // Clear inner_dirty flag
        self.inner_dirty = false;

        // Collect state snapshots to avoid borrow issues during re-evaluation.
        let instances_snapshot: Vec<(ObjectId, ArrayInstanceState)> = self
            .instances
            .iter()
            .map(|(id, state)| (*id, state.clone()))
            .collect();

        for (outer_id, old_state) in instances_snapshot {
            // Re-evaluate subgraph
            let (new_array, new_provenance) =
                self.evaluate_subgraph(&old_state.correlation_value, io, row_loader);

            if old_state.array_result != new_array || old_state.provenance != new_provenance {
                let old_tuple = self.build_output_tuple(
                    &old_state.outer_tuple,
                    &old_state.correlation_value,
                    &old_state.array_result,
                    &old_state.provenance,
                );
                let new_tuple = self.build_output_tuple(
                    &old_state.outer_tuple,
                    &old_state.correlation_value,
                    &new_array,
                    &new_provenance,
                );

                match (old_tuple, new_tuple) {
                    (Some(old_tuple), Some(new_tuple)) => {
                        result.updated.push((old_tuple.clone(), new_tuple.clone()));
                        self.current_tuples.remove(&old_tuple);
                        self.current_tuples.insert(new_tuple);
                    }
                    (Some(old_tuple), None) => {
                        self.current_tuples.remove(&old_tuple);
                        result.removed.push(old_tuple);
                    }
                    (None, Some(new_tuple)) => {
                        self.current_tuples.insert(new_tuple.clone());
                        result.added.push(new_tuple);
                    }
                    (None, None) => {}
                }

                self.instances.insert(
                    outer_id,
                    ArrayInstanceState {
                        outer_tuple: old_state.outer_tuple.clone(),
                        correlation_value: old_state.correlation_value.clone(),
                        array_result: new_array,
                        provenance: new_provenance,
                    },
                );
            }
        }

        result
    }

    /// Get the output tuple descriptor.
    pub fn output_tuple_descriptor(&self) -> &TupleDescriptor {
        &self.output_tuple_descriptor
    }
}

impl RowNode for ArraySubqueryNode {
    fn output_descriptor(&self) -> &RowDescriptor {
        &self.output_descriptor
    }

    fn process(&mut self, input: TupleDelta) -> TupleDelta {
        // This is a simplified process that doesn't have access to io/om.
        // Real processing should use process_with_context.
        // For now, just pass through with empty arrays.
        let mut result = TupleDelta::new();

        for tuple in input.removed {
            if let Some(outer_id) = tuple.first_id() {
                self.instances.remove(&outer_id);
            }
            let correlation_value = self
                .extract_correlation_value(&tuple)
                .unwrap_or(Value::Null);
            if let Some(output) = self.build_output_tuple(
                &tuple,
                &correlation_value,
                &Value::Array(vec![]),
                &TupleProvenance::default(),
            ) {
                self.current_tuples.remove(&output);
                result.removed.push(output);
            }
        }

        for tuple in input.added {
            if let (Some(outer_id), Some(correlation_value)) =
                (tuple.first_id(), self.extract_correlation_value(&tuple))
            {
                // Without context, we can't evaluate - store empty array
                self.instances.insert(
                    outer_id,
                    ArrayInstanceState {
                        outer_tuple: tuple.clone(),
                        correlation_value,
                        array_result: Value::Array(vec![]),
                        provenance: TupleProvenance::default(),
                    },
                );
            }
            let correlation_value = self
                .extract_correlation_value(&tuple)
                .unwrap_or(Value::Null);
            if let Some(output) = self.build_output_tuple(
                &tuple,
                &correlation_value,
                &Value::Array(vec![]),
                &TupleProvenance::default(),
            ) {
                self.current_tuples.insert(output.clone());
                result.added.push(output);
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
    use crate::query_manager::graph_nodes::subgraph::SubgraphBuilder;
    use crate::query_manager::types::TableName;

    fn test_schema() -> Schema {
        let mut schema = Schema::new();
        schema.insert(
            TableName::new("users"),
            RowDescriptor::new(vec![
                ColumnDescriptor::new("id", ColumnType::Integer),
                ColumnDescriptor::new("name", ColumnType::Text),
            ])
            .into(),
        );
        schema.insert(
            TableName::new("posts"),
            RowDescriptor::new(vec![
                ColumnDescriptor::new("id", ColumnType::Integer),
                ColumnDescriptor::new("title", ColumnType::Text),
                ColumnDescriptor::new("author_id", ColumnType::Integer),
            ])
            .into(),
        );
        schema
    }

    #[test]
    fn array_subquery_node_creates_output_descriptor() {
        let schema = test_schema();

        let outer_descriptor = TupleDescriptor::single_with_materialization(
            "users",
            schema
                .get(&TableName::new("users"))
                .unwrap()
                .columns
                .clone(),
            true,
        );

        let template = SubgraphBuilder::new("posts")
            .correlate("author_id")
            .select(&["id", "title"])
            .build(&schema)
            .unwrap();

        let node = ArraySubqueryNode::new(
            outer_descriptor,
            template,
            Correlate::Col(0),
            ArraySubqueryRequirement::Optional,
            "posts".to_string(),
            schema,
        );

        // Output should have: id, name, posts (array)
        assert_eq!(node.output_descriptor().columns.len(), 3);
        assert_eq!(node.output_descriptor().columns[0].name, "id");
        assert_eq!(node.output_descriptor().columns[1].name, "name");
        assert_eq!(node.output_descriptor().columns[2].name, "posts");
    }

    #[test]
    fn array_subquery_extracts_correlation_value() {
        let schema = test_schema();

        let outer_descriptor = TupleDescriptor::single_with_materialization(
            "users",
            schema
                .get(&TableName::new("users"))
                .unwrap()
                .columns
                .clone(),
            true,
        );

        let template = SubgraphBuilder::new("posts")
            .correlate("author_id")
            .build(&schema)
            .unwrap();

        let node = ArraySubqueryNode::new(
            outer_descriptor,
            template,
            Correlate::Col(0),
            ArraySubqueryRequirement::Optional,
            "posts".to_string(),
            schema.clone(),
        );

        // Create a tuple with user id=42
        let user_values = vec![Value::Integer(42), Value::Text("Alice".into())];
        let user_row_desc = &schema.get(&TableName::new("users")).unwrap().columns;
        let user_data = encode_row(user_row_desc, &user_values).unwrap();
        let user_tuple = Tuple::new(vec![TupleElement::Row {
            id: ObjectId::new(),
            content: user_data,
            commit_id: CommitId([0; 32]),
        }]);

        let correlation = node.extract_correlation_value(&user_tuple);
        assert_eq!(correlation, Some(Value::Integer(42)));
    }

    #[test]
    fn array_subquery_extracts_object_id_correlation_value() {
        let schema = test_schema();

        let outer_descriptor = TupleDescriptor::single_with_materialization(
            "users",
            schema
                .get(&TableName::new("users"))
                .unwrap()
                .columns
                .clone(),
            true,
        );

        let template = SubgraphBuilder::new("posts")
            .correlate("author_id")
            .build(&schema)
            .unwrap();

        let node = ArraySubqueryNode::new(
            outer_descriptor,
            template,
            Correlate::Id,
            ArraySubqueryRequirement::Optional,
            "posts".to_string(),
            schema.clone(),
        );

        let row_id = ObjectId::new();
        let user_values = vec![Value::Integer(42), Value::Text("Alice".into())];
        let user_row_desc = &schema.get(&TableName::new("users")).unwrap().columns;
        let user_data = encode_row(user_row_desc, &user_values).unwrap();
        let user_tuple = Tuple::new(vec![TupleElement::Row {
            id: row_id,
            content: user_data,
            commit_id: CommitId([0; 32]),
        }]);

        let correlation = node.extract_correlation_value(&user_tuple);
        assert_eq!(correlation, Some(Value::Uuid(row_id)));
    }
}
