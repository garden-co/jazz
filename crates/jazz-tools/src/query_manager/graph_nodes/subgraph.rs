//! Subgraph template and instance management for correlated subqueries.
//!
//! A SubgraphTemplate represents a parameterized query that can be instantiated
//! multiple times with different parameter bindings. Each instantiation creates
//! a SubgraphInstance with its own state.

use crate::query_manager::graph::QueryGraph;
use crate::query_manager::query::{Query, QueryBuilder};
use crate::query_manager::types::{RowDescriptor, Schema, Value};

/// Template for creating subgraph instances.
///
/// Holds a query definition with correlation parameters that get bound
/// when creating instances. Currently uses the "recompile per binding" approach
/// for simplicity - each instance gets a fresh query graph compiled with the
/// bound parameter values.
#[derive(Debug, Clone)]
pub struct SubgraphTemplate {
    /// Base query for the subgraph (without correlation filters applied).
    base_query: Query,
    /// Column in the inner table to correlate on.
    inner_column: String,
    /// Columns to select from inner query results.
    select_columns: Vec<String>,
    /// Output descriptor for individual result rows.
    output_descriptor: RowDescriptor,
}

impl SubgraphTemplate {
    /// Create a new subgraph template.
    ///
    /// # Arguments
    /// * `base_query` - The inner query definition
    /// * `inner_column` - Column in the inner table to match against outer value
    /// * `select_columns` - Columns to include in results (empty = all)
    /// * `output_descriptor` - Descriptor for result rows
    pub fn new(
        base_query: Query,
        inner_column: String,
        select_columns: Vec<String>,
        output_descriptor: RowDescriptor,
    ) -> Self {
        Self {
            base_query,
            inner_column,
            select_columns,
            output_descriptor,
        }
    }

    /// Create a subgraph instance with a bound correlation value.
    ///
    /// This compiles a fresh query graph with the correlation value as an
    /// equality filter on the inner column.
    pub fn instantiate(
        &self,
        correlation_value: Value,
        schema: &Schema,
    ) -> Option<SubgraphInstance> {
        // Build query with correlation filter
        let mut query_builder = QueryBuilder::new(self.base_query.table);

        // Add joins from base query
        for join_spec in &self.base_query.joins {
            query_builder = query_builder.join(join_spec.table);
            if let Some(ref alias) = join_spec.alias {
                query_builder = query_builder.alias(alias);
            }
            if let Some((ref left, ref right)) = join_spec.on {
                query_builder = query_builder.on(left, right);
            }
        }

        // Add correlation filter: inner_column = correlation_value
        query_builder = query_builder.filter_eq(&self.inner_column, correlation_value.clone());

        // Apply original filters from base query
        for disjunct in &self.base_query.disjuncts {
            for condition in &disjunct.conditions {
                query_builder = match condition {
                    crate::query_manager::query::Condition::Eq { column, value } => {
                        query_builder.filter_eq(column, value.clone())
                    }
                    crate::query_manager::query::Condition::Ne { column, value } => {
                        query_builder.filter_ne(column, value.clone())
                    }
                    crate::query_manager::query::Condition::Lt { column, value } => {
                        query_builder.filter_lt(column, value.clone())
                    }
                    crate::query_manager::query::Condition::Le { column, value } => {
                        query_builder.filter_le(column, value.clone())
                    }
                    crate::query_manager::query::Condition::Gt { column, value } => {
                        query_builder.filter_gt(column, value.clone())
                    }
                    crate::query_manager::query::Condition::Ge { column, value } => {
                        query_builder.filter_ge(column, value.clone())
                    }
                    crate::query_manager::query::Condition::Between { column, min, max } => {
                        query_builder.filter_between(column, min.clone(), max.clone())
                    }
                    crate::query_manager::query::Condition::Contains { column, value } => {
                        query_builder.filter_contains(column, value.clone())
                    }
                    crate::query_manager::query::Condition::IsNull { column } => {
                        query_builder.filter_is_null(column)
                    }
                    crate::query_manager::query::Condition::IsNotNull { column } => {
                        query_builder.filter_is_not_null(column)
                    }
                };
            }
        }

        // Apply order by
        for (col, dir) in &self.base_query.order_by {
            query_builder = match dir {
                crate::query_manager::graph_nodes::sort::SortDirection::Ascending => {
                    query_builder.order_by(col)
                }
                crate::query_manager::graph_nodes::sort::SortDirection::Descending => {
                    query_builder.order_by_desc(col)
                }
            };
        }

        // Apply limit/offset
        if let Some(limit) = self.base_query.limit {
            query_builder = query_builder.limit(limit);
        }
        if self.base_query.offset > 0 {
            query_builder = query_builder.offset(self.base_query.offset);
        }

        // Apply select columns
        if !self.select_columns.is_empty() {
            let cols: Vec<&str> = self.select_columns.iter().map(|s| s.as_str()).collect();
            query_builder = query_builder.select(&cols);
        }

        let mut query = query_builder.build();

        // Copy branches from base query (important for schema-aware branch names)
        query.branches = self.base_query.branches.clone();

        // Copy nested array subqueries from base query
        query.array_subqueries = self.base_query.array_subqueries.clone();
        query.result_element_index = self.base_query.result_element_index;
        query.refresh_relation_ir().ok()?;

        let graph = QueryGraph::compile(&query, schema)?;

        Some(SubgraphInstance {
            graph,
            correlation_value,
            current_results: Vec::new(),
        })
    }

    /// Get the inner table name.
    pub fn table(&self) -> &str {
        &self.base_query.table.0
    }

    /// Get the inner correlation column.
    pub fn inner_column(&self) -> &str {
        &self.inner_column
    }

    /// Get the output descriptor for result rows.
    pub fn output_descriptor(&self) -> &RowDescriptor {
        &self.output_descriptor
    }
}

/// A live instance of a subgraph for one outer row.
///
/// Contains the compiled query graph with bound parameters and tracks
/// the current array result.
#[derive(Debug)]
pub struct SubgraphInstance {
    /// The instantiated query graph with bound correlation value.
    pub graph: QueryGraph,
    /// The correlation value this instance is bound to.
    pub correlation_value: Value,
    /// Current array result (values from settling the graph).
    pub current_results: Vec<Value>,
}

impl SubgraphInstance {
    /// Get the current results as an array Value.
    pub fn as_array(&self) -> Value {
        Value::Array(self.current_results.clone())
    }
}

/// Builder for creating SubgraphTemplates.
#[derive(Debug)]
pub struct SubgraphBuilder {
    table: String,
    inner_column: String,
    select_columns: Vec<String>,
    filters: Vec<(String, Value)>, // Simple equality filters for now
    order_by: Vec<(String, bool)>, // (column, is_descending)
    limit: Option<usize>,
}

impl SubgraphBuilder {
    /// Create a new subgraph builder for the given table.
    pub fn new(table: impl Into<String>) -> Self {
        Self {
            table: table.into(),
            inner_column: String::new(),
            select_columns: Vec::new(),
            filters: Vec::new(),
            order_by: Vec::new(),
            limit: None,
        }
    }

    /// Set the correlation column (inner table column to match against outer value).
    pub fn correlate(mut self, inner_column: impl Into<String>) -> Self {
        self.inner_column = inner_column.into();
        self
    }

    /// Select specific columns.
    pub fn select(mut self, columns: &[&str]) -> Self {
        self.select_columns = columns.iter().map(|s| s.to_string()).collect();
        self
    }

    /// Add an equality filter.
    pub fn filter_eq(mut self, column: impl Into<String>, value: Value) -> Self {
        self.filters.push((column.into(), value));
        self
    }

    /// Add ascending order by.
    pub fn order_by(mut self, column: impl Into<String>) -> Self {
        self.order_by.push((column.into(), false));
        self
    }

    /// Add descending order by.
    pub fn order_by_desc(mut self, column: impl Into<String>) -> Self {
        self.order_by.push((column.into(), true));
        self
    }

    /// Set a limit on results.
    pub fn limit(mut self, n: usize) -> Self {
        self.limit = Some(n);
        self
    }

    /// Build the SubgraphTemplate.
    pub fn build(self, schema: &Schema) -> Option<SubgraphTemplate> {
        let table_name = crate::query_manager::types::TableName::new(&self.table);
        let table_schema = schema.get(&table_name)?;
        let descriptor = table_schema.descriptor.clone();

        // Build base query
        let mut query_builder = QueryBuilder::new(&self.table);

        for (col, value) in &self.filters {
            query_builder = query_builder.filter_eq(col, value.clone());
        }

        for (col, is_desc) in &self.order_by {
            query_builder = if *is_desc {
                query_builder.order_by_desc(col)
            } else {
                query_builder.order_by(col)
            };
        }

        if let Some(limit) = self.limit {
            query_builder = query_builder.limit(limit);
        }

        if !self.select_columns.is_empty() {
            let cols: Vec<&str> = self.select_columns.iter().map(|s| s.as_str()).collect();
            query_builder = query_builder.select(&cols);
        }

        let base_query = query_builder.build();

        // Build output descriptor (selected columns or all columns)
        let output_descriptor = if self.select_columns.is_empty() {
            descriptor
        } else {
            let columns = self
                .select_columns
                .iter()
                .filter_map(|name| descriptor.columns.iter().find(|c| &c.name == name).cloned())
                .collect();
            RowDescriptor::new(columns)
        };

        Some(SubgraphTemplate::new(
            base_query,
            self.inner_column,
            self.select_columns,
            output_descriptor,
        ))
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;
    use crate::query_manager::types::{ColumnDescriptor, ColumnType, TableName};

    fn test_schema() -> Schema {
        let mut schema = HashMap::new();
        schema.insert(
            TableName::new("posts"),
            RowDescriptor::new(vec![
                ColumnDescriptor::new("id", ColumnType::Integer),
                ColumnDescriptor::new("title", ColumnType::Text),
                ColumnDescriptor::new("author_id", ColumnType::Integer),
            ])
            .into(),
        );
        schema.insert(
            TableName::new("users"),
            RowDescriptor::new(vec![
                ColumnDescriptor::new("id", ColumnType::Integer),
                ColumnDescriptor::new("name", ColumnType::Text),
            ])
            .into(),
        );
        schema
    }

    #[test]
    fn subgraph_builder_creates_template() {
        let schema = test_schema();

        let template = SubgraphBuilder::new("posts")
            .correlate("author_id")
            .select(&["id", "title"])
            .order_by_desc("id")
            .limit(10)
            .build(&schema);

        assert!(template.is_some());
        let template = template.unwrap();
        assert_eq!(template.table(), "posts");
        assert_eq!(template.inner_column(), "author_id");
        assert_eq!(template.output_descriptor().columns.len(), 2);
    }

    #[test]
    fn subgraph_template_instantiates() {
        let schema = test_schema();

        let template = SubgraphBuilder::new("posts")
            .correlate("author_id")
            .build(&schema)
            .unwrap();

        let instance = template.instantiate(Value::Integer(42), &schema);
        assert!(instance.is_some());

        let instance = instance.unwrap();
        assert_eq!(instance.correlation_value, Value::Integer(42));
    }

    #[test]
    fn subgraph_instance_as_array() {
        let schema = test_schema();

        let template = SubgraphBuilder::new("posts")
            .correlate("author_id")
            .build(&schema)
            .unwrap();

        let mut instance = template.instantiate(Value::Integer(1), &schema).unwrap();
        instance.current_results = vec![Value::Integer(10), Value::Integer(20)];

        let array = instance.as_array();
        assert_eq!(
            array,
            Value::Array(vec![Value::Integer(10), Value::Integer(20)])
        );
    }
}
